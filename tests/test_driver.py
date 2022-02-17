#
# This file is part of ap_verify.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (http://www.lsst.org).
# See the COPYRIGHT file at the top-level directory of this distribution
# for details of code ownership.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
#

import functools
import os
import shutil
import tempfile
import unittest.mock

from lsst.daf.base import PropertySet
import lsst.utils.tests
from lsst.obs.base import RawIngestTask, DefineVisitsTask
from lsst.ap.verify import pipeline_driver
from lsst.ap.verify.testUtils import DataTestCase
from lsst.ap.verify import Dataset, WorkspaceGen3


TESTDIR = os.path.abspath(os.path.dirname(__file__))


def _getDataIds(butler):
    return list(butler.registry.queryDataIds({"instrument", "visit", "detector"}, datasets="raw"))


def patchApPipeGen3(method):
    """Shortcut decorator for consistently patching AP code.
    """
    @functools.wraps(method)
    def wrapper(self, *args, **kwargs):
        dbPatcher = unittest.mock.patch("lsst.ap.verify.pipeline_driver.makeApdb")
        execPatcher = unittest.mock.patch("lsst.ctrl.mpexec.CmdLineFwk")
        patchedMethod = execPatcher(dbPatcher(method))
        return patchedMethod(self, *args, **kwargs)
    return wrapper


class PipelineDriverTestSuiteGen3(DataTestCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()

        cls.dataset = Dataset(cls.testDataset)

    def setUp(self):
        super().setUp()

        self._testDir = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self._testDir, ignore_errors=True)

        self.workspace = WorkspaceGen3(self._testDir)
        self.dataset.makeCompatibleRepoGen3(self.workspace.repo)
        raws = [os.path.join(self.dataset.rawLocation, "lsst_a_204595_R11_S01_i.fits")]
        rawIngest = RawIngestTask(butler=self.workspace.workButler, config=RawIngestTask.ConfigClass())
        rawIngest.run(raws, run=None)
        defineVisit = DefineVisitsTask(butler=self.workspace.workButler,
                                       config=DefineVisitsTask.ConfigClass())
        defineVisit.run(self.workspace.workButler.registry.queryDataIds("exposure", datasets="raw"))
        ids = _getDataIds(self.workspace.workButler)
        self.apPipeArgs = pipeline_driver.ApPipeParser().parse_args(
            ["--data-query", f"instrument = '{ids[0]['instrument']}' AND visit = {ids[0]['visit']}",
             "--pipeline", os.path.join(TESTDIR, "MockApPipe.yaml")])

    @staticmethod
    def dummyMetadata():
        result = PropertySet()
        result.add("lsst.pipe.base.calibrate.cycleCount", 42)
        return result

    def setUpMockPatch(self, target, **kwargs):
        """Create and register a patcher for a test suite.

        The patching process is guaranteed to avoid resource leaks or
        side effects lasting beyond the test case that calls this method.

        Parameters
        ----------
        target : `str`
            The target to patch. Must obey all restrictions listed
            for the ``target`` parameter of `unittest.mock.patch`.
        kwargs : any
            Any keyword arguments that are allowed for `unittest.mock.patch`,
            particularly optional attributes for a `unittest.mock.Mock`.

        Returns
        -------
        mock : `unittest.mock.Mock`
            Object representing the same type of entity as ``target``. For
            example, if ``target`` is the name of a class, this method shall
            return a replacement class (rather than a replacement object of
            that class).
        """
        patcher = unittest.mock.patch(target, **kwargs)
        mock = patcher.start()
        self.addCleanup(patcher.stop)
        return mock

    def testrunApPipeGen3Steps(self):
        """Test that runApPipeGen3 runs the entire pipeline.
        """
        pipeline_driver.runApPipeGen3(self.workspace, self.apPipeArgs)

        # Use datasets as a proxy for pipeline completion
        id = _getDataIds(self.workspace.analysisButler)[0]
        self.assertTrue(self.workspace.analysisButler.datasetExists("calexp", id))
        self.assertTrue(self.workspace.analysisButler.datasetExists("src", id))
        self.assertTrue(self.workspace.analysisButler.datasetExists("goodSeeingDiff_differenceExp", id))
        self.assertTrue(self.workspace.analysisButler.datasetExists("goodSeeingDiff_diaSrc", id))
        self.assertTrue(self.workspace.analysisButler.datasetExists("apdb_marker", id))
        self.assertTrue(self.workspace.analysisButler.datasetExists("goodSeeingDiff_assocDiaSrc", id))

    def _getCmdLineArgs(self, parseAndRunArgs):
        if parseAndRunArgs[0]:
            return parseAndRunArgs[0][0]
        elif "args" in parseAndRunArgs[1]:
            return parseAndRunArgs[1]["args"]
        else:
            self.fail("No command-line args passed to parseAndRun!")

    @patchApPipeGen3
    def testrunApPipeGen3WorkspaceDb(self, mockDb, _mockFwk):
        """Test that runApPipeGen3 places a database in the workspace location by default.
        """
        pipeline_driver.runApPipeGen3(self.workspace, self.apPipeArgs)

        # Test the call to make_apdb.py
        mockDb.assert_called_once()
        cmdLineArgs = self._getCmdLineArgs(mockDb.call_args)
        self.assertIn("db_url=sqlite:///" + self.workspace.dbLocation, cmdLineArgs)

        # Test the call to the AP pipeline
        id = _getDataIds(self.workspace.analysisButler)[0]
        apdbConfig = self.workspace.analysisButler.get("apdb_marker", id)
        self.assertEqual(apdbConfig.db_url, "sqlite:///" + self.workspace.dbLocation)

    @patchApPipeGen3
    def testrunApPipeGen3WorkspaceCustom(self, mockDb, _mockFwk):
        """Test that runApPipeGen3 places a database in the specified location.
        """
        self.apPipeArgs.db = "postgresql://somebody@pgdb.misc.org/custom_db"
        pipeline_driver.runApPipeGen3(self.workspace, self.apPipeArgs)

        # Test the call to make_apdb.py
        mockDb.assert_called_once()
        cmdLineArgs = self._getCmdLineArgs(mockDb.call_args)
        self.assertIn("db_url=" + self.apPipeArgs.db, cmdLineArgs)

        # Test the call to the AP pipeline
        id = _getDataIds(self.workspace.analysisButler)[0]
        apdbConfig = self.workspace.analysisButler.get("apdb_marker", id)
        self.assertEqual(apdbConfig.db_url, self.apPipeArgs.db)

    @unittest.skip("Fix test in DM-27117")
    @patchApPipeGen3
    def testrunApPipeGen3Reuse(self, _mockDb, mockFwk):
        """Test that runApPipeGen3 does not run the pipeline at all (not even with
        --skip-existing) if --skip-pipeline is provided.
        """
        skipArgs = pipeline_driver.ApPipeParser().parse_args(["--skip-pipeline"])
        pipeline_driver.runApPipeGen3(self.workspace, skipArgs)
        mockFwk().parseAndRun.assert_not_called()


class MemoryTester(lsst.utils.tests.MemoryTestCase):
    pass


def setup_module(module):
    lsst.utils.tests.init()


if __name__ == "__main__":
    lsst.utils.tests.init()
    unittest.main()
