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

import lsst.utils.tests
from lsst.obs.base import RawIngestTask, DefineVisitsTask
from lsst.ap.verify import pipeline_driver
from lsst.ap.verify.testUtils import DataTestCase
from lsst.ap.verify import Dataset, WorkspaceGen3


def patchApPipeGen3(method):
    """Shortcut decorator for consistently patching AP code.
    """
    @functools.wraps(method)
    def wrapper(self, *args, **kwargs):
        dbPatcher = unittest.mock.patch("lsst.ap.verify.pipeline_driver._makeApdb")
        pipePatcher = unittest.mock.patch("lsst.ap.verify.pipeline_driver.subprocess")
        patchedMethod = dbPatcher(pipePatcher(method))
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
        self.dataset.makeCompatibleRepoGen3(self.workspace.repo, "sasquatch", None)
        raws = [os.path.join(self.dataset.rawLocation, "lsst_a_204595_R11_S01_i.fits")]
        rawIngest = RawIngestTask(butler=self.workspace.workButler, config=RawIngestTask.ConfigClass())
        rawIngest.run(raws, run=None)
        defineVisit = DefineVisitsTask(butler=self.workspace.workButler,
                                       config=DefineVisitsTask.ConfigClass())
        defineVisit.run(self.workspace.workButler.registry.queryDataIds("exposure", datasets="raw"))
        self.apPipeArgs = pipeline_driver.ApPipeParser().parse_args(["--pipeline", "foo.yaml"])

    def _getArgs(self, call_args):
        if call_args.args:
            return call_args.args[1]
        elif "args" in call_args.kwargs:
            return call_args.kwargs["args"]
        else:
            self.fail(f"No APDB args passed to {call_args}!")

    @patchApPipeGen3
    def testrunApPipeGen3WorkspaceDb(self, _mockPipe, mockDb):
        """Test that runApPipeGen3 places a database in the workspace location by default.
        """
        pipeline_driver.runApPipeGen3(self.workspace, self.apPipeArgs)

        mockDb.assert_called_once()
        dbArgs = self._getArgs(mockDb.call_args)
        self.assertIn("db_url", dbArgs)
        self.assertEqual(dbArgs["db_url"], "sqlite:///" + self.workspace.dbLocation)

    @patchApPipeGen3
    def testrunApPipeGen3WorkspaceCustom(self, _mockPipe, mockDb):
        """Test that runApPipeGen3 places a database in the specified location.
        """
        self.apPipeArgs.db = "postgresql://somebody@pgdb.misc.org/custom_db"
        pipeline_driver.runApPipeGen3(self.workspace, self.apPipeArgs)

        mockDb.assert_called_once()
        dbArgs = self._getArgs(mockDb.call_args)
        self.assertIn("db_url", dbArgs)
        self.assertEqual(dbArgs["db_url"], self.apPipeArgs.db)

    @patchApPipeGen3
    def testrunApPipeGen3Reuse(self, mockPipe, _mockDb):
        """Test that runApPipeGen3 does not run the pipeline at all (not even with
        --skip-existing) if --skip-pipeline is provided.
        """
        skipArgs = pipeline_driver.ApPipeParser().parse_args(["--skip-pipeline"])
        pipeline_driver.runApPipeGen3(self.workspace, skipArgs)
        mockPipe.assert_not_called()


class MemoryTester(lsst.utils.tests.MemoryTestCase):
    pass


def setup_module(module):
    lsst.utils.tests.init()


if __name__ == "__main__":
    lsst.utils.tests.init()
    unittest.main()
