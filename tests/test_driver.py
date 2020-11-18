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

import argparse
import functools
import os
import shutil
import tempfile
import unittest.mock

from lsst.daf.base import PropertySet
from lsst.pipe.base import DataIdContainer, Struct
import lsst.utils.tests
from lsst.ap.pipe import ApPipeTask
from lsst.ap.verify import pipeline_driver
from lsst.ap.verify.workspace import WorkspaceGen2, WorkspaceGen3


def _getDataIds():
    return [{"visit": 42, "ccd": 0}]


def patchApPipe(method):
    """Shortcut decorator for consistently patching ApPipeTask.
    """
    @functools.wraps(method)
    def wrapper(self, *args, **kwargs):
        parsedCmd = argparse.Namespace()
        parsedCmd.id = DataIdContainer()
        parsedCmd.id.idList = _getDataIds()
        parReturn = Struct(
            argumentParser=None,
            parsedCmd=parsedCmd,
            taskRunner=None,
            resultList=[Struct(exitStatus=0)])
        dbPatcher = unittest.mock.patch("lsst.ap.verify.pipeline_driver.makeApdb")
        pipePatcher = unittest.mock.patch("lsst.ap.pipe.ApPipeTask",
                                          **{"parseAndRun.return_value": parReturn},
                                          _DefaultName=ApPipeTask._DefaultName,
                                          ConfigClass=ApPipeTask.ConfigClass)
        patchedMethod = pipePatcher(dbPatcher(method))
        return patchedMethod(self, *args, **kwargs)
    return wrapper


def patchApPipeGen3(method):
    """Shortcut decorator for consistently patching AP code.
    """
    @functools.wraps(method)
    def wrapper(self, *args, **kwargs):
        parsedCmd = argparse.Namespace()
        parsedCmd.id = DataIdContainer()
        parsedCmd.id.idList = _getDataIds()
        dbPatcher = unittest.mock.patch("lsst.ap.verify.pipeline_driver.makeApdb")
        execPatcher = unittest.mock.patch("lsst.ctrl.mpexec.CmdLineFwk")
        patchedMethod = execPatcher(dbPatcher(method))
        return patchedMethod(self, *args, **kwargs)
    return wrapper


class PipelineDriverTestSuiteGen2(lsst.utils.tests.TestCase):
    def setUp(self):
        self._testDir = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self._testDir, ignore_errors=True)

        # Fake Butler to avoid Workspace initialization overhead
        self.setUpMockPatch("lsst.daf.persistence.Butler", autospec=True)

        self.workspace = WorkspaceGen2(self._testDir)
        self.apPipeArgs = pipeline_driver.ApPipeParser().parse_args(
            ["--id", "visit=%d" % _getDataIds()[0]["visit"]])

    @staticmethod
    def dummyMetadata():
        result = PropertySet()
        result.add("lsst.ap.pipe.ccdProcessor.cycleCount", 42)
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
        mock : `unittest.mock.MagicMock`
            Object representing the same type of entity as ``target``. For
            example, if ``target`` is the name of a class, this method shall
            return a replacement class (rather than a replacement object of
            that class).
        """
        patcher = unittest.mock.patch(target, **kwargs)
        mock = patcher.start()
        self.addCleanup(patcher.stop)
        return mock

    # Mock up ApPipeTask to avoid doing any processing.
    @patchApPipe
    def testrunApPipeGen2Steps(self, mockDb, mockClass):
        """Test that runApPipeGen2 runs the entire pipeline.
        """
        pipeline_driver.runApPipeGen2(self.workspace, self.apPipeArgs)

        mockDb.assert_called_once()
        mockClass.parseAndRun.assert_called_once()

    @patchApPipe
    def testrunApPipeGen2DataIdReporting(self, _mockDb, _mockClass):
        """Test that runApPipeGen2 reports the data IDs that were processed.
        """
        results = pipeline_driver.runApPipeGen2(self.workspace, self.apPipeArgs)
        ids = results.parsedCmd.id

        self.assertEqual(ids.idList, _getDataIds())

    def _getCmdLineArgs(self, parseAndRunArgs):
        if parseAndRunArgs[0]:
            return parseAndRunArgs[0][0]
        elif "args" in parseAndRunArgs[1]:
            return parseAndRunArgs[1]["args"]
        else:
            self.fail("No command-line args passed to parseAndRun!")

    @patchApPipe
    def testrunApPipeGen2CustomConfig(self, _mockDb, mockClass):
        """Test that runApPipeGen2 can pass custom configs from a workspace to ApPipeTask.
        """
        mockParse = mockClass.parseAndRun
        pipeline_driver.runApPipeGen2(self.workspace, self.apPipeArgs)
        mockParse.assert_called_once()
        cmdLineArgs = self._getCmdLineArgs(mockParse.call_args)
        self.assertIn(os.path.join(self.workspace.configDir, "apPipe.py"), cmdLineArgs)

    @patchApPipe
    def testrunApPipeGen2WorkspaceDb(self, mockDb, mockClass):
        """Test that runApPipeGen2 places a database in the workspace location by default.
        """
        mockParse = mockClass.parseAndRun
        pipeline_driver.runApPipeGen2(self.workspace, self.apPipeArgs)

        mockDb.assert_called_once()
        cmdLineArgs = self._getCmdLineArgs(mockDb.call_args)
        self.assertIn("db_url=sqlite:///" + self.workspace.dbLocation, cmdLineArgs)

        mockParse.assert_called_once()
        cmdLineArgs = self._getCmdLineArgs(mockParse.call_args)
        self.assertIn("diaPipe.apdb.db_url=sqlite:///" + self.workspace.dbLocation, cmdLineArgs)

    @patchApPipe
    def testrunApPipeGen2WorkspaceDbCustom(self, mockDb, mockClass):
        """Test that runApPipeGen2 places a database in the specified location.
        """
        self.apPipeArgs.db = "postgresql://somebody@pgdb.misc.org/custom_db"
        mockParse = mockClass.parseAndRun
        pipeline_driver.runApPipeGen2(self.workspace, self.apPipeArgs)

        mockDb.assert_called_once()
        cmdLineArgs = self._getCmdLineArgs(mockDb.call_args)
        self.assertIn("db_url=" + self.apPipeArgs.db, cmdLineArgs)

        mockParse.assert_called_once()
        cmdLineArgs = self._getCmdLineArgs(mockParse.call_args)
        self.assertIn("diaPipe.apdb.db_url=" + self.apPipeArgs.db, cmdLineArgs)

    @patchApPipe
    def testrunApPipeGen2Reuse(self, _mockDb, mockClass):
        """Test that runApPipeGen2 does not run the pipeline at all (not even with
        --reuse-outputs-from) if --skip-pipeline is provided.
        """
        mockParse = mockClass.parseAndRun
        skipArgs = pipeline_driver.ApPipeParser().parse_args(["--skip-pipeline"])
        pipeline_driver.runApPipeGen2(self.workspace, skipArgs)
        mockParse.assert_not_called()


class PipelineDriverTestSuiteGen3(lsst.utils.tests.TestCase):
    def setUp(self):
        self._testDir = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self._testDir, ignore_errors=True)

        # Fake Butler to avoid Workspace initialization overhead
        self.setUpMockPatch("lsst.daf.butler.Registry",
                            **{"queryDatasets.return_value": []})
        self.setUpMockPatch("lsst.daf.butler.Butler")

        self.workspace = WorkspaceGen3(self._testDir)
        self.apPipeArgs = pipeline_driver.ApPipeParser().parse_args(
            ["--id", "visit = %d" % _getDataIds()[0]["visit"]])

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

    @unittest.skip("Fix test in DM-27117")
    # Mock up CmdLineFwk to avoid doing any processing.
    @patchApPipeGen3
    def testrunApPipeGen3Steps(self, mockDb, mockFwk):
        """Test that runApPipeGen3 runs the entire pipeline.
        """
        pipeline_driver.runApPipeGen3(self.workspace, self.apPipeArgs)

        mockDb.assert_called_once()
        mockFwk().parseAndRun.assert_called_once()

    def _getCmdLineArgs(self, parseAndRunArgs):
        if parseAndRunArgs[0]:
            return parseAndRunArgs[0][0]
        elif "args" in parseAndRunArgs[1]:
            return parseAndRunArgs[1]["args"]
        else:
            self.fail("No command-line args passed to parseAndRun!")

    @unittest.skip("Fix test in DM-27117")
    @patchApPipeGen3
    def testrunApPipeGen3WorkspaceDb(self, mockDb, mockFwk):
        """Test that runApPipeGen3 places a database in the workspace location by default.
        """
        pipeline_driver.runApPipeGen3(self.workspace, self.apPipeArgs)

        mockDb.assert_called_once()
        cmdLineArgs = self._getCmdLineArgs(mockDb.call_args)
        self.assertIn("db_url=sqlite:///" + self.workspace.dbLocation, cmdLineArgs)

        mockParse = mockFwk().parseAndRun
        mockParse.assert_called_once()
        cmdLineArgs = self._getCmdLineArgs(mockParse.call_args)
        self.assertIn("diaPipe:apdb.db_url=sqlite:///" + self.workspace.dbLocation, cmdLineArgs)

    @unittest.skip("Fix test in DM-27117")
    @patchApPipeGen3
    def testrunApPipeGen3WorkspaceCustom(self, mockDb, mockFwk):
        """Test that runApPipeGen3 places a database in the specified location.
        """
        self.apPipeArgs.db = "postgresql://somebody@pgdb.misc.org/custom_db"
        pipeline_driver.runApPipeGen3(self.workspace, self.apPipeArgs)

        mockDb.assert_called_once()
        cmdLineArgs = self._getCmdLineArgs(mockDb.call_args)
        self.assertIn("db_url=" + self.apPipeArgs.db, cmdLineArgs)

        mockParse = mockFwk().parseAndRun
        mockParse.assert_called_once()
        cmdLineArgs = self._getCmdLineArgs(mockParse.call_args)
        self.assertIn("diaPipe:apdb.db_url=" + self.apPipeArgs.db, cmdLineArgs)

    @unittest.skip("Fix test in DM-27117")
    @patchApPipeGen3
    def testrunApPipeGen3Reuse(self, _mockDb, mockFwk):
        """Test that runApPipeGen2 does not run the pipeline at all (not even with
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
