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
import lsst.obs.test
from lsst.ap.pipe import ApPipeTask
from lsst.ap.verify import pipeline_driver
from lsst.ap.verify.workspace import Workspace


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
            resultList=[None])
        patcher = unittest.mock.patch("lsst.ap.pipe.ApPipeTask",
                                      **{"parseAndRun.return_value": parReturn},
                                      _DefaultName=ApPipeTask._DefaultName,
                                      ConfigClass=ApPipeTask.ConfigClass)
        patchedMethod = patcher(method)
        return patchedMethod(self, *args, **kwargs)
    return wrapper


class InitRecordingMock(unittest.mock.MagicMock):
    """A MagicMock for classes that records requests for objects of that class.

    Because ``__init__`` cannot be mocked directly, the calls cannot be
    identified with the usual ``object.method`` syntax. Instead, filter the
    object's calls for a ``name`` attribute equal to ``__init__``.
    """
    def __call__(self, *args, **kwargs):
        # super() unsafe because MagicMock does not guarantee support
        instance = unittest.mock.MagicMock.__call__(self, *args, **kwargs)
        initCall = unittest.mock.call(*args, **kwargs)
        initCall.name = "__init__"
        instance.mock_calls.append(initCall)
        return instance


class PipelineDriverTestSuite(lsst.utils.tests.TestCase):
    def setUp(self):
        self._testDir = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self._testDir, ignore_errors=True)

        # Fake Butler to avoid Workspace initialization overhead
        butler = self.setUpMockPatch("lsst.daf.persistence.Butler", autospec=True)
        butler.getMapperClass.return_value = lsst.obs.test.TestMapper
        self.dataIds = _getDataIds()
        dataRef = self.setUpMockPatch("lsst.daf.persistence.ButlerDataRef",
                                      autospec=True, dataId=self.dataIds[0])
        self.setUpMockPatch("lsst.daf.persistence.searchDataRefs", return_value=[dataRef])

        self.workspace = Workspace(self._testDir)
        self.apPipeArgs = pipeline_driver.ApPipeParser().parse_args(["--id", "visit=42"])

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
    @unittest.mock.patch("lsst.ap.verify.pipeline_driver._getConfig", return_value=None)
    @patchApPipe
    def testRunApPipeSteps(self, _mockConfig, mockClass):
        """Test that runApPipe runs the entire pipeline.
        """
        pipeline_driver.runApPipe(self.workspace, self.apPipeArgs)

        mockClass.parseAndRun.assert_called_once()

    @unittest.mock.patch("lsst.ap.verify.pipeline_driver._getConfig", return_value=None)
    @patchApPipe
    def testRunApPipeDataIdReporting(self, _mockConfig, mockClass):
        """Test that runApPipe reports the data IDs that were processed.
        """
        ids = pipeline_driver.runApPipe(self.workspace, self.apPipeArgs)

        self.assertEqual(ids.idList, self.dataIds)

    def _getCmdLineArgs(self, parseAndRunArgs):
        if parseAndRunArgs[0]:
            return parseAndRunArgs[0][0]
        elif "args" in parseAndRunArgs[1]:
            return parseAndRunArgs[1]["args"]
        else:
            self.fail("No command-line args passed to parseAndRun!")

    def testRunApPipeCustomConfig(self):
        """Test that runApPipe can pass custom configs from a workspace to ApPipeTask.
        """
        with unittest.mock.patch.object(ApPipeTask, "parseAndRun") as mockParse:
            pipeline_driver.runApPipe(self.workspace, self.apPipeArgs)
            mockParse.assert_called_once()
            cmdLineArgs = self._getCmdLineArgs(mockParse.call_args)
            self.assertIn(os.path.join(self.workspace.configDir, "apPipe.py"), cmdLineArgs)

    def testRunApPipeWorkspaceDb(self):
        """Test that runApPipe places a database in the workspace location by default.
        """
        with unittest.mock.patch.object(ApPipeTask, "parseAndRun") as mockParse:
            pipeline_driver.runApPipe(self.workspace, self.apPipeArgs)
            mockParse.assert_called_once()
            cmdLineArgs = self._getCmdLineArgs(mockParse.call_args)
            self.assertIn("ppdb.db_url=sqlite:///" + self.workspace.dbLocation, cmdLineArgs)


class MemoryTester(lsst.utils.tests.MemoryTestCase):
    pass


def setup_module(module):
    lsst.utils.tests.init()


if __name__ == "__main__":
    lsst.utils.tests.init()
    unittest.main()
