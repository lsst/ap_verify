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

import astropy.units as u

from lsst.daf.base import PropertySet
import lsst.utils.tests
import lsst.verify
import lsst.obs.test
from lsst.ap.pipe import ApPipeTask
from lsst.ap.verify import pipeline_driver
from lsst.ap.verify.workspace import Workspace


def patchApPipe(method):
    """Shortcut decorator for consistently patching ApPipeTask.
    """
    @functools.wraps(method)
    def wrapper(self, *args, **kwargs):
        patcher = unittest.mock.patch("lsst.ap.pipe.ApPipeTask",
                                      autospec=True,
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
        self.setUpMockPatch("lsst.daf.persistence.searchDataRefs", return_value=[{"visit": 42, "ccd": 0}])

        self.job = lsst.verify.Job()
        self.workspace = Workspace(self._testDir)
        self.apPipeArgs = pipeline_driver.ApPipeParser().parse_args(["--id", "visit=42"])

        self.subtaskJob = lsst.verify.Job(measurements=[lsst.verify.Measurement("ip_isr.IsrTime", 2.0 * u.s)])

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
        # This test case is sensitive to the implementation of pipeline_driver
        # Specifically, it needs to know that ApPipeTask.run is not called
        pipeline_driver.runApPipe(self.job, self.workspace, self.apPipeArgs)

        mockClass.return_value.runProcessCcd.assert_called_once()
        mockClass.return_value.runDiffIm.assert_called_once()
        mockClass.return_value.runAssociation.assert_called_once()

    def testUpdateMetricsEmpty(self):
        """Test that _updateMetrics does not add metrics if no job files are provided.
        """
        metadata = PipelineDriverTestSuite.dummyMetadata()

        pipeline_driver._updateMetrics(metadata, self.job)

        self.assertFalse(self.job.measurements)

    def testUpdateMetricsReal(self):
        """Test that _updateMetrics can load metrics when given temporary Job files.
        """
        subtaskFile = os.path.join(self._testDir, "ccdProcessor.persist")
        self.subtaskJob.write(subtaskFile)
        metadata = PipelineDriverTestSuite.dummyMetadata()
        metadata.add("lsst.ap.pipe.ccdProcessor.verify_json_path", subtaskFile)

        self.assertNotEqual(self.job.measurements, self.subtaskJob.measurements)

        pipeline_driver._updateMetrics(metadata, self.job)

        self.assertEqual(self.job.measurements, self.subtaskJob.measurements)

    # Mock up ApPipeTask to avoid doing any processing.
    @unittest.mock.patch("lsst.ap.verify.pipeline_driver._getConfig", return_value=None)
    @patchApPipe
    def testUpdateMetricsOnError(self, _mockConfig, mockClass):
        """Test that runApPipe stores metrics in a job even when the pipeline fails.
        """
        subtaskFile = os.path.join(self._testDir, "ccdProcessor.persist")
        self.subtaskJob.write(subtaskFile)
        metadata = PipelineDriverTestSuite.dummyMetadata()
        metadata.add("lsst.ap.pipe.ccdProcessor.verify_json_path", subtaskFile)

        mockClass.return_value.getFullMetadata.return_value = metadata
        mockClass.return_value.runDiffIm.side_effect = RuntimeError("DECam is weird!")

        self.assertNotEqual(self.job.measurements, self.subtaskJob.measurements)

        with self.assertRaises(RuntimeError):
            pipeline_driver.runApPipe(self.job, self.workspace, self.apPipeArgs)

        self.assertEqual(self.job.measurements, self.subtaskJob.measurements)

    def testRunApPipeCustomConfig(self):
        """Test that runApPipe can pass custom configs from a workspace to ApPipeTask.
        """
        configFile = os.path.join(self.workspace.configDir, "apPipe.py")
        with open(configFile, "w") as f:
            # Illegal value; would never be set by a real config
            f.write("config.differencer.doWriteSources = False\n")
            f.write("config.ppdb.db_url = 'sqlite://'\n")

        task = self.setUpMockPatch("lsst.ap.pipe.ApPipeTask",
                                   spec=True,
                                   new_callable=InitRecordingMock,
                                   _DefaultName=ApPipeTask._DefaultName,
                                   ConfigClass=ApPipeTask.ConfigClass).return_value

        pipeline_driver.runApPipe(self.job, self.workspace, self.apPipeArgs)
        initCalls = (c for c in task.mock_calls if c.name == "__init__")
        for call in initCalls:
            kwargs = call[2]
            self.assertIn("config", kwargs)
            taskConfig = kwargs["config"]
            self.assertFalse(taskConfig.differencer.doWriteSources)
            self.assertNotEqual(taskConfig.ppdb.db_url, "sqlite:///" + self.workspace.dbLocation)

    def testRunApPipeWorkspaceDb(self):
        """Test that runApPipe places a database in the workspace location by default.
        """
        task = self.setUpMockPatch("lsst.ap.pipe.ApPipeTask",
                                   spec=True,
                                   new_callable=InitRecordingMock,
                                   _DefaultName=ApPipeTask._DefaultName,
                                   ConfigClass=ApPipeTask.ConfigClass).return_value

        pipeline_driver.runApPipe(self.job, self.workspace, self.apPipeArgs)
        initCalls = (c for c in task.mock_calls if c.name == "__init__")
        for call in initCalls:
            kwargs = call[2]
            self.assertIn("config", kwargs)
            taskConfig = kwargs["config"]
            self.assertEqual(taskConfig.ppdb.db_url, "sqlite:///" + self.workspace.dbLocation)


class MemoryTester(lsst.utils.tests.MemoryTestCase):
    pass


def setup_module(module):
    lsst.utils.tests.init()


if __name__ == "__main__":
    lsst.utils.tests.init()
    unittest.main()
