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

import os
import shutil
import tempfile
import unittest.mock

from lsst.utils import getPackageDir
import lsst.utils.tests
import lsst.pipe.tasks as pipeTasks
import lsst.pex.exceptions as pexExcept
import lsst.obs.test
from lsst.ap.verify import ingestion


class IngestionTestSuite(lsst.utils.tests.TestCase):

    @classmethod
    def setUpClass(cls):
        try:
            cls.testData = os.path.join(lsst.utils.getPackageDir("obs_test"), 'data', 'input')
        except pexExcept.NotFoundError:
            message = "obs_test not setup. Skipping."
            raise unittest.SkipTest(message)

        obsDir = os.path.join(getPackageDir('obs_test'), 'config')
        cls.config = ingestion.DatasetIngestConfig()
        cls.config.dataIngester.load(os.path.join(obsDir, 'ingest.py'))
        cls.config.calibIngester.load(os.path.join(obsDir, 'ingestCalibs.py'))
        cls.config.defectIngester.load(os.path.join(obsDir, 'ingestCalibs.py'))
        cls.config.freeze()

        cls.testApVerifyData = os.path.join('tests', 'ingestion')
        cls.rawDataId = {'visit': 229388, 'ccdnum': 1}

        cls.rawData = [{'file': 'raw_v1_fg.fits.gz', 'visit': 890104911, 'filter': 'g', 'exptime': 15.0},
                       {'file': 'raw_v2_fg.fits.gz', 'visit': 890106021, 'filter': 'g', 'exptime': 15.0},
                       {'file': 'raw_v3_fr.fits.gz', 'visit': 890880321, 'filter': 'r', 'exptime': 15.0},
                       ]
        cls.calibData = [{'type': 'bias', 'file': 'bias.fits.gz', 'filter': '_unknown_',
                          'date': '1999-01-17'},
                         {'type': 'flat', 'file': 'flat_fg.fits.gz', 'filter': 'g', 'date': '1999-01-17'},
                         {'type': 'flat', 'file': 'flat_fr.fits.gz', 'filter': 'r', 'date': '1999-01-17'},
                         ]

    def setUp(self):
        # Repositories still get used by IngestTask despite Butler being a mock object
        self._repo = self._calibRepo = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self._repo, ignore_errors=True)

        # Fake Butler and RegisterTask to avoid initialization or DB overhead
        def mockGet(datasetType, dataId=None):
            """Minimally fake a butler.get().
            """
            if "raw_filename" in datasetType:
                matchingFiles = [datum['file'] for datum in IngestionTestSuite.rawData
                                 if datum['visit'] == dataId['visit']]
                return [os.path.join(self._repo, file) for file in matchingFiles]
            elif "bias_filename" in datasetType:
                matchingFiles = [datum['file'] for datum in IngestionTestSuite.calibData
                                 if datum['type'] == 'bias']
                return [os.path.join(self._repo, file) for file in matchingFiles]
            elif "flat_filename" in datasetType:
                matchingFiles = [datum['file'] for datum in IngestionTestSuite.calibData
                                 if datum['type'] == 'flat' and datum['filter'] == dataId['filter']]
                return [os.path.join(self._repo, file) for file in matchingFiles]
            else:
                return None

        butlerPatcher = unittest.mock.patch("lsst.daf.persistence.Butler", autospec=True)
        self._butler = butlerPatcher.start()
        self._butler.getMapperClass.return_value = lsst.obs.test.TestMapper
        self._butler.return_value.get = mockGet
        self.addCleanup(butlerPatcher.stop)

        self._task = ingestion.DatasetIngestTask(config=IngestionTestSuite.config)

    def setUpRawRegistry(self):
        """Mock up the RegisterTask used for ingesting raw data.

        This method initializes ``self._registerTask`` and ``self._registryHandle``. It should be
        called at the start of any test case that attempts raw ingestion.

        Behavior is undefined if both `setUpRawRegistry` and `setUpCalibRegistry` are called.
        """
        patcherRegister = unittest.mock.patch.object(self._task.dataIngester, "register",
                                                     spec=pipeTasks.ingest.RegisterTask,
                                                     new_callable=unittest.mock.NonCallableMagicMock)
        self._registerTask = patcherRegister.start()
        self.addCleanup(patcherRegister.stop)
        # the mocked entry point of the Registry context manager, needed for querying the registry
        self._registryHandle = self._registerTask.openRegistry().__enter__()

    def setUpCalibRegistry(self):
        """Mock up the RegisterTask used for ingesting calib data.

        This method initializes ``self._registerTask`` and ``self._registryHandle``. It should be
        called at the start of any test case that attempts calib ingestion.

        Behavior is undefined if both `setUpRawRegistry` and `setUpCalibRegistry` are called.
        """
        patcherRegister = unittest.mock.patch.object(self._task.calibIngester, "register",
                                                     spec=pipeTasks.ingestCalibs.CalibsRegisterTask,
                                                     new_callable=unittest.mock.NonCallableMagicMock)
        self._registerTask = patcherRegister.start()
        self._registerTask.config = self._task.config.calibIngester.register
        self.addCleanup(patcherRegister.stop)
        # the mocked entry point of the Registry context manager, needed for querying the registry
        self._registryHandle = self._registerTask.openRegistry().__enter__()

    def testDataIngest(self):
        """Test that ingesting a science image adds it to a repository.
        """
        self.setUpRawRegistry()
        testDir = os.path.join(IngestionTestSuite.testData, 'raw')
        files = [os.path.join(testDir, datum['file']) for datum in IngestionTestSuite.rawData]
        self._task._doIngestRaws(self._repo, files, [])

        for datum in IngestionTestSuite.rawData:
            # TODO: find a way to avoid having to know exact data ID expansion
            dataId = {'visit': datum['visit'], 'expTime': datum['exptime'], 'filter': datum['filter']}
            # TODO: I don't think we actually care about the keywords -- especially since they're defaults
            self._registerTask.addRow.assert_any_call(self._registryHandle, dataId,
                                                      create=False, dryrun=False)
        self.assertEqual(self._registerTask.addRow.call_count, len(IngestionTestSuite.rawData))

    def testCalibIngest(self):
        """Test that ingesting calibrations adds them to a repository.
        """
        files = [os.path.join(IngestionTestSuite.testData, datum['type'], datum['file'])
                 for datum in IngestionTestSuite.calibData]
        self.setUpCalibRegistry()

        self._task._doIngestCalibs(self._repo, self._calibRepo, files)

        for datum in IngestionTestSuite.calibData:
            # TODO: find a way to avoid having to know exact data ID expansion
            dataId = {'calibDate': datum['date'], 'filter': datum['filter']}
            # TODO: I don't think we actually care about the keywords -- especially since they're defaults
            self._registerTask.addRow.assert_any_call(self._registryHandle, dataId,
                                                      create=False, dryrun=False, table=datum['type'])
        self.assertEqual(self._registerTask.addRow.call_count, len(IngestionTestSuite.rawData))

    def testNoFileIngest(self):
        """Test that attempts to ingest nothing raise an exception.
        """
        files = []
        self.setUpRawRegistry()

        with self.assertRaises(RuntimeError):
            self._task._doIngestRaws(self._repo, files, [])
        with self.assertRaises(RuntimeError):
            self._task._doIngestCalibs(self._repo, self._calibRepo, files)

        self._registerTask.addRow.assert_not_called()

    def testBadFileIngest(self):
        """Test that ingestion of raw data ignores blacklisted files.
        """
        badFiles = ['raw_v2_fg.fits.gz']
        self.setUpRawRegistry()

        testDir = os.path.join(IngestionTestSuite.testData, 'raw')
        files = [os.path.join(testDir, datum['file']) for datum in IngestionTestSuite.rawData]
        self._task._doIngestRaws(self._repo, files, badFiles)

        for datum in IngestionTestSuite.rawData:
            dataId = {'visit': datum['visit'], 'expTime': datum['exptime'], 'filter': datum['filter']}
            call = unittest.mock.call(self._registryHandle, dataId, create=False, dryrun=False)
            if datum['file'] not in badFiles:
                self.assertIn(call, self._registerTask.addRow.mock_calls)
            else:
                self.assertNotIn(call, self._registerTask.addRow.mock_calls)
        self.assertEqual(self._registerTask.addRow.call_count,
                         len(IngestionTestSuite.rawData) - len(badFiles))

    def testFindMatchingFiles(self):
        """Test that _findMatchingFiles finds the desired files.
        """
        testDir = os.path.join(IngestionTestSuite.testData)

        self.assertSetEqual(
            ingestion._findMatchingFiles(testDir, ['raw_*.fits.gz']),
            {os.path.join(testDir, f) for f in
             {'raw/raw_v1_fg.fits.gz', 'raw/raw_v2_fg.fits.gz', 'raw/raw_v3_fr.fits.gz'}}
        )
        self.assertSetEqual(
            ingestion._findMatchingFiles(testDir, ['raw_*.fits.gz'], ['*fr*']),
            {os.path.join(testDir, f) for f in {'raw/raw_v1_fg.fits.gz', 'raw/raw_v2_fg.fits.gz'}}
        )
        self.assertSetEqual(
            ingestion._findMatchingFiles(testDir, ['raw_*.fits.gz'], ['*_v?_f?.fits.gz']),
            set()
        )


class MemoryTester(lsst.utils.tests.MemoryTestCase):
    pass


def setup_module(module):
    lsst.utils.tests.init()


if __name__ == "__main__":
    lsst.utils.tests.init()
    unittest.main()
