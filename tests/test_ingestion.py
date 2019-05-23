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
from lsst.obs.base.read_defects import read_all_defects
from lsst.ap.verify import ingestion
from lsst.ap.verify.dataset import Dataset
from lsst.ap.verify.workspace import Workspace


class MockDetector(object):
    def getName(self):
        return '0'

    def getId(self):
        return 0


class MockCamera(object):
    def __init__(self, detector):
        self.det_list = [detector, ]
        self.det_dict = {'0': detector}

    def __getitem__(self, item):
        if type(item) is int:
            return self.det_list[item]
        else:
            return self.det_dict[item]


class IngestionTestSuite(lsst.utils.tests.TestCase):

    @classmethod
    def setUpClass(cls):
        try:
            cls.testData = os.path.join(lsst.utils.getPackageDir("obs_test"), 'data', 'input')
        except pexExcept.NotFoundError:
            message = "obs_test not setup. Skipping."
            raise unittest.SkipTest(message)

        cls.mockCamera = MockCamera(MockDetector())
        cls.config = cls.makeTestConfig()
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

    @staticmethod
    def makeTestConfig():
        obsDir = os.path.join(getPackageDir('obs_test'), 'config')
        config = ingestion.DatasetIngestConfig()
        config.dataIngester.load(os.path.join(obsDir, 'ingest.py'))
        config.calibIngester.load(os.path.join(obsDir, 'ingestCalibs.py'))
        config.defectIngester.load(os.path.join(obsDir, 'ingestDefects.py'))
        return config

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
            elif "defects_filename" in datasetType:
                return [os.path.join(self._repo, 'defects', 'defects.fits'), ]
            elif "camera" in datasetType:
                return IngestionTestSuite.mockCamera
            else:
                return None

        butlerPatcher = unittest.mock.patch("lsst.daf.persistence.Butler", autospec=True)
        self._butler = butlerPatcher.start()
        self._butler.getMapperClass.return_value = lsst.obs.test.TestMapper
        self._butler.return_value.get = mockGet
        self.addCleanup(butlerPatcher.stop)

        # Fake Dataset and Workspace because it's too hard to make real ones
        self._dataset = unittest.mock.NonCallableMock(
            spec=Dataset,
            rawLocation=os.path.join(IngestionTestSuite.testData, 'raw'),
            defectLocation=os.path.join(getPackageDir('obs_test_data'), 'test', 'defects')
        )
        self._workspace = unittest.mock.NonCallableMock(
            spec=Workspace,
            dataRepo=self._repo,
            calibRepo=self._calibRepo,
        )

        self._task = ingestion.DatasetIngestTask(config=IngestionTestSuite.config)

    def setUpRawRegistry(self):
        """Mock up the RegisterTask used for ingesting raw data.

        This method initializes ``self._registerTask``. It should be
        called at the start of any test case that attempts raw ingestion.

        Behavior is undefined if more than one of `setUpRawRegistry`, `setUpCalibRegistry`,
        or `setupDefectRegistry` is called.
        """
        patcherRegister = unittest.mock.patch.object(self._task.dataIngester, "register",
                                                     spec=pipeTasks.ingest.RegisterTask,
                                                     new_callable=unittest.mock.NonCallableMagicMock)
        self._registerTask = patcherRegister.start()
        self.addCleanup(patcherRegister.stop)

    def setUpCalibRegistry(self):
        """Mock up the RegisterTask used for ingesting calib data.

        This method initializes ``self._registerTask``. It should be
        called at the start of any test case that attempts calib ingestion.

        Behavior is undefined if more than one of `setUpRawRegistry`, `setUpCalibRegistry`,
        or `setupDefectRegistry` is called.
        """
        patcherRegister = unittest.mock.patch.object(self._task.calibIngester, "register",
                                                     spec=pipeTasks.ingestCalibs.CalibsRegisterTask,
                                                     new_callable=unittest.mock.NonCallableMagicMock)
        self._registerTask = patcherRegister.start()
        self._registerTask.config = self._task.config.calibIngester.register
        self.addCleanup(patcherRegister.stop)

    def assertRawRegistryCalls(self, registryMock, expectedData):
        """Test that a particular set of science data is registered correctly.

        Parameters
        ----------
        registryMock : `unittest.mock.Mock`
            a mock object representing the repository's registry. Must have a
            mock for the `~lsst.pipe.tasks.ingest.RegisterTask.addRow` method.
        expectedData : iterable of `dict`
            a collection of dictionaries, each representing one item that
            should have been ingested. Each dictionary must contain the
            following keys:
            - ``file``: file name to be ingested (`str`).
            - ``filter``: the filter of the file, or "_unknown_" if not applicable (`str`).
            - ``visit``: visit ID of the file (`int`).
            - ``exptime``: the exposure time of the file (`float`).
        calib : `bool`
            `True` if ``expectedData`` represents calibration data, `False` if
            it represents science data
        """
        kwargs = {'create': False, 'dryrun': False}
        for datum in expectedData:
            # TODO: find a way to avoid having to know exact data ID expansion
            dataId = {'visit': datum['visit'], 'expTime': datum['exptime'], 'filter': datum['filter']}
            # TODO: I don't think we actually care about the keywords -- especially since they're defaults
            registryMock.addRow.assert_any_call(registryMock.openRegistry().__enter__(), dataId,
                                                **kwargs)

        self.assertEqual(registryMock.addRow.call_count, len(expectedData))

    def assertCalibRegistryCalls(self, registryMock, expectedData):
        """Test that a particular set of calibration data is registered correctly.

        Parameters
        ----------
        registryMock : `unittest.mock.Mock`
            a mock object representing the repository's registry. Must have a
            mock for the `~lsst.pipe.tasks.ingest.CalibsRegisterTask.addRow` method.
        expectedData : iterable of `dict`
            a collection of dictionaries, each representing one item that
            should have been ingested. Each dictionary must contain the
            following keys:
            - ``file``: file name to be ingested (`str`).
            - ``filter``: the filter of the file, or "_unknown_" if not applicable (`str`).
            - ``type``: a valid calibration dataset type (`str`).
            - ``date``: the calibration date in YYY-MM-DD format (`str`).
        calib : `bool`
            `True` if ``expectedData`` represents calibration data, `False` if
            it represents science data
        """
        kwargs = {'create': False, 'dryrun': False}
        for datum in expectedData:
            # TODO: find a way to avoid having to know exact data ID expansion
            dataId = {'calibDate': datum['date'], 'filter': datum['filter']}
            kwargs['table'] = datum['type']
            # TODO: I don't think we actually care about the keywords -- especially since they're defaults
            registryMock.addRow.assert_any_call(registryMock.openRegistry().__enter__(), dataId,
                                                **kwargs)

        self.assertEqual(registryMock.addRow.call_count, len(expectedData))

    def testDataIngest(self):
        """Test that ingesting science images given specific files adds them to a repository.
        """
        self.setUpRawRegistry()
        files = [os.path.join(self._dataset.rawLocation, datum['file'])
                 for datum in IngestionTestSuite.rawData]
        self._task._doIngestRaws(self._repo, files, [])

        self.assertRawRegistryCalls(self._registerTask, IngestionTestSuite.rawData)

    def testDataIngestDriver(self):
        """Test that ingesting science images starting from an abstract dataset adds them to a repository.
        """
        self.setUpRawRegistry()
        self._task._ingestRaws(self._dataset, self._workspace)

        self.assertRawRegistryCalls(self._registerTask, IngestionTestSuite.rawData)

    def testCalibIngest(self):
        """Test that ingesting calibrations given specific files adds them to a repository.
        """
        files = [os.path.join(IngestionTestSuite.testData, datum['type'], datum['file'])
                 for datum in IngestionTestSuite.calibData]
        self.setUpCalibRegistry()

        self._task._doIngestCalibs(self._repo, self._calibRepo, files)

        self.assertCalibRegistryCalls(self._registerTask, IngestionTestSuite.calibData)

    def testCalibIngestDriver(self):
        """Test that ingesting calibrations starting from an abstract dataset adds them to a repository.
        """
        self.setUpCalibRegistry()
        # obs_test doesn't store calibs together; emulate normal behavior with two calls
        self._dataset.calibLocation = os.path.join(IngestionTestSuite.testData, 'bias')
        self._task._ingestCalibs(self._dataset, self._workspace)
        self._dataset.calibLocation = os.path.join(IngestionTestSuite.testData, 'flat')
        self._task._ingestCalibs(self._dataset, self._workspace)

        self.assertCalibRegistryCalls(self._registerTask, IngestionTestSuite.calibData)

    def testDefectIngest(self):
        """Test that ingesting defects starting from a concrete file adds them to a repository.
        """
        self.setUpCalibRegistry()

        defects = read_all_defects(self._dataset.defectLocation, IngestionTestSuite.mockCamera)
        numDefects = 0
        # These are keyes on sensor and validity date
        for s in defects:
            for d in defects[s]:
                numDefects += len(defects[s][d])
        self._task._doIngestDefects(self._repo, self._calibRepo, self._dataset.defectLocation)

        self.assertEqual(504, numDefects)  # Update if the number of defects in obs_test_data changes

    def testDefectIngestDriver(self):
        """Test that ingesting defects starting from an abstract dataset adds them to a repository.
        """
        self.setUpCalibRegistry()
        defects = read_all_defects(self._dataset.defectLocation, IngestionTestSuite.mockCamera)
        numDefects = 0
        # These are keyes on sensor and validity date
        for s in defects:
            for d in defects[s]:
                numDefects += len(defects[s][d])

        self._task._ingestDefects(self._dataset, self._workspace)

        self.assertEqual(504, numDefects)  # Update if the number of defects in obs_test_data changes

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

    def testNoFileIngestDriver(self):
        """Test that attempts to ingest nothing using high-level methods raise an exception.
        """
        emptyDir = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, emptyDir, ignore_errors=True)
        self._dataset.rawLocation = self._dataset.calibLocation = emptyDir
        with self.assertRaises(RuntimeError):
            self._task._ingestRaws(self._dataset, self._workspace)
        with self.assertRaises(RuntimeError):
            self._task._ingestCalibs(self._dataset, self._workspace)

    def testBadFileIngest(self):
        """Test that ingestion of raw data ignores blacklisted files.
        """
        badFiles = ['raw_v2_fg.fits.gz']
        self.setUpRawRegistry()

        files = [os.path.join(self._dataset.rawLocation, datum['file'])
                 for datum in IngestionTestSuite.rawData]
        self._task._doIngestRaws(self._repo, files, badFiles)

        filteredData = [datum for datum in IngestionTestSuite.rawData if datum['file'] not in badFiles]
        self.assertRawRegistryCalls(self._registerTask, filteredData)

        for datum in IngestionTestSuite.rawData:
            if datum['file'] in badFiles:
                dataId = {'visit': datum['visit'], 'expTime': datum['exptime'], 'filter': datum['filter']}
                # This call should never happen for badFiles
                call = unittest.mock.call(self._registerTask.openRegistry().__enter__(), dataId,
                                          create=False, dryrun=False)
                self.assertNotIn(call, self._registerTask.addRow.mock_calls)

    def testFindMatchingFiles(self):
        """Test that _findMatchingFiles finds the desired files.
        """
        testDir = os.path.join(IngestionTestSuite.testData)

        self.assertSetEqual(
            ingestion._findMatchingFiles(testDir, ['raw_*.fits.gz']),
            {os.path.join(testDir, 'raw', f) for f in
             {'raw_v1_fg.fits.gz', 'raw_v2_fg.fits.gz', 'raw_v3_fr.fits.gz'}}
        )
        self.assertSetEqual(
            ingestion._findMatchingFiles(testDir, ['raw_*.fits.gz'], ['*fr*']),
            {os.path.join(testDir, 'raw', f) for f in {'raw_v1_fg.fits.gz', 'raw_v2_fg.fits.gz'}}
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
