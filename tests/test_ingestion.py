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
from lsst.ap.verify import ingestion
from lsst.ap.verify.testUtils import DataTestCase
from lsst.ap.verify.dataset import Dataset
from lsst.ap.verify.workspace import WorkspaceGen2, WorkspaceGen3


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


class IngestionTestSuite(DataTestCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()

        cls.mockCamera = MockCamera(MockDetector())
        cls.config = cls.makeTestConfig()
        cls.config.validate()
        cls.config.freeze()

        cls.testApVerifyData = os.path.join('tests', 'ingestion')

        cls.rawData = [{'file': 'lsst_a_204595_R11_S01_i.fits', 'expId': 204595, 'filter': 'i',
                        'exptime': 30.0},
                       ]
        cls.calibData = [{'type': 'bias', 'file': 'bias-R11-S01-det037_2022-01-01.fits.gz',
                          'filter': 'NONE', 'date': '2022-01-01'},
                         {'type': 'flat', 'file': 'flat_i-R11-S01-det037_2022-08-06.fits.gz',
                          'filter': 'i', 'date': '2022-08-06'},
                         ]

    @staticmethod
    def makeTestConfig():
        obsDir = os.path.join(getPackageDir('obs_lsst'), 'config')
        config = ingestion.DatasetIngestConfig()
        config.dataIngester.load(os.path.join(obsDir, 'ingest.py'))
        config.dataIngester.load(os.path.join(obsDir, 'imsim', 'ingest.py'))
        config.calibIngester.load(os.path.join(obsDir, 'ingestCalibs.py'))
        config.curatedCalibIngester.load(os.path.join(obsDir, 'ingestCuratedCalibs.py'))
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
                                 if datum['expId'] == dataId['expId']]
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

        butlerPatcher = unittest.mock.patch("lsst.daf.persistence.Butler")
        self._butler = butlerPatcher.start()
        self._butler.getMapperClass.return_value = lsst.obs.lsst.imsim.ImsimMapper
        self._butler.return_value.get = mockGet
        self.addCleanup(butlerPatcher.stop)

        self._dataset = Dataset(self.datasetKey)
        # Fake Workspace because it's too hard to make a real one with a fake Butler
        self._workspace = unittest.mock.NonCallableMock(
            spec=WorkspaceGen2,
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
            - ``filter``: the filter of the file, or "NONE" if not applicable (`str`).
            - ``expId``: exposure ID of the file (`int`).
            - ``exptime``: the exposure time of the file (`float`).
        calib : `bool`
            `True` if ``expectedData`` represents calibration data, `False` if
            it represents science data
        """
        for datum in expectedData:
            found = False
            dataId = {'expId': datum['expId'], 'expTime': datum['exptime'], 'filter': datum['filter']}
            for call in registryMock.addRow.call_args_list:
                args = call[0]
                registeredId = args[1]
                self.assertLessEqual(set(dataId.keys()), set(registeredId.keys()))  # subset

                if registeredId['expId'] == datum['expId']:
                    found = True
                    for dimension in dataId:
                        self.assertEqual(registeredId[dimension], dataId[dimension])
            self.assertTrue(found, msg=f"No call with {dataId}.")

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
            - ``filter``: the filter of the file, or "NONE" if not applicable (`str`).
            - ``type``: a valid calibration dataset type (`str`).
            - ``date``: the calibration date in YYY-MM-DD format (`str`).
        calib : `bool`
            `True` if ``expectedData`` represents calibration data, `False` if
            it represents science data
        """
        for datum in expectedData:
            found = False
            dataId = {'calibDate': datum['date'], 'filter': datum['filter']}
            for call in registryMock.addRow.call_args_list:
                args = call[0]
                kwargs = call[1]
                registeredId = args[1]
                self.assertLessEqual(set(dataId.keys()), set(registeredId.keys()))  # subset

                if kwargs["table"] == datum["type"] and registeredId['filter'] == datum['filter'] \
                        and registeredId['calibDate'] == datum['date']:
                    found = True
            self.assertTrue(found, msg=f"No call with {dataId}.")

        self.assertEqual(registryMock.addRow.call_count, len(expectedData))

    def testDataIngest(self):
        """Test that ingesting science images given specific files adds them to a repository.
        """
        self.setUpRawRegistry()
        files = [os.path.join(self._dataset.rawLocation, datum['file'])
                 for datum in IngestionTestSuite.rawData]
        self._task._doIngestRaws(self._repo, self._calibRepo, files, [])

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
        files = [os.path.join(self._dataset.calibLocation, datum['file'])
                 for datum in IngestionTestSuite.calibData]
        self.setUpCalibRegistry()

        self._task._doIngestCalibs(self._repo, self._calibRepo, files)

        self.assertCalibRegistryCalls(self._registerTask, IngestionTestSuite.calibData)

    def testCalibIngestDriver(self):
        """Test that ingesting calibrations starting from an abstract dataset adds them to a repository.
        """
        self.setUpCalibRegistry()
        self._task._ingestCalibs(self._dataset, self._workspace)

        self.assertCalibRegistryCalls(self._registerTask, IngestionTestSuite.calibData)

    def testNoFileIngest(self):
        """Test that attempts to ingest nothing raise an exception.
        """
        files = []
        self.setUpRawRegistry()

        with self.assertRaises(RuntimeError):
            self._task._doIngestRaws(self._repo, self._calibRepo, files, [])
        with self.assertRaises(RuntimeError):
            self._task._doIngestCalibs(self._repo, self._calibRepo, files)

        self._registerTask.addRow.assert_not_called()

    def testBadFileIngest(self):
        """Test that ingestion of raw data ignores forbidden files.
        """
        badFiles = ['raw_v2_fg.fits.gz']
        self.setUpRawRegistry()

        files = [os.path.join(self._dataset.rawLocation, datum['file'])
                 for datum in IngestionTestSuite.rawData]
        self._task._doIngestRaws(self._repo, self._calibRepo, files, badFiles)

        filteredData = [datum for datum in IngestionTestSuite.rawData if datum['file'] not in badFiles]
        self.assertRawRegistryCalls(self._registerTask, filteredData)

        for datum in IngestionTestSuite.rawData:
            if datum['file'] in badFiles:
                dataId = {'expId': datum['expId'], 'expTime': datum['exptime'], 'filter': datum['filter']}
                # This call should never happen for badFiles
                call = unittest.mock.call(self._registerTask.openRegistry().__enter__(), dataId,
                                          create=False, dryrun=False)
                self.assertNotIn(call, self._registerTask.addRow.mock_calls)


class IngestionTestSuiteGen3(DataTestCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()

        cls.dataset = Dataset(cls.datasetKey)

        cls.INSTRUMENT = cls.dataset.instrument.getName()
        cls.VISIT_ID = 204595
        cls.DETECTOR_ID = 37

        cls.rawData = [{'type': 'raw', 'file': 'lsst_a_204595_R11_S01_i.fits',
                        'exposure': cls.VISIT_ID, 'detector': cls.DETECTOR_ID,
                        'instrument': cls.INSTRUMENT},
                       ]

        cls.calibData = [{'type': 'bias', 'file': 'bias-R11-S01-det037_2022-01-01.fits.gz',
                          'detector': cls.DETECTOR_ID, 'instrument': cls.INSTRUMENT},
                         {'type': 'flat', 'file': 'flat_i-R11-S01-det037_2022-08-06.fits.gz',
                          'detector': cls.DETECTOR_ID, 'instrument': cls.INSTRUMENT,
                          'physical_filter': 'i'},
                         ]

    @staticmethod
    def makeTestConfig():
        config = ingestion.Gen3DatasetIngestConfig()
        return config

    def setUp(self):
        super().setUp()

        self.config = self.makeTestConfig()
        self.config.validate()
        self.config.freeze()

        self.root = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.root, ignore_errors=True)
        self.workspace = WorkspaceGen3(self.root)
        self.task = ingestion.Gen3DatasetIngestTask(config=self.config,
                                                    dataset=self.dataset, workspace=self.workspace)

        self.butler = self.workspace.workButler

    def assertIngestedDataFiles(self, data, collection):
        """Test that data have been loaded into a specific collection.

        Parameters
        ----------
        data : `collections.abc.Iterable` [`collections.abc.Mapping`]
            An iterable of mappings, each representing the properties of a
            single input dataset. Each mapping must contain a `"type"` key
            that maps to the dataset's Gen 3 type.
        collection : `lsst.daf.butler.CollectionType`
            Any valid :ref:`collection expression <daf_butler_collection_expressions>`
            for the collection expected to contain the data.
        """
        for datum in data:
            dataId = datum.copy()
            dataId.pop("type", None)
            dataId.pop("file", None)

            matches = [x for x in self.butler.registry.queryDatasets(datum['type'],
                                                                     collections=collection,
                                                                     dataId=dataId)]
            self.assertNotEqual(matches, [])

    def testDataIngest(self):
        """Test that ingesting science images given specific files adds them to a repository.
        """
        files = [os.path.join(self.dataset.rawLocation, datum['file']) for datum in self.rawData]
        self.task._ingestRaws(files)
        self.assertIngestedDataFiles(self.rawData, self.dataset.instrument.makeDefaultRawIngestRunName())

    def testDataDoubleIngest(self):
        """Test that re-ingesting science images raises RuntimeError.
        """
        files = [os.path.join(self.dataset.rawLocation, datum['file']) for datum in self.rawData]
        self.task._ingestRaws(files)
        with self.assertRaises(RuntimeError):
            self.task._ingestRaws(files)

    def testDataIngestDriver(self):
        """Test that ingesting science images starting from an abstract dataset adds them to a repository.
        """
        self.task._ensureRaws()
        self.assertIngestedDataFiles(self.rawData, self.dataset.instrument.makeDefaultRawIngestRunName())

    def testCalibIngestDriver(self):
        """Test that ingesting calibrations starting from an abstract dataset adds them to a repository.
        """
        self.task._ensureRaws()  # Should not affect calibs, but would be run
        self.assertIngestedDataFiles(self.calibData, self.dataset.instrument.makeCollectionName("calib"))

    def testNoFileIngest(self):
        """Test that attempts to ingest nothing raise an exception.
        """
        with self.assertRaises(RuntimeError):
            self.task._ingestRaws([])

    def testCopyConfigs(self):
        """Test that "ingesting" configs stores them in the workspace for later reference.
        """
        self.task._copyConfigs()
        self.assertTrue(os.path.exists(self.workspace.configDir))
        # Only testdata file that *must* be supported in the future
        self.assertTrue(os.path.exists(os.path.join(self.workspace.configDir, "datasetIngest.py")))

    def testFindMatchingFiles(self):
        """Test that _findMatchingFiles finds the desired files.
        """
        testDir = self.dataset.datasetRoot
        allFiles = {os.path.join(testDir, 'calib', f) for f in
                    {'bias-R11-S01-det037_2022-01-01.fits.gz',
                     'flat_i-R11-S01-det037_2022-08-06.fits.gz',
                     }}

        self.assertSetEqual(
            ingestion._findMatchingFiles(testDir, ['*.fits.gz']), allFiles
        )
        self.assertSetEqual(
            ingestion._findMatchingFiles(testDir, ['*.fits.gz'], exclude=['*_i-*']),
            {os.path.join(testDir, 'calib', f) for f in
             {'bias-R11-S01-det037_2022-01-01.fits.gz'}}
        )
        self.assertSetEqual(
            ingestion._findMatchingFiles(testDir, ['*.fits.gz'], exclude=['*R11-S01*']),
            set()
        )
        # Exclude filters should not match directories
        self.assertSetEqual(
            ingestion._findMatchingFiles(testDir, ['*.fits.gz'], exclude=['calib']),
            allFiles
        )


class MemoryTester(lsst.utils.tests.MemoryTestCase):
    pass


def setup_module(module):
    lsst.utils.tests.init()


if __name__ == "__main__":
    lsst.utils.tests.init()
    unittest.main()
