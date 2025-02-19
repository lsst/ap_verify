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
import pickle
import shutil
import tempfile
import unittest

import lsst.utils.tests
from lsst.daf.butler import CollectionType
from lsst.ap.verify import ingestion
from lsst.ap.verify.testUtils import DataTestCase
from lsst.ap.verify.dataset import Dataset
from lsst.ap.verify.workspace import WorkspaceGen3


class IngestionTestSuiteGen3(DataTestCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()

        cls.dataset = Dataset(cls.testDataset)

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
                          'physical_filter': 'i_sim_1.4'},
                         ]

    @classmethod
    def makeTestConfig(cls):
        instrument = cls.dataset.instrument
        config = ingestion.Gen3DatasetIngestConfig()
        instrument.applyConfigOverrides(ingestion.Gen3DatasetIngestTask._DefaultName, config)
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
                                                    dataset=self.dataset,
                                                    workspace=self.workspace,
                                                    namespace="sasquatch",
                                                    url=None)

        self.butler = self.workspace.workButler

    def assertIngestedDataFiles(self, data, collection):
        """Test that data have been loaded into a specific collection.

        Parameters
        ----------
        data : `collections.abc.Iterable` [`collections.abc.Mapping`]
            An iterable of mappings, each representing the properties of a
            single input dataset. Each mapping must contain a `"type"` key
            that maps to the dataset's Gen 3 type.
        collection
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
        self.task._ingestRaws(files, processes=1)
        self.assertIngestedDataFiles(self.rawData, self.dataset.instrument.makeDefaultRawIngestRunName())

    def testDataDoubleIngest(self):
        """Test that re-ingesting science images raises RuntimeError.
        """
        files = [os.path.join(self.dataset.rawLocation, datum['file']) for datum in self.rawData]
        self.task._ingestRaws(files, processes=1)
        with self.assertRaises(RuntimeError):
            self.task._ingestRaws(files, processes=1)

    def testDataIngestDriver(self):
        """Test that ingesting science images starting from an abstract dataset adds them to a repository.
        """
        self.task._ensureRaws(processes=1)
        self.assertIngestedDataFiles(self.rawData, self.dataset.instrument.makeDefaultRawIngestRunName())

    def testCalibIngestDriver(self):
        """Test that ingesting calibrations starting from an abstract dataset adds them to a repository.
        """
        self.task._ensureRaws(processes=1)  # Should not affect calibs, but would be run
        # queryDatasets cannot (yet) search CALIBRATION collections, so we
        # instead search the RUN-type collections that calibrations are
        # ingested into first before being associated with a validity range.
        calibrationRunPattern = self.dataset.instrument.makeCollectionName("calib") + "/*"
        calibrationRuns = list(
            self.butler.registry.queryCollections(
                calibrationRunPattern,
                collectionTypes={CollectionType.RUN},
            )
        )
        self.assertIngestedDataFiles(self.calibData, calibrationRuns)

    def testNoFileIngest(self):
        """Test that attempts to ingest nothing raise an exception.
        """
        with self.assertRaises(RuntimeError):
            self.task._ingestRaws([], processes=1)

    def testVisitDefinition(self):
        """Test that the final repository supports indexing by visit.
        """
        self.task._ensureRaws(processes=1)
        self.task._defineVisits(processes=1)

        testId = {"visit": self.VISIT_ID, "instrument": self.INSTRUMENT, }
        exposures = list(self.butler.registry.queryDataIds("exposure", dataId=testId))
        self.assertEqual(len(exposures), 1)
        self.assertEqual(exposures[0]["exposure"], self.VISIT_ID)

    def testVisitDoubleDefinition(self):
        """Test that re-defining visits is guarded against.
        """
        self.task._ensureRaws(processes=1)
        self.task._defineVisits(processes=1)
        self.task._defineVisits(processes=1)  # must not raise

        testId = {"visit": self.VISIT_ID, "instrument": self.INSTRUMENT, }
        exposures = list(self.butler.registry.queryDataIds("exposure", dataId=testId))
        self.assertEqual(len(exposures), 1)

    def testVisitsUndefinable(self):
        """Test that attempts to define visits with no exposures raise an exception.
        """
        with self.assertRaises(RuntimeError):
            self.task._defineVisits(processes=1)

    def testCopyConfigs(self):
        """Test that "ingesting" configs stores them in the workspace for later reference.
        """
        self.task._copyConfigs()
        self.assertTrue(os.path.exists(self.workspace.configDir))
        self.assertTrue(os.path.exists(self.workspace.pipelineDir))
        self.assertTrue(os.path.exists(os.path.join(self.workspace.pipelineDir, "ApVerify.yaml")))

    def testPickling(self):
        """Test that a Gen3DatasetIngestTask can be pickled correctly.

        This is needed for multiprocessing support.
        """
        stream = pickle.dumps(self.task)
        copy = pickle.loads(stream)
        self.assertEqual(self.task.getFullName(), copy.getFullName())
        self.assertEqual(self.task.log.name, copy.log.name)
        # Equality for config ill-behaved; skip testing it
        self.assertEqual(self.task.dataset, copy.dataset)
        self.assertEqual(self.task.workspace, copy.workspace)


class MemoryTester(lsst.utils.tests.MemoryTestCase):
    pass


def setup_module(module):
    lsst.utils.tests.init()


if __name__ == "__main__":
    lsst.utils.tests.init()
    unittest.main()
