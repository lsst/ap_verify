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
import unittest
import unittest.mock

from lsst.obs.test import TestMapper

from lsst.utils import getPackageDir
import lsst.utils.tests
import lsst.pipe.tasks.ingest
import lsst.pipe.tasks.ingestCalibs
# import lsst.pipe.tasks as pipeTasks
import lsst.pex.exceptions as pexExcept
import lsst.ap.verify.ingestion


class IngestionTestSuite(lsst.utils.tests.TestCase):

    @classmethod
    def setUpClass(cls):
        try:
            cls.testData = os.path.join(lsst.utils.getPackageDir("obs_test"), 'data', 'input')
        except pexExcept.NotFoundError:
            message = "obs_test not setup. Skipping."
            raise unittest.SkipTest(message)

        obsDir = os.path.join(getPackageDir('obs_test'), 'config')
        cls.config = lsst.ap.verify.ingestion.DatasetIngestConfig()
        cls.config.dataIngester.load(os.path.join(obsDir, 'ingest.py'))
        cls.config.calibIngester.load(os.path.join(obsDir, 'ingestCalibs.py'))
        cls.config.defectIngester.load(os.path.join(obsDir, 'ingestCalibs.py'))
        cls.config.freeze()

        cls.testApVerifyData = os.path.join('tests', 'ingestion')
        cls.rawDataId = {'visit': 229388, 'ccdnum': 1}

        cls.visitToFile = {890104911: 'raw_v1_fg.fits.gz',
                           890106021: 'raw_v2_fg.fits.gz',
                           890880321: 'raw_v3_fr.fits.gz'}
        cls.rawData = [{'file': 'raw_v1_fg.fits.gz', 'visit': 890104911, 'filter': 'g', 'exptime': 15.0},
                       {'file': 'raw_v2_fg.fits.gz', 'visit': 890106021, 'filter': 'g', 'exptime': 15.0},
                       {'file': 'raw_v3_fr.fits.gz', 'visit': 890880321, 'filter': 'r', 'exptime': 15.0},
                       ]
        cls.calibData = [{'type': 'bias', 'file': 'bias.fits.gz', 'filter': '_unknown_'},
                         {'type': 'flat', 'file': 'flat_fg.fits.gz', 'filter': 'g'},
                         {'type': 'flat', 'file': 'flat_fr.fits.gz', 'filter': 'r'},
                         ]

    def setUp(self):
        # Mandatory argument to _doIngest*, used by _doIngestDefects to unpack tar
        self._repo = self._calibRepo = tempfile.mkdtemp()

        def mock_get(datasetType, dataId=None):
            """Minimally fake a butler.get()"""
            if "raw_filename" in datasetType:
                # butler.get('_filename') returns a list.
                return [os.path.join(self._repo, IngestionTestSuite.visitToFile[dataId['visit']]), ]
            return None

        patcherButler = unittest.mock.patch("lsst.daf.persistence.Butler", autospec=True)
        self._butler = patcherButler.start()
        # NOTE: we have to force the MapperClass here, because we don't have a
        # properly initialized butler repository, so getMapperClass() wouldn't
        # work with a real butler.
        self._butler.getMapperClass.return_value = TestMapper
        self._butler.return_value.get = mock_get
        self.addCleanup(patcherButler.stop)

        self._task = lsst.ap.verify.ingestion.DatasetIngestTask(config=IngestionTestSuite.config)
        patcherRegister = unittest.mock.patch.object(self._task.dataIngester, "register",
                                                     spec=lsst.pipe.tasks.ingest.RegisterTask)
        self._register = patcherRegister.start()
        self.addCleanup(patcherRegister.stop)
        # the mocked entry point of the Registry context manager
        self._contextEnter = self._register.openRegistry().__enter__()

    def tearDown(self):
        shutil.rmtree(self._repo, ignore_errors=True)

    def testDataIngest(self):
        """Test that ingesting a science image adds it to a repository.
        """
        testDir = os.path.join(IngestionTestSuite.testData, 'raw')
        files = [os.path.join(testDir, datum['file']) for datum in IngestionTestSuite.rawData]
        self._task._doIngestRaws(self._repo, files, [])

        self.assertEqual(self._register.addRow.call_count, len(IngestionTestSuite.rawData))
        self.assertEqual(self._register.addVisits.call_count, 1)
        # print("register:", self._register.mock_calls)
        for item in IngestionTestSuite.rawData:
            # outfile = os.path.join(self._repo, item['file'])
            # self.assertTrue(os.path.exists(outfile))
            dataId = {'visit': item['visit'], 'expTime': item['exptime'], 'filter': item['filter']}
            self._register.addRow.assert_any_call(self._contextEnter, dataId, create=False, dryrun=False)
        self._register.addVisits.assert_called_once_with(self._contextEnter, dryrun=False)

        import ipdb; ipdb.set_trace()

        for datum in IngestionTestSuite.rawData:
            dataId = {'visit': datum['visit']}
            self.assertTrue(self._butler.datasetExists('raw', dataId))
            self.assertEqual(self._butler.queryMetadata('raw', 'filter', dataId),
                             [datum['filter']])
            self.assertEqual(self._butler.queryMetadata('raw', 'expTime', dataId),
                             [datum['exptime']])
        self.assertFalse(_isEmpty(self._butler, 'raw'))
        self.assertFalse(self._butler.datasetExists('flat', filter='g'))

    def testCalibIngest(self):
        """Test that ingesting calibrations adds them to a repository.
        """
        files = [os.path.join(IngestionTestSuite.testData, datum['type'], datum['file'])
                 for datum in IngestionTestSuite.calibData]

        self._task._doIngestCalibs(self._repo, self._calibRepo, files)

        for datum in IngestionTestSuite.calibData:
            self.assertTrue(self._butler.datasetExists(datum['type'], filter=datum['filter']))
            # queryMetadata does not work on calibs
        self.assertFalse(self._butler.datasetExists('flat', filter='z'))

    def testDefectIngest(self):
        """Test that ingesting defects adds them to a repository.
        """
        tarFile = os.path.join(IngestionTestSuite.testApVerifyData, 'defects.tar.gz')

        self._task._doIngestDefects(self._repo, self._calibRepo, tarFile)

        self.assertTrue(self._butler.datasetExists('defect'))

    def testNoFileIngest(self):
        """Test that attempts to ingest nothing raise an exception.
        """
        files = []

        with self.assertRaises(RuntimeError):
            self._task._doIngestRaws(self._repo, files, [])
        with self.assertRaises(RuntimeError):
            self._task._doIngestCalibs(self._repo, self._calibRepo, files)

        self.assertTrue(_isEmpty(self._butler, 'raw'))

    def testBadFileIngest(self):
        """Test that ingestion of raw data ignores blacklisted files.
        """
        badFiles = ['raw_v2_fg.fits.gz']

        testDir = os.path.join(IngestionTestSuite.testData, 'raw')
        files = [os.path.join(testDir, datum['file']) for datum in IngestionTestSuite.rawData]
        self._task._doIngestRaws(self._repo, files, badFiles)

        for datum in IngestionTestSuite.rawData:
            # dataId = {'visit': datum['visit']}
            # self.assertEqual(self._butler.datasetExists('raw', dataId), datum['file'] not in badFiles)
            dataId = {'visit': datum['visit'], 'expTime': datum['exptime'], 'filter': datum['filter']}
            call = unittest.mock.call(self._contextEnter, dataId, create=False, dryrun=False)
            if datum['file'] not in badFiles:
                self.assertIn(call, self._register.addRow.mock_calls)
            else:
                self.assertNotIn(call, self._register.addRow.mock_calls)

    def testFindMatchingFiles(self):
        """Test that _findMatchingFiles finds the desired files.
        """
        testDir = os.path.join(IngestionTestSuite.testData)

        self.assertSetEqual(
            lsst.ap.verify.ingestion._findMatchingFiles(testDir, ['raw_*.fits.gz']),
            {os.path.join(testDir, f) for f in
             {'raw/raw_v1_fg.fits.gz', 'raw/raw_v2_fg.fits.gz', 'raw/raw_v3_fr.fits.gz'}}
        )
        self.assertSetEqual(
            lsst.ap.verify.ingestion._findMatchingFiles(testDir, ['raw_*.fits.gz'], ['*fr*']),
            {os.path.join(testDir, f) for f in {'raw/raw_v1_fg.fits.gz', 'raw/raw_v2_fg.fits.gz'}}
        )
        self.assertSetEqual(
            lsst.ap.verify.ingestion._findMatchingFiles(testDir, ['raw_*.fits.gz'], ['*_v?_f?.fits.gz']),
            set()
        )


def _isEmpty(butler, datasetType):
    """Test that a butler repository contains no objects.

    Parameters
    ----------
    datasetType : `str`
        The type of dataset to search for.

    Notes
    -----
    .. warning::
       Does not work for calib datasets, because they're not discoverable.
    """
    possibleDataRefs = butler.subset(datasetType)
    for dataRef in possibleDataRefs:
        if dataRef.datasetExists():
            return False
    return True


class MemoryTester(lsst.utils.tests.MemoryTestCase):
    pass


def setup_module(module):
    lsst.utils.tests.init()


if __name__ == "__main__":
    lsst.utils.tests.init()
    unittest.main()
