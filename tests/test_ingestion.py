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

from lsst.utils import getPackageDir
import lsst.utils.tests
import lsst.pex.exceptions as pexExcept
import lsst.daf.persistence as dafPersist
import lsst.ap.verify.ingestion as ingestion


class IngestionTestSuite(lsst.utils.tests.TestCase):

    @classmethod
    def setUpClass(cls):
        try:
            cls.testData = os.path.join(lsst.utils.getPackageDir("obs_test"), 'data', 'input')
        except pexExcept.NotFoundError:
            message = "obs_test not setup. Skipping."
            raise unittest.SkipTest(message)

        cls.testApVerifyData = os.path.join('tests', 'ingestion')
        cls.rawDataId = {'visit': 229388, 'ccdnum': 1}

    def setUp(self):
        self._repo = tempfile.mkdtemp()
        self._calibRepo = os.path.join(self._repo, 'calibs')
        templateRepo = os.path.join(IngestionTestSuite.testApVerifyData, 'repoTemplate')

        # Initialize as a valid repository
        # Can't call copytree on (templateRepo, self._repo) because latter already exists
        testFiles = os.listdir(templateRepo)
        for testFile in testFiles:
            original = os.path.join(templateRepo, testFile)
            copy = os.path.join(self._repo, testFile)
            if os.path.isdir(original):
                shutil.copytree(original, copy)
            else:
                shutil.copy2(original, copy)

        # Initialize calib repo
        # Making the directory appears to be both necessary and sufficient
        os.mkdir(self._calibRepo)

        obsDir = os.path.join(getPackageDir('obs_test'), 'config')
        config = ingestion.DatasetIngestConfig()
        config.load(os.path.join(obsDir, 'datasetIngest.py'))
        self._task = ingestion.DatasetIngestTask(config=config)

    def tearDown(self):
        shutil.rmtree(self._repo, ignore_errors=True)

    def _rawButler(self):
        """Return a way to query calibration repositories.

        Returns
        -------
        butler : `lsst.daf.persistence.Butler`
            A butler that should be capable of finding ingested science data.
        """
        return dafPersist.Butler(inputs={'root': self._repo, 'mode': 'r'})

    def _calibButler(self):
        """Return a way to query calibration repositories.

        Returns
        -------
        butlers : `lsst.daf.persistence.Butler`
            A butler that should be capable of finding ingested calibration data.
        """
        return dafPersist.Butler(inputs={
            'root': self._repo,
            'mode': 'r',
            'mapperArgs': {'calibRoot': self._calibRepo}})

    def testDataIngest(self):
        """Test that ingesting a science image adds it to a repository.
        """
        rawData = [{'file': 'raw_v1_fg.fits.gz', 'visit': 890104911, 'filter': 'g', 'exptime': 15.0},
                   {'file': 'raw_v2_fg.fits.gz', 'visit': 890106021, 'filter': 'g', 'exptime': 15.0},
                   {'file': 'raw_v3_fr.fits.gz', 'visit': 890880321, 'filter': 'r', 'exptime': 15.0},
                   ]
        testDir = os.path.join(IngestionTestSuite.testData, 'raw')
        files = [os.path.join(testDir, datum['file']) for datum in rawData]
        self._task._doIngest(self._repo, files, [])

        butler = self._rawButler()
        for datum in rawData:
            dataId = {'visit': datum['visit']}
            self.assertTrue(butler.datasetExists('raw', dataId))
            self.assertEqual(butler.queryMetadata('raw', 'filter', dataId),
                             [datum['filter']])
            self.assertEqual(butler.queryMetadata('raw', 'exptime', dataId),
                             [datum['exptime']])
        self.assertFalse(_isEmpty(butler, 'raw'))
        self.assertFalse(butler.datasetExists('flat', filter='g'))

    def testCalibIngest(self):
        """Test that ingesting calibrations adds them to a repository.
        """
        calibData = [{'type': 'bias', 'file': 'bias.fits.gz', 'filter': 'None'},
                     {'type': 'flat', 'file': 'flat_fg.fits.gz', 'filter': 'g'},
                     {'type': 'flat', 'file': 'flat_fr.fits.gz', 'filter': 'r'},
                     ]
        files = [os.path.join(IngestionTestSuite.testData, datum['type'], datum['file'])
                 for datum in calibData]

        self._task._doIngestCalibs(self._repo, self._calibRepo, files)

        butler = self._calibButler()
        for datum in calibData:
            self.assertTrue(butler.datasetExists(datum['type'], filter=datum['filter']))
            # queryMetadata does not work on calibs
        self.assertFalse(butler.datasetExists('flat', filter='z'))

    def testNoFileIngest(self):
        """Test that attempts to ingest nothing raise an exception.
        """
        files = []

        with self.assertRaises(RuntimeError):
            self._task._doIngest(self._repo, files, [])
        with self.assertRaises(RuntimeError):
            self._task._doIngestCalibs(self._repo, self._calibRepo, files)

        butler = self._calibButler()
        self.assertTrue(_isEmpty(butler, 'raw'))

    # TODO: add unit test for _doIngest(..., badFiles) once DM-13835 resolved

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
