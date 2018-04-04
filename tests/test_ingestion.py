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

from __future__ import absolute_import, division, print_function

import os
import shutil
import tempfile
import unittest

from lsst.utils import getPackageDir
import lsst.utils.tests
import lsst.pex.exceptions as pexExcept
import lsst.daf.persistence as dafPersist
import lsst.ap.verify.ingestion as ingestion

from lsst.obs.decam.ingest import DecamIngestTask


# TODO: convert test data to obs_test in DM-13849
# In the meantime, run scons -j or pytest --forked
class IngestionTestSuite(lsst.utils.tests.TestCase):

    @classmethod
    def setUpClass(cls):
        try:
            cls.testData = lsst.utils.getPackageDir("testdata_decam")
        except pexExcept.NotFoundError:
            message = "testdata_decam not setup. Skipping."
            raise unittest.SkipTest(message)

        cls.testApVerifyData = os.path.join('tests', 'ingestion')
        cls.rawDataId = {'visit': 229388, 'ccdnum': 1}
        # TODO: butler queries fail without this extra info; related to DM-12672?
        cls.calibDataId = {'visit': 229388, 'ccdnum': 1, 'filter': 'z', 'date': '2013-09-01'}
        cls.defectDataId = {'path': os.path.join('defects', 'D_n20150105t0115_c23_r2134p01_bpm.fits')}

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

        decamDir = os.path.join(getPackageDir('obs_decam'), 'config')
        config = ingestion.DatasetIngestConfig()
        config.dataIngester.retarget(DecamIngestTask)
        config.dataIngester.load(os.path.join(decamDir, 'ingest.py'))
        config.calibIngester.load(os.path.join(decamDir, 'ingestCalibs.py'))
        config.defectIngester.load(os.path.join(decamDir, 'ingestCalibs.py'))
        config.defectTarball = 'defects_2014-12-05.tar.gz'
        self._task = ingestion.DatasetIngestTask(config=config)

    def tearDown(self):
        shutil.rmtree(self._repo, ignore_errors=True)

    def _rawButler(self):
        """Return multiple ways of querying calibration repositories.

        Returns
        -------
        butler : `lsst.daf.persistence.Butler`
            A butler that should be capable of finding ingested science data.
        """
        return dafPersist.Butler(inputs={'root': self._repo, 'mode': 'r'})

    def _calibButler(self):
        """Return multiple ways of querying calibration repositories.

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
        testDir = os.path.join(IngestionTestSuite.testData, 'rawData', '2013-09-01', 'z')
        files = [os.path.join(testDir, 'decam0229388.fits.fz')]
        self._task._doIngest(self._repo, files, [])

        butler = self._rawButler()
        self.assertTrue(butler.datasetExists('raw', dataId=IngestionTestSuite.rawDataId))

    def testCalibIngest(self):
        """Test that ingesting calibrations adds them to a repository.
        """
        testDir = os.path.join(IngestionTestSuite.testData, 'rawData', 'cpCalib', 'masterCal')
        files = [os.path.join(testDir, calibFile) for calibFile in
                 ['fci.fits',
                  'zci.fits']
                 ]

        self._task._doIngestCalibs(self._repo, self._calibRepo, files)

        butler = self._calibButler()
        self.assertTrue(butler.datasetExists('cpBias', dataId=IngestionTestSuite.calibDataId))
        self.assertTrue(butler.datasetExists('cpFlat', dataId=IngestionTestSuite.calibDataId))

    def testDefectIngest(self):
        """Test that ingesting defects adds them to a repository.
        """
        tarFile = os.path.join(IngestionTestSuite.testApVerifyData, 'defects.tar.gz')

        self._task._doIngestDefects(self._repo, self._calibRepo, tarFile)

        butler = self._calibButler()
        self.assertTrue(butler.datasetExists('defects', dataId=IngestionTestSuite.defectDataId))

    @unittest.skip("Ingestion functions cannot handle empty file lists, see DM-13835")
    @unittest.skip("Dataset enumeration requires specific data keys for date, filter, etc., see DM-12762")
    def testNoFileIngest(self):
        """Test that attempts to ingest nothing do nothing.
        """
        files = []

        self._task._doIngest(self._repo, files, [])
        self._task._doIngestCalibs(self._repo, self._calibRepo, files)

        butler = self._calibButler()
        self.assertTrue(_isEmpty(butler, 'raw'))
        self.assertTrue(_isEmpty(butler, 'cpBias'))
        self.assertTrue(_isEmpty(butler, 'cpFlat'))
        self.assertTrue(_isEmpty(butler, 'defects'))

    # TODO: add unit test for _doIngest(..., badFiles) once DM-13835 resolved

    def testFindMatchingFiles(self):
        """Test that _findMatchingFiles finds the desired files.
        """
        testDir = os.path.join(IngestionTestSuite.testData, 'rawData', 'cpCalib')

        self.assertSetEqual(
            ingestion._findMatchingFiles(testDir, ['*ci.fits']),
            {os.path.join(testDir, f) for f in {"masterCal/fci.fits", "masterCal/zci.fits"}}
        )
        self.assertSetEqual(
            ingestion._findMatchingFiles(testDir, ['*ci.fits'], ['*zci*']),
            {os.path.join(testDir, "masterCal/fci.fits")}
        )
        self.assertSetEqual(
            ingestion._findMatchingFiles(testDir, ['*ci.fits'], ['*masterCal*']),
            set()
        )


def _isEmpty(butler, datasetType):
    """Test that a butler repository contains no objects.
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
