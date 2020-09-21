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

import lsst.utils.tests
import lsst.daf.butler as dafButler
from lsst.ap.verify.dataset import Dataset
from lsst.ap.verify.testUtils import DataTestCase


class DatasetTestSuite(DataTestCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()

        cls.obsPackage = 'obs_lsst'
        cls.camera = 'imsim'
        cls.gen3Camera = 'LSST-ImSim'

    def setUp(self):
        self._testbed = Dataset(DatasetTestSuite.datasetKey)

    def testRepr(self):
        # Required to match constructor call
        self.assertEqual(repr(self._testbed), "Dataset(" + repr(self.datasetKey) + ")")

    def testDatasets(self):
        """Verify that a Dataset knows its supported datasets.
        """
        datasets = Dataset.getSupportedDatasets()
        self.assertIn(DatasetTestSuite.datasetKey, datasets)  # assumed by other tests

    def testBadDataset(self):
        """Verify that Dataset construction fails gracefully on unsupported datasets.
        """
        with self.assertRaises(ValueError):
            Dataset("TotallyBogusDataset")

    def testDirectories(self):
        """Verify that a Dataset reports the desired directory structure.
        """
        root = self._testbed.datasetRoot
        self.assertEqual(self._testbed.rawLocation, os.path.join(root, 'raw'))
        self.assertEqual(self._testbed.calibLocation, os.path.join(root, 'calib'))
        self.assertEqual(self._testbed.templateLocation, os.path.join(root, 'templates'))
        self.assertEqual(self._testbed.refcatsLocation, os.path.join(root, 'refcats'))

    def testObsPackage(self):
        """Verify that a Dataset knows its associated obs package and camera.
        """
        self.assertEqual(self._testbed.obsPackage, DatasetTestSuite.obsPackage)
        self.assertEqual(self._testbed.camera, DatasetTestSuite.camera)
        self.assertEqual(self._testbed.instrument.getName(), DatasetTestSuite.gen3Camera)

    def testOutput(self):
        """Verify that a Dataset can create an output repository as desired.
        """
        testDir = tempfile.mkdtemp()
        outputDir = os.path.join(testDir, 'goodOut')
        calibRepoDir = outputDir

        try:
            self._testbed.makeCompatibleRepo(outputDir, calibRepoDir)
            self.assertTrue(os.path.exists(outputDir), 'Output directory must exist.')
            self.assertTrue(os.listdir(outputDir), 'Output directory must not be empty.')
            self.assertTrue(os.path.exists(os.path.join(outputDir, '_mapper')) or
                            os.path.exists(os.path.join(outputDir, 'repositoryCfg.yaml')),
                            'Output directory must have a _mapper or repositoryCfg.yaml file.')
        finally:
            if os.path.exists(testDir):
                shutil.rmtree(testDir, ignore_errors=True)

    def testExistingOutput(self):
        """Verify that a Dataset can handle pre-existing output directories,
        including directories made by external code.
        """
        testDir = tempfile.mkdtemp()
        outputDir = os.path.join(testDir, 'badOut')
        calibRepoDir = outputDir

        try:
            os.makedirs(outputDir)
            output = os.path.join(outputDir, 'foo.txt')
            with open(output, 'w') as dummy:
                dummy.write('This is a test!')

            self._testbed.makeCompatibleRepo(outputDir, calibRepoDir)
            self.assertTrue(os.path.exists(outputDir), 'Output directory must exist.')
            self.assertTrue(os.listdir(outputDir), 'Output directory must not be empty.')
            self.assertTrue(os.path.exists(os.path.join(outputDir, '_mapper')) or
                            os.path.exists(os.path.join(outputDir, 'repositoryCfg.yaml')),
                            'Output directory must have a _mapper or repositoryCfg.yaml file.')
        finally:
            if os.path.exists(testDir):
                shutil.rmtree(testDir, ignore_errors=True)

    def _checkOutputGen3(self, repo):
        """Perform various integrity checks on a repository.

        Parameters
        ----------
        repo : `str`
            The repository to test. Currently only filesystem repositories
            are supported.
        """
        self.assertTrue(os.path.exists(repo), 'Output directory must exist.')
        # Call to Butler will fail if repo is corrupted
        butler = dafButler.Butler(repo)
        self.assertIn("LSST-ImSim/calib", butler.registry.queryCollections())

    def testOutputGen3(self):
        """Verify that a Dataset can create an output repository as desired.
        """
        testDir = tempfile.mkdtemp()
        outputDir = os.path.join(testDir, 'goodOut')

        try:
            self._testbed.makeCompatibleRepoGen3(outputDir)
            self._checkOutputGen3(outputDir)
        finally:
            if os.path.exists(testDir):
                shutil.rmtree(testDir, ignore_errors=True)

    def testExistingOutputGen3(self):
        """Verify that a Dataset can handle pre-existing output directories,
        including directories made by external code.
        """
        testDir = tempfile.mkdtemp()
        outputDir = os.path.join(testDir, 'badOut')

        try:
            self._testbed.makeCompatibleRepoGen3(outputDir)
            self._checkOutputGen3(outputDir)
            self._testbed.makeCompatibleRepoGen3(outputDir)
            self._checkOutputGen3(outputDir)
        finally:
            if os.path.exists(testDir):
                shutil.rmtree(testDir, ignore_errors=True)


class MemoryTester(lsst.utils.tests.MemoryTestCase):
    pass


def setup_module(module):
    lsst.utils.tests.init()


if __name__ == "__main__":
    lsst.utils.tests.init()
    unittest.main()
