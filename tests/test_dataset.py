#
# LSST Data Management System
# Copyright 2017 LSST Corporation.
#
# This product includes software developed by the
# LSST Project (http://www.lsst.org/).
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
# You should have received a copy of the LSST License Statement and
# the GNU General Public License along with this program.  If not,
# see <http://www.lsstcorp.org/LegalNotices/>.
#

from __future__ import absolute_import, division, print_function

import os
import shutil
import unittest

import lsst.utils.tests
from lsst.ap.verify.dataset import Dataset


class DatasetTestSuite(lsst.utils.tests.TestCase):

    def setUp(self):
        self._testbed = Dataset('HiTS2015')

    def test_init(self):
        """Verify that if a Dataset object exists, the corresponding data are available.
        """
        # EUPS does not provide many guarantees about what setting up a package means
        self.assertIsNotNone(os.getenv('AP_VERIFY_HITS2015_DIR'))

    def test_datasets(self):
        """Verify that a Dataset knows its supported datasets.
        """
        datasets = Dataset.get_supported_datasets()
        self.assertIn('HiTS2015', datasets)  # assumed by other tests

        # Initializing another Dataset has side effects, alas, but should not
        # invalidate tests of whether HiTS2015 has been loaded
        for dataset in datasets:
            Dataset(dataset)

    def test_directories(self):
        """Verify that a Dataset reports the desired directory structure.
        """
        root = self._testbed.dataset_root
        self.assertEqual(self._testbed.data_location, os.path.join(root, 'raw'))
        self.assertEqual(self._testbed.calib_location, os.path.join(root, 'calib'))
        self.assertEqual(self._testbed.template_location, os.path.join(root, 'templates'))
        self.assertEqual(self._testbed.refcat_location, os.path.join(root, 'ref_cats'))

    def test_output(self):
        """Verify that a Dataset can create an output repository as desired.
        """
        test_dir = os.path.dirname(__file__)
        output = os.path.join(test_dir, 'hitsOut')
        self.assertFalse(os.path.exists(output), 'Output test invalid if directory exists.')

        try:
            self._testbed.make_output_repo(output)
            self.assertTrue(os.path.exists(output), 'Output directory must exist.')
            self.assertTrue(os.listdir(output), 'Output directory must not be empty.')
            self.assertTrue(os.path.exists(os.path.join(output, '_mapper')),
                            'Output directory must have a _mapper file.')
        finally:
            if os.path.exists(output):
                shutil.rmtree(output)

    def test_bad_output(self):
        """Verify that a Dataset will not create an output directory if it is unsafe to do so.
        """
        test_dir = os.path.dirname(__file__)
        output_dir = os.path.join(test_dir, 'badOut')

        try:
            os.makedirs(output_dir)
            output = os.path.join(output_dir, 'foo.txt')
            with open(output, 'w') as dummy:
                dummy.write('This is a test!')

            with self.assertRaises(IOError):
                self._testbed.make_output_repo(output_dir)
        finally:
            if os.path.exists(output_dir):
                shutil.rmtree(output_dir)


class MemoryTester(lsst.utils.tests.MemoryTestCase):
    pass


def setup_module(module):
    lsst.utils.tests.init()


if __name__ == "__main__":
    lsst.utils.tests.init()
    unittest.main()
