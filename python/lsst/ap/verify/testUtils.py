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

"""Common code for ap_verify unit tests.
"""

import unittest

import lsst.utils.tests

import lsst.pex.exceptions as pexExcept
from lsst.ap.verify.config import Config


class DataTestCase(lsst.utils.tests.TestCase):
    """Unit test class for tests that need to use the Dataset framework.

    Unit tests based on this class will search for a designated dataset
    (`testDataset`), and skip all tests if the dataset is not available.

    Subclasses must call `DataTestCase.setUpClass()` if they override
    ``setUpClass`` themselves.
    """

    testDataset = 'ap_verify_testdata'
    """The EUPS package name of the dataset to use for testing (`str`).
    """
    datasetKey = 'test'
    """The ap_verify dataset name that would be used on the command line (`str`).
    """

    @classmethod
    def setUpClass(cls):
        try:
            lsst.utils.getPackageDir(cls.testDataset)
        except pexExcept.NotFoundError:
            raise unittest.SkipTest(cls.testDataset + ' not set up')

        # Hack the config for testing purposes
        # Note that Config.instance is supposed to be immutable, so, depending on initialization order,
        # this modification may cause other tests to see inconsistent config values
        Config.instance._allInfo['datasets.' + cls.datasetKey] = cls.testDataset
