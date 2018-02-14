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

# Needed for urllib
from future.standard_library import install_aliases
install_aliases()

import os
import shutil
import tempfile
import unittest
from urllib.request import url2pathname

import lsst.utils.tests
from lsst.ap.verify.workspace import Workspace


class WorkspaceTestSuite(lsst.utils.tests.TestCase):

    def setUp(self):
        self._testWorkspace = tempfile.mkdtemp()
        self._testbed = Workspace(self._testWorkspace)

    def tearDown(self):
        shutil.rmtree(self._testWorkspace, ignore_errors=True)

    def _assertInDir(self, path, baseDir):
        """Test that ``path`` is a subpath of ``baseDir``.
        """
        _canonPath = os.path.abspath(os.path.realpath(path))
        _canonDir = os.path.abspath(os.path.realpath(baseDir))
        ancestor = os.path.commonprefix([_canonPath, _canonDir])
        self.assertEqual(ancestor, _canonDir)

    def testMakeDir(self):
        """Verify that a Workspace creates the workspace directory if it does not exist.
        """
        newPath = '_temp'
        shutil.rmtree(newPath, ignore_errors=True)
        self.assertFalse(os.path.exists(newPath), 'Workspace directory must not exist before test.')

        try:
            Workspace(newPath)
            self.assertTrue(os.path.exists(newPath), 'Workspace directory must exist.')
        finally:
            shutil.rmtree(newPath, ignore_errors=True)

    def testDirectories(self):
        """Verify that a Workspace creates repositories in the target directory.

        The exact repository locations are not tested, as they are likely to change.
        """
        root = self._testWorkspace
        # Workspace spec allows these to be URIs or paths, whatever the Butler accepts
        self._assertInDir(url2pathname(self._testbed.dataRepo), root)
        self._assertInDir(url2pathname(self._testbed.calibRepo), root)
        self._assertInDir(url2pathname(self._testbed.templateRepo), root)
        self._assertInDir(url2pathname(self._testbed.outputRepo), root)


class MemoryTester(lsst.utils.tests.MemoryTestCase):
    pass


def setup_module(module):
    lsst.utils.tests.init()


if __name__ == "__main__":
    lsst.utils.tests.init()
    unittest.main()
