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

    def _assertNotInDir(self, path, baseDir):
        """Test that ``path`` is not a subpath of ``baseDir``.
        """
        _canonPath = os.path.abspath(os.path.realpath(path))
        _canonDir = os.path.abspath(os.path.realpath(baseDir))
        ancestor = os.path.commonprefix([_canonPath, _canonDir])
        self.assertNotEqual(ancestor, _canonDir)

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

    @staticmethod
    def _allRepos(workspace):
        """An iterator over all repos exposed by a Workspace.
        """
        yield workspace.dataRepo
        yield workspace.calibRepo
        yield workspace.templateRepo
        yield workspace.outputRepo

    def testDirectories(self):
        """Verify that a Workspace creates repositories in the target directory.

        The exact repository locations are not tested, as they are likely to change.
        """
        root = self._testWorkspace
        self.assertEqual(self._testbed.workDir, root)
        self._assertInDir(self._testbed.configDir, root)
        for repo in WorkspaceTestSuite._allRepos(self._testbed):
            # Workspace spec allows these to be URIs or paths, whatever the Butler accepts
            self._assertInDir(url2pathname(repo), root)

    def testDatabase(self):
        """Verify that a Workspace requests a database file in the target
        directory, but not in any repository.
        """
        root = self._testWorkspace
        self._assertInDir(self._testbed.dbLocation, root)
        for repo in WorkspaceTestSuite._allRepos(self._testbed):
            # Workspace spec allows these to be URIs or paths, whatever the Butler accepts
            self._assertNotInDir(self._testbed.dbLocation, url2pathname(repo))


class MemoryTester(lsst.utils.tests.MemoryTestCase):
    pass


def setup_module(module):
    lsst.utils.tests.init()


if __name__ == "__main__":
    lsst.utils.tests.init()
    unittest.main()
