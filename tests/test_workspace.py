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
import lsst.daf.butler
from lsst.ap.verify.workspace import WorkspaceGen2, WorkspaceGen3


class WorkspaceGen2TestSuite(lsst.utils.tests.TestCase):

    def setUp(self):
        # Use realpath to avoid link problems
        self._testWorkspace = os.path.realpath(tempfile.mkdtemp())
        self._testbed = WorkspaceGen2(self._testWorkspace)

    def tearDown(self):
        shutil.rmtree(self._testWorkspace, ignore_errors=True)

    def testRepr(self):
        # Required to match constructor call
        self.assertEqual(repr(self._testbed), "WorkspaceGen2(" + repr(self._testWorkspace) + ")")

    def testEq(self):
        copied = WorkspaceGen2(self._testWorkspace)
        self.assertEqual(self._testbed, copied)

        alternate = WorkspaceGen3(self._testWorkspace)
        self.assertNotEqual(self._testbed, alternate)
        self.assertNotEqual(copied, alternate)

        with tempfile.TemporaryDirectory() as temp:
            different = WorkspaceGen2(temp)
            self.assertNotEqual(self._testbed, different)
            self.assertNotEqual(copied, different)

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
        newPath = '_temp2'  # can't use mkdtemp because creation is what we're testing
        shutil.rmtree(newPath, ignore_errors=True)
        self.assertFalse(os.path.exists(newPath), 'Workspace directory must not exist before test.')

        try:
            WorkspaceGen2(newPath)
            self.assertTrue(os.path.exists(newPath), 'Workspace directory must exist.')
        finally:
            shutil.rmtree(newPath, ignore_errors=True)

    @staticmethod
    def _allRepos(workspace):
        """An iterator over all repos exposed by a WorkspaceGen2.
        """
        yield workspace.dataRepo
        yield workspace.calibRepo
        yield workspace.templateRepo
        yield workspace.outputRepo

    def testDirectories(self):
        """Verify that a WorkspaceGen2 creates repositories in the target directory.

        The exact repository locations are not tested, as they are likely to change.
        """
        # Workspace should report all paths as absolute
        root = os.path.abspath(os.path.realpath(self._testWorkspace))
        self.assertEqual(self._testbed.workDir, root)
        self._assertInDir(self._testbed.configDir, root)
        for repo in self._allRepos(self._testbed):
            # Workspace spec allows these to be URIs or paths, whatever the Butler accepts
            self._assertInDir(url2pathname(repo), root)

    def testDatabase(self):
        """Verify that a WorkspaceGen2 requests a database file in the target
        directory, but not in any repository.
        """
        root = self._testWorkspace
        self._assertInDir(self._testbed.dbLocation, root)
        for repo in self._allRepos(self._testbed):
            # Workspace spec allows these to be URIs or paths, whatever the Butler accepts
            self._assertNotInDir(self._testbed.dbLocation, url2pathname(repo))

    def testAlerts(self):
        """Verify that a WorkspaceGen2 requests an alert dump in the target
        directory, but not in any repository.
        """
        root = self._testWorkspace
        self._assertInDir(self._testbed.alertLocation, root)
        for repo in self._allRepos(self._testbed):
            # Workspace spec allows these to be URIs or paths, whatever the Butler accepts
            self._assertNotInDir(self._testbed.alertLocation, url2pathname(repo))


class WorkspaceGen3TestSuite(lsst.utils.tests.TestCase):

    def setUp(self):
        # Use realpath to avoid link problems
        self._testWorkspace = os.path.realpath(tempfile.mkdtemp())
        self._testbed = WorkspaceGen3(self._testWorkspace)

    def tearDown(self):
        shutil.rmtree(self._testWorkspace, ignore_errors=True)

    def testRepr(self):
        # Required to match constructor call
        self.assertEqual(repr(self._testbed), "WorkspaceGen3(" + repr(self._testWorkspace) + ")")

    def testEq(self):
        copied = WorkspaceGen3(self._testWorkspace)
        self.assertEqual(self._testbed, copied)

        alternate = WorkspaceGen2(self._testWorkspace)
        self.assertNotEqual(self._testbed, alternate)
        self.assertNotEqual(copied, alternate)

        with tempfile.TemporaryDirectory() as temp:
            different = WorkspaceGen3(temp)
            self.assertNotEqual(self._testbed, different)
            self.assertNotEqual(copied, different)

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
        newPath = '_temp3'  # can't use mkdtemp because creation is what we're testing
        shutil.rmtree(newPath, ignore_errors=True)
        self.assertFalse(os.path.exists(newPath), 'Workspace directory must not exist before test.')

        try:
            WorkspaceGen3(newPath)
            self.assertTrue(os.path.exists(newPath), 'Workspace directory must exist.')
        finally:
            shutil.rmtree(newPath, ignore_errors=True)

    def testDirectories(self):
        """Verify that a WorkspaceGen3 creates subdirectories in the target directory.

        The exact locations are not tested, as they are likely to change.
        """
        # Workspace should report all paths as absolute
        root = os.path.abspath(os.path.realpath(self._testWorkspace))
        self.assertEqual(self._testbed.workDir, root)
        self._assertInDir(self._testbed.configDir, root)
        # Workspace spec allows these to be URIs or paths, whatever the Butler accepts
        self._assertInDir(url2pathname(self._testbed.repo), root)

    def testDatabase(self):
        """Verify that a WorkspaceGen3 requests a database file in the target
        directory, but not in any repository.
        """
        root = self._testWorkspace
        self._assertInDir(self._testbed.dbLocation, root)
        # Workspace spec allows these to be URIs or paths, whatever the Butler accepts
        self._assertNotInDir(self._testbed.dbLocation, url2pathname(self._testbed.repo))

    def testAlerts(self):
        """Verify that a WorkspaceGen3 requests an alert dump in the target
        directory, but not in any repository.
        """
        root = self._testWorkspace
        self._assertInDir(self._testbed.alertLocation, root)
        # Workspace spec allows these to be URIs or paths, whatever the Butler accepts
        self._assertNotInDir(self._testbed.alertLocation, url2pathname(self._testbed.repo))

    def testWorkButler(self):
        """Verify that the Gen 3 Butler is available if and only if the repository is set up.
        """
        with self.assertRaises(RuntimeError):
            self._testbed.workButler
        lsst.daf.butler.Butler.makeRepo(self._testbed.repo)
        # Can't really test Butler's state, so just make sure it exists
        self.assertTrue(self._testbed.workButler.isWriteable())

    def testAnalysisButler(self):
        """Verify that the Gen 3 Butler is available if and only if the repository is set up.
        """
        with self.assertRaises(RuntimeError):
            self._testbed.analysisButler
        lsst.daf.butler.Butler.makeRepo(self._testbed.repo)
        # Can't really test Butler's state, so just make sure it exists
        self.assertFalse(self._testbed.analysisButler.isWriteable())


class MemoryTester(lsst.utils.tests.MemoryTestCase):
    pass


def setup_module(module):
    lsst.utils.tests.init()


if __name__ == "__main__":
    lsst.utils.tests.init()
    unittest.main()
