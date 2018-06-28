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

import shlex
import unittest

import lsst.utils.tests
import lsst.ap.verify.ap_verify as ap_verify
import lsst.ap.verify.testUtils


class CommandLineTestSuite(lsst.ap.verify.testUtils.DataTestCase):
    # DataTestCase's test dataset is needed for successful parsing of the --dataset argument

    def _parseString(self, commandLine, parser=None):
        """Tokenize and parse a command line string.

        Parameters
        ----------
        commandLine : `str`
            a string containing Unix-style command line arguments, but not the
            name of the program
        parser : `argparse.ArgumentParser`, optional
            the parser to use. Defaults to ``ap_verify``'s primary parser.

        Returns
        -------
        parsed : `argparse.Namespace`
            The parsed command line.
        """
        if not parser:
            parser = ap_verify._ApVerifyParser()

        return parser.parse_args(shlex.split(commandLine))

    def testMissingMain(self):
        """Verify that a command line consisting missing required arguments is rejected.
        """
        args = '--dataset %s --output tests/output/foo' % CommandLineTestSuite.datasetKey
        with self.assertRaises(SystemExit):
            self._parseString(args)

    def testMissingIngest(self):
        """Verify that a command line consisting missing required arguments is rejected.
        """
        args = '--dataset %s' % CommandLineTestSuite.datasetKey
        with self.assertRaises(SystemExit):
            self._parseString(args, ap_verify._IngestOnlyParser())

    def testMinimumMain(self):
        """Verify that a command line consisting only of required arguments parses correctly.
        """
        args = '--dataset %s --output tests/output/foo --id "visit=54123"' % CommandLineTestSuite.datasetKey
        parsed = self._parseString(args)
        self.assertIn('dataset', dir(parsed))
        self.assertIn('output', dir(parsed))
        self.assertIn('dataId', dir(parsed))

    def testMinimumIngest(self):
        """Verify that a command line consisting only of required arguments parses correctly.
        """
        args = '--dataset %s --output tests/output/foo' % CommandLineTestSuite.datasetKey
        parsed = self._parseString(args, ap_verify._IngestOnlyParser())
        self.assertIn('dataset', dir(parsed))
        self.assertIn('output', dir(parsed))

    def testRerun(self):
        """Verify that a command line with reruns is handled correctly.
        """
        args = '--dataset %s --rerun me --id "visit=54123"' % CommandLineTestSuite.datasetKey
        parsed = self._parseString(args)
        out = ap_verify._getOutputDir('non_lsst_repo/', parsed.output, parsed.rerun)
        self.assertEqual(out, 'non_lsst_repo/rerun/me')

    def testRerunInput(self):
        """Verify that a command line trying to redirect input is rejected.
        """
        args = '--dataset %s --rerun from:to --id "visit=54123"' % CommandLineTestSuite.datasetKey
        with self.assertRaises(SystemExit):
            self._parseString(args)

    def testTwoOutputs(self):
        """Verify that a command line with both --output and --rerun is rejected.
        """
        args = '--dataset %s --output tests/output/foo --rerun me --id "visit=54123"' \
            % CommandLineTestSuite.datasetKey
        with self.assertRaises(SystemExit):
            self._parseString(args)

    def testBadDataset(self):
        """Verify that a command line with an unregistered dataset is rejected.
        """
        args = '--dataset FooScope --output tests/output/foo --id "visit=54123"'
        with self.assertRaises(SystemExit):
            self._parseString(args)

    def testBadKeyMain(self):
        """Verify that a command line with unsupported arguments is rejected.
        """
        args = '--dataset %s --output tests/output/foo --id "visit=54123" --clobber' \
            % CommandLineTestSuite.datasetKey
        with self.assertRaises(SystemExit):
            self._parseString(args)

    def testBadKeyIngest(self):
        """Verify that a command line with unsupported arguments is rejected.
        """
        args = '--dataset %s --output tests/output/foo --id "visit=54123"' \
            % CommandLineTestSuite.datasetKey
        with self.assertRaises(SystemExit):
            self._parseString(args, ap_verify._IngestOnlyParser())


class MemoryTester(lsst.utils.tests.MemoryTestCase):
    pass


def setup_module(module):
    lsst.utils.tests.init()


if __name__ == "__main__":
    lsst.utils.tests.init()
    unittest.main()
