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

import unittest

import lsst.utils.tests
import lsst.afw.image as afwImage
import lsst.pipe.base.testUtils as pipelineTests
from lsst.ap.verify.testPipeline import MockIsrTask


class MockTaskTestSuite(unittest.TestCase):
    """Test that mock tasks have the correct inputs and outputs for the task
    they are replacing.

    These tests assume that the mock tasks use real config and connection
    classes, and therefore out-of-date mocks won't match their connections.
    """

    def testMockIsr(self):
        # Testing MockIsrTask is tricky because the real ISR has an unstable
        # interface with dozens of potential inputs, too many to pass through
        # runTestQuantum. I don't see a good way to test the inputs;
        # fortunately, this is unlikely to matter for the overall goal of
        # testing ap_verify's interaction with the AP pipeline.
        task = MockIsrTask()
        pipelineTests.assertValidInitOutput(task)
        # Skip runTestQuantum
        result = task.run(afwImage.ExposureF())
        pipelineTests.assertValidOutput(task, result)


class MemoryTester(lsst.utils.tests.MemoryTestCase):
    pass


def setup_module(module):
    lsst.utils.tests.init()


if __name__ == "__main__":
    lsst.utils.tests.init()
    unittest.main()
