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

import unittest

import astropy.units as u

from lsst.ip.isr import IsrTask
import lsst.utils.tests
from lsst.verify import Measurement
from lsst.ap.verify.measurements.profiling import measure_runtime


class MeasureRuntimeTestSuite(lsst.utils.tests.TestCase):

    def setUp(self):
        self.task = IsrTask()
        try:
            self.task.run(ccdExposure=None)
        except AttributeError:
            # I wanted the run to fail...
            pass

    def tearDown(self):
        del self.task

    def test_valid(self):
        """Verify that timing information can be recovered.
        """
        meas = measure_runtime(self.task.getFullMetadata(), task_name='isr', metric_name='ip_isr.IsrTime')
        self.assertIsInstance(meas, Measurement)
        self.assertEqual(meas.metric_name, lsst.verify.Name(metric='ip_isr.IsrTime'))
        self.assertGreater(meas.quantity, 0.0 * u.second)
        # The Task didn't actually do anything, so it should be short
        self.assertLess(meas.quantity, 1.0 * u.second)

    def test_no_metric(self):
        """Verify that trying to measure a nonexistent metric fails.
        """
        with self.assertRaises(TypeError):
            measure_runtime(self.task.getFullMetadata(), task_name='isr', metric_name='foo.bar.FooBarTime')

    def test_not_run(self):
        """Verify that trying to measure a real but inapplicable metric returns None.
        """
        not_run = IsrTask(IsrTask.ConfigClass())
        meas = measure_runtime(not_run.getFullMetadata(), task_name='isr', metric_name='ip_isr.IsrTime')
        self.assertIsNone(meas)


class MemoryTester(lsst.utils.tests.MemoryTestCase):
    pass


def setup_module(module):
    lsst.utils.tests.init()


if __name__ == "__main__":
    lsst.utils.tests.init()
    unittest.main()
