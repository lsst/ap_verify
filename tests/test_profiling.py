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

import unittest

import numpy as np
import astropy.units as u

import lsst.utils.tests
import lsst.afw.image as afwImage
from lsst.ip.isr import FringeTask
import lsst.verify
from lsst.ap.verify.measurements.profiling import measureRuntime


def _createFringe(width, height, filterName):
    """Create a fringe frame

    Parameters
    ----------
    width, height: `int`
        Size of image
    filterName: `str`
        name of the filterName to use

    Returns
    -------
    fringe: `lsst.afw.image.ExposureF`
        Fringe frame
    """
    image = afwImage.ImageF(width, height)
    array = image.getArray()
    freq = np.pi / 10.0
    x, y = np.indices(array.shape)
    array[x, y] = np.sin(freq * x) + np.sin(freq * y)
    exp = afwImage.makeExposure(afwImage.makeMaskedImage(image))
    exp.setFilter(afwImage.Filter(filterName))
    return exp


class MeasureRuntimeTestSuite(lsst.utils.tests.TestCase):

    def setUp(self):
        """Run a dummy instance of `FringeTask` so that test cases can measure it.
        """
        # Create dummy filter and fringe so that `FringeTask` has short but
        # significant run time.
        # Code adapted from lsst.ip.isr.test_fringes
        size = 128
        dummyFilter = 'FILTER'
        afwImage.utils.defineFilter(dummyFilter, lambdaEff=0)
        exp = _createFringe(size, size, dummyFilter)

        # Create and run `FringeTask` itself
        config = FringeTask.ConfigClass()
        config.filters = [dummyFilter]
        config.num = 1000
        config.small = 1
        config.large = size // 4
        config.pedestal = False
        self.task = FringeTask(name="fringe", config=config)
        self.task.run(exp, exp)

    def tearDown(self):
        del self.task

    def testValid(self):
        """Verify that timing information can be recovered.
        """
        meas = measureRuntime(self.task.getFullMetadata(), taskName='fringe', metricName='ip_isr.IsrTime')
        self.assertIsInstance(meas, lsst.verify.Measurement)
        self.assertEqual(meas.metric_name, lsst.verify.Name(metric='ip_isr.IsrTime'))
        self.assertGreater(meas.quantity, 0.0 * u.second)
        # Task normally takes 0.2 s, so this should be a safe margin of error
        self.assertLess(meas.quantity, 10.0 * u.second)

    def testNoMetric(self):
        """Verify that trying to measure a nonexistent metric fails.
        """
        with self.assertRaises(TypeError):
            measureRuntime(self.task.getFullMetadata(), taskName='fringe', metricName='foo.bar.FooBarTime')

    def testNotRun(self):
        """Verify that trying to measure a real but inapplicable metric returns None.
        """
        notRun = FringeTask(name="fringe", config=FringeTask.ConfigClass())
        meas = measureRuntime(notRun.getFullMetadata(), taskName='fringe', metricName='ip_isr.IsrTime')
        self.assertIsNone(meas)


class MemoryTester(lsst.utils.tests.MemoryTestCase):
    pass


def setup_module(module):
    lsst.utils.tests.init()


if __name__ == "__main__":
    lsst.utils.tests.init()
    unittest.main()
