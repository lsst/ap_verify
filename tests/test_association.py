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
import os
import shutil
import tempfile

import lsst.daf.persistence as dafPersist
import lsst.afw.geom as afwGeom
import lsst.afw.table as afwTable
import lsst.obs.test as obsTest
import lsst.utils.tests
from lsst.verify import Measurement
from lsst.ap.verify.measurements.profiling import \
    measure_dia_sources_to_sci_sources

# Define the root of the tests relative to this file
ROOT = os.path.abspath(os.path.dirname(__file__))

# Define a generic dataId
dataId_dict = {'visit': 1111,
               'filter': 'r'}


def create_test_sources(n_sources=5, schema=None):
    """ Create dummy DIASources for use in our tests.

    Parameters
    ----------
    n_sources : int (optional)
        Number of fake sources to create for testing.

    Returns
    -------
    A lsst.afw.SourceCatalog
    """
    if schema is None:
        schema = afwTable.SourceTable.makeMinimalSchema()

    sources = afwTable.SourceCatalog(schema)

    for src_idx in range(n_sources):
        src = sources.addNew()
        src['id'] = src_idx
        src['coord_ra'] = afwGeom.Angle(0.0 + 1. * src_idx,
                                        units=afwGeom.degrees)
        src['coord_dec'] = afwGeom.Angle(0.0 + 1. * src_idx,
                                         units=afwGeom.degrees)
        # Add a flux at some point

    return sources


class MeasureAssociationTestSuite(lsst.utils.tests.TestCase):

    def setUp(self):

        # Create a empty butler repository and put data in it.
        self.testDir = tempfile.mkdtemp(
            dir=ROOT, prefix="TestAssocMeasurements-")
        inputRepoArgs = dafPersist.RepositoryArgs(
            root=os.path.join(ROOT, 'butlerAlias', 'data', 'input'),
            mapper=obsTest.TestMapper,
            tags='baArgs')
        outputRepoArgs = dafPersist.RepositoryArgs(
            root=os.path.join(self.testDir, 'repoA'),
            mapper=obsTest.TestMapper,
            mode='rw')
        self.butler = dafPersist.Butler(
            inputs=inputRepoArgs, outputs=outputRepoArgs)
        test_sources = create_test_sources(10)
        test_dia_sources = create_test_sources(5)
        self.butler.put(obj=test_sources,
                        datasetType='src',
                        dataId=dataId_dict)
        self.butler.put(obj=test_dia_sources,
                        datasetType='deepDiff_diaSrc',
                        dataId=dataId_dict)

    def tearDown(self):
        if os.path.exists(self.testDir):
            shutil.rmtree(self.testDir)
        if os.path.exists(os.path.join(ROOT,
                                       'butlerAlias/repositoryCfg.yaml')):
            os.remove(os.path.join(ROOT, 'butlerAlias/repositoryCfg.yaml'))
        if hasattr(self, "butler"):
            del self.butler

    def test_valid(self):
        """Verify that assocition information can be recovered.
        """

        meas = measure_dia_sources_to_sci_sources(
            self.butler,
            dataId_dict=dataId_dict,
            metric_name='ip_diffim.fracDiaSrcToSciSrc')
        self.assertIsInstance(meas, Measurement)
        self.assertEqual(
            meas.metric_name,
            lsst.verify.Name(metric='ap_association.fracDiaSrcToSciSrc'))
        # We put in half the number of DIASources as detected sources.
        self.assertEqual(meas.quantity, 0.5 * u.dimensionless_unscaled)

    def test_no_butler_data(self):

        with self.assertRaises(dafPersist.NoResults):
            measure_dia_sources_to_sci_sources(
                self.butler,
                dataId_dict={'visit': 1000, 'filter': 'r'},
                metric_name='ip_diffim.fracDiaSrcToSciSrc')

    def test_no_metric(self):
        """Verify that trying to measure a nonexistent metric fails.
        """
        with self.assertRaises(TypeError):
            measure_dia_sources_to_sci_sources(
                self.butler, dataId='isr', metric_name='foo.bar.FooBarTime')

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
