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

import astropy.units as u
import os

import lsst.daf.base as dafBase
import lsst.dax.ppdb as daxPpdb
import lsst.afw.geom as afwGeom
import lsst.afw.table as afwTable
from lsst.ap.association import \
    make_dia_source_schema, \
    make_dia_object_schema
import lsst.utils.tests
from lsst.verify import Measurement
from lsst.ap.verify.measurements.association import \
    measureTotalUnassociatedDiaObjects

# Define the root of the tests relative to this file
ROOT = os.path.abspath(os.path.dirname(__file__))

# Define a generic dataId
dataIdDict = {'visit': 1111,
              'filter': 'r'}


def createTestPoints(nPoints,
                     startId=0,
                     schema=None):
    """Create dummy DIASources or DIAObjects for use in our tests.

    Parameters
    ----------
    nPoints : `int`
        Number of data points to create.
    startId : `int`
        Unique id of the first object to create. The remaining sources are
        incremented by one from the first id.
    schema : `lsst.afw.table.Schema`
        Schema of the objects to create. Defaults to the DIASource schema.

    Returns
    -------
    testPoints : `lsst.afw.table.SourceCatalog`
        Catalog of points to test.
    """
    if schema is None:
        schema = make_dia_source_schema()
    sources = afwTable.SourceCatalog(schema)

    for src_idx in range(nPoints):
        src = sources.addNew()
        # Set everything to a simple default value.
        for subSchema in schema:
            if subSchema.getField().getTypeString() == "Angle":
                continue
            elif subSchema.getField().getTypeString() == "String":
                # Assume that the string column contains the filter name.
                src[subSchema.getField().getName()] = 'g'
            else:
                src[subSchema.getField().getName()] = 1
        # Set the ids by hand
        src['id'] = src_idx + startId
        coord = afwGeom.SpherePoint(src_idx, src_idx, afwGeom.degrees)
        src.setCoord(coord)

    return sources


class MeasureAssociationTestSuite(lsst.utils.tests.TestCase):

    def setUp(self):

        self.numTestDiaObjects = 5
        self.diaObjects = createTestPoints(
            5, schema=make_dia_object_schema())
        for diaObject in self.diaObjects:
            diaObject['nDiaSources'] = 1

        self.ppdbCfg = daxPpdb.PpdbConfig()
        # Create DB in memory.
        self.ppdbCfg.db_url = 'sqlite://'
        self.ppdbCfg.isolation_level = "READ_UNCOMMITTED"
        self.ppdbCfg.dia_object_index = "baseline"
        self.ppdbCfg.dia_object_columns = []
        self.ppdb = daxPpdb.Ppdb(
            config=self.ppdbCfg,
            afw_schemas=dict(DiaObject=make_dia_object_schema(),
                             DiaSource=make_dia_source_schema()))
        self.ppdb.makeSchema(drop=True)

        dateTime = dafBase.DateTime(nsecs=1400000000 * 10**9)
        self.ppdb.storeDiaObjects(self.diaObjects, dateTime.toPython())

    def tearDown(self):
        del self.ppdb

    def testValidFromPpdb(self):
        # Need to have a valid ppdb object so that the internal sqlalchemy
        # calls work.
        meas = measureTotalUnassociatedDiaObjects(
            self.ppdb,
            metricName='association.numTotalUnassociatedDiaObjects')
        self.assertIsInstance(meas, Measurement)
        self.assertEqual(
            meas.metric_name,
            lsst.verify.Name(
                metric='association.numTotalUnassociatedDiaObjects'))
        self.assertEqual(meas.quantity, self.numTestDiaObjects * u.count)

    def testNoMetric(self):
        """Verify that trying to measure a nonexistent metric fails.
        """
        with self.assertRaises(TypeError):
            measureTotalUnassociatedDiaObjects(
                self.ppdb, metricName='foo.bar.FooBar')


class MemoryTester(lsst.utils.tests.MemoryTestCase):
    pass


def setup_module(module):
    lsst.utils.tests.init()


if __name__ == "__main__":
    lsst.utils.tests.init()
    unittest.main()
