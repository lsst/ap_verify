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
from unittest.mock import NonCallableMock

import astropy.units as u
import os

import lsst.daf.base as dafBase
import lsst.daf.persistence as dafPersist
import lsst.dax.ppdb as daxPpdb
import lsst.afw.geom as afwGeom
import lsst.afw.table as afwTable
from lsst.ap.association import \
    make_dia_source_schema, \
    make_dia_object_schema, \
    AssociationTask
import lsst.pipe.base as pipeBase
import lsst.utils.tests
from lsst.verify import Measurement
from lsst.ap.verify.measurements.association import \
    measureNumberNewDiaObjects, \
    measureNumberUnassociatedDiaObjects, \
    measureFractionUpdatedDiaObjects, \
    measureNumberSciSources, \
    measureFractionDiaSourcesToSciSources, \
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

        # Create an unrun AssociationTask.
        self.assocTask = AssociationTask()

        # Create a empty butler repository and put data in it.
        self.numTestSciSources = 10
        self.numTestDiaSources = 5
        testSources = createTestPoints(self.numTestSciSources)
        testDiaSources = createTestPoints(self.numTestDiaSources)

        self.numTestDiaObjects = 5
        self.diaObjects = createTestPoints(
            5, schema=make_dia_object_schema())
        for diaObject in self.diaObjects:
            diaObject['nDiaSources'] = 1

        # Fake Butler to avoid initialization and I/O overhead
        def mockGet(datasetType, dataId=None):
            """An emulator for `lsst.daf.persistence.Butler.get` that can only handle test data.
            """
            # Check whether dataIdDict is a subset of dataId
            if dataIdDict.items() <= dataId.items():
                if datasetType == 'src':
                    return testSources
                elif datasetType == 'deepDiff_diaSrc':
                    return testDiaSources
            raise dafPersist.NoResults("Dataset not found:", datasetType, dataId)

        self.butler = NonCallableMock(spec=dafPersist.Butler, get=mockGet)

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
        del self.assocTask
        del self.ppdb

        if hasattr(self, "butler"):
            del self.butler

    def testValidFromMetadata(self):
        """Verify that association information can be recovered from metadata.
        """
        # Insert data into the task metadata.
        nUpdatedDiaObjects = 5
        nNewDiaObjects = 6
        nUnassociatedDiaObjects = 7
        testAssocResult = pipeBase.Struct(
            n_updated_dia_objects=nUpdatedDiaObjects,
            n_new_dia_objects=nNewDiaObjects,
            n_unassociated_dia_objects=nUnassociatedDiaObjects,)
        self.assocTask._add_association_meta_data(testAssocResult)
        metadata = self.assocTask.getFullMetadata()

        meas = measureNumberNewDiaObjects(
            metadata,
            "association",
            "association.numNewDIAObjects")
        self.assertIsInstance(meas, Measurement)
        self.assertEqual(
            meas.metric_name,
            lsst.verify.Name(metric="association.numNewDIAObjects"))
        self.assertEqual(meas.quantity, nNewDiaObjects * u.count)

        meas = measureNumberUnassociatedDiaObjects(
            metadata,
            'association',
            'association.fracUpdatedDIAObjects')
        self.assertIsInstance(meas, Measurement)
        self.assertEqual(
            meas.metric_name,
            lsst.verify.Name(
                metric='association.fracUpdatedDIAObjects'))
        self.assertEqual(meas.quantity, nUnassociatedDiaObjects * u.count)

        meas = measureFractionUpdatedDiaObjects(
            metadata,
            'association',
            'association.fracUpdatedDIAObjects')
        self.assertIsInstance(meas, Measurement)
        self.assertEqual(
            meas.metric_name,
            lsst.verify.Name(metric='association.fracUpdatedDIAObjects'))
        value = nUpdatedDiaObjects / (nUpdatedDiaObjects + nUnassociatedDiaObjects)
        self.assertEqual(meas.quantity, value * u.dimensionless_unscaled)

    def testValidFromButler(self):
        """ Test the association measurements that require a butler.
        """
        meas = measureNumberSciSources(
            self.butler,
            dataIdDict=dataIdDict,
            metricName='ip_diffim.numSciSrc')
        self.assertIsInstance(meas, Measurement)
        self.assertEqual(
            meas.metric_name,
            lsst.verify.Name(metric='ip_diffim.numSciSrc'))
        self.assertEqual(meas.quantity, self.numTestSciSources * u.count)

        meas = measureFractionDiaSourcesToSciSources(
            self.butler,
            dataIdDict=dataIdDict,
            metricName='ip_diffim.fracDiaSrcToSciSrc')
        self.assertIsInstance(meas, Measurement)
        self.assertEqual(
            meas.metric_name,
            lsst.verify.Name(metric='ip_diffim.fracDiaSrcToSciSrc'))
        self.assertEqual(meas.quantity,
                         self.numTestDiaSources / self.numTestSciSources * u.dimensionless_unscaled)

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

    def testNoButlerData(self):
        """ Test attempting to create a measurement with data that the butler
        does not contain.
        """

        with self.assertRaises(dafPersist.NoResults):
            measureNumberSciSources(
                self.butler,
                dataIdDict={'visit': 1000, 'filter': 'r'},
                metricName='ip_diffim.fracDiaSrcToSciSrc')

        with self.assertRaises(dafPersist.NoResults):
            measureFractionDiaSourcesToSciSources(
                self.butler,
                dataIdDict={'visit': 1000, 'filter': 'r'},
                metricName='ip_diffim.fracDiaSrcToSciSrc')

        with self.assertRaises(dafPersist.NoResults):
            measureNumberSciSources(
                self.butler,
                dataIdDict={'visit': 1111, 'filter': 'g'},
                metricName='ip_diffim.fracDiaSrcToSciSrc')

        with self.assertRaises(dafPersist.NoResults):
            measureFractionDiaSourcesToSciSources(
                self.butler,
                dataIdDict={'visit': 1111, 'filter': 'g'},
                metricName='ip_diffim.fracDiaSrcToSciSrc')

    def testMetadataNotCreated(self):
        """ Test for the correct failure when measuring from non-existent
        metadata.
        """
        metadata = self.assocTask.getFullMetadata()

        meas = measureNumberNewDiaObjects(
            metadata,
            "association",
            "association.numNewDIAObjects")
        self.assertIsNone(meas)

    def testNoMetric(self):
        """Verify that trying to measure a nonexistent metric fails.
        """
        testAssocResult = pipeBase.Struct(
            n_updated_dia_objects=5,
            n_new_dia_objects=6,
            n_unassociated_dia_objects=7,)
        self.assocTask._add_association_meta_data(testAssocResult)
        metadata = self.assocTask.getFullMetadata()
        with self.assertRaises(TypeError):
            measureNumberNewDiaObjects(
                metadata, "association", "foo.bar.FooBar")
        with self.assertRaises(TypeError):
            measureNumberUnassociatedDiaObjects(
                metadata, "association", "foo.bar.FooBar")
        with self.assertRaises(TypeError):
            measureFractionUpdatedDiaObjects(
                metadata, "association", "foo.bar.FooBar")

        with self.assertRaises(TypeError):
            measureNumberSciSources(
                self.butler, dataId=dataIdDict,
                metricName='foo.bar.FooBar')
        with self.assertRaises(TypeError):
            measureFractionDiaSourcesToSciSources(
                self.butler, dataId=dataIdDict,
                metricName='foo.bar.FooBar')

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
