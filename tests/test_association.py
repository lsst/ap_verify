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

import astropy.units as u
import numpy as np
import os
import shutil
import sqlite3
import tempfile

import lsst.daf.persistence as dafPersist
from lsst.afw.coord import Coord
import lsst.afw.geom as afwGeom
import lsst.afw.table as afwTable
from lsst.ap.association import \
    make_minimal_dia_source_schema, \
    DIAObject, \
    DIAObjectCollection, \
    AssociationDBSqliteTask, \
    AssociationDBSqliteConfig, \
    AssociationTask
import lsst.obs.test as obsTest
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


def createTestDiaObjects(nObjects=1,
                         nSources=1,
                         startId=0,
                         objectCentersDegrees=None,
                         scatterArcsec=1.0):
    """ Create DIAObjects with a specified number of DIASources attached.

    Parameters
    ----------
    nObjects : int
        Number of DIAObjects to generate.
    nSources : int
        Number of DIASources to generate for each DIAObject.
    startId : int
        Starting index to increment the created DIAObjects from.
    objectCentersDegrees : (N, 2) list of floats
        Centers of each DIAObject to create.
    scatterArcsec : float
        Scatter to add to the position of each DIASource.

    Returns
    -------
    A list of DIAObjects
    """

    if objectCentersDegrees is None:
        objectCentersDegrees = [[idx, idx] for idx in range(nObjects)]

    outputDiaObjects = []
    for objIdx in range(nObjects):
        srcCat = createTestSources(
            nSources,
            startId + objIdx * nSources,
            [objectCentersDegrees[objIdx] for srcIdx in range(nSources)],
            scatterArcsec)
        outputDiaObjects.append(DIAObject(srcCat))
    return outputDiaObjects


def createTestSources(nSources=5,
                      startId=0,
                      sourceLocsDeg=None,
                      scatterArcsec=1.0):
    """ Create dummy DIASources for use in our tests.

    Parameters
    ----------
    nSources : int
        Number of fake sources to create for testing.
    startId : int
        Unique id of the first object to create. The remaining sources are
        incremented by one from the first id.
    sourceLocsDeg : (N, 2) list of floats
        Positions of the DIASources to create.
    scatterArcsec : float
        Scatter to add to the position of each DIASource.

    Returns
    -------
    A lsst.afw.SourceCatalog
    """
    sources = afwTable.SourceCatalog(make_minimal_dia_source_schema())

    if sourceLocsDeg is None:
        sourceLocsDeg = [[idx, idx] for idx in range(nSources)]

    for srcIdx in range(nSources):
        src = sources.addNew()
        src['id'] = srcIdx + startId
        coord = Coord(sourceLocsDeg[srcIdx][0] * afwGeom.degrees,
                      sourceLocsDeg[srcIdx][1] * afwGeom.degrees)
        if scatterArcsec > 0.0:
            coord.offset(
                np.random.rand() * 360 * afwGeom.degrees,
                np.random.rand() * scatterArcsec * afwGeom.arcseconds)
        src.setCoord(coord)

    return sources


class MeasureAssociationTestSuite(lsst.utils.tests.TestCase):

    def setUp(self):

        # Create an unrun AssociationTask.
        self.assocTask = AssociationTask()

        # Create a empty butler repository and put data in it.
        self.testDir = tempfile.mkdtemp(
            dir=ROOT, prefix="TestAssocMeasurements-")
        outputRepoArgs = dafPersist.RepositoryArgs(
            root=os.path.join(self.testDir, 'repoA'),
            mapper=obsTest.TestMapper,
            mode='rw')
        self.butler = dafPersist.Butler(
            outputs=outputRepoArgs)
        self.numTestSciSources = 10
        self.numTestDiaSources = 5
        testSources = createTestSources(self.numTestSciSources)
        testDiaSources = createTestSources(self.numTestDiaSources)
        self.butler.put(obj=testSources,
                        datasetType='src',
                        dataId=dataIdDict)
        self.butler.put(obj=testDiaSources,
                        datasetType='deepDiff_diaSrc',
                        dataId=dataIdDict)

        (self.tmpFile, self.dbFile) = tempfile.mkstemp(
            dir=os.path.dirname(__file__))
        assocDbConfig = AssociationDBSqliteConfig()
        assocDbConfig.db_name = self.dbFile
        assocDb = AssociationDBSqliteTask(config=assocDbConfig)
        assocDb.create_tables()

        self.numTestDiaObjects = 5
        diaCollection = DIAObjectCollection(
            createTestDiaObjects(self.numTestDiaObjects))
        assocDb.store(diaCollection, True)
        assocDb.close()

    def tearDown(self):
        del self.assocTask

        if os.path.exists(self.testDir):
            shutil.rmtree(self.testDir)
        if hasattr(self, "butler"):
            del self.butler

        del self.tmpFile
        os.remove(self.dbFile)

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
        self.assertEqual(meas.quantity,
                         nUpdatedDiaObjects /
                         (nUpdatedDiaObjects + nUnassociatedDiaObjects) *
                         u.dimensionless_unscaled)

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
        self.assertEqual(meas.quantity, 10 * u.count)

        meas = measureFractionDiaSourcesToSciSources(
            self.butler,
            dataIdDict=dataIdDict,
            metricName='ip_diffim.fracDiaSrcToSciSrc')
        self.assertIsInstance(meas, Measurement)
        self.assertEqual(
            meas.metric_name,
            lsst.verify.Name(metric='ip_diffim.fracDiaSrcToSciSrc'))
        # We put in half the number of DIASources as detected sources.
        self.assertEqual(meas.quantity, 0.5 * u.dimensionless_unscaled)

    def testValidFromSqlite(self):
        conn = sqlite3.connect(self.dbFile)
        cursor = conn.cursor()

        meas = measureTotalUnassociatedDiaObjects(
            cursor,
            metricName='association.numTotalUnassociatedDiaObjects')
        self.assertIsInstance(meas, Measurement)
        self.assertEqual(
            meas.metric_name,
            lsst.verify.Name(
                metric='association.numTotalUnassociatedDiaObjects'))
        self.assertEqual(meas.quantity, 5 * u.count)

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

    def testInvalidDb(self):
        """ Test that the measurement raises the correct error when given an
        improper database.
        """
        conn = sqlite3.connect(":memory:")
        cursor = conn.cursor()
        with self.assertRaises(sqlite3.OperationalError):
            measureTotalUnassociatedDiaObjects(
                cursor,
                metricName='association.numTotalUnassociatedDiaObjects')

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

        conn = sqlite3.connect(self.dbFile)
        cursor = conn.cursor()
        with self.assertRaises(TypeError):
            measureTotalUnassociatedDiaObjects(
                cursor, metricName='foo.bar.FooBar')


class MemoryTester(lsst.utils.tests.MemoryTestCase):
    pass


def setup_module(module):
    lsst.utils.tests.init()


if __name__ == "__main__":
    lsst.utils.tests.init()
    unittest.main()
