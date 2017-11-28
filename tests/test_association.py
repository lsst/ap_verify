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
    measure_number_new_dia_objects, \
    measure_number_unassociated_dia_objects, \
    measure_fraction_updated_dia_objects, \
    measure_number_sci_sources, \
    measure_fraction_dia_sources_to_sci_sources, \
    measure_total_unassociated_dia_objects

# Define the root of the tests relative to this file
ROOT = os.path.abspath(os.path.dirname(__file__))

# Define a generic dataId
dataId_dict = {'visit': 1111,
               'filter': 'r'}


def create_test_dia_objects(n_objects=1,
                            n_sources=1,
                            start_id=0,
                            object_centers_degrees=None,
                            scatter_arcsec=1.0):
    """ Create DIAObjects with a specified number of DIASources attached.

    Parameters
    ----------
    n_objects : int
        Number of DIAObjects to generate.
    n_src : int
        Number of DIASources to generate for each DIAObject.
    start_id : int
        Starting index to increment the created DIAObjects from.
    object_centers_degrees : (N, 2) list of floats
        Centers of each DIAObject to create.
    scatter_arcsec : float
        Scatter to add to the position of each DIASource.

    Returns
    -------
    A list of DIAObjects
    """

    if object_centers_degrees is None:
        object_centers_degrees = [[idx, idx] for idx in range(n_objects)]

    output_dia_objects = []
    for obj_idx in range(n_objects):
        src_cat = create_test_sources(
            n_sources,
            start_id + obj_idx * n_sources,
            [object_centers_degrees[obj_idx] for src_idx in range(n_sources)],
            scatter_arcsec)
        output_dia_objects.append(DIAObject(src_cat))
    return output_dia_objects


def create_test_sources(n_sources=5,
                        start_id=0,
                        source_locs_deg=None,
                        scatter_arcsec=1.0):
    """ Create dummy DIASources for use in our tests.

    Parameters
    ----------
    n_sources : int
        Number of fake sources to create for testing.
    start_id : int
        Unique id of the first object to create. The remaining sources are
        incremented by one from the first id.
    source_locs_deg : (N, 2) list of floats
        Positions of the DIASources to create.
    scatter_arcsec : float
        Scatter to add to the position of each DIASource.

    Returns
    -------
    A lsst.afw.SourceCatalog
    """
    sources = afwTable.SourceCatalog(make_minimal_dia_source_schema())

    if source_locs_deg is None:
        source_locs_deg = [[idx, idx] for idx in range(n_sources)]

    for src_idx in range(n_sources):
        src = sources.addNew()
        src['id'] = src_idx + start_id
        coord = Coord(source_locs_deg[src_idx][0] * afwGeom.degrees,
                      source_locs_deg[src_idx][1] * afwGeom.degrees)
        if scatter_arcsec > 0.0:
            coord.offset(
                np.random.rand() * 360 * afwGeom.degrees,
                np.random.rand() * scatter_arcsec * afwGeom.arcseconds)
        src.setCoord(coord)

    return sources


class MeasureAssociationTestSuite(lsst.utils.tests.TestCase):

    def setUp(self):

        # Create an unrun AssociationTask.
        self.assoc_task = AssociationTask()

        # Create a empty butler repository and put data in it.
        self.testDir = tempfile.mkdtemp(
            dir=ROOT, prefix="TestAssocMeasurements-")
        outputRepoArgs = dafPersist.RepositoryArgs(
            root=os.path.join(self.testDir, 'repoA'),
            mapper=obsTest.TestMapper,
            mode='rw')
        self.butler = dafPersist.Butler(
            outputs=outputRepoArgs)
        test_sources = create_test_sources(10)
        test_dia_sources = create_test_sources(5)
        self.butler.put(obj=test_sources,
                        datasetType='src',
                        dataId=dataId_dict)
        self.butler.put(obj=test_dia_sources,
                        datasetType='deepDiff_diaSrc',
                        dataId=dataId_dict)

        (self.tmp_file, self.db_file) = tempfile.mkstemp(
            dir=os.path.dirname(__file__))
        assoc_db_config = AssociationDBSqliteConfig()
        assoc_db_config.db_name = self.db_file
        assoc_db = AssociationDBSqliteTask(config=assoc_db_config)
        assoc_db.create_tables()

        dia_collection = DIAObjectCollection(create_test_dia_objects(5))
        assoc_db.store(dia_collection, True)
        assoc_db.close()

    def tearDown(self):
        del self.assoc_task

        if os.path.exists(self.testDir):
            shutil.rmtree(self.testDir)
        if hasattr(self, "butler"):
            del self.butler

        del self.tmp_file
        os.remove(self.db_file)

    def test_valid_from_metadata(self):
        """Verify that association information can be recovered from metadata.
        """
        # Insert data into the task metadata.
        test_assoc_result = pipeBase.Struct(
            n_updated_dia_objects=5,
            n_new_dia_objects=6,
            n_unassociated_dia_objects=7,)
        self.assoc_task._add_association_meta_data(test_assoc_result)
        metadata = self.assoc_task.getFullMetadata()

        meas = measure_number_new_dia_objects(
            metadata,
            "association",
            "association.numNewDIAObjects")
        self.assertIsInstance(meas, Measurement)
        self.assertEqual(
            meas.metric_name,
            lsst.verify.Name(metric="association.numNewDIAObjects"))
        self.assertEqual(meas.quantity, 6 * u.count)

        meas = measure_number_unassociated_dia_objects(
            metadata,
            'association',
            'association.fracUpdatedDIAObjects')
        self.assertIsInstance(meas, Measurement)
        self.assertEqual(
            meas.metric_name,
            lsst.verify.Name(
                metric='association.fracUpdatedDIAObjects'))
        self.assertEqual(meas.quantity, 7 * u.count)

        meas = measure_fraction_updated_dia_objects(
            metadata,
            'association',
            'association.fracUpdatedDIAObjects')
        self.assertIsInstance(meas, Measurement)
        self.assertEqual(
            meas.metric_name,
            lsst.verify.Name(metric='association.fracUpdatedDIAObjects'))
        self.assertEqual(meas.quantity, 5 / (5 + 7) * u.dimensionless_unscaled)


    def test_valid_from_butler(self):
        """ Test the association measurements that require a butler.
        """
        meas = measure_number_sci_sources(
            self.butler,
            dataId_dict=dataId_dict,
            metric_name='ip_diffim.numSciSrc')
        self.assertIsInstance(meas, Measurement)
        self.assertEqual(
            meas.metric_name,
            lsst.verify.Name(metric='ip_diffim.numSciSrc'))
        self.assertEqual(meas.quantity, 10 * u.count)

        meas = measure_fraction_dia_sources_to_sci_sources(
            self.butler,
            dataId_dict=dataId_dict,
            metric_name='ip_diffim.fracDiaSrcToSciSrc')
        self.assertIsInstance(meas, Measurement)
        self.assertEqual(
            meas.metric_name,
            lsst.verify.Name(metric='ip_diffim.fracDiaSrcToSciSrc'))
        # We put in half the number of DIASources as detected sources.
        self.assertEqual(meas.quantity, 0.5 * u.dimensionless_unscaled)


    def test_valid_from_sqlite(self):
        conn = sqlite3.connect(self.db_file)
        cursor = conn.cursor()

        meas = measure_total_unassociated_dia_objects(
            cursor,
            metric_name='association.numTotalUnassociatedDiaObjects')
        self.assertIsInstance(meas, Measurement)
        self.assertEqual(
            meas.metric_name,
            lsst.verify.Name(
                metric='association.numTotalUnassociatedDiaObjects'))
        self.assertEqual(meas.quantity, 5 * u.count)

    def test_no_butler_data(self):
        """ Test attempting to create a measurement with data that the butler
        does not contain.
        """

        with self.assertRaises(dafPersist.NoResults):
            measure_number_sci_sources(
                self.butler,
                dataId_dict={'visit': 1000, 'filter': 'r'},
                metric_name='ip_diffim.fracDiaSrcToSciSrc')

        with self.assertRaises(dafPersist.NoResults):
            measure_fraction_dia_sources_to_sci_sources(
                self.butler,
                dataId_dict={'visit': 1000, 'filter': 'r'},
                metric_name='ip_diffim.fracDiaSrcToSciSrc')

    def test_metadata_not_created(self):
        """ Test for the correct failure when measuring from non-existant
        metadata.
        """
        metadata = self.assoc_task.getFullMetadata()

        meas = measure_number_new_dia_objects(
            metadata,
            "association",
            "association.numNewDIAObjects")
        self.assertIsNone(meas)
  
    def test_invalid_db(self):
        """ Test that the measurement raises the correct error when given an
        improper database.
        """
        conn = sqlite3.connect(":memory:")
        cursor = conn.cursor()
        with self.assertRaises(sqlite3.OperationalError):
            measure_total_unassociated_dia_objects(
                cursor,
                metric_name='association.numTotalUnassociatedDiaObjects')

    def test_no_metric(self):
        """Verify that trying to measure a nonexistent metric fails.
        """
        test_assoc_result = pipeBase.Struct(
            n_updated_dia_objects=5,
            n_new_dia_objects=6,
            n_unassociated_dia_objects=7,)
        self.assoc_task._add_association_meta_data(test_assoc_result)
        metadata = self.assoc_task.getFullMetadata()
        with self.assertRaises(TypeError):
            measure_number_new_dia_objects(metadata, "association", "foo.bar.FooBar")
        with self.assertRaises(TypeError):
            measure_number_unassociated_dia_objects(
                metadata, "association", "foo.bar.FooBar")
        with self.assertRaises(TypeError):
            measure_fraction_updated_dia_objects(
                metadata, "association", "foo.bar.FooBar")
            
        with self.assertRaises(TypeError):
            measure_number_sci_sources(
                self.butler, dataId={'visit': 1111, 'filter': 'r'},
                metric_name='foo.bar.FooBar')
        with self.assertRaises(TypeError):
            measure_fraction_dia_sources_to_sci_sources(
                self.butler, dataId={'visit': 1111, 'filter': 'r'},
                metric_name='foo.bar.FooBar')
        
        conn = sqlite3.connect(self.db_file)
        cursor = conn.cursor()
        with self.assertRaises(TypeError):
            measure_total_unassociated_dia_objects(cursor, metric_name='foo.bar.FooBar')


class MemoryTester(lsst.utils.tests.MemoryTestCase):
    pass


def setup_module(module):
    lsst.utils.tests.init()


if __name__ == "__main__":
    lsst.utils.tests.init()
    unittest.main()
