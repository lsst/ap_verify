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

"""Code for measuring software performance metrics.

All measurements assume the necessary information is present in a Task's metadata.
"""

from __future__ import absolute_import, division, print_function

__all__ = ["measure_association"]

import astropy.units as u
import lsst.verify


def measure_number_new_dia_objects(metadata, task_name, metric_name):
    """Computes a wall-clock measurement from metadata provided
    by @pipe.base.timeMethod.

    Parameters
    ----------
    metadata: `lsst.daf.base.PropertySet`
        The metadata to search for timing information.
    task_name: `str`
        The name of the Task, e.g., "processCcd". SubTask names must be the
        ones assigned by the parent Task and may be disambiguated using the
        parent Task name, as in "processCcd:calibrate".
        If `task_name` matches multiple runs of a subTask in different
        contexts, the information for only one run will be provided.
    metric_name: `str`
        The fully qualified name of the metric being measured, e.g.,
        "pipe_tasks.ProcessCcdTime"

    Returns
    -------
    an `lsst.verify.Measurement` for `metric_name`, or `None` if the timing
    information for `task_name` is not present in `metadata`
    """
    if task_name is not "AssociationTask":
        return None

    n_new = metadata.getAsInt("AssociationTask.numNewDIAObjects")
    meas = lsst.verify.Measurement(metric_name, n_new * u.count)
    return meas


def measure_number_unassociated_dia_objects(metadata, task_name, metric_name):
    """Computes a wall-clock measurement from metadata provided
    by @pipe.base.timeMethod.

    Parameters
    ----------
    metadata: `lsst.daf.base.PropertySet`
        The metadata to search for timing information.
    task_name: `str`
        The name of the Task, e.g., "processCcd". SubTask names must be the
        ones assigned by the parent Task and may be disambiguated using the
        parent Task name, as in "processCcd:calibrate".
        If `task_name` matches multiple runs of a subTask in different
        contexts, the information for only one run will be provided.
    metric_name: `str`
        The fully qualified name of the metric being measured, e.g.,
        "pipe_tasks.ProcessCcdTime"

    Returns
    -------
    an `lsst.verify.Measurement` for `metric_name`, or `None` if the timing
    information for `task_name` is not present in `metadata`
    """
    if task_name is not "AssociationTask":
        return None

    n_unassociated = metadata.getAsInt(
        "AssociationTask.numUnassociatedDIAObjects")
    meas = lsst.verify.Measurement(metric_name, n_unassociated * u.count)
    return meas


def measure_fraction_updated_dia_objects(metadata, task_name, metric_name):
    """Computes a wall-clock measurement from metadata provided
    by @pipe.base.timeMethod.

    Parameters
    ----------
    metadata: `lsst.daf.base.PropertySet`
        The metadata to search for timing information.
    task_name: `str`
        The name of the Task, e.g., "processCcd". SubTask names must be the
        ones assigned by the parent Task and may be disambiguated using the
        parent Task name, as in "processCcd:calibrate".
        If `task_name` matches multiple runs of a subTask in different
        contexts, the information for only one run will be provided.
    metric_name: `str`
        The fully qualified name of the metric being measured, e.g.,
        "pipe_tasks.ProcessCcdTime"

    Returns
    -------
    an `lsst.verify.Measurement` for `metric_name`, or `None` if the timing
    information for `task_name` is not present in `metadata`
    """
    if task_name is not "AssociationTask":
        return None

    n_updated = metadata.getAsDouble("AssociationTask.numUpdatedDIAObjects")
    n_unassociated = metadata.getAsDouble(
        "AssociationTask.numUnassociatedDIAObjects")
    meas = lsst.verify.Measurement(
        metric_name,
        n_updated / (n_updated + n_unassociated) * u.dimensionless_unscaled)
    return meas


def measure_dia_sources_to_sci_sources(butler, dataId_dict, metric_name):
    """ Compute the ratio of cataloged science sources to different image
    sources per ccd per visit.

    Parameters
    ----------
    butler: lsst.daf.percistence.Butler instance
        The output repository location to read from disk.
    dataId_dict: dictionary
        Butler identifier naming the data to be processed (e.g., visit and
        ccdnum) formatted in the usual way (e.g., 'visit=54321 ccdnum=7').
    metric_name: `str`
        The fully qualified name of the metric being measured, e.g.,
        "pipe_tasks.ProcessCcdTime"

    Returns
    -------
    an `lsst.verify.Measurement` for `metric_name`, or `None`
    """

    # Parse the input dataId string and convert to a dictionary of values.
    # Hard coded assuming the same input formate as in ap_pipe.

    n_sci_sources = len(butler.get('src', dataId=dataId_dict))
    n_dia_sources = len(butler.get('deepDiff_diaSrc', dataId=dataId_dict))
    meas = lsst.verify.Measurment(
        metric_name,
        n_dia_sources / n_sci_sources * u.dimensionless_unscaled)
    return meas


def measure_total_unassociated_dia_objects(db_cursor, metric_name):
    """ Compute number of DIAObjects with only one association DIASource.

    Parameters
    ----------
    db_cursor : sqlite3.Cursor instance
        Cursor to the sqlite data base created from a previous run of
        AssociationDBSqlite task to load.
    metric_name: `str`
        The fully qualified name of the metric being measured, e.g.,
        "pipe_tasks.ProcessCcdTime"

    Returns
    -------
    an `lsst.verify.Measurement` for `metric_name`, or `None`
    """

    db_cursor.execute("SELECT count(*) FROM dia_objects "
                      "WHERE n_dia_sources = 1")
    (n_unassociated_dia_objects,) = db_cursor.fetchall()[0]

    meas = lsst.verify.Measurment(
        metric_name,
        n_unassociated_dia_objects * u.count)
    return meas
