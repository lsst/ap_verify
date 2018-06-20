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

"""Code for measuring software performance metrics.
"""

__all__ = ["measureNumberNewDiaObjects",
           "measureNumberUnassociatedDiaObjects",
           "measureFractionUpdatedDiaObjects",
           "measureNumberSciSources",
           "measureFractionDiaSourcesToSciSources",
           "measureTotalUnassociatedDiaObjects"]

import astropy.units as u
import lsst.verify


def measureNumberNewDiaObjects(metadata, taskName, metricName):
    """Compute the number of newly created DIAObjects from metadata.

    Parameters
    ----------
    metadata : `lsst.daf.base.PropertySet`
        The metadata to search for object count information.
    taskName : `str`
        The name of the Task, e.g., "processCcd". SubTask names must be the
        ones assigned by the parent Task and may be disambiguated using the
        parent Task name, as in "processCcd:calibrate".
        If `taskName` matches multiple runs of a subTask in different
        contexts, the information for only one run will be provided.
    metricName : `str`
        The fully qualified name of the metric being measured, e.g.,
        "association.numNewDiaObjects"

    Returns
    -------
    measurement : `lsst.verify.Measurement`
        a value of `metricName`, or `None` if the object counts for
        `taskName` are not present in `metadata`
    """
    if not metadata.exists("association.numNewDiaObjects"):
        return None

    nNew = metadata.getAsInt("association.numNewDiaObjects")
    meas = lsst.verify.Measurement(metricName, nNew * u.count)
    return meas


def measureNumberUnassociatedDiaObjects(metadata, taskName, metricName):
    """Compute the number of previously created DIAObjects that were loaded
    but did not have a new association in this visit, ccd.

    Parameters
    ----------
    metadata : `lsst.daf.base.PropertySet`
        The metadata to search for object count information.
    taskName : `str`
        The name of the Task, e.g., "processCcd". SubTask names must be the
        ones assigned by the parent Task and may be disambiguated using the
        parent Task name, as in "processCcd:calibrate".
        If `taskName` matches multiple runs of a subTask in different
        contexts, the information for only one run will be provided.
    metricName : `str`
        The fully qualified name of the metric being measured, e.g.,
        "association.numUnassociatedDiaObjects"

    Returns
    -------
    measurement : `lsst.verify.Measurement`
        a value for `metricName`, or `None` if the object counts for
        `taskName` are not present in `metadata`
    """
    if not metadata.exists("association.numUnassociatedDiaObjects"):
        return None

    nUnassociated = metadata.getAsInt(
        "association.numUnassociatedDiaObjects")
    meas = lsst.verify.Measurement(metricName, nUnassociated * u.count)
    return meas


def measureFractionUpdatedDiaObjects(metadata, taskName, metricName):
    """Compute the fraction of previously created DIAObjects that have a new
    association in this visit, ccd.

    Parameters
    ----------
    metadata : `lsst.daf.base.PropertySet`
        The metadata to search for object count information.
    taskName : `str`
        The name of the Task, e.g., "processCcd". SubTask names must be the
        ones assigned by the parent Task and may be disambiguated using the
        parent Task name, as in "processCcd:calibrate".
        If `taskName` matches multiple runs of a subTask in different
        contexts, the information for only one run will be provided.
    metricName : `str`
        The fully qualified name of the metric being measured, e.g.,
        "association.fracUpdatedDiaObjects"

    Returns
    -------
    measurement : `lsst.verify.Measurement`
        a value for `metricName`, or `None` if the object counts for
        `taskName` are not present in `metadata`
    """
    if not metadata.exists("association.numUpdatedDiaObjects") or \
       not metadata.exists("association.numUnassociatedDiaObjects"):
        return None

    nUpdated = metadata.getAsDouble("association.numUpdatedDiaObjects")
    nUnassociated = metadata.getAsDouble(
        "association.numUnassociatedDiaObjects")
    if nUpdated <= 0. or nUnassociated <= 0.:
        return lsst.verify.Measurement(metricName, 0. * u.dimensionless_unscaled)
    meas = lsst.verify.Measurement(
        metricName,
        nUpdated / (nUpdated + nUnassociated) * u.dimensionless_unscaled)
    return meas


def measureNumberSciSources(butler, dataIdDict, metricName):
    """Compute the number of cataloged science sources.

    Parameters
    ----------
    butler : `lsst.daf.persistence.Butler`
        The output repository location to read from disk.
    dataIdDict : `dict`
        Butler identifier naming the data to be processed (e.g., visit and
        ccdnum) formatted in the usual way (e.g., 'visit=54321 ccdnum=7').
    metricName : `str`
        The fully qualified name of the metric being measured, e.g.,
        "ip_diffim.numSciSources"

    Returns
    -------
    measurement: `lsst.verify.Measurement`
        a value for `metricName`, or `None`
    """

    # Parse the input dataId string and convert to a dictionary of values.
    # Hard coded assuming the same input formate as in ap_pipe.

    nSciSources = len(butler.get('src', dataId=dataIdDict))
    meas = lsst.verify.Measurement(
        metricName, nSciSources * u.count)
    return meas


def measureFractionDiaSourcesToSciSources(butler,
                                          dataIdDict,
                                          metricName):
    """Compute the ratio of cataloged science sources to different image
    sources per ccd per visit.

    Parameters
    ----------
    butler : `lsst.daf.percistence.Butler`
        The output repository location to read from disk.
    dataIdDict : `dict`
        Butler identifier naming the data to be processed (e.g., visit and
        ccdnum) formatted in the usual way (e.g., 'visit=54321 ccdnum=7').
    metricName : `str`
        The fully qualified name of the metric being measured, e.g.,
        "ip_diffim.fracDiaSourcesToSciSources"

    Returns
    -------
    measurement : `lsst.verify.Measurement`
        a value for `metricName`, or `None`
    """

    # Parse the input dataId string and convert to a dictionary of values.
    # Hard coded assuming the same input formate as in ap_pipe.

    nSciSources = len(butler.get('src', dataId=dataIdDict))
    nDiaSources = len(butler.get('deepDiff_diaSrc', dataId=dataIdDict))
    meas = lsst.verify.Measurement(
        metricName,
        nDiaSources / nSciSources * u.dimensionless_unscaled)
    return meas


def measureTotalUnassociatedDiaObjects(dbCursor, metricName):
    """ Compute number of DIAObjects with only one association DIASource.

    Parameters
    ----------
    dbCursor : `sqlite3.Cursor`
        Cursor to the sqlite data base created from a previous run of
        AssociationDBSqlite task to load.
    metricName : `str`
        The fully qualified name of the metric being measured, e.g.,
        "association.totalUnassociatedDiaObjects"

    Returns
    -------
    measurement : `lsst.verify.Measurement`
        a value for `metricName`, or `None`
    """

    dbCursor.execute("SELECT count(*) FROM dia_objects "
                     "WHERE nDiaSources = 1")
    (nUnassociatedDiaObjects,) = dbCursor.fetchall()[0]

    meas = lsst.verify.Measurement(
        metricName,
        nUnassociatedDiaObjects * u.count)
    return meas
