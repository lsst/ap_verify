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

"""Interface for running measurement code.

The rest of `ap_verify` should access `measurements` through the functions
defined here, rather than depending on individual measurement functions.
"""

from __future__ import absolute_import, division, print_function

__all__ = ["measure_from_metadata",
           "measure_from_butler_repo",
           "measure_from_L1_db_sqlite"]

import sqlite3
import re

from lsst.ap.verify.config import Config
import lsst.daf.persistence as dafPersist
from .profiling import measure_runtime
from .association import measure_number_new_dia_objects, \
                         measure_number_unassociated_dia_objects, \
                         measure_fraction_updated_dia_objects, \
                         measure_number_sci_sources, \
                         measure_fraction_dia_sources_to_sci_sources, \
                         measure_total_unassociated_dia_objects


def measure_from_metadata(metadata):
    """Attempts to compute all known metrics on Task metadata.

    Metrics and measurement information are registered in the ap_verify
    configuration file under the `measurements` label.

    Parameters
    ----------
    metadata: `lsst.daf.base.PropertySet`
        The metadata to search for measurements.

    Returns
    -------
    a list of `lsst.verify.Measurement` derived from `metadata`. May be empty.

    Raises
    ------
    `RuntimeError`:
        the config file exists, but does not contain the expected data
    """
    result = []

    timing_map = Config.instance['measurements.timing']
    for task in timing_map.names():
        measurement = measure_runtime(metadata, task, timing_map[task])
        if measurement is not None:
            result.append(measurement)

    measurement = measure_number_new_dia_objects(
        metadata, 'association', 'association.numNewDiaObjects')
    if measurement is not None:
        result.append(measurement)
    measurement = measure_fraction_updated_dia_objects(
        metadata, 'association', 'association.fracUpdatedDiaObjects')
    if measurement is not None:
        result.append(measurement)
    measurement = measure_number_unassociated_dia_objects(
        metadata, 'association', 'association.numUnassociatedDiaObjects')
    if measurement is not None:
        result.append(measurement)
    return result


def measure_from_butler_repo(repo, dataId):
    """ Create measurements from a butler repository.

    Parameters
    ----------
    repo: `str`
        The output repository location to read from disk.
    dataId: `str`
        Butler identifier naming the data to be processed (e.g., visit and
        ccdnum) formatted in the usual way (e.g., 'visit=54321 ccdnum=7').

    Returns
    -------
    a list of `lsst.verify.Measurement` derived from `metadata`. May be empty.
    """
    result = []

    dataId_dict = _convert_data_id_string(dataId)

    butler = dafPersist.Butler(repo)
    measurement = measure_number_sci_sources(
        butler, dataId_dict, "ip_diffim.numSciSources")
    if measurement is not None:
        result.append(measurement)

    measurement = measure_fraction_dia_sources_to_sci_sources(
        butler, dataId_dict, "ip_diffim.fracDiaSourcesToSciSources")
    if measurement is not None:
        result.append(measurement)
    return result


def _convert_data_id_string(dataId):
    """ Convert the input data ID string information to a dict readable by the
    butler.

    Parameters
    ----------
    dataId: `str`
        Butler identifier naming the data to be processed (e.g., visit and
        ccdnum) formatted in the usual way (e.g., 'visit=54321 ccdnum=7').

    Returns
    -------
    dict of Butler dataIds.
    """
    dataId_items = re.split('[ +=]', dataId)
    dataId_dict = dict(zip(dataId_items[::2], dataId_items[1::2]))
    # Unfortunately this currently hard codes these measurements to be
    # from one ccd/visit and requires them to be from DECam because
    # of ccdnum. Buttler.get appears to require that visit and ccdnum
    # both be ints rather than allowing them to be string type.
    if 'visit' not in dataId_dict.keys():
        raise RuntimeError('The dataId string is missing \'visit\'')
    else:
        visit = int(dataId_dict['visit'])
        dataId_dict['visit'] = visit
    if 'ccdnum' not in dataId_dict.keys():
        raise RuntimeError('The dataId string is missing \'ccdnum\'')
    else:
        ccdnum = int(dataId_dict['ccdnum'])
        dataId_dict['ccdnum'] = ccdnum

    return dataId_dict


def measure_from_L1_db_sqlite(db_name):
    """ Make measurements on a sqlite database containing the results of from
    source association.

    db_name : `str`
        Name of the sqlite data base created from a previous run of
        AssociationDBSqlite task to load.
    """
    db_connection = sqlite3.connect(db_name)
    db_cursor = db_connection.cursor()

    result = []
    measurement = measure_total_unassociated_dia_objects(
        db_cursor, "association.totalUnassociatedDiaObjects")
    if measurement is not None:
        result.append(measurement)

    db_connection.close()
    return result
