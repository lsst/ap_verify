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

"""Interface for running measurement code.

The rest of `ap_verify` should access `measurements` through the functions
defined here, rather than depending on individual measurement functions.
"""

__all__ = ["measureFromButlerRepo",
           "measureFromPpdb"]

import re

from lsst.ap.pipe import ApPipeTask
from lsst.ap.verify.config import Config
import lsst.daf.persistence as dafPersist
import lsst.dax.ppdb as daxPpdb
from .profiling import measureRuntime
from .association import measureNumberNewDiaObjects, \
    measureNumberUnassociatedDiaObjects, \
    measureFractionUpdatedDiaObjects, \
    measureNumberSciSources, \
    measureFractionDiaSourcesToSciSources, \
    measureTotalUnassociatedDiaObjects


def measureFromMetadata(metadata):
    """Compute all known metrics on Task metadata.

    Metrics and measurement information are registered in the ``ap_verify``
    configuration file under the ``measurements`` label.

    Parameters
    ----------
    metadata : `lsst.daf.base.PropertySet`
        The metadata to search for measurements.

    Returns
    -------
    measurements : iterable of `lsst.verify.Measurement`
        all the measurements derived from ``metadata``. May be empty.

    Raises
    ------
    RuntimeError
        the ``ap_verify`` configuration file exists, but does not contain the
        expected data under ``measurements``
    """
    result = []

    timingMap = Config.instance['measurements.timing']
    for task in timingMap.names():
        measurement = measureRuntime(metadata, task, timingMap[task])
        if measurement is not None:
            result.append(measurement)

    measurement = measureNumberNewDiaObjects(
        metadata, 'apPipe:associator', 'ap_association.numNewDiaObjects')
    if measurement is not None:
        result.append(measurement)
    measurement = measureFractionUpdatedDiaObjects(
        metadata, 'apPipe:associator', 'ap_association.fracUpdatedDiaObjects')
    if measurement is not None:
        result.append(measurement)
    measurement = measureNumberUnassociatedDiaObjects(
        metadata, 'apPipe:associator', 'ap_association.numUnassociatedDiaObjects')
    if measurement is not None:
        result.append(measurement)
    return result


def measureFromButlerRepo(repo, dataId):
    """Create measurements from a butler repository.

    Parameters
    ----------
    repo : `str`
        The output repository location to read from disk.
    dataId : `str`
        Butler identifier naming the data to be processed (e.g., visit and
        ccdnum) formatted in the usual way (e.g., 'visit=54321 ccdnum=7').

    Returns
    -------
    measurements : iterable of `lsst.verify.Measurement`
        all the measurements derived from ``metadata``. May be empty.
    """
    result = []

    dataIdDict = _convertDataIdString(dataId)

    butler = dafPersist.Butler(repo)
    measurement = measureNumberSciSources(
        butler, dataIdDict, "ip_diffim.numSciSources")
    if measurement is not None:
        result.append(measurement)

    measurement = measureFractionDiaSourcesToSciSources(
        butler, dataIdDict, "ip_diffim.fracDiaSourcesToSciSources")
    if measurement is not None:
        result.append(measurement)

    metadata = butler.get(ApPipeTask._DefaultName + '_metadata', dataId=dataIdDict)
    result.extend(measureFromMetadata(metadata))
    return result


def _convertDataIdString(dataId):
    """Convert the input data ID string information to a `dict` readable by the
    butler.

    Parameters
    ----------
    dataId : `str`
        Butler identifier naming the data to be processed (e.g., visit and
        ccdnum) formatted in the usual way (e.g., 'visit=54321 ccdnum=7').

    Returns
    -------
    dataId : `dict`
        the data units, in a format compatible with the
        `lsst.daf.persistence` API
    """
    dataIdItems = re.split('[ +=]', dataId)
    dataIdDict = dict(zip(dataIdItems[::2], dataIdItems[1::2]))
    # Unfortunately this currently hard codes these measurements to be
    # from one ccd/visit and requires them to be from DECam because
    # of ccdnum. Buttler.get appears to require that visit and ccdnum
    # both be ints rather than allowing them to be string type.
    if 'visit' not in dataIdDict.keys():
        raise RuntimeError('The dataId string is missing \'visit\'')
    else:
        visit = int(dataIdDict['visit'])
        dataIdDict['visit'] = visit
    if 'ccdnum' not in dataIdDict.keys():
        raise RuntimeError('The dataId string is missing \'ccdnum\'')
    else:
        ccdnum = int(dataIdDict['ccdnum'])
        dataIdDict['ccdnum'] = ccdnum

    return dataIdDict


def measureFromPpdb(configurable):
    """Make measurements on a ppdb database containing the results of
    source association.

    configurable : `lsst.pex.config.ConfigurableInstance`
        A configurable object for a `lsst.dax.ppdb.Ppdb` or similar type.
    """
    result = []
    ppdb = daxPpdb.Ppdb(config=configurable)
    measurement = measureTotalUnassociatedDiaObjects(
        ppdb, "ap_association.totalUnassociatedDiaObjects")
    if measurement is not None:
        result.append(measurement)

    return result
