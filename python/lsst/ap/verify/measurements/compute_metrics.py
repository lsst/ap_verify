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

__all__ = ["measureFromButlerRepo"]

import copy

from lsst.verify.gen2tasks import MetricsControllerTask
from lsst.ap.pipe import ApPipeTask
from .association import measureTotalUnassociatedDiaObjects


def measureFromButlerRepo(metricsConfig, butler, rawDataId):
    """Create measurements from a butler repository.

    Parameters
    ----------
    metricsConfig : `str`
        A file containing a `~lsst.verify.gen2tasks.MetricsControllerConfig`.
    butler : `lsst.daf.persistence.Butler`
        A butler opened to the repository to read.
    rawDataId : `lsst.daf.persistence.DataId` or `dict`
        Butler identifier naming the data given to ap_pipe.

    Returns
    -------
    measurements : iterable of `lsst.verify.Measurement`
        all the measurements derived from ``metadata``. May be empty.
    """
    result = []

    dataId = copy.copy(rawDataId)
    # Workaround for bug where Butler tries to load HDU
    # even when template doesn't have one
    if "hdu" in dataId:
        del dataId["hdu"]

    timingConfig = MetricsControllerTask.ConfigClass()
    timingConfig.load(metricsConfig)
    _runMetricTasks(timingConfig, butler, dataId)

    config = butler.get(ApPipeTask._DefaultName + '_config')
    result.extend(measureFromPpdb(config.ppdb))

    return result


def _runMetricTasks(config, butler, dataId):
    """Run MetricControllerTask on a single dataset.

    Parameters
    ----------
    config : `lsst.verify.gen2tasks.MetricsControllerConfig`
        The config for running `~lsst.verify.gen2tasks.MetricsControllerTask`.
    butler : `lsst.daf.persistence.Butler`
        A butler opened to ap_verify's output repository.
    dataId : `lsst.daf.persistence.DataId` or `dict`
        The data ID for this run of ``ap_verify``.
    """
    allMetricTasks = MetricsControllerTask(config)

    # Don't particularly want calexps, but they tend to have compatible
    # data IDs with other processed data types
    processedDatarefs = butler.subset('calexp', dataId=dataId)
    allMetricTasks.runDataRefs(processedDatarefs)


def measureFromPpdb(configurable):
    """Make measurements on a ppdb database containing the results of
    source association.

    configurable : `lsst.pex.config.ConfigurableInstance`
        A configurable object for a `lsst.dax.ppdb.Ppdb` or similar type.
    """
    result = []
    ppdb = configurable.apply()
    measurement = measureTotalUnassociatedDiaObjects(
        ppdb, "ap_association.totalUnassociatedDiaObjects")
    if measurement is not None:
        result.append(measurement)

    return result
