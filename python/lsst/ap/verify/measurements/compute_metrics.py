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

__all__ = ["measure_from_metadata"]

from lsst.ap.verify.config import Config
from .profiling import measure_runtime


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
    for metric in timing_map.names():
        measurement = measure_runtime(metadata, timing_map[metric], metric)
        if measurement is not None:
            result.append(measurement)

    return result
