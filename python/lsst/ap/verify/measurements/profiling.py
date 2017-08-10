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

__all__ = ["measure_runtime"]

import astropy.units as u

import lsst.verify


def measure_runtime(metadata, task_name, metric_name):
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
    end_key = "%s.runEndCpuTime" % task_name

    keys = metadata.names(topLevelOnly=False)
    timed_methods = [(key.replace("EndCpuTime", "StartCpuTime"), key)
                     for key in keys if key.endswith(end_key)]
    if timed_methods:
        start, end = (metadata.getAsDouble(key) for key in timed_methods[0])
        meas = lsst.verify.Measurement(metric_name, (end - start) * u.second)
        meas.notes['estimator'] = 'pipe.base.timeMethod'
        return meas
    else:
        return None
