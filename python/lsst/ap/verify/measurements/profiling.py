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

All measurements assume the necessary information is present in a task's metadata.
"""

__all__ = ["measureRuntime"]

import astropy.units as u

import lsst.verify


def measureRuntime(metadata, taskName, metricName):
    """Compute a wall-clock measurement from metadata provided
    by @`lsst.pipe.base.timeMethod`.

    Parameters
    ----------
    metadata : `lsst.daf.base.PropertySet`
        The metadata to search for timing information.
    taskName : `str`
        The name of the task, e.g., "processCcd". Subtask names must be the
        ones assigned by the parent task and may be disambiguated using the
        parent task name, as in "processCcd:calibrate".
        If `taskName` matches multiple runs of a subtask in different
        contexts, the information for only one run will be provided.
    metricName : `str`
        The fully qualified name of the metric being measured, e.g.,
        "pipe_tasks.ProcessCcdTime"

    Returns
    -------
    measurement : `lsst.verify.Measurement`
        the value of `metricName`, or `None` if the timing information for
        `taskName` is not present in `metadata`
    """
    # Some tasks have only run, others only runDataRef
    # If both are present, run takes precedence
    for methodName in ("run", "runDataRef"):
        endKey = "%s.%sEndCpuTime" % (taskName, methodName)

        keys = metadata.paramNames(topLevelOnly=False)
        timedMethods = [(key.replace("EndCpuTime", "StartCpuTime"), key)
                        for key in keys if key.endswith(endKey)]
        if timedMethods:
            start, end = (metadata.getAsDouble(key) for key in timedMethods[0])
            meas = lsst.verify.Measurement(metricName, (end - start) * u.second)
            meas.notes['estimator'] = 'pipe.base.timeMethod'
            return meas

    return None
