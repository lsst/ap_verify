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

__all__ = ["measureRuntime", "TimingMetricConfig", "TimingMetricTask"]

import astropy.units as u

import lsst.pex.config as pexConfig
from lsst.pipe.base import Struct, InputDatasetField
from lsst.verify import Measurement, Name, MetricComputationError
from lsst.verify.compatibility import MetricTask


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
            meas = Measurement(metricName, (end - start) * u.second)
            meas.notes['estimator'] = 'pipe.base.timeMethod'
            return meas

    return None


class TimingMetricConfig(MetricTask.ConfigClass):
    """Information that distinguishes one timing metric from another.
    """
    metadata = InputDatasetField(
        doc="The timed top-level task's metadata. The name must be set to the "
            "metadata's butler type, such as 'processCcd_metadata'.",
        storageClass="PropertySet",
        dimensions=["Instrument", "Exposure", "Detector"],
    )
    target = pexConfig.Field(
        dtype=str,
        doc="The method to time, optionally prefixed by one or more tasks "
            "in the format of `lsst.pipe.base.Task.getFullMetadata()`. "
            "The times of all matching methods/tasks are added together.")
    metric = pexConfig.Field(
        dtype=str,
        doc="The fully qualified name of the metric to store the timing information.")


class TimingMetricTask(MetricTask):
    """A Task that measures a timing metric using metadata produced by the
    `lsst.pipe.base.timeMethod` decorator.

    Parameters
    ----------
    args
    kwargs
        Constructor parameters are the same as for
        `lsst.verify.compatibility.MetricTask`.
    """

    ConfigClass = TimingMetricConfig
    _DefaultName = "timingMetric"

    @classmethod
    def _getInputMetadataKeyRoot(cls, config):
        """Get a search string for the metadata.

        The string contains the name of the target method, optionally
        prefixed by one or more tasks in the format of
        `lsst.pipe.base.Task.getFullMetadata()`.

        Parameters
        ----------
        config : ``cls.ConfigClass``
            Configuration for this task.

        Returns
        -------
        keyRoot : `str`
            A string identifying the class(es) and method(s) for this task.
        """
        return config.target

    @staticmethod
    def _searchMetadataKeys(metadata, keyFragment):
        """Search the metadata for all keys matching a substring.

        Parameters
        ----------
        metadata : `lsst.daf.base.PropertySet`
            A metadata object with task-qualified keys as returned by
            `lsst.pipe.base.Task.getFullMetadata()`.
        keyFragment : `str`
            A substring for a full metadata key.

        Returns
        -------
        keys : `set` of `str`
            All keys in ``metadata`` that have ``keyFragment`` as a substring.
        """
        keys = metadata.paramNames(topLevelOnly=False)
        return {key for key in keys if keyFragment in key}

    def run(self, metadata):
        """Compute a wall-clock measurement from metadata provided by
        `lsst.pipe.base.timeMethod`.

        Parameters
        ----------
        metadata : iterable of `lsst.daf.base.PropertySet`
            A collection of metadata objects, one for each unit of science
            processing to be incorporated into this metric. Its elements
            may be `None` to represent missing data.

        Returns
        -------
        result : `lsst.pipe.base.Struct`
            A `~lsst.pipe.base.Struct` containing the following component:

            - ``measurement``: the total running time of the target method
              across all elements of ``metadata`` (`lsst.verify.Measurement`
              or `None`)

        Raises
        ------
        MetricComputationError
            Raised if any of the timing metadata are invalid.

        Notes
        -----
        This method does not return a measurement if any element of
        ``metadata`` is ``None``. The reason for this policy is that if a
        science processing run was aborted without writing metadata, then any
        timing measurement cannot be compared to other results anyway. This
        method also does not return a measurement if no timing information was
        provided by any of the metadata.
        """
        keyBase = self._getInputMetadataKeyRoot(self.config)
        endBase = keyBase + "EndCpuTime"

        timingFound = False  # some timings are indistinguishable from 0, so don't test totalTime > 0
        totalTime = 0.0
        for singleMetadata in metadata:
            if singleMetadata is not None:
                matchingKeys = TimingMetricTask._searchMetadataKeys(singleMetadata, endBase)
                for endKey in matchingKeys:
                    startKey = endKey.replace("EndCpuTime", "StartCpuTime")
                    try:
                        start, end = (singleMetadata.getAsDouble(key) for key in (startKey, endKey))
                    except (LookupError, TypeError) as e:
                        raise MetricComputationError("Invalid metadata") from e
                    totalTime += end - start
                    timingFound = True
            else:
                self.log.warn("At least one task run did not write metadata; aborting.")
                return Struct(measurement=None)

        if timingFound:
            meas = Measurement(self.getOutputMetricName(self.config), totalTime * u.second)
            meas.notes['estimator'] = 'pipe.base.timeMethod'
        else:
            self.log.info("Nothing to do: no timing information for %s found.", keyBase)
            meas = None
        return Struct(measurement=meas)

    # TODO: remove this once MetricTask has a generic implementation
    @classmethod
    def getInputDatasetTypes(cls, config):
        """Return input dataset types for this task.

        Parameters
        ----------
        config : ``cls.ConfigClass``
            Configuration for this task.

        Returns
        -------
        metadata : `dict` from `str` to `str`
            Dictionary with one key, ``"metadata"``, mapping to the dataset
            type of the target task's metadata.s
        """
        return {'metadata': config.metadata.name}

    @classmethod
    def getOutputMetricName(cls, config):
        return Name(config.metric)
