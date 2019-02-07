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

__all__ = ["TimingMetricConfig", "TimingMetricTask"]

import astropy.units as u

import lsst.pex.config as pexConfig
from lsst.pipe.base import Struct, InputDatasetField
from lsst.verify import Measurement, Name, MetricComputationError
from lsst.verify.gen2tasks import registerMultiple, MetricTask


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


@registerMultiple("timing")
class TimingMetricTask(MetricTask):
    """A Task that measures a timing metric using metadata produced by the
    `lsst.pipe.base.timeMethod` decorator.

    Parameters
    ----------
    args
    kwargs
        Constructor parameters are the same as for
        `lsst.verify.gen2tasks.MetricTask`.
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

    @classmethod
    def getOutputMetricName(cls, config):
        return Name(config.metric)
