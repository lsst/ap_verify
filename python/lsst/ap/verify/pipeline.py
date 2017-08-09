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

from __future__ import absolute_import, division, print_function

from abc import ABCMeta, abstractmethod
import json

from lsst.verify import Job


class MeasurementStorageError(RuntimeError):
    pass


class Pipeline(object):
    """A wrapper for an LSST pipeline.

    Subclasses of this class represent a particular data processing framework.
    If the `ap_verify` framework is ever extended or adapted to pipelines other
    than `ap_pipe`, Pipeline objects may be used to decouple the choice of
    pipeline from the rest of the framework (e.g., using a factory).

    Subclasses must implement the `run` method, which executes the complete
    pipeline.

    Parameters
    ----------
    dataset: `dataset.Dataset`
        The dataset on which the pipeline will be run.
    working_dir: `str`
        The repository in which temporary products will be created. Must be
        compatible with `dataset`.

    Attributes
    ----------
    dataset: `dataset.Dataset`
        the dataset for this pipeline; to be used for ingestion and metadata
    repo: `str`
        the location of the repository in which the pipeline will work.
        Subclasses may impose subrepositories or other structure.
    """

    __metaclass__ = ABCMeta

    def __init__(self, dataset, working_dir):
        self.dataset = dataset
        self.repo = working_dir

    @abstractmethod
    def run(self, metrics_job):
        """Execute this pipeline.

        An implementation must ingest raw data from `self.dataset` to
        `self.repo`, then carry out all required processing. It may store
        measurements and metadata in `metrics_job`, but must not export or
        otherwise post-process those measurements.

        This method is not called by `Pipeline`.

        Parameters
        ----------
        metrics_job: `verify.Job`
            The Job object to which to add any metric measurements made.

        Returns
        -------
        The metadata from any Tasks called by the pipeline. May be empty.

        Raises
        ------
        `MeasurementStorageError`
            Measurements were made, but `metrics_job` could not be updated
            with them. `metrics_job` may be changed in the event of this
            exception, so long as it is left in a valid state.
        """
        raise NotImplementedError

    @staticmethod
    def store_metrics_from_files(metadata, job):
        """Update a Job object with the measurements created from running a Task.

        The metadata shall be searched for the locations of Job dump files from
        the most recent run of a Task and its subTasks; the contents of these
        files shall be added to `job`. This method is a temporary workaround
        for the `verify` framework's limited persistence support, and will be
        removed in a future version.

        This method is intended to help implement subclasses, and should not be
        called by external code. It is not called by `Pipeline` itself.

        Parameters
        ----------
        metadata: `lsst.daf.base.PropertySet`
            The metadata from running a Task(s). Assumed to contain keys of
            the form `<standard task prefix>.verify_json_path` that maps to the
            absolute file location of that Task's serialized measurements. All
            other metadata fields are ignored.
        job: `verify.Job`
            The Job object to which to add measurements. This object shall be
            left in a consistent state if this method raises exceptions.

        Raises
        ------
        `IOError`
            Serialized measurements could not be located or read from disk.
        `TypeError`
            A `verify_json_path` key does not map to a string.
        """
        keys = metadata.names(topLevelOnly=False)
        files = [metadata.getAsString(key) for key in keys if key.endswith('verify_json_path')]

        for measurement_file in files:
            with open(measurement_file) as f:
                task_job = Job.deserialize(**json.load(f))
            job += task_job
