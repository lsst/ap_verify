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

__all__ = ["ApPipeParser", "run_ap_pipe"]

import argparse
from future.utils import raise_from

import json

import lsst.log
import lsst.daf.base as dafBase
import lsst.ap.pipe as ap_pipe
from lsst.verify import Job


class ApPipeParser(argparse.ArgumentParser):
    """An argument parser for data needed by ap_pipe activities.

    This parser is not complete, and is designed to be passed to another parser
    using the `parent` parameter.
    """

    def __init__(self):
        # Help and documentation will be handled by main program's parser
        argparse.ArgumentParser.__init__(self, add_help=False)
        self.add_argument('--dataIdString', dest='dataId', required=True,
                          help='An identifier for the data to process. '
                          'May not support all features of a Butler dataId; '
                          'see the ap_pipe documentation for details.')
        self.add_argument("-j", "--processes", default=1, type=int,
                          help="Number of processes to use. Not yet implemented.")


class MeasurementStorageError(RuntimeError):
    pass


def _update_metrics(metadata, job):
    """Update a Job object with the measurements created from running a Task.

    The metadata shall be searched for the locations of Job dump files from
    the most recent run of a Task and its subTasks; the contents of these
    files shall be added to `job`. This method is a temporary workaround
    for the `verify` framework's limited persistence support, and will be
    removed in a future version.

    Parameters
    ----------
    metadata: `lsst.daf.base.PropertySet`
        The metadata from running a Task(s). No action taken if None.
        Assumed to contain keys of the form
        `<standard task prefix>.verify_json_path` that maps to the
        absolute file location of that Task's serialized measurements.
        All other metadata fields are ignored.
    job: `verify.Job`
        The Job object to which to add measurements. This object shall be
        left in a consistent state if this method raises exceptions.

    Raises
    ------
    `MeasurementStorageError`
        A `verify_json_path` key does not map to a string, or serialized
        measurements could not be located or read from disk.
    """
    if metadata is None:
        return
    try:
        keys = metadata.names(topLevelOnly=False)
        files = [metadata.getAsString(key) for key in keys if key.endswith('verify_json_path')]

        for measurement_file in files:
            with open(measurement_file) as f:
                task_job = Job.deserialize(**json.load(f))
            job += task_job
    except (IOError, TypeError) as e:
        raise_from(
            MeasurementStorageError('Task metadata could not be read; possible downstream bug'),
            e)


def _ingest_raws(dataset, working_repo, metrics_job):
    """Ingest the raw data for use by LSST.

    The original data directory shall not be modified.

    Parameters
    ----------
    dataset: `dataset.Dataset`
        The dataset on which the pipeline will be run.
    working_repo: `str`
        The repository in which temporary products will be created. Must be
        compatible with `dataset`.
    metrics_job: `verify.Job`
        The Job object to which to add any metric measurements made.

    Returns
    -------
    The metadata from any Tasks called by this method. May be empty.

    Raises
    ------
    `pipeline.MeasurementStorageError`
        Measurements were made, but `metrics_job` could not be updated
        with them.
    """
    metadata = ap_pipe.doIngest(working_repo, dataset.data_location, dataset.refcats_location)
    _update_metrics(metadata, metrics_job)
    return metadata


def _ingest_calibs(dataset, working_repo, metrics_job):
    """Ingest the raw calibrations for use by LSST.

    The original calibration directory shall not be modified.

    Parameters
    ----------
    dataset: `dataset.Dataset`
        The dataset on which the pipeline will be run.
    working_repo: `str`
        The repository in which temporary products will be created. Must be
        compatible with `dataset`.
    metrics_job: `verify.Job`
        The Job object to which to add any metric measurements made.

    Returns
    -------
    The metadata from any Tasks called by this method. May be empty.

    Raises
    ------
    `pipeline.MeasurementStorageError`
        Measurements were made, but `metrics_job` could not be updated
        with them.
    """
    metadata = ap_pipe.doIngestCalibs(working_repo, dataset.calib_location, dataset.defect_location)
    _update_metrics(metadata, metrics_job)
    return metadata


def _process(working_repo, dataId, parallelization, metrics_job):
    """Run single-frame processing on a dataset.

    Parameters
    ----------
    working_repo: `str`
        The repository containing the input and output data.
    dataId: `str`
        Butler identifier naming the data to be processed by the underlying
        Task(s).
    parallelization: `int`
        Parallelization level at which to run underlying Task(s).
    metrics_job: `verify.Job`
        The Job object to which to add any metric measurements made.

    Returns
    -------
    The metadata from any Tasks called by this method. May be empty.

    Raises
    ------
    `pipeline.MeasurementStorageError`
        Measurements were made, but `metrics_job` could not be updated
        with them.
    """
    metadata = ap_pipe.doProcessCcd(working_repo, dataId)
    _update_metrics(metadata, metrics_job)
    return metadata


def _difference(dataset, working_repo, dataId, parallelization, metrics_job):
    """Run image differencing on a dataset.

    Parameters
    ----------
    dataset: `dataset.Dataset`
        The dataset on which the pipeline will be run.
    working_repo: `str`
        The repository containing the input and output data.
    dataId: `str`
        Butler identifier naming the data to be processed by the underlying
        Task(s).
    parallelization: `int`
        Parallelization level at which to run underlying Task(s).
    metrics_job: `verify.Job`
        The Job object to which to add any metric measurements made.

    Returns
    -------
    The metadata from any Tasks called by this method. May be empty.

    Raises
    ------
    `pipeline.MeasurementStorageError`
        Measurements were made, but `metrics_job` could not be updated
        with them.
    """
    metadata = ap_pipe.doDiffIm(working_repo, dataset.template_location, dataId)
    _update_metrics(metadata, metrics_job)
    return metadata


def _associate(working_repo, dataId, parallelization, metrics_job):
    """Run source association on a dataset.

    Parameters
    ----------
    working_repo: `str`
        The repository containing the input and output data.
    dataId: `str`
        Butler identifier naming the data to be processed by the underlying
        Task(s).
    parallelization: `int`
        Parallelization level at which to run underlying Task(s).
    metrics_job: `verify.Job`
        The Job object to which to add any metric measurements made.

    Returns
    -------
    The metadata from any Tasks called by this method. May be empty.

    Raises
    ------
    `pipeline.MeasurementStorageError`
        Measurements were made, but `metrics_job` could not be updated
        with them.
    """
    metadata = ap_pipe.doAssociation(working_repo, dataId)
    _update_metrics(metadata, metrics_job)
    return metadata


def _post_process(working_repo):
    """Run post-processing on a dataset.

    This step is called the "afterburner" in some design documents.

    Parameters
    ----------
    working_repo: `str`
        The repository containing the input and output data.
    """
    pass


def run_ap_pipe(dataset, working_repo, parsed_cmd_line, metrics_job):
    """Run `ap_pipe` on this object's dataset.

    Parameters
    ----------
    dataset: `dataset.Dataset`
        The dataset on which the pipeline will be run.
    working_repo: `str`
        The repository in which temporary products will be created. Must be
        compatible with `dataset`.
    parsed_cmd_line: `argparse.Namespace`
        Command-line arguments, including all arguments supported by `ApPipeParser`.
    metrics_job: `verify.Job`
        The Job object to which to add any metric measurements made.

    Returns
    -------
    The metadata from any Tasks called by the pipeline. May be empty.

    Raises
    ------
    `MeasurementStorageError`
        Measurements were made, but `metrics_job` could not be updated
        with all of them.
    """
    log = lsst.log.Log.getLogger('ap.verify.pipeline_driver.run_ap_pipe')

    # Easiest way to defend against None return values
    metadata = dafBase.PropertySet()
    metadata.combine(_ingest_raws(dataset, working_repo, metrics_job))
    metadata.combine(_ingest_calibs(dataset, working_repo, metrics_job))
    _get_ap_pipe_repos(metadata)
    log.info('Data ingested')

    dataId = parsed_cmd_line.dataId
    processes = parsed_cmd_line.processes
    metadata.combine(_process(working_repo, dataId, processes, metrics_job))
    log.info('Single-frame processing complete')

    metadata.combine(_difference(dataset, working_repo, dataId, processes, metrics_job))
    log.info('Image differencing complete')
    metadata.combine(_associate(working_repo, dataId, processes, metrics_job))
    log.info('Source association complete')

    _post_process(working_repo)
    log.info('Pipeline complete')
    return metadata


def _get_ap_pipe_repos(metadata):
    """ Retrieve the subdirectories and reops defined in ap_pipe and store them
    in the metedata.

    Parameters
    ----------
    metadata : `lsst.daf.base.PropertySet`
        A set of metadata to append to.
    """
    metadata.add("ap_pipe.RAW_DIR", ap_pipe.ap_pipe.RAW_DIR)
    metadata.add("ap_pipe.MASTERCAL_DIR", ap_pipe.ap_pipe.MASTERCAL_DIR)
    metadata.add("ap_pipe.DEFECT_DIR", ap_pipe.ap_pipe.DEFECT_DIR)
    metadata.add("ap_pipe.REFCATS_DIR", ap_pipe.ap_pipe.REFCATS_DIR)
    metadata.add("ap_pipe.TEMPLATES_DIR", ap_pipe.ap_pipe.TEMPLATES_DIR)

    metadata.add("ap_pipe.INGESTED_DIR", ap_pipe.ap_pipe.INGESTED_DIR)
    metadata.add("ap_pipe.CALIBINGESTED_DIR", ap_pipe.ap_pipe.CALIBINGESTED_DIR)
    metadata.add("ap_pipe.PROCESSED_DIR", ap_pipe.ap_pipe.PROCESSED_DIR)
    metadata.add("ap_pipe.DIFFIM_DIR", ap_pipe.ap_pipe.DIFFIM_DIR)
    metadata.add("ap_pipe.DB_DIR", ap_pipe.ap_pipe.DB_DIR)
