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

"""Interface between `ap_verify` and `ap_pipe`.

This module handles calling `ap_pipe` and converting any information
as needed. It also attempts to collect measurements step-by-step, so
that a total pipeline failure still allows some measurements to be
recovered.
"""

from __future__ import absolute_import, division, print_function

__all__ = ["ApPipeParser", "MeasurementStorageError", "runApPipe"]

import os
import argparse
from functools import wraps
from future.utils import raise_from

import json

import lsst.log
import lsst.daf.base as dafBase
import lsst.ap.pipe as apPipe
from lsst.verify import Job

_OUTPUT_REPO = 'output'


class ApPipeParser(argparse.ArgumentParser):
    """An argument parser for data needed by ``ap_pipe`` activities.

    This parser is not complete, and is designed to be passed to another parser
    using the `parent` parameter.
    """

    def __init__(self):
        # Help and documentation will be handled by main program's parser
        argparse.ArgumentParser.__init__(self, add_help=False)
        self.add_argument('--id', dest='dataId', required=True,
                          help='An identifier for the data to process. '
                          'May not support all features of a Butler dataId; '
                          'see the ap_pipe documentation for details.')
        self.add_argument("-j", "--processes", default=1, type=int,
                          help="Number of processes to use. Not yet implemented.")


class MeasurementStorageError(RuntimeError):
    pass


def _updateMetrics(metadata, job):
    """Update a Job object with the measurements created from running a task.

    The metadata shall be searched for the locations of Job dump files from
    the most recent run of a task and its subtasks; the contents of these
    files shall be added to `job`. This method is a temporary workaround
    for the `verify` framework's limited persistence support, and will be
    removed in a future version.

    Parameters
    ----------
    metadata : `lsst.daf.base.PropertySet`
        The full metadata from running a task(s). Assumed to contain keys of
        the form "<standard task prefix>.verify_json_path" that maps to the
        absolute file location of that task's serialized measurements.
        All other metadata fields are ignored.
    job : `lsst.verify.Job`
        The Job object to which to add measurements. This object shall be
        left in a consistent state if this method raises exceptions.

    Raises
    ------
    `lsst.ap.verify.pipeline_driver.MeasurementStorageError`
        A "verify_json_path" key does not map to a string, or serialized
        measurements could not be located or read from disk.
    """
    try:
        keys = metadata.names(topLevelOnly=False)
        files = [metadata.getAsString(key) for key in keys if key.endswith('verify_json_path')]

        for measurementFile in files:
            with open(measurementFile) as f:
                taskJob = Job.deserialize(**json.load(f))
            job += taskJob
    except (IOError, TypeError) as e:
        raise_from(
            MeasurementStorageError('Task metadata could not be read; possible downstream bug'),
            e)


def _MetricsRecovery(pipelineStep):
    """Carry out a pipeline step while handling metrics defensively.

    Parameters
    ----------
    pipelineStep: callable
        The pipeline step to decorate. Must return metadata from the task(s)
        executed, or `None`.

    Returns
    -------
    A callable that expects a `verify.Job` as its first parameter,
    followed by the arguments to `pipelineStep`, in order. Its
    behavior shall be to execute `pipelineStep`, update the `Job`
    object with any metrics produced by `pipelineStep`, and return
    (possibly empty) metadata.

    The returned callable shall raise `pipeline.MeasurementStorageError`
    if measurements were made, but the `Job` object could not be
    updated with them. Any side effects of `pipelineStep` shall
    remain in effect in the event of this exception.
    """
    @wraps(pipelineStep)
    def wrapper(job, *args, **kwargs):
        metadata = pipelineStep(*args, **kwargs)
        if metadata is None:
            metadata = dafBase.PropertySet()

        _updateMetrics(metadata, job)
        return metadata
    return wrapper


@_MetricsRecovery
def _process(workingRepo, dataId, parallelization):
    """Run single-frame processing on a dataset.

    Parameters
    ----------
    workingRepo : `str`
        The repository containing the input and output data.
    dataId : `str`
        Butler identifier naming the data to be processed by the underlying
        task(s).
    parallelization : `int`
        Parallelization level at which to run underlying task(s).

    Returns
    -------
    metadata : `lsst.daf.base.PropertySet`
        The full metadata from any Tasks called by this method, or `None`.
    """
    dataRepo = os.path.join(workingRepo, apPipe.ap_pipe.INGESTED_DIR)
    calibRepo = os.path.join(workingRepo, apPipe.ap_pipe.CALIBINGESTED_DIR)
    outputRepo = os.path.join(workingRepo, _OUTPUT_REPO)
    return apPipe.doProcessCcd(dataRepo, calibRepo, outputRepo, dataId, skip=False)


@_MetricsRecovery
def _difference(workingRepo, dataId, parallelization):
    """Run image differencing on a dataset.

    Parameters
    ----------
    workingRepo : `str`
        The repository containing the input and output data.
    dataId : `str`
        Butler identifier naming the data to be processed by the underlying
        task(s).
    parallelization : `int`
        Parallelization level at which to run underlying task(s).

    Returns
    -------
    metadata : `lsst.daf.base.PropertySet`
        The full metadata from any Tasks called by this method, or `None`.
    """
    templateRepo = os.path.join(workingRepo, apPipe.ap_pipe.INGESTED_DIR)
    outputRepo = os.path.join(workingRepo, _OUTPUT_REPO)
    return apPipe.doDiffIm(outputRepo, dataId, 'coadd', templateRepo, outputRepo, skip=False)


@_MetricsRecovery
def _associate(workingRepo, dataId, parallelization):
    """Run source association on a dataset.

    Parameters
    ----------
    workingRepo : `str`
        The repository containing the input and output data.
    dataId : `str`
        Butler identifier naming the data to be processed by the underlying
        task(s).
    parallelization : `int`
        Parallelization level at which to run underlying task(s).

    Returns
    -------
    metadata : `lsst.daf.base.PropertySet`
        The full metadata from any Tasks called by this method, or `None`.
    """
    outputRepo = os.path.join(workingRepo, _OUTPUT_REPO)
    return apPipe.doAssociation(outputRepo, dataId, outputRepo, skip=False)


def _postProcess(workingRepo):
    """Run post-processing on a dataset.

    This step is called the "afterburner" in some design documents.

    Parameters
    ----------
    workingRepo : `str`
        The repository containing the input and output data.
    """
    pass


def runApPipe(metricsJob, workingRepo, parsedCmdLine):
    """Run `ap_pipe` on this object's dataset.

    Parameters
    ----------
    workingRepo : `str`
        The repository in which temporary products will be created.
    parsedCmdLine : `argparse.Namespace`
        Command-line arguments, including all arguments supported by `ApPipeParser`.
    metricsJob : `lsst.verify.Job`
        The Job object to which to add any metric measurements made.

    Returns
    -------
    metadata : `lsst.daf.base.PropertySet`
        The metadata from any tasks called by the pipeline. May be empty.

    Raises
    ------
    `lsst.ap.verify.pipeline_driver.MeasurementStorageError`
        Measurements were made, but `metricsJob` could not be updated
        with all of them.
    """
    log = lsst.log.Log.getLogger('ap.verify.pipeline_driver.runApPipe')

    metadata = dafBase.PropertySet()
    _getApPipeRepos(metadata)

    dataId = parsedCmdLine.dataId
    processes = parsedCmdLine.processes
    metadata.combine(_process(metricsJob, workingRepo, dataId, processes))
    log.info('Single-frame processing complete')

    metadata.combine(_difference(metricsJob, workingRepo, dataId, processes))
    log.info('Image differencing complete')
    metadata.combine(_associate(metricsJob, workingRepo, dataId, processes))
    log.info('Source association complete')

    _postProcess(workingRepo)
    log.info('Pipeline complete')
    return metadata


def _getApPipeRepos(metadata):
    """ Retrieve the subdirectories and repos defined in ``ap_pipe`` and store
    them in the metedata.

    Parameters
    ----------
    metadata : `lsst.daf.base.PropertySet`
        A set of metadata to append to.
    """
    metadata.add("ap_pipe.INGESTED_DIR", apPipe.ap_pipe.INGESTED_DIR)
    metadata.add("ap_pipe.CALIBINGESTED_DIR", apPipe.ap_pipe.CALIBINGESTED_DIR)
    metadata.add("ap_pipe.PROCESSED_DIR", _OUTPUT_REPO)
    metadata.add("ap_pipe.DIFFIM_DIR", _OUTPUT_REPO)
    metadata.add("ap_pipe.DB_DIR", _OUTPUT_REPO)
