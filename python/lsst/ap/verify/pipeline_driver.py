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

__all__ = ["ApPipeParser", "MeasurementStorageError", "runApPipe"]

import argparse
from future.utils import raise_from

import json

import lsst.log
import lsst.daf.base as dafBase
import lsst.ap.pipe as apPipe
from lsst.verify import Job


class ApPipeParser(argparse.ArgumentParser):
    """An argument parser for data needed by ``ap_pipe`` activities.

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
        The metadata from running a task(s). No action taken if `None`.
        Assumed to contain keys of the form
        "<standard task prefix>.verify_json_path" that maps to the
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
    if metadata is None:
        return
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


def _ingestRaws(dataset, workingRepo, metricsJob):
    """Ingest the raw data for use by LSST.

    The original data directory shall not be modified.

    Parameters
    ----------
    dataset : `lsst.ap.verify.dataset.Dataset`
        The dataset on which the pipeline will be run.
    workingRepo : `str`
        The repository in which temporary products will be created. Must be
        compatible with `dataset`.
    metricsJob : `lsst.verify.Job`
        The Job object to which to add any metric measurements made.

    Returns
    -------
    metadata : `lsst.daf.base.PropertySet`
        The metadata from any tasks called by this method. May be empty.

    Raises
    ------
    `lsst.ap.verify.pipeline_driver.MeasurementStorageError`
        Measurements were made, but `metricsJob` could not be updated
        with them.
    """
    metadata = apPipe.doIngest(workingRepo, dataset.rawLocation, dataset.refcatsLocation)
    _updateMetrics(metadata, metricsJob)
    return metadata


def _ingestCalibs(dataset, workingRepo, metricsJob):
    """Ingest the raw calibrations for use by LSST.

    The original calibration directory shall not be modified.

    Parameters
    ----------
    dataset : `lsst.ap.verify.dataset.Dataset`
        The dataset on which the pipeline will be run.
    workingRepo : `str`
        The repository in which temporary products will be created. Must be
        compatible with `dataset`.
    metricsJob : `lsst.verify.Job`
        The Job object to which to add any metric measurements made.

    Returns
    -------
    metadata : `lsst.daf.base.PropertySet`
        The metadata from any tasks called by this method. May be empty.

    Raises
    ------
    `lsst.ap.verify.pipeline_driver.MeasurementStorageError`
        Measurements were made, but `metricsJob` could not be updated
        with them.
    """
    metadata = apPipe.doIngestCalibs(workingRepo, dataset.calibLocation, dataset.defectLocation)
    _updateMetrics(metadata, metricsJob)
    return metadata


def _process(workingRepo, dataId, parallelization, metricsJob):
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
    metricsJob : `lsst.verify.Job`
        The Job object to which to add any metric measurements made.

    Returns
    -------
    metadata : `lsst.daf.base.PropertySet`
        The metadata from any tasks called by this method. May be empty.

    Raises
    ------
    `lsst.ap.verify.pipeline_driver.MeasurementStorageError`
        Measurements were made, but `metricsJob` could not be updated
        with them.
    """
    metadata = apPipe.doProcessCcd(workingRepo, dataId)
    _updateMetrics(metadata, metricsJob)
    return metadata


def _difference(dataset, workingRepo, dataId, parallelization, metricsJob):
    """Run image differencing on a dataset.

    Parameters
    ----------
    dataset : `lsst.ap.verify.dataset.Dataset`
        The dataset on which the pipeline will be run.
    workingRepo : `str`
        The repository containing the input and output data.
    dataId : `str`
        Butler identifier naming the data to be processed by the underlying
        task(s).
    parallelization : `int`
        Parallelization level at which to run underlying task(s).
    metricsJob : `lsst.verify.Job`
        The Job object to which to add any metric measurements made.

    Returns
    -------
    metadata : `lsst.daf.base.PropertySet`
        The metadata from any tasks called by this method. May be empty.

    Raises
    ------
    `lsst.ap.verify.pipeline_driver.MeasurementStorageError`
        Measurements were made, but `metricsJob` could not be updated
        with them.
    """
    metadata = apPipe.doDiffIm(workingRepo, dataset.templateLocation, dataId)
    _updateMetrics(metadata, metricsJob)
    return metadata


def _associate(workingRepo, dataId, parallelization, metricsJob):
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
    metricsJob : `lsst.verify.Job`
        The Job object to which to add any metric measurements made.

    Returns
    -------
    metadata : `lsst.daf.base.PropertySet`
        The metadata from any tasks called by this method. May be empty.

    Raises
    ------
    `lsst.ap.verify.pipeline_driver.MeasurementStorageError`
        Measurements were made, but `metricsJob` could not be updated
        with them.
    """
    metadata = apPipe.doAssociation(workingRepo, dataId)
    _updateMetrics(metadata, metricsJob)
    return metadata


def _postProcess(workingRepo):
    """Run post-processing on a dataset.

    This step is called the "afterburner" in some design documents.

    Parameters
    ----------
    workingRepo : `str`
        The repository containing the input and output data.
    """
    pass


def runApPipe(dataset, workingRepo, parsedCmdLine, metricsJob):
    """Run `ap_pipe` on this object's dataset.

    Parameters
    ----------
    dataset : `lsst.ap.verify.dataset.Dataset`
        The dataset on which the pipeline will be run.
    workingRepo : `str`
        The repository in which temporary products will be created. Must be
        compatible with `dataset`.
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

    # Easiest way to defend against None return values
    metadata = dafBase.PropertySet()
    metadata.combine(_ingestRaws(dataset, workingRepo, metricsJob))
    metadata.combine(_ingestCalibs(dataset, workingRepo, metricsJob))
    _getApPipeRepos(metadata)
    log.info('Data ingested')

    dataId = parsedCmdLine.dataId
    processes = parsedCmdLine.processes
    metadata.combine(_process(workingRepo, dataId, processes, metricsJob))
    log.info('Single-frame processing complete')

    metadata.combine(_difference(dataset, workingRepo, dataId, processes, metricsJob))
    log.info('Image differencing complete')
    metadata.combine(_associate(workingRepo, dataId, processes, metricsJob))
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
    metadata.add("ap_pipe.RAW_DIR", apPipe.ap_pipe.RAW_DIR)
    metadata.add("ap_pipe.MASTERCAL_DIR", apPipe.ap_pipe.MASTERCAL_DIR)
    metadata.add("ap_pipe.DEFECT_DIR", apPipe.ap_pipe.DEFECT_DIR)
    metadata.add("ap_pipe.REFCATS_DIR", apPipe.ap_pipe.REFCATS_DIR)
    metadata.add("ap_pipe.TEMPLATES_DIR", apPipe.ap_pipe.TEMPLATES_DIR)

    metadata.add("ap_pipe.INGESTED_DIR", apPipe.ap_pipe.INGESTED_DIR)
    metadata.add("ap_pipe.CALIBINGESTED_DIR", apPipe.ap_pipe.CALIBINGESTED_DIR)
    metadata.add("ap_pipe.PROCESSED_DIR", apPipe.ap_pipe.PROCESSED_DIR)
    metadata.add("ap_pipe.DIFFIM_DIR", apPipe.ap_pipe.DIFFIM_DIR)
    metadata.add("ap_pipe.DB_DIR", apPipe.ap_pipe.DB_DIR)
