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

"""Interface between `ap_verify` and `ap_pipe`.

This module handles calling `ap_pipe` and converting any information
as needed. It also attempts to collect measurements step-by-step, so
that a total pipeline failure still allows some measurements to be
recovered.
"""

__all__ = ["ApPipeParser", "MeasurementStorageError", "runApPipe"]

import argparse
import os
import re

import json

import lsst.log
import lsst.daf.persistence as dafPersist
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
        raise MeasurementStorageError('Task metadata could not be read; possible downstream bug') from e


def _process(pipeline, workspace, dataId, parallelization):
    """Run single-frame processing on a dataset.

    Parameters
    ----------
    pipeline : `lsst.ap.pipe.ApPipeTask`
        An instance of the AP pipeline.
    workspace : `lsst.ap.verify.workspace.Workspace`
        The abstract location containing input and output repositories.
    dataId : `dict` from `str` to any
        Butler identifier naming the data to be processed by the underlying
        task(s).
    parallelization : `int`
        Parallelization level at which to run underlying task(s).
    """
    for dataRef in dafPersist.searchDataRefs(workspace.workButler, datasetType='raw', dataId=dataId):
        pipeline.runProcessCcd(dataRef)


def _difference(pipeline, workspace, dataId, parallelization):
    """Run image differencing on a dataset.

    Parameters
    ----------
    pipeline : `lsst.ap.pipe.ApPipeTask`
        An instance of the AP pipeline.
    workspace : `lsst.ap.verify.workspace.Workspace`
        The abstract location containing input and output repositories.
    dataId : `dict` from `str` to any
        Butler identifier naming the data to be processed by the underlying
        task(s).
    parallelization : `int`
        Parallelization level at which to run underlying task(s).
    """
    for dataRef in dafPersist.searchDataRefs(workspace.workButler, datasetType='calexp', dataId=dataId):
        pipeline.runDiffIm(dataRef)


def _associate(pipeline, workspace, dataId, parallelization):
    """Run source association on a dataset.

    Parameters
    ----------
    pipeline : `lsst.ap.pipe.ApPipeTask`
        An instance of the AP pipeline.
    workspace : `lsst.ap.verify.workspace.Workspace`
        The abstract location containing output repositories.
    dataId : `dict` from `str` to any
        Butler identifier naming the data to be processed by the underlying
        task(s).
    parallelization : `int`
        Parallelization level at which to run underlying task(s).
    """
    for dataRef in dafPersist.searchDataRefs(workspace.workButler, datasetType='calexp', dataId=dataId):
        pipeline.runAssociation(dataRef)


def _postProcess(workspace):
    """Run post-processing on a dataset.

    This step is called the "afterburner" in some design documents.

    Parameters
    ----------
    workspace : `lsst.ap.verify.workspace.Workspace`
        The abstract location containing output repositories.
    """
    pass


def runApPipe(metricsJob, workspace, parsedCmdLine):
    """Run `ap_pipe` on this object's dataset.

    Parameters
    ----------
    metricsJob : `lsst.verify.Job`
        The Job object to which to add any metric measurements made.
    workspace : `lsst.ap.verify.workspace.Workspace`
        The abstract location containing input and output repositories.
    parsedCmdLine : `argparse.Namespace`
        Command-line arguments, including all arguments supported by `ApPipeParser`.

    Returns
    -------
    metadata : `lsst.daf.base.PropertySet`
        The metadata from any tasks called by the pipeline. May be empty.

    Raises
    ------
    `lsst.ap.verify.pipeline_driver.MeasurementStorageError`
        Measurements were made, but `metricsJob` could not be updated
        with all of them. This exception may suppress exceptions raised by
        the pipeline itself.
    """
    log = lsst.log.Log.getLogger('ap.verify.pipeline_driver.runApPipe')

    dataId = _parseDataId(parsedCmdLine.dataId)
    processes = parsedCmdLine.processes

    pipeline = apPipe.ApPipeTask(workspace.workButler,
                                 os.path.join(workspace.outputRepo, 'association.db'),
                                 config=_getConfig(workspace))
    try:
        _process(pipeline, workspace, dataId, processes)
        log.info('Single-frame processing complete')

        _difference(pipeline, workspace, dataId, processes)
        log.info('Image differencing complete')
        _associate(pipeline, workspace, dataId, processes)
        log.info('Source association complete')

        _postProcess(workspace)
        log.info('Pipeline complete')
        return pipeline.getFullMetadata()
    finally:
        # Recover any metrics from completed pipeline steps, even if the pipeline fails
        _updateMetrics(pipeline.getFullMetadata(), metricsJob)


def _getConfig(workspace):
    """Return the config for running ApPipeTask on this workspace.

    Parameters
    ----------
    workspace : `lsst.ap.verify.workspace.Workspace`
        A Workspace whose config directory may contain an
        `~lsst.ap.pipe.ApPipeTask` config.

    Returns
    -------
    config : `lsst.ap.pipe.ApPipeConfig`
        The config for running `~lsst.ap.pipe.ApPipeTask`.
    """
    overrideFile = apPipe.ApPipeTask._DefaultName + ".py"
    # TODO: may not be needed depending on resolution of DM-13887
    mapper = dafPersist.Butler.getMapperClass(workspace.dataRepo)
    packageDir = lsst.utils.getPackageDir(mapper.getPackageName())

    config = apPipe.ApPipeTask.ConfigClass()
    for path in [
        os.path.join(packageDir, 'config'),
        os.path.join(packageDir, 'config', mapper.getCameraName()),
        workspace.configDir,
    ]:
        overridePath = os.path.join(path, overrideFile)
        if os.path.exists(overridePath):
            config.load(overridePath)
    return config


def _deStringDataId(dataId):
    '''
    Replace a dataId's values with numbers, where appropriate.

    Parameters
    ----------
    dataId: `dict` from `str` to any
        The dataId to be cleaned up.
    '''
    integer = re.compile('^\s*[+-]?\d+\s*$')
    for key, value in dataId.items():
        if isinstance(value, str) and integer.match(value) is not None:
            dataId[key] = int(value)


def _parseDataId(rawDataId):
    """Convert a dataId from a command-line string to a dict.

    Parameters
    ----------
    rawDataId : `str`
        A string in a format like "visit=54321 ccdnum=7".

    Returns
    -------
    dataId: `dict` from `str` to any type
        A dataId ready for passing to Stack operations.
    """
    dataIdItems = re.split('[ +=]', rawDataId)
    dataId = dict(zip(dataIdItems[::2], dataIdItems[1::2]))
    _deStringDataId(dataId)
    return dataId
