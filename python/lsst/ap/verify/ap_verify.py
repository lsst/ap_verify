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

"""Command-line program for running and analyzing AP pipeline.

In addition to containing ap_verify's main function, this module manages
command-line argument parsing.
"""

__all__ = ["runApVerify"]

import argparse
import os
import re

import lsst.log
from .dataset import Dataset
from .ingestion import ingestDataset
from .metrics import MetricsParser, checkSquashReady, AutoJob
from .pipeline_driver import ApPipeParser, runApPipe
from .measurements import measureFromMetadata, \
    measureFromButlerRepo, \
    measureFromL1DbSqlite
from .workspace import Workspace


class _InputOutputParser(argparse.ArgumentParser):
    """An argument parser for program-wide input and output.

    This parser is not complete, and is designed to be passed to another parser
    using the `parent` parameter.
    """

    def __init__(self):
        # Help and documentation will be handled by main program's parser
        argparse.ArgumentParser.__init__(self, add_help=False)
        self.add_argument('--dataset', choices=Dataset.getSupportedDatasets(), required=True,
                          help='The source of data to pass through the pipeline.')

        output = self.add_mutually_exclusive_group(required=True)
        output.add_argument('--output',
                            help='The location of the workspace to use for pipeline repositories.')
        output.add_argument(
            '--rerun', metavar='OUTPUT',
            type=_FormattedType('[^:]+',
                                'Invalid name "%s"; ap_verify supports only output reruns. '
                                'You have entered something that appears to be of the form INPUT:OUTPUT. '
                                'Please specify only OUTPUT.'),
            help='The location of the workspace to use for pipeline repositories, as DATASET/rerun/OUTPUT')


class _ApVerifyParser(argparse.ArgumentParser):
    """An argument parser for data needed by this script.
    """

    def __init__(self):
        argparse.ArgumentParser.__init__(
            self,
            description='Executes the LSST DM AP pipeline and analyzes its performance using metrics.',
            epilog='',
            parents=[_InputOutputParser(), ApPipeParser(), MetricsParser()],
            add_help=True)

        self.add_argument('--version', action='version', version='%(prog)s 0.1.0')


class _FormattedType:
    """An argparse type converter that requires strings in a particular format.

    Leaves the input as a string if it matches, else raises `argparse.ArgumentTypeError`.

    Parameters
    ----------
    fmt : `str`
        A regular expression that values must satisfy to be accepted. The *entire* string must match the
        expression in order to pass.
    msg : `str`
        An error string to display for invalid values. The first "%s" shall be filled with the
        invalid argument.
    """
    def __init__(self, fmt, msg='"%s" does not have the expected format.'):
        fullFormat = fmt
        if not fullFormat.startswith('^'):
            fullFormat = '^' + fullFormat
        if not fullFormat.endswith('$'):
            fullFormat += '$'
        self._format = re.compile(fullFormat)
        self._message = msg

    def __call__(self, value):
        if self._format.match(value):
            return value
        else:
            raise argparse.ArgumentTypeError(self._message % value)


def _getOutputDir(inputDir, outputArg, rerunArg):
    """Choose an output directory based on program arguments.

    Parameters
    ----------
    inputDir : `str`
        The root directory of the input dataset.
    outputArg : `str`
        The directory given using the ``--output`` command line argument. May
        be `None`.
    rerunArg : `str`
        The subdirectory given using the ``--rerun`` command line argument.  May
        be `None`, otherwise must be relative to `inputDir`.

    Raises
    ------
    `ValueError`
        Neither `outputArg` nor `rerunArg` is `None`, or both are.
    """
    if outputArg and rerunArg:
        raise ValueError('Cannot provide both --output and --rerun.')
    if not outputArg and not rerunArg:
        raise ValueError('Must provide either --output or --rerun.')
    if outputArg:
        return outputArg
    else:
        return os.path.join(inputDir, "rerun", rerunArg)


def _measureFinalProperties(metricsJob, metadata, workspace, args):
    """Measure any metrics that apply to the final result of the AP pipeline,
    rather than to a particular processing stage.

    Parameters
    ----------
    metricsJob : `lsst.verify.Job`
        The Job object to which to add any metric measurements made.
    metadata : `lsst.daf.base.PropertySet`
        The metadata produced by the AP pipeline.
    workspace : `lsst.ap.verify.workspace.Workspace`
        The abstract location containing input and output repositories.
    args : `argparse.Namespace`
        All command-line arguments passed to this program, including those
        supported by `lsst.ap.verify.pipeline_driver.ApPipeParser`.
    """
    # TODO: remove this function's dependency on pipeline_driver (possibly after DM-11372)
    measurements = []
    measurements.extend(measureFromMetadata(metadata))
    # In the current version of ap_pipe, DIFFIM_DIR has a parent of
    # PROCESSED_DIR. This means that a butler created from the DIFFIM_DIR reop
    # includes data from PROCESSED_DIR.
    measurements.extend(measureFromButlerRepo(workspace.outputRepo, args.dataId))
    measurements.extend(measureFromL1DbSqlite(os.path.join(workspace.outputRepo, "association.db")))

    for measurement in measurements:
        metricsJob.measurements.insert(measurement)


def runApVerify(cmdLine=None):
    """Execute the AP pipeline while handling metrics.

    This is the main function for ``ap_verify``, and handles logging,
    command-line argument parsing, pipeline execution, and metrics
    generation.

    After this function returns, metrics will be available in a file
    named :file:`ap_verify.verify.json` in the working directory.

    Parameters
    ----------
    cmdLine : `list` of `str`
        an optional command line used to execute `runApVerify` from other
        Python code. If `None`, `sys.argv` will be used.
    """
    lsst.log.configure()
    log = lsst.log.Log.getLogger('ap.verify.ap_verify.main')
    # TODO: what is LSST's policy on exceptions escaping into main()?
    args = _ApVerifyParser().parse_args(args=cmdLine)
    checkSquashReady(args)
    log.debug('Command-line arguments: %s', args)

    testData = Dataset(args.dataset)
    log.info('Dataset %s set up.', args.dataset)
    workspace = Workspace(_getOutputDir(testData.datasetRoot, args.output, args.rerun))
    ingestDataset(testData, workspace)

    with AutoJob(args) as job:
        log.info('Running pipeline...')
        metadata = runApPipe(job, workspace, args)
        _measureFinalProperties(job, metadata, workspace, args)
