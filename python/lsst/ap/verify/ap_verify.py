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

"""Command-line program for running and analyzing AP pipeline.

In addition to containing ap_verify's main function, this module manages
command-line argument parsing.
"""

from __future__ import absolute_import, division, print_function

__all__ = ["runApVerify"]

import argparse
import os
import re

import lsst.log
from .dataset import Dataset
from .metrics import MetricsParser, checkSquashReady, AutoJob
from .pipeline_driver import ApPipeParser, runApPipe
from .measurements import measureFromMetadata, \
    measureFromButlerRepo, \
    measureFromL1DbSqlite


class _VerifyApParser(argparse.ArgumentParser):
    """An argument parser for data needed by this script.
    """

    def __init__(self):
        argparse.ArgumentParser.__init__(
            self,
            description='Executes the LSST DM AP pipeline and analyzes its performance using metrics.',
            epilog='',
            parents=[ApPipeParser(), MetricsParser()],
            add_help=True)
        self.add_argument('--dataset', choices=Dataset.getSupportedDatasets(), required=True,
                          help='The source of data to pass through the pipeline.')

        output = self.add_mutually_exclusive_group(required=True)
        output.add_argument('--output', help='The location of the repository to use for program output.')
        output.add_argument(
            '--rerun', metavar='OUTPUT',
            type=_FormattedType('[^:]+',
                                'Invalid name "%s"; ap_verify supports only output reruns. '
                                'You have entered something that appears to be of the form INPUT:OUTPUT. '
                                'Please specify only OUTPUT.'),
            help='The location of the repository to use for program output, as DATASET/rerun/OUTPUT')

        self.add_argument('--version', action='version', version='%(prog)s 0.1.0')


class _FormattedType:
    """An argparse type converter that requires strings in a particular format.

    Leaves the input as a string if it matches, else raises ArgumentTypeError.

    Parameters
    ----------
    fmt: `str`
        A regular expression that values must satisfy to be accepted. The *entire* string must match the
        expression in order to pass.
    msg: `str`
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
    inputDir: `str`
        The root directory of the input dataset.
    outputArg: `str`
        The directory given using the `--output` command line argument. May
        be None.
    rerunArg: `str`
        The subdirectory given using the `--rerun` command line argument.  May
        be None, otherwise must be relative to `inputDir`.

    Raises
    ------
    `ValueError`:
        Neither `outputArg` nor `rerunArg` is None, or both are.
    """
    if outputArg and rerunArg:
        raise ValueError('Cannot provide both --output and --rerun.')
    if not outputArg and not rerunArg:
        raise ValueError('Must provide either --output or --rerun.')
    if outputArg:
        return outputArg
    else:
        return os.path.join(inputDir, "rerun", rerunArg)


def _measureFinalProperties(metadata, outputDir, args, metricsJob):
    """Measure any metrics that apply to the final result of the AP pipeline,
    rather than to a particular processing stage.

    Parameters
    ----------
    metadata: `lsst.daf.base.PropertySet`
        The metadata produced by the AP pipeline.
    metricsJob: `verify.Job`
        The Job object to which to add any metric measurements made.
    """
    measurements = []
    measurements.extend(measureFromMetadata(metadata))
    # In the current version of ap_pipe, DIFFIM_DIR has a parent of
    # PROCESSED_DIR. This means that a butler created from the DIFFIM_DIR reop
    # includes data from PROCESSED_DIR.
    measurements.extend(measureFromButlerRepo(
        os.path.join(outputDir, metadata.getAsString('ap_pipe.DIFFIM_DIR')), args.dataId))
    measurements.extend(measureFromL1DbSqlite(
        os.path.join(outputDir, metadata.getAsString('ap_pipe.DB_DIR'), "association.db")))

    for measurement in measurements:
        metricsJob.measurements.insert(measurement)


def runApVerify(cmdLine=None):
    """Execute the AP pipeline while handling metrics.

    Parameters
    ----------
    cmdLine: `list` of `str`
        an optional command line used to execute `runApVerify` from other
        Python code. If `None`, `sys.argv` will be used.
    """
    lsst.log.configure()
    log = lsst.log.Log.getLogger('ap.verify.ap_verify.main')
    # TODO: what is LSST's policy on exceptions escaping into main()?
    args = _VerifyApParser().parse_args(args=cmdLine)
    checkSquashReady(args)
    log.debug('Command-line arguments: %s', args)

    testData = Dataset(args.dataset)
    log.info('Dataset %s set up.', args.dataset)
    output = _getOutputDir(testData.datasetRoot, args.output, args.rerun)
    testData.makeOutputRepo(output)
    log.info('Output repo at %s created.', output)

    with AutoJob(args) as job:
        log.info('Running pipeline...')
        metadata = runApPipe(testData, output, args, job)
        _measureFinalProperties(metadata, output, args, job)
