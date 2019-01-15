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

__all__ = ["runApVerify", "runIngestion"]

import argparse
import re

import lsst.log
from .dataset import Dataset
from .ingestion import ingestDataset
from .metrics import MetricsParser, checkSquashReady, AutoJob
from .pipeline_driver import ApPipeParser, runApPipe
from .measurements import measureFromButlerRepo
from .workspace import Workspace


class _InputOutputParser(argparse.ArgumentParser):
    """An argument parser for program-wide input and output.

    This parser is not complete, and is designed to be passed to another parser
    using the `parent` parameter.
    """

    def __init__(self):
        # Help and documentation will be handled by main program's parser
        argparse.ArgumentParser.__init__(self, add_help=False)
        self.add_argument('--dataset', action=_DatasetAction, choices=Dataset.getSupportedDatasets(),
                          required=True, help='The source of data to pass through the pipeline.')

        self.add_argument('--output', required=True,
                          help='The location of the workspace to use for pipeline repositories.')


class _ApVerifyParser(argparse.ArgumentParser):
    """An argument parser for data needed by the main ap_verify program.
    """

    def __init__(self):
        argparse.ArgumentParser.__init__(
            self,
            description='Executes the LSST DM AP pipeline and analyzes its performance using metrics.',
            epilog='',
            parents=[_InputOutputParser(), ApPipeParser(), MetricsParser()],
            add_help=True)


class _IngestOnlyParser(argparse.ArgumentParser):
    """An argument parser for data needed by dataset ingestion.
    """

    def __init__(self):
        argparse.ArgumentParser.__init__(
            self,
            description='Ingests a dataset into a pair of Butler repositories.'
            'The program will create a data repository in <OUTPUT>/ingested and a calib repository '
            'in <OUTPUT>/calibingested. '
            'These repositories may be used directly by ap_verify.py by '
            'passing the same --output argument, or by other programs that accept '
            'Butler repositories as input.',
            epilog='',
            parents=[_InputOutputParser()],
            add_help=True)


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


class _DatasetAction(argparse.Action):
    """A converter for dataset arguments.

    Not an argparse type converter so that the ``choices`` parameter can be
    expressed using strings; ``choices`` checks happen after type conversion
    but before actions.
    """
    def __call__(self, _parser, namespace, values, _option_string=None):
        setattr(namespace, self.dest, Dataset(values))


def _measureFinalProperties(workspace, dataIds, args):
    """Measure any metrics that apply to the final result of the AP pipeline,
    rather than to a particular processing stage.

    Parameters
    ----------
    workspace : `lsst.ap.verify.workspace.Workspace`
        The abstract location containing input and output repositories.
    dataIds : `lsst.pipe.base.DataIdContainer`
        The data IDs ap_pipe was run on. Each data ID must be complete.
    args : `argparse.Namespace`
        Command-line arguments, including arguments controlling output.
    """
    for dataRef in dataIds.refList:
        with AutoJob(workspace.workButler, dataRef.dataId, args) as metricsJob:
            measurements = measureFromButlerRepo(workspace.analysisButler, dataRef.dataId)
            for measurement in measurements:
                metricsJob.measurements.insert(measurement)


def runApVerify(cmdLine=None):
    """Execute the AP pipeline while handling metrics.

    This is the main function for ``ap_verify``, and handles logging,
    command-line argument parsing, pipeline execution, and metrics
    generation.

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

    workspace = Workspace(args.output)
    ingestDataset(args.dataset, workspace)

    log.info('Running pipeline...')
    expandedDataIds = runApPipe(workspace, args)
    _measureFinalProperties(workspace, expandedDataIds, args)


def runIngestion(cmdLine=None):
    """Ingest a dataset, but do not process it.

    This is the main function for ``ingest_dataset``, and handles logging,
    command-line argument parsing, and ingestion.

    Parameters
    ----------
    cmdLine : `list` of `str`
        an optional command line used to execute `runIngestion` from other
        Python code. If `None`, `sys.argv` will be used.
    """
    lsst.log.configure()
    log = lsst.log.Log.getLogger('ap.verify.ap_verify.ingest')
    # TODO: what is LSST's policy on exceptions escaping into main()?
    args = _IngestOnlyParser().parse_args(args=cmdLine)
    log.debug('Command-line arguments: %s', args)

    workspace = Workspace(args.output)
    ingestDataset(args.dataset, workspace)
