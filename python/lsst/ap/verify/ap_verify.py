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
import os
import sys
import logging

import lsst.log

from .dataset import Dataset
from .ingestion import ingestDatasetGen3, IngestionParser
from .pipeline_driver import ApPipeParser, runApPipeGen3, _getPipelineFile
from .workspace import WorkspaceGen3

_LOG = logging.getLogger(__name__)


def _configure_logger():
    """Configure Python logging.

    Does basic Python logging configuration and
    forwards LSST logger to Python logging.
    """
    logging.basicConfig(level=logging.INFO, stream=sys.stdout)
    lsst.log.configure_pylog_MDC("DEBUG", MDC_class=None)


class _InputOutputParser(argparse.ArgumentParser):
    """An argument parser for program-wide input and output.

    This parser is not complete, and is designed to be passed to another parser
    using the `parent` parameter.
    """

    def __init__(self):
        # Help and documentation will be handled by main program's parser
        argparse.ArgumentParser.__init__(self, add_help=False)
        self.add_argument('--dataset', action=_DatasetAction,
                          required=True, help='The source of data to pass through the pipeline.')
        self.add_argument('--output', required=True,
                          help='The location of the workspace to use for pipeline repositories.')


class _ProcessingParser(argparse.ArgumentParser):
    """An argument parser for general run-time characteristics.

    This parser is not complete, and is designed to be passed to another parser
    using the `parent` parameter.
    """

    def __init__(self):
        # Help and documentation will be handled by main program's parser
        argparse.ArgumentParser.__init__(self, add_help=False)
        self.add_argument("-j", "--processes", default=1, type=int,
                          help="Number of processes to use.")


class _ApVerifyParser(argparse.ArgumentParser):
    """An argument parser for data needed by the main ap_verify program.
    """

    def __init__(self):
        argparse.ArgumentParser.__init__(
            self,
            description='Executes the LSST DM AP pipeline and analyzes its performance using metrics.',
            epilog='',
            parents=[IngestionParser(), _InputOutputParser(), _ProcessingParser(), ApPipeParser(), ],
            add_help=True)


class _IngestOnlyParser(argparse.ArgumentParser):
    """An argument parser for data needed by dataset ingestion.
    """

    def __init__(self):
        argparse.ArgumentParser.__init__(
            self,
            description='Ingests an ap_verify dataset into a repository. '
            'The program will create a repository in the ``repo`` subdirectory of <OUTPUT>. '
            'These repositories may be used directly by ap_verify.py by '
            'passing the same --output argument, or by other programs that accept '
            'Butler repositories as input.',
            epilog='',
            parents=[IngestionParser(), _InputOutputParser(), _ProcessingParser()],
            add_help=True)


class _DatasetAction(argparse.Action):
    """A converter for dataset arguments.

    Not an argparse type converter so that the ``choices`` parameter can be
    expressed using strings; ``choices`` checks happen after type conversion
    but before actions.
    """
    def __call__(self, _parser, namespace, values, _option_string=None):
        setattr(namespace, self.dest, Dataset(values))


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

    Returns
    -------
    nFailed : `int`
        The number of data IDs that were not successfully processed, up to 127,
        or 127 if the task runner framework failed.
    """
    _configure_logger()
    log = _LOG.getChild('main')
    # TODO: what is LSST's policy on exceptions escaping into main()?
    args = _ApVerifyParser().parse_args(args=cmdLine)
    log.debug('Command-line arguments: %s', args)

    workspace = WorkspaceGen3(args.output)
    # Set the pipeline name as extra parameter for SasquatchDatastore to used
    pipelineFile = _getPipelineFile(workspace, args)
    extra = dict(vars(args)['extra'])
    if 'pipeline' not in extra.keys():
        extra['pipeline'] = os.path.basename(pipelineFile)

    ingestDatasetGen3(
        args.dataset, workspace, args.namespace, args.restProxyUrl,
        extra=extra, processes=args.processes)
    log.info('Running pipeline...')
    # Gen 3 pipeline includes both AP and metrics
    return runApPipeGen3(workspace, args, processes=args.processes)


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
    _configure_logger()
    log = _LOG.getChild('ingest')
    # TODO: what is LSST's policy on exceptions escaping into main()?
    args = _IngestOnlyParser().parse_args(args=cmdLine)
    log.debug('Command-line arguments: %s', args)

    workspace = WorkspaceGen3(args.output)
    ingestDatasetGen3(args.dataset, workspace, processes=args.processes)
