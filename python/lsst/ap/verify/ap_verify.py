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
import warnings

import lsst.log

from .dataset import Dataset
from .ingestion import ingestDataset, ingestDatasetGen3
from .metrics import MetricsParser, computeMetrics
from .pipeline_driver import ApPipeParser, runApPipeGen2, runApPipeGen3
from .workspace import WorkspaceGen2, WorkspaceGen3

_LOG = lsst.log.Log.getLogger(__name__)


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

        gen23 = self.add_mutually_exclusive_group()
        # Because store_true and store_false use the same dest, add explicit
        # default to avoid ambiguity.
        gen23.add_argument('--gen2', dest='useGen3', action='store_false', default=True,
                           help='Handle the ap_verify dataset using the Gen 2 framework (default).')
        gen23.add_argument('--gen3', dest='useGen3', action='store_true', default=True,
                           help='Handle the ap_verify dataset using the Gen 3 framework (default).')


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
            parents=[_InputOutputParser(), _ProcessingParser(), ApPipeParser(), MetricsParser()],
            add_help=True)

    def parse_args(self, args=None, namespace=None):
        namespace = super().parse_args(args, namespace)
        # Code duplication; too hard to implement at shared _InputOutputParser level
        if not namespace.useGen3:
            warnings.warn("The --gen2 flag is deprecated; it will be removed after release 23.",
                          category=FutureWarning)
        return namespace


class _IngestOnlyParser(argparse.ArgumentParser):
    """An argument parser for data needed by dataset ingestion.
    """

    def __init__(self):
        argparse.ArgumentParser.__init__(
            self,
            description='Ingests an ap_verify dataset into a pair of Butler repositories. '
            'The program will create repository(ies) appropriate for --gen2 or --gen3 '
            'in subdirectories of <OUTPUT>. '
            'These repositories may be used directly by ap_verify.py by '
            'passing the same --output argument, or by other programs that accept '
            'Butler repositories as input.',
            epilog='',
            parents=[_InputOutputParser(), _ProcessingParser()],
            add_help=True)

    def parse_args(self, args=None, namespace=None):
        namespace = super().parse_args(args, namespace)
        # Code duplication; too hard to implement at shared _InputOutputParser level
        if not namespace.useGen3:
            warnings.warn("The --gen2 flag is deprecated; it will be removed after release 23.",
                          category=FutureWarning)
        return namespace


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
    lsst.log.configure()
    log = _LOG.getChild('main')
    # TODO: what is LSST's policy on exceptions escaping into main()?
    args = _ApVerifyParser().parse_args(args=cmdLine)
    log.debug('Command-line arguments: %s', args)

    if args.useGen3:
        workspace = WorkspaceGen3(args.output)
        ingestDatasetGen3(args.dataset, workspace, processes=args.processes)
        log.info('Running pipeline...')
        # Gen 3 pipeline includes both AP and metrics
        return runApPipeGen3(workspace, args, processes=args.processes)
    else:
        workspace = WorkspaceGen2(args.output)
        ingestDataset(args.dataset, workspace)
        log.info('Running pipeline...')
        apPipeResults = runApPipeGen2(workspace, args, processes=args.processes)
        computeMetrics(workspace, apPipeResults.parsedCmd.id, args)
        return _getCmdLineExitStatus(apPipeResults.resultList)


def _getCmdLineExitStatus(resultList):
    """Return the exit status following the conventions of
    :ref:`running a CmdLineTask from the command line
    <command-line-task-argument-reference>`.

    Parameters
    ----------
    resultList : `list` [`Struct`] or `None`
        A list of `Struct`, as returned by `ApPipeTask.parseAndRun`. Each
        element must contain at least an ``exitStatus`` member.

    Returns
    -------
    exitStatus : `int`
        The number of failed runs in ``resultList``, up to 127, or 127 if
        ``resultList`` is `None`.
    """
    if resultList:
        # ApPipeTaskRunner does not override default results handling, exitStatus always defined
        return min(127, sum(((res.exitStatus != 0) for res in resultList)))
    else:
        return 127


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
    log = _LOG.getChild('ingest')
    # TODO: what is LSST's policy on exceptions escaping into main()?
    args = _IngestOnlyParser().parse_args(args=cmdLine)
    log.debug('Command-line arguments: %s', args)

    if args.useGen3:
        workspace = WorkspaceGen3(args.output)
        ingestDatasetGen3(args.dataset, workspace, processes=args.processes)
    else:
        workspace = WorkspaceGen2(args.output)
        ingestDataset(args.dataset, workspace)
