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

__all__ = ["run_ap_verify"]

import argparse
import os
import re

import lsst.log
from .dataset import Dataset
from .metrics import MetricsParser, check_squash_ready, AutoJob
from .pipeline_driver import ApPipeParser, run_ap_pipe
from .measurements import measure_from_metadata, \
                          measure_from_butler_repo, \
                          measure_from_L1_db_sqlite


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
        self.add_argument('--dataset', choices=Dataset.get_supported_datasets(), required=True,
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
        full_format = fmt
        if not full_format.startswith('^'):
            full_format = '^' + full_format
        if not full_format.endswith('$'):
            full_format += '$'
        self._format = re.compile(full_format)
        self._message = msg

    def __call__(self, value):
        if self._format.match(value):
            return value
        else:
            raise argparse.ArgumentTypeError(self._message % value)


def _get_output_dir(input_dir, output_arg, rerun_arg):
    """Choose an output directory based on program arguments.

    Parameters
    ----------
    input_dir: `str`
        The root directory of the input dataset.
    output_arg: `str`
        The directory given using the `--output` command line argument.
    rerun_arg: `str`
        The subdirectory given using the `--rerun` command line argument. Must
        be relative to `input_rerun`.

    Raises
    ------
    `ValueError`:
        Neither `output_arg` nor `rerun_arg` is None, or both are.
    """
    if output_arg and rerun_arg:
        raise ValueError('Cannot provide both --output and --rerun.')
    if not output_arg and not rerun_arg:
        raise ValueError('Must provide either --output or --rerun.')
    if output_arg:
        return output_arg
    else:
        return os.path.join(input_dir, "rerun", rerun_arg)


def _measure_final_properties(metadata, output_dir, args, metrics_job):
    """Measure any metrics that apply to the final result of the AP pipeline,
    rather than to a particular processing stage.

    Parameters
    ----------
    metadata: `lsst.daf.base.PropertySet`
        The metadata produced by the AP pipeline.
    metrics_job: `verify.Job`
        The Job object to which to add any metric measurements made.
    """
    measurements = []
    measurements.extend(measure_from_metadata(metadata))
    measurements.extend(measure_from_butler_repo(output_dir, args.dataId))
    measurements.extend(measure_from_L1_db_sqlite(db_name))

    for measurement in measurements:
        metrics_job.measurements.insert(measurement)


def run_ap_verify():
    lsst.log.configure()
    log = lsst.log.Log.getLogger('ap.verify.ap_verify.main')
    # TODO: what is LSST's policy on exceptions escaping into main()?
    args = _VerifyApParser().parse_args()
    check_squash_ready(args)
    log.debug('Command-line arguments: %s', args)

    test_data = Dataset(args.dataset)
    log.info('Dataset %s set up.', args.dataset)
    output = _get_output_dir(test_data.dataset_root, args.output, args.rerun)
    test_data.make_output_repo(output)
    log.info('Output repo at %s created.', output)

    with AutoJob(args) as job:
        log.info('Running pipeline...')
        metadata = run_ap_pipe(test_data, output, args, job)
        _measure_final_properties(metadata, output, args, job)
