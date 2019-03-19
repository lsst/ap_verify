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

"""Verification metrics handling for the AP pipeline.

This module handles metrics loading and export (via the `AutoJob` class), but not
processing of individual measurements. Measurements are handled in the
``ap_verify`` module or in the appropriate pipeline step, as appropriate.
"""

__all__ = ["MetricsParser", "computeMetrics"]

import argparse
import collections
import copy
import os
import warnings

import lsst.utils
from lsst.verify.gen2tasks import MetricsControllerTask


class MetricsParser(argparse.ArgumentParser):
    """An argument parser for data needed by metrics activities.

    This parser is not complete, and is designed to be passed to another parser
    using the `parent` parameter.
    """

    def __init__(self):
        # Help and documentation will be handled by main program's parser
        argparse.ArgumentParser.__init__(self, add_help=False)
        self.add_argument(
            '--metrics-file', default='{output}/ap_verify.{dataId}.verify.json',
            help="The file template to which to output metrics in lsst.verify "
                 "format. {output} will be replaced with the value of the "
                 "--output argument, while {dataId} will be replaced with the "
                 "job\'s data ID. Defaults to {output}/ap_verify.{dataId}.verify.json.")
        # TODO: remove --silent in DM-18120
        self.add_argument('--silent', dest='submitMetrics', nargs=0,
                          action=DeprecatedAction,
                          deprecationReason="SQuaSH upload is no longer supported",
                          help='Do NOT submit metrics to SQuaSH.')
        self.add_argument('--dataset-metrics-config',
                          help='The config file specifying the dataset-level metrics to measure. '
                               'Defaults to config/default_dataset_metrics.py.')
        self.add_argument('--image-metrics-config',
                          help='The config file specifying the image-level metrics to measure. '
                               'Defaults to config/default_image_metrics.py.')


class DeprecatedAction(argparse.Action):
    """An `argparse.Action` that stores nothing and issues a `FutureWarning`.

    Parameters
    ----------
    args
        Positional arguments to `argparse.Action`.
    deprecationReason : `str`
        A mandatory keyword argument to `argparse.ArgumentParser.add_argument`
        that describes why the argument was deprecated. The explanation will be
        printed if the argument is used.
    kwargs
        Keyword arguments to `argparse.Action`.
    """
    def __init__(self, *args, deprecationReason, **kwargs):
        super().__init__(*args, **kwargs)
        self.reason = deprecationReason

    def __call__(self, _parser, _namespace, _values, option_string=None):
        message = "%s has been deprecated, because %s. It will be removed in a future version." \
            % (option_string, self.reason)
        warnings.warn(message, category=FutureWarning)


class _OptionalFormatDict(collections.UserDict):
    """A dictionary that, when used with a formatter, preserves unknown
    replacement fields.

    This lets clients perform partial string substitution without `str.format`
    or `str.format_map` failing over missing keywords.
    """
    def __missing__(self, key):
        """Re-create the replacement field if there is no replacement.
        """
        return "{%s}" % key


def computeMetrics(workspace, dataIds, args):
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
    # Substitute all fields that won't be filled in by MetricsControllerTask
    # _OptionalFormatDict makes format_map preserve unspecified fields for later replacement
    metricsFile = args.metrics_file.format_map(
        _OptionalFormatDict(output=workspace.workDir))

    imageConfig = _getMetricsConfig(args.image_metrics_config,
                                    "default_image_metrics.py",
                                    metricsFile)
    _runMetricTasks(imageConfig, dataIds.refList)

    datasetConfig = _getMetricsConfig(args.dataset_metrics_config,
                                      "default_dataset_metrics.py",
                                      metricsFile)
    _runMetricTasks(datasetConfig, [workspace.workButler.dataRef("apPipe_config")])


def _getMetricsConfig(userFile, defaultFile, metricsOutputTemplate=None):
    """Load a metrics config based on program settings.

    Parameters
    ----------
    userFile : `str` or `None`
        The path provided by the user for this config file.
    defaultFile : `str`
        The filename (not a path) of the default config file.
    metricsOutputTemplate : `str` or `None`
        The files to which to write metrics. If not `None`, this argument
        overrides any output files set by either config file.

    Returns
    -------
    config : `lsst.verify.gen2tasks.MetricsControllerConfig`
        The config from ``userFile`` if the user provided one, otherwise the
        default config.
    """
    timingConfig = MetricsControllerTask.ConfigClass()

    if userFile is not None:
        timingConfig.load(userFile)
    else:
        timingConfig.load(os.path.join(lsst.utils.getPackageDir("ap_verify"), "config", defaultFile))
    if metricsOutputTemplate:
        timingConfig.jobFileTemplate = metricsOutputTemplate
    return timingConfig


def _runMetricTasks(config, dataRefs):
    """Run MetricControllerTask on a single dataset.

    Parameters
    ----------
    config : `lsst.verify.gen2tasks.MetricsControllerConfig`
        The config for running `~lsst.verify.gen2tasks.MetricsControllerTask`.
    dataRefs : `list` [`lsst.daf.persistence.ButlerDataRef`]
        The data references over which to compute metrics. The granularity
        determines the metric granularity; see
        `MetricsControllerTask.runDataRef` for more details.
    """
    allMetricTasks = MetricsControllerTask(config)
    allMetricTasks.runDataRefs([_sanitizeRef(ref) for ref in dataRefs])


def _sanitizeRef(dataRef):
    """Remove data ID tags that can cause problems when loading arbitrary
    dataset types.

    Parameters
    ----------
    dataRef : `lsst.daf.persistence.ButlerDataRef`
        The dataref to sanitize.

    Returns
    -------
    clean : `lsst.daf.persistence.ButlerDataRef`
        A dataref that is safe to use.
    """
    newDataRef = copy.deepcopy(dataRef)
    if "hdu" in newDataRef.dataId:
        del newDataRef.dataId["hdu"]
    return newDataRef
