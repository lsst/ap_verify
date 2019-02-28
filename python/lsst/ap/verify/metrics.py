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

__all__ = ["MetricsParser"]

import argparse
import warnings


class MetricsParser(argparse.ArgumentParser):
    """An argument parser for data needed by metrics activities.

    This parser is not complete, and is designed to be passed to another parser
    using the `parent` parameter.
    """

    def __init__(self):
        # Help and documentation will be handled by main program's parser
        argparse.ArgumentParser.__init__(self, add_help=False)
        self.add_argument(
            '--metrics-file', default='ap_verify.{dataId}.verify.json',
            help="The file template to which to output metrics in lsst.verify "
                 "format; {dataId} will be replaced with the job\'s data ID. "
                 "Defaults to ap_verify.{dataId}.verify.json.")
        # TODO: remove --silent in DM-18120
        self.add_argument('--silent', dest='submitMetrics', nargs=0,
                          action=DeprecatedAction,
                          deprecationReason="SQuaSH upload is no longer supported",
                          help='Do NOT submit metrics to SQuaSH.')


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
