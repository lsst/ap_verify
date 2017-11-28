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

"""Verification metrics handling for the AP pipeline.

This module handles metrics loading and export (via the `AutoJob` class), but not
processing of individual measurements. Measurements are handled in the
``ap_verify`` module or in the appropriate pipeline step, as appropriate.
"""

from __future__ import absolute_import, division, print_function

__all__ = ["AutoJob", "MetricsParser", "checkSquashReady"]

import argparse
import os

import lsst.log
import lsst.verify

# Standard environment variables for interoperating with lsst.verify.dispatch_verify.py
_ENV_USER = 'SQUASH_USER'
_ENV_PASSWORD = 'SQUASH_PASSWORD'
_ENV_URL = 'SQUASH_URL'
_SQUASH_DEFAULT_URL = 'https://squash.lsst.codes/dashboard/api'


def checkSquashReady(parsedCmdLine):
    """Test whether the program has everything it needs for the SQuaSH API.

    As a special case, this function never raises if `parsedCmdLine.submitMetrics` is unset.

    Parameters
    ----------
    parsedCmdLine : `argparse.Namespace`
        Command-line arguments, including all arguments supported by `MetricsParser`.

    Raises
    ------
    `RuntimeError`
        A configuration problem would prevent SQuaSH features from being used.
    """
    if parsedCmdLine.submitMetrics:
        for var in (_ENV_USER, _ENV_PASSWORD):
            if var not in os.environ:
                raise RuntimeError('Need to define environment variable "%s" to use SQuaSH; '
                                   'pass --silent to skip.' % var)


class MetricsParser(argparse.ArgumentParser):
    """An argument parser for data needed by metrics activities.

    This parser is not complete, and is designed to be passed to another parser
    using the `parent` parameter.
    """

    def __init__(self):
        # Help and documentation will be handled by main program's parser
        argparse.ArgumentParser.__init__(self, add_help=False)
        self.add_argument('--silent', dest='submitMetrics', action='store_false',
                          help='Do NOT submit metrics to SQuaSH (not yet implemented).')
        # Config info we don't want on the command line
        self.set_defaults(user=os.getenv(_ENV_USER), password=os.getenv(_ENV_PASSWORD),
                          squashUrl=os.getenv(_ENV_URL, _SQUASH_DEFAULT_URL))


class AutoJob:
    """A wrapper for an `lsst.verify.Job` that automatically handles
    initialization and shutdown.

    When used in a `with... as...` statement, the wrapper assigns the
    underlying job to the `as` target.

    This object shall always attempt to dump metrics to disk, but shall only
    submit to SQuaSH if the program ran without errors.

    Parameters
    ----------
    args : `argparse.Namespace`
        Command-line arguments, including all arguments supported by `MetricsParser`.
    """

    def __init__(self, args):
        self._job = lsst.verify.Job.load_metrics_package()
        # TODO: add Job metadata (camera, filter, etc.) in DM-11321
        self._submitMetrics = args.submitMetrics
        self._squashUser = args.user
        self._squashPassword = args.password
        self._squashUrl = args.squashUrl

    def _saveMeasurements(self, fileName):
        """Save a set of measurements for later use.

        Parameters
        ----------
        fileName : `str`
            The file to which the measurements will be saved.
        """
        self.job.write(fileName)

    def _sendToSquash(self):
        """Submit a set of measurements to the SQuaSH system.

        Parameters
        ----------
        fileName : `str`
            a file containing measurements in lsst.verify format
        """
        self.job.dispatch(api_user=self._squashUser, api_password=self._squashPassword,
                          api_url=self._squashUrl)

    @property
    def job(self):
        """The Job contained by this object.
        """
        return self._job

    def __enter__(self):
        """Allow the underlying Job to be used in with statements.
        """
        return self.job

    def __exit__(self, excType, excValue, traceback):
        """Package all metric measurements performed during this run.

        The measurements shall be exported to :file:`ap_verify.verify.json`,
        and the metrics framework shall be shut down. If the context was
        exited normally and the appropriate flag was passed to this object's
        constructor, the measurements shall be sent to SQuaSH.
        """
        log = lsst.log.Log.getLogger('ap.verify.metrics.AutoJob.__exit__')

        outFile = 'ap_verify.verify.json'
        try:
            self._saveMeasurements(outFile)
            log.debug('Wrote measurements to %s', outFile)
        except IOError:
            if excType is None:
                raise
            else:
                return False  # don't suppress `excValue`

        if excType is None and self._submitMetrics:
            self._sendToSquash()
            log.info('Submitted measurements to SQuaSH')
        return False
