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

# TODO: module deprecated by lsst.verify.gen2tasks.MetricsControllerTask, remove after DM-16536
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
    RuntimeError
        Raised if a configuration problem would prevent SQuaSH features from being used.
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
        self.add_argument(
            '--metrics-file', default='ap_verify.{dataId}.verify.json',
            help="The file template to which to output metrics in lsst.verify "
                 "format; {dataId} will be replaced with the job\'s data ID. "
                 "Defaults to ap_verify.{dataId}.verify.json.")
        self.add_argument('--silent', dest='submitMetrics', action='store_false',
                          help='Do NOT submit metrics to SQuaSH (not yet implemented).')
        # Config info we don't want on the command line
        self.set_defaults(user=os.getenv(_ENV_USER), password=os.getenv(_ENV_PASSWORD),
                          squashUrl=os.getenv(_ENV_URL, _SQUASH_DEFAULT_URL))


# borrowed from validate_drp
def _extract_instrument_from_butler(butler):
    """Extract the last part of the mapper name from a Butler repo.
    'lsst.obs.lsstSim.lsstSimMapper.LsstSimMapper' -> 'LSSTSIM'
    'lsst.obs.cfht.megacamMapper.MegacamMapper' -> 'CFHT'
    'lsst.obs.decam.decamMapper.DecamMapper' -> 'DECAM'
    'lsst.obs.hsc.hscMapper.HscMapper' -> 'HSC'
    """
    camera = butler.get('camera')
    instrument = camera.getName()
    return instrument.upper()


class AutoJob:
    """A wrapper for an `lsst.verify.Job` that automatically handles
    initialization and shutdown.

    When used in a `with... as...` statement, the wrapper assigns the
    underlying job to the `as` target.

    This object shall always attempt to dump metrics to disk, but shall only
    submit to SQuaSH if the program ran without errors.

    Parameters
    ----------
    butler : `lsst.daf.persistence.Butler`
        The repository associated with this ``Job``.
    dataId : `lsst.daf.persistence.DataId` or `dict`
        The data ID associated with this job. Must be complete, and represent
        the finest granularity of any measurement that may be stored in
        this job.
    args : `argparse.Namespace`
        Command-line arguments, including all arguments supported by `MetricsParser`.
    """

    def __init__(self, butler, dataId, args):
        self._job = lsst.verify.Job.load_metrics_package()

        #  Insert job metadata including dataId
        self._job.meta.update({'instrument': _extract_instrument_from_butler(butler)})
        self._job.meta.update(dataId)

        # Construct an OS-friendly string (i.e., no quotes, {}, or spaces)
        idString = "_".join("%s%s" % (key, dataId[key]) for key in dataId)
        self._outputFile = args.metrics_file.format(dataId=idString)

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

        try:
            self._saveMeasurements(self._outputFile)
            log.debug('Wrote measurements to %s', self._outputFile)
        except IOError:
            if excType is None:
                raise
            else:
                return False  # don't suppress `excValue`

        if excType is None and self._submitMetrics:
            self._sendToSquash()
            log.info('Submitted measurements to SQuaSH')
        return False
