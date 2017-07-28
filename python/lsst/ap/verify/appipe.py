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

from __future__ import absolute_import, division, print_function

__all__ = ["ApPipeParser", "ApPipe"]

import argparse

import lsst.log
from lsst.ap.verify.dataset import Dataset
from lsst.ap.verify.pipeline import Pipeline


class ApPipeParser(argparse.ArgumentParser):
    """An argument parser for data needed by ap_pipe activities.

    This parser is not complete, and is designed to be passed to another parser
    using the `parent` parameter.
    """

    def __init__(self):
        # Help and documentation will be handled by main program's parser
        argparse.ArgumentParser.__init__(self, add_help=False)
        self.add_argument('--dataIdString', dest='dataId', required=True,
                          help='An identifier for the data to process. '
                          'May not support all features of a Butler dataId; '
                          'see the ap_pipe documentation for details.')
        self.add_argument("-j", "--processes", default=1, type=int, help="Number of processes to use")


class ApPipe(Pipeline):
    """Wrapper for `lsst.ap.pipe` that executes all steps through source
    association.

    This class is not designed to have subclasses.

    Parameters
    ----------
    dataset: `dataset.Dataset`
        The dataset on which the pipeline will be run.
    working_dir: `str`
        The repository in which temporary products will be created. Must be
        compatible with `dataset`.
    parsed_cmd_line: `argparse.Namespace`
        Command-line arguments, including all arguments supported by `ApPipeParser`.
    """

    def __init__(self, dataset, working_dir, parsed_cmd_line):
        Pipeline.__init__(self, dataset, working_dir)
        self._dataId = parsed_cmd_line.dataId
        self._parallelization = parsed_cmd_line.processes

    def _ingest_raws(self):
        """Ingest the raw data for use by LSST.

        The original data directory shall not be modified.
        """
        # use self.dataset and self.repo
        raise NotImplementedError

    def _ingest_calibs(self):
        """Ingest the raw calibrations for use by LSST.

        The original calibration directory shall not be modified.
        """
        # use self.dataset and self.repo
        raise NotImplementedError

    def _ingest_templates(self):
        """Ingest precomputed templates for use by LSST.

        The templates may be either LSST `calexp` or LSST
        `deepCoadd_psfMatchedWarp`. The original template directory shall not
        be modified.
        """
        # use self.dataset and self.repo
        raise NotImplementedError

    def _process(self, metrics_job):
        """Run single-frame processing on a dataset.

        Parameters
        ----------
        metrics_job: `verify.Job`
            The Job object to which to add any metric measurements made.
        """
        # use self.repo, self._dataId, self._parallelization
        raise NotImplementedError

    def _difference(self, metrics_job):
        """Run image differencing on a dataset.

        Parameters
        ----------
        metrics_job: `verify.Job`
            The Job object to which to add any metric measurements made.
        """
        # use self.repo, self._dataId, self._parallelization
        raise NotImplementedError

    def _associate(self, metrics_job):
        """Run source association on a dataset.

        Parameters
        ----------
        metrics_job: `verify.Job`
            The Job object to which to add any metric measurements made.
        """
        # use self.repo, self._parallelization
        raise NotImplementedError

    def _post_process(self):
        """Run post-processing on a dataset.

        This step is called the "afterburner" in some design documents.
        """
        # use self.repo
        pass

    def run(self, metrics_job):
        """Run `ap_pipe` on this object's dataset.

        Parameters
        ----------
        metrics_job: `verify.Job`
            The Job object to which to add any metric measurements made.
        """
        log = lsst.log.Log.getLogger('ap.verify.appipe.ApPipe.run')

        self._ingest_raws()
        self._ingest_calibs()
        self._ingest_templates()
        log.info('Data ingested')

        self._process(metrics_job)
        log.info('Single-frame processing complete')
        self._difference(metrics_job)
        log.info('Image differencing complete')
        self._associate(metrics_job)
        log.info('Source association complete')
        self._post_process()
        log.info('Pipeline complete')
