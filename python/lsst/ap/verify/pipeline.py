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

from abc import ABCMeta, abstractmethod


class Pipeline(object):
    """A wrapper for an LSST pipeline.

    Subclasses of this class represent a particular data processing framework.
    If the `ap_verify` framework is ever extended or adapted to pipelines other
    than `ap_pipe`, Pipeline objects may be used to decouple the choice of
    pipeline from the rest of the framework (e.g., using a factory).

    Subclasses must implement the `run` method, which executes the complete
    pipeline.

    Parameters
    ----------
    dataset: `dataset.Dataset`
        The dataset on which the pipeline will be run.
    working_dir: `str`
        The repository in which temporary products will be created. Must be
        compatible with `dataset`.

    Attributes
    ----------
    dataset: `dataset.Dataset`
        the dataset for this pipeline; to be used for ingestion and metadata
    repo: `str`
        the location of the repository in which the pipeline will work.
        Subclasses may impose subrepositories or other structure.
    """

    __metaclass__ = ABCMeta

    def __init__(self, dataset, working_dir):
        self.dataset = dataset
        self.repo = working_dir

    @abstractmethod
    def run(self, metrics_job):
        """Execute this pipeline.

        An implementation must ingest raw data from `self.dataset` to
        `self.repo`, then carry out all required processing. It may store
        measurements and metadata in `metrics_job`, but must not export or
        otherwise post-process those measurements.

        This method is not called by Pipeline, so implementation decisions in
        a subclass will not have unintended consequences.

        Parameters
        ----------
        metrics_job: `verify.Job`
            The Job object to which to add any metric measurements made.
        """
        raise NotImplementedError
