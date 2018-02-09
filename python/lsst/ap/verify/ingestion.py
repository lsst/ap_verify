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

"""Data ingestion for ap_verify.

This module handles ingestion of a dataset into an appropriate repository, so
that pipeline code need not be aware of the dataset framework.

At present, the code is specific to DECam; it will be generalized to other
instruments in the future.
"""

from __future__ import absolute_import, division, print_function

__all__ = ["ingestDataset"]

import lsst.log
import lsst.daf.base as dafBase
import lsst.ap.pipe as apPipe


def ingestDataset(dataset, repository):
    """Ingest the contents of a dataset into a Butler repository.

    The original data directory shall not be modified.

    Parameters
    ----------
    dataset : `lsst.ap.verify.dataset.Dataset`
        The dataset to be ingested.
    repository : `str`
        The file location of a repository to which the data will be ingested.
        Shall be created if it does not exist. If `repository` does exist, it
        must be compatible with `dataset` (in particular, it must support the
        relevant ``obs`` package).

    Returns
    -------
    metadata : `lsst.daf.base.PropertySet`
        The full metadata from any Tasks called by this function.
    """
    # TODO: generalize to support arbitrary URIs (DM-11482)
    log = lsst.log.Log.getLogger('ap.verify.ingestion.ingestDataset')
    dataset.makeOutputRepo(repository)
    log.info('Output repo at %s created.', repository)
    metadata = dafBase.PropertySet()
    temp = _ingestRaws(dataset, repository)
    if temp is not None:
        metadata.combine(temp)
    temp = _ingestCalibs(dataset, repository)
    if temp is not None:
        metadata.combine(temp)
    log.info('Data ingested')
    return metadata


def _ingestRaws(dataset, workingRepo):
    """Ingest the science data for use by LSST.

    The original data directory shall not be modified.

    Parameters
    ----------
    dataset : `lsst.ap.verify.dataset.Dataset`
        The dataset on which the pipeline will be run.
    workingRepo : `str`
        The repository in which temporary products will be created. Must be
        compatible with `dataset`.

    Returns
    -------
    metadata : `lsst.daf.base.PropertySet`
        The full metadata from any Tasks called by this method, or `None`.
    """
    return apPipe.doIngest(workingRepo, dataset.rawLocation, dataset.refcatsLocation)


def _ingestCalibs(dataset, workingRepo):
    """Ingest the calibration files for use by LSST.

    The original calibration directory shall not be modified.

    Parameters
    ----------
    dataset : `lsst.ap.verify.dataset.Dataset`
        The dataset on which the pipeline will be run.
    workingRepo : `str`
        The repository in which temporary products will be created. Must be
        compatible with `dataset`.

    Returns
    -------
    metadata : `lsst.daf.base.PropertySet`
        The full metadata from any Tasks called by this method, or `None`.
    """
    return apPipe.doIngestCalibs(workingRepo, dataset.calibLocation, dataset.defectLocation)
