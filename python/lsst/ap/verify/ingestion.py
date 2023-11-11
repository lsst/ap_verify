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

"""Data ingestion for ap_verify.

This module handles ingestion of an ap_verify dataset into an appropriate repository, so
that pipeline code need not be aware of the dataset framework.
"""

__all__ = ["Gen3DatasetIngestConfig", "ingestDatasetGen3"]

import fnmatch
import os
import re
import shutil
from glob import glob
import logging

import lsst.utils
import lsst.pex.config as pexConfig
import lsst.pipe.base as pipeBase

import lsst.daf.butler
import lsst.obs.base

_LOG = logging.getLogger(__name__)


class Gen3DatasetIngestConfig(pexConfig.Config):
    """Settings and defaults for `Gen3DatasetIngestTask`.

    The correct target for `ingester` can be found in the documentation of
    the appropriate ``obs`` package.
    """

    ingester = pexConfig.ConfigurableField(
        target=lsst.obs.base.RawIngestTask,
        doc="Task used to perform raw data ingestion.",
    )
    visitDefiner = pexConfig.ConfigurableField(
        target=lsst.obs.base.DefineVisitsTask,
        doc="Task used to organize raw exposures into visits.",
    )
    # Normally file patterns should be user input, but put them in a config so
    # the ap_verify dataset can configure them
    dataFiles = pexConfig.ListField(
        dtype=str,
        default=["*.fits", "*.fz", "*.fits.gz"],
        doc="Names of raw science files (no path; wildcards allowed) to ingest from the ap_verify dataset.",
    )
    dataBadFiles = pexConfig.ListField(
        dtype=str,
        default=[],
        doc="Names of raw science files (no path; wildcards allowed) to not ingest, "
            "supersedes ``dataFiles``.",
    )


class Gen3DatasetIngestTask(pipeBase.Task):
    """Task for automating ingestion of a ap_verify dataset.

    Each dataset configures this task as appropriate for the files it provides
    and the target instrument. Therefore, this task takes no input besides the
    ap_verify dataset to load and the repositories to ingest to.

    Parameters
    ----------
    dataset : `lsst.ap.verify.dataset.Dataset`
        The ``ap_verify`` dataset to be ingested.
    workspace : `lsst.ap.verify.workspace.WorkspaceGen3`
        The abstract location for all ``ap_verify`` outputs, including
        a Gen 3 repository.
    """

    ConfigClass = Gen3DatasetIngestConfig
    # Suffix is de-facto convention for distinguishing Gen 2 and Gen 3 config overrides
    _DefaultName = "datasetIngest-gen3"

    def __init__(self, dataset, workspace, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.workspace = workspace
        self.dataset = dataset
        # workspace.workButler is undefined until the repository is created
        self.dataset.makeCompatibleRepoGen3(self.workspace.repo)
        self.makeSubtask("ingester", butler=self.workspace.workButler)
        self.makeSubtask("visitDefiner", butler=self.workspace.workButler)

    def _reduce_kwargs(self):
        # Add extra parameters to pickle
        return dict(**super()._reduce_kwargs(), dataset=self.dataset, workspace=self.workspace)

    def run(self, processes=1):
        """Ingest the contents of a dataset into a Butler repository.

        Parameters
        ----------
        processes : `int`
            The number processes to use to ingest.
        """
        self._ensureRaws(processes=processes)
        self._defineVisits(processes=processes)
        self._copyConfigs()

    def _ensureRaws(self, processes):
        """Ensure that the repository in ``workspace`` has raws ingested.

        After this method returns, this task's repository contains all science
        data from this task's ap_verify dataset. Butler operations on the
        repository are not able to modify ``dataset`` in any way.

        Parameters
        ----------
        processes : `int`
            The number processes to use to ingest, if ingestion must be run.

        Raises
        ------
        RuntimeError
            Raised if there are no files to ingest.
        """
        # TODO: regex is workaround for DM-25945
        rawCollectionFilter = re.compile(self.dataset.instrument.makeDefaultRawIngestRunName())
        rawCollections = list(self.workspace.workButler.registry.queryCollections(rawCollectionFilter))
        rawData = list(self.workspace.workButler.registry.queryDatasets(
            'raw',
            collections=rawCollections,
            dataId={"instrument": self.dataset.instrument.getName()})) \
            if rawCollections else []

        if rawData:
            self.log.info("Raw images for %s were previously ingested, skipping...",
                          self.dataset.instrument.getName())
        else:
            self.log.info("Ingesting raw images...")
            dataFiles = _findMatchingFiles(self.dataset.rawLocation, self.config.dataFiles,
                                           exclude=self.config.dataBadFiles)
            if dataFiles:
                self._ingestRaws(dataFiles, processes=processes)
                self.log.info("Images are now ingested in {0}".format(self.workspace.repo))
            else:
                raise RuntimeError("No raw files found at %s." % self.dataset.rawLocation)

    def _ingestRaws(self, dataFiles, processes):
        """Ingest raw images into a repository.

        This task's repository is populated with *links* to ``dataFiles``.

        Parameters
        ----------
        dataFiles : `list` of `str`
            A list of filenames to ingest. May contain wildcards.
        processes : `int`
            The number processes to use to ingest.

        Raises
        ------
        RuntimeError
            Raised if ``dataFiles`` is empty or any file has already been ingested.
        """
        if not dataFiles:
            raise RuntimeError("No raw files to ingest (expected list of filenames, got %r)." % dataFiles)

        try:
            # run=None because expect ingester to name a new collection.
            # HACK: update_exposure_records=True to modernize exposure records
            # from old ap_verify datasets. Since the exposure records are
            # generated from the same files, the only changes should be
            # schema-related.
            self.ingester.run(dataFiles, run=None, processes=processes, update_exposure_records=True)
        except lsst.daf.butler.registry.ConflictingDefinitionError as detail:
            raise RuntimeError("Not all raw files are unique") from detail

    def _defineVisits(self, processes):
        """Map visits to the ingested exposures.

        This step is necessary to be able to run most pipelines on raw datasets.

        Parameters
        ----------
        processes : `int`
            The number processes to use to define visits.

        Raises
        ------
        RuntimeError
            Raised if there are no exposures in the repository.
        """
        exposures = set(self.workspace.workButler.registry.queryDataIds(["exposure"]))
        if not exposures:
            raise RuntimeError(f"No exposures defined in {self.workspace.repo}.")

        exposureKeys = list(exposures)[0].dimensions
        exposuresWithVisits = {x.subset(exposureKeys) for x in
                               self.workspace.workButler.registry.queryDataIds(["exposure", "visit"])}
        exposuresNoVisits = exposures - exposuresWithVisits
        if exposuresNoVisits:
            self.log.info("Defining visits...")
            self.visitDefiner.run(exposuresNoVisits)
        else:
            self.log.info("Visits were previously defined, skipping...")

    def _copyConfigs(self):
        """Give a workspace a copy of all configs associated with the
        ingested data.

        After this method returns, the config directory in the workspace
        contains all config files from the ap_verify dataset, and the
        pipelines directory in the workspace contains all pipeline files
        from the dataset.
        """
        if os.listdir(self.workspace.pipelineDir):
            self.log.info("Configs already copied, skipping...")
        else:
            self.log.info("Storing data-specific configs...")
            for configFile in _findMatchingFiles(self.dataset.configLocation, ['*.py']):
                shutil.copy2(configFile, self.workspace.configDir)
            self.log.info("Configs are now stored in %s.", self.workspace.configDir)
            for pipelineFile in _findMatchingFiles(self.dataset.pipelineLocation, ['*.yaml']):
                shutil.copy2(pipelineFile, self.workspace.pipelineDir)
            self.log.info("Configs are now stored in %s.", self.workspace.pipelineDir)


def ingestDatasetGen3(dataset, workspace, processes=1):
    """Ingest the contents of an ap_verify dataset into a Gen 3 Butler repository.

    The original data directory is not modified.

    Parameters
    ----------
    dataset : `lsst.ap.verify.dataset.Dataset`
        The ap_verify dataset to be ingested.
    workspace : `lsst.ap.verify.workspace.WorkspaceGen3`
        The abstract location where the epository is be created, if it does
        not already exist.
    processes : `int`
        The number processes to use to ingest.
    """
    log = _LOG.getChild("ingestDataset")

    ingester = Gen3DatasetIngestTask(dataset, workspace, config=_getConfig(Gen3DatasetIngestTask, dataset))
    ingester.run(processes=processes)
    log.info("Data ingested")


def _getConfig(task, dataset):
    """Return the ingestion config associated with a specific dataset.

    Parameters
    ----------
    task : `lsst.pipe.base.Task`-type
        The task whose config is needed
    dataset : `lsst.ap.verify.dataset.Dataset`
        The dataset whose ingestion config is desired.

    Returns
    -------
    config : ``task.ConfigClass``
        The config for running ``task`` on ``dataset``.
    """
    config = task.ConfigClass()
    dataset.instrument.applyConfigOverrides(task._DefaultName, config)
    return config


def _findMatchingFiles(basePath, include, exclude=None):
    """Recursively identify files matching one set of patterns and not matching another.

    Parameters
    ----------
    basePath : `str`
        The path on disk where the files in ``include`` are located.
    include : iterable of `str`
        A collection of files (with wildcards) to include. Must not
        contain paths.
    exclude : iterable of `str`, optional
        A collection of filenames (with wildcards) to exclude. Must not
        contain paths. If omitted, all files matching ``include`` are returned.

    Returns
    -------
    files : `set` of `str`
        The files in ``basePath`` or any subdirectory that match ``include``
        but not ``exclude``.
    """
    _exclude = exclude if exclude is not None else []

    allFiles = set()
    for pattern in include:
        allFiles.update(glob(os.path.join(basePath, '**', pattern), recursive=True))

    for pattern in _exclude:
        excludedFiles = [f for f in allFiles if fnmatch.fnmatch(os.path.basename(f), pattern)]
        allFiles.difference_update(excludedFiles)
    return allFiles
