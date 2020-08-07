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

__all__ = ["DatasetIngestConfig", "Gen3DatasetIngestConfig", "ingestDataset", "ingestDatasetGen3"]

import fnmatch
import os
import re
import shutil
import tarfile
from glob import glob
import sqlite3

import lsst.utils
import lsst.log
import lsst.pex.config as pexConfig
import lsst.pipe.base as pipeBase

import lsst.daf.butler
import lsst.obs.base
from lsst.pipe.tasks.ingest import IngestTask
from lsst.pipe.tasks.ingestCalibs import IngestCalibsTask
from lsst.pipe.tasks.ingestCuratedCalibs import IngestCuratedCalibsTask


class DatasetIngestConfig(pexConfig.Config):
    """Settings and defaults for `DatasetIngestTask`.

    The correct targets for this task's subtasks can be found in the
    documentation of the appropriate ``obs`` package.

    Because `DatasetIngestTask` is not designed to be run from the command line,
    and its arguments are completely determined by the choice of dataset,
    this config includes settings that would normally be passed as command-line
    arguments to `~lsst.pipe.tasks.ingest.IngestTask`.
    """

    dataIngester = pexConfig.ConfigurableField(
        target=IngestTask,
        doc="Task used to perform raw data ingestion.",
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

    calibIngester = pexConfig.ConfigurableField(
        target=IngestCalibsTask,
        doc="Task used to ingest flats, biases, darks, fringes, or sky.",
    )
    calibFiles = pexConfig.ListField(
        dtype=str,
        default=["*.fits", "*.fz", "*.fits.gz"],
        doc="Names of calib files (no path; wildcards allowed) to ingest from the ap_verify dataset.",
    )
    calibBadFiles = pexConfig.ListField(
        dtype=str,
        default=[],
        doc="Names of calib files (no path; wildcards allowed) to not ingest, supersedes ``calibFiles``.",
    )
    calibValidity = pexConfig.Field(
        dtype=int,
        default=9999,
        doc="Calibration validity period (days). Assumed equal for all calib types.")

    curatedCalibPaths = pexConfig.ListField(
        dtype=str,
        default=[],
        doc="Paths to the top level of each curated calib's tree (e.g., defects, crosstalk). "
            "Each path should be a directory which contains one subdirectory per sensor."
    )
    curatedCalibIngester = pexConfig.ConfigurableField(
        target=IngestCuratedCalibsTask,
        doc="Task used to ingest curated calibs.",
    )

    refcats = pexConfig.DictField(
        keytype=str,
        itemtype=str,
        default={},
        doc="Map from a refcat name to a tar.gz file containing the sharded catalog. May be empty.",
    )


class DatasetIngestTask(pipeBase.Task):
    """Task for automating ingestion of a ap_verify dataset.

    Each dataset configures this task as appropriate for the files it provides
    and the target instrument. Therefore, this task takes no input besides the
    ap_verify dataset to load and the repositories to ingest to.
    """

    ConfigClass = DatasetIngestConfig
    _DefaultName = "datasetIngest"

    def __init__(self, *args, **kwargs):
        pipeBase.Task.__init__(self, *args, **kwargs)
        self.makeSubtask("dataIngester")
        self.makeSubtask("calibIngester")
        self.makeSubtask("curatedCalibIngester")

    def run(self, dataset, workspace):
        """Ingest the contents of a dataset into a Butler repository.

        Parameters
        ----------
        dataset : `lsst.ap.verify.dataset.Dataset`
            The dataset to be ingested.
        workspace : `lsst.ap.verify.workspace.WorkspaceGen2`
            The abstract location where ingestion repositories will be created.
            If the repositories already exist, they must support the same
            ``obs`` package as this task's subtasks.
        """
        # We're assuming ingest tasks always give absolute path to butler
        dataset.makeCompatibleRepo(workspace.dataRepo, os.path.abspath(workspace.calibRepo))
        self._ingestRaws(dataset, workspace)
        self._ingestCalibs(dataset, workspace)
        self._ingestCuratedCalibs(dataset, workspace)
        self._ingestRefcats(dataset, workspace)
        self._copyConfigs(dataset, workspace)

    def _ingestRaws(self, dataset, workspace):
        """Ingest the science data for use by LSST.

        After this method returns, the data repository in ``workspace`` shall
        contain all science data from ``dataset``. Butler operations on the
        repository shall not be able to modify ``dataset``.

        Parameters
        ----------
        dataset : `lsst.ap.verify.dataset.Dataset`
            The dataset on which the pipeline will be run.
        workspace : `lsst.ap.verify.workspace.WorkspaceGen2`
            The location containing all ingestion repositories.

        Raises
        ------
        RuntimeError
            Raised if there are no files to ingest.
        """
        if os.path.exists(os.path.join(workspace.dataRepo, "registry.sqlite3")):
            self.log.info("Raw images were previously ingested, skipping...")
        else:
            self.log.info("Ingesting raw images...")
            dataFiles = _findMatchingFiles(dataset.rawLocation, self.config.dataFiles)
            if dataFiles:
                self._doIngestRaws(workspace.dataRepo, workspace.calibRepo,
                                   dataFiles, self.config.dataBadFiles)
                self.log.info("Images are now ingested in {0}".format(workspace.dataRepo))
            else:
                raise RuntimeError("No raw files found at %s." % dataset.rawLocation)

    def _doIngestRaws(self, repo, calibRepo, dataFiles, badFiles):
        """Ingest raw images into a repository.

        ``repo`` shall be populated with *links* to ``dataFiles``.

        Parameters
        ----------
        repo : `str`
            The output repository location on disk for raw images. Must exist.
        calibRepo : `str`
            The output calibration repository location on disk.
        dataFiles : `list` of `str`
            A list of filenames to ingest. May contain wildcards.
        badFiles : `list` of `str`
            A list of filenames to exclude from ingestion. Must not contain paths.
            May contain wildcards.

        Raises
        ------
        RuntimeError
            Raised if ``dataFiles`` is empty.
        """
        if not dataFiles:
            raise RuntimeError("No raw files to ingest (expected list of filenames, got %r)." % dataFiles)

        args = [repo, "--calib", calibRepo, "--mode", "link"]
        args.extend(dataFiles)
        if badFiles:
            args.append('--badFile')
            args.extend(badFiles)
        try:
            _runIngestTask(self.dataIngester, args)
        except sqlite3.IntegrityError as detail:
            raise RuntimeError("Not all raw files are unique") from detail

    def _ingestCalibs(self, dataset, workspace):
        """Ingest the calibration files for use by LSST.

        After this method returns, the calibration repository in ``workspace``
        shall contain all calibration data from ``dataset``. Butler operations
        on the repository shall not be able to modify ``dataset``.

        Parameters
        ----------
        dataset : `lsst.ap.verify.dataset.Dataset`
            The dataset on which the pipeline will be run.
        workspace : `lsst.ap.verify.workspace.WorkspaceGen2`
            The location containing all ingestion repositories.

        Raises
        ------
        RuntimeError
            Raised if there are no files to ingest.
        """
        if os.path.exists(os.path.join(workspace.calibRepo, "calibRegistry.sqlite3")):
            self.log.info("Calibration files were previously ingested, skipping...")
        else:
            self.log.info("Ingesting calibration files...")
            calibDataFiles = _findMatchingFiles(dataset.calibLocation,
                                                self.config.calibFiles, self.config.calibBadFiles)
            if calibDataFiles:
                self._doIngestCalibs(workspace.dataRepo, workspace.calibRepo, calibDataFiles)
                self.log.info("Calibrations corresponding to {0} are now ingested in {1}".format(
                    workspace.dataRepo, workspace.calibRepo))
            else:
                raise RuntimeError("No calib files found at %s." % dataset.calibLocation)

    def _doIngestCalibs(self, repo, calibRepo, calibDataFiles):
        """Ingest calibration images into a calibration repository.

        Parameters
        ----------
        repo : `str`
            The output repository location on disk for raw images. Must exist.
        calibRepo : `str`
            The output repository location on disk for calibration files. Must
            exist.
        calibDataFiles : `list` of `str`
            A list of filenames to ingest. Supported files vary by instrument
            but may include flats, biases, darks, fringes, or sky. May contain
            wildcards.

        Raises
        ------
        RuntimeError
            Raised if ``calibDataFiles`` is empty.
        """
        if not calibDataFiles:
            raise RuntimeError("No calib files to ingest (expected list of filenames, got %r)."
                               % calibDataFiles)

        # TODO: --output is workaround for DM-11668
        args = [repo, "--calib", calibRepo, "--output", os.path.join(calibRepo, "dummy"),
                "--mode", "link", "--validity", str(self.config.calibValidity)]
        args.extend(calibDataFiles)
        try:
            _runIngestTask(self.calibIngester, args)
        except sqlite3.IntegrityError as detail:
            raise RuntimeError("Not all calibration files are unique") from detail

    def _ingestCuratedCalibs(self, dataset, workspace):
        """Ingest the curated calib files for use by LSST.

        After this method returns, the calibration repository in ``workspace``
        shall contain all curated calibs mentioned in curatedCalibPaths. Butler
        operations on the repository shall not be able to modify ``dataset``.

        Parameters
        ----------
        dataset : `lsst.ap.verify.dataset.Dataset`
            The dataset on which the pipeline will be run.
        workspace : `lsst.ap.verify.workspace.WorkspaceGen2`
            The location containing all ingestion repositories.
        """
        for curated in self.config.curatedCalibPaths:
            self.log.info("Ingesting curated calibs...")
            self._doIngestCuratedCalibs(workspace.dataRepo, workspace.calibRepo, curated)
            self.log.info("Curated calibs are now ingested in {0}".format(workspace.calibRepo))

    def _doIngestCuratedCalibs(self, repo, calibRepo, curatedPath):
        """Ingest curated calib data.

        Parameters
        ----------
        repo : `str`
            The output repository location on disk for raw images. Must exist.
        calibRepo : `str`
            The output repository location on disk for calibration files. Must
            exist.
        curatedPath : `str`
            Path to the curated calibs in standard text form.  This is probably
            a path in ``obs_*_data``.
        """

        curatedargs = [repo, curatedPath, "--calib", calibRepo]
        try:
            _runIngestTask(self.curatedCalibIngester, curatedargs)
        except sqlite3.IntegrityError as detail:
            raise RuntimeError("Not all curated calib files are unique") from detail

    def _ingestRefcats(self, dataset, workspace):
        """Ingest the refcats for use by LSST.

        After this method returns, the data repository in ``workspace`` shall
        contain all reference catalogs from ``dataset``. Operations on the
        repository shall not be able to modify ``dataset``.

        Parameters
        ----------
        dataset : `lsst.ap.verify.dataset.Dataset`
            The dataset on which the pipeline will be run.
        workspace : `lsst.ap.verify.workspace.WorkspaceGen2`
            The location containing all ingestion repositories.

        Notes
        -----
        Refcats are not, at present, registered as part of the repository. They
        are not guaranteed to be visible to anything other than a
        ``refObjLoader``. See the [refcat Community thread](https://community.lsst.org/t/1523)
        for more details.
        """
        if os.path.exists(os.path.join(workspace.dataRepo, "ref_cats")):
            self.log.info("Refcats were previously ingested, skipping...")
        else:
            self.log.info("Ingesting reference catalogs...")
            self._doIngestRefcats(workspace.dataRepo, dataset.refcatsLocation)
            self.log.info("Reference catalogs are now ingested in {0}".format(workspace.dataRepo))

    def _doIngestRefcats(self, repo, refcats):
        """Place refcats inside a particular repository.

        Parameters
        ----------
        repo : `str`
            The output repository location on disk for raw images. Must exist.
        refcats : `str`
            A directory containing .tar.gz files with LSST-formatted astrometric
            or photometric reference catalog information.
        """
        for refcatName, tarball in self.config.refcats.items():
            tarball = os.path.join(refcats, tarball)
            refcatDir = os.path.join(repo, "ref_cats", refcatName)
            with tarfile.open(tarball, "r") as opened:
                opened.extractall(refcatDir)

    def _copyConfigs(self, dataset, workspace):
        """Give a workspace a copy of all configs associated with the ingested data.

        After this method returns, the config directory in ``workspace`` shall
        contain all config files from ``dataset``.

        Parameters
        ----------
        dataset : `lsst.ap.verify.dataset.Dataset`
            The dataset on which the pipeline will be run.
        workspace : `lsst.ap.verify.workspace.WorkspaceGen2`
            The location containing the config directory.
        """
        if os.listdir(workspace.configDir):
            self.log.info("Configs already copied, skipping...")
        else:
            self.log.info("Storing data-specific configs...")
            self._doCopyConfigs(workspace.configDir, dataset.configLocation)
            self.log.info("Configs are now stored in {0}".format(workspace.configDir))

    def _doCopyConfigs(self, destination, source):
        """Place configs inside a particular repository.

        Parameters
        ----------
        destination : `str`
            The directory to which the configs must be copied. Must exist.
        source : `str`
            A directory containing Task config files.
        """
        for configFile in _findMatchingFiles(source, ['*.py']):
            shutil.copy2(configFile, destination)


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

    def run(self):
        """Ingest the contents of a dataset into a Butler repository.
        """
        self._ensureRaws()
        self._defineVisits()
        self._copyConfigs()

    def _ensureRaws(self):
        """Ensure that the repository in ``workspace`` has raws ingested.

        After this method returns, this task's repository contains all science
        data from this task's ap_verify dataset. Butler operations on the
        repository are not able to modify ``dataset`` in any way.

        Raises
        ------
        RuntimeError
            Raised if there are no files to ingest.
        """
        # TODO: regex is workaround for DM-25945
        rawCollectionFilter = re.compile(self.dataset.instrument.makeDefaultRawIngestRunName())
        rawCollections = list(self.workspace.workButler.registry.queryCollections(rawCollectionFilter))
        if rawCollections:
            self.log.info("Raw images for %s were previously ingested, skipping...",
                          self.dataset.instrument.getName())
        else:
            self.log.info("Ingesting raw images...")
            dataFiles = _findMatchingFiles(self.dataset.rawLocation, self.config.dataFiles,
                                           exclude=self.config.dataBadFiles)
            if dataFiles:
                self._ingestRaws(dataFiles)
                self.log.info("Images are now ingested in {0}".format(self.workspace.repo))
            else:
                raise RuntimeError("No raw files found at %s." % self.dataset.rawLocation)

    def _ingestRaws(self, dataFiles):
        """Ingest raw images into a repository.

        This task's repository is populated with *links* to ``dataFiles``.

        Parameters
        ----------
        dataFiles : `list` of `str`
            A list of filenames to ingest. May contain wildcards.

        Raises
        ------
        RuntimeError
            Raised if ``dataFiles`` is empty or any file has already been ingested.
        """
        if not dataFiles:
            raise RuntimeError("No raw files to ingest (expected list of filenames, got %r)." % dataFiles)

        try:
            self.ingester.run(dataFiles, run=None)  # expect ingester to name a new collection
        except lsst.daf.butler.registry.ConflictingDefinitionError as detail:
            raise RuntimeError("Not all raw files are unique") from detail

    def _defineVisits(self):
        """Map visits to the ingested exposures.

        This step is necessary to be able to run most pipelines on raw datasets.

        Raises
        ------
        RuntimeError
            Raised if there are no exposures in the repository.
        """
        exposures = set(self.workspace.workButler.registry.queryDataIds(["exposure"]))
        if not exposures:
            raise RuntimeError(f"No exposures defined in {self.workspace.repo}.")

        exposureKeys = list(exposures)[0].graph
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
        contains all config files from the ap_verify dataset.
        """
        if os.listdir(self.workspace.configDir):
            self.log.info("Configs already copied, skipping...")
        else:
            self.log.info("Storing data-specific configs...")
            for configFile in _findMatchingFiles(self.dataset.configLocation, ['*.py']):
                shutil.copy2(configFile, self.workspace.configDir)
            self.log.info("Configs are now stored in {0}".format(self.workspace.configDir))


def ingestDataset(dataset, workspace):
    """Ingest the contents of an ap_veify dataset into a Butler repository.

    The original data directory shall not be modified.

    Parameters
    ----------
    dataset : `lsst.ap.verify.dataset.Dataset`
        The ap_verify dataset to be ingested.
    workspace : `lsst.ap.verify.workspace.WorkspaceGen2`
        The abstract location where ingestion repositories will be created.
        If the repositories already exist, they must be compatible with
        ``dataset`` (in particular, they must support the relevant
        ``obs`` package).
    """
    # TODO: generalize to support arbitrary URIs (DM-11482)
    log = lsst.log.Log.getLogger("ap.verify.ingestion.ingestDataset")

    ingester = DatasetIngestTask(config=_getConfig(DatasetIngestTask, dataset))
    ingester.run(dataset, workspace)
    log.info("Data ingested")


def ingestDatasetGen3(dataset, workspace):
    """Ingest the contents of an ap_verify dataset into a Gen 3 Butler repository.

    The original data directory is not modified.

    Parameters
    ----------
    dataset : `lsst.ap.verify.dataset.Dataset`
        The ap_verify dataset to be ingested.
    workspace : `lsst.ap.verify.workspace.WorkspaceGen3`
        The abstract location where the epository is be created, if it does
        not already exist.
    """
    log = lsst.log.Log.getLogger("ap.verify.ingestion.ingestDataset")

    ingester = Gen3DatasetIngestTask(dataset, workspace, config=_getConfig(Gen3DatasetIngestTask, dataset))
    ingester.run()
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
    # Can't use dataset.instrument.applyConfigOverrides for this, because the
    # dataset might not have Gen 3 support.
    overrideFile = task._DefaultName + ".py"
    packageDir = lsst.utils.getPackageDir(dataset.obsPackage)

    config = task.ConfigClass()
    for path in [
        os.path.join(packageDir, 'config'),
        os.path.join(packageDir, 'config', dataset.camera),
        dataset.configLocation,
    ]:
        overridePath = os.path.join(path, overrideFile)
        if os.path.exists(overridePath):
            config.load(overridePath)
    return config


def _runIngestTask(task, args):
    """Run an ingestion task on a set of inputs.

    Parameters
    ----------
    task : `lsst.pipe.tasks.IngestTask`
        The task to run.
    args : list of command-line arguments, split using Python conventions
        The command-line arguments for ``task``. Must be compatible with ``task.ArgumentParser``.
    """
    argumentParser = task.ArgumentParser(name=task.getName())
    try:
        parsedCmd = argumentParser.parse_args(config=task.config, args=args)
    except SystemExit as e:
        # SystemExit is not an appropriate response when the arguments aren't user-supplied
        raise ValueError("Invalid ingestion arguments: %s" % args) from e
    task.run(parsedCmd)


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
