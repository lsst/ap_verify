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

This module handles ingestion of a dataset into an appropriate repository, so
that pipeline code need not be aware of the dataset framework.
"""

__all__ = ["DatasetIngestConfig", "ingestDataset"]

import fnmatch
import os
import pathlib
import tarfile
from glob import glob
import sqlite3

import lsst.utils
import lsst.log
import lsst.pex.config as pexConfig
import lsst.pipe.base as pipeBase

from lsst.pipe.tasks.ingest import IngestTask
from lsst.pipe.tasks.ingestCalibs import IngestCalibsTask


class DatasetIngestConfig(pexConfig.Config):
    """Settings and defaults for `DatasetIngestTask`.

    The correct targets for this task's subtasks can be found in the
    documentation of the appropriate ``obs`` package.

    Because `DatasetIngestTask` is not designed to be run from the command line,
    and its arguments are completely determined by the choice of dataset,
    this config includes settings that would normally be passed as command-line
    arguments to `IngestTask`.
    """

    dataIngester = pexConfig.ConfigurableField(
        target=IngestTask,
        doc="Task used to perform raw data ingestion.",
    )
    dataFiles = pexConfig.ListField(
        dtype=str,
        default=["*.fits", "*.fz"],
        doc="Names of raw science files (no path; wildcards allowed) to ingest from the dataset.",
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
        default=["*.fits", "*.fz"],
        doc="Names of calib files (no path; wildcards allowed) to ingest from the dataset.",
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

    defectIngester = pexConfig.ConfigurableField(
        target=IngestCalibsTask,
        doc="Task used to ingest defects.",
    )
    defectTarball = pexConfig.Field(
        dtype=str,
        default=None,
        doc="Name of tar.gz file containing defects. May be empty. Defect files may be in any format and "
            "directory layout supported by the obs package.",
    )
    defectValidity = pexConfig.Field(
        dtype=int,
        default=9999,
        doc="Defect validity period (days).")

    refcats = pexConfig.DictField(
        keytype=str,
        itemtype=str,
        default={},
        doc="Map from a refcat name to a tar.gz file containing the sharded catalog. May be empty.",
    )


class DatasetIngestTask(pipeBase.Task):
    """Task for automating ingestion of a dataset.

    Each dataset configures this task as appropriate for the files it provides
    and the target instrument. Therefore, this task takes no input besides the
    dataset to load and the repositories to ingest to.
    """

    ConfigClass = DatasetIngestConfig
    _DefaultName = "datasetIngest"

    def __init__(self, *args, **kwargs):
        pipeBase.Task.__init__(self, *args, **kwargs)
        self.makeSubtask("dataIngester")
        self.makeSubtask("calibIngester")
        self.makeSubtask("defectIngester")

    def run(self, dataset, workspace):
        """Ingest the contents of a dataset into a Butler repository.

        Parameters
        ----------
        dataset : `lsst.ap.verify.dataset.Dataset`
            The dataset to be ingested.
        workspace : `lsst.ap.verify.workspace.Workspace`
            The abstract location where ingestion repositories will be created.
            If the repositories already exist, they must support the same
            ``obs`` package as this task's subtasks.
        """
        self._makeRepos(dataset, workspace)
        self._ingestRaws(dataset, workspace)
        self._ingestCalibs(dataset, workspace)
        self._ingestDefects(dataset, workspace)
        self._ingestRefcats(dataset, workspace)

    def _makeRepos(self, dataset, workspace):
        """Create empty repositories to ingest into.

        After this method returns, all repositories mentioned by ``workspace``
        except ``workspace.outputRepo`` shall be valid repositories compatible
        with ``dataset``. They may be empty.

        Parameters
        ----------
        dataset : `lsst.ap.verify.dataset.Dataset`
            The dataset to be ingested.
        workspace : `lsst.ap.verify.workspace.Workspace`
            The abstract location where ingestion repositories will be created.
            If the repositories already exist, they must support the same
            ``obs`` package as this task's subtasks.
        """
        dataset.makeCompatibleRepo(workspace.dataRepo)
        pathlib.Path(workspace.calibRepo).mkdir(parents=True, exist_ok=True)
        pathlib.Path(workspace.templateRepo).mkdir(parents=True, exist_ok=True)

    def _ingestRaws(self, dataset, workspace):
        """Ingest the science data for use by LSST.

        After this method returns, the data repository in ``workspace`` shall
        contain all science data from ``dataset``. Butler operations on the
        repository shall not be able to modify ``dataset``.

        Parameters
        ----------
        dataset : `lsst.ap.verify.dataset.Dataset`
            The dataset on which the pipeline will be run.
        workspace : `lsst.ap.verify.workspace.Workspace`
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
                self._doIngestRaws(workspace.dataRepo, dataFiles, self.config.dataBadFiles)
                self.log.info("Images are now ingested in {0}".format(workspace.dataRepo))
            else:
                raise RuntimeError("No raw files found at %s." % dataset.rawLocation)

    def _doIngestRaws(self, repo, dataFiles, badFiles):
        """Ingest raw images into a repository.

        ``repo`` shall be populated with *links* to ``dataFiles``.

        Parameters
        ----------
        repo : `str`
            The output repository location on disk for raw images. Must exist.
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

        args = [repo, "--mode", "link"]
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
        workspace : `lsst.ap.verify.workspace.Workspace`
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

    def _ingestDefects(self, dataset, workspace):
        """Ingest the defect files for use by LSST.

        After this method returns, the calibration repository in ``workspace``
        shall contain all defects from ``dataset``. Butler operations on the
        repository shall not be able to modify ``dataset``.

        Parameters
        ----------
        dataset : `lsst.ap.verify.dataset.Dataset`
            The dataset on which the pipeline will be run.
        workspace : `lsst.ap.verify.workspace.Workspace`
            The location containing all ingestion repositories.

        Raises
        ------
        RuntimeError
            Raised if defect ingestion requested but no defects found.
        """
        if os.path.exists(os.path.join(workspace.calibRepo, "defects")):
            self.log.info("Defects were previously ingested, skipping...")
        else:
            if self.config.defectTarball:
                self.log.info("Ingesting defects...")
                defectFile = os.path.join(dataset.defectLocation, self.config.defectTarball)
                self._doIngestDefects(workspace.dataRepo, workspace.calibRepo, defectFile)
                self.log.info("Defects are now ingested in {0}".format(workspace.calibRepo))
            else:
                self.log.info("No defects to ingest, skipping...")

    def _doIngestDefects(self, repo, calibRepo, defectTarball):
        """Ingest defect images.

        Parameters
        ----------
        repo : `str`
            The output repository location on disk for raw images. Must exist.
        calibRepo : `str`
            The output repository location on disk for calibration files. Must
            exist.
        defectTarball : `str`
            The name of a .tar.gz file that contains all the compressed
            defect images.

        Raises
        ------
        RuntimeError
            Raised if ``defectTarball`` exists but is empty.
        """
        # TODO: clean up implementation after DM-5467 resolved
        defectDir = os.path.join(calibRepo, "defects")
        with tarfile.open(defectTarball, "r") as opened:
            if opened.getnames():
                pathlib.Path(defectDir).mkdir(parents=True, exist_ok=True)
                opened.extractall(defectDir)
            else:
                raise RuntimeError("Defect archive %s is empty." % defectTarball)
        defectFiles = _findMatchingFiles(defectDir, ["*.*"])

        # TODO: --output is workaround for DM-11668
        defectargs = [repo, "--calib", calibRepo, "--output", os.path.join(calibRepo, "dummy"),
                      "--calibType", "defect",
                      "--mode", "skip", "--validity", str(self.config.defectValidity)]
        defectargs.extend(defectFiles)
        try:
            _runIngestTask(self.defectIngester, defectargs)
        except sqlite3.IntegrityError as detail:
            raise RuntimeError("Not all defect files are unique") from detail

    def _ingestRefcats(self, dataset, workspace):
        """Ingest the refcats for use by LSST.

        After this method returns, the data repository in ``workspace`` shall
        contain all reference catalogs from ``dataset``. Operations on the
        repository shall not be able to modify ``dataset``.

        Parameters
        ----------
        dataset : `lsst.ap.verify.dataset.Dataset`
            The dataset on which the pipeline will be run.
        workspace : `lsst.ap.verify.workspace.Workspace`
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


def ingestDataset(dataset, workspace):
    """Ingest the contents of a dataset into a Butler repository.

    The original data directory shall not be modified.

    Parameters
    ----------
    dataset : `lsst.ap.verify.dataset.Dataset`
        The dataset to be ingested.
    workspace : `lsst.ap.verify.workspace.Workspace`
        The abstract location where ingestion repositories will be created.
        If the repositories already exist, they must be compatible with
        ``dataset`` (in particular, they must support the relevant
        ``obs`` package).

    Returns
    -------
    metadata : `lsst.daf.base.PropertySet`
        The full metadata from any Tasks called by this function.
    """
    # TODO: generalize to support arbitrary URIs (DM-11482)
    log = lsst.log.Log.getLogger("ap.verify.ingestion.ingestDataset")

    ingester = DatasetIngestTask(config=_getConfig(dataset))
    ingester.run(dataset, workspace)
    log.info("Data ingested")
    return ingester.getFullMetadata()


def _getConfig(dataset):
    """Return the ingestion config associated with a specific dataset.

    Parameters
    ----------
    dataset : `lsst.ap.verify.dataset.Dataset`
        The dataset whose ingestion config is desired.

    Returns
    -------
    config : `DatasetIngestConfig`
        The config for running `DatasetIngestTask` on ``dataset``.
    """
    overrideFile = DatasetIngestTask._DefaultName + ".py"
    packageDir = lsst.utils.getPackageDir(dataset.obsPackage)

    config = DatasetIngestTask.ConfigClass()
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
        allFiles.difference_update(fnmatch.filter(allFiles, pattern))
    return allFiles
