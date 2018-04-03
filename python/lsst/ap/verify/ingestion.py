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

At present, the code is specific to DECam; it will be generalized to other
instruments in the future.
"""

from __future__ import absolute_import, division, print_function

__all__ = ["DatasetIngestConfig", "ingestDataset"]

import fnmatch
import os
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
        self._ingestRaws(dataset, workspace)
        self._ingestCalibs(dataset, workspace)
        self._ingestRefcats(dataset, workspace)
        self._ingestTemplates(dataset, workspace)

    def _ingestRaws(self, dataset, workspace):
        """Ingest the science data for use by LSST.

        The original data directory shall not be modified.

        Parameters
        ----------
        dataset : `lsst.ap.verify.dataset.Dataset`
            The dataset on which the pipeline will be run.
        workspace : `lsst.ap.verify.workspace.Workspace`
            The abstract location where ingestion repositories will be created.
        """
        dataset.makeCompatibleRepo(workspace.dataRepo)
        dataFiles = [os.path.join(dataset.rawLocation, fileName) for fileName in self.config.dataFiles]
        self._doIngest(workspace.dataRepo, dataFiles, self.config.dataBadFiles)

    def _doIngest(self, repo, dataFiles, badFiles):
        """Ingest raw images into a repository.

        ``repo`` shall be populated with *links* to ``dataFiles``.

        Parameters
        ----------
        repo : `str`
            The output repository location on disk for raw images.
        dataFiles : `list` of `str`
            A list of filenames to ingest. May contain wildcards.
        badFiles : `list` of `str`
            A list of filenames to exclude from ingestion. Must not contain paths.
            May contain wildcards.
        """
        if os.path.exists(os.path.join(repo, "registry.sqlite3")):
            self.log.info("Raw images were previously ingested, skipping...")
            return
        # TODO: make this a new-style repository (DM-12662)
        if not os.path.isdir(repo):
            os.mkdir(repo)
        # make a text file that handles the mapper, per the obs_decam github README
        with open(os.path.join(repo, "_mapper"), "w") as f:
            print("lsst.obs.decam.DecamMapper", file=f)

        self.log.info("Ingesting raw images...")
        args = [repo, "--filetype", "raw", "--mode", "link"]
        args.extend(dataFiles)
        if badFiles:
            args.append('--badFile')
            args.extend(badFiles)
        _runIngestTask(self.dataIngester, args)

        self.log.info("Images are now ingested in {0}".format(repo))

    def _ingestCalibs(self, dataset, workspace):
        """Ingest the calibration files for use by LSST.

        The original calibration directory shall not be modified.

        Parameters
        ----------
        dataset : `lsst.ap.verify.dataset.Dataset`
            The dataset on which the pipeline will be run.
        workspace : `lsst.ap.verify.workspace.Workspace`
            The abstract location where ingestion repositories will be created.
        """
        calibDataFiles = _getCalibDataFiles(self.config, dataset.calibLocation)
        if self.config.defectTarball:
            defectFiles = _getDefectFiles(dataset.defectLocation, self.config.defectTarball)
        else:
            defectFiles = []
        self._doIngestCalibs(workspace.dataRepo, workspace.calibRepo, calibDataFiles, defectFiles)

    def _doIngestCalibs(self, repo, calibRepo, calibDataFiles, defectFiles):
        """Ingest calibration files into a calibration repository.

        ``calibRepo`` shall be populated with *links* to ``calibDataFiles``.
        Defect images shall be registered in ``calibRepo`` but not linked.

        Parameters
        ----------
        repo : `str`
            The output repository location on disk for raw images.
        calibRepo : `str`
            The output repository location on disk for calibration files.
        calibDataFiles : `list` of `str`
            A list of non-defect filenames to ingest. Supported files vary by
            instrument but may include flats, biases, darks, fringes, or sky.
            May contain wildcards.
        defectFiles : `list` of `str`
            A list of defect filenames. The first element in this list must be
            the name of a .tar.gz file that contains all the compressed
            defect images, while the remaining elements are the defect images
            themselves.
        """
        if not os.path.isdir(calibRepo):
            os.mkdir(calibRepo)
            self._flatBiasIngest(repo, calibRepo, calibDataFiles)
            self._defectIngest(repo, calibRepo, defectFiles)
        elif os.path.exists(os.path.join(calibRepo, "cpBIAS")):
            self.log.info("Flats and biases were previously ingested, skipping...")
            self._defectIngest(repo, calibRepo, defectFiles)
        else:
            self._flatBiasIngest(repo, calibRepo, calibDataFiles)
            self._defectIngest(repo, calibRepo, defectFiles)

    def _flatBiasIngest(self, repo, calibRepo, calibDataFiles):
        """Ingest flats and biases into a calibration repository.

        Parameters
        ----------
        repo : `str`
            The output repository location on disk for raw images.
        calibRepo : `str`
            The output repository location on disk for calibration files.
        calibDataFiles : `list` of `str`
            A list of filenames to ingest. May contain wildcards.
        """
        self.log.info("Ingesting flats and biases...")
        args = [repo, "--calib", calibRepo, "--mode", "link", "--validity", str(self.config.calibValidity)]
        args.extend(calibDataFiles)
        try:
            _runIngestTask(self.calibIngester, args)
        except sqlite3.IntegrityError as detail:
            self.log.error("sqlite3.IntegrityError: ", detail)
            self.log.error("(sqlite3 doesn't think all the calibration files are unique)")
            raise
        else:
            self.log.info("Success!")
            self.log.info("Calibrations corresponding to {0} are now ingested in {1}".format(repo, calibRepo))

    def _defectIngest(self, repo, calibRepo, defectFiles):
        """Ingest defect images.

        Parameters
        ----------
        repo : `str`
            The output repository location on disk for raw images.
        calibRepo : `str`
            The output repository location on disk for calibration files.
        defectFiles : `list` of `str`
            A list of defect filenames. The first element in this list must be
            the name of a .tar.gz file that contains all the compressed
            defect images, while the remaining elements are the defect images
            themselves.

        Notes
        -----
        This function assumes very particular things about defect ingestion:
        - They must live in a .tar.gz file in the same location on disk as the other calibs
        - They will be ingested using ingestCalibs.py run from the ``calibRepo`` directory
        - They will be manually uncompressed and saved in :file:`calibRepo/defects/<tarballname>/`.
        - They will be added to the calib registry, but not linked like the flats and biases
        """
        # TODO: clean up implementation after DM-5467 resolved
        if not defectFiles:
            self.log.info("No defects to ingest, skipping...")
            return

        absRepo = os.path.abspath(repo)
        defectTarball = os.path.abspath(defectFiles[0] + ".tar.gz")
        startDir = os.path.abspath(os.getcwd())
        # CameraMapper does not accept absolute paths
        os.chdir(calibRepo)
        try:
            os.mkdir("defects")
        except OSError:
            # most likely the defects directory already exists
            if os.path.isdir("defects"):
                self.log.info("Defects were previously ingested, skipping...")
            else:
                self.log.error("Defect ingestion failed because 'defects' dir could not be created")
                raise
        else:
            self.log.info("Ingesting defects...")
            defectargs = [absRepo, "--calib", ".", "--calibType", "defect",
                          "--mode", "skip", "--validity", str(self.config.defectValidity)]
            tarfile.open(defectTarball, "r").extractall("defects")
            defectFiles = []
            for path, dirs, files in os.walk("defects"):
                for file in files:
                    if file.endswith(".fits"):
                        defectFiles.append(os.path.join(path, file))
            defectargs.extend(defectFiles)
            _runIngestTask(self.defectIngester, defectargs)
        finally:
            os.chdir(startDir)

    def _ingestRefcats(self, dataset, workspace):
        """Ingest the refcats for use by LSST.

        The original template repository shall not be modified.

        Parameters
        ----------
        dataset : `lsst.ap.verify.dataset.Dataset`
            The dataset on which the pipeline will be run.
        workspace : `lsst.ap.verify.workspace.Workspace`
            The abstract location where ingestion repositories will be created.
        """
        self._doIngestRefcats(workspace.dataRepo, dataset.refcatsLocation)

    def _doIngestRefcats(self, repo, refcats):
        """Ingest refcats so they are visible to a particular repository.

        Parameters
        ----------
        repo : `str`
            The output repository location on disk for raw images.
        refcats : `str`
            A directory containing .tar.gz files with LSST-formatted astrometric
            or photometric reference catalog information.

        Notes
        -----
        Refcats are not, at present, registered as part of the repository. They
        are not guaranteed to be visible to anything other than a ``refObjLoader``.
        """
        for refcatName, tarball in self.config.refcats.items():
            tarball = os.path.join(refcats, tarball)
            refcatDir = os.path.join(repo, "ref_cats", refcatName)
            tarfile.open(tarball, "r").extractall(refcatDir)

    def _ingestTemplates(self, dataset, workspace):
        """Ingest the templates for use by LSST.

        The original template repository shall not be modified.

        Parameters
        ----------
        dataset : `lsst.ap.verify.dataset.Dataset`
            The dataset on which the pipeline will be run.
        workspace : `lsst.ap.verify.workspace.Workspace`
            The abstract location where ingestion repositories will be created.
        """
        self._doIngestTemplates(workspace.templateRepo, dataset.templateLocation)

    def _doIngestTemplates(self, templateRepo, inputTemplates):
        """Ingest templates into the input repository, so that
        GetCoaddAsTemplateTask can find them.

        After this method returns, butler queries against `templateRepo` can find the
        templates in `inputTemplates`.

        Parameters
        ----------
        templateRepo: `str`
            The output repository location on disk for templates.
        inputTemplates: `str`
            The input repository location where templates have been previously computed.
        """
        # TODO: this check will need to be rewritten when Butler directories change, ticket TBD
        if os.path.exists(os.path.join(templateRepo, "deepCoadd")):
            self.log.info("Templates were previously ingested, skipping...")
        else:
            # TODO: chain inputTemplates to templateRepo once DM-12662 resolved
            if not os.path.isdir(templateRepo):
                os.mkdir(templateRepo)
            for baseName in os.listdir(inputTemplates):
                oldDir = os.path.abspath(os.path.join(inputTemplates, baseName))
                if os.path.isdir(oldDir):
                    os.symlink(oldDir, os.path.join(templateRepo, baseName))


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
    parsedCmd = argumentParser.parse_args(config=task.config, args=args)
    task.run(parsedCmd)


def _getCalibDataFiles(config, calibLocation):
    """Retrieve a list of the flat and bias files for use during ingestion.

    Parameters
    ----------
    config : `DatasetIngestConfig`
        The config for running `DatasetIngestTask` on ``dataset``.
    calibLocation : `str`
        The path on disk to where the calibration files live.

    Returns
    -------
    calibDataFiles : `list` of `str`
        A list of the filenames of each flat and bias image file.
    """
    allCalibDataFiles = []
    for files in config.calibFiles:
        allCalibDataFiles.extend(glob(os.path.join(calibLocation, files)))

    calibDataFiles = []
    filesToIgnore = config.calibBadFiles
    for calibFile in allCalibDataFiles:
        if all(not fnmatch.fnmatch(calibFile, string) for string in filesToIgnore):
            calibDataFiles.append(calibFile)
    return calibDataFiles


def _getDefectFiles(defectLocation, defectTarball):
    """Retrieve a list of the defect files for use during ingestion.

    Parameters
    ----------
    defectLocation : `str`
        The path on disk to where the defect tarball lives.
    defectTarball : `str`
        The filename of the tarball containing the defect files.

    Returns
    -------
    defectFiles : `list` of `str`
        A list of the filenames of each defect image file.
        The first element in this list will be the name of a .tar.gz file
        which contains all the compressed defect images.
    """
    # Retrieve defect filenames from tarball
    defectTarfilePath = os.path.join(defectLocation, defectTarball)
    defectFiles = tarfile.open(defectTarfilePath).getnames()
    defectFiles = [os.path.join(defectLocation, defectFile) for defectFile in defectFiles]
    return defectFiles
