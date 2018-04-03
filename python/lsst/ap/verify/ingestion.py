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

import lsst.log
import lsst.pex.config as pexConfig
from lsst.pipe.tasks.ingest import IngestTask
from lsst.pipe.tasks.ingestCalibs import IngestCalibsTask
import lsst.daf.base as dafBase


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

    refcats = pexConfig.DictField(
        keytype=str,
        itemtype=str,
        default={},
        doc="Map from a refcat name to a tar.gz file containing the sharded catalog. May be empty.",
    )


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
    log = lsst.log.Log.getLogger('ap.verify.ingestion.ingestDataset')

    metadata = dafBase.PropertySet()
    metadata.combine(_ingestRaws(dataset, workspace))
    metadata.combine(_ingestCalibs(dataset, workspace))
    metadata.combine(_ingestRefcats(dataset, workspace))
    metadata.combine(_ingestTemplates(dataset, workspace))
    log.info('Data ingested')
    return metadata


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
    path = dataset.configLocation
    config = DatasetIngestConfig()
    config.load(os.path.join(path, 'datasetIngest.py'))
    return config


def _ingestRaws(dataset, workspace):
    """Ingest the science data for use by LSST.

    The original data directory shall not be modified.

    Parameters
    ----------
    dataset : `lsst.ap.verify.dataset.Dataset`
        The dataset on which the pipeline will be run.
    workspace : `lsst.ap.verify.workspace.Workspace`
        The abstract location where ingestion repositories will be created.

    Returns
    -------
    metadata : `lsst.daf.base.PropertySet`
        The full metadata from any Tasks called by this function, or `None`.
    """
    dataset.makeCompatibleRepo(workspace.dataRepo)
    config = _getConfig(dataset)
    dataFiles = [os.path.join(dataset.rawLocation, fileName) for fileName in config.dataFiles]
    return _doIngest(config, workspace.dataRepo, dataFiles)


def _ingestCalibs(dataset, workspace):
    """Ingest the calibration files for use by LSST.

    The original calibration directory shall not be modified.

    Parameters
    ----------
    dataset : `lsst.ap.verify.dataset.Dataset`
        The dataset on which the pipeline will be run.
    workspace : `lsst.ap.verify.workspace.Workspace`
        The abstract location where ingestion repositories will be created.

    Returns
    -------
    metadata : `lsst.daf.base.PropertySet`
        The full metadata from any Tasks called by this function, or `None`.
    """
    config = _getConfig(dataset)
    calibDataFiles = _getCalibDataFiles(config, dataset.calibLocation)
    defectFiles = _getDefectFiles(dataset.defectLocation, config.defectTarball)
    return _doIngestCalibs(config, workspace.dataRepo, workspace.calibRepo, calibDataFiles, defectFiles)


def _ingestTemplates(dataset, workspace):
    """Ingest the templates for use by LSST.

    The original template repository shall not be modified.

    Parameters
    ----------
    dataset : `lsst.ap.verify.dataset.Dataset`
        The dataset on which the pipeline will be run.
    workspace : `lsst.ap.verify.workspace.Workspace`
        The abstract location where ingestion repositories will be created.

    Returns
    -------
    metadata : `lsst.daf.base.PropertySet`
        The full metadata from any Tasks called by this function, or `None`.
    """
    return _doIngestTemplates(workspace.templateRepo, dataset.templateLocation)


def _doIngestTemplates(templateRepo, inputTemplates):
    '''Ingest templates into the input repository, so that
    GetCoaddAsTemplateTask can find them.

    After this method returns, butler queries against `templateRepo` can find the
    templates in `inputTemplates`.

    Parameters
    ----------
    templateRepo: `str`
        The output repository location on disk where ingested templates live.
    inputTemplates: `str`
        The input repository location where templates have been previously computed.

    Returns
    -------
    calibingest_metadata: `PropertySet` or None
        Metadata from any tasks run by this method
    '''
    log = lsst.log.Log.getLogger('ap.verify.ingestion._doIngestTemplates')
    # TODO: this check will need to be rewritten when Butler directories change, ticket TBD
    if os.path.exists(os.path.join(templateRepo, 'deepCoadd')):
        log.warn('Templates were previously ingested, skipping...')
        return None
    else:
        # TODO: chain inputTemplates to templateRepo once DM-12662 resolved
        if not os.path.isdir(templateRepo):
            os.mkdir(templateRepo)
        for baseName in os.listdir(inputTemplates):
            oldDir = os.path.abspath(os.path.join(inputTemplates, baseName))
            if os.path.isdir(oldDir):
                os.symlink(oldDir, os.path.join(templateRepo, baseName))
        return None


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


def _doIngest(config, repo, dataFiles):
    """Ingest raw DECam images into a repository with a corresponding registry

    ``repo`` shall be populated with *links* to ``dataFiles``.

    Parameters
    ----------
    config : `DatasetIngestConfig`
        The ingestion configuration.
    repo : `str`
        The output repository location on disk where ingested raw images live.
    dataFiles : `list` of `str`
        A list of the filenames of each raw image file.

    Returns
    -------
    metadata : `PropertySet` or `None`
        Metadata from the `IngestTask` for use by ``ap_verify``

    Notes
    -----
    This function ingests *all* the images, not just the ones for the
    specified visits and/or filters. We may want to revisit this in the future.
    """
    log = lsst.log.Log.getLogger('ap.pipe._doIngest')
    if os.path.exists(os.path.join(repo, 'registry.sqlite3')):
        log.warn('Raw images were previously ingested, skipping...')
        return None
    # TODO: make this a new-style repository (DM-12662)
    if not os.path.isdir(repo):
        os.mkdir(repo)
    # make a text file that handles the mapper, per the obs_decam github README
    with open(os.path.join(repo, '_mapper'), 'w') as f:
        print('lsst.obs.decam.DecamMapper', file=f)
    log.info('Ingesting raw images...')
    # save arguments you'd put on the command line after 'ingestImagesDecam.py'
    # (extend the list with all the filenames as the last set of arguments)
    args = [repo, '--filetype', 'raw', '--mode', 'link']
    args.extend(dataFiles)
    ingestTask = config.dataIngester.apply()
    _runIngestTask(ingestTask, args)

    log.info('Images are now ingested in {0}'.format(repo))
    metadata = ingestTask.getFullMetadata()
    return metadata


def _ingestRefcats(dataset, workspace):
    """Ingest the refcats for use by LSST.

    The original template repository shall not be modified.

    Parameters
    ----------
    dataset : `lsst.ap.verify.dataset.Dataset`
        The dataset on which the pipeline will be run.
    workspace : `lsst.ap.verify.workspace.Workspace`
        The abstract location where ingestion repositories will be created.
    """
    _doIngestRefcats(_getConfig(dataset), workspace.dataRepo, dataset.refcatsLocation)


def _doIngestRefcats(config, repo, refcats):
    """Ingest refcats so they are visible to a particular repository.

    Parameters
    ----------
    config : `DatasetIngestConfig`
        The ingestion configuration.
    repo : `str`
        The output repository location on disk where ingested raw images live.
    refcats : `str`
        A directory containing .tar.gz files with LSST-formatted astrometric
        or photometric reference catalog information.

    Notes
    -----
    Refcats are not, at present, registered as part of the repository. They
    are not guaranteed to be visible to anything other than a ``refObjLoader``.
    """
    for refcatName, tarball in config.refcats.items():
        tarball = os.path.join(refcats, tarball)
        refcatDir = os.path.join(repo, 'ref_cats', refcatName)
        tarfile.open(tarball, 'r').extractall(refcatDir)


def _flatBiasIngest(config, repo, calibRepo, calibDataFiles):
    """Ingest DECam flats and biases

    Parameters
    ----------
    config : `DatasetIngestConfig`
        The ingestion configuration.
    repo : `str`
        The output repository location on disk where ingested raw images live.
    calibRepo : `str`
        The output repository location on disk where ingested calibration images live.
    calibDataFiles : `list` of `str`
        A list of the filenames of each flat and bias image file.

    Returns
    -------
    metadata : `PropertySet` or `None`
        Metadata from the `IngestCalibTask` (flats and biases) for use by ``ap_verify``
    """
    log = lsst.log.Log.getLogger('ap.pipe._flatBiasIngest')
    log.info('Ingesting flats and biases...')
    args = [repo, '--calib', calibRepo, '--mode', 'link', '--validity', '999']
    args.extend(calibDataFiles)
    calibIngestTask = config.calibIngester.apply()
    try:
        _runIngestTask(calibIngestTask, args)
    except sqlite3.IntegrityError as detail:
        log.error('sqlite3.IntegrityError: ', detail)
        log.error('(sqlite3 doesn\'t think all the calibration files are unique)')
        raise
    else:
        log.info('Success!')
        log.info('Calibrations corresponding to {0} are now ingested in {1}'.format(repo, calibRepo))
        metadata = calibIngestTask.getFullMetadata()
    return metadata


def _defectIngest(config, repo, calibRepo, defectFiles):
    """Ingest DECam defect images

    Parameters
    ----------
    config : `DatasetIngestConfig`
        The ingestion configuration.
    repo : `str`
        The output repository location on disk where ingested raw images live.
    calibRepo : `str`
        The output repository location on disk where ingested calibration images live.
    defectFiles : `list` of `str`
        A list of the filenames of each defect image file.
        The first element in this list must be the name of a .tar.gz file,
        without the extension, which contains all the compressed defect images.

    Returns
    -------
    metadata : `PropertySet` or `None`
        Metadata from the `IngestCalibTask` (defects) for use by ``ap_verify``

    Notes
    -----
    This function assumes very particular things about defect ingestion:
    - They must live in a .tar.gz file in the same location on disk as the other calibs
    - They will be ingested using ingestCalibs.py run from the ``calibRepo`` directory
    - They will be manually uncompressed and saved in :file:`calibRepo/defects/<tarballname>/`.
    - They will be added to the calib registry, but not linked like the flats and biases
    """
    # TODO: clean up implementation after DM-5467 resolved
    log = lsst.log.Log.getLogger('ap.pipe._defectIngest')
    absRepo = os.path.abspath(repo)
    defectTarball = os.path.abspath(defectFiles[0] + '.tar.gz')
    startDir = os.path.abspath(os.getcwd())
    # CameraMapper does not accept absolute paths
    os.chdir(calibRepo)
    try:
        os.mkdir('defects')
    except OSError:
        # most likely the defects directory already exists
        if os.path.isdir('defects'):
            log.warn('Defects were previously ingested, skipping...')
            metadata = None
        else:
            log.error('Defect ingestion failed because \'defects\' dir could not be created')
            raise
    else:
        log.info('Ingesting defects...')
        defectargs = [absRepo, '--calib', '.', '--calibType', 'defect',
                      '--mode', 'skip', '--validity', '999']
        tarfile.open(defectTarball, 'r').extractall('defects')
        defectFiles = []
        for path, dirs, files in os.walk('defects'):
            for file in files:
                if file.endswith('.fits'):
                    defectFiles.append(os.path.join(path, file))
        defectargs.extend(defectFiles)
        defectIngestTask = config.defectIngester.apply()
        _runIngestTask(defectIngestTask, defectargs)
        metadata = defectIngestTask.getFullMetadata()
    finally:
        os.chdir(startDir)
    return metadata


def _doIngestCalibs(config, repo, calibRepo, calibDataFiles, defectFiles):
    """Ingest DECam MasterCal biases, flats, and defects into a calibration
    repository with a corresponding registry.

    ``calibRepo`` shall populated with *links* to ``calibDataFiles`` (bias and
    flat images only). Defect images shall be registered in ``calibRepo`` but
    not linked.

    Parameters
    ----------
    config : `DatasetIngestConfig`
        The ingestion configuration.
    repo : `str`
        The output repository location on disk where ingested raw images live.
    calibRepo : `str`
        The output repository location on disk where ingested calibration images live.
    calibDataFiles : `list` of `str`
        A list of the filenames of each flat and bias image file.
    defectFiles : `list` of `str`
            A list of the filenames of each defect image file.
            The first element in this list must be the name of a .tar.gz file
            which contains all the compressed defect images.

    Returns
    -------
    metadata : `PropertySet` or `None`
        Metadata from the `IngestCalibTask` (flats and biases) and from the
        `IngestCalibTask` (defects) for use by ``ap_verify``

    Notes
    -----
    calib ingestion ingests *all* the calibs, not just the ones needed
    for certain visits. We may want to ...revisit... this in the future.
    """
    log = lsst.log.Log.getLogger('ap.pipe._doIngestCalibs')
    if not os.path.isdir(calibRepo):
        os.mkdir(calibRepo)
        flatBiasMetadata = _flatBiasIngest(config, repo, calibRepo, calibDataFiles)
        defectMetadata = _defectIngest(config, repo, calibRepo, defectFiles)
    elif os.path.exists(os.path.join(calibRepo, 'cpBIAS')):
        log.warn('Flats and biases were previously ingested, skipping...')
        flatBiasMetadata = None
        defectMetadata = _defectIngest(config, repo, calibRepo, defectFiles)
    else:
        flatBiasMetadata = _flatBiasIngest(config, repo, calibRepo, calibDataFiles)
        defectMetadata = _defectIngest(config, repo, calibRepo, defectFiles)
    # Handle the case where one or both of the calib metadatas may be None
    if flatBiasMetadata is not None:
        calibIngestMetadata = flatBiasMetadata
        if defectMetadata is not None:
            calibIngestMetadata.combine(defectMetadata)
    else:
        calibIngestMetadata = defectMetadata
    return calibIngestMetadata


def _getCalibDataFiles(config, calibLocation):
    """Retrieve a list of the DECam MasterCal flat and bias files for use during ingestion.

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
    """Retrieve a list of the DECam defect files for use during ingestion.

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
