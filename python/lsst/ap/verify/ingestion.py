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

import os
import tarfile
from glob import glob
import sqlite3

import lsst.log
from lsst.obs.decam import ingest
from lsst.obs.decam import ingestCalibs
from lsst.obs.decam.ingest import DecamParseTask
from lsst.pipe.tasks.ingest import IngestConfig
from lsst.pipe.tasks.ingestCalibs import IngestCalibsConfig, IngestCalibsTask
from lsst.pipe.tasks.ingestCalibs import IngestCalibsArgumentParser
import lsst.daf.base as dafBase
from lsst.ap.pipe import doIngestTemplates

# Name of defects tarball residing in dataset's defects directory
_DEFECT_TARBALL = 'defects_2014-12-05.tar.gz'


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
    metadata.combine(_ingestTemplates(dataset, workspace))
    log.info('Data ingested')
    return metadata


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
    dataFiles = _getDataFiles(dataset.rawLocation)
    return _doIngest(workspace.dataRepo, dataset.refcatsLocation, dataFiles)


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
    calibDataFiles = _getCalibDataFiles(dataset.calibLocation)
    defectFiles = _getDefectFiles(dataset.defectLocation)
    return _doIngestCalibs(workspace.dataRepo, workspace.calibRepo, calibDataFiles, defectFiles)


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
    # TODO: move doIngestTemplates to this module once DM-11865 resolved
    return doIngestTemplates(workspace.dataRepo, workspace.templateRepo, dataset.templateLocation)


def _doIngest(repo, refcats, dataFiles):
    """Ingest raw DECam images into a repository with a corresponding registry

    ``repo`` shall be populated with *links* to ``dataFiles``.

    Parameters
    ----------
    repo : `str`
        The output repository location on disk where ingested raw images live.
    refcats : `str`
        A directory containing two .tar.gz files with LSST-formatted astrometric
        and photometric reference catalog information. The files must be named
        :file:`gaia_HiTS_2015.tar.gz` and :file:`ps1_HiTS_2015.tar.gz`.
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
    # Names of tarballs containing astrometric and photometric reference catalog files
    ASTROM_REFCAT_TAR = 'gaia_HiTS_2015.tar.gz'
    PHOTOM_REFCAT_TAR = 'ps1_HiTS_2015.tar.gz'

    # Names of reference catalog directories processCcd expects to find in repo
    ASTROM_REFCAT_DIR = 'ref_cats/gaia'
    PHOTOM_REFCAT_DIR = 'ref_cats/pan-starrs'

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
    # set up the decam ingest task so it can take arguments
    # ('name' says which file in obs_decam/config to use)
    argumentParser = ingest.DecamIngestArgumentParser(name='ingest')
    # create an instance of ingest configuration
    # the retarget command is from line 2 of obs_decam/config/ingest.py
    config = IngestConfig()
    config.parse.retarget(DecamParseTask)
    # create an *instance* of the decam ingest task
    ingestTask = ingest.DecamIngestTask(config=config)
    # feed everything to the argument parser
    parsedCmd = argumentParser.parse_args(config=config, args=args)
    # finally, run the ingestTask
    ingestTask.run(parsedCmd)
    # Copy refcats files to repo (needed for doProcessCcd)
    astrometryTarball = os.path.join(refcats, ASTROM_REFCAT_TAR)
    photometryTarball = os.path.join(refcats, PHOTOM_REFCAT_TAR)
    tarfile.open(astrometryTarball, 'r').extractall(os.path.join(repo, ASTROM_REFCAT_DIR))
    tarfile.open(photometryTarball, 'r').extractall(os.path.join(repo, PHOTOM_REFCAT_DIR))
    log.info('Images are now ingested in {0}'.format(repo))
    metadata = ingestTask.getFullMetadata()
    return metadata


def _flatBiasIngest(repo, calibRepo, calibDataFiles):
    """Ingest DECam flats and biases

    Parameters
    ----------
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
    argumentParser = IngestCalibsArgumentParser(name='ingestCalibs')
    config = IngestCalibsConfig()
    config.parse.retarget(ingestCalibs.DecamCalibsParseTask)
    calibIngestTask = IngestCalibsTask(config=config, name='ingestCalibs')
    parsedCmd = argumentParser.parse_args(config=config, args=args)
    try:
        calibIngestTask.run(parsedCmd)
    except sqlite3.IntegrityError as detail:
        log.error('sqlite3.IntegrityError: ', detail)
        log.error('(sqlite3 doesn\'t think all the calibration files are unique)')
        raise
    else:
        log.info('Success!')
        log.info('Calibrations corresponding to {0} are now ingested in {1}'.format(repo, calibRepo))
        metadata = calibIngestTask.getFullMetadata()
    return metadata


def _defectIngest(repo, calibRepo, defectFiles):
    """Ingest DECam defect images

    Parameters
    ----------
    repo : `str`
        The output repository location on disk where ingested raw images live.
    calibRepo : `str`
        The output repository location on disk where ingested calibration images live.
    defectFiles : `list` of `str`
        A list of the filenames of each defect image file.
        The first element in this list must be the name of a .tar.gz file
        which contains all the compressed defect images.

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
        defectFiles = glob(os.path.join('defects', os.path.basename(defectFiles[0]), '*.fits'))
        defectargs.extend(defectFiles)
        defectArgumentParser = IngestCalibsArgumentParser(name='ingestCalibs')
        defectConfig = IngestCalibsConfig()
        defectConfig.parse.retarget(ingestCalibs.DecamCalibsParseTask)
        DefectIngestTask = IngestCalibsTask(config=defectConfig, name='ingestCalibs')
        defectParsedCmd = defectArgumentParser.parse_args(config=defectConfig, args=defectargs)
        DefectIngestTask.run(defectParsedCmd)
        metadata = DefectIngestTask.getFullMetadata()
    finally:
        os.chdir(startDir)
    return metadata


def _doIngestCalibs(repo, calibRepo, calibDataFiles, defectFiles):
    """Ingest DECam MasterCal biases, flats, and defects into a calibration
    repository with a corresponding registry.

    ``calibRepo`` shall populated with *links* to ``calibDataFiles`` (bias and
    flat images only). Defect images shall be registered in ``calibRepo`` but
    not linked.

    Parameters
    ----------
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
        flatBiasMetadata = _flatBiasIngest(repo, calibRepo, calibDataFiles)
        defectMetadata = _defectIngest(repo, calibRepo, defectFiles)
    elif os.path.exists(os.path.join(calibRepo, 'cpBIAS')):
        log.warn('Flats and biases were previously ingested, skipping...')
        flatBiasMetadata = None
        defectMetadata = _defectIngest(repo, calibRepo, defectFiles)
    else:
        flatBiasMetadata = _flatBiasIngest(repo, calibRepo, calibDataFiles)
        defectMetadata = _defectIngest(repo, calibRepo, defectFiles)
    # Handle the case where one or both of the calib metadatas may be None
    if flatBiasMetadata is not None:
        calibIngestMetadata = flatBiasMetadata
        if defectMetadata is not None:
            calibIngestMetadata.combine(defectMetadata)
    else:
        calibIngestMetadata = defectMetadata
    return calibIngestMetadata


def _getDataFiles(rawLocation):
    """Retrieve a list of the raw DECam images for use during ingestion.

    Parameters
    ----------
    rawLocation : `str`
        The path on disk to where the raw files live.

    Returns
    -------
    dataFiles : `list` of `str`
        A list of the filenames of each raw image file.
    """
    types = ('*.fits', '*.fz')
    dataFiles = []
    for files in types:
        dataFiles.extend(glob(os.path.join(rawLocation, files)))
    return dataFiles


def _getCalibDataFiles(calibLocation):
    """Retrieve a list of the DECam MasterCal flat and bias files for use during ingestion.

    Parameters
    ----------
    calibLocation : `str`
        The path on disk to where the calibration files live.

    Returns
    -------
    calibDataFiles : `list` of `str`
        A list of the filenames of each flat and bias image file.
    """
    types = ('*.fits', '*.fz')
    allCalibDataFiles = []
    for files in types:
        allCalibDataFiles.extend(glob(os.path.join(calibLocation, files)))
    # Ignore wtmaps and illumcors
    # These data products may be useful in the future, but are not yet supported by the Stack
    # and will confuse the ingester
    calibDataFiles = []
    filesToIgnore = ['fcw', 'zcw', 'ici']
    for calibFile in allCalibDataFiles:
        if all(string not in calibFile for string in filesToIgnore):
            calibDataFiles.append(calibFile)
    return calibDataFiles


def _getDefectFiles(defectLocation, defectTarball=_DEFECT_TARBALL):
    """Retrieve a list of the DECam defect files for use during ingestion.

    Parameters
    ----------
    defectLocation : `str`
        The path on disk to where the defect tarball lives.
    defectTarball : `str`, optional
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


def _getOutputRepo(outputRoot, outputDir):
    """Return location on disk for one output repository used by ``ap_pipe``.

    Parameters
    ----------
    outputRoot: `str`
        The top-level directory where the output will live.
    outputDir: `str`
        Name of the subdirectory to be created in ``outputRoot``.

    Returns
    -------
    outputPath: `str`
        Repository (directory on disk) where desired output product will live.
    """
    if not os.path.isdir(outputRoot):
        os.mkdir(outputRoot)
    outputPath = os.path.join(outputRoot, outputDir)
    return outputPath
