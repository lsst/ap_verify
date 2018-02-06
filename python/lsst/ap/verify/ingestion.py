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

# Names of directories to be created in specified repository
INGESTED_DIR = 'ingested'
CALIBINGESTED_DIR = 'calibingested'

# Name of defects tarball residing in dataset's defects directory
DEFECT_TARBALL = 'defects_2014-12-05.tar.gz'


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
    dataset.makeCompatibleRepo(repository)
    log.info('Input repo at %s created.', repository)

    metadata = dafBase.PropertySet()
    metadata.combine(_ingestRaws(dataset, repository))
    metadata.combine(_ingestCalibs(dataset, repository))
    metadata.combine(_ingestTemplates(dataset, repository))
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
    raw_repo = _get_output_repo(workingRepo, INGESTED_DIR)
    datafiles = _get_datafiles(dataset.rawLocation)
    return _doIngest(raw_repo, dataset.refcatsLocation, datafiles)


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
    repo = _get_output_repo(workingRepo, INGESTED_DIR)
    calib_repo = _get_output_repo(workingRepo, CALIBINGESTED_DIR)
    calib_datafiles = _get_calib_datafiles(dataset.calibLocation)
    defectfiles = _get_defectfiles(dataset.defectLocation)
    return _doIngestCalibs(repo, calib_repo, calib_datafiles, defectfiles)


def _ingestTemplates(dataset, workingRepo):
    """Ingest the templates for use by LSST.

    The original template repository shall not be modified.

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
    # TODO: move doIngestTemplates to this module once DM-11865 resolved
    rawRepo = _get_output_repo(workingRepo, INGESTED_DIR)
    return doIngestTemplates(rawRepo, rawRepo, dataset.templateLocation)


def _doIngest(repo, refcats, datafiles):
    '''
    Ingest raw DECam images into a repository with a corresponding registry

    Parameters
    ----------
    repo: `str`
        The output repository location on disk where ingested raw images live.
    refcats: `str`
        A directory containing two .tar.gz files with LSST-formatted astrometric
        and photometric reference catalog information. The filenames are set below.
    datafiles: `list`
        A list of the filenames of each raw image file.

    BASH EQUIVALENT:
    $ ingestImagesDecam.py repo --filetype raw --mode link datafiles
    ** If run from bash, refcats must also be manually copied or symlinked to repo

    Returns
    -------
    ingest_metadata: `PropertySet` or None
        Metadata from the IngestTask for use by ap_verify

    RESULT:
    repo populated with *links* to datafiles, organized by date
    sqlite3 database registry of ingested images also created in repo

    NOTE:
    This functions ingests *all* the images, not just the ones for the
    specified visits and/or filters. We may want to revisit this in the future.
    '''
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
    args.extend(datafiles)
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
    astrom_tarball = os.path.join(refcats, ASTROM_REFCAT_TAR)
    photom_tarball = os.path.join(refcats, PHOTOM_REFCAT_TAR)
    tarfile.open(astrom_tarball, 'r').extractall(os.path.join(repo, ASTROM_REFCAT_DIR))
    tarfile.open(photom_tarball, 'r').extractall(os.path.join(repo, PHOTOM_REFCAT_DIR))
    log.info('Images are now ingested in {0}'.format(repo))
    ingest_metadata = ingestTask.getFullMetadata()
    return ingest_metadata


def _flatBiasIngest(repo, calib_repo, calib_datafiles):
    '''
    Ingest DECam flats and biases (called by _doIngestCalibs)

    Parameters
    ----------
    repo: `str`
        The output repository location on disk where ingested raw images live.
    calib_repo: `str`
        The output repository location on disk where ingested calibration images live.
    calib_datafiles: `list`
        A list of the filenames of each flat and bias image file.

    Returns
    -------
    flatBias_metadata: `PropertySet` or None
        Metadata from the IngestCalibTask (flats and biases) for use by ap_verify

    BASH EQUIVALENT:
    $ ingestCalibs.py repo --calib calib_repo --mode=link --validity 999 calib_datafiles
    '''
    log = lsst.log.Log.getLogger('ap.pipe._flatBiasIngest')
    log.info('Ingesting flats and biases...')
    args = [repo, '--calib', calib_repo, '--mode', 'link', '--validity', '999']
    args.extend(calib_datafiles)
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
        log.info('Calibrations corresponding to {0} are now ingested in {1}'.format(repo, calib_repo))
        flatBias_metadata = calibIngestTask.getFullMetadata()
    return flatBias_metadata


def _defectIngest(repo, calib_repo, defectfiles):
    '''
    Ingest DECam defect images (called by _doIngestCalibs)

    Parameters
    ----------
    repo: `str`
        The output repository location on disk where ingested raw images live.
    calib_repo: `str`
        The output repository location on disk where ingested calibration images live.
    defectfiles: `list`
        A list of the filenames of each defect image file.
        The first element in this list must be the name of a .tar.gz file
        which contains all the compressed defect images.

    Returns
    -------
    defect_metadata: `PropertySet` or None
        Metadata from the IngestCalibTask (defects) for use by ap_verify

    BASH EQUIVALENT:
    $ cd calib_repo
    $ ingestCalibs.py ../../repo --calib . --mode=skip --calibType defect --validity 999 defectfiles
    $ cd ..

    This function assumes very particular things about defect ingestion:
    - They must live in a .tar.gz file in the same location on disk as the other calibs
    - They will be ingested using ingestCalibs.py run from the calib_repo directory
    - They will be manually uncompressed and saved in calib_repo/defects/<tarballname>/.
    - They will be added to the calib registry, but not linked like the flats and biases
    '''
    # TODO: clean up implementation after DM-5467 resolved
    log = lsst.log.Log.getLogger('ap.pipe._defectIngest')
    absRepo = os.path.abspath(repo)
    defect_tarball = os.path.abspath(defectfiles[0] + '.tar.gz')
    startDir = os.path.abspath(os.getcwd())
    # CameraMapper does not accept absolute paths
    os.chdir(calib_repo)
    try:
        os.mkdir('defects')
    except OSError:
        # most likely the defects directory already exists
        if os.path.isdir('defects'):
            log.warn('Defects were previously ingested, skipping...')
            defect_metadata = None
        else:
            log.error('Defect ingestion failed because \'defects\' dir could not be created')
            raise
    else:
        log.info('Ingesting defects...')
        defectargs = [absRepo, '--calib', '.', '--calibType', 'defect',
                      '--mode', 'skip', '--validity', '999']
        tarfile.open(defect_tarball, 'r').extractall('defects')
        defectfiles = glob(os.path.join('defects', os.path.basename(defectfiles[0]), '*.fits'))
        defectargs.extend(defectfiles)
        defectArgumentParser = IngestCalibsArgumentParser(name='ingestCalibs')
        defectConfig = IngestCalibsConfig()
        defectConfig.parse.retarget(ingestCalibs.DecamCalibsParseTask)
        DefectIngestTask = IngestCalibsTask(config=defectConfig, name='ingestCalibs')
        defectParsedCmd = defectArgumentParser.parse_args(config=defectConfig, args=defectargs)
        DefectIngestTask.run(defectParsedCmd)
        defect_metadata = DefectIngestTask.getFullMetadata()
    finally:
        os.chdir(startDir)
    return defect_metadata


def _doIngestCalibs(repo, calib_repo, calib_datafiles, defectfiles):
    '''
    Ingest DECam MasterCal biases and flats into a calibration repository with a corresponding registry.
    Also ingest DECam defects into the calib registry.

    Parameters
    ----------
    repo: `str`
        The output repository location on disk where ingested raw images live.
    calib_repo: `str`
        The output repository location on disk where ingested calibration images live.
    calib_datafiles: `list`
        A list of the filenames of each flat and bias image file.
    defectfiles: `list`
            A list of the filenames of each defect image file.
            The first element in this list must be the name of a .tar.gz file
            which contains all the compressed defect images.

    Returns
    -------
    calibingest_metadata: `PropertySet` or None
        Metadata from the IngestCalibTask (flats and biases) and from the
        IngestCalibTask (defects) for use by ap_verify

    RESULT:
    calib_repo populated with *links* to calib_datafiles,
    organized by date (bias and flat images only)
    sqlite3 database registry of ingested calibration products (bias, flat,
    and defect images) created in calib_repo

    NOTE:
    calib ingestion ingests *all* the calibs, not just the ones needed
    for certain visits. We may want to ...revisit... this in the future.
    '''
    log = lsst.log.Log.getLogger('ap.pipe._doIngestCalibs')
    if not os.path.isdir(calib_repo):
        os.mkdir(calib_repo)
        flatBias_metadata = _flatBiasIngest(repo, calib_repo, calib_datafiles)
        defect_metadata = _defectIngest(repo, calib_repo, defectfiles)
    elif os.path.exists(os.path.join(calib_repo, 'cpBIAS')):
        log.warn('Flats and biases were previously ingested, skipping...')
        flatBias_metadata = None
        defect_metadata = _defectIngest(repo, calib_repo, defectfiles)
    else:
        flatBias_metadata = _flatBiasIngest(repo, calib_repo, calib_datafiles)
        defect_metadata = _defectIngest(repo, calib_repo, defectfiles)
    # Handle the case where one or both of the calib metadatas may be None
    if flatBias_metadata is not None:
        calibingest_metadata = flatBias_metadata
        if defect_metadata is not None:
            calibingest_metadata.combine(defect_metadata)
    else:
        calibingest_metadata = defect_metadata
    return calibingest_metadata


def _get_datafiles(raw_location):
    '''
    Retrieve a list of the raw DECam images for use during ingestion.

    Parameters
    ----------
    raw_location: `str`
        The path on disk to where the raw files live.

    Returns
    -------
    datafiles: `list`
        A list of the filenames of each raw image file.
    '''
    types = ('*.fits', '*.fz')
    datafiles = []
    for files in types:
        datafiles.extend(glob(os.path.join(raw_location, files)))
    return datafiles


def _get_calib_datafiles(calib_location):
    '''
    Retrieve a list of the DECam MasterCal flat and bias files for use during ingestion.

    Parameters
    ----------
    calib_location: `str`
        The path on disk to where the calibration files live.

    Returns
    -------
    calib_datafiles: `list`
        A list of the filenames of each flat and bias image file.
    '''
    types = ('*.fits', '*.fz')
    all_calib_datafiles = []
    for files in types:
        all_calib_datafiles.extend(glob(os.path.join(calib_location, files)))
    # Ignore wtmaps and illumcors
    # These data products may be useful in the future, but are not yet supported by the Stack
    # and will confuse the ingester
    calib_datafiles = []
    files_to_ignore = ['fcw', 'zcw', 'ici']
    for file in all_calib_datafiles:
        if all(string not in file for string in files_to_ignore):
            calib_datafiles.append(file)
    return calib_datafiles


def _get_defectfiles(defect_location, defect_tarball=DEFECT_TARBALL):
    '''
    Retrieve a list of the DECam defect files for use during ingestion.

    Parameters
    ----------
    defect_location: `str`
        The path on disk to where the defect tarball lives.
    defect_tarball: `str`
        The filename of the tarball containing the defect files.

    Returns
    -------
    defectfiles: `list`
        A list of the filenames of each defect image file.
        The first element in this list will be the name of a .tar.gz file
        which contains all the compressed defect images.
    '''
    # Retrieve defect filenames from tarball
    defect_tarfile_path = os.path.join(defect_location, defect_tarball)
    defectfiles = tarfile.open(defect_tarfile_path).getnames()
    defectfiles = [os.path.join(defect_location, file) for file in defectfiles]
    return defectfiles


def _get_output_repo(output_root, output_dir):
    '''
    Return location on disk for one output repository used by ap_pipe.

    Parameters
    ----------
    output_root: `str`
        The top-level directory where the output will live.
    output_dir: `str`
        Name of the subdirectory to be created in output_root.

    Returns
    -------
    output_path: `str`
        Repository (directory on disk) where desired output product will live.
    '''
    if not os.path.isdir(output_root):
        os.mkdir(output_root)
    output_path = os.path.join(output_root, output_dir)
    return output_path
