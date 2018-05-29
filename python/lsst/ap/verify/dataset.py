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

import os

from lsst.daf.persistence import Butler
import lsst.pex.exceptions as pexExcept
from lsst.utils import getPackageDir

from .config import Config


class Dataset:
    """A dataset supported by ``ap_verify``.

    Any object of this class is guaranteed to represent a ready-for-use
    dataset, barring concurrent changes to the file system or EUPS operations.
    Constructing a Dataset does not create a compatible output repository(ies),
    which can be done by calling `makeCompatibleRepo`.

    Parameters
    ----------
    datasetId : `str`
       A tag identifying the dataset.

    Raises
    ------
    `RuntimeError`
        `datasetId` exists, but is not correctly organized or incomplete
    `ValueError`
        `datasetId` is not a recognized dataset. No side effects if this
        exception is raised.
    """

    def __init__(self, datasetId):
        # daf.persistence.Policy's behavior on missing keys is apparently undefined
        # test for __getattr__ *either* raising KeyError or returning None
        try:
            datasetPackage = self._getDatasetInfo()[datasetId]
            if datasetPackage is None:
                raise KeyError
        except KeyError:
            raise ValueError('Unsupported dataset: ' + datasetId)

        try:
            self._dataRootDir = getPackageDir(datasetPackage)
        except pexExcept.NotFoundError as e:
            error = 'Dataset %s requires the %s package, which has not been set up.' \
                % (datasetId, datasetPackage)
            raise RuntimeError(error) from e
        else:
            self._validatePackage()

        self._initPackage(datasetPackage)

    def _initPackage(self, name):
        """Prepare the package backing this dataset.

        Parameters
        ----------
        name : `str`
           The EUPS package identifier for the desired package.
        """
        # No initialization required at present
        pass

    @staticmethod
    def getSupportedDatasets():
        """The dataset IDs that can be passed to this class's constructor.

        Returns
        -------
        datasets : `set` of `str`
            the set of IDs that will be accepted

        Raises
        ------
        `IoError`
            if the config file does not exist or is not readable
        `RuntimeError`
            if the config file exists, but does not contain the expected data
        """
        return Dataset._getDatasetInfo().keys()

    @staticmethod
    def _getDatasetInfo():
        """Return external data on supported datasets.

        If an exception is raised, the program state shall be unchanged.

        Returns
        -------
        datasetToPackage : `dict`-like
            a map from dataset IDs to package names.

        Raises
        ------
        `RuntimeError`
            the config file exists, but does not contain the expected data
        """
        return Config.instance['datasets']

    @property
    def datasetRoot(self):
        """The parent directory containing everything related to the dataset (`str`, read-only).
        """
        return self._dataRootDir

    @property
    def rawLocation(self):
        """The directory containing the "raw" input data (`str`, read-only).
        """
        return os.path.join(self.datasetRoot, 'raw')

    @property
    def calibLocation(self):
        """The directory containing the calibration data (`str`, read-only).
        """
        return os.path.join(self.datasetRoot, 'calib')

    @property
    def defectLocation(self):
        """The directory containing defect files (`str`, read-only).
        """
        return self.calibLocation

    @property
    def refcatsLocation(self):
        """The directory containing external astrometric and photometric
        reference catalogs (`str`, read-only).
        """
        return os.path.join(self.datasetRoot, 'refcats')

    @property
    def templateLocation(self):
        """The directory containing the image subtraction templates (`str`, read-only).
        """
        return os.path.join(self.datasetRoot, 'templates')

    @property
    def configLocation(self):
        """The directory containing configs that can be used to process the dataset (`str`, read-only).
        """
        return os.path.join(self.datasetRoot, 'config')

    @property
    def obsPackage(self):
        """The name of the obs package associated with this dataset (`str`, read-only).
        """
        return Butler.getMapperClass(self._stubInputRepo).getPackageName()

    @property
    def camera(self):
        """The name of the camera associated with this dataset (`str`, read-only).
        """
        return Butler.getMapperClass(self._stubInputRepo).getCameraName()

    @property
    def _stubInputRepo(self):
        """The directory containing the data set's input stub (`str`, read-only).
        """
        return os.path.join(self.datasetRoot, 'repo')

    def _validatePackage(self):
        """Confirm that the dataset directory satisfies all assumptions.

        Raises
        ------
        `RuntimeError`
            the package represented by this object does not conform to the
            dataset framework

        Notes
        -----
        Requires that `self._dataRootDir` has been initialized.
        """
        if not os.path.exists(self.datasetRoot):
            raise RuntimeError('Could not find dataset at ' + self.datasetRoot)
        if not os.path.exists(self.rawLocation):
            raise RuntimeError('Dataset at ' + self.datasetRoot + 'is missing data directory')
        if not os.path.exists(self.calibLocation):
            raise RuntimeError('Dataset at ' + self.datasetRoot + 'is missing calibration directory')
        if not os.path.exists(self.defectLocation):
            raise RuntimeError('Dataset at ' + self.datasetRoot + 'is missing defect directory')
        # Template and refcat directories might not be subdirectories of self.datasetRoot
        if not os.path.exists(self.templateLocation):
            raise RuntimeError('Dataset is missing template directory at ' + self.templateLocation)
        if not os.path.exists(self.refcatsLocation):
            raise RuntimeError('Dataset is missing reference catalog directory at ' + self.refcatsLocation)
        if not os.path.exists(self._stubInputRepo):
            raise RuntimeError('Dataset at ' + self.datasetRoot + 'is missing stub repo')
        if not _isRepo(self._stubInputRepo):
            raise RuntimeError('Stub repo at ' + self._stubInputRepo + 'is missing mapper file')

    def makeCompatibleRepo(self, repoDir):
        """Set up a directory as a repository compatible with this dataset.

        If the directory already exists, any files required by the dataset will
        be added if absent; otherwise the directory will remain unchanged.

        Parameters
        ----------
        repoDir : `str`
            The directory where the output repository will be created.
        """
        if _isRepo(self.templateLocation):
            # Stub repo is not a parent because can't mix v1 and v2 repositories in parents list
            Butler(inputs=[{"root": self.templateLocation, "mode": "r"}],
                   outputs=[{"root": repoDir, "mode": "rw"}])
        else:
            Butler(inputs=[{"root": self._stubInputRepo, "mode": "r"}],
                   outputs=[{"root": repoDir, "mode": "rw"}])


def _isRepo(repoDir):
    """Test whether a directory has been set up as a repository.
    """
    return os.path.exists(os.path.join(repoDir, '_mapper')) \
        or os.path.exists(os.path.join(repoDir, 'repositoryCfg.yaml'))
