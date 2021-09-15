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

__all__ = ["Dataset"]

import os
import warnings

from deprecated.sphinx import deprecated

import lsst.daf.persistence as dafPersistence
import lsst.daf.butler as dafButler
import lsst.obs.base as obsBase
from lsst.utils import getPackageDir

from .config import Config


class Dataset:
    """A dataset supported by ``ap_verify``.

    Any object of this class is guaranteed to represent a ready-for-use
    ap_verify dataset, barring concurrent changes to the file system or EUPS
    operations. Constructing a Dataset does not create a compatible output
    repository(ies), which can be done by calling `makeCompatibleRepo`.

    Parameters
    ----------
    datasetId : `str`
       The name of the dataset package. A tag identifying the dataset is also
       accepted, but this usage is deprecated.

    Raises
    ------
    RuntimeError
        Raised if `datasetId` exists, but is not correctly organized or incomplete
    ValueError
        Raised if `datasetId` could not be loaded.
    """

    def __init__(self, datasetId):
        self._id = datasetId
        # daf.persistence.Policy's behavior on missing keys is apparently undefined
        # test for __getattr__ *either* raising KeyError or returning None
        try:
            datasetPackage = self._getDatasetInfo()[datasetId]
            if datasetPackage is None:
                raise KeyError
            else:
                warnings.warn(f"The {datasetId} name is deprecated, and will be removed after v24.0. "
                              f"Use {datasetPackage} instead.", category=FutureWarning)
        except KeyError:
            # if datasetId not known, assume it's a package name
            datasetPackage = datasetId

        try:
            self._dataRootDir = getPackageDir(datasetPackage)
        except LookupError as e:
            error = f"Cannot find the {datasetPackage} package; is it set up?"
            raise ValueError(error) from e
        else:
            self._validatePackage()

        self._initPackage(datasetPackage)

    def _initPackage(self, name):
        """Prepare the package backing this ap_verify dataset.

        Parameters
        ----------
        name : `str`
           The EUPS package identifier for the desired package.
        """
        # No initialization required at present
        pass

    # TODO: remove in DM-29042
    @staticmethod
    @deprecated(reason="The concept of 'supported' datasets is deprecated. This "
                       "method will be removed after v24.0.", version="v22.0", category=FutureWarning)
    def getSupportedDatasets():
        """The ap_verify dataset IDs that can be passed to this class's constructor.

        Returns
        -------
        datasets : `set` of `str`
            the set of IDs that will be accepted

        Raises
        ------
        IoError
            Raised if the config file does not exist or is not readable
        RuntimeError
            Raised if the config file exists, but does not contain the expected data
        """
        return Dataset._getDatasetInfo().keys()

    # TODO: remove in DM-29042
    @staticmethod
    def _getDatasetInfo():
        """Return external data on supported ap_verify datasets.

        If an exception is raised, the program state shall be unchanged.

        Returns
        -------
        datasetToPackage : `dict`-like
            a map from dataset IDs to package names.

        Raises
        ------
        RuntimeError
            Raised if the config file exists, but does not contain the expected data
        """
        return Config.instance['datasets']

    @property
    def datasetRoot(self):
        """The parent directory containing everything related to the
        ap_verify dataset (`str`, read-only).
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
        """The directory containing configs that can be used to process the data (`str`, read-only).
        """
        return os.path.join(self.datasetRoot, 'config')

    @property
    def obsPackage(self):
        """The name of the obs package associated with this data (`str`, read-only).
        """
        return dafPersistence.Butler.getMapperClass(self._stubInputRepo).getPackageName()

    @property
    def camera(self):
        """The name of the Gen 2 camera associated with this data (`str`, read-only).
        """
        return dafPersistence.Butler.getMapperClass(self._stubInputRepo).getCameraName()

    @property
    def instrument(self):
        """The Gen 3 instrument associated with this data (`lsst.obs.base.Instrument`, read-only).
        """
        butler = dafButler.Butler(self._preloadedRepo, writeable=False)
        instruments = list(butler.registry.queryDataIds('instrument'))
        if len(instruments) != 1:
            raise RuntimeError(f"Dataset does not have exactly one instrument; got {instruments}.")
        else:
            return obsBase.Instrument.fromName(instruments[0]["instrument"], butler.registry)

    @property
    def _stubInputRepo(self):
        """The directory containing the data set's input stub (`str`, read-only).
        """
        return os.path.join(self.datasetRoot, 'repo')

    @property
    def _preloadedRepo(self):
        """The directory containing the pre-ingested Gen 3 repo (`str`, read-only).
        """
        return os.path.join(self.datasetRoot, 'preloaded')

    @property
    def _preloadedExport(self):
        """The file containing an exported registry of `_preloadedRepo` (`str`, read-only).
        """
        return os.path.join(self.configLocation, 'export.yaml')

    def _validatePackage(self):
        """Confirm that the dataset directory satisfies all assumptions.

        Raises
        ------
        RuntimeError
            Raised if the package represented by this object does not conform to the
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
        # Template and refcat directories might not be subdirectories of self.datasetRoot
        if not os.path.exists(self.templateLocation):
            raise RuntimeError('Dataset is missing template directory at ' + self.templateLocation)
        if not os.path.exists(self.refcatsLocation):
            raise RuntimeError('Dataset is missing reference catalog directory at ' + self.refcatsLocation)
        if not os.path.exists(self._stubInputRepo):
            raise RuntimeError('Dataset at ' + self.datasetRoot + 'is missing stub repo')
        if not _isRepo(self._stubInputRepo):
            raise RuntimeError('Stub repo at ' + self._stubInputRepo + 'is missing mapper file')

    def __eq__(self, other):
        """Test that two Dataset objects are equal.

        Two objects are equal iff they refer to the same ap_verify dataset.
        """
        return self.datasetRoot == other.datasetRoot

    def __repr__(self):
        """A string representation that can be used to reconstruct the dataset.
        """
        return f"Dataset({self._id!r})"

    def makeCompatibleRepo(self, repoDir, calibRepoDir):
        """Set up a directory as a Gen 2 repository compatible with this ap_verify dataset.

        If the directory already exists, any files required by the dataset will
        be added if absent; otherwise the directory will remain unchanged.

        Parameters
        ----------
        repoDir : `str`
            The directory where the output repository will be created.
        calibRepoDir : `str`
            The directory where the output calibration repository will be created.
        """
        mapperArgs = {'mapperArgs': {'calibRoot': calibRepoDir}}
        if _isRepo(self.templateLocation):
            # Stub repo is not a parent because can't mix v1 and v2 repositories in parents list
            dafPersistence.Butler(inputs=[{"root": self.templateLocation, "mode": "r"}],
                                  outputs=[{"root": repoDir, "mode": "rw", **mapperArgs}])
        else:
            dafPersistence.Butler(inputs=[{"root": self._stubInputRepo, "mode": "r"}],
                                  outputs=[{"root": repoDir, "mode": "rw", **mapperArgs}])

    def makeCompatibleRepoGen3(self, repoDir):
        """Set up a directory as a Gen 3 repository compatible with this ap_verify dataset.

        If the repository already exists, this call has no effect.

        Parameters
        ----------
        repoDir : `str`
            The directory where the output repository will be created.
        """
        # No way to tell makeRepo "create only what's missing"
        try:
            seedConfig = dafButler.Config()
            # Checksums greatly slow importing of large repositories
            seedConfig["datastore", "checksum"] = False
            repoConfig = dafButler.Butler.makeRepo(repoDir, config=seedConfig)
            butler = dafButler.Butler(repoConfig, writeable=True)
            butler.import_(directory=self._preloadedRepo, filename=self._preloadedExport,
                           transfer="auto")
        except FileExistsError:
            pass


def _isRepo(repoDir):
    """Test whether a directory has been set up as a repository.
    """
    return os.path.exists(os.path.join(repoDir, '_mapper')) \
        or os.path.exists(os.path.join(repoDir, 'repositoryCfg.yaml'))
