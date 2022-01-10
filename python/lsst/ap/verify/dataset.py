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

import lsst.daf.butler as dafButler
import lsst.obs.base as obsBase
from lsst.utils import getPackageDir


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

        try:
            self._dataRootDir = getPackageDir(datasetId)
        except LookupError as e:
            error = f"Cannot find the {datasetId} package; is it set up?"
            raise ValueError(error) from e
        else:
            self._validatePackage()

        self._initPackage(datasetId)

    def _initPackage(self, name):
        """Prepare the package backing this ap_verify dataset.

        Parameters
        ----------
        name : `str`
           The EUPS package identifier for the desired package.
        """
        # No initialization required at present
        pass

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
    def configLocation(self):
        """The directory containing configs that can be used to process the data (`str`, read-only).
        """
        return os.path.join(self.datasetRoot, 'config')

    @property
    def pipelineLocation(self):
        """The directory containing pipelines that can be used to process the
        data in Gen 3 (`str`, read-only).
        """
        return os.path.join(self.datasetRoot, 'pipelines')

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

    def __eq__(self, other):
        """Test that two Dataset objects are equal.

        Two objects are equal iff they refer to the same ap_verify dataset.
        """
        return self.datasetRoot == other.datasetRoot

    def __repr__(self):
        """A string representation that can be used to reconstruct the dataset.
        """
        return f"Dataset({self._id!r})"

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
