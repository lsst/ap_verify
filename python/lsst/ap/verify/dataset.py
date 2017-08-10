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

from __future__ import absolute_import, division, print_function

import os
import shutil

from eups import Eups

from lsst.utils import getPackageDir

from lsst.ap.verify.config import Config


class Dataset(object):
    """A dataset supported by ap_verify.

    Any object of this class is guaranteed to represent a ready-for-use
    dataset, barring concurrent changes to the file system or EUPS operations.
    Constructing a Dataset does not create a compatible output repository(ies),
    which can be done by calling `makeOutputRepo`.

    Parameters
    ----------
    dataset_id : `str`
       A tag identifying the dataset.

    Raises
    ------
    `RuntimeError`:
        `dataset_id` exists, but is not correctly organized or incomplete
    `ValueError`:
        `dataset_id` is not a recognized dataset. No side effects if this
        exception is raised.
    """

    def __init__(self, dataset_id):
        try:
            dataset_package = self._getDatasetInfo()[dataset_id]
        except KeyError:
            raise ValueError('Unsupported dataset: ' + dataset_id)

        self._data_root_dir = getPackageDir(dataset_package)
        self._validate_package()

        self._init_package(dataset_package)

    def _init_package(self, name):
        """Load the package backing this dataset.

        Parameters
        ----------
        name : `str`
           The EUPS package identifier for the desired package.
        """
        Eups().setup(name)

    @staticmethod
    def get_supported_datasets():
        """The dataset IDs that can be passed to this class's constructor.

        Returns
        -------
        A set of strings of valid tags

        Raises
        ------
        `IoError`:
            if the config file does not exist or is not readable
        `RuntimeError`:
            if the config file exists, but does not contain the expected data
        """
        return Dataset._getDatasetInfo().keys()

    @staticmethod
    def _getDatasetInfo():
        """Return external data on supported datasets.

        If an exception is raised, the program state shall be unchanged.

        Returns
        -------
        A map from dataset IDs to package names.

        Raises
        ------
        `RuntimeError`:
            the config file exists, but does not contain the expected data
        """
        if not hasattr(Dataset, '_dataset_config'):
            Dataset._dataset_config = Config.instance['datasets']

        return Dataset._dataset_config

    @property
    def dataset_root(self):
        """The parent directory containing everything related to the dataset.

        Returns
        -------
        a string giving the location of the base directory
        """
        return self._data_root_dir

    @property
    def data_location(self):
        """The directory containing the "raw" input data.

        Returns
        -------
        a string giving the location of the top-level directory for telescope output files
        """
        return os.path.join(self.dataset_root, 'raw')

    @property
    def calib_location(self):
        """The directory containing the calibration data.

        Returns
        -------
        a string giving the location of the top-level directory for master calibration files
        """
        return os.path.join(self.dataset_root, 'calib')

    @property
    def defect_location(self):
        """The directory containing defect files.

        Returns
        -------
        a string giving the location of the top-level directory for defect files
        """
        return self.calib_location

    @property
    def refcat_location(self):
        """The directory containing external reference catalogs.

        Returns
        -------
        a string giving the location of the top-level directory for astrometric and photometric catalogs
        """
        return os.path.join(self.dataset_root, 'refcats')

    @property
    def template_location(self):
        """The directory containing the image subtraction templates.

        Returns
        -------
        a string giving the location of the top-level directory for precomputed templates
        """
        return os.path.join(self.dataset_root, 'templates')

    @property
    def _stub_input_repo(self):
        """The directory containing the data set's input stub.

        Returns
        -------
        a string giving the location of the stub input repo
        """
        return os.path.join(self.dataset_root, 'repo')

    def _validate_package(self):
        """Confirm that the dataset directory satisfies all assumptions.

        Requires that self._data_root_dir has been initialized.

        Raises
        ------
        `RuntimeError`:
            if any problems are found with the package
        """
        if not os.path.exists(self.dataset_root):
            raise RuntimeError('Could not find dataset at ' + self.dataset_root)
        if not os.path.exists(self.data_location):
            raise RuntimeError('Dataset at ' + self.dataset_root + 'is missing data directory')
        if not os.path.exists(self.calib_location):
            raise RuntimeError('Dataset at ' + self.dataset_root + 'is missing calibration directory')
        if not os.path.exists(self.defect_location):
            raise RuntimeError('Dataset at ' + self.dataset_root + 'is missing defect directory')
        # Template and refcat directories might not be subdirectories of self.dataset_root
        if not os.path.exists(self.template_location):
            raise RuntimeError('Dataset is missing template directory at ' + self.template_location)
        if not os.path.exists(self.refcat_location):
            raise RuntimeError('Dataset is missing reference catalog directory at ' + self.refcat_location)
        if not os.path.exists(self._stub_input_repo):
            raise RuntimeError('Dataset at ' + self.dataset_root + 'is missing stub repo')
        if not os.path.exists(os.path.join(self._stub_input_repo, '_mapper')):
            raise RuntimeError('Stub repo at ' + self._stub_input_repo + 'is missing mapper file')

    def make_output_repo(self, output_dir):
        """Set up a directory as an output repository compatible with this dataset.

        Parameters
        ----------
        output_dir: `str`
            The directory where the output repository will be created. Must be
            empty or non-existent.
        """
        if os.path.exists(output_dir):
            if not os.path.isdir(output_dir):
                raise IOError(output_dir + 'is not a directory')
            if os.listdir(output_dir):
                raise IOError(output_dir + 'is already occupied')

            # copytree does not allow empty destination directories
            shutil.rmtree(output_dir)

        shutil.copytree(self._stub_input_repo, output_dir)
