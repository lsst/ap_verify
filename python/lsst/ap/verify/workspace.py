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

import abc
import os
import pathlib
import re
import stat

import lsst.skymap
import lsst.daf.persistence as dafPersist
import lsst.daf.butler as dafButler
import lsst.obs.base as obsBase


class Workspace(metaclass=abc.ABCMeta):
    """A directory used by ``ap_verify`` to handle data and outputs.

    Any object of this class represents a working directory containing
    (possibly empty) subdirectories for various purposes. Subclasses are
    typically specialized for particular workflows. Keeping such details in
    separate classes makes it easier to provide guarantees without forcing
    awkward directory structures on users.

    All Workspace classes must guarantee the existence of any subdirectories
    inside the workspace. Directories corresponding to repositories do not need
    to be initialized, since creating a valid repository usually requires
    external information.

    Parameters
    ----------
    location : `str`
       The location on disk where the workspace will be set up. Will be
       created if it does not already exist.

    Raises
    ------
    EnvironmentError
        Raised if ``location`` is not readable or not writeable
    """
    def __init__(self, location):
        # Properties must be `str` for backwards compatibility
        self._location = str(pathlib.Path(location).resolve())

        self.mkdir(self._location)
        self.mkdir(self.configDir)

    @staticmethod
    def mkdir(directory):
        """Create a directory for the workspace.

        This method is intended to be called only by subclasses, and should
        not be used by external code.

        Parameters
        ----------
        directory : `str`
            The directory to create.
        """
        mode = stat.S_IRWXU | stat.S_IRGRP | stat.S_IROTH  # a+r, u+rwx
        pathlib.Path(directory).mkdir(parents=True, exist_ok=True, mode=mode)

    def __eq__(self, other):
        """Test whether two workspaces are of the same type and have the
        same location.
        """
        return type(self) == type(other) and self.workDir == other.workDir

    def __repr__(self):
        """A string representation that can be used to reconstruct the Workspace.
        """
        return f"{type(self).__name__}({self.workDir!r})"

    @property
    def workDir(self):
        """The absolute location of the workspace as a whole
        (`str`, read-only).
        """
        return self._location

    @property
    def configDir(self):
        """The absolute location of a directory containing custom Task config
        files for use with the data (`str`, read-only).
        """
        return os.path.join(self._location, 'config')

    @property
    @abc.abstractmethod
    def dbLocation(self):
        """The default absolute location of the source association database to
        be created or updated by the pipeline (`str`, read-only).

        Shall be a pathname to a database suitable for the backend of `Apdb`.
        """

    @property
    @abc.abstractmethod
    def alertLocation(self):
        """The absolute location of an output directory for persisted
        alert packets (`str`, read-only).
        """

    @property
    @abc.abstractmethod
    def workButler(self):
        """A Butler that can produce pipeline inputs and outputs (read-only).
        The type is class-dependent.
        """

    @property
    @abc.abstractmethod
    def analysisButler(self):
        """A Butler that can read pipeline outputs (read-only).
        The type is class-dependent.

        The Butler should be read-only, if its type supports the restriction.
        """


class WorkspaceGen2(Workspace):
    """A directory used by ``ap_verify`` to handle data.

    Any object of this class represents a working directory containing
    (possibly empty) subdirectories for repositories. Constructing a
    WorkspaceGen2 does not *initialize* its repositories, as this requires
    external information.

    Parameters
    ----------
    location : `str`
       The location on disk where the workspace will be set up. Will be
       created if it does not already exist.

    Raises
    ------
    EnvironmentError
        Raised if ``location`` is not readable or not writeable
    """

    def __init__(self, location):
        super().__init__(location)

        self.mkdir(self.dataRepo)
        self.mkdir(self.calibRepo)
        self.mkdir(self.templateRepo)
        self.mkdir(self.outputRepo)

        # Lazy evaluation to optimize butlers
        self._workButler = None
        self._analysisButler = None

    @property
    def dataRepo(self):
        """The absolute path/URI to a Butler repo for science data
        (`str`, read-only).
        """
        return os.path.join(self._location, 'ingested')

    @property
    def calibRepo(self):
        """The absolute path/URI to a Butler repo for calibration data
        (`str`, read-only).
        """
        return os.path.join(self._location, 'calibingested')

    @property
    def templateRepo(self):
        """The absolute path/URI to a Butler repo for precomputed templates
        (`str`, read-only).
        """
        return self.dataRepo

    @property
    def outputRepo(self):
        """The absolute path/URI to a Butler repo for AP pipeline products
        (`str`, read-only).
        """
        return os.path.join(self._location, 'output')

    @property
    def dbLocation(self):
        return os.path.join(self._location, 'association.db')

    @property
    def alertLocation(self):
        return os.path.join(self._location, 'alerts')

    @property
    def workButler(self):
        """A Butler that can produce pipeline inputs and outputs
        (`lsst.daf.persistence.Butler`, read-only).
        """
        if self._workButler is None:
            self._workButler = self._makeButler()
        return self._workButler

    def _makeButler(self):
        """Create a butler for accessing the entire workspace.

        Returns
        -------
        butler : `lsst.daf.persistence.Butler`
            A butler accepting `dataRepo`, `calibRepo`, and `templateRepo` as
            inputs, and `outputRepo` as an output.

        Notes
        -----
        Assumes all `*Repo` properties have been initialized.
        """
        # common arguments for butler elements
        mapperArgs = {"calibRoot": os.path.abspath(self.calibRepo)}

        inputs = [{"root": self.dataRepo, "mapperArgs": mapperArgs}]
        outputs = [{"root": self.outputRepo, "mode": "rw", "mapperArgs": mapperArgs}]

        if not os.path.samefile(self.dataRepo, self.templateRepo):
            inputs.append({'root': self.templateRepo, 'mode': 'r', 'mapperArgs': mapperArgs})

        return dafPersist.Butler(inputs=inputs, outputs=outputs)

    @property
    def analysisButler(self):
        """A Butler that can read pipeline outputs (`lsst.daf.persistence.Butler`, read-only).
        """
        if self._analysisButler is None:
            self._analysisButler = dafPersist.Butler(inputs={"root": self.outputRepo, "mode": "r"})
        return self._analysisButler


class WorkspaceGen3(Workspace):
    """A directory used by ``ap_verify`` to handle data.

    Any object of this class represents a working directory containing
    subdirectories for a repository and for non-repository files. Constructing
    a WorkspaceGen3 does not *initialize* its repository, as this requires
    external information.

    Parameters
    ----------
    location : `str`
       The location on disk where the workspace will be set up. Will be
       created if it does not already exist.

    Raises
    ------
    EnvironmentError
        Raised if ``location`` is not readable or not writeable
    """

    def __init__(self, location):
        super().__init__(location)

        self.mkdir(self.repo)

        # Gen 3 name of the output
        self.outputName = "ap_verify-output"

        # Lazy evaluation to optimize butlers
        self._workButler = None
        self._analysisButler = None

    @property
    def repo(self):
        """The absolute path/URI to a Butler repo for AP pipeline processing
        (`str`, read-only).
        """
        return os.path.join(self._location, 'repo')

    @property
    def dbLocation(self):
        return os.path.join(self._location, 'association.db')

    @property
    def alertLocation(self):
        return os.path.join(self._location, 'alerts')

    def _ensureCollection(self, registry, name, collectionType):
        """Add a collection to a repository if it does not already exist.

        Parameters
        ----------
        registry : `lsst.daf.butler.Registry`
            The repository to which to add the collection.
        name : `str`
            The name of the collection to test for and add.
        collectionType : `lsst.daf.butler.CollectionType`
            The type of collection to add. This field is ignored when
            testing if a collection exists.
        """
        matchingCollections = list(registry.queryCollections(re.compile(name)))
        if not matchingCollections:
            registry.registerCollection(name, type=collectionType)

    @property
    def workButler(self):
        """A Butler that can read and write to a Gen 3 repository (`lsst.daf.butler.Butler`, read-only).

        Notes
        -----
        Assumes `repo` has been initialized.
        """
        if self._workButler is None:
            try:
                # Hard-code the collection names because it's hard to infer the inputs from the Butler
                queryButler = dafButler.Butler(self.repo, writeable=True)  # writeable for _workButler
                inputs = {
                    lsst.skymap.BaseSkyMap.SKYMAP_RUN_COLLECTION_NAME,
                }
                for dimension in queryButler.registry.queryDataIds('instrument'):
                    instrument = obsBase.Instrument.fromName(dimension["instrument"], queryButler.registry)
                    rawName = instrument.makeDefaultRawIngestRunName()
                    inputs.add(rawName)
                    self._ensureCollection(queryButler.registry, rawName, dafButler.CollectionType.RUN)
                    inputs.add(instrument.makeCalibrationCollectionName())
                    inputs.add(instrument.makeRefCatCollectionName())
                inputs.update(queryButler.registry.queryCollections(re.compile(r"templates/\w+")))

                # Create an output chain here, so that workButler can see it.
                # Definition does not conflict with what pipetask --output uses.
                # Regex is workaround for DM-25945.
                if not list(queryButler.registry.queryCollections(re.compile(self.outputName))):
                    queryButler.registry.registerCollection(self.outputName,
                                                            dafButler.CollectionType.CHAINED)
                    queryButler.registry.setCollectionChain(self.outputName, inputs)

                self._workButler = dafButler.Butler(butler=queryButler, collections=self.outputName)
            except OSError as e:
                raise RuntimeError(f"{self.repo} is not a Gen 3 repository") from e
        return self._workButler

    @property
    def analysisButler(self):
        """A Butler that can read from a Gen 3 repository with outputs (`lsst.daf.butler.Butler`, read-only).

        Notes
        -----
        Assumes `repo` has been initialized.
        """
        if self._analysisButler is None:
            try:
                self._analysisButler = dafButler.Butler(self.repo, collections=self.outputName,
                                                        writeable=False)
            except OSError as e:
                raise RuntimeError(f"{self.repo} is not a Gen 3 repository") from e
        return self._analysisButler
