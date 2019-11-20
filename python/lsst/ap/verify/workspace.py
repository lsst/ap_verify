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
import pathlib
import stat

import lsst.daf.persistence as dafPersist


class Workspace:
    """A directory used by ``ap_verify`` to handle data.

    Any object of this class represents a working directory containing
    (possibly empty) subdirectories for repositories. At present, constructing
    a Workspace does not *initialize* its repositories; for compatibility
    reasons, this is best deferred to individual tasks.

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
        self._location = location

        mode = stat.S_IRWXU | stat.S_IRGRP | stat.S_IROTH  # a+r, u+rwx
        kwargs = {"parents": True, "exist_ok": True, "mode": mode}
        pathlib.Path(self._location).mkdir(**kwargs)
        pathlib.Path(self.configDir).mkdir(**kwargs)
        pathlib.Path(self.dataRepo).mkdir(**kwargs)
        pathlib.Path(self.calibRepo).mkdir(**kwargs)
        pathlib.Path(self.templateRepo).mkdir(**kwargs)
        pathlib.Path(self.outputRepo).mkdir(**kwargs)

        # Lazy evaluation to optimize workButler and analysisButler
        self._workButler = None
        self._analysisButler = None

    @property
    def workDir(self):
        """The location of the workspace as a whole (`str`, read-only).
        """
        return self._location

    @property
    def configDir(self):
        """The location of a directory containing custom Task config files for
        use with the data (`str`, read-only).
        """
        return os.path.join(self._location, 'config')

    @property
    def dataRepo(self):
        """The URI to a Butler repo for science data (`str`, read-only).
        """
        return os.path.join(self._location, 'ingested')

    @property
    def calibRepo(self):
        """The URI to a Butler repo for calibration data (`str`, read-only).
        """
        return os.path.join(self._location, 'calibingested')

    @property
    def templateRepo(self):
        """The URI to a Butler repo for precomputed templates (`str`, read-only).
        """
        return self.dataRepo

    @property
    def outputRepo(self):
        """The URI to a Butler repo for AP pipeline products (`str`, read-only).
        """
        return os.path.join(self._location, 'output')

    @property
    def dbLocation(self):
        """The default location of the source association database to be
        created or updated by the pipeline (`str`, read-only).

        Shall be a filename to a database file suitable
        for the sqlite backend of `Apdb`.
        """
        return os.path.join(self._location, 'association.db')

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
