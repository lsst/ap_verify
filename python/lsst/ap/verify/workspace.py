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

from __future__ import absolute_import, division, print_function

import os
import stat

import lsst.daf.persistence as dafPersist


class Workspace(object):
    """A directory used by ``ap_verify`` to handle data.

    Any object of this class represents a plan for organizing a working
    directory. At present, constructing a Workspace does not initialize its
    repositories; for compatibility reasons, this is best deferred to
    individual tasks.

    Parameters
    ----------
    location : `str`
       The location on disk where the workspace will be set up. Will be
       created if it does not already exist.

    Raises
    ------
    `EnvironmentError`
        ``location`` is not readable or not writeable
    """

    def __init__(self, location):
        # Can't use exceptions to reliably distinguish existing directory from other cases in Python 2
        if os.path.isdir(location):
            if not os.access(location, os.R_OK | os.W_OK):
                raise IOError('Workspace % cannot be read.' % location)
        else:
            os.makedirs(location, stat.S_IRWXU | stat.S_IRGRP | stat.S_IROTH)

        self._location = location

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
    def workButler(self):
        """A Butler that can produce pipeline inputs and outputs
        (`lsst.daf.persistence.Butler`, read-only).
        """
        return self._makeButler()

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
        mapperArgs = {"calibRoot": self.calibRepo}

        inputs = [{"root": self.dataRepo, "mapperArgs": mapperArgs}]
        outputs = [{"root": self.outputRepo, "mode": "rw", "mapperArgs": mapperArgs}]

        if not os.path.samefile(self.dataRepo, self.templateRepo):
            inputs.append({'root': self.templateRepo, 'mode': 'r', 'mapperArgs': mapperArgs})

        return dafPersist.Butler(inputs=inputs, outputs=outputs)

    @property
    def analysisButler(self):
        """A Butler that can read pipeline outputs (`lsst.daf.persistence.Butler`, read-only).
        """
        return dafPersist.Butler(inputs={"root": self.outputRepo, "mode": "r"})
