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

"""Code for measuring software performance metrics.
"""

__all__ = ["measureTotalUnassociatedDiaObjects"]

import astropy.units as u
import lsst.verify
from lsst.dax.ppdb import countUnassociatedObjects


def measureTotalUnassociatedDiaObjects(ppdb, metricName):
    """ Compute number of DIAObjects with only one association DIASource.

    Parameters
    ----------
    ppdb : `lsst.dax.ppdb.Ppdb`
        Ppdb object connected to the relevant database.
    metricName : `str`
        The fully qualified name of the metric being measured, e.g.,
        "ap_association.totalUnassociatedDiaObjects"

    Returns
    -------
    measurement : `lsst.verify.Measurement`
        a value for `metricName`, or `None`
    """
    nUnassociatedDiaObjects = countUnassociatedObjects(ppdb)

    meas = lsst.verify.Measurement(
        metricName,
        nUnassociatedDiaObjects * u.count)
    return meas
