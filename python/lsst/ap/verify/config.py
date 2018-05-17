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

from lsst.daf.persistence import Policy


class Config:
    """Confuration manager for ``ap_verify``.

    This is a singleton `lsst.daf.persistence.Policy` that may be accessed
    from other modules in ``ap_verify`` as needed using `Config.instance`.
    Please do not construct objects of this class directly.

    Objects of this type are immutable.
    """

    def __init__(self):
        path = Policy.defaultPolicyFile('ap_verify', 'dataset_config.yaml', 'config')
        self._allInfo = Policy(path)
        self._validate()

    def _validate(self):
        """Test that the loaded configuration is correct.

        Raises
        ------
        `RuntimeError`
            Validation failed
        """
        try:
            datasetMap = self._allInfo['datasets']
            if not isinstance(datasetMap, Policy):
                raise TypeError('`datasets` is not a dictionary')
        except (KeyError, TypeError) as e:
            raise RuntimeError('Invalid config file.') from e

        try:
            measurementMap = self._allInfo['measurements']
            if not isinstance(measurementMap, Policy):
                raise TypeError('`measurements` is not a dictionary')
            timingMap = measurementMap['timing']
            if not isinstance(timingMap, Policy):
                raise TypeError('`measurements.timing` is not a dictionary')
        except (KeyError, TypeError) as e:
            raise RuntimeError('Invalid config file.') from e

    def __getitem__(self, key):
        return self._allInfo[key]

    def __contains__(self, key):
        return key in self._allInfo


Config.instance = Config()
"""The sole `Config` object used by the program.
"""
