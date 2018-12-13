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

"""Interface between `ap_verify` and `ap_pipe`.

This module handles calling `ap_pipe` and converting any information
as needed.
"""

__all__ = ["ApPipeParser", "runApPipe"]

import argparse
import os

import lsst.log
import lsst.ap.pipe as apPipe


class ApPipeParser(argparse.ArgumentParser):
    """An argument parser for data needed by ``ap_pipe`` activities.

    This parser is not complete, and is designed to be passed to another parser
    using the `parent` parameter.
    """

    def __init__(self):
        # Help and documentation will be handled by main program's parser
        argparse.ArgumentParser.__init__(self, add_help=False)
        self.add_argument('--id', dest='dataId', default="",
                          help='An identifier for the data to process.')
        self.add_argument("-j", "--processes", default=1, type=int,
                          help="Number of processes to use.")


def runApPipe(workspace, parsedCmdLine):
    """Run `ap_pipe` on this object's dataset.

    Parameters
    ----------
    workspace : `lsst.ap.verify.workspace.Workspace`
        The abstract location containing input and output repositories.
    parsedCmdLine : `argparse.Namespace`
        Command-line arguments, including all arguments supported by `ApPipeParser`.

    Returns
    -------
    dataIds : `lsst.pipe.base.DataIdContainer`
        The set of complete data IDs fed into ``ap_pipe``.
    """
    log = lsst.log.Log.getLogger('ap.verify.pipeline_driver.runApPipe')

    args = [workspace.dataRepo,
            "--output", workspace.outputRepo,
            "--calib", workspace.calibRepo,
            "--template", workspace.templateRepo]
    args.extend(_getConfigArguments(workspace))
    if parsedCmdLine.dataId:
        args.extend(["--id", *parsedCmdLine.dataId.split(" ")])
    else:
        args.extend(["--id"])
    args.extend(["--processes", str(parsedCmdLine.processes)])
    args.extend(["--noExit"])

    results = apPipe.ApPipeTask.parseAndRun(args)
    log.info('Pipeline complete')

    return results.parsedCmd.id


def _getConfigArguments(workspace):
    """Return the config options for running ApPipeTask on this workspace, as
    command-line arguments.

    Parameters
    ----------
    workspace : `lsst.ap.verify.workspace.Workspace`
        A Workspace whose config directory may contain an
        `~lsst.ap.pipe.ApPipeTask` config.

    Returns
    -------
    args : `list` of `str`
        Command-line arguments calling ``--config`` or ``--configFile``,
        following the conventions of `sys.argv`.
    """
    overrideFile = apPipe.ApPipeTask._DefaultName + ".py"
    overridePath = os.path.join(workspace.configDir, overrideFile)

    args = ["--configfile", overridePath]
    # ApVerify will use the sqlite hooks for the Ppdb.
    args.extend(["--config", "ppdb.db_url=sqlite:///" + workspace.dbLocation])
    args.extend(["--config", "ppdb.isolation_level=READ_UNCOMMITTED"])

    return args
