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
import lsst.pipe.base as pipeBase
import lsst.ap.pipe as apPipe
from lsst.ap.pipe.make_apdb import makeApdb


class ApPipeParser(argparse.ArgumentParser):
    """An argument parser for data needed by ``ap_pipe`` activities.

    This parser is not complete, and is designed to be passed to another parser
    using the `parent` parameter.
    """

    def __init__(self):
        # Help and documentation will be handled by main program's parser
        argparse.ArgumentParser.__init__(self, add_help=False)
        self.add_argument('--id', dest='dataIds', action='append', default=[],
                          help='An identifier for the data to process.')
        self.add_argument("-j", "--processes", default=1, type=int,
                          help="Number of processes to use.")
        self.add_argument("--skip-pipeline", action="store_true",
                          help="Do not run the AP pipeline itself. This argument is useful "
                               "for testing metrics on a fixed data set.")


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
    apPipeReturn : `Struct`
        The `Struct` returned from `~lsst.ap.pipe.ApPipeTask.parseAndRun` with
        ``doReturnResults=False``. This object is valid even if
        `~lsst.ap.pipe.ApPipeTask` was never run.
    """
    log = lsst.log.Log.getLogger('ap.verify.pipeline_driver.runApPipe')

    configArgs = _getConfigArguments(workspace)
    makeApdb(configArgs)

    pipelineArgs = [workspace.dataRepo,
                    "--output", workspace.outputRepo,
                    "--calib", workspace.calibRepo,
                    "--template", workspace.templateRepo]
    pipelineArgs.extend(configArgs)
    if parsedCmdLine.dataIds:
        for singleId in parsedCmdLine.dataIds:
            pipelineArgs.extend(["--id", *singleId.split(" ")])
    else:
        pipelineArgs.extend(["--id"])
    pipelineArgs.extend(["--processes", str(parsedCmdLine.processes)])
    pipelineArgs.extend(["--noExit"])

    if not parsedCmdLine.skip_pipeline:
        results = apPipe.ApPipeTask.parseAndRun(pipelineArgs)
        log.info('Pipeline complete')
    else:
        log.info('Skipping AP pipeline entirely.')
        apPipeParser = apPipe.ApPipeTask._makeArgumentParser()
        apPipeParsed = apPipeParser.parse_args(config=apPipe.ApPipeTask.ConfigClass(), args=pipelineArgs)
        results = pipeBase.Struct(
            argumentParser=apPipeParser,
            parsedCmd=apPipeParsed,
            taskRunner=apPipe.ApPipeTask.RunnerClass(TaskClass=apPipe.ApPipeTask, parsedCmd=apPipeParsed),
            resultList=[],
        )

    return results


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
    # ApVerify will use the sqlite hooks for the Apdb.
    args.extend(["--config", "diaPipe.apdb.db_url=sqlite:///" + workspace.dbLocation])
    args.extend(["--config", "diaPipe.apdb.isolation_level=READ_UNCOMMITTED"])
    # Put output alerts into the workspace.
    args.extend(["--config", "diaPipe.alertPackager.alertWriteLocation=" + workspace.workDir + "/alerts"])

    return args
