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

__all__ = ["ApPipeParser", "runApPipeGen3"]

import argparse
import os
import re
import subprocess
import logging

import lsst.ctrl.mpexec.execFixupDataId  # not part of lsst.ctrl.mpexec
import lsst.ctrl.mpexec.cli.pipetask
from lsst.ap.pipe.make_apdb import makeApdb

_LOG = logging.getLogger(__name__)


class ApPipeParser(argparse.ArgumentParser):
    """An argument parser for data needed by ``ap_pipe`` activities.

    This parser is not complete, and is designed to be passed to another parser
    using the `parent` parameter.
    """

    def __init__(self):
        # Help and documentation will be handled by main program's parser
        argparse.ArgumentParser.__init__(self, add_help=False)
        # namespace.dataIds will always be a list of 0 or more nonempty strings, regardless of inputs.
        # TODO: in Python 3.8+, action='extend' handles nargs='?' more naturally than 'append'.
        self.add_argument('--id', '-d', '--data-query', dest='dataIds',
                          action=self.AppendOptional, nargs='?', default=[],
                          help='An identifier for the data to process.')
        self.add_argument("-p", "--pipeline", default=None,
                          help="A custom version of the ap_verify pipeline (e.g., with different metrics). "
                               "Defaults to the ApVerify.yaml within --dataset.")
        self.add_argument("--db", "--db_url", default=None,
                          help="A location for the AP database, formatted as if for ApdbConfig.db_url. "
                               "Defaults to an SQLite file in the --output directory.")
        self.add_argument("--skip-pipeline", action="store_true",
                          help="Do not run the AP pipeline itself. This argument is useful "
                               "for testing metrics on a fixed data set.")
        self.add_argument("--clean-run", action="store_true",
                          help="Run the pipeline with a new run collection, "
                               "even if one already exists.")

    class AppendOptional(argparse.Action):
        """A variant of the built-in "append" action that ignores None values
        instead of appending them.
        """
        # This class can't safely inherit from the built-in "append" action
        # because there is no public class that implements it.
        def __call__(self, parser, namespace, values, option_string=None):
            if values is not None:
                try:
                    allValues = getattr(namespace, self.dest)
                    allValues.append(values)
                except AttributeError:
                    setattr(namespace, self.dest, [values])


def runApPipeGen3(workspace, parsedCmdLine, processes=1):
    """Run `ap_pipe` on this object's dataset.

    Parameters
    ----------
    workspace : `lsst.ap.verify.workspace.WorkspaceGen3`
        The abstract location containing input and output repositories.
    parsedCmdLine : `argparse.Namespace`
        Command-line arguments, including all arguments supported by `ApPipeParser`.
    processes : `int`
        The number of processes with which to call the AP pipeline

    Returns
    -------
    code : `int`
        An error code that is zero if the pipeline ran without problems, or
        nonzero if there were errors. The exact meaning of nonzereo values
        is an implementation detail.
    """
    log = _LOG.getChild('runApPipeGen3')

    makeApdb(_getApdbArguments(workspace, parsedCmdLine))

    pipelineFile = _getPipelineFile(workspace, parsedCmdLine)
    pipelineArgs = ["pipetask", "run",
                    "--butler-config", workspace.repo,
                    "--pipeline", pipelineFile,
                    ]
    # TODO: workaround for inability to generate crosstalk sources in main
    # processing pipeline (DM-31492).
    instruments = {id["instrument"] for id in workspace.workButler.registry.queryDataIds("instrument")}
    if "DECam" in instruments:
        crosstalkPipeline = "${AP_PIPE_DIR}/pipelines/DarkEnergyCamera/RunIsrForCrosstalkSources.yaml"
        crosstalkArgs = ["pipetask", "run",
                         "--butler-config", workspace.repo,
                         "--pipeline", crosstalkPipeline,
                         ]
        crosstalkArgs.extend(_getCollectionArguments(workspace, reuse=(not parsedCmdLine.clean_run)))
        if parsedCmdLine.dataIds:
            for singleId in parsedCmdLine.dataIds:
                crosstalkArgs.extend(["--data-query", singleId])
        crosstalkArgs.extend(["--processes", str(processes)])
        crosstalkArgs.extend(["--register-dataset-types"])
        subprocess.run(crosstalkArgs, capture_output=False, shell=False, check=False)

        # Force same output run for crosstalk and main processing.
        pipelineArgs.extend(_getCollectionArguments(workspace, reuse=True))
    else:
        # TODO: collections should be determined exclusively by Workspace.workButler,
        # but I can't find a way to hook that up to the graph builder. So use the CLI
        # for now and revisit once DM-26239 is done.
        pipelineArgs.extend(_getCollectionArguments(workspace, reuse=(not parsedCmdLine.clean_run)))

    pipelineArgs.extend(_getConfigArgumentsGen3(workspace, parsedCmdLine))
    if parsedCmdLine.dataIds:
        for singleId in parsedCmdLine.dataIds:
            pipelineArgs.extend(["--data-query", singleId])
    pipelineArgs.extend(["--processes", str(processes)])
    pipelineArgs.extend(["--register-dataset-types"])
    pipelineArgs.extend(["--graph-fixup", "lsst.ap.verify.pipeline_driver._getExecOrder"])

    if not parsedCmdLine.skip_pipeline:
        # subprocess is an unsafe workaround for DM-26239
        # TODO: generalize this code in DM-26028
        # TODO: work off of workspace.workButler after DM-26239
        results = subprocess.run(pipelineArgs, capture_output=False, shell=False, check=False)
        log.info('Pipeline complete.')
        return results.returncode
    else:
        log.info('Skipping AP pipeline entirely.')


def _getExecOrder():
    """Return any constraints on the Gen 3 execution order.

    The current constraints are that executions of DiaPipelineTask must be
    ordered by visit ID, but this is subject to change.

    Returns
    -------
    order : `lsst.ctrl.mpexec.ExecutionGraphFixup`
        An object encoding the desired execution order as an algorithm for
        modifying inter-quantum dependencies.

    Notes
    -----
    This function must be importable, but need not be public.
    """
    # Source association algorithm is not time-symmetric. Force execution of
    # association (through DiaPipelineTask) in order of ascending visit number.
    return lsst.ctrl.mpexec.execFixupDataId.ExecFixupDataId(
        taskLabel="diaPipe", dimensions=["visit", ], reverse=False)


def _getPipelineFile(workspace, parsed):
    """Return the config options for running make_apdb.py on this workspace,
    as command-line arguments.

    Parameters
    ----------
    workspace : `lsst.ap.verify.workspace.Workspace`
        A Workspace whose pipeline directory may contain an ApVerify pipeline.
    parsed : `argparse.Namespace`
        Command-line arguments, including all arguments supported by `ApPipeParser`.

    Returns
    -------
    pipeline : `str`
        The location of the pipeline file to use for running ap_verify.
    """
    if parsed.pipeline:
        return parsed.pipeline
    else:
        customPipeline = os.path.join(workspace.pipelineDir, "ApVerify.yaml")
        if os.path.exists(customPipeline):
            return customPipeline
        else:
            return os.path.join("${AP_VERIFY_DIR}", "pipelines", "ApVerify.yaml")


def _getApdbArguments(workspace, parsed):
    """Return the config options for running make_apdb.py on this workspace,
    as command-line arguments.

    Parameters
    ----------
    workspace : `lsst.ap.verify.workspace.Workspace`
        A Workspace whose config directory may contain an
        `~lsst.ap.pipe.ApPipeTask` config.
    parsed : `argparse.Namespace`
        Command-line arguments, including all arguments supported by `ApPipeParser`.

    Returns
    -------
    args : `list` of `str`
        Command-line arguments calling ``--config`` or ``--config-file``,
        following the conventions of `sys.argv`.
    """
    if not parsed.db:
        parsed.db = "sqlite:///" + workspace.dbLocation

    args = ["--config", "db_url=" + parsed.db]
    # Same special-case check as ApdbConfig.validate()
    if parsed.db.startswith("sqlite"):
        args.extend(["--config", "isolation_level=READ_UNCOMMITTED"])

    return args


def _getConfigArgumentsGen3(workspace, parsed):
    """Return the config options for running the Gen 3 AP Pipeline on this
    workspace, as command-line arguments.

    Parameters
    ----------
    workspace : `lsst.ap.verify.workspace.WorkspaceGen3`
        A Workspace whose config directory may contain various configs.
    parsed : `argparse.Namespace`
        Command-line arguments, including all arguments supported by `ApPipeParser`.

    Returns
    -------
    args : `list` of `str`
        Command-line arguments calling ``--config`` or ``--config-file``,
        following the conventions of `sys.argv`.
    """
    # Translate APDB-only arguments to work as a sub-config
    args = [("diaPipe:apdb." + arg if arg != "--config" else arg)
            for arg in _getApdbArguments(workspace, parsed)]
    args.extend([
        # Put output alerts into the workspace.
        "--config", "diaPipe:alertPackager.alertWriteLocation=" + workspace.alertLocation,
    ])
    return args


def _getCollectionArguments(workspace, reuse):
    """Return the collections for running the Gen 3 AP Pipeline on this
    workspace, as command-line arguments.

    Parameters
    ----------
    workspace : `lsst.ap.verify.workspace.WorkspaceGen3`
        A Workspace with a Gen 3 repository.
    reuse : `bool`
        If true, use the previous run collection if one exists. Otherwise,
        create a new run.

    Returns
    -------
    args : `list` of `str`
        Command-line arguments calling ``--input`` or ``--output``,
        following the conventions of `sys.argv`.
    """
    # workspace.outputName is a chained collection containing all inputs
    args = ["--output", workspace.outputName,
            "--clobber-outputs",
            ]

    registry = workspace.workButler.registry
    # Should refresh registry to see crosstalk run from DM-31492, but this
    # currently leads to a bug involving --skip-existing. The only downside of
    # the cached registry is that, with two runs for DECam datasets, a rerun of
    # ap_verify will re-run crosstalk sources in the second run. Using
    # skip-existing-in would work around that, but would lead to a worse bug in
    # the case that the user is alternating runs with and without --clean-run.
    # registry.refresh()
    oldRuns = list(registry.queryCollections(re.compile(workspace.outputName + r"/\d+T\d+Z")))
    if reuse and oldRuns:
        args.extend(["--extend-run", "--skip-existing"])
    return args
