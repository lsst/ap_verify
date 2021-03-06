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

__all__ = ["ApPipeParser", "runApPipeGen2", "runApPipeGen3"]

import argparse
import os
import re

import click.testing

import lsst.log
from lsst.utils import getPackageDir
import lsst.pipe.base as pipeBase
import lsst.obs.base as obsBase
import lsst.ctrl.mpexec.cli.pipetask
import lsst.ap.pipe as apPipe
from lsst.ap.pipe.make_apdb import makeApdb


class ApPipeParser(argparse.ArgumentParser):
    """An argument parser for data needed by ``ap_pipe`` activities.

    This parser is not complete, and is designed to be passed to another parser
    using the `parent` parameter.
    """

    def __init__(self):
        defaultPipeline = os.path.join(getPackageDir("ap_verify"), "pipelines", "ApVerify.yaml")

        # Help and documentation will be handled by main program's parser
        argparse.ArgumentParser.__init__(self, add_help=False)
        # namespace.dataIds will always be a list of 0 or more nonempty strings, regardless of inputs.
        # TODO: in Python 3.8+, action='extend' handles nargs='?' more naturally than 'append'.
        self.add_argument('--id', '-d', '--data-query', dest='dataIds',
                          action=self.AppendOptional, nargs='?', default=[],
                          help='An identifier for the data to process.')
        self.add_argument("-p", "--pipeline", default=defaultPipeline,
                          help="A custom version of the ap_verify pipeline (e.g., with different metrics).")
        self.add_argument("--db", "--db_url", default=None,
                          help="A location for the AP database, formatted as if for ApdbConfig.db_url. "
                               "Defaults to an SQLite file in the --output directory.")
        self.add_argument("--skip-pipeline", action="store_true",
                          help="Do not run the AP pipeline itself. This argument is useful "
                               "for testing metrics on a fixed data set.")

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


def runApPipeGen2(workspace, parsedCmdLine, processes=1):
    """Run `ap_pipe` on this object's dataset.

    Parameters
    ----------
    workspace : `lsst.ap.verify.workspace.WorkspaceGen2`
        The abstract location containing input and output repositories.
    parsedCmdLine : `argparse.Namespace`
        Command-line arguments, including all arguments supported by `ApPipeParser`.
    processes : `int`
        The number of processes with which to call the AP pipeline

    Returns
    -------
    apPipeReturn : `Struct`
        The `Struct` returned from `~lsst.ap.pipe.ApPipeTask.parseAndRun` with
        ``doReturnResults=False``. This object is valid even if
        `~lsst.ap.pipe.ApPipeTask` was never run.
    """
    log = lsst.log.Log.getLogger('ap.verify.pipeline_driver.runApPipeGen2')

    makeApdb(_getApdbArguments(workspace, parsedCmdLine))

    pipelineArgs = [workspace.dataRepo,
                    "--output", workspace.outputRepo,
                    "--calib", workspace.calibRepo,
                    "--template", workspace.templateRepo]
    pipelineArgs.extend(_getConfigArguments(workspace, parsedCmdLine))
    if parsedCmdLine.dataIds:
        for singleId in parsedCmdLine.dataIds:
            pipelineArgs.extend(["--id", *singleId.split(" ")])
    else:
        pipelineArgs.extend(["--id"])
    pipelineArgs.extend(["--processes", str(processes)])
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
    """
    log = lsst.log.Log.getLogger('ap.verify.pipeline_driver.runApPipeGen3')

    makeApdb(_getApdbArguments(workspace, parsedCmdLine))

    pipelineArgs = ["run",
                    "--butler-config", workspace.repo,
                    "--pipeline", parsedCmdLine.pipeline,
                    ]
    # TODO: collections should be determined exclusively by Workspace.workButler,
    # but I can't find a way to hook that up to the graph builder. So use the CLI
    # for now and revisit once DM-26239 is done.
    pipelineArgs.extend(_getCollectionArguments(workspace))
    pipelineArgs.extend(_getConfigArgumentsGen3(workspace, parsedCmdLine))
    if parsedCmdLine.dataIds:
        for singleId in parsedCmdLine.dataIds:
            pipelineArgs.extend(["--data-query", singleId])
    pipelineArgs.extend(["--processes", str(processes)])
    pipelineArgs.extend(["--register-dataset-types"])

    if not parsedCmdLine.skip_pipeline:
        # CliRunner is an unsafe workaround for DM-26239
        runner = click.testing.CliRunner()
        # TODO: generalize this code in DM-26028
        # TODO: work off of workspace.workButler after DM-26239
        results = runner.invoke(lsst.ctrl.mpexec.cli.pipetask.cli, pipelineArgs)
        if results.exception:
            raise RuntimeError("Pipeline failed.") from results.exception

        log.info('Pipeline complete.')
        return results.exit_code
    else:
        log.info('Skipping AP pipeline entirely.')


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


def _getConfigArguments(workspace, parsed):
    """Return the config options for running ApPipeTask on this workspace, as
    command-line arguments.

    Parameters
    ----------
    workspace : `lsst.ap.verify.workspace.WorkspaceGen2`
        A Workspace whose config directory may contain an
        `~lsst.ap.pipe.ApPipeTask` config.
    parsed : `argparse.Namespace`
        Command-line arguments, including all arguments supported by `ApPipeParser`.

    Returns
    -------
    args : `list` of `str`
        Command-line arguments calling ``--config`` or ``--configfile``,
        following the conventions of `sys.argv`.
    """
    overrideFile = apPipe.ApPipeTask._DefaultName + ".py"
    overridePath = os.path.join(workspace.configDir, overrideFile)

    args = ["--configfile", overridePath]
    # Translate APDB-only arguments to work as a sub-config
    args.extend([("diaPipe.apdb." + arg if arg != "--config" else arg)
                 for arg in _getApdbArguments(workspace, parsed)])
    # Put output alerts into the workspace.
    args.extend(["--config", "diaPipe.alertPackager.alertWriteLocation=" + workspace.alertLocation])
    args.extend(["--config", "diaPipe.doPackageAlerts=True"])

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
        "--config", "diaPipe:doPackageAlerts=True",
        # TODO: the configs below should not be needed after DM-26140
        "--config-file", "calibrate:" + os.path.join(workspace.configDir, "calibrate.py"),
        "--config-file", "imageDifference:" + os.path.join(workspace.configDir, "imageDifference.py"),
    ])
    # TODO: reverse-engineering the instrument should not be needed after DM-26140
    # pipetask will crash if there is more than one instrument
    for idRecord in workspace.workButler.registry.queryDataIds("instrument").expanded():
        className = idRecord.records["instrument"].class_name
        args.extend(["--instrument", className])

    return args


def _getCollectionArguments(workspace):
    """Return the collections for running the Gen 3 AP Pipeline on this
    workspace, as command-line arguments.

    Parameters
    ----------
    workspace : `lsst.ap.verify.workspace.WorkspaceGen3`
        A Workspace with a Gen 3 repository.

    Returns
    -------
    args : `list` of `str`
        Command-line arguments calling ``--input`` or ``--output``,
        following the conventions of `sys.argv`.
    """
    butler = workspace.workButler
    # Hard-code the collection names because it's hard to infer the inputs from the Butler
    inputs = {"skymaps", "refcats"}
    for dimension in butler.registry.queryDataIds('instrument'):
        instrument = obsBase.Instrument.fromName(dimension["instrument"], butler.registry)
        inputs.add(instrument.makeDefaultRawIngestRunName())
        inputs.add(instrument.makeCalibrationCollectionName())
    inputs.update(butler.registry.queryCollections(re.compile(r"templates/\w+")))

    return ["--input", ",".join(inputs),
            "--output-run", workspace.runName,
            ]
