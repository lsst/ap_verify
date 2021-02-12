"""Sphinx configuration file for an LSST stack package.

This configuration only affects single-package Sphinx documenation builds.
"""

from documenteer.conf.pipelinespkg import *  # noqa F403, import *


project = "ap_verify"
html_theme_options["logotext"] = project  # noqa F405, unknown name
html_title = project
html_short_title = project
doxylink = {}
