#!/usr/bin/env python

from documenteer.sphinxconfig.stackconf import build_package_configs

import lsst.ap.verify

_g = globals()
_g.update(build_package_configs(
    project_name="ap_verify",
    copyright="2017 Association of Univerities for "
              "Research in Astronomy, Inc.",
    version=lsst.ap.verify.version.__version__))
