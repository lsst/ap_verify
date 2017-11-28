.. currentmodule:: lsst.ap.verify.measurements

.. _lsst.ap.verify.measurements:

###########################
lsst.ap.verify.measurements
###########################

The ``lsst.ap.verify.measurements`` package provides implementation code for metrics defined for the AP pipeline.
It exposes functions that measure all applicable metrics from task metadata or processed Butler repositories.
The set of metrics measured is deliberately kept opaque, so that ``ap_verify`` itself need not be modified every time a new metric is implemented.

.. _lsst-ap-verify-measurements-overview:

Python API reference
====================

.. automodapi:: lsst.ap.verify.measurements

