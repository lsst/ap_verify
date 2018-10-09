.. py:currentmodule:: lsst.ap.verify

.. _ap-verify-new-metrics:

###############################
Adding new metrics to ap_verify
###############################

The ``lsst.ap.verify.measurements`` module provides implementation code for metrics defined for the AP pipeline.
It exposes functions that measure all applicable metrics from task metadata or processed Butler repositories.
The set of metrics measured is deliberately kept opaque, so that ``ap_verify.py`` itself need not be modified every time a new metric is implemented.
