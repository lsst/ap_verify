.. py:currentmodule:: lsst.ap.verify

.. program:: ap_verify.py

.. _ap-verify-new-metrics:

#################################
Configuring metrics for ap_verify
#################################

``ap_verify`` handles metrics through the :lsst-task:`~lsst.verify.tasks.metricTask.MetricTask` framework.
Each metric has an associated :lsst-task:`~lsst.verify.tasks.metricTask.MetricTask`, typically in the package associated with the metric.
For example, the code for computing ``ip_diffim.numSciSources`` can be found in the ``ip_diffim`` package, not in ``ap_verify``.

The metrics computed by ``ap_verify`` are configured as part of the pipeline.
The pipeline can be overridden using the :option:`--pipeline` command-line option.

The ``ap_verify`` package provides a default-instrumented pipeline in :file:`pipelines/ApVerify.yaml`.
To make it easy to mix and match metrics, all :lsst-task:`~lsst.verify.tasks.metricTask.MetricTask` configuration is done in separate sub-pipelines that are then included in :file:`ApVerify.yaml`.

Further reading
===============

- :doc:`running`
- :lsst-task:`lsst.verify.tasks.MetricTask`
