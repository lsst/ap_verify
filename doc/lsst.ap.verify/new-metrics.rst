.. py:currentmodule:: lsst.ap.verify

.. program:: ap_verify.py

.. _ap-verify-new-metrics:

##################################
Registering metrics with ap_verify
##################################

``ap_verify`` handles metrics through the :lsst-task:`~lsst.verify.gen2tasks.metricTask.MetricTask` framework.
Each metric has an associated :lsst-task:`~lsst.verify.gen2tasks.metricTask.MetricTask`, typically in the package associated with the metric.
For example, the code for computing ``ip_diffim.numSciSources`` can be found in the ``ip_diffim`` package, not in ``ap_verify``.

The metrics computed by ``ap_verify`` are configured through two command-line options, :option:`--image-metrics-config` and :option:`--dataset-metrics-config`.
These options each take a config file for a `~lsst.verify.gen2tasks.metricsControllerTask.MetricsControllerConfig`, the former for metrics that are computed over individual images and the latter for metrics that apply to the entire dataset.
Typically, a file configures each metric through ``config.measurers[<name>]``; see the documentation for :lsst-task:`~lsst.verify.gen2tasks.metricsControllerTask.MetricsControllerTask` for examples.

The ``ap_verify`` package provides two config files in the :file:`config/` directory, which define the image- and dataset-level configs that are run by default (for example, during CI).
These files feature complex logic to minimize code duplication and minimize the work in adding new metrics.
This complexity is not required by ``MetricsControllerTask``; a config that's just a list of assignments will also work.
