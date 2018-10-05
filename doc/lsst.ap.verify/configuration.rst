.. py:currentmodule:: lsst.ap.verify

.. _ap-verify-configuration:

######################################
ap_verify configuration file reference
######################################

This page describes the file-based configuration options used by ``ap_verify``.
Most users should not need to adjust these settings, but they allow capabilities such as registering new :doc:`datasets<datasets>`.

.. TODO: more generic name? or split up file? (DM-12850)

The ``ap_verify`` configuration file is located at :file:`config/dataset_config.yaml`.
It consists of a list of dictionaries, each representing specific aspects of the program.

.. _ap-verify-configuration-dataset:

datasets
========

The ``datasets`` dictionary maps dataset names (which must be provided on the ``ap_verify`` command line) to GitHub repository names.
Adding a dataset to the config is necessary for ``ap_verify`` to recognize it; in practice, the entry will be made once by the dataset author and then committed.
A dataset must still be :doc:`installed<datasets-install>` on the machine before it can be used.

.. _ap-verify-configuration-measurements:

measurements
============

.. warning::

   The metrics being used by ``ap_verify`` are still being defined.
   The syntax used to register them will likely change, and may be moved to a dedicated package entirely.
   This section of the configuration file should be treated as preliminary and subject to change.

The ``measurements`` dictionary contains sub-dictionaries for each kind of metric.
Currently there is only one:

``timing``
    A dictionary from tasks to the metrics that time them.
    Subtasks must be identified by the name the parent task assigns them, and should be prefixed by the parent task name (as in "imageDifference:detection") to avoid ambiguity.
    Metrics must use the full name following the convention of `lsst.verify.metrics`, as in "meas_algorithms.SourceDetectionTime".
