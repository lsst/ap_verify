.. py:currentmodule:: lsst.ap.verify

.. program:: ap_verify.py

.. _ap-verify-configuration:

######################################
ap_verify configuration file reference
######################################

This page describes the file-based configuration options used by ``ap_verify``.
It does *not* describe the configuration of ``MetricTask``\ s for ``ap_verify``; see :doc:`new-metrics` instead.

The ``ap_verify`` configuration file is located at :file:`config/dataset_config.yaml`.
It consists of a list of dictionaries, each representing specific aspects of the program.
Most users should not need to adjust these settings, but they allow capabilities such as registering new :doc:`datasets<datasets>`.

.. _ap-verify-configuration-dataset:

datasets
========

The ``datasets`` dictionary maps dataset names (which must be provided through :option:`ap_verify.py --dataset`) to GitHub repository names.
Adding a dataset to the config is necessary for ``ap_verify`` to recognize it; in practice, the entry will be made once by the dataset author and then committed.
A dataset must still be :doc:`installed<datasets-install>` on the machine before it can be used.

Further reading
===============

- :doc:`datasets`
