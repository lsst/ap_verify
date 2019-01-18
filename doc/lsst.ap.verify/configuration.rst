.. py:currentmodule:: lsst.ap.verify

.. _ap-verify-configuration:

######################################
ap_verify configuration file reference
######################################

This page describes the file-based configuration options used by ``ap_verify``.
Most users should not need to adjust these settings, but they allow capabilities such as registering new :doc:`datasets<datasets>`.

The ``ap_verify`` configuration file is located at :file:`config/dataset_config.yaml`.
It consists of a list of dictionaries, each representing specific aspects of the program.

.. _ap-verify-configuration-dataset:

datasets
========

The ``datasets`` dictionary maps dataset names (which must be provided on the :command:`ap_verify.py` command line) to GitHub repository names.
Adding a dataset to the config is necessary for ``ap_verify`` to recognize it; in practice, the entry will be made once by the dataset author and then committed.
A dataset must still be :doc:`installed<datasets-install>` on the machine before it can be used.
