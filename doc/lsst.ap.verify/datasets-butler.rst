.. py:currentmodule:: lsst.ap.verify

.. _ap-verify-datasets-butler:

################################
Datasets vs. Butler repositories
################################

:doc:`Datasets <datasets>` are organized using a :ref:`specific directory structure<ap-verify-datasets-structure>` instead of an :ref:`LSST Butler repository<butler>`.
This is by design:
:ref:`ingestion of observatory files into a repository<ingest>` is considered part of the pipeline system being tested by ``ap_verify``, so ``ap_verify`` must be fed uningested data as its input.
The ingestion step creates a valid repository that is then used by the rest of the pipeline.

A secondary benefit of this approach is that dataset maintainers do not need to manually ensure that the Git repository associated with a dataset remains a valid Butler repository despite changes to the dataset.
The dataset format merely requires that files be segregated into science and calibration directories, a much looser integrity constraint.

While datasets are not Butler repositories themselves, the dataset format includes a directory, :file:`repo`, that serves as a template for :ref:`repositories created by ap_verify.py <ap-verify-run-output>`.
This template helps ensure that all repositories based on the dataset will be properly set up, in particular that any observatory-specific settings will be applied.
:file:`repo` is never modified by ``ap_verify``; all repositories created by the pipeline must be located elsewhere, whether or not they are backed by the file system.
