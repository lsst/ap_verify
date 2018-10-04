.. py:currentmodule:: lsst.ap.verify

.. _ap-verify-running:

#######################################
Running ap_verify from the command line
#######################################

:command:`ap_verify.py` is a Python script designed to be run on both developer machines and verification servers.
While :command:`ap_verify.py` is not a :doc:`command-line task</modules/lsst.pipe.base/index>`, the command-line interface is designed to resemble that of command-line tasks where practical.
This page describes the minimum options needed to run ``ap_verify``.
For more details, see the :doc:`command-line-reference` or run :option:`ap_verify.py -h`.

.. _ap-verify-dataset-name:

Datasets as input arguments
===========================

Since ``ap_verify`` begins with an uningested :doc:`dataset<datasets>`, the input argument is a dataset name rather than a repository.

Datasets are identified by a name that gets mapped to an :doc:`eups-registered directory <datasets-install>` containing the data.
The mapping is :ref:`configurable<ap-verify-configuration-dataset>`.
The dataset names are a placeholder for a future data repository versioning system, and may be replaced in a later version of ``ap_verify``.

.. _ap-verify-run-output:

How to run ap_verify in a new workspace
=======================================

Using the :doc:`HiTS 2015 </packages/ap_verify_hits2015/index>` dataset as an example, one can run :command:`ap_verify.py` as follows:

.. prompt:: bash

   ap_verify.py --dataset HiTS2015 --id "visit=412518 ccdnum=25 filter=g" --output workspaces/hits/ --silent

Here the inputs are:

* :command:`HiTS2015` is the :ref:`dataset name <ap-verify-dataset-name>`,
* :command:`visit=412518 ccdnum=25 filter=g` is the :ref:`dataId<command-line-task-dataid-howto-about-dataid-keys>` to process,

while the output is:

* :command:`workspaces/hits/` is the location where the pipeline will create any :ref:`Butler repositories<command-line-task-data-repo-using-uris>` necessary,

* :command:`--silent` disables SQuaSH metrics reporting.

This call will create a new directory at :file:`workspaces/hits`, ingest the HiTS data into a new repository based on :file:`<hits-data>/repo/`, then run visit 412518 through the entire AP pipeline.

.. note::

   The command-line interface for :command:`ap_verify.py` is at present much more limited than those of command-line tasks.
   In particular, only file-based repositories are supported, and compound dataIds cannot be provided.
   See the :doc:`command-line-reference` for details.

.. _ap-verify-run-ingest:

How to run ingestion by itself
==============================

``ap_verify`` includes a separate program, :command:`ingest_dataset.py`, that ingests datasets but does not run the pipeline on them.
This is useful if the data need special processing or as a precursor to massive processing runs.
Running :command:`ap_verify.py` with the same arguments as a previous run of :command:`ingest_dataset.py` will automatically skip ingestion.

Using the :doc:`HiTS 2015 </packages/ap_verify_hits2015/index>` dataset as an example, one can run ``ingest_dataset`` as follows:

.. prompt:: bash

   ingest_dataset.py --dataset HiTS2015 --output workspaces/hits/

The :option:`--dataset <ap_verify.py --dataset>` and :option:`--output <ap_verify.py --output>` arguments behave the same way as for :command:`ap_verify.py`.
Other options from :command:`ap_verify.py` are not available.

.. _ap-verify-results:

How to use measurements of metrics
==================================

After ``ap_verify`` has run, it will produce a file named, by default, :file:`ap_verify.verify.json` in the caller's directory.
The file name may be customized using the :option:`--metrics-file <ap_verify.py --metrics-file>` command-line argument.
This file contains metric measurements in ``lsst.verify`` format, and can be loaded and read as described in the :doc:`lsst.verify documentation</modules/lsst.verify/index>` or in `SQR-019 <https://sqr-019.lsst.io>`_.

Unless the :option:`--silent <ap_verify.py --silent>` argument is provided, ``ap_verify`` will also upload measurements to the `SQuaSH service <https://squash.lsst.codes/>`_ on completion.
See the SQuaSH documentation for details.

If the pipeline is interrupted by a fatal error, completed measurements will be saved to the metrics file for debugging purposes, but nothing will get sent to SQuaSH.
See the :ref:`error-handling policy <ap-verify-failsafe-partialmetric>` for details.

Further reading
===============

- :doc:`datasets-install`
- :doc:`command-line-reference`
