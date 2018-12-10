.. py:currentmodule:: lsst.ap.verify

.. program:: ap_verify.py

.. _ap-verify-cmd:

################################
ap_verify command-line reference
################################

This page describes the command-line arguments and environment variables used by :command:`ap_verify.py`.

.. _ap-verify-cmd-basic:

Signature and syntax
====================

The basic call signature of :command:`ap_verify.py` is:

.. prompt:: bash

   ap_verify.py --dataset DATASET --output WORKSPACE --id DATAID

These three arguments are mandatory, all others are optional.

.. _ap-verify-cmd-return:

Status code
===========

:command:`ap_verify.py` returns a status code of ``0`` if the pipeline ran to completion.
If the pipeline fails, the status code will be an interpreter-dependent nonzero value.

.. _ap-verify-cmd-args:

Named arguments
===============

Required arguments are :option:`--dataset`, :option:`--id`, and :option:`--output`.

.. option:: --id <dataId>

   **Butler data ID.**

   The input data ID is required for all ``ap_verify`` runs except when using :option:`--help`.

   Specify data ID to process using data ID syntax.
   For example, ``--id "visit=12345 ccd=1 filter=g"``.
   
   Currently this argument is heavily restricted compared to its :doc:`command line task counterpart</modules/lsst.pipe.base/command-line-task-dataid-howto>`.
   In particular, the dataId must specify exactly one visit and exactly one CCD, and may not be left blank to mean "all data".

.. option:: --dataset <dataset_name>

   **Input dataset designation.**

   The input dataset is required for all ``ap_verify`` runs except when using :option:`--help`.

   The argument is a unique name for the dataset, which can be associated with a repository in the :ref:`configuration file<ap-verify-configuration-dataset>`.
   See :ref:`ap-verify-dataset-name` for more information on dataset names.

   Allowed names can be queried using the :option:`--help` argument.

.. option:: -h, --help

   **Print help.**

   The help is equivalent to this documentation page, describing command-line arguments.

.. option:: -j <processes>, --processes <processes>

   **Number of processes to use.**

   When ``processes`` is larger than 1 the pipeline may use the Python `multiprocessing` module to parallelize processing of multiple datasets across multiple processors.
   
   .. note::

      This option is provided for forward-compatibility, but is not yet supported by ``ap_verify``.

.. option:: --metrics-file <filename>

   **Output metrics file.**

   The template for a file to contain metrics measured by ``ap_verify``, in a format readable by the :doc:`lsst.verify</modules/lsst.verify/index>` framework.
   The string ``{dataId}`` shall be replaced with the data ID associated with the job, and its use is strongly recommended.
   If omitted, the output will go to files named after ``ap_verify.{dataId}.verify.json`` in the user's working directory.

.. option:: --output <workspace_dir>

   **Output and intermediate product path.**

   The output argument is required for all ``ap_verify`` runs except when using :option:`--help`.

   The workspace will be created if it does not exist, and will contain both input and output repositories required for processing the data.
   The path may be absolute or relative to the current working directory.

.. option:: --silent

   **Do not report measurements to SQuaSH.**

   Disables upload of measurements, so that ``ap_verify`` can be run for testing purposes by developers.

   .. note::

      Ingestion of :doc:`lsst.verify</modules/lsst.verify/index>` metrics is not yet supported by SQuaSH, so this flag should always be provided for now.


.. _ap-verify-cmd-envvar:

Environment variables
=====================

The :envvar:`SQUASH_USER`, :envvar:`SQUASH_PASSWORD`, and :envvar:`SQUASH_URL` environment variables are used by :doc:`the verify framework</modules/lsst.verify/index>` to configure SQuaSH upload.
:envvar:`SQUASH_USER` and :envvar:`SQUASH_PASSWORD` must be defined in any environment where :command:`ap_verify.py` is run unless the :option:`--silent` flag is used.

.. TODO: remove this once `lsst.verify` documents them, and update the link (DM-12849)

.. envvar:: SQUASH_USER

   User name to use for SQuaSH submissions.

.. envvar:: SQUASH_PASSWORD

   Unencrypted password for :envvar:`SQUASH_USER`.

.. envvar:: SQUASH_URL

   The location for a SQuaSH REST API. Defaults to the SQuaSH server at ``lsst.codes``.
