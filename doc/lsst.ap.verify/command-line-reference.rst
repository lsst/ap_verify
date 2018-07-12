.. _ap-verify-cmd:

.. program:: ap_verify.py

######################
Command-Line Reference
######################

This page describes the command-line arguments and environment variables used by ``ap_verify``.

Signature and syntax
====================

The basic call signature of ``ap_verify`` is:

.. prompt:: bash

   ap_verify.py --dataset DATASET --output WORKSPACE --id DATAID

These three arguments are mandatory, all others are optional.

Status code
===========

``ap_verify`` returns a status code of ``0`` if the pipeline ran to completion.
If the pipeline fails, the status code will be an interpreter-dependent nonzero value.

Named arguments
===============

Required arguments are :option:`--dataset`, :option:`--id`, and :option:`--output`.

.. option:: --id <dataId>

   **Butler data ID.**

   The input data ID is required for all ``ap_verify`` runs except when using :option:`--help` or :option:`--version`.

   Specify data ID to process using data ID syntax.
   For example, ``--id "visit=12345 ccd=1 filter=g"``.
   
   Currently this argument is heavily restricted compared to its :ref:`command line task counterpart<command-line-task-dataid-howto>`.
   In particular, the dataId must specify exactly one visit and exactly one CCD, and may not be left blank to mean "all data".

.. option:: --dataset <dataset_name>

   **Input dataset designation.**

   The input dataset is required for all ``ap_verify`` runs except when using :option:`--help` or :option:`--version`.

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

   The name of a file to contain the metrics measured by ``ap_verify``, in a format readable by the `lsst.verify` framework.
   If omitted, the output will go to a file named :file:`ap_verify.verify.json` in the user's working directory.

   This argument can be used to run multiple instances of ``ap_verify`` concurrently, with each instance producing output to a different metrics file.

.. option:: --output <workspace_dir>

   **Output and intermediate product path.**

   The output argument is required for all ``ap_verify`` runs except when using :option:`--help` or :option:`--version`.

   The workspace will be created if it does not exist, and will contain both input and output repositories required for processing the data.
   The path may be absolute or relative to the current working directory.

.. option:: --silent

   **Do not report measurements to SQuaSH.**

   Disables upload of measurements, so that ``ap_verify`` can be run for testing purposes by developers.

   .. note::

      Ingestion of `lsst.verify` metrics is not yet supported by SQuaSH, so this flag should always be provided for now.

.. option:: --version

   **Print version number.**

   Since ``ap_verify`` is not yet officially part of the Stack, the version number is arbitrary.


.. _command-line-task-envvar:

Environment variables
=====================

The :envvar:`SQUASH_USER`, :envvar:`SQUASH_PASSWORD`, and :envvar:`SQUASH_URL` environment variables are used by :ref:`the verify framework<lsst.verify>` to configure SQuaSH upload.
:envvar:`SQUASH_USER` and :envvar:`SQUASH_PASSWORD` must be defined in any environment where ``ap_verify`` is run unless the :option:`--silent` flag is used.

.. TODO: remove this once `lsst.verify` documents them, and update the link (DM-12849)

.. envvar:: SQUASH_USER

   User name to use for SQuaSH submissions.

.. envvar:: SQUASH_PASSWORD

   Unencrypted password for :envvar:`SQUASH_USER`.

.. envvar:: SQUASH_URL

   The location for a SQuaSH REST API. Defaults to the SQuaSH server at ``lsst.codes``.

.. _command-line-task-envvar-examples:

