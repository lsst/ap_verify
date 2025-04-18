.. py:currentmodule:: lsst.ap.verify

.. program:: ap_verify.py

.. _ap-verify-cmd:

################################
ap_verify command-line reference
################################

This page describes the command-line arguments and environment variables used by :command:`ap_verify.py`.
See :doc:`running` for an overview.

.. _ap-verify-cmd-basic:

Signature and syntax
====================

The basic call signature of :command:`ap_verify.py` is:

.. prompt:: bash

   ap_verify.py --dataset DATASET --output WORKSPACE

These two arguments are mandatory, all others are optional.

.. _ap-verify-cmd-return:

Status code
===========

:command:`ap_verify.py` returns 0 on success, and a non-zero value if there were any processing problems.

An uncaught exception may cause :command:`ap_verify.py` to return an interpreter-dependent nonzero value instead of the above.

.. _ap-verify-cmd-args:

Named arguments
===============

Required arguments are :option:`--dataset` and :option:`--output`.

.. option:: --clean-run

   **Rerun ap_verify in a clean Gen 3 run even if the workspace already exists.**

   By default, when ``ap_verify`` is run multiple times with the same :option:`--output` workspace, the previous run collection is reused to avoid repeating processing.
   If this is undesirable (e.g., experimental config changes), this flag creates a new run, and the pipeline is run from the beginning.
   This flag has no effect if :option:`--output` is a fresh directory.

   .. note::

      The ``--clean-run`` flag does *not* reset the alert production database,
      as this is not something that can be done without knowledge of the
      specific database system being used. If the database has been written to
      by a previous run, clear it by hand before running with ``--clean-run``.

.. option:: -d, --data-query <dataId>

   **Butler data ID.**

   Specify data ID to process.
   This should use :ref:`dimension expression syntax <daf_butler_dimension_expressions>`, such as ``--data-query "visit=12345 and detector in (1..6) and band='g'"``.

   Multiple copies of this argument are allowed.

   If this argument is omitted, then all data IDs in the dataset will be processed.

.. option:: --dataset <dataset_package>

   **Input dataset package.**

   The :doc:`input dataset <datasets>` is required for all ``ap_verify`` runs except when using :option:`--help`.

   The argument is the name of the Git LFS repository containing the dataset to process.
   The repository must be set up before running ``ap_verify``.

   This documentation includes a :ref:`list of supported datasets <ap-verify-datasets-index>`.

.. option:: --db, --db_url

   **Target Alert Production Database**

   A URI string identifying the database in which to store source associations.
   The string must be in the format expected by `lsst.dax.apdb.ApdbConfig.db_url`, i.e. an SQLAlchemy connection string.
   The indicated database is created if it does not exist and this is appropriate for the database type.

   If this argument is omitted, ``ap_verify`` creates an SQLite database inside the directory indicated by :option:`--output`.

.. option:: --namespace <sasquatch_namespace>

   The sasquastch namespace to use for the ap_verify metrics upload.
   If this is provided, then a valid REST proxy URL must be provided with :option:`--restProxyUrl`.

.. option:: --restProxyUrl <sasquastch_proxy_url>

   A URI string identifying the Sasquastch url to use for the ap_verify metrics upload. If this is provided, then a valid :option:`--namespace` must be provided.

.. option:: -h, --help

   **Print help.**

   The help is equivalent to this documentation page, describing command-line arguments.

.. option:: -j <processes>, --processes <processes>

   **Number of processes to use.**

   When ``processes`` is larger than 1 the pipeline may use the Python `multiprocessing` module to parallelize processing of multiple datasets across multiple processors.

.. option:: --output <workspace_dir>

   **Output and intermediate product path.**

   The output argument is required for all ``ap_verify`` runs except when using :option:`--help`.

   The workspace will be created if it does not exist, and will contain the repository required for processing the data.
   The path may be absolute or relative to the current working directory.

.. option:: -p, --pipeline <filename>

   **Custom ap_verify pipeline.**

   A pipeline definition file containing a custom verification pipeline.
   This pipeline must be specialized as necessary for the instrument and dataset being processed.
   If omitted, :file:`<dataset>/pipelines/ApVerify.yaml` will be used.

   The most common use for a custom pipeline is adding or removing metrics to be run along with the AP pipeline.

   .. note::

      At present, ap_verify assumes that the provided pipeline includes the ``apdb_config`` parameter, which should be a path to the file created by ``apdb-cli create-cassandra`` or ``apdb-cli create-sql`` when initializing the APDB.
      It will likely crash if this task is missing.

.. option:: --extra <key=value>

   **Optional extra key=value arguments.**

   These arguments are passed directly to the ``ap_verify`` pipeline, they are used in case a SasquastchDatastore is created, and are in the form ``key=value``. In the context of CI, a Jenkins job would tag the test run with these extra parameters.
