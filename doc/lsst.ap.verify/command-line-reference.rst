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

These two arguments are mandatory, all others are optional (though use of either :option:`--gen2` or :option:`--gen3` is highly recommended).

.. _ap-verify-cmd-return:

Status code
===========

:command:`ap_verify.py` returns 0 on success, and a non-zero value if there were any processing problems.
In :option:`--gen2` mode, the status code is the number of data IDs that could not be processed, like for :ref:`command-line tasks <command-line-task-argument-reference>`.

With both :option:`--gen2` and :option:`--gen3`, an uncaught exception may cause :command:`ap_verify.py` to return an interpreter-dependent nonzero value instead of the above.

.. _ap-verify-cmd-args:

Named arguments
===============

Required arguments are :option:`--dataset` and :option:`--output`.

.. option:: --id <dataId>

   **Butler data ID.**

   Specify data ID to process.
   If using :option:`--gen2`, this should use :doc:`data ID syntax </modules/lsst.pipe.base/command-line-task-dataid-howto>`, such as ``--id "visit=12345 ccd=1..6 filter=g"``.
   If using :option:`--gen3`, this should use :ref:`dimension expression syntax <daf_butler_dimension_expressions>`, such as ``--id "visit=12345 and detector in (1..6) and band='g'"``.
   Consider using :option:`--data-query` instead of ``--id`` for forward-compatibility and consistency with Gen 3 pipelines.

   Multiple copies of this argument are allowed.
   For compatibility with the syntax used by command line tasks, ``--id`` with no argument processes all data IDs.

   If this argument is omitted, then all data IDs in the dataset will be processed.
   
.. option:: -d, --data-query <dataId>

   **Butler data ID.**

   This option is identical to :option:`--id`, and will become the primary data ID argument as Gen 2 is retired.
   It is recommended over :option:`--id` for :option:`--gen3` runs.

.. option:: --dataset <dataset_name>

   **Input dataset designation.**

   The :doc:`input dataset <datasets>` is required for all ``ap_verify`` runs except when using :option:`--help`.

   The argument is a unique name for the dataset, which can be associated with a repository in the :ref:`configuration file<ap-verify-configuration-dataset>`.
   See :ref:`ap-verify-dataset-name` for more information on dataset names.

   :ref:`Allowed names <ap-verify-datasets-index>` can be queried using the :option:`--help` argument.

.. option:: --dataset-metrics-config <filename>

   **Input dataset-level metrics config. (Gen 2 only)**

   A config file containing a `~lsst.verify.gen2tasks.MetricsControllerConfig`, which specifies which metrics are measured and sets any options.
   If this argument is omitted, :file:`config/default_dataset_metrics.py` will be used.

   Use :option:`--image-metrics-config` to configure image-level metrics instead.
   For the Gen 3 equivalent to this option, see :option:`--pipeline`.
   See also :doc:`new-metrics`.

.. option:: --db, --db_url

   **Target Alert Production Database**

   A URI string identifying the database in which to store source associations.
   The string must be in the format expected by `lsst.dax.apdb.ApdbConfig.db_url`, i.e. an SQLAlchemy connection string.
   The indicated database is created if it does not exist and this is appropriate for the database type.

   If this argument is omitted, ``ap_verify`` creates an SQLite database inside the directory indicated by :option:`--output`.

.. option:: --gen2
.. option:: --gen3

   **Choose Gen 2 or Gen 3 processing.**

   These optional flags run either the Gen 2 pipeline (`~lsst.ap.pipe.ApPipeTask`), or the Gen 3 pipeline (:file:`apPipe.yaml`).
   If neither flag is provided, the Gen 2 pipeline will be run.

   .. note::

      The current default is provided for backward-compatibility with old scripts that assumed Gen 2 processing.
      The default will change to ``--gen3`` once Gen 3 processing is officially supported by the Science Pipelines, at which point Gen 2 support will be deprecated.
      Until the default stabilizes, users should be explicit about which pipeline they wish to run.

.. option:: -h, --help

   **Print help.**

   The help is equivalent to this documentation page, describing command-line arguments.

.. option:: -j <processes>, --processes <processes>

   **Number of processes to use.**

   When ``processes`` is larger than 1 the pipeline may use the Python `multiprocessing` module to parallelize processing of multiple datasets across multiple processors.
   In Gen 3 mode, data ingestion may also be parallelized.
   
.. option:: --image-metrics-config <filename>

   **Input image-level metrics config. (Gen 2 only)**

   A config file containing a `~lsst.verify.gen2tasks.MetricsControllerConfig`, which specifies which metrics are measured and sets any options.
   If this argument is omitted, :file:`config/default_image_metrics.py` will be used.

   Use :option:`--dataset-metrics-config` to configure dataset-level metrics instead.
   For the Gen 3 equivalent to this option, see :option:`--pipeline`.
   See also :doc:`new-metrics`.

.. option:: --metrics-file <filename>

   **Output metrics file. (Gen 2 only)**

   The template for a file to contain metrics measured by ``ap_verify``, in a format readable by the :doc:`lsst.verify</modules/lsst.verify/index>` framework.
   The string ``{dataId}`` shall be replaced with the data ID associated with the job, and its use is strongly recommended.
   If omitted, the output will go to files named after ``ap_verify.{dataId}.verify.json`` in the user's working directory.

.. option:: --output <workspace_dir>

   **Output and intermediate product path.**

   The output argument is required for all ``ap_verify`` runs except when using :option:`--help`.

   The workspace will be created if it does not exist, and will contain both input and output repositories required for processing the data.
   The path may be absolute or relative to the current working directory.

.. option:: -p, --pipeline <filename>

   **Custom ap_verify pipeline. (Gen 3 only)**

   A pipeline definition file containing a custom verification pipeline.
   If omitted, :file:`pipelines/ApVerify.yaml` will be used.

   The most common use for a custom pipeline is adding or removing metrics to be run along with the AP pipeline.

   .. note::

      At present, ap_verify assumes that the provided pipeline is some superset of the AP pipeline.
      It will likely crash if any AP tasks are missing.

   For the Gen 2 equivalent to this option, see :option:`--dataset-metrics-config` and :option:`--image-metrics-config`.
