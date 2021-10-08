.. py:currentmodule:: lsst.ap.verify

.. program:: ap_verify.py

.. _ap-verify-running:

#######################################
Running ap_verify from the command line
#######################################

:command:`ap_verify.py` is a Python script designed to be run on both developer machines and verification servers.
While :command:`ap_verify.py` is not a :doc:`command-line task</modules/lsst.pipe.base/index>`, the command-line interface is designed to resemble that of command-line tasks where practical.
This page describes the most common options used to run ``ap_verify``.
For more details, see the :doc:`command-line-reference` or run :option:`ap_verify.py -h`.

.. _ap-verify-run-output:

How to run ap_verify in a new workspace (Gen 2 pipeline)
========================================================

Using the `Cosmos PDR2`_ CI dataset as an example, one can run :command:`ap_verify.py` as follows:

.. _Cosmos PDR2: https://github.com/lsst/ap_verify_ci_cosmos_pdr2/

.. prompt:: bash

   ap_verify.py --dataset ap_verify_ci_cosmos_pdr2 --gen2 --id "visit=59150^59160 filter=HSC-G" -j4 --output workspaces/cosmos/

Here the inputs are:

* :command:`ap_verify_ci_cosmos_pdr2` is the ``ap_verify`` :ref:`dataset <ap-verify-datasets>` to process,
* :option:`--gen2` specifies to process the dataset using the Gen 2 pipeline framework,
* :command:`visit=59150^59160 filter=HSC-G` is the :ref:`dataId<command-line-task-dataid-howto-about-dataid-keys>` to process,
* :option:`-j` causes the ingest and processing pipelines to use 4 processes: choose a value appropriate for your machine; the system does not automatically determine how many parallel processes to use.

while the output is:

* :command:`workspaces/cosmos/` is the location where the pipeline will create any :ref:`Butler repositories<command-line-task-data-repo-using-uris>` necessary,

This call will create a new directory at :file:`workspaces/cosmos`, ingest the Cosmos data into a new repository based on :file:`<cosmos-data>/repo/`, then run visits 59150 and 59160 through the entire AP pipeline.

It's also possible to run an entire dataset by omitting the :option:`--id` argument (as some datasets are very large, do this with caution):

.. prompt:: bash

   ap_verify.py --dataset ap_verify_ci_cosmos_pdr2 --gen2 -j4 --output workspaces/cosmos/

.. note::

   The command-line interface for :command:`ap_verify.py` is at present more limited than those of command-line tasks.
   See the :doc:`command-line-reference` for details.

.. _ap-verify-run-output-gen3:

How to run ap_verify in a new workspace (Gen 3 pipeline)
========================================================

Using the `Cosmos PDR2`_ CI dataset as an example, one can run :command:`ap_verify.py` as follows:

.. _Cosmos PDR2: https://github.com/lsst/ap_verify_ci_cosmos_pdr2/

.. prompt:: bash

   ap_verify.py --dataset ap_verify_ci_cosmos_pdr2 --gen3 --data-query "visit in (59150, 59160) and band='g'" -j4 --output workspaces/cosmos/

Here the inputs are:

* :command:`ap_verify_ci_cosmos_pdr2` is the ``ap_verify`` :ref:`dataset <ap-verify-datasets>` to process,
* :option:`--gen3` specifies to process the dataset using the Gen 3 pipeline framework,
* :command:`visit in (59150, 59160) and band='g'` is the :ref:`data ID query <daf_butler_dimension_expressions>` to process,
* :option:`-j` causes the ingest and processing pipelines to use 4 processes: choose a value appropriate for your machine; the system does not automatically determine how many parallel processes to use.

while the output is:

* :command:`workspaces/cosmos/` is the location where the pipeline will create a Butler repository along with other outputs such as the alert production database.

This call will create a new directory at :file:`workspaces/cosmos`, ingest the Cosmos data into a new repository, then run visits 59150 and 59160 through the entire AP pipeline.

It's also possible to run an entire dataset by omitting the :option:`--data-query` argument (as some datasets are very large, do this with caution):

.. prompt:: bash

   ap_verify.py --dataset ap_verify_ci_cosmos_pdr2 --gen3 -j4 --output workspaces/cosmos/

.. note::

   Because the science pipelines are still being converted to Gen 3, Gen 3 processing may not be supported for all ap_verify datasets.
   See the individual dataset's documentation for more details.

.. warning::

    Some datasets require particular data queries in order to successfully run through the pipeline, due to missing data or other limitations.
    Check the ``README.md`` in each dataset's main directory for what additional arguments might be necessary.


.. _ap-verify-run-ingest:

How to run ingestion by itself
==============================

``ap_verify`` includes a separate program, :command:`ingest_dataset.py`, that :doc:`ingests datasets into repositories <datasets-butler>` but does not run the pipeline on them.
This is useful if the data need special processing or as a precursor to massive processing runs.
Running :command:`ap_verify.py` with the same arguments as a previous run of :command:`ingest_dataset.py` will automatically skip ingestion.

Using the `Cosmos PDR2`_ dataset as an example, one can run ``ingest_dataset`` in Gen 2 as follows:

.. prompt:: bash

   ingest_dataset.py --dataset ap_verify_ci_cosmos_pdr2 --gen3 -j4 --output workspaces/cosmos/

The :option:`--dataset`, :option:`--output`, :option:`--gen2`, :option:`--gen3`, :option:`-j` (does not apply to :option:`--gen2`), and :option:`--processes` arguments behave the same way as for :command:`ap_verify.py`.
Other options from :command:`ap_verify.py` are not available.

.. _ap-verify-results:

How to use measurements of metrics (Gen 2 pipeline)
===================================================

After ``ap_verify`` has run, it will produce files named, by default, :file:`ap_verify.<dataId>.verify.json` in the caller's directory.
The file name may be customized using the :option:`--metrics-file` command-line argument.
These files contain metric measurements in ``lsst.verify`` format, and can be loaded and read as described in the :doc:`lsst.verify documentation</modules/lsst.verify/index>` or in `SQR-019 <https://sqr-019.lsst.io>`_.

If the pipeline is interrupted by a fatal error, completed measurements will be saved to metrics files for debugging purposes.
See the :ref:`error-handling policy <ap-verify-failsafe-partialmetric>` for details.

.. _ap-verify-results-gen3:

How to use measurements of metrics (Gen 3 pipeline)
===================================================

After ``ap_verify`` has run, it will produce Butler datasets named ``metricValue_<metric package>_<metric>``.
These can be queried, like any Butler dataset, using methods like `~lsst.daf.butler.Registry.queryDatasetTypes` and `~lsst.daf.butler.Butler.get`.

.. note::

   Not all metric values need have the same data ID as the data run through the pipeline.
   For example, metrics describing the full focal plane have a visit but no detector.

Further reading
===============

- :doc:`datasets-install`
- :doc:`new-metrics`
- :doc:`failsafe`
- :doc:`command-line-reference`
