.. py:currentmodule:: lsst.ap.verify

.. program:: ap_verify.py

.. _ap-verify-running:

#######################################
Running ap_verify from the command line
#######################################

:command:`ap_verify.py` is a Python script designed to be run on both developer machines and verification servers.
This page describes the most common options used to run ``ap_verify``.
For more details, see the :doc:`command-line-reference` or run :option:`ap_verify.py -h`.

This guide assumes that the dataset(s) to be run are already installed on the machine.
If this is not the case, see :doc:`datasets-install`.

.. _ap-verify-run-output-gen3:

How to run ap_verify in a new workspace
=======================================

Using the `Cosmos PDR2`_ CI dataset as an example, first setup the dataset, if it isn't already.

.. _Cosmos PDR2: https://github.com/lsst/ap_verify_ci_cosmos_pdr2/

.. prompt:: bash

   setup [-r] ap_verify_ci_cosmos_pdr2

You will need to setup the dataset once each session.

You can then run :command:`ap_verify.py` as follows.

.. prompt:: bash

   ap_verify.py --dataset ap_verify_ci_cosmos_pdr2 --data-query "visit in (59150, 59160)" -j4 --output workspaces/cosmos/

Here the inputs are:

* :command:`ap_verify_ci_cosmos_pdr2` is the ``ap_verify`` :ref:`dataset <ap-verify-datasets>` to process,
* :command:`visit in (59150, 59160)` is the :ref:`data ID query <daf_butler_dimension_expressions>` to process,
* :option:`-j` causes the ingest and processing pipelines to use 4 processes: choose a value appropriate for your machine; the system does not automatically determine how many parallel processes to use.

while the output is:

* :command:`workspaces/cosmos/` is the location where the pipeline will create a Butler repository along with other outputs such as the alert production database.

This call will create a new directory at :file:`workspaces/cosmos`, ingest the Cosmos data into a new repository, then run visits 59150 and 59160 through the entire AP pipeline.

It's also possible to run an entire dataset by omitting the :option:`--data-query` argument (as some datasets are very large, do this with caution):

.. prompt:: bash

   ap_verify.py --dataset ap_verify_ci_cosmos_pdr2 -j4 --output workspaces/cosmos/

.. warning::

    Some datasets require particular data queries in order to successfully run through the pipeline, due to missing data or other limitations.
    Check the ``README.md`` in each dataset's main directory for what additional arguments might be necessary.


.. _ap-verify-run-ingest:

How to run ingestion by itself
==============================

``ap_verify`` includes a separate program, :command:`ingest_dataset.py`, that ingests datasets into repositories but does not run the pipeline on them.
This is useful if the data need special processing or as a precursor to massive processing runs.
Running :command:`ap_verify.py` with the same arguments as a previous run of :command:`ingest_dataset.py` will automatically skip ingestion.

Using the `Cosmos PDR2`_ dataset as an example, one can run ``ingest_dataset`` as follows:

.. prompt:: bash

   ingest_dataset.py --dataset ap_verify_ci_cosmos_pdr2 -j4 --output workspaces/cosmos/

The :option:`--dataset`, :option:`--output`, :option:`-j`, and :option:`--processes` arguments behave the same way as for :command:`ap_verify.py`.
Other options from :command:`ap_verify.py` are not available.

.. _ap-verify-results-gen3:

How to use measurements of metrics
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
- :doc:`command-line-reference`
