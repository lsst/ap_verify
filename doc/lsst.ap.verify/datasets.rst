.. py:currentmodule:: lsst.ap.verify

.. _ap-verify-datasets:

#####################
The dataset framework
#####################

The ``ap_verify`` system is designed to allow integration testing of the Alert Production pipeline on a variety of LSST precursor data.
The dataset framework provides a common format and delivery system for the test data.

.. _ap-verify-datasets-overview:

Overview
========

Datasets are implemented as :ref:`Git LFS repositories with a specific format<ap-verify-datasets-structure>`.
They provide the raw and calibration data files needed for an ``ap_verify`` run, and identify the observatory used to take the data.
The observatory's ``obs`` package can then be used by ``ap_verify`` to ingest the data into the LSST system and run the pipeline.

.. _ap-verify-datasets-details:

In depth
========

.. toctree::
   :maxdepth: 1

   datasets-creation
   datasets-install

.. _ap-verify-datasets-index:

Supported datasets
==================

These datasets are maintained by the ``ap_verify`` group.
There may be other datasets :ref:`formatted<ap-verify-datasets-structure>` for use with ``ap_verify``.

* `ap_verify_hits2015 (HiTS 2015 with 2014 templates) <https://github.com/lsst/ap_verify_hits2015/>`_
* `ap_verify_ci_hits2015 (HiTS 2015 CI Subset) <https://github.com/lsst/ap_verify_ci_hits2015/>`_
* `ap_verify_ci_cosmos_pdr2 (Cosmos DR2 ultradeep fields) <https://github.com/lsst/ap_verify_ci_cosmos_pdr2/>`_

..
   TODO: switch to toctree once these docs included in pipelines.lsst.io
   .. toctree::
      :maxdepth: 1

      /packages/ap_verify_hits2015/index
      /packages/ap_verify_ci_hits2015/index
      /packages/ap_verify_ci_cosmos_pdr2/index
