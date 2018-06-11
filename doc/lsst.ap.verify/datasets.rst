.. _ap-verify-datasets:

#####################
The Dataset Framework
#####################

The ``ap_verify`` system is designed to allow integration testing of the Alert Production pipeline on a variety of LSST precursor data.
The dataset framework provides a common format and delivery system for the test data.
In effect, datasets serve as an adapter between raw observatory output and the :ref:`LSST observatory interface (obs) framework<obs-framework>`.

Overview
--------

Datasets are implemented as :ref:`Git LFS repositories with a specific format<ap-verify-datasets-structure>`.
They provide the raw and calibration data files needed for an ``ap_verify`` run, and identify the observatory used to take the data.
The observatory's ``obs`` package can then be used by ``ap_verify`` to ingest the data into the LSST system and run the pipeline.
Datasets are deliberately simple to allow them to be created and maintained without much knowledge of the LSST stack, particularly of the :ref:`Butler I/O framework<butler>`.

In Depth
--------

.. toctree::
   :maxdepth: 1

   datasets-creation.rst
   datasets-install.rst
   datasets-butler.rst

.. _ap-verify-datasets-index:

Existing Datasets
-----------------

* :ref:`HiTS2015 <ap_verify_hits2015-package>`

..
   TODO: switch to toctree once these docs included in pipelines.lsst.io
   .. toctree::
      :maxdepth: 1

      ../../packages/ap_verify_hits2015/index.rst

