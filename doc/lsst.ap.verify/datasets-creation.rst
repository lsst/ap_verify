.. py:currentmodule:: lsst.ap.verify

.. program:: ap_verify.py

.. _ap-verify-datasets-creation:

.. _ap-verify-datasets-structure:

###########################
Packaging data as a dataset
###########################

:doc:`datasets` can represent data from any observatory that has an :ref:`observatory interface (obs) package<obs-framework>` in the LSST software stack.
This page describes how to create and maintain a dataset.

.. _ap-verify-datasets-creation-gitlfs:

Creating a dataset repository
=============================

Datasets are Git LFS repositories with a particular directory and file structure.
The easiest way to create a new dataset is to `create an LFS repository <https://developer.lsst.io/git/git-lfs.html#git-lfs-create>`_, and add a copy of the `dataset template repository`_ as the initial commit.
This will create empty directories for all data and will add placeholder files for dataset metadata.

.. _dataset template repository: https://github.com/lsst-dm/ap_verify_dataset_template

.. _ap-verify-datasets-creation-layout:

Organizing the data
===================

* The :file:`raw` directory contains uningested science data.
  The directory may have any internal structure.
* The :file:`preloaded` directory contains a :ref:`Gen 3 LSST Butler repository<lsst.daf.butler-using>` with calibration data, coadded difference imaging templates, refcats, and any other files needed for processing science data.
  It must not contain science data, which belongs only in :file:`raw`.
* The :file:`config/export.yaml` file is a `relative-path export <lsst.daf.butler.Butler.export>` of the repository at :file:`preloaded`, used to set up a separate repository for running ``ap_verify``.
* The :file:`config` and :file:`pipelines` directories contain :ref:`configuration overrides needed to run the AP pipeline on the data<ap-verify-datasets-creation-config>`.

The templates and reference catalogs need not be all-sky, but should cover the combined footprint of all the raw images.

Datasets should contain a :file:`scripts` directory with scripts for (re)generatating and maintaining the contents of the dataset.
This allows the dataset, particularly calibs and templates, to be updated with pipeline improvements.
The :file:`scripts` directory is not formally part of the dataset framework, and its exact contents are up to the maintainer.

.. _ap-verify-datasets-creation-docs:

Documenting datasets
====================

Datasets provide package-level documentation in their :file:`doc` directory.
An example is provided in the `dataset template repository`_.

The dataset's package-level documentation should include:

* the source of the data (e.g., a particular survey with specific cuts applied)
* whether or not optional files such as image differencing templates are provided
* the expected use of the data

.. _ap-verify-datasets-creation-config:

Configuring dataset use
=======================

The files in :file:`config` or :file:`pipelines` should :ref:`override any config fields<pipeline_creating_config>` that are constrained by the input data, such as template type (deep, goodSeeing, etc.) or refcat filters, even if the current defaults match.
This policy makes the datasets more self-contained and prevents them from breaking when the pipeline defaults change but only one value is valid (e.g., ``coaddName`` *must* be ``"deep"`` for a dataset with deep coadds).

Each :file:`pipelines` directory should contain pipeline files corresponding to the pipelines in the :file:`ap_verify/pipelines` directory (at the time of writing, :file:`ApPipe.yaml`, :file:`ApVerify.yaml`, and :file:`ApVerifyWithFakes.yaml`).
The default execution of ``ap_verify`` assumes these files exist for each dataset, though :option:`--pipeline` can override it.

Configuration settings specific to an instrument rather than a dataset should be handled with ordinary :ref:`configuration override files<command-line-task-config-howto-obs>`.

.. _ap-verify-datasets-creation-obs:

Registering an observatory package
==================================

To ensure dataset processing does not crash, :file:`ups/<package>.table` must contain a line reading ``setupRequired(<obs-package>)``.
For example, for DECam data this would read ``setupRequired(obs_decam)``.
If any other unusual packages are required to process the data, they should have their own ``setupRequired`` lines.
