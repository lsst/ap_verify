.. py:currentmodule:: lsst.ap.verify

.. program:: ap_verify.py

.. _ap-verify-datasets-creation:

.. _ap-verify-datasets-structure:

###########################
Packaging data as a dataset
###########################

:doc:`datasets` is designed to be as generic as possible, and should be able to accommodate any collection of observations so long as the source observatory has an :ref:`observatory interface (obs) package<obs-framework>` in the LSST software stack.
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

* The :file:`raw` and :file:`calib` directories contain science and calibration data, respectively.
  The directories may have any internal structure.
* The :file:`templates` directory contains an :ref:`LSST Butler repository<butler>` containing processed images usable as templates.
  Template files must be ``TemplateCoadd`` files produced by a compatible version of the LSST science pipeline.
* The :file:`refcats` directory contains one or more tar files, each containing one astrometric or photometric reference catalog in HTM shard format.

The templates and reference catalogs need not be all-sky, but should cover the combined footprint of all the raw images.

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

Configuring dataset ingestion and use
=====================================

Each dataset's :file:`config` directory should contain a :ref:`task config file<command-line-task-config-howto-configfile>` named :file:`datasetIngest.py`, which specifies a `DatasetIngestConfig`.
The file typically contains filenames or file patterns specific to the dataset.
In particular, the default config ignores reference catalogs, so the config file should provide a ``dict`` from catalog names to their tar files.

Each :file:`config` directory may contain a task config file named :file:`apPipe.py`, specifying an `lsst.ap.pipe.ApPipeConfig`.
The file contains pipeline flags specific to the dataset, such as the available reference catalogs (both their names and configuration) or the type of template provided to `~lsst.pipe.tasks.imageDifference.ImageDifferenceTask`.

Each :file:`pipelines` directory should contain pipeline files corresponding to the pipelines in the :file:`ap_verify/pipelines` directory (at the time of writing, :file:`ApPipe.yaml`, :file:`ApVerify.yaml`, and :file:`ApVerifyWithFakes.yaml`).
These files should incorporate the same dataset-specific configuration overrides as described above for :file:`apPipe.py`.

Configuration settings specific to an instrument rather than a dataset should be handled with ordinary :ref:`configuration override files<command-line-task-config-howto-obs>`.

.. _ap-verify-datasets-creation-obs:

Registering an observatory package
==================================

The observatory package must be named in two files:

* :file:`ups/<package>.table` must contain a line reading ``setupRequired(<obs-package>)``.
  For example, for DECam data this would read ``setupRequired(obs_decam)``.
  If any other packages are required to process the data, they should have their own ``setupRequired`` lines.
* :file:`repo/_mapper` must contain a single line with the name of the obs package's mapper class.
  For DECam data this is ``lsst.obs.decam.DecamMapper``.
