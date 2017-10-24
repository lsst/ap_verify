.. _ap-verify-datasets-creation:

.. _ap-verify-datasets-structure:

###########################
Packaging Data as a Dataset
###########################

:ref:`ap-verify-datasets` is designed to be as generic as possible, and should be able to accomodate any collection of observations so long as the source observatory has an :ref:`observatory interface (obs) package<obs-framework>` in the LSST software stack.
This page describes how to create and maintain a dataset.
It does not include :ref:`configuring ap_verify to use the dataset<ap-verify-configuration>`.

.. _ap-ver-fy-datasets-creation-gitlfs:

Creating a Dataset Repository
-----------------------------

Datasets are Git LFS repositories with a particular directory and file structure.
The easiest way to create a new dataset is to create a repository, and add a copy of the `dataset template repository`_ as the initial commit.
This will create empty directories for all data and will add placeholder files for dataset metadata.

.. _dataset template repository: https://github.com/lsst-dm/ap_verify_dataset_template

.. _ap-ver-fy-datasets-creation-layout:

Organizing the Data
-------------------

.. TODO: talk to David/Meredith to confirm details of how this works, and how flexible the system is (DM-12851)

* The :file:`raw` and :file:`calib` directories contain science and calibration data, respectively.
  The directories must be organized in the same way as the observatory's normal data products, for compatibility with the appropriate ``obs`` package.
* The :file:`templates` directory contains a :ref:`LSST Butler repository<butler>` containing processed images useable as templates. Template files must be ``TemplateCoadd`` files produced by a compatible version of the LSST science pipeline.

.. TODO: are these more standardized? are they available from somewhere? (DM-12851)

* The :file:`refcats` directory contains one or more tar'd HTM files containing astrometric and photometric reference catalogs.

The templates and reference catalogs need not be all-sky, but should cover the combined footprint of all the raw images.

.. _ap-ver-fy-datasets-creation-obs:

Registering an Observatory Package
----------------------------------

The observatory package must be named in two files:

* :file:`ups/<package>.table` must contain a line reading ``setupRequired(<obs-package>)``.
  For example, for DECam data this would read ``setupRequired(obs_decam)``.
  If any other packages are required to process the data, they should have their own ``setupRequired`` lines.
* :file:`repo/_mapper` must contain a single line with the name of the obs package's mapper class.
  For DECam data this is ``lsst.obs.decam.DecamMapper``.

