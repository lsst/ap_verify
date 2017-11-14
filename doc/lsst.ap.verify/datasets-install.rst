.. _ap-verify-datasets-install:

###################
Installing Datasets
###################

:ref:`ap-verify-datasets` packages data in self-contained units that are intended to be easy to install for LSST Stack users.
It is not necessary to install all datasets supported by ``ap_verify``, only those you intend to use.

Prerequisites
-------------

The Dataset framework requires that the computer have version 13.0 or later of the LSST Stack (specifically, the ``obs`` packages and their dependencies) installed.
:ref:`Installing lsst_distrib <part-installation>` is the simplest way to ensure all dependencies are satisfied.

The framework also requires `Git LFS`_ and the `EUPS`_ package management system.
EUPS is included in the Stack installation.

.. _Git LFS: https://developer.lsst.io/tools/git_lfs.html
.. _EUPS: https://developer.lsst.io/build-ci/eups_tutorial.html

Installation Procedure
----------------------

Use Git LFS to clone the desired dataset's GitHub repository.
To get the URL, see the :ref:`package documentation<ap-verify-datasets-index>` for the dataset in question.

.. TODO: should we have a proper versioning system for datasets? (DM-12853)

Once the dataset has been installed, use :command:`eups declare` to register the downloaded directory.
The product name given to EUPS must match the repository name; the version can be anything.
It is also possible to register the dataset using :command:`setup`, but this is recommended only for temporary tests.

For example, to install the :ref:`HiTS 2015 <ap_verify_hits2015-package>` dataset,

.. prompt:: bash

   $ git clone https://github.com/lsst/ap_verify_hits2015 mydata
   $ eups declare -r mydata ap_verify_hits2015 v1

Once this is done, ``ap_verify`` will be able to find the HiTS data upon request.

