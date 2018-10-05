.. py:currentmodule:: lsst.ap.verify

.. _ap-verify-datasets-install:

###################
Installing datasets
###################

:doc:`datasets` packages data in self-contained units that are intended to be easy to install for LSST Stack users.
It is not necessary to install all datasets supported by ``ap_verify``, only those you intend to use.

Prerequisites
=============

The Dataset framework requires that the computer have version 13.0 or later of the LSST Stack (specifically, the ``obs`` packages and their dependencies) installed.
:ref:`Installing lsst_distrib <part-installation>` is the simplest way to ensure all dependencies are satisfied.

The framework also requires `Git LFS`_ and the `EUPS`_ package management system.
EUPS is included in the Stack installation.

.. _Git LFS: https://developer.lsst.io/tools/git_lfs.html
.. _EUPS: https://developer.lsst.io/build-ci/eups_tutorial.html

Installation procedure
======================

Use the `LSST Software Build Tool <https://developer.lsst.io/stack/lsstsw.html>`_ to request the dataset by its package name.
A :ref:`list of existing datasets <ap-verify-datasets-index>` is maintained as part of this documentation.
Because of their large size (typically hundreds of GB), datasets are *never* installed as a dependency of another package; they must be requested explicitly.

For example, to install the :doc:`HiTS 2015 </packages/ap_verify_hits2015/index>` dataset,

.. prompt:: bash

   rebuild -u ap_verify_hits2015

Once this is done, ``ap_verify`` will be able to find the HiTS data upon request.
