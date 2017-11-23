.. currentmodule:: lsst.ap.verify

.. _lsst.ap.verify:

##############
lsst.ap.verify
##############

The ``lsst.ap.verify`` package provides an executable python program for pipeline verification.
It runs the alert production pipeline (encapsulated in the `lsst.ap.pipe` package), computes verification metrics on both the pipeline's state and its output, and works with the `SQuaSH <https://squash.lsst.codes/>`_ system to allow their monitoring and analysis.

.. _lsst-ap-verify-overview:

Overview
========

``ap_verify`` is designed to work with small, standardized :ref:`datasets<ap-verify-datasets>` that can be interchanged to test the Stack's performance under different conditions.
To ensure consistent results, it :ref:`runs the entire AP pipeline<ap-verify-running>` as a single unit, from data ingestion to source association.
It produces measurements, using the :ref:`lsst.verify<lsst.verify>` framework, that can be used for both monitoring stack development and debugging failure cases.

Using ap.verify
===============

.. toctree::
   :maxdepth: 1

   running.rst
   datasets.rst
   failsafe.rst
   command-line-reference.rst
   configuration.rst

Python API reference
====================

.. automodapi:: lsst.ap.verify
.. automodapi:: lsst.ap.verify.measurements

