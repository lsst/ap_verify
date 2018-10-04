.. py:currentmodule:: lsst.ap.verify

.. _lsst.ap.verify:

##############
lsst.ap.verify
##############

The ``lsst.ap.verify`` package provides an executable python program for pipeline verification.
It runs the alert production pipeline (encapsulated in the `lsst.ap.pipe` package), computes `lsst.verify` metrics on both the pipeline's state and its output, and works with the `SQuaSH <https://squash.lsst.codes/>`_ system to allow their monitoring and analysis.

``ap_verify`` is designed to work with small, standardized :ref:`datasets<ap-verify-datasets>` that can be interchanged to test the Stack's performance under different conditions.
To ensure consistent results, it :ref:`runs the entire AP pipeline<ap-verify-running>` as a single unit, from data ingestion to source association.
It produces measurements, using the :ref:`lsst.verify<lsst.verify>` framework, that can be used for both monitoring stack development and debugging failure cases.

.. _lsst.ap.verify-using:

Using lsst.ap.verify
====================

.. toctree::
   :maxdepth: 1

   running.rst
   datasets.rst
   failsafe.rst
   command-line-reference.rst
   configuration.rst

.. _lsst.ap.verify-contributing:

Contributing
============

``lsst.ap.verify`` is developed at https://github.com/lsst/ap_verify.
You can find Jira issues for this module under the `ap_verify <https://jira.lsstcorp.org/issues/?jql=project%20%3D%20DM%20AND%20component%20%3D%20ap_verify>`_ component.

.. toctree::
   :maxdepth: 1

   new-metrics.rst

.. _lsst.ap.verify-pyapi:

Python API reference
====================

.. automodapi:: lsst.ap.verify
   :no-main-docstr:
   :no-inheritance-diagram:
.. automodapi:: lsst.ap.verify.measurements
   :no-main-docstr:
   :no-inheritance-diagram:
