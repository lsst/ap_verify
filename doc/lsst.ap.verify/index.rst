.. py:currentmodule:: lsst.ap.verify

.. _lsst.ap.verify:

##############
lsst.ap.verify
##############

The ``lsst.ap.verify`` package provides an executable python program for pipeline verification.
It runs the alert production pipeline (encapsulated in the :doc:`lsst.ap.pipe </modules/lsst.ap.pipe/index>` package) and computes :doc:`lsst.verify </modules/lsst.verify/index>` metrics on both the pipeline's state and its output.

``ap_verify`` is designed to work with small, standardized :doc:`datasets<datasets>` that can be interchanged to test the Stack's performance under different conditions.
To ensure consistent results, it :doc:`runs the entire AP pipeline<running>` as a single unit, from data ingestion to source association.
It produces measurements, using the :doc:`lsst.verify</modules/lsst.verify/index>` framework, that can be used for both monitoring stack development and debugging failure cases.

.. _lsst.ap.verify-using:

Using lsst.ap.verify
====================

.. toctree::
   :maxdepth: 1

   running
   datasets
   failsafe
   command-line-reference
   configuration

.. _lsst.ap.verify-contributing:

Contributing
============

``lsst.ap.verify`` is developed at https://github.com/lsst/ap_verify.
You can find Jira issues for this module under the `ap_verify <https://jira.lsstcorp.org/issues/?jql=project%20%3D%20DM%20AND%20component%20%3D%20ap_verify>`_ component.

.. toctree::
   :maxdepth: 1

   new-metrics

.. _lsst.ap.verify-pyapi:

Python API reference
====================

.. automodapi:: lsst.ap.verify
   :no-main-docstr:
   :no-inheritance-diagram:
.. automodapi:: lsst.ap.verify.measurements
   :no-main-docstr:
   :no-inheritance-diagram:
