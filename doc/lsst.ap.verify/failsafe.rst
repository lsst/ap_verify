.. py:currentmodule:: lsst.ap.verify

.. _ap-verify-failsafe:

###########################################
Error-handling in ap_verify and failed runs
###########################################

The Alert Production pipeline may fail for reasons ranging from corrupted data to improperly configured datasets to bugs in the code.
The ``ap_verify`` framework tries to handle failures gracefully to minimize wasted server time and maximize debugging potential.

.. _ap-verify-failsafe-catch:

Error-handling policy
=====================

``ap_verify`` does not attempt to resolve exceptions emitted by pipeline tasks, on the grounds that it does not have enough information about the pipeline implementation to provide any meaningful resolution.
Nor does it try to ignore errors and press forward (although it does not prevent individual tasks from adopting this approach), as doing so tends to lead to cascading failures from an incomplete and possibly corrupted data set.
Terminating on failure allows pipeline problems to be detected quickly during testing, rather than after a day or more of processing.

If a task fails with a fatal error, :command:`ap_verify.py` will clean up and shut down.
In particular, where possible it will :ref:`preserve metrics<ap-verify-failsafe-partialmetric>` computed before the failure point.

.. _ap-verify-failsafe-partialmetric:

Recovering Metrics From Partial Runs
====================================

``ap_verify`` produces some measurements even if the pipeline cannot run to completion.
Specifically, if a task fails, any previously completed tasks that store measurements to disk will have done so.
In addition, if a metric cannot be computed, ``ap_verify`` may attempt to store the values of the remaining metrics.

If the pipeline fails, ``ap_verify`` may not preserve measurements computed from the dataset.
Once the framework for handling metrics is finalized, ``ap_verify`` may be able to offer a broader guarantee that does not depend on how or where any individual metric is implemented.
