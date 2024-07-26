.. py:currentmodule:: lsst.ap.verify

.. program:: ap_verify.py

.. _ap-verify-request:

########################################
mpSkyEphemerisQueryTask external request 
########################################

mpSkyEphemerisQueryTask
=======================
``ap_verify`` now runs mpSkyEphemerisQuery, which includes a request to mpSky. 
mpSky is not expected to host ephemerides for all test datasets, so it will 
often (or always) fail. When requests fail, mpSkyEphemerisQuery should return 
an empty DataFrame, and no detections will be associated.
