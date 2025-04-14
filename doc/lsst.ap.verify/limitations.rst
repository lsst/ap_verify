.. py:currentmodule:: lsst.ap.verify

.. _ap-verify-request:

#################
Known Limitations 
#################

mpSkyEphemerisQueryTask
=======================

The AP pipelines now run :py:class:`lsst.ap.association.MPSkyEphemerisQueryTask`, which includes a request to mpSky.
mpSky is not expected to host ephemerides for all test datasets, (or be reachable from Jenkins) so it will often (or always) fail.
When requests fail, mpSkyEphemerisQuery will return nothing, and associateApdb will skip ssoAssociation.
