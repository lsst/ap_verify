#
# LSST Data Management System
#
# Copyright 2008-2017  AURA/LSST.
#
# This product includes software developed by the
# LSST Project (http://www.lsst.org/).
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the LSST License Statement and
# the GNU General Public License along with this program.  If not,
# see <https://www.lsstcorp.org/LegalNotices/>.
#

"""Run ap_verify on HiTS data
This is a temporary script, to be removed once ap_pipe can handle multiple dataIds
"""

from __future__ import absolute_import, division, print_function

from collections import defaultdict
import glob
import json
import os
import shlex

import lsst.log
from lsst.verify import Job, Measurement
from lsst.ap.verify import runApVerify

visits = [410915, 410929, 410931, 410971, 410985, 410987,
          411021, 411035, 411037, 411055, 411069, 411071, 411255, 411269, 411271,
          411305, 411319, 411321, 411355, 411369, 411371, 411406, 411420, 411422,
          411456, 411470, 411472, 411657, 411671, 411673, 411707, 411721, 411724,
          411758, 411772, 411774, 411808, 411822, 411824, 411858, 411872, 411874,
          412060, 412074, 412076, 412250, 412264, 412266, 412307, 412321,
          412324, 412504, 412518, 412520, 412554, 412568, 412570, 412604,
          412618, 412620, 412654, 412668, 412670, 412704, 412718, 412720,
          413635, 413649, 413651, 413680, 413694, 413696,
          415314, 415328, 415330, 415364, 415378, 415380,
          419791, 419802, 419804, 421590, 421604, 421606]
# CCD 1 has image subtraction problems, CCDs 2 and 61 are missing
ccds = list(range(3, 61)) + [62]    # range does not support concatenation in Python 3


def processImage(visit, ccd):
    """Run the AP pipeline for one CCD of one visit, and collect measurements.

    The output is a .verify.json file labelled with the visit and CCD number.
    The function may also leave temporary files behind.

    Returns
    -------
    The job file of the last run (needed for certain metrics).
    """
    try:
        dataId = 'visit=%d ccdnum=%d filter=g' % (visit, ccd)
        args = '--dataset HiTS2015 --output temp/ --dataIdString "%s" --silent' % dataId
        runApVerify(shlex.split(args))
    finally:
        jobFile = 'ap_verify.verify.json'
        if os.path.isfile(jobFile):
            newFile = 'ap_verify_v%d_c%02d.verify.json' % (visit, ccd)
            os.rename(jobFile, newFile)

    return newFile


def unpersistJob(fileName):
    """Unpersist a Job object from the filename of its serialized form.

    Returns
    -------
    The `lsst.verify.Job` object contained in `fileName`.
    """
    with open(fileName) as handle:
        return Job.deserialize(**json.load(handle))


def merge(jobs, lastJob):
    """Combine measurements from multiple chips or visits.

    Other job properties will be dictionary-merged (i.e., if multiple entries
    are assigned to the same key, only one will be preserved).

    Parameters
    ----------
    jobs: iterable of `lsst.verify.Job`
        The jobs containing data to combine.
    lastJob:
        The job corresponding to the final run of ap_verify.

    Return
    ------
    A single `lsst.verify.Job` object containing merged measurements from
    `jobs`.
    """
    merged = Job.load_metrics_package()
    # Visible Job state:
    #     job.measurements
    #     job.meta
    #     job.metrics (guaranteed by load_metrics_package)
    #     job.specs (guaranteed by load_metrics_package)

    measurementsPerMetric = defaultdict(list)
    for job in jobs:
        for metricName in job.measurements:
            measurementsPerMetric[str(metricName)].append(job.measurements[metricName])

    for metric in measurementsPerMetric:
        # Running times, object counts
        if metric.endswith("Time") or metric in {
                "ip_diffim.numSciSources",
                "association.numNewDiaObjects",
                "association.totalUnassociatedDiaObjects"}:
            addIfDefined(merged.measurements,
                         sumMeasurements(measurementsPerMetric[metric]))

    # Fractions require special handling
    addIfDefined(
        merged.measurements,
        # Due to time constraints, no metric for total DIAObjects was implemented,
        # so we have to work around its absence
        mergeFractionsPartial(
            measurementsPerMetric["association.fracUpdatedDiaObjects"],
            measurementsPerMetric["association.numUnassociatedDiaObjects"]))
    addIfDefined(
        merged.measurements,
        mergeFractions(
            measurementsPerMetric["ip_diffim.fracDiaSourcesToSciSources"],
            measurementsPerMetric["ip_diffim.numSciSources"]))

    # L1 database metrics are cumulative, not per-CCD, so just copy them over
    for metric in ["association.totalUnassociatedDiaObjects"]:
        if metric in lastJob.measurements:
            addIfDefined(merged.measurements, lastJob.measurements[metric])

    for job in jobs:
        merged.meta.update(job.meta)

    return merged


def addIfDefined(measurementSet, measurement):
    """Adds a measurement to a set if it is not None, else does nothing.
    """
    if measurement is not None:
        measurementSet.insert(measurement)


def sumMeasurements(measurements):
    """Adds the values of some measurements.

    Extras and notes will be dictionary-merged (i.e., if multiple extras or
    notes are assigned to the same key, only one will be preserved).

    .. warning::

       This function does NOT perform input validation

    Parameters
    ----------
    measurements: iterable of `lsst.verify.Measurement`
        The measurements to add. Must be measurements of the same metric.

    Returns
    -------
    A Measurement containing the sum of `measurements`, and as much auxiliary
    data as could be reasonably saved.
    """
    # Visible Measurement state:
    #     measurement.blobs (assumed to only contain extras)
    #     measurement.extras
    #     measurement.metric
    #     measurement.metric_name
    #     measurement.notes
    #     measurement.quantity
    if not measurements:
        return None

    extras = {}
    notes = {}
    metric = None
    quantity = 0
    for measurement in measurements:
        if metric is None:
            if measurement.metric is not None:
                metric = measurement.metric
            else:
                metric = measurement.metric_name
        quantity += measurement.quantity
        extras.update(measurement.extras)
        notes.update(measurement.notes)

    return Measurement(metric, quantity, extras=extras, notes=notes)


def mergeFractions(fractions, denominators):
    """Weighted sum of some fractions.

    Extras and notes will be dictionary-merged (i.e., if multiple extras or
    notes are assigned to the same key, only one will be preserved) from
    `fractions`.

    .. warning::

       This function does NOT perform input validation

    Parameters
    ----------
    fractions: list of `lsst.verify.Measurement`
        The measurements to combine. Must be measurements of the same metric.
    denominators: list of `lsst.verify.Measurement`
        The denominators of `fractions`. Must be measurements of the same
        metric, and must have a one-to-one correspondence with `fractions`.

    Returns
    -------
    A Measurement containing the average of `fractions`, weighted by
    `denominators`, and as much auxiliary data as could be reasonably saved.
    """
    if not fractions:
        return None

    extras = {}
    notes = {}
    metric = None
    numerator = 0
    denominator = 0
    for fraction, weight in zip(fractions, denominators):
        if metric is None:
            if fraction.metric is not None:
                metric = fraction.metric
            else:
                metric = fraction.metric_name
        numerator += fraction.quantity * weight.quantity
        denominator += weight.quantity
        extras.update(fraction.extras)
        notes.update(fraction.notes)

    return Measurement(metric, numerator / denominator, extras=extras, notes=notes)


def mergeFractionsPartial(fractions, denominatorsMinusNumerators):
    """Weighted sum of some fractions.

    Extras and notes will be dictionary-merged (i.e., if multiple extras or
    notes are assigned to the same key, only one will be preserved) from
    `fractions`.

    .. warning::

       This function does NOT perform input validation

    Parameters
    ----------
    fractions: list of `lsst.verify.Measurement`
        The measurements to combine. Must be measurements of the same metric.
    denominatorsMinusNumerators: list of `lsst.verify.Measurement`
        The denominators of `fractions` minus the numerators. Must be
        measurements of the same metric, and must have a one-to-one
        correspondence with `fractions`.

    Returns
    -------
    A Measurement containing the average of `fractions`, weighted
    appropriately, and as much auxiliary data as could be reasonably saved.
    """
    denominators = []
    for fraction, partial in zip(fractions, denominatorsMinusNumerators):
        denominator = partial.quantity / (1.0 - fraction.quantity)
        denominators.append(
            Measurement(partial.metric, denominator, extras=partial.extras, notes=partial.notes))

    return mergeFractions(fractions, denominators)


if __name__ == "__main__":
    lsst.log.configure()
    log = lsst.log.Log.getLogger('ap.verify.VTCrunner')
    for visit in visits:
        for ccd in ccds:
            lastFile = processImage(visit, ccd)
        log.info("Processed visit %d" % visit)

    jobs = [unpersistJob(f) for f in glob.glob('ap_verify_v*_c*.verify.json')]
    lastJob = unpersistJob(lastFile)

    finalJob = merge(jobs, lastJob)
    finalFile = 'ap_verify.verify.json'
    finalJob.write(finalFile)
    log.info("Measurements have been written to %s." % finalFile)
