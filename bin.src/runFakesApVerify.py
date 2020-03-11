#!/usr/bin/env python
#
# This file is part of ap_verify.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (http://www.lsst.org).
# See the COPYRIGHT file at the top-level directory of this distribution
# for details of code ownership.
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
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
#


import argparse

from astropy.table import Table
from glob import glob
import numpy as np
from subprocess import Popen

from lsst.daf.persistence import Butler
from lsst.utils import getPackageDir


# Specify variables.

# Configurables to make:
#     number of jobs

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--nFakes", default=180000, type=int,
                        help="Number of fakes to produce over each tract. "
                             "180k is roughly LSST DiaSource density.")
    parser.add_argument("--output", type=str,
                        help="Full or relative path directory to output "
                             "injected fakes.")
    parser.add_argument("--mag_min", type=float, default=19.0,
                        help="Minimum magnitude for fakes. Distributed "
                             "uniformly in magnitude.")
    parser.add_argument("--mag_max", type=float, default=24.0,
                        help="Minimum magnitude for fakes. Distributed "
                             "uniformly in magnitude.")
    parser.add_argument("--mag_scatter", type=float, default=0.02,
                        help="Factional scatter to add to calexp injected "
                             "fakes.")
    parser.add_argument("--nJobs", default=24, type=int,
                        help="Number of parallel processes to use.")
    args = parser.parse_args()
    nFakes = args.nFakes
    output = args.output
    nJobs = args.nJobs
    mag_min = args.mag_min
    mag_max = args.mag_max
    mag_scatter = args.mag_scatter

    subObjects = int(nFakes / 3)
    # Create initial ap_verify dataset
    job = Popen("ap_verify.py --dataset HiTS2015 --id filter=g "
                "--output %s -j% i" % (output, nJobs),
                shell=True)
    job.wait()

    # Grab the SkyMap of the hits2015 dataset.
    b = Butler(getPackageDir("ap_verify_hits2015") + "/templates")
    skyMap = b.get("deepCoadd_skyMap")

    # Data only has one tract but we'll keep this loop for later flexibility.
    for tract in [0]:
        # Create random RA/DEC locations over the tract.
        tractPoly = skyMap.generateTract(tract).getInnerSkyPolygon()
        decs = np.arcsin(np.random.uniform(
            np.sin(tractPoly.getBoundingBox().getLat().getA().asRadians()),
            np.sin(tractPoly.getBoundingBox().getLat().getB().asRadians()),
            nFakes))
        ras = np.random.uniform(
            tractPoly.getBoundingBox().getLon().getA().asRadians(),
            tractPoly.getBoundingBox().getLon().getB().asRadians(),
            nFakes)

        # Create Random magnitudes
        # Make min amd max configurable.
        mags = np.random.uniform(mag_min, mag_max, nFakes)

        # We split the initial data into parts.
        # two thirds are in the calexp, two thirds are in the coadd. An
        # lapping third is created in both for variables.

        # The calexps are made with an extra 2% scatter on the magnitudes.
        calExpTable = Table(
            {"raJ2000": ras[:(2 * subObjects)],
             "decJ2000": decs[:(2 * subObjects)],
             "DiskHalfLightRadius": np.ones(2 * subObjects, dtype="float"),
             "BulgeHalfLightRadius": np.ones(2 * subObjects, dtype="float"),
             "gmagVar":
                 (mags[:(2 * subObjects)] *
                  (1 + np.random.uniform(-mag_scatter,
                                         mag_scatter,
                                         2 * subObjects))),
             "disk_n": np.full(2 * subObjects, 1.0),
             "bulge_n": np.full(2 * subObjects, 1.0),
             "a_d": np.full(2 * subObjects, 1.0),
             "a_b": np.full(2 * subObjects, 1.0),
             "b_d": np.full(2 * subObjects, 1.0),
             "b_b": np.full(2 * subObjects, 1.0),
             "pa_disk": np.full(2 * subObjects, 1.0),
             "pa_bluge": np.full(2 * subObjects, 1.0),
             "sourceType": ["star"] * (2 * subObjects)})

        # Create the coadd fakes.
        coaddTable = Table(
            {"raJ2000": ras[subObjects:],
             "decJ2000": decs[subObjects:],
             "DiskHalfLightRadius": np.ones(2 * subObjects, dtype="float"),
             "BulgeHalfLightRadius": np.ones(2 * subObjects, dtype="float"),
             "gmagVar": mags[subObjects:],
             "disk_n": np.full(2 * subObjects, 1.0),
             "bulge_n": np.full(2 * subObjects, 1.0),
             "a_d": np.full(2 * subObjects, 1.0),
             "a_b": np.full(2 * subObjects, 1.0),
             "b_d": np.full(2 * subObjects, 1.0),
             "b_b": np.full(2 * subObjects, 1.0),
             "pa_disk": np.full(2 * subObjects, 1.0),
             "pa_bluge": np.full(2 * subObjects, 1.0),
             "sourceType": ["star"] * (2 * subObjects)})

        # Write out the data.
        calExpTable.write(
            "%s/calexpFakesTract%i.csv" % (output, tract),
            format="ascii.csv",
            overwrite=True)
        coaddTable.write(
            "%s/coaddFakesTract%i.csv" % (output, tract),
            format="ascii.csv",
            overwrite=True)

        # Insert fakes into the cooads
        coaddJob = Popen(
            "insertFakes.py %s/output --rerun testFakes --id tract=%i filter=g "
            "-c fakeType=%s/coaddFakesTract%i.csv "
            "--clobber-config --clobber-versions" % (output, tract,
                                                     output, tract),
            shell=True)
        # Insert Fakes into the calexps.
        procJobs = []
        for ccd in [1, 3, 4, 5, 6, 7, 8, 9, 10, 11,
                    12, 13, 14, 15, 16, 17, 18, 19, 20, 21,
                    22, 23, 24, 25, 26, 27, 28, 29, 30, 31,
                    32, 33, 34, 35, 36, 37, 38, 39, 40, 41,
                    42, 43, 44, 45, 46, 47, 48, 49, 50, 51,
                    52, 53, 54, 55, 56, 57, 58, 59, 60, 62]:
            procJobs.append(Popen(
                "processCcdWithFakes.py %s/output --rerun testFakes "
                "--id ccdnum=%i tract=%i filter=g "
                "-c insertFakes.fakeType=%s/calexpFakesTract%i.csv "
                "--clobber-config --clobber-versions" % (output,
                                                         ccd,
                                                         tract,
                                                         output,
                                                         tract),
                shell=True))
            if len(procJobs) >= nJobs:
                for job in procJobs:
                    job.wait()
                procJobs = []
        # Wait for processCcdWithFakes jobs to finish.
        for job in procJobs:
            job.wait()
        # Wait for coadds jobs to finish.
        coaddJob.wait()

        coaddFiles = glob("%s/output/rerun/testFakes/deepCoadd/g/%i/*.fits" %
                          (output, tract))
        for coaddFile in coaddFiles:
            job = Popen("cp %s %s" % (coaddFile, coaddFile[:-11] + ".fits"),
                        shell=True)
            job.wait()

    # Copy the DECam mapper file to use fakes instead of regular calexps.
    job = Popen("cp " + getPackageDir("obs_decam") + "/policy/DecamMapperFakes.yaml " +
                getPackageDir("obs_decam") + "/policy/DecamMapper.yaml",
                shell=True)
    job.wait()

    # Create the output Apdb.
    print("sqlite:///%s/output/rerun/testFakes/fakesAssociation.db")
    job = Popen(
        "make_apdb.py -c diaPipe.apdb.isolation_level=READ_UNCOMMITTED "
        "-c diaPipe.apdb.db_url=sqlite:///%s/output/rerun/testFakes/fakesAssociation.db" %
        output,
        shell=True)
    job.wait()

    # Submit ap_pipe jobs.
    apJobs = []
    for ccdnum in [1, 3, 4, 5, 6, 7, 8, 9, 10, 11,
                   12, 13, 14, 15, 16, 17, 18, 19, 20, 21,
                   22, 23, 24, 25, 26, 27, 28, 29, 30, 31,
                   32, 33, 34, 35, 36, 37, 38, 39, 40, 41,
                   42, 43, 44, 45, 46, 47, 48, 49, 50, 51,
                   52, 53, 54, 55, 56, 57, 58, 59, 60, 62]:
        apJobs.append(Popen("ap_pipe.py %s/output --rerun testFakes --id ccdnum=%i filter=g "
                            "--reuse-outputs-from 'ccdProcessor' "
                            "--template %s/output/rerun/testFakes "
                            "-C %s/config/apPipe.py "
                            "-c diaPipe.apdb.db_url=sqlite:///%s"
                            "/output/rerun/testFakes/fakesAssociation.db "
                            "-c diaPipe.apdb.isolation_level=READ_UNCOMMITTED "
                            "-c differencer.coaddName='deep' "
                            "-c differencer.getTemplate.coaddName='deep' "
                            "-c differencer.getTemplate.warpType='direct' "
                            "--clobber-config --clobber-versions" %
                            (output,
                             ccdnum,
                             output,
                             getPackageDir("ap_verify_hits2015"),
                             output),
                            shell=True))
        if len(apJobs) >= nJobs:
            for job in apJobs:
                job.wait()
            apJobs = []
    # Wait for jobs to finish.
    for job in apJobs:
        job.wait()
    # Restore the default DECam mapper behavior.
    Popen("cp " + getPackageDir("obs_decam") + "/policy/DecamMapperDefault.yaml " +
          getPackageDir("obs_decam") + "/policy/DecamMapper.yaml", shell=True)
