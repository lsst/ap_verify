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
from multiprocessing import Pool

from astropy.table import Table
import pandas as pd

from lsst.daf.persistence import Butler, NoResults
from lsst.geom import SpherePoint, radians, Box2D
from lsst.sphgeom import ConvexPolygon


visits = [410915, 410971, 411021, 411055, 411255, 411305, 411355, 411406,
          411456, 411657, 411707, 411758, 411808, 411858, 412060, 412250,
          412307, 412504, 412554, 412604, 412654, 412704, 413635, 413680,
          415314, 415364, 419791, 421590, 410929, 410985, 411035, 411069,
          411269, 411319, 411369, 411420, 411470, 411671, 411721, 411772,
          411822, 411872, 412074, 412264, 412321, 412518, 412568, 412618,
          412668, 412718, 413649, 413694, 415328, 415378, 419802, 421604,
          410931, 410987, 411037, 411071, 411271, 411321, 411371, 411422,
          411472, 411673, 411724, 411774, 411824, 411874, 412076, 412266,
          412324, 412520, 412570, 412620, 412670, 412720, 413651, 413696,
          415330, 415380, 419804, 421606]

# CI:
#visits = [411420, 411420, 419802, 419802, 411371, 411371]

ccds = [1, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
        21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37,
        38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54,
        55, 56, 57, 58, 59, 60, 62]


# CI:
#ccds = [5, 10, 56, 60]

def runVisit(data):
    visit = data["visit"]
    repo = data["repo"]
    b = Butler(repo + "/output")
    calexpFakes = Table.read(repo + "/calexpFakesTract0.csv").to_pandas()
    coaddFakes = Table.read(repo + "/coaddFakesTract0.csv").to_pandas()

    calexpInserts = calexpFakes[:60000]
    bothInserts = calexpFakes[60000:]
    coaddInserts = coaddFakes[60000:]

    calexpInserts.loc[:,'where_inserted'] = 'calexp'
    bothInserts.loc[:,'where_inserted'] = 'both'
    coaddInserts.loc[:,'where_inserted'] = 'coadd'
    calexpInserts.loc[:,'visit'] = visit
    bothInserts.loc[:,'visit'] = visit
    coaddInserts.loc[:,'visit'] = visit

    output = []

    for ccd in ccds:
        try:
            diffIm = b.get("deepDiff_differenceExp",
                           visit=visit, ccdnum=ccd, filter="g")
        except NoResults:
            print(f"No data for dataId={visit}, {ccd}")
            continue
        wcs = diffIm.getWcs()
        bbox = Box2D(diffIm.getBBox())

        skyCorners = wcs.pixelToSky(bbox.getCorners())
        region = ConvexPolygon([s.getVector() for s in skyCorners])

        def trim(row):
            coord = SpherePoint(row["raJ2000"], row["decJ2000"], radians)
            return region.contains(coord.getVector())

        wCalexp = calexpInserts.apply(trim, axis=1)
        wBoth = bothInserts.apply(trim, axis=1)
        wCoadd = coaddInserts.apply(trim, axis=1)

        subCalexpInserts = calexpInserts[wCalexp]
        subBothInserts = bothInserts[wBoth]
        subCoaddInserts = coaddInserts[wCoadd]

        subCalexpInserts.loc[:, 'ccd'] = ccd
        subBothInserts.loc[:, 'ccd'] = ccd
        subCoaddInserts.loc[:, 'ccd'] = ccd

        insertedDf = pd.concat([subCalexpInserts,
                                subBothInserts,
                                subCoaddInserts])
        output.append(insertedDf)

    return pd.concat(output)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--repo", type=str,
                        help="Full or relative path directory repository "
                             "where fakes are.")
    parser.add_argument("--nJobs", default=24, type=int,
                        help="Number of parallel processes to use.")
    args = parser.parse_args()

    output = []

    pool = Pool(args.nJobs)
    visit_results = pool.imap_unordered(
        runVisit,
        ({"visit": visit, "repo": args.repo} for visit in visits))
    pool.close()
    pool.join()
    output.extend(visit_results)

    df = pd.concat(output)
    df = df.loc[df.inserted,:]
    df.drop('inserted',axis='columns')
    df.to_parquet(args.repo + "insertCounts.parquet", compression="gzip")
