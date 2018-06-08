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

import os
import shutil
import glob
import tempfile
import argparse
import unittest
from collections import defaultdict

from lsst.utils import getPackageDir
import lsst.utils.tests
import lsst.pipe.base as pipeBase
import lsst.pipe.tasks as pipeTasks
import lsst.pex.exceptions as pexExcept
import lsst.ap.verify.ingestion as ingestion


class _RepoStub:
    """Simplified representation of a repository.

    While this object can be read as if it were a `lsst.daf.persistence.Butler`,
    it has some extra input methods to avoid needing a separate registry stub.

    Note that this object does *not* support extraction of actual datasets using
    ``get``; only registry queries are allowed.
    """

    def __init__(self):
        self._datasets = defaultdict(list)    # datasetType to filenames
        self._metadata = {}                   # filename to metadata

    def registerFile(self, filename, datasetType, metadata):
        """Register a file and its metadata internally so that it can be queried later.

        Parameters
        ----------
        filename : `str`
            The name of a file being "ingested".
        datasetType : `str`
            The Butler data type of ``filename``.
        metadata : `dict` from `str` to any
            The metadata columns to be associated with ``filename``.
        """
        self._datasets[datasetType].append(filename)
        self._metadata[filename] = metadata

    def queryMetadata(self, datasetType, queryFormat, dataId=None, **rest):
        if dataId is None:
            dataId = {}
        dataId.update(**rest)

        candidateFiles = self._datasets[datasetType]
        result = []
        for filename in candidateFiles:
            if self._fileMatches(filename, dataId):
                metadata = self._metadata[filename]
                if isinstance(queryFormat, str):
                    result.append(metadata[queryFormat])
                else:
                    result.append(tuple(metadata[key] for key in queryFormat))
        return result

    def datasetExists(self, datasetType, dataId=None, **rest):
        if dataId is None:
            dataId = {}
        dataId.update(**rest)

        candidateFiles = self._datasets[datasetType]
        return any(self._fileMatches(file, dataId) for file in candidateFiles)

    def subset(self, datasetType, dataId=None, **rest):
        """Return an iterable of objects supporting a `datasetExists` method.
        """
        if dataId is None:
            dataId = {}
        dataId.update(**rest)

        candidateFiles = self._datasets[datasetType]
        return [self.dataRef(datasetType, self._metadata[filename])
                for filename in candidateFiles if self._fileMatches(filename, dataId)]

    def _fileMatches(self, filename, dataId):
        """Test whether a file can be described by a particular (partial) data ID.
        """
        metadata = self._metadata[filename]
        for key, value in dataId.items():
            if key in metadata and value != metadata[key]:
                return False
        return True

    def dataRef(self, datasetType, dataId=None, **rest):
        """Return an object supporting a `datasetExists` method.
        """
        if dataId is None:
            dataId = {}
        dataId.update(**rest)
        parent = self

        class Temp:
            def datasetExists(self):
                return parent.datasetExists(datasetType, dataId)

        return Temp()


class _ArgumentParserStub(argparse.ArgumentParser):
    """Emulation of `lsst.pipe.tasks.IngestArgumentParser` and
    `lsst.pipe.tasks.IngestCalibsArgumentParser` that does not create a
    Butler as a side effect.
    """

    def __init__(self, name, *args, **kwargs):
        # _ArgumentParserStub is not an pipeBase.ArgumentParser; use composition instead of inheritance
        standardArgs = pipeBase.ArgumentParser(self, name, *args, **kwargs)

        argparse.ArgumentParser.__init__(self, parents=[standardArgs], add_help=False)
        self.add_argument("--mode", choices=["move", "copy", "link", "skip"], default="link",
                          help="Mode of delivering the files to their destination")
        self.add_argument("--validity", type=int, help="Calibration validity period (days)")
        self.add_argument("--calibType", type=str, default=None,
                          choices=[None, "bias", "dark", "flat", "fringe", "sky", "defect"],
                          help="Type of the calibration data to be ingested;" +
                               " if omitted, the type is determined from" +
                               " the file header information")
        self.add_argument("--ignore-ingested", dest="ignoreIngested", action="store_true",
                          help="Don't register files that have already been registered")
        self.add_argument("--badFile", nargs="*", default=[],
                          help="Names of bad files (no path; wildcards allowed)")
        self.add_argument("files", nargs="+", help="Names of file")

    def parse_args(self, config, args=None, log=None, override=None):
        return argparse.ArgumentParser.parse_args(self, args=args)


class _IngestTaskStub(pipeBase.Task):
    """Simplified version of an ingestion task that avoids Butler operations.
    """
    ConfigClass = pipeTasks.ingest.IngestConfig
    ArgumentParser = _ArgumentParserStub

    repo = None
    """Dummy "repository" that registers "ingested" files (`_RepoStub`).

    This object should be shared with other code that will read or write the same files.
    As a result, this attribute is never assigned to by this object's code.
    """

    def __init__(self, *args, **kwargs):
        pipeBase.Task.__init__(self, *args, **kwargs)
        self.makeSubtask("parse")

    def run(self, args):
        """A substitute for `lsst.pipe.tasks.IngestTask.run` that registers
        files with a `_RepoStub`.

        `self.repo` MUST be initialized before this method is called.
        """
        for file in _IngestTaskStub._expandFiles(args.files):
            if os.path.basename(file) in args.badFile:
                continue

            if args.calibType:
                datasetType = args.calibType
            else:
                datasetType = self.getType(file)
            fileInfo, _ = self.parse.getInfo(file)

            self.repo.registerFile(file, datasetType, fileInfo)

    @staticmethod
    def _expandFiles(files):
        result = []
        for pattern in files:
            expanded = glob.glob(pattern)
            if expanded:
                result.extend(expanded)
        return result

    def getType(self, _filename):
        return 'raw'


class _IngestCalibsTaskStub(_IngestTaskStub):
    ConfigClass = pipeTasks.ingestCalibs.IngestCalibsConfig
    ArgumentParser = _ArgumentParserStub

    def getType(self, filename):
        return self.parse.getCalibType(filename)


class IngestionTestSuite(lsst.utils.tests.TestCase):

    @classmethod
    def setUpClass(cls):
        try:
            cls.testData = os.path.join(lsst.utils.getPackageDir("obs_test"), 'data', 'input')
        except pexExcept.NotFoundError:
            message = "obs_test not setup. Skipping."
            raise unittest.SkipTest(message)

        obsDir = os.path.join(getPackageDir('obs_test'), 'config')
        cls.config = ingestion.DatasetIngestConfig()
        cls.config.dataIngester.retarget(_IngestTaskStub)
        cls.config.dataIngester.load(os.path.join(obsDir, 'ingest.py'))
        cls.config.calibIngester.retarget(_IngestCalibsTaskStub)
        cls.config.calibIngester.load(os.path.join(obsDir, 'ingestCalibs.py'))
        cls.config.defectIngester.retarget(_IngestCalibsTaskStub)
        cls.config.defectIngester.load(os.path.join(obsDir, 'ingestCalibs.py'))
        cls.config.freeze()

        cls.testApVerifyData = os.path.join('tests', 'ingestion')
        cls.rawDataId = {'visit': 229388, 'ccdnum': 1}

        cls.rawData = [{'file': 'raw_v1_fg.fits.gz', 'visit': 890104911, 'filter': 'g', 'exptime': 15.0},
                       {'file': 'raw_v2_fg.fits.gz', 'visit': 890106021, 'filter': 'g', 'exptime': 15.0},
                       {'file': 'raw_v3_fr.fits.gz', 'visit': 890880321, 'filter': 'r', 'exptime': 15.0},
                       ]
        cls.calibData = [{'type': 'bias', 'file': 'bias.fits.gz', 'filter': '_unknown_'},
                         {'type': 'flat', 'file': 'flat_fg.fits.gz', 'filter': 'g'},
                         {'type': 'flat', 'file': 'flat_fr.fits.gz', 'filter': 'r'},
                         ]

    def setUp(self):
        # Mandatory argument to _doIngest*, used by _doIngestDefects to unpack tar
        self._repo = self._calibRepo = tempfile.mkdtemp()

        self._task = ingestion.DatasetIngestTask(config=IngestionTestSuite.config)
        self._butler = _RepoStub()
        self._task.dataIngester.repo = self._butler
        self._task.calibIngester.repo = self._butler
        self._task.defectIngester.repo = self._butler

    def tearDown(self):
        shutil.rmtree(self._repo, ignore_errors=True)

    def testDataIngest(self):
        """Test that ingesting a science image adds it to a repository.
        """
        testDir = os.path.join(IngestionTestSuite.testData, 'raw')
        files = [os.path.join(testDir, datum['file']) for datum in IngestionTestSuite.rawData]
        self._task._doIngest(self._repo, files, [])

        for datum in IngestionTestSuite.rawData:
            dataId = {'visit': datum['visit']}
            self.assertTrue(self._butler.datasetExists('raw', dataId))
            self.assertEqual(self._butler.queryMetadata('raw', 'filter', dataId),
                             [datum['filter']])
            self.assertEqual(self._butler.queryMetadata('raw', 'expTime', dataId),
                             [datum['exptime']])
        self.assertFalse(_isEmpty(self._butler, 'raw'))
        self.assertFalse(self._butler.datasetExists('flat', filter='g'))

    def testCalibIngest(self):
        """Test that ingesting calibrations adds them to a repository.
        """
        files = [os.path.join(IngestionTestSuite.testData, datum['type'], datum['file'])
                 for datum in IngestionTestSuite.calibData]

        self._task._doIngestCalibs(self._repo, self._calibRepo, files)

        for datum in IngestionTestSuite.calibData:
            self.assertTrue(self._butler.datasetExists(datum['type'], filter=datum['filter']))
            # queryMetadata does not work on calibs
        self.assertFalse(self._butler.datasetExists('flat', filter='z'))

    def testDefectIngest(self):
        """Test that ingesting defects adds them to a repository.
        """
        tarFile = os.path.join(IngestionTestSuite.testApVerifyData, 'defects.tar.gz')

        self._task._doIngestDefects(self._repo, self._calibRepo, tarFile)

        self.assertTrue(self._butler.datasetExists('defect'))

    def testNoFileIngest(self):
        """Test that attempts to ingest nothing raise an exception.
        """
        files = []

        with self.assertRaises(RuntimeError):
            self._task._doIngest(self._repo, files, [])
        with self.assertRaises(RuntimeError):
            self._task._doIngestCalibs(self._repo, self._calibRepo, files)

        self.assertTrue(_isEmpty(self._butler, 'raw'))

    def testBadFileIngest(self):
        """Test that ingestion of raw data ignores blacklisted files.
        """
        badFiles = ['raw_v2_fg.fits.gz']

        testDir = os.path.join(IngestionTestSuite.testData, 'raw')
        files = [os.path.join(testDir, datum['file']) for datum in IngestionTestSuite.rawData]
        self._task._doIngest(self._repo, files, badFiles)

        for datum in IngestionTestSuite.rawData:
            dataId = {'visit': datum['visit']}
            self.assertEqual(self._butler.datasetExists('raw', dataId), datum['file'] not in badFiles)

    def testFindMatchingFiles(self):
        """Test that _findMatchingFiles finds the desired files.
        """
        testDir = os.path.join(IngestionTestSuite.testData)

        self.assertSetEqual(
            ingestion._findMatchingFiles(testDir, ['raw_*.fits.gz']),
            {os.path.join(testDir, f) for f in
             {'raw/raw_v1_fg.fits.gz', 'raw/raw_v2_fg.fits.gz', 'raw/raw_v3_fr.fits.gz'}}
        )
        self.assertSetEqual(
            ingestion._findMatchingFiles(testDir, ['raw_*.fits.gz'], ['*fr*']),
            {os.path.join(testDir, f) for f in {'raw/raw_v1_fg.fits.gz', 'raw/raw_v2_fg.fits.gz'}}
        )
        self.assertSetEqual(
            ingestion._findMatchingFiles(testDir, ['raw_*.fits.gz'], ['*_v?_f?.fits.gz']),
            set()
        )


def _isEmpty(butler, datasetType):
    """Test that a butler repository contains no objects.

    Parameters
    ----------
    datasetType : `str`
        The type of dataset to search for.

    Notes
    -----
    .. warning::
       Does not work for calib datasets, because they're not discoverable.
    """
    possibleDataRefs = butler.subset(datasetType)
    for dataRef in possibleDataRefs:
        if dataRef.datasetExists():
            return False
    return True


class MemoryTester(lsst.utils.tests.MemoryTestCase):
    pass


def setup_module(module):
    lsst.utils.tests.init()


if __name__ == "__main__":
    lsst.utils.tests.init()
    unittest.main()
