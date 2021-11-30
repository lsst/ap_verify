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

import shutil
import tempfile
import unittest

import lsst.utils.tests
import lsst.geom
import lsst.afw.image as afwImage
import lsst.afw.math as afwMath
import lsst.afw.table as afwTable
import lsst.daf.butler.tests as butlerTests
import lsst.pipe.base.testUtils as pipelineTests
from lsst.ap.verify.testPipeline import MockIsrTask, MockCharacterizeImageTask, \
    MockCalibrateTask


class MockTaskTestSuite(unittest.TestCase):
    """Test that mock tasks have the correct inputs and outputs for the task
    they are replacing.

    These tests assume that the mock tasks use real config and connection
    classes, and therefore out-of-date mocks won't match their connections.
    """

    @classmethod
    def setUpClass(cls):
        super().setUpClass()

        repoDir = tempfile.mkdtemp()
        cls.addClassCleanup(shutil.rmtree, repoDir, ignore_errors=True)
        cls.repo = butlerTests.makeTestRepo(repoDir)

        INSTRUMENT = "notACam"
        VISIT = 42
        CCD = 101
        HTM = 42
        # Mock instrument by hand, because some tasks care about parameters
        instrumentRecord = cls.repo.registry.dimensions["instrument"].RecordClass(
            name=INSTRUMENT, visit_max=256, exposure_max=256, detector_max=128)
        cls.repo.registry.syncDimensionData("instrument", instrumentRecord)
        butlerTests.addDataIdValue(cls.repo, "exposure", VISIT)
        butlerTests.addDataIdValue(cls.repo, "visit", VISIT)
        butlerTests.addDataIdValue(cls.repo, "detector", CCD)

        cls.exposureId = cls.repo.registry.expandDataId(
            {"instrument": INSTRUMENT, "exposure": VISIT, "detector": CCD})
        cls.visitId = cls.repo.registry.expandDataId(
            {"instrument": INSTRUMENT, "visit": VISIT, "detector": CCD})
        cls.htmId = cls.repo.registry.expandDataId({"htm7": HTM})

        butlerTests.addDatasetType(cls.repo, "postISRCCD", cls.exposureId.keys(), "Exposure")
        butlerTests.addDatasetType(cls.repo, "icExp", cls.visitId.keys(), "ExposureF")
        butlerTests.addDatasetType(cls.repo, "icSrc", cls.visitId.keys(), "SourceCatalog")
        butlerTests.addDatasetType(cls.repo, "icExpBackground", cls.visitId.keys(), "Background")
        butlerTests.addDatasetType(cls.repo, "cal_ref_cat", cls.htmId.keys(), "SimpleCatalog")
        butlerTests.addDatasetType(cls.repo, "calexp", cls.visitId.keys(), "ExposureF")
        butlerTests.addDatasetType(cls.repo, "src", cls.visitId.keys(), "SourceCatalog")
        butlerTests.addDatasetType(cls.repo, "calexpBackground", cls.visitId.keys(), "Background")
        butlerTests.addDatasetType(cls.repo, "srcMatch", cls.visitId.keys(), "Catalog")
        butlerTests.addDatasetType(cls.repo, "srcMatchFull", cls.visitId.keys(), "Catalog")

    def setUp(self):
        super().setUp()
        self.butler = butlerTests.makeTestCollection(self.repo, uniqueId=self.id())

    def testMockIsr(self):
        # Testing MockIsrTask is tricky because the real ISR has an unstable
        # interface with dozens of potential inputs, too many to pass through
        # runTestQuantum. I don't see a good way to test the inputs;
        # fortunately, this is unlikely to matter for the overall goal of
        # testing ap_verify's interaction with the AP pipeline.
        task = MockIsrTask()
        pipelineTests.assertValidInitOutput(task)
        result = task.run(afwImage.ExposureF())
        pipelineTests.assertValidOutput(task, result)
        # Skip runTestQuantum

    def testMockCharacterizeImageTask(self):
        task = MockCharacterizeImageTask()
        pipelineTests.assertValidInitOutput(task)
        result = task.run(afwImage.ExposureF())
        pipelineTests.assertValidOutput(task, result)

        self.butler.put(afwImage.ExposureF(), "postISRCCD", self.exposureId)
        quantum = pipelineTests.makeQuantum(
            task, self.butler, self.visitId,
            {"exposure": self.exposureId,
             "characterized": self.visitId,
             "sourceCat": self.visitId,
             "backgroundModel": self.visitId,
             })
        pipelineTests.runTestQuantum(task, self.butler, quantum, mockRun=False)

    def testMockCalibrateTask(self):
        task = MockCalibrateTask()
        pipelineTests.assertValidInitOutput(task)
        # Even the real CalibrateTask won't pass assertValidOutput, because for
        # some reason the outputs are injected in runQuantum rather than run.

        self.butler.put(afwImage.ExposureF(), "icExp", self.visitId)
        self.butler.put(afwMath.BackgroundList(), "icExpBackground", self.visitId)
        self.butler.put(afwTable.SourceCatalog(), "icSrc", self.visitId)
        self.butler.put(afwTable.SimpleCatalog(), "cal_ref_cat", self.htmId)
        quantum = pipelineTests.makeQuantum(
            task, self.butler, self.visitId,
            {"exposure": self.visitId,
             "background": self.visitId,
             "icSourceCat": self.visitId,
             "astromRefCat": [self.htmId],
             "photoRefCat": [self.htmId],
             "outputExposure": self.visitId,
             "outputCat": self.visitId,
             "outputBackground": self.visitId,
             "matches": self.visitId,
             "matchesDenormalized": self.visitId,
             })
        pipelineTests.runTestQuantum(task, self.butler, quantum, mockRun=False)


class MemoryTester(lsst.utils.tests.MemoryTestCase):
    pass


def setup_module(module):
    lsst.utils.tests.init()


if __name__ == "__main__":
    lsst.utils.tests.init()
    unittest.main()