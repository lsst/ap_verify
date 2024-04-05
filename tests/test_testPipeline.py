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

import pandas

import lsst.utils.tests
import lsst.geom
import lsst.afw.image as afwImage
import lsst.afw.math as afwMath
import lsst.afw.table as afwTable
import lsst.skymap
import lsst.daf.butler.tests as butlerTests
import lsst.pipe.base.testUtils as pipelineTests
from lsst.ap.verify.testPipeline import MockIsrTask, MockCharacterizeImageTask, \
    MockCalibrateTask, MockGetTemplateTask, \
    MockAlardLuptonSubtractTask, MockDetectAndMeasureTask, MockTransformDiaSourceCatalogTask, \
    MockRBTransiNetTask, MockDiaPipelineTask, MockFilterDiaSourceCatalogTask


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

        INSTRUMENT = "DummyCam"
        VISIT = 42
        CCD = 101
        HTM = 42
        SKYMAP = "TreasureMap"
        TRACT = 28
        PATCH = 4
        BAND = 'k'
        PHYSICAL = 'k2022'
        SUB_FILTER = 9
        # Mock instrument by hand, because some tasks care about parameters
        instrumentRecord = cls.repo.dimensions["instrument"].RecordClass(
            name=INSTRUMENT, visit_max=256, exposure_max=256, detector_max=128,
            class_name="lsst.obs.base.instrument_tests.DummyCam",
        )
        cls.repo.registry.syncDimensionData("instrument", instrumentRecord)
        butlerTests.addDataIdValue(cls.repo, "physical_filter", PHYSICAL, band=BAND)
        butlerTests.addDataIdValue(cls.repo, "subfilter", SUB_FILTER)
        butlerTests.addDataIdValue(cls.repo, "exposure", VISIT)
        butlerTests.addDataIdValue(cls.repo, "visit", VISIT)
        butlerTests.addDataIdValue(cls.repo, "detector", CCD)
        butlerTests.addDataIdValue(cls.repo, "skymap", SKYMAP)
        butlerTests.addDataIdValue(cls.repo, "tract", TRACT)
        butlerTests.addDataIdValue(cls.repo, "patch", PATCH)

        cls.emptyId = cls.repo.registry.expandDataId({})
        cls.exposureId = cls.repo.registry.expandDataId(
            {"instrument": INSTRUMENT, "exposure": VISIT, "detector": CCD})
        cls.visitId = cls.repo.registry.expandDataId(
            {"instrument": INSTRUMENT, "visit": VISIT, "detector": CCD})
        cls.visitOnlyId = cls.repo.registry.expandDataId(
            {"instrument": INSTRUMENT, "visit": VISIT})
        cls.skymapId = cls.repo.registry.expandDataId({"skymap": SKYMAP})
        cls.skymapVisitId = cls.repo.registry.expandDataId(
            {"instrument": INSTRUMENT, "visit": VISIT, "detector": CCD, "skymap": SKYMAP})
        cls.patchId = cls.repo.registry.expandDataId(
            {"skymap": SKYMAP, "tract": TRACT, "patch": PATCH, "band": BAND})
        cls.subfilterId = cls.repo.registry.expandDataId(
            {"skymap": SKYMAP, "tract": TRACT, "patch": PATCH, "band": BAND, "subfilter": SUB_FILTER})
        cls.htmId = cls.repo.registry.expandDataId({"htm7": HTM})

        butlerTests.addDatasetType(cls.repo, "postISRCCD", cls.exposureId.dimensions, "Exposure")
        butlerTests.addDatasetType(cls.repo, "icExp", cls.visitId.dimensions, "ExposureF")
        butlerTests.addDatasetType(cls.repo, "icSrc", cls.visitId.dimensions, "SourceCatalog")
        butlerTests.addDatasetType(cls.repo, "icExpBackground", cls.visitId.dimensions, "Background")
        butlerTests.addDatasetType(cls.repo, "gaia_dr3_20230707", cls.htmId.dimensions, "SimpleCatalog")
        butlerTests.addDatasetType(cls.repo, "ps1_pv3_3pi_20170110", cls.htmId.dimensions, "SimpleCatalog")
        butlerTests.addDatasetType(cls.repo, "calexp", cls.visitId.dimensions, "ExposureF")
        butlerTests.addDatasetType(cls.repo, "src", cls.visitId.dimensions, "SourceCatalog")
        butlerTests.addDatasetType(cls.repo, "calexpBackground", cls.visitId.dimensions, "Background")
        butlerTests.addDatasetType(cls.repo, "srcMatch", cls.visitId.dimensions, "Catalog")
        butlerTests.addDatasetType(cls.repo, "srcMatchFull", cls.visitId.dimensions, "Catalog")
        butlerTests.addDatasetType(cls.repo, lsst.skymap.BaseSkyMap.SKYMAP_DATASET_TYPE_NAME,
                                   cls.skymapId.dimensions, "SkyMap")
        butlerTests.addDatasetType(cls.repo, "goodSeeingCoadd", cls.patchId.dimensions, "ExposureF")
        butlerTests.addDatasetType(cls.repo, "deepDiff_differenceExp", cls.visitId.dimensions, "ExposureF")
        butlerTests.addDatasetType(cls.repo, "deepDiff_differenceTempExp", cls.visitId.dimensions,
                                   "ExposureF")
        butlerTests.addDatasetType(cls.repo, "deepDiff_templateExp", cls.visitId.dimensions, "ExposureF")
        butlerTests.addDatasetType(cls.repo, "goodSeeingDiff_templateExp", cls.visitId.dimensions,
                                   "ExposureF")
        butlerTests.addDatasetType(cls.repo, "deepDiff_matchedExp", cls.visitId.dimensions, "ExposureF")
        butlerTests.addDatasetType(cls.repo, "deepDiff_diaSrc", cls.visitId.dimensions, "SourceCatalog")
        butlerTests.addDatasetType(cls.repo, "deepDiff_diaSrcTable", cls.visitId.dimensions, "DataFrame")
        butlerTests.addDatasetType(cls.repo, "deepDiff_spatiallySampledMetrics", cls.visitId.dimensions,
                                   "ArrowAstropy")
        butlerTests.addDatasetType(cls.repo, "deepDiff_candidateDiaSrc", cls.visitId.dimensions,
                                   "SourceCatalog")
        butlerTests.addDatasetType(cls.repo, "visitSsObjects", cls.visitOnlyId.dimensions, "DataFrame")
        butlerTests.addDatasetType(cls.repo, "apdb_marker", cls.visitId.dimensions, "Config")
        butlerTests.addDatasetType(cls.repo, "deepDiff_assocDiaSrc", cls.visitId.dimensions, "DataFrame")
        butlerTests.addDatasetType(cls.repo, "deepDiff_longTrailedSrc", cls.visitId.dimensions, "DataFrame")
        butlerTests.addDatasetType(cls.repo, "deepRealBogusSources", cls.visitId.dimensions, "Catalog")
        butlerTests.addDatasetType(cls.repo, "deepDiff_diaForcedSrc", cls.visitId.dimensions, "DataFrame")
        butlerTests.addDatasetType(cls.repo, "deepDiff_diaObject", cls.visitId.dimensions, "DataFrame")

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
        self.butler.put(afwTable.SimpleCatalog(), "gaia_dr3_20230707", self.htmId)
        self.butler.put(afwTable.SimpleCatalog(), "ps1_pv3_3pi_20170110", self.htmId)
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

    def testMockGetTemplateTask(self):
        task = MockGetTemplateTask()
        pipelineTests.assertValidInitOutput(task)
        result = task.run(coaddExposures=[afwImage.ExposureF()],
                          bbox=lsst.geom.Box2I(),
                          wcs=None,  # Should not be allowed, but too hard to fake a SkyWcs
                          dataIds=[],
                          )
        pipelineTests.assertValidOutput(task, result)

        self.butler.put(afwImage.ExposureF(), "calexp", self.visitId)
        skymap = lsst.skymap.DiscreteSkyMap(lsst.skymap.DiscreteSkyMapConfig())
        self.butler.put(skymap, lsst.skymap.BaseSkyMap.SKYMAP_DATASET_TYPE_NAME, self.skymapId)
        self.butler.put(afwImage.ExposureF(), "goodSeeingCoadd", self.patchId)
        quantum = pipelineTests.makeQuantum(
            task, self.butler, self.skymapVisitId,
            {"bbox": self.visitId,
             "wcs": self.visitId,
             "skyMap": self.skymapId,
             "coaddExposures": [self.patchId],
             "template": self.visitId,
             })
        pipelineTests.runTestQuantum(task, self.butler, quantum, mockRun=False)

    def testMockAlardLuptonSubtractTask(self):
        task = MockAlardLuptonSubtractTask()
        pipelineTests.assertValidInitOutput(task)
        result = task.run(afwImage.ExposureF(), afwImage.ExposureF(), afwTable.SourceCatalog())
        pipelineTests.assertValidOutput(task, result)

        self.butler.put(afwImage.ExposureF(), "deepDiff_templateExp", self.visitId)
        self.butler.put(afwImage.ExposureF(), "calexp", self.visitId)
        self.butler.put(afwTable.SourceCatalog(), "src", self.visitId)
        quantum = pipelineTests.makeQuantum(
            task, self.butler, self.visitId,
            {"template": self.visitId,
             "science": self.visitId,
             "sources": self.visitId,
             "difference": self.visitId,
             "matchedTemplate": self.visitId,
             })
        pipelineTests.runTestQuantum(task, self.butler, quantum, mockRun=False)

    def testMockDetectAndMeasureTask(self):
        task = MockDetectAndMeasureTask()
        pipelineTests.assertValidInitOutput(task)
        result = task.run(science=afwImage.ExposureF(),
                          matchedTemplate=afwImage.ExposureF(),
                          difference=afwImage.ExposureF(),
                          )
        pipelineTests.assertValidOutput(task, result)

        self.butler.put(afwImage.ExposureF(), "calexp", self.visitId)
        self.butler.put(afwImage.ExposureF(), "deepDiff_matchedExp", self.visitId)
        self.butler.put(afwImage.ExposureF(), "deepDiff_differenceTempExp", self.visitId)
        self.butler.put(afwTable.SourceCatalog(), "src", self.visitId)
        quantum = pipelineTests.makeQuantum(
            task, self.butler, self.visitId,
            {"science": self.visitId,
             "matchedTemplate": self.visitId,
             "difference": self.visitId,
             "diaSources": self.visitId,
             "subtractedMeasuredExposure": self.visitId,
             "spatiallySampledMetrics": self.visitId,
             })
        pipelineTests.runTestQuantum(task, self.butler, quantum, mockRun=False)

    def testMockFilterDiaSourceCatalogTask(self):
        task = MockFilterDiaSourceCatalogTask()
        pipelineTests.assertValidInitOutput(task)
        result = task.run(afwTable.SourceCatalog())
        pipelineTests.assertValidOutput(task, result)

    def testMockRBTransiNetTask(self):
        task = MockRBTransiNetTask()
        pipelineTests.assertValidInitOutput(task)
        result = task.run(template=afwImage.ExposureF(),
                          science=afwImage.ExposureF(),
                          difference=afwImage.ExposureF(),
                          diaSources=afwTable.SourceCatalog(),
                          )
        pipelineTests.assertValidOutput(task, result)

        self.butler.put(afwImage.ExposureF(), "calexp", self.visitId)
        self.butler.put(afwImage.ExposureF(), "deepDiff_differenceExp", self.visitId)
        self.butler.put(afwImage.ExposureF(), "deepDiff_templateExp", self.visitId)
        self.butler.put(afwTable.SourceCatalog(), "deepDiff_candidateDiaSrc", self.visitId)
        quantum = pipelineTests.makeQuantum(
            task, self.butler, self.visitId,
            {"template": self.visitId,
             "science": self.visitId,
             "difference": self.visitId,
             "diaSources": self.visitId,
             "pretrainedModel": self.emptyId,
             "classifications": self.visitId,
             })
        pipelineTests.runTestQuantum(task, self.butler, quantum, mockRun=False)

    def testMockTransformDiaSourceCatalogTask(self):
        task = MockTransformDiaSourceCatalogTask(initInputs=afwTable.SourceCatalog())
        pipelineTests.assertValidInitOutput(task)
        result = task.run(afwTable.SourceCatalog(), afwImage.ExposureF(), 'k', 42)
        pipelineTests.assertValidOutput(task, result)

        self.butler.put(afwTable.SourceCatalog(), "deepDiff_candidateDiaSrc", self.visitId)
        self.butler.put(afwImage.ExposureF(), "deepDiff_differenceExp", self.visitId)
        quantum = pipelineTests.makeQuantum(
            task, self.butler, self.visitId,
            {"diaSourceCat": self.visitId,
             "diffIm": self.visitId,
             "diaSourceTable": self.visitId,
             })
        pipelineTests.runTestQuantum(task, self.butler, quantum, mockRun=False)

    def testMockDiaPipelineTask(self):
        config = MockDiaPipelineTask.ConfigClass()
        config.apdb.db_url = "testing_only"
        task = MockDiaPipelineTask(config=config)
        pipelineTests.assertValidInitOutput(task)
        result = task.run(pandas.DataFrame(), pandas.DataFrame(), afwImage.ExposureF(),
                          afwImage.ExposureF(), afwImage.ExposureF(), 42, 'k')
        pipelineTests.assertValidOutput(task, result)

        self.butler.put(pandas.DataFrame(), "deepDiff_diaSrcTable", self.visitId)
        self.butler.put(pandas.DataFrame(), "visitSsObjects", self.visitId)
        self.butler.put(afwImage.ExposureF(), "deepDiff_differenceExp", self.visitId)
        self.butler.put(afwImage.ExposureF(), "calexp", self.visitId)
        self.butler.put(afwImage.ExposureF(), "deepDiff_templateExp", self.visitId)
        quantum = pipelineTests.makeQuantum(
            task, self.butler, self.visitId,
            {"diaSourceTable": self.visitId,
             "solarSystemObjectTable": self.visitId,
             "diffIm": self.visitId,
             "exposure": self.visitId,
             "template": self.visitId,
             "apdbMarker": self.visitId,
             "associatedDiaSources": self.visitId,
             "longTrailedSources": self.visitId,
             "diaForcedSources": self.visitId,
             "diaObjects": self.visitId,
             })
        pipelineTests.runTestQuantum(task, self.butler, quantum, mockRun=False)


class MemoryTester(lsst.utils.tests.MemoryTestCase):
    pass


def setup_module(module):
    lsst.utils.tests.init()


if __name__ == "__main__":
    lsst.utils.tests.init()
    unittest.main()
