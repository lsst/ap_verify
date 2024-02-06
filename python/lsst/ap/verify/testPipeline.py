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


# These classes exist only to be included in a mock pipeline, and don't need
# to be public for that.
__all__ = []


import numpy as np
import pandas

import lsst.geom as geom
import lsst.afw.image as afwImage
import lsst.afw.math as afwMath
import lsst.afw.table as afwTable
from lsst.pipe.base import PipelineTask, Struct
from lsst.ip.isr import IsrTaskConfig
from lsst.ip.diffim import GetTemplateConfig, AlardLuptonSubtractConfig, DetectAndMeasureConfig
from lsst.pipe.tasks.characterizeImage import CharacterizeImageConfig
from lsst.pipe.tasks.calibrate import CalibrateConfig
from lsst.meas.transiNet import RBTransiNetConfig
from lsst.ap.association import TransformDiaSourceCatalogConfig, DiaPipelineConfig


class MockIsrTask(PipelineTask):
    """A do-nothing substitute for IsrTask.
    """
    ConfigClass = IsrTaskConfig
    _DefaultName = "notIsr"

    def run(self, ccdExposure, *, camera=None, bias=None, linearizer=None,
            crosstalk=None, crosstalkSources=None,
            dark=None, flat=None, ptc=None, bfKernel=None, bfGains=None, defects=None,
            fringes=Struct(fringes=None), opticsTransmission=None, filterTransmission=None,
            sensorTransmission=None, atmosphereTransmission=None,
            detectorNum=None, strayLightData=None, illumMaskedImage=None,
            deferredCharge=None,
            ):
        """Accept ISR inputs, and produce ISR outputs with no processing.

        Parameters
        ----------
        ccdExposure : `lsst.afw.image.Exposure`
            The raw exposure that is to be run through ISR.  The
            exposure is modified by this method.
        camera : `lsst.afw.cameraGeom.Camera`, optional
            The camera geometry for this exposure. Required if
            one or more of ``ccdExposure``, ``bias``, ``dark``, or
            ``flat`` does not have an associated detector.
        bias : `lsst.afw.image.Exposure`, optional
            Bias calibration frame.
        linearizer : `lsst.ip.isr.linearize.LinearizeBase`, optional
            Functor for linearization.
        crosstalk : `lsst.ip.isr.crosstalk.CrosstalkCalib`, optional
            Calibration for crosstalk.
        crosstalkSources : `list`, optional
            List of possible crosstalk sources.
        dark : `lsst.afw.image.Exposure`, optional
            Dark calibration frame.
        flat : `lsst.afw.image.Exposure`, optional
            Flat calibration frame.
        ptc : `lsst.ip.isr.PhotonTransferCurveDataset`, optional
            Photon transfer curve dataset, with, e.g., gains
            and read noise.
        bfKernel : `numpy.ndarray`, optional
            Brighter-fatter kernel.
        bfGains : `dict` of `float`, optional
            Gains used to override the detector's nominal gains for the
            brighter-fatter correction. A dict keyed by amplifier name for
            the detector in question.
        defects : `lsst.ip.isr.Defects`, optional
            List of defects.
        fringes : `lsst.pipe.base.Struct`, optional
            Struct containing the fringe correction data, with
            elements:
            - ``fringes``: fringe calibration frame (`afw.image.Exposure`)
            - ``seed``: random seed derived from the ccdExposureId for random
                number generator (`uint32`)
        opticsTransmission: `lsst.afw.image.TransmissionCurve`, optional
            A ``TransmissionCurve`` that represents the throughput of the
            optics, to be evaluated in focal-plane coordinates.
        filterTransmission : `lsst.afw.image.TransmissionCurve`
            A ``TransmissionCurve`` that represents the throughput of the
            filter itself, to be evaluated in focal-plane coordinates.
        sensorTransmission : `lsst.afw.image.TransmissionCurve`
            A ``TransmissionCurve`` that represents the throughput of the
            sensor itself, to be evaluated in post-assembly trimmed detector
            coordinates.
        atmosphereTransmission : `lsst.afw.image.TransmissionCurve`
            A ``TransmissionCurve`` that represents the throughput of the
            atmosphere, assumed to be spatially constant.
        detectorNum : `int`, optional
            The integer number for the detector to process.
        isGen3 : bool, optional
            Flag this call to run() as using the Gen3 butler environment.
        strayLightData : `object`, optional
            Opaque object containing calibration information for stray-light
            correction.  If `None`, no correction will be performed.
        illumMaskedImage : `lsst.afw.image.MaskedImage`, optional
            Illumination correction image.

        Returns
        -------
        result : `lsst.pipe.base.Struct`
            Result struct with components:

            ``exposure``
                The fully ISR corrected exposure (`afw.image.Exposure`).
            ``outputExposure``
                An alias for ``exposure`` (`afw.image.Exposure`).
            ``ossThumb``
                Thumbnail image of the exposure after overscan subtraction
                (`numpy.ndarray`).
            ``flattenedThumb``
                Thumbnail image of the exposure after flat-field correction
                (`numpy.ndarray`).
            - ``outputStatistics`` : mapping [`str`]
                Values of the additional statistics calculated.
        """
        return Struct(exposure=afwImage.ExposureF(),
                      outputExposure=afwImage.ExposureF(),
                      ossThumb=np.empty((1, 1)),
                      flattenedThumb=np.empty((1, 1)),
                      preInterpExposure=afwImage.ExposureF(),
                      outputOssThumbnail=np.empty((1, 1)),
                      outputFlattenedThumbnail=np.empty((1, 1)),
                      outputStatistics={},
                      )


class MockCharacterizeImageTask(PipelineTask):
    """A do-nothing substitute for CharacterizeImageTask.
    """
    ConfigClass = CharacterizeImageConfig
    _DefaultName = "notCharacterizeImage"

    def __init__(self, refObjLoader=None, schema=None, **kwargs):
        super().__init__(**kwargs)
        self.outputSchema = afwTable.SourceCatalog()

    def runQuantum(self, butlerQC, inputRefs, outputRefs):
        inputs = butlerQC.get(inputRefs)
        if 'idGenerator' not in inputs.keys():
            inputs['idGenerator'] = self.config.idGenerator.apply(butlerQC.quantum.dataId)
        outputs = self.run(**inputs)
        butlerQC.put(outputs, outputRefs)

    def run(self, exposure, background=None, idGenerator=None):
        """Produce characterization outputs with no processing.

        Parameters
        ----------
        exposure : `lsst.afw.image.Exposure`
            Exposure to characterize.
        background : `lsst.afw.math.BackgroundList`, optional
            Initial model of background already subtracted from exposure.
        idGenerator : `lsst.meas.base.IdGenerator`, optional
            Object that generates source IDs and provides random number
            generator seeds.

        Returns
        -------
        result : `lsst.pipe.base.Struct`
            Struct containing these fields:

            ``characterized``
                Characterized exposure (`lsst.afw.image.Exposure`).
            ``sourceCat``
                Detected sources (`lsst.afw.table.SourceCatalog`).
            ``backgroundModel``
                Model of background subtracted from exposure (`lsst.afw.math.BackgroundList`)
            ``psfCellSet``
                Spatial cells of PSF candidates (`lsst.afw.math.SpatialCellSet`)
        """
        # Can't persist empty BackgroundList; DM-33714
        bg = afwMath.BackgroundMI(geom.Box2I(geom.Point2I(0, 0), geom.Point2I(16, 16)),
                                  afwImage.MaskedImageF(16, 16))
        return Struct(characterized=exposure,
                      sourceCat=afwTable.SourceCatalog(),
                      backgroundModel=afwMath.BackgroundList(bg),
                      psfCellSet=afwMath.SpatialCellSet(exposure.getBBox(), 10),
                      )


class MockCalibrateTask(PipelineTask):
    """A do-nothing substitute for CalibrateTask.
    """
    ConfigClass = CalibrateConfig
    _DefaultName = "notCalibrate"

    def __init__(self, astromRefObjLoader=None,
                 photoRefObjLoader=None, icSourceSchema=None,
                 initInputs=None, **kwargs):
        super().__init__(**kwargs)
        self.outputSchema = afwTable.SourceCatalog()

    def runQuantum(self, butlerQC, inputRefs, outputRefs):
        inputs = butlerQC.get(inputRefs)
        inputs['idGenerator'] = self.config.idGenerator.apply(butlerQC.quantum.dataId)

        if self.config.doAstrometry:
            inputs.pop('astromRefCat')
        if self.config.doPhotoCal:
            inputs.pop('photoRefCat')

        outputs = self.run(**inputs)

        if self.config.doWriteMatches and self.config.doAstrometry:
            normalizedMatches = afwTable.packMatches(outputs.astromMatches)
            if self.config.doWriteMatchesDenormalized:
                # Just need an empty BaseCatalog with a valid schema.
                outputs.matchesDenormalized = afwTable.BaseCatalog(outputs.outputCat.schema)
            outputs.matches = normalizedMatches
        butlerQC.put(outputs, outputRefs)

    def run(self, exposure, background=None,
            icSourceCat=None, idGenerator=None):
        """Produce calibration outputs with no processing.

        Parameters
        ----------
        exposure : `lsst.afw.image.Exposure`
            Exposure to calibrate.
        background : `lsst.afw.math.BackgroundList`, optional
            Background model already subtracted from exposure.
        icSourceCat : `lsst.afw.table.SourceCatalog`, optional
             A SourceCatalog from CharacterizeImageTask from which we can copy some fields.
        idGenerator : `lsst.meas.base.IdGenerator`, optional
            Object that generates source IDs and provides random number
            generator seeds.

        Returns
        -------
        result : `lsst.pipe.base.Struct`
            Struct containing these fields:

            ``outputExposure``
                Calibrated science exposure with refined WCS and PhotoCalib
                (`lsst.afw.image.Exposure`).
            ``outputBackground``
                Model of background subtracted from exposure
                (`lsst.afw.math.BackgroundList`).
            ``outputCat``
                Catalog of measured sources (`lsst.afw.table.SourceCatalog`).
            ``astromMatches``
                List of source/refObj matches from the astrometry solver
                (`list` [`lsst.afw.table.ReferenceMatch`]).
        """
        # Can't persist empty BackgroundList; DM-33714
        bg = afwMath.BackgroundMI(geom.Box2I(geom.Point2I(0, 0), geom.Point2I(16, 16)),
                                  afwImage.MaskedImageF(16, 16))
        return Struct(outputExposure=exposure,
                      outputBackground=afwMath.BackgroundList(bg),
                      outputCat=afwTable.SourceCatalog(),
                      astromMatches=[],
                      )


class MockGetTemplateTask(PipelineTask):
    """A do-nothing substitute for GetTemplateTask.
    """
    ConfigClass = GetTemplateConfig
    _DefaultName = "notGetTemplate"

    def runQuantum(self, butlerQC, inputRefs, outputRefs):
        inputs = butlerQC.get(inputRefs)
        # Mock GetTemplateTask.getOverlappingExposures
        results = Struct(coaddExposures=[],
                         dataIds=[],
                         )
        inputs["coaddExposures"] = results.coaddExposures
        inputs["dataIds"] = results.dataIds
        outputs = self.run(**inputs)
        butlerQC.put(outputs, outputRefs)

    def run(self, coaddExposures, bbox, wcs, dataIds, **kwargs):
        """Warp coadds from multiple tracts to form a template for image diff.

        Where the tracts overlap, the resulting template image is averaged.
        The PSF on the template is created by combining the CoaddPsf on each
        template image into a meta-CoaddPsf.

        Parameters
        ----------
        coaddExposures : `list` of `lsst.afw.image.Exposure`
            Coadds to be mosaicked
        bbox : `lsst.geom.Box2I`
            Template Bounding box of the detector geometry onto which to
            resample the coaddExposures
        wcs : `lsst.afw.geom.SkyWcs`
            Template WCS onto which to resample the coaddExposures
        dataIds : `list` of `lsst.daf.butler.DataCoordinate`
            Record of the tract and patch of each coaddExposure.
        **kwargs
            Any additional keyword parameters.

        Returns
        -------
        result : `lsst.pipe.base.Struct` containing
            - ``template`` : a template coadd exposure assembled out of patches
        """
        return Struct(template=afwImage.ExposureF(),
                      )


class MockAlardLuptonSubtractTask(PipelineTask):
    """A do-nothing substitute for AlardLuptonSubtractTask.
    """
    ConfigClass = AlardLuptonSubtractConfig
    _DefaultName = "notAlardLuptonSubtract"

    def run(self, template, science, sources, finalizedPsfApCorrCatalog=None, visitSummary=None):
        """PSF match, subtract, and decorrelate two images.

        Parameters
        ----------
        template : `lsst.afw.image.ExposureF`
            Template exposure, warped to match the science exposure.
        science : `lsst.afw.image.ExposureF`
            Science exposure to subtract from the template.
        sources : `lsst.afw.table.SourceCatalog`
            Identified sources on the science exposure. This catalog is used to
            select sources in order to perform the AL PSF matching on stamp
            images around them.
        finalizedPsfApCorrCatalog : `lsst.afw.table.ExposureCatalog`, optional
            Exposure catalog with finalized psf models and aperture correction
            maps to be applied if config.doApplyFinalizedPsf=True.  Catalog
            uses the detector id for the catalog id, sorted on id for fast
            lookup. Deprecated in favor of ``visitSummary``, and will be
            removed after v26.
        visitSummary : `lsst.afw.table.ExposureCatalog`, optional
            Exposure catalog with external calibrations to be applied. Catalog
            uses the detector id for the catalog id, sorted on id for fast
            lookup. Ignored (for temporary backwards compatibility) if
            ``finalizedPsfApCorrCatalog`` is provided.

        Returns
        -------
        results : `lsst.pipe.base.Struct`
            ``difference`` : `lsst.afw.image.ExposureF`
                Result of subtracting template and science.
            ``matchedTemplate`` : `lsst.afw.image.ExposureF`
                Warped and PSF-matched template exposure.
            ``backgroundModel`` : `lsst.afw.math.Function2D`
                Background model that was fit while solving for the
                PSF-matching kernel
            ``psfMatchingKernel`` : `lsst.afw.math.Kernel`
                Kernel used to PSF-match the convolved image.
        """
        return Struct(difference=afwImage.ExposureF(),
                      matchedTemplate=afwImage.ExposureF(),
                      backgroundModel=afwMath.NullFunction2D(),
                      psfMatchingKernel=afwMath.FixedKernel(),
                      )


class MockDetectAndMeasureConfig(DetectAndMeasureConfig):

    def setDefaults(self):
        super().setDefaults()
        # Avoid delegating to lsst.obs.base.Instrument specialization for the
        # data ID packing algorithm to use, since test code often does not use a
        # real Instrument in its data IDs.
        self.idGenerator.packer.name = "observation"


class MockDetectAndMeasureTask(PipelineTask):
    """A do-nothing substitute for DetectAndMeasureTask.
    """
    ConfigClass = MockDetectAndMeasureConfig
    _DefaultName = "notDetectAndMeasure"

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.outputSchema = afwTable.SourceCatalog()

    def runQuantum(self, butlerQC, inputRefs, outputRefs):
        inputs = butlerQC.get(inputRefs)
        idFactory = afwTable.IdFactory.makeSimple()

        outputs = self.run(inputs['science'],
                           inputs['matchedTemplate'],
                           inputs['difference'],
                           idFactory=idFactory)
        butlerQC.put(outputs, outputRefs)

    def run(self, science, matchedTemplate, difference,
            idFactory=None):
        """Detect and measure sources on a difference image.

        Parameters
        ----------
        science : `lsst.afw.image.ExposureF`
            Science exposure that the template was subtracted from.
        matchedTemplate : `lsst.afw.image.ExposureF`
            Warped and PSF-matched template that was used produce the
            difference image.
        difference : `lsst.afw.image.ExposureF`
            Result of subtracting template from the science image.
        idFactory : `lsst.afw.table.IdFactory`, optional
            Generator object to assign ids to detected sources in the difference image.

        Returns
        -------
        results : `lsst.pipe.base.Struct`
            ``subtractedMeasuredExposure`` : `lsst.afw.image.ExposureF`
                Subtracted exposure with detection mask applied.
            ``diaSources``  : `lsst.afw.table.SourceCatalog`
                The catalog of detected sources.
        """
        return Struct(subtractedMeasuredExposure=difference,
                      diaSources=afwTable.SourceCatalog(),
                      )


class MockRBTransiNetTask(PipelineTask):
    """A do-nothing substitute for RBTransiNetTask.
    """
    _DefaultName = "notRbTransiNet"
    ConfigClass = RBTransiNetConfig

    def run(self, template, science, difference, diaSources, pretrainedModel=None):
        return Struct(classifications=afwTable.BaseCatalog(afwTable.Schema()))


class MockTransformDiaSourceCatalogTask(PipelineTask):
    """A do-nothing substitute for TransformDiaSourceCatalogTask.
    """
    ConfigClass = TransformDiaSourceCatalogConfig
    _DefaultName = "notTransformDiaSourceCatalog"

    def __init__(self, initInputs, **kwargs):
        super().__init__(**kwargs)

    def runQuantum(self, butlerQC, inputRefs, outputRefs):
        inputs = butlerQC.get(inputRefs)
        idGenerator = self.config.idGenerator.apply(butlerQC.quantum.dataId)
        inputs["ccdVisitId"] = idGenerator.catalog_id
        inputs["band"] = butlerQC.quantum.dataId["band"]

        outputs = self.run(**inputs)

        butlerQC.put(outputs, outputRefs)

    def run(self, diaSourceCat, diffIm, band, ccdVisitId, funcs=None):
        """Produce transformation outputs with no processing.

        Parameters
        ----------
        diaSourceCat : `lsst.afw.table.SourceCatalog`
            The catalog to transform.
        diffIm : `lsst.afw.image.Exposure`
            An image, to provide supplementary information.
        band : `str`
            The band in which the sources were observed.
        ccdVisitId : `int`
            The exposure ID in which the sources were observed.
        funcs, optional
            Unused.

        Returns
        -------
        results : `lsst.pipe.base.Struct`
            Results struct with components:

            ``diaSourceTable``
                Catalog of DiaSources (`pandas.DataFrame`).
        """
        return Struct(diaSourceTable=pandas.DataFrame(),
                      )


class MockDiaPipelineConfig(DiaPipelineConfig):

    def setDefaults(self):
        super().setDefaults()
        # Avoid delegating to lsst.obs.base.Instrument specialization for the
        # data ID packing algorithm to use, since test code often does not use a
        # real Instrument in its data IDs.
        self.idGenerator.packer.name = "observation"


class MockDiaPipelineTask(PipelineTask):
    """A do-nothing substitute for DiaPipelineTask.
    """
    ConfigClass = MockDiaPipelineConfig
    _DefaultName = "notDiaPipe"

    def runQuantum(self, butlerQC, inputRefs, outputRefs):
        inputs = butlerQC.get(inputRefs)
        inputs["idGenerator"] = self.config.idGenerator.apply(butlerQC.quantum.dataId)
        # Need to set ccdExposureIdBits (now deprecated) to None and pass it,
        # since there are non-optional positional arguments after it.
        inputs["ccdExposureIdBits"] = None
        inputs["band"] = butlerQC.quantum.dataId["band"]
        if not self.config.doSolarSystemAssociation:
            inputs["solarSystemObjectTable"] = None

        outputs = self.run(**inputs)

        butlerQC.put(outputs, outputRefs)

    def run(self,
            diaSourceTable,
            solarSystemObjectTable,
            diffIm,
            exposure,
            template,
            ccdExposureIdBits,
            band,
            idGenerator=None):
        """Produce DiaSource and DiaObject outputs with no processing.

        Parameters
        ----------
        diaSourceTable : `pandas.DataFrame`
            Newly detected DiaSources.
        solarSystemObjectTable : `pandas.DataFrame`
            Expected solar system objects in the field of view.
        diffIm : `lsst.afw.image.ExposureF`
            Difference image exposure in which the sources in ``diaSourceCat``
            were detected.
        exposure : `lsst.afw.image.ExposureF`
            Calibrated exposure differenced with a template to create
            ``diffIm``.
        template : `lsst.afw.image.ExposureF`
            Template exposure used to create diffIm.
        ccdExposureIdBits : `int`
            Number of bits used for a unique ``ccdVisitId``.  Deprecated in
            favor of ``idGenerator``, and ignored if that is present.  Pass
            `None` explicitly to avoid a deprecation warning (a default is
            impossible given that later positional arguments are not
            defaulted).
        band : `str`
            The band in which the new DiaSources were detected.
        idGenerator : `lsst.meas.base.IdGenerator`, optional
            Object that generates source IDs and random number generator seeds.
            Will be required after ``ccdExposureIdBits`` is removed.

        Returns
        -------
        results : `lsst.pipe.base.Struct`
            Results struct with components:

            ``apdbMarker``
                Marker dataset to store in the Butler indicating that this
                ccdVisit has completed successfully (`lsst.dax.apdb.ApdbConfig`).
            ``associatedDiaSources``
                Catalog of newly associated DiaSources (`pandas.DataFrame`).
        """
        return Struct(apdbMarker=self.config.apdb.value,
                      associatedDiaSources=pandas.DataFrame(),
                      diaForcedSources=pandas.DataFrame(),
                      diaObjects=pandas.DataFrame(),
                      longTrailedSources=pandas.DataFrame(),
                      )
