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

import astropy.table
import numpy as np
import pandas

import lsst.geom as geom
import lsst.afw.image as afwImage
import lsst.afw.math as afwMath
import lsst.afw.table as afwTable
from lsst.ap.association import (TransformDiaSourceCatalogConfig,
                                 DiaPipelineConfig, FilterDiaSourceCatalogConfig)
from lsst.pipe.base import PipelineTask, Struct
from lsst.ip.isr import IsrTaskConfig
from lsst.ip.diffim import (GetTemplateConfig, AlardLuptonSubtractConfig,
                            DetectAndMeasureConfig, SpatiallySampledMetricsConfig)
from lsst.pipe.tasks.calibrateImage import CalibrateImageConfig
from lsst.meas.transiNet import RBTransiNetConfig


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


class MockCalibrateImageTask(PipelineTask):
    """A do-nothing substitute for CalibrateImageTask.
    """
    ConfigClass = CalibrateImageConfig
    _DefaultName = "notCalibrateImage"

    def __init__(self, initial_stars_schema=None, **kwargs):
        super().__init__(**kwargs)
        self.initial_stars_schema = afwTable.SourceCatalog()

    def runQuantum(self, butlerQC, inputRefs, outputRefs):
        inputs = butlerQC.get(inputRefs)
        exposures = inputs.pop("exposures")
        id_generator = self.config.id_generator.apply(butlerQC.quantum.dataId)
        outputs = self.run(exposures=exposures, id_generator=id_generator)
        butlerQC.put(outputs, outputRefs)

    def run(self, *, exposures, id_generator=None, result=None):
        """Produce calibration outputs with no processing.

        Parameters
        ----------
        exposures : `lsst.afw.image.Exposure` or `list` [`lsst.afw.image.Exposure`]
            Post-ISR exposure(s), with an initial WCS, VisitInfo, and Filter.
            Modified in-place during processing if only one is passed.
            If two exposures are passed, treat them as snaps and combine
            before doing further processing.
        id_generator : `lsst.meas.base.IdGenerator`, optional
            Object that generates source IDs and provides random seeds.
        result : `lsst.pipe.base.Struct`, optional
            Result struct that is modified to allow saving of partial outputs
            for some failure conditions. If the task completes successfully,
            this is also returned.

        Returns
        -------
        result : `lsst.pipe.base.Struct`
            Results as a struct with attributes:

            ``exposure``
                Calibrated exposure, with pixels in nJy units.
                (`lsst.afw.image.Exposure`)
            ``stars``
                Stars that were used to calibrate the exposure, with
                calibrated fluxes and magnitudes.
                (`astropy.table.Table`)
            ``stars_footprints``
                Footprints of stars that were used to calibrate the exposure.
                (`lsst.afw.table.SourceCatalog`)
            ``psf_stars``
                Stars that were used to determine the image PSF.
                (`astropy.table.Table`)
            ``psf_stars_footprints``
                Footprints of stars that were used to determine the image PSF.
                (`lsst.afw.table.SourceCatalog`)
            ``background``
                Background that was fit to the exposure when detecting
                ``stars``. (`lsst.afw.math.BackgroundList`)
            ``applied_photo_calib``
                Photometric calibration that was fit to the star catalog and
                applied to the exposure. (`lsst.afw.image.PhotoCalib`)
            ``astrometry_matches``
                Reference catalog stars matches used in the astrometric fit.
                (`list` [`lsst.afw.table.ReferenceMatch`] or `lsst.afw.table.BaseCatalog`)
            ``photometry_matches``
                Reference catalog stars matches used in the photometric fit.
                (`list` [`lsst.afw.table.ReferenceMatch`] or `lsst.afw.table.BaseCatalog`)
        """
        # Can't persist empty BackgroundList; DM-33714
        bg = afwMath.BackgroundMI(geom.Box2I(geom.Point2I(0, 0), geom.Point2I(16, 16)),
                                  afwImage.MaskedImageF(16, 16))
        return Struct(exposure=afwImage.ExposureF(),
                      background=afwMath.BackgroundList(bg),
                      stars_footprints=afwTable.SourceCatalog(),
                      stars=afwTable.SourceCatalog().asAstropy(),
                      psf_stars_footprints=afwTable.SourceCatalog(),
                      psf_stars=afwTable.SourceCatalog().asAstropy(),
                      applied_photo_calib=afwImage.PhotoCalib(),
                      astrometry_matches=None,
                      photometry_matches=None,
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


class MockFilterDiaSourceCatalogTask(PipelineTask):
    """A do-nothing substitute for FilterDiaSourceCatalogTask.
    """
    ConfigClass = FilterDiaSourceCatalogConfig
    _DefaultName = "notFilterDiaSourceCatalog"

    def run(self, diaSourceCat, diffImVisitInfo):
        """Produce filtering outputs with no processing.

        Parameters
        ----------
        diaSourceCat : `lsst.afw.table.SourceCatalog`
            Catalog of sources measured on the difference image.
        diffImVisitInfo:  `lsst.afw.image.VisitInfo`
            VisitInfo for the difference image corresponding to diaSourceCat.

        Returns
        -------
        results : `lsst.pipe.base.Struct`
            Results struct with components.

            ``filteredDiaSourceCat`` : `lsst.afw.table.SourceCatalog`
                The catalog of filtered sources.
            ``rejectedDiaSources`` : `lsst.afw.table.SourceCatalog`
                The catalog of rejected sky sources.
            ``longTrailedDiaSources`` : `astropy.table.Table`
                DiaSources which have trail lengths greater than
                max_trail_length*exposure_time.
        """
        # TODO Add docstrings for diffIm
        return Struct(filteredDiaSourceCat=afwTable.SourceCatalog(),
                      rejectedDiaSources=afwTable.SourceCatalog(),
                      longTrailedSources=afwTable.SourceCatalog().asAstropy(),
                      )


class MockSpatiallySampledMetricsTask(PipelineTask):
    """A do-nothing substitute for SpatiallySampledMetricsTask.
    """
    ConfigClass = SpatiallySampledMetricsConfig
    _DefaultName = "notSpatiallySampledMetricsTask"

    def run(self, science, matchedTemplate, template, difference, diaSources, psfMatchingKernel):
        """Produce spatially sampled metrics

        Parameters
        ----------
        science : `lsst.afw.image.ExposureF`
            Science exposure that the template was subtracted from.
        matchedTemplate : `lsst.afw.image.ExposureF`
            Warped and PSF-matched template that was used produce the
            difference image.
        template : `lsst.afw.image.ExposureF`
            Warped and non PSF-matched template that was used to produce
            the difference image.
        difference : `lsst.afw.image.ExposureF`
            Result of subtracting template from the science image.
        diaSources : `lsst.afw.table.SourceCatalog`
                The catalog of detected sources.
        psfMatchingKernel : `~lsst.afw.math.LinearCombinationKernel`
            The PSF matching kernel of the subtraction to evaluate.

        Returns
        -------
        results : `lsst.pipe.base.Struct`
            Results struct with components.
        """

        return Struct(spatiallySampledMetrics=astropy.table.Table())


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
        inputs["band"] = butlerQC.quantum.dataId["band"]
        inputs["visit"] = butlerQC.quantum.dataId["visit"]
        inputs["detector"] = butlerQC.quantum.dataId["detector"]

        outputs = self.run(**inputs)

        butlerQC.put(outputs, outputRefs)

    def run(self, diaSourceCat, diffIm, band, visit, detector, funcs=None):
        """Produce transformation outputs with no processing.

        Parameters
        ----------
        diaSourceCat : `lsst.afw.table.SourceCatalog`
            The catalog to transform.
        diffIm : `lsst.afw.image.Exposure`
            An image, to provide supplementary information.
        band : `str`
            The band in which the sources were observed.
        visit, detector: `int`
            Visit and detector the sources were detected on.
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
                      diaObjects=pandas.DataFrame(),)
