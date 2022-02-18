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
import lsst.obs.base as obsBase
from lsst.pipe.base import PipelineTask, Struct
from lsst.ip.isr import IsrTaskConfig
from lsst.pipe.tasks.characterizeImage import CharacterizeImageConfig
from lsst.pipe.tasks.calibrate import CalibrateConfig
from lsst.pipe.tasks.imageDifference import ImageDifferenceConfig
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
            isGen3=False,
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
        """
        return Struct(exposure=afwImage.ExposureF(),
                      outputExposure=afwImage.ExposureF(),
                      ossThumb=np.empty((1, 1)),
                      flattenedThumb=np.empty((1, 1)),
                      )


class MockCharacterizeImageTask(PipelineTask):
    """A do-nothing substitute for CharacterizeImageTask.
    """
    ConfigClass = CharacterizeImageConfig
    _DefaultName = "notCharacterizeImage"

    def __init__(self, butler=None, refObjLoader=None, schema=None, **kwargs):
        super().__init__(**kwargs)
        self.outputSchema = afwTable.SourceCatalog()

    def runQuantum(self, butlerQC, inputRefs, outputRefs):
        inputs = butlerQC.get(inputRefs)
        if 'exposureIdInfo' not in inputs.keys():
            inputs['exposureIdInfo'] = obsBase.ExposureIdInfo.fromDataId(
                butlerQC.quantum.dataId, "visit_detector")
        outputs = self.run(**inputs)
        butlerQC.put(outputs, outputRefs)

    def run(self, exposure, exposureIdInfo=None, background=None):
        """Produce characterization outputs with no processing.

        Parameters
        ----------
        exposure : `lsst.afw.image.Exposure`
            Exposure to characterize.
        exposureIdInfo : `lsst.obs.base.ExposureIdInfo`
            ID info for exposure.
        background : `lsst.afw.math.BackgroundList`
            Initial model of background already subtracted from exposure.

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

    def __init__(self, butler=None, astromRefObjLoader=None,
                 photoRefObjLoader=None, icSourceSchema=None,
                 initInputs=None, **kwargs):
        super().__init__(**kwargs)
        self.outputSchema = afwTable.SourceCatalog()

    def runQuantum(self, butlerQC, inputRefs, outputRefs):
        inputs = butlerQC.get(inputRefs)
        inputs['exposureIdInfo'] = obsBase.ExposureIdInfo.fromDataId(
            butlerQC.quantum.dataId, "visit_detector")

        if self.config.doAstrometry:
            inputs.pop('astromRefCat')
        if self.config.doPhotoCal:
            inputs.pop('photoRefCat')

        outputs = self.run(**inputs)

        if self.config.doWriteMatches and self.config.doAstrometry:
            normalizedMatches = afwTable.packMatches(outputs.astromMatches)
            if self.config.doWriteMatchesDenormalized:
                outputs.matchesDenormalized = outputs.astromMatches
            outputs.matches = normalizedMatches
        butlerQC.put(outputs, outputRefs)

    def run(self, exposure, exposureIdInfo=None, background=None,
            icSourceCat=None):
        """Produce calibration outputs with no processing.

        Parameters
        ----------
        exposure : `lsst.afw.image.Exposure`
            Exposure to calibrate.
        exposureIdInfo : `lsst.obs.base.ExposureIdInfo`
            ID info for exposure.
        background : `lsst.afw.math.BackgroundList`
            Background model already subtracted from exposure.
        icSourceCat : `lsst.afw.table.SourceCatalog`
             A SourceCatalog from CharacterizeImageTask from which we can copy some fields.

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


class MockImageDifferenceTask(PipelineTask):
    """A do-nothing substitute for ImageDifferenceTask.
    """
    ConfigClass = ImageDifferenceConfig
    _DefaultName = "notImageDifference"

    def __init__(self, butler=None, **kwargs):
        super().__init__(**kwargs)
        self.outputSchema = afwTable.SourceCatalog()

    def runQuantum(self, butlerQC, inputRefs, outputRefs):
        inputs = butlerQC.get(inputRefs)
        outputs = self.run(exposure=inputs['exposure'],
                           templateExposure=afwImage.ExposureF(),
                           idFactory=obsBase.ExposureIdInfo(8, 4).makeSourceIdFactory())
        butlerQC.put(outputs, outputRefs)

    def run(self, exposure=None, selectSources=None, templateExposure=None, templateSources=None,
            idFactory=None, calexpBackgroundExposure=None, subtractedExposure=None):
        """Produce differencing outputs with no processing.

        Parameters
        ----------
        exposure : `lsst.afw.image.ExposureF`, optional
            The science exposure, the minuend in the image subtraction.
            Can be None only if ``config.doSubtract==False``.
        selectSources : `lsst.afw.table.SourceCatalog`, optional
            Identified sources on the science exposure. This catalog is used to
            select sources in order to perform the AL PSF matching on stamp images
            around them. The selection steps depend on config options and whether
            ``templateSources`` and ``matchingSources`` specified.
        templateExposure : `lsst.afw.image.ExposureF`, optional
            The template to be subtracted from ``exposure`` in the image subtraction.
            ``templateExposure`` is modified in place if ``config.doScaleTemplateVariance==True``.
            The template exposure should cover the same sky area as the science exposure.
            It is either a stich of patches of a coadd skymap image or a calexp
            of the same pointing as the science exposure. Can be None only
            if ``config.doSubtract==False`` and ``subtractedExposure`` is not None.
        templateSources : `lsst.afw.table.SourceCatalog`, optional
            Identified sources on the template exposure.
        idFactory : `lsst.afw.table.IdFactory`
            Generator object to assign ids to detected sources in the difference image.
        calexpBackgroundExposure : `lsst.afw.image.ExposureF`, optional
            Background exposure to be added back to the science exposure
            if ``config.doAddCalexpBackground==True``
        subtractedExposure : `lsst.afw.image.ExposureF`, optional
            If ``config.doSubtract==False`` and ``config.doDetection==True``,
            performs the post subtraction source detection only on this exposure.
            Otherwise should be None.

        Returns
        -------
        results : `lsst.pipe.base.Struct`

            ``subtractedExposure`` : `lsst.afw.image.ExposureF`
                Difference image.
            ``scoreExposure`` : `lsst.afw.image.ExposureF` or `None`
                The zogy score exposure, if calculated.
            ``matchedExposure`` : `lsst.afw.image.ExposureF`
                The matched PSF exposure.
            ``warpedExposure`` : `lsst.afw.image.ExposureF`
                The warped PSF exposure.
            ``subtractRes`` : `lsst.pipe.base.Struct`
                The returned result structure of the ImagePsfMatchTask subtask.
            ``diaSources``  : `lsst.afw.table.SourceCatalog`
                The catalog of detected sources.
            ``selectSources`` : `lsst.afw.table.SourceCatalog`
                The input source catalog with optionally added Qa information.
        """
        return Struct(
            subtractedExposure=afwImage.ExposureF(),
            scoreExposure=afwImage.ExposureF(),
            warpedExposure=afwImage.ExposureF(),
            matchedExposure=afwImage.ExposureF(),
            subtractRes=Struct(),
            diaSources=afwTable.SourceCatalog(),
            selectSources=afwTable.SourceCatalog(),
        )


class MockTransformDiaSourceCatalogTask(PipelineTask):
    """A do-nothing substitute for TransformDiaSourceCatalogTask.
    """
    ConfigClass = TransformDiaSourceCatalogConfig
    _DefaultName = "notTransformDiaSourceCatalog"

    def __init__(self, initInputs, **kwargs):
        super().__init__(**kwargs)

    def runQuantum(self, butlerQC, inputRefs, outputRefs):
        inputs = butlerQC.get(inputRefs)
        expId, expBits = butlerQC.quantum.dataId.pack("visit_detector",
                                                      returnMaxBits=True)
        inputs["ccdVisitId"] = expId
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
        funcs
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


class MockDiaPipelineTask(PipelineTask):
    """A do-nothing substitute for DiaPipelineTask.
    """
    ConfigClass = DiaPipelineConfig
    _DefaultName = "notDiaPipe"

    def runQuantum(self, butlerQC, inputRefs, outputRefs):
        inputs = butlerQC.get(inputRefs)
        expId, expBits = butlerQC.quantum.dataId.pack("visit_detector",
                                                      returnMaxBits=True)
        inputs["ccdExposureIdBits"] = expBits
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
            warpedExposure,
            ccdExposureIdBits,
            band):
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
        warpedExposure : `lsst.afw.image.ExposureF`
            Template exposure used to create diffIm.
        ccdExposureIdBits : `int`
            Number of bits used for a unique ``ccdVisitId``.
        band : `str`
            The band in which the new DiaSources were detected.

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
                      )
