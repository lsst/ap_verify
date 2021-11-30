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

import lsst.geom as geom
import lsst.afw.image as afwImage
import lsst.afw.math as afwMath
import lsst.afw.table as afwTable
import lsst.obs.base as obsBase
from lsst.pipe.base import PipelineTask, Struct
from lsst.ip.isr import IsrTaskConfig
from lsst.pipe.tasks.characterizeImage import CharacterizeImageConfig
from lsst.pipe.tasks.calibrate import CalibrateConfig


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
