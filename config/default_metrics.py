from lsst.ap.verify.measurements.profiling import TimingMetricConfig

config.jobFileTemplate = "ap_verify.metricTask{id}.{dataId}.verify.json"

config.measurers = ["timing"]

timingConfigs = {
    "apPipe.runDataRef": "ap_pipe.ApPipeTime",
    "apPipe:ccdProcessor.runDataRef": "pipe_tasks.ProcessCcdTime",
    "apPipe:ccdProcessor:isr.runDataRef": "ip_isr.IsrTime",
    "apPipe:ccdProcessor:charImage.runDataRef": "pipe_tasks.CharacterizeImageTime",
    "apPipe:ccdProcessor:calibrate.runDataRef": "pipe_tasks.CalibrateTime",
    "apPipe:differencer.runDataRef": "pipe_tasks.ImageDifferenceTime",
    "apPipe:differencer:astrometer.loadAndMatch": "meas_astrom.AstrometryTime",
    "apPipe:differencer:register.run": "pipe_tasks.RegisterImageTime",
    "apPipe:differencer:subtract.subtractExposures": "ip_diffim.ImagePsfMatchTime",
    "apPipe:differencer:detection.run": "meas_algorithms.SourceDetectionTime",
    "apPipe:differencer:measurement.run": "ip_diffim.DipoleFitTime",
    "apPipe:associator.run": "ap_association.AssociationTime",
}
for target, metric in timingConfigs.items():
    subConfig = TimingMetricConfig()
    subConfig.target = target
    subConfig.metric = metric
    config.measurers["timing"].configs[target] = subConfig
for subConfig in config.measurers["timing"].configs.values():
    subConfig.metadata.name = "apPipe_metadata"
