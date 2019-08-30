from itertools import chain
from lsst.verify.tasks.commonMetrics import TimingMetricConfig, MemoryMetricConfig
# Import these modules to ensure the metrics are registered
import lsst.ip.diffim.metrics  # noqa: F401
import lsst.ap.association.metrics  # noqa: F401

metadataConfigs = ["numNewDiaObjects",
                   "numUnassociatedDiaObjects",
                   "fracUpdatedDiaObjects"]
config.measurers = ["timing", "memory",
                    "numSciSources", "fracDiaSourcesToSciSources",
                    ] + metadataConfigs

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
memoryConfigs = {
    "apPipe.runDataRef": "ap_pipe.ApPipeMemory",
}
for target, metric in timingConfigs.items():
    subConfig = TimingMetricConfig()
    subConfig.target = target
    subConfig.metric = metric
    config.measurers["timing"].configs[target] = subConfig
for target, metric in memoryConfigs.items():
    subConfig = MemoryMetricConfig()
    subConfig.target = target
    subConfig.metric = metric
    config.measurers["memory"].configs[target] = subConfig
for subConfig in chain(config.measurers["timing"].configs.values(),
                       config.measurers["memory"].configs.values(),
                       ):
    subConfig.connections.taskName = "apPipe"
# List comprehension would be cleaner, but can't refer to config inside one
for subConfig in metadataConfigs:
    config.measurers[subConfig].connections.taskName = "apPipe"
