from lsst.verify.tasks import ConfigPpdbLoader
# Import these modules to ensure the metrics are registered
import lsst.ap.association.metrics  # noqa: F401

ppdbConfigs = ["totalUnassociatedDiaObjects"]
config.measurers = ppdbConfigs

# List comprehension would be cleaner, but can't refer to config inside one
for subConfig in ppdbConfigs:
    config.measurers[subConfig].dbLoader.retarget(ConfigPpdbLoader)
    config.measurers[subConfig].dbInfo.name = "apPipe_config"
