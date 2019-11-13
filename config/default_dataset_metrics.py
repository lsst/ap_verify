from lsst.verify.tasks import ConfigApdbLoader
# Import these modules to ensure the metrics are registered
import lsst.ap.association.metrics  # noqa: F401

apdbConfigs = ["totalUnassociatedDiaObjects"]
config.measurers = apdbConfigs

# List comprehension would be cleaner, but can't refer to config inside one
for subConfig in apdbConfigs:
    config.measurers[subConfig].dbLoader.retarget(ConfigApdbLoader)
    config.measurers[subConfig].connections.taskName = "apPipe"
