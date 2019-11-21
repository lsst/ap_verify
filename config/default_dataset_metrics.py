# Import these modules to ensure the metrics are registered
import lsst.verify.tasks  # noqa: F401
import lsst.ap.association.metrics  # noqa: F401

apdbConfigs = ["totalUnassociatedDiaObjects"]
config.measurers = apdbConfigs
