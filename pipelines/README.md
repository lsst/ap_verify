# Pipeline Definitions

This directory contains pipeline definition YAML files which are used when processing data with the LSST Science Pipelines.

The pipelines defined here come in three flavors: camera-specific (within named directories), camera-agnostic (top-level, if any), and building-block ingredients (within the [\_ingredients](_ingredients) directory).
Pipelines within the ingredients directory are meant to be imported by other pipelines, and are not intended to be used directly by end-users.

The `pipetask build` command can be used to expand a pipeline YAML and resolve any imports for the purposes of visualizing it.
For example, to visualize the `apPipeSingleFrame` subset from the [LSSTCam-imSim ApVerify pipeline](https://github.com/lsst/ap_verify/blob/main/pipelines/LSSTCam-imSim/ApVerify.yaml) pipeline, run:

```bash
pipetask build \
-p $AP_VERIFY_DIR/pipelines/LSSTCam-imSim/ApVerify.yaml#apPipeSingleFrame \
--show pipeline
```
