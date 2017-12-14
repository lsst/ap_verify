# lsst.ap.verify

This package manages end-to-end testing and metric generation for the LSST DM Alert Production pipeline. Metrics are tested against both project- and lower-level requirements, and will be deliverable to the SQuaSH metrics service.

`ap_verify` is part of the LSST Science Pipelines. You can learn how to install the Pipelines at https://pipelines.lsst.io/install/index.html.

## Configuration

`ap_verify` is configured from `config/dataset_config.yaml`. The file currently must have:

* a dictionary named `datasets`, which maps from user-visible dataset names to the eups package that implements them (see `Setting Up a Dataset`, below)
* a dictionary named `measurements`, which contains dictionaries needed for different metrics:
    * `timing`: maps from Tasks or subTasks to the names of metrics that time them. The names of subTasks must be those assigned by the parent Task, and may be prefixed by the parent Task name(s) followed by a colon, as in "imageDifference:detection". Metric names must exist in `verify_metrics` and include the package they're associated with, as in "meas_algorithms.SourceDetectionTime".

Other configuration options may be added in the future.

### Setting Up a Dataset

`ap_verify` requires that all data be in a [dataset package](https://github.com/lsst-dm/ap_verify_dataset_template). It will create a workspace modeled after the package's `repo` directory, then process any data found in the `raw` and `ref_cats` in the new workspace. Anything placed in `repo` will be copied to a `ap_verify` run's workspace as-is, and must at least include a `_mapper` file naming the CameraMapper for the data.

The dataset package must work with eups, and must be registered in `config/dataset_config.yaml` in order for `ap_verify` to support it. `ap_verify` will use `eups setup` to prepare the dataset package and any dependencies; typically, they will include the `obs_` package for the instrument that took the data.

## Running ap_verify

A basic run on HiTS data:

    python python/lsst/ap/verify/ap_verify.py --dataset HiTS2015 --output workspace/hits/ --dataIdString "visit=54123"

This will create a workspace (a Butler repository) in `workspace/hits` based on `<hits-data>/data/`, ingest the HiTS data into it, then run visit 54123 through the entire AP pipeline. `ap_verify` also supports the `--rerun` system:

    python python/lsst/ap/verify/ap_verify.py --dataset HiTS2015 --rerun run1 --dataIdString "visit=54123"

This will create a workspace in `<hits-data>/rerun/run1/`. Since datasets are not, in general, repositories, many of the complexities of `--rerun` for Tasks (e.g., always using the highest-level repository) do not apply. In addition, the `--rerun` argument does not support input directories; the input for `ap_verify` will always be determined by the `--dataset`.

### Optional Arguments

`--silent`: Normally, `ap_verify` submits measurements to SQuaSH for quality tracking. This argument disables reporting for test runs. `ap_verify` will dump measurements to `ap_verify.verify.json` regardless of whether this flag is set.

`-j, --processes`: Specify a particular degree of parallelism. Like in Tasks, this argument may be taken at face value with no intelligent thread management.

`-h, --help, --version`: These arguments print a brief usage guide and the program version, respectively.
