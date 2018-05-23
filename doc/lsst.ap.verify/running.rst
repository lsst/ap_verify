.. _ap-verify-running:

#######################################
Running ap_verify From the Command Line
#######################################

``ap_verify`` is a Python script designed to be run on both developer machines and verification servers.
While ``ap_verify`` is not a :ref:`command-line task<lsst.pipe.base>`, the command-line interface is designed to resemble that of command-line tasks where practical.
This page describes the minimum options needed to run ``ap_verify``.
For more details, see the :ref:`ap-verify-cmd` or run :option:`ap_verify.py -h`.

.. _ap-verify-dataset-name:

Datasets as Input Arguments
---------------------------

Since ``ap_verify`` begins with an uningested :ref:`dataset<ap-verify-datasets>`, the input argument is a dataset name rather than a repository.

Datasets are identified by a name that gets mapped to an :ref:`eups-registered directory <ap-verify-datasets-install>` containing the data.
The mapping is :ref:`configurable<ap-verify-configuration-dataset>`.
The dataset names are a placeholder for a future data repository versioning system, and may be replaced in a later version of ``ap_verify``.

.. _ap-verify-run-output:

How to Run ap_verify in a New Workspace
---------------------------------------

Using the :ref:`HiTS 2015 <ap_verify_hits2015-package>` dataset as an example, one can run ``ap_verify`` as follows:

.. prompt:: bash

   python ap_verify/bin/ap_verify.py --dataset HiTS2015 --id "visit=54123 ccd=25 filter=g" --output workspaces/hits/ --silent

Here the inputs are:

* :command:`HiTS2015` is the :ref:`dataset name <ap-verify-dataset-name>`,
* :command:`visit=54123 ccd=25 filter=g` is the :ref:`dataId<command-line-task-dataid-howto-about-dataid-keys>` to process,

while the output is:

* :command:`workspaces/hits/` is the location where the pipeline will create any :ref:`Butler repositories<command-line-task-data-repo-using-uris>` necessary,

. :command:`--silent` disables SQuaSH metrics reporting.

This call will create a new directory at :file:`workspaces/hits`, ingest the HiTS data into a new repository based on :file:`<hits-data>/repo/`, then run visit 54123 through the entire AP pipeline.

.. note::

   The command-line interface for ``ap_verify`` is at present much more limited than those of command-line tasks.
   In particular, only file-based repositories are supported, and compound dataIds cannot be provided.
   See the :ref:`ap-verify-cmd` for details.

.. _ap-verify-run-rerun:

How to Run ap_verify in the Dataset Directory
---------------------------------------------

It is also possible to place a workspace in a subdirectory of a dataset directory. The syntax for this mode is:

.. prompt:: bash

   python python/lsst/ap/verify/ap_verify.py --dataset HiTS2015 --rerun run1 --id "visit=54123 ccd=25 filter=g" --silent

The :command:`--rerun run1` argument will create a directory at :file:`<hits-data>/rerun/run1/`.
Since neither :ref:`datasets<ap-verify-datasets-butler>` nor ``ap_verify`` output directories are repositories, the :option:`--rerun <ap_verify.py --rerun>` parameter only superficially resembles the analogous argument for command-line tasks.
In particular, ``ap_verify``'s ``--rerun`` does not support repository chaining (as in :command:`--rerun input:output`); the input for ``ap_verify`` will always be determined by the :option:`--dataset <ap_verify.py --dataset>`.

.. _ap-verify-run-ingest:

How to Run Ingestion By Itself
------------------------------

``ap_verify`` includes a separate program, :command:`ingest_dataset.py`, that ingests datasets but does not run the pipeline on them.
This is useful if the data need special processing or as a precursor to massive processing runs.
Running ``ap_verify`` with the same arguments as a previous run of ``ingest_dataset`` will automatically skip ingestion.

Using the :ref:`HiTS 2015 <ap_verify_hits2015-package>` dataset as an example, one can run ``ingest_dataset`` as follows:

.. prompt:: bash

   ingest_dataset.py --dataset HiTS2015 --output workspaces/hits/

The :option:`--dataset <ap_verify.py --dataset>`, :option:`--output <ap_verify.py --output>`, and :option:`--rerun <ap_verify.py --rerun>` arguments behave the same way as for ``ap_verify``.
Other options from ``ap_verify`` are not available.

.. _ap-verify-results:

How to Use Measurements of Metrics
----------------------------------

After ``ap_verify`` has run, it will produce a file named, by default, :file:`ap_verify.verify.json` in the caller's directory.
The file name may be customized using the :option:`--metrics-file <ap_verify.py --metrics-file>` command-line argument.
This file contains metric measurements in `lsst.verify` format, and can be loaded and read as described in the `lsst.verify` documentation or in `SQR-019 <https://sqr-019.lsst.io>`_.

Unless the :option:`--silent <ap_verify.py --silent>` argument is provided, ``ap_verify`` will also upload measurements to the `SQuaSH service <https://squash.lsst.codes/>`_ on completion.
See the SQuaSH documentation for details.

If the pipeline is interrupted by a fatal error, completed measurements will be saved to the metrics file for debugging purposes, but nothing will get sent to SQuaSH.
See the :ref:`error-handling policy <ap-verify-failsafe-partialmetric>` for details.

