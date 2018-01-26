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

.. TODO: why dataIdString instead of id or dataId? (DM-12853)

.. prompt:: bash

   python ap_verify/bin/ap_verify.py --dataset HiTS2015 --output workspace/hits/ --dataIdString "visit=54123 ccd=25 filter=g" --silent

Here:

* :command:`HiTS2015` is the :ref:`dataset name <ap-verify-dataset-name>`,
* :command:`workspace/hits/` is the location of the :ref:`Butler repository<command-line-task-data-repo-using-uris>` in which the pipeline will work,
* :command:`visit=54123 ccd=25 filter=g` is the :ref:`dataId<command-line-task-dataid-howto-about-dataid-keys>` to process, and
* :command:`--silent` disables SQuaSH metrics reporting.

This will create a workspace (a Butler repository) in :file:`workspace/hits` based on :file:`<hits-data>/data/`, ingest the HiTS data into it, then run visit 54123 through the entire AP pipeline.

.. note::

   The command-line interface for ``ap_verify`` is at present much more limited than those of command-line tasks.
   In particular, only file-based repositories are supported, and compound dataIds cannot be provided.
   See the :ref:`ap-verify-cmd` for details.

.. TODO: remove this note after resolving DM-13042

.. warning::

   ``ap_verify.py`` does not support running multiple instances concurrently.
   Attempting to run two or more programs, particularly from the same working directory, may cause them to compete for access to the workspace or to overwrite each others' metrics.

.. _ap-verify-run-rerun:

How to Run ap_verify in the Dataset Directory
---------------------------------------------

It is also possible to place a workspace in a subdirectory of a dataset directory. The syntax for this mode is:

.. prompt:: bash

   python python/lsst/ap/verify/ap_verify.py --dataset HiTS2015 --rerun run1 --dataIdString "visit=54123 ccd=25 filter=g" --silent

The :command:`--rerun run1` argument will create a workspace in :file:`<hits-data>/rerun/run1/`.
Since datasets are :ref:`not, in general, repositories<ap-verify-datasets-butler>`, the :option:`--rerun <ap_verify.py --rerun>` parameter only superficially resembles the analogous argument for command-line tasks.
In particular, ``ap_verify``'s ``--rerun`` does not support repository chaining (as in :command:`--rerun input:output`); the input for ``ap_verify`` will always be determined by the :option:`--dataset <ap_verify.py --dataset>`.

.. _ap-verify-results:

How to Use Measurements of Metrics
----------------------------------

After ``ap_verify`` has run, it will produce a file named :file:`ap_verify.verify.json` in the working directory.
This file contains metric measurements in `lsst.verify` format, and can be loaded and read as described in the `lsst.verify` documentation or in `SQR-019 <https://sqr-019.lsst.io>`_.
The file name is currently hard-coded, but may be customizable in a future version.

Unless the :option:`--silent <ap_verify.py --silent>` argument is provided, ``ap_verify`` will also upload measurements to the `SQuaSH service <https://squash.lsst.codes/>`_ on completion.
See the SQuaSH documentation for details.

If the pipeline is interrupted by a fatal error, completed measurements will be saved to :file:`ap_verify.verify.json` for debugging purposes, but nothing will get sent to SQuaSH.
See the :ref:`error-handling policy <ap-verify-failsafe-partialmetric>` for details.

