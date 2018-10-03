#!/bin/bash

# This file is part of ap_verify.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (https://www.lsst.org).
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
# along with this program.  If not, see <https://www.gnu.org/licenses/>.

# Simple script for running an entire dataset through ap_verify
# Assumes that the requested dataset is already set up in EUPS
#
# The ap_verify workspace is a directory with the dataset name; until DM-13887
# gets resolved, metrics are dumped to the workspace directory with names
# containing each run's data ID.

set -e

print_error() {
    >&2 echo "$@"
}

usage() {
    print_error
    print_error "Usage: $0 -d DATASET [-h]"
    print_error
    print_error "Specifc options:"
    print_error "   -d          Dataset name"
    print_error "   -h          show this message"
    exit 1
}

while getopts "d:h" option; do
    case "$option" in
        d)  DATASET="$OPTARG";;
        h)  usage;;
        *)  usage;;
    esac
done
if [[ -z "${DATASET}" ]]; then
    print_error "$0: mandatory argument -- d"
    usage
    exit 1
fi
shift $((OPTIND-1))

PRODUCT_DIR=${AP_VERIFY_DIR}
# OS X El Capitan SIP swallows DYLD_LIBRARY_PATH so export the duplicate in LSST_LIBRARY_PATH
if [[ -z $DYLD_LIBRARY_PATH ]]; then
    export DYLD_LIBRARY_PATH=$LSST_LIBRARY_PATH
fi

WORKSPACE=${DATASET}
if [[ -d $WORKSPACE ]]; then
   rm -rf "${WORKSPACE}"
fi
# Would be created by ap_verify, but the OS might try to open the log first
mkdir "${WORKSPACE}"

# Store processor count (parellelism not yet implemented, see DM-13887)
MACH=$(uname -s)
if [[ $MACH == Darwin ]]; then
    sys_proc=$(sysctl -n hw.logicalcpu)
else
    sys_proc=$(grep -c processor /proc/cpuinfo)
fi
max_proc=8
NUMPROC=${NUMPROC:-$((sys_proc < max_proc ? sys_proc : max_proc))}

# Extract desired dataIds runs from config file
# Workaround for DM-13887
IDLIST="${PRODUCT_DIR}"/config/${DATASET}.list

while read -r ID; do
    echo "Running ap_verify on ${ID}..."
    ap_verify.py --dataset "${DATASET}" \
        --id "${ID}" \
        --output "${WORKSPACE}" \
        --processes "${NUMPROC}" \
        --metrics-file "${WORKSPACE}/ap_verify.${ID}.verify.json" \
        --silent \
        &>> "${WORKSPACE}"/apVerify.log
done < "${IDLIST}"