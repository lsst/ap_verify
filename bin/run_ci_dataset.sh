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
# The ap_verify workspace is a directory with the dataset name; metrics are
# dumped to the workspace directory with names containing each run's data ID.

set -e

print_error() {
    >&2 echo "$@"
}

usage() {
    print_error
    print_error "Usage: $0 -d DATASET [-g NUM] [-p PATH] [-n NAMESPACE] [-u URL] [-h]"
    print_error
    print_error "Specific options:"
    print_error "   -d          Dataset name"
    print_error "   -g          Middleware generation number (int) [currently unused]"
    print_error "   -p          Pipeline to run"
    print_error "   -n          Namespace for metrics upload (optional, but required if -u is set)"
    print_error "   -u          URL for metrics upload (optional, but required if -n is set)"
    print_error "   -h          show this message"
    exit 1
}

while getopts "d:g:p:n:u:h" option; do
    case "$option" in
        d)  DATASET="$OPTARG";;
        g)  GEN="$OPTARG";;
        p)  PIPE="$OPTARG";;
        n)  NAMESPACE="$OPTARG";;
        u)  URL="$OPTARG";;
        h)  usage;;
        *)  usage;;
    esac
done

if [[ -z "${DATASET}" ]]; then
    print_error "$0: mandatory argument -- d"
    usage
    exit 1
fi

# Ensure both NAMESPACE and URL exist, or neither does
if { [[ -n "${NAMESPACE}" ]] && [[ -z "${URL}" ]]; } || { [[ -z "${NAMESPACE}" ]] && [[ -n "${URL}" ]]; }; then
    print_error "Error: Both -n (namespace) and -u (URL) must be provided together, or neither."
    usage
    exit 1
fi

# Set PIPE argument if provided
if [[ -n "${PIPE}" ]]; then
    PIPE="--pipeline ${PIPE}"
fi

# Set NAMESPACE and URL arguments if both are provided
if [[ -n "${NAMESPACE}" && -n "${URL}" ]]; then
    NAMESPACE_ARG="--namespace ${NAMESPACE}"
    URL_ARG="--restProxyUrl ${URL}"
else
    NAMESPACE_ARG=""
    URL_ARG=""
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

# Store processor count
MACH=$(uname -s)
if [[ $MACH == Darwin ]]; then
    sys_proc=$(sysctl -n hw.logicalcpu)
else
    sys_proc=$(grep -c processor /proc/cpuinfo)
fi
max_proc=8
NUMPROC=${NUMPROC:-$((sys_proc < max_proc ? sys_proc : max_proc))}

echo "Running ap_verify on ${DATASET}..."
ap_verify.py --dataset "${DATASET}" \
    ${PIPE} \
    --output "${WORKSPACE}" \
    --processes "${NUMPROC}" \
    ${NAMESPACE_ARG} \
    ${URL_ARG} \
    &>> "${WORKSPACE}"/apVerify.log
