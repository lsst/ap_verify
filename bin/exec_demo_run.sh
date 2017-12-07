#!/bin/bash

source /software/lsstsw/stack/loadLSST.bash
setup lsst_distrib
setup obs_decam

AP_VERIFY_HITS2015_DIR=/project/krzys001/ap_verify_hits2015
setup -k -r $AP_VERIFY_HITS2015_DIR


BASE_DIR=./
setup -k -r $BASE_DIR/ap_association
setup -k -r $BASE_DIR/ap_pipe
setup -k -r $BASE_DIR/ap_verify



# CCD 1 has image subtraction problems, CCDs 2 and 61 are missing
for i in `seq 3 60`; do
	ap_verify.py --dataset HiTS2015 --output ap_verify_HiTS --dataIdString "visit=${1} ccdnum=$i filter=g" --silent;
done

ap_verify.py --dataset HiTS2015 --output ap_verify_HiTS --dataIdString "visit=${1} ccdnum=62 filter=g" --silent;
