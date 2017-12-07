#!/bin/bash -l

#SBATCH -p debug
#SBATCH -N 1
#SBATCH -n 4953
#SBATCH -t 00:10:00
#SBATCH -J ap_verify

srun --output job%j-%2t.out --ntasks=4953 --multi-prog demo_cmds.conf
