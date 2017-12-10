#!/bin/bash -l

#SBATCH -p debug
#SBATCH -N 6
#SBATCH -n 84
#SBATCH -t 12:00:00
#SBATCH -J ap_verify

srun --output job%j-%2t.out --ntasks=84 --multi-prog demo_cmds.conf
