#!/bin/bash
#SBATCH -N 2
#SBATCH -A radix
#SBATCH --ntasks-per-node=1
#SBATCH --time=15:00
#SBATCH -p bdwall

export FI_PSM2_NAME_SERVER=1

echo "## MPI (one way, double the latency for round trip):"
mpirun ./osu_latency

echo "## Margo OFI/PSM2 (round trip):"
mpirun ./margo-p2p-latency -i 100000 -n psm2://ib0:5000
echo "## Margo OFI/PSM2 (round trip, Hg busy spin):"
mpirun ./margo-p2p-latency -i 100000 -n psm2://ib0:5000 -t 0,0


