#!/bin/bash
#SBATCH -N 2
#SBATCH -A radix
#SBATCH --ntasks-per-node=1
#SBATCH --time=15:00
#SBATCH -p bdwall

export FI_PSM2_NAME_SERVER=1

echo "## MPI (one way, double the latency for round trip, bdw):"
mpirun ./osu_latency

# NOTE: the -IB option forces Intel MPI to use Infiniband fabric instead of 
#  PSM2.  If we use PSM2 there is a conflict and Mercury won't start
#  (apparently because MPI is using MR_SCALABLE and Mercury is using MR_BASIC?)

export I_MPI_FABRICS=shm:ofa

echo "## Margo OFI/PSM2 (round trip, bdw):"
mpirun ./margo-p2p-latency -i 100000 -n psm2://ib0:5000
echo "## Margo OFI/PSM2 (round trip, bdw, Hg busy spin):"
mpirun ./margo-p2p-latency -i 100000 -n psm2://ib0:5000 -t 0,0

unset I_MPI_FABRICS

echo "## Margo OFI/VERBS (round trip, bdw):"
mpirun ./margo-p2p-latency -i 100000 -n verbs://ib0:5000
echo "## Margo OFI/VERBS (round trip, bdw, Hg busy spin):"
mpirun ./margo-p2p-latency -i 100000 -n verbs://ib0:5000 -t 0,0
