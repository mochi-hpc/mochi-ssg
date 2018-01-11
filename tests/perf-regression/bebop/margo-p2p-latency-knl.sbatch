#!/bin/bash
#SBATCH -N 2
#SBATCH -A radix
#SBATCH --ntasks-per-node=1
#SBATCH --time=15:00
#SBATCH -p knlall

# NOTE: is this still needed?  Assume so for now.
export FI_PSM2_NAME_SERVER=1

echo "## MPI (one way, double the latency for round trip, knl):"
mpirun ./osu_latency

# NOTE: the -IB option forces Intel MPI to use Infiniband fabric instead of 
#  PSM2.  If we use PSM2 there is a conflict and Mercury won't start
#  (apparently because MPI is using MR_SCALABLE and Mercury is using MR_BASIC?)

echo "## Margo OFI/PSM2 (round trip, knl):"
mpirun -IB ./margo-p2p-latency -i 100000 -n psm2://ib0:5000
echo "## Margo OFI/PSM2 (round trip, knl, Hg busy spin):"
mpirun -IB ./margo-p2p-latency -i 100000 -n psm2://ib0:5000 -t 0,0


