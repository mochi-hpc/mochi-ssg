#!/bin/bash
#SBATCH -N 2
#SBATCH -A radix
#SBATCH --ntasks-per-node=1
#SBATCH --time=20:00
#SBATCH -p knlall

export FI_PSM2_NAME_SERVER=1

echo "## MPI (one way, double the latency for round trip, knl):"
mpirun ./osu_latency

# NOTE: the -IB option forces Intel MPI to use Infiniband fabric instead of 
#  PSM2.  If we use PSM2 there is a conflict and Mercury won't start
#  (apparently because MPI is using MR_SCALABLE and Mercury is using MR_BASIC?)

export I_MPI_FABRICS=shm:ofa

echo "## Margo OFI/PSM2 (round trip, knl):"
mpirun ./margo-p2p-latency -i 100000 -n psm2://ib0:5000
echo "## Margo OFI/PSM2 (bw, knl):"
mpirun ./margo-p2p-bw -x 1048576 -n psm2://ib0:5000 -c 1 -D 20
echo "## Margo OFI/PSM2 (bw, 8x concurrency, knl):"
mpirun ./margo-p2p-bw -x 1048576 -n psm2://ib0:5000 -c 8 -D 20

echo "## Margo OFI/PSM2 (round trip, knl, Hg busy spin):"
mpirun ./margo-p2p-latency -i 100000 -n psm2://ib0:5000 -t 0,0
echo "## Margo OFI/PSM2 (bw, knl, Hg busy spin):"
mpirun ./margo-p2p-bw -x 1048576 -n psm2://ib0:5000 -c 1 -D 20 -t 0,0
echo "## Margo OFI/PSM2 (bw, 8x concurrency, knl, Hg busy spin):"
mpirun ./margo-p2p-bw -x 1048576 -n psm2://ib0:5000 -c 8 -D 20 -t 0,0

unset I_MPI_FABRICS

echo "## Margo OFI/VERBS (round trip, knl):"
mpirun ./margo-p2p-latency -i 100000 -n verbs://ib0:5000
echo "## Margo OFI/VERBS (bw, knl):"
mpirun ./margo-p2p-bw -x 1048576 -n verbs://ib0:5000 -c 1 -D 20
echo "## Margo OFI/VERBS (bw, 8x concurrency, knl):"
mpirun ./margo-p2p-bw -x 1048576 -n verbs://ib0:5000 -c 8 -D 20

echo "## Margo OFI/VERBS (round trip, knl, Hg busy spin):"
mpirun ./margo-p2p-latency -i 100000 -n verbs://ib0:5000 -t 0,0
echo "## Margo OFI/VERBS (bw, knl, Hg busy spin):"
mpirun ./margo-p2p-bw -x 1048576 -n verbs://ib0:5000 -c 1 -D 20 -t 0,0
echo "## Margo OFI/VERBS (bw, 8x concurrency, knl, Hg busy spin):"
mpirun ./margo-p2p-bw -x 1048576 -n verbs://ib0:5000 -c 8 -D 20 -t 0,0

