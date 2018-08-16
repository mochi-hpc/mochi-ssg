#!/bin/bash
#SBATCH -N 2
#SBATCH -A radix
#SBATCH --ntasks-per-node=1
#SBATCH --time=15:00
#SBATCH -p bdwall

# example of running benchmarks with spack-compiled software stack

export LMOD_PAGER=none

module load numactl boost cmake

. /home/carns/working/src/spack/share/spack/setup-env.sh

spack load -r ssg

module list

# force Intel MPI to use Infiniband instead of Omnipath/PSM2.  If we let 
# Intel MPI use it's preferred PSM2 path then Mercury can't open interface
# because of apparent MR_SCALABLE/MR_BASIC conflict
# we should also try MR_SCALABLE in Mercury NA as a possible workaround
export I_MPI_FABRICS=shm:ofa

echo "## Margo OFI/PSM2 (round trip, bdw):"
mpirun ./margo-p2p-latency -i 100000 -n psm2://enp6s0f0:5000
echo "## Margo OFI/PSM2 (bw, bdw):"
mpirun ./margo-p2p-bw -x 1048576 -n psm2://enp6s0f0:5000 -c 1 -D 20
echo "## Margo OFI/PSM2 (bw, 8x concurrency, bdw):"
mpirun ./margo-p2p-bw -x 1048576 -n psm2://enp6s0f0:5000 -c 8 -D 20

echo "## Margo OFI/PSM2 (round trip, bdw, Hg busy spin):"
mpirun ./margo-p2p-latency -i 100000 -n psm2://enp6s0f0:5000 -t 0,0
echo "## Margo OFI/PSM2 (bw, bdw, Hg busy spin):"
mpirun ./margo-p2p-bw -x 1048576 -n psm2://enp6s0f0:5000 -c 1 -D 20 -t 0,0
echo "## Margo OFI/PSM2 (bw, 8x concurrency, bdw, Hg busy spin):"
mpirun ./margo-p2p-bw -x 1048576 -n psm2://enp6s0f0:5000 -c 8 -D 20 -t 0,0

 
