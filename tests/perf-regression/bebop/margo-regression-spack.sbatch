#!/bin/bash
#SBATCH -N 2
#SBATCH -A radix
#SBATCH --ntasks-per-node=1
#SBATCH --time=15:00
#SBATCH -p bdwall

# example of running benchmarks with spack-compiled software stack
# builds correctly but fails to open network device as of 8-13-2018

module load numactl

. /home/carns/working/src/spack/share/spack/setup-env.sh

spack load -r ssg

export FI_PSM2_NAME_SERVER=1

echo "## Margo OFI/PSM2 (round trip, bdw):"
mpirun ./margo-p2p-latency -i 100000 -n psm2://ib0:5000
echo "## Margo OFI/PSM2 (bw, bdw):"
mpirun ./margo-p2p-bw -x 1048576 -n psm2://ib0:5000 -c 1 -D 20
echo "## Margo OFI/PSM2 (bw, 8x concurrency, bdw):"
mpirun ./margo-p2p-bw -x 1048576 -n psm2://ib0:5000 -c 8 -D 20

echo "## Margo OFI/PSM2 (round trip, bdw, Hg busy spin):"
mpirun ./margo-p2p-latency -i 100000 -n psm2://ib0:5000 -t 0,0
echo "## Margo OFI/PSM2 (bw, bdw, Hg busy spin):"
mpirun ./margo-p2p-bw -x 1048576 -n psm2://ib0:5000 -c 1 -D 20 -t 0,0
echo "## Margo OFI/PSM2 (bw, 8x concurrency, bdw, Hg busy spin):"
mpirun ./margo-p2p-bw -x 1048576 -n psm2://ib0:5000 -c 8 -D 20 -t 0,0

 
