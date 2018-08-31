#!/bin/bash

# This is a shell script to be run from a login node of the Theta system at
# the ALCF, that will download, compile, and execute the ssg performance 
# regression tests, including any dependencies

# SEE README.spack.md for environment setup information!  This script will not
#     work properly without properly configured spack environment

# exit on any error
set -e

module swap PrgEnv-intel PrgEnv-gnu
module load cce

export CFLAGS="-O3"
export CRAYPE_LINK_TYPE=dynamic

SANDBOX=~/tmp/mochi-regression-sandbox-$$
PREFIX=~/tmp/mochi-regression-install-$$
JOBDIR=~/tmp/mochi-regression-job-$$

# scratch area to clone and build things
mkdir -p $SANDBOX
cp spack-shell.patch  $SANDBOX/

# scratch area for job submission
mkdir -p $JOBDIR
cp margo-regression-spack.qsub $JOBDIR

cd $SANDBOX
git clone https://github.com/spack/spack.git
git clone https://xgitlab.cels.anl.gov/sds/sds-repo.git
git clone https://xgitlab.cels.anl.gov/sds/ssg.git
wget http://mvapich.cse.ohio-state.edu/download/mvapich/osu-micro-benchmarks-5.3.2.tar.gz
tar -xvzf osu-micro-benchmarks-5.3.2.tar.gz
git clone https://github.com/pdlfs/mercury-runner.git

# set up most of the libraries in spack
echo "=== BUILD SPACK PACKAGES AND LOAD ==="
cd $SANDBOX/spack
patch -p1 < ../spack-shell.patch
export SPACK_SHELL=bash
. $SANDBOX/spack/share/spack/setup-env.sh
spack repo add $SANDBOX/sds-repo
spack uninstall -R -y argobots mercury libfabric  | true
spack install ssg
# deliberately repeat setup-env step after building modules to ensure
#   that we pick up the right module paths
. $SANDBOX/spack/share/spack/setup-env.sh
spack load -r ssg

# OSU MPI benchmarks
# echo "=== BUILDING OSU MICRO BENCHMARKS ==="
# cd $SANDBOX/osu-micro-benchmarks-5.3.2
# mkdir build
# cd build
# ../configure --prefix=$PREFIX CC=mpicc CXX=mpicxx
# make -j 3
# make install

# ssg
echo "=== BUILDING SSG TEST PROGRAMS ==="
cd $SANDBOX/ssg
libtoolize
./prepare.sh
mkdir build
cd build
../configure --prefix=$PREFIX CC=cc
make -j 3
make tests
make install

# mercury-runner benchmark
# echo "=== BUILDING MERCURY-RUNNER BENCHMARK ==="
# cd $SANDBOX/mercury-runner
# mkdir build
# cd build
# CC=mpicc CXX=mpicxx CXXFLAGS='-D__STDC_FORMAT_MACROS' cmake -DCMAKE_PREFIX_PATH=$PREFIX -DCMAKE_INSTALL_PREFIX=$PREFIX -DMPI=ON ..
# make -j 3
# make install

# set up job to run
echo "=== SUBMITTING AND WAITING FOR JOB ==="
cp $SANDBOX/ssg/build/tests/perf-regression/.libs/margo-p2p-latency $JOBDIR
cp $SANDBOX/ssg/build/tests/perf-regression/.libs/margo-p2p-bw $JOBDIR
# cp $PREFIX/libexec/osu-micro-benchmarks/mpi/pt2pt/osu_latency $JOBDIR
# cp $PREFIX/bin/mercury-runner $JOBDIR
cd $JOBDIR
JOBID=`qsub --env LD_LIBRARY_PATH=$PREFIX/lib --env SANDBOX=$SANDBOX ./margo-regression-spack.qsub`
cqwait $JOBID

echo "=== JOB DONE, COLLECTING AND SENDING RESULTS ==="
# gather output, strip out funny characters, mail
cat $JOBID.* > combined.$JOBID.txt
#dos2unix combined.$JOBID.txt
mailx -s "margo-regression (theta)" carns@mcs.anl.gov < combined.$JOBID.txt
cat combined.$JOBID.txt

cd /tmp
spack repo rm $SANDBOX/sds-repo
rm -rf $SANDBOX
rm -rf $PREFIX
