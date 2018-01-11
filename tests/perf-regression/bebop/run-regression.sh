#!/bin/bash

# This is a shell script to be run from a login node of the Bebop system at
# the LCRC, that will download, compile, and execute the ssg performance 
# regression tests, including any dependencies

# exit on any error
set -e

SANDBOX=/tmp/mochi-regression-$$
PREFIX=~/tmp/mochi-regression-install-$$
JOBDIR=~/tmp/mochi-regression-job-$$

export CFLAGS="-O3"
export PKG_CONFIG_PATH=$PREFIX/lib/pkgconfig

module load numactl
module load cmake
module load boost

# scratch area to clone and build things
mkdir $SANDBOX

# scratch area for job submission
mkdir $JOBDIR

cp margo-p2p-latency.sbatch $JOBDIR
# TODO: this is temporary, the real fix is in topic_ofi branch and will
#  be merged at some point
cp mercury-psm2-mr-basic.patch $SANDBOX

cd $SANDBOX
git clone http://git.mpich.org/openpa.git/
git clone https://github.com/01org/opa-psm2.git
git clone https://github.com/ofiwg/libfabric.git
git clone https://github.com/carns/argobots.git
git clone https://github.com/mercury-hpc/mercury.git
wget http://dist.schmorp.de/libev/libev-4.24.tar.gz
tar -xvzf libev-4.24.tar.gz
git clone https://xgitlab.cels.anl.gov/sds/abt-snoozer.git
git clone https://xgitlab.cels.anl.gov/sds/margo.git
git clone https://xgitlab.cels.anl.gov/sds/ssg.git
wget http://mvapich.cse.ohio-state.edu/download/mvapich/osu-micro-benchmarks-5.3.2.tar.gz
tar -xvzf osu-micro-benchmarks-5.3.2.tar.gz
git clone https://github.com/pdlfs/mercury-runner.git

# OSU MPI benchmarks
echo "=== BUILDING OSU MICRO BENCHMARKS ==="
cd $SANDBOX/osu-micro-benchmarks-5.3.2
mkdir build
cd build
../configure --prefix=$PREFIX CC=mpicc CXX=mpicxx
make -j 3
make install

# argobots
echo "=== BUILDING ARGOBOTS ==="
cd $SANDBOX/argobots
git checkout dev-get-dev-basic
libtoolize
./autogen.sh
mkdir build
cd build
../configure --prefix=$PREFIX --enable-perf-opt
make -j 3
make install
  
# opa-psm2
echo "=== BUILDING LIBFABRIC ==="
cd $SANDBOX/opa-psm2
make
make DESTDIR=$PREFIX install

# TODO: it doesn't seem like this should be necessary; we are specifying path
# to lib in libfabric build.
LD_LIBRARY_PATH=$PREFIX/usr/lib64:$LD_LIBRARY_PATH

# libfabric
echo "=== BUILDING LIBFABRIC ==="
cd $SANDBOX/libfabric
./autogen.sh
mkdir build
cd build
../configure --prefix=$PREFIX --enable-sockets --enable-psm2=$PREFIX/usr --enable-verbs 
make -j 3
make install

# openpa
echo "=== BUILDING OPENPA ==="
cd $SANDBOX/openpa
./autogen.sh
mkdir build
cd build
../configure --prefix=$PREFIX 
make -j 3
make install

# mercury
echo "=== BUILDING MERCURY ==="
cd $SANDBOX/mercury
git submodule update --init
patch -p1 < $SANDBOX/mercury-psm2-mr-basic.patch
mkdir build
cd build
cmake -DNA_USE_OFI:BOOL=ON -DMERCURY_USE_BOOST_PP:BOOL=ON -DCMAKE_INSTALL_PREFIX=/$PREFIX -DBUILD_SHARED_LIBS:BOOL=ON -DMERCURY_USE_SELF_FORWARD:BOOL=ON -DNA_USE_SM:BOOL=ON ../
make -j 3
make install

# libev
echo "=== BUILDING LIBEV ==="
cd $SANDBOX/libev-4.24
mkdir build
cd build
../configure --prefix=$PREFIX
make -j 3
make install

# abt-snoozer
echo "=== BUILDING ABT-SNOOZER ==="
cd $SANDBOX/abt-snoozer
libtoolize
./prepare.sh
mkdir build
cd build
../configure --prefix=$PREFIX 
make -j 3
make install

# margo
echo "=== BUILDING MARGO ==="
cd $SANDBOX/margo
libtoolize
./prepare.sh
mkdir build
cd build
../configure --prefix=$PREFIX 
make -j 3
make install

# ssg
echo "=== BUILDING SSG ==="
cd $SANDBOX/ssg
libtoolize
./prepare.sh
mkdir build
cd build
../configure --prefix=$PREFIX CC=mpicc
make -j 3
make install
make tests

# mercury-runner benchmark
echo "=== BUILDING MERCURY-RUNNER BENCHMARK ==="
cd $SANDBOX/mercury-runner
mkdir build
cd build
CC=mpicc CXX=mpicxx CXXFLAGS='-D__STDC_FORMAT_MACROS' cmake -DCMAKE_PREFIX_PATH=$PREFIX -DCMAKE_INSTALL_PREFIX=$PREFIX -DMPI=ON ..
make -j 3
make install

# set up job to run
echo "=== SUBMITTING AND WAITING FOR JOB ==="
cp $SANDBOX/ssg/build/tests/perf-regression/.libs/margo-p2p-latency $JOBDIR
cp $PREFIX/libexec/osu-micro-benchmarks/mpi/pt2pt/osu_latency $JOBDIR
cp $PREFIX/bin/mercury-runner $JOBDIR
cd $JOBDIR
sbatch --wait --export=ALL ./margo-p2p-latency.sbatch

echo "=== JOB DONE, COLLECTING AND SENDING RESULTS ==="
# gather output, strip out funny characters, mail
cat *.out > combined.JOBIDtxt
dos2unix combined.JOBID.txt
mailx -s "margo-p2p-latency (bebop)" carns@lists.mcs.anl.gov < combined.JOBID.txt

cd /tmp
rm -rf $SANDBOX
rm -rf $PREFIX
