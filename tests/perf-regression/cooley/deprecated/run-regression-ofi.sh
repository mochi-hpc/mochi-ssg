#!/bin/bash

# This is a shell script to be run from a login node of the Cooley system at
# the ALCF, that will download, compile, and execute the ssg performance 
# regression tests, including any dependencies

# exit on any error
set -e

SANDBOX=/tmp/mochi-regression-$$
PREFIX=~/tmp/mochi-regression-install-$$
JOBDIR=~/tmp/mochi-regression-job-$$

# less ancient gcc
export CC=/soft/compilers/gcc/7.1.0/bin/gcc
export CFLAGS="-O3"
export PKG_CONFIG_PATH=$PREFIX/lib/pkgconfig
export PATH=/soft/buildtools/cmake/current/bin/:$PATH # working cmake version

# scratch area to clone and build things
mkdir $SANDBOX

# scratch area for job submission
mkdir $JOBDIR
cp margo-regression-ofi.qsub $JOBDIR
cp hg-verbs-no-lock.patch $SANDBOX

cd $SANDBOX
git clone https://github.com/carns/argobots.git
git clone https://github.com/ofiwg/libfabric.git
#git clone https://github.com/carns/cci.git
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
 
# libfabric
# NOTE: check out 1.5.x branch; as of late January 2018, master includes an
#       initial perf counter implementation that won't work on Cooley's old 
#       kernel
echo "=== BUILDING LIBFABRIC ==="
cd $SANDBOX/libfabric
libtoolize
./autogen.sh
mkdir build
cd build
../configure --prefix=$PREFIX --enable-sockets --enable-verbs 
make -j 3
make install

# cci
#echo "=== BUILDING CCI ==="
#cd $SANDBOX/cci
#git checkout dev-destroy-endpoint-hang
#libtoolize
#./autogen.pl
#mkdir build
#cd build
#../configure --prefix=$PREFIX --enable-plugins-no-build=ctp-sm
#make -j 3
#make install
 
# mercury
echo "=== BUILDING MERCURY ==="
cd $SANDBOX/mercury
git submodule update --init
# patch to turn off extra locking for verbs provider
patch -p1 < $SANDBOX/hg-verbs-no-lock.patch
mkdir build
cd build
cmake -DNA_USE_OFI:BOOL=ON -DMERCURY_USE_BOOST_PP:BOOL=ON -DCMAKE_INSTALL_PREFIX=/$PREFIX -DBoost_NO_BOOST_CMAKE=TRUE -DBUILD_SHARED_LIBS:BOOL=ON -DMERCURY_USE_SELF_FORWARD:BOOL=ON -DNA_USE_SM:BOOL=OFF ../
#cmake -DNA_USE_OFI:BOOL=ON -DNA_USE_CCI:BOOL=ON -DMERCURY_USE_BOOST_PP:BOOL=ON -DCMAKE_INSTALL_PREFIX=/$PREFIX -DBoost_NO_BOOST_CMAKE=TRUE -DBUILD_SHARED_LIBS:BOOL=ON -DMERCURY_USE_SELF_FORWARD:BOOL=ON -DNA_USE_SM:BOOL=OFF ../
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
cp $SANDBOX/ssg/build/tests/perf-regression/.libs/margo-p2p-bw $JOBDIR
cp $PREFIX/libexec/osu-micro-benchmarks/mpi/pt2pt/osu_latency $JOBDIR
cp $PREFIX/bin/mercury-runner $JOBDIR
cd $JOBDIR
JOBID=`qsub --env LD_LIBRARY_PATH=$PREFIX/lib ./margo-regression-ofi.qsub`
cqwait $JOBID

echo "=== JOB DONE, COLLECTING AND SENDING RESULTS ==="
# gather output, strip out funny characters, mail
cat $JOBID.* > combined.$JOBID.txt
#dos2unix combined.$JOBID.txt
mailx -s "margo-regression (cooley, ofi)" carns@mcs.anl.gov < combined.$JOBID.txt

cd /tmp
rm -rf $SANDBOX
rm -rf $PREFIX