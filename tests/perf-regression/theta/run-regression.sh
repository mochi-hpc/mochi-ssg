#!/bin/bash

# This is a shell script to be run from a login node of the Theta system at
# the ALCF, that will download, compile, and execute the ssg performance 
# regression tests, including any dependencies

# exit on any error
set -e

SANDBOX=/tmp/mochi-regression-$$
PREFIX=~/tmp/mochi-regression-install-$$
JOBDIR=~/tmp/mochi-regression-job-$$

# gcc
module swap PrgEnv-intel PrgEnv-gnu
module load boost/gnu

export CC=gcc
export CFLAGS="-O3 -I$BOOST_ROOT/include"
export PKG_CONFIG_PATH="$PREFIX/lib/pkgconfig:$PKG_CONFIG_PATH"
export CRAYPE_LINK_TYPE=dynamic

# scratch area to clone and build things
mkdir $SANDBOX

# scratch area for job submission
mkdir $JOBDIR
cp margo-p2p-latency.qsub $JOBDIR

cd $SANDBOX
git clone https://github.com/ofiwg/libfabric.git
git clone https://github.com/pmodels/argobots.git
git clone https://github.com/mercury-hpc/mercury.git
wget http://dist.schmorp.de/libev/libev-4.24.tar.gz
tar -xvzf libev-4.24.tar.gz
git clone https://xgitlab.cels.anl.gov/sds/abt-snoozer.git
git clone https://xgitlab.cels.anl.gov/sds/margo.git
git clone https://xgitlab.cels.anl.gov/sds/ssg.git

# argobots
echo "=== BUILDING ARGOBOTS ==="
cd $SANDBOX/argobots
./autogen.sh
mkdir build
cd build
../configure --prefix=$PREFIX --enable-perf-opt
make -j 3
make install
 
# libfabric
echo "=== BUILDING LIBFABRIC ==="
cd $SANDBOX/libfabric
./autogen.sh
mkdir build
cd build
../configure --prefix=$PREFIX --enable-ugni-static --disable-rxd --disable-rxm --disable-udp --disable-usnic --disable-verbs --disable-sockets
make -j 3
make install

# mercury
echo "=== BUILDING MERCURY ==="
cd $SANDBOX/mercury
git checkout topic_ofi_gni
mkdir build
cd build
cmake -DNA_USE_OFI:BOOL=ON -DMERCURY_USE_BOOST_PP:BOOL=ON -DCMAKE_INSTALL_PREFIX=/$PREFIX -DMERCURY_USE_CHECKSUMS:BOOL=OFF -DBUILD_SHARED_LIBS:BOOL=ON -DMERCURY_USE_SELF_FORWARD:BOOL=ON -DNA_USE_SM:BOOL=ON ../
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
./prepare.sh
mkdir build
cd build
../configure --prefix=$PREFIX 
make -j 3
make install

# margo
echo "=== BUILDING MARGO ==="
cd $SANDBOX/margo
./prepare.sh
mkdir build
cd build
../configure --prefix=$PREFIX 
make -j 3
make install

# ssg
echo "=== BUILDING SSG ==="
cd $SANDBOX/ssg
./prepare.sh
mkdir build
cd build
../configure --prefix=$PREFIX --host=x86_64 CC=cc 
make -j 3
make install
make tests

# set up job to run
echo "=== SUBMITTING AND WAITING FOR JOB ==="
cp $SANDBOX/ssg/build/tests/perf-regression/margo-p2p-latency $JOBDIR
cd $JOBDIR
JOBID=`qsub --env LD_LIBRARY_PATH=$PREFIX/lib ./margo-p2p-latency.qsub`
cqwait $JOBID

echo "=== JOB DONE, COLLECTING AND SENDING RESULTS ==="
# gather output, strip out funny characters, mail
cat $JOBID.* > combined.$JOBID.txt
dos2unix combined.$JOBID.txt
mailx -s "margo-p2p-latency (theta, ofi/gni)" sds-commits@lists.mcs.anl.gov < combined.$JOBID.txt

cd /tmp
rm -rf $SANDBOX
rm -rf $PREFIX
