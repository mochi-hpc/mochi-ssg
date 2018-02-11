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
module load craype-hugepages2M

# need newer cmake
module load cmake

export CC=cc
export CXX=CC
export CFLAGS="-O3 -I$BOOST_ROOT/include"
export CXXFLAGS="-O3"
export PKG_CONFIG_PATH="$PREFIX/lib/pkgconfig:$PKG_CONFIG_PATH"

# scratch area to clone and build things
mkdir $SANDBOX

# scratch area for job submission
mkdir $JOBDIR
cp margo-regression-static.qsub $JOBDIR

cd $SANDBOX
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
# git clone https://github.com/pdlfs/mercury-runner.git

# OSU MPI benchmarks
echo "=== BUILDING OSU MICRO BENCHMARKS ==="
cd $SANDBOX/osu-micro-benchmarks-5.3.2
mkdir build
cd build
../configure --prefix=$PREFIX --host=x86_64-linux 
make -j 3
make install

# argobots
echo "=== BUILDING ARGOBOTS ==="
cd $SANDBOX/argobots
git checkout dev-get-dev-basic
./autogen.sh
mkdir build
cd build
../configure --prefix=$PREFIX --enable-perf-opt --enable-shared=no --enable-static=yes --host=x86_64-linux 
make -j 3
make install
 
# libfabric
echo "=== BUILDING LIBFABRIC ==="
cd $SANDBOX/libfabric
./autogen.sh
mkdir build
cd build
../configure --prefix=$PREFIX --enable-gni --with-kdreg=no --enable-ugni-static --enable-sockets --disable-rxd --disable-rxm --disable-udp --disable-usnic --disable-verbs --host=x86_64-linux --enable-static=yes --enable-shared=no
make -j 3
make install

# mercury
echo "=== BUILDING MERCURY ==="
cd $SANDBOX/mercury
git submodule update --init
mkdir build
cd build
cmake -DNA_USE_OFI:BOOL=ON -DMERCURY_USE_BOOST_PP:BOOL=ON -DCMAKE_INSTALL_PREFIX=/$PREFIX -DMERCURY_USE_SELF_FORWARD:BOOL=ON -DNA_USE_SM:BOOL=ON ../
make -j 3
make install

# fix mercury and libfabric pc file

# explanation: the static lib dependencies for libfabric might be more complex
#    than just -lfabric; pull everything from their .pc file
sed -i 's/Requires:/Requires: libfabric/g' $PREFIX/lib/pkgconfig/mercury.pc

# explanation: libfabric seems to use some shm_ functions that are provided 
#    by -lrt
sed -i '/^Libs:/ s/$/ -lrt/' $PREFIX/lib/pkgconfig/libfabric.pc
sed -i '/^Libs.private:/ s/$/ -lrt/' $PREFIX/lib/pkgconfig/libfabric.pc

# explanation: this is an ordering problem; really mercury_util probably just
#    needs to come after na so that na symbol usage can be resolved, but
#    we just append here because it's easier with sed and duplicates don't
#    really hurt anything
sed -i '/^Libs:/ s/$/ -lmercury_util/' $PREFIX/lib/pkgconfig/mercury.pc

# libev
echo "=== BUILDING LIBEV ==="
cd $SANDBOX/libev-4.24
mkdir build
cd build
../configure --prefix=$PREFIX --enable-shared=no --enable-static=yes --host=x86_64-linux 
make -j 3
make install

# abt-snoozer
echo "=== BUILDING ABT-SNOOZER ==="
cd $SANDBOX/abt-snoozer
./prepare.sh
mkdir build
cd build
../configure --prefix=$PREFIX --enable-shared=no  --host=x86_64-linux
make -j 3
make install

# margo
echo "=== BUILDING MARGO ==="
cd $SANDBOX/margo
./prepare.sh
mkdir build
cd build
../configure --prefix=$PREFIX  --enable-shared=no --host=x86_64-linux
make -j 3 
make install

# ssg
echo "=== BUILDING SSG ==="
cd $SANDBOX/ssg
./prepare.sh
mkdir build
cd build
../configure --prefix=$PREFIX  --enable-shared=no --host=x86_64-linux 
make -j 3 
make install
make tests

# mercury-runner benchmark
#echo "=== BUILDING MERCURY-RUNNER BENCHMARK ==="
#cd $SANDBOX/mercury-runner
#mkdir build
#cd build
#cmake -DCMAKE_PREFIX_PATH=$PREFIX -DCMAKE_INSTALL_PREFIX=$PREFIX -DMPI=ON ..
#make -j 3
#make install

# set up job to run
echo "=== SUBMITTING AND WAITING FOR JOB ==="
cp $SANDBOX/ssg/build/tests/perf-regression/margo-p2p-latency $JOBDIR
cp $SANDBOX/ssg/build/tests/perf-regression/margo-p2p-bw $JOBDIR
cp $PREFIX/bin/fi_pingpong $JOBDIR
cp $PREFIX/libexec/osu-micro-benchmarks/mpi/pt2pt/osu_latency $JOBDIR
#cp $PREFIX/bin/mercury-runner $JOBDIR
cd $JOBDIR
JOBID=`qsub ./margo-regression-static.qsub`
cqwait $JOBID

echo "=== JOB DONE, COLLECTING AND SENDING RESULTS ==="
# gather output, strip out funny characters, mail
cat $JOBID.* > combined.$JOBID.txt
dos2unix combined.$JOBID.txt
mailx -s "margo-regression (theta static)" sds-commits@lists.mcs.anl.gov < combined.$JOBID.txt

cd /tmp
echo sandbox: $SANDBOX
echo prefix: $PREFIX
echo jobdir: $JOBDIR
rm -rf $SANDBOX
rm -rf $PREFIX
