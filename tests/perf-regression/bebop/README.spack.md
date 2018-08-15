To compile the full stack on bebop as of August 2018 using spack:

# load numactl in environment; it looks like this is an implied dependency
# of one of the packages

```
module load numactl
```

# get spack repos:
```
git clone https://github.com/spack/spack.git
cd spack
# as of 2018-08-13, need to patch to bump libfabric to 1.6.1 so it will
# build with Intel compiler
patch -p0 < spack-libfabric-1.6.1.patch
. share/spack/setup-env.sh
# do not bootstrap; we will use existing lmod install on Bebop

git clone git@xgitlab.cels.anl.gov:sds/sds-repo
spack repo add .
```

# edit .spack/linux/packages.yaml to look like this:
# The important things are to use the existing Intel compiler and MPI library
```
packages:
    all:
        compiler: [intel, gcc]
        providers:
            mpi: [intel-mpi]
    openssl:
        paths:
            openssl@1.0.2k: /usr
        buildable: False
    bzip2:
        paths:
            bzip2@1.0.6: /
        buildable: False
    bison:
        paths:
            bison@3.0.4: /
        buildable: False
    flex:
        paths:
            flex@2.5.37: /
        buildable: False
    coreutils:
        paths:
            coreutils@8.22: /usr
        buildable: False
    zlib:
        paths:
            zlib@1.2.7: /usr
        buildable: False
    tar:
        paths:
            tar@1.26: /
        buildable: False
    gettext:
        paths:
            gettext@0.19: /usr
        buildable: False
    tcl:
        paths:
            tcl@8.5.13: /usr
        buildable: False
    perl:
        paths:
            perl@5.16.3: /usr
        buildable: False
    autoconf:
        paths:
            autoconf@2.69: /usr
        buildable: False
    automake:
        paths:
            automake@1.13.4: /usr
        buildable: False
    ncurses:
        paths:
            ncurses@5.9: /usr
        buildable: False
    intel-mpi:
        paths:
            intel-mpi@2017.3: /blues/gpfs/home/software/spack-0.10.1/opt/spack/linux-centos7-x86_64/intel-17.0.4/intel-mpi-2017.3-dfphq6kavje2olnichisvjjndtridrok
        buildable: False
    ssg:
        variants: +mpi
    libfabric:
        variants: fabrics=psm2
```
# compile everything and load module for ssg
spack install ssg
spack load -r ssg

