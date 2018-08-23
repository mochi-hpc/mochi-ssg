To compile the full stack on theta as of August 2018 using spack:
```

# get spack repos:
```
git clone https://github.com/spack/spack.git
cd spack
. share/spack/setup-env.sh
# do not bootstrap; we will use existing module environment on theta

git clone git@xgitlab.cels.anl.gov:sds/sds-repo
spack repo add .
```

# edit .spack/cray/packages.yaml to look like this:
# The important things are to use the existing compiler and MPI library
```
packages:
    mpich:
        modules: 
            mpich@7.7.2 arch=cray-cnl6-mic_knl: cray-mpich/7.7.2
        buildable: False
    autoconf:
        paths:
            autoconf@2.69: /usr
        buildable: False
    m4:
        paths:
            m4@1.4.16: /usr
        buildable: False
    automake:
        paths:
            automake@1.13.4: /usr
        buildable: False
    pkg-config:
        paths:
            pkg-config@0.29: /usr
        buildable: False
    libtool:
        paths:
            libtool@2.4.2: /usr
        buildable: False
    perl:
        paths:
            perl@5.18.2: /usr
        buildable: False
    cmake:
        paths:
            cmake@3.5.2: /usr
        buildable: False
    ssg:
        variants: +mpi
    libfabric:
        variants: fabrics=gni
    all:
        providers:
            mpi: [mpich]
```
# compile everything and load module for ssg
spack install ssg
# you may need to re-run setup-env.sh before loading to avoid some problems with finding modules
spack load -r ssg

