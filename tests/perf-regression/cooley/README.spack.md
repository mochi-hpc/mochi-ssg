To compile the full stack on cooley as of June 2018 using spack:

# set up .soft.cooley like this:
```
+gcc-7.1.0
+autotools-feb2016
+cmake-3.9.1
+mvapich2
@default
```

# get spack repos:
```
git clone https://github.com/spack/spack.git
. spack/share/spack/setup-env.sh
spack bootstrap
git clone git@xgitlab.cels.anl.gov:sds/sds-repo
spack repo add .
```

# edit .spack/linux/packages.yaml to look like this:
```
packages:
    openssl:
        paths:
            openssl@1.0.2k: /usr
        buildable: False
    cmake:
        paths:
            cmake@3.9.1: /soft/buildtools/cmake/3.9.1
        buildable: False
    autoconf:
        paths:
            autoconf@2.69: /soft/buildtools/autotools/feb2016
        buildable: False
    automake:
        paths:
            automake@1.15: /soft/buildtools/autotools/feb2016
        buildable: False
    ssg:
        variants: +mpi
    libfabric:
        variants: fabrics=verbs,rxm
    all:
        providers:
            # prefer MPICH by default
            mpi: [mpich,openmpi]

```

# compile everything and load module for ssg
# note that the --dirty option is needed because gcc 7.1 on cooley only works if it can inherit LD_LIBRARY_PATH from softenv
spack install --dirty ssg
spack load -r ssg

