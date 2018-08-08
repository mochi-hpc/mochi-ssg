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
# as of 2018-06-12, need to pull in PR changes to allow SPACK_SHELL override
cd spack
git remote add jrood-nrel https://github.com/jrood-nrel/spack.git
git fetch --all
git merge jrood-nrel/fix_spack_shell_bootstrap
. share/spack/setup-env.sh
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
    bzip2:
        paths:
            bzip2@1.0.6: /
        buildable: False
    bison:
        paths:
            bison@3.0.4: /
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
    ncurses:
        paths:
            ncurses@5.9: /usr
        buildable: False
    tcl:
        paths:
            tcl@8.5.13: /usr
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

