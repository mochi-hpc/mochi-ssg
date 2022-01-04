#                                               -*- Autoconf -*-
# Process this file with autoconf to produce a configure script.

AC_PREREQ([2.63])
AC_INIT([ssg], [0.5.1], [],[],[])
AC_CONFIG_MACRO_DIR([m4])
LT_INIT

AC_CANONICAL_TARGET
AC_CANONICAL_SYSTEM
AC_CANONICAL_HOST

AM_INIT_AUTOMAKE([foreign subdir-objects -Wall])

# we should remove this soon, only needed for automake 1.10 and older
m4_ifdef([AM_SILENT_RULES], [AM_SILENT_RULES([yes])])

AC_CONFIG_SRCDIR([README.md])
AC_CONFIG_HEADERS([ssg-config.h])

# Checks for programs.
AC_PROG_CC
AC_PROG_CC_C99

dnl
dnl Add warning flags by default
dnl
CFLAGS="-Wall -Wextra $CFLAGS"
CXXFLAGS="-Wall -Wextra $CXXFLAGS"

dnl
dnl Verify pkg-config
dnl
PKG_PROG_PKG_CONFIG
PKG_CONFIG="pkg-config --static"

# coreutils checks for OSX
AC_ARG_VAR([TIMEOUT], timeout program)
AC_ARG_VAR([MKTEMP], mktemp program)

if test -z "$TIMEOUT" ; then
    AC_CHECK_PROGS(TIMEOUT, [timeout gtimeout])
    if test -z "$TIMEOUT" ; then
        AC_MSG_ERROR([Could not find timeout command (can optionally provide via the TIMEOUT variable)])
    fi
else
    AC_SUBST([TIMEOUT], ["$TIMEOUT"])
fi

if test -z "$MKTEMP" ; then
    AC_CHECK_PROGS(MKTEMP, [mktemp gmktemp])
    if test -z "$MKTEMP" ; then
        AC_MSG_ERROR([Could not find mktemp command (can optionally provide via the MKTEMP variable)])
    fi
else
    AC_SUBST([MKTEMP], ["$MKTEMP"])
fi

check_mpi=auto
AC_ARG_ENABLE([mpi],
    [  --enable-mpi            enable MPI (default: dynamic check)],
    [ case "${enableval}" in
     yes) check_mpi=yes ;;
     no)  check_mpi=no ;;
     *) AC_MSG_ERROR([bad value ${enableval} for --enable-mpi]) ;;
     esac],
    [])

check_pmix=auto
AC_ARG_ENABLE([pmix],
    [  --enable-pmix           enable PMIx (default: dynamic check)],
    [ case "${enableval}" in
     yes) check_pmix=yes ;;
     no)  check_pmix=no ;;
     *) AC_MSG_ERROR([bad value ${enableval} for --enable-pmix]) ;;
     esac],
    [])

check_drc=no
AC_ARG_ENABLE([drc],
    [  --enable-drc           enable DRC  (default: disabled)],
    [ case "${enableval}" in
     yes) check_drc=yes ;;
     no)  check_drc=no ;;
     *) AC_MSG_ERROR([bad value ${enableval} for --enable-drc]) ;;
     esac],
    [])

check_valgrind=no
AC_ARG_ENABLE([valgrind],
    [  --enable-valgrind           enable Valgrind  (default: disabled)],
    [ case "${enableval}" in
     yes) check_valgrind=yes ;;
     no)  check_valgrind=no ;;
     *) AC_MSG_ERROR([bad value ${enableval} for --enable-valgrind]) ;;
     esac],
    [])


check_mpi_status=fail
if test "x${check_mpi}" = xauto -o "x${check_mpi}" = xyes ; then
    AC_MSG_CHECKING([If MPI programs can be compiled])
    AC_LINK_IFELSE(
        [AC_LANG_PROGRAM([[#include<mpi.h>]], [[MPI_Init(0,0);]])],
        [AC_DEFINE([SSG_HAVE_MPI], [1], [Define to 1 if compiled with MPI support])
         AC_MSG_RESULT([yes])
         check_mpi_status=success],
        [AC_MSG_RESULT([no])])
fi

check_pmix_status=fail
AC_SUBST([PMIX_PKGCONFIG], [])
AC_CHECK_LIB([pmix], [PMIx_Init])
if test "x${check_pmix}" = xauto -o "x${check_pmix}" = xyes ; then
    AC_MSG_CHECKING([If PMIx programs can be compiled])
    AC_LINK_IFELSE(
        [AC_LANG_PROGRAM([[#include<pmix.h>]], [[PMIx_Init(NULL, NULL,0);]])],
        [AC_DEFINE([SSG_HAVE_PMIX], [1], [Define to 1 if compiled with PMIx support])
         PKG_CHECK_MODULES([PMIX],[pmix],[AC_SUBST([PMIX_PKGCONFIG],[pmix])],[do_nothing=1])
         AC_MSG_RESULT([yes])
         check_pmix_status=success],
        [AC_MSG_RESULT([no])])
fi

check_drc_status=fail
if test "x${check_drc}" = xauto -o "x${check_drc}" = xyes ; then
    PKG_CHECK_MODULES([DRC],[cray-drc],
            [AC_DEFINE([SSG_HAVE_DRC], 1, [Use Cray dynamic rdma credentials]) ]
            check_drc_status=success,
            [])
    LIBS="$DRC_LIBS $LIBS"
    CPPFLAGS="$DRC_CFLAGS $CPPFLAGS"
    CFLAGS="$DRC_CFLAGS $CFLAGS"
fi

if test "x${check_valgrind}" = xauto -o "x${check_valgrind}" = xyes ; then
    CFLAGS="$CFLAGS -DVALGRIND"
    CPPLAGS="$CPPFLAGS -DVALGRIND"
fi

if test "x${check_mpi_status}" = xfail -a "x${check_mpi}" = xyes; then
    AC_MSG_ERROR([MPI requested but unable to be used. Did you specify an MPI compiler?])
fi
if test "x${check_pmix_status}" = xfail -a "x${check_pmix}" = xyes; then
    AC_MSG_ERROR([PMIx requested but unable to be used.])
fi
if test "x${check_drc_status}" = xfail -a "x${check_drc}" = xyes; then
    AC_MSG_ERROR([DRC requested but unable to be used.])
fi

AM_CONDITIONAL([SSG_HAVE_MPI], [test "x${check_mpi_status}" = xsuccess])
AM_CONDITIONAL([SSG_HAVE_PMIX], [test "x${check_pmix_status}" = xsuccess])
AM_CONDITIONAL([SSG_HAVE_DRC], [test "x${check_drc_status}" = xsuccess])

PKG_CHECK_MODULES([MARGO],[margo],[],
   [AC_MSG_ERROR([Could not find working margo installation!])])
LIBS="$MARGO_LIBS $LIBS"
CPPFLAGS="$MARGO_CFLAGS $CPPFLAGS"
CFLAGS="$MARGO_CFLAGS $CFLAGS"


# margo_set_environment: function in margo that can be used to set argobots
# environment with settings desired by margo if ssg initializes argobots
AC_CHECK_FUNCS(margo_set_environment)

AC_CONFIG_FILES([Makefile maint/ssg.pc])
AC_OUTPUT
