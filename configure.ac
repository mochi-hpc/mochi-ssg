#                                               -*- Autoconf -*-
# Process this file with autoconf to produce a configure script.

AC_PREREQ([2.63])
AC_INIT([ssg], [0.1], [],[],[]) 
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

check_mpi=auto
AC_ARG_ENABLE([mpi],
    [--enable-mpi    Enable MPI (default: dynamic check)],
    [ case "${enableval}" in
     yes) check_mpi=yes ;;
     no)  check_mpi=no ;;
     *) AC_MSG_ERROR([bad value ${enableval} for --enable-mpi]) ;;
     esac],
    [])


check_mpi_status=fail
if test "x${check_mpi}" = xauto -o "x${check_mpi}" = xyes ; then
    AC_MSG_CHECKING([If MPI programs can be compiled])
    AC_LINK_IFELSE(
        [AC_LANG_PROGRAM([[#include<mpi.h>]], [[MPI_Init(0,0);]])],
        [AC_DEFINE([HAVE_MPI], [1], [Define to 1 if compiled with MPI support])
         AC_MSG_RESULT([yes])
         check_mpi_status=success],
        [AC_MSG_RESULT([no])])
fi

if test "x${check_mpi_status}" = xfail -a "x${check_mpi}" = xyes; then
    AC_MSG_ERROR([MPI requested but unable to be used. Did you specify an MPI compiler?])
fi

AM_CONDITIONAL([HAVE_MPI], [test "x${check_mpi_status}" = xsuccess])

check_margo=auto
AC_ARG_ENABLE([margo],
    [--enable-margo    Enable Margo (default: dynamic check)],
    [case "${enableval}" in
     yes) check_margo=yes ;;
     no)  check_margo=no  ;;
     *)   AC_MSG_ERROR([Invalid value ${enableval} for --enable-margo])
     esac])

if test "x${check_margo}" = xauto -o "x${check_margo}" = xyes ; then
    PKG_CHECK_MODULES([MARGO],[margo],
        [AC_DEFINE([HAVE_MARGO], [1], [Define to 1 if compiled with Margo support])
         LIBS="$MARGO_LIBS $LIBS"
         CPPFLAGS="$MARGO_CFLAGS $CPPFLAGS"
         CFLAGS="$MARGO_CFLAGS $CFLAGS"
         check_margo_status=success], [check_margo_status=fail])
fi

if test "x${check_margo_status}" = xfail -a "x${check_margo}" = xyes; then
    AC_MSG_ERROR([Margo requested but unable to be used. See config.log])
fi

MARGO_EXTRA_PROGS=
AM_CONDITIONAL([HAVE_MARGO], [test "x${check_margo_status}" = xsuccess])
if test "x${check_margo_status}" = xsuccess ; then
    MARGO_EXTRA_PROGS="examples/ssg-example-margo examples/ssg-example-margo-dblgrp"
fi

AC_SUBST([MARGO_EXTRA_PROGS])

# mercury check goes last - libs should be linked in before margo
PKG_CHECK_MODULES([MERCURY],[mercury],
    [LIBS="$MERCURY_LIBS $LIBS"
     CPPFLAGS="$MERCURY_CFLAGS $CPPFLAGS"
     CFLAGS="$MERCURY_CFLAGS $CFLAGS"])

AC_CONFIG_FILES([Makefile maint/ssg.pc])
AC_OUTPUT
