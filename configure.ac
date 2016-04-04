#                                               -*- Autoconf -*-
# Process this file with autoconf to produce a configure script.

AC_PREREQ([2.67])
AC_INIT([ssg], [0.1], [],[],[]) 

AC_CANONICAL_TARGET
AC_CANONICAL_SYSTEM
AC_CANONICAL_HOST

AM_INIT_AUTOMAKE([foreign subdir-objects -Wall])

# we should remove this soon, only needed for automake 1.10 and older
m4_ifdef([AM_SILENT_RULES], [AM_SILENT_RULES([yes])])

AC_CONFIG_SRCDIR([README.md])
AC_CONFIG_HEADERS([ssg-config.h])
AC_CONFIG_MACRO_DIR([m4])

AC_LANG([C])

# Checks for programs.
AC_PROG_CC
AC_PROG_CC_C99
AC_PROG_RANLIB

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

PKG_CHECK_MODULES_STATIC([MERCURY],[mercury],[],
   [AC_MSG_ERROR([Could not find working mercury installation!])])
LIBS="$MERCURY_LIBS $LIBS"
CPPFLAGS="$MERCURY_CFLAGS $CPPFLAGS"
CFLAGS="$MERCURY_CFLAGS $CFLAGS"

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

AC_CONFIG_FILES([Makefile maint/ssg.pc])
AC_OUTPUT
