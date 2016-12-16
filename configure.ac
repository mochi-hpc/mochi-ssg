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

PKG_CHECK_MODULES([MARGO],[margo],[],
   [AC_MSG_ERROR([Could not find working margo installation!])])
LIBS="$MARGO_LIBS $LIBS"
CPPFLAGS="$MARGO_CFLAGS $CPPFLAGS"
CFLAGS="$MARGO_CFLAGS $CFLAGS"

check_swim_fd=auto
AC_ARG_ENABLE([swim-fd],
    [--enable-swim-fd   Enable SWIM failure detection (default: dynamic check)],
    [case "${enableval}" in
     yes) check_swim_fd=yes ;;
     no)  check_swim_fd=no  ;;
     *)   AC_MSG_ERROR([Invalid value ${enableval} for --enable-swim-fd])
     esac])

check_swim_fd_status=fail
if test "x${check_swim_fd}" = xauto -o "x${check_swim_fd}" = xyes ; then
    PKG_CHECK_MODULES([SWIM_FD],[swim-fd],
        [AC_DEFINE([HAVE_SWIM_FD], [1], [Defined to 1 if compiled with SWIM failure detection support])
         LIBS="$SWIM_FD_LIBS $LIBS"
         CPPFLAGS="$SWIM_FD_CFLAGS $CPPFLAGS"
         CFLAGS="$SWIM_FD_CFLAGS $CFLAGS"
         check_swim_fd_status=success], [check_swim_fd_status=fail])
fi

if test "x${check_swim_fd_status}" = xfail -a "x${check_swim_fd}" = xyes; then
    AC_MSG_ERROR([SWIM failure detector requested but unable to be used. See config.log])
fi

AC_CONFIG_FILES([Makefile maint/ssg.pc])
AC_OUTPUT
