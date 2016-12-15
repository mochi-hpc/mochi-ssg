AUTOMAKE_OPTIONS = foreign
ACLOCAL_AMFLAGS = -I m4

bin_PROGRAMS =
bin_SCRIPTS =
noinst_LTLIBRARIES =
noinst_PROGRAMS =
lib_LTLIBRARIES =
noinst_HEADERS =
TESTS =
XFAIL_TESTS = 
check_PROGRAMS =
EXTRA_PROGRAMS =
CLEANFILES = $(bin_SCRIPTS)
MAINTAINERCLEANFILES =
EXTRA_DIST =
BUILT_SOURCES =
include_HEADERS = include/ssg.h include/ssg-margo.h
EXTRA_HEADERS = include/ssg-mpi.h

EXTRA_DIST += prepare.sh

AM_CPPFLAGS = -I$(top_srcdir)/include

AM_CFLAGS =

AM_LIBS =

if HAVE_MPI
include_HEADERS += include/ssg-mpi.h
endif

noinst_HEADERS += ssg-config.h

lib_LTLIBRARIES += src/libssg.la
src_libssg_la_SOURCES = src/ssg.c

pkgconfigdir = $(libdir)/pkgconfig
pkgconfig_DATA = maint/ssg.pc

include Make.rules

include $(top_srcdir)/tests/Makefile.subdir
