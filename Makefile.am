AUTOMAKE_OPTIONS = foreign
ACLOCAL_AMFLAGS = -I m4

bin_PROGRAMS =
bin_SCRIPTS =
noinst_PROGRAMS =
TESTS =
XFAIL_TESTS = 
check_PROGRAMS =
EXTRA_PROGRAMS =
CLEANFILES = $(bin_SCRIPTS)
MAINTAINERCLEANFILES =
EXTRA_DIST =
BUILT_SOURCES =
include_HEADERS = include/ssg.h
if SSG_HAVE_MPI
include_HEADERS += include/ssg-mpi.h
endif
if SSG_HAVE_PMIX
include_HEADERS += include/ssg-pmix.h
endif
noinst_HEADERS = ssg-config.h
TESTS_ENVIRONMENT =

EXTRA_DIST += prepare.sh

AM_CPPFLAGS = -I$(top_srcdir)/include -I$(top_srcdir)/src
AM_CFLAGS =
AM_LIBS =

lib_LTLIBRARIES = src/libssg.la
src_libssg_la_SOURCES =

LDADD = src/libssg.la

pkgconfigdir = $(libdir)/pkgconfig
pkgconfig_DATA = maint/ssg.pc

include Make.rules

include $(top_srcdir)/src/Makefile.subdir
include $(top_srcdir)/src/swim-fd/Makefile.subdir
include $(top_srcdir)/tests/Makefile.subdir
include $(top_srcdir)/tests/unit-tests/Makefile.subdir
