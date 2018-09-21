SUBDIRS = swim

AUTOMAKE_OPTIONS = foreign
ACLOCAL_AMFLAGS = -I m4

bin_PROGRAMS =
bin_SCRIPTS =
noinst_LTLIBRARIES =
noinst_PROGRAMS =
TESTS =
XFAIL_TESTS = 
check_PROGRAMS =
EXTRA_PROGRAMS =
CLEANFILES = $(bin_SCRIPTS)
MAINTAINERCLEANFILES =
EXTRA_DIST =
BUILT_SOURCES =
src_libssg_la_SOURCES =
include_HEADERS = include/ssg.h
if SSG_HAVE_MPI
include_HEADERS += include/ssg-mpi.h
endif 
noinst_HEADERS = ssg-config.h
lib_LTLIBRARIES = src/libssg.la

EXTRA_DIST += prepare.sh

AM_CPPFLAGS = -I$(top_srcdir)/include -I$(top_srcdir)/src -I$(top_srcdir)/swim/include
AM_CFLAGS =
AM_LIBS =

src_libssg_la_LIBADD = swim/src/libswim.la

pkgconfigdir = $(libdir)/pkgconfig
pkgconfig_DATA = maint/ssg.pc

include Make.rules
include $(top_srcdir)/src/Makefile.subdir
include $(top_srcdir)/tests/Makefile.subdir
