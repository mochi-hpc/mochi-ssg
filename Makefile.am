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
src_libssg_la_SOURCES =
include_HEADERS = include/ssg.h ssg-config.h
lib_LTLIBRARIES += src/libssg.la

EXTRA_DIST += prepare.sh

AM_CPPFLAGS = -I$(top_srcdir)/include -I$(top_srcdir)/src

AM_CFLAGS =

AM_LIBS =

pkgconfigdir = $(libdir)/pkgconfig
pkgconfig_DATA = maint/ssg.pc

include Make.rules

include $(top_srcdir)/src/Makefile.subdir
if USE_SWIM_FD
include $(top_srcdir)/src/swim-fd/Makefile.subdir
endif

include $(top_srcdir)/tests/Makefile.subdir
