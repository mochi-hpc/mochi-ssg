AUTOMAKE_OPTIONS = foreign
ACLOCAL_AMFLAGS = -I m4

bin_PROGRAMS =
bin_SCRIPTS =
noinst_LIBRARIES =
noinst_PROGRAMS =
lib_LIBRARIES =
noinst_HEADERS =
TESTS =
XFAIL_TESTS = 
check_PROGRAMS =
EXTRA_PROGRAMS =
CLEANFILES = $(bin_SCRIPTS)
MAINTAINERCLEANFILES =
EXTRA_DIST =
BUILT_SOURCES =
include_HEADERS = include/ssg.h
EXTRA_HEADERS = include/ssg-mpi.h

EXTRA_DIST += prepare.sh

AM_CPPFLAGS = -I$(top_srcdir)/include

AM_CFLAGS =

AM_LIBS =

if HAVE_MPI
include_HEADERS += include/ssg-mpi.h
endif

if HAVE_MARGO
include_HEADERS += include/ssg-margo.h
endif

noinst_HEADERS += \
  ssg-config.h \
  examples/rpc.h

lib_LIBRARIES += src/libssg.a
src_libssg_a_SOURCES = src/ssg.c

noinst_PROGRAMS += examples/ssg-example
examples_ssg_example_SOURCES = examples/ssg-example.c examples/rpc.c
examples_ssg_example_LDADD = src/libssg.a

pkgconfigdir = $(libdir)/pkgconfig
pkgconfig_DATA = maint/ssg.pc

include Make.rules
