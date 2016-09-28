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

lib_LTLIBRARIES += src/libssg.la
src_libssg_la_SOURCES = src/ssg.c

EXTRA_PROGRAMS += examples/ssg-example-margo examples/ssg-example-margo-dblgrp
examples_ssg_example_margo_SOURCES = examples/ssg-example-margo.c examples/rpc.c
examples_ssg_example_margo_LDADD = src/libssg.la
examples_ssg_example_margo_dblgrp_SOURCES = examples/ssg-example-margo-dblgrp.c examples/rpc.c
examples_ssg_example_margo_dblgrp_LDADD = src/libssg.la

noinst_PROGRAMS += examples/ssg-example $(MARGO_EXTRA_PROGS)
examples_ssg_example_SOURCES = examples/ssg-example.c examples/rpc.c
examples_ssg_example_LDADD = src/libssg.la

pkgconfigdir = $(libdir)/pkgconfig
pkgconfig_DATA = maint/ssg.pc

include Make.rules
