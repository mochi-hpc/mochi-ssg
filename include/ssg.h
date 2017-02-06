/*
 * Copyright (c) 2016 UChicago Argonne, LLC
 *
 * See COPYRIGHT in top-level directory.
 */

#pragma once

// "simple static group" interface
//
// Contains trivial wireup and connection management functionality, with a
// model of a static (at startup) member list.

#ifdef __cplusplus
extern "C" {
#endif

#include <mercury.h>
#include <margo.h>

#ifdef HAVE_MPI
#include <mpi.h>
#endif

// using pointer so that we can use proc (proc has to allocate in this case)
typedef struct ssg *ssg_t;

// some defines
// null pointer shim
#define SSG_NULL ((ssg_t)NULL)

/// group member initialization

// config file based - load up the given config file containing a set of hostnames
ssg_t ssg_init_config(margo_instance_id mid, const char * fname);

#ifdef HAVE_MPI
// mpi based (no config file) - all participants (defined by the input
// communicator) do a global address exchange
// in this case, the caller has already initialized HG with its address
ssg_t ssg_init_mpi(margo_instance_id mid, MPI_Comm comm);
#endif

/// finalization

// teardown all state associated with the given ssg group
void ssg_finalize(ssg_t s);

/// accessors

// get my rank in the group
int ssg_get_group_rank(const ssg_t s);

// get the size of the group
int ssg_get_group_size(const ssg_t s);

// get the HG address for the group member at the given rank
hg_addr_t ssg_get_addr(const ssg_t s, int rank);

/// mercury support

#if 0
// group serialization mechanism
hg_return_t hg_proc_ssg_t(hg_proc_t proc, ssg_t *s);

/// utility functions

// dump address list to the given file
// returns -1 on error, corresponding to the return code of open/write/close,
// and sets errno
int ssg_dump(const ssg_t s, const char *fname);

// set up barrier data structures. Separate call to resolve the margo -> barrier
// race condition - call this before kicking off the progress loop with margo
void ssg_register_barrier(ssg_t s, hg_class_t *hgcl);

// perform a naive N-1 barrier using margo.
// requires ssg_set_margo_id to have been called.
// NOTE: rank must be set on all ranks prior to calling this. I.e. should init
// the rank prior to starting up margo
hg_return_t ssg_barrier_margo(ssg_t s);
#endif

#ifdef __cplusplus
}
#endif
