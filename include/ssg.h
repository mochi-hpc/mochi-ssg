/*
 * Copyright (c) 2016 UChicago Argonne, LLC
 *
 * See COPYRIGHT in top-level directory.
 */

#pragma once

// "simple stupid group" interface
//
// Contains trivial wireup and connection management functionality, with a
// model of a static (at startup) member list.

#ifdef __cplusplus
extern "C" {
#endif

#include <mercury.h>
#include <na.h>

// using pointer so that we can use proc
typedef struct ssg *ssg_t;

// some defines
// null pointer shim
#define SSG_NULL ((ssg_t)NULL)
// after init, rank is possibly unknown
#define SSG_RANK_UNKNOWN (-1)
// if ssg_t is gotten from another process (RPC output), then, by definition,
// the receiving entity is not part of the group
#define SSG_EXTERNAL_RANK (-2)

/// participant initialization

// config file based - all participants load up the given config file
// containing a set of hostnames
ssg_t ssg_init_config(const char * fname);

// once the ssg has been initialized, wireup (a collection of NA_Addr_lookups)
// note that this is a simple blocking implementation - no margo/etc here
na_return_t ssg_lookup(
        na_class_t *nacl,
        na_context_t *nactx,
        ssg_t s);

/// finalization

// teardown all connections associated with the given ssg
void ssg_finalize(ssg_t s);

/// accessors

// get my rank
int ssg_get_rank(const ssg_t s);

// get the number of participants
int ssg_get_count(const ssg_t s);

// get the address for the group member at the given rank
na_addr_t ssg_get_addr(const ssg_t s, int rank);

// get the string hostname for the group member at the given rank
const char * ssg_get_addr_str(const ssg_t s, int rank);

/// mercury support

// group serialization mechanism
hg_return_t hg_proc_ssg_t(hg_proc_t proc, ssg_t *s);

#ifdef __cplusplus
}
#endif

/**
 * vim: ft=c sw=4 ts=4 sts=4 tw=80 expandtab
 */
