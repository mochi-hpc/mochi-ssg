/*
 * Copyright (c) 2016 UChicago Argonne, LLC
 *
 * See COPYRIGHT in top-level directory.
 */

#pragma once

// lookup integrated into margo

#include <margo.h>

#include "ssg.h"

#ifdef __cplusplus
extern "C" {
#endif

// set up barrier data structures. Separate call to resolve the margo -> barrier
// race condition - call this before kicking off the progress loop with margo
void ssg_register_barrier(ssg_t s, hg_class_t *hgcl);

// set/get the margo instance id used in margo communication calls
void ssg_set_margo_id(ssg_t s, margo_instance_id mid);
margo_instance_id ssg_get_margo_id(ssg_t s);

// a "margo-aware" version of hg_lookup - still looks up everyone in one go.
// requires ssg_set_margo_id to have been called.
hg_return_t ssg_lookup_margo(ssg_t s);

// perform a naive N-1 barrier using margo.
// requires ssg_set_margo_id to have been called.
// NOTE: rank must be set on all ranks prior to calling this. I.e. should init
// the rank prior to starting up margo
hg_return_t ssg_barrier_margo(ssg_t s);

#ifdef __cplusplus
}
#endif

/**
 * vim: ft=c sw=4 ts=4 sts=4 tw=80 expandtab
 */
