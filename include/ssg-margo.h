/*
 * Copyright (c) 2016 UChicago Argonne, LLC
 *
 * See COPYRIGHT in top-level directory.
 */

#pragma once

// lookup integrated into margo

#include <mpi.h>
#include <margo.h>

#include "ssg.h"

#ifdef __cplusplus
extern "C" {
#endif

// a "margo-aware" version of hg_lookup - still looks up everyone in one go
hg_return_t ssg_lookup_margo(ssg_t s, margo_instance_id mid);

#ifdef __cplusplus
}
#endif

/**
 * vim: ft=c sw=4 ts=4 sts=4 tw=80 expandtab
 */
