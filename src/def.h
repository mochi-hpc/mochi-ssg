/*
 * Copyright (c) 2016 UChicago Argonne, LLC
 *
 * See COPYRIGHT in top-level directory.
 */

#pragma once

#include <ssg-config.h>

#include <mercury_types.h>
#include <abt.h>
#include <margo.h>

#ifdef HAVE_SWIM_FD
#include <swim.h>
#endif

struct ssg
{
    hg_class_t *hgcl;
    char **addr_strs;
    hg_addr_t *addrs;
    void *backing_buf;
    int num_addrs;
    int buf_size;
    int rank;
    margo_instance_id mid;
    hg_id_t barrier_rpc_id;
    int barrier_id;
    int barrier_count;
    ABT_mutex barrier_mutex;
    ABT_cond  barrier_cond;
    ABT_eventual barrier_eventual;
#if HAVE_SWIM_FD
    swim_context_t *swim_ctx;
#endif
};
