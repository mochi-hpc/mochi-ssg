/*
 * Copyright (c) 2016 UChicago Argonne, LLC
 *
 * See COPYRIGHT in top-level directory.
 */
#pragma once
#include <margo.h>
#include <ssg.h>

#if USE_SWIM_FD
#include "swim-fd/swim-fd.h"
#endif

typedef struct ssg_view ssg_view_t;
typedef struct ssg_member_state ssg_member_state_t;

struct ssg_view
{
    int self_rank;
    int group_size;
    ssg_member_state_t *member_states;
};

struct ssg_member_state
{
    ssg_member_status_t status;
    hg_addr_t addr;
    char *addr_str;
};

struct ssg
{
    margo_instance_id mid;
    ssg_view_t view;
    void *addr_str_buf;
    int addr_str_buf_size;
#if USE_SWIM_FD
    swim_context_t *swim_ctx;
#endif
#if 0
    hg_id_t barrier_rpc_id;
    int barrier_id;
    int barrier_count;
    ABT_mutex barrier_mutex;
    ABT_cond  barrier_cond;
    ABT_eventual barrier_eventual;
#endif
};
