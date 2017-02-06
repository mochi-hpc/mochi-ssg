/*
 * Copyright (c) 2016 UChicago Argonne, LLC
 *
 * See COPYRIGHT in top-level directory.
 */

#pragma once

#ifdef __cplusplus
extern "C" {
#endif

#include <mercury.h>
#include <abt.h>
#include <margo.h>
#include <ssg.h>

#if USE_SWIM_FD
#include "swim-fd/swim-fd.h"
#endif

// define an identifier for an unknown group rank value
// TODO: move to SWIM? only used by the swim module so far
#define SSG_MEMBER_RANK_UNKNOWN (-1)

// debug printing macro for SSG
#ifdef DEBUG
#define SSG_DEBUG(__s, __fmt, ...) do { \
    double __now = ABT_get_wtime(); \
    fprintf(__s->dbg_strm, "%.6lf <%d>: " __fmt, \
        __now, __s->view.self_rank, ## __VA_ARGS__); \
    fflush(__s->dbg_strm); \
} while(0)
#else
#define SSG_DEBUG(__s, __fmt, ...) do { \
} while(0)
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
    hg_addr_t addr;
#if USE_SWIM_FD
    int swim_susp_level;
    int swim_inc_nr;
#endif
};

struct ssg
{
    margo_instance_id mid;
    ssg_view_t view; // TODO: we probably need to protect access to this structure at very least
#if USE_SWIM_FD
    swim_context_t *swim_ctx;
#endif
#ifdef DEBUG
    FILE *dbg_strm;
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

#ifdef __cplusplus
}
#endif
