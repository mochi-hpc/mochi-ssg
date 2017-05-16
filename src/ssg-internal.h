/*
 * Copyright (c) 2016 UChicago Argonne, LLC
 *
 * See COPYRIGHT in top-level directory.
 */

#pragma once

#ifdef __cplusplus
extern "C" {
#endif

#include <stdint.h>
#include <inttypes.h>

#include <mercury.h>
#include <abt.h>
#include <margo.h>

#include <ssg.h>
#if USE_SWIM_FD
#include "swim-fd/swim-fd.h"
#endif

/* debug printing macro for SSG */
/* TODO: direct debug output to file? */
#ifdef DEBUG
#define SSG_DEBUG(__g, __fmt, ...) do { \
    double __now = ABT_get_wtime(); \
    fprintf(stdout, "%.6lf <%s:%d>: " __fmt, __now, \
        __g->name, __g->self_rank, ## __VA_ARGS__); \
    fflush(stdout); \
} while(0)
#else
#define SSG_DEBUG(__g, __fmt, ...) do { \
} while(0)
#endif

typedef struct ssg_group ssg_group_t;
typedef struct ssg_group_view ssg_group_view_t;
typedef struct ssg_member_state ssg_member_state_t;

struct ssg_member_state
{
    hg_addr_t addr;
    int is_member;
};

struct ssg_group_view
{
    int group_size;
    ssg_member_state_t *member_states;
};

struct ssg_group
{
    char *name;
    ssg_group_id_t id;
    int self_rank;
    ssg_group_view_t view;
#if USE_SWIM_FD
    swim_context_t *swim_ctx;
#endif
};

#ifdef __cplusplus
}
#endif
