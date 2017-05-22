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

#include "ssg.h"

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

extern void hashlittle2(const void *key, size_t length, uint32_t *pc, uint32_t *pb);
#define ssg_hashlittle2 hashlittle2

#define SSG_MAGIC_NR 17321588

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
    void *fd_ctx; /* failure detector context (currently just SWIM) */
};

/* XXX: is this right? can this be a global? */
extern margo_instance_id ssg_mid; 

#ifdef __cplusplus
}
#endif
