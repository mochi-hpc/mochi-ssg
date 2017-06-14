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
#include "uthash.h"

#define SSG_MAGIC_NR 17321588

/* debug printing macro for SSG */
/* TODO: direct debug output to file? */
/* TODO: how do we debug attachers? */
#ifdef DEBUG
#define SSG_DEBUG(__g, __fmt, ...) do { \
    double __now = ABT_get_wtime(); \
    fprintf(stdout, "%.6lf <%s:%"PRIu64">: " __fmt, __now, \
        __g->group_name, __g->self_id, ## __VA_ARGS__); \
    fflush(stdout); \
} while(0)
#else
#define SSG_DEBUG(__g, __fmt, ...) do { \
} while(0)
#endif

/* SSG internal dataypes */

typedef struct ssg_group ssg_group_t;
typedef struct ssg_view ssg_view_t;
typedef struct ssg_member_state ssg_member_state_t;
typedef struct ssg_instance ssg_instance_t;

struct ssg_member_state
{
    hg_addr_t addr;
    int is_member;
};

struct ssg_view
{
    int size;
    ssg_member_state_t *member_states;
};

struct ssg_group
{
    char *group_name;
    ssg_group_id_t group_id;
    ssg_member_id_t self_id;
    ssg_view_t group_view;
    void *fd_ctx; /* failure detector context (currently just SWIM) */
    UT_hash_handle hh;
};

struct ssg_instance
{
    margo_instance_id mid;
    ssg_group_t *group_table;
};

/* SSG internal function prototypes */

#define ssg_hashlittle2 hashlittle2
extern void hashlittle2(const void *key, size_t length, uint32_t *pc, uint32_t *pb);

void ssg_register_rpcs(
    void);
hg_return_t ssg_group_lookup(
    ssg_group_t * g,
    const char * const addr_strs[]);
hg_return_t ssg_group_attach_send(
    const char *member_addr_str);

/* XXX: is this right? can this be a global? */
extern ssg_instance_t *ssg_inst; 

#ifdef __cplusplus
}
#endif
