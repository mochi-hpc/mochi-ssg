/*
 * Copyright (c) 2016 UChicago Argonne, LLC
 *
 * See COPYRIGHT in top-level directory.
 */

#pragma once

#include <stdint.h>
#include <inttypes.h>

#include <mercury.h>
#include <mercury_proc_string.h>
#include <abt.h>
#include <margo.h>

#include "ssg.h"
#include "swim-fd/swim-fd.h"
#include "uthash.h"
#include "utlist.h"

#ifdef __cplusplus
extern "C" {
#endif

#define SSG_MAGIC_NR 17321588

/* debug printing macro for SSG */
/* TODO: direct debug output to file? */
/* TODO: how do we debug attachers? */
#ifdef DEBUG
#define SSG_DEBUG(__g, __fmt, ...) do { \
    double __now = ABT_get_wtime(); \
    fprintf(stdout, "[%.6lf] %20"PRIu64" (%s): SSG " __fmt, __now, \
        __g->self_id, __g->name, ## __VA_ARGS__); \
    fflush(stdout); \
} while(0)
#else
#define SSG_DEBUG(__g, __fmt, ...) do { \
} while(0)
#endif

/* SSG internal dataypes */

typedef struct ssg_member_state
{
    ssg_member_id_t id;
    char *addr_str;
    hg_addr_t addr;
    swim_member_state_t swim_state;
    struct ssg_member_state *next;
    UT_hash_handle hh;
} ssg_member_state_t;

/* TODO: associate a version number with a descriptor */
typedef struct ssg_group_descriptor
{
    uint64_t magic_nr;
    uint64_t name_hash;
    char *addr_str;
    int owner_status;
    int ref_count;
} ssg_group_descriptor_t;

typedef struct ssg_group_view
{
    unsigned int size;
    ssg_member_state_t *member_map;
} ssg_group_view_t;

typedef struct ssg_group
{
    char *name;
    ssg_member_id_t self_id;
    ssg_group_view_t view;
    ssg_group_descriptor_t *descriptor;
    swim_context_t *swim_ctx;
    ssg_member_state_t *member_list;
    ssg_membership_update_cb update_cb;
    void *update_cb_dat;
    UT_hash_handle hh;
} ssg_group_t;

typedef struct ssg_attached_group
{
    char *name;
    ssg_group_descriptor_t *descriptor;
    ssg_group_view_t view;
    UT_hash_handle hh;
} ssg_attached_group_t;

typedef struct ssg_instance
{
    margo_instance_id mid;
    ssg_group_t *group_table;
    ssg_attached_group_t *attached_group_table;
} ssg_instance_t;

enum ssg_group_descriptor_owner_status
{
    SSG_OWNER_IS_UNASSOCIATED = 0,
    SSG_OWNER_IS_MEMBER,
    SSG_OWNER_IS_ATTACHER
};

/* SSG internal function prototypes */

#define ssg_hashlittle2 hashlittle2
extern void hashlittle2(const void *key, size_t length, uint32_t *pc, uint32_t *pb);
static inline uint64_t ssg_hash64_str(const char * str)
{
    uint32_t lower = 0, upper = 0;
    uint64_t hash;
    ssg_hashlittle2(str, strlen(str), &lower, &upper);
    hash = lower + (((uint64_t)upper)<<32);
    return hash;
}

void ssg_register_rpcs(
    void);
int ssg_group_attach_send(
    ssg_group_descriptor_t * group_descriptor,
    char ** group_name,
    int * group_size, 
    void ** view_buf);

/* XXX: is this right? can this be a global? */
extern ssg_instance_t *ssg_inst; 

#ifdef __cplusplus
}
#endif
