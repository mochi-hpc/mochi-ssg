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

#define SSG_GET_SELF_ADDR_STR(__mid, __addr_str) do { \
    hg_addr_t __self_addr; \
    hg_size_t __size; \
    __addr_str = NULL; \
    if (margo_addr_self(__mid, &__self_addr) != HG_SUCCESS) break; \
    if (margo_addr_to_string(__mid, NULL, &__size, __self_addr) != HG_SUCCESS) { \
        margo_addr_free(__mid, __self_addr); \
        break; \
    } \
    if ((__addr_str = malloc(__size)) == NULL) { \
        margo_addr_free(__mid, __self_addr); \
        break; \
    } \
    if (margo_addr_to_string(__mid, __addr_str, &__size, __self_addr) != HG_SUCCESS) { \
        free(__addr_str); \
        __addr_str = NULL; \
        margo_addr_free(__mid, __self_addr); \
        break; \
    } \
    margo_addr_free(__mid, __self_addr); \
} while(0)

/* debug printing macro for SSG */
/* TODO: how do we debug attachers? */
#ifdef DEBUG
#define SSG_DEBUG(__g, __fmt, ...) do { \
    double __now = ABT_get_wtime(); \
    fprintf(g->dbg_log, "[%.6lf] %20"PRIu64" (%s): SSG " __fmt, __now, \
        __g->self_id, __g->name, ## __VA_ARGS__); \
    fflush(g->dbg_log); \
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

typedef struct ssg_group_target_list
{
    ssg_member_state_t **targets;
    unsigned int nslots;
    unsigned int len;
    unsigned int dping_ndx;
} ssg_group_target_list_t;

typedef struct ssg_group
{
    char *name;
    ssg_member_id_t self_id;
    ssg_group_view_t view;
    ssg_group_target_list_t target_list;
    ssg_member_state_t *dead_members;
    ssg_group_descriptor_t *descriptor;
    swim_context_t *swim_ctx;
    ABT_rwlock lock;
    ssg_membership_update_cb update_cb;
    void *update_cb_dat;
#ifdef DEBUG
    FILE *dbg_log;
#endif
    UT_hash_handle hh;
} ssg_group_t;

typedef struct ssg_attached_group
{
    char *name;
    ssg_group_view_t view;
    ssg_group_descriptor_t *descriptor;
    ABT_rwlock lock;
    UT_hash_handle hh;
} ssg_attached_group_t;

typedef struct ssg_instance
{
    margo_instance_id mid;
    ssg_group_t *group_table;
    ssg_attached_group_t *attached_group_table;
    ABT_rwlock lock;
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
int ssg_group_join_send(
    ssg_group_descriptor_t * group_descriptor,
    hg_addr_t group_target_addr,
    char ** group_name,
    int * group_size, 
    void ** view_buf);
int ssg_group_leave_send(
    ssg_group_descriptor_t * group_descriptor,
    ssg_member_id_t self_id,
    hg_addr_t group_target_addr);
int ssg_group_attach_send(
    ssg_group_descriptor_t * group_descriptor,
    char ** group_name,
    int * group_size, 
    void ** view_buf);
void ssg_apply_swim_user_updates(
    void *group_data,
    swim_user_update_t *updates,
    hg_size_t update_count);

extern ssg_instance_t *ssg_inst; 

#ifdef __cplusplus
}
#endif
