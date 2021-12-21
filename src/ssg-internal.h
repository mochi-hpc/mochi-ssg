/*
 * Copyright (c) 2016 UChicago Argonne, LLC
 *
 * See COPYRIGHT in top-level directory.
 */

#pragma once

#include "ssg-config.h"

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
#include "utarray.h"

#ifdef __cplusplus
extern "C" {
#endif

#define SSG_MAGIC_NR 17321588

/* debug printing macro for SSG */
#ifdef DEBUG
#define SSG_DEBUG(__mid_state,  __fmt, ...) do { \
    double __now = ABT_get_wtime(); \
    fprintf(__mid_state->dbg_log, "[%.6lf] %20lu: " __fmt, __now, \
        __mid_state->self_id, ## __VA_ARGS__); \
    fflush(__mid_state->dbg_log); \
} while(0)
#else
#define SSG_DEBUG(__mid_state, __fmt, ...) do { \
} while(0)
#endif

#define SSG_GROUP_READ(__gid, __gd) do { \
    ABT_rwlock_rdlock(ssg_rt->lock); \
    HASH_FIND(hh, ssg_rt->gd_table, &__gid, sizeof(ssg_group_id_t), __gd); \
    if (__gd) ABT_rwlock_rdlock(__gd->lock); \
    ABT_rwlock_unlock(ssg_rt->lock); \
} while(0)

#define SSG_GROUP_WRITE(__gid, __gd) do { \
    ABT_rwlock_rdlock(ssg_rt->lock); \
    HASH_FIND(hh, ssg_rt->gd_table, &__gid, sizeof(ssg_group_id_t), __gd); \
    if (__gd) ABT_rwlock_wrlock(__gd->lock); \
    ABT_rwlock_unlock(ssg_rt->lock); \
} while(0)

#define SSG_GROUP_RELEASE(__gd) do { \
    ABT_rwlock_unlock(__gd->lock); \
} while(0)

#define SSG_GROUP_REF_INCR(__gd) do { \
    ABT_mutex_lock(__gd->ref_mutex); \
    __gd->ref_count++; \
    ABT_mutex_unlock(__gd->ref_mutex); \
} while(0)

#define SSG_GROUP_REF_DECR(__gd) do { \
    ABT_mutex_lock(__gd->ref_mutex); \
    __gd->ref_count--; \
    ABT_cond_signal(__gd->ref_cond); \
    ABT_mutex_unlock(__gd->ref_mutex); \
} while(0)

#define SSG_GROUP_REFS_WAIT(__gd) do { \
    ABT_mutex_lock(__gd->ref_mutex); \
    while(gd->ref_count) ABT_cond_wait(__gd->ref_cond, __gd->ref_mutex); \
    ABT_mutex_unlock(__gd->ref_mutex); \
} while(0)

/* SSG internal dataypes */

typedef struct ssg_runtime_state
{
    struct ssg_group_descriptor *gd_table;
    struct ssg_mid_state *mid_list;
#ifdef SSG_HAVE_PMIX
    size_t pmix_failure_evhdlr_ref;
#endif
    int abt_init_flag;
    ABT_rwlock lock;
} ssg_runtime_state_t;

typedef struct ssg_mid_state
{
    margo_instance_id mid;
    hg_addr_t self_addr;
    char *self_addr_str;
    ssg_member_id_t self_id;
    ABT_pool pool;
    hg_id_t join_rpc_id;
    hg_id_t leave_rpc_id;
    hg_id_t refresh_rpc_id;
    hg_id_t swim_dping_req_rpc_id;
    hg_id_t swim_dping_ack_rpc_id;
    hg_id_t swim_iping_req_rpc_id;
    hg_id_t swim_iping_ack_rpc_id;
    int ref_count;
    struct ssg_mid_state *next;
#ifdef DEBUG
    FILE *dbg_log;
#endif
} ssg_mid_state_t;

typedef struct ssg_member_state
{
    ssg_member_id_t id;
    char *addr_str;
    hg_addr_t addr;
    swim_member_state_t swim_state;
    UT_hash_handle hh;
} ssg_member_state_t;

typedef struct ssg_group_view
{
    unsigned int size;
    ssg_member_state_t *member_map;
    UT_array *rank_array;
} ssg_group_view_t;

typedef struct ssg_update_cb {
    ssg_membership_update_cb  update_cb;
    void*                     update_cb_dat;
    struct ssg_update_cb*     next;
} ssg_update_cb;

typedef struct ssg_group_state
{
    ssg_group_config_t config;
    ssg_group_view_t view;
    ssg_member_state_t *dead_members;
    swim_context_t *swim_ctx;
    ssg_update_cb* update_cb_list;
} ssg_group_state_t;

typedef struct ssg_group_descriptor
{
    ssg_group_id_t g_id;
    char *name;
    int is_member;
    ssg_group_view_t *view;
    ssg_group_state_t *group;
    ssg_mid_state_t *mid_state;
    int64_t cred;
    ABT_rwlock lock;
    ABT_mutex ref_mutex;
    ABT_cond ref_cond;
    int ref_count;
    UT_hash_handle hh;
} ssg_group_descriptor_t;

inline static int add_membership_update_cb(
        ssg_group_state_t* group,
        ssg_membership_update_cb cb,
        void* uargs)
{
    ssg_update_cb* tmp = (ssg_update_cb*)calloc(1, sizeof(*tmp));
    tmp->update_cb     = cb;
    tmp->update_cb_dat = uargs;
    tmp->next          = NULL;
    if (group->update_cb_list) {
        ssg_update_cb* prev = NULL;
        ssg_update_cb* cur = group->update_cb_list;
        while(cur) {
            if (cur->update_cb == cb
            &&  cur->update_cb_dat == uargs) {
                free(tmp);
                return SSG_ERR_INVALID_ARG;
            }
            prev = cur;
            cur = cur->next;
        }
        prev->next = tmp;
    } else {
        group->update_cb_list = tmp;
    }
    return SSG_SUCCESS;
}

inline static int remove_membership_update_cb(
        ssg_group_state_t* group,
        ssg_membership_update_cb cb,
        void* uargs)
{
    ssg_update_cb* previous = NULL;
    ssg_update_cb* current = group->update_cb_list;
    while (current) {
        if (current->update_cb == cb
        &&  current->update_cb_dat == uargs) {
            if (previous) {
                previous->next = current->next;
            } else { // deleting the first callback
                group->update_cb_list = current->next;
            }
            free(current);
            return SSG_SUCCESS;
        }
        previous = current;
        current = current->next;
    }
    return SSG_ERR_INVALID_ARG;
}

inline static void free_all_membership_update_cb(
        ssg_group_state_t* group)
{
    ssg_update_cb* tmp = group->update_cb_list;
    ssg_update_cb* next = NULL;
    while(tmp) {
        next = tmp->next;
        free(tmp);
        tmp = next;
    }
}

inline static void execute_all_membership_update_cb(
        ssg_group_state_t* group,
        ssg_member_id_t member_id,
        ssg_member_update_type_t update_type,
        ABT_rwlock *lock)
{
    ssg_update_cb* list;

    ABT_rwlock_rdlock(*lock);
    list = group->update_cb_list;
    while(list) {
       if(list->update_cb) {
            ABT_rwlock_unlock(*lock);
            (list->update_cb)(
                list->update_cb_dat,
                member_id,
                update_type);
            ABT_rwlock_rdlock(*lock);
       }
       list = list->next;
    }
    ABT_rwlock_unlock(*lock);
}

typedef struct ssg_member_update
{
    ssg_member_update_type_t type;
    union
    {
        char *member_addr_str;
        ssg_member_id_t member_id;
    } u;
} ssg_member_update_t;

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
    ssg_mid_state_t *mid_state);
void ssg_deregister_rpcs(
    ssg_mid_state_t *mid_state);
int ssg_group_join_send(
    ssg_group_id_t g_id,
    hg_addr_t target_addr,
    ssg_mid_state_t * mid_state,
    int * group_size,
    ssg_group_config_t * group_config,
    void ** view_buf);
int ssg_group_leave_send(
    ssg_group_id_t g_id,
    hg_addr_t target_addr,
    ssg_mid_state_t * mid_state);
int ssg_group_refresh_send(
    ssg_group_id_t g_id,
    hg_addr_t target_addr,
    ssg_mid_state_t * mid_state,
    int * group_size,
    void ** view_buf);
int ssg_apply_member_updates(
    ssg_group_descriptor_t  * gd,
    ssg_member_update_t * updates,
    int update_count,
    int swim_apply_flag);
hg_return_t hg_proc_ssg_member_update_t(
    hg_proc_t proc, void *data);

static const UT_icd ut_ssg_member_id_t_icd = {sizeof(ssg_member_id_t),NULL,NULL,NULL};

extern ssg_runtime_state_t *ssg_rt;

#ifdef __cplusplus
}
#endif
