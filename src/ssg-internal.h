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
#define SSG_DEBUG(__g, __fmt, ...) do { \
    double __now = ABT_get_wtime(); \
    fprintf(__g->dbg_log, "%.6lf %20"PRIu64" (%s): " __fmt, __now, \
        __g->mid_state->self_id, __g->name, ## __VA_ARGS__); \
    fflush(__g->dbg_log); \
} while(0)
#else
#define SSG_DEBUG(__g, __fmt, ...) do { \
} while(0)
#endif

/* SSG internal dataypes */

typedef struct ssg_runtime_state
{
    struct ssg_group_descriptor *g_desc_table;
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
    hg_id_t join_rpc_id;
    hg_id_t leave_rpc_id;
    hg_id_t observe_rpc_id;
    hg_id_t swim_dping_req_rpc_id;
    hg_id_t swim_dping_ack_rpc_id;
    hg_id_t swim_iping_req_rpc_id;
    hg_id_t swim_iping_ack_rpc_id;
    int ref_count;
    struct ssg_mid_state *next;
} ssg_mid_state_t;

typedef struct ssg_group_descriptor
{
    ssg_group_id_t g_id;
    size_t num_addr_strs;
    char **addr_strs;
    int64_t cred;
    int owner_status;
    union
    {
        struct ssg_group *g;
        struct ssg_observed_group *og;
    } g_data;
    UT_hash_handle hh;
} ssg_group_descriptor_t;

enum ssg_group_descriptor_owner_status
{
    SSG_OWNER_IS_UNASSOCIATED = 0,
    SSG_OWNER_IS_MEMBER,
    SSG_OWNER_IS_OBSERVER
};

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
    uint64_t hash;
    ssg_member_state_t *member_map;
    UT_array *rank_array;
} ssg_group_view_t;

typedef struct ssg_update_cb {
    ssg_membership_update_cb  update_cb;
    void*                     update_cb_dat;
    struct ssg_update_cb*     next;
} ssg_update_cb;

typedef struct ssg_group
{
    ssg_mid_state_t *mid_state;
    char *name;
    ssg_group_view_t view;
    ssg_group_config_t config;
    ssg_member_state_t *dead_members;
    swim_context_t *swim_ctx;
    ssg_update_cb* update_cb_list;
    ABT_rwlock lock;
#ifdef DEBUG
    FILE *dbg_log;
#endif
} ssg_group_t;

typedef struct ssg_observed_group
{
    ssg_mid_state_t *mid_state;
    char *name;
    ssg_group_view_t view;
    ABT_rwlock lock;
} ssg_observed_group_t;

inline static void update_group_hash(ssg_group_t* g, ssg_member_id_t member_id)
{
    g->view.hash ^= member_id;
    printf("Modified member id %lu in hash, hash becomes %lu\n", member_id, g->view.hash);
}

inline static void compute_member_group_hash(ssg_group_t* g)
{
    ssg_member_state_t* state = NULL;
    ssg_member_state_t* tmp = NULL;
    g->view.hash = 0;
    HASH_ITER(hh, g->view.member_map, state, tmp)
    {
        g->view.hash ^= state->id;
    }
    g->view.hash ^= g->mid_state->self_id;
}

inline static void compute_observed_group_hash(ssg_observed_group_t* g)
{
    ssg_member_state_t* state = NULL;
    ssg_member_state_t* tmp = NULL;
    g->view.hash = 0;
    HASH_ITER(hh, g->view.member_map, state, tmp)
    {
        g->view.hash ^= state->id;
    }
}

inline static int add_membership_update_cb(
        ssg_group_t* group,
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
                return -1;
            }
            prev = cur;
            cur = cur->next;
        }
        prev->next = tmp;
    } else {
        group->update_cb_list = tmp;
    }
    return 0;
}

inline static int remove_membership_update_cb(
        ssg_group_t* group,
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
            return 0;
        }
        previous = current;
        current = current->next;
    }
    return -1;
}

inline static void free_all_membership_update_cb(
        ssg_group_t* group)
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
        ssg_update_cb* list,
        ssg_member_id_t member_id,
        ssg_member_update_type_t update_type)
{
    while(list) {
       if(list->update_cb) {
            (list->update_cb)(
                list->update_cb_dat,
                member_id,
                update_type);
       }
       list = list->next;
    }
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
    hg_addr_t group_target_addr,
    ssg_mid_state_t * mid_state,
    char ** group_name,
    int * group_size,
    ssg_group_config_t * group_config,
    void ** view_buf);
int ssg_group_leave_send(
    ssg_group_id_t g_id,
    hg_addr_t group_target_addr,
    ssg_mid_state_t * mid_state);
int ssg_group_observe_send(
    ssg_group_id_t g_id,
    hg_addr_t group_target_addr,
    ssg_mid_state_t * mid_state,
    char ** group_name,
    int * group_size,
    void ** view_buf);
void ssg_apply_member_updates(
    ssg_group_t  * g,
    ssg_member_update_t * updates,
    hg_size_t update_count);
hg_return_t hg_proc_ssg_member_update_t(
    hg_proc_t proc, void *data);

static const UT_icd ut_ssg_member_id_t_icd = {sizeof(ssg_member_id_t),NULL,NULL,NULL};

extern ssg_runtime_state_t *ssg_rt;

#ifdef __cplusplus
}
#endif
