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
    ssg_member_state_t *member_map;
    UT_array *rank_array;
} ssg_group_view_t;

typedef struct ssg_group
{
    ssg_mid_state_t *mid_state;
    char *name;
    ssg_group_view_t view;
    ssg_group_config_t config;
    ssg_member_state_t *dead_members;
    swim_context_t *swim_ctx;
    ssg_membership_update_cb update_cb;
    void *update_cb_dat;
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
