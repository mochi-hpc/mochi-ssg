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
        __g->ssg_inst->self_id, __g->name, ## __VA_ARGS__); \
    fflush(__g->dbg_log); \
} while(0)
#else
#define SSG_DEBUG(__g, __fmt, ...) do { \
} while(0)
#endif

/* SSG internal dataypes */

typedef struct ssg_instance
{
    margo_instance_id mid;
    char *self_addr_str;
    hg_addr_t self_addr;
    ssg_member_id_t self_id;
    struct ssg_group_descriptor *g_desc_table;
#ifdef SSG_HAVE_PMIX
    size_t pmix_failure_evhdlr_ref;
#endif
    ABT_rwlock lock;
} ssg_instance_t;

typedef struct ssg_group_descriptor
{
    uint64_t magic_nr;
    ssg_group_id_t g_id;
    char *addr_str;
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
    char *name;
    ssg_instance_t *ssg_inst;
    ssg_group_view_t view;
    ssg_member_state_t *dead_members;
    ssg_group_descriptor_t *descriptor;
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
    char *name;
    ssg_instance_t *ssg_inst;
    ssg_group_view_t view;
    ssg_group_descriptor_t *descriptor;
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

static inline uint64_t ssg_hash64_str(const char * str)
{
    unsigned hash;
    HASH_JEN(str, strlen(str), hash);
    return (uint64_t)hash;
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
int ssg_group_observe_send(
    ssg_group_descriptor_t * group_descriptor,
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

extern ssg_instance_t *ssg_inst; 

#ifdef __cplusplus
}
#endif
