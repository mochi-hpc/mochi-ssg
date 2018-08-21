/*
 * Copyright (c) 2016 UChicago Argonne, LLC
 *
 * See COPYRIGHT in top-level directory.
 */

#pragma once

#include <abt.h>
#include <margo.h>

#include "swim-fd.h"
#include "utlist.h"

#ifdef __cplusplus
extern "C" {
#endif

/* SWIM protocol parameter defaults */
#define SWIM_DEF_PROTOCOL_PERIOD_LEN    2000.0  /* milliseconds */
#define SWIM_DEF_SUSPECT_TIMEOUT        5       /* protocol period lengths */
#define SWIM_DEF_SUBGROUP_SIZE          2
#define SWIM_MAX_SUBGROUP_SIZE          5
#define SWIM_MAX_PIGGYBACK_ENTRIES      8
#define SWIM_MAX_PIGGYBACK_TX_COUNT     50

/* debug printing macro for SWIM */
#ifdef DEBUG
#define SWIM_DEBUG(__swim_ctx, __fmt, ...) do { \
    double __now = ABT_get_wtime(); \
    fprintf(stdout, "[%.6lf] %20"PRIu64": SWIM " __fmt, __now, \
        __swim_ctx->self_id, ## __VA_ARGS__); \
    fflush(stdout); \
} while(0)
#else
#define SWIM_DEBUG(__swim_ctx, __fmt, ...) do { \
} while(0)
#endif

/* internal swim context implementation */
struct swim_context
{
    margo_instance_id mid;
    /* void pointer to user group data */
    void *group_data;
    /* group management callbacks */
    swim_group_mgmt_callbacks_t swim_callbacks;
    /* SWIM protocol parameters */
    double prot_period_len;
    int prot_susp_timeout;
    int prot_subgroup_sz;
    /* SWIM protocol internal state */
    swim_member_id_t self_id;
    swim_member_inc_nr_t self_inc_nr;
    swim_member_id_t dping_target_id;
    swim_member_inc_nr_t dping_target_inc_nr;
    hg_addr_t dping_target_addr;
    double dping_timeout;
    swim_member_id_t iping_target_ids[SWIM_MAX_SUBGROUP_SIZE];
    hg_addr_t iping_target_addrs[SWIM_MAX_SUBGROUP_SIZE];
    int iping_target_ndx;
    int ping_target_acked;
    void *suspect_list;
    void *recent_update_list;
    int shutdown_flag;
    /* argobots pool for launching SWIM threads */
    ABT_pool swim_pool;
    /* mutex for modifying SWIM group state */
    ABT_mutex swim_mutex;
    /* swim protocol ULT handle */
    ABT_thread prot_thread;
};

/* SWIM ping function prototypes */
void swim_register_ping_rpcs(
    swim_context_t * swim_ctx);
void swim_dping_send_ult(
    void * t_arg);
void swim_iping_send_ult(
    void * t_arg);

/* SWIM membership update function prototypes */
void swim_retrieve_membership_updates(
    swim_context_t *swim_ctx,
    swim_member_update_t *updates,
    hg_size_t *update_count);
void swim_apply_membership_updates(
    swim_context_t *swim_ctx,
    swim_member_update_t *updates,
    hg_size_t update_count);

#ifdef __cplusplus
}
#endif
