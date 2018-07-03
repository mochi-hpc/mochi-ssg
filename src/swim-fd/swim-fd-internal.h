/*
 * Copyright (c) 2016 UChicago Argonne, LLC
 *
 * See COPYRIGHT in top-level directory.
 */

#pragma once

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

typedef struct swim_member_update swim_member_update_t;

struct swim_member_update
{
    ssg_member_id_t id;
    swim_member_status_t status;
    swim_member_inc_nr_t inc_nr;
};

/* internal swim context implementation */
struct swim_context
{
    /* argobots pool for launching SWIM threads */
    ABT_pool prot_pool;
    /* SWIM internal state */
    ssg_member_id_t ping_target;
    swim_member_inc_nr_t ping_target_inc_nr;
    int ping_target_acked;
    double dping_timeout;
    ssg_member_id_t subgroup_members[SWIM_MAX_SUBGROUP_SIZE];
    int shutdown_flag;
    /* current membership state */
    swim_member_inc_nr_t *member_inc_nrs;
    void *suspect_list;
    void *recent_update_list;
    /* SWIM protocol parameters */
    double prot_period_len;
    int prot_susp_timeout;
    int prot_subgroup_sz;
    /* swim protocol ULT handle */
    ABT_thread prot_thread;
};

/* SWIM ping function prototypes */
void swim_register_ping_rpcs(
    ssg_group_t * g);
void swim_dping_send_ult(
    void * t_arg);
void swim_iping_send_ult(
    void * t_arg);

/* SWIM membership update function prototypes */
void swim_retrieve_membership_updates(
    ssg_group_t * g,
    swim_member_update_t * updates,
    int update_count);
void swim_apply_membership_updates(
    ssg_group_t * g,
    swim_member_update_t * updates,
    int update_count);

#ifdef __cplusplus
}
#endif
