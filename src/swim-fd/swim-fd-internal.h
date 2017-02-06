/*
 * Copyright (c) 2016 UChicago Argonne, LLC
 *
 * See COPYRIGHT in top-level directory.
 */

#pragma once

#ifdef __cplusplus
extern "C" {
#endif

#include <ssg.h>

#define SWIM_DEF_PROTOCOL_PERIOD_LEN    2000.0  /* milliseconds */
#define SWIM_DEF_SUSPECT_TIMEOUT        5       /* protocol period lengths */
#define SWIM_DEF_SUBGROUP_SIZE          2
#define SWIM_MAX_SUBGROUP_SIZE          5
#define SWIM_MAX_PIGGYBACK_ENTRIES      8
#define SWIM_MAX_PIGGYBACK_TX_COUNT     50

/* internal swim context implementation */
struct swim_context
{
    /* argobots pool for launching SWIM threads */
    ABT_pool prot_pool;
    /* SWIM internal state */
    int ping_target;
    int ping_target_acked;
    double dping_timeout;
    int subgroup_members[SWIM_MAX_SUBGROUP_SIZE];
    int shutdown_flag;
    /* current membership state */
    void *suspect_list;
    void *recent_update_list;
    /* SWIM protocol parameters */
    double prot_period_len;
    int prot_susp_timeout;
    int prot_subgroup_sz;
    /* swim protocol ULT handle */
    ABT_thread prot_thread;
};

void swim_register_ping_rpcs(
    ssg_t s);

void swim_dping_send_ult(
    void *t_arg);

void swim_iping_send_ult(
    void *t_arg);

#ifdef __cplusplus
}
#endif
