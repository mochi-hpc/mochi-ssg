/*
 * Copyright (c) 2016 UChicago Argonne, LLC
 *
 * See COPYRIGHT in top-level directory.
 */

#pragma once

#include <abt.h>
#include <margo.h>

#include "ssg.h"
#include "ssg-internal.h"
#include "swim-fd.h"
#include "utlist.h"

#ifdef __cplusplus
extern "C" {
#endif

/* SWIM protocol parameter defaults */
#define SWIM_DEF_PROTOCOL_PERIOD_LEN    2000.0  /* milliseconds */
#define SWIM_DEF_SUSPECT_TIMEOUT        5       /* protocol period lengths */
#define SWIM_DEF_SUBGROUP_SIZE          0 // XXX
#define SWIM_MAX_SUBGROUP_SIZE          5
#define SWIM_MAX_PIGGYBACK_ENTRIES      8
#define SWIM_MAX_PIGGYBACK_TX_COUNT     3

typedef struct swim_ping_target_list
{
    ssg_member_state_t **targets;
    unsigned int nslots;
    unsigned int len;
    unsigned int dping_ndx;
} swim_ping_target_list_t;

typedef struct swim_member_update
{
    ssg_member_id_t id;
    swim_member_state_t state;
} swim_member_update_t;

/* internal swim context implementation */
struct swim_context
{
    /* SWIM protocol parameters */
    double prot_period_len;
    int prot_susp_timeout;
    int prot_subgroup_sz;
    /* SWIM protocol internal state */
    swim_member_inc_nr_t self_inc_nr;
    ssg_member_id_t dping_target_id;
    hg_addr_t dping_target_addr;
    swim_member_inc_nr_t dping_target_inc_nr;
    double dping_timeout;
    ssg_member_id_t iping_target_ids[SWIM_MAX_SUBGROUP_SIZE];
    hg_addr_t iping_target_addrs[SWIM_MAX_SUBGROUP_SIZE];
    int iping_target_ndx;
    int ping_target_acked;
    int shutdown_flag;
    /* list of SWIM ping targets */
    swim_ping_target_list_t target_list;
    /* list of currently supspected SWIM targets */
    void *suspect_list;
    /* lists of SWIM and SSG membership updates to gossip */
    void *swim_update_list;
    void *ssg_update_list;
    /* swim protocol ULT handle */
    ABT_thread prot_thread;
    /* swim protocol lock */
    ABT_rwlock swim_lock;
};

/* SWIM ping function prototypes */
void swim_dping_req_send_ult(
    void * t_arg);
void swim_iping_req_send_ult(
    void * t_arg);

/* SWIM update function prototypes */
void swim_retrieve_member_updates(
    ssg_group_descriptor_t * gd,
    swim_member_update_t * updates,
    hg_size_t *update_count);
void swim_retrieve_ssg_member_updates(
    ssg_group_descriptor_t * gd,
    ssg_member_update_t * updates,
    hg_size_t *update_count);
void swim_apply_member_updates(
    ssg_group_descriptor_t * gd,
    swim_member_update_t * updates,
    hg_size_t update_count);

#ifdef __cplusplus
}
#endif
