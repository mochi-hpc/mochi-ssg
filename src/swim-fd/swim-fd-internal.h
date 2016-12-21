/*
 * (C) 2016 The University of Chicago
 * 
 * See COPYRIGHT in top-level directory.
 */

#ifndef __SWIM_INTERNAL_H
#define __SWIM_INTERNAL_H

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

#define SWIM_MEMBER_ID_UNKNOWN (-1)

typedef int32_t                     swim_member_id_t;
typedef uint8_t                     swim_member_status_t;
typedef uint32_t                    swim_member_inc_nr_t;
typedef struct swim_member_state_s  swim_member_state_t;
typedef enum swim_return            swim_return_t;

/* internal swim context implementation */
struct swim_context
{
    /* margo, mercury, and ssg context */
    margo_instance_id mid;
    hg_context_t *hg_ctx;
    ssg_t group;
    /* argobots pool for launching SWIM threads */
    ABT_pool prot_pool;
    /* SWIM internal state */
    swim_member_id_t ping_target;
    swim_member_id_t subgroup_members[SWIM_MAX_SUBGROUP_SIZE];
    int ping_target_acked;
    double dping_timeout;
    int shutdown_flag;
    /* current membership state */
    swim_member_state_t *membership_view;
    void *suspect_list;
    void *recent_update_list;
    /* SWIM protocol parameters */
    double prot_period_len;
    int prot_susp_timeout;
    int prot_subgroup_sz;
    /* swim protocol ULT handle */
    ABT_thread prot_thread;
};

#if 0
enum swim_member_status
{
    SWIM_MEMBER_ALIVE = 0,
    SWIM_MEMBER_SUSPECT,
    SWIM_MEMBER_DEAD
};

struct swim_member_state_s
{
    swim_member_id_t member;
    swim_member_status_t status;
    swim_member_inc_nr_t inc_nr;
};


void swim_register_ping_rpcs(
    hg_class_t *hg_cls,
    swim_context_t *swim_ctx);

void swim_dping_send_ult(
    void *t_arg);

void swim_iping_send_ult(
    void *t_arg);

void swim_init_membership_view(
    swim_context_t *swim_ctx);

swim_member_id_t swim_get_self_id(
    swim_context_t *swim_ctx);

swim_member_inc_nr_t swim_get_self_inc_nr(
    swim_context_t *swim_ctx);

hg_addr_t swim_get_member_addr(
    swim_context_t *swim_ctx,
    swim_member_id_t member);

void swim_get_rand_member(
    swim_context_t *swim_ctx,
    swim_member_id_t *member);

void swim_get_rand_member_set(
    swim_context_t *swim_ctx,
    swim_member_id_t *member_array,
    int num_members,
    swim_member_id_t excluded_member);

void swim_suspect_member(
    swim_context_t *swim_ctx,
    swim_member_id_t member);

void swim_unsuspect_member(
    swim_context_t *swim_ctx,
    swim_member_id_t member);

void swim_kill_member(
    swim_context_t *swim_ctx,
    swim_member_id_t member);

void swim_update_suspected_members(
    swim_context_t *swim_ctx,
    double susp_timeout);

void swim_retrieve_membership_updates(
    swim_context_t *swim_ctx,
    swim_member_state_t *membership_updates,
    int update_count);

void swim_apply_membership_updates(
    swim_context_t *swim_ctx,
    swim_member_state_t *membership_updates,
    int update_count);
#endif

#ifdef __cplusplus
}
#endif

#endif /* __SWIM_FD_INTERNAL_H */
