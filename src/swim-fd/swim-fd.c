/*
 * Copyright (c) 2016 UChicago Argonne, LLC
 *
 * See COPYRIGHT in top-level directory.
 */
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <assert.h>

#include <abt.h>
#include <margo.h>

#include "swim-fd.h"
#include "swim-fd-internal.h"

typedef struct swim_suspect_member_link
{
    swim_member_id_t member_id;
    swim_member_state_t *member_state;
    double susp_start;
    struct swim_suspect_member_link *next;
} swim_suspect_member_link_t;

typedef struct swim_member_update_link
{
    swim_member_update_t update;
    int tx_count;
    struct swim_member_update_link *next;
} swim_member_update_link_t;

/* SWIM ABT ULT prototypes */
static void swim_prot_ult(
    void *t_arg);
static void swim_tick_ult(
    void *t_arg);

/* SWIM group membership utility function prototypes */
static void swim_suspect_member(
    swim_context_t *swim_ctx, swim_member_id_t member_id,
    swim_member_inc_nr_t inc_nr);
static void swim_unsuspect_member(
    swim_context_t *swim_ctx, swim_member_id_t member_id,
    swim_member_inc_nr_t inc_nr);
static void swim_kill_member(
    swim_context_t *swim_ctx, swim_member_id_t member_id,
    swim_member_inc_nr_t inc_nr);
static void swim_update_suspected_members(
    swim_context_t *swim_ctx, double susp_timeout);
static void swim_add_recent_member_update(
    swim_context_t *swim_ctx, swim_member_update_t update);

/******************************************************
 * SWIM protocol init/finalize functions and ABT ULTs *
 ******************************************************/

swim_context_t * swim_init(
    margo_instance_id mid,
    void * group_data,
    swim_member_id_t self_id,
    swim_group_mgmt_callbacks_t swim_callbacks,
    int active)
{
    swim_context_t *swim_ctx;
    int ret;

    /* allocate structure for storing swim context */
    swim_ctx = malloc(sizeof(*swim_ctx));
    if (!swim_ctx) return NULL;
    memset(swim_ctx, 0, sizeof(*swim_ctx));
    swim_ctx->mid = mid;
    swim_ctx->group_data = group_data;
    swim_ctx->self_id = self_id;
    swim_ctx->self_inc_nr = 0;
    swim_ctx->swim_callbacks = swim_callbacks;

    /* set protocol parameters */
    swim_ctx->prot_period_len = SWIM_DEF_PROTOCOL_PERIOD_LEN;
    swim_ctx->prot_susp_timeout = SWIM_DEF_SUSPECT_TIMEOUT;
    swim_ctx->prot_subgroup_sz = SWIM_DEF_SUBGROUP_SIZE;

    margo_get_handler_pool(swim_ctx->mid, &swim_ctx->swim_pool);
    ABT_mutex_create(&swim_ctx->swim_mutex);

    /* NOTE: set this flag so we don't inadvertently suspect a member
     * on the first iteration of the protocol
     */
    swim_ctx->ping_target_acked = 1;

    swim_register_ping_rpcs(swim_ctx);

    if(active)
    {
        ret = ABT_thread_create(swim_ctx->swim_pool, swim_prot_ult, swim_ctx,
            ABT_THREAD_ATTR_NULL, &(swim_ctx->prot_thread));
        if(ret != ABT_SUCCESS)
        {
            fprintf(stderr, "Error: unable to create SWIM protocol ULT.\n");
            free(swim_ctx);
            return(NULL);
        }
    }

    return(swim_ctx);
}

static void swim_prot_ult(
    void * t_arg)
{
    int ret;
    swim_context_t *swim_ctx = (swim_context_t *)t_arg;

    assert(swim_ctx != NULL);

    SWIM_DEBUG(swim_ctx,
        "protocol start (period_len=%.4f, susp_timeout=%d, subgroup_size=%d)\n",
        swim_ctx->prot_period_len, swim_ctx->prot_susp_timeout,
        swim_ctx->prot_subgroup_sz);

    while(!(swim_ctx->shutdown_flag))
    {
        /* spawn a ULT to run this tick */
        ret = ABT_thread_create(swim_ctx->swim_pool, swim_tick_ult, swim_ctx,
            ABT_THREAD_ATTR_NULL, NULL);
        if(ret != ABT_SUCCESS)
        {
            fprintf(stderr, "Error: unable to create ULT for SWIM protocol tick\n");
        }

        /* sleep for a protocol period length */
        margo_thread_sleep(swim_ctx->mid, swim_ctx->prot_period_len);
    }

    SWIM_DEBUG(swim_ctx, "protocol shutdown\n");

    return;
}

static void swim_tick_ult(
    void * t_arg)
{
    swim_context_t *swim_ctx = (swim_context_t *)t_arg;
    int i;
    int ret;

    assert(swim_ctx != NULL);

    /* update status of any suspected members */
    swim_update_suspected_members(swim_ctx, swim_ctx->prot_susp_timeout *
        swim_ctx->prot_period_len);

    /* check whether the ping target from the previous protocol tick
     * ever successfully acked a (direct/indirect) ping request
     */
    if(!(swim_ctx->ping_target_acked))
    {
        /* no response from direct/indirect pings, suspect this member */
        swim_suspect_member(swim_ctx, swim_ctx->dping_target_id,
            swim_ctx->dping_target_inc_nr);
    }

    /* pick a random member from view to ping */
    ret = swim_ctx->swim_callbacks.get_dping_target(
        swim_ctx->group_data, &swim_ctx->dping_target_id,
        &swim_ctx->dping_target_inc_nr, &swim_ctx->dping_target_addr);
    if(ret != 0)
    {
        /* no available members, back out */
        SWIM_DEBUG(swim_ctx, "no group members available to dping\n");
        return;
    }

    /* TODO: calculate estimated RTT using sliding window of past RTTs */
    swim_ctx->dping_timeout = 250.0;

    /* kick off dping request ULT */
    swim_ctx->ping_target_acked = 0;
    ret = ABT_thread_create(swim_ctx->swim_pool, swim_dping_send_ult, swim_ctx,
        ABT_THREAD_ATTR_NULL, NULL);
    if(ret != ABT_SUCCESS)
    {
        fprintf(stderr, "Error: unable to create ULT for SWIM dping send\n");
        return;
    }

    /* sleep for an RTT and wait for an ack for this dping req */
    margo_thread_sleep(swim_ctx->mid, swim_ctx->dping_timeout);

    /* if we don't hear back from the target after an RTT, kick off
     * a set of indirect pings to a subgroup of group members
     */
    if(!(swim_ctx->ping_target_acked) && (swim_ctx->prot_subgroup_sz > 0))
    {
        /* get a random subgroup of members to send indirect pings to */
        int iping_target_count = swim_ctx->prot_subgroup_sz;
        swim_ctx->swim_callbacks.get_iping_targets(
            swim_ctx->group_data, &iping_target_count, swim_ctx->iping_target_ids,
            swim_ctx->iping_target_addrs);
        if(iping_target_count == 0)
        {
            /* no available subgroup members, back out */
            SWIM_DEBUG(swim_ctx, "no subgroup members available to iping\n");
            return;
        }

        swim_ctx->iping_target_ndx = 0;
        for(i = 0; i < iping_target_count; i++)
        {
            ret = ABT_thread_create(swim_ctx->swim_pool, swim_iping_send_ult,
                swim_ctx, ABT_THREAD_ATTR_NULL, NULL);
            if(ret != ABT_SUCCESS)
            {
                fprintf(stderr, "Error: unable to create ULT for SWIM iping send\n");
                return;
            }
        }
    }

    return;
}

void swim_finalize(swim_context_t *swim_ctx)
{
    /* set shutdown flag so ULTs know to start wrapping up */
    swim_ctx->shutdown_flag = 1;

    if(swim_ctx->prot_thread)
    {
        /* wait for the protocol ULT to terminate */
        ABT_thread_join(swim_ctx->prot_thread);
        ABT_thread_free(&(swim_ctx->prot_thread));
    }

    free(swim_ctx);

    return;
}

/************************************
 * SWIM membership update functions *
 ************************************/

void swim_retrieve_membership_updates(
    swim_context_t *swim_ctx,
    swim_member_update_t *updates,
    hg_size_t *update_count)
{
    swim_member_update_link_t *iter, *tmp;
    swim_member_update_link_t *recent_update_list =
        (swim_member_update_link_t *)swim_ctx->recent_update_list;
    hg_size_t i = 0;
    hg_size_t max_updates = *update_count;

    LL_FOREACH_SAFE(recent_update_list, iter, tmp)
    {
        if(i == max_updates)
            break;

        memcpy(&updates[i], &iter->update, sizeof(iter->update));

        /* remove this update if it has been piggybacked enough */
        iter->tx_count++;
        if(iter->tx_count == SWIM_MAX_PIGGYBACK_TX_COUNT)
        {
            LL_DELETE(recent_update_list, iter);
            free(iter);
        }
        i++;
    }
    *update_count = i;

    return;
}

void swim_apply_membership_updates(
    swim_context_t *swim_ctx,
    swim_member_update_t *updates,
    hg_size_t update_count)
{
    hg_size_t i;

    for(i = 0; i < update_count; i++)
    {
        switch(updates[i].state.status)
        {
            case SWIM_MEMBER_ALIVE:
                /* ignore alive updates for self */
                if(updates[i].id != swim_ctx->self_id)
                    swim_unsuspect_member(swim_ctx, updates[i].id, updates[i].state.inc_nr);
                break;
            case SWIM_MEMBER_SUSPECT:
                if(updates[i].id == swim_ctx->self_id)
                {
                    /* increment our incarnation number if we are suspected
                     * in the current incarnation
                     */
                    if(updates[i].state.inc_nr == swim_ctx->self_inc_nr)
                    {
                        swim_ctx->self_inc_nr++;
                        SWIM_DEBUG(swim_ctx, "self SUSPECT received (new inc_nr=%u)\n",
                            swim_ctx->self_inc_nr);
                    }
                }
                else
                {
                    swim_suspect_member(swim_ctx, updates[i].id, updates[i].state.inc_nr);
                }
                break;
            case SWIM_MEMBER_DEAD:
                /* if we get an update that we are dead, just shut down */
                if(updates[i].id == swim_ctx->self_id)
                {
                    SWIM_DEBUG(swim_ctx, "self confirmed DEAD (inc_nr=%u)\n",
                        updates[i].state.inc_nr);
                    swim_finalize(swim_ctx);
                    return;
                }
                else
                {
                    swim_kill_member(swim_ctx, updates[i].id, updates[i].state.inc_nr);
                }
                break;
            default:
                fprintf(stderr, "Error: invalid SWIM member update\n");
        }
    }

    return;
}

/*******************************************
 * SWIM group membership utility functions *
 *******************************************/

static void swim_suspect_member(
    swim_context_t *swim_ctx, swim_member_id_t member_id, swim_member_inc_nr_t inc_nr)
{
    swim_member_state_t *cur_swim_state;
    swim_suspect_member_link_t *iter, *tmp;
    swim_suspect_member_link_t *suspect_link = NULL;
    swim_suspect_member_link_t *suspect_list_p =
        (swim_suspect_member_link_t *)swim_ctx->suspect_list;
    swim_member_update_t update;

    /* if there is no suspicion timeout, just kill the member */
    if(swim_ctx->prot_susp_timeout == 0)
    {
        swim_kill_member(swim_ctx, member_id, inc_nr);
        return;
    }

    /* lock access to group's swim state */
    ABT_mutex_lock(swim_ctx->swim_mutex);

    /* get current swim state for member */
    swim_ctx->swim_callbacks.get_member_state(
        swim_ctx->group_data, member_id, &cur_swim_state);

    /* ignore updates for dead members */
    if(cur_swim_state->status == SWIM_MEMBER_DEAD)
    {
        ABT_mutex_unlock(swim_ctx->swim_mutex);
        return;
    }

    /* determine if this member is already suspected */
    LL_FOREACH_SAFE(suspect_list_p, iter, tmp)
    {
        if(iter->member_id == member_id)
        {
            if(inc_nr <= cur_swim_state->inc_nr)
            {
                /* ignore a suspicion in an incarnation number less than
                 * or equal to the current suspicion's incarnation
                 */
                ABT_mutex_unlock(swim_ctx->swim_mutex);
                return;
            }

            /* otherwise, we have a suspicion in a more recent incarnation --
             * remove the current suspicion so we can update it
             */
            LL_DELETE(suspect_list_p, iter);
            suspect_link = iter;
        }
    }

    /* ignore suspicions for a member that is alive in a newer incarnation */
    if((suspect_link == NULL) && (inc_nr < cur_swim_state->inc_nr))
    {
        ABT_mutex_unlock(swim_ctx->swim_mutex);
        return;
    }

    SWIM_DEBUG(swim_ctx, "member %lu SUSPECT (inc_nr=%u)\n", member_id, inc_nr);

    if(suspect_link == NULL)
    {
        /* if this member is not already on the suspect list,
         * allocate a link for it
         */
        suspect_link = malloc(sizeof(*suspect_link));
        if (!suspect_link)
        {
            ABT_mutex_unlock(swim_ctx->swim_mutex);
            return;
        }
        memset(suspect_link, 0, sizeof(*suspect_link));
        suspect_link->member_id = member_id;
        suspect_link->member_state = cur_swim_state;
    }
    suspect_link->susp_start = ABT_get_wtime();

    /* add to end of suspect list */
    LL_APPEND(suspect_list_p, suspect_link);

    /* update swim membership state */
    cur_swim_state->inc_nr = inc_nr;
    cur_swim_state->status = SWIM_MEMBER_SUSPECT;

    /* add this update to recent update list so it will be piggybacked
     * on future protocol messages
     */
    update.id = member_id;
    update.state.status = SWIM_MEMBER_SUSPECT;
    update.state.inc_nr = inc_nr;
    swim_add_recent_member_update(swim_ctx, update);

    ABT_mutex_unlock(swim_ctx->swim_mutex);

    return;
}

static void swim_unsuspect_member(
    swim_context_t *swim_ctx, swim_member_id_t member_id, swim_member_inc_nr_t inc_nr)
{
    swim_member_state_t *cur_swim_state;
    swim_suspect_member_link_t *iter, *tmp;
    swim_suspect_member_link_t *suspect_list =
        (swim_suspect_member_link_t *)swim_ctx->suspect_list;
    swim_member_update_t update;

    /* lock access to group's swim state */
    ABT_mutex_lock(swim_ctx->swim_mutex);

    /* get current swim state for member */
    swim_ctx->swim_callbacks.get_member_state(
        swim_ctx->group_data, member_id, &cur_swim_state);

    /* ignore updates for dead members */
    if(cur_swim_state->status == SWIM_MEMBER_DEAD)
    {
        ABT_mutex_unlock(swim_ctx->swim_mutex);
        return;
    }

    /* ignore alive updates for incarnation numbers that aren't new */
    if(inc_nr <= cur_swim_state->inc_nr)
    {
        ABT_mutex_unlock(swim_ctx->swim_mutex);
        return;
    }

    SWIM_DEBUG(swim_ctx, "member %lu ALIVE (inc_nr=%u)\n", member_id, inc_nr);

    /* if member is suspected, remove from suspect list */
    LL_FOREACH_SAFE(suspect_list, iter, tmp)
    {
        if(iter->member_id == member_id)
        {
            LL_DELETE(suspect_list, iter);
            free(iter);
            break;
        }
    }

    /* update swim membership state */
    cur_swim_state->inc_nr = inc_nr;
    cur_swim_state->status = SWIM_MEMBER_ALIVE;

    /* add this update to recent update list so it will be piggybacked
     * on future protocol messages
     */
    update.id = member_id;
    update.state.status = SWIM_MEMBER_ALIVE;
    update.state.inc_nr = inc_nr;
    swim_add_recent_member_update(swim_ctx, update);

    ABT_mutex_unlock(swim_ctx->swim_mutex);

    return;
}

static void swim_kill_member(
    swim_context_t *swim_ctx, swim_member_id_t member_id, swim_member_inc_nr_t inc_nr)
{
    swim_member_state_t *cur_swim_state;
    swim_suspect_member_link_t *iter, *tmp;
    swim_suspect_member_link_t *suspect_list_p =
        (swim_suspect_member_link_t *)swim_ctx->suspect_list;
    swim_member_update_t update;

    /* lock access to group's swim state */
    ABT_mutex_lock(swim_ctx->swim_mutex);

    /* get current swim state for member */
    swim_ctx->swim_callbacks.get_member_state(
        swim_ctx->group_data, member_id, &cur_swim_state);

    /* ignore updates for dead members */
    if(cur_swim_state->status == SWIM_MEMBER_DEAD)
    {
        ABT_mutex_unlock(swim_ctx->swim_mutex);
        return;
    }

    SWIM_DEBUG(swim_ctx, "member %lu DEAD (inc_nr=%u)\n", member_id, inc_nr);

    LL_FOREACH_SAFE(suspect_list_p, iter, tmp)
    {
        if(iter->member_id == member_id)
        {
            /* remove member from suspect list */
            LL_DELETE(suspect_list_p, iter);
            free(iter);
            break;
        }
    }

    /* update swim membership state */
    cur_swim_state->inc_nr = inc_nr;
    cur_swim_state->status = SWIM_MEMBER_DEAD;

    /* add this update to recent update list so it will be piggybacked
     * on future protocol messages
     */
    update.id = member_id;
    update.state.status = SWIM_MEMBER_DEAD;
    update.state.inc_nr = inc_nr;
    swim_add_recent_member_update(swim_ctx, update);

    /* have group management layer apply this update */
    swim_ctx->swim_callbacks.apply_member_update(
        swim_ctx->group_data, update);

    ABT_mutex_unlock(swim_ctx->swim_mutex);

    return;
}

static void swim_update_suspected_members(
    swim_context_t *swim_ctx, double susp_timeout)
{
    double now = ABT_get_wtime();
    double susp_dur;
    swim_suspect_member_link_t *iter, *tmp;
    swim_suspect_member_link_t *suspect_list_p =
        (swim_suspect_member_link_t *)swim_ctx->suspect_list;

    ABT_mutex_lock(swim_ctx->swim_mutex);

    LL_FOREACH_SAFE(suspect_list_p, iter, tmp)
    {
        susp_dur = now - iter->susp_start;
        if(susp_dur >= (susp_timeout / 1000.0))
        {
            /* if this member has exceeded its allowable suspicion timeout,
             * we mark it as dead
             */
            swim_kill_member(swim_ctx, iter->member_id,
                iter->member_state->inc_nr);
        }
    }

    ABT_mutex_unlock(swim_ctx->swim_mutex);

    return;
}

static void swim_add_recent_member_update(
    swim_context_t *swim_ctx, swim_member_update_t update)
{
    swim_member_update_link_t *iter, *tmp;
    swim_member_update_link_t *update_link = NULL;
    swim_member_update_link_t *recent_update_list =
        (swim_member_update_link_t *)swim_ctx->recent_update_list;

    /* search and remove any recent updates corresponding to this member */
    LL_FOREACH_SAFE(recent_update_list, iter, tmp)
    {
        if(iter->update.id == update.id)
        {
            LL_DELETE(recent_update_list, iter);
            update_link = iter;
        }
    }

    if(update_link == NULL)
    {
        update_link = malloc(sizeof(*update_link));
        assert(update_link);
        memset(update_link, 0, sizeof(*update_link));
    }

    memcpy(&update_link->update, &update, sizeof(update));

    /* add to recent update list */
    update_link->tx_count = 0;
    LL_APPEND(recent_update_list, update_link);

    return;
}
