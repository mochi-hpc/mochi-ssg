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

#if 0
typedef struct swim_suspect_member_link
{
    double susp_start;
    struct swim_suspect_member_link *next;
} swim_suspect_member_link_t;

typedef struct swim_member_update_link
{
    swim_member_update_t update;
    int tx_count;
    struct swim_member_update_link *next;
} swim_member_update_link_t;
#endif

/* SWIM ABT ULT prototypes */
static void swim_prot_ult(
    void *t_arg);
static void swim_tick_ult(
    void *t_arg);

#if 0
/* SWIM group membership utility function prototypes */
static void swim_suspect_member(
    ssg_group_t *g, ssg_member_id_t member_id, swim_member_inc_nr_t inc_nr);
static void swim_unsuspect_member(
    ssg_group_t *g, ssg_member_id_t member_id, swim_member_inc_nr_t inc_nr);
static void swim_kill_member(
    ssg_group_t *g, ssg_member_id_t member_id, swim_member_inc_nr_t inc_nr);
static void swim_update_suspected_members(
    ssg_group_t *g, double susp_timeout);
static void swim_add_recent_member_update(
    ssg_group_t *g, swim_member_update_t update);
#endif

/******************************************************
 * SWIM protocol init/finalize functions and ABT ULTs *
 ******************************************************/

swim_context_t * swim_init(
    margo_instance_id mid,
    void * group_data,
    swim_group_mgmt_callbacks_t swim_callbacks,
    int active)
{
    swim_context_t *swim_ctx;
    int i, ret;

    /* allocate structure for storing swim context */
    swim_ctx = malloc(sizeof(*swim_ctx));
    if (!swim_ctx) return NULL;
    memset(swim_ctx, 0, sizeof(*swim_ctx));
    swim_ctx->mid = mid;
    swim_ctx->group_data = group_data;
    swim_ctx->swim_callbacks = swim_callbacks;

    /* initialize SWIM context */
    margo_get_handler_pool(swim_ctx->mid, &swim_ctx->swim_pool);
    swim_ctx->dping_target_addr = HG_ADDR_NULL;
    for(i = 0; i < SWIM_MAX_SUBGROUP_SIZE; i++)
        swim_ctx->iping_subgroup_addrs[i] = HG_ADDR_NULL;

    /* set protocol parameters */
    swim_ctx->prot_period_len = SWIM_DEF_PROTOCOL_PERIOD_LEN;
    swim_ctx->prot_susp_timeout = SWIM_DEF_SUSPECT_TIMEOUT;
    swim_ctx->prot_subgroup_sz = SWIM_DEF_SUBGROUP_SIZE;

    /* XXX */
#if 0
    swim_register_ping_rpcs(g);
#endif

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

#if 0
    SSG_DEBUG(g, "SWIM: protocol start (period_len=%.4f, susp_timeout=%d, subgroup_size=%d)\n",
        swim_ctx->prot_period_len, swim_ctx->prot_susp_timeout,
        swim_ctx->prot_subgroup_sz);
#endif
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
#if 0
    SSG_DEBUG(g, "SWIM: protocol shutdown\n");
#endif
    return;
}

static void swim_tick_ult(
    void * t_arg)
{
    swim_context_t *swim_ctx = (swim_context_t *)t_arg;
    int i;
    int ret;

    assert(swim_ctx != NULL);

#if 0
    /* update status of any suspected members */
    swim_update_suspected_members(g, swim_ctx->prot_susp_timeout *
        swim_ctx->prot_period_len);

    /* check whether the ping target from the previous protocol tick
     * ever successfully acked a (direct/indirect) ping request
     */
    if((swim_ctx->ping_target != SSG_MEMBER_ID_INVALID) &&
        !(swim_ctx->ping_target_acked))
    {
        /* no response from direct/indirect pings, suspect this member */
        swim_suspect_member(g, swim_ctx->ping_target, swim_ctx->ping_target_inc_nr);
    }
#endif

    /* pick a random member from view and ping */
    ret = swim_ctx->swim_callbacks.get_dping_target(swim_ctx->group_data,
        &swim_ctx->dping_target_addr, &swim_ctx->dping_target_state);
    if(ret != 0)
    {
        /* no available members, back out */
#if 0
        SSG_DEBUG(g, "SWIM: no group members available to dping\n");
#endif
        return;
    }

    /* TODO: calculate estimated RTT using sliding window of past RTTs */
    swim_ctx->dping_timeout = 250.0;

#if 0
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
    margo_thread_sleep(ssg_inst->mid, swim_ctx->dping_timeout);

    /* if we don't hear back from the target after an RTT, kick off
     * a set of indirect pings to a subgroup of group members
     */
    if(!(swim_ctx->ping_target_acked) && (swim_ctx->prot_subgroup_sz > 0))
    {
        /* get a random subgroup of members to send indirect pings to */
        int this_subgroup_sz = swim_get_rand_group_member_set(g,
            swim_ctx->subgroup_members, swim_ctx->prot_subgroup_sz,
            swim_ctx->ping_target);
        if(this_subgroup_sz == 0)
        {
            /* no available subgroup members, back out */
            SSG_DEBUG(g, "SWIM: no subgroup members available to iping\n");
            return;
        }

        for(i = 0; i < this_subgroup_sz; i++)
        {
            ret = ABT_thread_create(swim_ctx->prot_pool, swim_iping_send_ult, g,
                ABT_THREAD_ATTR_NULL, NULL);
            if(ret != ABT_SUCCESS)
            {
                fprintf(stderr, "Error: unable to create ULT for SWIM iping send\n");
                return;
            }
        }
    }
#endif

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

#if 0
void swim_retrieve_membership_updates(
    ssg_group_t * g,
    swim_member_update_t * updates,
    int update_count)
{
    swim_context_t *swim_ctx = g->swim_ctx;
    swim_member_update_link_t *iter, *tmp;
    swim_member_update_link_t *recent_update_list_p =
        (swim_member_update_link_t *)swim_ctx->recent_update_list;
    int i = 0;

    LL_FOREACH_SAFE(recent_update_list_p, iter, tmp)
    {
        if(i == update_count)
            break;

        memcpy(&updates[i], &iter->update, sizeof(iter->update));

        /* remove this update if it has been piggybacked enough */
        iter->tx_count++;
        if(iter->tx_count == SWIM_MAX_PIGGYBACK_TX_COUNT)
        {
            LL_DELETE(recent_update_list_p, iter);
            free(iter);
        }
        i++;
    }

    /* invalidate remaining updates */
    for(; i < update_count; i++)
    {
        updates[i].id = SSG_MEMBER_ID_INVALID;
    }

    return;
}

void swim_apply_membership_updates(
    ssg_group_t *g,
    swim_member_update_t *updates,
    int update_count)
{
    swim_context_t *swim_ctx = g->swim_ctx;
    ssg_member_id_t self_id = g->self_id;
    int i;

    for(i = 0; i < update_count; i++)
    {
        if(updates[i].id == SSG_MEMBER_ID_INVALID)
            break;

        switch(updates[i].status)
        {
            case SWIM_MEMBER_ALIVE:
                if(updates[i].id == self_id)
                {
                    assert(updates[i].inc_nr <= swim_ctx->member_inc_nrs[self_id]);
                }
                else
                {
                    swim_unsuspect_member(g, updates[i].id, updates[i].inc_nr);
                }
                break;
            case SWIM_MEMBER_SUSPECT:
                if(updates[i].id == self_id)
                {
                    assert(updates[i].inc_nr <= swim_ctx->member_inc_nrs[self_id]);
                    /* increment our incarnation number if we are suspected
                     * in the current incarnation
                     */
                    if(updates[i].inc_nr == swim_ctx->member_inc_nrs[self_id])
                    {
                        swim_ctx->member_inc_nrs[self_id]++;
                        SSG_DEBUG(g, "SWIM: self SUSPECT received (new inc_nr=%d)\n",
                            swim_ctx->member_inc_nrs[self_id]);
                    }
                }
                else
                {
                    swim_suspect_member(g, updates[i].id, updates[i].inc_nr);
                }
                break;
            case SWIM_MEMBER_DEAD:
                if(updates[i].id == self_id)
                {
                    /* if we get an update that we are dead, just shut down */
                    assert(updates[i].inc_nr <= swim_ctx->member_inc_nrs[self_id]);
                    swim_ctx->member_inc_nrs[self_id] = updates[i].inc_nr;

                    SSG_DEBUG(g, "SWIM: self confirmed DEAD (inc_nr=%d)\n",
                        swim_ctx->member_inc_nrs[self_id]);

                    swim_finalize(swim_ctx);
                }
                else
                {
                    swim_kill_member(g, updates[i].id, updates[i].inc_nr);
                }
                break;
            default:
                SSG_DEBUG(g, "SWIM: invalid membership status update\n");
        }
    }

    return;
}

/*******************************************
 * SWIM group membership utility functions *
 *******************************************/

static void swim_suspect_member(
    ssg_group_t *g, ssg_member_id_t member_id, swim_member_inc_nr_t inc_nr)
{
    swim_context_t *swim_ctx = g->swim_ctx;
    swim_suspect_member_link_t *iter, *tmp;
    swim_suspect_member_link_t *suspect_link = NULL;
    swim_suspect_member_link_t *suspect_list_p =
        (swim_suspect_member_link_t *)swim_ctx->suspect_list;
    swim_member_update_t update;

    /* ignore updates for dead members */
#if 0
    if(!(g->view.member_states[member_id].is_member))
        return;
#endif

    /* determine if this member is already suspected */
    LL_FOREACH_SAFE(suspect_list_p, iter, tmp)
    {
        if(iter->member_id == member_id)
        {
            if(inc_nr <= swim_ctx->member_inc_nrs[member_id])
            {
                /* ignore a suspicion in an incarnation number less than
                 * or equal to the current suspicion's incarnation
                 */
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
    if((suspect_link == NULL) && (inc_nr < swim_ctx->member_inc_nrs[member_id]))
        return;

    /* if there is no suspicion timeout, just kill the member */
    if(swim_ctx->prot_susp_timeout == 0)
    {
        swim_kill_member(g, member_id, inc_nr);
        return;
    }

    SSG_DEBUG(g, "SWIM: member %d SUSPECT (inc_nr=%d)\n",
        (int)member_id, (int)inc_nr);

    if(suspect_link == NULL)
    {
        /* if this member is not already on the suspect list,
         * allocate a link for it
         */
        suspect_link = malloc(sizeof(*suspect_link));
        if (!suspect_link)
            return;
        memset(suspect_link, 0, sizeof(*suspect_link));
        suspect_link->member_id = member_id;
    }
    suspect_link->susp_start = ABT_get_wtime();

    /* add to end of suspect list */
    LL_APPEND(suspect_list_p, suspect_link);

    /* update swim membership state */
    swim_ctx->member_inc_nrs[member_id] = inc_nr;

    /* add this update to recent update list so it will be piggybacked
     * on future protocol messages
     */
    update.id = member_id;
    update.status = SWIM_MEMBER_SUSPECT;
    update.inc_nr = inc_nr;
    swim_add_recent_member_update(g, update);

    return;
}

static void swim_unsuspect_member(
    ssg_group_t *g, ssg_member_id_t member_id, swim_member_inc_nr_t inc_nr)
{
    swim_context_t *swim_ctx = g->swim_ctx;
    swim_suspect_member_link_t *iter, *tmp;
    swim_suspect_member_link_t *suspect_list_p =
        (swim_suspect_member_link_t *)swim_ctx->suspect_list;
    swim_member_update_t update;

    /* ignore updates for dead members */
#if 0
    if(!(g->view.member_states[member_id].is_member))
        return;
#endif

    /* ignore alive updates for incarnation numbers that aren't new */
    if(inc_nr <= swim_ctx->member_inc_nrs[member_id])
        return;

    SSG_DEBUG(g, "SWIM: member %d ALIVE (inc_nr=%d)\n",
        (int)member_id, (int)inc_nr);

    /* if member is suspected, remove from suspect list */
    LL_FOREACH_SAFE(suspect_list_p, iter, tmp)
    {
        if(iter->member_id == member_id)
        {
            LL_DELETE(suspect_list_p, iter);
            free(iter);
            break;
        }
    }

    /* update swim membership state */
    swim_ctx->member_inc_nrs[member_id] = inc_nr;

    /* add this update to recent update list so it will be piggybacked
     * on future protocol messages
     */
    update.id = member_id;
    update.status = SWIM_MEMBER_ALIVE;
    update.inc_nr = inc_nr;
    swim_add_recent_member_update(g, update);

    return;
}

static void swim_kill_member(
    ssg_group_t *g, ssg_member_id_t member_id, swim_member_inc_nr_t inc_nr)
{
    swim_context_t *swim_ctx = g->swim_ctx;
    ssg_member_state_t *member_state;
    swim_suspect_member_link_t *iter, *tmp;
    swim_suspect_member_link_t *suspect_list_p =
        (swim_suspect_member_link_t *)swim_ctx->suspect_list;
    swim_member_update_t swim_update;
    ssg_membership_update_t ssg_update;

    HASH_FIND(hh, g->view.member_map, &member_id, sizeof(ssg_member_id_t),
        member_state);
    if(!member_state)
    {
        fprintf(stderr, "Error: unable to kill member %lu, not in view\n", member_id);
        return;
    }

    /* ignore updates for dead members */
#if 0
    if(!(g->view.member_states[member_id].is_member))
        return;
#endif

    SSG_DEBUG(g, "SWIM: member %lu DEAD (inc_nr=%u)\n", member_id, inc_nr);

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
    swim_ctx->member_inc_nrs[member_id] = inc_nr;

    /* add this update to recent update list so it will be piggybacked
     * on future protocol messages
     */
    swim_update.id = member_id;
    swim_update.status = SWIM_MEMBER_DEAD;
    swim_update.inc_nr = inc_nr;
    swim_add_recent_member_update(g, swim_update);
    /* have SSG apply the membership update */
    ssg_update.member = member_id;
    ssg_update.type = SSG_MEMBER_REMOVE;
    ssg_apply_membership_update(g, ssg_update);

    return;
}

static void swim_update_suspected_members(
    ssg_group_t *g, double susp_timeout)
{
    swim_context_t *swim_ctx = g->swim_ctx;
    double now = ABT_get_wtime();
    double susp_dur;
    swim_suspect_member_link_t *iter, *tmp;
    swim_suspect_member_link_t *suspect_list_p =
        (swim_suspect_member_link_t *)swim_ctx->suspect_list;

    LL_FOREACH_SAFE(suspect_list_p, iter, tmp)
    {
        susp_dur = now - iter->susp_start;
        if(susp_dur >= (susp_timeout / 1000.0))
        {
            /* if this member has exceeded its allowable suspicion timeout,
             * we mark it as dead
             */
            swim_kill_member(g, iter->member_id,
                swim_ctx->member_inc_nrs[iter->member_id]);
        }
    }

    return;
}

static void swim_add_recent_member_update(
    ssg_group_t *g, swim_member_update_t update)
{
    swim_context_t *swim_ctx = g->swim_ctx;
    swim_member_update_link_t *iter, *tmp;
    swim_member_update_link_t *update_link = NULL;
    swim_member_update_link_t **recent_update_list_p =
        (swim_member_update_link_t **)&(swim_ctx->recent_update_list);

    /* search and remove any recent updates corresponding to this member */
    LL_FOREACH_SAFE(*recent_update_list_p, iter, tmp)
    {
        if(iter->update.id == update.id)
        {
            LL_DELETE(*recent_update_list_p, iter);
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
    LL_APPEND(*recent_update_list_p, update_link);

    return;
}
#endif
