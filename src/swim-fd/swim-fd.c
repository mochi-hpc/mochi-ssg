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

#include "ssg.h"
#include "ssg-internal.h"
#include "swim-fd.h"
#include "swim-fd-internal.h"

typedef struct swim_suspect_member_link
{
    ssg_member_id_t member_id;
    swim_member_inc_nr_t inc_nr;
    double susp_start;
    struct swim_suspect_member_link *next;
} swim_suspect_member_link_t;

typedef struct swim_member_update_link
{
    swim_member_update_t update;
    int tx_count;
    struct swim_member_update_link *next;
} swim_member_update_link_t;

typedef struct swim_ssg_member_update_link
{
    ssg_member_update_t update;
    int tx_count;
    struct swim_ssg_member_update_link *next;
} swim_ssg_member_update_link_t;

/* SWIM protocol ABT ULT prototypes */
static void swim_prot_ult(
    void * t_arg);
static void swim_tick_ult(
    void * t_arg);

/* SWIM ping target selection prototypes */
static void swim_get_dping_target(
    ssg_group_t *group, ssg_member_id_t *target_id,
    swim_member_inc_nr_t *target_inc_nr, hg_addr_t *target_addr);
static void swim_get_iping_targets(
    ssg_group_t *group, ssg_member_id_t dping_target_id, int *num_targets,
    ssg_member_id_t *target_ids, hg_addr_t *target_addrs);
static void swim_shuffle_ping_target_list(
    swim_ping_target_list_t *list);

/* SWIM group membership update prototypes */
static void swim_process_suspect_member_update(
    ssg_group_t *group, ssg_member_id_t member_id,
    swim_member_inc_nr_t inc_nr);
static void swim_process_alive_member_update(
    ssg_group_t *group, ssg_member_id_t member_id,
    swim_member_inc_nr_t inc_nr);
static void swim_process_dead_member_update(
    ssg_group_t *group, ssg_member_id_t member_id,
    swim_member_inc_nr_t inc_nr);
static void swim_check_suspected_members(
    ssg_group_t *group, double susp_timeout);
static void swim_register_member_update(
    swim_context_t *swim_ctx, swim_member_update_t update);
static void swim_register_ssg_member_update(
    swim_context_t *swim_ctx, ssg_member_update_t update);

/***************************************
 *** SWIM protocol init and shutdown ***
 ***************************************/

int swim_init(
    ssg_group_t * group,
    margo_instance_id mid,
    int active)
{
    swim_context_t *swim_ctx;
    ssg_member_state_t *ms, *tmp_ms;
    int i;
    int ret;

    /* allocate structure for storing swim context */
    swim_ctx = malloc(sizeof(*swim_ctx));
    if (!swim_ctx) return(SSG_FAILURE);
    memset(swim_ctx, 0, sizeof(*swim_ctx));
    swim_ctx->mid = mid;
    swim_ctx->self_inc_nr = 0;
    swim_ctx->dping_target_id = SSG_MEMBER_ID_INVALID;
    for (i = 0; i < SWIM_MAX_SUBGROUP_SIZE; i++)
        swim_ctx->iping_target_ids[i] = SSG_MEMBER_ID_INVALID;
    margo_get_handler_pool(swim_ctx->mid, &swim_ctx->swim_pool);
    ABT_rwlock_create(&swim_ctx->swim_lock);

    swim_ctx->target_list.targets = malloc(group->view.size *
        sizeof(*swim_ctx->target_list.targets));
    if (swim_ctx->target_list.targets == NULL)
    {
        free(swim_ctx);
        return(SSG_FAILURE);
    }
    swim_ctx->target_list.nslots = swim_ctx->target_list.len = group->view.size;
    swim_ctx->target_list.dping_ndx = 0;
    i = 0;
    HASH_ITER(hh, group->view.member_map, ms, tmp_ms)
    {
        ms->swim_state.status = SWIM_MEMBER_ALIVE;
        ms->swim_state.inc_nr = 0;
        swim_ctx->target_list.targets[i] = ms;
        i++;
    }

    /* set protocol parameters */
    swim_ctx->prot_period_len = SWIM_DEF_PROTOCOL_PERIOD_LEN;
    swim_ctx->prot_susp_timeout = SWIM_DEF_SUSPECT_TIMEOUT;
    swim_ctx->prot_subgroup_sz = SWIM_DEF_SUBGROUP_SIZE;

    group->swim_ctx = swim_ctx;
    swim_register_ping_rpcs(group);

    if(active)
    {
        ret = ABT_thread_create(swim_ctx->swim_pool, swim_prot_ult, group,
            ABT_THREAD_ATTR_NULL, &(swim_ctx->prot_thread));
        if(ret != ABT_SUCCESS)
        {
            fprintf(stderr, "Error: unable to create SWIM protocol ULT.\n");
            free(swim_ctx->target_list.targets);
            free(swim_ctx);
            return(SSG_FAILURE);
        }
    }

    return(SSG_SUCCESS);
}

void swim_finalize(
    ssg_group_t * group)
{
    swim_context_t *swim_ctx = group->swim_ctx;
    swim_suspect_member_link_t **suspect_list_p =
        (swim_suspect_member_link_t **)&swim_ctx->suspect_list;
    swim_member_update_link_t **swim_update_list_p =
        (swim_member_update_link_t **)&swim_ctx->swim_update_list;
    swim_ssg_member_update_link_t **ssg_update_list_p =
        (swim_ssg_member_update_link_t **)&swim_ctx->ssg_update_list;
    swim_suspect_member_link_t *suspect_iter, *suspect_tmp;
    swim_member_update_link_t *swim_update_iter, *swim_update_tmp;
    swim_ssg_member_update_link_t *ssg_update_iter, *ssg_update_tmp;

    /* set shutdown flag so ULTs know to start wrapping up */
    ABT_rwlock_wrlock(swim_ctx->swim_lock);
    swim_ctx->shutdown_flag = 1;
    ABT_rwlock_unlock(swim_ctx->swim_lock);

    if(swim_ctx->prot_thread)
    {
        /* wait for the protocol ULT to terminate */
        ABT_thread_join(swim_ctx->prot_thread);
        ABT_thread_free(&(swim_ctx->prot_thread));
    }

    LL_FOREACH_SAFE(*suspect_list_p, suspect_iter, suspect_tmp)
    {
        LL_DELETE(*suspect_list_p, suspect_iter);
        free(suspect_iter);
    }
    LL_FOREACH_SAFE(*swim_update_list_p, swim_update_iter, swim_update_tmp)
    {
        LL_DELETE(*swim_update_list_p, swim_update_iter);
        free(swim_update_iter);
    }
    LL_FOREACH_SAFE(*ssg_update_list_p, ssg_update_iter, ssg_update_tmp)
    {
        LL_DELETE(*ssg_update_list_p, ssg_update_iter);
        free(ssg_update_iter);
    }

    ABT_rwlock_free(&swim_ctx->swim_lock);
    free(swim_ctx->target_list.targets);
    free(swim_ctx);
    group->swim_ctx = NULL;

    return;
}

/**************************
 *** SWIM protocol ULTs ***
 **************************/

static void swim_prot_ult(
    void * t_arg)
{
    ssg_group_t *group = (ssg_group_t *)t_arg;
    swim_context_t *swim_ctx;
    int i;
    int ret;

    assert(group != NULL);
    swim_ctx = group->swim_ctx;
    assert(swim_ctx != NULL);

    SSG_DEBUG(group, "SWIM protocol start " \
        "(period_len=%.4f, susp_timeout=%d, subgroup_size=%d)\n",
        swim_ctx->prot_period_len, swim_ctx->prot_susp_timeout,
        swim_ctx->prot_subgroup_sz);

    ABT_rwlock_rdlock(swim_ctx->swim_lock);
    while(!(swim_ctx->shutdown_flag))
    {
        ABT_rwlock_unlock(swim_ctx->swim_lock);

        /* spawn a ULT to run this tick */
        ret = ABT_thread_create(swim_ctx->swim_pool, swim_tick_ult, group,
            ABT_THREAD_ATTR_NULL, NULL);
        if(ret != ABT_SUCCESS)
        {
            fprintf(stderr, "Error: unable to create ULT for SWIM protocol tick\n");
        }

        /* sleep for a protocol period length */
        margo_thread_sleep(swim_ctx->mid, swim_ctx->prot_period_len);

        ABT_rwlock_wrlock(swim_ctx->swim_lock);

        /* cleanup state from previous period */
        if(swim_ctx->dping_target_id != SSG_MEMBER_ID_INVALID)
        {
            margo_addr_free(swim_ctx->mid, swim_ctx->dping_target_addr);
        }
        for(i = 0; i < swim_ctx->prot_subgroup_sz; i++)
        {
            if(swim_ctx->iping_target_ids[i] != SSG_MEMBER_ID_INVALID)
            {
                margo_addr_free(swim_ctx->mid, swim_ctx->iping_target_addrs[i]);
                swim_ctx->iping_target_ids[i] = SSG_MEMBER_ID_INVALID;
            }
            else
            {
                break;
            }
        }

    }
    ABT_rwlock_unlock(swim_ctx->swim_lock);

    SSG_DEBUG(group, "SWIM protocol shutdown\n");

    return;
}

static void swim_tick_ult(
    void * t_arg)
{
    ssg_group_t *group = (ssg_group_t *)t_arg;
    swim_context_t *swim_ctx;
    int i;
    int ret;

    assert(group != NULL);
    swim_ctx = group->swim_ctx;
    assert(swim_ctx != NULL);

    /* check status of any suspected members */
    swim_check_suspected_members(group, swim_ctx->prot_susp_timeout *
        swim_ctx->prot_period_len);

    /* check whether the ping target from the previous protocol tick
     * ever successfully acked a (direct/indirect) ping request
     */
    if((swim_ctx->dping_target_id != SSG_MEMBER_ID_INVALID) &&
        !(swim_ctx->ping_target_acked))
    {
        /* no response from direct/indirect pings, suspect this member */
        swim_process_suspect_member_update(group, swim_ctx->dping_target_id,
            swim_ctx->dping_target_inc_nr);
    }

    /* pick a random member from view to ping */
    swim_get_dping_target(group, &swim_ctx->dping_target_id,
        &swim_ctx->dping_target_inc_nr, &swim_ctx->dping_target_addr);
    if(swim_ctx->dping_target_id == SSG_MEMBER_ID_INVALID)
    {
        /* no available members, back out */
        SSG_DEBUG(group, "SWIM no group members available to dping\n");
        return;
    }

    /* TODO: calculate estimated RTT using sliding window of past RTTs */
    swim_ctx->dping_timeout = 250.0;

    /* kick off dping request ULT */
    swim_ctx->ping_target_acked = 0;
    ret = ABT_thread_create(swim_ctx->swim_pool, swim_dping_req_send_ult, group,
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
        swim_get_iping_targets(group, swim_ctx->dping_target_id,
            &iping_target_count, swim_ctx->iping_target_ids,
            swim_ctx->iping_target_addrs);
        if(iping_target_count == 0)
        {
            /* no available subgroup members, back out */
            SSG_DEBUG(group, "SWIM no subgroup members available to iping\n");
            return;
        }

        swim_ctx->iping_target_ndx = 0;
        for(i = 0; i < iping_target_count; i++)
        {
            ret = ABT_thread_create(swim_ctx->swim_pool, swim_iping_req_send_ult,
                group, ABT_THREAD_ATTR_NULL, NULL);
            if(ret != ABT_SUCCESS)
            {
                fprintf(stderr, "Error: unable to create ULT for SWIM iping send\n");
                return;
            }
        }
    }

    return;
}

/**********************************
 *** SWIM ping target selection ***
 **********************************/

static void swim_get_dping_target(
    ssg_group_t *group, ssg_member_id_t *target_id,
    swim_member_inc_nr_t *target_inc_nr, hg_addr_t *target_addr)
{
    swim_context_t *swim_ctx = group->swim_ctx;
    ssg_member_state_t *tmp_ms;
    hg_return_t hret;

    *target_id = SSG_MEMBER_ID_INVALID;
    ABT_rwlock_wrlock(group->lock);

    /* find dping target */
    while(swim_ctx->target_list.len > 0)
    {
        /* reshuffle member list after a complete traversal */
        if(swim_ctx->target_list.dping_ndx == swim_ctx->target_list.len)
        {
            swim_shuffle_ping_target_list(&swim_ctx->target_list);
            swim_ctx->target_list.dping_ndx = 0;
            continue;
        }

        /* pull next dping target using saved state */  
        tmp_ms = swim_ctx->target_list.targets[swim_ctx->target_list.dping_ndx++];
        assert(tmp_ms);

        /* skip dead members */
        if(tmp_ms->swim_state.status == SWIM_MEMBER_DEAD) continue;

        hret = margo_addr_dup(swim_ctx->mid, tmp_ms->addr, target_addr);
        if(hret != HG_SUCCESS)
        {
            ABT_rwlock_unlock(group->lock);
            return;
        }
        *target_id = tmp_ms->id;
        *target_inc_nr = tmp_ms->swim_state.inc_nr;

        break;
    }

    ABT_rwlock_unlock(group->lock);

    return;
}

static void swim_get_iping_targets(
    ssg_group_t *group, ssg_member_id_t dping_target_id, int *num_targets,
    ssg_member_id_t *target_ids, hg_addr_t *target_addrs)
{
    swim_context_t *swim_ctx = group->swim_ctx;
    int max_targets = *num_targets;
    int iping_target_count = 0;
    int i;
    int r_start, r_ndx;
    ssg_member_state_t *tmp_ms;
    hg_return_t hret;

    *num_targets = 0;

    ABT_rwlock_rdlock(group->lock);

    if (swim_ctx->target_list.len == 0)
    {
        ABT_rwlock_unlock(group->lock);
        return;
    }

    /* pick random index in the target list, and pull out a set of iping
     * targets starting from that index
     */
    r_start = rand() % swim_ctx->target_list.len;
    i = 0;
    while (iping_target_count < max_targets)
    {
        r_ndx = (r_start + i) % swim_ctx->target_list.len;
        /* if we've iterated through the entire target list, stop */
        if ((i > 0 ) && (r_ndx == r_start)) break;

        tmp_ms = swim_ctx->target_list.targets[r_ndx];

        /* do not select the dping target or dead members */
        if ((tmp_ms->id == dping_target_id) || 
            (tmp_ms->swim_state.status == SWIM_MEMBER_DEAD))
        {
            i++;
            continue;
        }

        hret = margo_addr_dup(swim_ctx->mid, tmp_ms->addr,
            &target_addrs[iping_target_count]);
        if(hret != HG_SUCCESS)
        {
            ABT_rwlock_unlock(group->lock);
            return;
        }
        target_ids[iping_target_count] = tmp_ms->id;
        iping_target_count++;
        i++;
    }

    ABT_rwlock_unlock(group->lock);

    *num_targets = iping_target_count;

    return;
}

static void swim_shuffle_ping_target_list(
    swim_ping_target_list_t *list)
{
    unsigned int i, r; 
    ssg_member_state_t *tmp_ms;

    /* filter and drop dead members */
    for (i = 0; i < list->len; i++)
    {
        if (list->targets[i]->swim_state.status == SWIM_MEMBER_DEAD)
        {
            list->len--;
            memcpy(&list->targets[i], &list->targets[i+1],
                (list->len-i)*sizeof(*list->targets));
        }
    }

    if (list->len <= 1) return;

    /* run fisher-yates shuffle over list of target members */
    for (i = list->len - 1; i > 0; i--)
    {
        r = rand() % (i + 1);
        tmp_ms = list->targets[r];
        list->targets[r] = list->targets[i];
        list->targets[i] = tmp_ms;
    }

    return;
}

/*************************************
 *** SWIM group membership updates ***
 *************************************/

static void swim_process_suspect_member_update(
    ssg_group_t *group, ssg_member_id_t member_id, swim_member_inc_nr_t inc_nr)
{
    swim_context_t *swim_ctx = group->swim_ctx;
    ssg_member_state_t *ms = NULL;
    swim_member_status_t prev_status;
    swim_suspect_member_link_t *iter, *tmp;
    swim_suspect_member_link_t *suspect_link = NULL;
    swim_suspect_member_link_t **suspect_list_p =
        (swim_suspect_member_link_t **)&swim_ctx->suspect_list;
    swim_member_update_t update;

    /* if there is no suspicion timeout, just kill the member */
    if(swim_ctx->prot_susp_timeout == 0)
    {
        swim_process_dead_member_update(group, member_id, inc_nr);
        return;
    }

    ABT_rwlock_wrlock(group->lock);

    HASH_FIND(hh, group->view.member_map, &member_id, sizeof(member_id), ms);
    if(!ms ||
       ((ms->swim_state.status == SWIM_MEMBER_SUSPECT) && (inc_nr <= ms->swim_state.inc_nr)) ||
       ((ms->swim_state.status == SWIM_MEMBER_ALIVE) && (inc_nr < ms->swim_state.inc_nr)))
    {
        /* ignore updates for:
         *    - members not in the view (this includes DEAD members)
         *    - members that are SUSPECT in a gte incarnation number
         *    - members that are ALIVE in a gt incarnation number
         */ 
        ABT_rwlock_unlock(group->lock);
        return;
    }
    prev_status = ms->swim_state.status;

    /* update SWIM membership state */
    ms->swim_state.inc_nr = inc_nr;
    ms->swim_state.status = SWIM_MEMBER_SUSPECT;

    ABT_rwlock_unlock(group->lock);

    ABT_rwlock_wrlock(swim_ctx->swim_lock);
    if(prev_status == SWIM_MEMBER_SUSPECT)
    {
        /* find the suspect link for an already suspected member */
        LL_FOREACH_SAFE(*suspect_list_p, iter, tmp)
        {
            if(iter->member_id == member_id)
            {
                LL_DELETE(*suspect_list_p, iter);
                suspect_link = iter;
            }
        }
        assert(suspect_link); /* better be there */
    }
    else
    {
        /* if this member is not already on the suspect list,
         * allocate a link for it
         */
        suspect_link = malloc(sizeof(*suspect_link));
        if (!suspect_link)
        {
            ABT_rwlock_unlock(swim_ctx->swim_lock);
            return;
        }
        memset(suspect_link, 0, sizeof(*suspect_link));
        suspect_link->member_id = member_id;
    }
    suspect_link->susp_start = ABT_get_wtime();
    suspect_link->inc_nr = inc_nr;

    /* add to end of suspect list */
    LL_APPEND(*suspect_list_p, suspect_link);
    ABT_rwlock_unlock(swim_ctx->swim_lock);

    /* register this update so it's piggybacked on future SWIM messages */
    update.id = member_id;
    update.state.status = SWIM_MEMBER_SUSPECT;
    update.state.inc_nr = inc_nr;
    swim_register_member_update(swim_ctx, update);

    SSG_DEBUG(group, "SWIM member %lu SUSPECT (inc_nr=%u)\n", member_id, inc_nr);

    return;
}

static void swim_process_alive_member_update(
    ssg_group_t *group, ssg_member_id_t member_id, swim_member_inc_nr_t inc_nr)
{
    swim_context_t *swim_ctx = group->swim_ctx;
    ssg_member_state_t *ms = NULL;
    swim_member_status_t prev_status;
    swim_suspect_member_link_t *iter, *tmp;
    swim_suspect_member_link_t **suspect_list_p =
        (swim_suspect_member_link_t **)&swim_ctx->suspect_list;
    swim_member_update_t update;

    ABT_rwlock_wrlock(group->lock);

    HASH_FIND(hh, group->view.member_map, &member_id, sizeof(member_id), ms);
    if(!ms ||
       (inc_nr <= ms->swim_state.inc_nr))
    {
        /* ignore updates for:
         *    - members not in the view (this includes DEAD members)
         *    - members (ALIVE or SUSPECT) that have a gte incarnation number
         */
        ABT_rwlock_unlock(group->lock);
        return;
    }
    prev_status = ms->swim_state.status;

    /* update SWIM membership state */
    ms->swim_state.inc_nr = inc_nr;
    ms->swim_state.status = SWIM_MEMBER_ALIVE;

    ABT_rwlock_unlock(group->lock);

    if(prev_status == SWIM_MEMBER_SUSPECT)
    {
        ABT_rwlock_wrlock(swim_ctx->swim_lock);
        /* if member is suspected, remove from suspect list */
        LL_FOREACH_SAFE(*suspect_list_p, iter, tmp)
        {
            if(iter->member_id == member_id)
            {
                LL_DELETE(*suspect_list_p, iter);
                free(iter);
                break;
            }
        }
        ABT_rwlock_unlock(swim_ctx->swim_lock);
    }

    /* register this update so it's piggybacked on future SWIM messages */
    update.id = member_id;
    update.state.status = SWIM_MEMBER_ALIVE;
    update.state.inc_nr = inc_nr;
    swim_register_member_update(swim_ctx, update);

    SSG_DEBUG(group, "SWIM member %lu ALIVE (inc_nr=%u)\n", member_id, inc_nr);

    return;
}

static void swim_process_dead_member_update(
    ssg_group_t *group, ssg_member_id_t member_id, swim_member_inc_nr_t inc_nr)
{
    swim_context_t *swim_ctx = group->swim_ctx;
    ssg_member_state_t *ms = NULL;
    swim_member_status_t prev_status;
    swim_suspect_member_link_t *iter, *tmp;
    swim_suspect_member_link_t **suspect_list_p =
        (swim_suspect_member_link_t **)&swim_ctx->suspect_list;
    swim_member_update_t swim_update;
    ssg_member_update_t ssg_update;

    ABT_rwlock_wrlock(group->lock);

    HASH_FIND(hh, group->view.member_map, &member_id, sizeof(member_id), ms);
    if(!ms)
    {
        /* ignore updates for:
         *    - members not in the view (this includes already DEAD members)
         */
        ABT_rwlock_unlock(group->lock);
        return;
    }
    prev_status = ms->swim_state.status;

    /* update SWIM membership state */
    ms->swim_state.inc_nr = inc_nr;
    ms->swim_state.status = SWIM_MEMBER_DEAD;

    ABT_rwlock_unlock(group->lock);

    if(prev_status == SWIM_MEMBER_SUSPECT)
    {
        ABT_rwlock_wrlock(swim_ctx->swim_lock);
        LL_FOREACH_SAFE(*suspect_list_p, iter, tmp)
        {
            if(iter->member_id == member_id)
            {
                /* remove member from suspect list */
                LL_DELETE(*suspect_list_p, iter);
                free(iter);
                break;
            }
        }
        ABT_rwlock_unlock(swim_ctx->swim_lock);
    }

    /* register this update so it's piggybacked on future SWIM messages */
    swim_update.id = member_id;
    swim_update.state.status = SWIM_MEMBER_DEAD;
    swim_update.state.inc_nr = inc_nr;
    swim_register_member_update(swim_ctx, swim_update);

    SSG_DEBUG(group, "SWIM member %lu DEAD (inc_nr=%u)\n", member_id, inc_nr);

    /* have SSG apply this member failure update */
    ssg_update.type = SSG_MEMBER_DIED;
    ssg_update.u.member_id = member_id;
    ssg_apply_member_updates(group, &ssg_update, 1);

    return;
}

static void swim_check_suspected_members(
    ssg_group_t *group, double susp_timeout)
{
    swim_context_t *swim_ctx = group->swim_ctx;
    double now = ABT_get_wtime();
    double susp_dur;
    swim_suspect_member_link_t *iter, *tmp;
    swim_suspect_member_link_t **suspect_list_p =
        (swim_suspect_member_link_t **)&swim_ctx->suspect_list;

    ABT_rwlock_rdlock(swim_ctx->swim_lock);
    LL_FOREACH_SAFE(*suspect_list_p, iter, tmp)
    {
        susp_dur = now - iter->susp_start;
        if(susp_dur >= (susp_timeout / 1000.0))
        {
            /* if this member has exceeded its allowable suspicion timeout,
             * we mark it as dead
             */
            ABT_rwlock_unlock(swim_ctx->swim_lock);
            swim_process_dead_member_update(group, iter->member_id, iter->inc_nr);
            ABT_rwlock_rdlock(swim_ctx->swim_lock);
        }
    }
    ABT_rwlock_unlock(swim_ctx->swim_lock);

    return;
}

static void swim_register_member_update(
    swim_context_t *swim_ctx, swim_member_update_t update)
{
    swim_member_update_link_t *iter, *tmp;
    swim_member_update_link_t *update_link = NULL;
    swim_member_update_link_t **swim_update_list_p =
        (swim_member_update_link_t **)&swim_ctx->swim_update_list;

    ABT_rwlock_wrlock(swim_ctx->swim_lock);

    /* search and remove any recent updates corresponding to this member */
    LL_FOREACH_SAFE(*swim_update_list_p, iter, tmp)
    {
        if(iter->update.id == update.id)
        {
            LL_DELETE(*swim_update_list_p, iter);
            update_link = iter;
        }
    }

    if(update_link == NULL)
    {
        update_link = malloc(sizeof(*update_link));
        assert(update_link);
    }

    /* set update */
    memcpy(&update_link->update, &update, sizeof(update));
    update_link->tx_count = 0;

    /* add to recent update list */
    LL_APPEND(*swim_update_list_p, update_link);

    ABT_rwlock_unlock(swim_ctx->swim_lock);

    return;
}

static void swim_register_ssg_member_update(
    swim_context_t *swim_ctx, ssg_member_update_t update)
{
    swim_ssg_member_update_link_t *iter, *tmp;
    swim_ssg_member_update_link_t *update_link = NULL;
    swim_ssg_member_update_link_t **ssg_update_list_p =
        (swim_ssg_member_update_link_t **)&swim_ctx->ssg_update_list;
    int match = 0;

    ABT_rwlock_wrlock(swim_ctx->swim_lock);

    /* ignore updates we already are aware of */
    LL_FOREACH_SAFE(*ssg_update_list_p, iter, tmp)
    {
        if(iter->update.type == update.type)
        {
            if(update.type == SSG_MEMBER_JOINED)
            {
                if(strcmp(iter->update.u.member_addr_str, update.u.member_addr_str) == 0)
                    match = 1;
            }
            else /* update.type == SSG_MEMBER_DIED || SSG_MEMBER_LEFT */
            {
                if(iter->update.u.member_id == update.u.member_id)
                    match = 1;
            }

            if (match)
            {
                ABT_rwlock_unlock(swim_ctx->swim_lock);
                return;
            }
        }
    }

    /* allocate and initialize this update */
    update_link = malloc(sizeof(*update_link));
    assert(update_link);
    update_link->update.type = update.type;
    if(update.type == SSG_MEMBER_JOINED)
    {
        /* for join updates, dup the update address string */
        update_link->update.u.member_addr_str = strdup(update.u.member_addr_str);
    }
    update_link->tx_count = 0;

    /* add to recent update list */
    LL_APPEND(*ssg_update_list_p, update_link);

    ABT_rwlock_unlock(swim_ctx->swim_lock);

    return;
}

void swim_retrieve_member_updates(
    ssg_group_t * group,
    swim_member_update_t * updates,
    hg_size_t * update_count)
{
    swim_member_update_link_t *iter, *tmp;
    swim_member_update_link_t **swim_update_list_p =
        (swim_member_update_link_t **)&group->swim_ctx->swim_update_list;
    hg_size_t i = 0;
    hg_size_t max_updates = *update_count;

    ABT_rwlock_rdlock(group->swim_ctx->swim_lock);
    LL_FOREACH_SAFE(*swim_update_list_p, iter, tmp)
    {
        if(i == max_updates)
            break;

        memcpy(&updates[i], &iter->update, sizeof(iter->update));

        /* remove this update if it has been piggybacked enough */
        iter->tx_count++;
        if(iter->tx_count == SWIM_MAX_PIGGYBACK_TX_COUNT)
        {
            LL_DELETE(*swim_update_list_p, iter);
            free(iter);
        }
        i++;
    }
    ABT_rwlock_unlock(group->swim_ctx->swim_lock);
    *update_count = i;

    return;
}

void swim_retrieve_ssg_member_updates(
    ssg_group_t * group,
    ssg_member_update_t * updates,
    hg_size_t * update_count)
{
    swim_ssg_member_update_link_t *iter, *tmp;
    swim_ssg_member_update_link_t **ssg_update_list_p =
        (swim_ssg_member_update_link_t **)&group->swim_ctx->ssg_update_list;
    hg_size_t i = 0;
    hg_size_t max_updates = *update_count;

    ABT_rwlock_rdlock(group->swim_ctx->swim_lock);
    LL_FOREACH_SAFE(*ssg_update_list_p, iter, tmp)
    {
        if(i == max_updates)
            break;

        memcpy(&updates[i], &iter->update, sizeof(iter->update));

        /* remove this update if it has been piggybacked enough */
        iter->tx_count++;
        if(iter->tx_count == SWIM_MAX_PIGGYBACK_TX_COUNT)
        {
            LL_DELETE(*ssg_update_list_p, iter);
            if(iter->update.type == SSG_MEMBER_JOINED)
                free(iter->update.u.member_addr_str);
            free(iter);
        }
        i++;
    }
    ABT_rwlock_unlock(group->swim_ctx->swim_lock);
    *update_count = i;

    return;
}

void swim_apply_member_updates(
    ssg_group_t * group,
    swim_member_update_t * updates,
    hg_size_t update_count)
{
    hg_size_t i;

    for(i = 0; i < update_count; i++)
    {
        switch(updates[i].state.status)
        {
            case SWIM_MEMBER_ALIVE:
                /* ignore alive updates for self */
                if(updates[i].id != group->ssg_inst->self_id)
                    swim_process_alive_member_update(group, updates[i].id,
                        updates[i].state.inc_nr);
                break;
            case SWIM_MEMBER_SUSPECT:
                if(updates[i].id == group->ssg_inst->self_id)
                {
                    /* increment our incarnation number if we are suspected
                     * in the current incarnation
                     */
                    if(updates[i].state.inc_nr == group->swim_ctx->self_inc_nr)
                    {
                        group->swim_ctx->self_inc_nr++;
                        SSG_DEBUG(group, "SWIM self SUSPECT received (new inc_nr=%u)\n",
                            group->swim_ctx->self_inc_nr);
                    }
                }
                else
                {
                    swim_process_suspect_member_update(group, updates[i].id,
                        updates[i].state.inc_nr);
                }
                break;
            case SWIM_MEMBER_DEAD:
                /* if we get an update that we are dead, just shut down */
                if(updates[i].id == group->ssg_inst->self_id)
                {
                    SSG_DEBUG(group, "SWIM self confirmed DEAD (inc_nr=%u)\n",
                        updates[i].state.inc_nr);
                    swim_finalize(group);
                    return;
                }
                else
                {
                    swim_process_dead_member_update(group, updates[i].id,
                        updates[i].state.inc_nr);
                }
                break;
            default:
                fprintf(stderr, "Error: invalid SWIM member update [%lu,%d]\n", group->ssg_inst->self_id,updates[i].state.status);
                break;
        }
    }

    return;
}

int swim_apply_ssg_member_update(
    ssg_group_t * group,
    ssg_member_state_t * ms,
    ssg_member_update_t update)
{
    swim_context_t *swim_ctx;

    assert(group != NULL);
    swim_ctx = group->swim_ctx;
    assert(swim_ctx != NULL);

    switch(update.type)
    {
        case SSG_MEMBER_JOINED:
            /* initialize SWIM member state */
            ABT_rwlock_wrlock(group->lock);
            ms->swim_state.status = SWIM_MEMBER_ALIVE;
            ms->swim_state.inc_nr = 0;

            /* add to target list */
            if (swim_ctx->target_list.len == swim_ctx->target_list.nslots)
            {
                /* realloc target list, use fixed incr for now */
                /* XXX constants bad... */
                swim_ctx->target_list.targets = realloc(swim_ctx->target_list.targets,
                    (swim_ctx->target_list.len + 10) * sizeof(*swim_ctx->target_list.targets));
                if (!swim_ctx->target_list.targets) return SSG_FAILURE;
                swim_ctx->target_list.nslots += 10;
            }
            swim_ctx->target_list.targets[swim_ctx->target_list.len++] = ms;
            ABT_rwlock_unlock(group->lock);

            break;
        case SSG_MEMBER_LEFT:
        case SSG_MEMBER_DIED:
            /* just mark as dead, this member will be cleaned from ping target
             * list on the next re-shuffle
             */
            ABT_rwlock_wrlock(group->lock);
            ms->swim_state.status = SWIM_MEMBER_DEAD;
            ABT_rwlock_unlock(group->lock);

            break;
        default:
            SSG_DEBUG(group, "Warning: Invalid SSG update type given to SWIM\n");
            return(SSG_FAILURE);
    }

    /* register this SSG update with SWIM so it is gossiped */
    swim_register_ssg_member_update(swim_ctx, update);

    return(SSG_SUCCESS);
}
