/*
 * Copyright (c) 2016 UChicago Argonne, LLC
 *
 * See COPYRIGHT in top-level directory.
 */

#include <ssg-config.h>

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <assert.h>
#include <time.h>

#include <ssg.h>
#include "ssg-internal.h"
#include "swim-fd.h"
#include "swim-fd-internal.h"
#include "utlist.h"

typedef struct swim_suspect_member_link
{
    int member_rank;
    double susp_start;
    swim_inc_nr_t inc_nr;
    struct swim_suspect_member_link *next;
} swim_suspect_member_link_t;

typedef struct swim_member_update_link
{
    int member_rank;
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
    ssg_t s, int member_rank, swim_inc_nr_t inc_nr);
static void swim_unsuspect_member(
    ssg_t s, int member_rank, swim_inc_nr_t inc_nr);
static void swim_kill_member(
    ssg_t s, int member_rank, swim_inc_nr_t inc_nr);
static void swim_update_suspected_members(
    ssg_t s, double susp_timeout);
static int swim_get_rand_group_member(
    ssg_t s, int *member_rank);
static int swim_get_rand_group_member_set(
    ssg_t s, int *member_ranks, int num_members, int excluded_rank);

/******************************************************
 * SWIM protocol init/finalize functions and ABT ULTs *
 ******************************************************/

swim_context_t *swim_init(
    ssg_t s,
    int active)
{
    swim_context_t *swim_ctx;
    int i, ret;

    assert(s != SSG_NULL);

    /* seed RNG with time+rank combination to avoid identical seeds */
    srand(time(NULL) + s->view.self_rank);

    /* allocate structure for storing swim context */
    swim_ctx = malloc(sizeof(*swim_ctx));
    assert(swim_ctx);
    memset(swim_ctx, 0, sizeof(*swim_ctx));

    /* initialize swim context */
    swim_ctx->prot_pool = *margo_get_handler_pool(s->mid);
    swim_ctx->ping_target = SSG_MEMBER_RANK_UNKNOWN;
    for(i = 0; i < SWIM_MAX_SUBGROUP_SIZE; i++)
        swim_ctx->subgroup_members[i] = SSG_MEMBER_RANK_UNKNOWN;

    swim_ctx->member_inc_nrs = malloc(s->view.group_size *
        sizeof(*(swim_ctx->member_inc_nrs)));
    assert(swim_ctx->member_inc_nrs);
    memset(swim_ctx->member_inc_nrs, 0, s->view.group_size *
        sizeof(*(swim_ctx->member_inc_nrs)));

    /* set protocol parameters */
    swim_ctx->prot_period_len = SWIM_DEF_PROTOCOL_PERIOD_LEN;
    swim_ctx->prot_susp_timeout = SWIM_DEF_SUSPECT_TIMEOUT;
    swim_ctx->prot_subgroup_sz = SWIM_DEF_SUBGROUP_SIZE;

    swim_register_ping_rpcs(s);

    if(active)
    {
        ret = ABT_thread_create(swim_ctx->prot_pool, swim_prot_ult, s,
            ABT_THREAD_ATTR_NULL, &(swim_ctx->prot_thread));
        if(ret != ABT_SUCCESS)
        {
            fprintf(stderr, "Error: unable to create SWIM protocol ULT.\n");
            return(NULL);
        }
    }

    return(swim_ctx);
}

static void swim_prot_ult(
    void *t_arg)
{
    int ret;
    ssg_t s = (ssg_t)t_arg;

    assert(s != SSG_NULL);

    SSG_DEBUG(s, "swim protocol start\n");
    while(!(s->swim_ctx->shutdown_flag))
    {
        /* spawn a ULT to run this tick */
        ret = ABT_thread_create(s->swim_ctx->prot_pool, swim_tick_ult, s,
            ABT_THREAD_ATTR_NULL, NULL);
        if(ret != ABT_SUCCESS)
        {
            fprintf(stderr, "Error: unable to create ULT for SWIM protocol tick\n");
        }

        /* sleep for a protocol period length */
        margo_thread_sleep(s->mid, s->swim_ctx->prot_period_len);
    }
    SSG_DEBUG(s, "swim protocol shutdown\n");

    return;
}

static void swim_tick_ult(
    void *t_arg)
{
    int i;
    int ret;
    swim_context_t *swim_ctx;
    ssg_t s = (ssg_t)t_arg;

    assert(s != SSG_NULL);
    swim_ctx = s->swim_ctx;
    assert(swim_ctx != NULL);

    /* update status of any suspected members */
    swim_update_suspected_members(s, swim_ctx->prot_susp_timeout *
        swim_ctx->prot_period_len);

    /* check whether the ping target from the previous protocol tick
     * ever successfully acked a (direct/indirect) ping request
     */
    if((swim_ctx->ping_target != SSG_MEMBER_RANK_UNKNOWN) &&
        !(swim_ctx->ping_target_acked))
    {
        /* no response from direct/indirect pings, suspect this member */
        swim_suspect_member(s, swim_ctx->ping_target, swim_ctx->ping_target_inc_nr);
    }

    /* pick a random member from view and ping */
    if(swim_get_rand_group_member(s, &(swim_ctx->ping_target)) == 0)
    {
        /* no available members, back out */
        SSG_DEBUG(s, "no group members available to dping\n");
        return;
    }

    /* TODO: calculate estimated RTT using sliding window of past RTTs */
    swim_ctx->dping_timeout = 250.0;

    /* kick off dping request ULT */
    swim_ctx->ping_target_inc_nr = swim_ctx->member_inc_nrs[swim_ctx->ping_target];
    swim_ctx->ping_target_acked = 0;
    ret = ABT_thread_create(swim_ctx->prot_pool, swim_dping_send_ult, s,
        ABT_THREAD_ATTR_NULL, NULL);
    if(ret != ABT_SUCCESS)
    {
        fprintf(stderr, "Error: unable to create ULT for SWIM dping send\n");
        return;
    }

    /* sleep for an RTT and wait for an ack for this dping req */
    margo_thread_sleep(s->mid, swim_ctx->dping_timeout);

    /* if we don't hear back from the target after an RTT, kick off
     * a set of indirect pings to a subgroup of group members
     */
    if(!(swim_ctx->ping_target_acked))
    {
        /* get a random subgroup of members to send indirect pings to */
        int this_subgroup_sz = swim_get_rand_group_member_set(s,
            swim_ctx->subgroup_members, swim_ctx->prot_subgroup_sz,
            swim_ctx->ping_target);
        if(this_subgroup_sz == 0)
        {
            /* no available subgroup members, back out */
            SSG_DEBUG(s, "no subgroup members available to iping\n");
            return;
        }

        for(i = 0; i < this_subgroup_sz; i++)
        {
            ret = ABT_thread_create(swim_ctx->prot_pool, swim_iping_send_ult, s,
                ABT_THREAD_ATTR_NULL, NULL);
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

    free(swim_ctx->member_inc_nrs);
    free(swim_ctx);

    return;
}

/*******************************************
 * SWIM group membership utility functions *
 *******************************************/

static void swim_suspect_member(ssg_t s, int member_rank, swim_inc_nr_t inc_nr)
{
    swim_context_t *swim_ctx = s->swim_ctx;
    swim_suspect_member_link_t *iter, *tmp;
    swim_suspect_member_link_t *suspect_link = NULL;
    swim_suspect_member_link_t **suspect_list_p =
        (swim_suspect_member_link_t **)&(swim_ctx->suspect_list);

    /* ignore members already confirmed as dead */
    if(!(s->view.member_states[member_rank].is_member))
        return;

    /* if there is no suspicion timeout, just kill the member */
    if(swim_ctx->prot_susp_timeout == 0)
    {
        swim_kill_member(s, member_rank, inc_nr);
        return;
    }

    /* determine if this member is already suspected */
    LL_FOREACH_SAFE(*suspect_list_p, iter, tmp)
    {
        if(iter->member_rank == member_rank)
        {
            if(inc_nr <= iter->inc_nr)
            {
                /* ignore a suspicion in an incarnation number less than
                 * or equal to the current suspicion's incarnation
                 */
                return;
            }

            /* otherwise, we have a suspicion in a more recent incarnation --
             * remove the current suspicion so we can update it
             */
            LL_DELETE(*suspect_list_p, iter);
            suspect_link = iter;
        }
    }

    SSG_DEBUG(s, "swim member %d SUSPECT (inc_nr=%d)\n", member_rank, inc_nr);

    if(suspect_link == NULL)
    {
        /* if this member is not already on the suspect list,
         * allocate a link for it
         */
        suspect_link = malloc(sizeof(*suspect_link));
        assert(suspect_link);
        memset(suspect_link, 0, sizeof(*suspect_link));
        suspect_link->member_rank = member_rank;
    }

    suspect_link->susp_start = ABT_get_wtime();
    suspect_link->inc_nr = inc_nr;

    /* add to end of suspect list */
    LL_APPEND(*suspect_list_p, suspect_link);

    /* update swim membership state */
    swim_ctx->member_inc_nrs[member_rank] = inc_nr;

#if 0
    /* add this update to recent update list so it will be piggybacked
     * on future protocol messages
     */
    swim_add_recent_member_update(swim_ctx, member);
#endif

    return;
}

static void swim_unsuspect_member(ssg_t s, int member_rank, swim_inc_nr_t inc_nr)
{
    swim_context_t *swim_ctx = s->swim_ctx;
    swim_suspect_member_link_t *iter, *tmp;
    swim_suspect_member_link_t **suspect_list_p =
        (swim_suspect_member_link_t **)&(swim_ctx->suspect_list);

    /* ignore members already confirmed as dead */
    if(!(s->view.member_states[member_rank].is_member))
        return;

    SSG_DEBUG(s, "swim member %d ALIVE (inc_nr=%d)\n", member_rank, inc_nr);

    /* remove this member from the suspect list */
    LL_FOREACH_SAFE(*suspect_list_p, iter, tmp)
    {
        if(iter->member_rank == member_rank)
        {
            LL_DELETE(*suspect_list_p, iter);
            free(iter);
            break;
        }
    }

    /* update swim membership state */
    swim_ctx->member_inc_nrs[member_rank] = inc_nr;

#if 0
    /* add this update to recent update list so it will be piggybacked
     * on future protocol messages
     */
    swim_add_recent_member_update(swim_ctx, member);
#endif

    return;
}

static void swim_kill_member(ssg_t s, int member_rank, swim_inc_nr_t inc_nr)
{
    swim_context_t *swim_ctx = s->swim_ctx;
    swim_suspect_member_link_t *iter, *tmp;
    swim_suspect_member_link_t **suspect_list_p =
        (swim_suspect_member_link_t **)&(swim_ctx->suspect_list);

    /* ignore members already confirmed as dead */
    if(!(s->view.member_states[member_rank].is_member))
        return;

    SSG_DEBUG(s, "swim member %d DEAD (inc_nr=%d)\n", member_rank, inc_nr);

    LL_FOREACH_SAFE(*suspect_list_p, iter, tmp)
    {
        if(iter->member_rank == member_rank)
        {
            /* remove member from suspect list */
            LL_DELETE(*suspect_list_p, iter);
            free(iter);
            break;
        }
    }

    /* update swim membership state */
    swim_ctx->member_inc_nrs[member_rank] = inc_nr;

    /* TODO: some sort of callback to ssg to do something more elaborate? */
    s->view.member_states[member_rank].is_member = 0;

#if 0
    /* add this update to recent update list so it will be piggybacked
     * on future protocol messages
     */
    swim_add_recent_member_update(swim_ctx, member);
#endif

    return;
}

static void swim_update_suspected_members(ssg_t s, double susp_timeout)
{
    swim_context_t *swim_ctx = s->swim_ctx;
    double now = ABT_get_wtime();
    double susp_dur;
    swim_suspect_member_link_t *iter, *tmp;
    swim_suspect_member_link_t **suspect_list_p =
        (swim_suspect_member_link_t **)&(swim_ctx->suspect_list);

    LL_FOREACH_SAFE(*suspect_list_p, iter, tmp)
    {
        susp_dur = now - iter->susp_start;
        if(susp_dur >= (susp_timeout / 1000))
        {
            /* if this member has exceeded its allowable suspicion timeout,
             * we mark it as dead
             */
            swim_kill_member(s, iter->member_rank, iter->inc_nr);
        }
    }

    return;
}

static int swim_get_rand_group_member(ssg_t s, int *member_rank)
{
    int ret = swim_get_rand_group_member_set(s, member_rank, 1,
        SSG_MEMBER_RANK_UNKNOWN);

    return(ret);
}

static int swim_get_rand_group_member_set(ssg_t s, int *member_ranks,
    int num_members, int excluded_rank)
{
    int i, rand_ndx = 0;
    int rand_member;
    int avail_members = s->view.group_size - 1;

    if(excluded_rank != SSG_MEMBER_RANK_UNKNOWN)
        avail_members--;

    /* TODO: what data structure could we use to avoid looping to look
     * for a set of random ranks
     */
    do
    {
        rand_member = rand() % s->view.group_size;
        if(rand_member == s->view.self_rank || rand_member == excluded_rank)
            continue;

        if(!(s->view.member_states[rand_member].is_member))
        {
            avail_members--;
            continue;
        }

        /* make sure there aren't duplicates */
        for(i = 0; i < rand_ndx; i++)
        {
            if(rand_member == member_ranks[i])
                break;
        }
        if(i != rand_ndx)
            continue;

        member_ranks[rand_ndx++] = rand_member;
        avail_members--;
    } while((rand_ndx < num_members) && (avail_members > 0));

    return(rand_ndx);
}
