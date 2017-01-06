/*
 * (C) 2016 The University of Chicago
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
#include "swim-fd.h"
#include "swim-fd-internal.h"

/* SWIM ABT ULT prototypes */
static void swim_prot_ult(
    void *t_arg);
static void swim_tick_ult(
    void *t_arg);

swim_context_t *swim_init(
    margo_instance_id mid,
    ssg_t swim_group,
    int active)
{
    swim_context_t *swim_ctx;
    int ret;

    /* TODO: this may end up being identical across many processes => hotspots */
    //srand(time(NULL));

    /* allocate structure for storing swim context */
    swim_ctx = malloc(sizeof(*swim_ctx));
    assert(swim_ctx);
    memset(swim_ctx, 0, sizeof(*swim_ctx));

    /* initialize swim state */
    swim_ctx->mid = mid;
    swim_ctx->hg_ctx = margo_get_context(mid);
    swim_ctx->prot_pool = *margo_get_handler_pool(mid);
    swim_ctx->group = swim_group;
    swim_ctx->ping_target = SWIM_MEMBER_ID_UNKNOWN;

#if 0
    /* initialize membership context */
    swim_init_membership_view(swim_ctx);
#endif

    /* set protocol parameters */
    swim_ctx->prot_period_len = SWIM_DEF_PROTOCOL_PERIOD_LEN;
    swim_ctx->prot_susp_timeout = SWIM_DEF_SUSPECT_TIMEOUT;
    swim_ctx->prot_subgroup_sz = SWIM_DEF_SUBGROUP_SIZE;

#if 0
    swim_register_ping_rpcs(margo_get_class(mid), swim_ctx);
#endif

    if(active)
    {
        ret = ABT_thread_create(swim_ctx->prot_pool, swim_prot_ult, swim_ctx,
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
    swim_context_t *swim_ctx = (swim_context_t *)t_arg;

    while(!(swim_ctx->shutdown_flag))
    {
        /* spawn a ULT to run this tick */
        ret = ABT_thread_create(swim_ctx->prot_pool, swim_tick_ult, swim_ctx,
            ABT_THREAD_ATTR_NULL, NULL);
        if(ret != ABT_SUCCESS)
        {
            fprintf(stderr, "Error: unable to create ULT for SWIM protocol tick\n");
        }

        /* sleep for a protocol period length */
        margo_thread_sleep(swim_ctx->mid, swim_ctx->prot_period_len);
    }

    return;
}

static void swim_tick_ult(
    void *t_arg)
{
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

#if 0
    free(swim_ctx->membership_view);
#endif
    free(swim_ctx);

    return;
}

