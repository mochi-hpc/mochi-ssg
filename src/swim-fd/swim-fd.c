/*
 * (C) 2016 The University of Chicago
 * 
 * See COPYRIGHT in top-level directory.
 */

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <assert.h>
#include <time.h>

#include "swim-fd.h"

swim_context_t *swim_init(
    margo_instance_id mid,
    ssg_t swim_group,
    int active)
{
    swim_context_t *swim_ctx;
#if 0
    int ret;

    /* TODO: this may end up being identical across many processes => hotspots */
    srand(time(NULL));

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

    /* initialize membership context */
    swim_init_membership_view(swim_ctx);

    /* set protocol parameters */
    swim_ctx->prot_period_len = SWIM_DEF_PROTOCOL_PERIOD_LEN;
    swim_ctx->prot_susp_timeout = SWIM_DEF_SUSPECT_TIMEOUT;
    swim_ctx->prot_subgroup_sz = SWIM_DEF_SUBGROUP_SIZE;

    swim_register_ping_rpcs(margo_get_class(mid), swim_ctx);

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
#endif

    return(swim_ctx);
}

void swim_finalize(swim_context_t *swim_ctx)
{
#if 0
    /* set shutdown flag so ULTs know to start wrapping up */
    swim_ctx->shutdown_flag = 1;

    if(swim_ctx->prot_thread)
    {
        /* wait for the protocol ULT to terminate */
        DEBUG_LOG("attempting to shutdown ult %p\n", swim_ctx, swim_ctx->prot_thread);
        ABT_thread_join(swim_ctx->prot_thread);
        ABT_thread_free(&(swim_ctx->prot_thread));
        DEBUG_LOG("swim protocol shutdown complete\n", swim_ctx);
    }

    free(swim_ctx->membership_view);
    free(swim_ctx);
#endif

    return;
}

