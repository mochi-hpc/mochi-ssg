/*
 * Copyright (c) 2016 UChicago Argonne, LLC
 *
 * See COPYRIGHT in top-level directory.
 */

#include <assert.h>
#include <stdio.h>

#include <mercury.h>
#include <ssg.h>
#include <ssg-margo.h>
#include "rpc.h"

#define DO_DEBUG 0
#define DEBUG(fmt, ...) \
    do { \
        if (DO_DEBUG) { \
            printf(fmt, ##__VA_ARGS__); \
            fflush(stdout); \
        } \
    } while(0)

// impls are equivalent in this trivial case
static void ping_rpc_ult(void *arg)
{
    ping_rpc_handler(arg);
}
DEFINE_MARGO_RPC_HANDLER(ping_rpc_ult)

static void shutdown_rpc_ult(void *arg)
{
    hg_return_t hret;
    struct hg_info *info;
    int rank;
    rpc_context_t *c;
    margo_instance_id mid;
    hg_handle_t h = arg;

    info = HG_Get_info(h);
    assert(info != NULL);

    // get ssg data
    c = HG_Registered_data(info->hg_class, info->id);
    assert(c != NULL && c->s != SSG_NULL);
    rank = ssg_get_rank(c->s);
    assert(rank != SSG_RANK_UNKNOWN && rank != SSG_EXTERNAL_RANK);

    mid = ssg_get_margo_id(c->s);
    assert(mid != MARGO_INSTANCE_NULL);

    DEBUG("%d: received shutdown request\n", rank);
    fflush(stdout);

    hret = margo_respond(mid, h, NULL);
    assert(hret == HG_SUCCESS);

    DEBUG("%d: responded, shutting down\n", rank);
    fflush(stdout);

    HG_Destroy(h);
    ssg_finalize(c->s);
    margo_finalize(mid);
}
DEFINE_MARGO_RPC_HANDLER(shutdown_rpc_ult)

hg_return_t ping_rpc_handler(hg_handle_t h)
{
    hg_return_t hret;
    ping_t out;
    ping_t in;
    struct hg_info *info;
    rpc_context_t *c;

    hret = HG_Get_input(h, &in);
    assert(hret == HG_SUCCESS);

    info = HG_Get_info(h);
    assert(info != NULL);

    // get ssg data
    c = HG_Registered_data(info->hg_class, info->id);
    assert(c != NULL && c->s != SSG_NULL);
    out.rank = ssg_get_rank(c->s);
    assert(out.rank != SSG_RANK_UNKNOWN && out.rank != SSG_EXTERNAL_RANK);

    DEBUG("%d: got ping from rank %d\n", out.rank, in.rank);

    HG_Respond(h, NULL, NULL, &out);

    hret = HG_Free_input(h, &in);
    assert(hret == HG_SUCCESS);
    hret = HG_Destroy(h);
    assert(hret == HG_SUCCESS);
    return HG_SUCCESS;
}

static hg_return_t shutdown_post_respond(const struct hg_cb_info *cb_info)
{
    hg_handle_t h;
    struct hg_info *info;
    rpc_context_t *c;

    h = cb_info->info.respond.handle;
    info = HG_Get_info(h);
    assert(info != NULL);

    c = HG_Registered_data(info->hg_class, info->id);
    DEBUG("%d: post-respond, setting shutdown flag\n", ssg_get_rank(c->s));

    c->shutdown_flag = 1;
    HG_Destroy(h);
    return HG_SUCCESS;
}

static hg_return_t shutdown_post_forward(const struct hg_cb_info *cb_info)
{
    hg_handle_t fwd_handle, resp_handle;
    rpc_context_t *c;
    int rank;
    hg_return_t hret;
    struct hg_info *info;

    // RPC has completed, respond to previous rank
    fwd_handle = cb_info->info.forward.handle;
    resp_handle = cb_info->arg;
    info = HG_Get_info(fwd_handle);
    c = HG_Registered_data(info->hg_class, info->id);
    assert(c != NULL && c->s != SSG_NULL);
    rank = ssg_get_rank(c->s);
    assert(rank != SSG_RANK_UNKNOWN && rank != SSG_EXTERNAL_RANK);
    if (rank > 0) {
        DEBUG("%d: sending shutdown response\n", rank);
        hret = HG_Respond(resp_handle, &shutdown_post_respond, NULL, NULL);
        assert(hret == HG_SUCCESS);
        return HG_SUCCESS;
    }
    else {
        c->shutdown_flag = 1;
        DEBUG("%d: noone to respond to, setting shutdown flag\n", rank);
    }

    HG_Destroy(fwd_handle);
    return HG_SUCCESS;
}

// shutdown - do a ring communication for simplicity, really would want some
// multicast or something
hg_return_t shutdown_rpc_handler(hg_handle_t h)
{
    hg_return_t hret;
    struct hg_info *info;
    int rank;
    rpc_context_t *c;

    info = HG_Get_info(h);
    assert(info != NULL);

    // get ssg data
    c = HG_Registered_data(info->hg_class, info->id);
    assert(c != NULL && c->s != SSG_NULL);
    rank = ssg_get_rank(c->s);
    assert(rank != SSG_RANK_UNKNOWN && rank != SSG_EXTERNAL_RANK);

    DEBUG("%d: received shutdown request\n", rank);

    // forward shutdown to neighbor
    rank++;
    // end-of the line, respond and shut down
    if (rank == ssg_get_count(c->s)) {
        DEBUG("%d: sending response and setting shutdown flag\n", rank-1);
        hret = HG_Respond(h, &shutdown_post_respond, NULL, NULL);
        assert(hret == HG_SUCCESS);
        c->shutdown_flag = 1;
    }
    else {
        hg_handle_t next_handle;
        na_addr_t next_addr;

        next_addr = ssg_get_addr(c->s, rank);
        assert(next_addr != NULL);

        hret = HG_Create(info->context, next_addr, info->id, &next_handle);
        assert(hret == HG_SUCCESS);

        DEBUG("%d: forwarding shutdown to next\n", rank-1);
        hret = HG_Forward(next_handle, &shutdown_post_forward, h, NULL);
        assert(hret == HG_SUCCESS);

        hret = HG_Destroy(next_handle);
        assert(hret == HG_SUCCESS);
    }

    return HG_SUCCESS;
}
