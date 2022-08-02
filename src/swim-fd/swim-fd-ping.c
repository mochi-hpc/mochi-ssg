/*
 * (C) 2016 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <assert.h>

#include <mercury.h>
#include <margo.h>
#include <margo-logging.h>

#include "ssg.h"
#include "ssg-internal.h"
#include "swim-fd.h"
#include "swim-fd-internal.h"

/* NOTE: keep these defines in sync with defs in swim.h */
#define hg_proc_swim_member_status_t    hg_proc_uint8_t
#define hg_proc_swim_member_inc_nr_t    hg_proc_uint32_t
MERCURY_GEN_STRUCT_PROC(swim_member_state_t, \
    ((swim_member_inc_nr_t) (inc_nr)) \
    ((swim_member_status_t) (status)));
MERCURY_GEN_STRUCT_PROC(swim_member_update_t, \
    ((ssg_member_id_t) (id)) \
    ((swim_member_state_t) (state)));

/* a swim message is the membership information piggybacked (gossiped)
 * on the ping and ack messages generated by the protocol
 */
typedef struct swim_message_s
{
    ssg_group_id_t source_g_id;
    ssg_member_id_t source_id;
    swim_member_inc_nr_t source_inc_nr;
    swim_member_state_t target_state;
    hg_size_t swim_pb_buf_count;
    hg_size_t ssg_pb_buf_count;
    swim_member_update_t swim_pb_buf[SWIM_MAX_PIGGYBACK_ENTRIES]; //TODO: dynamic array?
    ssg_member_update_t ssg_pb_buf[SWIM_MAX_PIGGYBACK_ENTRIES]; //TODO: dynamic array?
} swim_message_t;

/* HG encode/decode routines for SWIM RPCs */
static hg_return_t hg_proc_swim_message_t(
    hg_proc_t proc, void *data);

MERCURY_GEN_PROC(swim_dping_req_t, \
    ((ssg_member_id_t) (iping_ack_forward_id)) \
    ((uint32_t) (seq_nr)) \
    ((swim_message_t) (msg)));
MERCURY_GEN_PROC(swim_dping_ack_t, \
    ((ssg_member_id_t) (iping_ack_forward_id)) \
    ((uint32_t) (seq_nr)) \
    ((swim_message_t) (msg)));
MERCURY_GEN_PROC(swim_iping_req_t, \
    ((ssg_member_id_t) (target_id)) \
    ((uint32_t) (seq_nr)) \
    ((swim_message_t) (msg)));
MERCURY_GEN_PROC(swim_iping_ack_t, \
    ((ssg_member_id_t) (target_id)) \
    ((uint32_t) (seq_nr)) \
    ((swim_message_t) (msg)));

/* SWIM message pack/unpack prototypes */
static void swim_pack_message(
    ssg_group_descriptor_t *gd, swim_message_t *msg,
    ssg_member_id_t target_member);
static void swim_unpack_message(
    ssg_group_descriptor_t *gd, swim_message_t *msg);
static void swim_free_packed_message(
    swim_message_t *msg);

DECLARE_MARGO_RPC_HANDLER(swim_dping_req_recv_ult)
DECLARE_MARGO_RPC_HANDLER(swim_dping_ack_recv_ult)
DECLARE_MARGO_RPC_HANDLER(swim_iping_req_recv_ult)
DECLARE_MARGO_RPC_HANDLER(swim_iping_ack_recv_ult)

void swim_register_ping_rpcs(
    struct ssg_mid_state *mid_state)
{
    /* register RPC handlers for SWIM pings */
    mid_state->swim_dping_req_rpc_id = MARGO_REGISTER(mid_state->mid, "swim_dping_req",
        swim_dping_req_t, void, swim_dping_req_recv_ult);
    mid_state->swim_dping_ack_rpc_id = MARGO_REGISTER(mid_state->mid, "swim_dping_ack",
        swim_dping_ack_t, void, swim_dping_ack_recv_ult);
    mid_state->swim_iping_req_rpc_id = MARGO_REGISTER(mid_state->mid, "swim_iping_req",
        swim_iping_req_t, void, swim_iping_req_recv_ult);
    mid_state->swim_iping_ack_rpc_id = MARGO_REGISTER(mid_state->mid, "swim_iping_ack",
        swim_iping_ack_t, void, swim_iping_ack_recv_ult);

    /* disable responses to make SWIM RPCs one-way */
    margo_registered_disable_response(mid_state->mid, mid_state->swim_dping_req_rpc_id, 1);
    margo_registered_disable_response(mid_state->mid, mid_state->swim_dping_ack_rpc_id, 1);
    margo_registered_disable_response(mid_state->mid, mid_state->swim_iping_req_rpc_id, 1);
    margo_registered_disable_response(mid_state->mid, mid_state->swim_iping_ack_rpc_id, 1);

    return;
}

void swim_deregister_ping_rpcs(
    struct ssg_mid_state *mid_state)
{

    margo_deregister(mid_state->mid, mid_state->swim_dping_req_rpc_id);
    margo_deregister(mid_state->mid, mid_state->swim_dping_ack_rpc_id);
    margo_deregister(mid_state->mid, mid_state->swim_iping_req_rpc_id);
    margo_deregister(mid_state->mid, mid_state->swim_iping_ack_rpc_id);

    return;
}

/********************************
 *       SWIM direct pings      *
 ********************************/

void swim_dping_req_send_ult(
    void *t_arg)
{
    ssg_group_descriptor_t *gd = (ssg_group_descriptor_t *)t_arg;
    swim_context_t *swim_ctx;
    swim_dping_req_t dping_req;
    hg_handle_t handle;
    hg_return_t hret;

    if (!gd || !gd->group || !gd->group->swim_ctx)
    {
        margo_error(MARGO_INSTANCE_NULL,
            "[ssg] SWIM dping req send error: invalid group state");
        return;
    }
    swim_ctx = gd->group->swim_ctx;

    hret = margo_create(gd->mid_state->mid, swim_ctx->dping_target_addr,
        gd->mid_state->swim_dping_req_rpc_id, &handle);
    if(hret != HG_SUCCESS)
        return;

    SSG_DEBUG(gd->mid_state, "SWIM: send dping req to %lu (seq_nr=%u)\n",
        swim_ctx->dping_target_id, swim_ctx->seq_nr);

    /* fill the direct ping request with current membership state */
    dping_req.iping_ack_forward_id = SSG_MEMBER_ID_INVALID; /* no iping forward */
    dping_req.seq_nr = swim_ctx->seq_nr;
    swim_pack_message(gd, &(dping_req.msg), swim_ctx->dping_target_id);

    /* send the dping req */
    hret = margo_forward(handle, &dping_req);
    if (hret != HG_SUCCESS)
        SSG_DEBUG(gd->mid_state, "SWIM: dping req forward error (err=%d)\n", hret);

    swim_free_packed_message(&(dping_req.msg));
    margo_destroy(handle);
    return;
}

static void swim_dping_req_recv_ult(
    hg_handle_t handle)
{
    const struct hg_info *hgi;
    margo_instance_id mid;
    ssg_group_descriptor_t *gd;
    swim_dping_req_t dping_req;
    swim_dping_ack_t dping_ack;
    hg_handle_t ack_handle;
    hg_return_t hret;

    assert(ssg_rt);

    /* get handle info and margo instance */
    hgi = margo_get_info(handle);
    assert(hgi);
    mid = margo_hg_info_get_instance(hgi);
    if (mid == MARGO_INSTANCE_NULL)
    {
        margo_error(mid, "[ssg] SWIM dping req recv error: invalid Margo state");
        margo_destroy(handle);
        return;
    }

    hret = margo_get_input(handle, &dping_req);
    if (hret != HG_SUCCESS)
    {
        margo_destroy(handle);
        return;
    }

    /* find referenced group */
    SSG_GROUP_READ(dping_req.msg.source_g_id, gd);
    if (!gd)
    {
        margo_error(mid, "[ssg] SWIM dping req recv error: group %lu not found",
            dping_req.msg.source_g_id);
        margo_free_input(handle, &dping_req);
        margo_destroy(handle);
        return;
    }

    if (!gd->is_member)
    {
        margo_error(mid, "[ssg] SWIM dping req recv error: not a member");
        SSG_GROUP_RELEASE(gd);
        margo_free_input(handle, &dping_req);
        margo_destroy(handle);
        return;
    }

    /* prevent group from being destroyed from underneath us while
     * we potentially callback to users to report membership updates
     */
    SSG_GROUP_REF_INCR(gd);
    SSG_GROUP_RELEASE(gd);

    SSG_DEBUG(gd->mid_state, "SWIM: recv dping req from %lu (seq_nr=%u)\n",
        dping_req.msg.source_id, dping_req.seq_nr);

    /* extract sender's membership state from dping req */
    swim_unpack_message(gd, &(dping_req.msg));

    /* fill the dping ack with current membership state */
    dping_ack.iping_ack_forward_id = dping_req.iping_ack_forward_id;
    dping_ack.seq_nr = dping_req.seq_nr;
    swim_pack_message(gd, &(dping_ack.msg), dping_req.msg.source_id);

    hret = margo_create(gd->mid_state->mid, hgi->addr,
        gd->mid_state->swim_dping_ack_rpc_id, &ack_handle);
    if(hret != HG_SUCCESS)
    {
        SSG_GROUP_REF_DECR(gd);
        swim_free_packed_message(&(dping_ack.msg));
        margo_free_input(handle, &dping_req);
        margo_destroy(handle);
        return;
    }

    SSG_DEBUG(gd->mid_state, "SWIM: send dping ack to %lu (seq_nr=%u)\n",
        dping_req.msg.source_id, dping_ack.seq_nr);

    SSG_GROUP_REF_DECR(gd);

    hret = margo_forward(ack_handle, &dping_ack);
    if(hret != HG_SUCCESS)
    {
        margo_error(mid, "[ssg] SWIM dping ack forward error (hret=%d)", hret);
    }

    swim_free_packed_message(&(dping_ack.msg));
    margo_free_input(handle, &dping_req);
    margo_destroy(handle);
    margo_destroy(ack_handle);
    return;
}
DEFINE_MARGO_RPC_HANDLER(swim_dping_req_recv_ult)

static void swim_dping_ack_recv_ult(
    hg_handle_t handle)
{
    const struct hg_info *hgi;
    margo_instance_id mid;
    ssg_group_descriptor_t *gd;
    swim_context_t *swim_ctx;
    swim_dping_ack_t dping_ack;
    hg_return_t hret;

    assert(ssg_rt);

    /* get handle info and margo instance */
    hgi = margo_get_info(handle);
    assert(hgi);
    mid = margo_hg_info_get_instance(hgi);
    if (mid == MARGO_INSTANCE_NULL)
    {
        margo_error(mid, "[ssg] SWIM dping ack recv error: invalid Margo state");
        margo_destroy(handle);
        return;
    }

    hret = margo_get_input(handle, &dping_ack);
    if(hret != HG_SUCCESS)
    {
        margo_destroy(handle);
        return;
    }

    /* find referenced group */
    SSG_GROUP_READ(dping_ack.msg.source_g_id, gd);
    if (!gd)
    {
        margo_error(mid, "[ssg] SWIM dping ack recv error: group %lu not found",
            dping_ack.msg.source_g_id);
        margo_free_input(handle, &dping_ack);
        margo_destroy(handle);
        return;
    }

    if (!gd->is_member)
    {
        margo_error(mid, "[ssg] SWIM dping ack recv error: not a member");
        SSG_GROUP_RELEASE(gd);
        margo_free_input(handle, &dping_ack);
        margo_destroy(handle);
        return;
    }
    swim_ctx = gd->group->swim_ctx;

    /* prevent group from being destroyed from underneath us while
     * we potentially callback to users to report membership updates
     */
    SSG_GROUP_REF_INCR(gd);
    SSG_GROUP_RELEASE(gd);

    SSG_DEBUG(gd->mid_state, "SWIM: recv dping ack from %lu (seq_nr=%u)\n",
        dping_ack.msg.source_id, dping_ack.seq_nr);

    /* extract sender's membership state from dping ack */
    swim_unpack_message(gd, &(dping_ack.msg));

    if(dping_ack.iping_ack_forward_id == SSG_MEMBER_ID_INVALID)
    {
        /* this is a normal dping ack, just mark the target as acked */
        ABT_rwlock_wrlock(swim_ctx->swim_lock);
        if((dping_ack.msg.source_id == swim_ctx->dping_target_id) &&
            (dping_ack.seq_nr == swim_ctx->seq_nr))
        {
            /* make sure we are acking the right member/sequence number */
            swim_ctx->ping_target_acked = 1;
        }
        ABT_rwlock_unlock(swim_ctx->swim_lock);
        SSG_GROUP_REF_DECR(gd);
    }
    else
    {
        ssg_member_state_t *origin_ms;
        hg_handle_t ack_handle;
        swim_iping_ack_t iping_ack;

        /* this dping ack corresponds to an iping req -- ack the iping req */

        /* get the address of the origin member to forward the ack to */
        ABT_rwlock_rdlock(gd->lock);
        HASH_FIND(hh, gd->view->member_map, &dping_ack.iping_ack_forward_id,
            sizeof(dping_ack.iping_ack_forward_id), origin_ms);
        if(!origin_ms)
        {
            SSG_DEBUG(gd->mid_state, "SWIM: ignoring iping ack for unknown group member %lu\n",
                dping_ack.iping_ack_forward_id);
            ABT_rwlock_unlock(gd->lock);
            SSG_GROUP_REF_DECR(gd);
            margo_free_input(handle, &dping_ack);
            margo_destroy(handle);
            return;
        }

        hret = margo_create(gd->mid_state->mid, origin_ms->addr,
            gd->mid_state->swim_iping_ack_rpc_id, &ack_handle);
        if(hret != HG_SUCCESS)
        {
            ABT_rwlock_unlock(gd->lock);
            SSG_GROUP_REF_DECR(gd);
            margo_free_input(handle, &dping_ack);
            margo_destroy(handle);
            return;
        }
        ABT_rwlock_unlock(gd->lock);

        SSG_DEBUG(gd->mid_state, "SWIM: send iping ack to %lu (target=%lu, seq_nr=%u)\n",
            dping_ack.iping_ack_forward_id, dping_ack.msg.source_id, dping_ack.seq_nr);

        /* fill the iping ack with current membership state */
        iping_ack.target_id = dping_ack.msg.source_id;
        iping_ack.seq_nr = dping_ack.seq_nr;
        swim_pack_message(gd, &(iping_ack.msg), dping_ack.iping_ack_forward_id);

        SSG_GROUP_REF_DECR(gd);

        hret = margo_forward(ack_handle, &iping_ack);
        if(hret != HG_SUCCESS)
        {
            margo_error(mid, "[ssg] SWIM iping ack forward error (hret=%d)", hret);
        }

        swim_free_packed_message(&(iping_ack.msg));
        margo_destroy(ack_handle);
    }

    margo_free_input(handle, &dping_ack);
    margo_destroy(handle);
    return;
}
DEFINE_MARGO_RPC_HANDLER(swim_dping_ack_recv_ult)

/********************************
 *     SWIM indirect pings      *
 ********************************/

void swim_iping_req_send_ult(
    void *t_arg)
{
    ssg_group_descriptor_t *gd = (ssg_group_descriptor_t *)t_arg;
    swim_context_t *swim_ctx;
    ssg_member_id_t iping_target_id;
    hg_addr_t iping_target_addr;
    hg_handle_t handle;
    swim_iping_req_t iping_req;
    hg_return_t hret;

    if (!gd || !gd->group || !gd->group->swim_ctx)
    {
        margo_error(MARGO_INSTANCE_NULL,
            "[ssg] SWIM iping req send error: invalid group state");
        return;
    }
    swim_ctx = gd->group->swim_ctx;

    ABT_rwlock_wrlock(swim_ctx->swim_lock);
    iping_target_id = swim_ctx->iping_target_ids[swim_ctx->iping_target_ndx];
    iping_target_addr = swim_ctx->iping_target_addrs[swim_ctx->iping_target_ndx];
    swim_ctx->iping_target_ndx++;
    ABT_rwlock_unlock(swim_ctx->swim_lock);

    hret = margo_create(gd->mid_state->mid, iping_target_addr,
        gd->mid_state->swim_iping_req_rpc_id, &handle);
    if(hret != HG_SUCCESS)
        return;

    SSG_DEBUG(gd->mid_state, "SWIM: send iping req to %lu (target=%lu, seq_nr=%u)\n",
        iping_target_id, swim_ctx->dping_target_id, swim_ctx->seq_nr);

    /* fill the iping req with target member and current membership state */
    iping_req.target_id = swim_ctx->dping_target_id;
    iping_req.seq_nr = swim_ctx->seq_nr;
    swim_pack_message(gd, &(iping_req.msg), iping_target_id);

    /* send this iping req */
    hret = margo_forward(handle, &iping_req);
    if (hret != HG_SUCCESS)
        SSG_DEBUG(gd->mid_state, "SWIM: iping req forward error (err=%d)\n", hret);

    swim_free_packed_message(&(iping_req.msg));
    margo_destroy(handle);
    return;
}

static void swim_iping_req_recv_ult(hg_handle_t handle)
{
    const struct hg_info *hgi;
    margo_instance_id mid;
    ssg_group_descriptor_t *gd;
    ssg_member_state_t *target_ms;
    swim_iping_req_t iping_req;
    swim_dping_req_t dping_req;
    hg_handle_t dping_handle;
    hg_return_t hret;

    assert(ssg_rt);

    /* get handle info and margo instance */
    hgi = margo_get_info(handle);
    assert(hgi);
    mid = margo_hg_info_get_instance(hgi);
    if (mid == MARGO_INSTANCE_NULL)
    {
        margo_error(mid, "[ssg] SWIM iping req recv error: invalid Margo state");
        margo_destroy(handle);
        return;
    }

    hret = margo_get_input(handle, &iping_req);
    if(hret != HG_SUCCESS)
    {
        margo_destroy(handle);
        return;
    }

    /* find referenced group */
    SSG_GROUP_READ(iping_req.msg.source_g_id, gd);
    if (!gd)
    {
        margo_error(mid, "[ssg] SWIM iping req recv error: group %lu not found",
            iping_req.msg.source_g_id);
        margo_free_input(handle, &iping_req);
        margo_destroy(handle);
        return;
    }

    if (!gd->is_member)
    {
        margo_error(mid, "[ssg] SWIM iping req recv error: not a member");
        SSG_GROUP_RELEASE(gd);
        margo_free_input(handle, &iping_req);
        margo_destroy(handle);
        return;
    }

    /* prevent group from being destroyed from underneath us while
     * we potentially callback to users to report membership updates
     */
    SSG_GROUP_REF_INCR(gd);
    SSG_GROUP_RELEASE(gd);

    SSG_DEBUG(gd->mid_state, "SWIM: recv iping req from %lu (target=%lu, seq_nr=%u)\n",
        iping_req.msg.source_id, iping_req.target_id, iping_req.seq_nr);

    /* extract sender's membership state from request */
    swim_unpack_message(gd, &(iping_req.msg));

    /* get the address of the ping target */
    ABT_rwlock_rdlock(gd->lock);
    HASH_FIND(hh, gd->view->member_map, &iping_req.target_id,
        sizeof(iping_req.target_id), target_ms);
    if(!target_ms)
    {
        SSG_DEBUG(gd->mid_state, "SWIM: ignoring iping req for unknown group member %lu\n",
            iping_req.target_id);
        ABT_rwlock_unlock(gd->lock);
        SSG_GROUP_REF_DECR(gd);
        margo_free_input(handle, &iping_req);
        margo_destroy(handle);
        return;
    }

    hret = margo_create(gd->mid_state->mid, target_ms->addr,
        gd->mid_state->swim_dping_req_rpc_id, &dping_handle);
    if(hret != HG_SUCCESS)
    {
        ABT_rwlock_unlock(gd->lock);
        SSG_GROUP_REF_DECR(gd);
        margo_free_input(handle, &iping_req);
        margo_destroy(handle);
        return;
    }
    ABT_rwlock_unlock(gd->lock);

    SSG_DEBUG(gd->mid_state, "SWIM: send dping req to %lu (seq_nr=%u)\n",
        iping_req.target_id, iping_req.seq_nr);
    
    /* fill the direct ping request with current membership state */
    dping_req.iping_ack_forward_id = iping_req.msg.source_id;
    dping_req.seq_nr = iping_req.seq_nr;
    swim_pack_message(gd, &(dping_req.msg), iping_req.target_id);

    SSG_GROUP_REF_DECR(gd);

    /* send dping req to target on behalf of member who sent iping req */
    hret = margo_forward(dping_handle, &dping_req);
    if (hret != HG_SUCCESS)
        SSG_DEBUG(gd->mid_state, "SWIM: dping req forward error (err=%d)\n", hret);

    swim_free_packed_message(&(dping_req.msg));
    margo_free_input(handle, &iping_req);
    margo_destroy(handle);
    margo_destroy(dping_handle);
    return;
}
DEFINE_MARGO_RPC_HANDLER(swim_iping_req_recv_ult)

static void swim_iping_ack_recv_ult(hg_handle_t handle)
{
    const struct hg_info *hgi;
    margo_instance_id mid;
    ssg_group_descriptor_t *gd;
    swim_iping_ack_t iping_ack;
    hg_return_t hret;

    assert(ssg_rt);

    /* get handle info and margo instance */
    hgi = margo_get_info(handle);
    assert(hgi);
    mid = margo_hg_info_get_instance(hgi);
    if (mid == MARGO_INSTANCE_NULL)
    {
        margo_error(mid, "[ssg] SWIM iping ack recv error: invalid Margo state");
        margo_destroy(handle);
        return;
    }

    hret = margo_get_input(handle, &iping_ack);
    if(hret != HG_SUCCESS)
    {
        margo_destroy(handle);
        return;
    }

    /* find referenced group */
    SSG_GROUP_READ(iping_ack.msg.source_g_id, gd);
    if (!gd)
    {
        margo_error(mid, "[ssg] SWIM iping ack recv error: group %lu not found",
            iping_ack.msg.source_g_id);
        margo_free_input(handle, &iping_ack);
        margo_destroy(handle);
        return;
    }

    if (!gd->is_member)
    {
        margo_error(mid, "[ssg] SWIM iping ack recv error: not a member");
        SSG_GROUP_RELEASE(gd);
        margo_free_input(handle, &iping_ack);
        margo_destroy(handle);
        return;
    }

    /* prevent group from being destroyed from underneath us while
     * we potentially callback to users to report membership updates
     */
    SSG_GROUP_REF_INCR(gd);
    SSG_GROUP_RELEASE(gd);

    SSG_DEBUG(gd->mid_state, "SWIM: recv iping ack from %lu (target=%lu, seq_nr=%u)\n",
        iping_ack.msg.source_id, iping_ack.target_id, iping_ack.seq_nr);

    /* extract target's membership state from response */
    swim_unpack_message(gd, &(iping_ack.msg));

    ABT_rwlock_wrlock(gd->group->swim_ctx->swim_lock);
    if((iping_ack.target_id == gd->group->swim_ctx->dping_target_id) &&
        (iping_ack.seq_nr == gd->group->swim_ctx->seq_nr))
    {
        /* make sure we are acking the right member/sequence number */
        gd->group->swim_ctx->ping_target_acked = 1;
    }
    ABT_rwlock_unlock(gd->group->swim_ctx->swim_lock);
    SSG_GROUP_REF_DECR(gd);

    margo_free_input(handle, &iping_ack);
    margo_destroy(handle);
    return;
}
DEFINE_MARGO_RPC_HANDLER(swim_iping_ack_recv_ult)

/********************************
 *      SWIM ping helpers       *
 ********************************/

static void swim_pack_message(ssg_group_descriptor_t *gd, swim_message_t *msg,
    ssg_member_id_t target_member)
{
    swim_context_t *swim_ctx = gd->group->swim_ctx;
    ssg_member_state_t *target_ms;

    memset(msg, 0, sizeof(*msg));

    /* fill in self information */
    ABT_rwlock_rdlock(swim_ctx->swim_lock);
    msg->source_id = gd->mid_state->self_id;
    msg->source_g_id = gd->g_id;
    msg->source_inc_nr = swim_ctx->self_inc_nr;
    ABT_rwlock_unlock(swim_ctx->swim_lock);

    ABT_rwlock_rdlock(gd->lock);
    HASH_FIND(hh, gd->view->member_map, &target_member,
        sizeof(target_member), target_ms);
    if(!target_ms)
    {
        /* check dead list if target member not in view */
        HASH_FIND(hh, gd->group->dead_members, &target_member,
            sizeof(target_member), target_ms);
    }
    if(target_ms)
    {
        msg->target_state.inc_nr = target_ms->swim_state.inc_nr;
        msg->target_state.status = target_ms->swim_state.status;
    }
    else
    {
        /* dummy state that target will just ignore */
        msg->target_state.inc_nr = 0;
        msg->target_state.status = SWIM_MEMBER_ALIVE;
    }
    ABT_rwlock_unlock(gd->lock);

    /* piggyback SWIM & SSG updates on the message */
    msg->swim_pb_buf_count = SWIM_MAX_PIGGYBACK_ENTRIES;
    msg->ssg_pb_buf_count = SWIM_MAX_PIGGYBACK_ENTRIES;
    swim_retrieve_member_updates(gd, msg->swim_pb_buf, &msg->swim_pb_buf_count);
    swim_retrieve_ssg_member_updates(gd, msg->ssg_pb_buf, &msg->ssg_pb_buf_count);

    return;
}

static void swim_unpack_message(ssg_group_descriptor_t *gd, swim_message_t *msg)
{
    swim_member_update_t tmp_update;

    /* XXX is there a way to prevent packing of this data in response? */
    /* apply (implicit) sender update */
    tmp_update.id = msg->source_id;
    tmp_update.state.status = SWIM_MEMBER_ALIVE;
    tmp_update.state.inc_nr = msg->source_inc_nr;
    swim_apply_member_updates(gd, &tmp_update, 1);

    /* apply message source's view of our membership state */
    tmp_update.id = gd->mid_state->self_id;
    tmp_update.state.status = msg->target_state.status;
    tmp_update.state.inc_nr = msg->target_state.inc_nr;
    swim_apply_member_updates(gd, &tmp_update, 1);

    /* apply SWIM updates */
    if(msg->swim_pb_buf_count > 0)
        swim_apply_member_updates(gd, msg->swim_pb_buf, msg->swim_pb_buf_count);

    /* apply SSG updates */
    if(msg->ssg_pb_buf_count > 0)
        ssg_apply_member_updates(gd, msg->ssg_pb_buf, msg->ssg_pb_buf_count, 1);

    return;
}

static void swim_free_packed_message(swim_message_t *msg)
{
    hg_size_t i;

    for(i = 0; i < msg->ssg_pb_buf_count; i++)
    {
        if(msg->ssg_pb_buf[i].type == SSG_MEMBER_JOINED)
            free(msg->ssg_pb_buf[i].u.member_addr_str);
    }

    return;
}

/* manual serialization/deserialization routine for swim messages */
static hg_return_t hg_proc_swim_message_t(hg_proc_t proc, void *data)
{
    swim_message_t *msg = (swim_message_t *)data;
    hg_return_t hret = HG_PROTOCOL_ERROR;
    hg_size_t i;

    switch(hg_proc_get_op(proc))
    {
        case HG_ENCODE:
            hret = hg_proc_ssg_group_id_t(proc, &(msg->source_g_id));
            if(hret != HG_SUCCESS)
            {
                hret = HG_PROTOCOL_ERROR;
                return hret;
            }
            hret = hg_proc_ssg_member_id_t(proc, &(msg->source_id));
            if(hret != HG_SUCCESS)
            {
                hret = HG_PROTOCOL_ERROR;
                return hret;
            }
            hret = hg_proc_swim_member_inc_nr_t(proc, &(msg->source_inc_nr));
            if(hret != HG_SUCCESS)
            {
                hret = HG_PROTOCOL_ERROR;
                return hret;
            }
            hret = hg_proc_swim_member_state_t(proc, &(msg->target_state));
            if(hret != HG_SUCCESS)
            {
                hret = HG_PROTOCOL_ERROR;
                return hret;
            }
            hret = hg_proc_hg_size_t(proc, &(msg->swim_pb_buf_count));
            if(hret != HG_SUCCESS)
            {
                hret = HG_PROTOCOL_ERROR;
                return hret;
            }
            hret = hg_proc_hg_size_t(proc, &(msg->ssg_pb_buf_count));
            if(hret != HG_SUCCESS)
            {
                hret = HG_PROTOCOL_ERROR;
                return hret;
            }
            for(i = 0; i < msg->swim_pb_buf_count; i++)
            {
                hret = hg_proc_swim_member_update_t(proc, &(msg->swim_pb_buf[i]));
                if(hret != HG_SUCCESS)
                {
                    hret = HG_PROTOCOL_ERROR;
                    return hret;
                }
            }
            for(i = 0; i < msg->ssg_pb_buf_count; i++)
            {
                hret = hg_proc_ssg_member_update_t(proc, &(msg->ssg_pb_buf[i]));
                if(hret != HG_SUCCESS)
                {
                    hret = HG_PROTOCOL_ERROR;
                    return hret;
                }
            }
            break;
        case HG_DECODE:
            hret = hg_proc_ssg_group_id_t(proc, &(msg->source_g_id));
            if(hret != HG_SUCCESS)
            {
                hret = HG_PROTOCOL_ERROR;
                return hret;
            }
            hret = hg_proc_ssg_member_id_t(proc, &(msg->source_id));
            if(hret != HG_SUCCESS)
            {
                hret = HG_PROTOCOL_ERROR;
                return hret;
            }
            hret = hg_proc_swim_member_inc_nr_t(proc, &(msg->source_inc_nr));
            if(hret != HG_SUCCESS)
            {
                hret = HG_PROTOCOL_ERROR;
                return hret;
            }
            hret = hg_proc_swim_member_state_t(proc, &(msg->target_state));
            if(hret != HG_SUCCESS)
            {
                hret = HG_PROTOCOL_ERROR;
                return hret;
            }
            hret = hg_proc_hg_size_t(proc, &(msg->swim_pb_buf_count));
            if(hret != HG_SUCCESS)
            {
                hret = HG_PROTOCOL_ERROR;
                return hret;
            }
            hret = hg_proc_hg_size_t(proc, &(msg->ssg_pb_buf_count));
            if(hret != HG_SUCCESS)
            {
                hret = HG_PROTOCOL_ERROR;
                return hret;
            }
            for(i = 0; i < msg->swim_pb_buf_count; i++)
            {
                memset(&(msg->swim_pb_buf[i]), 0, sizeof(msg->swim_pb_buf[i]));
                hret = hg_proc_swim_member_update_t(proc, &(msg->swim_pb_buf[i]));
                if(hret != HG_SUCCESS)
                {
                    hret = HG_PROTOCOL_ERROR;
                    return hret;
                }
            }
            for(i = 0; i < msg->ssg_pb_buf_count; i++)
            {
                memset(&(msg->ssg_pb_buf[i]), 0, sizeof(msg->ssg_pb_buf[i]));
                hret = hg_proc_ssg_member_update_t(proc, &(msg->ssg_pb_buf[i]));
                if(hret != HG_SUCCESS)
                {
                    hret = HG_PROTOCOL_ERROR;
                    return hret;
                }
            }
            break;
        case HG_FREE:
            for(i = 0; i < msg->ssg_pb_buf_count; i++)
            {
                hret = hg_proc_ssg_member_update_t(proc, &(msg->ssg_pb_buf[i]));
                if(hret != HG_SUCCESS)
                {
                    hret = HG_PROTOCOL_ERROR;
                    return hret;
                }
            }
            hret = HG_SUCCESS;
            break;
        default:
            break;
    }

    return(hret);
}
