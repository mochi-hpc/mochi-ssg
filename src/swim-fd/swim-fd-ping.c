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

#include <mercury.h>
#include <margo.h>
#include <ssg.h>
#include "ssg-internal.h"
#include "swim-fd.h"
#include "swim-fd-internal.h"

/* NOTE these defines must be kept in sync with typedefs in
 * swim-internal.h
 */
#define hg_proc_swim_member_id_t hg_proc_int64_t
#define hg_proc_swim_member_status_t hg_proc_uint8_t
#define hg_proc_swim_member_inc_nr_t hg_proc_uint32_t

MERCURY_GEN_STRUCT_PROC(swim_member_update_t, \
    ((swim_member_id_t) (id)) \
    ((swim_member_status_t) (status)) \
    ((swim_member_inc_nr_t) (inc_nr)));

/* a swim message is the membership information piggybacked (gossiped)
 * on the ping and ack messages generated by the protocol
 */
typedef struct swim_message_s
{
    swim_member_id_t source_id;
    swim_member_inc_nr_t source_inc_nr;
    swim_member_update_t pb_buf[SWIM_MAX_PIGGYBACK_ENTRIES]; //TODO: can we do dynamic array instead?
} swim_message_t;

static hg_return_t hg_proc_swim_message_t(hg_proc_t proc, void *data);

MERCURY_GEN_PROC(swim_dping_req_t, \
    ((swim_message_t) (msg)));

MERCURY_GEN_PROC(swim_dping_resp_t, \
    ((swim_message_t) (msg)));

MERCURY_GEN_PROC(swim_iping_req_t, \
    ((swim_member_id_t) (target_id)) \
    ((swim_message_t) (msg)));

MERCURY_GEN_PROC(swim_iping_resp_t, \
    ((swim_message_t) (msg)));

DECLARE_MARGO_RPC_HANDLER(swim_dping_recv_ult)
DECLARE_MARGO_RPC_HANDLER(swim_iping_recv_ult)

static void swim_pack_message(ssg_t s, swim_message_t *msg);
static void swim_unpack_message(ssg_t s, swim_message_t *msg);

static hg_id_t dping_rpc_id;
static hg_id_t iping_rpc_id;

void swim_register_ping_rpcs(
    ssg_t s)
{
    hg_class_t *hg_cls = margo_get_class(s->mid);

    /* register RPC handlers for SWIM pings */
    dping_rpc_id = MERCURY_REGISTER(hg_cls, "dping", swim_dping_req_t,
        swim_dping_resp_t, swim_dping_recv_ult_handler);
    iping_rpc_id = MERCURY_REGISTER(hg_cls, "iping", swim_iping_req_t,
        swim_iping_resp_t, swim_iping_recv_ult_handler);

    /* TODO: disable responses for RPCs to make them one-way operations */
    //HG_Registered_disable_response(hg_cls, dping_rpc_id, HG_TRUE);
    //HG_Registered_disable_response(hg_cls, iping_rpc_id, HG_TRUE);

    /* register swim context data structure with each RPC type */
    HG_Register_data(hg_cls, dping_rpc_id, s, NULL);
    HG_Register_data(hg_cls, iping_rpc_id, s, NULL);

    return;
}

/********************************
 *       SWIM direct pings      *
 ********************************/

static int swim_send_dping(
    ssg_t s, swim_member_id_t target);

void swim_dping_send_ult(
    void *t_arg)
{
    ssg_t s = (ssg_t)t_arg;
    swim_context_t *swim_ctx;
    swim_member_id_t target;
    int ret;

    assert(s != SSG_NULL);
    swim_ctx = s->swim_ctx;
    assert(swim_ctx != NULL);

    target = swim_ctx->ping_target;
    ret = swim_send_dping(s, target);
    if (ret == 0)
    {
        /* mark this dping req as acked, double checking to make
         * sure we aren't inadvertently ack'ing a ping request
         * for a more recent tick of the protocol
         */
        if(swim_ctx->ping_target == target)
            swim_ctx->ping_target_acked = 1;
    }

    return;
}

static int swim_send_dping(
    ssg_t s, swim_member_id_t target)
{
    swim_context_t *swim_ctx = s->swim_ctx;
    hg_addr_t target_addr = HG_ADDR_NULL;
    hg_handle_t handle;
    swim_dping_req_t dping_req;
    swim_dping_resp_t dping_resp;
    hg_return_t hret;
    int ret = -1;

    target_addr = s->view.member_states[target].addr;
    if(target_addr == HG_ADDR_NULL)
        return(ret);

    hret = HG_Create(margo_get_context(s->mid), target_addr, dping_rpc_id,
        &handle);
    if(hret != HG_SUCCESS)
        return(ret);

    SSG_DEBUG(s, "SWIM: send dping req to %d\n", (int)target);

    /* fill the direct ping request with current membership state */
    swim_pack_message(s, &(dping_req.msg));

    /* send a direct ping that expires at the end of the protocol period */
    hret = margo_forward_timed(s->mid, handle, &dping_req,
        swim_ctx->prot_period_len);
    if (hret == HG_SUCCESS)
    {
        hret = HG_Get_output(handle, &dping_resp);
        if(hret != HG_SUCCESS)
            return(ret);

        SSG_DEBUG(s, "SWIM: recv dping ack from %d\n", (int)dping_resp.msg.source_id);
        assert(dping_resp.msg.source_id == target);

        /* extract target's membership state from response */
        swim_unpack_message(s, &(dping_resp.msg));

        ret = 0;
    }
    else if(hret != HG_TIMEOUT)
    {
        SSG_DEBUG(s, "SWIM: dping req error from %d (err=%d)\n", (int)target, hret);
    }

    HG_Destroy(handle);
    return(ret);
}

static void swim_dping_recv_ult(hg_handle_t handle)
{
    ssg_t s;
    swim_context_t *swim_ctx;
    const struct hg_info *info;
    swim_dping_req_t dping_req;
    swim_dping_resp_t dping_resp;
    hg_return_t hret;

    /* get ssg & swim state */
    info = HG_Get_info(handle);
    if(info == NULL)
        return;
    s = (ssg_t)HG_Registered_data(info->hg_class, dping_rpc_id);
    assert(s != SSG_NULL);
    swim_ctx = s->swim_ctx;
    assert(swim_ctx != NULL);

    /* XXX: make rank 1 unresponsive */
    //int drop = rand() % 2;
    int drop = 1;
    if(s->view.self_rank == 1 && drop)
    {
        HG_Destroy(handle);
        return;
    }

    hret = HG_Get_input(handle, &dping_req);
    if(hret != HG_SUCCESS)
        return;

    SSG_DEBUG(s, "SWIM: recv dping req from %d\n", (int)dping_req.msg.source_id);

    /* extract sender's membership state from request */
    swim_unpack_message(s, &(dping_req.msg));

    /* fill the direct ping response with current membership state */
    swim_pack_message(s, &(dping_resp.msg));

    SSG_DEBUG(s, "SWIM: send dping ack to %d\n", (int)dping_req.msg.source_id);

    /* respond to sender of the dping req */
    margo_respond(s->mid, handle, &dping_resp);

    HG_Destroy(handle);
    return;
}
DEFINE_MARGO_RPC_HANDLER(swim_dping_recv_ult)

/********************************
 *     SWIM indirect pings      *
 ********************************/

void swim_iping_send_ult(
    void *t_arg)
{
    ssg_t s = (ssg_t)t_arg;
    swim_context_t *swim_ctx;
    int i;
    swim_member_id_t my_subgroup_member = SSG_MEMBER_RANK_UNKNOWN;
    hg_addr_t target_addr = HG_ADDR_NULL;
    hg_handle_t handle;
    swim_iping_req_t iping_req;
    swim_iping_resp_t iping_resp;
    hg_return_t hret;

    assert(s != SSG_NULL);
    swim_ctx = s->swim_ctx;
    assert(swim_ctx != NULL);

    for(i = 0; i < swim_ctx->prot_subgroup_sz; i++)
    {
        if(swim_ctx->subgroup_members[i] != SSG_MEMBER_RANK_UNKNOWN)
        {
            my_subgroup_member = swim_ctx->subgroup_members[i];
            swim_ctx->subgroup_members[i] = SSG_MEMBER_RANK_UNKNOWN;
            break;
        }
    }
    assert(my_subgroup_member != SSG_MEMBER_RANK_UNKNOWN);

    target_addr = s->view.member_states[my_subgroup_member].addr;
    if(target_addr == HG_ADDR_NULL)
        return;

    hret = HG_Create(margo_get_context(s->mid), target_addr, iping_rpc_id,
        &handle);
    if(hret != HG_SUCCESS)
        return;

    SSG_DEBUG(s, "SWIM: send iping req to %d (target=%d)\n",
        (int)my_subgroup_member, (int)swim_ctx->ping_target);

    /* fill the indirect ping request with target member and current
     * membership state
     */
    iping_req.target_id = swim_ctx->ping_target;
    swim_pack_message(s, &(iping_req.msg));

    /* send this indirect ping */
    /* NOTE: the timeout is just the protocol period length minus
     * the dping timeout, which should cause this iping to timeout
     * right at the end of the current protocol period.
     */
    hret = margo_forward_timed(s->mid, handle, &iping_req,
        (swim_ctx->prot_period_len - swim_ctx->dping_timeout));
    if (hret == HG_SUCCESS)
    {
        hret = HG_Get_output(handle, &iping_resp);
        if(hret != HG_SUCCESS)
            return;

        SSG_DEBUG(s, "SWIM: recv iping ack from %d (target=%d)\n",
            (int)iping_resp.msg.source_id, (int)swim_ctx->ping_target);

        /* extract target's membership state from response */
        swim_unpack_message(s, &(iping_resp.msg));

        /* mark this iping req as acked, double checking to make
         * sure we aren't inadvertently ack'ing a ping request
         * for a more recent tick of the protocol
         */
        if(swim_ctx->ping_target == iping_req.target_id)
            swim_ctx->ping_target_acked = 1;
    }
    else if(hret != HG_TIMEOUT)
    {
        SSG_DEBUG(s, "SWIM: iping req error from %d (target=%d, err=%d)\n",
            (int)my_subgroup_member, hret, (int)swim_ctx->ping_target);
    }

    HG_Destroy(handle);
    return;
}

static void swim_iping_recv_ult(hg_handle_t handle)
{
    ssg_t s;
    swim_context_t *swim_ctx;
    const struct hg_info *info;
    swim_iping_req_t iping_req;
    swim_iping_resp_t iping_resp;
    hg_return_t hret;
    int ret;

    /* get the swim state */
    info = HG_Get_info(handle);
    if(info == NULL)
        return;
    s = (ssg_t)HG_Registered_data(info->hg_class, dping_rpc_id);
    assert(s != SSG_NULL);
    swim_ctx = s->swim_ctx;
    assert(swim_ctx != NULL);

    /* XXX: make rank 1 unresponsive */
    if(s->view.self_rank == 1)
    {
        HG_Destroy(handle);
        return;
    }

    hret = HG_Get_input(handle, &iping_req);
    if(hret != HG_SUCCESS)
        return;

    SSG_DEBUG(s, "SWIM: recv iping req from %d (target=%d)\n",
        (int)iping_req.msg.source_id, (int)iping_req.target_id);

    /* extract sender's membership state from request */
    swim_unpack_message(s, &(iping_req.msg));

    /* send direct ping to target on behalf of who sent iping req */
    ret = swim_send_dping(s, iping_req.target_id);
    if(ret == 0)
    {
        /* if the dping req succeeds, fill the indirect ping
         * response with current membership state
         */
        swim_pack_message(s, &(iping_resp.msg));

        SSG_DEBUG(s, "SWIM: send iping ack to %d (target=%d)\n",
            (int)iping_req.msg.source_id, (int)iping_req.target_id);

        /* respond to sender of the iping req */
        margo_respond(s->mid, handle, &iping_resp);
    }

    HG_Destroy(handle);
    return;
}
DEFINE_MARGO_RPC_HANDLER(swim_iping_recv_ult)

/********************************
 *      SWIM ping helpers       *
 ********************************/

static void swim_pack_message(ssg_t s, swim_message_t *msg)
{
    swim_context_t *swim_ctx = s->swim_ctx;

    memset(msg, 0, sizeof(*msg));

    /* fill in self information */
    msg->source_id = s->view.self_rank;
    msg->source_inc_nr = swim_ctx->member_inc_nrs[s->view.self_rank];

    /* piggyback a set of membership states on this message */
    swim_retrieve_membership_updates(s, msg->pb_buf, SWIM_MAX_PIGGYBACK_ENTRIES);

    return;
}

static void swim_unpack_message(ssg_t s, swim_message_t *msg)
{
    swim_member_update_t sender_update;

    /* apply (implicit) sender update */
    sender_update.id = msg->source_id;
    sender_update.status = SWIM_MEMBER_ALIVE;
    sender_update.inc_nr = msg->source_inc_nr;
    swim_apply_membership_updates(s, &sender_update, 1);

    /* update membership status using piggybacked membership updates */
    swim_apply_membership_updates(s, msg->pb_buf, SWIM_MAX_PIGGYBACK_ENTRIES);

    return;
}

/* manual serialization/deserialization routine for swim messages */
static hg_return_t hg_proc_swim_message_t(hg_proc_t proc, void *data)
{
    swim_message_t *msg = (swim_message_t *)data;
    hg_return_t hret = HG_PROTOCOL_ERROR;
    int i;

    switch(hg_proc_get_op(proc))
    {
        case HG_ENCODE:
            hret = hg_proc_swim_member_id_t(proc, &(msg->source_id));
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
            for(i = 0; i < SWIM_MAX_PIGGYBACK_ENTRIES; i++)
            {
                hret = hg_proc_swim_member_update_t(proc, &(msg->pb_buf[i]));
                if(hret != HG_SUCCESS)
                {
                    hret = HG_PROTOCOL_ERROR;
                    return hret;
                }
            }
            break;
        case HG_DECODE:
            hret = hg_proc_swim_member_id_t(proc, &(msg->source_id));
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
            for(i = 0; i < SWIM_MAX_PIGGYBACK_ENTRIES; i++)
            {
                hret = hg_proc_swim_member_update_t(proc, &(msg->pb_buf[i]));
                if(hret != HG_SUCCESS)
                {
                    hret = HG_PROTOCOL_ERROR;
                    return hret;
                }
            }
            break;
        case HG_FREE:
            /* do nothing */
            hret = HG_SUCCESS;
            break;
        default:
            break;
    }

    return(hret);
}
