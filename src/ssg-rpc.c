/*
 * Copyright (c) 2016 UChicago Argonne, LLC
 *
 * See COPYRIGHT in top-level directory.
 */

#include "ssg-config.h"

#include <stdlib.h>
#include <assert.h>

#include <mercury.h>
#include <abt.h>
#include <margo.h>
#include <margo-logging.h>

#include "ssg.h"
#include "ssg-internal.h"

#define SSG_VIEW_BUF_DEF_SIZE (128 * 1024)
#define SSG_DEF_RPC_TIMEOUT (5 * 1000.0)

/* SSG RPC types and (de)serialization routines */

/* TODO join and refresh are nearly identical -- refactor */

MERCURY_GEN_STRUCT_PROC(ssg_group_config_t, \
    ((int32_t) (swim_period_length_ms))
    ((int32_t) (swim_suspect_timeout_periods))
    ((int32_t) (swim_subgroup_member_count))
    ((uint8_t) (swim_disabled))
    ((int64_t) (ssg_credential)));

MERCURY_GEN_PROC(ssg_group_join_request_t, \
    ((ssg_group_id_t)   (g_id))
    ((hg_string_t)      (addr_str))
    ((hg_bulk_t)        (bulk_handle)));
MERCURY_GEN_PROC(ssg_group_join_response_t, \
    ((uint32_t)             (group_size))
    ((ssg_group_config_t)   (group_config))
    ((hg_size_t)            (view_buf_size))
    ((uint32_t)             (ret)));

MERCURY_GEN_PROC(ssg_group_leave_request_t, \
    ((ssg_group_id_t)   (g_id)) \
    ((ssg_member_id_t)  (member_id)));
MERCURY_GEN_PROC(ssg_group_leave_response_t, \
    ((uint32_t)  (ret)));

MERCURY_GEN_PROC(ssg_group_refresh_request_t, \
    ((ssg_group_id_t)   (g_id)) \
    ((hg_bulk_t)        (bulk_handle)));

MERCURY_GEN_PROC(ssg_group_refresh_response_t, \
    ((uint32_t)     (group_size)) \
    ((hg_size_t)    (view_buf_size))
    ((uint32_t)     (ret)));

/* SSG RPC handler prototypes */
DECLARE_MARGO_RPC_HANDLER(ssg_group_join_recv_ult)
DECLARE_MARGO_RPC_HANDLER(ssg_group_leave_recv_ult)
DECLARE_MARGO_RPC_HANDLER(ssg_group_refresh_recv_ult)

/* internal helper routine prototypes */
static int ssg_group_serialize(
    ssg_group_descriptor_t *gd, void **buf, hg_size_t *buf_size, int *group_size);

/* ssg_register_rpcs
 *
 *
 */
void ssg_register_rpcs(
    ssg_mid_state_t *mid_state)
{
    /* register RPCs for SSG */
    mid_state->join_rpc_id =
        MARGO_REGISTER(mid_state->mid, "ssg_group_join",
        ssg_group_join_request_t, ssg_group_join_response_t,
        ssg_group_join_recv_ult);
    mid_state->leave_rpc_id =
        MARGO_REGISTER(mid_state->mid, "ssg_group_leave",
        ssg_group_leave_request_t, ssg_group_leave_response_t,
        ssg_group_leave_recv_ult);
    mid_state->refresh_rpc_id =
		MARGO_REGISTER(mid_state->mid, "ssg_group_refresh",
        ssg_group_refresh_request_t, ssg_group_refresh_response_t,
        ssg_group_refresh_recv_ult);

    swim_register_ping_rpcs(mid_state);

    return;
}

/* ssg_deregister_rpcs
 *
 *
 */
void ssg_deregister_rpcs(
    ssg_mid_state_t *mid_state)
{
    /* de-register RPCs for SSG */
    margo_deregister(mid_state->mid, mid_state->join_rpc_id);
    margo_deregister(mid_state->mid, mid_state->leave_rpc_id);
    margo_deregister(mid_state->mid, mid_state->refresh_rpc_id);

    swim_deregister_ping_rpcs(mid_state);

    return;
}

/* ssg_group_join_send
 *
 *
 */
int ssg_group_join_send(
    ssg_group_id_t g_id,
    hg_addr_t target_addr,
    ssg_mid_state_t * mid_state,
    int * group_size,
    ssg_group_config_t * group_config,
    void ** view_buf)
{
    hg_handle_t handle = HG_HANDLE_NULL;
    hg_bulk_t bulk_handle = HG_BULK_NULL;
    void *tmp_view_buf = NULL, *b;
    hg_size_t tmp_view_buf_size = SSG_VIEW_BUF_DEF_SIZE;
    ssg_group_join_request_t join_req;
    ssg_group_join_response_t join_resp;
    hg_return_t hret;
    int ret;

    *group_size = 0;
    *view_buf = NULL;

    /* send join request to given group member */
    hret = margo_create(mid_state->mid, target_addr, mid_state->join_rpc_id,
        &handle);
    if (hret != HG_SUCCESS)
    {
        ret = SSG_MAKE_HG_ERROR(hret);
        goto fini;
    }

    /* allocate a buffer to try to store the group view in */
    /* NOTE: We don't know if this buffer is big enough to store the complete
     * view. If the buffer is not large enough, the group member we are
     * sending to will send a NACK indicating the necessary buffer size
     */
    tmp_view_buf = malloc(tmp_view_buf_size);
    if (!tmp_view_buf)
    {
        ret = SSG_ERR_ALLOCATION;
        goto fini;
    }

    hret = margo_bulk_create(mid_state->mid, 1, &tmp_view_buf, &tmp_view_buf_size,
        HG_BULK_WRITE_ONLY, &bulk_handle);
    if (hret != HG_SUCCESS)
    {
        ret = SSG_MAKE_HG_ERROR(hret);
        goto fini;
    }

    /* send join request to the target group member address */
    join_req.g_id = g_id;
    join_req.addr_str = mid_state->self_addr_str;
    join_req.bulk_handle = bulk_handle;
    hret = margo_forward_timed(handle, &join_req, SSG_DEF_RPC_TIMEOUT);
    if (hret != HG_SUCCESS)
    {
        margo_error(mid_state->mid, "[ssg] unable to forward group join RPC");
        ret = SSG_MAKE_HG_ERROR(hret);
        goto fini;
    }

    hret = margo_get_output(handle, &join_resp);
    if (hret != HG_SUCCESS)
    {
        ret = SSG_MAKE_HG_ERROR(hret);
        goto fini;
    }

    /* if our initial buffer is too small, reallocate to the exact size & rejoin */
    while (join_resp.ret == SSG_ERR_NOBUFS)
    {
        b = realloc(tmp_view_buf, join_resp.view_buf_size);
        if(!b)
        {
            margo_free_output(handle, &join_resp);
            ret = SSG_ERR_ALLOCATION;
            goto fini;
        }
        tmp_view_buf = b;
        tmp_view_buf_size = join_resp.view_buf_size;
        margo_free_output(handle, &join_resp);

        /* free old bulk handle and recreate it */
        margo_bulk_free(bulk_handle);
        hret = margo_bulk_create(mid_state->mid, 1, &tmp_view_buf,
            &tmp_view_buf_size, HG_BULK_WRITE_ONLY, &bulk_handle);
        if (hret != HG_SUCCESS)
        {
            ret = SSG_MAKE_HG_ERROR(hret);
            goto fini;
        }

        join_req.bulk_handle = bulk_handle;
        hret = margo_forward_timed(handle, &join_req, SSG_DEF_RPC_TIMEOUT);
        if (hret != HG_SUCCESS)
        {
            margo_error(mid_state->mid, "[ssg] unable to forward group join RPC");
            ret = SSG_MAKE_HG_ERROR(hret);
            goto fini;
        }

        hret = margo_get_output(handle, &join_resp);
        if (hret != HG_SUCCESS)
        {
            ret = SSG_MAKE_HG_ERROR(hret);
            goto fini;
        }
    }

    /* readjust view buf size if initial guess was too large */
    if ((join_resp.view_buf_size < tmp_view_buf_size) &&
        (join_resp.ret == SSG_SUCCESS))
    {
        b = realloc(tmp_view_buf, join_resp.view_buf_size);
        if(!b)
        {
            margo_free_output(handle, &join_resp);
            ret = SSG_ERR_ALLOCATION;
            goto fini;
        }
        tmp_view_buf = b;
    }

    /* set output pointers according to the returned view parameters */
    *group_size = (int)join_resp.group_size;
    memcpy(group_config, &join_resp.group_config, sizeof(*group_config));
    ret = join_resp.ret;
    margo_free_output(handle, &join_resp);

    if (ret == SSG_SUCCESS)
    {
        *view_buf = tmp_view_buf;
        tmp_view_buf = NULL; /* don't free on success */
    }
fini:
    if (handle != HG_HANDLE_NULL) margo_destroy(handle);
    if (bulk_handle != HG_BULK_NULL) margo_bulk_free(bulk_handle);
    free(tmp_view_buf);

    return ret;
}

static void ssg_group_join_recv_ult(
    hg_handle_t handle)
{
    const struct hg_info *hgi = NULL;
    margo_instance_id mid;
    ssg_group_descriptor_t *gd = NULL;
    ssg_group_join_request_t join_req;
    ssg_group_join_response_t join_resp;
    hg_size_t view_size_requested;
    void *view_buf = NULL;
    hg_size_t view_buf_size = 0;
    hg_bulk_t bulk_handle = HG_BULK_NULL;
    ssg_member_update_t join_update;
    int group_size = 0;
    hg_return_t hret;
    int ret;

    if (!ssg_rt)
    {
        ret = SSG_ERR_NOT_INITIALIZED;
        goto fini;
    }

    hgi = margo_get_info(handle);
    if (!hgi)
    {
        ret = SSG_ERR_MARGO_FAILURE;
        goto fini;
    }
    mid = margo_hg_info_get_instance(hgi);
    if (mid == MARGO_INSTANCE_NULL)
    {
        ret = SSG_ERR_MARGO_FAILURE;
        goto fini;
    }

    hret = margo_get_input(handle, &join_req);
    if (hret != HG_SUCCESS)
    {
        ret = SSG_MAKE_HG_ERROR(hret);
        goto fini;
    }
    view_size_requested = margo_bulk_get_size(join_req.bulk_handle);

    /* look for the given group in my local table of groups */
    SSG_GROUP_READ(join_req.g_id, gd);
    if (!gd)
    {
        margo_error(MARGO_INSTANCE_NULL, "[ssg] unable to find group for join request");
        margo_free_input(handle, &join_req);
        ret = SSG_ERR_GROUP_NOT_FOUND;
        goto fini;
    }

    SSG_DEBUG(gd->mid_state, "received join request for group %lu from member %s\n",
        join_req.g_id, join_req.addr_str);

    /* can't accept join requests if we are not a member ourselves */
    if(!gd->is_member)
    {
        margo_error(gd->mid_state->mid,
            "[ssg] unable to accept join request for group as non-member");
        SSG_GROUP_RELEASE(gd);
        margo_free_input(handle, &join_req);
        ret  = SSG_ERR_NOT_SUPPORTED;
        goto fini;
    }

    /* dynamic groups can't be supported if SWIM is disabled */
    if (gd->group->config.swim_disabled)
    {
        margo_error(gd->mid_state->mid,
            "[ssg] unable to join group if SWIM is disabled");
        SSG_GROUP_RELEASE(gd);
        margo_free_input(handle, &join_req);
        ret  = SSG_ERR_NOT_SUPPORTED;
        goto fini;
    }

    /* serialize group view and stash some other group metadata for response */
    ret = ssg_group_serialize(gd, &view_buf, &view_buf_size, &group_size);
    if (ret != SSG_SUCCESS)
    {
        SSG_GROUP_RELEASE(gd);
        margo_free_input(handle, &join_req);
        goto fini;
    }

    /* prevent group from being destroyed from underneath us while
     * we potentially callback to users to report membership updates
     */
    SSG_GROUP_REF_INCR(gd);
    SSG_GROUP_RELEASE(gd);

    if (view_size_requested < view_buf_size)
    {
        SSG_GROUP_REF_DECR(gd);
        margo_free_input(handle, &join_req);
        ret = SSG_ERR_NOBUFS;
        goto fini;
    }

    /* if request buf is large enough, transfer the view */
    hret = margo_bulk_create(mid, 1, &view_buf, &view_buf_size,
        HG_BULK_READ_ONLY, &bulk_handle);
    if (hret != HG_SUCCESS)
    {
        SSG_GROUP_REF_DECR(gd);
        margo_free_input(handle, &join_req);
        ret = SSG_MAKE_HG_ERROR(hret);
        goto fini;
    }

    hret = margo_bulk_transfer(mid, HG_BULK_PUSH, hgi->addr,
        join_req.bulk_handle, 0, bulk_handle, 0, view_buf_size);
    if (hret != HG_SUCCESS)
    {
        SSG_GROUP_REF_DECR(gd);
        margo_free_input(handle, &join_req);
        ret = SSG_MAKE_HG_ERROR(hret);
        goto fini;
    }

    /* apply group join locally */
    join_update.type = SSG_MEMBER_JOINED;
    join_update.u.member_addr_str = join_req.addr_str;
    ret = ssg_apply_member_updates(gd, &join_update, 1, 1);

    memcpy(&join_resp.group_config, &gd->group->config,
        sizeof(join_resp.group_config));
    SSG_GROUP_REF_DECR(gd);
    margo_free_input(handle, &join_req);
fini:
    /* respond */
    join_resp.ret = ret;
    join_resp.group_size = group_size;
    join_resp.view_buf_size = view_buf_size;
    margo_respond(handle, &join_resp);

    /* cleanup */
    if (bulk_handle != HG_BULK_NULL) margo_bulk_free(bulk_handle);
    margo_destroy(handle);
    free(view_buf);

    return;
}
DEFINE_MARGO_RPC_HANDLER(ssg_group_join_recv_ult)

/* ssg_group_leave_send
 *
 *
 */
int ssg_group_leave_send(
    ssg_group_id_t g_id,
    hg_addr_t target_addr,
    ssg_mid_state_t * mid_state)
{
    hg_handle_t handle = HG_HANDLE_NULL;
    ssg_group_leave_request_t leave_req;
    ssg_group_leave_response_t leave_resp;
    hg_return_t hret;
    int ret;

    /* send leave request to given group member */
    hret = margo_create(mid_state->mid, target_addr, mid_state->leave_rpc_id,
        &handle);
    if (hret != HG_SUCCESS)
    {
        ret = SSG_MAKE_HG_ERROR(hret);
        goto fini;
    }

    leave_req.g_id = g_id;
    leave_req.member_id = mid_state->self_id;
    hret = margo_forward_timed(handle, &leave_req, SSG_DEF_RPC_TIMEOUT);
    if (hret != HG_SUCCESS)
    {
        margo_error(mid_state->mid, "[ssg] unable to forward group leave RPC");
        ret = SSG_MAKE_HG_ERROR(hret);
        goto fini;
    }

    hret = margo_get_output(handle, &leave_resp);
    if (hret != HG_SUCCESS)
    {
        ret = SSG_MAKE_HG_ERROR(hret);
        goto fini;
    }

    ret = leave_resp.ret;
    margo_free_output(handle, &leave_resp);
fini:
    if (handle != HG_HANDLE_NULL) margo_destroy(handle);

    return ret;
}

static void ssg_group_leave_recv_ult(
    hg_handle_t handle)
{
    const struct hg_info *hgi = NULL;
    margo_instance_id mid;
    ssg_group_descriptor_t *gd = NULL;
    ssg_group_leave_request_t leave_req;
    ssg_group_leave_response_t leave_resp;
    ssg_member_update_t leave_update;
    hg_return_t hret;
    int ret;

    if (!ssg_rt)
    {
        ret = SSG_ERR_NOT_INITIALIZED;
        goto fini;
    }

    hgi = margo_get_info(handle);
    if (!hgi)
    {
        ret = SSG_ERR_MARGO_FAILURE;
        goto fini;
    }
    mid = margo_hg_info_get_instance(hgi);
    if (mid == MARGO_INSTANCE_NULL)
    {
        ret = SSG_ERR_MARGO_FAILURE;
        goto fini;
    }

    hret = margo_get_input(handle, &leave_req);
    if (hret != HG_SUCCESS)
    {
        ret = SSG_MAKE_HG_ERROR(hret);
        goto fini;
    }

    /* look for the given group in my local table of groups */
    SSG_GROUP_READ(leave_req.g_id, gd);
    if (!gd)
    {
        margo_error(MARGO_INSTANCE_NULL,
            "[ssg] unable to find group for leave request");
        margo_free_input(handle, &leave_req);
        ret = SSG_ERR_GROUP_NOT_FOUND;
        goto fini;
    }

    SSG_DEBUG(gd->mid_state, "received leave request for group %lu from member %lu\n",
        leave_req.g_id, leave_req.member_id);

    /* can't accept leave requests if we are not a member ourselves */
    if(!gd->is_member)
    {
        margo_error(gd->mid_state->mid,
            "[ssg] unable to accept leave request for group as non-member");
        SSG_GROUP_RELEASE(gd);
        margo_free_input(handle, &leave_req);
        ret  = SSG_ERR_NOT_SUPPORTED;
        goto fini;
    }

    /* prevent group from being destroyed from underneath us while
     * we potentially callback to users to report membership updates
     */
    SSG_GROUP_REF_INCR(gd);
    SSG_GROUP_RELEASE(gd);

    /* apply group leave locally */
    leave_update.type = SSG_MEMBER_LEFT;
    leave_update.u.member_id = leave_req.member_id;
    ret = ssg_apply_member_updates(gd, &leave_update, 1, 1);

    SSG_GROUP_REF_DECR(gd);
    margo_free_input(handle, &leave_req);
fini:
    /* respond */
    leave_resp.ret = ret;
    margo_respond(handle, &leave_resp);

    /* cleanup */
    margo_destroy(handle);

    return;
}
DEFINE_MARGO_RPC_HANDLER(ssg_group_leave_recv_ult)

/* ssg_group_refresh_send
 *
 *
 */
int ssg_group_refresh_send(
    ssg_group_id_t g_id,
    hg_addr_t target_addr,
    ssg_mid_state_t * mid_state,
    int * group_size,
    void ** view_buf)
{
    hg_handle_t handle = HG_HANDLE_NULL;
    hg_bulk_t bulk_handle = HG_BULK_NULL;
    void *tmp_view_buf = NULL, *b;
    hg_size_t tmp_view_buf_size = SSG_VIEW_BUF_DEF_SIZE;
    ssg_group_refresh_request_t refresh_req;
    ssg_group_refresh_response_t refresh_resp;
    hg_return_t hret;
    int ret;

    *group_size = 0;
    *view_buf = NULL;

    /* send refresh request to given group member */
    hret = margo_create(mid_state->mid, target_addr, mid_state->refresh_rpc_id,
        &handle);
    if (hret != HG_SUCCESS)
    {
        ret = SSG_MAKE_HG_ERROR(hret);
        goto fini;
    }

    /* allocate a buffer of the given size to try to store the group view in */
    /* NOTE: We don't know if this buffer is big enough to store the complete
     * view. If the buffers is not large enough, the group member we are
     * sending to will send a NACK indicating the necessary buffer size
     */
    tmp_view_buf = malloc(tmp_view_buf_size);
    if (!tmp_view_buf)
    {
        ret = SSG_ERR_ALLOCATION;
        goto fini;
    }

    hret = margo_bulk_create(mid_state->mid, 1, &tmp_view_buf, &tmp_view_buf_size,
        HG_BULK_WRITE_ONLY, &bulk_handle);
    if (hret != HG_SUCCESS)
    {
        ret = SSG_MAKE_HG_ERROR(hret);
        goto fini;
    }

    /* send refresh request to the target group member address */
    refresh_req.g_id = g_id;
    refresh_req.bulk_handle = bulk_handle;
    hret = margo_forward_timed(handle, &refresh_req, SSG_DEF_RPC_TIMEOUT);
    if (hret != HG_SUCCESS)
    {
        margo_error(mid_state->mid, "[ssg] unable to forward group refresh RPC");
        ret = SSG_MAKE_HG_ERROR(hret);
        goto fini;
    }

    hret = margo_get_output(handle, &refresh_resp);
    if (hret != HG_SUCCESS)
    {
        ret = SSG_MAKE_HG_ERROR(hret);
        goto fini;
    }

    /* if our initial buffer is too small, reallocate to the exact size & resend */
    while (refresh_resp.ret == SSG_ERR_NOBUFS)
    {
        b = realloc(tmp_view_buf, refresh_resp.view_buf_size);
        if(!b)
        {
            margo_free_output(handle, &refresh_resp);
            ret = SSG_ERR_ALLOCATION;
            goto fini;
        }
        tmp_view_buf = b;
        tmp_view_buf_size = refresh_resp.view_buf_size;
        margo_free_output(handle, &refresh_resp);

        /* free old bulk handle and recreate it */
        margo_bulk_free(bulk_handle);
        hret = margo_bulk_create(mid_state->mid, 1, &tmp_view_buf,
            &tmp_view_buf_size, HG_BULK_WRITE_ONLY, &bulk_handle);
        if (hret != HG_SUCCESS)
        {
            ret = SSG_MAKE_HG_ERROR(hret);
            goto fini;
        }

        refresh_req.bulk_handle = bulk_handle;
        hret = margo_forward_timed(handle, &refresh_req, SSG_DEF_RPC_TIMEOUT);
        if (hret != HG_SUCCESS)
        {
            margo_error(mid_state->mid, "[ssg] unable to forward group refresh RPC");
            ret = SSG_MAKE_HG_ERROR(hret);
            goto fini;
        }

        hret = margo_get_output(handle, &refresh_resp);
        if (hret != HG_SUCCESS)
        {
            ret = SSG_MAKE_HG_ERROR(hret);
            goto fini;
        }
    }

    /* readjust view buf size if initial guess was too large */
    if ((refresh_resp.view_buf_size < tmp_view_buf_size) &&
        ((refresh_resp.ret == SSG_SUCCESS)))
    {
        b = realloc(tmp_view_buf, refresh_resp.view_buf_size);
        if(!b)
        {
            margo_free_output(handle, &refresh_resp);
            ret = SSG_ERR_ALLOCATION;
            goto fini;
        }
        tmp_view_buf = b;
    }

    /* set output pointers according to the returned response */
    *group_size = (int)refresh_resp.group_size;
    ret = refresh_resp.ret;
    margo_free_output(handle, &refresh_resp);

    if (ret == SSG_SUCCESS)
    {
        *view_buf = tmp_view_buf;
        tmp_view_buf = NULL; /* don't free on success */
    }
fini:
    if (handle != HG_HANDLE_NULL) margo_destroy(handle);
    if (bulk_handle != HG_BULK_NULL) margo_bulk_free(bulk_handle);
    free(tmp_view_buf);

    return ret;
}

static void ssg_group_refresh_recv_ult(
    hg_handle_t handle)
{
    const struct hg_info *hgi = NULL;
    margo_instance_id mid;
    ssg_group_descriptor_t *gd = NULL;
    ssg_group_refresh_request_t refresh_req;
    ssg_group_refresh_response_t refresh_resp;
    hg_size_t view_size_requested;
    void *view_buf = NULL;
    hg_size_t view_buf_size = 0;
    hg_bulk_t bulk_handle = HG_BULK_NULL;
    int group_size = 0;
    int ret;
    hg_return_t hret;

    if(!ssg_rt)
    {
        ret = SSG_ERR_NOT_INITIALIZED;
        goto fini;
    }

    hgi = margo_get_info(handle);
    if (!hgi)
    {
        ret = SSG_ERR_MARGO_FAILURE;
        goto fini;
    }
    mid = margo_hg_info_get_instance(hgi);
    if (mid == MARGO_INSTANCE_NULL)
    {
        ret = SSG_ERR_MARGO_FAILURE;
        goto fini;
    }

    hret = margo_get_input(handle, &refresh_req);
    if (hret != HG_SUCCESS)
    {
        ret = SSG_MAKE_HG_ERROR(hret);
        goto fini;
    }
    view_size_requested = margo_bulk_get_size(refresh_req.bulk_handle);

    /* look for the given group in my local table of groups */
    SSG_GROUP_READ(refresh_req.g_id, gd);
    if (!gd)
    {
        margo_error(MARGO_INSTANCE_NULL, "[ssg] unable to find group for refresh request");
        margo_free_input(handle, &refresh_req);
        ret = SSG_ERR_GROUP_NOT_FOUND;
        goto fini;
    }

    SSG_DEBUG(gd->mid_state, "received refresh request for group %lu\n",
        refresh_req.g_id);

    /* serialize group view and stash some other group metadata for response */
    ret = ssg_group_serialize(gd, &view_buf, &view_buf_size, &group_size);
    if (ret != SSG_SUCCESS)
    {
        SSG_GROUP_RELEASE(gd);
        margo_free_input(handle, &refresh_req);
        goto fini;
    }

    SSG_GROUP_RELEASE(gd);

    if (view_size_requested < view_buf_size)
    {
        margo_free_input(handle, &refresh_req);
        ret = SSG_ERR_NOBUFS;
        goto fini;
    }

    /* if request buf is large enough, transfer the view */
    hret = margo_bulk_create(mid, 1, &view_buf, &view_buf_size,
        HG_BULK_READ_ONLY, &bulk_handle);
    if (hret != HG_SUCCESS)
    {
        margo_free_input(handle, &refresh_req);
        ret = SSG_MAKE_HG_ERROR(hret);
        goto fini;
    }

    hret = margo_bulk_transfer(mid, HG_BULK_PUSH, hgi->addr,
        refresh_req.bulk_handle, 0, bulk_handle, 0, view_buf_size);
    if (hret != HG_SUCCESS)
    {
        margo_free_input(handle, &refresh_req);
        ret = SSG_MAKE_HG_ERROR(hret);
        goto fini;
    }

    ret = SSG_SUCCESS;
    margo_free_input(handle, &refresh_req);
fini:
    /* respond */
    refresh_resp.ret = ret;
    refresh_resp.group_size = group_size;
    refresh_resp.view_buf_size = view_buf_size;
    margo_respond(handle, &refresh_resp);

    /* cleanup */
    if (bulk_handle != HG_BULK_NULL) margo_bulk_free(bulk_handle);
    margo_destroy(handle);
    free(view_buf);

    return;
}
DEFINE_MARGO_RPC_HANDLER(ssg_group_refresh_recv_ult)

static int ssg_group_serialize(
    ssg_group_descriptor_t *gd, void **buf, hg_size_t *buf_size, int *group_size)
{
    ssg_member_state_t *member_state, *tmp;
    hg_size_t group_buf_size = 0;
    void *group_buf;
    char *buf_p;
    int sz;

    *buf = NULL;
    *buf_size = 0;
    *group_size = 0;

    /* first determine size */
    if (gd->is_member)
    {
        group_buf_size = strlen(gd->mid_state->self_addr_str) + 1;
    }
    HASH_ITER(hh, gd->view->member_map, member_state, tmp)
    {
        group_buf_size += strlen(member_state->addr_str) + 1;
    }

    group_buf = malloc(group_buf_size);
    if (!group_buf)
    {
        return SSG_ERR_ALLOCATION;
    }

    buf_p = group_buf;
    if (gd->is_member)
    {
        strcpy(buf_p, gd->mid_state->self_addr_str);
        buf_p += strlen(gd->mid_state->self_addr_str) + 1;
    }
    HASH_ITER(hh, gd->view->member_map, member_state, tmp)
    {
        strcpy(buf_p, member_state->addr_str);
        buf_p += strlen(member_state->addr_str) + 1;
    }

    sz = gd->view->size;

    *buf = group_buf;
    *buf_size = group_buf_size;
    *group_size = sz;

    return SSG_SUCCESS;
}

/* custom SSG RPC proc routines */

hg_return_t hg_proc_ssg_member_update_t(
    hg_proc_t proc, void *data)
{
    ssg_member_update_t *update = (ssg_member_update_t *)data;
    hg_return_t hret = HG_PROTOCOL_ERROR;

    switch(hg_proc_get_op(proc))
    {
        case HG_ENCODE:
            hret = hg_proc_uint8_t(proc, &(update->type));
            if (hret != HG_SUCCESS)
            {
                hret = HG_PROTOCOL_ERROR;
                return hret;
            }
            if (update->type == SSG_MEMBER_JOINED)
            {
                hret = hg_proc_hg_string_t(proc, &(update->u.member_addr_str));
                if (hret != HG_SUCCESS)
                {
                    hret = HG_PROTOCOL_ERROR;
                    return hret;
                }
            }
            else if (update->type == SSG_MEMBER_LEFT || update->type == SSG_MEMBER_DIED)
            {
                hret = hg_proc_ssg_member_id_t(proc, &(update->u.member_id));
                if (hret != HG_SUCCESS)
                {
                    hret = HG_PROTOCOL_ERROR;
                    return hret;
                }
            }
            else
            {
                hret = HG_PROTOCOL_ERROR;
            }
            break;
        case HG_DECODE:
            hret = hg_proc_uint8_t(proc, &(update->type));
            if (hret != HG_SUCCESS)
            {
                hret = HG_PROTOCOL_ERROR;
                return hret;
            }
            if (update->type == SSG_MEMBER_JOINED)
            {
                hret = hg_proc_hg_string_t(proc, &(update->u.member_addr_str));
                if (hret != HG_SUCCESS)
                {
                    hret = HG_PROTOCOL_ERROR;
                    return hret;
                }
            }
            else if (update->type == SSG_MEMBER_LEFT || update->type == SSG_MEMBER_DIED)
            {
                hret = hg_proc_ssg_member_id_t(proc, &(update->u.member_id));
                if (hret != HG_SUCCESS)
                {
                    hret = HG_PROTOCOL_ERROR;
                    return hret;
                }
            }
            else
            {
                hret = HG_PROTOCOL_ERROR;
            }
            break;
        case HG_FREE:
            if (update->type == SSG_MEMBER_JOINED)
            {
                hret = hg_proc_hg_string_t(proc, &(update->u.member_addr_str));
                if (hret != HG_SUCCESS)
                {
                    hret = HG_PROTOCOL_ERROR;
                    return hret;
                }
            }
            else
            {
                hret = HG_SUCCESS;
            }
            break;
        default:
            break;
    }

    return hret;
}
