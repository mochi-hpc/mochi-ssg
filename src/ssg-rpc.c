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

#include "ssg.h"
#include "ssg-internal.h"

#define SSG_VIEW_BUF_DEF_SIZE (128 * 1024)

/* SSG RPC types and (de)serialization routines */

/* TODO join and observe are nearly identical -- refactor */

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
    ((hg_string_t)          (group_name))
    ((uint32_t)             (group_size))
    ((ssg_group_config_t)   (group_config))
    ((hg_size_t)            (view_buf_size))
    ((uint32_t)              (ret)));

MERCURY_GEN_PROC(ssg_group_leave_request_t, \
    ((ssg_group_id_t)   (g_id)) \
    ((ssg_member_id_t)  (member_id)));
MERCURY_GEN_PROC(ssg_group_leave_response_t, \
    ((uint32_t)  (ret)));

MERCURY_GEN_PROC(ssg_group_observe_request_t, \
    ((ssg_group_id_t)   (g_id)) \
    ((hg_bulk_t)        (bulk_handle)));

MERCURY_GEN_PROC(ssg_group_observe_response_t, \
    ((hg_string_t)  (group_name)) \
    ((uint32_t)     (group_size)) \
    ((hg_size_t)    (view_buf_size))
    ((uint32_t)      (ret)));

/* SSG RPC handler prototypes */
DECLARE_MARGO_RPC_HANDLER(ssg_group_join_recv_ult)
DECLARE_MARGO_RPC_HANDLER(ssg_group_leave_recv_ult)
DECLARE_MARGO_RPC_HANDLER(ssg_group_observe_recv_ult)

/* internal helper routine prototypes */
static int ssg_group_serialize(
    ssg_group_t *g, void **buf, hg_size_t *buf_size, int *group_size);

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
    mid_state->observe_rpc_id =
		MARGO_REGISTER(mid_state->mid, "ssg_group_observe",
        ssg_group_observe_request_t, ssg_group_observe_response_t,
        ssg_group_observe_recv_ult);

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
    margo_deregister(mid_state->mid, mid_state->observe_rpc_id);

    swim_deregister_ping_rpcs(mid_state);

    return;
}

/* ssg_group_join_send
 *
 *
 */
int ssg_group_join_send(
    ssg_group_id_t g_id,
    hg_addr_t group_target_addr,
    ssg_mid_state_t * mid_state,
    char ** group_name,
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

    *group_name = NULL;
    *group_size = 0;
    *view_buf = NULL;

    /* send join request to given group member */
    hret = margo_create(mid_state->mid, group_target_addr,
        mid_state->join_rpc_id, &handle);
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

    hret = margo_bulk_create(mid_state->mid, 1, &tmp_view_buf,
        &tmp_view_buf_size, HG_BULK_WRITE_ONLY, &bulk_handle);
    if (hret != HG_SUCCESS)
    {
        ret = SSG_MAKE_HG_ERROR(hret);
        goto fini;
    }

    join_req.g_id = g_id;
    join_req.addr_str = mid_state->self_addr_str;
    join_req.bulk_handle = bulk_handle;
    hret = margo_forward(handle, &join_req);
    if (hret != HG_SUCCESS)
    {
        fprintf(stderr, "Error: SSG unable to forward group join RPC\n");
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
    while (join_resp.view_buf_size > tmp_view_buf_size)
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
        hret = margo_forward(handle, &join_req);
        if (hret != HG_SUCCESS)
        {
            fprintf(stderr, "Error: SSG unable to forward group join RPC\n");
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
    if (join_resp.view_buf_size < tmp_view_buf_size)
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
    if(join_resp.group_name)
        *group_name = strdup(join_resp.group_name);
    if(!(*group_name))
    {
        margo_free_output(handle, &join_resp);
        ret = SSG_ERR_ALLOCATION;
        goto fini;
    }
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
    ssg_group_descriptor_t *g_desc = NULL;
    ssg_group_join_request_t join_req;
    ssg_group_join_response_t join_resp;
    hg_size_t view_size_requested;
    void *view_buf = NULL;
    hg_size_t view_buf_size;
    hg_bulk_t bulk_handle = HG_BULK_NULL;
    ssg_member_update_t join_update;
    int group_size = 0;
    char *group_name = NULL;
    int ret;
    hg_return_t hret;

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

    ABT_rwlock_rdlock(ssg_rt->lock);

    /* look for the given group in my local table of groups */
    HASH_FIND(hh, ssg_rt->g_desc_table, &join_req.g_id,
        sizeof(ssg_group_id_t), g_desc);
    if (!g_desc)
    {
        ABT_rwlock_unlock(ssg_rt->lock);
        fprintf(stderr, "Error: SSG unable to find group for join request\n");
        margo_free_input(handle, &join_req);
        ret = SSG_ERR_GROUP_NOT_FOUND;
        goto fini;
    }
    /* sanity checks */
    assert(g_desc->owner_status == SSG_OWNER_IS_MEMBER);
    assert(mid == g_desc->g_data.g->mid_state->mid);

    /* dynamic groups can't be supported if SWIM is disabled */
    if (g_desc->g_data.g->config.swim_disabled)
    {
        ABT_rwlock_unlock(ssg_rt->lock);
        fprintf(stderr, "Error: SSG unable to join group if SWIM is disabled\n");
        margo_free_input(handle, &join_req);
        ret  = SSG_ERR_NOT_SUPPORTED;
        goto fini;
    }

    SSG_GROUP_REF_INCR(g_desc->g_data.g);
    ABT_rwlock_unlock(ssg_rt->lock);

    /* serialize group view and stash some other group metadata for response */
    ret = ssg_group_serialize(g_desc->g_data.g, &view_buf, &view_buf_size, &group_size);
    if (ret != SSG_SUCCESS)
    {
        SSG_GROUP_REF_DECR(g_desc->g_data.g);
        margo_free_input(handle, &join_req);
        goto fini;
    }
    join_resp.view_buf_size = view_buf_size;

    if (view_size_requested < view_buf_size)
    {
        SSG_GROUP_REF_DECR(g_desc->g_data.g);
        margo_free_input(handle, &join_req);
        ret = SSG_ERR_NOBUFS;
        goto fini;
    }

    /* if joiner's buf is large enough, transfer the view */
    hret = margo_bulk_create(mid, 1, &view_buf, &view_buf_size,
        HG_BULK_READ_ONLY, &bulk_handle);
    if (hret != HG_SUCCESS)
    {
        SSG_GROUP_REF_DECR(g_desc->g_data.g);
        margo_free_input(handle, &join_req);
        ret = SSG_MAKE_HG_ERROR(hret);
        goto fini;
    }

    hret = margo_bulk_transfer(mid, HG_BULK_PUSH, hgi->addr,
        join_req.bulk_handle, 0, bulk_handle, 0, view_buf_size);
    if (hret != HG_SUCCESS)
    {
        SSG_GROUP_REF_DECR(g_desc->g_data.g);
        margo_free_input(handle, &join_req);
        ret = SSG_MAKE_HG_ERROR(hret);
        goto fini;
    }

    group_name = strdup(g_desc->g_data.g->name);
    if(!group_name)
    {
        SSG_GROUP_REF_DECR(g_desc->g_data.g);
        margo_free_input(handle, &join_req);
        ret = SSG_ERR_ALLOCATION;
        goto fini;
    }

    /* set the response for successful join */
    memcpy(&join_resp.group_config, &g_desc->g_data.g->config,
        sizeof(join_resp.group_config));
    ret = SSG_SUCCESS;

    /* apply group join locally */
    join_update.type = SSG_MEMBER_JOINED;
    join_update.u.member_addr_str = join_req.addr_str;
    ssg_apply_member_updates(g_desc->g_data.g, &join_update, 1, 1);

    SSG_GROUP_REF_DECR(g_desc->g_data.g);

    margo_free_input(handle, &join_req);
fini:
    /* respond */
    join_resp.ret = ret;
    join_resp.group_name = group_name;
    join_resp.group_size = group_size;
    margo_respond(handle, &join_resp);

    /* cleanup */
    if (bulk_handle != HG_BULK_NULL) margo_bulk_free(bulk_handle);
    margo_destroy(handle);
    free(view_buf);
    free(group_name);

    return;
}
DEFINE_MARGO_RPC_HANDLER(ssg_group_join_recv_ult)

/* ssg_group_leave_send
 *
 *
 */
int ssg_group_leave_send(
    ssg_group_id_t g_id,
    hg_addr_t group_target_addr,
    ssg_mid_state_t * mid_state)
{
    hg_handle_t handle = HG_HANDLE_NULL;

    ssg_group_leave_request_t leave_req;
    ssg_group_leave_response_t leave_resp;
    hg_return_t hret;
    int ret;

    /* send leave request to given group member */
    hret = margo_create(mid_state->mid, group_target_addr,
        mid_state->leave_rpc_id, &handle);
    if (hret != HG_SUCCESS)
    {
        ret = SSG_MAKE_HG_ERROR(hret);
        goto fini;
    }

    leave_req.g_id = g_id;
    leave_req.member_id = mid_state->self_id;
    hret = margo_forward(handle, &leave_req);
    if (hret != HG_SUCCESS)
    {
        fprintf(stderr, "Error: SSG unable to forward group leave RPC\n");
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
    ssg_group_descriptor_t *g_desc = NULL;
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

    ABT_rwlock_rdlock(ssg_rt->lock);

    /* look for the given group in my local table of groups */
    HASH_FIND(hh, ssg_rt->g_desc_table, &leave_req.g_id,
        sizeof(ssg_group_id_t), g_desc);
    if (!g_desc)
    {
        ABT_rwlock_unlock(ssg_rt->lock);
        fprintf(stderr, "Error: SSG unable to find group for leave request\n");
        margo_free_input(handle, &leave_req);
        ret = SSG_ERR_GROUP_NOT_FOUND;
        goto fini;
    }
    /* sanity checks */
    assert(g_desc->owner_status == SSG_OWNER_IS_MEMBER);
    assert(mid == g_desc->g_data.g->mid_state->mid);

    SSG_GROUP_REF_INCR(g_desc->g_data.g);
    ABT_rwlock_unlock(ssg_rt->lock);

    /* apply group leave locally */
    leave_update.type = SSG_MEMBER_LEFT;
    leave_update.u.member_id = leave_req.member_id;
    ssg_apply_member_updates(g_desc->g_data.g, &leave_update, 1, 1);

    SSG_GROUP_REF_DECR(g_desc->g_data.g);

    margo_free_input(handle, &leave_req);
    ret = SSG_SUCCESS;
fini:
    /* respond */
    leave_resp.ret = ret;
    margo_respond(handle, &leave_resp);

    /* cleanup */
    margo_destroy(handle);

    return;
}
DEFINE_MARGO_RPC_HANDLER(ssg_group_leave_recv_ult)


/* ssg_group_observe_send
 *
 *
 */
int ssg_group_observe_send(
    ssg_group_id_t g_id,
    hg_addr_t group_target_addr,
    ssg_mid_state_t * mid_state,
    char ** group_name,
    int * group_size,
    void ** view_buf)
{
    hg_handle_t handle = HG_HANDLE_NULL;
    hg_bulk_t bulk_handle = HG_BULK_NULL;
    void *tmp_view_buf = NULL, *b;
    hg_size_t tmp_view_buf_size = SSG_VIEW_BUF_DEF_SIZE;
    ssg_group_observe_request_t observe_req;
    ssg_group_observe_response_t observe_resp;
    hg_return_t hret;
    int ret;

    *group_name = NULL;
    *group_size = 0;
    *view_buf = NULL;

    /* send observe request to given group member */
    hret = margo_create(mid_state->mid, group_target_addr,
        mid_state->observe_rpc_id, &handle);
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

    /* send an observe request to the given group member address */
    observe_req.g_id = g_id;
    observe_req.bulk_handle = bulk_handle;
    ret = margo_forward(handle, &observe_req);
    if (hret != HG_SUCCESS)
    {
        fprintf(stderr, "Error: SSG unable to forward group observe RPC\n");
        ret = SSG_MAKE_HG_ERROR(hret);
        goto fini;
    }

    hret = margo_get_output(handle, &observe_resp);
    if (hret != HG_SUCCESS)
    {
        ret = SSG_MAKE_HG_ERROR(hret);
        goto fini;
    }

    /* if our initial buffer is too small, reallocate to the exact size & resend */
    while (observe_resp.view_buf_size > tmp_view_buf_size)
    {
        b = realloc(tmp_view_buf, observe_resp.view_buf_size);
        if(!b)
        {
            margo_free_output(handle, &observe_resp);
            ret = SSG_ERR_ALLOCATION;
            goto fini;
        }
        tmp_view_buf = b;
        tmp_view_buf_size = observe_resp.view_buf_size;
        margo_free_output(handle, &observe_resp);

        /* free old bulk handle and recreate it */
        margo_bulk_free(bulk_handle);
        hret = margo_bulk_create(mid_state->mid, 1, &tmp_view_buf,
            &tmp_view_buf_size, HG_BULK_WRITE_ONLY, &bulk_handle);
        if (hret != HG_SUCCESS)
        {
            ret = SSG_MAKE_HG_ERROR(hret);
            goto fini;
        }

        observe_req.bulk_handle = bulk_handle;
        hret = margo_forward(handle, &observe_req);
        if (hret != HG_SUCCESS)
        {
            fprintf(stderr, "Error: SSG unable to forward group observe RPC\n");
            ret = SSG_MAKE_HG_ERROR(hret);
            goto fini;
        }

        hret = margo_get_output(handle, &observe_resp);
        if (hret != HG_SUCCESS) goto fini;
        {
            ret = SSG_MAKE_HG_ERROR(hret);
            goto fini;
        }
    }

    /* readjust view buf size if initial guess was too large */
    if (observe_resp.view_buf_size < tmp_view_buf_size)
    {
        b = realloc(tmp_view_buf, observe_resp.view_buf_size);
        if(!b)
        {
            margo_free_output(handle, &observe_resp);
            ret = SSG_ERR_ALLOCATION;
            goto fini;
        }
        tmp_view_buf = b;
    }

    /* set output pointers according to the returned view parameters */
    if(observe_resp.group_name)
        *group_name = strdup(observe_resp.group_name);
    if(!(*group_name))
    {
        margo_free_output(handle, &observe_resp);
        ret = SSG_ERR_ALLOCATION;
        goto fini;
    }
    *group_size = (int)observe_resp.group_size;
    ret = observe_resp.ret;
    margo_free_output(handle, &observe_resp);

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

static void ssg_group_observe_recv_ult(
    hg_handle_t handle)
{
    const struct hg_info *hgi = NULL;
    margo_instance_id mid;
    ssg_group_descriptor_t *g_desc = NULL;
    ssg_group_observe_request_t observe_req;
    ssg_group_observe_response_t observe_resp;
    hg_size_t view_size_requested;
    void *view_buf = NULL;
    hg_size_t view_buf_size;
    hg_bulk_t bulk_handle = HG_BULK_NULL;
    int group_size = 0;
    char *group_name = NULL;
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

    hret = margo_get_input(handle, &observe_req);
    if (hret != HG_SUCCESS)
    {
        ret = SSG_MAKE_HG_ERROR(hret);
        goto fini;
    }
    view_size_requested = margo_bulk_get_size(observe_req.bulk_handle);

    ABT_rwlock_rdlock(ssg_rt->lock);

    /* look for the given group in my local table of groups */
    HASH_FIND(hh, ssg_rt->g_desc_table, &observe_req.g_id,
        sizeof(ssg_group_id_t), g_desc);
    if (!g_desc)
    {
        ABT_rwlock_unlock(ssg_rt->lock);
        fprintf(stderr, "Error: SSG unable to find group for observe request\n");
        margo_free_input(handle, &observe_req);
        ret = SSG_ERR_GROUP_NOT_FOUND;
        goto fini;
    }
    /* sanity checks */
    assert(g_desc->owner_status == SSG_OWNER_IS_MEMBER);
    assert(mid == g_desc->g_data.g->mid_state->mid);

    SSG_GROUP_REF_INCR(g_desc->g_data.g);
    ABT_rwlock_unlock(ssg_rt->lock);

    /* serialize group view and stash some other group metadata for response */
    ret = ssg_group_serialize(g_desc->g_data.g, &view_buf, &view_buf_size, &group_size);
    if (ret != SSG_SUCCESS)
    {
        SSG_GROUP_REF_DECR(g_desc->g_data.g);
        margo_free_input(handle, &observe_req);
        goto fini;
    }
    observe_resp.view_buf_size = view_buf_size;

    if (view_size_requested < view_buf_size)
    {
        SSG_GROUP_REF_DECR(g_desc->g_data.g);
        margo_free_input(handle, &observe_req);
        ret = SSG_ERR_NOBUFS;
        goto fini;
    }

    /* if observer's buf is large enough, transfer the view */
    hret = margo_bulk_create(mid, 1, &view_buf, &view_buf_size,
        HG_BULK_READ_ONLY, &bulk_handle);
    if (hret != HG_SUCCESS)
    {
        SSG_GROUP_REF_DECR(g_desc->g_data.g);
        margo_free_input(handle, &observe_req);
        ret = SSG_MAKE_HG_ERROR(hret);
        goto fini;
    }

    hret = margo_bulk_transfer(mid, HG_BULK_PUSH, hgi->addr,
        observe_req.bulk_handle, 0, bulk_handle, 0, view_buf_size);
    if (hret != HG_SUCCESS)
    {
        SSG_GROUP_REF_DECR(g_desc->g_data.g);
        margo_free_input(handle, &observe_req);
        ret = SSG_MAKE_HG_ERROR(hret);
        goto fini;
    }

    group_name = strdup(g_desc->g_data.g->name);
    if(!group_name)
    {
        SSG_GROUP_REF_DECR(g_desc->g_data.g);
        margo_free_input(handle, &observe_req);
        ret = SSG_ERR_ALLOCATION;
        goto fini;
    }

    ret = SSG_SUCCESS;

    SSG_GROUP_REF_DECR(g_desc->g_data.g);

    margo_free_input(handle, &observe_req);
fini:
    /* respond */
    observe_resp.ret = ret;
    observe_resp.group_name = group_name;
    observe_resp.group_size = group_size;
    margo_respond(handle, &observe_resp);

    /* cleanup */
    if (bulk_handle != HG_BULK_NULL) margo_bulk_free(bulk_handle);
    margo_destroy(handle);
    free(view_buf);
    free(group_name);

    return;
}
DEFINE_MARGO_RPC_HANDLER(ssg_group_observe_recv_ult)

static int ssg_group_serialize(
    ssg_group_t *g, void **buf, hg_size_t *buf_size, int *group_size)
{
    ssg_member_state_t *member_state, *tmp;
    hg_size_t group_buf_size = 0;
    void *group_buf;
    char *buf_p;
    int sz;

    *buf = NULL;
    *buf_size = 0;
    *group_size = 0;

    ABT_rwlock_rdlock(g->lock);

    /* first determine size */
    group_buf_size = strlen(g->mid_state->self_addr_str) + 1;
    HASH_ITER(hh, g->view.member_map, member_state, tmp)
    {
        group_buf_size += strlen(member_state->addr_str) + 1;
    }

    group_buf = malloc(group_buf_size);
    if(!group_buf)
    {
        ABT_rwlock_unlock(g->lock);
        return SSG_ERR_ALLOCATION;
    }

    buf_p = group_buf;
    strcpy(buf_p, g->mid_state->self_addr_str);
    buf_p += strlen(g->mid_state->self_addr_str) + 1;
    HASH_ITER(hh, g->view.member_map, member_state, tmp)
    {
        strcpy(buf_p, member_state->addr_str);
        buf_p += strlen(member_state->addr_str) + 1;
    }

    sz = g->view.size;

    ABT_rwlock_unlock(g->lock);

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
