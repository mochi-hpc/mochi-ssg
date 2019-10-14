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

/* NOTE: keep in sync with ssg_group_descriptor_t definition in ssg-internal.h */
MERCURY_GEN_STRUCT_PROC(ssg_group_descriptor_t, \
    ((uint64_t)         (magic_nr)) \
    ((ssg_group_id_t)   (g_id)) \
    ((hg_string_t)      (addr_str)));

MERCURY_GEN_PROC(ssg_group_join_request_t, \
    ((ssg_group_descriptor_t)   (g_desc))
    ((hg_string_t)              (addr_str))
    ((hg_bulk_t)                (bulk_handle)));
MERCURY_GEN_PROC(ssg_group_join_response_t, \
    ((hg_string_t)  (group_name)) \
    ((uint32_t)     (group_size)) \
    ((hg_size_t)    (view_buf_size))
    ((uint8_t)      (ret)));

MERCURY_GEN_PROC(ssg_group_leave_request_t, \
    ((ssg_group_descriptor_t)   (g_desc))
    ((ssg_member_id_t)          (member_id)));
MERCURY_GEN_PROC(ssg_group_leave_response_t, \
    ((uint8_t)  (ret)));

MERCURY_GEN_PROC(ssg_group_observe_request_t, \
    ((ssg_group_descriptor_t)   (g_desc))
    ((hg_bulk_t)                (bulk_handle)));

MERCURY_GEN_PROC(ssg_group_observe_response_t, \
    ((hg_string_t)  (group_name)) \
    ((uint32_t)     (group_size)) \
    ((hg_size_t)    (view_buf_size))
    ((uint8_t)      (ret)));

/* SSG RPC handler prototypes */
DECLARE_MARGO_RPC_HANDLER(ssg_group_join_recv_ult)
DECLARE_MARGO_RPC_HANDLER(ssg_group_leave_recv_ult)
DECLARE_MARGO_RPC_HANDLER(ssg_group_observe_recv_ult)

/* internal helper routine prototypes */
static int ssg_group_serialize(
    ssg_group_descriptor_t *g_desc, void **buf, hg_size_t *buf_size);

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

    return;
}

/* ssg_group_join_send
 *
 *
 */
int ssg_group_join_send(
    ssg_group_descriptor_t * g_desc,
    char ** group_name,
    int * group_size,
    void ** view_buf)
{
    hg_addr_t group_target_addr;
    hg_handle_t handle = HG_HANDLE_NULL;
    hg_bulk_t bulk_handle = HG_BULK_NULL;
    void *tmp_view_buf = NULL, *b;
    hg_size_t tmp_view_buf_size = SSG_VIEW_BUF_DEF_SIZE;
    ssg_group_join_request_t join_req;
    ssg_group_join_response_t join_resp;
    hg_return_t hret;
    int sret = SSG_FAILURE;

    *group_name = NULL;
    *group_size = 0;
    *view_buf = NULL;

    /* get group target address */
    hret = margo_addr_lookup(g_desc->mid_state->mid, g_desc->addr_str,
        &group_target_addr);
    if (hret != HG_SUCCESS) goto fini;

    hret = margo_create(g_desc->mid_state->mid, group_target_addr,
        g_desc->mid_state->join_rpc_id, &handle);
    if (hret != HG_SUCCESS) goto fini;

    /* allocate a buffer to try to store the group view in */
    /* NOTE: We don't know if this buffer is big enough to store the complete
     * view. If the buffer is not large enough, the group member we are
     * sending to will send a NACK indicating the necessary buffer size
     */
    tmp_view_buf = malloc(tmp_view_buf_size);
    if (!tmp_view_buf) goto fini;

    hret = margo_bulk_create(g_desc->mid_state->mid, 1, &tmp_view_buf,
        &tmp_view_buf_size, HG_BULK_WRITE_ONLY, &bulk_handle);
    if (hret != HG_SUCCESS) goto fini;

    /* send a join request to the given group member address */
    /* XXX is the whole descriptor really needed? */
    memcpy(&join_req.g_desc, g_desc, sizeof(*g_desc));
    join_req.addr_str = g_desc->mid_state->self_addr_str;
    join_req.bulk_handle = bulk_handle;
    hret = margo_forward(handle, &join_req);
    if (hret != HG_SUCCESS) goto fini;

    hret = margo_get_output(handle, &join_resp);
    if (hret != HG_SUCCESS) goto fini;

    /* if our initial buffer is too small, reallocate to the exact size & rejoin */
    if (join_resp.view_buf_size > tmp_view_buf_size)
    {
        b = realloc(tmp_view_buf, join_resp.view_buf_size);
        if(!b)
        {
            margo_free_output(handle, &join_resp);
            goto fini;
        }
        tmp_view_buf = b;
        tmp_view_buf_size = join_resp.view_buf_size;
        margo_free_output(handle, &join_resp);

        /* free old bulk handle and recreate it */
        margo_bulk_free(bulk_handle);
        hret = margo_bulk_create(g_desc->mid_state->mid, 1, &tmp_view_buf,
            &tmp_view_buf_size, HG_BULK_WRITE_ONLY, &bulk_handle);
        if (hret != HG_SUCCESS) goto fini;

        join_req.bulk_handle = bulk_handle;
        hret = margo_forward(handle, &join_req);
        if (hret != HG_SUCCESS) goto fini;

        hret = margo_get_output(handle, &join_resp);
        if (hret != HG_SUCCESS) goto fini;
    }

    /* readjust view buf size if initial guess was too large */
    if (join_resp.view_buf_size < tmp_view_buf_size)
    {
        b = realloc(tmp_view_buf, join_resp.view_buf_size);
        if(!b)
        {
            margo_free_output(handle, &join_resp);
            goto fini;
        }
        tmp_view_buf = b;
    }

    /* set output pointers according to the returned view parameters */
    *group_name = strdup(join_resp.group_name);
    *group_size = (int)join_resp.group_size;
    *view_buf = tmp_view_buf;
    sret = join_resp.ret;
    margo_free_output(handle, &join_resp);

    if (sret == SSG_SUCCESS)
        tmp_view_buf = NULL; /* don't free on success */
fini:
    if (group_target_addr != HG_ADDR_NULL)
        margo_addr_free(g_desc->mid_state->mid, group_target_addr);
    if (handle != HG_HANDLE_NULL) margo_destroy(handle);
    if (bulk_handle != HG_BULK_NULL) margo_bulk_free(bulk_handle);
    free(tmp_view_buf);

    return sret;
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
    int sret;
    hg_return_t hret;

    join_resp.ret = SSG_FAILURE;

    if (!ssg_rt) goto fini;

    hgi = margo_get_info(handle);
    if (!hgi) goto fini;
    mid = margo_hg_info_get_instance(hgi);
    if (mid == MARGO_INSTANCE_NULL) goto fini;

    hret = margo_get_input(handle, &join_req);
    if (hret != HG_SUCCESS) goto fini;
    view_size_requested = margo_bulk_get_size(join_req.bulk_handle);

    ABT_rwlock_rdlock(ssg_rt->lock);

    /* look for the given group in my local table of groups */
    HASH_FIND(hh, ssg_rt->g_desc_table, &join_req.g_desc.g_id,
        sizeof(ssg_group_id_t), g_desc);
    if (!g_desc)
    {
        ABT_rwlock_unlock(ssg_rt->lock);
        margo_free_input(handle, &join_req);
        goto fini;
    }
    /* sanity checks */
    assert(g_desc->owner_status == SSG_OWNER_IS_MEMBER);
    assert(mid == g_desc->mid_state->mid);

    sret = ssg_group_serialize(g_desc, &view_buf, &view_buf_size);
    if (sret != SSG_SUCCESS)
    {
        ABT_rwlock_unlock(ssg_rt->lock);
        margo_free_input(handle, &join_req);
        goto fini;
    }

    ABT_rwlock_unlock(ssg_rt->lock);

    if (view_size_requested >= view_buf_size)
    {
        /* if joiner's buf is large enough, transfer the view */
        hret = margo_bulk_create(mid, 1, &view_buf, &view_buf_size,
            HG_BULK_READ_ONLY, &bulk_handle);
        if (hret != HG_SUCCESS)
        {
            margo_free_input(handle, &join_req);
            goto fini;
        }

        hret = margo_bulk_transfer(mid, HG_BULK_PUSH, hgi->addr,
            join_req.bulk_handle, 0, bulk_handle, 0, view_buf_size);
        if (hret != HG_SUCCESS)
        {
            margo_free_input(handle, &join_req);
            goto fini;
        }

#if 0
        /* apply group join locally */
        join_update.type = SSG_MEMBER_JOINED;
        join_update.u.member_addr_str = join_req.addr_str;
        ssg_apply_member_updates(g_desc->g_data.g, &join_update, 1);
#endif
    }
    margo_free_input(handle, &join_req);

    /* set the response and send back */
    join_resp.group_name = g_desc->g_data.g->name;
    join_resp.group_size = (int)g_desc->g_data.g->view.size;
    join_resp.view_buf_size = view_buf_size;
    join_resp.ret = SSG_SUCCESS;
fini:
    /* respond */
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
    ssg_group_descriptor_t * g_desc)
{
    hg_handle_t handle = HG_HANDLE_NULL;
    ssg_group_leave_request_t leave_req;
    ssg_group_leave_response_t leave_resp;
    hg_return_t hret;
    int sret = SSG_FAILURE;

    /* send leave request to first member in group view */
    hret = margo_create(g_desc->mid_state->mid, g_desc->g_data.g->view.member_map->addr,
        g_desc->mid_state->leave_rpc_id, &handle);
    if (hret != HG_SUCCESS) goto fini;

    /* XXX is the whole descriptor really needed? */
    memcpy(&leave_req.g_desc, g_desc, sizeof(*g_desc));
    leave_req.member_id = g_desc->mid_state->self_id;
    hret = margo_forward(handle, &leave_req);
    if (hret != HG_SUCCESS) goto fini;

    hret = margo_get_output(handle, &leave_resp);
    if (hret != HG_SUCCESS) goto fini;

    sret = leave_resp.ret;
    margo_free_output(handle, &leave_resp);
fini:
    if (handle != HG_HANDLE_NULL) margo_destroy(handle);

    return sret;
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

    leave_resp.ret = SSG_FAILURE;

    if (!ssg_rt) goto fini;

    hgi = margo_get_info(handle);
    if (!hgi) goto fini;
    mid = margo_hg_info_get_instance(hgi);
    if (mid == MARGO_INSTANCE_NULL) goto fini;

    hret = margo_get_input(handle, &leave_req);
    if (hret != HG_SUCCESS) goto fini;

    ABT_rwlock_rdlock(ssg_rt->lock);

    /* look for the given group in my local table of groups */
    HASH_FIND(hh, ssg_rt->g_desc_table, &leave_req.g_desc.g_id,
        sizeof(ssg_group_id_t), g_desc);
    if (!g_desc)
    {
        ABT_rwlock_unlock(ssg_rt->lock);
        margo_free_input(handle, &leave_req);
        goto fini;
    }
    /* sanity checks */
    assert(g_desc->owner_status == SSG_OWNER_IS_MEMBER);
    assert(mid == g_desc->mid_state->mid);

    ABT_rwlock_unlock(ssg_rt->lock);

#if 0
    /* apply group leave locally */
    leave_update.type = SSG_MEMBER_LEFT;
    leave_update.u.member_id = leave_req.member_id;
    ssg_apply_member_updates(g_desc->g_data.g, &leave_update, 1);
#endif

    margo_free_input(handle, &leave_req);
    leave_resp.ret = SSG_SUCCESS;
fini:
    /* respond */
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
    ssg_group_descriptor_t * g_desc,
    char ** group_name,
    int * group_size,
    void ** view_buf)
{
    hg_addr_t member_addr = HG_ADDR_NULL;
    hg_handle_t handle = HG_HANDLE_NULL;
    hg_bulk_t bulk_handle = HG_BULK_NULL;
    void *tmp_view_buf = NULL, *b;
    hg_size_t tmp_view_buf_size = SSG_VIEW_BUF_DEF_SIZE;
    ssg_group_observe_request_t observe_req;
    ssg_group_observe_response_t observe_resp;
    hg_return_t hret;
    int sret = SSG_FAILURE;

    *group_name = NULL;
    *group_size = 0;
    *view_buf = NULL;

    /* lookup the address of the group member associated with the descriptor */
    hret = margo_addr_lookup(g_desc->mid_state->mid, g_desc->addr_str,
        &member_addr);
    if (hret != HG_SUCCESS) goto fini;

    hret = margo_create(g_desc->mid_state->mid, member_addr,
        g_desc->mid_state->observe_rpc_id, &handle);
    if (hret != HG_SUCCESS) goto fini;

    /* allocate a buffer of the given size to try to store the group view in */
    /* NOTE: We don't know if this buffer is big enough to store the complete
     * view. If the buffers is not large enough, the group member we are
     * sending to will send a NACK indicating the necessary buffer size
     */
    tmp_view_buf = malloc(tmp_view_buf_size);
    if (!tmp_view_buf) goto fini;

    hret = margo_bulk_create(g_desc->mid_state->mid, 1, &tmp_view_buf, &tmp_view_buf_size,
        HG_BULK_WRITE_ONLY, &bulk_handle);
    if (hret != HG_SUCCESS) goto fini;

    /* send an observe request to the given group member address */
    memcpy(&observe_req.g_desc, g_desc, sizeof(*g_desc));
    observe_req.bulk_handle = bulk_handle;
    hret = margo_forward(handle, &observe_req);
    if (hret != HG_SUCCESS) goto fini;

    hret = margo_get_output(handle, &observe_resp);
    if (hret != HG_SUCCESS) goto fini;

    /* if our initial buffer is too small, reallocate to the exact size & resend */
    if (observe_resp.view_buf_size > tmp_view_buf_size)
    {
        b = realloc(tmp_view_buf, observe_resp.view_buf_size);
        if(!b)
        {
            margo_free_output(handle, &observe_resp);
            goto fini;
        }
        tmp_view_buf = b;
        tmp_view_buf_size = observe_resp.view_buf_size;
        margo_free_output(handle, &observe_resp);

        /* free old bulk handle and recreate it */
        margo_bulk_free(bulk_handle);
        hret = margo_bulk_create(g_desc->mid_state->mid, 1, &tmp_view_buf,
            &tmp_view_buf_size, HG_BULK_WRITE_ONLY, &bulk_handle);
        if (hret != HG_SUCCESS) goto fini;

        observe_req.bulk_handle = bulk_handle;
        hret = margo_forward(handle, &observe_req);
        if (hret != HG_SUCCESS) goto fini;

        hret = margo_get_output(handle, &observe_resp);
        if (hret != HG_SUCCESS) goto fini;
    }

    /* readjust view buf size if initial guess was too large */
    if (observe_resp.view_buf_size < tmp_view_buf_size)
    {
        b = realloc(tmp_view_buf, observe_resp.view_buf_size);
        if(!b)
        {
            margo_free_output(handle, &observe_resp);
            goto fini;
        }
        tmp_view_buf = b;
    }

    /* set output pointers according to the returned view parameters */
    *group_name = strdup(observe_resp.group_name);
    *group_size = (int)observe_resp.group_size;
    *view_buf = tmp_view_buf;
    sret = observe_resp.ret;
    margo_free_output(handle, &observe_resp);

    if (sret == SSG_SUCCESS)
        tmp_view_buf = NULL; /* don't free on success */
fini:
    if (member_addr != HG_ADDR_NULL)
        margo_addr_free(g_desc->mid_state->mid, member_addr);
    if (handle != HG_HANDLE_NULL) margo_destroy(handle);
    if (bulk_handle != HG_BULK_NULL) margo_bulk_free(bulk_handle);
    free(tmp_view_buf);

    return sret;
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
    int sret;
    hg_return_t hret;

    observe_resp.ret = SSG_FAILURE;

    if(!ssg_rt) goto fini;

    hgi = margo_get_info(handle);
    if (!hgi) goto fini;
    mid = margo_hg_info_get_instance(hgi);
    if (mid == MARGO_INSTANCE_NULL) goto fini;

    hret = margo_get_input(handle, &observe_req);
    if (hret != HG_SUCCESS) goto fini;
    view_size_requested = margo_bulk_get_size(observe_req.bulk_handle);

    ABT_rwlock_rdlock(ssg_rt->lock);

    /* look for the given group in my local table of groups */
    HASH_FIND(hh, ssg_rt->g_desc_table, &observe_req.g_desc.g_id,
        sizeof(ssg_group_id_t), g_desc);
    if (!g_desc)
    {
        ABT_rwlock_unlock(ssg_rt->lock);
        margo_free_input(handle, &observe_req);
        goto fini;
    }
    /* sanity checks */
    assert(g_desc->owner_status == SSG_OWNER_IS_MEMBER);
    assert(mid == g_desc->mid_state->mid);

    sret = ssg_group_serialize(g_desc, &view_buf, &view_buf_size);
    if (sret != SSG_SUCCESS)
    {
        ABT_rwlock_unlock(ssg_rt->lock);
        margo_free_input(handle, &observe_req);
        goto fini;
    }

    ABT_rwlock_unlock(ssg_rt->lock);

    if (view_size_requested >= view_buf_size)
    {
        /* if observer's buf is large enough, transfer the view */
        hret = margo_bulk_create(mid, 1, &view_buf, &view_buf_size,
            HG_BULK_READ_ONLY, &bulk_handle);
        if (hret != HG_SUCCESS)
        {
            margo_free_input(handle, &observe_req);
            goto fini;
        }

        hret = margo_bulk_transfer(mid, HG_BULK_PUSH, hgi->addr,
            observe_req.bulk_handle, 0, bulk_handle, 0, view_buf_size);
        if (hret != HG_SUCCESS)
        {
            margo_free_input(handle, &observe_req);
            goto fini;
        }
    }
    margo_free_input(handle, &observe_req);

    /* set the response and send back */
    observe_resp.group_name = g_desc->g_data.g->name;
    observe_resp.group_size = (int)g_desc->g_data.g->view.size;
    observe_resp.view_buf_size = view_buf_size;
    observe_resp.ret = SSG_SUCCESS;
fini:
    /* respond */
    margo_respond(handle, &observe_resp);

    /* cleanup */
    if (bulk_handle != HG_BULK_NULL) margo_bulk_free(bulk_handle);
    margo_destroy(handle);
    free(view_buf);

    return;
}
DEFINE_MARGO_RPC_HANDLER(ssg_group_observe_recv_ult)

static int ssg_group_serialize(
    ssg_group_descriptor_t *g_desc, void **buf, hg_size_t *buf_size)
{
    ssg_member_state_t *member_state, *tmp;
    hg_size_t group_buf_size = 0;
    void *group_buf;
    void *buf_p;

    *buf = NULL;
    *buf_size = 0;

    ABT_rwlock_rdlock(g_desc->g_data.g->lock);

    /* first determine size */
    group_buf_size = strlen(g_desc->mid_state->self_addr_str) + 1;
    HASH_ITER(hh, g_desc->g_data.g->view.member_map, member_state, tmp)
    {
        group_buf_size += strlen(member_state->addr_str) + 1;
    }

    group_buf = malloc(group_buf_size);
    if(!group_buf)
    {
        ABT_rwlock_unlock(g_desc->g_data.g->lock);
        return SSG_FAILURE;
    }

    buf_p = group_buf;
    strcpy(buf_p, g_desc->mid_state->self_addr_str);
    buf_p += strlen(g_desc->mid_state->self_addr_str) + 1;
    HASH_ITER(hh, g_desc->g_data.g->view.member_map, member_state, tmp)
    {
        strcpy(buf_p, member_state->addr_str);
        buf_p += strlen(member_state->addr_str) + 1;
    }

    *buf = group_buf;
    *buf_size = group_buf_size;

    ABT_rwlock_unlock(g_desc->g_data.g->lock);

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
