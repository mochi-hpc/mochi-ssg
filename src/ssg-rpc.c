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

/* TODO join and attach are nearly identical -- refactor */

/* NOTE: keep in sync with ssg_group_descriptor_t definition in ssg-internal.h */
MERCURY_GEN_STRUCT_PROC(ssg_group_descriptor_t, \
    ((uint64_t)     (magic_nr)) \
    ((uint64_t)     (name_hash)) \
    ((hg_string_t)  (addr_str)));

MERCURY_GEN_PROC(ssg_group_join_request_t, \
    ((ssg_group_descriptor_t)   (group_descriptor))
    ((hg_bulk_t)                (bulk_handle)));

MERCURY_GEN_PROC(ssg_group_join_response_t, \
    ((hg_string_t)  (group_name)) \
    ((uint32_t)     (group_size)) \
    ((hg_size_t)    (view_buf_size)));

#if 0
MERCURY_GEN_PROC(ssg_group_leave_request_t, \
    ((ssg_group_descriptor_t)   (group_descriptor)));
#endif

MERCURY_GEN_PROC(ssg_group_attach_request_t, \
    ((ssg_group_descriptor_t)   (group_descriptor))
    ((hg_bulk_t)                (bulk_handle)));

MERCURY_GEN_PROC(ssg_group_attach_response_t, \
    ((hg_string_t)  (group_name)) \
    ((uint32_t)     (group_size)) \
    ((hg_size_t)    (view_buf_size)));

/* SSG RPC handler prototypes */
DECLARE_MARGO_RPC_HANDLER(ssg_group_join_recv_ult)
#if 0
DECLARE_MARGO_RPC_HANDLER(ssg_group_leave_recv_ult)
#endif
DECLARE_MARGO_RPC_HANDLER(ssg_group_attach_recv_ult)

/* internal helper routine prototypes */
static int ssg_group_view_serialize(
    ssg_group_view_t *view, void **buf, hg_size_t *buf_size);

/* SSG RPC IDs */
static hg_id_t ssg_group_join_rpc_id;
#if 0
static hg_id_t ssg_group_leave_rpc_id;
#endif
static hg_id_t ssg_group_attach_rpc_id;

/* ssg_register_rpcs
 *
 *
 */
void ssg_register_rpcs()
{
    /* register HG RPCs for SSG */
    ssg_group_join_rpc_id =
        MARGO_REGISTER(ssg_inst->mid, "ssg_group_join",
        ssg_group_join_request_t, ssg_group_join_response_t,
        ssg_group_join_recv_ult);
#if 0
    ssg_group_leave_rpc_id =
        MARGO_REGISTER(ssg_inst->mid, "ssg_group_leave",
        ssg_group_leave_request_t, void,
        ssg_group_leave_recv_ult);
#endif
    ssg_group_attach_rpc_id =
		MARGO_REGISTER(ssg_inst->mid, "ssg_group_attach",
        ssg_group_attach_request_t, ssg_group_attach_response_t,
        ssg_group_attach_recv_ult);

    return;
}

/* ssg_group_join_send
 *
 *
 */
int ssg_group_join_send(
    ssg_group_descriptor_t * group_descriptor,
    char ** group_name,
    int * group_size,
    void ** view_buf)
{
    hg_addr_t member_addr = HG_ADDR_NULL;
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

    /* lookup the address of the given group member */
    hret = margo_addr_lookup(ssg_inst->mid, group_descriptor->addr_str,
        &member_addr);
    if (hret != HG_SUCCESS) goto fini;

    hret = margo_create(ssg_inst->mid, member_addr,
        ssg_group_join_rpc_id, &handle);
    if (hret != HG_SUCCESS) goto fini;

    /* allocate a buffer to try to store the group view in */
    /* NOTE: We don't know if this buffer is big enough to store the complete
     * view. If the buffer is not large enough, the group member we are
     * attaching too will send a NACK indicating the necessary buffer size
     */
    tmp_view_buf = malloc(tmp_view_buf_size);
    if (!tmp_view_buf) goto fini;

    hret = margo_bulk_create(ssg_inst->mid, 1, &tmp_view_buf, &tmp_view_buf_size,
        HG_BULK_WRITE_ONLY, &bulk_handle);
    if (hret != HG_SUCCESS) goto fini;

    /* send a join request to the given group member address */
    memcpy(&join_req.group_descriptor, group_descriptor, sizeof(*group_descriptor));
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
        hret = margo_bulk_create(ssg_inst->mid, 1, &tmp_view_buf, &tmp_view_buf_size,
            HG_BULK_WRITE_ONLY, &bulk_handle);
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
            HG_Free_output(handle, &join_resp);
            goto fini;
        }
        tmp_view_buf = b;
    }

    /* set output pointers according to the returned view parameters */
    *group_name = strdup(join_resp.group_name);
    *group_size = (int)join_resp.group_size;
    *view_buf = tmp_view_buf;

    margo_free_output(handle, &join_resp);
    tmp_view_buf = NULL;
    sret = SSG_SUCCESS;
fini:
    if (member_addr != HG_ADDR_NULL) margo_addr_free(ssg_inst->mid, member_addr);
    if (handle != HG_HANDLE_NULL) margo_destroy(handle);
    if (bulk_handle != HG_BULK_NULL) margo_bulk_free(bulk_handle);
    free(tmp_view_buf);

    return sret;
}

static void ssg_group_join_recv_ult(
    hg_handle_t handle)
{
    const struct hg_info *hgi = NULL;
    ssg_group_t *g = NULL;
    ssg_group_join_request_t join_req;
    ssg_group_join_response_t join_resp;
    hg_size_t view_size_requested;
    void *view_buf = NULL;
    hg_size_t view_buf_size;
    hg_bulk_t bulk_handle = HG_BULK_NULL;
    int sret;
    hg_return_t hret;

    if (!ssg_inst) goto fini;

    hgi = margo_get_info(handle);
    if (!hgi) goto fini;

    hret = margo_get_input(handle, &join_req);
    if (hret != HG_SUCCESS) goto fini;
    view_size_requested = margo_bulk_get_size(join_req.bulk_handle);

    /* look for the given group in my local table of groups */
    HASH_FIND(hh, ssg_inst->group_table, &join_req.group_descriptor.name_hash,
        sizeof(uint64_t), g);
    if (!g)
    {
        margo_free_input(handle, &join_req);
        goto fini;
    }

    sret = ssg_group_view_serialize(&g->view, &view_buf, &view_buf_size);
    if (sret != SSG_SUCCESS)
    {
        margo_free_input(handle, &join_req);
        goto fini;
    }

    if (view_size_requested >= view_buf_size)
    {
        /* if attacher's buf is large enough, transfer the view */
        hret = margo_bulk_create(ssg_inst->mid, 1, &view_buf, &view_buf_size,
            HG_BULK_READ_ONLY, &bulk_handle);
        if (hret != HG_SUCCESS)
        {
            margo_free_input(handle, &join_req);
            goto fini;
        }

        hret = margo_bulk_transfer(ssg_inst->mid, HG_BULK_PUSH, hgi->addr,
            join_req.bulk_handle, 0, bulk_handle, 0, view_buf_size);
        if (hret != HG_SUCCESS)
        {
            margo_free_input(handle, &join_req);
            goto fini;
        }
    }

    /* XXX what else? need to add to view/target list */
    printf("***SDS: received JOINNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNN REQUESTTTTTTTTTTTTTT\n");

    /* set the response and send back */
    join_resp.group_name = g->name;
    join_resp.group_size = (int)g->view.size;
    join_resp.view_buf_size = view_buf_size;
    margo_respond(handle, &join_resp);

    margo_free_input(handle, &join_req);
fini:
    free(view_buf);
    if (handle != HG_HANDLE_NULL) margo_destroy(handle);
    if (bulk_handle != HG_BULK_NULL) margo_bulk_free(bulk_handle);

    return;
}
DEFINE_MARGO_RPC_HANDLER(ssg_group_join_recv_ult)

#if 0
/* ssg_group_leave_send
 *
 *
 */
int ssg_group_leave_send(
    ssg_group_descriptor_t * group_descriptor,
    char ** group_name,
    int * group_size,
    void ** view_buf)
{
    hg_class_t *hgcl = NULL;
    hg_addr_t member_addr = HG_ADDR_NULL;
    hg_handle_t handle = HG_HANDLE_NULL;

    /* send a join request to the given group member address */

    return SSG_SUCCESS;
}
#endif

/* ssg_group_attach_send
 *
 *
 */
int ssg_group_attach_send(
    ssg_group_descriptor_t * group_descriptor,
    char ** group_name,
    int * group_size,
    void ** view_buf)
{
    hg_addr_t member_addr = HG_ADDR_NULL;
    hg_handle_t handle = HG_HANDLE_NULL;
    hg_bulk_t bulk_handle = HG_BULK_NULL;
    void *tmp_view_buf = NULL, *b;
    hg_size_t tmp_view_buf_size = SSG_VIEW_BUF_DEF_SIZE;
    ssg_group_attach_request_t attach_req;
    ssg_group_attach_response_t attach_resp;
    hg_return_t hret;
    int sret = SSG_FAILURE;

    *group_name = NULL;
    *group_size = 0;
    *view_buf = NULL;

    /* lookup the address of the given group member */
    hret = margo_addr_lookup(ssg_inst->mid, group_descriptor->addr_str,
        &member_addr);
    if (hret != HG_SUCCESS) goto fini;

    hret = margo_create(ssg_inst->mid, member_addr,
        ssg_group_attach_rpc_id, &handle);
    if (hret != HG_SUCCESS) goto fini;

    /* allocate a buffer to try to store the group view in */
    /* NOTE: We don't know if this buffer is big enough to store the complete
     * view. If the buffer is not large enough, the group member we are
     * attaching too will send a NACK indicating the necessary buffer size
     */
    tmp_view_buf = malloc(tmp_view_buf_size);
    if (!tmp_view_buf) goto fini;

    hret = margo_bulk_create(ssg_inst->mid, 1, &tmp_view_buf, &tmp_view_buf_size,
        HG_BULK_WRITE_ONLY, &bulk_handle);
    if (hret != HG_SUCCESS) goto fini;

    /* send an attach request to the given group member address */
    memcpy(&attach_req.group_descriptor, group_descriptor, sizeof(*group_descriptor));
    attach_req.bulk_handle = bulk_handle;
    hret = margo_forward(handle, &attach_req);
    if (hret != HG_SUCCESS) goto fini;

    hret = margo_get_output(handle, &attach_resp);
    if (hret != HG_SUCCESS) goto fini;

    /* if our initial buffer is too small, reallocate to the exact size & reattach */
    if (attach_resp.view_buf_size > tmp_view_buf_size)
    {
        b = realloc(tmp_view_buf, attach_resp.view_buf_size);
        if(!b)
        {
            margo_free_output(handle, &attach_resp);
            goto fini;
        }
        tmp_view_buf = b;
        tmp_view_buf_size = attach_resp.view_buf_size;
        margo_free_output(handle, &attach_resp);

        /* free old bulk handle and recreate it */
        margo_bulk_free(bulk_handle);
        hret = margo_bulk_create(ssg_inst->mid, 1, &tmp_view_buf, &tmp_view_buf_size,
            HG_BULK_WRITE_ONLY, &bulk_handle);
        if (hret != HG_SUCCESS) goto fini;

        attach_req.bulk_handle = bulk_handle;
        hret = margo_forward(handle, &attach_req);
        if (hret != HG_SUCCESS) goto fini;

        hret = margo_get_output(handle, &attach_resp);
        if (hret != HG_SUCCESS) goto fini;
    }

    /* readjust view buf size if initial guess was too large */
    if (attach_resp.view_buf_size < tmp_view_buf_size)
    {
        b = realloc(tmp_view_buf, attach_resp.view_buf_size);
        if(!b)
        {
            margo_free_output(handle, &attach_resp);
            goto fini;
        }
        tmp_view_buf = b;
    }

    /* set output pointers according to the returned view parameters */
    *group_name = strdup(attach_resp.group_name);
    *group_size = (int)attach_resp.group_size;
    *view_buf = tmp_view_buf;

    margo_free_output(handle, &attach_resp);
    tmp_view_buf = NULL;
    sret = SSG_SUCCESS;
fini:
    if (member_addr != HG_ADDR_NULL) margo_addr_free(ssg_inst->mid, member_addr);
    if (handle != HG_HANDLE_NULL) margo_destroy(handle);
    if (bulk_handle != HG_BULK_NULL) margo_bulk_free(bulk_handle);
    free(tmp_view_buf);

    return sret;
}

static void ssg_group_attach_recv_ult(
    hg_handle_t handle)
{
    const struct hg_info *hgi = NULL;
    ssg_group_t *g = NULL;
    ssg_group_attach_request_t attach_req;
    ssg_group_attach_response_t attach_resp;
    hg_size_t view_size_requested;
    void *view_buf = NULL;
    hg_size_t view_buf_size;
    hg_bulk_t bulk_handle = HG_BULK_NULL;
    int sret;
    hg_return_t hret;

    if (!ssg_inst) goto fini;

    hgi = margo_get_info(handle);
    if (!hgi) goto fini;

    hret = margo_get_input(handle, &attach_req);
    if (hret != HG_SUCCESS) goto fini;
    view_size_requested = margo_bulk_get_size(attach_req.bulk_handle);

    /* look for the given group in my local table of groups */
    HASH_FIND(hh, ssg_inst->group_table, &attach_req.group_descriptor.name_hash,
        sizeof(uint64_t), g);
    if (!g)
    {
        margo_free_input(handle, &attach_req);
        goto fini;
    }

    sret = ssg_group_view_serialize(&g->view, &view_buf, &view_buf_size);
    if (sret != SSG_SUCCESS)
    {
        margo_free_input(handle, &attach_req);
        goto fini;
    }

    if (view_size_requested >= view_buf_size)
    {
        /* if attacher's buf is large enough, transfer the view */
        hret = margo_bulk_create(ssg_inst->mid, 1, &view_buf, &view_buf_size,
            HG_BULK_READ_ONLY, &bulk_handle);
        if (hret != HG_SUCCESS)
        {
            margo_free_input(handle, &attach_req);
            goto fini;
        }

        hret = margo_bulk_transfer(ssg_inst->mid, HG_BULK_PUSH, hgi->addr,
            attach_req.bulk_handle, 0, bulk_handle, 0, view_buf_size);
        if (hret != HG_SUCCESS)
        {
            margo_free_input(handle, &attach_req);
            goto fini;
        }
    }

    /* set the response and send back */
    attach_resp.group_name = g->name;
    attach_resp.group_size = (int)g->view.size;
    attach_resp.view_buf_size = view_buf_size;
    margo_respond(handle, &attach_resp);

    margo_free_input(handle, &attach_req);
fini:
    free(view_buf);
    if (handle != HG_HANDLE_NULL) margo_destroy(handle);
    if (bulk_handle != HG_BULK_NULL) margo_bulk_free(bulk_handle);

    return;
}
DEFINE_MARGO_RPC_HANDLER(ssg_group_attach_recv_ult)

static int ssg_group_view_serialize(
    ssg_group_view_t *view, void **buf, hg_size_t *buf_size)
{
    ssg_member_state_t *member_state, *tmp;
    hg_size_t view_buf_size = 0;
    void *view_buf;
    void *buf_p, *str_p;

    *buf = NULL;
    *buf_size = 0;

    /* first determine view size */
    HASH_ITER(hh, view->member_map, member_state, tmp)
    {
        view_buf_size += strlen(member_state->addr_str) + 1;
    }

    view_buf = malloc(view_buf_size);
    if(!view_buf)
        return SSG_FAILURE;

    buf_p = view_buf;
    HASH_ITER(hh, view->member_map, member_state, tmp)
    {
        str_p = member_state->addr_str;
        strcpy(buf_p, str_p);
        buf_p += strlen(member_state->addr_str) + 1;
    }

    *buf = view_buf;
    *buf_size = view_buf_size;

    return SSG_SUCCESS;
}

/* custom SSG RPC proc routines */

hg_return_t hg_proc_ssg_group_id_t(
    hg_proc_t proc, void *data)
{
    ssg_group_descriptor_t **group_descriptor = (ssg_group_descriptor_t **)data;
    hg_return_t hret = HG_PROTOCOL_ERROR;

    switch(hg_proc_get_op(proc))
    {
        case HG_ENCODE:
            hret = hg_proc_ssg_group_descriptor_t(proc, *group_descriptor);
            if (hret != HG_SUCCESS)
            {
                hret = HG_PROTOCOL_ERROR;
                return hret;
            }
            break;
        case HG_DECODE:
            *group_descriptor = malloc(sizeof(**group_descriptor));
            if (!(*group_descriptor))
            {
                hret = HG_NOMEM_ERROR;
                return hret;
            }
            memset(*group_descriptor, 0, sizeof(**group_descriptor));
            hret = hg_proc_ssg_group_descriptor_t(proc, *group_descriptor);
            if (hret != HG_SUCCESS)
            {
                hret = HG_PROTOCOL_ERROR;
                return hret;
            }
            (*group_descriptor)->ref_count = 1;
            break;
        case HG_FREE:
            if((*group_descriptor)->ref_count == 1)
            {
                free((*group_descriptor)->addr_str);
                free(*group_descriptor);
            }
            else
            {
                (*group_descriptor)->ref_count--;
            }
            hret = HG_SUCCESS;
            break;
        default:
            break;
    }

    return hret;
}
