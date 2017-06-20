/*
 * Copyright (c) 2016 UChicago Argonne, LLC
 *
 * See COPYRIGHT in top-level directory.
 */

#include "ssg-config.h"

#include <stdlib.h>

#include <mercury.h>
#include <abt.h>
#include <margo.h>

#include "ssg.h"
#include "ssg-internal.h"

/* SSG RPCS handler prototypes */
static void ssg_lookup_ult(void * arg);
DECLARE_MARGO_RPC_HANDLER(ssg_group_attach_recv_ult)

/* SSG RPC (de)serialization routine prototypes */
static hg_return_t hg_proc_ssg_group_id_t(hg_proc_t proc, void *data);

/* SSG RPC ids */
static hg_id_t ssg_group_attach_rpc_id;

/* ssg_register_rpcs
 *
 *
 */
void ssg_register_rpcs()
{
    hg_class_t *hgcl = NULL;

    hgcl = margo_get_class(ssg_inst->mid);
    if (!hgcl) return;

    /* register HG RPCs for SSG */
    ssg_group_attach_rpc_id = MERCURY_REGISTER(hgcl, "ssg_group_attach",
        ssg_group_descriptor_t, void, ssg_group_attach_recv_ult_handler);

    return;
}

/* ssg_group_lookup
 *
 * 
 */
struct lookup_ult_args
{
    ssg_group_t *g; 
    ssg_member_id_t member_id;
    const char *addr_str;
    hg_return_t out;
};

hg_return_t ssg_group_lookup(
    ssg_group_t * g,
    const char * const addr_strs[])
{
    ABT_thread *ults;
    struct lookup_ult_args *args;
    unsigned int i, r;
    int aret;
    hg_return_t hret = HG_SUCCESS;

    if (g == NULL) return HG_INVALID_PARAM;

    /* initialize ULTs */
    ults = malloc(g->group_view.size * sizeof(*ults));
    if (ults == NULL) return HG_NOMEM_ERROR;
    args = malloc(g->group_view.size * sizeof(*args));
    if (args == NULL)
    {
        free(ults);
        return HG_NOMEM_ERROR;
    }
    for (i = 0; i < g->group_view.size; i++)
        ults[i] = ABT_THREAD_NULL;
    
    for (i = 1; i < g->group_view.size; i++)
    {
        r = (g->self_id + i) % g->group_view.size;
        args[r].g = g;
        args[r].member_id = r;
        args[r].addr_str = addr_strs[r];
        aret = ABT_thread_create(*margo_get_handler_pool(ssg_inst->mid),
                &ssg_lookup_ult, &args[r], ABT_THREAD_ATTR_NULL, &ults[r]);
        if (aret != ABT_SUCCESS) {
            hret = HG_OTHER_ERROR;
            goto fini;
        }
    }

    /* wait on all */
    for (i = 1; i < g->group_view.size; i++)
    {
        r = (g->self_id + i) % g->group_view.size;
        aret = ABT_thread_join(ults[r]);
        ABT_thread_free(&ults[r]); 
        ults[r] = ABT_THREAD_NULL; // in case of cascading failure from join
        if (aret != ABT_SUCCESS)
        {   
            hret = HG_OTHER_ERROR;
            break;
        }
        else if (args[r].out != HG_SUCCESS)
        {
            fprintf(stderr, "Error: SSG unable to lookup HG address for rank %d"
                "(err=%d)\n", r, args[r].out);
            hret = args[r].out;
            break;
        }
    }

fini:
    /* cleanup */ 
    for (i = 0; i < g->group_view.size; i++)
    {
        if (ults[i] != ABT_THREAD_NULL)
        {
            ABT_thread_cancel(ults[i]);
            ABT_thread_free(ults[i]);
        }
    }
    free(ults);
    free(args);

    return hret;
}

static void ssg_lookup_ult(
    void * arg)
{
    struct lookup_ult_args *l = arg;
    ssg_group_t *g = l->g;

    l->out = margo_addr_lookup(ssg_inst->mid, l->addr_str,
        &g->group_view.member_states[l->member_id].addr);
    return;
}

/* ssg_group_attach_send
 *
 *
 */
hg_return_t ssg_group_attach_send(ssg_group_descriptor_t * group_descriptor)
{
    hg_class_t *hgcl = NULL;
    hg_addr_t member_addr = HG_ADDR_NULL;
    hg_handle_t handle = HG_HANDLE_NULL;
    hg_return_t hret;

    hgcl = margo_get_class(ssg_inst->mid);
    if (!hgcl) goto fini;

    /* lookup the address of the given group member */
    hret = margo_addr_lookup(ssg_inst->mid, group_descriptor->addr_str,
        &member_addr);
    if (hret != HG_SUCCESS) goto fini;

    hret = HG_Create(margo_get_context(ssg_inst->mid), member_addr,
        ssg_group_attach_rpc_id, &handle);
    if (hret != HG_SUCCESS) goto fini;

    /* send an attach request to the given group member address */
    hret = margo_forward(ssg_inst->mid, handle, group_descriptor);
    if (hret != HG_SUCCESS) goto fini;

    /* TODO: hold on to leader addr so we don't have to look it up again? */
fini:
    if (hgcl && member_addr != HG_ADDR_NULL) HG_Addr_free(hgcl, member_addr);
    if (handle != HG_HANDLE_NULL) HG_Destroy(handle);

    return hret;
}

static void ssg_group_attach_recv_ult(hg_handle_t handle)
{
    ssg_group_t *g = NULL;
    ssg_group_descriptor_t group_descriptor;
    hg_return_t hret;

    /* TODO: how to handle errors */
    if (!ssg_inst) goto fini;

    hret = HG_Get_input(handle, &group_descriptor);
    if (hret != HG_SUCCESS) goto fini;

    /* look for the given group in my local table of groups */
    HASH_FIND(hh, ssg_inst->group_table, &group_descriptor.name_hash,
        sizeof(uint64_t), g);
    if (!g)
    {
        HG_Free_input(handle, &group_descriptor);
        goto fini;
    }

    margo_respond(ssg_inst->mid, handle, NULL);

    HG_Free_input(handle, &group_descriptor);

fini:
    HG_Destroy(handle);
    return;
}
DEFINE_MARGO_RPC_HANDLER(ssg_group_attach_recv_ult)

/* SSG RPC (de)serialization routines */

#if 0
static hg_return_t hg_proc_ssg_group_id_t(hg_proc_t proc, void *data)
{
    ssg_group_descriptor_t *group_descriptor = (ssg_group_descriptor_t *)data;
    hg_return_t hret = HG_PROTOCOL_ERROR;

    switch(hg_proc_get_op(proc))
    {
        case HG_ENCODE:
            break;
        case HG_DECODE:
            break;
        case HG_FREE:
            break;
        default:
            break;
    }

    return hret;
}
#endif
