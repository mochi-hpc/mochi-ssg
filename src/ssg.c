/*
 * Copyright (c) 2016 UChicago Argonne, LLC
 *
 * See COPYRIGHT in top-level directory.
 */

#include "ssg-config.h"

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <errno.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <linux/limits.h>
#include <assert.h>
#ifdef SSG_HAVE_MPI
#include <mpi.h>
#endif
#ifdef SSG_HAVE_PMIX
#include <pmix.h>
#endif

#include <mercury.h>
#include <abt.h>
#include <margo.h>

#include "ssg.h"
#ifdef SSG_HAVE_MPI
#include "ssg-mpi.h"
#endif
#ifdef SSG_HAVE_PMIX
#include "ssg-pmix.h"
#endif
#include "ssg-internal.h"
#include "swim-fd/swim-fd.h"

/* SSG helper routine prototypes */
ssg_mid_state_t *ssg_acquire_mid_state(
    margo_instance_id mid);
static void ssg_release_mid_state(
    ssg_mid_state_t *mid_state);
static ssg_group_id_t ssg_group_create_internal(
    ssg_mid_state_t *mid_state, const char * group_name,
    const char * const group_addr_strs[], int group_size,
    ssg_group_config_t *group_conf, ssg_membership_update_cb update_cb,
    void *update_cb_dat);
static int ssg_group_view_create(
    const char * const group_addr_strs[], int group_size,
    const char * self_addr_str, ssg_mid_state_t *mid_state,
    ABT_rwlock view_lock, ssg_group_view_t * view);
static ssg_member_state_t * ssg_group_view_add_member(
    const char * addr_str, hg_addr_t addr, ssg_member_id_t member_id,
    ssg_group_view_t * view);
static ssg_group_descriptor_t * ssg_group_descriptor_create(
    ssg_group_id_t g_id, int num_addrs, char ** addr_strs,
    int64_t cred, int owner_status);
static void ssg_group_destroy_internal(
    ssg_group_t * g);
static void ssg_observed_group_destroy(
    ssg_observed_group_t * og);
static void ssg_group_view_destroy(
    ssg_group_view_t * view, margo_instance_id mid);
static void ssg_group_descriptor_free(
    ssg_group_descriptor_t * descriptor);
static ssg_member_id_t ssg_gen_member_id(
    const char * addr_str);
static char ** ssg_addr_str_buf_to_list(
    const char * buf, int num_addrs);
static int ssg_member_id_sort_cmp( 
    const void *a, const void *b);
static int ssg_get_group_member_rank_internal(
    ssg_group_view_t *view, ssg_member_id_t member_id);
#ifdef SSG_HAVE_PMIX
void ssg_pmix_proc_failure_notify_fn(
    size_t evhdlr_registration_id, pmix_status_t status, const pmix_proc_t *source,
    pmix_info_t info[], size_t ninfo, pmix_info_t results[], size_t nresults,
    pmix_event_notification_cbfunc_fn_t cbfunc, void *cbdata);
void ssg_pmix_proc_failure_reg_cb(
    pmix_status_t status, size_t evhdlr_ref, void *cbdata);
#endif 

/* arguments for group lookup ULTs */
struct ssg_group_lookup_ult_args
{
    margo_instance_id mid;
    const char *addr_str;
    ssg_group_view_t *view;
    ABT_rwlock lock;
    int out;
};
static void ssg_group_lookup_ult(void * arg);

/* global SSG runtime state */
ssg_runtime_state_t *ssg_rt = NULL;

/***************************************************
 *** SSG runtime intialization/shutdown routines ***
 ***************************************************/

int ssg_init()
{
    struct timespec ts;
    int ret;

    /* XXX: note this init routine is not thread-safe */
    if (ssg_rt)
        return SSG_FAILURE;

    /* initialize SSG runtime state */
    ssg_rt = malloc(sizeof(*ssg_rt));
    if (!ssg_rt)
        return SSG_FAILURE;
    memset(ssg_rt, 0, sizeof(*ssg_rt));

    if (ABT_initialized() == ABT_ERR_UNINITIALIZED)
    {
        ret = ABT_init(0, NULL); /* XXX: argc/argv not currently used by ABT ... */
        if (ret != 0)
            return SSG_FAILURE;
        ssg_rt->abt_init_flag = 1;
    }
    ABT_rwlock_create(&ssg_rt->lock);

#ifdef SSG_HAVE_PMIX 
    /* use PMIx event registrations to inform us of terminated/aborted procs */
    pmix_status_t err_codes[2] = {PMIX_PROC_TERMINATED, PMIX_ERR_PROC_ABORTED};
    PMIx_Register_event_handler(err_codes, 2, NULL, 0,
        ssg_pmix_proc_failure_notify_fn, ssg_pmix_proc_failure_reg_cb,
        &ssg_rt->pmix_failure_evhdlr_ref);
#endif

    /* seed RNG */
    clock_gettime(CLOCK_MONOTONIC, &ts);
    srand(ts.tv_nsec + getpid());

    return SSG_SUCCESS;
}

int ssg_finalize()
{
    ssg_group_descriptor_t *g_desc, *g_desc_tmp;
    ssg_mid_state_t *mid_state, *mid_state_tmp;

    if (!ssg_rt)
        return SSG_FAILURE;

    ABT_rwlock_wrlock(ssg_rt->lock);

#ifdef SSG_HAVE_PMIX
    if (ssg_rt->pmix_failure_evhdlr_ref)
        PMIx_Deregister_event_handler(ssg_rt->pmix_failure_evhdlr_ref, NULL, NULL);
#endif

    /* destroy all active groups */
    HASH_ITER(hh, ssg_rt->g_desc_table, g_desc, g_desc_tmp)
    {
        HASH_DEL(ssg_rt->g_desc_table, g_desc);
        ABT_rwlock_unlock(ssg_rt->lock);
        if (g_desc->owner_status == SSG_OWNER_IS_MEMBER)
            ssg_group_destroy_internal(g_desc->g_data.g);
        else if (g_desc->owner_status == SSG_OWNER_IS_OBSERVER)
            ssg_observed_group_destroy(g_desc->g_data.og);
        ssg_group_descriptor_free(g_desc);
        ABT_rwlock_wrlock(ssg_rt->lock);
    }

    /* free any mid state */
    LL_FOREACH_SAFE(ssg_rt->mid_list, mid_state, mid_state_tmp)
    {
        LL_DELETE(ssg_rt->mid_list, mid_state);
        ssg_deregister_rpcs(mid_state);
        margo_addr_free(mid_state->mid, mid_state->self_addr);
        free(mid_state->self_addr_str);
        free(mid_state);
    }

    ABT_rwlock_unlock(ssg_rt->lock);

    ABT_rwlock_free(&ssg_rt->lock);
    if (ssg_rt->abt_init_flag)
        ABT_finalize(); /* finalize ABT if SSG initialized it */
    free(ssg_rt);
    ssg_rt = NULL;

    return SSG_SUCCESS;
}

/*************************************
 *** SSG group management routines ***
 *************************************/

ssg_group_id_t ssg_group_create(
    margo_instance_id mid,
    const char * group_name,
    const char * const group_addr_strs[],
    int group_size,
    ssg_group_config_t *group_conf,
    ssg_membership_update_cb update_cb,
    void * update_cb_dat)
{
    ssg_mid_state_t *mid_state;
    ssg_group_id_t g_id = SSG_GROUP_ID_INVALID;

    if (!ssg_rt) return g_id;

    mid_state = ssg_acquire_mid_state(mid);
    if (!mid_state) return g_id;

    g_id = ssg_group_create_internal(mid_state, group_name, group_addr_strs,
            group_size, group_conf, update_cb, update_cb_dat);
    if (g_id == SSG_GROUP_ID_INVALID)
        ssg_release_mid_state(mid_state);

    return g_id;
}

ssg_group_id_t ssg_group_create_config(
    margo_instance_id mid,
    const char * group_name,
    const char * file_name,
    ssg_group_config_t *group_conf,
    ssg_membership_update_cb update_cb,
    void * update_cb_dat)
{
    ssg_mid_state_t *mid_state = NULL;
    int fd=-1;
    struct stat st;
    char *rd_buf = NULL;
    ssize_t rd_buf_size;
    char *tok;
    void *addr_str_buf = NULL;
    int addr_str_buf_len = 0, num_addrs = 0;
    const char **addr_strs = NULL;
    int ret;
    ssg_group_id_t g_id = SSG_GROUP_ID_INVALID;

    if (!ssg_rt) goto fini;

    mid_state = ssg_acquire_mid_state(mid);
    if (!mid_state) goto fini;

    /* open config file for reading */
    fd = open(file_name, O_RDONLY);
    if (fd == -1)
    {
        fprintf(stderr, "Error: SSG unable to open config file %s for group %s\n",
            file_name, group_name);
        goto fini;
    }

    /* get file size and allocate a buffer to store it */
    ret = fstat(fd, &st);
    if (ret == -1)
    {
        fprintf(stderr, "Error: SSG unable to stat config file %s for group %s\n",
            file_name, group_name);
        goto fini;
    }
    rd_buf = malloc(st.st_size+1);
    if (rd_buf == NULL) goto fini;

    /* load it all in one fell swoop */
    rd_buf_size = read(fd, rd_buf, st.st_size);
    if (rd_buf_size != st.st_size)
    {
        fprintf(stderr, "Error: SSG unable to read config file %s for group %s\n",
            file_name, group_name);
        goto fini;
    }
    rd_buf[rd_buf_size]='\0';

    /* strtok the result - each space-delimited address is assumed to be
     * a unique mercury address
     */
    tok = strtok(rd_buf, "\r\n\t ");
    if (tok == NULL) goto fini;

    /* build up the address buffer */
    addr_str_buf = malloc(rd_buf_size);
    if (addr_str_buf == NULL) goto fini;
    do
    {
        int tok_size = strlen(tok);
        memcpy((char*)addr_str_buf + addr_str_buf_len, tok, tok_size+1);
        addr_str_buf_len += tok_size+1;
        num_addrs++;
        tok = strtok(NULL, "\r\n\t ");
    } while (tok != NULL);
    if (addr_str_buf_len != rd_buf_size)
    {
        /* adjust buffer size if our initial guess was wrong */
        void *tmp = realloc(addr_str_buf, addr_str_buf_len);
        if (tmp == NULL) goto fini;
        addr_str_buf = tmp;
    }

    /* set up address string array for group members */
    addr_strs = (const char **)ssg_addr_str_buf_to_list(addr_str_buf, num_addrs);
    if (!addr_strs) goto fini;

    /* invoke the generic group create routine using our list of addrs */
    g_id = ssg_group_create_internal(mid_state, group_name, addr_strs, num_addrs,
        group_conf, update_cb, update_cb_dat);

fini:
    /* cleanup before returning */
    if (fd != -1) close(fd);
    free(rd_buf);
    free(addr_str_buf);
    free(addr_strs);
    if (g_id == SSG_GROUP_ID_INVALID)
        ssg_release_mid_state(mid_state);

    return g_id;
}

#ifdef SSG_HAVE_MPI
ssg_group_id_t ssg_group_create_mpi(
    margo_instance_id mid,
    const char * group_name,
    MPI_Comm comm,
    ssg_group_config_t *group_conf,
    ssg_membership_update_cb update_cb,
    void * update_cb_dat)
{
    ssg_mid_state_t *mid_state=NULL;
    int i;
    int self_addr_str_size = 0;
    char *addr_str_buf = NULL;
    int *sizes = NULL;
    int *sizes_psum = NULL;
    int comm_size = 0, comm_rank = 0;
    const char **addr_strs = NULL;
    ssg_group_id_t g_id = SSG_GROUP_ID_INVALID;

    if (!ssg_rt) goto fini;

    mid_state = ssg_acquire_mid_state(mid);
    if(!mid_state) goto fini;

    /* gather the buffer sizes */
    MPI_Comm_size(comm, &comm_size);
    MPI_Comm_rank(comm, &comm_rank);
    sizes = malloc(comm_size * sizeof(*sizes));
    if (sizes == NULL) goto fini;
    self_addr_str_size = (int)strlen(mid_state->self_addr_str) + 1;
    sizes[comm_rank] = self_addr_str_size;
    MPI_Allgather(MPI_IN_PLACE, 0, MPI_BYTE, sizes, 1, MPI_INT, comm);

    /* compute a exclusive prefix sum of the data sizes, including the
     * total at the end
     */
    sizes_psum = malloc((comm_size+1) * sizeof(*sizes_psum));
    if (sizes_psum == NULL) goto fini;
    sizes_psum[0] = 0;
    for (i = 1; i < comm_size+1; i++)
        sizes_psum[i] = sizes_psum[i-1] + sizes[i-1];

    /* allgather the addresses */
    addr_str_buf = malloc(sizes_psum[comm_size]);
    if (addr_str_buf == NULL) goto fini;
    MPI_Allgatherv(mid_state->self_addr_str, self_addr_str_size, MPI_BYTE,
            addr_str_buf, sizes, sizes_psum, MPI_BYTE, comm);

    /* set up address string array for group members */
    addr_strs = (const char **)ssg_addr_str_buf_to_list(addr_str_buf, comm_size);
    if (!addr_strs) goto fini;

    /* invoke the generic group create routine using our list of addrs */
    g_id = ssg_group_create_internal(mid_state, group_name, addr_strs, comm_size,
        group_conf, update_cb, update_cb_dat);

fini:
    /* cleanup before returning */
    free(sizes);
    free(sizes_psum);
    free(addr_str_buf);
    free(addr_strs);
    if (g_id == SSG_GROUP_ID_INVALID)
        ssg_release_mid_state(mid_state);

    return g_id;
}
#endif

#ifdef SSG_HAVE_PMIX
ssg_group_id_t ssg_group_create_pmix(
    margo_instance_id mid,
    const char * group_name,
    const pmix_proc_t proc,
    ssg_group_config_t *group_conf,
    ssg_membership_update_cb update_cb,
    void * update_cb_dat)
{
    ssg_mid_state_t *mid_state;
    pmix_proc_t tmp_proc;
    pmix_data_array_t my_ids_array, *tmp_id_array_ptr;
    pmix_value_t value;
    pmix_value_t *val_p;
    pmix_value_t *addr_vals = NULL;
    unsigned int nprocs = 0;
    char key[512];
    pmix_info_t *info;
    bool flag;
    const char **addr_strs = NULL;
    unsigned int n;
    size_t i;
    int match;
    ssg_member_id_t *ids;
    pmix_status_t ret;
    ssg_group_id_t g_id = SSG_GROUP_ID_INVALID;

    if (!ssg_rt || !PMIx_Initialized()) goto fini;

    mid_state = ssg_acquire_mid_state(mid);
    if(!mid_state) goto fini;

    /* we need to store a mapping of PMIx ranks to SSG member IDs so that
     * if we later receive notice of a PMIx rank failure we know how to
     * map to affected SSG group members
     */
    snprintf(key, 512, "ssg-%s-%d-id", proc.nspace, proc.rank);
    PMIX_INFO_CREATE(info, 1);
    flag = true;
    PMIX_INFO_LOAD(info, PMIX_IMMEDIATE, &flag, PMIX_BOOL);
    ret = PMIx_Get(&proc, key, info, 1, &val_p);
    PMIX_INFO_FREE(info, 1);
    if (ret != PMIX_SUCCESS)
    {
        /* no key present, add the rank mapping for the first time */
        my_ids_array.type = PMIX_UINT64;
        my_ids_array.size = 1;
        my_ids_array.array = &mid_state->self_id;
        PMIX_VALUE_LOAD(&value, &my_ids_array, PMIX_DATA_ARRAY);
        ret = PMIx_Put(PMIX_GLOBAL, key, &value);
        if (ret != PMIX_SUCCESS)
            fprintf(stderr, "Warning: unable to store PMIx rank->ID mapping for"\
                "SSG member %lu\n", mid_state->self_id);
    }
    else
    {
        /* rank mapping found, see if we need to add this self ID to it */
        tmp_id_array_ptr = val_p->data.darray;
        if (tmp_id_array_ptr && (tmp_id_array_ptr->type == PMIX_UINT64))
        {
            match = 0;
            ids = (ssg_member_id_t *)tmp_id_array_ptr->array;
            for (i = 0; i < tmp_id_array_ptr->size; i++)
            {
                if (ids[i] == mid_state->self_id)
                {
                    match = 1;
                    break;
                }
            }

            if (!match)
            {
                /* update existing mapping to include this self ID */
                ids = malloc((tmp_id_array_ptr->size + 1) * sizeof(*ids));
                if (!ids) goto fini;
                memcpy(ids, tmp_id_array_ptr->array,
                    tmp_id_array_ptr->size * sizeof(*ids));
                ids[tmp_id_array_ptr->size + 1] = mid_state->self_id;
                my_ids_array.type = PMIX_UINT64;
                my_ids_array.size = tmp_id_array_ptr->size + 1;
                my_ids_array.array = ids;
                PMIX_VALUE_LOAD(&value, &my_ids_array, PMIX_DATA_ARRAY);
                ret = PMIx_Put(PMIX_GLOBAL, key, &value);
                free(ids);
                if (ret != PMIX_SUCCESS)
                    fprintf(stderr, "Warning: unable to store PMIx rank->ID mapping for"\
                        "SSG member %lu\n", mid_state->self_id);
            }
        }
        else
        {
            fprintf(stderr, "Warning: unexpected format for PMIx rank->ID mapping\n");
        }
        PMIX_VALUE_RELEASE(val_p);
    }

    /* XXX note we are assuming every process in the job wants to join this group... */
    /* get the total nprocs in the job */
    PMIX_PROC_LOAD(&tmp_proc, proc.nspace, PMIX_RANK_WILDCARD);
    ret = PMIx_Get(&tmp_proc, PMIX_JOB_SIZE, NULL, 0, &val_p);
    if (ret != PMIX_SUCCESS) goto fini;
    nprocs = (int)val_p->data.uint32;
    PMIX_VALUE_RELEASE(val_p);

    /* put my address string using a well-known key */
    snprintf(key, 512, "ssg-%s-%s-%d-hg-addr", group_name, proc.nspace, proc.rank);
    PMIX_VALUE_LOAD(&value, mid_state->self_addr_str, PMIX_STRING);
    ret = PMIx_Put(PMIX_GLOBAL, key, &value);
    if (ret != PMIX_SUCCESS) goto fini;

    /* commit the put data to the local pmix server */
    ret = PMIx_Commit();
    if (ret != PMIX_SUCCESS) goto fini;

    /* barrier, additionally requesting to collect relevant process data */
    PMIX_INFO_CREATE(info, 1);
    flag = true;
    PMIX_INFO_LOAD(info, PMIX_COLLECT_DATA, &flag, PMIX_BOOL);
    ret = PMIx_Fence(&proc, 1, info, 1);
    PMIX_INFO_FREE(info, 1);
    if (ret != PMIX_SUCCESS) goto fini;

    addr_strs = malloc(nprocs * sizeof(*addr_strs));
    if (addr_strs == NULL) goto fini;

    /* finalize exchange by getting each member's address */
    PMIX_VALUE_CREATE(addr_vals, nprocs);
    for (n = 0; n < nprocs; n++)
    {
        /* skip ourselves */
        if(n == proc.rank)
        {
            addr_strs[n] = mid_state->self_addr_str;
            continue;
        }

        if (snprintf(key, 128, "ssg-%s-%s-%d-hg-addr", group_name,
            proc.nspace, n) >= 128) goto fini;

        tmp_proc.rank = n;
        val_p = &addr_vals[n];
        ret = PMIx_Get(&tmp_proc, key, NULL, 0, &val_p);
        if (ret != PMIX_SUCCESS) goto fini;

        addr_strs[n] = val_p->data.string;
    }

    /* invoke the generic group create routine using our list of addrs */
    g_id = ssg_group_create_internal(mid_state, group_name, addr_strs, nprocs,
        group_conf, update_cb, update_cb_dat);

fini:
    /* cleanup before returning */
    free(addr_strs);
    PMIX_VALUE_FREE(addr_vals, nprocs);
    if (g_id == SSG_GROUP_ID_INVALID)
        ssg_release_mid_state(mid_state);

    return g_id;
}
#endif 

int ssg_group_destroy(
    ssg_group_id_t group_id)
{
    ssg_group_descriptor_t *g_desc;

    if (!ssg_rt || group_id == SSG_GROUP_ID_INVALID) return SSG_FAILURE;

    ABT_rwlock_wrlock(ssg_rt->lock);

    /* find the group structure and destroy it */
    HASH_FIND(hh, ssg_rt->g_desc_table, &group_id, sizeof(ssg_group_id_t), g_desc);
    if (!g_desc)
    {
        ABT_rwlock_unlock(ssg_rt->lock);
        fprintf(stderr, "Error: SSG unable to find expected group ID\n");
        return SSG_FAILURE;
    }
    HASH_DEL(ssg_rt->g_desc_table, g_desc);

    ABT_rwlock_unlock(ssg_rt->lock);

    /* destroy the group, free the descriptor */
    ssg_group_destroy_internal(g_desc->g_data.g);
    ssg_group_descriptor_free(g_desc);

    return SSG_SUCCESS;
}

int ssg_group_join_target(
    margo_instance_id mid,
    ssg_group_id_t group_id,
    const char *target_addr_str,
    ssg_membership_update_cb update_cb,
    void * update_cb_dat)
{
    ssg_mid_state_t *mid_state=NULL;
    ssg_group_descriptor_t *g_desc;
    hg_addr_t target_addr = HG_ADDR_NULL;
    char *group_name = NULL;
    int group_size;
    ssg_group_config_t group_config;
    void *view_buf = NULL;
    const char **addr_strs = NULL;
    ssg_group_id_t create_g_id;
    hg_return_t hret;
    int sret = SSG_FAILURE;

    if (!ssg_rt || group_id == SSG_GROUP_ID_INVALID) goto fini;

    ABT_rwlock_wrlock(ssg_rt->lock);

    /* find the group structure to join */
    HASH_FIND(hh, ssg_rt->g_desc_table, &group_id, sizeof(ssg_group_id_t), g_desc);
    if (!g_desc)
    {
        ABT_rwlock_unlock(ssg_rt->lock);
        fprintf(stderr, "Error: SSG unable to find expected group ID\n");
        goto fini;
    }

    if (g_desc->owner_status == SSG_OWNER_IS_MEMBER)
    {
        ABT_rwlock_unlock(ssg_rt->lock);
        fprintf(stderr, "Error: SSG unable to join a group it is already a member of\n");
        goto fini;
    }
    else if (g_desc->owner_status == SSG_OWNER_IS_OBSERVER)
    {
        ABT_rwlock_unlock(ssg_rt->lock);
        fprintf(stderr, "Error: SSG unable to join a group it is an observer of\n");
        goto fini;
    }

    /* remove the descriptor since we re-add it as part of group creation */
    HASH_DEL(ssg_rt->g_desc_table, g_desc);

    ABT_rwlock_unlock(ssg_rt->lock);

    mid_state = ssg_acquire_mid_state(mid);
    if(!mid_state) goto fini;

    /* if no target specified, use random address string from descriptor */
    if (!target_addr_str)
    {
        int addr_index = mid_state->self_id % g_desc->num_addr_strs;
        target_addr_str = g_desc->addr_strs[addr_index];
    }

    hret = margo_addr_lookup(mid_state->mid, target_addr_str,
        &target_addr);
    if (hret != HG_SUCCESS) goto fini;

    sret = ssg_group_join_send(group_id, target_addr, mid_state,
        &group_name, &group_size, &group_config, &view_buf);
    if (sret != SSG_SUCCESS || !group_name || !view_buf) goto fini;

    /* free old descriptor */
    ssg_group_descriptor_free(g_desc);

    /* set up address string array for all group members */
    addr_strs = (const char **)ssg_addr_str_buf_to_list(view_buf, group_size);
    if (!addr_strs) goto fini;

    /* append self address string to list of group member address strings */
    addr_strs = realloc(addr_strs, (group_size+1)*sizeof(char *));
    if(!addr_strs) goto fini;
    addr_strs[group_size++] = mid_state->self_addr_str;

    create_g_id = ssg_group_create_internal(mid_state, group_name, addr_strs, group_size,
            &group_config, update_cb, update_cb_dat);

    if (create_g_id != SSG_GROUP_ID_INVALID)
    {
        assert(create_g_id == group_id);
        sret = SSG_SUCCESS;

        /* don't free on success */
        group_name = NULL;
    }

fini:
    if (target_addr != HG_ADDR_NULL)
        margo_addr_free(mid_state->mid, target_addr);
    free(addr_strs);
    free(view_buf);
    free(group_name);

    return sret;
}

int ssg_group_leave_target(
    ssg_group_id_t group_id,
    const char *target_addr_str)
{
    ssg_group_descriptor_t *g_desc;
    hg_addr_t target_addr = HG_ADDR_NULL;
    hg_return_t hret;
    int sret = SSG_FAILURE;

    if (!ssg_rt || group_id == SSG_GROUP_ID_INVALID) return sret;

    ABT_rwlock_wrlock(ssg_rt->lock);

    /* find the group structure to leave */
    HASH_FIND(hh, ssg_rt->g_desc_table, &group_id, sizeof(ssg_group_id_t), g_desc);
    if (!g_desc)
    {
        ABT_rwlock_unlock(ssg_rt->lock);
        fprintf(stderr, "Error: SSG unable to find expected group ID\n");
        return sret;
    }

    if (g_desc->owner_status != SSG_OWNER_IS_MEMBER)
    {
        ABT_rwlock_unlock(ssg_rt->lock);
        fprintf(stderr, "Error: SSG unable to leave group it is not a member of\n");
        return sret;
    }

    /* remove the descriptor */
    HASH_DEL(ssg_rt->g_desc_table, g_desc);

    ABT_rwlock_unlock(ssg_rt->lock);

    if (!target_addr_str)
    {
        /* if no target specified, just send to first member in our view */
        ABT_rwlock_rdlock(g_desc->g_data.g->lock);
        if (g_desc->g_data.g->view.size > 1)
            target_addr = g_desc->g_data.g->view.member_map->addr;
        ABT_rwlock_unlock(g_desc->g_data.g->lock);
    }
    else
    {
        hret = margo_addr_lookup(g_desc->g_data.g->mid_state->mid, target_addr_str,
            &target_addr);
        if (hret != HG_SUCCESS) return sret;
    }

    if (target_addr != HG_ADDR_NULL)
    {
        /* send leave request to target member if one is available */
        sret = ssg_group_leave_send(group_id, target_addr,
            g_desc->g_data.g->mid_state);
        /* XXX note that the leave request forward is best effort currently --
         * it is possible that no other group member receives the leave request,
         * in which case the member will have to be evicted by fault detection
         */

        if (target_addr_str)
            margo_addr_free(g_desc->g_data.g->mid_state->mid, target_addr);
    }

    /* at this point we've tried forwarding the leave request to a group member --
     * safe to shutdown the group locally
     */

    /* destroy group and free old descriptor */
    ssg_group_destroy_internal(g_desc->g_data.g);
    ssg_group_descriptor_free(g_desc);

    sret = SSG_SUCCESS;

    return sret;
}

int ssg_group_observe_target(
    margo_instance_id mid,
    ssg_group_id_t group_id,
    const char *target_addr_str)
{
    ssg_mid_state_t *mid_state = NULL;
    ssg_group_descriptor_t *g_desc;
    hg_addr_t target_addr = HG_ADDR_NULL;
    ssg_observed_group_t *og = NULL;
    char *group_name = NULL;
    int group_size;
    void *view_buf = NULL;
    const char **addr_strs = NULL;
    hg_return_t hret;
    int sret = SSG_FAILURE;

    if (!ssg_rt || group_id == SSG_GROUP_ID_INVALID) {
	fprintf(stderr, "SSG init not called or Invalid group id\n");
	goto fini;
    }

    ABT_rwlock_rdlock(ssg_rt->lock);

    /* find the group structure to observe */
    HASH_FIND(hh, ssg_rt->g_desc_table, &group_id, sizeof(ssg_group_id_t), g_desc);
    if (!g_desc)
    {
        fprintf(stderr, "Error: SSG unable to find expected group ID\n");
        ABT_rwlock_unlock(ssg_rt->lock);
        goto fini;
    }

    if (g_desc->owner_status == SSG_OWNER_IS_MEMBER)
    {
        fprintf(stderr, "Error: SSG unable to observe a group it is a member of\n");
        ABT_rwlock_unlock(ssg_rt->lock);
        goto fini;
    }
    else if (g_desc->owner_status == SSG_OWNER_IS_OBSERVER)
    {
        fprintf(stderr, "Error: SSG unable to observe a group it is already observing\n");
        ABT_rwlock_unlock(ssg_rt->lock);
        goto fini;
    }

    /* remove the descriptor  */
    HASH_DEL(ssg_rt->g_desc_table, g_desc);

    ABT_rwlock_unlock(ssg_rt->lock);

    mid_state = ssg_acquire_mid_state(mid);
    if(!mid_state) {
	fprintf(stderr, "Error: ssg_acquire_mid_state failed\n");
	goto fini;
    }

    /* if no target specified, use random address string from descriptor */
    if (!target_addr_str)
    {
        int addr_index = mid_state->self_id % g_desc->num_addr_strs;
        target_addr_str = g_desc->addr_strs[addr_index];
    }

    hret = margo_addr_lookup(mid_state->mid, target_addr_str,
        &target_addr);
    if (hret != HG_SUCCESS) {
	fprintf(stderr, "unable to resolve address\n");
	goto fini;
    }
    /* send the observe request to the target to initiate a bulk transfer
     * of the group's membership view
     */
    sret = ssg_group_observe_send(group_id, target_addr, mid_state,
        &group_name, &group_size, &view_buf);
    if (sret != SSG_SUCCESS || !group_name || !view_buf) {
	fprintf(stderr, "unable to send observe request (ret: %d; %lu %p %p)\n", sret, group_id, group_name, view_buf);
	goto fini;
    }

    /* set up address string array for all group members */
    addr_strs = (const char **)ssg_addr_str_buf_to_list(view_buf, group_size);
    if (!addr_strs) {
	fprintf(stderr, "unable to set up address string array\n");
	goto fini;
    }

    /* allocate an SSG observed group data structure and initialize some of it */
    og = malloc(sizeof(*og));
    if (!og) goto fini;
    memset(og, 0, sizeof(*og));
    og->mid_state = mid_state;
    og->name = strdup(group_name);
    ABT_rwlock_create(&og->lock);

    /* create the view for the group */
    sret = ssg_group_view_create(addr_strs, group_size, NULL, mid_state,
        og->lock, &og->view);
    if (sret != SSG_SUCCESS) {
	fprintf(stderr, "unable to create view\n");
	goto fini;
    }

    /* add this group reference to our group table */
    ABT_rwlock_wrlock(ssg_rt->lock);
    g_desc->owner_status = SSG_OWNER_IS_OBSERVER;
    g_desc->g_data.og = og;
    HASH_ADD(hh, ssg_rt->g_desc_table, g_id, sizeof(ssg_group_id_t), g_desc);
    ABT_rwlock_unlock(ssg_rt->lock);

    sret = SSG_SUCCESS;

    /* don't free on success */
    free(group_name);
    group_name = NULL;

    og = NULL;
fini:
    if (target_addr != HG_ADDR_NULL)
        margo_addr_free(mid_state->mid, target_addr);
    if (og)
    {
        ssg_group_view_destroy(&og->view, og->mid_state->mid);
        ABT_rwlock_free(&og->lock);
        free(og->name);
        free(og);
    }
    free(addr_strs);
    free(view_buf);
    free(group_name);
    if ((sret == SSG_FAILURE) && mid_state)
        ssg_release_mid_state(mid_state);

    return sret;
}

int ssg_group_unobserve(
    ssg_group_id_t group_id)
{
    ssg_group_descriptor_t *g_desc;

    if (!ssg_rt || group_id == SSG_GROUP_ID_INVALID) return SSG_FAILURE;

    ABT_rwlock_wrlock(ssg_rt->lock);

    /* find the group structure to unobserve */
    HASH_FIND(hh, ssg_rt->g_desc_table, &group_id, sizeof(ssg_group_id_t), g_desc);
    if (!g_desc)
    {
        fprintf(stderr, "Error: SSG unable to find expected group ID\n");
        ABT_rwlock_unlock(ssg_rt->lock);
        return SSG_FAILURE;
    }

    if (g_desc->owner_status != SSG_OWNER_IS_OBSERVER)
    {
        fprintf(stderr, "Error: SSG unable to unobserve group that was never observed\n");
        ABT_rwlock_unlock(ssg_rt->lock);
        return SSG_FAILURE;
    }
    HASH_DEL(ssg_rt->g_desc_table, g_desc);

    ABT_rwlock_unlock(ssg_rt->lock);

    ssg_observed_group_destroy(g_desc->g_data.og);
    ssg_group_descriptor_free(g_desc);

    return SSG_SUCCESS;
}

/*********************************************************
 *** SSG routines for obtaining self/group information ***
 *********************************************************/

ssg_member_id_t ssg_get_self_id(
    margo_instance_id mid)
{
    ssg_mid_state_t *mid_state;
    ssg_member_id_t self_id = SSG_MEMBER_ID_INVALID;

    if (!ssg_rt) return SSG_MEMBER_ID_INVALID;

    ABT_rwlock_rdlock(ssg_rt->lock);
    LL_SEARCH_SCALAR(ssg_rt->mid_list, mid_state, mid, mid);
    if(mid_state) self_id = mid_state->self_id;
    ABT_rwlock_unlock(ssg_rt->lock);

    return self_id;
}

int ssg_get_group_size(
    ssg_group_id_t group_id)
{
    ssg_group_descriptor_t *g_desc;
    int group_size = 0;

    if (!ssg_rt || group_id == SSG_GROUP_ID_INVALID) return 0;

    ABT_rwlock_rdlock(ssg_rt->lock);

    /* find the group descriptor */
    HASH_FIND(hh, ssg_rt->g_desc_table, &group_id, sizeof(ssg_group_id_t), g_desc);
    if (!g_desc)
    {
        ABT_rwlock_unlock(ssg_rt->lock);
        fprintf(stderr, "Error: SSG unable to find expected group ID\n");
        return 0;
    }

    if (g_desc->owner_status == SSG_OWNER_IS_MEMBER)
    {
        ABT_rwlock_rdlock(g_desc->g_data.g->lock);
        group_size = g_desc->g_data.g->view.size;
        ABT_rwlock_unlock(g_desc->g_data.g->lock);
    }
    else if (g_desc->owner_status == SSG_OWNER_IS_OBSERVER)
    {
        ABT_rwlock_rdlock(g_desc->g_data.og->lock);
        group_size = g_desc->g_data.og->view.size;
        ABT_rwlock_unlock(g_desc->g_data.og->lock);
    }
    else
    {
        fprintf(stderr, "Error: SSG can only obtain size of groups that the caller" \
            " is a member of or an observer of\n");
    }

    ABT_rwlock_unlock(ssg_rt->lock);

    return group_size;
}

hg_addr_t ssg_get_group_member_addr(
    ssg_group_id_t group_id,
    ssg_member_id_t member_id)
{
    ssg_group_descriptor_t *g_desc;
    ssg_member_state_t *member_state;
    hg_addr_t member_addr = HG_ADDR_NULL;

    if (!ssg_rt || group_id == SSG_GROUP_ID_INVALID ||
            member_id == SSG_MEMBER_ID_INVALID)
        return HG_ADDR_NULL;

    ABT_rwlock_rdlock(ssg_rt->lock);

    /* find the group descriptor */
    HASH_FIND(hh, ssg_rt->g_desc_table, &group_id, sizeof(ssg_group_id_t), g_desc);
    if (!g_desc)
    {
        ABT_rwlock_unlock(ssg_rt->lock);
        fprintf(stderr, "Error: SSG unable to find expected group ID\n");
        return HG_ADDR_NULL;
    }

    if (g_desc->owner_status == SSG_OWNER_IS_MEMBER)
    {
        ssg_group_t *g = g_desc->g_data.g;

        if (member_id == g->mid_state->self_id)
            member_addr = g->mid_state->self_addr;
        else
        {
            ABT_rwlock_rdlock(g->lock);
            HASH_FIND(hh, g->view.member_map, &member_id,
                sizeof(ssg_member_id_t), member_state);
            if (member_state) 
                member_addr = member_state->addr;
            ABT_rwlock_unlock(g->lock);
        }
    }
    else if (g_desc->owner_status == SSG_OWNER_IS_OBSERVER)
    {
        ssg_observed_group_t *og = g_desc->g_data.og;

        ABT_rwlock_rdlock(og->lock);
        HASH_FIND(hh, og->view.member_map, &member_id,
            sizeof(ssg_member_id_t), member_state);
        if (member_state) 
            member_addr = member_state->addr;
        ABT_rwlock_unlock(og->lock);
    }
    else
    {
        fprintf(stderr, "Error: SSG can only obtain member addresses of groups" \
            " that the caller is a member of or an observer of\n");
    }

    ABT_rwlock_unlock(ssg_rt->lock);

    return member_addr;
}

int ssg_get_group_self_rank(
    ssg_group_id_t group_id)
{
    ssg_group_descriptor_t *g_desc;
    int rank;

    if (!ssg_rt || group_id == SSG_GROUP_ID_INVALID) return -1;

    ABT_rwlock_rdlock(ssg_rt->lock);

    HASH_FIND(hh, ssg_rt->g_desc_table, &group_id, sizeof(ssg_group_id_t), g_desc);
    if (!g_desc)
    {
        ABT_rwlock_unlock(ssg_rt->lock);
        fprintf(stderr, "Error: SSG unable to find expected group ID\n");
        return -1;
    }

    if (g_desc->owner_status != SSG_OWNER_IS_MEMBER)
    {
        ABT_rwlock_unlock(ssg_rt->lock);
        fprintf(stderr, "Error: SSG unable to obtain self rank for non-group members\n");
        return -1;
    }

    ABT_rwlock_rdlock(g_desc->g_data.g->lock);
    rank = ssg_get_group_member_rank_internal(&g_desc->g_data.g->view,
        g_desc->g_data.g->mid_state->self_id);
    ABT_rwlock_unlock(g_desc->g_data.g->lock);

    ABT_rwlock_unlock(ssg_rt->lock);

    return rank;
}

int ssg_get_group_member_rank(
    ssg_group_id_t group_id,
    ssg_member_id_t member_id)
{
    ssg_group_descriptor_t *g_desc;
    int rank;

    if (!ssg_rt || group_id == SSG_GROUP_ID_INVALID) return -1;

    ABT_rwlock_rdlock(ssg_rt->lock);

    HASH_FIND(hh, ssg_rt->g_desc_table, &group_id, sizeof(ssg_group_id_t), g_desc);
    if (!g_desc)
    {
        ABT_rwlock_unlock(ssg_rt->lock);
        fprintf(stderr, "Error: SSG unable to find expected group ID\n");
        return -1;
    }

    if (g_desc->owner_status == SSG_OWNER_IS_MEMBER)
    {
        ssg_group_t *g = g_desc->g_data.g;

        ABT_rwlock_rdlock(g->lock);
        rank = ssg_get_group_member_rank_internal(&g->view, member_id);
        ABT_rwlock_unlock(g->lock);
    }
    else if (g_desc->owner_status == SSG_OWNER_IS_OBSERVER)
    {
        ssg_observed_group_t *og = g_desc->g_data.og;

        ABT_rwlock_rdlock(og->lock);
        rank = ssg_get_group_member_rank_internal(&og->view, member_id);
        ABT_rwlock_unlock(og->lock);
    }
    else
    {
        ABT_rwlock_unlock(ssg_rt->lock);
        fprintf(stderr, "Error: SSG unable to obtain rank for group caller is"
            "not a member or an observer of\n");
        return -1;
    }

    ABT_rwlock_unlock(ssg_rt->lock);

    return rank;
}

ssg_member_id_t ssg_get_group_member_id_from_rank(
    ssg_group_id_t group_id,
    int rank)
{
    ssg_group_descriptor_t *g_desc;
    ssg_member_id_t member_id;

    if (!ssg_rt || group_id == SSG_GROUP_ID_INVALID || rank < 0)
        return SSG_MEMBER_ID_INVALID;

    ABT_rwlock_rdlock(ssg_rt->lock);

    HASH_FIND(hh, ssg_rt->g_desc_table, &group_id, sizeof(ssg_group_id_t), g_desc);
    if (!g_desc)
    {
        ABT_rwlock_unlock(ssg_rt->lock);
        fprintf(stderr, "Error: SSG unable to find expected group ID\n");
        return SSG_MEMBER_ID_INVALID;
    }

    if (g_desc->owner_status == SSG_OWNER_IS_MEMBER)
    {
        ssg_group_t *g = g_desc->g_data.g;

        ABT_rwlock_rdlock(g->lock);
        if (rank >= (int)g->view.size)
        {
            ABT_rwlock_unlock(g->lock);
            ABT_rwlock_unlock(ssg_rt->lock);
            return SSG_MEMBER_ID_INVALID;
        }

        member_id = *(ssg_member_id_t *)utarray_eltptr(
            g->view.rank_array, (unsigned int)rank);
        ABT_rwlock_unlock(g->lock);
    }
    else if (g_desc->owner_status == SSG_OWNER_IS_OBSERVER)
    {
        ssg_observed_group_t *og = g_desc->g_data.og;

        ABT_rwlock_rdlock(og->lock);
        if (rank >= (int)og->view.size)
        {
            ABT_rwlock_unlock(og->lock);
            ABT_rwlock_unlock(ssg_rt->lock);
            return SSG_MEMBER_ID_INVALID;
        }

        member_id = *(ssg_member_id_t *)utarray_eltptr(
            og->view.rank_array, (unsigned int)rank);
        ABT_rwlock_unlock(og->lock);
    }
    else
    {
        ABT_rwlock_unlock(ssg_rt->lock);
        fprintf(stderr, "Error: SSG unable to obtain member ID for group caller is"
            "not a member or an observer of\n");
        return SSG_MEMBER_ID_INVALID;
    }

    ABT_rwlock_unlock(ssg_rt->lock);

    return member_id;
}

int ssg_get_group_member_ids_from_range(
    ssg_group_id_t group_id,
    int rank_start,
    int rank_end,
    ssg_member_id_t *range_ids)
{
    ssg_group_descriptor_t *g_desc;
    ssg_member_id_t *member_start;

    if (!ssg_rt || group_id == SSG_GROUP_ID_INVALID || rank_start < 0 ||
            rank_end < 0 || rank_end <= rank_start)
        return 0;

    ABT_rwlock_rdlock(ssg_rt->lock);

    HASH_FIND(hh, ssg_rt->g_desc_table, &group_id, sizeof(ssg_group_id_t), g_desc);
    if (!g_desc)
    {
        ABT_rwlock_unlock(ssg_rt->lock);
        fprintf(stderr, "Error: SSG unable to find expected group ID\n");
        return 0;
    }

    if (g_desc->owner_status == SSG_OWNER_IS_MEMBER)
    {
        ssg_group_t *g = g_desc->g_data.g;

        ABT_rwlock_rdlock(g->lock);
        if (rank_end >= (int)g->view.size)
        {
            ABT_rwlock_unlock(g->lock);
            ABT_rwlock_unlock(ssg_rt->lock);
            return 0;
        }

        member_start = (ssg_member_id_t *)utarray_eltptr(
            g->view.rank_array, (unsigned int)rank_start);
        memcpy(range_ids, member_start, (rank_end-rank_start)*sizeof(ssg_member_id_t));
        ABT_rwlock_unlock(g->lock);
    }
    else if (g_desc->owner_status == SSG_OWNER_IS_OBSERVER)
    {
        ssg_observed_group_t *og = g_desc->g_data.og;

        ABT_rwlock_rdlock(og->lock);
        if (rank_end >= (int)og->view.size)
        {
            ABT_rwlock_unlock(og->lock);
            ABT_rwlock_unlock(ssg_rt->lock);
            return 0;
        }

        member_start = (ssg_member_id_t *)utarray_eltptr(
            og->view.rank_array, (unsigned int)rank_start);
        memcpy(range_ids, member_start, (rank_end-rank_start)*sizeof(ssg_member_id_t));
        ABT_rwlock_unlock(og->lock);
    }
    else
    {
        ABT_rwlock_unlock(ssg_rt->lock);
        fprintf(stderr, "Error: SSG unable to obtain member ID for group caller is"
            "not a member or an observer of\n");
        return 0;
    }
    ABT_rwlock_unlock(ssg_rt->lock);

    return (rank_end-rank_start);
}

char *ssg_group_id_get_addr_str(
    ssg_group_id_t group_id,
    unsigned int addr_index)
{
    ssg_group_descriptor_t *g_desc;
    char *addr_str;

    if (!ssg_rt || group_id == SSG_GROUP_ID_INVALID) return NULL;

    ABT_rwlock_rdlock(ssg_rt->lock);

    /* find the group descriptor */
    HASH_FIND(hh, ssg_rt->g_desc_table, &group_id, sizeof(ssg_group_id_t), g_desc);
    if (!g_desc)
    {
        ABT_rwlock_unlock(ssg_rt->lock);
        fprintf(stderr, "Error: SSG unable to find expected group ID\n");
        return NULL;
    }

    if (addr_index >= g_desc->num_addr_strs)
    {
        ABT_rwlock_unlock(ssg_rt->lock);
        return NULL;
    }

    addr_str = strdup(g_desc->addr_strs[addr_index]);

    ABT_rwlock_unlock(ssg_rt->lock);

    return addr_str;
}

int64_t ssg_group_id_get_cred(
    ssg_group_id_t group_id)
{
    ssg_group_descriptor_t *g_desc;
    int64_t cred;

    if (!ssg_rt || group_id == SSG_GROUP_ID_INVALID) return -1;

    ABT_rwlock_rdlock(ssg_rt->lock);

    /* find the group descriptor */
    HASH_FIND(hh, ssg_rt->g_desc_table, &group_id, sizeof(ssg_group_id_t), g_desc);
    if (!g_desc)
    {
        ABT_rwlock_unlock(ssg_rt->lock);
        fprintf(stderr, "Error: SSG unable to find expected group ID\n");
        return -1;
    }

    cred = g_desc->cred;

    ABT_rwlock_unlock(ssg_rt->lock);

    return cred;
}

void ssg_group_id_serialize(
    ssg_group_id_t group_id,
    int num_addrs,
    char ** buf_p,
    size_t * buf_size_p)
{
    ssg_group_descriptor_t *g_desc;
    uint64_t magic_nr = SSG_MAGIC_NR;
    uint64_t gid_size, addr_str_size = 0;
    uint32_t num_addrs_buf=0;
    char *gid_buf, *p;
    unsigned int i;
    ssg_group_view_t *view = NULL;
    ABT_rwlock view_lock;
    ssg_member_state_t *state, *tmp;

    *buf_p = NULL;
    *buf_size_p = 0;

    if (!ssg_rt || group_id == SSG_GROUP_ID_INVALID || num_addrs == 0) return;

    ABT_rwlock_rdlock(ssg_rt->lock);

    /* find the group descriptor */
    HASH_FIND(hh, ssg_rt->g_desc_table, &group_id, sizeof(ssg_group_id_t), g_desc);
    if (!g_desc)
    {
        ABT_rwlock_unlock(ssg_rt->lock);
        fprintf(stderr, "Error: SSG unable to find expected group ID\n");
        return;
    }

    /* determine needed buffer size */
    gid_size = (sizeof(magic_nr) + sizeof(g_desc->g_id) + sizeof(num_addrs_buf));
    switch (g_desc->owner_status)
    {
        case SSG_OWNER_IS_UNASSOCIATED:
            assert(g_desc->addr_strs);
            if ((num_addrs == SSG_ALL_MEMBERS) ||
                    (g_desc->num_addr_strs < (unsigned int)num_addrs))
                num_addrs_buf = g_desc->num_addr_strs;
            else
                num_addrs_buf = num_addrs;
            for (i = 0; i < num_addrs_buf; i++)
                addr_str_size += strlen(g_desc->addr_strs[i]) + 1;
            break;
        case SSG_OWNER_IS_MEMBER:
            view = &g_desc->g_data.g->view;
            view_lock = g_desc->g_data.g->lock;
            i = 1;
            /* serialize self string first ... */
            addr_str_size += strlen(g_desc->g_data.g->mid_state->self_addr_str) + 1;
            /* deliberately falls through to view serialization.  Extra comment for gcc Wimplicit-fallthrough */
            // falls through
        case SSG_OWNER_IS_OBSERVER:
            if (!view)
            {
                view = &g_desc->g_data.og->view;
                view_lock = g_desc->g_data.og->lock;
                i = 0;
            }
            if ((num_addrs == SSG_ALL_MEMBERS) || (view->size < (unsigned int)num_addrs))
                num_addrs_buf = view->size;
            else
                num_addrs_buf = num_addrs;
            ABT_rwlock_rdlock(view_lock);
            HASH_ITER(hh, view->member_map, state, tmp)
            {
                if (i == num_addrs_buf) break;
                addr_str_size += strlen(state->addr_str) + 1;
                i++;
            }
            ABT_rwlock_unlock(view_lock);
            break;
    }
    if (g_desc->cred >= 0)
        gid_size += sizeof(g_desc->cred);

    gid_buf = malloc((size_t)(gid_size + addr_str_size));
    if (!gid_buf)
    {
        ABT_rwlock_unlock(ssg_rt->lock);
        return;
    }

    /* serialize */
    p = gid_buf;
    *(uint64_t *)p = magic_nr;
    p += sizeof(magic_nr);
    *(ssg_group_id_t *)p = g_desc->g_id;
    p += sizeof(g_desc->g_id);
    *(uint32_t *)p = num_addrs_buf;
    p += sizeof(num_addrs_buf);
    i = 0;
    switch (g_desc->owner_status)
    {
        case SSG_OWNER_IS_UNASSOCIATED:
            for (; i < num_addrs_buf; i++)
            {
                strcpy(p, g_desc->addr_strs[i]);
                p += strlen(g_desc->addr_strs[i]) + 1;
            }
            break;
        case SSG_OWNER_IS_MEMBER:
            /* serialize self string first ... */
            i = 1;
            strcpy(p, g_desc->g_data.g->mid_state->self_addr_str);
            p += strlen(g_desc->g_data.g->mid_state->self_addr_str) + 1;
            /* fall-through to view serialization.  Next line for gcc Wimplicit-fallthrough */
            // falls through
        case SSG_OWNER_IS_OBSERVER:
            HASH_ITER(hh, view->member_map, state, tmp)
            {
                if (i == num_addrs_buf) break;
                strcpy(p, state->addr_str);
                p += strlen(state->addr_str) + 1;
                i++;
            }
            break;
    }
    if (g_desc->cred >= 0)
        *(int64_t *)p = g_desc->cred;
    /* the rest of the descriptor is stateful and not appropriate for serializing... */

    ABT_rwlock_unlock(ssg_rt->lock);

    *buf_p = gid_buf;
    *buf_size_p = gid_size + addr_str_size;

    return;
}

void ssg_group_id_deserialize(
    const char * buf,
    size_t buf_size,
    int * num_addrs,
    ssg_group_id_t * group_id_p)
{
    const char *tmp_buf = buf;
    size_t min_buf_size;
    uint64_t magic_nr;
    ssg_group_id_t g_id;
    uint32_t num_addrs_buf;
    int tmp_num_addrs = *num_addrs;
    char **addr_strs;
    int64_t cred;
    int i;
    ssg_group_descriptor_t *g_desc;

    *group_id_p = SSG_GROUP_ID_INVALID;
    *num_addrs = 0;

    if (!ssg_rt || !buf || buf_size == 0 || tmp_num_addrs == 0) return;

    /* check to ensure the buffer contains enough data to make a group ID */
    min_buf_size = (sizeof(magic_nr) + sizeof(g_desc->g_id) + sizeof(num_addrs_buf) + 1);
    if (buf_size < min_buf_size)
    {
        fprintf(stderr, "Error: Serialized buffer does not contain a valid SSG group ID\n");
        return;
    }

    /* deserialize */
    magic_nr = *(uint64_t *)tmp_buf;
    if (magic_nr != SSG_MAGIC_NR)
    {
        fprintf(stderr, "Error: Magic number mismatch when deserializing SSG group ID\n");
        return;
    }
    tmp_buf += sizeof(uint64_t);
    g_id = *(ssg_group_id_t *)tmp_buf;
    tmp_buf += sizeof(ssg_group_id_t);
    num_addrs_buf = *(uint32_t *)tmp_buf;
    tmp_buf += sizeof(uint32_t);
    if ((tmp_num_addrs == SSG_ALL_MEMBERS) ||
            (num_addrs_buf < (unsigned int)tmp_num_addrs))
        tmp_num_addrs = num_addrs_buf;

    /* convert buffer of address strings to an arrray */
    addr_strs = ssg_addr_str_buf_to_list(tmp_buf, tmp_num_addrs);
    if (!addr_strs)
        return;
    tmp_buf = addr_strs[tmp_num_addrs - 1] + strlen(addr_strs[tmp_num_addrs - 1]) + 1;
    if ((buf_size - (tmp_buf - buf)) == sizeof(int64_t))
        cred = *(int64_t *)tmp_buf;
    else
        cred = -1;

    for (i = 0; i < tmp_num_addrs; i++)
    {
        /* we dup addresses from the given buf, since we don't know its lifetime */
        addr_strs[i] = strdup(addr_strs[i]);
    }

    g_desc = ssg_group_descriptor_create(g_id, tmp_num_addrs, addr_strs, cred,
        SSG_OWNER_IS_UNASSOCIATED);
    if (!g_desc)
    {
        for (i = 0; i < tmp_num_addrs; i++)
            free(addr_strs[i]);
        free(addr_strs);
        return;
    }

    /* add this group descriptor to our global table */
    /* NOTE: g_id is not associated with any group -- caller must join or observe first */
    ABT_rwlock_wrlock(ssg_rt->lock);
    HASH_ADD(hh, ssg_rt->g_desc_table, g_id, sizeof(ssg_group_id_t), g_desc);
    ABT_rwlock_unlock(ssg_rt->lock);

    *group_id_p = g_id;
    *num_addrs = tmp_num_addrs;

    return;
}

int ssg_group_id_store(
    const char * file_name,
    ssg_group_id_t group_id,
    int num_addrs)
{
    int fd;
    char *buf;
    size_t buf_size;
    ssize_t bytes_written;

    fd = open(file_name, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd < 0)
    {
        fprintf(stderr, "Error: Unable to open file %s for storing SSG group ID: %s\n",
            file_name, strerror(errno));
        return SSG_FAILURE;
    }

    ssg_group_id_serialize(group_id, num_addrs, &buf, &buf_size);
    if (buf == NULL)
    {
        fprintf(stderr, "Error: Unable to serialize SSG group ID.\n");
        close(fd);
        return SSG_FAILURE;
    }

    bytes_written = write(fd, buf, buf_size);
    if (bytes_written != (ssize_t)buf_size)
    {
        fprintf(stderr, "Error: Unable to write SSG group ID to file %s\n", file_name);
        close(fd);
        free(buf);
        return SSG_FAILURE;
    }

    close(fd);
    free(buf);
    return SSG_SUCCESS;
}

int ssg_group_id_load(
    const char * file_name,
    int * num_addrs,
    ssg_group_id_t * group_id_p)
{
    int fd;
    struct stat fstats;
    char *buf;
    ssize_t bytes_read;
    int ret;

    *group_id_p = SSG_GROUP_ID_INVALID;

    fd = open(file_name, O_RDONLY);
    if (fd < 0)
    {
        fprintf(stderr, "Error: Unable to open file %s for loading SSG group ID\n",
            file_name);
        return SSG_FAILURE;
    }

    ret = fstat(fd, &fstats);
    if (ret != 0)
    {
        fprintf(stderr, "Error: Unable to stat file %s\n", file_name);
        close(fd);
        return SSG_FAILURE;
    }
    if (fstats.st_size == 0)
    {
        fprintf(stderr, "Error: SSG group ID file %s is empty\n", file_name);
        close(fd);
        return SSG_FAILURE;
    }

    buf = malloc(fstats.st_size);
    if (buf == NULL)
    {
        close(fd);
        return SSG_FAILURE;
    }

    bytes_read = read(fd, buf, fstats.st_size);
    if (bytes_read != (ssize_t)fstats.st_size)
    {
        fprintf(stderr, "Error: Unable to read SSG group ID from file %s\n", file_name);
        close(fd);
        free(buf);
        return SSG_FAILURE;
    }

    ssg_group_id_deserialize(buf, (size_t)bytes_read, num_addrs, group_id_p);
    if (*group_id_p == SSG_GROUP_ID_INVALID)
    {
        fprintf(stderr, "Error: Unable to deserialize SSG group ID\n");
        close(fd);
        free(buf);
        return SSG_FAILURE;
    }

    close(fd);
    free(buf);
    return SSG_SUCCESS;
}

void ssg_group_dump(
    ssg_group_id_t group_id)
{
    ssg_group_descriptor_t *g_desc;
    ssg_mid_state_t *mid_state;
    ssg_group_view_t *group_view = NULL;
    ABT_rwlock group_lock;
    int group_size;
    char *group_name = NULL;
    char group_role[32];
    char group_self_id[32];

    if (!ssg_rt || group_id == SSG_GROUP_ID_INVALID) return;

    ABT_rwlock_rdlock(ssg_rt->lock);

    /* find the group descriptor */
    HASH_FIND(hh, ssg_rt->g_desc_table, &group_id, sizeof(ssg_group_id_t), g_desc);
    if (!g_desc)
    {
        ABT_rwlock_unlock(ssg_rt->lock);
        fprintf(stderr, "Error: SSG unable to find expected group ID\n");
        return;
    }

    if (g_desc->owner_status == SSG_OWNER_IS_MEMBER)
    {
        ssg_group_t *g = g_desc->g_data.g;

        ABT_rwlock_rdlock(g->lock);
        mid_state = g->mid_state;
        group_view = &g->view;
        group_lock = g->lock;
        group_size = g->view.size;
        group_name = g->name;
        strcpy(group_role, "member");
        sprintf(group_self_id, "%lu", g->mid_state->self_id);
    }
    else if (g_desc->owner_status == SSG_OWNER_IS_OBSERVER)
    {
        ssg_observed_group_t *og = g_desc->g_data.og;

        ABT_rwlock_rdlock(og->lock);
        mid_state = og->mid_state;
        group_view = &og->view;
        group_lock = og->lock;
        group_size = og->view.size;
        group_name = og->name;
        strcpy(group_role, "observer");
    }
    else
    {
        fprintf(stderr, "Error: SSG can only dump membership information for" \
            " groups that the caller is a member of or an observer of\n");
    }

    if (group_view)
    {
        unsigned int i;
        ssg_member_id_t member_id;
        char *member_addr_str;
        ssg_member_state_t *member_state;
        char hostname[1024];
        gethostname(hostname, 1024);

        printf("SSG membership information for group '%s':\n", group_name);
        printf("\trole: %s\n", group_role);
        printf("\thost: %s\n", hostname);
        if (strcmp(group_role, "member") == 0)
            printf("\tself_id: %s\n", group_self_id);
        printf("\tsize: %d\n", group_size);
        printf("\tview:\n");
        for (i = 0; i < group_view->size; i++)
        {
            member_id = *(ssg_member_id_t *)utarray_eltptr(group_view->rank_array, i);
            if((strcmp(group_role, "member") == 0) && 
                (member_id == mid_state->self_id))
                member_addr_str = mid_state->self_addr_str;
            else
            {
                HASH_FIND(hh, group_view->member_map, &member_id,
                    sizeof(ssg_member_id_t), member_state);
                member_addr_str = member_state->addr_str;
            }
            printf("\t\tid: %20lu\taddr: %s\n", member_id,
                member_addr_str);
        }
        ABT_rwlock_unlock(group_lock);
        fflush(stdout);
    }

    ABT_rwlock_unlock(ssg_rt->lock);

    return;
}

/************************************
 *** SSG internal helper routines ***
 ************************************/

ssg_mid_state_t *ssg_acquire_mid_state(
    margo_instance_id mid)
{
    ssg_mid_state_t *mid_state;
    hg_size_t self_addr_str_size;
    hg_return_t hret;

    ABT_rwlock_wrlock(ssg_rt->lock);
    LL_SEARCH_SCALAR(ssg_rt->mid_list, mid_state, mid, mid);
    if(!mid_state)
    {
        mid_state = malloc(sizeof(*mid_state));
        if(!mid_state)
        {
            ABT_rwlock_unlock(ssg_rt->lock);
            return NULL;
        }
        memset(mid_state, 0, sizeof(*mid_state));
        mid_state->mid = mid;

        /* get my self address string and ID (which are constant per-mid) */
        hret = margo_addr_self(mid, &mid_state->self_addr);
        if (hret != HG_SUCCESS)
        {
            ABT_rwlock_unlock(ssg_rt->lock);
            free(mid_state);
            return NULL;
        }

        hret = margo_addr_to_string(mid, NULL, &self_addr_str_size, mid_state->self_addr);
        if (hret != HG_SUCCESS)
        {
            ABT_rwlock_unlock(ssg_rt->lock);
            margo_addr_free(mid, mid_state->self_addr);
            free(mid_state);
            return NULL;
        }

        mid_state->self_addr_str = malloc(self_addr_str_size);
        if (mid_state->self_addr_str == NULL)
        {
            ABT_rwlock_unlock(ssg_rt->lock);
            margo_addr_free(mid, mid_state->self_addr);
            free(mid_state);
            return NULL;
        }

        hret = margo_addr_to_string(mid, mid_state->self_addr_str, &self_addr_str_size,
            mid_state->self_addr);
        if (hret != HG_SUCCESS)
        {
            ABT_rwlock_unlock(ssg_rt->lock);
            free(mid_state->self_addr_str);
            margo_addr_free(mid, mid_state->self_addr);
            free(mid_state);
            return NULL;
        }

        mid_state->self_id = ssg_gen_member_id(mid_state->self_addr_str);

        /* register RPCs */
        ssg_register_rpcs(mid_state);

        /* add to mid list */
        LL_APPEND(ssg_rt->mid_list, mid_state);
    }
    mid_state->ref_count++;

    ABT_rwlock_unlock(ssg_rt->lock);

    return mid_state;
}

static void ssg_release_mid_state(
    ssg_mid_state_t *mid_state)
{
    ABT_rwlock_wrlock(ssg_rt->lock);

    mid_state->ref_count--;
    if (!mid_state->ref_count)
    {
        LL_DELETE(ssg_rt->mid_list, mid_state);
        ssg_deregister_rpcs(mid_state);
        margo_addr_free(mid_state->mid, mid_state->self_addr);
        free(mid_state->self_addr_str);
        free(mid_state);
    }
    ABT_rwlock_unlock(ssg_rt->lock);

    return;
}

static ssg_group_id_t ssg_group_create_internal(
    ssg_mid_state_t *mid_state, const char * group_name,
    const char * const group_addr_strs[], int group_size,
    ssg_group_config_t *group_conf, ssg_membership_update_cb update_cb,
    void *update_cb_dat)
{
    ssg_group_descriptor_t *g_desc = NULL, *g_desc_check;
    ssg_group_id_t g_id;
    ssg_group_t *g;
    ssg_group_config_t tmp_group_conf = SSG_GROUP_CONFIG_INITIALIZER;
    int success = 0;
    int sret;

    if (!group_conf) group_conf = &tmp_group_conf;

    /* allocate an SSG group data structure and initialize some of it */
    g = malloc(sizeof(*g));
    if (!g) goto fini;
    memset(g, 0, sizeof(*g));
    g->mid_state = mid_state;
    g->name = strdup(group_name);
    if (!g->name) goto fini;
    memcpy(&g->config, group_conf, sizeof(*group_conf));
    g->update_cb = update_cb;
    g->update_cb_dat = update_cb_dat;
    ABT_rwlock_create(&g->lock);

#ifdef DEBUG
    /* set debug output pointer */
    char *dbg_log_dir = getenv("SSG_DEBUG_LOGDIR");
    if (dbg_log_dir)
    {
        char dbg_log_path[PATH_MAX];
        snprintf(dbg_log_path, PATH_MAX, "%s/ssg-%s-%lu.log",
            dbg_log_dir, g->name, mid_state->self_id);
        g->dbg_log = fopen(dbg_log_path, "a");
        if (!g->dbg_log) goto fini;
    }
    else
    {
        g->dbg_log = stdout;
    }
#endif

    /* generate unique descriptor for this group */
    g_id = ssg_hash64_str(group_name);
    g_desc = ssg_group_descriptor_create(g_id, 0, NULL, group_conf->ssg_credential,
        SSG_OWNER_IS_MEMBER);
    if (g_desc == NULL) goto fini;
    g_desc->g_data.g = g;

    /* first make sure we aren't re-creating an existing group, then go ahead and
     * stash this group descriptor -- we will use the group lock while we finish
     * group creation
     */
    ABT_rwlock_wrlock(ssg_rt->lock);
    HASH_FIND(hh, ssg_rt->g_desc_table, &g_id, sizeof(ssg_group_id_t), g_desc_check);
    if(g_desc_check)
    {
        ABT_rwlock_unlock(ssg_rt->lock);
        goto fini;
    }
    HASH_ADD(hh, ssg_rt->g_desc_table, g_id, sizeof(ssg_group_id_t), g_desc);
    ABT_rwlock_unlock(ssg_rt->lock);

    /* initialize the group view */
    sret = ssg_group_view_create(group_addr_strs, group_size, mid_state->self_addr_str,
        mid_state, g->lock, &g->view);
    if (sret != SSG_SUCCESS)
    {
        HASH_DEL(ssg_rt->g_desc_table, g_desc);
        goto fini;
    }

    /* initialize swim failure detector if everything succeeds */
    sret = swim_init(g, g_desc->g_id, group_conf, 1);
    if (sret != SSG_SUCCESS)
    {
        HASH_DEL(ssg_rt->g_desc_table, g_desc);
        goto fini;
    }

    SSG_DEBUG(g, "group create successful (size=%d, self=%s)\n",
        group_size, mid_state->self_addr_str);
    success = 1;

fini:
    if (!success)
    {
        g_id = SSG_GROUP_ID_INVALID;

        if (g_desc) ssg_group_descriptor_free(g_desc);
        if (g)
        {
#ifdef DEBUG
            if (g->dbg_log && getenv("SSG_DEBUG_LOGDIR")) fclose(g->dbg_log);
#endif
            ssg_group_view_destroy(&g->view, mid_state->mid);
            ABT_rwlock_free(&g->lock);
            free(g->name);
            free(g);
        }
    }

    return g_id;
}

static int ssg_group_view_create(
    const char * const group_addr_strs[], int group_size,
    const char * self_addr_str, ssg_mid_state_t * mid_state,
    ABT_rwlock view_lock, ssg_group_view_t * view)
{
    int i, j, r;
    ABT_thread *lookup_ults = NULL;
    struct ssg_group_lookup_ult_args *lookup_ult_args = NULL;
    const char *self_addr_substr = NULL;
    const char *addr_substr = NULL;
    int self_found = 0;
    int aret;
    int sret = SSG_FAILURE;

    utarray_new(view->rank_array, &ut_ssg_member_id_t_icd);
    utarray_reserve(view->rank_array, group_size);

    /* allocate lookup ULTs */
    lookup_ults = malloc(group_size * sizeof(*lookup_ults));
    if (lookup_ults == NULL) goto fini;
    for (i = 0; i < group_size; i++) lookup_ults[i] = ABT_THREAD_NULL;
    lookup_ult_args = malloc(group_size * sizeof(*lookup_ult_args));
    if (lookup_ult_args == NULL) goto fini;

    if(self_addr_str)
    {
        /* strstr is used here b/c there may be inconsistencies in whether the class
         * is included in the address or not (it should not be in HG_Addr_to_string,
         * but it's possible that it is in the list of group address strings)
         */
        self_addr_substr = strstr(self_addr_str, "://");
        if (self_addr_substr == NULL)
            self_addr_substr = self_addr_str;
        else
            self_addr_substr += 3;
    }

    /* construct view using ULTs to lookup the address of each group member */
    r = rand() % group_size;
    for (i = 0; i < group_size; i++)
    {
        /* randomize our starting index so all group members aren't looking
         * up other group members in the same order
         */
        j = (r + i) % group_size;

        if (group_addr_strs[j] == NULL || strlen(group_addr_strs[j]) == 0) continue;

        if (self_addr_substr)
        {
            addr_substr = strstr(group_addr_strs[j], "://");
            if (addr_substr == NULL)
                addr_substr = group_addr_strs[j];
            else
                addr_substr += 3;

            if (strcmp(self_addr_substr, addr_substr) == 0)
            {
                /* don't look up our own address, we already know it */
                self_found = 1;
                utarray_push_back(view->rank_array, &mid_state->self_id);
                view->size++;
                continue;
            }
        }

        /* XXX limit outstanding lookups to some max */
        lookup_ult_args[j].mid = mid_state->mid;
        lookup_ult_args[j].addr_str = group_addr_strs[j];
        lookup_ult_args[j].view = view;
        lookup_ult_args[j].lock = view_lock;
        ABT_pool pool;
        margo_get_handler_pool(mid_state->mid, &pool);
        aret = ABT_thread_create(pool, &ssg_group_lookup_ult,
            &lookup_ult_args[j], ABT_THREAD_ATTR_NULL,
            &lookup_ults[j]);
        if (aret != ABT_SUCCESS) goto fini;
    }

    /* wait on all lookup ULTs to terminate */
    for (i = 0; i < group_size; i++)
    {
        if (lookup_ults[i] == ABT_THREAD_NULL) continue;

        aret = ABT_thread_join(lookup_ults[i]);
        ABT_thread_free(&lookup_ults[i]);
        lookup_ults[i] = ABT_THREAD_NULL;
        if (aret != ABT_SUCCESS) goto fini;
        else if (lookup_ult_args[i].out != SSG_SUCCESS)
        {
            fprintf(stderr, "Error: SSG unable to lookup HG address %s\n",
                lookup_ult_args[i].addr_str);
            goto fini;
        }
    }

    /* if we provided a self address string and didn't find ourselves,
     * then we return with an error
     */
    if (self_addr_str && !self_found)
    {
        fprintf(stderr, "Error: SSG unable to resolve self ID in group\n");
        goto fini;
    }

    /* setup rank-based array */
    utarray_sort(view->rank_array, ssg_member_id_sort_cmp);

    /* clean exit */
    sret = SSG_SUCCESS;

fini:
    if (sret != SSG_SUCCESS)
    {
        for (i = 0; i < group_size; i++)
        {
            if (lookup_ults[i] != ABT_THREAD_NULL)
            {
                ABT_thread_cancel(lookup_ults[i]);
                ABT_thread_free(&lookup_ults[i]);
            }
        }
        ssg_group_view_destroy(view, mid_state->mid);
    }
    free(lookup_ults);
    free(lookup_ult_args);

    return sret;
}

static void ssg_group_lookup_ult(
    void * arg)
{
    struct ssg_group_lookup_ult_args *l = arg;
    ssg_member_id_t member_id = ssg_gen_member_id(l->addr_str);
    hg_addr_t member_addr;
    ssg_member_state_t *ms;
    hg_return_t hret;

    hret = margo_addr_lookup(l->mid, l->addr_str, &member_addr);
    if (hret != HG_SUCCESS)
    {
        l->out = SSG_FAILURE;
        return;
    }

    ABT_rwlock_wrlock(l->lock);
    ms = ssg_group_view_add_member(l->addr_str, member_addr, member_id, l->view);
    if (ms)
        l->out = SSG_SUCCESS;
    else
        l->out = SSG_FAILURE;
    ABT_rwlock_unlock(l->lock);

    return;
}

static ssg_member_state_t * ssg_group_view_add_member(
    const char * addr_str, hg_addr_t addr, ssg_member_id_t member_id,
    ssg_group_view_t * view)
{
    ssg_member_state_t *ms;

    ms = calloc(1, sizeof(*ms));
    if (!ms) return NULL;
    ms->addr_str = strdup(addr_str);
    ms->addr = addr;
    if (!ms->addr_str)
    {
        free(ms);
        return NULL;
    }
    ms->id = member_id;

    HASH_ADD(hh, view->member_map, id, sizeof(ssg_member_id_t), ms);
    utarray_push_back(view->rank_array, &member_id);
    view->size++;

    return ms;
}

static ssg_group_descriptor_t * ssg_group_descriptor_create(
    ssg_group_id_t g_id, int num_addrs, char ** addr_strs,
    int64_t cred, int owner_status)
{
    ssg_group_descriptor_t *descriptor;

    descriptor = malloc(sizeof(*descriptor));
    if (!descriptor) return NULL;
    memset(descriptor, 0, sizeof(*descriptor));

    descriptor->g_id = g_id;
    descriptor->num_addr_strs = num_addrs;
    descriptor->addr_strs = addr_strs;
    descriptor->cred = cred;
    descriptor->owner_status = owner_status;
    return descriptor;
}

static void ssg_group_destroy_internal(
    ssg_group_t * g)
{
    ssg_member_state_t *state, *tmp;

    /* free up SWIM state */
    swim_finalize(g);

    /* destroy group state */
    ABT_rwlock_wrlock(g->lock);
    ssg_group_view_destroy(&g->view, g->mid_state->mid);

    HASH_ITER(hh, g->dead_members, state, tmp)
    {
        HASH_DEL(g->dead_members, state);
        free(state->addr_str);
        /* address freed earlier */
        free(state);
    }

    ssg_release_mid_state(g->mid_state);

#ifdef DEBUG
    char *dbg_log_dir = getenv("SSG_DEBUG_LOGDIR");

    fflush(g->dbg_log);
    if (dbg_log_dir)
        fclose(g->dbg_log);
#endif
    ABT_rwlock_unlock(g->lock);

    ABT_rwlock_free(&g->lock);
    free(g->name);
    free(g);

    return;
}

static void ssg_observed_group_destroy(
    ssg_observed_group_t * og)
{
    ABT_rwlock_wrlock(og->lock);
    ssg_group_view_destroy(&og->view, og->mid_state->mid);
    ABT_rwlock_unlock(og->lock);

    ssg_release_mid_state(og->mid_state);
    ABT_rwlock_free(&og->lock);
    free(og->name);
    free(og);
    return;
}

static void ssg_group_view_destroy(
    ssg_group_view_t * view, margo_instance_id mid)
{
    ssg_member_state_t *state, *tmp;

    /* destroy state for all group members */
    HASH_ITER(hh, view->member_map, state, tmp)
    {
        HASH_DEL(view->member_map, state);
        free(state->addr_str);
        margo_addr_free(mid, state->addr);
        free(state);
    }
    utarray_free(view->rank_array);

    return;
}

static void ssg_group_descriptor_free(
    ssg_group_descriptor_t * descriptor)
{
    unsigned int i;

    if (descriptor)
    {
        if (descriptor->addr_strs)
        {
            for (i = 0; i < descriptor->num_addr_strs; i++)
                free(descriptor->addr_strs[i]);
            free(descriptor->addr_strs);
        }
        free(descriptor);
    }
    return;
}

static ssg_member_id_t ssg_gen_member_id(
    const char * addr_str)
{
    char tmp[64] = {0};
    ssg_member_id_t id = (ssg_member_id_t)ssg_hash64_str(addr_str);
    while (id == SSG_MEMBER_ID_INVALID)
    {
        if (tmp[0] == 0) strncpy(tmp, addr_str, 63);
        tmp[0]++;
        id = (ssg_member_id_t)ssg_hash64_str(tmp);
    }
    return id;
}

static char ** ssg_addr_str_buf_to_list(
    const char * buf, int num_addrs)
{
    int i;
    char **ret = malloc(num_addrs * sizeof(*ret));
    if (ret == NULL) return NULL;

    ret[0] = (char *)buf;
    for (i = 1; i < num_addrs; i++)
    {
        char * a = ret[i-1];
        ret[i] = a + strlen(a) + 1;
    }
    return ret;
}

static int ssg_member_id_sort_cmp(
    const void *a, const void *b)
{
    ssg_member_id_t member_a = *(ssg_member_id_t *)a;
    ssg_member_id_t member_b = *(ssg_member_id_t *)b;
    if(member_a < member_b)
        return -1;
    else
        return 1;
    return 0;
}

static int ssg_get_group_member_rank_internal(
    ssg_group_view_t *view, ssg_member_id_t member_id)
{
    unsigned int i;
    ssg_member_id_t iter_member_id;

    if (member_id == SSG_MEMBER_ID_INVALID) return -1;

    /* XXX need a better way to find rank than just iterating the array */
    for (i = 0; i < view->size; i++)
    {
        iter_member_id = *(ssg_member_id_t *)utarray_eltptr(view->rank_array, i);
        if (iter_member_id == member_id)
            return (int)i;
    }

    return -1;
}

#ifdef SSG_HAVE_PMIX
void ssg_pmix_proc_failure_notify_fn(
    size_t evhdlr_registration_id, pmix_status_t status, const pmix_proc_t *source,
    pmix_info_t info[], size_t ninfo, pmix_info_t results[], size_t nresults,
    pmix_event_notification_cbfunc_fn_t cbfunc, void *cbdata)
{
    char key[512];
    bool flag;
    pmix_info_t *get_info;
    pmix_value_t *val_p;
    pmix_data_array_t *id_array_ptr;
    ssg_member_id_t *ids;
    size_t i;
    pmix_status_t ret;
    ssg_group_descriptor_t *g_desc, *g_desc_tmp;
    ssg_member_update_t fail_update;

    assert(status == PMIX_PROC_TERMINATED || status == PMIX_ERR_PROC_ABORTED);

    snprintf(key, 512, "ssg-%s-%d-id", source->nspace, source->rank);
    PMIX_INFO_CREATE(get_info, 1);
    flag = true;
    PMIX_INFO_LOAD(get_info, PMIX_IMMEDIATE, &flag, PMIX_BOOL);
    ret = PMIx_Get(source, key, get_info, 1, &val_p);
    PMIX_INFO_FREE(get_info, 1);
    if (ret != PMIX_SUCCESS)
    {
        fprintf(stderr, "Warning: unable to retrieve PMIx rank mapping for rank %d\n",
            source->rank);
    }
    else
    {
        id_array_ptr = val_p->data.darray;
        if (id_array_ptr && (id_array_ptr->type == PMIX_UINT64))
        {
            ids = (ssg_member_id_t *)id_array_ptr->array;
            fail_update.type = SSG_MEMBER_DIED;

            /* iterate all SSG member IDs associated with the failed PMIx rank */
            ABT_rwlock_rdlock(ssg_rt->lock);
            for (i = 0; i < id_array_ptr->size; i++)
            {
                /* remove this member from any group its a member of */
                fail_update.u.member_id = ids[i];

                HASH_ITER(hh, ssg_rt->g_desc_table, g_desc, g_desc_tmp)
                {
                    SSG_DEBUG(g_desc->g_data.g, "RECEIVED FAIL UPDATE FOR MEMBER %lu\n",
                        fail_update.u.member_id);
                    ssg_apply_member_updates(g_desc->g_data.g, &fail_update, 1);
                }
            }
            ABT_rwlock_unlock(ssg_rt->lock);
        }
        else
        {
            fprintf(stderr, "Warning: unexpected format for PMIx rank->ID mapping\n");
        }
        PMIX_VALUE_RELEASE(val_p);
    }

    /* execute PMIx event notification callback */
    if (cbfunc != NULL)
        cbfunc(ret, NULL, 0, NULL, NULL, cbdata);

    return;
}

void ssg_pmix_proc_failure_reg_cb(
    pmix_status_t status, size_t evhdlr_ref, void *cbdata)
{
    size_t *proc_failure_evhdlr_ref = (size_t *)cbdata;

    if (status != PMIX_SUCCESS)
    {
        fprintf(stderr, "Error: PMIx event notification registration failed! [%d]\n", status);
        return;
    }

    /* store evhdlr_ref for eventual deregister */
    *proc_failure_evhdlr_ref = evhdlr_ref;

    return;
}
#endif

/**************************************
 *** SWIM group management routines ***
 **************************************/

void ssg_apply_member_updates(
    ssg_group_t  * g,
    ssg_member_update_t * updates,
    hg_size_t update_count)
{
    hg_size_t i;
    ssg_member_state_t *update_ms;
    int update_rank;
    hg_return_t hret;
    int ret;

    assert(g != NULL);

    for (i = 0; i < update_count; i++)
    {
        if (updates[i].type == SSG_MEMBER_JOINED)
        {
            ssg_member_id_t join_id = ssg_gen_member_id(updates[i].u.member_addr_str);

            if (join_id == g->mid_state->self_id)
            {
                /* ignore joins for self */
                continue;
            }

            ABT_rwlock_wrlock(g->lock);
            HASH_FIND(hh, g->view.member_map, &join_id, sizeof(join_id), update_ms);
            if (update_ms)
            {
                /* ignore join messages for members already in view */
                ABT_rwlock_unlock(g->lock);
                continue;
            }

            HASH_FIND(hh, g->dead_members, &join_id, sizeof(join_id), update_ms);
            if (update_ms)
            {
                /* ignore join messages for dead members */
                ABT_rwlock_unlock(g->lock);
                continue;
            }

            /* add member to the view */
            /* NOTE: we temporarily add the member to the view with a NULL addr
             * to hold its place in the view and prevent competing joins
             */
            update_ms = ssg_group_view_add_member(updates[i].u.member_addr_str,
                HG_ADDR_NULL, join_id, &g->view);
            if (update_ms == NULL)
            {
                SSG_DEBUG(g, "Warning: SSG unable to add joining group member %s\n",
                    updates[i].u.member_addr_str);
                ABT_rwlock_unlock(g->lock);
                continue;
            }

            /* setup rank-based array */
            utarray_sort(g->view.rank_array, ssg_member_id_sort_cmp);

            ABT_rwlock_unlock(g->lock);

            /* lookup address of joining member */
            hret = margo_addr_lookup(g->mid_state->mid, updates[i].u.member_addr_str,
                &update_ms->addr);
            if (hret != HG_SUCCESS)
            {
                SSG_DEBUG(g, "Warning: SSG unable to lookup joining group member %s addr\n",
                    updates[i].u.member_addr_str);
                ABT_rwlock_wrlock(g->lock);
                HASH_DEL(g->view.member_map, update_ms);
                g->view.size--;
                free(update_ms->addr_str);
                free(update_ms);
                ABT_rwlock_unlock(g->lock);
                continue;
            }

            /* have SWIM apply the join update */
            ret = swim_apply_ssg_member_update(g, update_ms, updates[i]);
            if (ret != SSG_SUCCESS)
            {
                SSG_DEBUG(g, "Warning: SWIM unable to apply SSG update for joining"\
                    "group member %s\n", updates[i].u.member_addr_str);
                ABT_rwlock_wrlock(g->lock);
                HASH_DEL(g->view.member_map, update_ms);
                g->view.size--;
                free(update_ms->addr_str);
                free(update_ms);
                ABT_rwlock_unlock(g->lock);
                continue;
            }

            SSG_DEBUG(g, "successfully added member %lu\n", join_id);

            /* invoke user callback to apply the SSG update */
            if (g->update_cb)
                g->update_cb(g->update_cb_dat, join_id, updates[i].type);
        }
        else if (updates[i].type == SSG_MEMBER_LEFT)
        {
            ABT_rwlock_wrlock(g->lock);
            HASH_FIND(hh, g->view.member_map, &updates[i].u.member_id,
                sizeof(updates[i].u.member_id), update_ms);
            if (!update_ms)
            {
                /* ignore leave messages for members not in view */
                ABT_rwlock_unlock(g->lock);
                continue;
            }

            /* remove from view and add to dead list */
            HASH_DEL(g->view.member_map, update_ms);
            update_rank = ssg_get_group_member_rank_internal(&g->view, update_ms->id);
            utarray_erase(g->view.rank_array, (unsigned int)update_rank, 1);
            g->view.size--;
            HASH_ADD(hh, g->dead_members, id, sizeof(update_ms->id), update_ms);
            margo_addr_free(g->mid_state->mid, update_ms->addr);
            update_ms->addr= HG_ADDR_NULL;
            ABT_rwlock_unlock(g->lock);

            /* have SWIM apply the leave update */
            ret = swim_apply_ssg_member_update(g, update_ms, updates[i]);
            if (ret != SSG_SUCCESS)
            {
                SSG_DEBUG(g, "Warning: SWIM unable to apply SSG update for leaving"\
                    "group member %lu\n", updates[i].u.member_id);
                continue;
            }

            SSG_DEBUG(g, "successfully removed leaving member %lu\n", updates[i].u.member_id);

            /* invoke user callback to apply the SSG update */
            if (g->update_cb)
                g->update_cb(g->update_cb_dat, updates[i].u.member_id, updates[i].type);
        }
        else if (updates[i].type == SSG_MEMBER_DIED)
        {
            if (updates[i].u.member_id == g->mid_state->self_id)
            {
                ssg_group_descriptor_t *g_desc, *g_desc_tmp;

                /* if dead member is self, just destroy group locally ... */
    
                /* first, notify user they have been marked dead in this group */
                if (g->update_cb)
                    g->update_cb(g->update_cb_dat, updates[i].u.member_id,
                        updates[i].type);

                /* XXX we need to find the corresponding group descriptor and
                 * remove it. there isn't a convenient way to map a group pointer
                 * to a descriptor so we just iterate the descriptor table until
                 * we find one that matches our group pointer...
                 */
                /* XXX we know that the global ssg_rt lock is held in rd mode in
                 * this code path, so we release and reacquire in wr mode to remove
                 * the descriptor
                 */
                ABT_rwlock_unlock(ssg_rt->lock);
                ABT_rwlock_wrlock(ssg_rt->lock);
                HASH_ITER(hh, ssg_rt->g_desc_table, g_desc, g_desc_tmp)
                {
                    if (g_desc->g_data.g == g)
                    {
                        HASH_DEL(ssg_rt->g_desc_table, g_desc);
                        ABT_rwlock_unlock(ssg_rt->lock);
                        ssg_group_destroy_internal(g);

                        return; /* no need to keep processing updates */
                    }
                }
                ABT_rwlock_unlock(ssg_rt->lock);

                return; /* no need to keep processing updates */
            }
            else
            {
                ABT_rwlock_wrlock(g->lock);
                HASH_FIND(hh, g->view.member_map, &updates[i].u.member_id,
                    sizeof(updates[i].u.member_id), update_ms);
                if (!update_ms)
                {
                    /* ignore fail messages for members not in view */
                    ABT_rwlock_unlock(g->lock);
                    continue;
                }

                /* remove from view and add to dead list */
                HASH_DEL(g->view.member_map, update_ms);
                update_rank = ssg_get_group_member_rank_internal(&g->view, update_ms->id);
                utarray_erase(g->view.rank_array, (unsigned int)update_rank, 1);
                g->view.size--;
                HASH_ADD(hh, g->dead_members, id, sizeof(update_ms->id), update_ms);
                margo_addr_free(g->mid_state->mid, update_ms->addr);
                update_ms->addr= HG_ADDR_NULL;
                ABT_rwlock_unlock(g->lock);

                /* have SWIM apply the dead update */
                ret = swim_apply_ssg_member_update(g, update_ms, updates[i]);
                if (ret != SSG_SUCCESS)
                {
                    SSG_DEBUG(g, "Warning: SWIM unable to apply SSG update for dead"\
                        "group member %lu\n", updates[i].u.member_id);
                    continue;
                }

                SSG_DEBUG(g, "successfully removed dead member %lu\n", updates[i].u.member_id);
            }

            /* invoke user callback to apply the SSG update */
            if (g->update_cb)
                g->update_cb(g->update_cb_dat, updates[i].u.member_id, updates[i].type);
        }
        else
        {
            SSG_DEBUG(g, "Warning: invalid SSG update received, ignoring.\n");
        }
    }

    return;
}
