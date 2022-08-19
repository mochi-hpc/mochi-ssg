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
#include <margo-logging.h>

#include "ssg.h"
#ifdef SSG_HAVE_MPI
#include "ssg-mpi.h"
#endif
#ifdef SSG_HAVE_PMIX
#include "ssg-pmix.h"
#endif
#include "ssg-internal.h"
#include "swim-fd/swim-fd.h"

#ifndef PATH_MAX
#define PATH_MAX 4096
#endif

#define SSG_MAX_REQUEST_RETRY_COUNT 10

/* SSG helper routine prototypes */
static int ssg_acquire_mid_state(
    margo_instance_id mid, ssg_mid_state_t **msp);
static void ssg_release_mid_state(
    ssg_mid_state_t *mid_state);
static int ssg_group_create_internal(
    ssg_mid_state_t *mid_state, const char * group_name,
    const char * const group_addr_strs[], int group_size,
    ssg_group_config_t *group_conf, ssg_membership_update_cb update_cb,
    void *update_cb_dat, ssg_group_id_t *group_id);
static int ssg_group_join_internal(
    ssg_group_id_t group_id, ssg_mid_state_t *mid_state,
    void *view_buf, int group_size, ssg_group_config_t *group_conf,
    ssg_membership_update_cb update_cb, void *update_cb_dat);
static int ssg_group_leave_internal(
    ssg_group_id_t group_id);
static int ssg_group_refresh_internal(
    ssg_group_id_t group_id, ssg_mid_state_t *mid_state,
    void *view_buf, int group_size);
static int ssg_group_view_create(
    const char * const group_addr_strs[], int group_size,
    const char * self_addr_str, ssg_mid_state_t *mid_state,
    ssg_group_view_t * view);
static ssg_member_state_t * ssg_group_view_add_member(
    const char * addr_str, hg_addr_t addr, ssg_member_id_t member_id,
    ssg_group_view_t * view);
static ssg_group_descriptor_t * ssg_group_descriptor_create(
    ssg_group_id_t g_id, const char * name, ssg_mid_state_t *mid_state,
    ssg_group_state_t * group, int64_t cred);
static void ssg_group_destroy_internal(
    ssg_group_state_t * g, ssg_mid_state_t *mid_state);
static void ssg_group_view_destroy(
    ssg_group_view_t * view, ssg_mid_state_t *mid_state);
static void ssg_group_descriptor_free(
    ssg_group_descriptor_t * descriptor);
static ssg_member_id_t ssg_gen_member_id(
    const char * addr_str);
static char ** ssg_addr_str_buf_to_list(
    const char * buf, int num_addrs);
static int ssg_member_id_sort_cmp( 
    const void *a, const void *b);
static int ssg_get_group_member_rank_internal(
    ssg_group_view_t *view, ssg_member_id_t member_id, int *rank);
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
    ssg_member_state_t *ms;
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
        return SSG_ERR_ALREADY_INITIALIZED;

    /* initialize SSG runtime state */
    ssg_rt = malloc(sizeof(*ssg_rt));
    if (!ssg_rt)
        return SSG_ERR_ALLOCATION;
    memset(ssg_rt, 0, sizeof(*ssg_rt));

    ret = ABT_initialized();
    if (ret == ABT_ERR_UNINITIALIZED)
    {
        free(ssg_rt);
        ssg_rt = NULL;
        return SSG_MAKE_ABT_ERROR(ret);
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
    ssg_group_descriptor_t *gd, *gd_tmp;
    ssg_mid_state_t *mid_state, *mid_state_tmp;

    if (!ssg_rt)
        return SSG_ERR_NOT_INITIALIZED;

    ABT_rwlock_wrlock(ssg_rt->lock);

#ifdef SSG_HAVE_PMIX
    if (ssg_rt->pmix_failure_evhdlr_ref)
        PMIx_Deregister_event_handler(ssg_rt->pmix_failure_evhdlr_ref, NULL, NULL);
#endif

    /* destroy all active groups */
    HASH_ITER(hh, ssg_rt->gd_table, gd, gd_tmp)
    {
        HASH_DEL(ssg_rt->gd_table, gd);
        /* wait on any outstanding group references in ULT handlers */
        SSG_GROUP_REFS_WAIT(gd);
        if (gd->is_member)
        {
            /* members must destroy the internal group structures, which includes view */
            ssg_group_destroy_internal(gd->group, gd->mid_state);
        }
        else
        {
            /* non-members don't have internal group structure, but must destroy view */
            ssg_group_view_destroy(gd->view, gd->mid_state);
        }
        /* NOTE: we intentionally break the linkage between the group
         * descriptor and the mid_state here (if present).  The mid_states
         * will all be forcibly destroyed later on in this function anyway,
         * and we don't want ssg_release_mid_state() to attempt to acquire
         * the ssg_rt->lock() that is already held in this code path.
         */
        gd->mid_state = NULL;
        ssg_group_descriptor_free(gd);
    }

    /* free any mid state */
    /* XXX should this be part of group destroy?
     * NOTE: if this loop were to be moved, then we have to revisit the
     * gd->mid_state = NULL statement above. See previous NOTE comment.
     */
    LL_FOREACH_SAFE(ssg_rt->mid_list, mid_state, mid_state_tmp)
    {
        LL_DELETE(ssg_rt->mid_list, mid_state);
        ssg_deregister_rpcs(mid_state);
        margo_addr_free(mid_state->mid, mid_state->self_addr);
        free(mid_state->self_addr_str);
        free(mid_state);
    }

    ABT_rwlock_free(&ssg_rt->lock);
    free(ssg_rt);
    ssg_rt = NULL;

    return SSG_SUCCESS;
}

/*************************************
 *** SSG group management routines ***
 *************************************/

int ssg_group_create(
    margo_instance_id mid,
    const char * group_name,
    const char * const group_addr_strs[],
    int group_size,
    ssg_group_config_t *group_conf,
    ssg_membership_update_cb update_cb,
    void * update_cb_dat,
    ssg_group_id_t *g_id)
{
    ssg_mid_state_t *mid_state;
    int ret;

    if (!ssg_rt)
        return SSG_ERR_NOT_INITIALIZED;

    ret = ssg_acquire_mid_state(mid, &mid_state);
    if (ret != SSG_SUCCESS)
    {
        margo_error(mid, "[ssg] unable to acquire Margo instance information");
        return ret;
    }

    ret = ssg_group_create_internal(mid_state, group_name, group_addr_strs,
            group_size, group_conf, update_cb, update_cb_dat, g_id);
    if (ret != SSG_SUCCESS)
        ssg_release_mid_state(mid_state);

    return ret;
}

int ssg_group_create_config(
    margo_instance_id mid,
    const char * group_name,
    const char * file_name,
    ssg_group_config_t *group_conf,
    ssg_membership_update_cb update_cb,
    void * update_cb_dat,
    ssg_group_id_t *g_id)
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

    if (!ssg_rt)
        return SSG_ERR_NOT_INITIALIZED;

    ret = ssg_acquire_mid_state(mid, &mid_state);
    if (ret != SSG_SUCCESS)
    {
        margo_error(mid, "[ssg] unable to acquire Margo instance information");
        return ret;
    }

    /* open config file for reading */
    fd = open(file_name, O_RDONLY);
    if (fd == -1)
    {
        margo_error(mid, "[ssg] unable to open config file %s for group %s",
            file_name, group_name);
        ret = SSG_ERR_FILE_IO;
        goto fini;
    }

    /* get file size and allocate a buffer to store it */
    ret = fstat(fd, &st);
    if (ret == -1)
    {
        margo_error(mid, "[ssg] unable to stat config file %s for group %s",
            file_name, group_name);
        ret = SSG_ERR_FILE_IO;
        goto fini;
    }
    rd_buf = malloc(st.st_size+1);
    if (rd_buf == NULL)
    {
        ret = SSG_ERR_ALLOCATION;
        goto fini;
    }

    /* load it all in one fell swoop */
    rd_buf_size = read(fd, rd_buf, st.st_size);
    if (rd_buf_size != st.st_size)
    {
        margo_error(mid, "[ssg] unable to read config file %s for group %s",
            file_name, group_name);
        ret = SSG_ERR_FILE_IO;
        goto fini;
    }
    rd_buf[rd_buf_size]='\0';

    /* strtok the result - each space-delimited address is assumed to be
     * a unique mercury address
     */
    tok = strtok(rd_buf, "\r\n\t ");
    if (tok == NULL)
    {
        margo_error(mid, "[ssg] unable to read addresses from config file %s for group %s",
            file_name, group_name);
        ret = SSG_ERR_FILE_FORMAT;
        goto fini;
    }

    /* build up the address buffer */
    addr_str_buf = malloc(rd_buf_size);
    if (addr_str_buf == NULL)
    {
        ret = SSG_ERR_ALLOCATION;
        goto fini;
    }
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
        if (tmp == NULL)
        {
            ret = SSG_ERR_ALLOCATION;
            goto fini;
        }
        addr_str_buf = tmp;
    }

    /* set up address string array for group members */
    addr_strs = (const char **)ssg_addr_str_buf_to_list(addr_str_buf, num_addrs);
    if (!addr_strs)
    {
        ret = SSG_ERR_ALLOCATION;
        goto fini;
    }

    /* invoke the generic group create routine using our list of addrs */
    ret = ssg_group_create_internal(mid_state, group_name, addr_strs, num_addrs,
        group_conf, update_cb, update_cb_dat, g_id);

fini:
    /* cleanup before returning */
    if (fd != -1) close(fd);
    free(rd_buf);
    free(addr_str_buf);
    free(addr_strs);
    if (ret != SSG_SUCCESS)
        ssg_release_mid_state(mid_state);

    return ret;
}

#ifdef SSG_HAVE_MPI
int ssg_group_create_mpi(
    margo_instance_id mid,
    const char * group_name,
    MPI_Comm comm,
    ssg_group_config_t *group_conf,
    ssg_membership_update_cb update_cb,
    void * update_cb_dat,
    ssg_group_id_t *g_id)
{
    ssg_mid_state_t *mid_state=NULL;
    int i;
    int self_addr_str_size = 0;
    char *addr_str_buf = NULL;
    int *sizes = NULL;
    int *sizes_psum = NULL;
    int comm_size = 0, comm_rank = 0;
    const char **addr_strs = NULL;
    int ret;

    if (!ssg_rt)
        return SSG_ERR_NOT_INITIALIZED;

    ret = ssg_acquire_mid_state(mid, &mid_state);
    if (ret != SSG_SUCCESS)
    {
        margo_error(mid, "[ssg] unable to acquire Margo instance information");
        return ret;
    }

    /* gather the buffer sizes */
    MPI_Comm_size(comm, &comm_size);
    MPI_Comm_rank(comm, &comm_rank);
    sizes = malloc(comm_size * sizeof(*sizes));
    if (sizes == NULL)
    {
        ret = SSG_ERR_ALLOCATION;
        goto fini;
    }
    self_addr_str_size = (int)strlen(mid_state->self_addr_str) + 1;
    sizes[comm_rank] = self_addr_str_size;
    MPI_Allgather(MPI_IN_PLACE, 0, MPI_BYTE, sizes, 1, MPI_INT, comm);

    /* compute a exclusive prefix sum of the data sizes, including the
     * total at the end
     */
    sizes_psum = malloc((comm_size+1) * sizeof(*sizes_psum));
    if (sizes_psum == NULL)
    {
        ret = SSG_ERR_ALLOCATION;
        goto fini;
    }
    sizes_psum[0] = 0;
    for (i = 1; i < comm_size+1; i++)
        sizes_psum[i] = sizes_psum[i-1] + sizes[i-1];

    /* allgather the addresses */
    addr_str_buf = malloc(sizes_psum[comm_size]);
    if (addr_str_buf == NULL)
    {
        ret = SSG_ERR_ALLOCATION;
        goto fini;
    }
    MPI_Allgatherv(mid_state->self_addr_str, self_addr_str_size, MPI_BYTE,
            addr_str_buf, sizes, sizes_psum, MPI_BYTE, comm);

    /* set up address string array for group members */
    addr_strs = (const char **)ssg_addr_str_buf_to_list(addr_str_buf, comm_size);
    if (!addr_strs)
    {
        ret = SSG_ERR_ALLOCATION;
        goto fini;
    }

    /* invoke the generic group create routine using our list of addrs */
    ret = ssg_group_create_internal(mid_state, group_name, addr_strs, comm_size,
        group_conf, update_cb, update_cb_dat, g_id);

fini:
    /* cleanup before returning */
    free(sizes);
    free(sizes_psum);
    free(addr_str_buf);
    free(addr_strs);
    if (ret != SSG_SUCCESS)
        ssg_release_mid_state(mid_state);

    return ret;
}
#endif

#ifdef SSG_HAVE_PMIX
int ssg_group_create_pmix(
    margo_instance_id mid,
    const char * group_name,
    const pmix_proc_t proc,
    ssg_group_config_t *group_conf,
    ssg_membership_update_cb update_cb,
    void * update_cb_dat,
    ssg_group_id_t *g_id)
{
    ssg_mid_state_t *mid_state=NULL;
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
    int ret;

    if (!ssg_rt)
        return SSG_ERR_NOT_INITIALIZED;

    if (!PMIx_Initialized())
    {
        margo_error(mid, "[ssg] unable to use PMIx (uninitialized)");
        return SSG_ERR_PMIX_FAILURE;
    }

    ret = ssg_acquire_mid_state(mid, &mid_state);
    if (ret != SSG_SUCCESS)
    {
        margo_error(mid, "[ssg] unable to acquire Margo instance information");
        return ret;
    }

    /* we need to store a mapping of PMIx ranks to SSG member IDs so that
     * if we later receive notice of a PMIx rank failure we know how to
     * map to affected SSG group members
     */
    snprintf(key, 512, "ssg-%s-%d-id", proc.nspace, proc.rank);
    PMIX_INFO_CREATE(info, 1);
    flag = true;
    int wait_secs=1;
    PMIX_INFO_LOAD(info, PMIX_TIMEOUT, &wait_secs, PMIX_INT);
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
            margo_warning(mid, "Unable to store PMIx rank->ID mapping for "
                "SSG member %lu", mid_state->self_id);
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
                if (!ids)
                {
                    ret = SSG_ERR_ALLOCATION;
                    goto fini;
                }
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
                    margo_warning(mid, "Unable to store PMIx rank->ID mapping for "
                        "SSG member %lu", mid_state->self_id);
            }
        }
        else
        {
            margo_warning(mid, "Unexpected format for PMIx rank->ID mapping");
        }
        PMIX_VALUE_RELEASE(val_p);
    }

    /* XXX note we are assuming every process in the job wants to join this group... */
    /* get the total nprocs in the job */
    PMIX_PROC_LOAD(&tmp_proc, proc.nspace, PMIX_RANK_WILDCARD);
    ret = PMIx_Get(&tmp_proc, PMIX_JOB_SIZE, NULL, 0, &val_p);
    if (ret != PMIX_SUCCESS)
    {
        margo_error(mid, "[ssg] unable to determine PMIx job size");
        ret = SSG_ERR_PMIX_FAILURE;
        goto fini;
    }
    nprocs = (int)val_p->data.uint32;
    PMIX_VALUE_RELEASE(val_p);

    /* put my address string using a well-known key */
    snprintf(key, 512, "ssg-%s-%s-%d-hg-addr", group_name, proc.nspace, proc.rank);
    PMIX_VALUE_LOAD(&value, mid_state->self_addr_str, PMIX_STRING);
    ret = PMIx_Put(PMIX_GLOBAL, key, &value);
    if (ret != PMIX_SUCCESS)
    {
        margo_error(mid, "[ssg] unable to put address string in PMIx kv");
        ret = SSG_ERR_PMIX_FAILURE;
        goto fini;
    }

    /* commit the put data to the local pmix server */
    ret = PMIx_Commit();
    if (ret != PMIX_SUCCESS)
    {
        margo_error(mid, "[ssg] unable to commit address string to PMIx kv");
        ret = SSG_ERR_PMIX_FAILURE;
        goto fini;
    }

    /* barrier, additionally requesting to collect relevant process data */
    PMIX_INFO_CREATE(info, 1);
    flag = true;
    PMIX_INFO_LOAD(info, PMIX_COLLECT_DATA, &flag, PMIX_BOOL);
    ret = PMIx_Fence(&proc, 1, info, 1);
    PMIX_INFO_FREE(info, 1);
    if (ret != PMIX_SUCCESS)
    {
        margo_error(mid, "[ssg] unable to collect PMIx kv data");
        ret = SSG_ERR_PMIX_FAILURE;
        goto fini;
    }

    addr_strs = malloc(nprocs * sizeof(*addr_strs));
    if (addr_strs == NULL)
    {
        ret = SSG_ERR_ALLOCATION;
        goto fini;
    }

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
        snprintf(key, 512, "ssg-%s-%s-%d-hg-addr", group_name, proc.nspace, n);

        tmp_proc.rank = n;
        val_p = &addr_vals[n];
        ret = PMIx_Get(&tmp_proc, key, NULL, 0, &val_p);
        if (ret != PMIX_SUCCESS)
        {
            margo_error(mid, "[ssg] unable to get PMIx rank %d address", n);
            ret = SSG_ERR_PMIX_FAILURE;
            goto fini;
        }
        addr_strs[n] = val_p->data.string;
    }

    /* invoke the generic group create routine using our list of addrs */
    ret = ssg_group_create_internal(mid_state, group_name, addr_strs, nprocs,
        group_conf, update_cb, update_cb_dat, g_id);

fini:
    /* cleanup before returning */
    free(addr_strs);
    PMIX_VALUE_FREE(addr_vals, nprocs);
    if (ret != SSG_SUCCESS)
        ssg_release_mid_state(mid_state);

    return ret;
}
#endif

int ssg_group_destroy(
    ssg_group_id_t group_id)
{
    ssg_group_descriptor_t *gd;

    if (!ssg_rt)
        return SSG_ERR_NOT_INITIALIZED;

    if (group_id == SSG_GROUP_ID_INVALID)
        return SSG_ERR_INVALID_ARG;

    ABT_rwlock_wrlock(ssg_rt->lock);

    /* find the group structure and destroy it */
    HASH_FIND(hh, ssg_rt->gd_table, &group_id, sizeof(ssg_group_id_t), gd);
    if (!gd)
    {
        ABT_rwlock_unlock(ssg_rt->lock);
        margo_error(MARGO_INSTANCE_NULL, "[ssg] unable to find group ID");
        return SSG_ERR_GROUP_NOT_FOUND;
    }
    HASH_DEL(ssg_rt->gd_table, gd);

    ABT_rwlock_unlock(ssg_rt->lock);

    /* make sure there is noone accessing group */
    ABT_rwlock_wrlock(gd->lock);
    ABT_rwlock_unlock(gd->lock);

    if (gd->mid_state)
    {
        /* SSG debugging only available for mids that it is aware of */
        SSG_DEBUG(gd->mid_state, "destroying group %s (g_id=%lu, size=%d)\n",
            gd->name, gd->g_id, gd->view->size);
    }

    /* wait on any outstanding group references in ULT handlers */
    SSG_GROUP_REFS_WAIT(gd);

    if (gd->is_member)
    {
        /* members must destroy the internal group structures, which includes view */
        ssg_group_destroy_internal(gd->group, gd->mid_state);
    }
    else
    {
        /* non-members don't have internal group structure, but must destroy view */
        ssg_group_view_destroy(gd->view, gd->mid_state);
    }
    ssg_group_descriptor_free(gd);

    return SSG_SUCCESS;
}

int ssg_group_add_membership_update_callback(
        ssg_group_id_t group_id,
        ssg_membership_update_cb update_cb,
        void* update_cb_dat)
{
    ssg_group_descriptor_t *gd;

    if (!ssg_rt)
        return SSG_ERR_NOT_INITIALIZED;

    if (group_id == SSG_GROUP_ID_INVALID)
        return SSG_ERR_INVALID_ARG;

    /* find the group structure to modify */
    SSG_GROUP_WRITE(group_id, gd);
    if (!gd)
    {
        return SSG_ERR_GROUP_NOT_FOUND;
    }
    if (!(gd->is_member))
    {
        SSG_GROUP_RELEASE(gd);
        return SSG_ERR_INVALID_OPERATION;
    }

    /* add the membership callback */
    int ret = add_membership_update_cb(
            gd->group,
            update_cb,
            update_cb_dat);

    SSG_GROUP_RELEASE(gd);

    return ret;
}

int ssg_group_remove_membership_update_callback(
        ssg_group_id_t group_id,
        ssg_membership_update_cb update_cb,
        void* update_cb_dat)
{
    ssg_group_descriptor_t *gd;

    if (!ssg_rt)
        return SSG_ERR_NOT_INITIALIZED;

    if (group_id == SSG_GROUP_ID_INVALID)
        return SSG_ERR_INVALID_ARG;

    /* find the group structure to modify */
    SSG_GROUP_WRITE(group_id, gd);
    if (!gd)
    {
        return SSG_ERR_GROUP_NOT_FOUND;
    }
    if (!(gd->is_member))
    {
        SSG_GROUP_RELEASE(gd);
        return SSG_ERR_INVALID_OPERATION;
    }

    /* remove the membership callback */
    int ret = remove_membership_update_cb(
            gd->group,
            update_cb,
            update_cb_dat);

    SSG_GROUP_RELEASE(gd);

    return ret;
}

int ssg_group_join(
    margo_instance_id mid,
    ssg_group_id_t group_id,
    ssg_membership_update_cb update_cb,
    void * update_cb_dat)
{
    ssg_mid_state_t *mid_state = NULL;
    ssg_group_descriptor_t *gd;
    int rank;
    ssg_member_id_t member;
    ssg_member_state_t *ms;
    hg_addr_t target_addr;
    char *target_addr_str;
    struct timespec ts;
    int group_size;
    ssg_group_config_t group_config;
    void *view_buf = NULL;
    int retries = SSG_MAX_REQUEST_RETRY_COUNT;
    hg_return_t hret;
    int ret;

    if (!ssg_rt)
        return SSG_ERR_NOT_INITIALIZED;

    if (mid == MARGO_INSTANCE_NULL || group_id == SSG_GROUP_ID_INVALID)
        return SSG_ERR_INVALID_ARG;

    ret = ssg_acquire_mid_state(mid, &mid_state);
    if (ret != SSG_SUCCESS)
    {
        margo_error(mid, "[ssg] unable to acquire Margo instance information");
        return ret;
    }

    clock_gettime(CLOCK_MONOTONIC, &ts);

    /* get read access to the group structure to refresh */
    SSG_GROUP_READ(group_id, gd);
    if (!gd)
    {
        ssg_release_mid_state(mid_state);
        margo_error(mid, "[ssg] unable to find group ID to join");
        return SSG_ERR_GROUP_NOT_FOUND;
    }
    else if (gd->is_member)
    {
        SSG_GROUP_RELEASE(gd);
        ssg_release_mid_state(mid_state);
        margo_error(mid, "[ssg] unable to join a group it is already a member of");
        return SSG_ERR_INVALID_OPERATION;
    }

    rank = ((int)mid_state->self_id + (int)ts.tv_nsec) % gd->view->size;
    do {
        target_addr = HG_ADDR_NULL;
        target_addr_str = NULL;
        member = *(ssg_member_id_t *)utarray_eltptr(
            gd->view->rank_array, (unsigned int)rank);
        HASH_FIND(hh, gd->view->member_map, &member,
            sizeof(ssg_member_id_t), ms);
        assert(ms);
        if (ms->addr != HG_ADDR_NULL)
        {
            /* use existing member addr if it's set */
            hret = margo_addr_dup(mid_state->mid, ms->addr,
                &target_addr);
            if (hret != HG_SUCCESS)
            {
                SSG_GROUP_RELEASE(gd);
                ssg_release_mid_state(mid_state);
                return SSG_MAKE_HG_ERROR(hret);
            }
        }
        else
        {
            /* otherwise, stash the address string so we can lookup next */
            target_addr_str = strdup(ms->addr_str);
            if (!target_addr_str)
            {
                SSG_GROUP_RELEASE(gd);
                ssg_release_mid_state(mid_state);
                return SSG_ERR_ALLOCATION;
            }
        }

        SSG_DEBUG(mid_state, "sending join request for group %lu to member %lu\n",
            group_id, member);

        /* let others access group while we kickoff lookups / join RPCs */
        SSG_GROUP_RELEASE(gd);

        if (target_addr == HG_ADDR_NULL)
        {
            assert(target_addr_str);
            hret = margo_addr_lookup(mid_state->mid, target_addr_str, &target_addr);
            if (hret != HG_SUCCESS)
            {
                margo_error(mid, "[ssg] unable to lookup group member %s address",
                    target_addr_str);
                free(target_addr_str);
                ret = SSG_MAKE_HG_ERROR(hret);
                goto retry;
            }
            free(target_addr_str);
        }

        /* send the join request to the target to initiate a bulk transfer
         * of the group's membership view
         */
        ret = ssg_group_join_send(group_id, target_addr, mid_state,
            &group_size, &group_config, &view_buf);
        if (ret == SSG_SUCCESS)
        {
            /* join request sent successfully, break out of retry loop */
            margo_addr_free(mid_state->mid, target_addr);
            break;
        }

        margo_error(mid, "[sgg] unable to send group join request to target "
            "[%s]\n", ssg_strerror(ret));
        margo_addr_free(mid_state->mid, target_addr);

retry:
        if(!--retries)
        {
            ssg_release_mid_state(mid_state);
            margo_error(mid, "[ssg] exceeded max retries for joining group");
            return SSG_ERR_MAX_RETRIES;
        }
        /* we have to re-check the group descriptor here */
        SSG_GROUP_READ(group_id, gd);
        if (!gd)
        {
            ssg_release_mid_state(mid_state);
            margo_error(mid, "[ssg] unable to find group ID to join");
            return SSG_ERR_GROUP_NOT_FOUND;
        }
        else if (gd->is_member)
        {
            SSG_GROUP_RELEASE(gd);
            ssg_release_mid_state(mid_state);
            margo_error(mid, "[ssg] unable to join a group it is already a member of");
            return SSG_ERR_INVALID_OPERATION;
        }
        rank = (rank + 1) % gd->view->size;
    } while (1);

    ret = ssg_group_join_internal(group_id, mid_state, view_buf, group_size,
        &group_config, update_cb, update_cb_dat);
    if (ret != SSG_SUCCESS)
    {
        ssg_release_mid_state(mid_state);
    }
    free(view_buf);

    return ret;
}

int ssg_group_join_target(
    margo_instance_id mid,
    ssg_group_id_t group_id,
    hg_addr_t target_addr,
    ssg_membership_update_cb update_cb,
    void * update_cb_dat)
{
    ssg_mid_state_t *mid_state=NULL;
    ssg_group_descriptor_t *gd;
    int group_size;
    ssg_group_config_t group_config;
    void *view_buf = NULL;
    int ret;

    if (!ssg_rt)
        return SSG_ERR_NOT_INITIALIZED;

    if (mid == MARGO_INSTANCE_NULL || group_id == SSG_GROUP_ID_INVALID ||
        target_addr == HG_ADDR_NULL)
        return SSG_ERR_INVALID_ARG;

    ret = ssg_acquire_mid_state(mid, &mid_state);
    if (ret != SSG_SUCCESS)
    {
        margo_error(mid, "[ssg] unable to acquire Margo instance information");
        return ret;
    }

    /* find the group structure to join */
    SSG_GROUP_READ(group_id, gd);
    if (!gd)
    {
        ssg_release_mid_state(mid_state);
        margo_error(mid, "[ssg] unable to find group ID to join");
        return SSG_ERR_GROUP_NOT_FOUND;
    }
    else if (gd->is_member)
    {
        SSG_GROUP_RELEASE(gd);
        ssg_release_mid_state(mid_state);
        margo_error(mid, "[ssg] unable to join a group it is already a member of");
        return SSG_ERR_INVALID_OPERATION;
    }

    SSG_DEBUG(mid_state, "sending join request for group %lu\n", group_id);

    /* let others access group while we kickoff join RPCs */
    SSG_GROUP_RELEASE(gd);

    /* send the join request to the target to initiate a bulk transfer
     * of the group's membership view
     */
    ret = ssg_group_join_send(group_id, target_addr, mid_state,
        &group_size, &group_config, &view_buf);
    if (ret != SSG_SUCCESS)
    {
        ssg_release_mid_state(mid_state);
        margo_error(mid, "[ssg] unable to send join request to target "
            "[%s]\n", ssg_strerror(ret));
        return ret;
    }

    ret = ssg_group_join_internal(group_id, mid_state, view_buf, group_size,
        &group_config, update_cb, update_cb_dat);
    if (ret != SSG_SUCCESS)
    {
        ssg_release_mid_state(mid_state);
    }
    free(view_buf);

    return ret;
}

int ssg_group_leave(
    ssg_group_id_t group_id)
{
    ssg_group_descriptor_t *gd;
    int rank;
    ssg_member_id_t member;
    ssg_member_state_t *ms;
    hg_addr_t target_addr;
    struct timespec ts;
    int retries = SSG_MAX_REQUEST_RETRY_COUNT;
    hg_return_t hret;
    int ret;

    if (!ssg_rt)
        return SSG_ERR_NOT_INITIALIZED;

    if (group_id == SSG_GROUP_ID_INVALID)
        return SSG_ERR_INVALID_ARG;

    clock_gettime(CLOCK_MONOTONIC, &ts);

    /* find the group structure to leave */
    SSG_GROUP_READ(group_id, gd);
    if (!gd)
    {
        margo_error(MARGO_INSTANCE_NULL, "[ssg] unable to find group ID to leave");
        return SSG_ERR_GROUP_NOT_FOUND;
    }
    else if(!gd->is_member)
    {
        SSG_GROUP_RELEASE(gd);
        margo_error(gd->mid_state->mid, "[ssg] unable to leave group it is not a member of");
        return SSG_ERR_INVALID_OPERATION;
    }
    else if(gd->group->config.swim_disabled)
    {
        /* dynamic groups can't be supported if SWIM is disabled */
        SSG_GROUP_RELEASE(gd);
        margo_error(gd->mid_state->mid, "[ssg] unable to leave group if SWIM is disabled");
        return SSG_ERR_NOT_SUPPORTED;
    }

    rank = ((int)gd->mid_state->self_id + (int)ts.tv_nsec) % gd->view->size;
    do {
        if (gd->view->size == 1)
        {
            SSG_GROUP_RELEASE(gd);
            break;
        }
        target_addr = HG_ADDR_NULL;
        member = *(ssg_member_id_t *)utarray_eltptr(
            gd->view->rank_array, (unsigned int)rank);
        if (member == gd->mid_state->self_id)
        {
            /* skip ourselves in the rank list */
            rank = (rank + 1) % gd->view->size;
            continue;
        }
        HASH_FIND(hh, gd->view->member_map, &member,
            sizeof(ssg_member_id_t), ms);
        assert(ms);
        assert(ms->addr != HG_ADDR_NULL);

        hret = margo_addr_dup(gd->mid_state->mid, ms->addr, &target_addr);
        if (hret != HG_SUCCESS)
        {
            SSG_GROUP_RELEASE(gd);
            return SSG_MAKE_HG_ERROR(hret);
        }

        SSG_DEBUG(gd->mid_state, "sending leave request for group %lu to member %lu\n",
            group_id, member);

        /* let others access group while we kickoff leave RPCs */
        SSG_GROUP_RELEASE(gd);

        ret = ssg_group_leave_send(group_id, target_addr, gd->mid_state);
        if (ret == SSG_SUCCESS)
        {
            /* leave request sent successfully, break out of retry loop */
            margo_addr_free(gd->mid_state->mid, target_addr);
            break;
        }

        margo_error(gd->mid_state->mid, "[ssg] unable to send group leave request to target "
            "[%s]", ssg_strerror(ret));
        margo_addr_free(gd->mid_state->mid, target_addr);

        if(!--retries)
        {
            margo_error(gd->mid_state->mid, "[ssg] exceeded max retries for leaving group");
            return SSG_ERR_MAX_RETRIES;
        }
        /* we have to re-check the group descriptor here */
        SSG_GROUP_READ(group_id, gd);
        if (!gd)
        {
            margo_error(MARGO_INSTANCE_NULL, "[ssg] unable to find group ID to join");
            return SSG_ERR_GROUP_NOT_FOUND;
        }
        else if(!gd->is_member)
        {
            SSG_GROUP_RELEASE(gd);
            margo_error(gd->mid_state->mid,
                "[ssg] unable to leave group this process is not a member of");
            return SSG_ERR_INVALID_OPERATION;
        }
        else if(gd->group->config.swim_disabled)
        {
            /* dynamic groups can't be supported if SWIM is disabled */
            SSG_GROUP_RELEASE(gd);
            margo_error(gd->mid_state->mid, "[ssg] unable to leave group if SWIM is disabled");
            return SSG_ERR_NOT_SUPPORTED;
        }
        rank = (rank + 1) % gd->view->size;
    } while (1);

    ret = ssg_group_leave_internal(group_id);

    return ret;
}

int ssg_group_leave_target(
    ssg_group_id_t group_id,
    hg_addr_t target_addr)
{
    ssg_group_descriptor_t *gd;
    int ret;

    if (!ssg_rt)
        return SSG_ERR_NOT_INITIALIZED;

    if (group_id == SSG_GROUP_ID_INVALID || target_addr == HG_ADDR_NULL)
        return SSG_ERR_INVALID_ARG;

    /* find the group structure to join */
    SSG_GROUP_WRITE(group_id, gd);
    if (!gd)
    {
        margo_error(MARGO_INSTANCE_NULL, "[ssg] unable to find group ID to leave");
        return SSG_ERR_GROUP_NOT_FOUND;
    }
    else if(!gd->is_member)
    {
        SSG_GROUP_RELEASE(gd);
        margo_error(gd->mid_state->mid,
            "[ssg] unable to leave group this process is not a member of");
        return SSG_ERR_INVALID_OPERATION;
    }
    else if(gd->group->config.swim_disabled)
    {
        /* dynamic groups can't be supported if SWIM is disabled */
        SSG_GROUP_RELEASE(gd);
        margo_error(gd->mid_state->mid,
            "[ssg] unable to leave group if SWIM is disabled");
        return SSG_ERR_NOT_SUPPORTED;
    }

    SSG_DEBUG(gd->mid_state, "sending leave request for group %lu\n", group_id);

    /* let others access group while we kickoff leave RPCs */
    SSG_GROUP_RELEASE(gd);

    /* send leave request to target member if one is available */
    ret = ssg_group_leave_send(group_id, target_addr, gd->mid_state);
    if (ret != SSG_SUCCESS)
    {
        margo_error(gd->mid_state->mid,
            "[ssg] unable to send group leave request [%s]", ssg_strerror(ret));
        return ret;
    }

    ret = ssg_group_leave_internal(group_id);

    return ret;
}

int ssg_group_refresh(
    margo_instance_id mid,
    ssg_group_id_t group_id)
{
    ssg_mid_state_t *mid_state = NULL;
    ssg_group_descriptor_t *gd;
    int rank;
    ssg_member_id_t member;
    ssg_member_state_t *ms;
    hg_addr_t target_addr;
    char *target_addr_str;
    struct timespec ts;
    int group_size;
    void *view_buf = NULL;
    int retries = SSG_MAX_REQUEST_RETRY_COUNT;
    hg_return_t hret;
    int ret;

    if (!ssg_rt)
        return SSG_ERR_NOT_INITIALIZED;

    if (mid == MARGO_INSTANCE_NULL || group_id == SSG_GROUP_ID_INVALID)
        return SSG_ERR_INVALID_ARG;

    ret = ssg_acquire_mid_state(mid, &mid_state);
    if (ret != SSG_SUCCESS)
    {
        margo_error(mid, "[ssg] unable to acquire Margo instance information");
        return ret;
    }

    clock_gettime(CLOCK_MONOTONIC, &ts);

    /* get read access to the group structure to refresh */
    SSG_GROUP_READ(group_id, gd);
    if (!gd)
    {
        ssg_release_mid_state(mid_state);
        margo_error(mid, "[ssg] unable to find group ID to refresh");
        return SSG_ERR_GROUP_NOT_FOUND;
    }
    else if (gd->is_member)
    {
        /* refresh is a NOP for group members, where views are updated via SWIM */
        SSG_GROUP_RELEASE(gd);
        ssg_release_mid_state(mid_state);
        return SSG_SUCCESS;
    }

    rank = ((int)mid_state->self_id + (int)ts.tv_nsec) % gd->view->size;
    do
    {
        target_addr = HG_ADDR_NULL;
        target_addr_str = NULL;
        member = *(ssg_member_id_t *)utarray_eltptr(
            gd->view->rank_array, (unsigned int)rank);
        HASH_FIND(hh, gd->view->member_map, &member,
            sizeof(ssg_member_id_t), ms);
        assert(ms);
        if (ms->addr != HG_ADDR_NULL)
        {
            /* use existing member addr if it's set */
            hret = margo_addr_dup(mid_state->mid, ms->addr,
                &target_addr);
            if (hret != HG_SUCCESS)
            {
                SSG_GROUP_RELEASE(gd);
                ssg_release_mid_state(mid_state);
                return SSG_MAKE_HG_ERROR(hret);
            }
        }
        else
        {
            /* otherwise, stash the address string so we can lookup next */
            target_addr_str = strdup(ms->addr_str);
            if (!target_addr_str)
            {
                SSG_GROUP_RELEASE(gd);
                ssg_release_mid_state(mid_state);
                return SSG_ERR_ALLOCATION;
            }
        }

        SSG_DEBUG(mid_state, "sending refresh request for group %lu to member %lu\n",
            group_id, member);

        /* let others access group while we kickoff lookups / refresh RPCs */
        SSG_GROUP_RELEASE(gd);

        if (target_addr == HG_ADDR_NULL)
        {
            assert(target_addr_str);
            hret = margo_addr_lookup(mid_state->mid, target_addr_str, &target_addr);
            if (hret != HG_SUCCESS)
            {
                margo_error(mid, "[ssg] unable to lookup group member %s address",
                    target_addr_str);
                free(target_addr_str);
                ret = SSG_MAKE_HG_ERROR(hret);
                goto retry;
            }
            free(target_addr_str);
        }

        /* send the refresh request to the target to initiate a bulk transfer
         * of the group's membership view
         */
        ret = ssg_group_refresh_send(group_id, target_addr, mid_state,
            &group_size, &view_buf);
        if (ret == SSG_SUCCESS)
        {
            /* refresh request sent successfully, break out of retry loop */
            margo_addr_free(mid_state->mid, target_addr);
            break;
        }

        margo_error(mid, "[ssg] unable to send group refresh request to target [%s]",
            ssg_strerror(ret));
        margo_addr_free(mid_state->mid, target_addr);

retry:
        if(!--retries)
        {
            ssg_release_mid_state(mid_state);
            margo_error(mid, "[ssg] exceeded max retries for refreshing group");
            return SSG_ERR_MAX_RETRIES;
        }
        /* we have to re-check the group descriptor here */
        SSG_GROUP_READ(group_id, gd);
        if (!gd)
        {
            ssg_release_mid_state(mid_state);
            margo_error(mid, "[ssg] unable to find group ID to refresh");
            return SSG_ERR_GROUP_NOT_FOUND;
        }
        else if (gd->is_member)
        {
            SSG_GROUP_RELEASE(gd);
            ssg_release_mid_state(mid_state);
            return SSG_SUCCESS;
        }
        rank = (rank + 1) % gd->view->size;
    } while (1);

    ret = ssg_group_refresh_internal(group_id, mid_state, view_buf, group_size);
    if (ret != SSG_SUCCESS)
    {
        ssg_release_mid_state(mid_state);
    }
    free(view_buf);

    return ret;
}

int ssg_group_refresh_target(
    margo_instance_id mid,
    ssg_group_id_t group_id,
    hg_addr_t target_addr)
{
    ssg_mid_state_t *mid_state = NULL;
    ssg_group_descriptor_t *gd;
    int group_size;
    void *view_buf = NULL;
    int ret;

    if (!ssg_rt)
        return SSG_ERR_NOT_INITIALIZED;

    if (mid == MARGO_INSTANCE_NULL || group_id == SSG_GROUP_ID_INVALID ||
        target_addr == HG_ADDR_NULL)
        return SSG_ERR_INVALID_ARG;

    ret = ssg_acquire_mid_state(mid, &mid_state);
    if (ret != SSG_SUCCESS)
    {
        margo_error(mid, "[ssg] unable to acquire Margo instance information");
        return ret;
    }

    /* find the group structure to refresh */
    SSG_GROUP_READ(group_id, gd);
    if (!gd)
    {
        ssg_release_mid_state(mid_state);
        margo_error(mid, "[ssg] unable to find group ID to refresh");
        return SSG_ERR_GROUP_NOT_FOUND;
    }
    else if (gd->is_member)
    {
        /* refresh is a NOP for group members, where views are updated via SWIM */
        SSG_GROUP_RELEASE(gd);
        ssg_release_mid_state(mid_state);
        return SSG_SUCCESS;
    }

    SSG_DEBUG(mid_state, "sending refresh request for group %lu\n", group_id);

    /* let others access group while we kickoff refresh RPCs */
    SSG_GROUP_RELEASE(gd);

    /* send the refresh request to the target to initiate a bulk transfer
     * of the group's membership view
     */
    ret = ssg_group_refresh_send(group_id, target_addr, mid_state,
        &group_size, &view_buf);
    if (ret != SSG_SUCCESS)
    {
        ssg_release_mid_state(mid_state);
        margo_error(mid, "[ssg] unable to send group refresh request to target [%s]",
            ssg_strerror(ret));
        return ret;
    }

    ret = ssg_group_refresh_internal(group_id, mid_state, view_buf, group_size);
    if (ret != SSG_SUCCESS)
    {
        ssg_release_mid_state(mid_state);
    }
    free(view_buf);

    return ret;
}

/*********************************************************
 *** SSG routines for obtaining self/group information ***
 *********************************************************/

int ssg_get_self_id(
    margo_instance_id mid,
    ssg_member_id_t *self_id)
{
    ssg_mid_state_t *mid_state;
    int ret;

    *self_id = SSG_MEMBER_ID_INVALID;

    if (!ssg_rt) return SSG_ERR_NOT_INITIALIZED;

    ABT_rwlock_rdlock(ssg_rt->lock);
    LL_SEARCH_SCALAR(ssg_rt->mid_list, mid_state, mid, mid);
    if(mid_state)
    {
        *self_id = mid_state->self_id;
        ret = SSG_SUCCESS;
    }
    else
    {
        ret = SSG_ERR_MID_NOT_FOUND;
    }
    ABT_rwlock_unlock(ssg_rt->lock);

    return ret;
}

int ssg_get_group_size(
    ssg_group_id_t group_id,
    int *group_size)
{
    ssg_group_descriptor_t *gd;

    *group_size = 0;

    if (!ssg_rt) return SSG_ERR_NOT_INITIALIZED;

    if (group_id == SSG_GROUP_ID_INVALID) return SSG_ERR_INVALID_ARG;

    /* find the group structure to read */
    SSG_GROUP_READ(group_id, gd);
    if (!gd)
    {
        margo_error(MARGO_INSTANCE_NULL, "[ssg] unable to find group ID");
        return SSG_ERR_GROUP_NOT_FOUND;
    }

    assert(gd->view);
    *group_size = gd->view->size;

    SSG_GROUP_RELEASE(gd);

    return SSG_SUCCESS;
}

int ssg_get_group_member_addr(
    ssg_group_id_t group_id,
    ssg_member_id_t member_id,
    hg_addr_t *member_addr)
{
    ssg_group_descriptor_t *gd;
    ssg_member_state_t *member_state;
    hg_return_t hret;
    int ret;

    *member_addr = HG_ADDR_NULL;

    if (!ssg_rt) return SSG_ERR_NOT_INITIALIZED;

    if (group_id == SSG_GROUP_ID_INVALID || member_id == SSG_MEMBER_ID_INVALID)
        return SSG_ERR_INVALID_ARG;

    /* find the group structure to read */
    SSG_GROUP_READ(group_id, gd);
    if (!gd)
    {
        margo_error(MARGO_INSTANCE_NULL, "[ssg] unable to find group ID");
        return SSG_ERR_GROUP_NOT_FOUND;
    }

    if (gd->is_member && (member_id == gd->mid_state->self_id))
    {
        hret = margo_addr_dup(gd->mid_state->mid,
            gd->mid_state->self_addr, member_addr);
        if (hret != HG_SUCCESS)
            ret = SSG_MAKE_HG_ERROR(hret);
        else
            ret = SSG_SUCCESS;
    }
    else if (gd->mid_state)
    {
        assert(gd->view);
        HASH_FIND(hh, gd->view->member_map, &member_id,
            sizeof(ssg_member_id_t), member_state);
        if (member_state) 
        {
            if (member_state->addr != HG_ADDR_NULL)
            {
                hret = margo_addr_dup(gd->mid_state->mid,
                    member_state->addr, member_addr);
            }
            else
            {
                hret = margo_addr_lookup(gd->mid_state->mid,
                    member_state->addr_str, member_addr);
            }
            if (hret != HG_SUCCESS)
                ret = SSG_MAKE_HG_ERROR(hret);
            else
                ret = SSG_SUCCESS;
        }
        else
        {
            ret = SSG_ERR_MEMBER_NOT_FOUND;
        }
    }
    else
    {
        ret = SSG_ERR_MID_NOT_FOUND;
    }

    SSG_GROUP_RELEASE(gd);

    return ret;
}

int ssg_get_group_member_addr_str(
    ssg_group_id_t group_id,
    ssg_member_id_t member_id,
    char **member_addr_str)
{
    ssg_group_descriptor_t *gd;
    ssg_member_state_t *member_state;
    int ret;

    *member_addr_str = NULL;

    if (!ssg_rt) return SSG_ERR_NOT_INITIALIZED;

    if (group_id == SSG_GROUP_ID_INVALID || member_id == SSG_MEMBER_ID_INVALID)
        return SSG_ERR_INVALID_ARG;

    /* find the group structure to read */
    SSG_GROUP_READ(group_id, gd);
    if (!gd)
    {
        margo_error(MARGO_INSTANCE_NULL, "[ssg] unable to find group ID");
        return SSG_ERR_GROUP_NOT_FOUND;
    }

    if (gd->is_member && (member_id == gd->mid_state->self_id))
    {
        *member_addr_str = strdup(gd->mid_state->self_addr_str);
        if (*member_addr_str)
            ret = SSG_SUCCESS;
        else
            ret = SSG_ERR_ALLOCATION;
    }
    else
    {
        assert(gd->view);
        HASH_FIND(hh, gd->view->member_map, &member_id,
            sizeof(ssg_member_id_t), member_state);
        if (member_state) 
        {
            *member_addr_str = strdup(member_state->addr_str);
            if (*member_addr_str)
                ret = SSG_SUCCESS;
            else
                ret = SSG_ERR_ALLOCATION;
        }
        else
        {
            ret = SSG_ERR_MEMBER_NOT_FOUND;
        }
    }

    SSG_GROUP_RELEASE(gd);

    return ret;
}

int ssg_get_group_self_rank(
    ssg_group_id_t group_id,
    int *rank)
{
    ssg_group_descriptor_t *gd;
    int ret;

    *rank = -1;

    if (!ssg_rt) return SSG_ERR_NOT_INITIALIZED;

    if (group_id == SSG_GROUP_ID_INVALID) return SSG_ERR_INVALID_ARG;

    /* find the group structure to read */
    SSG_GROUP_READ(group_id, gd);
    if (!gd)
    {
        margo_error(MARGO_INSTANCE_NULL, "[ssg] unable to find group ID");
        return SSG_ERR_GROUP_NOT_FOUND;
    }
    if (!gd->is_member)
    {
        SSG_GROUP_RELEASE(gd);
        margo_error(gd->mid_state->mid,
            "[ssg] unable to obtain self rank for non-group members");
        return SSG_ERR_INVALID_OPERATION;
    }

    assert(gd->view);
    ret = ssg_get_group_member_rank_internal(gd->view,
        gd->mid_state->self_id, rank);

    SSG_GROUP_RELEASE(gd);

    return ret;
}

int ssg_get_group_member_rank(
    ssg_group_id_t group_id,
    ssg_member_id_t member_id,
    int *rank)
{
    ssg_group_descriptor_t *gd;
    int ret;

    *rank = -1;

    if (!ssg_rt) return SSG_ERR_NOT_INITIALIZED;

    if (group_id == SSG_GROUP_ID_INVALID || member_id == SSG_MEMBER_ID_INVALID)
        return SSG_ERR_INVALID_ARG;

    /* find the group structure to read */
    SSG_GROUP_READ(group_id, gd);
    if (!gd)
    {
        margo_error(MARGO_INSTANCE_NULL, "[ssg] unable to find group ID");
        return SSG_ERR_GROUP_NOT_FOUND;
    }

    assert(gd->view);
    ret = ssg_get_group_member_rank_internal(gd->view, member_id, rank);

    SSG_GROUP_RELEASE(gd);

    return ret;
}

int ssg_get_group_member_id_from_rank(
    ssg_group_id_t group_id,
    int rank,
    ssg_member_id_t *member_id)
{
    ssg_group_descriptor_t *gd;

    *member_id = SSG_MEMBER_ID_INVALID;

    if (!ssg_rt) return SSG_ERR_NOT_INITIALIZED;

    if (group_id == SSG_GROUP_ID_INVALID || rank < 0)
        return SSG_ERR_INVALID_ARG;

    /* find the group structure to read */
    SSG_GROUP_READ(group_id, gd);
    if (!gd)
    {
        margo_error(MARGO_INSTANCE_NULL, "[ssg] unable to find group ID");
        return SSG_ERR_GROUP_NOT_FOUND;
    }

    assert(gd->view);
    if (rank >= (int)gd->view->size)
    {
        SSG_GROUP_RELEASE(gd);
        return SSG_ERR_INVALID_ARG;
    }
    *member_id = *(ssg_member_id_t *)utarray_eltptr(
        gd->view->rank_array, (unsigned int)rank);

    SSG_GROUP_RELEASE(gd);

    return SSG_SUCCESS;
}

int ssg_get_group_member_ids_from_range(
    ssg_group_id_t group_id,
    int rank_start,
    int rank_end,
    ssg_member_id_t *range_ids)
{
    ssg_group_descriptor_t *gd;
    ssg_member_id_t *member_start;

    if (!ssg_rt) return SSG_ERR_NOT_INITIALIZED;

    if (group_id == SSG_GROUP_ID_INVALID || rank_start < 0 ||
            rank_end < 0 || rank_end < rank_start)
        return SSG_ERR_INVALID_ARG;

    /* find the group structure to read */
    SSG_GROUP_READ(group_id, gd);
    if (!gd)
    {
        margo_error(MARGO_INSTANCE_NULL, "[ssg] unable to find group ID");
        return SSG_ERR_GROUP_NOT_FOUND;
    }

    assert(gd->view);
    if (rank_end >= (int)gd->view->size)
    {
        SSG_GROUP_RELEASE(gd);
        return SSG_ERR_INVALID_ARG;
    }
    member_start = (ssg_member_id_t *)utarray_eltptr(
        gd->view->rank_array, (unsigned int)rank_start);
    memcpy(range_ids, member_start, (rank_end-rank_start+1)*sizeof(ssg_member_id_t));

    SSG_GROUP_RELEASE(gd);

    return SSG_SUCCESS;
}

int ssg_group_id_serialize(
    ssg_group_id_t group_id,
    int num_addrs,
    char ** buf_p,
    size_t * buf_size_p)
{
    ssg_group_descriptor_t *gd;
    uint64_t magic_nr = SSG_MAGIC_NR;
    uint64_t gid_size, addr_str_size = 0;
    uint32_t num_addrs_buf=0;
    char *gid_buf, *p;
    unsigned int i = 0;
    ssg_member_state_t *state, *tmp;

    *buf_p = NULL;
    *buf_size_p = 0;

    if (!ssg_rt) return SSG_ERR_NOT_INITIALIZED;

    if (group_id == SSG_GROUP_ID_INVALID || num_addrs == 0)
        return SSG_ERR_INVALID_ARG;

    /* find the group structure to serialize */
    SSG_GROUP_READ(group_id, gd);
    if (!gd)
    {
        margo_error(MARGO_INSTANCE_NULL, "[ssg] unable to find group ID");
        return SSG_ERR_GROUP_NOT_FOUND;
    }

    /* determine needed buffer size */
    gid_size = sizeof(magic_nr) + sizeof(gd->g_id) +
        (strlen(gd->name) + 1) + sizeof(num_addrs_buf) + sizeof(gd->cred);
    if(gd->is_member)
    {
        i = 1;
        /* serialize self string first if member ... */
        addr_str_size += strlen(gd->mid_state->self_addr_str) + 1;
    }

    /* next serialize addresses in view until limit is reached */
    if ((num_addrs == SSG_ALL_MEMBERS) || (gd->view->size < (unsigned int)num_addrs))
        num_addrs_buf = gd->view->size;
    else
        num_addrs_buf = num_addrs;
    HASH_ITER(hh, gd->view->member_map, state, tmp)
    {
        if (i == num_addrs_buf) break;
        addr_str_size += strlen(state->addr_str) + 1;
        i++;
    }

    gid_buf = malloc((size_t)(gid_size + addr_str_size));
    if (!gid_buf)
    {
        SSG_GROUP_RELEASE(gd);
        return SSG_ERR_ALLOCATION;
    }

    /* serialize */
    p = gid_buf;
    *(uint64_t *)p = magic_nr;
    p += sizeof(magic_nr);
    *(ssg_group_id_t *)p = gd->g_id;
    p += sizeof(gd->g_id);
    strcpy(p, gd->name);
    p += strlen(gd->name) + 1;
    *(uint32_t *)p = num_addrs_buf;
    p += sizeof(num_addrs_buf);
    i = 0;
    if(gd->is_member)
    {
        i = 1;
        strcpy(p, gd->mid_state->self_addr_str);
        p += strlen(gd->mid_state->self_addr_str) + 1;
    }
    HASH_ITER(hh, gd->view->member_map, state, tmp)
    {
        if (i == num_addrs_buf) break;
        strcpy(p, state->addr_str);
        p += strlen(state->addr_str) + 1;
        i++;
    }
    *(int64_t *)p = gd->cred;
    /* the rest of the descriptor is stateful and not appropriate for serializing... */

    SSG_GROUP_RELEASE(gd);

    *buf_p = gid_buf;
    *buf_size_p = gid_size + addr_str_size;

    return SSG_SUCCESS;
}

int ssg_group_id_deserialize(
    const char * buf,
    size_t buf_size,
    int * num_addrs,
    ssg_group_id_t * group_id_p)
{
    const char *tmp_buf = buf;
    size_t min_buf_size;
    uint64_t magic_nr;
    ssg_group_id_t g_id;
    const char *g_name;
    uint32_t num_addrs_buf;
    int tmp_num_addrs = *num_addrs;
    char **addr_strs;
    int64_t cred;
    ssg_group_descriptor_t *gd, *gd_check;
    int ret;

    *group_id_p = SSG_GROUP_ID_INVALID;
    *num_addrs = 0;

    if (!ssg_rt) return SSG_ERR_NOT_INITIALIZED;

    if (!buf || tmp_num_addrs == 0)
        return SSG_ERR_INVALID_ARG;

    /* check to ensure the buffer contains enough data to make a group ID */
    min_buf_size = (sizeof(magic_nr) + sizeof(g_id) + sizeof(num_addrs_buf) + 2);
    if (buf_size < min_buf_size)
    {
        margo_error(MARGO_INSTANCE_NULL,
            "[ssg] serialized buffer does not contain a valid SSG group ID");
        return SSG_ERR_INVALID_ARG;
    }

    /* deserialize */
    magic_nr = *(uint64_t *)tmp_buf;
    if (magic_nr != SSG_MAGIC_NR)
    {
        margo_error(MARGO_INSTANCE_NULL,
            "[ssg] magic number mismatch when deserializing SSG group ID");
        return SSG_ERR_INVALID_ARG;
    }
    tmp_buf += sizeof(uint64_t);
    g_id = *(ssg_group_id_t *)tmp_buf;
    tmp_buf += sizeof(ssg_group_id_t);
    g_name = tmp_buf;
    tmp_buf += strlen(g_name) + 1;
    num_addrs_buf = *(uint32_t *)tmp_buf;
    tmp_buf += sizeof(uint32_t);

    /* convert buffer of address strings to an array */
    addr_strs = ssg_addr_str_buf_to_list(tmp_buf, num_addrs_buf);
    if (!addr_strs)
        return SSG_ERR_ALLOCATION;
    tmp_buf = addr_strs[num_addrs_buf - 1] + strlen(addr_strs[num_addrs_buf - 1]) + 1;

    /* credential is at very end */
    cred = *(int64_t *)tmp_buf;

    if ((tmp_num_addrs == SSG_ALL_MEMBERS) ||
            (num_addrs_buf < (unsigned int)tmp_num_addrs))
        tmp_num_addrs = num_addrs_buf;

    addr_strs = realloc(addr_strs, tmp_num_addrs * sizeof(*addr_strs));
    if (!addr_strs)
        return SSG_ERR_ALLOCATION;

    /* create the group descriptor */
    gd = ssg_group_descriptor_create(g_id, g_name, NULL, NULL, cred);
    if (!gd)
    {
        free(addr_strs);
        return SSG_ERR_ALLOCATION;
    }

    /* initialize the group view */
    ret = ssg_group_view_create((const char * const *)addr_strs, tmp_num_addrs,
        NULL, NULL, gd->view);
    if (ret != SSG_SUCCESS)
    {
        ssg_group_descriptor_free(gd);
        free(addr_strs);
        return ret;
    }

    free(addr_strs);

    /* add this group descriptor to our global table, first making sure not re-create */
    ABT_rwlock_wrlock(ssg_rt->lock);
    HASH_FIND(hh, ssg_rt->gd_table, &g_id, sizeof(ssg_group_id_t), gd_check);
    if(gd_check)
    {
        ABT_rwlock_unlock(ssg_rt->lock);
        ssg_group_view_destroy(gd->view, NULL);
        ssg_group_descriptor_free(gd);
        return SSG_ERR_GROUP_EXISTS;
    }
    HASH_ADD(hh, ssg_rt->gd_table, g_id, sizeof(ssg_group_id_t), gd);
    ABT_rwlock_unlock(ssg_rt->lock);

    *group_id_p = g_id;
    *num_addrs = tmp_num_addrs;

    return SSG_SUCCESS;
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
    int ret;

    ret = ssg_group_id_serialize(group_id, num_addrs, &buf, &buf_size);
    if (ret != SSG_SUCCESS)
    {
        margo_error(MARGO_INSTANCE_NULL, "[ssg] unable to serialize SSG group ID");
        return ret;
    }

    fd = open(file_name, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd < 0)
    {
        margo_error(MARGO_INSTANCE_NULL,
            "[ssg] unable to open file %s for storing SSG group ID: %s",
            file_name, strerror(errno));
        free(buf);
        return SSG_ERR_FILE_IO;
    }

    bytes_written = write(fd, buf, buf_size);
    if (bytes_written != (ssize_t)buf_size)
    {
        margo_error(MARGO_INSTANCE_NULL,
            "[ssg] unable to write SSG group ID to file %s", file_name);
        free(buf);
        close(fd);
        return SSG_ERR_FILE_IO;
    }

    free(buf);
    close(fd);
    return SSG_SUCCESS;
}

int ssg_group_id_load(
    const char * file_name,
    int * num_addrs,
    ssg_group_id_t * group_id)
{
    int fd;
    char *buf;
    ssize_t bufsize=1024;
    ssize_t total=0, bytes_read;
    int eof = 0;
    int ret;

    *group_id = SSG_GROUP_ID_INVALID;

    fd = open(file_name, O_RDONLY);
    if (fd < 0)
    {
        margo_error(MARGO_INSTANCE_NULL,
            "[ssg] unable to open file %s for loading SSG group ID",
            file_name);
        return SSG_ERR_FILE_IO;
    }

    /* we used to stat the file to see how big it is.  stat is expensive, so let's skip that */
    buf = malloc(bufsize);
    if (buf == NULL)
    {
        close(fd);
        return SSG_ERR_ALLOCATION;
    }

    do {
        bytes_read = read(fd, buf+total, bufsize-total);
        if (bytes_read == -1 || bytes_read == 0)
        {
            margo_error(MARGO_INSTANCE_NULL,
                "[ssg] unable to read SSG group ID from file %s: %ld (%s)",
                file_name, bytes_read, strerror(errno));
            close(fd);
            free(buf);
            return SSG_ERR_FILE_IO;
        }
        if (bytes_read == bufsize - total) {
            bufsize *= 2;
            buf = realloc(buf, bufsize);
        } else {
            eof = 1;
        }
        total += bytes_read;
    } while (!eof);

    ret = ssg_group_id_deserialize(buf, (size_t)total, num_addrs, group_id);
    if (ret != SSG_SUCCESS)
        margo_error(MARGO_INSTANCE_NULL, "[ssg] unable to deserialize SSG group ID");

    close(fd);
    free(buf);
    return ret;
}

int ssg_get_group_cred_from_buf(
    const char * buf,
    size_t buf_size,
    int64_t *cred)
{
    *cred = -1;

    if (!buf || buf_size < 8)
        return SSG_ERR_INVALID_ARG;

    *cred = *(int64_t *)(buf + buf_size - 8);

    return SSG_SUCCESS;
}

int ssg_get_group_cred_from_file(
    const char * file_name,
    int64_t *cred)
{
    int fd;
    char *buf;
    ssize_t bufsize=1024;
    ssize_t total=0, bytes_read;
    int eof = 0;
    int ret;

    *cred = -1;

    fd = open(file_name, O_RDONLY);
    if (fd < 0)
    {
        margo_error(MARGO_INSTANCE_NULL,
            "[ssg] unable to open file %s for reading SSG group credential",
            file_name);
        return SSG_ERR_FILE_IO;
    }

    /* we used to stat the file to see how big it is.  stat is expensive, so let's skip that */
    buf = malloc(bufsize);
    if (buf == NULL)
    {
        close(fd);
        return SSG_ERR_ALLOCATION;
    }

    do {
        bytes_read = read(fd, buf+total, bufsize-total);
        if (bytes_read == -1 || bytes_read == 0)
        {
            margo_error(MARGO_INSTANCE_NULL,
                "[ssg] unable to read SSG group credential from file %s: %ld (%s)",
                file_name, bytes_read, strerror(errno));
            close(fd);
            free(buf);
            return SSG_ERR_FILE_IO;
        }
        if (bytes_read == bufsize - total) {
            bufsize *= 2;
            buf = realloc(buf, bufsize);
        } else {
            eof = 1;
        }
        total += bytes_read;
    } while (!eof);

    ret = ssg_get_group_cred_from_buf(buf, (size_t)total, cred);
    if (ret != SSG_SUCCESS)
        margo_error(MARGO_INSTANCE_NULL,
            "[ssg] unable to get SSG group credential from buffer");

    close(fd);
    free(buf);
    return ret;
}

int ssg_get_group_transport_from_buf(
    const char * buf,
    size_t buf_size,
    char * tbuf,
    size_t tbuf_size)
{
    const char *tmp_buf = buf;
    size_t min_buf_size;
    uint64_t magic_nr;
    ssg_group_id_t g_id;
    const char *g_name;
    uint32_t num_addrs_buf;
    const char *addr_str;
    const char *transport_end;
    size_t transport_len;

    if (!buf || !tbuf)
        return SSG_ERR_INVALID_ARG;

    tbuf[0] = '\0';

    /* check to ensure the buffer contains enough data to make a group ID */
    min_buf_size = (sizeof(magic_nr) + sizeof(g_id) + sizeof(num_addrs_buf) + 2);
    if (buf_size < min_buf_size)
    {
        margo_error(MARGO_INSTANCE_NULL,
            "[ssg] serialized buffer does not contain a valid SSG group");
        return SSG_ERR_INVALID_ARG;
    }

    /* deserialize */
    magic_nr = *(uint64_t *)tmp_buf;
    if (magic_nr != SSG_MAGIC_NR)
    {
        margo_error(MARGO_INSTANCE_NULL,
            "[ssg] magic number mismatch when deserializing SSG group ID");
        return SSG_ERR_INVALID_ARG;
    }
    tmp_buf += sizeof(uint64_t);
    g_id = *(ssg_group_id_t *)tmp_buf;
    tmp_buf += sizeof(ssg_group_id_t);
    g_name = tmp_buf;
    tmp_buf += strlen(g_name) + 1;
    num_addrs_buf = *(uint32_t *)tmp_buf;
    tmp_buf += sizeof(uint32_t);

    addr_str = tmp_buf;
    transport_end = strstr(addr_str, ":");
    transport_len = transport_end - addr_str;
    if ((transport_len + 1) > tbuf_size)
        return SSG_ERR_NOBUFS;

    memcpy(tbuf, addr_str, transport_len);
    tbuf[transport_len] = '\0';

    return SSG_SUCCESS;
}

int ssg_get_group_transport_from_file(
    const char * file_name,
    char * tbuf,
    size_t tbuf_size)
{
    int fd;
    char *buf;
    ssize_t bufsize=1024;
    ssize_t total=0, bytes_read;
    int eof = 0;
    int ret;

    tbuf[0] = '\0';

    fd = open(file_name, O_RDONLY);
    if (fd < 0)
    {
        margo_error(MARGO_INSTANCE_NULL,
            "[ssg] unable to open file %s for reading SSG group transport",
            file_name);
        return SSG_ERR_FILE_IO;
    }

    /* we used to stat the file to see how big it is.  stat is expensive, so let's skip that */
    buf = malloc(bufsize);
    if (buf == NULL)
    {
        close(fd);
        return SSG_ERR_ALLOCATION;
    }

    do {
        bytes_read = read(fd, buf+total, bufsize-total);
        if (bytes_read == -1 || bytes_read == 0)
        {
            margo_error(MARGO_INSTANCE_NULL,
                "[ssg] unable to read SSG group transport from file %s: %ld (%s)",
                file_name, bytes_read, strerror(errno));
            close(fd);
            free(buf);
            return SSG_ERR_FILE_IO;
        }
        if (bytes_read == bufsize - total) {
            bufsize *= 2;
            buf = realloc(buf, bufsize);
        } else {
            eof = 1;
        }
        total += bytes_read;
    } while (!eof);

    ret = ssg_get_group_transport_from_buf(buf, (size_t)total, tbuf, tbuf_size);
    if (ret != SSG_SUCCESS)
        margo_error(MARGO_INSTANCE_NULL,
            "[ssg] unable to get SSG group transport from buffer");

    close(fd);
    free(buf);
    return ret;
}

int ssg_group_dump(
    ssg_group_id_t group_id)
{
    ssg_group_descriptor_t *gd;
    char group_membership[32];
    char group_self_id[32];
    char hostname[1024];
    unsigned int i;
    ssg_member_id_t member_id;
    char *member_addr_str;
    ssg_member_state_t *member_state;

    if (!ssg_rt) return SSG_ERR_NOT_INITIALIZED;

    if (group_id == SSG_GROUP_ID_INVALID)
        return SSG_ERR_INVALID_ARG;

    gethostname(hostname, 1024);

    /* find the group structure to read */
    SSG_GROUP_READ(group_id, gd);
    if (!gd)
    {
        margo_error(MARGO_INSTANCE_NULL,
            "[ssg] unable to find group ID");
        return SSG_ERR_GROUP_NOT_FOUND;
    }

    if (gd->is_member)
    {
        strcpy(group_membership, "member");
        sprintf(group_self_id, "%lu", gd->mid_state->self_id);
    }
    else
    {
        strcpy(group_membership, "non-member");
        strcpy(group_self_id, "N/A");
    }

    printf("SSG membership information for group '%s' (id=%lu):\n",
        gd->name, gd->g_id);
    printf("\tmembership: %s\n", group_membership);
    printf("\tself_id: %s\n", group_self_id);
    printf("\thost: %s\n", hostname);
    printf("\tsize: %d\n", gd->view->size);
    printf("\tview:\n");
    for (i = 0; i < gd->view->size; i++)
    {
        member_id = *(ssg_member_id_t *)utarray_eltptr(gd->view->rank_array, i);
        if (gd->is_member && (member_id == gd->mid_state->self_id))
        {
            member_addr_str = gd->mid_state->self_addr_str;
        }
        else
        {
            HASH_FIND(hh, gd->view->member_map, &member_id,
                sizeof(ssg_member_id_t), member_state);
            member_addr_str = member_state->addr_str;
        }
        printf("\t\tid: %20lu\taddr: %s\n", member_id, member_addr_str);
    }

    SSG_GROUP_RELEASE(gd);

    fflush(stdout);

    return SSG_SUCCESS;
}

const char *ssg_strerror(int code)
{
    if (code < 0)
        return NULL;

    if (SSG_ERROR_IS_HG(code))
    {
        return HG_Error_to_string((hg_return_t)SSG_GET_HG_ERROR(code));
    }
    else if (SSG_ERROR_IS_ABT(code))
    {
        return "Argobots failure";
    }
    else
    {
        if (code >= SSG_ERR_MAX)
            return NULL;
        return ssg_error_messages[code];
    }
}

/************************************
 *** SSG internal helper routines ***
 ************************************/

static int ssg_acquire_mid_state(
    margo_instance_id mid, ssg_mid_state_t **msp)
{
    ssg_mid_state_t *mid_state;
    hg_size_t self_addr_str_size;
    hg_return_t hret;

    *msp = NULL;

    ABT_rwlock_wrlock(ssg_rt->lock);

    LL_SEARCH_SCALAR(ssg_rt->mid_list, mid_state, mid, mid);
    if(!mid_state)
    {
        mid_state = malloc(sizeof(*mid_state));
        if(!mid_state)
        {
            ABT_rwlock_unlock(ssg_rt->lock);
            return SSG_ERR_ALLOCATION;
        }
        memset(mid_state, 0, sizeof(*mid_state));
        mid_state->mid = mid;

        /* get my self address string and ID (which are constant per-mid) */
        hret = margo_addr_self(mid, &mid_state->self_addr);
        if (hret != HG_SUCCESS)
        {
            ABT_rwlock_unlock(ssg_rt->lock);
            free(mid_state);
            margo_error(mid, "[ssg] unable to obtain self address [HG rc=%d]", hret);
            return SSG_MAKE_HG_ERROR(hret);
        }

        hret = margo_addr_to_string(mid, NULL, &self_addr_str_size, mid_state->self_addr);
        if (hret != HG_SUCCESS)
        {
            ABT_rwlock_unlock(ssg_rt->lock);
            margo_addr_free(mid, mid_state->self_addr);
            free(mid_state);
            margo_error(mid, "[ssg] unable to convert self address to string [HG rc=%d]", hret);
            return SSG_MAKE_HG_ERROR(hret);
        }

        mid_state->self_addr_str = malloc(self_addr_str_size);
        if (mid_state->self_addr_str == NULL)
        {
            ABT_rwlock_unlock(ssg_rt->lock);
            margo_addr_free(mid, mid_state->self_addr);
            free(mid_state);
            return SSG_ERR_ALLOCATION;
        }

        hret = margo_addr_to_string(mid, mid_state->self_addr_str, &self_addr_str_size,
            mid_state->self_addr);
        if (hret != HG_SUCCESS)
        {
            ABT_rwlock_unlock(ssg_rt->lock);
            free(mid_state->self_addr_str);
            margo_addr_free(mid, mid_state->self_addr);
            free(mid_state);
            margo_error(mid, "[ssg] unable to convert self address to string [HG rc=%d]", hret);
            return SSG_MAKE_HG_ERROR(hret);
        }

        mid_state->self_id = ssg_gen_member_id(mid_state->self_addr_str);

        margo_get_handler_pool(mid, &mid_state->pool);

#ifdef DEBUG
        /* set debug output pointer */
        char *dbg_log_dir = getenv("SSG_DEBUG_LOGDIR");
        if (dbg_log_dir)
        {
            char dbg_log_path[PATH_MAX];
            snprintf(dbg_log_path, PATH_MAX, "%s/ssg-%lu.log",
                dbg_log_dir, mid_state->self_id);
            mid_state->dbg_log = fopen(dbg_log_path, "a");
            if (!mid_state->dbg_log)
            {
                ABT_rwlock_unlock(ssg_rt->lock);
                free(mid_state->self_addr_str);
                margo_addr_free(mid, mid_state->self_addr);
                free(mid_state);
                return SSG_ERR_FILE_IO;
            }
        }
        else
        {
            mid_state->dbg_log = stdout;
        }
#endif

        /* register RPCs */
        ssg_register_rpcs(mid_state);

        /* add to mid list */
        LL_APPEND(ssg_rt->mid_list, mid_state);
    }
    mid_state->ref_count++;

    ABT_rwlock_unlock(ssg_rt->lock);

    *msp = mid_state;
    return SSG_SUCCESS;
}

static void ssg_release_mid_state(
    ssg_mid_state_t *mid_state)
{
    ABT_rwlock_wrlock(ssg_rt->lock);

    mid_state->ref_count--;
    if (!mid_state->ref_count)
    {
#ifdef DEBUG
        char *dbg_log_dir = getenv("SSG_DEBUG_LOGDIR");

        fflush(mid_state->dbg_log);
        if (dbg_log_dir)
            fclose(mid_state->dbg_log);
#endif
        LL_DELETE(ssg_rt->mid_list, mid_state);
        ssg_deregister_rpcs(mid_state);
        margo_addr_free(mid_state->mid, mid_state->self_addr);
        free(mid_state->self_addr_str);
        free(mid_state);
    }

    ABT_rwlock_unlock(ssg_rt->lock);

    return;
}

static int ssg_group_create_internal(
    ssg_mid_state_t *mid_state, const char * group_name,
    const char * const group_addr_strs[], int group_size,
    ssg_group_config_t *group_conf, ssg_membership_update_cb update_cb,
    void *update_cb_dat, ssg_group_id_t *group_id)
{
    ssg_group_descriptor_t *gd = NULL, *gd_check;
    ssg_group_id_t tmp_g_id;
    ssg_group_state_t *group;
    ssg_group_config_t tmp_group_conf = SSG_GROUP_CONFIG_INITIALIZER;
    int ret;

    *group_id = SSG_GROUP_ID_INVALID;

    if (!group_conf) group_conf = &tmp_group_conf;

    /* allocate an SSG group data structure and initialize some of it */
    group = calloc(1, sizeof(*group));
    if (!group)
    {
        ret = SSG_ERR_ALLOCATION;
        goto fini;
    }
    memcpy(&group->config, group_conf, sizeof(*group_conf));
    if(update_cb) {
        add_membership_update_cb(group, update_cb, update_cb_dat);
    }

    /* generate unique descriptor for this group */
    tmp_g_id = ssg_hash64_str(group_name);
    gd = ssg_group_descriptor_create(tmp_g_id, group_name, mid_state,
            group, group_conf->ssg_credential);
    if (gd == NULL)
    {
        ret = SSG_ERR_ALLOCATION;
        goto fini;
    }

    /* initialize the group view */
    ret = ssg_group_view_create(group_addr_strs, group_size, mid_state->self_addr_str,
        mid_state, &group->view);
    if (ret != SSG_SUCCESS)
    {
        goto fini;
    }

    if(!(group_conf->swim_disabled))
    {
        /* initialize swim as last step prior to making group visible */
        ret = swim_init(gd);
        if (ret != SSG_SUCCESS)
        {
            goto fini;
        }
    }

    /* make sure we aren't re-creating an existing group, then go ahead and
     * make the group visible by adding it to the table
     */
    ABT_rwlock_wrlock(ssg_rt->lock);
    HASH_FIND(hh, ssg_rt->gd_table, &tmp_g_id, sizeof(ssg_group_id_t), gd_check);
    if(gd_check)
    {
        ABT_rwlock_unlock(ssg_rt->lock);
        ret = SSG_ERR_GROUP_EXISTS;
        goto fini;
    }
    HASH_ADD(hh, ssg_rt->gd_table, g_id, sizeof(ssg_group_id_t), gd);

    SSG_DEBUG(mid_state, "created group %s (g_id=%lu, size=%d, self_addr=%s)\n",
        group_name, tmp_g_id, group_size, mid_state->self_addr_str);
    ABT_rwlock_unlock(ssg_rt->lock);

    *group_id = tmp_g_id;
    ret = SSG_SUCCESS;

fini:
    if (ret != SSG_SUCCESS)
    {
        if (group)
        {
            ssg_group_destroy_internal(group, mid_state);
        }
        if (gd)
        {
            /* caller responsible for releasing mid_state */
            gd->mid_state = NULL; 
            ssg_group_descriptor_free(gd);
        }
    }

    return ret;
}

static int ssg_group_join_internal(
    ssg_group_id_t group_id, ssg_mid_state_t *mid_state,
    void *view_buf, int group_size, ssg_group_config_t *group_conf,
    ssg_membership_update_cb update_cb, void *update_cb_dat)
{
    const char **addr_strs = NULL;
    ssg_group_descriptor_t *gd;
    ssg_group_id_t create_g_id = SSG_GROUP_ID_INVALID;
    int ret;

    /* set up address string array for all group members */
    addr_strs = (const char **)ssg_addr_str_buf_to_list(view_buf, group_size);
    if (!addr_strs)
    {
        return SSG_ERR_ALLOCATION;
    }

    /* append self address string to list of group member address strings */
    addr_strs = realloc(addr_strs, (group_size+1)*sizeof(char *));
    if(!addr_strs)
    {
        return SSG_ERR_ALLOCATION;
    }
    addr_strs[group_size++] = mid_state->self_addr_str;

    /* re-find group, remove it from global table -- it will be re-added
     * as part of the group creation process */
    ABT_rwlock_wrlock(ssg_rt->lock);
    HASH_FIND(hh, ssg_rt->gd_table, &group_id, sizeof(ssg_group_id_t), gd);
    if (!gd)
    {
        ABT_rwlock_unlock(ssg_rt->lock);
        free(addr_strs);
        margo_error(mid_state->mid, "[ssg] unable to find group ID to join");
        return SSG_ERR_GROUP_NOT_FOUND;
    }
    else if (gd->is_member)
    {
        ABT_rwlock_unlock(ssg_rt->lock);
        free(addr_strs);
        margo_error(mid_state->mid,
            "[ssg] unable to join a group this process is already a member of");
        return SSG_ERR_INVALID_OPERATION;
    }
    HASH_DEL(ssg_rt->gd_table, gd);
    ABT_rwlock_unlock(ssg_rt->lock);

    ret = ssg_group_create_internal(mid_state, gd->name, addr_strs, group_size,
            group_conf, update_cb, update_cb_dat, &create_g_id);
    if (ret == SSG_SUCCESS)
    {
        assert(create_g_id == group_id);

        /* free old group info */
        ssg_group_view_destroy(gd->view, gd->mid_state);
        ssg_group_descriptor_free(gd);
    }
    else
    {
        /* add group back so user could potentially re-try */
        ABT_rwlock_wrlock(ssg_rt->lock);
        HASH_ADD(hh, ssg_rt->gd_table, g_id, sizeof(ssg_group_id_t), gd);
        ABT_rwlock_unlock(ssg_rt->lock);
    }

    free(addr_strs);

    return ret;
}

static int ssg_group_leave_internal(
    ssg_group_id_t group_id)
{
    ssg_group_descriptor_t *gd;
    ssg_group_view_t *new_view;
    int self_rank;

    new_view = malloc(sizeof(*new_view));
    if (!new_view)
        return SSG_ERR_ALLOCATION;

    SSG_GROUP_READ(group_id, gd);
    if (!gd)
    {
        free(new_view);
        margo_error(MARGO_INSTANCE_NULL, "[ssg] unable to find group ID to leave");
        return SSG_ERR_GROUP_NOT_FOUND;
    }

    SSG_GROUP_REF_INCR(gd);
    SSG_GROUP_RELEASE(gd);

    /* XXX we can't just rely on ssg_group_destroy_internal, because that
     * function doesn't use locking and expects the group to no longer be
     * visible. we need to explicitly shutdown SWIM with locks released
     * before finalizing destruction of the group with locks held.
     */ 
    if (gd->group->swim_ctx)
    {
        swim_finalize(gd->group);
        gd->group->swim_ctx = NULL;
    }

    ABT_rwlock_wrlock(gd->lock);
    ssg_get_group_member_rank_internal(gd->view, gd->mid_state->self_id,
        &self_rank);
    utarray_erase(gd->view->rank_array, (unsigned int)self_rank, 1);
    gd->view->size--;
    /* preserve group view, then destroy group */
    gd->view = new_view;
    memcpy(gd->view, &gd->group->view, sizeof(*gd->view));
    /* prevent important view data from being destroyed */
    gd->group->view.member_map = NULL;
    gd->group->view.rank_array = NULL;
    ssg_group_destroy_internal(gd->group, gd->mid_state);
    gd->group = NULL;
    gd->is_member = 0;
    ABT_rwlock_unlock(gd->lock);

    SSG_DEBUG(gd->mid_state, "left group %s (g_id=%lu, size=%d)\n",
        gd->name, gd->g_id, gd->view->size);
    SSG_GROUP_REF_DECR(gd);

    return SSG_SUCCESS;
}

static int ssg_group_refresh_internal(
    ssg_group_id_t group_id, ssg_mid_state_t *mid_state,
    void *view_buf, int group_size)
{
    const char **addr_strs = NULL;
    ssg_group_view_t *new_view;
    ssg_group_descriptor_t *gd;
    int ret;

    /* set up address string array for all group members */
    addr_strs = (const char **)ssg_addr_str_buf_to_list(view_buf, group_size);
    if (!addr_strs)
    {
        return SSG_ERR_ALLOCATION;
    }

    new_view = malloc(sizeof(*new_view));
    if (!new_view)
    {
        free(addr_strs);
        return SSG_ERR_ALLOCATION;
    }
    memset(new_view, 0, sizeof(*new_view));

    /* create the new view for the group */
    ret = ssg_group_view_create(addr_strs, group_size, NULL, mid_state, new_view);
    if (ret != SSG_SUCCESS)
    {
        free(new_view);
        free(addr_strs);
        margo_error(MARGO_INSTANCE_NULL, "[ssg] unable to refresh view for group");
        return ret;
    }

    SSG_GROUP_WRITE(group_id, gd);
    if (!gd)
    {
        margo_error(MARGO_INSTANCE_NULL, "[ssg] unable to find group ID to finalize refresh");
        ssg_group_view_destroy(new_view, mid_state);
        free(new_view);
        free(addr_strs);
        return SSG_ERR_GROUP_NOT_FOUND;
    }

    /* reset group view to newly obtained view from above, destroying the 
     * old group view in the process
     */
    /* XXX is there a way to reuse and update the existing view? */
    ssg_group_view_destroy(gd->view, gd->mid_state);
    if (gd->mid_state)
        ssg_release_mid_state(gd->mid_state);
    /* NOTE: the ssg_group_view_destroy() function does not actually free
     * the view structure itself; we need to do that explicitly before
     * assigning new one in this case on a refresh.
     */
    free(gd->view);
    gd->view = new_view;
    gd->mid_state = mid_state;

    SSG_DEBUG(gd->mid_state, "refreshed group %s (g_id=%lu, size=%d)\n",
        gd->name, gd->g_id, gd->view->size);

    SSG_GROUP_RELEASE(gd);

    free(addr_strs);

    return SSG_SUCCESS;
}

static int ssg_group_view_create(
    const char * const group_addr_strs[], int group_size,
    const char * self_addr_str, ssg_mid_state_t * mid_state,
    ssg_group_view_t * view)
{
    int i, j, r;
    ABT_thread *lookup_ults = NULL;
    struct ssg_group_lookup_ult_args *lookup_ult_args = NULL;
    const char *self_addr_substr = NULL;
    const char *addr_substr = NULL;
    int self_found = 0;
    ssg_member_state_t *ms;
    int do_lookup = 0;
    int ret;

    if (mid_state)
    {
        do_lookup = 1;
        /* allocate lookup ULTs if we're given a Margo instance */
        lookup_ults = malloc(group_size * sizeof(*lookup_ults));
        if (lookup_ults == NULL)
        {
            ret = SSG_ERR_ALLOCATION;
            goto fini;
        }
        for (i = 0; i < group_size; i++) lookup_ults[i] = ABT_THREAD_NULL;
        lookup_ult_args = malloc(group_size * sizeof(*lookup_ult_args));
        if (lookup_ult_args == NULL)
        {
            ret = SSG_ERR_ALLOCATION;
            goto fini;
        }
    }

    if(self_addr_str)
    {
        assert(mid_state);

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

    utarray_new(view->rank_array, &ut_ssg_member_id_t_icd);
    utarray_reserve(view->rank_array, group_size);

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

        /* initialize group view with HG_ADDR_NULL addresses */
        ms = ssg_group_view_add_member(group_addr_strs[j], HG_ADDR_NULL,
            ssg_gen_member_id(group_addr_strs[j]), view);
        if (!ms)
        {
            ret = SSG_ERR_ALLOCATION;
            goto fini;
        }

        /* do lookups if given a Margo instance */
        if (do_lookup)
        {
            /* XXX limit outstanding lookups to some max */
            lookup_ult_args[j].mid = mid_state->mid;
            lookup_ult_args[j].ms = ms;
            ret = ABT_thread_create(mid_state->pool, &ssg_group_lookup_ult,
                &lookup_ult_args[j], ABT_THREAD_ATTR_NULL,
                &lookup_ults[j]);
            if (ret != ABT_SUCCESS)
            {
                ret = SSG_MAKE_ABT_ERROR(ret);
                goto fini;
            }
        }
    }

    /* wait on all lookup ULTs to terminate */
    if (do_lookup)
    {
        for (i = 0; i < group_size; i++)
        {
            if (lookup_ults[i] == ABT_THREAD_NULL) continue;

            ret = ABT_thread_join(lookup_ults[i]);
            ABT_thread_free(&lookup_ults[i]);
            lookup_ults[i] = ABT_THREAD_NULL;
            if (ret != ABT_SUCCESS)
            {
                ret = SSG_MAKE_ABT_ERROR(ret);
                goto fini;
            }
            else if (lookup_ult_args[i].out != SSG_SUCCESS)
            {
                margo_error(mid_state->mid, "[ssg] unable to lookup HG address %s",
                    lookup_ult_args[i].ms->addr_str);
                ret = lookup_ult_args[i].out;
                goto fini;
            }
        }
    }

    /* if we provided a self address string and didn't find ourselves,
     * then we return with an error
     */
    if (self_addr_str && !self_found)
    {
        margo_error(mid_state->mid, "[ssg] unable to resolve self ID in group");
        ret = SSG_ERR_SELF_NOT_FOUND;
        goto fini;
    }

    /* setup rank-based array */
    utarray_sort(view->rank_array, ssg_member_id_sort_cmp);

    /* clean exit */
    ret = SSG_SUCCESS;

fini:
    if (ret != SSG_SUCCESS)
    {
        if (do_lookup)
        {
            for (i = 0; i < group_size; i++)
            {
                if (lookup_ults[i] != ABT_THREAD_NULL)
                {
                    ABT_thread_cancel(lookup_ults[i]);
                    ABT_thread_free(&lookup_ults[i]);
                }
            }
        }
        ssg_group_view_destroy(view, mid_state);
    }
    free(lookup_ults);
    free(lookup_ult_args);

    return ret;
}

static void ssg_group_lookup_ult(
    void * arg)
{
    struct ssg_group_lookup_ult_args *l = arg;
    hg_return_t hret;

    hret = margo_addr_lookup(l->mid, l->ms->addr_str, &(l->ms->addr));
    if (hret != HG_SUCCESS)
    {
        l->out = SSG_MAKE_HG_ERROR(hret);
        return;
    }

    l->out = SSG_SUCCESS;
    return;
}

static ssg_member_state_t * ssg_group_view_add_member(
    const char * addr_str, hg_addr_t addr, ssg_member_id_t member_id,
    ssg_group_view_t * view)
{
    ssg_member_state_t *ms;

    ms = calloc(1, sizeof(*ms));
    if (!ms) return NULL;
    ms->addr = addr;
    ms->addr_str = strdup(addr_str);
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
    ssg_group_id_t g_id, const char * name, ssg_mid_state_t *mid_state,
    ssg_group_state_t * group, int64_t cred)
{
    ssg_group_descriptor_t *descriptor;

    descriptor = malloc(sizeof(*descriptor));
    if (!descriptor) return NULL;
    memset(descriptor, 0, sizeof(*descriptor));

    descriptor->g_id = g_id;
    if(name)
        descriptor->name = strdup(name);
    descriptor->mid_state = mid_state;
    descriptor->group = group;
    if (group)
    {
        /* group member views are maintained in internal group state */
        descriptor->view = &group->view;
        descriptor->is_member = 1;
    }
    else
    {
        /* allocate a view to associate with this group */
        descriptor->view = malloc(sizeof(*(descriptor->view)));
        memset(descriptor->view, 0, sizeof(*(descriptor->view)));
    }
    descriptor->cred = cred;
    ABT_rwlock_create(&descriptor->lock);
    ABT_mutex_create(&descriptor->ref_mutex);
    ABT_cond_create(&descriptor->ref_cond);

    return descriptor;
}

static void ssg_group_destroy_internal(
    ssg_group_state_t * g, ssg_mid_state_t *mid_state)
{
    ssg_member_state_t *state, *tmp;

    if (g->swim_ctx)
    {
        /* if SWIM was activated, free up its state */
        swim_finalize(g);
    }

    /* destroy group state */
    ssg_group_view_destroy(&g->view, mid_state);

    HASH_ITER(hh, g->dead_members, state, tmp)
    {
        HASH_DEL(g->dead_members, state);
        free(state->addr_str);
        /* address freed earlier */
        free(state);
    }

    free_all_membership_update_cb(g);

    free(g);

    return;
}

static void ssg_group_view_destroy(
    ssg_group_view_t * view, ssg_mid_state_t *mid_state)
{
    ssg_member_state_t *state, *tmp;

    /* destroy state for all group members */
    HASH_ITER(hh, view->member_map, state, tmp)
    {
        HASH_DEL(view->member_map, state);
        free(state->addr_str);
        if (mid_state && (state->addr != HG_ADDR_NULL))
            margo_addr_free(mid_state->mid, state->addr);
        free(state);
    }
    if(view->rank_array)
        utarray_free(view->rank_array);
    view->member_map = NULL;
    view->rank_array = NULL;
    view->size = 0;

    return;
}

static void ssg_group_descriptor_free(
    ssg_group_descriptor_t * descriptor)
{
    if (descriptor)
    {
        if (descriptor->mid_state)
            ssg_release_mid_state(descriptor->mid_state);
        if(descriptor->name)
            free(descriptor->name);
        if (!descriptor->is_member)
            free(descriptor->view);
        ABT_cond_free(&descriptor->ref_cond);
        ABT_mutex_free(&descriptor->ref_mutex);
        ABT_rwlock_free(&descriptor->lock);
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
    ssg_group_view_t *view, ssg_member_id_t member_id, int *rank)
{
    unsigned int i;
    ssg_member_id_t iter_member_id;

    *rank = -1;

    if (member_id == SSG_MEMBER_ID_INVALID)
        return SSG_ERR_INVALID_ARG;

    /* XXX need a better way to find rank than just iterating the array */
    for (i = 0; i < view->size; i++)
    {
        iter_member_id = *(ssg_member_id_t *)utarray_eltptr(view->rank_array, i);
        if (iter_member_id == member_id)
        {
            *rank = (int)i;
            return SSG_SUCCESS;
        }
    }

    return SSG_ERR_MEMBER_NOT_FOUND;
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
    int wait_secs=1;
    PMIX_INFO_LOAD(get_info, PMIX_TIMEOUT, &wait_secs, PMIX_INT);
    ret = PMIx_Get(source, key, get_info, 1, &val_p);
    PMIX_INFO_FREE(get_info, 1);
    if (ret != PMIX_SUCCESS)
    {
        margo_warning(mid, "[ssg] unable to retrieve PMIx rank mapping for rank %d",
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
            for (i = 0; i < id_array_ptr->size; i++)
            {
                /* remove this member from any group its a member of */
                fail_update.u.member_id = ids[i];

                ABT_rwlock_rdlock(ssg_rt->lock);
                HASH_ITER(hh, ssg_rt->g_desc_table, g_desc, g_desc_tmp)
                {
                    SSG_GROUP_REF_INCR(g_desc->g_data.g);
                    ABT_rwlock_unlock(ssg_rt->lock);
                    SSG_DEBUG(g_desc->g_data.g, "received FAIL update for member %lu\n",
                        fail_update.u.member_id);
                    ssg_apply_member_updates(g_desc->g_data.g, &fail_update, 1, 1);
                    SSG_GROUP_REF_DECR(g_desc->g_data.g);
                    ABT_rwlock_rdlock(ssg_rt->lock);
                }
                ABT_rwlock_unlock(ssg_rt->lock);
            }
        }
        else
        {
            margo_warning(mid, "[ssg] unexpected format for PMIx rank->ID mapping");
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
        margo_error(MARGO_INSTANCE_NULL, "[ssg] PMIx event notification registration failed! [%d]", status);
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

int ssg_apply_member_updates(
    ssg_group_descriptor_t  * gd,
    ssg_member_update_t * updates,
    int update_count,
    int swim_apply_flag)
{
    ssg_member_state_t *update_ms;
    ssg_member_id_t update_member_id;
    ssg_member_update_type_t update_type;
    int update_rank;
    int self_died = 0;
    int i;
    hg_return_t hret;

    for (i = 0; i < update_count; i++)
    {
        if (updates[i].type == SSG_MEMBER_JOINED)
        {
            ssg_member_id_t join_id = ssg_gen_member_id(updates[i].u.member_addr_str);
            hg_addr_t join_addr;

            if (join_id == gd->mid_state->self_id)
            {
                /* ignore joins for self */
                return SSG_SUCCESS;
            }

            ABT_rwlock_rdlock(gd->lock);
            HASH_FIND(hh, gd->view->member_map, &join_id, sizeof(join_id), update_ms);
            if (update_ms)
            {
                /* ignore join messages for members already in view */
                ABT_rwlock_unlock(gd->lock);
                return SSG_SUCCESS;
            }
            HASH_FIND(hh, gd->group->dead_members, &join_id, sizeof(join_id), update_ms);
            if (update_ms)
            {
                /* dead members cannot currently rejoin groups */
                ABT_rwlock_unlock(gd->lock);
                return SSG_ERR_INVALID_OPERATION; 
            }
            ABT_rwlock_unlock(gd->lock);

            /* lookup address of joining member */
            hret = margo_addr_lookup(gd->mid_state->mid, updates[i].u.member_addr_str,
                &join_addr);
            if (hret != HG_SUCCESS)
            {
                SSG_DEBUG(gd->mid_state,
                    "Warning: SSG unable to lookup joining group member %s addr\n",
                    updates[i].u.member_addr_str);
                return SSG_MAKE_HG_ERROR(hret);
            }

            /* add member to the view */
            /* NOTE: double-check to make sure a competing join hasn't added state for
             *       the joining member while we were looking up their address
             */
            ABT_rwlock_wrlock(gd->lock);
            HASH_FIND(hh, gd->view->member_map, &join_id, sizeof(join_id), update_ms);
            if (!update_ms)
            {
                update_ms = ssg_group_view_add_member(updates[i].u.member_addr_str,
                    join_addr, join_id, gd->view);
                if (update_ms == NULL)
                {
                    ABT_rwlock_unlock(gd->lock);
                    margo_addr_free(gd->mid_state->mid, join_addr);
                    return SSG_ERR_ALLOCATION;
                }

                /* setup rank-based array */
                utarray_sort(gd->view->rank_array, ssg_member_id_sort_cmp);

                /* have SWIM apply the join update */
                if(swim_apply_flag)
                {
                    /* NOTE: group lock is held into this call and released internally */
                    swim_apply_ssg_member_update(gd, update_ms, updates[i]);
                }
                else
                {
                    /* unlock needed on this code path */
                    ABT_rwlock_unlock(gd->lock);
                }

                SSG_DEBUG(gd->mid_state, "added joining member %lu\n", join_id);

                update_member_id = join_id;
                update_type = updates[i].type;
            }
            else
            {
                ABT_rwlock_unlock(gd->lock);
                margo_addr_free(gd->mid_state->mid, join_addr);
                return SSG_SUCCESS;
            }
        }
        else if (updates[i].type == SSG_MEMBER_LEFT)
        {
            ABT_rwlock_wrlock(gd->lock);
            HASH_FIND(hh, gd->view->member_map, &updates[i].u.member_id,
                sizeof(updates[i].u.member_id), update_ms);
            if (!update_ms)
            {
                /* ignore leave messages for members not in view */
                ABT_rwlock_unlock(gd->lock);
                continue;
            }

            /* remove from view and add to dead list */
            HASH_DEL(gd->view->member_map, update_ms);
            ssg_get_group_member_rank_internal(gd->view, update_ms->id, &update_rank);
            utarray_erase(gd->view->rank_array, (unsigned int)update_rank, 1);
            gd->view->size--;
            HASH_ADD(hh, gd->group->dead_members, id, sizeof(update_ms->id), update_ms);
            margo_addr_free(gd->mid_state->mid, update_ms->addr);
            update_ms->addr= HG_ADDR_NULL;

            /* have SWIM apply the leave update */
            if(swim_apply_flag)
            {
                /* NOTE: group lock is held into this call and released internally */
                swim_apply_ssg_member_update(gd, update_ms, updates[i]);
            }
            else
            {
                /* unlock needed on this code path */
                ABT_rwlock_unlock(gd->lock);
            }

            SSG_DEBUG(gd->mid_state, "removed leaving member %lu\n",
                updates[i].u.member_id);

            update_member_id = updates[i].u.member_id;
            update_type = updates[i].type;
        }
        else if (updates[i].type == SSG_MEMBER_DIED)
        {
            if (updates[i].u.member_id == gd->mid_state->self_id)
            {
                self_died = 1;
            }
            else
            {
                ABT_rwlock_wrlock(gd->lock);
                HASH_FIND(hh, gd->view->member_map, &updates[i].u.member_id,
                    sizeof(updates[i].u.member_id), update_ms);
                if (!update_ms)
                {
                    /* ignore fail messages for members not in view */
                    ABT_rwlock_unlock(gd->lock);
                    continue;
                }

                /* remove from view and add to dead list */
                HASH_DEL(gd->view->member_map, update_ms);
                ssg_get_group_member_rank_internal(gd->view, update_ms->id, &update_rank);
                utarray_erase(gd->view->rank_array, (unsigned int)update_rank, 1);
                gd->view->size--;
                HASH_ADD(hh, gd->group->dead_members, id, sizeof(update_ms->id), update_ms);
                margo_addr_free(gd->mid_state->mid, update_ms->addr);
                update_ms->addr= HG_ADDR_NULL;

                /* have SWIM apply the dead update */
                if(swim_apply_flag)
                {
                    /* NOTE: group lock is held into this call and released internally */
                    swim_apply_ssg_member_update(gd, update_ms, updates[i]);
                }
                else
                {
                    /* unlock needed on this code path */
                    ABT_rwlock_unlock(gd->lock);
                }

                SSG_DEBUG(gd->mid_state, "removed dead member %lu\n",
                    updates[i].u.member_id);
            }

            update_member_id = updates[i].u.member_id;
            update_type = updates[i].type;
        }
        else
        {
            SSG_DEBUG(gd->mid_state, "Warning: invalid SSG update received, ignoring.\n");
            return SSG_ERR_INVALID_ARG;
        }

        /* invoke user callbacks to apply the SSG update */
        execute_all_membership_update_cb(
            gd->group,
            update_member_id,
            update_type,
            &gd->lock);

        /* check if self died, in which case the group should be destroyed locally */
        if (self_died)
        {
            /* XXX refactor this code path -- nearly same as ssg_group_leave_internal */
            ssg_group_view_t *new_view;
            int self_rank;

            new_view = malloc(sizeof(*new_view));
            if (!new_view)
                return SSG_ERR_ALLOCATION;

            /* XXX we can't just rely on ssg_group_destroy_internal, because that
             * function doesn't use locking and expects the group to no longer be
             * visible. we need to explicitly shutdown SWIM with locks released
             * before finalizing destruction of the group with locks held.
             */
            if (gd->group->swim_ctx)
            {
                swim_finalize(gd->group);
                gd->group->swim_ctx = NULL;
            }

            ABT_rwlock_wrlock(gd->lock);
            ssg_get_group_member_rank_internal(gd->view, gd->mid_state->self_id,
                &self_rank);
            utarray_erase(gd->view->rank_array, (unsigned int)self_rank, 1);
            gd->view->size--;
            /* preserve group view, then destroy group */
            gd->view = new_view;
            memcpy(gd->view, &gd->group->view, sizeof(*gd->view));
            /* prevent important view data from being destroyed */
            gd->group->view.member_map = NULL;
            gd->group->view.rank_array = NULL;
            ssg_group_destroy_internal(gd->group, gd->mid_state);
            gd->group = NULL;
            gd->is_member = 0;
            ABT_rwlock_unlock(gd->lock);

            SSG_DEBUG(gd->mid_state, "self evicted from group %s (g_id=%lu, size=%d)\n",
                gd->name, gd->g_id, gd->view->size);

            /* no need to process further updates, just return an error */
            return SSG_ERR_SELF_FAILED;
        }
    }

    return SSG_SUCCESS;
}
