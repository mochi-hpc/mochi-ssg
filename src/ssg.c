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
#include <assert.h>
#ifdef SSG_HAVE_MPI
#include <mpi.h>
#endif

#include <mercury.h>
#include <abt.h>
#include <margo.h>

#include "ssg.h"
#include "ssg-internal.h"
#if USE_SWIM_FD
#include "swim-fd/swim-fd.h"
#endif

/* SSG helper routine prototypes */
static hg_return_t ssg_group_lookup(
    ssg_group_t * g, const char * const addr_strs[]);
static void ssg_generate_group_id(
    const char * name, const char * leader_addr_str,
    ssg_group_id_t *group_id);
static const char ** ssg_setup_addr_str_list(
    char * buf, int num_addrs);

/* XXX: is this right? can this be a global? */
margo_instance_id ssg_mid = MARGO_INSTANCE_NULL;

/* XXX: fix this */
ssg_group_t *the_group = NULL;

DECLARE_MARGO_RPC_HANDLER(ssg_attach_recv_ult)

static hg_id_t ssg_attach_rpc_id;

/***************************************************
 *** SSG runtime intialization/shutdown routines ***
 ***************************************************/

int ssg_init(
    margo_instance_id mid)
{
    hg_class_t *hg_cls = margo_get_class(mid);

    /* register HG RPCs for SSG */
    ssg_attach_rpc_id = MERCURY_REGISTER(hg_cls, "ssg_attach", void, void,
        ssg_attach_recv_ult_handler);

    ssg_mid = mid;

    return SSG_SUCCESS;
}

int ssg_finalize()
{
    return SSG_SUCCESS;
}

/*************************************
 *** SSG group management routines ***
 *************************************/

int ssg_group_create(
    const char * group_name,
    const char * const group_addr_strs[],
    int group_size,
    ssg_group_id_t * group_id)
{
    hg_class_t *hgcl = NULL;
    hg_addr_t self_addr = HG_ADDR_NULL;
    char *self_addr_str = NULL;
    hg_size_t self_addr_str_size = 0;
    const char *self_addr_substr = NULL;
    const char *addr_substr = NULL;
    int i;
    ssg_group_t *g = NULL;
    hg_return_t hret;
    int sret = SSG_ERROR;

    hgcl = margo_get_class(ssg_mid);
    if (!hgcl) goto fini;

    /* get my address */
    hret = HG_Addr_self(hgcl, &self_addr);
    if (hret != HG_SUCCESS) goto fini;
    hret = HG_Addr_to_string(hgcl, NULL, &self_addr_str_size, self_addr);
    if (hret != HG_SUCCESS) goto fini;
    self_addr_str = malloc(self_addr_str_size);
    if (self_addr_str == NULL) goto fini;
    hret = HG_Addr_to_string(hgcl, self_addr_str, &self_addr_str_size, self_addr);
    if (hret != HG_SUCCESS) goto fini;

    /* strstr is used here b/c there may be inconsistencies in whether the class
     * is included in the address or not (it should not be in HG_Addr_to_string,
     * but it's possible that it is in the list of group address strings)
     */
    self_addr_substr = strstr(self_addr_str, "://");
    if (self_addr_substr == NULL)
        self_addr_substr = self_addr_str;
    else
        self_addr_substr += 3;

    /* allocate an SSG group data structure and initialize some of it */
    g = malloc(sizeof(*g));
    if (!g) goto fini;
    memset(g, 0, sizeof(*g));
    g->group_name = strdup(group_name);
    if (!g->group_name) goto fini;
    // TODO? g->self_id = -1;
    g->group_view.size = group_size;
    g->group_view.member_states = malloc(
        group_size * sizeof(*g->group_view.member_states));
    if (!g->group_view.member_states) goto fini;
    memset(g->group_view.member_states, 0,
        group_size * sizeof(*g->group_view.member_states));

    /* resolve my rank within the group */
    for (i = 0; i < group_size; i++)
    {
        addr_substr = strstr(group_addr_strs[i], "://");
        if (addr_substr == NULL)
            addr_substr = group_addr_strs[i];
        else
            addr_substr += 3;
        if (strcmp(self_addr_substr, addr_substr) == 0)
        {
            /* this is my address -- my rank is the offset in the address array */
            g->self_id = i; // TODO 
            g->group_view.member_states[i].addr = self_addr;
        }
        else
        {
            /* initialize group member addresses to NULL before looking them up */
            g->group_view.member_states[i].addr = HG_ADDR_NULL;
        }
        g->group_view.member_states[i].is_member = 1;
    }
    /* TODO: if unable to resolve my rank within the group, error out */
#if 0
    if (g->self_id == -1)
    {
        fprintf(stderr, "Error: SSG unable to resolve rank in group %s\n",
            group_name);
        goto fini;
    }
#endif

    /* lookup hg addresses information for all group members */
    hret = ssg_group_lookup(g, group_addr_strs);
    if (hret != HG_SUCCESS)
    {
        fprintf(stderr, "Error: SSG unable to complete lookup for group %s\n",
            group_name);
        goto fini;
    }
    SSG_DEBUG(g, "group lookup successful (size=%d)\n", group_size);

#if USE_SWIM_FD
    int swim_active = 1;
#ifdef SWIM_FORCE_FAIL
    if (g->self_rank == 1)
        swim_active = 0;
#endif

    /* initialize swim failure detector */
    // TODO: we should probably barrier or sync somehow to avoid rpc failures
    // due to timing skew of different ranks initializing swim
    g->fd_ctx = (void *)swim_init(g, swim_active);
    if (g->fd_ctx == NULL) goto fini;
#endif

    /* TODO: add reference to this group to SSG runtime state */
    the_group = g;

    /* generate a unique ID for this group that can be used by other
     * processes to join or attach to the group
     */    
    ssg_generate_group_id(group_name, group_addr_strs[0], group_id);

    sret = SSG_SUCCESS;
    /* don't free these pointers on success */
    self_addr = HG_ADDR_NULL;
    g = NULL;
fini:
    if (hgcl && self_addr != HG_ADDR_NULL) HG_Addr_free(hgcl, self_addr);
    free(self_addr_str);
    if (g)
    {
        free(g->group_name);
        free(g->group_view.member_states);
        free(g);
    }

    return sret;
}

int ssg_group_create_config(
    const char * group_name,
    const char * file_name,
    ssg_group_id_t * group_id)
{
    int fd;
    struct stat st;
    char *rd_buf = NULL;
    ssize_t rd_buf_sz;
    char *tok;
    void *addr_buf = NULL;
    int addr_buf_len = 0, num_addrs = 0;
    int ret;
    const char **addr_strs = NULL;
    int sret = SSG_ERROR;

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
    rd_buf_sz = read(fd, rd_buf, st.st_size);
    if (rd_buf_sz != st.st_size)
    {
        fprintf(stderr, "Error: SSG unable to read config file %s for group %s\n",
            file_name, group_name);
        goto fini;
    }
    rd_buf[rd_buf_sz]='\0';

    /* strtok the result - each space-delimited address is assumed to be
     * a unique mercury address
     */
    tok = strtok(rd_buf, "\r\n\t ");
    if (tok == NULL) goto fini;

    /* build up the address buffer */
    addr_buf = malloc(rd_buf_sz);
    if (addr_buf == NULL) goto fini;
    do
    {
        int tok_sz = strlen(tok);
        memcpy((char*)addr_buf + addr_buf_len, tok, tok_sz+1);
        addr_buf_len += tok_sz+1;
        num_addrs++;
        tok = strtok(NULL, "\r\n\t ");
    } while (tok != NULL);
    if (addr_buf_len != rd_buf_sz)
    {
        /* adjust buffer size if our initial guess was wrong */
        void *tmp = realloc(addr_buf, addr_buf_len);
        if (tmp == NULL) goto fini;
        addr_buf = tmp;
    }

    /* set up address string array for group members */
    addr_strs = ssg_setup_addr_str_list(addr_buf, num_addrs);
    if (!addr_strs) goto fini;

    /* invoke the generic group create routine using our list of addrs */
    sret = ssg_group_create(group_name, addr_strs, num_addrs, group_id);

fini:
    /* cleanup before returning */
    if (fd != -1) close(fd);
    free(rd_buf);
    free(addr_buf);
    free(addr_strs);

    return sret;
}

#ifdef SSG_HAVE_MPI
int ssg_group_create_mpi(
    const char * group_name,
    MPI_Comm comm,
    ssg_group_id_t * group_id)
{
    hg_class_t *hgcl = NULL;
    hg_addr_t self_addr = HG_ADDR_NULL;
    char *self_addr_str = NULL;
    hg_size_t self_addr_str_size = 0;
    int self_addr_str_size_int = 0; /* for mpi-friendly conversion */
    char *addr_buf = NULL;
    int *sizes = NULL;
    int *sizes_psum = NULL;
    int comm_size = 0, comm_rank = 0;
    const char **addr_strs = NULL;
    hg_return_t hret;
    int sret = SSG_ERROR;

    hgcl = margo_get_class(ssg_mid);
    if (!hgcl) goto fini;

    /* get my address */
    hret = HG_Addr_self(hgcl, &self_addr);
    if (hret != HG_SUCCESS) goto fini;
    hret = HG_Addr_to_string(hgcl, NULL, &self_addr_str_size, self_addr);
    if (hret != HG_SUCCESS) goto fini;
    self_addr_str = malloc(self_addr_str_size);
    if (self_addr_str == NULL) goto fini;
    hret = HG_Addr_to_string(hgcl, self_addr_str, &self_addr_str_size, self_addr);
    if (hret != HG_SUCCESS) goto fini;
    self_addr_str_size_int = (int)self_addr_str_size; /* null char included in call */

    /* gather the buffer sizes */
    MPI_Comm_size(comm, &comm_size);
    MPI_Comm_rank(comm, &comm_rank);
    sizes = malloc(comm_size * sizeof(*sizes));
    if (sizes == NULL) goto fini;
    sizes[comm_rank] = self_addr_str_size_int;
    MPI_Allgather(MPI_IN_PLACE, 0, MPI_BYTE, sizes, 1, MPI_INT, comm);

    /* compute a exclusive prefix sum of the data sizes, including the
     * total at the end
     */
    sizes_psum = malloc((comm_size+1) * sizeof(*sizes_psum));
    if (sizes_psum == NULL) goto fini;
    sizes_psum[0] = 0;
    for (int i = 1; i < comm_size+1; i++)
        sizes_psum[i] = sizes_psum[i-1] + sizes[i-1];

    /* allgather the addresses */
    addr_buf = malloc(sizes_psum[comm_size]);
    if (addr_buf == NULL) goto fini;
    MPI_Allgatherv(self_addr_str, self_addr_str_size_int, MPI_BYTE,
            addr_buf, sizes, sizes_psum, MPI_BYTE, comm);

    /* set up address string array for group members */
    addr_strs = ssg_setup_addr_str_list(addr_buf, comm_size);
    if (!addr_strs) goto fini;

    /* invoke the generic group create routine using our list of addrs */
    sret = ssg_group_create(group_name, addr_strs, comm_size, group_id);

fini:
    /* cleanup before returning */
    free(sizes);
    free(sizes_psum);
    free(addr_buf);
    if (hgcl && self_addr != HG_ADDR_NULL) HG_Addr_free(hgcl, self_addr);
    free(self_addr_str);
    free(addr_strs);

    return sret;
}
#endif

int ssg_group_destroy(
    ssg_group_id_t group_id)
{
    int i;
    ssg_group_t *g = the_group;
    assert(g);

#if USE_SWIM_FD
    swim_context_t *swim_ctx = (swim_context_t *)g->fd_ctx;
    assert(swim_ctx);
    if(swim_ctx)
        swim_finalize(swim_ctx);
#endif

    for (i = 0; i < g->group_view.size; i++)
    {
        if (g->group_view.member_states[i].addr != HG_ADDR_NULL)
            HG_Addr_free(margo_get_class(ssg_mid),
                g->group_view.member_states[i].addr);
    }
    free(g->group_name);
    free(g->group_view.member_states);
    free(g);

    return SSG_SUCCESS;
}

int ssg_group_attach(
    ssg_group_id_t group_id)
{
#if 0
    hg_class_t *hgcl = NULL;
    hg_addr_t srvr_addr = HG_ADDR_NULL;
    hg_handle_t handle = HG_HANDLE_NULL;
    hg_return_t hret;

    hgcl = margo_get_class(ssg_mid);
    if (!hgcl) goto fini;

    /* lookup the address of the given group's leader server */
    hret = margo_addr_lookup(ssg_mid, group_id.addr_str, &srvr_addr);
    if (hret != HG_SUCCESS) goto fini;

    hret = HG_Create(margo_get_context(ssg_mid), srvr_addr, ssg_attach_rpc_id,
        &handle);
    if (hret != HG_SUCCESS) goto fini;

    /* XXX: send a request to the leader addr to attach to the group */
    hret = margo_forward(ssg_mid, handle, NULL);
    if (hret != HG_SUCCESS) goto fini;

    /* XXX: store the obtained view locally to refer to */

    /* TODO: hold on to leader addr so we don't have to look it up again? */
fini:
    if (hgcl && srvr_addr != HG_ADDR_NULL) HG_Addr_free(hgcl, srvr_addr);
    if (handle != HG_HANDLE_NULL) HG_Destroy(handle);

#endif
    return SSG_SUCCESS;
}

int ssg_group_detach(
    ssg_group_id_t group_id)
{

    return SSG_SUCCESS;
}

/*********************************
 *** SSG group access routines ***
 *********************************/

ssg_member_id_t ssg_get_group_rank(
    ssg_group_id_t group_id)
{
    return 0;
}

int ssg_get_group_size(
    ssg_group_id_t group_id)
{
    return 0;
}

hg_addr_t ssg_get_addr(
    ssg_group_id_t group_id,
    ssg_member_id_t member_id)
{
    //if (rank >= 0 && rank < s->view.group_size)
    return HG_ADDR_NULL;
}

/************************************
 *** SSG internal helper routines ***
 ************************************/

static void ssg_lookup_ult(void * arg);
struct lookup_ult_args
{
    ssg_group_t *g;
    ssg_member_id_t member_id;
    const char *addr_str;
    hg_return_t out;
};

static hg_return_t ssg_group_lookup(
    ssg_group_t * g, const char * const addr_strs[])
{
    ABT_thread *ults;
    struct lookup_ult_args *args;
    int i, r;
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
        aret = ABT_thread_create(*margo_get_handler_pool(ssg_mid), &ssg_lookup_ult,
                &args[r], ABT_THREAD_ATTR_NULL, &ults[r]);
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

    l->out = margo_addr_lookup(ssg_mid, l->addr_str,
        &g->group_view.member_states[l->member_id].addr);
    return;
}

#if 0
static void ssg_attach_recv_ult(hg_handle_t handle)
{
    HG_Destroy(handle);
    return;
}
DEFINE_MARGO_RPC_HANDLER(ssg_attach_recv_ult)
#endif

static void ssg_generate_group_id(
    const char * name, const char * leader_addr_str,
    ssg_group_id_t *group_id)
{
    uint32_t upper, lower;

    /* hash the group name to obtain an 64-bit unique ID */
    ssg_hashlittle2(name, strlen(name), &lower, &upper);

    group_id->magic_nr = SSG_MAGIC_NR;
    group_id->name_hash = lower + (((uint64_t)upper)<<32);
    strcpy(group_id->addr_str, leader_addr_str);

    return;
}

static const char ** ssg_setup_addr_str_list(
    char * buf, int num_addrs)
{
    const char **ret = malloc(num_addrs * sizeof(*ret));
    if (ret == NULL) return NULL;

    ret[0] = buf;
    for (int i = 1; i < num_addrs; i++)
    {
        const char * a = ret[i-1];
        ret[i] = a + strlen(a) + 1;
    }
    return ret;
}
