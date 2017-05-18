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
static const char ** ssg_setup_addr_str_list(
    char * buf, int num_addrs);

/* XXX: is this right? can this be a global? */
margo_instance_id ssg_mid = MARGO_INSTANCE_NULL;

/* XXX: fix this */
ssg_group_t *the_group = NULL;

/***************************************************
 *** SSG runtime intialization/shutdown routines ***
 ***************************************************/

int ssg_init(
    margo_instance_id mid)
{
    ssg_mid = mid;

    return SSG_SUCCESS;
}

void ssg_finalize()
{
    return;
}

/*************************************
 *** SSG group management routines ***
 *************************************/

ssg_group_id_t ssg_group_create(
    const char * group_name,
    const char * const group_addr_strs[],
    int group_size)
{
    hg_class_t *hgcl = NULL;
    hg_addr_t self_addr = HG_ADDR_NULL;
    char *self_addr_str = NULL;
    hg_size_t self_addr_size = 0;
    const char *self_addr_substr = NULL;
    const char *addr_substr = NULL;
    int i;
    hg_return_t hret;
    ssg_group_t *g = NULL;
    ssg_group_id_t g_id = SSG_GROUP_ID_NULL;

    hgcl = margo_get_class(ssg_mid);
    if (!hgcl) goto fini;

    /* get my address */
    hret = HG_Addr_self(hgcl, &self_addr);
    if (hret != HG_SUCCESS) goto fini;
    hret = HG_Addr_to_string(hgcl, NULL, &self_addr_size, self_addr);
    if (hret != HG_SUCCESS) goto fini;
    self_addr_str = malloc(self_addr_size);
    if (self_addr_str == NULL) goto fini;
    hret = HG_Addr_to_string(hgcl, self_addr_str, &self_addr_size, self_addr);
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
    g->name = strdup(group_name);
    if (!g->name) goto fini;
    g->self_rank = -1;
    g->view.group_size = group_size;
    g->view.member_states = malloc(group_size * sizeof(*g->view.member_states));
    if (!g->view.member_states) goto fini;
    memset(g->view.member_states, 0, group_size * sizeof(*g->view.member_states));

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
            g->self_rank = i;
            g->view.member_states[i].addr = self_addr;
        }
        else
        {
            /* initialize group member addresses to NULL before looking them up */
            g->view.member_states[i].addr = HG_ADDR_NULL;
        }
        g->view.member_states[i].is_member = 1;
    }
    /* if unable to resolve my rank within the group, error out */
    if(g->self_rank == -1)
    {
        fprintf(stderr, "Error: SSG unable to resolve rank in group %s\n",
            group_name);
        goto fini;
    }

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
    /* initialize swim failure detector */
    // TODO: we should probably barrier or sync somehow to avoid rpc failures
    // due to timing skew of different ranks initializing swim
    g->fd_ctx = (void *)swim_init(g, 1);
    if (g->fd_ctx == NULL) goto fini;
#endif

    /* TODO: last step => add reference to this group to SSG runtime state */
    the_group = g;

    /* don't free these pointers on success */
    self_addr = HG_ADDR_NULL;
    g = NULL;
fini:
    if (hgcl && self_addr != HG_ADDR_NULL) HG_Addr_free(hgcl, self_addr);
    free(self_addr_str);
    if (g)
    {
        free(g->name);
        free(g->view.member_states);
        free(g);
    }

    return g_id;
}

ssg_group_id_t ssg_group_create_config(
    const char * group_name,
    const char * file_name)
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
    ssg_group_id_t g_id = SSG_GROUP_ID_NULL;

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
    do {
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
    g_id = ssg_group_create(group_name, addr_strs, num_addrs);

fini:
    /* cleanup before returning */
    if (fd != -1) close(fd);
    free(rd_buf);
    free(addr_buf);
    free(addr_strs);

    return g_id;
}

#ifdef HAVE_MPI
ssg_group_id_t ssg_group_create_mpi(
    const char * group_name,
    MPI_Comm comm)
{
    hg_class_t *hgcl = NULL;
    hg_addr_t self_addr = HG_ADDR_NULL;
    char *self_addr_str = NULL;
    hg_size_t self_addr_size = 0;
    int self_addr_size_int = 0; /* for mpi-friendly conversion */
    char *addr_buf = NULL;
    int *sizes = NULL;
    int *sizes_psum = NULL;
    int comm_size = 0, comm_rank = 0;
    const char **addr_strs = NULL;
    hg_return_t hret;
    ssg_group_id_t g_id = SSG_GROUP_ID_NULL;

    hgcl = margo_get_class(ssg_mid);
    if (!hgcl) goto fini;

    /* get my address */
    hret = HG_Addr_self(hgcl, &self_addr);
    if (hret != HG_SUCCESS) goto fini;
    hret = HG_Addr_to_string(hgcl, NULL, &self_addr_size, self_addr);
    if (hret != HG_SUCCESS) goto fini;
    self_addr_str = malloc(self_addr_size);
    if (self_addr_str == NULL) goto fini;
    hret = HG_Addr_to_string(hgcl, self_addr_str, &self_addr_size, self_addr);
    if (hret != HG_SUCCESS) goto fini;
    self_addr_size_int = (int)self_addr_size; /* null char included in call */

    /* gather the buffer sizes */
    MPI_Comm_size(comm, &comm_size);
    MPI_Comm_rank(comm, &comm_rank);
    sizes = malloc(comm_size * sizeof(*sizes));
    if (sizes == NULL) goto fini;
    sizes[comm_rank] = self_addr_size_int;
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
    MPI_Allgatherv(self_addr_str, self_addr_size_int, MPI_BYTE,
            addr_buf, sizes, sizes_psum, MPI_BYTE, comm);

    /* set up address string array for group members */
    addr_strs = ssg_setup_addr_str_list(addr_buf, comm_size);
    if (!addr_strs) goto fini;

    /* invoke the generic group create routine using our list of addrs */
    g_id = ssg_group_create(group_name, addr_strs, comm_size);

fini:
    /* cleanup before returning */
    free(sizes);
    free(sizes_psum);
    free(addr_buf);
    if (hgcl && self_addr != HG_ADDR_NULL) HG_Addr_free(hgcl, self_addr);
    free(self_addr_str);
    free(addr_strs);

    return g_id;
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

    for (i = 0; i < g->view.group_size; i++) {
        if (g->view.member_states[i].addr != HG_ADDR_NULL)
            HG_Addr_free(margo_get_class(ssg_mid), g->view.member_states[i].addr);
    }
    free(g->view.member_states);
    free(g);

    return SSG_SUCCESS;
}

#if 0
/*** SSG group membership view access routines */

int ssg_get_group_rank(const ssg_t s)
{
    return s->view.self_rank;
}

int ssg_get_group_size(const ssg_t s)
{
    return s->view.group_size;
}

hg_addr_t ssg_get_addr(const ssg_t s, int rank)
{
    if (rank >= 0 && rank < s->view.group_size)
        return s->view.member_states[rank].addr;
    else
        return HG_ADDR_NULL;
}
#endif

/***************************
 *** SSG helper routines ***
 ***************************/

static void ssg_lookup_ult(void * arg);
struct lookup_ult_args
{
    ssg_group_t *g;
    int rank;
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
    ults = malloc(g->view.group_size * sizeof(*ults));
    if (ults == NULL) return HG_NOMEM_ERROR;
    args = malloc(g->view.group_size * sizeof(*args));
    if (args == NULL) {
        free(ults);
        return HG_NOMEM_ERROR;
    }
    for (i = 0; i < g->view.group_size; i++)
        ults[i] = ABT_THREAD_NULL;

    for (i = 1; i < g->view.group_size; i++) {
        r = (g->self_rank + i) % g->view.group_size;
        args[r].g = g;
        args[r].rank = r;
        args[r].addr_str = addr_strs[r];
#if 0
        aret = ABT_thread_create(*margo_get_handler_pool(ssg_mid), &ssg_lookup_ult,
                &args[r], ABT_THREAD_ATTR_NULL, &ults[r]);
        if (aret != ABT_SUCCESS) {
            hret = HG_OTHER_ERROR;
            goto fini;
        }
#endif
    }

    /* wait on all */
    for (i = 1; i < g->view.group_size; i++) {
        r = (g->self_rank + i) % g->view.group_size;
#if 1
        aret = ABT_thread_create(*margo_get_handler_pool(ssg_mid), &ssg_lookup_ult,
                &args[r], ABT_THREAD_ATTR_NULL, &ults[r]);
        if (aret != ABT_SUCCESS) {
            hret = HG_OTHER_ERROR;
            goto fini;
        }
#endif
        aret = ABT_thread_join(ults[r]);
        ABT_thread_free(&ults[r]);
        ults[r] = ABT_THREAD_NULL; // in case of cascading failure from join
        if (aret != ABT_SUCCESS) {
            hret = HG_OTHER_ERROR;
            break;
        }
        else if (args[r].out != HG_SUCCESS) {
            fprintf(stderr, "Error: SSG unable to lookup HG address for rank %d"
                "(err=%d)\n", r, args[r].out);
            hret = args[r].out;
            break;
        }
    }

fini:
    /* cleanup */
    for (i = 0; i < g->view.group_size; i++) {
        if (ults[i] != ABT_THREAD_NULL) {
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
        &g->view.member_states[l->rank].addr);
    return;
}

static const char ** ssg_setup_addr_str_list(
    char * buf, int num_addrs)
{
    const char **ret = malloc(num_addrs * sizeof(*ret));
    if (ret == NULL) return NULL;

    ret[0] = buf;
    for (int i = 1; i < num_addrs; i++) {
        const char * a = ret[i-1];
        ret[i] = a + strlen(a) + 1;
    }
    return ret;
}
