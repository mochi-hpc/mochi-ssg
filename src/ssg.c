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
#ifdef SSG_HAVE_MPI
#include "ssg-mpi.h"
#endif
#include "ssg-internal.h"
#ifdef SSG_USE_SWIM_FD
#include "swim-fd/swim-fd.h"
#endif
#include "uthash.h"

/* SSG helper routine prototypes */
static ssg_group_descriptor_t * ssg_generate_group_descriptor(
    const char * name, const char * leader_addr_str);
static const char ** ssg_setup_addr_str_list(
    char * buf, int num_addrs);
static int ssg_group_destroy_internal(
    ssg_group_t *g);

/* XXX: i think we ultimately need per-mid ssg instances rather than 1 global? */
ssg_instance_t *ssg_inst = NULL;

/***************************************************
 *** SSG runtime intialization/shutdown routines ***
 ***************************************************/

int ssg_init(
    margo_instance_id mid)
{
    if (ssg_inst)
        return SSG_FAILURE;

    /* initialize an SSG instance for this margo instance */
    ssg_inst = malloc(sizeof(*ssg_inst));
    if (!ssg_inst)
        return SSG_FAILURE;
    memset(ssg_inst, 0, sizeof(*ssg_inst));
    ssg_inst->mid = mid;

    ssg_register_rpcs();

    return SSG_SUCCESS;
}

int ssg_finalize()
{
    ssg_group_t *g, *tmp;

    if (!ssg_inst)
        return SSG_FAILURE;

    /* destroy all active groups */
    HASH_ITER(hh, ssg_inst->group_table, g, tmp)
    {
        HASH_DELETE(hh, ssg_inst->group_table, g);
        ssg_group_destroy_internal(g);
    }

    free(ssg_inst);
    ssg_inst = NULL;

    return SSG_SUCCESS;
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
    hg_size_t self_addr_str_size = 0;
    const char *self_addr_substr = NULL;
    const char *addr_substr = NULL;
    int i;
    ssg_group_descriptor_t *group_descriptor = NULL;
    ssg_group_t *g = NULL;
    hg_return_t hret;
    ssg_group_id_t group_id = SSG_GROUP_ID_NULL;

    if (!ssg_inst) goto fini;

    hgcl = margo_get_class(ssg_inst->mid);
    if (!hgcl) goto fini;

    /* generate a unique ID for this group  */
    group_descriptor = ssg_generate_group_descriptor(group_name, group_addr_strs[0]);
    if (group_descriptor == NULL) goto fini;

    /* make sure we aren't re-creating an existing group */
    HASH_FIND(hh, ssg_inst->group_table, &group_descriptor->name_hash,
        sizeof(uint64_t), g);
    if (g) goto fini;

    /* allocate an SSG group data structure and initialize some of it */
    g = malloc(sizeof(*g));
    if (!g) goto fini;
    memset(g, 0, sizeof(*g));
    g->group_name = strdup(group_name);
    if (!g->group_name) goto fini;
    g->group_descriptor = group_descriptor;
    // TODO? g->self_id = -1;
    g->group_view.size = group_size;
    g->group_view.member_states = malloc(
        group_size * sizeof(*g->group_view.member_states));
    if (!g->group_view.member_states) goto fini;
    memset(g->group_view.member_states, 0,
        group_size * sizeof(*g->group_view.member_states));

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
#if 0
    /* TODO: if unable to resolve my rank within the group, error out */
    if (g->self_id == -1)
    {
        fprintf(stderr, "Error: SSG unable to resolve rank in group %s\n",
            group_name);
        goto fini;
    }
#endif

    /* lookup hg address information for all group members */
    hret = ssg_group_lookup(g, group_addr_strs);
    if (hret != HG_SUCCESS)
    {
        fprintf(stderr, "Error: SSG unable to complete lookup for group %s\n",
            group_name);
        goto fini;
    }

#ifdef SSG_USE_SWIM_FD
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

    /* add this group reference to our group table */
    HASH_ADD(hh, ssg_inst->group_table, group_descriptor->name_hash,
        sizeof(uint64_t), g);

    /* everything successful -- set the output group identifier, which is just
     * an opaque pointer to the group descriptor structure
     */
    group_id = (ssg_group_id_t)group_descriptor;

    SSG_DEBUG(g, "group create successful (size=%d)\n", group_size);

    /* don't free these pointers on success */
    group_descriptor = NULL;
    self_addr = HG_ADDR_NULL;
    g = NULL;
fini:
    if (hgcl && self_addr != HG_ADDR_NULL) HG_Addr_free(hgcl, self_addr);
    free(self_addr_str);
    free(group_descriptor);
    if (g)
    {
        free(g->group_name);
        free(g->group_view.member_states);
        free(g);
    }

    return group_id;
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
    ssg_group_id_t group_id = SSG_GROUP_ID_NULL;

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
    group_id = ssg_group_create(group_name, addr_strs, num_addrs);

fini:
    /* cleanup before returning */
    if (fd != -1) close(fd);
    free(rd_buf);
    free(addr_buf);
    free(addr_strs);

    return group_id;
}

#ifdef SSG_HAVE_MPI
ssg_group_id_t ssg_group_create_mpi(
    const char * group_name,
    MPI_Comm comm)
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
    ssg_group_id_t group_id = SSG_GROUP_ID_NULL;

    if (!ssg_inst) goto fini;

    hgcl = margo_get_class(ssg_inst->mid);
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
    group_id = ssg_group_create(group_name, addr_strs, comm_size);

fini:
    /* cleanup before returning */
    free(sizes);
    free(sizes_psum);
    free(addr_buf);
    if (hgcl && self_addr != HG_ADDR_NULL) HG_Addr_free(hgcl, self_addr);
    free(self_addr_str);
    free(addr_strs);

    return group_id;
}
#endif

int ssg_group_destroy(
    ssg_group_id_t group_id)
{
    ssg_group_descriptor_t *group_descriptor = (ssg_group_descriptor_t *)group_id;
    ssg_group_t *g;
    int sret;

    if (!ssg_inst || group_id == SSG_GROUP_ID_NULL)
        return SSG_FAILURE;

    /* find the group structure and destroy it */
    HASH_FIND(hh, ssg_inst->group_table, &group_descriptor->name_hash,
        sizeof(uint64_t), g);
    HASH_DELETE(hh, ssg_inst->group_table, g);
    sret = ssg_group_destroy_internal(g);

    return sret;
}

int ssg_group_attach(
    ssg_group_id_t group_id)
{
    ssg_group_descriptor_t *group_descriptor = (ssg_group_descriptor_t *)group_id;
    hg_return_t hret;

    if (!ssg_inst || group_id == SSG_GROUP_ID_NULL)
        return SSG_FAILURE;

    hret = ssg_group_attach_send(group_descriptor);
    if (hret != HG_SUCCESS)
        return SSG_FAILURE;

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

ssg_member_id_t ssg_get_group_self_id(
    ssg_group_id_t group_id)
{
    ssg_group_descriptor_t *group_descriptor = (ssg_group_descriptor_t *)group_id;
    ssg_group_t *g;

    if (!ssg_inst || group_id == SSG_GROUP_ID_NULL)
        return SSG_MEMBER_ID_NULL;

    HASH_FIND(hh, ssg_inst->group_table, &group_descriptor->name_hash,
        sizeof(uint64_t), g);
    if (!g)
        return SSG_MEMBER_ID_NULL;

    return g->self_id;
}

int ssg_get_group_size(
    ssg_group_id_t group_id)
{
    ssg_group_descriptor_t *group_descriptor = (ssg_group_descriptor_t *)group_id;
    ssg_group_t *g;

    if (!ssg_inst || group_id == SSG_GROUP_ID_NULL)
        return 0;

    HASH_FIND(hh, ssg_inst->group_table, &group_descriptor->name_hash,
        sizeof(uint64_t), g);
    if (!g)
        return 0;

    return g->group_view.size;
}

hg_addr_t ssg_get_addr(
    ssg_group_id_t group_id,
    ssg_member_id_t member_id)
{
    ssg_group_descriptor_t *group_descriptor = (ssg_group_descriptor_t *)group_id;
    ssg_group_t *g;

    if (!ssg_inst || group_id == SSG_GROUP_ID_NULL)
        return HG_ADDR_NULL;

    HASH_FIND(hh, ssg_inst->group_table, &group_descriptor->name_hash,
        sizeof(uint64_t), g);
    if (!g)
        return HG_ADDR_NULL;

    return g->group_view.member_states[member_id].addr;
}

/************************************
 *** SSG internal helper routines ***
 ************************************/

static ssg_group_descriptor_t * ssg_generate_group_descriptor(
    const char * name, const char * leader_addr_str)
{
    ssg_group_descriptor_t *group_descriptor;
    uint32_t upper, lower;

    group_descriptor = malloc(sizeof(*group_descriptor));
    if (!group_descriptor) return NULL;

    /* hash the group name to obtain an 64-bit unique ID */
    ssg_hashlittle2(name, strlen(name), &lower, &upper);

    group_descriptor->magic_nr = SSG_MAGIC_NR;
    group_descriptor->name_hash = lower + (((uint64_t)upper)<<32);
    group_descriptor->addr_str = strdup(leader_addr_str);
    if (!group_descriptor->addr_str)
    {
        free(group_descriptor);
        return NULL;
    }

    return group_descriptor;
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

static int ssg_group_destroy_internal(ssg_group_t *g)
{
    unsigned int i;

    /* TODO: send a leave message to the group ? */

#ifdef SSG_USE_SWIM_FD
    /* free up failure detector state */
    if(g->fd_ctx)
        swim_finalize(g->fd_ctx);
#endif

    /* destroy group state */
    for (i = 0; i < g->group_view.size; i++)
    {
        if (g->group_view.member_states[i].addr != HG_ADDR_NULL)
        {
            HG_Addr_free(margo_get_class(ssg_inst->mid),
                g->group_view.member_states[i].addr);
        }
    }
    free(g->group_name);
    free(g->group_view.member_states);
    free(g);

    return SSG_SUCCESS;
}
