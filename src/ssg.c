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

/* arguments for group lookup ULTs */
struct ssg_group_lookup_ult_args
{
    ssg_group_view_t *view;
    ssg_member_state_t *member_state;
    hg_return_t out;
};
static void ssg_group_lookup_ult(void * arg);

/* SSG helper routine prototypes */
static ssg_group_descriptor_t * ssg_group_descriptor_create(
    uint64_t name_hash, const char * leader_addr_str, int owner_status);
static ssg_group_descriptor_t * ssg_group_descriptor_dup(
    ssg_group_descriptor_t * descriptor);
static void ssg_group_descriptor_free(
    ssg_group_descriptor_t * descriptor);
static int ssg_group_view_create(
    const char * const group_addr_strs[], int group_size,
    hg_addr_t self_addr, ssg_member_id_t * self_id,
    ssg_group_view_t * view);
static ssg_member_id_t ssg_gen_member_id(
    const char * addr_str);
static void ssg_group_view_destroy(
    ssg_group_view_t * view);
static void ssg_group_destroy_internal(
    ssg_group_t * g);
static void ssg_attached_group_destroy(
    ssg_attached_group_t * ag);
static const char ** ssg_addr_str_buf_to_list(
    const char * buf, int num_addrs);

/* XXX: i think we ultimately need per-mid ssg instances rather than 1 global? */
ssg_instance_t *ssg_inst = NULL;

/***************************************************
 *** SSG runtime intialization/shutdown routines ***
 ***************************************************/

int ssg_init(
    margo_instance_id mid)
{
    struct timespec ts;

    if (ssg_inst)
        return SSG_FAILURE;

    /* initialize an SSG instance for this margo instance */
    ssg_inst = malloc(sizeof(*ssg_inst));
    if (!ssg_inst)
        return SSG_FAILURE;
    memset(ssg_inst, 0, sizeof(*ssg_inst));
    ssg_inst->mid = mid;

    ssg_register_rpcs();

    /* seed RNG */
    clock_gettime(CLOCK_MONOTONIC, &ts);
    srand(ts.tv_nsec + getpid());

    return SSG_SUCCESS;
}

int ssg_finalize()
{
    ssg_group_t *g, *g_tmp;
    ssg_attached_group_t *ag, *ag_tmp;

    if (!ssg_inst)
        return SSG_FAILURE;

    /* destroy all active groups */
    HASH_ITER(hh, ssg_inst->group_table, g, g_tmp)
    {
        HASH_DELETE(hh, ssg_inst->group_table, g);
        ssg_group_destroy_internal(g);
    }

    /* detach from all attached groups */
    HASH_ITER(hh, ssg_inst->attached_group_table, ag, ag_tmp)
    {
        HASH_DELETE(hh, ssg_inst->attached_group_table, ag);
        ssg_attached_group_destroy(ag);
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
    int group_size,
    ssg_membership_update_cb update_cb,
    void * update_cb_dat)
{
    uint64_t name_hash;
    hg_addr_t self_addr = HG_ADDR_NULL;
    ssg_group_descriptor_t *tmp_descriptor;
    ssg_group_t *g = NULL;
    hg_return_t hret;
    int sret;
    ssg_group_id_t group_id = SSG_GROUP_ID_NULL;

    if (!ssg_inst) return group_id;

    name_hash = ssg_hash64_str(group_name);

    /* generate a unique ID for this group  */
    tmp_descriptor = ssg_group_descriptor_create(name_hash, group_addr_strs[0],
        SSG_OWNER_IS_MEMBER);
    if (tmp_descriptor == NULL) return group_id;

    /* make sure we aren't re-creating an existing group */
    HASH_FIND(hh, ssg_inst->group_table, &tmp_descriptor->name_hash,
        sizeof(uint64_t), g);
    if (g)
    {
        g = NULL;
        goto fini;
    }

    /* allocate an SSG group data structure and initialize some of it */
    g = malloc(sizeof(*g));
    if (!g) goto fini;
    memset(g, 0, sizeof(*g));
    g->name = strdup(group_name);
    if (!g->name) goto fini;
    g->descriptor = tmp_descriptor;
    g->update_cb = update_cb;
    g->update_cb_dat = update_cb_dat;

    /* get my address */
    hret = margo_addr_self(ssg_inst->mid, &self_addr);
    if (hret != HG_SUCCESS) goto fini;

    /* initialize the group view */
    sret = ssg_group_view_create(group_addr_strs, group_size, self_addr,
        &g->self_id, &g->view);
    if (sret != SSG_SUCCESS) goto fini;
    if (g->self_id == SSG_MEMBER_ID_INVALID)
    {
        /* if unable to resolve my rank within the group, error out */
        fprintf(stderr, "Error: SSG unable to resolve rank in group %s\n",
            group_name);
        goto fini;
    }

#ifdef SSG_USE_SWIM_FD
    /* initialize swim failure detector */
    // TODO: we should probably barrier or sync somehow to avoid rpc failures
    // due to timing skew of different ranks initializing swim
    g->fd_ctx = (void *)swim_init(g, 1);
    if (g->fd_ctx == NULL) goto fini;
#endif

    /* everything successful -- set the output group identifier, which is just
     * an opaque pointer to the group descriptor structure
     */
    group_id = (ssg_group_id_t)ssg_group_descriptor_dup(g->descriptor);
    if (group_id == SSG_GROUP_ID_NULL) goto fini;

    /* add this group reference to our group table */
    HASH_ADD(hh, ssg_inst->group_table, descriptor->name_hash,
        sizeof(uint64_t), g);

    SSG_DEBUG(g, "group create successful (size=%d)\n", group_size);

    /* don't free group pointer on success */
    g = NULL;
fini:
    if (self_addr != HG_ADDR_NULL) margo_addr_free(ssg_inst->mid, self_addr);
    if (g)
    {
        ssg_group_view_destroy(&g->view);
        free(g->name);
        free(g);
    }
    if (group_id == SSG_GROUP_ID_NULL)
        ssg_group_descriptor_free(tmp_descriptor);

    return group_id;
}

ssg_group_id_t ssg_group_create_config(
    const char * group_name,
    const char * file_name,
    ssg_membership_update_cb update_cb,
    void * update_cb_dat)
{
    int fd;
    struct stat st;
    char *rd_buf = NULL;
    ssize_t rd_buf_size;
    char *tok;
    void *addr_str_buf = NULL;
    int addr_str_buf_len = 0, num_addrs = 0;
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
    addr_strs = ssg_addr_str_buf_to_list(addr_str_buf, num_addrs);
    if (!addr_strs) goto fini;

    /* invoke the generic group create routine using our list of addrs */
    group_id = ssg_group_create(group_name, addr_strs, num_addrs,
        update_cb, update_cb_dat);

fini:
    /* cleanup before returning */
    if (fd != -1) close(fd);
    free(rd_buf);
    free(addr_str_buf);
    free(addr_strs);

    return group_id;
}

#ifdef SSG_HAVE_MPI
ssg_group_id_t ssg_group_create_mpi(
    const char * group_name,
    MPI_Comm comm,
    ssg_membership_update_cb update_cb,
    void * update_cb_dat)
{
    int i;
    hg_addr_t self_addr = HG_ADDR_NULL;
    char *self_addr_str = NULL;
    hg_size_t self_addr_str_size = 0;
    int self_addr_str_size_int = 0; /* for mpi-friendly conversion */
    char *addr_str_buf = NULL;
    int *sizes = NULL;
    int *sizes_psum = NULL;
    int comm_size = 0, comm_rank = 0;
    const char **addr_strs = NULL;
    hg_return_t hret;
    ssg_group_id_t group_id = SSG_GROUP_ID_NULL;

    if (!ssg_inst) goto fini;

    /* get my address */
    hret = margo_addr_self(ssg_inst->mid, &self_addr);
    if (hret != HG_SUCCESS) goto fini;
    hret = margo_addr_to_string(ssg_inst->mid, NULL, &self_addr_str_size, self_addr);
    if (hret != HG_SUCCESS) goto fini;
    self_addr_str = malloc(self_addr_str_size);
    if (self_addr_str == NULL) goto fini;
    hret = margo_addr_to_string(ssg_inst->mid, self_addr_str, &self_addr_str_size, self_addr);
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
    for (i = 1; i < comm_size+1; i++)
        sizes_psum[i] = sizes_psum[i-1] + sizes[i-1];

    /* allgather the addresses */
    addr_str_buf = malloc(sizes_psum[comm_size]);
    if (addr_str_buf == NULL) goto fini;
    MPI_Allgatherv(self_addr_str, self_addr_str_size_int, MPI_BYTE,
            addr_str_buf, sizes, sizes_psum, MPI_BYTE, comm);

    /* set up address string array for group members */
    addr_strs = ssg_addr_str_buf_to_list(addr_str_buf, comm_size);
    if (!addr_strs) goto fini;

    /* invoke the generic group create routine using our list of addrs */
    group_id = ssg_group_create(group_name, addr_strs, comm_size,
        update_cb, update_cb_dat);

fini:
    /* cleanup before returning */
    if (self_addr != HG_ADDR_NULL) margo_addr_free(ssg_inst->mid, self_addr);
    free(self_addr_str);
    free(sizes);
    free(sizes_psum);
    free(addr_str_buf);
    free(addr_strs);

    return group_id;
}
#endif

int ssg_group_destroy(
    ssg_group_id_t group_id)
{
    ssg_group_descriptor_t *group_descriptor = (ssg_group_descriptor_t *)group_id;
    ssg_group_t *g;

    if (!ssg_inst || group_id == SSG_GROUP_ID_NULL)
        return SSG_FAILURE;

    if (group_descriptor->owner_status != SSG_OWNER_IS_MEMBER)
    {
        fprintf(stderr, "Error: SSG unable to destroy a group it is not a member of\n");
        return SSG_FAILURE;
    }

    /* find the group structure and destroy it */
    HASH_FIND(hh, ssg_inst->group_table, &group_descriptor->name_hash,
        sizeof(uint64_t), g);
    if (!g)
    {
        fprintf(stderr, "Error: SSG unable to find expected group reference\n");
        return SSG_FAILURE;
    }
    HASH_DELETE(hh, ssg_inst->group_table, g);
    ssg_group_destroy_internal(g);

    return SSG_SUCCESS;
}

int ssg_group_attach(
    ssg_group_id_t group_id)
{
    ssg_group_descriptor_t *group_descriptor = (ssg_group_descriptor_t *)group_id;
    ssg_attached_group_t *ag = NULL;
    char *group_name = NULL;
    int group_size;
    void *view_buf = NULL;
    const char **addr_strs = NULL;
    int sret = SSG_FAILURE;

    if (!ssg_inst || group_id == SSG_GROUP_ID_NULL) goto fini;

    if (group_descriptor->owner_status == SSG_OWNER_IS_MEMBER)
    {
        fprintf(stderr, "Error: SSG unable to attach a group it is a member of\n");
        goto fini;
    }
    else if (group_descriptor->owner_status == SSG_OWNER_IS_ATTACHER)
    {
        fprintf(stderr, "Error: SSG unable to attach a group it is" \
            " already attached to\n");
        goto fini;
    }

    /* send the attach request to a group member to initiate a bulk transfer
     * of the group's membership view
     */
    sret = ssg_group_attach_send(group_descriptor, &group_name,
        &group_size, &view_buf);
    if (sret != SSG_SUCCESS || !group_name || !view_buf) goto fini;

    /* set up address string array for group members */
    addr_strs = ssg_addr_str_buf_to_list(view_buf, group_size);
    if (!addr_strs) goto fini;

    /* allocate an SSG attached group data structure and initialize some of it */
    ag = malloc(sizeof(*ag));
    if (!ag) goto fini;
    memset(ag, 0, sizeof(*ag));
    ag->name = group_name;
    ag->descriptor = ssg_group_descriptor_dup(group_descriptor);
    if (!ag->descriptor) goto fini;
    ag->descriptor->owner_status = SSG_OWNER_IS_ATTACHER;

    /* create the view for the group */
    sret = ssg_group_view_create(addr_strs, group_size, HG_ADDR_NULL,
        NULL, &ag->view);
    if (sret != SSG_SUCCESS) goto fini;

    /* add this group reference to our group table */
    HASH_ADD(hh, ssg_inst->attached_group_table, descriptor->name_hash,
        sizeof(uint64_t), ag);

    sret = SSG_SUCCESS;

    /* don't free on success */
    ag = NULL;
fini:
    free(view_buf);
    free(addr_strs);
    if (ag)
    {
        ssg_group_view_destroy(&ag->view);
        free(ag->name);
        free(ag);
        ssg_group_descriptor_free(ag->descriptor);
    }

    return sret;
}

int ssg_group_detach(
    ssg_group_id_t group_id)
{
    ssg_group_descriptor_t *group_descriptor = (ssg_group_descriptor_t *)group_id;
    ssg_attached_group_t *ag;

    if (group_descriptor->owner_status != SSG_OWNER_IS_ATTACHER)
    {
        fprintf(stderr, "Error: SSG unable to detach from group that" \
            " was never attached\n");
        return SSG_FAILURE;
    }

    /* find the attached group structure and destroy it */
    HASH_FIND(hh, ssg_inst->attached_group_table, &group_descriptor->name_hash,
        sizeof(uint64_t), ag);
    if (!ag)
    {
        fprintf(stderr, "Error: SSG unable to find expected attached group reference\n");
        return SSG_FAILURE;
    }
    HASH_DELETE(hh, ssg_inst->attached_group_table, ag);
    ssg_attached_group_destroy(ag);

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
        return SSG_MEMBER_ID_INVALID;

    if (group_descriptor->owner_status != SSG_OWNER_IS_MEMBER)
    {
        fprintf(stderr, "Error: SSG can only obtain a self ID from a group the" \
            " caller is a member of\n");
        return SSG_MEMBER_ID_INVALID;
    }

    HASH_FIND(hh, ssg_inst->group_table, &group_descriptor->name_hash,
        sizeof(uint64_t), g);
    if (!g)
        return SSG_MEMBER_ID_INVALID;

    return g->self_id;
}

int ssg_get_group_size(
    ssg_group_id_t group_id)
{
    ssg_group_descriptor_t *group_descriptor = (ssg_group_descriptor_t *)group_id;
    ssg_group_view_t *group_view = NULL;

    if (!ssg_inst || group_id == SSG_GROUP_ID_NULL)
        return 0;

    if (group_descriptor->owner_status == SSG_OWNER_IS_MEMBER)
    {
        ssg_group_t *g;

        HASH_FIND(hh, ssg_inst->group_table, &group_descriptor->name_hash,
            sizeof(uint64_t), g);
        if (g)
            group_view = &g->view;
    }
    else if (group_descriptor->owner_status == SSG_OWNER_IS_ATTACHER)
    {
        ssg_attached_group_t *ag;

        HASH_FIND(hh, ssg_inst->attached_group_table, &group_descriptor->name_hash,
            sizeof(uint64_t), ag);
        if (ag)
            group_view = &ag->view;
    }
    else
    {
        fprintf(stderr, "Error: SSG can only obtain size of groups that the caller" \
            " is a member of or an attacher of\n");
        return 0;
    }

    if (group_view)
        return group_view->size;
    else
        return 0;
}

hg_addr_t ssg_get_addr(
    ssg_group_id_t group_id,
    ssg_member_id_t member_id)
{
    ssg_group_descriptor_t *group_descriptor = (ssg_group_descriptor_t *)group_id;
    ssg_group_view_t *group_view = NULL;

    if (!ssg_inst || group_id == SSG_GROUP_ID_NULL)
        return HG_ADDR_NULL;

    if (group_descriptor->owner_status == SSG_OWNER_IS_MEMBER)
    {
        ssg_group_t *g;

        HASH_FIND(hh, ssg_inst->group_table, &group_descriptor->name_hash,
            sizeof(uint64_t), g);
        if (g)
            group_view = &g->view;
    }
    else if (group_descriptor->owner_status == SSG_OWNER_IS_ATTACHER)
    {
        ssg_attached_group_t *ag;

        HASH_FIND(hh, ssg_inst->attached_group_table, &group_descriptor->name_hash,
            sizeof(uint64_t), ag);
        if (ag)
            group_view = &ag->view;
    }
    else
    {
        fprintf(stderr, "Error: SSG can only obtain member addresses of groups" \
            " that the caller is a member of or an attacher of\n");
        return HG_ADDR_NULL;
    }

    if (group_view)
    {
        /* XXX for now we assume member ids are dense ranks and error out
         * if they are not within allowable range for the group view size
         */
        if (member_id >= group_view->size)
            return HG_ADDR_NULL;

        return group_view->member_states[member_id].addr;
    }
    else
        return HG_ADDR_NULL;
}

ssg_group_id_t ssg_group_id_dup(
    ssg_group_id_t group_id)
{
    ssg_group_descriptor_t *dup;

    dup = ssg_group_descriptor_dup((ssg_group_descriptor_t *)group_id);
    return (ssg_group_id_t)dup;
}

void ssg_group_id_free(
    ssg_group_id_t group_id)
{
    ssg_group_descriptor_t *descriptor = (ssg_group_descriptor_t *)group_id;

    ssg_group_descriptor_free(descriptor);
    descriptor = SSG_GROUP_ID_NULL;
    return;
}

char *ssg_group_id_get_addr_str(
    ssg_group_id_t group_id)
{
    ssg_group_descriptor_t *group_descriptor = (ssg_group_descriptor_t *)group_id;

    return strdup(group_descriptor->addr_str);
}


void ssg_group_id_serialize(
    ssg_group_id_t group_id,
    char ** buf_p,
    size_t * buf_size_p)
{
    ssg_group_descriptor_t *group_descriptor = (ssg_group_descriptor_t *)group_id;
    size_t alloc_size;
    char *gid_buf, *p; 

    *buf_p = NULL;
    *buf_size_p = 0;

    /* determine needed buffer size */
    alloc_size = (sizeof(group_descriptor->magic_nr) + sizeof(group_descriptor->name_hash) +
        strlen(group_descriptor->addr_str) + 1);

    gid_buf = malloc(alloc_size);
    if (!gid_buf)
        return;

    /* serialize */
    p = gid_buf;
    *(uint64_t *)p = group_descriptor->magic_nr;
    p += sizeof(uint64_t);
    *(uint64_t *)p = group_descriptor->name_hash;
    p += sizeof(uint64_t);
    strcpy(p, group_descriptor->addr_str);
    /* the rest of the descriptor is stateful and not appropriate for serializing... */

    *buf_p = gid_buf;
    *buf_size_p = alloc_size;

    return;
}

void ssg_group_id_deserialize(
    const char * buf,
    size_t buf_size,
    ssg_group_id_t * group_id_p)
{
    size_t min_buf_size;
    uint64_t magic_nr;
    uint64_t name_hash;
    const char *addr_str;
    ssg_group_descriptor_t *group_descriptor;

    *group_id_p = SSG_GROUP_ID_NULL;

    /* check to ensure the buffer contains enough data to make a group ID */
    min_buf_size = (sizeof(group_descriptor->magic_nr) +
        sizeof(group_descriptor->name_hash) + 1);
    if (buf_size < min_buf_size)
    {
        fprintf(stderr, "Error: Serialized buffer does not contain a valid SSG group ID\n");
        return;
    }

    /* deserialize */
    magic_nr = *(uint64_t *)buf;
    if (magic_nr != SSG_MAGIC_NR)
    {
        fprintf(stderr, "Error: Magic number mismatch when deserializing SSG group ID\n");
        return;
    }
    buf += sizeof(uint64_t);
    name_hash = *(uint64_t *)buf;
    buf += sizeof(uint64_t);
    addr_str = buf;

    group_descriptor = ssg_group_descriptor_create(name_hash, addr_str,
        SSG_OWNER_IS_UNASSOCIATED);
    if (!group_descriptor)
        return;

    *group_id_p = (ssg_group_id_t)group_descriptor;

    return;
}

int ssg_group_id_store(
    const char * file_name,
    ssg_group_id_t group_id)
{
    int fd;
    char *buf;
    size_t buf_size;
    ssize_t bytes_written;

    fd = open(file_name, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd < 0)
    {
        fprintf(stderr, "Error: Unable to open file %s for storing SSG group ID\n",
            file_name);
        return SSG_FAILURE;
    }

    ssg_group_id_serialize(group_id, &buf, &buf_size);
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
    ssg_group_id_t * group_id_p)
{
    int fd;
    struct stat fstats;
    char *buf;
    ssize_t bytes_read;
    int ret;

    *group_id_p = SSG_GROUP_ID_NULL;

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

    ssg_group_id_deserialize(buf, (size_t)bytes_read, group_id_p);
    if (*group_id_p == SSG_GROUP_ID_NULL)
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
    ssg_group_descriptor_t *group_descriptor = (ssg_group_descriptor_t *)group_id;
    ssg_group_view_t *group_view = NULL;
    char *group_name = NULL;
    char group_role[32];
    char group_self_id[32];

    if (group_descriptor->owner_status == SSG_OWNER_IS_MEMBER)
    {
        ssg_group_t *g;

        HASH_FIND(hh, ssg_inst->group_table, &group_descriptor->name_hash,
            sizeof(uint64_t), g);
        if (g)
        {
            group_view = &g->view;
            group_name = g->name;
            strcpy(group_role, "member");
            sprintf(group_self_id, "%"PRIu64, g->self_id);
        }
    }
    else if (group_descriptor->owner_status == SSG_OWNER_IS_ATTACHER)
    {
        ssg_attached_group_t *ag;

        HASH_FIND(hh, ssg_inst->attached_group_table, &group_descriptor->name_hash,
            sizeof(uint64_t), ag);
        if (ag)
        {
            group_view = &ag->view;
            group_name = ag->name;
            strcpy(group_role, "attacher");
        }
    }
    else
    {
        fprintf(stderr, "Error: SSG can only dump membership information for" \
            " groups that the caller is a member of or an attacher of\n");
        return;
    }

    if (group_view)
    {
        unsigned int i;

        printf("SSG membership information for group '%s':\n", group_name);
        printf("\trole: '%s'\n", group_role);
        if (strcmp(group_role, "member") == 0)
            printf("\tself_id: %s\n", group_self_id);
        printf("\tsize: %d\n", group_view->size);
        printf("\tview:\n");
        for (i = 0; i < group_view->size; i++)
        {
            if (group_view->member_states[i].is_member)
                printf("\t\tid: %d\taddr: %s\n", i,
                    group_view->member_states[i].addr_str);
        }
    }
    else
        fprintf(stderr, "Error: SSG unable to find group view associated" \
            " with the given group ID\n");

    return;
}

/************************************
 *** SSG internal helper routines ***
 ************************************/

void ssg_apply_membership_update(
    ssg_group_t *g,
    ssg_membership_update_t update)
{
    if(!ssg_inst || !g) return;

    if (update.type == SSG_MEMBER_REMOVE)
    {
        margo_addr_free(ssg_inst->mid, g->view.member_states[update.member].addr);
        free(g->view.member_states[update.member].addr_str);
        g->view.member_states[update.member].addr_str = NULL;
        g->view.member_states[update.member].is_member = 0;
        /* XXX: need to update size ... g->view.size--; */
    }
    else
    {
        assert(0); /* XXX: dynamic group joins aren't possible yet */
    }

    /* execute user-supplied membership update callback, if given */
    if (g->update_cb)
        g->update_cb(update, g->update_cb_dat);

    return;
}

static ssg_group_descriptor_t * ssg_group_descriptor_create(
    uint64_t name_hash, const char * leader_addr_str, int owner_status)
{
    ssg_group_descriptor_t *descriptor;

    descriptor = malloc(sizeof(*descriptor));
    if (!descriptor) return NULL;

    /* hash the group name to obtain an 64-bit unique ID */
    descriptor->magic_nr = SSG_MAGIC_NR;
    descriptor->name_hash = name_hash;
    descriptor->addr_str = strdup(leader_addr_str);
    if (!descriptor->addr_str)
    {
        free(descriptor);
        return NULL;
    }
    descriptor->owner_status = owner_status;
    descriptor->ref_count = 1;
    return descriptor;
}

static ssg_group_descriptor_t * ssg_group_descriptor_dup(
    ssg_group_descriptor_t * descriptor)
{
    descriptor->ref_count++;
    return descriptor;
}

static void ssg_group_descriptor_free(
    ssg_group_descriptor_t * descriptor)
{
    if (descriptor)
    {
        if(descriptor->ref_count == 1)
        {
            free(descriptor->addr_str);
            free(descriptor);
        }
        else
        {
            descriptor->ref_count--;
        }
    }
    return;
}

static int ssg_group_view_create(
    const char * const group_addr_strs[], int group_size,
    hg_addr_t self_addr, ssg_member_id_t * self_id,
    ssg_group_view_t * view)
{
    int i, j, r;
    ABT_thread *lookup_ults = NULL;
    struct ssg_group_lookup_ult_args *lookup_ult_args = NULL;
    char *self_addr_str = NULL;
    hg_size_t self_addr_str_size = 0;
    const char *self_addr_substr = NULL;
    const char *addr_substr = NULL;
    ssg_member_state_t *tmp_ms;
    hg_return_t hret;
    int aret;
    int sret = SSG_FAILURE;

    if (self_id)
        *self_id = SSG_MEMBER_ID_INVALID;
        
    if ((self_id != NULL && self_addr == HG_ADDR_NULL) || !view) goto fini;

    /* allocate lookup ULTs */
    lookup_ults = malloc(group_size * sizeof(*lookup_ults));
    if (lookup_ults == NULL) goto fini;
    for (i = 0; i < group_size; i++) lookup_ults[i] = ABT_THREAD_NULL;
    lookup_ult_args = malloc(group_size * sizeof(*lookup_ult_args));
    if (lookup_ult_args == NULL) goto fini;

    if(self_addr)
    {
        hret = margo_addr_to_string(ssg_inst->mid, NULL, &self_addr_str_size, self_addr);
        if (hret != HG_SUCCESS) goto fini;
        self_addr_str = malloc(self_addr_str_size);
        if (self_addr_str == NULL) goto fini;
        hret = margo_addr_to_string(ssg_inst->mid, self_addr_str, &self_addr_str_size, self_addr);
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
    }

    /* construct view using ULTs to lookup the address of each group member */
    view->size = group_size;
    r = rand() % view->size;
    for (i = 0; i < group_size; i++)
    {
        /* randomize our starting index so all group members aren't looking
         * up other group members in the same order
         */
        j = (r + i) % view->size;

        if (group_addr_strs[j] == NULL || strlen(group_addr_strs[j]) == 0) continue;

        tmp_ms = malloc(sizeof(*tmp_ms));
        if (!tmp_ms) goto fini;

        /* generate a unique member ID for this address */
        tmp_ms->id = ssg_gen_member_id(group_addr_strs[j]);
        tmp_ms->addr_str = strdup(group_addr_strs[j]);
        if (!tmp_ms->addr_str)
        {
            free(tmp_ms);
            goto fini;
        }
        
        /* resolve self id in group if caller asked for it */
        if (self_addr_substr)
        {
            addr_substr = strstr(group_addr_strs[j], "://");
            if (addr_substr == NULL)
                addr_substr = group_addr_strs[j];
            else
                addr_substr += 3;

            if (strcmp(self_addr_substr, addr_substr) == 0)
            {
                hret = margo_addr_dup(ssg_inst->mid, self_addr, &tmp_ms->addr);
                if (hret != HG_SUCCESS)
                {
                    free(tmp_ms->addr_str);
                    free(tmp_ms);
                    goto fini;
                }

                if (self_id)               
                    *self_id = tmp_ms->id;

                /* add self state to membership view */
                LL_PREPEND(view->member_list, tmp_ms);
                HASH_ADD(hh, view->member_map, id, sizeof(ssg_member_id_t),

                /* don't look up our own address, we already know it */
                continue;
            }
        }

        /* XXX limit outstanding lookups to some max */
        lookup_ult_args[j].view = view;
        lookup_ult_args[j].member_state = tmp_ms;
        ABT_pool pool;
        margo_get_handler_pool(ssg_inst->mid, &pool);
        aret = ABT_thread_create(pool, &ssg_group_lookup_ult,
            &lookup_ult_args[j], ABT_THREAD_ATTR_NULL,
            &lookup_ults[j]);
        if (aret != ABT_SUCCESS)
        {
            free(tmp_ms->addr_str);
            free(tmp_ms);
            goto fini;
        }
    }

    /* wait on all lookup ULTs to terminate */
    for (i = 0; i < group_size; i++)
    {
        if (lookup_ults[i] == ABT_THREAD_NULL) continue;

        aret = ABT_thread_join(lookup_ults[i]);
        ABT_thread_free(&lookup_ults[i]);
        lookup_ults[i] = ABT_THREAD_NULL;
        if (aret != ABT_SUCCESS) goto fini;
        else if (lookup_ult_args[i].out != HG_SUCCESS)
        {
            fprintf(stderr, "Error: SSG unable to lookup HG address for member %d"
                "(err=%d)\n", lookup_ult_args[i].member_state->id,
                lookup_ult_args[i].out);
            goto fini;
        }
    }

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
        ssg_group_view_destroy(view);
    }
    free(lookup_ults);
    free(lookup_ult_args);
    free(self_addr_str);

    return sret;
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


static void ssg_group_lookup_ult(
    void * arg)
{
    struct ssg_group_lookup_ult_args *l = arg;

    /* XXX: should be a timeout here? */
    l->out = margo_addr_lookup(ssg_inst->mid, l->member_state->addr_str,
        &l->member_state->addr);
    if (l->out == HG_SUCCESS)
    {
        LL_APPEND(l->view->member_list, l->member_state);
        HASH_ADD(hh, l->view->member_map, id, sizeof(ssg_member_id_t),
            l->member_state);
    }

    return;
}

static void ssg_group_view_destroy(
    ssg_group_view_t * view)
{
    ssg_member_state_t *state, *tmp;

    /* destroy state for all group members */
    LL_FOREACH_SAFE(view->member_list, state, tmp)
    {
        LL_DELETE(view->member_list, state);
        HASH_DEL(view->member_map, state);
        free(state->addr_str);
        margo_addr_free(ssg_inst->mid,  state->addr);
        free(state);
    }
    view->member_list = NULL;
    view->member_map = NULL;

    return;
}

static void ssg_group_destroy_internal(
    ssg_group_t * g)
{
    /* TODO: send a leave message to the group ? */

#ifdef SSG_USE_SWIM_FD
    /* free up failure detector state */
    if(g->fd_ctx)
        swim_finalize(g->fd_ctx);
#endif

    /* destroy group state */
    ssg_group_view_destroy(&g->view);
    g->descriptor->owner_status = SSG_OWNER_IS_UNASSOCIATED;
    ssg_group_descriptor_free(g->descriptor);
    free(g->name);
    free(g);

    return;
}

static void ssg_attached_group_destroy(
    ssg_attached_group_t * ag)
{
    ssg_group_view_destroy(&ag->view);
    ag->descriptor->owner_status = SSG_OWNER_IS_UNASSOCIATED;
    ssg_group_descriptor_free(ag->descriptor);
    free(ag->name);
    free(ag);
    return;
}

static const char ** ssg_addr_str_buf_to_list(
    const char * buf, int num_addrs)
{
    int i;
    const char **ret = malloc(num_addrs * sizeof(*ret));
    if (ret == NULL) return NULL;

    ret[0] = buf;
    for (i = 1; i < num_addrs; i++)
    {
        const char * a = ret[i-1];
        ret[i] = a + strlen(a) + 1;
    }
    return ret;
}
