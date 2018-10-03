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

#include <mercury.h>
#include <abt.h>
#include <margo.h>

#include "ssg.h"
#ifdef SSG_HAVE_MPI
#include "ssg-mpi.h"
#endif
#include "ssg-internal.h"

/* arguments for group lookup ULTs */
struct ssg_group_lookup_ult_args
{
    const char *addr_str;
    ssg_group_view_t *view;
    ABT_rwlock lock;
    int out;
};
static void ssg_group_lookup_ult(void * arg);

/* SSG helper routine prototypes */
static ssg_group_t * ssg_group_create_internal(
    const char * group_name, const char * const group_addr_strs[],
    int group_size, ssg_membership_update_cb update_cb, void *update_cb_dat);
static int ssg_group_view_create(
    const char * const group_addr_strs[], int group_size,
    const char * self_addr_str, ABT_rwlock view_lock,
    ssg_group_view_t * view, ssg_member_id_t * self_id);
static ssg_member_state_t * ssg_group_view_add_member(
    const char * addr_str, ssg_group_view_t * view, ABT_rwlock lock);
static ssg_group_descriptor_t * ssg_group_descriptor_create(
    uint64_t name_hash, const char * leader_addr_str, int owner_status);
static ssg_group_descriptor_t * ssg_group_descriptor_dup(
    ssg_group_descriptor_t * descriptor);
static void ssg_group_destroy_internal(
    ssg_group_t * g);
static void ssg_attached_group_destroy(
    ssg_attached_group_t * ag);
static void ssg_group_view_destroy(
    ssg_group_view_t * view);
static void ssg_group_descriptor_free(
    ssg_group_descriptor_t * descriptor);
static ssg_member_id_t ssg_gen_member_id(
    const char * addr_str);
static const char ** ssg_addr_str_buf_to_list(
    const char * buf, int num_addrs);
static void ssg_shuffle_member_list(
    ssg_group_target_list_t *list);

/* SWIM group management routine prototypes */
static int ssg_get_swim_dping_target(
    void *group_data,
    swim_member_id_t *target_id,
    swim_member_inc_nr_t *target_inc_nr,
    hg_addr_t *target_addr);
static int ssg_get_swim_iping_targets(
    void *group_data,
    swim_member_id_t dping_target_id,
    int *num_targets,
    swim_member_id_t *target_ids,
    hg_addr_t *target_addrs);
static void ssg_get_swim_member_addr(
    void *group_data,
    swim_member_id_t id,
    hg_addr_t *target_addr);
static void ssg_get_swim_member_state(
    void *group_data,
    swim_member_id_t id,
    swim_member_state_t **state);
static void ssg_apply_swim_member_update(
    void *group_data,
    swim_member_update_t update);

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
    ssg_group_t *g;
    ssg_group_id_t g_id = SSG_GROUP_ID_NULL;

    g = ssg_group_create_internal(group_name, group_addr_strs,
            group_size, update_cb, update_cb_dat);
    if (g)
    {
        /* on successful creation, dup the group descriptor and return
         * it for the caller to hold on to
         */
        g_id = (ssg_group_id_t)ssg_group_descriptor_dup(g->descriptor);
        if (g_id == SSG_GROUP_ID_NULL)
            ssg_group_destroy_internal(g);
    }

    return g_id;
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
    char *self_addr_str = NULL;
    int self_addr_str_size = 0;
    char *addr_str_buf = NULL;
    int *sizes = NULL;
    int *sizes_psum = NULL;
    int comm_size = 0, comm_rank = 0;
    const char **addr_strs = NULL;
    ssg_group_id_t group_id = SSG_GROUP_ID_NULL;

    if (!ssg_inst) goto fini;

    /* get my address */
    SSG_GET_SELF_ADDR_STR(ssg_inst->mid, self_addr_str);
    if (self_addr_str == NULL) goto fini;
    self_addr_str_size = (int)strlen(self_addr_str) + 1;

    /* gather the buffer sizes */
    MPI_Comm_size(comm, &comm_size);
    MPI_Comm_rank(comm, &comm_rank);
    sizes = malloc(comm_size * sizeof(*sizes));
    if (sizes == NULL) goto fini;
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
    MPI_Allgatherv(self_addr_str, self_addr_str_size, MPI_BYTE,
            addr_str_buf, sizes, sizes_psum, MPI_BYTE, comm);

    /* set up address string array for group members */
    addr_strs = ssg_addr_str_buf_to_list(addr_str_buf, comm_size);
    if (!addr_strs) goto fini;

    /* invoke the generic group create routine using our list of addrs */
    group_id = ssg_group_create(group_name, addr_strs, comm_size,
        update_cb, update_cb_dat);

fini:
    /* cleanup before returning */
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

    if (!ssg_inst || group_id == SSG_GROUP_ID_NULL) return SSG_FAILURE;

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
    ssg_group_destroy_internal(g);

    return SSG_SUCCESS;
}

ssg_group_id_t ssg_group_join(
    ssg_group_id_t in_group_id,
    ssg_membership_update_cb update_cb,
    void * update_cb_dat)
{
    ssg_group_descriptor_t *in_group_descriptor = (ssg_group_descriptor_t *)in_group_id;
    char *self_addr_str = NULL;
    char *group_name = NULL;
    int group_size;
    void *view_buf = NULL;
    const char **addr_strs = NULL;
    int sret;
    ssg_group_t *g = NULL;
    ssg_group_id_t g_id = SSG_GROUP_ID_NULL;

    if (!ssg_inst || in_group_id == SSG_GROUP_ID_NULL) goto fini;

    if (in_group_descriptor->owner_status == SSG_OWNER_IS_MEMBER)
    {
        fprintf(stderr, "Error: SSG unable to join a group it is already a member of\n");
        goto fini;
    }
    else if (in_group_descriptor->owner_status == SSG_OWNER_IS_ATTACHER)
    {
        fprintf(stderr, "Error: SSG unable to join a group it is attached to\n");
        goto fini;
    }

    /* get my address string */
    SSG_GET_SELF_ADDR_STR(ssg_inst->mid, self_addr_str);
    if (self_addr_str == NULL) goto fini;

    sret = ssg_group_join_send(in_group_descriptor, &group_name,
        &group_size, &view_buf);
    if (sret != SSG_SUCCESS || !group_name || !view_buf) goto fini;

    /* set up address string array for all group members */
    addr_strs = ssg_addr_str_buf_to_list(view_buf, group_size);
    if (!addr_strs) goto fini;

    /* append self address string to list of group member address strings */
    addr_strs = realloc(addr_strs, (group_size+1)*sizeof(char *));
    if(!addr_strs) goto fini;
    addr_strs[group_size++] = self_addr_str;

    g = ssg_group_create_internal(group_name, addr_strs, group_size,
            update_cb, update_cb_dat);
    if (g)
    {
        /* on successful creation, dup the group descriptor and return
         * it for the caller to hold on to
         */
        g_id = (ssg_group_id_t)ssg_group_descriptor_dup(g->descriptor);
        if (g_id == SSG_GROUP_ID_NULL) goto fini;
    }

    /* don't free on success */
    group_name = NULL;
    g = NULL;
fini:
    if (g) ssg_group_destroy_internal(g);
    free(addr_strs);
    free(view_buf);
    free(group_name);
    free(self_addr_str);

    return g_id;
}

int ssg_group_leave(
    ssg_group_id_t group_id)
{
    ssg_group_descriptor_t *group_descriptor = (ssg_group_descriptor_t *)group_id;

    if (!ssg_inst || group_id == SSG_GROUP_ID_NULL) return SSG_FAILURE;

    if (group_descriptor->owner_status != SSG_OWNER_IS_MEMBER)
    {
        fprintf(stderr, "Error: SSG unable to leave group it is not a member of\n");
        return SSG_FAILURE;
    }

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

    /* set up address string array for all group members */
    addr_strs = ssg_addr_str_buf_to_list(view_buf, group_size);
    if (!addr_strs) goto fini;

    /* allocate an SSG attached group data structure and initialize some of it */
    ag = malloc(sizeof(*ag));
    if (!ag) goto fini;
    memset(ag, 0, sizeof(*ag));
    ag->name = strdup(group_name);
    ag->descriptor = ssg_group_descriptor_dup(group_descriptor);
    if (!ag->descriptor) goto fini;
    ag->descriptor->owner_status = SSG_OWNER_IS_ATTACHER;

    /* create the view for the group */
    sret = ssg_group_view_create(addr_strs, group_size, NULL, ag->lock, &ag->view, NULL);
    if (sret != SSG_SUCCESS) goto fini;

    /* add this group reference to our group table */
    HASH_ADD(hh, ssg_inst->attached_group_table, descriptor->name_hash,
        sizeof(uint64_t), ag);

    sret = SSG_SUCCESS;

    /* don't free on success */
    group_name = NULL;
    ag = NULL;
fini:
    if (ag) ssg_attached_group_destroy(ag);
    free(addr_strs);
    free(view_buf);
    free(group_name);

    return sret;
}

int ssg_group_detach(
    ssg_group_id_t group_id)
{
    ssg_group_descriptor_t *group_descriptor = (ssg_group_descriptor_t *)group_id;
    ssg_attached_group_t *ag;

    if (!ssg_inst || group_id == SSG_GROUP_ID_NULL) return SSG_FAILURE;

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
        fprintf(stderr, "Error: SSG unable to find expected group attached\n");
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

    if (!ssg_inst || group_id == SSG_GROUP_ID_NULL) return SSG_MEMBER_ID_INVALID;

    if (group_descriptor->owner_status != SSG_OWNER_IS_MEMBER)
    {
        fprintf(stderr, "Error: SSG can only obtain a self ID from a group the" \
            " caller is a member of\n");
        return SSG_MEMBER_ID_INVALID;
    }

    HASH_FIND(hh, ssg_inst->group_table, &group_descriptor->name_hash,
        sizeof(uint64_t), g);
    if (!g) return SSG_MEMBER_ID_INVALID;

    return g->self_id;
}

int ssg_get_group_size(
    ssg_group_id_t group_id)
{
    ssg_group_descriptor_t *group_descriptor = (ssg_group_descriptor_t *)group_id;
    int group_size = 0;

    if (!ssg_inst || group_id == SSG_GROUP_ID_NULL) return 0;

    if (group_descriptor->owner_status == SSG_OWNER_IS_MEMBER)
    {
        ssg_group_t *g;

        HASH_FIND(hh, ssg_inst->group_table, &group_descriptor->name_hash,
            sizeof(uint64_t), g);
        if (g)
        {
            ABT_rwlock_rdlock(g->lock);
            group_size = g->view.size + 1; /* add ourself to view size */
            ABT_rwlock_unlock(g->lock);
        }
    }
    else if (group_descriptor->owner_status == SSG_OWNER_IS_ATTACHER)
    {
        ssg_attached_group_t *ag;

        HASH_FIND(hh, ssg_inst->attached_group_table, &group_descriptor->name_hash,
            sizeof(uint64_t), ag);
        if (ag)
        {
            ABT_rwlock_rdlock(ag->lock);
            group_size = ag->view.size;
            ABT_rwlock_unlock(ag->lock);
        }
    }
    else
    {
        fprintf(stderr, "Error: SSG can only obtain size of groups that the caller" \
            " is a member of or an attacher of\n");
        return 0;
    }

    return group_size;
}

hg_addr_t ssg_get_addr(
    ssg_group_id_t group_id,
    ssg_member_id_t member_id)
{
    ssg_group_descriptor_t *group_descriptor = (ssg_group_descriptor_t *)group_id;
    ssg_member_state_t *member_state;
    hg_addr_t member_addr = HG_ADDR_NULL;

    if (!ssg_inst || group_id == SSG_GROUP_ID_NULL ||
            member_id == SSG_MEMBER_ID_INVALID)
        return HG_ADDR_NULL;

    if (group_descriptor->owner_status == SSG_OWNER_IS_MEMBER)
    {
        ssg_group_t *g;

        HASH_FIND(hh, ssg_inst->group_table, &group_descriptor->name_hash,
            sizeof(uint64_t), g);
        if (g)
        {
            ABT_rwlock_rdlock(g->lock);
            HASH_FIND(hh, g->view.member_map, &member_id, sizeof(ssg_member_id_t),
                member_state);
            if (member_state) 
                member_addr = member_state->addr;
            ABT_rwlock_unlock(g->lock);
        }
    }
    else if (group_descriptor->owner_status == SSG_OWNER_IS_ATTACHER)
    {
        ssg_attached_group_t *ag;

        HASH_FIND(hh, ssg_inst->attached_group_table, &group_descriptor->name_hash,
            sizeof(uint64_t), ag);
        if (ag)
        {
            ABT_rwlock_rdlock(ag->lock);
            HASH_FIND(hh, ag->view.member_map, &member_id, sizeof(ssg_member_id_t),
                member_state);
            if (member_state) 
                member_addr = member_state->addr;
            ABT_rwlock_unlock(ag->lock);
        }
    }
    else
    {
        fprintf(stderr, "Error: SSG can only obtain member addresses of groups" \
            " that the caller is a member of or an attacher of\n");
        return HG_ADDR_NULL;
    }

    return member_addr;
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
            sprintf(group_self_id, "%lu", g->self_id);
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
        ssg_member_state_t *member_state, *tmp_ms;

        printf("SSG membership information for group '%s':\n", group_name);
        printf("\trole: '%s'\n", group_role);
        if (strcmp(group_role, "member") == 0)
            printf("\tself_id: %s\n", group_self_id);
        printf("\tsize: %d\n", group_view->size+1);
        printf("\tview:\n");
        HASH_ITER(hh, group_view->member_map, member_state, tmp_ms)
        {
            printf("\t\tid: %20lu\taddr: %s\n", member_state->id,
                member_state->addr_str);
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

static ssg_group_t * ssg_group_create_internal(
    const char * group_name, const char * const group_addr_strs[],
    int group_size, ssg_membership_update_cb update_cb, void *update_cb_dat)
{
    uint64_t name_hash;
    char *self_addr_str = NULL;
    ssg_member_state_t *ms, *tmp_ms;
    unsigned int i = 0;
    int sret;
    int success = 0;
    ssg_group_t *g = NULL;

    if (!ssg_inst) return NULL;

    name_hash = ssg_hash64_str(group_name);

    /* make sure we aren't re-creating an existing group */
    HASH_FIND(hh, ssg_inst->group_table, &name_hash, sizeof(uint64_t), g);
    if (g) return NULL;

    /* get my address string */
    SSG_GET_SELF_ADDR_STR(ssg_inst->mid, self_addr_str);
    if (self_addr_str == NULL) goto fini;

    /* allocate an SSG group data structure and initialize some of it */
    g = malloc(sizeof(*g));
    if (!g) goto fini;
    memset(g, 0, sizeof(*g));
    g->name = strdup(group_name);
    if (!g->name) goto fini;
    g->update_cb = update_cb;
    g->update_cb_dat = update_cb_dat;
    ABT_rwlock_create(&g->lock);

    /* generate unique descriptor for this group  */
    g->descriptor = ssg_group_descriptor_create(name_hash, self_addr_str,
        SSG_OWNER_IS_MEMBER);
    if (g->descriptor == NULL) goto fini;

    /* initialize the group view */
    sret = ssg_group_view_create(group_addr_strs, group_size, self_addr_str,
        g->lock, &g->view, &g->self_id);
    if (sret != SSG_SUCCESS) goto fini;
    if (g->self_id == SSG_MEMBER_ID_INVALID)
    {
        /* if unable to resolve my rank within the group, error out */
        fprintf(stderr, "Error: SSG unable to resolve rank in group %s\n",
            group_name);
        goto fini;
    }

    /* create a list of all target member states and shuffle it */
    g->target_list.targets = malloc(g->view.size * sizeof(*g->target_list.targets));
    if (g->target_list.targets == NULL) goto fini;
    g->target_list.nslots = g->target_list.len = g->view.size;
    g->target_list.dping_ndx = 0;
    HASH_ITER(hh, g->view.member_map, ms, tmp_ms)
    {
        g->target_list.targets[i] = ms;
        i++;
    }

    /* initialize swim failure detector */
    // TODO: we should probably barrier or sync somehow to avoid rpc failures
    // due to timing skew of different ranks initializing swim
    swim_group_mgmt_callbacks_t swim_callbacks = {
        .get_dping_target = &ssg_get_swim_dping_target,
        .get_iping_targets = &ssg_get_swim_iping_targets,
        .get_member_addr = ssg_get_swim_member_addr,
        .get_member_state = ssg_get_swim_member_state,
        .apply_member_update = ssg_apply_swim_member_update,
        .apply_user_updates = ssg_apply_swim_user_updates,
    };
    g->swim_ctx = swim_init(ssg_inst->mid, g, (swim_member_id_t)g->self_id,
        swim_callbacks, 1);
    if (g->swim_ctx == NULL) goto fini;

    /* add this group reference to our group table */
    HASH_ADD(hh, ssg_inst->group_table, descriptor->name_hash,
        sizeof(uint64_t), g);

#ifdef DEBUG
    /* set debug output pointer */
    char *dbg_log_dir = getenv("SSG_DEBUG_LOGDIR");
    if (dbg_log_dir)
    {
        char dbg_log_path[PATH_MAX];
        snprintf(dbg_log_path, PATH_MAX, "%s/ssg-%s-%lu.log",
            dbg_log_dir, g->name, g->self_id);
        g->dbg_log = fopen(dbg_log_path, "a");
        if (!g->dbg_log) goto fini;
    }
    else
    {
        g->dbg_log = stdout;
    }
#endif

    SSG_DEBUG(g, "group create successful (size=%d)\n", group_size);
    success = 1;
fini:
    if (!success && g)
    {
        if (g->descriptor) ssg_group_descriptor_free(g->descriptor);
        ssg_group_view_destroy(&g->view);
        free(g->target_list.targets);
        free(g->name);
        free(g);
        g = NULL;
    }
    free(self_addr_str);

    return g;
}

int ssg_group_add_member(
    ssg_group_t *g, const char * addr_str)
{
    ssg_member_state_t *new_ms;

    /* group view add member */
    new_ms = ssg_group_view_add_member(addr_str, &g->view, g->lock);
    if (new_ms == NULL) return SSG_FAILURE;

    ABT_rwlock_wrlock(g->lock);

    /* add to target list */
    if (g->target_list.len == g->target_list.nslots)
    {
        /* realloc target list, use fixed incr for now */
        /* XXX constants bad... */
        g->target_list.targets = realloc(g->target_list.targets, 
            (g->target_list.len + 10) * sizeof(*g->target_list.targets));
        if (!g->target_list.targets) return SSG_FAILURE;
        g->target_list.nslots += 10;
    }
    g->target_list.targets[g->target_list.len++] = new_ms;

    SSG_DEBUG(g, "successfully added joining member %lu\n", new_ms->id);

    ABT_rwlock_unlock(g->lock);

    return SSG_SUCCESS;
}

static int ssg_group_view_create(
    const char * const group_addr_strs[], int group_size,
    const char * self_addr_str, ABT_rwlock view_lock,
    ssg_group_view_t * view, ssg_member_id_t * self_id)
{
    int i, j, r;
    ABT_thread *lookup_ults = NULL;
    struct ssg_group_lookup_ult_args *lookup_ult_args = NULL;
    const char *self_addr_substr = NULL;
    const char *addr_substr = NULL;
    int aret;
    int sret = SSG_FAILURE;

    if (self_id)
        *self_id = SSG_MEMBER_ID_INVALID;
        
    if ((self_id != NULL && self_addr_str == NULL) || !view) goto fini;

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
                if (self_id)               
                    *self_id = ssg_gen_member_id(group_addr_strs[j]);

                /* don't look up our own address, we already know it */
                continue;
            }
        }

        /* XXX limit outstanding lookups to some max */
        lookup_ult_args[j].addr_str = group_addr_strs[j];
        lookup_ult_args[j].view = view;
        lookup_ult_args[j].lock = view_lock;
        ABT_pool pool;
        margo_get_handler_pool(ssg_inst->mid, &pool);
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

    return sret;
}

static void ssg_group_lookup_ult(
    void * arg)
{
    struct ssg_group_lookup_ult_args *l = arg;

    if (ssg_group_view_add_member(l->addr_str, l->view, l->lock) != NULL)
        l->out = SSG_SUCCESS;
    else
        l->out = SSG_FAILURE;

    return;
}

static ssg_member_state_t * ssg_group_view_add_member(
    const char * addr_str,
    ssg_group_view_t * view,
    ABT_rwlock lock)
{
    ssg_member_state_t *ms;
    hg_return_t hret;

    ms = calloc(1, sizeof(*ms));
    if (!ms) return NULL;
    ms->addr_str = strdup(addr_str);
    if (!ms->addr_str)
    {
        free(ms);
        return NULL;
    }
    ms->id = ssg_gen_member_id(addr_str);
    SWIM_MEMBER_STATE_INIT(ms->swim_state);

    hret = margo_addr_lookup(ssg_inst->mid, addr_str, &ms->addr);
    if (hret != HG_SUCCESS)
    {
        free(ms->addr_str);
        free(ms);
        return NULL;
    }

    ABT_rwlock_wrlock(lock);
    HASH_ADD(hh, view->member_map, id, sizeof(ssg_member_id_t), ms);
    view->size++;
    ABT_rwlock_unlock(lock);

    return ms;
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

static void ssg_group_destroy_internal(
    ssg_group_t * g)
{
    /* free up SWIM state */
    if(g->swim_ctx)
        swim_finalize(g->swim_ctx);

    /* XXX LOCK */

#ifdef DEBUG
    fflush(g->dbg_log);

    char *dbg_log_dir = getenv("SSG_DEBUG_LOGDIR");
    if (dbg_log_dir)
        fclose(g->dbg_log);
#endif

    /* destroy group state */
    HASH_DELETE(hh, ssg_inst->group_table, g);
    ssg_group_view_destroy(&g->view);
    g->descriptor->owner_status = SSG_OWNER_IS_UNASSOCIATED;
    ssg_group_descriptor_free(g->descriptor);
    ABT_rwlock_free(&g->lock);
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

static void ssg_group_view_destroy(
    ssg_group_view_t * view)
{
    ssg_member_state_t *state, *tmp;

    /* destroy state for all group members */
    HASH_ITER(hh, view->member_map, state, tmp)
    {
        HASH_DEL(view->member_map, state);
        free(state->addr_str);
        margo_addr_free(ssg_inst->mid, state->addr);
        free(state);
    }
    view->member_map = NULL;

    return;
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

static void ssg_shuffle_member_list(
    ssg_group_target_list_t *list)
{
    unsigned int i, r;
    ssg_member_state_t *tmp_ms;

    /* filter out dead members */
    for (i = 0; i < list->len; i++)
    {
        if (list->targets[i]->swim_state.status == SWIM_MEMBER_DEAD)
        {
            list->len--;
            memcpy(&list->targets[i], &list->targets[i+1],
                (list->len-i)*sizeof(*list->targets));
        }
    }

    if (list->len <= 1) return;

    /* run fisher-yates shuffle over list of target members */
    for (i = list->len - 1; i > 0; i--)
    {
        r = rand() % (i + 1);
        tmp_ms = list->targets[r];
        list->targets[r] = list->targets[i];
        list->targets[i] = tmp_ms;
    }

    return;
}

/**************************************
 *** SWIM group management routines ***
 **************************************/

static int ssg_get_swim_dping_target(
    void *group_data,
    swim_member_id_t *target_id,
    swim_member_inc_nr_t *target_inc_nr,
    hg_addr_t *target_addr)
{
    ssg_group_t *g = (ssg_group_t *)group_data;
    ssg_member_state_t *dping_target_ms;
    int ret = -1;

    assert(g != NULL);

    ABT_rwlock_wrlock(g->lock);

    /* find dping target */
    while (g->target_list.len > 0)
    {
        /* reshuffle member list after a complete traversal */
        if (g->target_list.dping_ndx == g->target_list.len)
        {
            ssg_shuffle_member_list(&g->target_list);
            g->target_list.dping_ndx = 0;
            continue;
        }

        /* pull next dping target using saved state */  
        dping_target_ms = g->target_list.targets[g->target_list.dping_ndx++];

        /* skip dead members */
        if (dping_target_ms->swim_state.status == SWIM_MEMBER_DEAD) continue;

        *target_id = (swim_member_id_t)dping_target_ms->id;
        *target_inc_nr = dping_target_ms->swim_state.inc_nr;
        *target_addr = dping_target_ms->addr;
        ret = 0;
        break;
    }

    ABT_rwlock_unlock(g->lock);

    return ret;
}

static int ssg_get_swim_iping_targets(
    void *group_data,
    swim_member_id_t dping_target_id,
    int *num_targets,
    swim_member_id_t *target_ids,
    hg_addr_t *target_addrs)
{
    ssg_group_t *g = (ssg_group_t *)group_data;
    int max_targets = *num_targets;
    int iping_target_count = 0;
    int i = 0;
    int r_start, r_ndx;
    ssg_member_state_t *tmp_ms;

    assert(g != NULL);

    *num_targets = 0;

    ABT_rwlock_rdlock(g->lock);

    if (g->target_list.len == 0)
    {
        ABT_rwlock_unlock(g->lock);
        return -1; /* no targets */
    }

    /* pick random index in the target list, and pull out a set of iping
     * targets starting from that index
     */
    r_start = rand() % g->target_list.len;
    while (iping_target_count < max_targets)
    {
        r_ndx = (r_start + i) % g->target_list.len;
        /* if we've iterated through the entire target list, stop */
        if ((i > 0 ) && (r_ndx == r_start)) break;

        tmp_ms = g->target_list.targets[r_ndx];

        /* do not select dead members or the dping target */
        if ((tmp_ms->swim_state.status == SWIM_MEMBER_DEAD) ||
            ((swim_member_id_t)tmp_ms->id == dping_target_id))
        {
            i++;
            continue;
        }

        target_ids[iping_target_count] = (swim_member_id_t)tmp_ms->id;
        target_addrs[iping_target_count] = tmp_ms->addr;
        iping_target_count++;
        i++;
    }

    ABT_rwlock_unlock(g->lock);

    *num_targets = iping_target_count;

    return 0;
}

static void ssg_get_swim_member_addr(
    void *group_data,
    swim_member_id_t id,
    hg_addr_t *addr)
{
    ssg_group_t *g = (ssg_group_t *)group_data;
    ssg_member_id_t ssg_id = (ssg_member_id_t)id;
    ssg_member_state_t *ms;

    assert(g != NULL);

    *addr = HG_ADDR_NULL;
    ABT_rwlock_rdlock(g->lock);

    HASH_FIND(hh, g->view.member_map, &ssg_id, sizeof(ssg_member_id_t), ms);
    /* XXX */
    if (ms)
        *addr = ms->addr;

    ABT_rwlock_unlock(g->lock);

    return;
}

static void ssg_get_swim_member_state(
    void *group_data,
    swim_member_id_t id,
    swim_member_state_t **state)
{
    ssg_group_t *g = (ssg_group_t *)group_data;
    ssg_member_id_t ssg_id = (ssg_member_id_t)id;
    ssg_member_state_t *ms;

    assert(g != NULL);

    *state = NULL;
    ABT_rwlock_rdlock(g->lock);

    HASH_FIND(hh, g->view.member_map, &ssg_id, sizeof(ssg_member_id_t), ms);
    /* XXX */
    if (ms)
        *state = &ms->swim_state;

    ABT_rwlock_unlock(g->lock);

    return;
}

static void ssg_apply_swim_member_update(
    void *group_data,
    swim_member_update_t update)
{
    ssg_group_t *g = (ssg_group_t *)group_data;
    ssg_member_id_t ssg_id = (ssg_member_id_t)update.id;
    ssg_member_update_t ssg_update;
    int ret;

    assert(g != NULL);

#if 0
    if (update.state.status == SWIM_MEMBER_DEAD)
    {
        /* an existing member has left the group */
        ssg_update.id = ssg_id;
        ssg_update.type = SSG_MEMBER_REMOVE;
    }
#endif

    /* invoke user callback to apply the SSG update */
    if (g->update_cb)
        g->update_cb(ssg_update, g->update_cb_dat);

    return;
}

#define SSG_USER_UPDATE_DESERIALIZE(__update, __type, __data) do { \
    assert(__update.size > (sizeof(uint8_t) + 1)); \
    void *__p = __update.data; \
    __type = *(uint8_t *)__p; \
    __data = __p + sizeof(uint8_t); \
} while(0)

void ssg_apply_swim_user_updates(
    void *group_data,
    swim_user_update_t *updates,
    hg_size_t update_count)
{
    ssg_group_t *g = (ssg_group_t *)group_data;
    ssg_update_type_t update_type;
    void *update_data;
    ssg_member_update_t ssg_update;
    hg_size_t i;
    int sret;

    assert(g != NULL);

    for (i = 0; i < update_count; i++)
    {
        SSG_USER_UPDATE_DESERIALIZE(updates[i], update_type, update_data);

        if (update_type == SSG_MEMBER_JOINED)
        {
            char *join_addr_str = (char *)update_data;
            ssg_member_id_t join_id = ssg_gen_member_id(join_addr_str);
            ssg_member_state_t *check = NULL;

            /* ignore join updates for self */
            if (join_id == g->self_id)
                continue;

            /* ignore join udpates for members already in view */
            HASH_FIND(hh, g->view.member_map, &join_id, sizeof(ssg_member_id_t), check);
            if (check)
                continue;

            sret = ssg_group_add_member(g, join_addr_str);
            if (sret != SSG_SUCCESS)
            {
                SSG_DEBUG(g, "Warning: SSG unable to add joining group member %s\n",
                    join_addr_str);
                continue;
            }
            ssg_update.type = update_type;
            ssg_update.id = join_id;
        }
        else
        {
            SSG_DEBUG(g, "Warning: invalid SSG update received, ignoring.\n");
            continue;
        }

        /* invoke user callback to apply the SSG update */
        if (g->update_cb)
            g->update_cb(ssg_update, g->update_cb_dat);
    }

    return;
}
