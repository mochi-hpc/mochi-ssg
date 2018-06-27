/*
 * Copyright (c) 2016 UChicago Argonne, LLC
 *
 * See COPYRIGHT in top-level directory.
 */

#include <ssg-config.h>

#include <unistd.h>
#include <stdio.h>
#include <string.h>
#include <assert.h>
#ifdef SSG_HAVE_MPI
#include <mpi.h>
#endif

#include <margo.h>
#include <mercury.h>
#include <ssg.h>
#ifdef SSG_HAVE_MPI
#include <ssg-mpi.h>
#endif

#define DIE_IF(cond_expr, err_fmt, ...) \
    do { \
        if (cond_expr) { \
            fprintf(stderr, "ERROR at %s:%d (" #cond_expr "): " \
                    err_fmt "\n", __FILE__, __LINE__, ##__VA_ARGS__); \
            exit(1); \
        } \
    } while(0)


DECLARE_MARGO_RPC_HANDLER(group_id_forward_recv_ult)

static void usage()
{
    fprintf(stderr,
        "Usage: "
        "ssg-test-attach [-s <time>] <addr> \n"
        "\t-s <time> - time to sleep between SSG group operations\n");
}

static void parse_args(int argc, char *argv[], int *sleep_time, const char **addr_str)
{
    int ndx = 1;

#ifndef SSG_HAVE_MPI
    fprintf(stderr, "Error: ssg-test-attach currently requries MPI support\n");
    exit(1);
#endif

    if (argc < 2)
    {
        usage();
        exit(1);
    }

    if (strcmp(argv[ndx], "-s") == 0)
    {
        char *check = NULL;
        ndx++;

        *sleep_time = (int)strtol(argv[ndx++], &check, 0);
        if(*sleep_time < 0 || (check && *check != '\0') || argc < 4)
        {
            usage();
            exit(1);
        }
    }

    *addr_str = argv[ndx];

    return;   
}

struct group_id_forward_context
{
    margo_instance_id mid;
    ssg_group_id_t *g_id_p;
};

int main(int argc, char *argv[])
{
    margo_instance_id mid = MARGO_INSTANCE_NULL;
    int sleep_time = 0;
    const char *addr_str;
    const char *group_name = "simple_group";
    ssg_group_id_t g_id;
    int group_id_forward_rpc_id;
    int is_attacher = 0;
    hg_addr_t attacher_addr;
    char attacher_addr_str[128];
    hg_size_t attacher_addr_str_sz = 128;
    hg_handle_t handle = HG_HANDLE_NULL;
    hg_return_t hret;
    int sret;

    parse_args(argc, argv, &sleep_time, &addr_str);

#ifdef SSG_HAVE_MPI
    MPI_Init(&argc, &argv);
#endif

    /* init margo */
    /* use the main xstream to drive progress & run handlers */
    mid = margo_init(addr_str, MARGO_SERVER_MODE, 0, -1);
    DIE_IF(mid == MARGO_INSTANCE_NULL, "margo_init");

    /* initialize SSG */
    sret = ssg_init(mid);
    DIE_IF(sret != SSG_SUCCESS, "ssg_init");

    /* register RPC for forwarding an SSG group identifier */
    group_id_forward_rpc_id = MARGO_REGISTER(mid, "group_id_forward",
        ssg_group_id_t, void, group_id_forward_recv_ult);
    hret = margo_register_data(mid, group_id_forward_rpc_id, &g_id, NULL);
    DIE_IF(hret != HG_SUCCESS, "margo_register_data");

    /* XXX do something for config file case? */
#ifdef SSG_HAVE_MPI
    int my_world_rank;
    int world_size;
    int color;
    MPI_Comm ssg_comm;

    /* create a communicator for the SSG group  */
    /* NOTE: rank 0 will not be in the group and will instead attach
     * as a client -- ranks 0:n-1 then represent the SSG group
     */
    MPI_Comm_size(MPI_COMM_WORLD, &world_size);
    if (world_size < 2)
    {
        fprintf(stderr, "Error: MPI_COMM_WORLD must contain at least 2 processes\n");
        exit(1);
    }

    MPI_Comm_rank(MPI_COMM_WORLD, &my_world_rank);
    if (my_world_rank == 0)
    {
        is_attacher = 1;
        color = MPI_UNDEFINED;
    }
    else
    {
        color = 0;
    }
    MPI_Comm_split(MPI_COMM_WORLD, color, my_world_rank, &ssg_comm);

    if (!is_attacher)
    {
        g_id = ssg_group_create_mpi(group_name, ssg_comm, NULL, NULL);
        DIE_IF(g_id == SSG_GROUP_ID_NULL, "ssg_group_create");

        if (my_world_rank == 1)
        {
            MPI_Recv(attacher_addr_str, 128, MPI_BYTE, 0, 0, MPI_COMM_WORLD,
                MPI_STATUS_IGNORE);

            /* send the identifier for the created group back to the attacher */
            hret = margo_addr_lookup(mid, attacher_addr_str, &attacher_addr);
            DIE_IF(hret != HG_SUCCESS, "margo_addr_lookup");
            hret = margo_create(mid, attacher_addr, group_id_forward_rpc_id, &handle);
            DIE_IF(hret != HG_SUCCESS, "margo_create");
            hret = margo_forward(handle, &g_id);
            DIE_IF(hret != HG_SUCCESS, "margo_forward");
            margo_addr_free(mid, attacher_addr);
            margo_destroy(handle);
        }
    }
    else
    {
        hret = margo_addr_self(mid, &attacher_addr);
        DIE_IF(hret != HG_SUCCESS, "margo_addr_self");
        hret = margo_addr_to_string(mid, attacher_addr_str, &attacher_addr_str_sz,
            attacher_addr);
        DIE_IF(hret != HG_SUCCESS, "margo_addr_to_string");
        margo_addr_free(mid, attacher_addr);

        /* send the attacher's address to a group member, so the group
         * member can send us back the corresponding SSG group identifier
         */
        MPI_Send(attacher_addr_str, 128, MPI_BYTE, 1, 0, MPI_COMM_WORLD);
    }
#endif

    /* for now, just sleep to give all procs an opportunity to create the group */
    /* XXX: we could replace this with a barrier eventually */
    if (sleep_time > 0) margo_thread_sleep(mid, sleep_time * 1000.0);

    /* attach client process to SSG server group */
    if (is_attacher)
    {
        sret = ssg_group_attach(g_id);
        DIE_IF(sret != SSG_SUCCESS, "ssg_group_attach");
    }

    /* for now, just sleep to give attacher a chance to finish attaching */
    /* XXX: we could replace this with a barrier eventually */
    if (sleep_time > 0) margo_thread_sleep(mid, sleep_time * 1000.0);

    /* have everyone dump their group state */
    ssg_group_dump(g_id);

    /* clean up */
    if (is_attacher)
    {
        ssg_group_detach(g_id);
    }
    else
    {
        ssg_group_destroy(g_id);
    }
    ssg_finalize();
    margo_finalize(mid);

#ifdef SSG_HAVE_MPI
    MPI_Finalize();
#endif

    return 0;
}

static void group_id_forward_recv_ult(hg_handle_t handle)
{
    const struct hg_info *info;
    margo_instance_id mid;
    ssg_group_id_t *g_id_p;
    ssg_group_id_t tmp_g_id;
    hg_return_t hret;

    info = margo_get_info(handle);
    DIE_IF(info == NULL, "margo_get_info");
    mid = margo_hg_info_get_instance(info);
    DIE_IF(mid == MARGO_INSTANCE_NULL, "margo_hg_info_get_instance");
    g_id_p = (ssg_group_id_t *)margo_registered_data(mid, info->id);
    DIE_IF(g_id_p == NULL, "margo_registered_data");

    hret = margo_get_input(handle, &tmp_g_id);
    DIE_IF(hret != HG_SUCCESS, "margo_get_input");

    *g_id_p = ssg_group_id_dup(tmp_g_id);

    margo_respond(handle, NULL);

    margo_free_input(handle, &tmp_g_id);
    margo_destroy(handle);
    return;
}
DEFINE_MARGO_RPC_HANDLER(group_id_forward_recv_ult)
