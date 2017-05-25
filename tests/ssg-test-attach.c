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
#include <mpi.h>

#include <margo.h>
#include <mercury.h>
#include <abt.h>
#include <ssg.h>

#define DIE_IF(cond_expr, err_fmt, ...) \
    do { \
        if (cond_expr) { \
            fprintf(stderr, "ERROR at %s:%d (" #cond_expr "): " \
                    err_fmt "\n", __FILE__, __LINE__, ##__VA_ARGS__); \
            exit(1); \
        } \
    } while(0)

static void usage()
{
    fprintf(stderr,
        "Usage: "
        "ssg-test-simple [-s <time>] <addr> \n"
        "\t-s <time> - time to sleep after SSG group creation\n");
}

static void parse_args(int argc, char *argv[], int *sleep_time, const char **addr_str)
{
    int ndx = 1;

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

int main(int argc, char *argv[])
{
    hg_class_t *hgcl = NULL;
    hg_context_t *hgctx = NULL;
    margo_instance_id mid = MARGO_INSTANCE_NULL;
    int sleep_time = 0;
    const char *addr_str;
    const char *group_name = "simple_group";
    ssg_group_id_t g_id;
    int my_world_rank;
    int my_ssg_rank;
    int color;
    MPI_Comm ssg_comm;
    int sret;

    parse_args(argc, argv, &sleep_time, &addr_str);

    ABT_init(argc, argv);
    MPI_Init(&argc, &argv);

    /* init HG */
    hgcl = HG_Init(addr_str, HG_TRUE);
    DIE_IF(hgcl == NULL, "HG_Init");
    hgctx = HG_Context_create(hgcl);
    DIE_IF(hgctx == NULL, "HG_Context_create");

    /* init margo in single threaded mode */
    mid = margo_init(0, -1, hgctx);
    DIE_IF(mid == MARGO_INSTANCE_NULL, "margo_init");

    /* initialize SSG */
    sret = ssg_init(mid);
    DIE_IF(sret != SSG_SUCCESS, "ssg_init");

    /* create a communicator for the SSG group  */
    /* NOTE: rank 0 will not be in the group and will instead attach
     * as a client -- ranks 0:n-1 then represent the SSG group
     */
    MPI_Comm_rank(MPI_COMM_WORLD, &my_world_rank);
    if (my_world_rank == 0)
        color = MPI_UNDEFINED;
    else
        color = 0;
    MPI_Comm_split(MPI_COMM_WORLD, color, my_world_rank, &ssg_comm);

    if (my_world_rank != 0)
    {
        sret = ssg_group_create_mpi(group_name, ssg_comm, &g_id);
        DIE_IF(sret != SSG_SUCCESS, "ssg_group_create");
    }

    /* for now, just sleep to give all procs an opportunity to create the group */
    /* XXX: we could replace this with a barrier eventually */
    if (sleep_time > 0) margo_thread_sleep(mid, sleep_time * 1000.0);

    /* XXX: cheat to get the group id from a member using MPI */


    /* attach client process to SSG server group */
    if (my_world_rank == 0)
    {
        sret = ssg_group_attach(g_id);
        DIE_IF(sret != SSG_SUCCESS, "ssg_group_attach");
    }

    /** cleanup **/
    if (my_world_rank == 0)
    {
        ssg_group_detach(g_id);
    }
    else
    {
        ssg_group_destroy(g_id);
    }
    ssg_finalize();

    margo_finalize(mid);

    if(hgctx) HG_Context_destroy(hgctx);
    if(hgcl) HG_Finalize(hgcl);

    MPI_Finalize();
    ABT_finalize();

    return 0;
}
