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
        "ssg-observe-group <addr> <GID>\n"
        "Observe group given by GID using Mercury address ADDR.\n");
}

static void parse_args(int argc, char *argv[], const char **addr_str, const char **gid_file)
{
    int ndx = 1;

    if (argc < 2)
    {
        usage();
        exit(1);
    }

    *addr_str = argv[ndx++];
    *gid_file = argv[ndx++];

    return;   
}

int main(int argc, char *argv[])
{
    margo_instance_id mid = MARGO_INSTANCE_NULL;
    const char *addr_str;
    const char *gid_file;
    ssg_group_id_t g_id;
    int num_addrs;
    int sret;

    parse_args(argc, argv, &addr_str, &gid_file);

#ifdef SSG_HAVE_MPI
    MPI_Init(&argc, &argv);
#endif

    /* init margo */
    /* use the main xstream to drive progress & run handlers */
    mid = margo_init(addr_str, MARGO_CLIENT_MODE, 0, -1);
    DIE_IF(mid == MARGO_INSTANCE_NULL, "margo_init");

    /* initialize SSG */
    sret = ssg_init();
    DIE_IF(sret != SSG_SUCCESS, "ssg_init");

    num_addrs = SSG_ALL_MEMBERS;
    sret = ssg_group_id_load(gid_file, &num_addrs, &g_id);
    DIE_IF(sret != SSG_SUCCESS, "ssg_group_id_load");
    DIE_IF(num_addrs < 1, "ssg_group_id_load");

    /* start observging the SSG server group */
    sret = ssg_group_observe(mid, g_id);
    DIE_IF(sret != SSG_SUCCESS, "ssg_group_observe");

    /* have everyone dump their group state */
    ssg_group_dump(g_id);

    /* clean up */
    ssg_group_unobserve(g_id);
    ssg_finalize();
    margo_finalize(mid);

#ifdef SSG_HAVE_MPI
    MPI_Finalize();
#endif

    return 0;
}
