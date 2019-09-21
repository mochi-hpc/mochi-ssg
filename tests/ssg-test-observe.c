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
        "ssg-test-observe [-s <time>] <addr> <GID>\n"
        "Observe group given by GID using Mercury address ADDR.\n"
        "\t-s <time> - time to sleep between SSG group operations\n");
}

static void parse_args(int argc, char *argv[], int *sleep_time, const char **addr_str, const char **gid_file)
{
    int ndx = 1;

#ifndef SSG_HAVE_MPI
    fprintf(stderr, "Error: ssg-test-observe currently requries MPI support\n");
    exit(1);
#endif

    if (argc < 3)
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

    *addr_str = argv[ndx++];
    *gid_file = argv[ndx++];

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
    const char *gid_file;
    ssg_group_id_t g_id;
    int sret;

    parse_args(argc, argv, &sleep_time, &addr_str, &gid_file);

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

    sret = ssg_group_id_load(gid_file, &g_id);
    DIE_IF(sret != SSG_SUCCESS, "ssg_group_id_load");

    /* start observging the SSG server group */
    sret = ssg_group_observe(g_id);
    DIE_IF(sret != SSG_SUCCESS, "ssg_group_observe");

    /* for now, just sleep to give observer a chance to establish connection */
    /* XXX: we could replace this with a barrier eventually */
    if (sleep_time > 0) margo_thread_sleep(mid, sleep_time * 1000.0);

    /* have everyone dump their group state */
    ssg_group_dump(g_id);

    if (sleep_time > 0) margo_thread_sleep(mid, sleep_time * 1000.0);

    /* clean up */
    ssg_group_unobserve(g_id);
    ssg_finalize();
    margo_finalize(mid);

#ifdef SSG_HAVE_MPI
    MPI_Finalize();
#endif

    return 0;
}
