/*
 * Copyright (c) 2016 UChicago Argonne, LLC
 *
 * See COPYRIGHT in top-level directory.
 */

#include <ssg-config.h>

#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>
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
            exit(EXIT_FAILURE); \
        } \
    } while(0)

struct group_join_leave_opts
{
    char *addr_str;
    char *gid_file;
    int join_time;
    int leave_time;
    int shutdown_time;
};

static void usage()
{
    fprintf(stderr,
        "Usage: "
        "ssg-join-leave-group [OPTIONS] <ADDR> <GID>\n"
        "Join, and potentially leave, an existing group given by GID using Mercury address ADDR.\n"
        "\n"
        "OPTIONS:\n"
        "\t-j TIME\t\tSpecify a time (relative to program start, in seconds) to join the group [default=0]\n"
        "\t-l TIME\t\tSpecify a time (relative to program start, in seconds) to leave the group [default=never]\n"
        "\t-s TIME\t\tSpecify a time (relative to program start, in seconds) to shutdown [default=10]\n"
        "NOTE: leave time must be after join time, and shutdown time must be after both join/leave times\n");
}

static void parse_args(int argc, char *argv[], struct group_join_leave_opts *opts)
{
    int c;
    const char *options = "j:l:s:";
    char *check = NULL;

    while ((c = getopt(argc, argv, options)) != -1)
    {
        switch (c)
        {
            case 'j':
                opts->join_time = (int)strtol(optarg, &check, 0);
                if (opts->join_time < 0 || (check && *check != '\0'))
                {
                    usage();
                    exit(EXIT_FAILURE);
                }
                break;
            case 'l':
                opts->leave_time = (int)strtol(optarg, &check, 0);
                if (opts->leave_time < 0 || (check && *check != '\0'))
                {
                    usage();
                    exit(EXIT_FAILURE);
                }
                break;
            case 's':
                opts->shutdown_time = (int)strtol(optarg, &check, 0);
                if (opts->shutdown_time < 0 || (check && *check != '\0'))
                {
                    usage();
                    exit(EXIT_FAILURE);
                }
                break;
            default:
                usage();
                exit(EXIT_FAILURE);
        }
    }

    if ((argc - optind) != 2)
    {
        usage();
        exit(EXIT_FAILURE);
    }

    if ((opts->leave_time >= 0) && (opts->leave_time <= opts->join_time))
    {
        usage();
        exit(EXIT_FAILURE);
    }
    if ((opts->shutdown_time <= opts->join_time) || (opts->shutdown_time <= opts->leave_time))
    {
        usage();
        exit(EXIT_FAILURE);
    }

    opts->addr_str = argv[optind++];
    opts->gid_file = argv[optind++];

    return;
}

int main(int argc, char *argv[])
{
    struct group_join_leave_opts opts;
    margo_instance_id mid = MARGO_INSTANCE_NULL;
    ssg_group_id_t g_id = SSG_GROUP_ID_INVALID;
    int num_addrs;
    ssg_group_id_t member_id;
    int member_rank;
    int group_size;
    hg_addr_t member_addr = HG_ADDR_NULL;
    int sret;

    /* set any default options (that may be overwritten by cmd args) */
    opts.join_time = 0; /* join the group immediately */
    opts.leave_time = -1; /* default to never leaving group */
    opts.shutdown_time = 10; /* default to shutting down after 10 seconds */

    /* parse cmdline arguments */
    parse_args(argc, argv, &opts);

#ifdef SSG_HAVE_MPI
    MPI_Init(&argc, &argv);
#endif

    /* init margo */
    /* use the main xstream to drive progress & run handlers */
    mid = margo_init(opts.addr_str, MARGO_SERVER_MODE, 0, -1);
    DIE_IF(mid == MARGO_INSTANCE_NULL, "margo_init");

    /* initialize SSG */
    sret = ssg_init();
    DIE_IF(sret != SSG_SUCCESS, "ssg_init (%s)", ssg_strerror(sret));

    /* load group info from file */
    num_addrs = SSG_ALL_MEMBERS;
    sret = ssg_group_id_load(opts.gid_file, &num_addrs, &g_id);
    DIE_IF(sret != SSG_SUCCESS, "ssg_group_id_load (%s)", ssg_strerror(sret));
    DIE_IF(num_addrs < 1, "ssg_group_id_load (%s)", ssg_strerror(sret));

    /* assert some things about loaded group */
    sret = ssg_get_group_size(g_id, &group_size);
    DIE_IF(sret != SSG_SUCCESS, "ssg_get_group_size (%s)", ssg_strerror(sret));
    sret = ssg_get_group_member_id_from_rank(g_id, 0, &member_id);
    DIE_IF(sret != SSG_SUCCESS, "ssg_get_group_member_id_from_rank (%s)", ssg_strerror(sret));
    sret = ssg_get_group_member_rank(g_id, member_id, &member_rank);
    DIE_IF(sret != SSG_SUCCESS, "ssg_get_group_member_rank (%s)", ssg_strerror(sret));
    DIE_IF(member_rank != 0, "ssg_get_group_member_rank (%s)", ssg_strerror(sret));
    /* get_group_member_addr() will fail since we do not have a margo
     * instance associated with the group, which happens later as
     * part of group_refresh()
     */
    sret = ssg_get_group_member_addr(g_id, member_id, &member_addr);
    DIE_IF(sret != SSG_ERR_MID_NOT_FOUND, "ssg_get_group_member_addr (%s)", ssg_strerror(sret));
    DIE_IF(member_addr != HG_ADDR_NULL, "ssg_get_group_member_addr (%s)", ssg_strerror(sret));

    /* sleep until time to join */
    if (opts.join_time > 0)
        margo_thread_sleep(mid, opts.join_time * 1000.0);

    sret = ssg_group_join(mid, g_id, NULL, NULL);
    DIE_IF(sret != SSG_SUCCESS, "ssg_group_join (%s)", ssg_strerror(sret));

    /* sleep for given duration to allow group time to run */
    if (opts.leave_time >= 0)
    {
        margo_thread_sleep(mid, (opts.leave_time - opts.join_time) * 1000.0);

        /* dump group to see view prior to leaving */
        ssg_group_dump(g_id);

        sret = ssg_group_leave(g_id);
        DIE_IF(sret != SSG_SUCCESS, "ssg_group_leave (%s)", ssg_strerror(sret));

        margo_thread_sleep(mid, (opts.shutdown_time - opts.leave_time) * 1000.0);

        ssg_group_dump(g_id);
    }
    else
    {
        margo_thread_sleep(mid, (opts.shutdown_time - opts.join_time) * 1000.0);

        ssg_group_dump(g_id);
    }

    ssg_group_destroy(g_id);
    ssg_finalize();
    margo_finalize(mid);

#ifdef SSG_HAVE_MPI
    MPI_Finalize();
#endif

    return 0;
}
