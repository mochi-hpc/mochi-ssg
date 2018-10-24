/*
 * Copyright (c) 2016 UChicago Argonne, LLC
 *
 * See COPYRIGHT in top-level directory.
 */

#include <ssg-config.h>

#include <unistd.h>
#include <stdio.h>
#include <string.h>

#include <margo.h>
#include <ssg.h>

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
    int join_time;
    int leave_time;
    int shutdown_time;
    char *addr_str;
    char *gid_file;
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
    ssg_group_id_t in_g_id = SSG_GROUP_ID_NULL;
    ssg_group_id_t out_g_id = SSG_GROUP_ID_NULL;
    int sret;

    /* set any default options (that may be overwritten by cmd args) */
    opts.join_time = 0; /* join the group immediately */
    opts.leave_time = -1; /* default to never leaving group */
    opts.shutdown_time = 10; /* default to shutting down after 10 seconds */

    /* parse cmdline arguments */
    parse_args(argc, argv, &opts);

    /* init margo */
    /* use the main xstream to drive progress & run handlers */
    mid = margo_init(opts.addr_str, MARGO_SERVER_MODE, 0, -1);
    DIE_IF(mid == MARGO_INSTANCE_NULL, "margo_init");

    /* initialize SSG */
    sret = ssg_init(mid);
    DIE_IF(sret != SSG_SUCCESS, "ssg_init");

    /* load GID from file */
    ssg_group_id_load(opts.gid_file, &in_g_id);

    /* sleep until time to join */
    if (opts.join_time > 0)
        margo_thread_sleep(mid, opts.join_time * 1000.0);

    /* XXX do we want to use callback for testing anything about group??? */
    out_g_id = ssg_group_join(in_g_id, NULL, NULL);
    DIE_IF(out_g_id == SSG_GROUP_ID_NULL, "ssg_group_join");
    ssg_group_id_free(in_g_id);

    /* sleep for given duration to allow group time to run */
    if (opts.leave_time >= 0)
    {
        margo_thread_sleep(mid, (opts.leave_time - opts.join_time) * 1000.0);

        /* dump group to see view prior to leaving */
        ssg_group_dump(out_g_id);

        sret = ssg_group_leave(out_g_id);
        DIE_IF(sret != SSG_SUCCESS, "ssg_group_leave");

        margo_thread_sleep(mid, (opts.shutdown_time - opts.leave_time) * 1000.0);
    }
    else
    {
        margo_thread_sleep(mid, (opts.shutdown_time - opts.join_time) * 1000.0);

        ssg_group_dump(out_g_id);
        ssg_group_destroy(out_g_id);
    }

    ssg_finalize();
    margo_finalize(mid);

    return 0;
}
