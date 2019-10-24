/*
 * Copyright (c) 2016 UChicago Argonne, LLC
 *
 * See COPYRIGHT in top-level directory.
 */

#include <unistd.h>
#include <stdio.h>
#include <string.h>
#include <pmix.h>

#include <margo.h>
#include <ssg.h>
#include <ssg-pmix.h>

#define DIE_IF(cond_expr, err_fmt, ...) \
    do { \
        if (cond_expr) { \
            fprintf(stderr, "ERROR at %s:%d (" #cond_expr "): " \
                    err_fmt "\n", __FILE__, __LINE__, ##__VA_ARGS__); \
            exit(EXIT_FAILURE); \
        } \
    } while(0)

struct group_launch_opts
{
    char *addr_str;
    int shutdown_time;
    int kill_time;
    int kill_rank;
};

static void usage()
{
    fprintf(stderr,
        "Usage: "
        "ssg-launch-group [OPTIONS] <ADDR>\n"
        "Create and launch group using given Mercury ADDR string.\n"
        "\n"
        "OPTIONS:\n"
        "\t-s <TIME>\t\tTime duration (in seconds) to run the group before shutting down\n"
        "\t-k <TIME>\t\tTime duration (in seconds) to kill group member\n"
        "\t-r <rank>\t\tPMIx rank to kill\n"
);
}

static void parse_args(int argc, char *argv[], struct group_launch_opts *opts)
{
    int c;
    const char *options = "s:k:r:";
    char *check = NULL;

    while ((c = getopt(argc, argv, options)) != -1)
    {
        switch (c)
        {
            case 's':
                opts->shutdown_time = (int)strtol(optarg, &check, 0);
                if (opts->shutdown_time < 0 || (check && *check != '\0'))
                {
                    usage();
                    exit(EXIT_FAILURE);
                }
                break;
            case 'k':
                opts->kill_time = (int)strtol(optarg, &check, 0);
                if (opts->kill_time < 0 || (check && *check != '\0'))
                {
                    usage();
                    exit(EXIT_FAILURE);
                }
                break;
            case 'r':
                opts->kill_rank = (int)strtol(optarg, &check, 0);
                if (opts->kill_rank < 0 || (check && *check != '\0'))
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

    if ((argc - optind) < 1)
    {
        usage();
        exit(EXIT_FAILURE);
    }

    opts->addr_str = argv[optind++];

    return;
}

int main(int argc, char *argv[])
{
    struct group_launch_opts opts;
    pmix_status_t ret;
    pmix_proc_t proc;
    margo_instance_id mid = MARGO_INSTANCE_NULL;
    ssg_group_id_t g_id = SSG_GROUP_ID_INVALID;
    ssg_member_id_t my_id;
    int group_size;
    int sret;

    /* set any default options (that may be overwritten by cmd args) */
    opts.shutdown_time = 30; /* default to running group for 30 seconds */
    opts.kill_time = 10; /* default to kill process in 10 seconds */
    opts.kill_rank = 0; /* default to kill rank 0 */

    /* parse cmdline arguments */
    parse_args(argc, argv, &opts);

    ret = PMIx_Init(&proc, NULL, 0);
    DIE_IF(ret != PMIX_SUCCESS, "PMIx_Init");

    /* init margo */
    /* use the main xstream to drive progress & run handlers */
    mid = margo_init(opts.addr_str, MARGO_SERVER_MODE, 0, 1);
    DIE_IF(mid == MARGO_INSTANCE_NULL, "margo_init");

    /* initialize SSG */
    sret = ssg_init();
    DIE_IF(sret != SSG_SUCCESS, "ssg_init");

    /* XXX do we want to use callback for testing anything about group??? */
    g_id = ssg_group_create_pmix(mid, "fail_group", proc, NULL, NULL, NULL);
    DIE_IF(g_id == SSG_GROUP_ID_INVALID, "ssg_group_create");

    /* get my group id and the size of the group */
    my_id = ssg_get_self_id(mid);
    DIE_IF(my_id == SSG_MEMBER_ID_INVALID, "ssg_get_group_self_id");
    group_size = ssg_get_group_size(g_id);
    DIE_IF(group_size == 0, "ssg_get_group_size");

    if (proc.rank == (unsigned int)opts.kill_rank)
    {
        /* sleep for given duration before killing rank */
        margo_thread_sleep(mid, opts.kill_time * 1000.0);
        fprintf(stderr, "%.6lf: KILL member %lu (PMIx rank %d)\n", ABT_get_wtime(), my_id, proc.rank);
        PMIx_Notify_event(PMIX_PROC_TERMINATED, &proc,
            PMIX_RANGE_GLOBAL, NULL, 0, NULL, NULL);
	ssg_finalize();
        margo_thread_sleep(mid, (opts.shutdown_time - opts.kill_time) * 1000.0);
        margo_finalize(mid);
    }
    else
    {
        /* sleep for given duration to allow group time to run */
        margo_thread_sleep(mid, opts.shutdown_time * 1000.0);
    }

    if (proc.rank != (unsigned int)opts.kill_rank)
    {
        /* print group at each alive member */
        ssg_group_dump(g_id);
        ssg_group_destroy(g_id);

        /** cleanup **/
        /* XXX cleanup fails on dead process currently */
        ssg_finalize();
        margo_finalize(mid);
    }

    PMIx_Finalize(NULL, 0);

    return 0;
}
