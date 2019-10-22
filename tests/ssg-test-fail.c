/*
 * Copyright (c) 2016 UChicago Argonne, LLC
 *
 * See COPYRIGHT in top-level directory.
 */

#include <ssg-config.h>

#include <unistd.h>
#include <stdio.h>
#include <string.h>
#ifdef SSG_HAVE_MPI
#include <mpi.h>
#endif

#include <margo.h>
#include <ssg.h>
#ifdef SSG_HAVE_MPI
#include <ssg-mpi.h>
#endif
#ifdef SSG_HAVE_PMIX
#include <ssg-pmix.h>
#endif

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
    char *group_mode;
    int shutdown_time;
    int kill_time;
    int kill_rank;
};

static void usage()
{
    fprintf(stderr,
        "Usage: "
        "ssg-launch-group [OPTIONS] <ADDR> <MODE>\n"
        "Create and launch group using given Mercury ADDR string and group create MODE (\"mpi\" or \"pmix\").\n"
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

    if ((argc - optind) < 2)
    {
        usage();
        exit(EXIT_FAILURE);
    }

    opts->addr_str = argv[optind++];
    opts->group_mode = argv[optind++];
    if (strcmp(opts->group_mode, "mpi") == 0)
    {
#ifdef SSG_HAVE_MPI
        if (optind != argc)
        {
            usage();
            exit(EXIT_FAILURE);
        }
#else
        fprintf(stderr, "Error: MPI support not built in\n");
        exit(EXIT_FAILURE);
#endif
    }
    else if (strcmp(opts->group_mode, "pmix") == 0)
    {
#ifdef SSG_HAVE_PMIX
        if (optind != argc)
        {
            usage();
            exit(EXIT_FAILURE);
        }
#else
        fprintf(stderr, "Error: PMIx support not built in\n");
        exit(EXIT_FAILURE);
#endif
    }
    else
    {
        usage();
        exit(EXIT_FAILURE);
    }

    return;
}

int main(int argc, char *argv[])
{
    struct group_launch_opts opts;
    int my_rank;
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

#ifdef SSG_HAVE_MPI
    int mpi_rank, mpi_size;
    if (strcmp(opts.group_mode, "mpi") == 0)
    {
        MPI_Init(&argc, &argv);
        MPI_Comm_rank(MPI_COMM_WORLD, &mpi_rank);
        MPI_Comm_size(MPI_COMM_WORLD, &mpi_size);
    }
    my_rank = mpi_rank;
#endif
#ifdef SSG_HAVE_PMIX
    pmix_status_t ret;
    pmix_proc_t proc;
    if (strcmp(opts.group_mode, "pmix") == 0)
    {
        ret = PMIx_Init(&proc, NULL, 0);
        DIE_IF(ret != PMIX_SUCCESS, "PMIx_Init");
    }
    my_rank = proc.rank;
#endif

    /* init margo */
    /* use the main xstream to drive progress & run handlers */
    mid = margo_init(opts.addr_str, MARGO_SERVER_MODE, 0, 1);
    DIE_IF(mid == MARGO_INSTANCE_NULL, "margo_init");

    /* initialize SSG */
    sret = ssg_init();
    DIE_IF(sret != SSG_SUCCESS, "ssg_init");

    /* XXX do we want to use callback for testing anything about group??? */
#ifdef SSG_HAVE_MPI
    if(strcmp(opts.group_mode, "mpi") == 0)
        g_id = ssg_group_create_mpi(mid, "fail_group", MPI_COMM_WORLD, NULL, NULL);
#endif
#ifdef SSG_HAVE_PMIX
    if(strcmp(opts.group_mode, "pmix") == 0)
        g_id = ssg_group_create_pmix(mid, "fail_group", proc, NULL, NULL);
#endif
    DIE_IF(g_id == SSG_GROUP_ID_INVALID, "ssg_group_create");

    /* get my group id and the size of the group */
    my_id = ssg_get_self_id(mid);
    DIE_IF(my_id == SSG_MEMBER_ID_INVALID, "ssg_get_group_self_id");
    group_size = ssg_get_group_size(g_id);
    DIE_IF(group_size == 0, "ssg_get_group_size");

    if (my_rank == opts.kill_rank)
    {
        /* sleep for given duration before killing rank */
        margo_thread_sleep(mid, opts.kill_time * 1000.0);
        fprintf(stderr, "%.6lf: KILL member %lu (PMIx rank %d)\n", ABT_get_wtime(), my_id, my_rank);
	/* XXX we can't really terminate the rank (or MPI would abort, for instance)... */
        ssg_group_destroy(g_id);
        ssg_finalize();
        margo_thread_sleep(mid, (opts.shutdown_time - opts.kill_time) * 1000.0);
        margo_finalize(mid);
    }
    else
    {
        /* sleep for given duration to allow group time to run */
        margo_thread_sleep(mid, opts.shutdown_time * 1000.0);
    }

    if (my_rank != opts.kill_rank)
    {
        /* print group at each alive member */
        ssg_group_dump(g_id);
        ssg_group_destroy(g_id);

        /** cleanup **/
        /* XXX cleanup fails on dead process currently */
        ssg_finalize();
        margo_finalize(mid);
    }

#ifdef SSG_HAVE_MPI
    if (strcmp(opts.group_mode, "mpi") == 0)
        MPI_Finalize();
#endif
#ifdef SSG_HAVE_PMIX
    if (strcmp(opts.group_mode, "pmix") == 0)
        PMIx_Finalize(NULL, 0);
#endif

    return 0;
}
