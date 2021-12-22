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
#ifdef SSG_HAVE_MPI
#include <mpi.h>
#endif
#ifdef SSG_HAVE_PMIX
#include <pmix.h>
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
    char *gid_file;
    char *group_name;
};

static void usage()
{
    fprintf(stderr,
        "Usage: "
        "ssg-launch-double-group [OPTIONS] <ADDR> <MODE> [CONFFILE]\n"
        "Create and launch two groups using given Mercury ADDR string and group create MODE (\"mpi\" or \"pmix\").\n"
        "\n"
        "OPTIONS:\n"
        "\t-s <TIME>\t\tTime duration (in seconds) to run the group before shutting down\n"
        "\t-f <FILE>\t\tFile path to store group ID in\n"
        "\t-n <NAME>\t\tName of the group to launch\n");
}

static void parse_args(int argc, char *argv[], struct group_launch_opts *opts)
{
    int c;
    const char *options = "s:f:n:";
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
            case 'f':
                opts->gid_file = optarg;
                break;
            case 'n':
                opts->group_name = optarg;
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
    margo_instance_id mid = MARGO_INSTANCE_NULL;
    ssg_group_id_t g1_id = SSG_GROUP_ID_INVALID;
    ssg_group_id_t g2_id = SSG_GROUP_ID_INVALID;
    ssg_member_id_t my_id;
    int group1_size;
    int group2_size;
    char scratch[1024] = {0};
    int sret;

    /* set any default options (that may be overwritten by cmd args) */
    opts.shutdown_time = 10; /* default to running group for 10 seconds */
    opts.group_name = "simple_group";
    opts.gid_file = NULL;

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
#endif
#ifdef SSG_HAVE_PMIX
    pmix_status_t ret;
    pmix_proc_t proc;
    if (strcmp(opts.group_mode, "pmix") == 0)
    {
        ret = PMIx_Init(&proc, NULL, 0);
        DIE_IF(ret != PMIX_SUCCESS, "PMIx_Init");
    }
#endif

    /* init margo */
    /* NOTE: dedicated progress thread needed to avoid MPI/margo deadlock
     * possible when running multiple groups
     */
    mid = margo_init(opts.addr_str, MARGO_SERVER_MODE, 1, -1);
    DIE_IF(mid == MARGO_INSTANCE_NULL, "margo_init");

    /* initialize SSG */
    sret = ssg_init();
    DIE_IF(sret != SSG_SUCCESS, "ssg_init");

    /* XXX do we want to use callback for testing anything about group??? */
#ifdef SSG_HAVE_MPI
    if(strcmp(opts.group_mode, "mpi") == 0)
    {
        snprintf(scratch, 1024, "%s-%d", opts.group_name, 1);
        sret = ssg_group_create_mpi(mid, scratch, MPI_COMM_WORLD, NULL, NULL, NULL, &g1_id);
        snprintf(scratch, 1024, "%s-%d", opts.group_name, 2);
        sret = ssg_group_create_mpi(mid, scratch, MPI_COMM_WORLD, NULL, NULL, NULL, &g2_id);
    }
#endif
#ifdef SSG_HAVE_PMIX
    if(strcmp(opts.group_mode, "pmix") == 0)
    {
        snprintf(scratch, 1024, "%s-%d", opts.group_name, 1);
        sret = ssg_group_create_pmix(mid, scratch, proc, NULL, NULL, NULL, &g1_id);
        snprintf(scratch, 1024, "%s-%d", opts.group_name, 2);
        sret = ssg_group_create_pmix(mid, scratch, proc, NULL, NULL, NULL, &g2_id);
    }
#endif
    DIE_IF(g1_id == SSG_GROUP_ID_INVALID, "ssg_group_create");
    DIE_IF(g2_id == SSG_GROUP_ID_INVALID, "ssg_group_create");

    /* store the gid if requested */
    if (opts.gid_file)
    {
        snprintf(scratch, 1024, "%s-%d", opts.gid_file, 1);
        ssg_group_id_store(scratch, g1_id, 1);
        snprintf(scratch, 1024, "%s-%d", opts.gid_file, 2);
        ssg_group_id_store(scratch, g2_id, 1);
    }

    /* sleep for given duration to allow group time to run */
    if (opts.shutdown_time > 0)
        margo_thread_sleep(mid, opts.shutdown_time * 1000.0);

    /* get my group id and the size of the group */
    sret = ssg_get_self_id(mid, &my_id);
    DIE_IF(sret != SSG_SUCCESS, "ssg_get_self_id");
    sret = ssg_get_group_size(g1_id, &group1_size);
    DIE_IF(sret != SSG_SUCCESS, "ssg_get_group_size");
    sret = ssg_get_group_size(g2_id, &group2_size);
    DIE_IF(sret != SSG_SUCCESS, "ssg_get_group_size");

    /* print group at each member */
    ssg_group_dump(g1_id);
    ssg_group_destroy(g1_id);
    ssg_group_dump(g2_id);
    ssg_group_destroy(g2_id);

    /** cleanup **/
    ssg_finalize();
    margo_finalize(mid);
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
