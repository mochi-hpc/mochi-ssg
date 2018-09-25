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
    char *group_addr_conf_file;
    int duration;
    char *gid_file;
    char *group_name;
};

static void usage()
{
    fprintf(stderr,
        "Usage: "
        "ssg-launch-group [OPTIONS] <ADDR> <MODE> [CONFFILE]\n"
        "Create and launch group using given Mercury ADDR string and group create MODE (\"mpi\" or \"conf\").\n"
        "NOTE: A path to an address CONFFILE is required when using \"conf\" mode.\n" 
        "\n"
        "OPTIONS:\n"
        "\t-d DUR\t\tSpecify a time duration (in seconds) to run the group for\n"
        "\t-f FILE\t\tStore group GID at a given file path\n"
        "\t-n NAME\t\tSpecify the name of the launched group\n");
}

static void parse_args(int argc, char *argv[], struct group_launch_opts *opts)
{
    int c;
    const char *options = "d:f:n:";
    char *check = NULL;

    while ((c = getopt(argc, argv, options)) != -1)
    {
        switch (c)
        {
            case 'd':
                opts->duration = (int)strtol(optarg, &check, 0);
                if (opts->duration < 0 || (check && *check != '\0'))
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
    if (strcmp(opts->group_mode, "conf") == 0)
    {
        if ((argc - optind) != 1)
        {
            usage();
            exit(EXIT_FAILURE);
        }
        opts->group_addr_conf_file = argv[optind++];
    }
    else if (strcmp(opts->group_mode, "mpi") == 0)
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
    ssg_group_id_t g_id = SSG_GROUP_ID_NULL;
    ssg_member_id_t my_id;
    int group_size;
    int sret;

    /* set any default options (that may be overwritten by cmd args) */
    opts.duration = 10; /* default to running group for 10 seconds */
    opts.group_name = "simple_group";
    opts.gid_file = NULL;

    /* parse cmdline arguments */
    parse_args(argc, argv, &opts);

#ifdef SSG_HAVE_MPI
    if (strcmp(opts.group_mode, "mpi") == 0)
        MPI_Init(&argc, &argv);
#endif

    /* init margo */
    /* use the main xstream to drive progress & run handlers */
    mid = margo_init(opts.addr_str, MARGO_SERVER_MODE, 0, -1);
    DIE_IF(mid == MARGO_INSTANCE_NULL, "margo_init");

    /* initialize SSG */
    sret = ssg_init(mid);
    DIE_IF(sret != SSG_SUCCESS, "ssg_init");

    /* XXX do we want to use callback for testing anything about group??? */
    if(strcmp(opts.group_mode, "conf") == 0)
        g_id = ssg_group_create_config(opts.group_name, opts.group_addr_conf_file,
            NULL, NULL);
#ifdef SSG_HAVE_MPI
    else if(strcmp(opts.group_mode, "mpi") == 0)
        g_id = ssg_group_create_mpi(opts.group_name, MPI_COMM_WORLD, NULL, NULL);
#endif
    DIE_IF(g_id == SSG_GROUP_ID_NULL, "ssg_group_create");

    /* store the gid if requested */
    if (opts.gid_file)
        ssg_group_id_store(opts.gid_file, g_id);

    /* sleep for given duration to allow group time to run */
    if (opts.duration > 0) margo_thread_sleep(mid, opts.duration * 1000.0);

    /* get my group id and the size of the group */
    my_id = ssg_get_group_self_id(g_id);
    DIE_IF(my_id == SSG_MEMBER_ID_INVALID, "ssg_get_group_self_id");
    group_size = ssg_get_group_size(g_id);
    DIE_IF(group_size == 0, "ssg_get_group_size");
    printf("group member %lu successfully created group (size == %d)\n",
        my_id, group_size);

    /* print group at each member */
    ssg_group_dump(g_id);

    /** cleanup **/
    ssg_group_destroy(g_id);
    ssg_finalize();
    margo_finalize(mid);
#ifdef SSG_HAVE_MPI
    if (strcmp(opts.group_mode, "mpi") == 0)
        MPI_Finalize();
#endif

    return 0;
}
