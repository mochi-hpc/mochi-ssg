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
            exit(1); \
        } \
    } while(0)

struct group_launch_opts
{
    int duration;
    char *gid_file;
    char *group_mode;
    char *group_addr_conf_file;
};

static void usage()
{
    fprintf(stderr,
        "Usage: "
        "ssg-launch-group [OPTIONS] <addr> <create mode> [config file]\n"
        "\t<addr> - Mercury address string\n"
        "\t<create mode> - \"mpi\" (if supported) or \"conf\"\n"
        "\tif \"conf\" is the mode, then [config file] is required\n");
}

static void parse_args(int argc, char *argv[], struct group_launch_opts *opts)
{
    int c;
    const char *options = "d:f:";
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
            default:
                usage();
                exit(EXIT_FAILURE);
        }
    }

    return;
#if 0
    *addr_str = argv[ndx++];
    *mode = argv[ndx++];

    if (strcmp(*mode, "conf") == 0)
    {
        if (ndx != (argc - 1))
        {
            usage();
            exit(1);
        }
        *conf_file = argv[ndx];
    }
    else if (strcmp(*mode, "mpi") == 0)
    {
#ifdef SSG_HAVE_MPI
        if (ndx != argc)
        {
            usage();
            exit(1);
        }
#else
        fprintf(stderr, "Error: MPI support not built in\n");
        exit(1);
#endif
    
    }
    else
    {
        usage();
        exit(1);
    }
#endif
}

int main(int argc, char *argv[])
{
    margo_instance_id mid = MARGO_INSTANCE_NULL;
    const char *addr_str;
    const char *group_name = "simple_group";
    ssg_group_id_t g_id = SSG_GROUP_ID_NULL;
    ssg_member_id_t my_id;
    int group_size;
    int sret;

    /* XXX default options? */
    struct group_launch_opts opts;
    parse_args(argc, argv, &opts);
    return 0;

#ifdef SSG_HAVE_MPI
    if (strcmp(mode, "mpi") == 0)
        MPI_Init(&argc, &argv);
#endif

    /* init margo */
    /* use the main xstream to drive progress & run handlers */
    mid = margo_init(addr_str, MARGO_SERVER_MODE, 0, -1);
    DIE_IF(mid == MARGO_INSTANCE_NULL, "margo_init");

    /* initialize SSG */
    sret = ssg_init(mid);
    DIE_IF(sret != SSG_SUCCESS, "ssg_init");

    if(strcmp(mode, "conf") == 0)
        g_id = ssg_group_create_config(group_name, conf_file, NULL, NULL);
#ifdef SSG_HAVE_MPI
    else if(strcmp(mode, "mpi") == 0)
        g_id = ssg_group_create_mpi(group_name, MPI_COMM_WORLD, NULL, NULL);
#endif
    DIE_IF(g_id == SSG_GROUP_ID_NULL, "ssg_group_create");

    /* sleep to give all group members a chance to create the group */
    if (sleep_time > 0) margo_thread_sleep(mid, sleep_time * 1000.0);

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
    if (strcmp(mode, "mpi") == 0)
        MPI_Finalize();
#endif

    return 0;
}
