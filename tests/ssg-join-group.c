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

struct group_join_opts
{
    char *addr_str;
    int duration;
    char *gid_file;
};

static void usage()
{
    fprintf(stderr,
        "Usage: "
        "ssg-join-group [OPTIONS] <ADDR> <GID>\n"
        "Join an existing group given by GID using Mercury address ADDR.\n"
        "\n"
        "OPTIONS:\n"
        "\t-d DUR\t\tSpecify a time duration (in seconds) to run the group for\n");
}

static void parse_args(int argc, char *argv[], struct group_join_opts *opts)
{
    int c;
    const char *options = "d:";
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

    opts->addr_str = argv[optind++];
    opts->gid_file = argv[optind++];

    return;
}

int main(int argc, char *argv[])
{
    struct group_join_opts opts;
    margo_instance_id mid = MARGO_INSTANCE_NULL;
    ssg_group_id_t in_g_id = SSG_GROUP_ID_NULL;
    ssg_group_id_t out_g_id = SSG_GROUP_ID_NULL;
    ssg_member_id_t my_id;
    int group_size;
    int sret;

    /* set any default options (that may be overwritten by cmd args) */
    opts.duration = 10; /* default to running for 10 seconds */

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

    /* XXX do we want to use callback for testing anything about group??? */
    out_g_id = ssg_group_join(in_g_id, NULL, NULL);
    DIE_IF(out_g_id == SSG_GROUP_ID_NULL, "ssg_group_join");
    ssg_group_id_free(in_g_id);

    /* sleep for given duration to allow group time to run */
    if (opts.duration > 0) margo_thread_sleep(mid, opts.duration * 1000.0);

    /* get my group id and the size of the group */
    my_id = ssg_get_group_self_id(out_g_id);
    DIE_IF(my_id == SSG_MEMBER_ID_INVALID, "ssg_get_group_self_id");
    group_size = ssg_get_group_size(out_g_id);
    DIE_IF(group_size == 0, "ssg_get_group_size");
    printf("group member %lu successfully created group (size == %d)\n",
        my_id, group_size);

    /* print group at each member */
    ssg_group_dump(out_g_id);

    /** cleanup **/
    ssg_group_destroy(out_g_id);
    ssg_finalize();
    margo_finalize(mid);

    return 0;
}
