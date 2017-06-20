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
#include <mercury.h>
#include <abt.h>
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

static void usage()
{
    fprintf(stderr,
        "Usage: "
        "ssg-test-simple [-s <time>] <addr> <create mode> [config file]\n"
        "\t-s <time> - time to sleep between init/finalize\n"
        "\t<create mode> - \"mpi\" (if supported) or \"conf\"\n"
        "\tif \"conf\" is the mode, then [config file] is required\n");
}

static void parse_args(int argc, char *argv[], int *sleep_time, const char **addr_str,
    const char **mode, const char **conf_file)
{
    int ndx = 1;

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
        if(*sleep_time < 0 || (check && *check != '\0') || argc < 5)
        {
            usage();
            exit(1);
        }
    }

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

    return;   
}

int main(int argc, char *argv[])
{
    hg_class_t *hgcl = NULL;
    hg_context_t *hgctx = NULL;
    margo_instance_id mid = MARGO_INSTANCE_NULL;
    int sleep_time = 0;
    const char *addr_str;
    const char *mode;
    const char *conf_file;
    const char *group_name = "simple_group";
    ssg_group_id_t g_id;
    ssg_member_id_t my_id;
    int group_size;
    int sret;

    parse_args(argc, argv, &sleep_time, &addr_str, &mode, &conf_file);

    ABT_init(argc, argv);

#ifdef SSG_HAVE_MPI
    if (strcmp(mode, "mpi") == 0)
        MPI_Init(&argc, &argv);
#endif

    /* init HG */
    hgcl = HG_Init(addr_str, HG_TRUE);
    DIE_IF(hgcl == NULL, "HG_Init");
    hgctx = HG_Context_create(hgcl);
    DIE_IF(hgctx == NULL, "HG_Context_create");

    /* init margo in single threaded mode */
    mid = margo_init(0, -1, hgctx);
    DIE_IF(mid == MARGO_INSTANCE_NULL, "margo_init");

    /* initialize SSG */
    sret = ssg_init(mid);
    DIE_IF(sret != SSG_SUCCESS, "ssg_init");

    if(strcmp(mode, "conf") == 0)
        sret = ssg_group_create_config(group_name, conf_file, &g_id);
#ifdef SSG_HAVE_MPI
    else if(strcmp(mode, "mpi") == 0)
        sret = ssg_group_create_mpi(group_name, MPI_COMM_WORLD, &g_id);
#endif
    DIE_IF(sret != SSG_SUCCESS, "ssg_group_create");

    /* sleep to give all group members a chance to create the group */
    if (sleep_time > 0) margo_thread_sleep(mid, sleep_time * 1000.0);

    /* get my group id and the size of the group */
    my_id = ssg_get_group_self_id(g_id);
    DIE_IF(my_id == SSG_MEMBER_ID_NULL, "ssg_get_group_self_id");
    group_size = ssg_get_group_size(g_id);
    DIE_IF(group_size == 0, "ssg_get_group_size");
    printf("group member %d of %d successfully created group\n",
        (int)my_id, group_size);

    /** cleanup **/

    ssg_group_destroy(g_id);
    ssg_finalize();

    margo_finalize(mid);

    if(hgctx) HG_Context_destroy(hgctx);
    if(hgcl) HG_Finalize(hgcl);

#ifdef SSG_HAVE_MPI
    if (strcmp(mode, "mpi") == 0)
        MPI_Finalize();
#endif

    ABT_finalize();

    return 0;
}
