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
#include <mercury.h>
#include <abt.h>
#include <ssg.h>

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
        "ssg-test-simple <addr> <create mode> [config file]\n"
        "\t<create mode> - \"mpi\" (if supported) or \"conf\"\n"
        "\tif \"conf\" is the mode, then [config file] is required\n");
}

static void parse_args(int argc, char *argv[], const char **addr_str,
    const char **mode, const char **conf_file)
{
    if (argc < 3)
    {
        usage();
        exit(1);
    }

    *addr_str = argv[1];
    *mode = argv[2];

    if (strcmp(*mode, "conf") == 0)
    {
        if (argc != 4)
        {
            usage();
            exit(1);
        }
        *conf_file = argv[3];
    }
    else if (strcmp(*mode, "mpi") == 0)
    {
#ifdef HAVE_MPI
        if (argc != 3)
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
    const char *addr_str;
    const char *mode;
    const char *conf_file;
    const char *group_name = "simple_group";
    ssg_group_id_t g_id = SSG_GROUP_ID_NULL;
    int ret;

    parse_args(argc, argv, &addr_str, &mode, &conf_file);

    ABT_init(argc, argv);

    if (strcmp(mode, "mpi") == 0)
        MPI_Init(&argc, &argv);

    /* init HG */
    hgcl = HG_Init(addr_str, HG_TRUE);
    DIE_IF(hgcl == NULL, "HG_Init");
    hgctx = HG_Context_create(hgcl);
    DIE_IF(hgctx == NULL, "HG_Context_create");

    /* init margo in single threaded mode */
    mid = margo_init(0, -1, hgctx);
    DIE_IF(mid == MARGO_INSTANCE_NULL, "margo_init");

    /* initialize SSG */
    ret = ssg_init(mid);
    DIE_IF(ret != SSG_SUCCESS, "ssg_init");

    if(strcmp(mode, "conf") == 0)
        g_id = ssg_group_create_config(group_name, conf_file);
    else if(strcmp(mode, "mpi") == 0)
        g_id = ssg_group_create_mpi(group_name, MPI_COMM_WORLD);
    // XXX DIE_IF(g_id == SSG_GROUP_ID_NULL, "ssg_group_create");

cleanup:
    /* cleanup */
    ssg_group_destroy(g_id);
    ssg_finalize();

    margo_finalize(mid);

    if(hgctx) HG_Context_destroy(hgctx);
    if(hgcl) HG_Finalize(hgcl);

    if (strcmp(mode, "mpi") == 0)
        MPI_Finalize();

    ABT_finalize();

    return 0;
}
