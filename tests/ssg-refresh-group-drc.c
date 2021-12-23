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
#include <rdmacred.h>

#define DIE_IF(cond_expr, err_fmt, ...) \
    do { \
        if (cond_expr) { \
            fprintf(stderr, "ERROR at %s:%d (" #cond_expr "): " \
                    err_fmt "\n", __FILE__, __LINE__, ##__VA_ARGS__); \
            exit(1); \
        } \
    } while(0)


DECLARE_MARGO_RPC_HANDLER(group_id_forward_recv_ult)

static void usage()
{
    fprintf(stderr,
        "Usage: "
        "ssg-refresh-group-drc <GID>\n"
        "Obtain group view given by GID.\n");
}

static void parse_args(int argc, char *argv[], const char **gid_file)
{
    int ndx = 1;

    if (argc != 2)
    {
        usage();
        exit(1);
    }

    *gid_file = argv[ndx++];

    return;   
}

int main(int argc, char *argv[])
{
    margo_instance_id mid = MARGO_INSTANCE_NULL;
    struct hg_init_info hii;
    const char *gid_file;
    ssg_group_id_t g_id;
    int num_addrs;
    int64_t ssg_cred;
    uint32_t drc_credential_id;
    drc_info_handle_t drc_credential_info;
    uint32_t drc_cookie;
    char drc_key_str[256] = {0};
    char config[1024];
    struct margo_init_info args = {0};
    int ret;

    /* set margo config json string */
    snprintf(config, 1024,
             "{ \"use_progress_thread\" : %s, \"rpc_thread_count\" : %d }",
             "false", -1);

    parse_args(argc, argv, &gid_file);

    memset(&hii, 0, sizeof(hii));

#ifdef SSG_HAVE_MPI
    MPI_Init(&argc, &argv);
#endif
#ifdef SSG_HAVE_PMIX
    pmix_proc_t proc;
    PMIx_Init(&proc, NULL, 0);
#endif

    char transport[256];
    ret = ssg_get_group_transport_from_file(gid_file, transport, 256);
    DIE_IF(ret != SSG_SUCCESS, "ssg_get_group_transport_from_file (%s)", ssg_strerror(ret));

    ret = ssg_get_group_cred_from_file(gid_file, &ssg_cred);
    DIE_IF(ret != SSG_SUCCESS, "ssg_get_group_cred_from_file (%s)", ssg_strerror(ret));
    drc_credential_id = (uint32_t)ssg_cred;

    /* access credential and covert to string for use by mercury */
    ret = drc_access(drc_credential_id, 0, &drc_credential_info);
    DIE_IF(ret != DRC_SUCCESS, "drc_access %u %ld", drc_credential_id, ssg_cred);
    drc_cookie = drc_get_first_cookie(drc_credential_info);
    sprintf(drc_key_str, "%u", drc_cookie);
    hii.na_init_info.auth_key = drc_key_str;

    /* init margo */
    /* use the main xstream to drive progress & run handlers */
    args.json_config = config;
    args.hg_init_info = &hii;
    mid = margo_init_ext(transport, MARGO_CLIENT_MODE, &args);
    DIE_IF(mid == MARGO_INSTANCE_NULL, "margo_init");

    /* initialize SSG */
    ret = ssg_init();
    DIE_IF(ret != SSG_SUCCESS, "ssg_init (%s)", strerror(ret));

    num_addrs = 1;
    ret = ssg_group_id_load(gid_file, &num_addrs, &g_id);
    DIE_IF(ret != SSG_SUCCESS, "ssg_group_id_load (%s)", ssg_strerror(ret));
    DIE_IF(num_addrs != 1, "ssg_group_id_load (%s)", ssg_strerror(ret));

    /* refresh the SSG server group view */
    ret = ssg_group_refresh(mid, g_id);
    DIE_IF(ret != SSG_SUCCESS, "ssg_group_refresh (%s)", ssg_strerror(ret));

    /* have everyone dump their group state */
    ssg_group_dump(g_id);

    /* clean up */
    ssg_group_destroy(g_id);
    margo_finalize(mid);
    ssg_finalize(); /* NOTE: moved after margo_finalize */

#ifdef SSG_HAVE_MPI
    MPI_Finalize();
#endif
#ifdef SSG_HAVE_PMIX
    PMIx_Finalize(NULL, 0);
#endif

    return 0;
}
