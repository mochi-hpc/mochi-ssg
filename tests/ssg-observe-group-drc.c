/*
 * Copyright (c) 2016 UChicago Argonne, LLC
 *
 * See COPYRIGHT in top-level directory.
 */

#include <ssg-config.h>

#include <unistd.h>
#include <stdio.h>
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
        "ssg-observe-group-drc [-s <time>] <addr> <GID>\n"
        "Observe group given by GID using Mercury address ADDR.\n"
        "\t-s <time> - time to sleep between SSG group operations\n");
}

static void parse_args(int argc, char *argv[], int *sleep_time, const char **addr_str, const char **gid_file)
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
        if(*sleep_time < 0 || (check && *check != '\0') || argc < 4)
        {
            usage();
            exit(1);
        }
    }

    *addr_str = argv[ndx++];
    *gid_file = argv[ndx++];

    return;   
}

int main(int argc, char *argv[])
{
    margo_instance_id mid = MARGO_INSTANCE_NULL;
    struct hg_init_info hii;
    int sleep_time = 0;
    const char *addr_str;
    const char *gid_file;
    ssg_group_id_t g_id;
    int64_t ssg_cred;
    uint32_t drc_credential_id;
    drc_info_handle_t drc_credential_info;
    uint32_t drc_cookie;
    char drc_key_str[256] = {0};
    int ret;

    parse_args(argc, argv, &sleep_time, &addr_str, &gid_file);

    memset(&hii, 0, sizeof(hii));

#ifdef SSG_HAVE_MPI
    MPI_Init(&argc, &argv);
#endif
#ifdef SSG_HAVE_PMIX
    pmix_proc_t proc;
    PMIx_Init(&proc, NULL, 0);
#endif

    /* initialize SSG */
    /* NOTE: we move SSG initialization ahead of margo_init here -- margo needs
     * to be configured to use the DRC credential to allow cross-job communication,
     * but we can't get the credential witout initializing SSG and loading the group
     */
    ret = ssg_init();
    DIE_IF(ret != SSG_SUCCESS, "ssg_init");

    ret = ssg_group_id_load(gid_file, &g_id);
    DIE_IF(ret != SSG_SUCCESS, "ssg_group_id_load");

    ssg_cred = ssg_group_id_get_cred(g_id);
    DIE_IF(ssg_cred == -1, "ssg_group_id_get_cred");
    drc_credential_id = (uint32_t)ssg_cred;

    /* access credential and covert to string for use by mercury */
    ret = drc_access(drc_credential_id, 0, &drc_credential_info);
    DIE_IF(ret != DRC_SUCCESS, "drc_access %u %ld", drc_credential_id, ssg_cred);
    drc_cookie = drc_get_first_cookie(drc_credential_info);
    sprintf(drc_key_str, "%u", drc_cookie);
    hii.na_init_info.auth_key = drc_key_str;

    /* init margo */
    /* use the main xstream to drive progress & run handlers */
    mid = margo_init_opt(addr_str, MARGO_CLIENT_MODE, &hii, 0, -1);
    DIE_IF(mid == MARGO_INSTANCE_NULL, "margo_init");

    /* start observging the SSG server group */
    ret = ssg_group_observe(mid, g_id);
    DIE_IF(ret != SSG_SUCCESS, "ssg_group_observe");

    /* for now, just sleep to give observer a chance to establish connection */
    /* XXX: we could replace this with a barrier eventually */
    if (sleep_time > 0) margo_thread_sleep(mid, sleep_time * 1000.0);

    /* have everyone dump their group state */
    ssg_group_dump(g_id);

    /* clean up */
    ssg_group_unobserve(g_id);
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
