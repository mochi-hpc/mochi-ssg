/*
 * Copyright (c) 2016 UChicago Argonne, LLC
 *
 * See COPYRIGHT in top-level directory.
 */

#include <ssg-config.h>

#define _GNU_SOURCE
#include <unistd.h>
#include <stdio.h>
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
#include <rdmacred.h>

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
        "ssg-launch-group-drc [OPTIONS] <ADDR> <MODE>\n"
        "Create and launch group using given Mercury ADDR string and group create MODE (\"mpi\" or \"pmix\").\n"
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
    struct hg_init_info hii;
    ssg_group_id_t g_id = SSG_GROUP_ID_INVALID;
    ssg_member_id_t my_id;
    ssg_group_config_t g_conf = SSG_GROUP_CONFIG_INITIALIZER;
    int group_size;
    uint32_t drc_credential_id;
    drc_info_handle_t drc_credential_info;
    uint32_t drc_cookie;
    char drc_key_str[256] = {0};
    int ret;

    int rank;

    /* set any default options (that may be overwritten by cmd args) */
    opts.shutdown_time = 10; /* default to running group for 10 seconds */
    opts.group_name = "simple_group";
    opts.gid_file = NULL;

    /* parse cmdline arguments */
    parse_args(argc, argv, &opts);

    memset(&hii, 0, sizeof(hii));

#ifdef SSG_HAVE_MPI
    int mpi_rank, mpi_size;
    if (strcmp(opts.group_mode, "mpi") == 0)
    {
        MPI_Init(&argc, &argv);
        MPI_Comm_rank(MPI_COMM_WORLD, &mpi_rank);
        MPI_Comm_size(MPI_COMM_WORLD, &mpi_size);

        /* acquire DRC cred on MPI rank 0 */
        if (mpi_rank == 0)
        {
            ret = drc_acquire(&drc_credential_id, DRC_FLAGS_FLEX_CREDENTIAL);
            DIE_IF(ret != DRC_SUCCESS, "drc_acquire");
        }
        MPI_Bcast(&drc_credential_id, 1, MPI_UINT32_T, 0, MPI_COMM_WORLD);
        rank = mpi_rank;
    }
#endif
#ifdef SSG_HAVE_PMIX
    pmix_status_t p_ret;
    pmix_proc_t proc, tmp_proc;
    char *drc_pmix_key;
    pmix_value_t value;
    pmix_value_t *val_p;
    pmix_info_t *info;
    bool flag;

    if (strcmp(opts.group_mode, "pmix") == 0)
    {
        p_ret = PMIx_Init(&proc, NULL, 0);
        DIE_IF(p_ret != PMIX_SUCCESS, "PMIx_Init");

        ret = asprintf(&drc_pmix_key, "ssg-drc-%s", proc.nspace);
        DIE_IF(ret <= 0, "asprintf");

        /* acquire DRC cred on PMIx rank 0, then put in global KV */
        if (proc.rank == 0)
        {
            p_ret = drc_acquire(&drc_credential_id, DRC_FLAGS_FLEX_CREDENTIAL);
            DIE_IF(p_ret != DRC_SUCCESS, "drc_acquire");

            PMIX_VALUE_LOAD(&value, &drc_credential_id, PMIX_UINT32);
            p_ret = PMIx_Put(PMIX_GLOBAL, drc_pmix_key, &value);
            DIE_IF(p_ret != PMIX_SUCCESS, "PMIx_Put");

            p_ret = PMIx_Commit();
            DIE_IF(p_ret != PMIX_SUCCESS, "PMIx_Commit");
        }

        /* fence and get the credential */
        PMIX_INFO_CREATE(info, 1);
        flag = true;
        PMIX_INFO_LOAD(info, PMIX_COLLECT_DATA, &flag, PMIX_BOOL);
        p_ret = PMIx_Fence(&proc, 1, info, 1);
        PMIX_INFO_FREE(info, 1);
        DIE_IF(p_ret != PMIX_SUCCESS, "PMIx_Fence");

        PMIX_PROC_LOAD(&tmp_proc, proc.nspace, 0);
        p_ret = PMIx_Get(&tmp_proc, drc_pmix_key, NULL, 0, &val_p);
        DIE_IF(p_ret != PMIX_SUCCESS, "PMIx_Get");
        drc_credential_id = val_p->data.uint32;
        PMIX_VALUE_RELEASE(val_p);

        free(drc_pmix_key);

        rank = proc.rank;
    }
#endif

    /* access credential on all ranks and convert to string for use by mercury */
    ret = drc_access(drc_credential_id, 0, &drc_credential_info);
    DIE_IF(ret != DRC_SUCCESS, "drc_access");
    drc_cookie = drc_get_first_cookie(drc_credential_info);
    sprintf(drc_key_str, "%u", drc_cookie);
    hii.na_init_info.auth_key = drc_key_str;

    /* rank 0 grants access to the credential, allowing other jobs to use it */
    if(rank == 0)
    {
        ret = drc_grant(drc_credential_id, drc_get_wlm_id(), DRC_FLAGS_TARGET_WLM);
        DIE_IF(ret != DRC_SUCCESS, "drc_grant");
    }

    /* init margo */
    /* use the main xstream to drive progress & run handlers */
    mid = margo_init_opt(opts.addr_str, MARGO_SERVER_MODE, &hii, 0, -1);
    DIE_IF(mid == MARGO_INSTANCE_NULL, "margo_init");

    /* initialize SSG */
    ret = ssg_init();
    DIE_IF(ret != SSG_SUCCESS, "ssg_init");

    /* set non-default group config parameters */
    g_conf.swim_period_length_ms = 1000; /* 1-second period length */
    g_conf.swim_suspect_timeout_periods = 4; /* 4-period suspicion length */
    g_conf.swim_subgroup_member_count = 3; /* 3-member subgroups for SWIM */
    g_conf.ssg_credential = (int64_t)drc_credential_id; /* DRC credential for group */

    /* XXX do we want to use callback for testing anything about group??? */
#ifdef SSG_HAVE_MPI
    if(strcmp(opts.group_mode, "mpi") == 0)
        g_id = ssg_group_create_mpi(mid, opts.group_name, MPI_COMM_WORLD, &g_conf,
            NULL, NULL);
#endif
#ifdef SSG_HAVE_PMIX
    if(strcmp(opts.group_mode, "pmix") == 0)
        g_id = ssg_group_create_pmix(mid, opts.group_name, proc, &g_conf,
            NULL, NULL);
#endif
    DIE_IF(g_id == SSG_GROUP_ID_INVALID, "ssg_group_create on rank %d", rank);

    /* store the gid if requested */
    if (opts.gid_file)
        ssg_group_id_store(opts.gid_file, g_id);

    /* sleep for given duration to allow group time to run */
    if (opts.shutdown_time > 0)
        margo_thread_sleep(mid, opts.shutdown_time * 1000.0);

    /* get my group id and the size of the group */
    my_id = ssg_get_self_id(mid);
    DIE_IF(my_id == SSG_MEMBER_ID_INVALID, "ssg_get_group_self_id");
    group_size = ssg_get_group_size(g_id);
    DIE_IF(group_size == 0, "ssg_get_group_size");

    /* print group at each member */
    ssg_group_dump(g_id);
    ssg_group_destroy(g_id);

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
