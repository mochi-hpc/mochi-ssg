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
    int kill_dur;
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
        "\t-r <rank>\t\tProcess rank to kill\n"
        "\t-t <TIME>\t\tTime duration (in seconds) to keep dead member down\n"
);
}

static void parse_args(int argc, char *argv[], struct group_launch_opts *opts)
{
    int c;
    const char *options = "s:k:r:t:";
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
            case 't':
                opts->kill_dur = (int)strtol(optarg, &check, 0);
                if (opts->kill_dur < 0 || (check && *check != '\0'))
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

    if ((opts->kill_time + opts->kill_dur) >= opts->shutdown_time)
    {
        usage();
        exit(EXIT_FAILURE);
    }

    return;
}

struct ssg_cb_dat
{
    ssg_group_id_t gid;
    margo_instance_id mid;
};

void ssg_member_update(
    void * group_data,
    ssg_member_id_t member_id,
    ssg_member_update_type_t update_type)
{
    struct ssg_cb_dat *cb_dat_p = (struct ssg_cb_dat *)group_data;
    int ret, gsize;
    ssg_member_id_t self_id;
    char *str;

    if (update_type == SSG_MEMBER_JOINED)
        str = "JOINED";
    else if (update_type == SSG_MEMBER_LEFT)
        str = "LEFT";
    else if (update_type == SSG_MEMBER_DIED)
        str = "DIED";
    else
        assert(0);

    ret = ssg_get_group_size(cb_dat_p->gid, &gsize);
    assert(ret == SSG_SUCCESS);

    ret = ssg_get_self_id(cb_dat_p->mid, &self_id);
    assert(ret == SSG_SUCCESS);

    if (self_id == member_id)
        fprintf(stderr, "*** SELF %s, new group size = %d\n", str, gsize);
    else
        fprintf(stderr, "*** member %lu %s, new group size = %d\n", member_id, str, gsize);

    return;
}

int main(int argc, char *argv[])
{
    struct group_launch_opts opts;
    int my_rank;
    margo_instance_id mid = MARGO_INSTANCE_NULL;
    ssg_member_id_t my_id;
    int group_size;
    struct ssg_cb_dat cb_dat;
    int sret;

    /* set any default options (that may be overwritten by cmd args) */
    opts.shutdown_time = 30; /* default to running group for 30 seconds */
    opts.kill_time = 10; /* default to kill process in 10 seconds */
    opts.kill_rank = 0; /* default to kill rank 0 */
    opts.kill_dur = 10; /* default to 10 second kill duration */

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
    mid = margo_init(opts.addr_str, MARGO_SERVER_MODE, 0, -1);
    DIE_IF(mid == MARGO_INSTANCE_NULL, "margo_init");

    /* initialize SSG */
    sret = ssg_init();
    DIE_IF(sret != SSG_SUCCESS, "ssg_init (%s)", strerror(sret));

    cb_dat.mid = mid;
#ifdef SSG_HAVE_MPI
    if(strcmp(opts.group_mode, "mpi") == 0)
        sret = ssg_group_create_mpi(mid, "fail_group", MPI_COMM_WORLD, NULL,
            ssg_member_update, &cb_dat, &cb_dat.gid);
#endif
#ifdef SSG_HAVE_PMIX
    if(strcmp(opts.group_mode, "pmix") == 0)
        sret = ssg_group_create_pmix(mid, "fail_group", proc, NULL,
            ssg_member_update, &cb_dat, &cb_dat.gid);
#endif
    DIE_IF(cb_dat.gid == SSG_GROUP_ID_INVALID, "ssg_group_create (%s)", strerror(sret));

    /* get my group id and the size of the group */
    sret = ssg_get_self_id(mid, &my_id);
    DIE_IF(sret != SSG_SUCCESS, "ssg_get_self_id (%s)", strerror(sret));
    sret = ssg_get_group_size(cb_dat.gid, &group_size);
    DIE_IF(sret != SSG_SUCCESS, "ssg_get_group_size (%s)", strerror(sret));

    if (my_rank == opts.kill_rank)
    {
        /* sleep for given duration before killing rank */
        margo_thread_sleep(mid, opts.kill_time * 1000.0);

        fprintf(stderr, "[%.6lf] KILL member %lu (rank %d)\n", ABT_get_wtime(), my_id, my_rank);
        fflush(stderr);

        /* XXX use sleep to make the killed member unresponsive for specified time duration */
        sleep(opts.kill_dur);

        fprintf(stderr, "[%.6lf] REVIVE member %lu (rank %d)\n", ABT_get_wtime(), my_id, my_rank);
        fflush(stderr);

        /* sleep for given duration before killing rank */
        margo_thread_sleep(mid, (opts.shutdown_time - opts.kill_time - opts.kill_dur) * 1000.0);

        /* print final group state on failed member */
        ssg_group_dump(cb_dat.gid);

        ssg_group_destroy(cb_dat.gid);
        ssg_finalize();
        margo_finalize(mid);
    }
    else
    {
        /* sleep for given duration to allow group time to run */
        margo_thread_sleep(mid, opts.shutdown_time * 1000.0);

        /* print group at each alive member */
        ssg_group_dump(cb_dat.gid);

        /** cleanup **/
        ssg_group_destroy(cb_dat.gid);
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
