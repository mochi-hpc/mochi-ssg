/*
 * Copyright (c) 2017 UChicago Argonne, LLC
 *
 * See COPYRIGHT in top-level directory.
 */

#include "ssg-config.h"

#include <unistd.h>
#include <stdio.h>
#include <string.h>
#include <assert.h>

#include <mpi.h>

#include <margo.h>
#ifdef HAVE_ABT_SNOOZER
#include <abt-snoozer.h>
#endif
#include <mercury.h>
#include <abt.h>
#include <ssg.h>
#include <ssg-mpi.h>

struct options
{
    int iterations;
    int snoozer_flag_client;
    int snoozer_flag_server;
    unsigned int mercury_timeout_client;
    unsigned int mercury_timeout_server;
    char* diag_file_name;
    char* na_transport;
};

static void parse_args(int argc, char **argv, struct options *opts);
static void usage(void);
static int run_benchmark(int iterations, hg_id_t id, ssg_member_id_t target, 
    ssg_group_id_t gid, margo_instance_id mid, double *measurement_array);
static void bench_routine_print(const char* op, int size, int iterations, 
    double* measurement_array);
static int measurement_cmp(const void* a, const void *b);
DECLARE_MARGO_RPC_HANDLER(noop_ult);

static hg_id_t noop_id;
static int rpcs_serviced = 0;
static ABT_eventual rpcs_serviced_eventual;
static struct options g_opts;

int main(int argc, char **argv) 
{
    margo_instance_id mid;
    int nranks;
    hg_context_t *hg_context;
    hg_class_t *hg_class;
    ABT_xstream xstream;
    ABT_pool pool;
    int ret;
    ssg_group_id_t gid;
    ssg_member_id_t self;
    int rank;
    double *measurement_array;
    int namelen;
    char processor_name[MPI_MAX_PROCESSOR_NAME];

    ABT_init(argc, argv);
    MPI_Init(&argc, &argv);

    /* 2 process rtt measurements only */
    MPI_Comm_size(MPI_COMM_WORLD, &nranks);
    if(nranks != 2)
    {
        usage();
        exit(EXIT_FAILURE);
    }
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Get_processor_name(processor_name,&namelen);
    printf("Process %d of %d is on %s\n",
	rank, nranks, processor_name);

    parse_args(argc, argv, &g_opts);

    /* boilerplate HG initialization steps */
    /***************************************/
    hg_class = HG_Init(g_opts.na_transport, HG_TRUE);
    if(!hg_class)
    {
        fprintf(stderr, "Error: HG_Init()\n");
        return(-1);
    }
    hg_context = HG_Context_create(hg_class);
    if(!hg_context)
    {
        fprintf(stderr, "Error: HG_Context_create()\n");
        HG_Finalize(hg_class);
        return(-1);
    }

    if((rank == 0 && g_opts.snoozer_flag_client) || 
        (rank == 1 && g_opts.snoozer_flag_server))
    {
#ifdef HAVE_ABT_SNOOZER
        /* set primary ES to idle without polling in scheduler */
        ret = ABT_snoozer_xstream_self_set();
        if(ret != 0)
        {
            fprintf(stderr, "Error: ABT_snoozer_xstream_self_set()\n");
            return(-1);
        }
#else
        fprintf(stderr, "Error: abt-snoozer scheduler is not supported\n");
        return(-1);
#endif
    }

    /* get main pool for running mercury progress and RPC handlers */
    ret = ABT_xstream_self(&xstream);
    if(ret != 0)
    {
        fprintf(stderr, "Error: ABT_xstream_self()\n");
        return(-1);
    }   
    ret = ABT_xstream_get_main_pools(xstream, 1, &pool);
    if(ret != 0)
    {
        fprintf(stderr, "Error: ABT_xstream_get_main_pools()\n");
        return(-1);
    }

    /* actually start margo */
    mid = margo_init_pool(pool, pool, hg_context);
    assert(mid);

    if(g_opts.diag_file_name)
        margo_diag_start(mid);

    /* adjust mercury timeout in Margo if requested */
    if(rank == 0 && g_opts.mercury_timeout_client != UINT_MAX)
        margo_set_param(mid, MARGO_PARAM_PROGRESS_TIMEOUT_UB, &g_opts.mercury_timeout_client);
    if(rank == 1 && g_opts.mercury_timeout_server != UINT_MAX)
        margo_set_param(mid, MARGO_PARAM_PROGRESS_TIMEOUT_UB, &g_opts.mercury_timeout_server);

    noop_id = MARGO_REGISTER_MPLEX(
        mid, 
        "noop_rpc", 
        void,
        void,
        noop_ult,
        MARGO_DEFAULT_MPLEX_ID,
        NULL);

    /* set up group */
    ret = ssg_init(mid);
    assert(ret == 0);
    gid = ssg_group_create_mpi("margo-p2p-latency", MPI_COMM_WORLD, NULL, NULL);
    assert(gid != SSG_GROUP_ID_NULL);

    assert(ssg_get_group_size(gid) == 2);

    self = ssg_get_group_self_id(gid);
#if 0
    printf("MPI rank %d has SSG ID %lu\n", rank, self);
#endif

    if(self == 0)
    {
        /* ssg id 0 runs benchmark */

        measurement_array = calloc(g_opts.iterations, sizeof(*measurement_array));
        assert(measurement_array);

        ret = run_benchmark(g_opts.iterations, noop_id, 1, gid, mid, measurement_array);
        assert(ret == 0);

        printf("# <op> <iterations> <size> <min> <q1> <med> <avg> <q3> <max>\n");
        bench_routine_print("noop", 0, g_opts.iterations, measurement_array);
        free(measurement_array);
    }
    else
    {
        /* ssg id 1 acts as server, waiting until iterations have been
         * completed
         */

        ret = ABT_eventual_create(0, &rpcs_serviced_eventual);
        assert(ret == 0);

        ABT_eventual_wait(rpcs_serviced_eventual, NULL);
        assert(rpcs_serviced == g_opts.iterations);
        sleep(3);
    }

    ssg_group_destroy(gid);
    ssg_finalize();

    if(g_opts.diag_file_name)
        margo_diag_dump(mid, g_opts.diag_file_name, 1);

    margo_finalize(mid);
    HG_Context_destroy(hg_context);
    HG_Finalize(hg_class);
    MPI_Finalize();
    ABT_finalize();

    return 0;
}

static void parse_args(int argc, char **argv, struct options *opts)
{
    int opt;
    int ret;
    char clientflag, serverflag;

    memset(opts, 0, sizeof(*opts));

#ifdef HAVE_ABT_SNOOZER
    /* default to enabling snoozer scheduler on both client and server */
    opts->snoozer_flag_client = 1;
    opts->snoozer_flag_server = 1;
#else
    opts->snoozer_flag_client = 0;
    opts->snoozer_flag_server = 0;
#endif
    /* default to using whatever the standard timeout is in margo */
    opts->mercury_timeout_client = UINT_MAX;
    opts->mercury_timeout_server = UINT_MAX; 

    while((opt = getopt(argc, argv, "n:i:d:s:t:")) != -1)
    {
        switch(opt)
        {
            case 'd':
                opts->diag_file_name = strdup(optarg);
                if(!opts->diag_file_name)
                {
                    perror("strdup");
                    exit(EXIT_FAILURE);
                }
                break;
            case 'i':
                ret = sscanf(optarg, "%d", &opts->iterations);
                if(ret != 1)
                {
                    usage();
                    exit(EXIT_FAILURE);
                }
                break;
            case 's':
                ret = sscanf(optarg, "%c,%c", &clientflag, &serverflag);
                if(ret != 2)
                {
                    usage();
                    exit(EXIT_FAILURE);
                }
                if(clientflag == '0') opts->snoozer_flag_client = 0;
                else if(clientflag == '1') opts->snoozer_flag_client = 1;
                if(serverflag == '0') opts->snoozer_flag_server = 0;
                else if(serverflag == '1') opts->snoozer_flag_server = 1;
                break;
            case 't':
                ret = sscanf(optarg, "%u,%u", &opts->mercury_timeout_client, &opts->mercury_timeout_server);
                if(ret != 2)
                {
                    usage();
                    exit(EXIT_FAILURE);
                }
                break;
            case 'n':
                opts->na_transport = strdup(optarg);
                if(!opts->na_transport)
                {
                    perror("strdup");
                    exit(EXIT_FAILURE);
                }
                break;
            default:
                usage();
                exit(EXIT_FAILURE);
        }
    }

    if(opts->iterations < 1 || !opts->na_transport)
    {
        usage();
        exit(EXIT_FAILURE);
    }

    return;
}

static void usage(void)
{
    fprintf(stderr,
        "Usage: "
        "margo-p2p-latency -i <iterations> -n <na>\n"
        "\t-i <iterations> - number of RPC iterations\n"
        "\t-n <na> - na transport\n"
        "\t[-d filename] - enable diagnostics output \n"
        "\t[-s <bool,bool>] - specify if snoozer scheduler is used on client and server\n"
        "\t\t(e.g., -s 0,1 means snoozer disabled on client and enabled on server)\n"
        "\t\texample: mpiexec -n 2 ./margo-p2p-latency -i 10000 -n verbs://\n"
        "\t\t(must be run with exactly 2 processes\n");
    
    return;
}


/* service a remote RPC for a no-op */
static void noop_ult(hg_handle_t handle)
{
    margo_respond(handle, NULL);
    margo_destroy(handle);

    rpcs_serviced++;
    if(rpcs_serviced == g_opts.iterations)
    {
        ABT_eventual_set(rpcs_serviced_eventual, NULL, 0);
    }

    return;
}
DEFINE_MARGO_RPC_HANDLER(noop_ult)

static int run_benchmark(int iterations, hg_id_t id, ssg_member_id_t target, 
    ssg_group_id_t gid, margo_instance_id mid, double *measurement_array)
{
    hg_handle_t handle;
    hg_addr_t target_addr;
    int i;
    int ret;
    double tm1, tm2;

    target_addr = ssg_get_addr(gid, target);
    assert(target_addr != HG_ADDR_NULL);

    ret = margo_create(mid, target_addr, id, &handle);
    assert(ret == 0);

    /* TODO: have command line option to toggle whether we reuse one handle
     * or create/release on every cycle
     */
    for(i=0; i<iterations; i++)
    {
        tm1 = ABT_get_wtime();
        ret = margo_forward(handle, NULL);
        tm2 = ABT_get_wtime();
        assert(ret == 0);
        measurement_array[i] = tm2-tm1;
    }

    margo_destroy(handle);

    return(0);
}

static void bench_routine_print(const char* op, int size, int iterations, double* measurement_array)
{
    double min, max, q1, q3, med, avg, sum;
    int bracket1, bracket2;
    int i;

    qsort(measurement_array, iterations, sizeof(double), measurement_cmp);

    min = measurement_array[0];
    max = measurement_array[iterations-1];

    sum = 0;
    for(i=0; i<iterations; i++)
    {
        sum += measurement_array[i];
    }
    avg = sum/(double)iterations;

    bracket1 = iterations/2;
    if(iterations%2)
        bracket2 = bracket1 + 1;
    else
        bracket2 = bracket1;
    med = (measurement_array[bracket1] + measurement_array[bracket2])/(double)2;

    bracket1 = iterations/4;
    if(iterations%4)
        bracket2 = bracket1 + 1;
    else
        bracket2 = bracket1;
    q1 = (measurement_array[bracket1] + measurement_array[bracket2])/(double)2;

    bracket1 *= 3;
    if(iterations%4)
        bracket2 = bracket1 + 1;
    else
        bracket2 = bracket1;
    q3 = (measurement_array[bracket1] + measurement_array[bracket2])/(double)2;

    printf("%s\t%d\t%d\t%.9f\t%.9f\t%.9f\t%.9f\t%.9f\t%.9f\n", op, iterations, size, min, q1, med, avg, q3, max);
#if 0
    for(i=0; i<iterations; i++)
    {
        printf("\t%.9f", measurement_array[i]);
    }
    printf("\n");
#endif
    fflush(NULL);

    return;
}

static int measurement_cmp(const void* a, const void *b)
{
    const double *d_a = a;
    const double *d_b = b;

    if(*d_a < *d_b)
        return(-1);
    else if(*d_a > *d_b)
        return(1);
    else
        return(0);
}


