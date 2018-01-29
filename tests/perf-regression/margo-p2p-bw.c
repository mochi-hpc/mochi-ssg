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
    int xfer_size;
    int duration_seconds;
    int concurrency;
    int threads;
    int snoozer_flag_client;
    int snoozer_flag_server;
    unsigned int mercury_timeout_client;
    unsigned int mercury_timeout_server;
    char* diag_file_name;
    char* na_transport;
};

#define BW_TOTAL_MEM_SIZE 2097152UL

static void parse_args(int argc, char **argv, struct options *opts);
static void usage(void);

MERCURY_GEN_PROC(bw_rpc_in_t,
        ((hg_bulk_t)(bulk_handle)))
DECLARE_MARGO_RPC_HANDLER(bw_ult);

static int run_benchmark(hg_id_t id, ssg_member_id_t target, 
    ssg_group_id_t gid, margo_instance_id mid);

struct bw_worker_arg
{
    double start_tm;
    margo_instance_id mid;
    ABT_mutex *cur_off_mutex;
    size_t *cur_off;
    hg_bulk_t *client_bulk_handle;
    const hg_addr_t *target_addr;
    hg_size_t bytes_moved;
    double end_ts;
};

static void bw_worker(void *_arg);

static hg_id_t g_bw_id;
static ABT_pool g_transfer_pool;
static ABT_eventual g_bw_done_eventual;
static struct options g_opts;
static char *g_buffer = NULL;
static hg_size_t g_buffer_size = BW_TOTAL_MEM_SIZE;
static hg_bulk_t g_bulk_handle = HG_BULK_NULL;

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
    int namelen;
    char processor_name[MPI_MAX_PROCESSOR_NAME];
    ABT_xstream *bw_worker_xstreams = NULL;

    ABT_init(argc, argv);
    MPI_Init(&argc, &argv);

    /* 2 process one-way bandwidth measurement only */
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

    /* allocate one big buffer for rdma transfers */
    g_buffer = calloc(g_buffer_size, 1);
    if(!g_buffer)
    {
        perror("calloc");
        fprintf(stderr, "Error: unable to allocate %lu byte buffer.\n", g_buffer_size);
        return(-1);
    }

    /* boilerplate HG initialization steps */
    /***************************************/

    if((rank == 0 && g_opts.mercury_timeout_client == 0) ||
       (rank == 1 && g_opts.mercury_timeout_server == 0))
    {
        struct hg_init_info hii;
        
        /* If mercury timeout of zero is requested, then set
         * init option to NO_BLOCK.  This allows some transports to go
         * faster because they do not have to set up or maintain the data
         * structures necessary for signaling completion on blocked
         * operations.
         */
        memset(&hii, 0, sizeof(hii));
        hii.na_init_info.progress_mode = NA_NO_BLOCK;
        hg_class = HG_Init_opt(g_opts.na_transport, HG_TRUE, &hii);
    }
    else
    {
        hg_class = HG_Init(g_opts.na_transport, HG_TRUE);
    }
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

    g_bw_id = MARGO_REGISTER_MPLEX(
        mid, 
        "bw_rpc", 
        bw_rpc_in_t,
        void,
        bw_ult,
        MARGO_DEFAULT_MPLEX_ID,
        NULL);

    /* set up group */
    ret = ssg_init(mid);
    assert(ret == 0);
    gid = ssg_group_create_mpi("margo-p2p-latency", MPI_COMM_WORLD, NULL, NULL);
    assert(gid != SSG_GROUP_ID_NULL);

    assert(ssg_get_group_size(gid) == 2);

    self = ssg_get_group_self_id(gid);

    if(self == 1)
    {
        /* server side: prep everything before letting the client initiate
         * benchmark
         */
        void* buffer = g_buffer;

        /* register memory for xfer */
        ret = margo_bulk_create(mid, 1, &buffer, &g_buffer_size, HG_BULK_READWRITE, &g_bulk_handle);
        assert(ret == 0);

        /* set up abt pool */
        if(g_opts.threads == 0)
        {
            /* run bulk transfers from primary pool on server */
            g_transfer_pool = pool;
        }
        else
        {
            /* run bulk transfers from a dedicated pool */
            bw_worker_xstreams = malloc(
                    g_opts.threads * sizeof(*bw_worker_xstreams));
            assert(bw_worker_xstreams);
            ret = ABT_snoozer_xstream_create(g_opts.threads, &g_transfer_pool,
                    bw_worker_xstreams);
            assert(ret == ABT_SUCCESS);
        }

        /* signaling mechanism for server to exit at conclusion of test */
        ret = ABT_eventual_create(0, &g_bw_done_eventual);
        assert(ret == 0);
    }

    MPI_Barrier(MPI_COMM_WORLD);

    if(self == 0)
    {
        /* ssg id 0 (client) initiates benchmark */
        ret = run_benchmark(g_bw_id, 1, gid, mid);
        assert(ret == 0);
    }
    else
    {
        /* ssg id 1 (server) waits for test RPC to complete */
        int i;

        ABT_eventual_wait(g_bw_done_eventual, NULL);

        /* cleanup dedicated pool if needed */
        for (i = 0; i < g_opts.threads; i++) {
            ABT_xstream_join(bw_worker_xstreams[i]);
            ABT_xstream_free(&bw_worker_xstreams[i]);
        }
        if(bw_worker_xstreams)
            free(bw_worker_xstreams);
    
        margo_bulk_free(g_bulk_handle);
    }

    ssg_group_destroy(gid);
    ssg_finalize();

    if(g_opts.diag_file_name)
        margo_diag_dump(mid, g_opts.diag_file_name, 1);

    free(g_buffer);

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

    opts->concurrency = 1;

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

    while((opt = getopt(argc, argv, "n:x:c:T:d:s:t:D:")) != -1)
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
            case 'x':
                ret = sscanf(optarg, "%d", &opts->xfer_size);
                if(ret != 1)
                {
                    usage();
                    exit(EXIT_FAILURE);
                }
                break;
            case 'c':
                ret = sscanf(optarg, "%d", &opts->concurrency);
                if(ret != 1)
                {
                    usage();
                    exit(EXIT_FAILURE);
                }
                break;
            case 'T':
                ret = sscanf(optarg, "%d", &opts->threads);
                if(ret != 1)
                {
                    usage();
                    exit(EXIT_FAILURE);
                }
                break;
            case 'D':
                ret = sscanf(optarg, "%d", &opts->duration_seconds);
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

    if(opts->xfer_size < 1 || opts->concurrency < 1 || opts->duration_seconds < 1 || !opts->na_transport)
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
        "margo-p2p-bw -x <xfer_size> -D <duration> -n <na>\n"
        "\t-x <xfer_size> - size of each bulk tranfer in bytes\n"
        "\t-D <duration> - duration of test in seconds\n"
        "\t-n <na> - na transport\n"
        "\t[-c concurrency] - number of concurrent operations to issue with ULTs\n"
        "\t[-T <os threads] - number of dedicated operating system threads to run ULTs on\n"
        "\t[-d filename] - enable diagnostics output \n"
        "\t[-s <bool,bool>] - specify if snoozer scheduler is used on client and server\n"
        "\t\t(e.g., -s 0,1 means snoozer disabled on client and enabled on server)\n"
        "\t\texample: mpiexec -n 2 ./margo-p2p-bw -x 4096 -D 30 -n verbs://\n"
        "\t\t(must be run with exactly 2 processes\n");
    
    return;
}

/* service an RPC that runs the bandwidth test */
static void bw_ult(hg_handle_t handle)
{
    int i;
    bw_rpc_in_t in;
    ABT_thread *tid_array;
    struct bw_worker_arg *arg_array;
    int ret;
    double start_time;
    margo_instance_id mid;
    const struct hg_info *hgi;
    size_t cur_off = 0;
    ABT_mutex cur_off_mutex;
    unsigned long bytes_moved = 0;
    double end_ts = 0.0;
    unsigned long bytes_to_check = 0;
    hg_size_t x;

    ABT_mutex_create(&cur_off_mutex);
    
    /* get handle info and margo instance */
    hgi = margo_get_info(handle);
    assert(hgi);
    mid = margo_hg_info_get_instance(hgi);
    assert(mid != MARGO_INSTANCE_NULL);

    ret = margo_get_input(handle, &in);
        assert(ret == HG_SUCCESS);

    tid_array = malloc(g_opts.concurrency * sizeof(*tid_array));
    assert(tid_array);
    arg_array = calloc(g_opts.concurrency, sizeof(*arg_array));
    assert(arg_array);

    start_time = ABT_get_wtime();
    /* create requested number of workers to run transfer */
    for(i=0; i<g_opts.concurrency; i++)
    {
        arg_array[i].start_tm = start_time;
        arg_array[i].mid = mid;
        arg_array[i].cur_off = &cur_off;
        arg_array[i].cur_off_mutex = &cur_off_mutex;
        arg_array[i].client_bulk_handle = &in.bulk_handle;
        arg_array[i].target_addr = &hgi->addr;

        ret = ABT_thread_create(g_transfer_pool, bw_worker, &arg_array[i], ABT_THREAD_ATTR_NULL, &tid_array[i]);
        assert(ret == 0);
    }

    for(i=0; i<g_opts.concurrency; i++)
    {
        ABT_thread_join(tid_array[i]);
        ABT_thread_free(&tid_array[i]);
        
        bytes_moved += arg_array[i].bytes_moved;
        if(arg_array[i].end_ts > end_ts)
            end_ts = arg_array[i].end_ts;
    }

    /* calculate how many bytes of the buffer have been transferred */
    bytes_to_check = (g_buffer_size / g_opts.xfer_size) * g_opts.xfer_size;
    if(bytes_moved < bytes_to_check)
        bytes_to_check = bytes_moved;

    /* check integrity of fill pattern.  Note that this isn't as strong as
     * checking every RDMA transfer separately since we are looping around
     * and overwriting in a ring-buffer style.  We could corrupt early and
     * then get it right later and miss the problem.
     */
    for(x=0; x<(bytes_to_check/sizeof(x)); x++)
        assert(((hg_size_t*)g_buffer)[x] == x);

    printf("<op>\t<concurrency>\t<threads>\t<bytes>\t<seconds>\t<MiB/s>\n");
    printf("PULL\t%d\t%d\t%lu\t%f\t%f\n",
        g_opts.concurrency,
        g_opts.threads,
        bytes_moved,
        (end_ts-start_time),
        ((double)bytes_moved/(end_ts-start_time))/(1024.0*1024.0));

    margo_respond(handle, NULL);
    margo_free_input(handle, &in);
    margo_destroy(handle);

    free(tid_array);
    free(arg_array);

    ABT_eventual_set(g_bw_done_eventual, NULL, 0);

    ABT_mutex_free(&cur_off_mutex);

    return;
}
DEFINE_MARGO_RPC_HANDLER(bw_ult)

static int run_benchmark(hg_id_t id, ssg_member_id_t target, 
    ssg_group_id_t gid, margo_instance_id mid)
{
    hg_handle_t handle;
    hg_addr_t target_addr;
    int ret;
    bw_rpc_in_t in;
    void* buffer = g_buffer;
    hg_size_t i;

    /* fill pattern in origin buffer */
    for(i=0; i<(g_buffer_size/sizeof(i)); i++)
        ((hg_size_t*)buffer)[i] = i;

    target_addr = ssg_get_addr(gid, target);
    assert(target_addr != HG_ADDR_NULL);

    ret = margo_create(mid, target_addr, id, &handle);
    assert(ret == 0);

    ret = margo_bulk_create(mid, 1, &buffer, &g_buffer_size, HG_BULK_READWRITE, &in.bulk_handle);
    assert(ret == 0);

    ret = margo_forward(handle, &in);
    assert(ret == 0);

    margo_bulk_free(in.bulk_handle);
    margo_destroy(handle);

    return(0);
}

/* function that assists in transferring data until end condition is met */
static void bw_worker(void *_arg)
{
    struct bw_worker_arg *arg = _arg;
    double now;
    size_t my_off;
    int ret;

    // printf("# DBG: worker started.\n");

    now = ABT_get_wtime();

    /* TODO: need two runs, one in each direction? */
    while((now - arg->start_tm) < g_opts.duration_seconds)
    {
        /* find the offset for this transfer and then increment for next
         * one
         */
        ABT_mutex_spinlock(*arg->cur_off_mutex);
        my_off = *arg->cur_off;
        (*arg->cur_off) += g_opts.xfer_size;
        if(*arg->cur_off > g_buffer_size)
            *arg->cur_off = 0;
        ABT_mutex_unlock(*arg->cur_off_mutex);

        ret = margo_bulk_transfer(arg->mid, HG_BULK_PULL,
                *arg->target_addr, *arg->client_bulk_handle, my_off, g_bulk_handle, my_off, g_opts.xfer_size);
        assert(ret == 0);

        arg->bytes_moved += g_opts.xfer_size;
        now = ABT_get_wtime();
    }
    arg->end_ts = now;

    // printf("# DBG: worker stopped.\n");
    return;
}
