/*
 * Copyright (c) 2016 UChicago Argonne, LLC
 *
 * See COPYRIGHT in top-level directory.
 */

#include <unistd.h>
#include <stdio.h>
#include <string.h>

#include <margo.h>
#include <abt.h>
#include <mercury.h>
#include <ssg.h>
#include <ssg-margo.h>
#include <ssg-config.h>
#include "rpc.h"

#ifdef HAVE_MPI
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

#define DO_DEBUG 0
#define DEBUG(fmt, ...) \
    do { \
        if (DO_DEBUG) { \
            printf(fmt, ##__VA_ARGS__); \
            fflush(stdout); \
        } \
    } while(0)

static void usage()
{
    fputs("Usage: "
          "./ssg-example [-s <time>] <addr> <config mode> [config file]\n"
          "  -s <time> - time to sleep before doing lookup\n"
          "  <config mode> - \"mpi\" (if supported) or \"conf\"\n"
          "  if conf is the mode, then [config file] is required\n",
          stderr);
}

typedef struct ping_dispatch_args
{
    hg_id_t ping_id;
    margo_instance_id mid;
    ssg_t ssg;
    int dest_rank;
} ping_dispatch_args_t;

static void ping_dispatch_ult(void *arg)
{
    DEBUG("in ping dispatch ult\n");
    ping_dispatch_args_t *pargs = arg;

    ping_t in; in.rank = pargs->dest_rank;

    hg_handle_t h;
    hg_return_t hret = HG_Create(margo_get_context(pargs->mid),
            ssg_get_addr(pargs->ssg, pargs->dest_rank), pargs->ping_id, &h);
    DIE_IF(hret != HG_SUCCESS, "HG_Create (ping)");

    hret = margo_forward(pargs->mid, h, &in);
    DIE_IF(hret != HG_SUCCESS, "margo_forward (ping)");
    DEBUG("%d: got ping response from %d\n", ssg_get_rank(pargs->ssg),
            pargs->dest_rank);
}

int main(int argc, char *argv[])
{
    // mercury
    hg_class_t *hgcl;
    hg_context_t *hgctx;
    hg_id_t ping_id, shutdown_id;

    // margo
    margo_instance_id mid = MARGO_INSTANCE_NULL;

    // dispatch threads
    ABT_thread *ults = NULL;
    ping_dispatch_args_t *args = NULL;

    // args
    const char * addr_str;
    const char * mode;
    int sleep_time = 0;

    // process state
    rpc_context_t c = { SSG_NULL, 0, 0 };
    int rank, size; // not mpi

    // return codes
    hg_return_t hret;

    ABT_init(argc, argv);

#ifdef HAVE_MPI
    MPI_Init(&argc, &argv);
#endif

    argc--; argv++;

    if (!argc) { usage(); return 1; }

    if (strcmp(argv[0], "-s") == 0) {
        if (argc < 2) { usage(); return 1; }
        sleep_time = atoi(argv[1]);
        argc -= 2; argv += 2;
    }

    if (!argc) { usage(); return 1; }
    addr_str = argv[0];
    argc--; argv++;

    // init HG
    hgcl = HG_Init(addr_str, HG_TRUE);
    DIE_IF(hgcl == NULL, "HG_Init");
    hgctx = HG_Context_create(hgcl);
    DIE_IF(hgctx == NULL, "HG_Context_create");

    ping_id =
        MERCURY_REGISTER(hgcl, "ping", ping_t, ping_t,
                &ping_rpc_ult_handler);
    shutdown_id =
        MERCURY_REGISTER(hgcl, "shutdown", void, void,
                &shutdown_rpc_ult_handler);

    hret = HG_Register_data(hgcl, ping_id, &c, NULL);
    DIE_IF(hret != HG_SUCCESS, "HG_Register_data");
    hret = HG_Register_data(hgcl, shutdown_id, &c, NULL);
    DIE_IF(hret != HG_SUCCESS, "HG_Register_data");

    // parse mode and attempt to initialize ssg
    if (!argc) { usage(); return 1; }
    mode = argv[0];
    argc--; argv++;
    if (strcmp(mode, "mpi") == 0) {
#ifdef HAVE_MPI
        c.s = ssg_init_mpi(hgcl, MPI_COMM_WORLD);
        sleep_time = 0; // ignore sleeping
#else
        fprintf(stderr, "Error: MPI support not built in\n");
        return 1;
#endif
    }
    else if (strcmp(mode, "conf") == 0) {
        const char * conf;
        if (!argc) { usage(); return 1; }
        conf = argv[0];
        argc--; argv++;
        c.s = ssg_init_config(conf, 1);
    }
    else {
        fprintf(stderr, "Error: bad mode passed in %s\n", mode);
        return 1;
    }

    hret = ssg_resolve_rank(c.s, hgcl);
    DIE_IF(hret != HG_SUCCESS, "ssg_resolve_rank");
    rank = ssg_get_rank(c.s);
    size = ssg_get_count(c.s);

    ssg_register_barrier(c.s, hgcl);

    DIE_IF(c.s == SSG_NULL, "ssg_init (mode %s)", mode);

    DEBUG("hg, ssg init complete, init margo...\n");

    // init margo in single threaded mode
    mid = margo_init(0, -1, hgctx);
    DIE_IF(mid == MARGO_INSTANCE_NULL, "margo_init");

    ssg_set_margo_id(c.s, mid);

    DEBUG("pre-run sleep\n");

    if (sleep_time >= 0) sleep(sleep_time);

    DEBUG("enter lookup\n");

    // resolve group addresses
    hret = ssg_lookup_margo(c.s);
    DIE_IF(hret != HG_SUCCESS, "ssg_lookup");

    DEBUG("leave lookup\n");

    // sanity check count - if we're on our own, don't bother sending RPCs
    if (size == 1)
        goto cleanup;

    if (rank == 0) {
        // init threads
        ults = malloc((size-1) * sizeof(*ults));
        DIE_IF(ults == NULL, "malloc ults");
        for (int i = 0; i < size-1; i++)
            ults[i] = ABT_THREAD_NULL;
        args = malloc((size-1) * sizeof(*args));
        DIE_IF(args == NULL, "malloc ult args");
        for (int i = 0; i < size-1; i++) {
            args[i].ping_id = ping_id;
            args[i].mid = mid;
            args[i].ssg = c.s;
            args[i].dest_rank = (i < rank) ? i : i+1;
        }
    }

    DEBUG("enter barrier\n");
    ssg_barrier_margo(c.s);
    DEBUG("leave barrier\n");

    // all ready to go - have rank 0 ping everyone concurrently
    if (rank == 0) {
        for (int i = 0; i < size-1; i++) {
            DEBUG("%d: pinging %d\n", rank, i<rank ? i : i+1);
            ABT_pool pool = *margo_get_handler_pool(mid);
            int aret = ABT_thread_create(pool, ping_dispatch_ult, &args[i],
                    ABT_THREAD_ATTR_NULL, &ults[i]);
            DIE_IF(aret != ABT_SUCCESS, "ABT_thread_create");
        }
        for (int i = 0; i < size-1; i++) {
            int aret = ABT_thread_join(ults[i]);
            DIE_IF(aret != ABT_SUCCESS, "ABT_thread_join");
            ABT_thread_free(&ults[i]);
        }

        DEBUG("%d: initiating shutdown\n", rank);
        hg_handle_t shutdown_handle = HG_HANDLE_NULL;
        for (int i = 1; i < size; i++) {
            hret = HG_Create(hgctx, ssg_get_addr(c.s, i), shutdown_id,
                    &shutdown_handle);
            DIE_IF(hret != HG_SUCCESS, "HG_Create");
            hret = margo_forward(mid, shutdown_handle, NULL);
            DIE_IF(hret != HG_SUCCESS, "margo_forward (shutdown)");
            HG_Destroy(shutdown_handle);
        }
        margo_finalize(mid);
    }
    else {
        DEBUG("%d: waiting for finalize\n", rank);
        margo_wait_for_finalize(mid);
    }

cleanup:
    DEBUG("%d: cleaning up\n", rank);
    // cleanup
    ssg_finalize(c.s);
    HG_Context_destroy(hgctx);
    HG_Finalize(hgcl);
    free(ults);
    free(args);

#ifdef HAVE_MPI
    MPI_Finalize();
#endif

    ABT_finalize();

    return 0;
}
