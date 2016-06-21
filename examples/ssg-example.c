/*
 * Copyright (c) 2016 UChicago Argonne, LLC
 *
 * See COPYRIGHT in top-level directory.
 */

#include <unistd.h>
#include <stdio.h>
#include <string.h>

#include <mercury.h>
#include <mercury_util/mercury_request.h>
#include <ssg.h>
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

static void usage()
{
    fputs("Usage: "
          "./ssg-example [-s <time>] <addr> <config mode> [config file]\n"
          "  -s <time> - time to sleep before doing lookup\n"
          "  <config mode> - \"mpi\" (if supported) or \"conf\"\n"
          "  if conf is the mode, then [config file] is required\n",
          stderr);
}

// hg_request_class_t progress/trigger
static int progress(unsigned int timeout, void *arg)
{
    if (HG_Progress((hg_context_t*)arg, timeout) == HG_SUCCESS)
        return HG_UTIL_SUCCESS;
    else
        return HG_UTIL_FAIL;
}
static int trigger(unsigned int timeout, unsigned int *flag, void *arg)
{
    (void)arg;
    if (HG_Trigger((hg_context_t*)arg, timeout, 1, flag) != HG_SUCCESS) {
        return HG_UTIL_FAIL;
    }
    else {
        *flag = (*flag) ? HG_UTIL_TRUE : HG_UTIL_FALSE;
        return HG_UTIL_SUCCESS;
    }
}

int main(int argc, char *argv[])
{
    // mercury
    hg_class_t *hgcl;
    hg_context_t *hgctx;
    hg_request_class_t *reqcl;
    hg_request_t *req;
    hg_id_t ping_id, shutdown_id;
    hg_handle_t ping_handle = HG_HANDLE_NULL;

    // args
    const char * addr_str;
    const char * mode;
    int sleep_time = 0;

    // process state
    rpc_context_t c = { SSG_NULL, 0 };
    int rank; // not mpi

    // comm vars
    int peer_rank;
    hg_addr_t peer_addr;
    ping_t ping_in;
    unsigned int req_complete_flag = 0;

    // return codes
    hg_return_t hret;
    int ret;

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
        MERCURY_REGISTER(hgcl, "ping", ping_t, ping_t, &ping_rpc_handler);
    shutdown_id =
        MERCURY_REGISTER(hgcl, "shutdown", void, void, &shutdown_rpc_handler);

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

    DIE_IF(c.s == SSG_NULL, "ssg_init (mode %s)", mode);

    if (sleep_time >= 0) sleep(sleep_time);

    // resolve group addresses
    hret = ssg_lookup(c.s, hgctx);
    DIE_IF(hret != HG_SUCCESS, "ssg_lookup");

    // get my (non-mpi) rank
    rank = ssg_get_rank(c.s);
    DIE_IF(rank == SSG_RANK_UNKNOWN || rank == SSG_EXTERNAL_RANK,
            "ssg_get_rank - bad rank %d", rank);

    // initialize request shim
    reqcl = hg_request_init(&progress, &trigger, hgctx);
    DIE_IF(reqcl == NULL, "hg_request_init");
    req = hg_request_create(reqcl);
    DIE_IF(req == NULL, "hg_request_create");

    // sanity check count - if we're on our own, don't bother sending RPCs
    if (ssg_get_count(c.s) == 1)
        goto cleanup;

    // all ready to go - ping my neighbor rank
    peer_rank = (rank+1) % ssg_get_count(c.s);
    peer_addr = ssg_get_addr(c.s, peer_rank);
    DIE_IF(peer_addr == HG_ADDR_NULL, "ssg_get_addr(%d)", peer_rank);

    printf("%d: pinging %d\n", rank, peer_rank);
    hret = HG_Create(hgctx, peer_addr, ping_id, &ping_handle);
    DIE_IF(hret != HG_SUCCESS, "HG_Create");
    ping_in.rank = rank;
    hret = HG_Forward(ping_handle, &hg_request_complete_cb, req, &ping_in);
    DIE_IF(hret != HG_SUCCESS, "HG_Forward");
    ret = hg_request_wait(req, HG_MAX_IDLE_TIME, &req_complete_flag);
    DIE_IF(ret == HG_UTIL_FAIL, "ping failed");
    DIE_IF(req_complete_flag == 0, "ping timed out");

    // rank 0 - initialize the shutdown process. All others - enter progress
    if (rank != 0) {
        unsigned int num_trigger;
        do {
            do {
                num_trigger = 0;
                hret = HG_Trigger(hgctx, 0, 1, &num_trigger);
            } while (hret == HG_SUCCESS && num_trigger == 1);

            hret = HG_Progress(hgctx, c.shutdown_flag ? 100 : HG_MAX_IDLE_TIME);
        } while ((hret == HG_SUCCESS || hret == HG_TIMEOUT) && !c.shutdown_flag);
        DIE_IF(hret != HG_SUCCESS && hret != HG_TIMEOUT, "HG_Progress");
        printf("%d: shutting down\n", rank);

        // trigger remaining
        do {
            num_trigger = 0;
            hret = HG_Trigger(hgctx, 0, 1, &num_trigger);
        } while (hret == HG_SUCCESS && num_trigger == 1);
    }
    else {
        printf("%d: initiating shutdown\n", rank);
        hg_handle_t shutdown_handle = HG_HANDLE_NULL;
        hret = HG_Create(hgctx, peer_addr, shutdown_id, &shutdown_handle);
        DIE_IF(hret != HG_SUCCESS, "HG_Create");
        hret = HG_Forward(shutdown_handle, &hg_request_complete_cb, req, NULL);
        DIE_IF(hret != HG_SUCCESS, "HG_Forward");
        req_complete_flag = 0;
        ret = hg_request_wait(req, HG_MAX_IDLE_TIME, &req_complete_flag);
        DIE_IF(ret != HG_UTIL_SUCCESS, "hg_request_wait");
        DIE_IF(req_complete_flag == 0, "hg_request_wait timeout");
        HG_Destroy(shutdown_handle);
    }

cleanup:
    printf("%d: cleaning up\n", rank);
    // cleanup
    HG_Destroy(ping_handle);
    ssg_finalize(c.s);
    hg_request_destroy(req);
    hg_request_finalize(reqcl, NULL);
    HG_Context_destroy(hgctx);
    HG_Finalize(hgcl);

#ifdef HAVE_MPI
    MPI_Finalize();
#endif
    return 0;
}
