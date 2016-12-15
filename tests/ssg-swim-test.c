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

#ifdef HAVE_MPI
#include <ssg-mpi.h>
#endif

#define DIE_IF(cond_expr, err_fmt, ...) \
    do { \
        if (cond_expr) { \
            fprintf(stderr, "ERROR at %s:%d (" #cond_expr "): " \
                    err_fmt "\n", __FILE__, __LINE__, ##__VA_ARGS__); \
            goto cleanup; \
        } \
    } while(0)

#define DO_DEBUG 1
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
          "./ssg-test-margo [-s <time>] <addr> <config mode> [config file]\n"
          "  -s <time> - time to sleep before doing lookup\n"
          "  <config mode> - \"mpi\" (if supported) or \"conf\"\n"
          "  if conf is the mode, then [config file] is required\n",
          stderr);
}


int main(int argc, char *argv[])
{
    // mercury
    hg_class_t *hgcl = NULL;
    hg_context_t *hgctx = NULL;

    // margo
    margo_instance_id mid = MARGO_INSTANCE_NULL;

    // ssg
    ssg_t s = NULL;

    // args
    const char * addr_str;
    const char * mode;
    int sleep_time = 0;

    // process state
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

#if 1
    if (!argc) { usage(); return 1; }
    addr_str = argv[0];
    argc--; argv++;
#else
    int mpi_rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &mpi_rank);
    if(mpi_rank == 0)
        addr_str = "bmi+tcp://3344";
    else
        addr_str = "bmi+tcp://3345";
#endif
    
    // init HG
    hgcl = HG_Init(addr_str, HG_TRUE);
    DIE_IF(hgcl == NULL, "HG_Init");
    hgctx = HG_Context_create(hgcl);
    DIE_IF(hgctx == NULL, "HG_Context_create");

    // init margo in single threaded mode
    mid = margo_init(0, -1, hgctx);
    DIE_IF(mid == MARGO_INSTANCE_NULL, "margo_init");

    // parse mode and attempt to initialize ssg
    if (!argc) { usage(); return 1; }
    mode = argv[0];
    argc--; argv++;
    if (strcmp(mode, "mpi") == 0) {
#ifdef HAVE_MPI
        s = ssg_init_mpi(hgcl, MPI_COMM_WORLD);
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
        s = ssg_init_config(hgcl, conf, 1);
    }
    else {
        fprintf(stderr, "Error: bad mode passed in %s\n", mode);
        return 1;
    }

    DIE_IF(s == SSG_NULL, "ssg_init (mode %s)", mode);

    ssg_set_margo_id(s, mid);
    rank = ssg_get_rank(s);
    size = ssg_get_count(s);

    // resolve group addresses
    hret = ssg_lookup_margo(s);
    DIE_IF(hret != HG_SUCCESS, "ssg_lookup");
    DEBUG("%d of %d: ssg_lookup complete\n", rank, size);

    if (sleep_time >= 0) margo_thread_sleep(mid, sleep_time * 1000.0);
    DEBUG("%d of %d: sleep over\n", rank, size);

cleanup:
    // cleanup
    if(s) ssg_finalize(s);
    if(mid != MARGO_INSTANCE_NULL) margo_finalize(mid);
    if(hgctx) HG_Context_destroy(hgctx);
    if(hgcl) HG_Finalize(hgcl);

#ifdef HAVE_MPI
    MPI_Finalize();
#endif

    ABT_finalize();

    return 0;
}
