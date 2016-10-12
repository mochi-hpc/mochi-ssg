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

#define DO_DEBUG 0
#define DEBUG(fmt, ...) \
    do { \
        if (DO_DEBUG) { \
            printf(fmt, ##__VA_ARGS__); \
            fflush(stdout); \
        } \
    } while(0)

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
          "./ssg-test-margo-dblgrp [-s <time>] <grp id> <addr> <config file 0> <config file 1>\n"
          "  -s <time> - time to sleep before doing lookup\n"
          "  <grp id> - 0 or 1\n"
          "  <addr> - process's listen addr\n"
          "  <config file 0,1> group config files\n",
          stderr);
}

#define ADVANCE argc--; argv++; if (!argc) { usage(); return 1; }

int main(int argc, char *argv[])
{
    // mercury
    hg_class_t *hgcl;
    hg_context_t *hgctx;
    hg_id_t ping_id, shutdown_id;

    // margo
    margo_instance_id mid = MARGO_INSTANCE_NULL;

    // args
    const char * addr_str;
    int sleep_time = 0;
    int grp_id;

    // process state
    rpc_context_t c = { SSG_NULL, 0, 0 };
    int32_t srank;
    ssg_t src_group, sink_group;
    hg_handle_t h;
    int i;

    // return codes
    hg_return_t hret;

    ABT_init(argc, argv);

    ADVANCE

    if (strcmp(argv[0], "-s") == 0) {
        if (argc < 2) { usage(); return 1; }
        sleep_time = atoi(argv[1]);
        ADVANCE ADVANCE
    }

    grp_id = atoi(*argv);
    DIE_IF(grp_id != 0 && grp_id != 1, "expected group id 0 or 1");

    ADVANCE
    addr_str = *argv;

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
    ADVANCE
    const char * conf0, * conf1;
    conf0 = *argv;
    ADVANCE
    conf1 = *argv;

    src_group =  ssg_init_config(hgcl, conf0, grp_id == 0);
    sink_group = ssg_init_config(hgcl, conf1, grp_id == 1);
    DIE_IF(src_group == SSG_NULL || sink_group == SSG_NULL,
            "ssg_init_config with %s and %s\n", conf0, conf1);

    ssg_register_barrier(grp_id == 0 ? src_group : sink_group, hgcl);

    DEBUG("hg, ssg init complete, init margo...\n");

    // init margo in single threaded mode
    mid = margo_init(0, -1, hgctx);
    DIE_IF(mid == MARGO_INSTANCE_NULL, "margo_init");

    ssg_set_margo_id(src_group, mid);
    ssg_set_margo_id(sink_group, mid);
    if (grp_id == 1) {
        c.s = sink_group;
    }

    DEBUG("pre-run sleep\n");

    if (grp_id == 0 && sleep_time >= 0) margo_thread_sleep(mid, sleep_time*1000.0);

    DEBUG("enter lookup\n");

    if (grp_id == 1) {
        srank = ssg_get_rank(sink_group);
        hret = ssg_lookup_margo(sink_group);
        DIE_IF(hret != HG_SUCCESS, "ssg_lookup_margo(sink)");
        c.lookup_flag = 1;
        DEBUG("%d:%d: enter wait\n", grp_id, srank);
        margo_wait_for_finalize(mid);
        DEBUG("%d:%d: exit wait\n", grp_id, srank);
    }
    else {
        srank = ssg_get_rank(src_group);
        DEBUG("%d:%d: lookup 0\n", grp_id, srank);
        hret = ssg_lookup_margo(src_group);
        DIE_IF(hret != HG_SUCCESS, "ssg_lookup_margo(src)");
        DEBUG("%d:%d: ...lookup 1\n", grp_id, srank);
        hret = ssg_lookup_margo(sink_group);
        DIE_IF(hret != HG_SUCCESS, "ssg_lookup_margo(sink) from src");
        DEBUG("%d:%d: ...success\n", grp_id, srank);

        DEBUG("%d:%d: enter barrier\n", grp_id, srank);
        hg_return_t hret = ssg_barrier_margo(src_group);
        DIE_IF(hret != HG_SUCCESS, "barrier");
        DEBUG("%d:%d: leave barrier\n", grp_id, srank);

        DEBUG("%d:%d: ping %d\n", grp_id, srank,
                srank % ssg_get_count(sink_group));
        hret = HG_Create(hgctx, ssg_get_addr(sink_group,
                    srank % ssg_get_count(sink_group)), ping_id, &h);
        DIE_IF(hret != HG_SUCCESS, "HG_Create (ping)\n");
        hret = margo_forward(mid, h, &srank);
        DIE_IF(hret != HG_SUCCESS, "margo_forward (ping)\n");
        DEBUG("%d:%d: ping complete\n", grp_id, srank);
        HG_Destroy(h);

        DEBUG("%d:%d: enter barrier\n", grp_id, srank);
        hret = ssg_barrier_margo(src_group);
        DIE_IF(hret != HG_SUCCESS, "barrier");
        DEBUG("%d:%d: leave barrier\n", grp_id, srank);

        if (srank == 0) {
            for (i = 0; i < ssg_get_count(sink_group); i++) {
                DEBUG("%d:%d: sending shutdown to %d\n", grp_id, srank, i);
                hret = HG_Create(hgctx, ssg_get_addr(sink_group, i),
                        shutdown_id, &h);
                DIE_IF(hret != HG_SUCCESS, "HG_Create");
                hret = margo_forward(mid, h, NULL);
                DIE_IF(hret != HG_SUCCESS, "margo_forward");
                HG_Destroy(h);
            }
        }
        hret = ssg_barrier_margo(src_group);
        DIE_IF(hret != HG_SUCCESS, "barrier");
    }

    DEBUG("%d:%d cleaning up\n", grp_id, srank);
    // cleanup
    ssg_finalize(src_group);
    ssg_finalize(sink_group);
    if (grp_id == 0) margo_finalize(mid);
    HG_Context_destroy(hgctx);
    HG_Finalize(hgcl);

    ABT_finalize();

    return 0;
}
