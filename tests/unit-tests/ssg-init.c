#include <stdio.h>
#include <sys/types.h>
#include <unistd.h>

#include <margo.h>
#include <ssg.h>
#include "helper-server.h"
#include "munit/munit.h"

struct test_context {
    margo_instance_id mid;
    int               remote_pid;
    char              remote_addr[256];
};

struct ssg_shutdown_context
{
    ssg_group_id_t gid;
};

static void ssg_shutdown_callback(void* data)
{
    int ret;
    struct ssg_shutdown_context *ss_ctx = (struct ssg_shutdown_context*)data;

    ret = ssg_group_destroy(ss_ctx->gid);
    munit_assert_int(ret, ==, SSG_SUCCESS);

    ret = ssg_finalize();
    munit_assert_int(ret, ==, SSG_SUCCESS);

    free(ss_ctx);
}

static int svr_init_fn(margo_instance_id mid, void* arg)
{
    (void) arg;
    int ret;
    char self_str[256] = {0};
    hg_size_t self_str_size = 256;
    hg_addr_t self_addr = HG_ADDR_NULL;
    const char *group_addr_strs[1];
    ssg_group_id_t gid;
    char gid_file[256] = {0};
    struct ssg_shutdown_context *ss_ctx = NULL;

    /* start an ssg group with this server process as the only member */

    ret = margo_addr_self(mid, &self_addr);
    munit_assert_int(ret, ==, HG_SUCCESS);

    ret = margo_addr_to_string(mid, self_str, &self_str_size, self_addr);
    munit_assert_int(ret, ==, HG_SUCCESS);

    ret = margo_addr_free(mid, self_addr);
    munit_assert_int(ret, ==, HG_SUCCESS);

    group_addr_strs[0] = self_str;

    ret = ssg_init();
    munit_assert_int(ret, ==, SSG_SUCCESS);

    ret = ssg_group_create(mid, "tests/unit/ssg-init", group_addr_strs, 1,
        NULL, NULL, NULL, &gid);
    munit_assert_int(ret, ==, SSG_SUCCESS);

    /* write group id to a file */
    snprintf(gid_file, 256, "/tmp/ssg.%d", getpid());
    ret = ssg_group_id_store(gid_file, gid, 1);
    munit_assert_int(ret, ==, SSG_SUCCESS);

    ss_ctx = malloc(sizeof(*ss_ctx));
    munit_assert_not_null(ss_ctx);
    ss_ctx->gid = gid;

    /* push callback to shut down ssg stuff */
    margo_push_prefinalize_callback(mid, ssg_shutdown_callback, ss_ctx);

    return(0);
}

static void* test_context_setup(const MunitParameter params[], void* user_data)
{
    (void) params;
    (void) user_data;
    struct test_context* ctx = calloc(1, sizeof(*ctx));

    /* start server process */
    hg_size_t remote_addr_size = 256;
    ctx->remote_pid = HS_start("na+sm://", NULL, svr_init_fn, NULL, NULL, &(ctx->remote_addr[0]), &remote_addr_size);
    munit_assert_int(ctx->remote_pid, >, 0);

    /* deliberately skip margo init on client side so that we can control
     * whether that step is actually done or not during unit tests
     */
#if 0
    ctx->mid = margo_init("na+sm://", MARGO_CLIENT_MODE, 0, 0);
    munit_assert_not_null(ctx->mid);
#endif

    return ctx;
}

static void test_context_tear_down(void *data)
{
    struct test_context *ctx = (struct test_context*)data;

    /* we assume that margo is uninitialized on client side at this point;
     * we initialize it now simply so that we can contact server to instruct
     * it to shut down
     */
    ctx->mid = margo_init("na+sm://", MARGO_CLIENT_MODE, 0, 0);
    munit_assert_not_null(ctx->mid);

    hg_addr_t remote_addr = HG_ADDR_NULL;
    margo_addr_lookup(ctx->mid, ctx->remote_addr, &remote_addr);
    margo_shutdown_remote_instance(ctx->mid, remote_addr);
    margo_addr_free(ctx->mid, remote_addr);

    HS_stop(ctx->remote_pid, 0);
    margo_finalize(ctx->mid);

    free(ctx);
}

/* init/finalize cycle without Argobots */
static MunitResult init_no_abt(const MunitParameter params[], void* data)
{
    (void)params;
    (void)data;
    int sret;

    /* ssg init will not succeed if Argobots is not initialized, but it
     * should fail gracefully
     */
    sret = ssg_init();
    munit_assert_int(sret, !=, SSG_SUCCESS);

    return MUNIT_OK;
}

/* init/finalize cycle */
static MunitResult init(const MunitParameter params[], void* data)
{
    (void)params;
    (void)data;
    int sret;

    sret = ABT_init(0, NULL);
    munit_assert_int(sret, ==, ABT_SUCCESS);

    sret = ssg_init();
    munit_assert_int(sret, ==, SSG_SUCCESS);

    sret = ssg_finalize();
    munit_assert_int(sret, ==, SSG_SUCCESS);

    ABT_finalize();

    return MUNIT_OK;
}

/* init/finalize cycle with id load */
static MunitResult init_load(const MunitParameter params[], void* data)
{
    (void)params;
    struct test_context *ctx = (struct test_context*)data;
    int sret;
    char gid_file[256] = {0};
    int nservers = 1;
    ssg_group_id_t gid;

    /* start margo for this unit test */
    ctx->mid = margo_init("na+sm://", MARGO_CLIENT_MODE, 0, 0);
    munit_assert_not_null(ctx->mid);

    snprintf(gid_file, 256, "/tmp/ssg.%d", ctx->remote_pid);

    sret = ABT_init(0, NULL);
    munit_assert_int(sret, ==, ABT_SUCCESS);

    sret = ssg_init();
    munit_assert_int(sret, ==, SSG_SUCCESS);

    sret = ssg_group_id_load(gid_file, &nservers, &gid);
    munit_assert_int(sret, ==, SSG_SUCCESS);

    sret = ssg_finalize();
    munit_assert_int(sret, ==, SSG_SUCCESS);

    margo_finalize(ctx->mid);

    ABT_finalize();

    return MUNIT_OK;
}

/* init/finalize cycle with id load */
static MunitResult init_load_no_margo(const MunitParameter params[], void* data)
{
    (void)params;
    struct test_context *ctx = (struct test_context*)data;
    int sret;
    char gid_file[256] = {0};
    int nservers = 1;
    ssg_group_id_t gid;

    snprintf(gid_file, 256, "/tmp/ssg.%d", ctx->remote_pid);

    sret = ABT_init(0, NULL);
    munit_assert_int(sret, ==, ABT_SUCCESS);

    sret = ssg_init();
    munit_assert_int(sret, ==, SSG_SUCCESS);

    sret = ssg_group_id_load(gid_file, &nservers, &gid);
    munit_assert_int(sret, ==, SSG_SUCCESS);

    sret = ssg_finalize();
    munit_assert_int(sret, ==, SSG_SUCCESS);

    ABT_finalize();

    return MUNIT_OK;
}

static MunitTest tests[] = {
    { "/init", init, test_context_setup, test_context_tear_down, MUNIT_TEST_OPTION_NONE, NULL},
    { "/init_no_abt", init_no_abt, test_context_setup, test_context_tear_down, MUNIT_TEST_OPTION_NONE, NULL},
    { "/init_load", init_load, test_context_setup, test_context_tear_down, MUNIT_TEST_OPTION_NONE, NULL},
    { "/init_load_no_margo", init_load_no_margo, test_context_setup, test_context_tear_down, MUNIT_TEST_OPTION_NONE, NULL},
    { NULL, NULL, NULL, NULL, MUNIT_TEST_OPTION_NONE, NULL }
};

static const MunitSuite test_suite = {
    "/ssg", tests, NULL, 1, MUNIT_SUITE_OPTION_NONE
};


int main(int argc, char **argv)
{
    return munit_suite_main(&test_suite, NULL, argc, argv);
}
