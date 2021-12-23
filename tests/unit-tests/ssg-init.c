
#include <margo.h>
#include <ssg.h>
#include "helper-server.h"
#include "munit/munit.h"

struct test_context {
    margo_instance_id mid;
};

static void* test_context_setup(const MunitParameter params[], void* user_data)
{
    (void) params;
    (void) user_data;
    struct test_context* ctx = calloc(1, sizeof(*ctx));

    return ctx;
}

static void test_context_tear_down(void *data)
{
    struct test_context *ctx = (struct test_context*)data;

    free(ctx);
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

    return MUNIT_OK;
}

static MunitTest tests[] = {
    { "/init", init, test_context_setup, test_context_tear_down, MUNIT_TEST_OPTION_NONE, NULL},
    { NULL, NULL, NULL, NULL, MUNIT_TEST_OPTION_NONE, NULL }
};

static const MunitSuite test_suite = {
    "/ssg", tests, NULL, 1, MUNIT_SUITE_OPTION_NONE
};


int main(int argc, char **argv)
{
    return munit_suite_main(&test_suite, NULL, argc, argv);
}
