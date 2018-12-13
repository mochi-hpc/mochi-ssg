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

#include <margo.h>

#include <pmix.h>

int main(int argc, char *argv[])
{
    pmix_proc_t proc_info;
    pmix_status_t ret;

    ret = PMIx_Init(&proc_info, NULL, 0);
    assert(ret == PMIX_SUCCESS);

    return 0;
}
