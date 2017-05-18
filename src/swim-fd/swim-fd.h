/*
 * Copyright (c) 2016 UChicago Argonne, LLC
 *
 * See COPYRIGHT in top-level directory.
 */

#pragma once

#ifdef __cplusplus
extern "C" {
#endif

#include "ssg.h"
#include "ssg-internal.h"

/* opaque swim context type */
typedef struct swim_context swim_context_t;

swim_context_t *swim_init(
    ssg_group_t *g,
    int active);

void swim_finalize(
    swim_context_t *swim_ctx);

#ifdef __cplusplus
}
#endif
