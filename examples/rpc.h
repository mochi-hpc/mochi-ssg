/*
 * Copyright (c) 2016 UChicago Argonne, LLC
 *
 * See COPYRIGHT in top-level directory.
 */

#pragma once

#include <mercury.h>
#include <mercury_macros.h>
#include <ssg.h>

/* visible API for example RPC operation */

typedef struct rpc_context
{
    ssg_t s;
    int shutdown_flag;
} rpc_context_t;

MERCURY_GEN_PROC(ping_t, ((int32_t)(rank)))

hg_return_t ping_rpc_handler(hg_handle_t h);
hg_return_t shutdown_rpc_handler(hg_handle_t h);
