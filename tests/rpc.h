/*
 * Copyright (c) 2016 UChicago Argonne, LLC
 *
 * See COPYRIGHT in top-level directory.
 */

#pragma once

#include <mercury.h>
#include <mercury_macros.h>
#include <margo.h>

#include <ssg.h>
#include <ssg-config.h>

/* visible API for example RPC operation */

typedef struct rpc_context
{
    ssg_t s;
    int shutdown_flag; // used in non-margo test
    int lookup_flag;   // used in dblgrp test
} rpc_context_t;

MERCURY_GEN_PROC(ping_t, ((int32_t)(rank)))

hg_return_t ping_rpc_handler(hg_handle_t h);
hg_return_t shutdown_rpc_handler(hg_handle_t h);

DECLARE_MARGO_RPC_HANDLER(ping_rpc_ult)
DECLARE_MARGO_RPC_HANDLER(shutdown_rpc_ult)
