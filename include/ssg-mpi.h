/*
 * Copyright (c) 2016 UChicago Argonne, LLC
 *
 * See COPYRIGHT in top-level directory.
 */

#pragma once

// mpi based initialization for ssg

#ifdef __cplusplus
extern "C" {
#endif

#include <mpi.h>
#include <na.h>

#include "ssg.h"

// mpi based (no config file) - all participants (defined by the input
// communicator) do a global address exchange
// in this case, the caller has already initialized NA with its address
ssg_t ssg_init_mpi(na_class_t *nacl, MPI_Comm comm);

/**
 * vim: ft=c sw=4 ts=4 sts=4 tw=80 expandtab
 */
