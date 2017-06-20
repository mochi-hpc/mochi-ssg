/*
 * Copyright (c) 2016 UChicago Argonne, LLC
 *
 * See COPYRIGHT in top-level directory.
 */

#pragma once

#include <mpi.h>
#include <ssg.h>

/**
 * Scalable Service Groups (SSG) interface
 *
 * An SSG group create routine based on MPI communicators.
 */

#ifdef __cplusplus
extern "C" {
#endif

/**
 * Creates an SSG group from a given MPI communicator.
 *
 * @param[in]  group_name   Name of the SSG group
 * @param[in]  comm         MPI communicator containing group members
 * @returns SSG group identifier on success, SSG_GROUP_ID_NULL otherwise
 */
ssg_group_id_t ssg_group_create_mpi(
    const char * group_name,
    MPI_Comm comm);

#ifdef __cplusplus
}
#endif
