/*
 * Copyright (c) 2016 UChicago Argonne, LLC
 *
 * See COPYRIGHT in top-level directory.
 */

#pragma once

#include <mpi.h>
#include <ssg.h>

/** @file ssg-mpi.h
 * Scalable Service Groups (SSG) interface
 *
 * An SSG group create routine based on MPI communicators.
 */

#ifdef __cplusplus
extern "C" {
#endif

/**
 * Creates an SSG group from a given MPI communicator. A 'NULL' value for
 * 'group_conf' will use SSG defaults for all configuration parameters.
 *
 * @param[in] mid           Corresponding Margo instance identifier
 * @param[in] group_name    Name of the SSG group
 * @param[in] comm          MPI communicator containing group members
 * @param[in] group_conf        Configuration parameters for the group
 * @param[in] update_cb     Callback function executed on group membership changes
 * @param[in] update_cb_dat User data pointer passed to membership update callback
 * @returns SSG group identifier for created group on success, SSG_GROUP_ID_INVALID otherwise
 */
ssg_group_id_t ssg_group_create_mpi(
    margo_instance_id mid,
    const char * group_name,
    MPI_Comm comm,
    ssg_group_config_t *group_conf,
    ssg_membership_update_cb update_cb,
    void * update_cb_dat);

#ifdef __cplusplus
}
#endif
