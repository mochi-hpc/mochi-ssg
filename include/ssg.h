/*
 * Copyright (c) 2016 UChicago Argonne, LLC
 *
 * See COPYRIGHT in top-level directory.
 */

#pragma once

/**
 * Scalable Service Groups (SSG) interface
 * 
 * An interface for creating and managing process groups using
 * Mercury and Argobots.
 */

#ifdef __cplusplus
extern "C" {
#endif

#include <mercury.h>
#include <margo.h>

#ifdef HAVE_MPI
#include <mpi.h>
#endif

/* SSG return codes */
#define SSG_SUCCESS 0
/* TODO: define some errors? */

#define SSG_GROUP_ID_NULL 0

/* XXX: actually define what these are */
typedef int ssg_group_id_t;

/***************************************************
 *** SSG runtime intialization/shutdown routines ***
 ***************************************************/

/**
 * Initializes the SSG runtime environment.
 * @param[in] mid Corresponding Margo instance identifier
 * @returns SSG_SUCCESS on success, SSG error code otherwise
 */
int ssg_init(
    margo_instance_id mid);

/**
 * Finalizes the SSG runtime environment.
 */
void ssg_finalize(
    void);

/*************************************
 *** SSG group management routines ***
 *************************************/

/**
 * Creates an SSG group from a given list of HG address strings.
 * @params[in] group_name       Name of the SSG group
 * @params[in] group_addr_strs  Array of HG address strings for each group member
 * @params[in] group_size       Number of group members
 * @returns SSG group ID on success, SSG_GROUP_ID_NULL otherwise
 *
 * NOTE: The HG address string of the caller of this function must be present in
 * the list of address strings given in 'group_addr_strs'. That is, the caller
 * of this function is required to be a member of the SSG group that is created.
 */
ssg_group_id_t ssg_group_create(
    const char * group_name,
    const char * const group_addr_strs[],
    int group_size);

/**
 * Creates an SSG group from a given config file containing the HG address strings
 * of all group members.
 * @params[in] group_name   Name of the SSG group
 * @params[in] file_name    Name of the config file containing the corresponding
 *                          HG address strings for this group
 * @returns SSG group ID on success, SSG_GROUP_ID_NULL otherwise
 * 
 * NOTE: The HG address string of the caller of this function must be present in
 * the list of address strings given in the config file. That is, the caller of
 * this function is required to be a member of the SSG group that is created.
 */
ssg_group_id_t ssg_group_create_config(
    const char * group_name,
    const char * file_name);

#ifdef HAVE_MPI
/**
 * Creates an SSG group from a given MPI communicator.
 * @params[in] group_name   Name of the SSG group
 * @params[in] comm         MPI communicator containing group members
 * @returns SSG group ID on success, SSG_GROUP_ID_NULL otherwise
 */
ssg_group_id_t ssg_group_create_mpi(
    const char * group_name,
    MPI_Comm comm);
#endif

/**
 * Destroys data structures associated with a given SSG group ID.
 * @params[in] group_id SSG group ID
 * @returns SSG_SUCCESS on success, SSG error code otherwise
 */
int ssg_group_destroy(
    ssg_group_id_t group_id);

#if 0
/*** SSG group membership view access routines */

// get my rank in the group
int ssg_get_group_rank(const ssg_t s);

// get the size of the group
int ssg_get_group_size(const ssg_t s);

// get the HG address for the group member at the given rank
hg_addr_t ssg_get_addr(const ssg_t s, int rank);
#endif

#ifdef __cplusplus
}
#endif
