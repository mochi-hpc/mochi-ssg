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
#define SSG_ERROR (-1)

/* SSG group identifier datatype */
/* TODO: this shouldn't be visible ... we can't use a typical
 * opaque pointer since we want to be able to xmit these to
 * other processes.
 */
#define SSG_GROUP_ID_MAX_ADDR_LEN 64
typedef struct ssg_group_id
{
    uint64_t magic_nr;
    uint64_t name_hash;
    char addr_str[SSG_GROUP_ID_MAX_ADDR_LEN];
} ssg_group_id_t;

/***************************************************
 *** SSG runtime intialization/shutdown routines ***
 ***************************************************/

/**
 * Initializes the SSG runtime environment.
 *
 * @param[in] mid Corresponding Margo instance identifier
 * @returns SSG_SUCCESS on success, SSG error code otherwise
 */
int ssg_init(
    margo_instance_id mid);

/**
 * Finalizes the SSG runtime environment.
 *
 * @returns SSG_SUCCESS on success, SSG error code otherwise
 */
int ssg_finalize(
    void);

/*************************************
 *** SSG group management routines ***
 *************************************/

/**
 * Creates an SSG group from a given list of HG address strings.
 *
 * @param[in]  group_name       Name of the SSG group
 * @param[in]  group_addr_strs  Array of HG address strings for each group member
 * @param[in]  group_size       Number of group members
 * @param[out] group_id         Pointer to output SSG group ID
 * @returns SSG_SUCCESS on success, SSG error code otherwise
 *
 * NOTE: The HG address string of the caller of this function must be present in
 * the list of address strings given in 'group_addr_strs'. That is, the caller
 * of this function is required to be a member of the SSG group that is created.
 */
int ssg_group_create(
    const char * group_name,
    const char * const group_addr_strs[],
    int group_size,
    ssg_group_id_t * group_id);

/**
 * Creates an SSG group from a given config file containing the HG address strings
 * of all group members.
 *
 * @param[in]  group_name   Name of the SSG group
 * @param[in]  file_name    Name of the config file containing the corresponding
 *                          HG address strings for this group
 * @param[out] group_id     Pointer to output SSG group ID
 * @returns SSG_SUCCESS on success, SSG error code otherwise
 * 
 * NOTE: The HG address string of the caller of this function must be present in
 * the list of address strings given in the config file. That is, the caller of
 * this function is required to be a member of the SSG group that is created.
 */
int ssg_group_create_config(
    const char * group_name,
    const char * file_name,
    ssg_group_id_t * group_id);

#ifdef HAVE_MPI
/**
 * Creates an SSG group from a given MPI communicator.
 *
 * @param[in]  group_name   Name of the SSG group
 * @param[in]  comm         MPI communicator containing group members
 * @param[out] group_id     Pointer to output SSG group ID
 * @returns SSG_SUCCESS on success, SSG error code otherwise
 */
int ssg_group_create_mpi(
    const char * group_name,
    MPI_Comm comm,
    ssg_group_id_t * group_id);
#endif

/**
 * Destroys data structures associated with a given SSG group ID.
 *
 * @param[in] group_id SSG group ID
 * @returns SSG_SUCCESS on success, SSG error code otherwise
 */
int ssg_group_destroy(
    ssg_group_id_t group_id);

/**
 * Attaches a client to an SSG group.
 *
 * @param[in] group_id SSG group ID
 * @returns SSG_SUCCESS on success, SSG error code otherwise
 *
 * NOTE: The "client" cannot be a member of the group -- attachment is merely
 * a way of making the membership view of an existing SSG group available to
 * non-group members.
 */
int ssg_group_attach(
    ssg_group_id_t group_id);

/**
 * Detaches a client from an SSG group.
 *
 * @param[in] group_id SSG group ID
 * @returns SSG_SUCCESS on success, SSG error code otherwise
 */
int ssg_group_detach(
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
