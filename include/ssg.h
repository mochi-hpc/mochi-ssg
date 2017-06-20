/*
 * Copyright (c) 2016 UChicago Argonne, LLC
 *
 * See COPYRIGHT in top-level directory.
 */

#pragma once

#include <mercury.h>
#include <margo.h>

/**
 * Scalable Service Groups (SSG) interface
 * 
 * An interface for creating and managing process groups using
 * Mercury and Argobots.
 */

#ifdef __cplusplus
extern "C" {
#endif

/* SSG return codes */
#define SSG_SUCCESS 0
#define SSG_FAILURE (-1)

typedef uint64_t ssg_member_id_t;
#define SSG_MEMBER_ID_NULL UINT64_MAX

/* opaque SSG group identifier type */
struct ssg_group_descriptor;
typedef struct ssg_group_descriptor *ssg_group_id_t;
#define SSG_GROUP_ID_NULL ((ssg_group_id_t)NULL)

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
 * @returns SSG group identifier on success, SSG_GROUP_ID_NULL otherwise
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
 *
 * @param[in]  group_name   Name of the SSG group
 * @param[in]  file_name    Name of the config file containing the corresponding
 *                          HG address strings for this group
 * @param[out] group_id     Pointer to output SSG group ID
 * @returns SSG group identifier on success, SSG_GROUP_ID_NULL otherwise
 * 
 * NOTE: The HG address string of the caller of this function must be present in
 * the list of address strings given in the config file. That is, the caller of
 * this function is required to be a member of the SSG group that is created.
 */
ssg_group_id_t ssg_group_create_config(
    const char * group_name,
    const char * file_name);

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

/*********************************
 *** SSG group access routines ***
 *********************************/

/**
 * Obtains the caller's member ID in the given SSG group.
 *
 * @param[in] group_id SSG group ID
 * @returns caller's group ID on success, SSG_MEMBER_ID_NULL otherwise
 */
ssg_member_id_t ssg_get_group_self_id(
    ssg_group_id_t group_id);

/**
 * Obtains the size of a given SSG group.
 *
 * @param[in] group_id SSG group ID
 * @returns size of the group on success, 0 otherwise
 */
int ssg_get_group_size(
    ssg_group_id_t group_id);

/**
 * Obtains the HG address of a member in a given SSG group.
 *
 * @param[in] group_id  SSG group ID
 * @param[in] member_id SSG group member ID
 * @returns HG address of given group member on success, HG_ADDR_NULL otherwise
 */
hg_addr_t ssg_get_addr(
    ssg_group_id_t group_id,
    ssg_member_id_t member_id);

#ifdef __cplusplus
}
#endif
