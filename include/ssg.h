/*
 * Copyright (c) 2016 UChicago Argonne, LLC
 *
 * See COPYRIGHT in top-level directory.
 */

#pragma once

#include <mercury.h>
#include <margo.h>

#include <stdint.h>
#include <inttypes.h>

/** @file ssg.h
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

/* opaque SSG group ID type */
typedef struct ssg_group_descriptor *ssg_group_id_t;
#define SSG_GROUP_ID_NULL ((ssg_group_id_t)NULL)

/* SSG group member ID type */
typedef uint64_t ssg_member_id_t;
#define SSG_MEMBER_ID_INVALID 0

/* SSG group member update types */
typedef enum ssg_member_update_type
{
    SSG_MEMBER_JOINED = 0,
    SSG_MEMBER_LEFT,
    SSG_MEMBER_DIED
} ssg_member_update_type_t;

typedef void (*ssg_membership_update_cb)(
    void * group_data,
    ssg_member_id_t member_id,
    ssg_member_update_type_t update_type);

/* HG proc routine prototypes for SSG types */
#define hg_proc_ssg_member_id_t hg_proc_int64_t
hg_return_t hg_proc_ssg_group_id_t(hg_proc_t proc, void *data);

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
 * @param[in] group_name        Name of the SSG group
 * @param[in] group_addr_strs   Array of HG address strings for each group member
 * @param[in] group_size        Number of group members
 * @param[in] update_cb         Callback function executed on group membership changes
 * @param[in] update_cb_dat     User data pointer passed to membership update callback
 * @returns SSG group identifier for created group on success, SSG_GROUP_ID_NULL otherwise
 *
 * NOTE: The HG address string of the caller of this function must be present in
 * the list of address strings given in 'group_addr_strs'. That is, the caller
 * of this function is required to be a member of the SSG group that is created.
 */
ssg_group_id_t ssg_group_create(
    const char * group_name,
    const char * const group_addr_strs[],
    int group_size,
    ssg_membership_update_cb update_cb,
    void * update_cb_dat);

/**
 * Creates an SSG group from a given config file containing the HG address strings
 * of all group members.
 *
 * @param[in] group_name    Name of the SSG group
 * @param[in] file_name     Name of the config file containing the corresponding
 *                          HG address strings for this group
 * @param[in] update_cb         Callback function executed on group membership changes
 * @param[in] update_cb_dat     User data pointer passed to membership update callback
 * @returns SSG group identifier for created group on success, SSG_GROUP_ID_NULL otherwise
 *
 * 
 * NOTE: The HG address string of the caller of this function must be present in
 * the list of address strings given in the config file. That is, the caller of
 * this function is required to be a member of the SSG group that is created.
 */
ssg_group_id_t ssg_group_create_config(
    const char * group_name,
    const char * file_name,
    ssg_membership_update_cb update_cb,
    void * update_cb_dat);

/**
 * Destroys data structures associated with a given SSG group ID.
 *
 * @param[in] group_id SSG group ID
 * @returns SSG_SUCCESS on success, SSG error code otherwise
 */
int ssg_group_destroy(
    ssg_group_id_t group_id);

/**
 * Adds the calling process to an SSG group.
 *
 * @param[in] in_group_id       Input SSG group ID
 * @param[in] update_cb         Callback function executed on group membership changes
 * @param[in] update_cb_dat     User data pointer passed to membership update callback
 * @returns SSG group identifier for joined group on success, SSG_GROUP_ID_NULL otherwise
 *
 * NOTE: Use the returned group ID to refer to the group, as the input group ID
 *       becomes stale after the join is completed.
 */
ssg_group_id_t ssg_group_join(
    ssg_group_id_t in_group_id,
    ssg_membership_update_cb update_cb,
    void * update_cb_dat);

/**
 * Removes the calling process from an SSG group.
 *
 * @param[in] group_id SSG group ID
 * @returns SSG_SUCCESS on success, SSG error code otherwise
 */
int ssg_group_leave(
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
 * @returns caller's group ID on success, SSG_MEMBER_ID_INVALID otherwise
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

/**
 * Duplicates the given SSG group identifier.
 *
 * @param[in] group_id SSG group ID
 * @returns SSG group identifier on success, SSG_GROUP_ID_NULL otherwise
 */
ssg_group_id_t ssg_group_id_dup(
    ssg_group_id_t group_id);

/** Frees the given SSG group identifier.
 *
 * @param[in] group_id SSG group ID
 */
void ssg_group_id_free(
    ssg_group_id_t group_id);

/**
 * Retrieves the HG address string associated with an SSG group identifier.
 *
 * @param[in] group_id SSG group ID
 * @returns address string on success, NULL otherwise
 * 
 * NOTE: returned string must be freed by caller.
 */
char *ssg_group_id_get_addr_str(
    ssg_group_id_t group_id);

/**
 * Serializes an SSG group identifier into a buffer.
 *
 * @param[in]   group_id    SSG group ID
 * @param[out]  buf_p       Pointer to store allocated buffer in
 * @param[out]  buf_size_p  Pointer to store buffer size in
 */
void ssg_group_id_serialize(
    ssg_group_id_t group_id,
    char ** buf_p,
    size_t * buf_size_p);

/**
 * Deserializes an SSG group identifier from a buffer.
 *
 * @param[in]   buf         Buffer containing the SSG group identifier
 * @param[in]   buf_size    Size of given buffer
 * @param[out]  group_id_p  Pointer to store group identifier in
 */
void ssg_group_id_deserialize(
    const char * buf,
    size_t buf_size,
    ssg_group_id_t * group_id_p);

/**
 * Stores an SSG group identifier in the given file name.
 *
 * @param[in]   file_name   File to store the group ID in
 * @param[in]   group_id    SSG group ID
 */
int ssg_group_id_store(
    const char * file_name,
    ssg_group_id_t group_id);

/**
 * Loads an SSG group identifier from the given file name.
 *
 * @param[in]   file_name   File to store the group ID in
 * @param[out]  group_id_p  Pointer to store group identifier in
 */
int ssg_group_id_load(
    const char * file_name,
    ssg_group_id_t * group_id_p);

/** Dumps details of caller's membership in a given group to stdout.
 *
 * @param[in] group_id SSG group ID
 */
void ssg_group_dump(
    ssg_group_id_t group_id);

#ifdef __cplusplus
}
#endif
