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
typedef uint64_t ssg_group_id_t;
#define SSG_GROUP_ID_INVALID 0

/* SSG group member ID type */
typedef uint64_t ssg_member_id_t;
#define SSG_MEMBER_ID_INVALID 0

typedef struct ssg_group_config
{
    int swim_period_length_ms;          /* period length in miliseconds */
    int swim_suspect_timeout_periods;   /* suspicion timeout in periods */
    int swim_subgroup_member_count;     /* iping subgroup count */
} ssg_group_config_t;

/* initializer macro to ensure SSG ignores unset config params */
#define SSG_GROUP_CONFIG_INITIALIZER \
{\
    .swim_period_length_ms = 0, \
    .swim_suspect_timeout_periods = -1, \
    .swim_subgroup_member_count = -1, \
}\

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
#define hg_proc_ssg_group_id_t hg_proc_uint64_t
#define hg_proc_ssg_member_id_t hg_proc_uint64_t

/***************************************************
 *** SSG runtime intialization/shutdown routines ***
 ***************************************************/

/**
 * Initializes the SSG runtime environment.
 *
 * @returns SSG_SUCCESS on success, SSG error code otherwise
 */
int ssg_init(
    void);

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
 * Creates an SSG group from a given list of HG address strings. A 'NULL' value for
 * 'group_conf' will use SSG defaults for all configuration parameters.
 *
 * @param[in] mid               Corresponding Margo instance identifier
 * @param[in] group_name        Name of the SSG group
 * @param[in] group_addr_strs   Array of HG address strings for each group member
 * @param[in] group_size        Number of group members
 * @param[in] group_conf        Configuration parameters for the group
 * @param[in] update_cb         Callback function executed on group membership changes
 * @param[in] update_cb_dat     User data pointer passed to membership update callback
 * @returns SSG group identifier for created group on success, SSG_GROUP_ID_NULL otherwise
 *
 * NOTE: The HG address string of the caller of this function must be present in
 * the list of address strings given in 'group_addr_strs'. That is, the caller
 * of this function is required to be a member of the SSG group that is created.
 */
ssg_group_id_t ssg_group_create(
    margo_instance_id mid,
    const char * group_name,
    const char * const group_addr_strs[],
    int group_size,
    ssg_group_config_t *group_conf,
    ssg_membership_update_cb update_cb,
    void * update_cb_dat);

/**
 * Creates an SSG group from a given config file containing the HG address strings
 * of all group members. A 'NULL' value for 'group_conf' will use SSG defaults for
 * all configuration parameters.
 *
 * @param[in] mid               Corresponding Margo instance identifier
 * @param[in] group_name        Name of the SSG group
 * @param[in] file_name         Name of the config file containing the corresponding
 *                              HG address strings for this group
 * @param[in] group_conf        Configuration parameters for the group
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
    margo_instance_id mid,
    const char * group_name,
    const char * file_name,
    ssg_group_config_t *group_conf,
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
 * @param[in] mid               Corresponding Margo instance identifier
 * @param[in] group_id          Input SSG group ID
 * @param[in] update_cb         Callback function executed on group membership changes
 * @param[in] update_cb_dat     User data pointer passed to membership update callback
 * @returns SSG_SUCCESS on success, SSG error code otherwise
 *
 * NOTE: Use the returned group ID to refer to the group, as the input group ID
 *       becomes stale after the join is completed.
 */
int ssg_group_join(
    margo_instance_id mid,
    ssg_group_id_t group_id,
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
 * Initiates a client's observation of an SSG group.
 *
 * @param[in] mid       Corresponding Margo instance identifier
 * @param[in] group_id  SSG group ID
 * @returns SSG_SUCCESS on success, SSG error code otherwise
 *
 * NOTE: The "client" cannot be a member of the group -- observation is merely
 * a way of making the membership view of an existing SSG group available to
 * non-group members.
 */
int ssg_group_observe(
    margo_instance_id mid,
    ssg_group_id_t group_id);

/**
 * Terminates a client's observation of an SSG group.
 *
 * @param[in] group_id SSG group ID
 * @returns SSG_SUCCESS on success, SSG error code otherwise
 */
int ssg_group_unobserve(
    ssg_group_id_t group_id);

/*********************************
 *** SSG group access routines ***
 *********************************/

/**
 * Obtains the caller's member ID in the given SSG group.
 *
 * @param[in] mid Corresponding Margo instance identifier
 * @returns caller's member ID on success, SSG_MEMBER_ID_INVALID otherwise
 */
ssg_member_id_t ssg_get_self_id(
    margo_instance_id mid);

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
hg_addr_t ssg_get_group_member_addr(
    ssg_group_id_t group_id,
    ssg_member_id_t member_id);

/**
 * Obtains the rank of the caller in a given SSG group.
 *
 * @param[in] group_id SSG group ID
 * @returns rank on success, -1 on failure
 */
int ssg_get_group_self_rank(
    ssg_group_id_t group_id);

/**
 * Obtains the rank of a member in a given SSG group.
 *
 * @param[in] group_id  SSG group ID
 * @param[in] member_id SSG group member ID
 * @returns rank on success, -1 otherwise
 */
int ssg_get_group_member_rank(
    ssg_group_id_t group_id,
    ssg_member_id_t member_id);

/**
 * Obtains the SSG member ID of the given group and rank.
 *
 * @param[in] group_id SSG group ID
 * @param[in] rank     SSG group rank
 * @returns caller's member ID on success, SSG_MEMBER_ID_INVALID otherwise
 */
ssg_member_id_t ssg_get_group_member_id_from_rank(
    ssg_group_id_t group_id,
    int rank);

/**
 * Obtains an array of SSG member IDs for a given rank range.
 *
 * @param[in] group_id      SSG group ID
 * @param[in] rank_start    Rank of range start
 * @param[in] rank_end      Rank of range end
 * @param[in,out] range_ids Buffer to store member IDs of requested range
 * @returns number of member IDs returned in range_ids on success, 0 otherwise
 *
 * NOTE: range_ids must be allocated by caller and must be large enough to hold
 *       requested range.
 */
int ssg_get_group_member_ids_from_range(
    ssg_group_id_t group_id,
    int rank_start,
    int rank_end,
    ssg_member_id_t *range_ids);

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
 * @returns SSG_SUCCESS on success, SSG error code otherwise
 */
int ssg_group_id_store(
    const char * file_name,
    ssg_group_id_t group_id);

/**
 * Loads an SSG group identifier from the given file name.
 *
 * @param[in]   file_name   File to store the group ID in
 * @param[out]  group_id_p  Pointer to store group identifier in
 * @returns SSG_SUCCESS on success, SSG error code otherwise
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
