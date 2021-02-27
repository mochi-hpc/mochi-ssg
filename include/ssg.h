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

/* opaque SSG group ID type */
typedef uint64_t ssg_group_id_t;
#define SSG_GROUP_ID_INVALID 0

/* SSG group member ID type */
typedef uint64_t ssg_member_id_t;
#define SSG_MEMBER_ID_INVALID 0

#define SSG_ALL_MEMBERS (-1)

typedef struct ssg_group_config
{
    int32_t swim_period_length_ms;          /* period length in miliseconds */
    int32_t swim_suspect_timeout_periods;   /* suspicion timeout in periods */
    int32_t swim_subgroup_member_count;     /* iping subgroup count */
    uint8_t swim_disabled;                  /* boolean indicating whether to disable SWIM */
    int64_t ssg_credential;                 /* generic credential to be stored with group */
} ssg_group_config_t;

/* initializer macro to ensure SSG ignores unset config params */
#define SSG_GROUP_CONFIG_INITIALIZER \
{\
    .swim_period_length_ms = 0, \
    .swim_suspect_timeout_periods = -1, \
    .swim_subgroup_member_count = -1, \
    .swim_disabled = 0, \
    .ssg_credential = -1, \
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

/* Errors in SSG are int32_t. The most-significant byte stores the Mercury error, if any.
 * The second most-significant byte stores the Argobots error, if any. The next 2 bytes store
 * the SSG error code defined bellow. Argobots and Mercury errors should be built using
 * SSG_MAKE_HG_ERROR and SSG_MAKE_ABT_ERROR. */

#define SSG_RETURN_VALUES \
    X(SSG_SUCCESS,                  "Success") \
    X(SSG_ERR_ALREADY_INITIALIZED,  "Already initialized") \
    X(SSG_ERR_NOT_INITIALIZED,      "Not initialized") \
    X(SSG_ERR_ALLOCATION,           "Allocation error") \
    X(SSG_ERR_INVALID_ARG,          "Invalid argument") \
    X(SSG_ERR_INVALID_ADDRESS,      "Invalid address") \
    X(SSG_ERR_INVALID_OPERATION,    "Invalid operation") \
    X(SSG_ERR_GROUP_NOT_FOUND,      "Group not found") \
    X(SSG_ERR_MEMBER_NOT_FOUND,     "Member not found") \
    X(SSG_ERR_SELF_NOT_FOUND,       "Self not found") \
    X(SSG_ERR_MID_NOT_FOUND,        "Margo instance not found") \
    X(SSG_ERR_GROUP_EXISTS,         "Group exists") \
    X(SSG_ERR_FILE_IO,              "File I/O") \
    X(SSG_ERR_FILE_FORMAT,          "File format") \
    X(SSG_ERR_NOT_SUPPORTED,        "Not supported") \
    X(SSG_ERR_MARGO_FAILURE,        "margo failure") \
    X(SSG_ERR_PMIX_FAILURE,         "PMIx failure") \
    X(SSG_ERR_NOBUFS,               "No buffer space") \
    X(SSG_ERR_MAX,                  "End of range for valid error codes")

#define X(__err__, __msg__) __err__,
typedef enum { SSG_RETURN_VALUES } ssg_return_t;
#undef X

static const char* const ssg_error_messages[] = {
#define X(__err__, __msg__) __msg__,
    SSG_RETURN_VALUES
#undef X
};

#define SSG_MAKE_HG_ERROR(__hg_err__) \
    (((int32_t)(__hg_err__)) << 24)

#define SSG_MAKE_ABT_ERROR(__abt_err__) \
    (((int32_t)(__abt_err__)) << 16)

#define SSG_GET_HG_ERROR(__err__) \
    (((__err__) & (0b11111111 << 24)) >> 24)

#define SSG_GET_ABT_ERROR(__err__) \
    (((__err__) & (0b11111111 << 16)) >> 16)

#define SSG_ERROR_IS_HG(__err__) \
    ((__err__) & (0b11111111 << 24))

#define SSG_ERROR_IS_ABT(__err__) \
    ((__err__) & (0b11111111 << 16))

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
 * @param[in]  mid              Corresponding Margo instance identifier
 * @param[in]  group_name       Name of the SSG group
 * @param[in]  group_addr_strs  Array of HG address strings for each group member
 * @param[in]  group_size       Number of group members
 * @param[in]  group_conf       Configuration parameters for the group
 * @param[in]  update_cb        Callback function executed on group membership changes
 * @param[in]  update_cb_dat    User data pointer passed to membership update callback
 * @param[out] group_id         Group identifier for created group (SSG_GROUP_ID_INVALID on failure)
 * @returns SSG_SUCCESS on success, SSG error code otherwise
 *
 * NOTE: The HG address string of the caller of this function must be present in
 * the list of address strings given in 'group_addr_strs'. That is, the caller
 * of this function is required to be a member of the SSG group that is created.
 */
int ssg_group_create(
    margo_instance_id mid,
    const char * group_name,
    const char * const group_addr_strs[],
    int group_size,
    ssg_group_config_t *group_conf,
    ssg_membership_update_cb update_cb,
    void * update_cb_dat,
    ssg_group_id_t *group_id);

/**
 * Creates an SSG group from a given config file containing the HG address strings
 * of all group members. A 'NULL' value for 'group_conf' will use SSG defaults for
 * all configuration parameters.
 *
 * @param[in]  mid              Corresponding Margo instance identifier
 * @param[in]  group_name       Name of the SSG group
 * @param[in]  file_name        Name of the config file containing the corresponding
 *                              HG address strings for this group
 * @param[in]  group_conf       Configuration parameters for the group
 * @param[in]  update_cb        Callback function executed on group membership changes
 * @param[in]  update_cb_dat    User data pointer passed to membership update callback
 * @param[out] group_id         Group identifier for created group (SSG_GROUP_ID_INVALID on failure)
 * @returns SSG_SUCCESS on success, SSG error code otherwise
 *
 * 
 * NOTE: The HG address string of the caller of this function must be present in
 * the list of address strings given in the config file. That is, the caller of
 * this function is required to be a member of the SSG group that is created.
 */
int ssg_group_create_config(
    margo_instance_id mid,
    const char * group_name,
    const char * file_name,
    ssg_group_config_t *group_conf,
    ssg_membership_update_cb update_cb,
    void * update_cb_dat,
    ssg_group_id_t *group_id);

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
 */
#define ssg_group_join(mid, group_id, update_cb, update_cb_dat) \
    ssg_group_join_target(mid, group_id, NULL, update_cb, update_cb_dat)

/**
 * Adds the calling process to an SSG group, specifying the address string
 * of the target group member to send the request to.
 *
 * @param[in] mid               Corresponding Margo instance identifier
 * @param[in] group_id          Input SSG group ID
 * @param[in] target_addr_str   Address string of group member target
 * @param[in] update_cb         Callback function executed on group membership changes
 * @param[in] update_cb_dat     User data pointer passed to membership update callback
 * @returns SSG_SUCCESS on success, SSG error code otherwise
 */
int ssg_group_join_target(
    margo_instance_id mid,
    ssg_group_id_t group_id,
    const char * target_addr_str,
    ssg_membership_update_cb update_cb,
    void * update_cb_dat);

/**
 * Removes the calling process from an SSG group.
 *
 * @param[in] group_id SSG group ID
 * @returns SSG_SUCCESS on success, SSG error code otherwise
 */
#define ssg_group_leave(group_id) \
    ssg_group_leave_target(group_id, NULL)

/**
 * Removes the calling process from an SSG group, specifying the address string
 * of the target group member to send the request to.
 *
 * @param[in] group_id SSG group ID
 * @param[in] target_addr_str   Address string of group member target
 * @returns SSG_SUCCESS on success, SSG error code otherwise
 */
int ssg_group_leave_target(
    ssg_group_id_t group_id,
    const char * target_addr_str);

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
#define ssg_group_observe(mid, group_id) \
    ssg_group_observe_target(mid, group_id, NULL)

/**
 * Initiates a client's observation of an SSG group, specifying the address string
 * of the target group member to send the request to.
 *
 * @param[in] mid               Corresponding Margo instance identifier
 * @param[in] group_id          SSG group ID
 * @param[in] target_addr_str   Address string of group member target
 * @returns SSG_SUCCESS on success, SSG error code otherwise
 *
 * NOTE: The "client" cannot be a member of the group -- observation is merely
 * a way of making the membership view of an existing SSG group available to
 * non-group members.
 */
int ssg_group_observe_target(
    margo_instance_id mid,
    ssg_group_id_t group_id,
    const char * target_addr_str);

/**
 * Terminates a client's observation of an SSG group.
 *
 * @param[in] group_id SSG group ID
 * @returns SSG_SUCCESS on success, SSG error code otherwise
 */
int ssg_group_unobserve(
    ssg_group_id_t group_id);

/**************************************
 *** SSG membership update routines ***
 **************************************/

/**
 * @brief Add a membership update callback to the group.
 * The callback is uniquely identified by the pair
 * <update_cb, update_cb_dat>, hence this function will
 * not add the callback if it was already added to the group.
 * The pair <update_cb, update_cb_dat> should also be used
 * to remove callbacks.
 *
 * @param group_id Group id
 * @param update_cb Update callback
 * @param update_cb_dat Data for the callback
 *
 * @return SSG_SUCCESS on success, SSG error code otherwise
 */
int ssg_group_add_membership_update_callback(
        ssg_group_id_t group_id,
        ssg_membership_update_cb update_cb,
        void* update_cb_dat);

/**
 * @brief Removes a membership update callback from the group.
 * The callback is uniquely identified by the pair
 * <update_cb, update_cb_dat>.
 *
 * @param group_id Group id
 * @param update_cb Update callback
 * @param update_cb_dat Data for the callback
 *
 * @return SSG_SUCCESS on success, SSG error code otherwise
 */
int ssg_group_remove_membership_update_callback(
        ssg_group_id_t group_id,
        ssg_membership_update_cb update_cb,
        void* update_cb_dat);

/*********************************
 *** SSG group access routines ***
 *********************************/

/**
 * Obtains the caller's member ID in the given SSG group.
 *
 * @param[in]  mid      Corresponding Margo instance identifier
 * @param[out] self_id  Caller's member ID (SSG_MEMBER_ID_INVALID on failure)
 * @returns SSG_SUCCESS on success, SSG error code otherwise
 */
int ssg_get_self_id(
    margo_instance_id mid,
    ssg_member_id_t *self_id);

/**
 * Obtains the size of a given SSG group.
 *
 * @param[in]  group_id     SSG group ID
 * @param[out] group_size   Size of the given group (0 on failure)
 * @returns SSG_SUCCESS on success, SSG error code otherwise
 */
int ssg_get_group_size(
    ssg_group_id_t group_id,
    int *group_size);

/**
 * Obtains the HG address of a member in a given SSG group.
 *
 * @param[in]  group_id     SSG group ID
 * @param[in]  member_id    SSG group member ID
 * @param[out] member_addr  HG address of given group member (HG_ADDR_NULL on failure)
 * @returns SSG_SUCCESS on success, SSG error code otherwise
 */
int ssg_get_group_member_addr(
    ssg_group_id_t group_id,
    ssg_member_id_t member_id,
    hg_addr_t *member_addr);

/**
 * Obtains the rank of the caller in a given SSG group.
 *
 * @param[in]  group_id     SSG group ID
 * @param[out] self_rank    Self rank (-1 on failure)
 * @returns SSG_SUCCESS on success, SSG error code otherwise
 */
int ssg_get_group_self_rank(
    ssg_group_id_t group_id,
    int *self_rank);

/**
 * Obtains the rank of a member in a given SSG group.
 *
 * @param[in]  group_id     SSG group ID
 * @param[in]  member_id    SSG group member ID
 * @param[out] member_rank  Member rank (-1 on failure)
 * @returns SSG_SUCCESS on success, SSG error code otherwise
 */
int ssg_get_group_member_rank(
    ssg_group_id_t group_id,
    ssg_member_id_t member_id,
    int *member_rank);

/**
 * Obtains the SSG member ID of the given group and rank.
 *
 * @param[in]  group_id     SSG group ID
 * @param[in]  rank         SSG group rank
 * @param[out] member_id    Member ID corresponding to the given rank (SSG_MEMBER_ID_INVALID on failure)
 * @returns SSG_SUCCESS on success, SSG error code otherwise
 */
int ssg_get_group_member_id_from_rank(
    ssg_group_id_t group_id,
    int rank,
    ssg_member_id_t *member_id);

/**
 * Obtains an array of SSG member IDs for a given rank range.
 *
 * @param[in]     group_id      SSG group ID
 * @param[in]     rank_start    Rank of range start (inclusive)
 * @param[in]     rank_end      Rank of range end (inclusive)
 * @param[in,out] range_ids     Buffer to store member IDs of requested range
 * @returns SSG_SUCCESS on success, SSG error code otherwise
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
 * @param[in]  group_id     SSG group ID
 * @param[in]  addr_index   Index (0-based) in GID's address list array
 * @param[out] addr_str     Group member address string on success (NULL on failure)
 * @returns SSG_SUCCESS on success, SSG error code otherwise
 * 
 * NOTE: returned string must be freed by caller.
 */
int ssg_group_id_get_addr_str(
    ssg_group_id_t group_id,
    unsigned int addr_index,
    char **addr_str);

/**
 * Retrieves the credential associated with an SSG group identifier.
 *
 * @param[in]  group_id SSG group ID
 * @param[out] cred     Returned credential (-1 on failure or missing credential)
 * @returns SSG_SUCCESS on success, SSG error code otherwise
 */
int ssg_group_id_get_cred(
    ssg_group_id_t group_id,
    int64_t *cred);

/**
 * Serializes an SSG group identifier into a buffer.
 *
 * @param[in]   group_id    SSG group ID
 * @param[in]   num_addrs   Number of group addressses to serialize (SSG_ALL_MEMBERS for all)
 * @param[out]  buf_p       Pointer to store allocated buffer in
 * @param[out]  buf_size_p  Pointer to store buffer size in
 * @returns SSG_SUCCESS on success, SSG error code otherwise
 */
int ssg_group_id_serialize(
    ssg_group_id_t group_id,
    int num_addrs,
    char ** buf_p,
    size_t * buf_size_p);

/**
 * Deserializes an SSG group identifier from a buffer.
 *
 * @param[in]       buf         Buffer containing the SSG group identifier
 * @param[in]       buf_size    Size of given buffer
 * @param[in/out]   num_addrs   Number of group addresses deserialized (input serves as max)
 * @param[out]      group_id_p  Pointer to store group identifier in
 * @returns SSG_SUCCESS on success, SSG error code otherwise
 */
int ssg_group_id_deserialize(
    const char * buf,
    size_t buf_size,
    int * num_addrs,
    ssg_group_id_t * group_id_p);

/**
 * Stores an SSG group identifier in the given file name.
 *
 * @param[in]   file_name   File to store the group ID in
 * @param[in]   group_id    SSG group ID
 * @param[in]   num_addrs   Number of group addressses to serialize (SSG_ALL_MEMBERS for all)
 * @returns SSG_SUCCESS on success, SSG error code otherwise
 */
int ssg_group_id_store(
    const char * file_name,
    ssg_group_id_t group_id,
    int num_addrs);

/**
 * Loads an SSG group identifier from the given file name.
 *
 * @param[in]       file_name   File to store the group ID in
 * @param[in/out]   num_addrs   Number of group addresses deserialized (input serves as max)
 * @param[out]      group_id_p  Pointer to store group identifier in
 * @returns SSG_SUCCESS on success, SSG error code otherwise
 */
int ssg_group_id_load(
    const char * file_name,
    int * num_addrs,
    ssg_group_id_t * group_id_p);

/** Dumps details of caller's membership in a given group to stdout.
 *
 * @param[in] group_id SSG group ID
 * @returns SSG_SUCCESS on success, SSG error code otherwise
 */
int ssg_group_dump(
    ssg_group_id_t group_id);

#ifdef __cplusplus
}
#endif
