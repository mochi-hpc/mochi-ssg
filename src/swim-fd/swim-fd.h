/*
 * Copyright (c) 2016 UChicago Argonne, LLC
 *
 * See COPYRIGHT in top-level directory.
 */

#pragma once

#include <stdint.h>
#include <inttypes.h>

#ifdef __cplusplus
extern "C" {
#endif

/* opaque swim context type */
typedef struct swim_context swim_context_t;

/* swim member specific types */
typedef uint64_t swim_member_id_t;
typedef uint32_t swim_member_inc_nr_t;
typedef enum swim_member_status
{
    SWIM_MEMBER_ALIVE = 0,
    SWIM_MEMBER_SUSPECT,
    SWIM_MEMBER_DEAD
} swim_member_status_t;

/* SWIM state associated with each group member */
typedef struct swim_member_state
{
    swim_member_inc_nr_t inc_nr;
    swim_member_status_t status;
} swim_member_state_t;

/* SWIM protocol update */
typedef struct swim_member_update
{
    swim_member_id_t id;
    swim_member_state_t state;
} swim_member_update_t;

#define SWIM_MEMBER_STATE_INIT(__ms) do { \
    __ms.inc_nr = 0; \
    __ms.status = SWIM_MEMBER_ALIVE; \
} while(0)

/* SWIM callbacks for integrating with an overlying group management layer */
typedef struct swim_group_mgmt_callbacks
{
    /**
     * Retrieve a (non-dead) random group member from the group
     * management layer to send a direct ping request to.
     * NOTE: to ensure time-bounded detection of faulty members,
     * round-robin selection of members is required.
     *
     * @param[in]  group_data   void pointer to group managment data
     * @param[out] target_id    ID of selected direct ping target
     * @param[out] inc_nr       SWIM incarnation number of target
     * @param[out] target_addr  HG address of target
     * @returns 1 on successful selection of a target, 0 if no targets available
     */
    int (*get_dping_target)(
            void *group_data,
            swim_member_id_t *target_id,
            swim_member_inc_nr_t *inc_nr,
            hg_addr_t *target_addr
            );
    /**
     * Retrieve a set of (non-dead) random group members from the group
     * management layer to send indirect ping requests to.
     *
     * @param[in]  group_data       void pointer to group managment data
     * @param[out] target_ids       IDs of selected indirect ping targets
     * @param[out] target_addrs     HG addresses of targets
     * @returns number of selected indirect ping targets, 0 if no targets available
     */
    int (*get_iping_targets)(
            void *group_data,
            swim_member_id_t *target_ids,
            hg_addr_t *target_addrs
            );
    /**
     * Get the HG address corresponding to a given member ID.
     *
     * @param[in]  group_data   void pointer to group managment data
     * @param[in]  id           member ID to query
     * @param[out] addr         HG address of given member
     */
    void (*get_member_addr)(
            void *group_data,
            swim_member_id_t id,
            hg_addr_t *addr
            );
    /**
     * Get the SWIM protocol state corresponding to a given member ID.
     *
     * @param[in]  group_data   void pointer to group managment data
     * @param[in]  id           member ID to query
     * @param[out] state        pointer to given member's SWIM state
     */
    void (*get_member_state)(
            void *group_data,
            swim_member_id_t id,
            swim_member_state_t **state
            );
    /**
     * Apply a SWIM protocol update in the group management layer.
     *
     * @param[in] group_data    void pointer to group managment data
     * @param[in] update        SWIM member update to apply to group
     */
    void (*apply_member_update)(
            void *group_data,
            swim_member_update_t update
            );
} swim_group_mgmt_callbacks_t;

/**
 * Initialize the SWIM protocol.
 *
 * @param[in] mid               Margo instance ID
 * @param[in] group_data        void pointer to group management data
 * @param[in] self_id           ID
 * @param[in] swim_callbacks    SWIM callbacks to group management layer
 * @param[in] active            boolean value indicating whether member should actively ping
 * @returns SWIM context pointer on success, NULL otherwise
 */
swim_context_t * swim_init(
    margo_instance_id mid,
    void * group_data,
    swim_member_id_t self_id,
    swim_group_mgmt_callbacks_t swim_callbacks,
    int active);

/**
 * Finalize the SWIM protocol.
 *
 * @param[in] swim_ctx  SWIM context pointer
 */
void swim_finalize(
    swim_context_t * swim_ctx);

#ifdef __cplusplus
}
#endif
