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
typedef uint32_t swim_member_inc_nr_t;
typedef enum swim_member_status
{
    SWIM_MEMBER_ALIVE = 0,
    SWIM_MEMBER_SUSPECT,
    SWIM_MEMBER_DEAD
} swim_member_status_t;

typedef struct swim_member_state
{
    swim_member_inc_nr_t inc_nr;
    swim_member_status_t status;
} swim_member_state_t;

#define SWIM_MEMBER_STATE_INIT(__ms) do { \
    __ms.inc_nr = 0; \
    __ms.status = SWIM_MEMBER_ALIVE; \
} while(0)

/* XXX rename once more clear what all is here */
typedef struct swim_group_mgmt_callbacks
{
    int (*get_dping_target)(
            void *group_data,
            hg_addr_t *target_addr,
            swim_member_state_t *target_ms
            );
    /* get_rand_iping_subgroup */
} swim_group_mgmt_callbacks_t;

/* Initialize SWIM */
swim_context_t * swim_init(
    margo_instance_id mid,
    void * group_data,
    swim_group_mgmt_callbacks_t swim_callbacks,
    int active);

/* Finalize SWIM */
void swim_finalize(
    swim_context_t * swim_ctx);

#ifdef __cplusplus
}
#endif
