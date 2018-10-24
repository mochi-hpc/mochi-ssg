/*
 * Copyright (c) 2016 UChicago Argonne, LLC
 *
 * See COPYRIGHT in top-level directory.
 */

#pragma once

#include <stdint.h>
#include <inttypes.h>

#include "ssg.h"
#include "ssg-internal.h"

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

/* SWIM state associated with each group member */
typedef struct swim_member_state
{
    swim_member_inc_nr_t inc_nr;
    swim_member_status_t status;
} swim_member_state_t;

/* forward declarations to work around weird SSG/SWIM circular dependency */
struct ssg_group;
struct ssg_member_state;
struct ssg_member_update;

/**
 * Initialize SWIM protocol for the given SSG group and Margo instance.
 *
 * @param[in] group             pointer to SSG group associated with this SWIM context
 * @param[in] mid               Margo instance ID
 * @param[in] active            boolean value indicating whether member should actively ping
 * @returns SSG_SUCCESS on success, SSG_FAILURE otherwise
 */
int swim_init(
    struct ssg_group * group,
    margo_instance_id mid,
    int active);

/**
 * Finalize the given SSG group's SWIM protocol.
 *
 * @param[in] group     pointer to SSG group to finalize SWIM for
 */
void swim_finalize(
    struct ssg_group * group);

/**
 *
 * @returns SSG_SUCCESS on success, SSG_FAILURE otherwise
 */
int swim_apply_ssg_member_update(
    struct ssg_group * group,
    struct ssg_member_state * ms,
    struct ssg_member_update update);

#ifdef __cplusplus
}
#endif
