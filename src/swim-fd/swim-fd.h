/*
 * (C) 2016 The University of Chicago
 * 
 * See COPYRIGHT in top-level directory.
 */

#ifndef __SWIM_FD_H
#define __SWIM_FD_H

#ifdef __cplusplus
extern "C" {
#endif

#include <ssg.h>

/* opaque swim context type */
typedef struct swim_context swim_context_t;

swim_context_t *swim_init(
    margo_instance_id m_id,
    ssg_t swim_group,
    int active);

void swim_finalize(
    swim_context_t *swim_ctx);

#ifdef __cplusplus
}
#endif

#endif /* __SWIM_FD_H */

