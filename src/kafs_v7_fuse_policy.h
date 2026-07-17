#pragma once

#include "kafs_context.h"

#include <errno.h>

typedef enum kafs_v7_controlled_write_op
{
  KAFS_V7_CONTROLLED_WRITE_OP_INVALID = 0,
  KAFS_V7_CONTROLLED_WRITE_OP_CREATE,
  KAFS_V7_CONTROLLED_WRITE_OP_WRITE,
  KAFS_V7_CONTROLLED_WRITE_OP_FSYNC,
  KAFS_V7_CONTROLLED_WRITE_OP_RELEASE,
} kafs_v7_controlled_write_op_t;

static inline void kafs_v7_fuse_policy_set_controlled_write(kafs_context_t *ctx, int enabled)
{
  if (ctx)
    ctx->c_v7_controlled_write_enabled = enabled ? 1u : 0u;
}

static inline int kafs_v7_fuse_policy_controlled_write_active(const kafs_context_t *ctx)
{
  return ctx && ctx->c_v7_controlled_write_enabled;
}

/*
 * Shared v4/v5 mutation implementations may never serve a v7 controlled-write
 * context. A future v7 planner must bypass this guard only after it has built a
 * complete v7-owned transaction.
 */
static inline int kafs_v7_fuse_policy_reject_legacy_mutation(const kafs_context_t *ctx)
{
  return kafs_v7_fuse_policy_controlled_write_active(ctx) ? -EOPNOTSUPP : 0;
}

static inline int kafs_v7_fuse_policy_check_controlled_write(const kafs_context_t *ctx,
                                                             kafs_v7_controlled_write_op_t op)
{
  if (!kafs_v7_fuse_policy_controlled_write_active(ctx))
    return -EROFS;

  switch (op)
  {
  case KAFS_V7_CONTROLLED_WRITE_OP_CREATE:
  case KAFS_V7_CONTROLLED_WRITE_OP_WRITE:
  case KAFS_V7_CONTROLLED_WRITE_OP_FSYNC:
  case KAFS_V7_CONTROLLED_WRITE_OP_RELEASE:
    return 0;
  case KAFS_V7_CONTROLLED_WRITE_OP_INVALID:
  default:
    return -EOPNOTSUPP;
  }
}
