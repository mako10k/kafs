#pragma once

#include "kafs.h"
#include "kafs_context.h"

#include <errno.h>

static inline int kafs_v6_controlled_write_active(const kafs_context_t *ctx)
{
  return ctx && ctx->c_v6_controlled_write_enabled;
}

static inline int kafs_v6_controlled_write_reject(const kafs_context_t *ctx, const char *op)
{
  if (!kafs_v6_controlled_write_active(ctx))
    return 0;

  kafs_log(KAFS_LOG_WARNING,
           "kafs: format v6 controlled write mount rejects %s; initial surface is "
           "regular-file create/write/fsync/release only\n",
           op ? op : "operation");
  return -EOPNOTSUPP;
}

static inline int kafs_v6_controlled_write_reject_if(const kafs_context_t *ctx, int condition,
                                                     const char *op)
{
  if (!condition)
    return 0;

  return kafs_v6_controlled_write_reject(ctx, op);
}

static inline int kafs_v6_controlled_write_require_regular_write(const kafs_context_t *ctx,
                                                                 int is_regular)
{
  if (!kafs_v6_controlled_write_active(ctx) || is_regular)
    return 0;

  return -EOPNOTSUPP;
}
