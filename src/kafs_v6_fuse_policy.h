#pragma once

#include "kafs.h"
#include "kafs_context.h"

#include <errno.h>

typedef enum kafs_v6_controlled_write_op
{
  KAFS_V6_CONTROLLED_WRITE_OP_OPERATION = 0,
  KAFS_V6_CONTROLLED_WRITE_OP_REFLINK,
  KAFS_V6_CONTROLLED_WRITE_OP_COPY,
  KAFS_V6_CONTROLLED_WRITE_OP_COPY_FILE_RANGE,
  KAFS_V6_CONTROLLED_WRITE_OP_CONTROL_PLANE_OPEN,
  KAFS_V6_CONTROLLED_WRITE_OP_OPEN_TRUNC,
  KAFS_V6_CONTROLLED_WRITE_OP_MKNOD,
  KAFS_V6_CONTROLLED_WRITE_OP_TRUNCATE,
  KAFS_V6_CONTROLLED_WRITE_OP_FALLOCATE,
  KAFS_V6_CONTROLLED_WRITE_OP_MKDIR,
  KAFS_V6_CONTROLLED_WRITE_OP_RMDIR,
  KAFS_V6_CONTROLLED_WRITE_OP_CONTROL_PLANE_WRITE,
  KAFS_V6_CONTROLLED_WRITE_OP_HOTPLUG_DELEGATED_WRITE,
  KAFS_V6_CONTROLLED_WRITE_OP_UTIMENS,
  KAFS_V6_CONTROLLED_WRITE_OP_UNLINK,
  KAFS_V6_CONTROLLED_WRITE_OP_RENAME,
  KAFS_V6_CONTROLLED_WRITE_OP_CHMOD,
  KAFS_V6_CONTROLLED_WRITE_OP_CHOWN,
  KAFS_V6_CONTROLLED_WRITE_OP_SYMLINK,
  KAFS_V6_CONTROLLED_WRITE_OP_LINK,
  KAFS_V6_CONTROLLED_WRITE_OP_FSYNCDIR,
} kafs_v6_controlled_write_op_t;

static inline int kafs_v6_controlled_write_active(const kafs_context_t *ctx)
{
  return ctx && ctx->c_v6_controlled_write_enabled;
}

static inline const char *kafs_v6_controlled_write_op_name(kafs_v6_controlled_write_op_t op)
{
  switch (op)
  {
  case KAFS_V6_CONTROLLED_WRITE_OP_REFLINK:
    return "reflink";
  case KAFS_V6_CONTROLLED_WRITE_OP_COPY:
    return "copy";
  case KAFS_V6_CONTROLLED_WRITE_OP_COPY_FILE_RANGE:
    return "copy_file_range";
  case KAFS_V6_CONTROLLED_WRITE_OP_CONTROL_PLANE_OPEN:
    return "control-plane open";
  case KAFS_V6_CONTROLLED_WRITE_OP_OPEN_TRUNC:
    return "open(O_TRUNC)";
  case KAFS_V6_CONTROLLED_WRITE_OP_MKNOD:
    return "mknod";
  case KAFS_V6_CONTROLLED_WRITE_OP_TRUNCATE:
    return "truncate";
  case KAFS_V6_CONTROLLED_WRITE_OP_FALLOCATE:
    return "fallocate";
  case KAFS_V6_CONTROLLED_WRITE_OP_MKDIR:
    return "mkdir";
  case KAFS_V6_CONTROLLED_WRITE_OP_RMDIR:
    return "rmdir";
  case KAFS_V6_CONTROLLED_WRITE_OP_CONTROL_PLANE_WRITE:
    return "control-plane write";
  case KAFS_V6_CONTROLLED_WRITE_OP_HOTPLUG_DELEGATED_WRITE:
    return "hotplug delegated write";
  case KAFS_V6_CONTROLLED_WRITE_OP_UTIMENS:
    return "utimens";
  case KAFS_V6_CONTROLLED_WRITE_OP_UNLINK:
    return "unlink";
  case KAFS_V6_CONTROLLED_WRITE_OP_RENAME:
    return "rename";
  case KAFS_V6_CONTROLLED_WRITE_OP_CHMOD:
    return "chmod";
  case KAFS_V6_CONTROLLED_WRITE_OP_CHOWN:
    return "chown";
  case KAFS_V6_CONTROLLED_WRITE_OP_SYMLINK:
    return "symlink";
  case KAFS_V6_CONTROLLED_WRITE_OP_LINK:
    return "link";
  case KAFS_V6_CONTROLLED_WRITE_OP_FSYNCDIR:
    return "fsyncdir";
  case KAFS_V6_CONTROLLED_WRITE_OP_OPERATION:
  default:
    return "operation";
  }
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

static inline int kafs_v6_controlled_write_reject_op(const kafs_context_t *ctx,
                                                     kafs_v6_controlled_write_op_t op)
{
  return kafs_v6_controlled_write_reject(ctx, kafs_v6_controlled_write_op_name(op));
}

static inline int kafs_v6_controlled_write_reject_if_op(const kafs_context_t *ctx, int condition,
                                                        kafs_v6_controlled_write_op_t op)
{
  if (!condition)
    return 0;

  return kafs_v6_controlled_write_reject_op(ctx, op);
}

static inline int kafs_v6_controlled_write_require_regular_write(const kafs_context_t *ctx,
                                                                 int is_regular)
{
  if (!kafs_v6_controlled_write_active(ctx) || is_regular)
    return 0;

  return -EOPNOTSUPP;
}

static inline int kafs_v6_controlled_write_preserve_zero_block(const kafs_context_t *ctx)
{
  return kafs_v6_controlled_write_active(ctx);
}

static inline int kafs_v6_controlled_write_skip_tail_layout(const kafs_context_t *ctx)
{
  return kafs_v6_controlled_write_active(ctx);
}

static inline int kafs_v6_controlled_write_skip_release_reclaim(const kafs_context_t *ctx)
{
  return kafs_v6_controlled_write_active(ctx);
}

static inline int kafs_v6_controlled_write_use_local_write_path(const kafs_context_t *ctx)
{
  return kafs_v6_controlled_write_active(ctx);
}
