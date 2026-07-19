#pragma once

#include "kafs.h"
#include "kafs_context.h"
#ifdef KAFS_V7_RUNTIME_ENTRYPOINT
#include "kafs_v7_runtime_view.h"
#endif

/*
 * Format-v6 FUSE init policy.
 *
 * Shared FUSE operation implementations live in kafs_shared_fuse_runtime.c,
 * but format-v6 runtime mounts must keep delayed/background mutation workers
 * suppressed.
 */

static inline int kafs_v6_fuse_init_suppresses_background_workers(kafs_context_t *ctx)
{
  if (!ctx || !ctx->c_superblock ||
      !kafs_format_uses_layout_descriptor(kafs_sb_format_version_get(ctx->c_superblock)))
    return 0;

  int rc;
#ifdef KAFS_V7_RUNTIME_ENTRYPOINT
  if (kafs_sb_format_version_get(ctx->c_superblock) == KAFS_FORMAT_VERSION_V7)
    rc = kafs_v7_runtime_view_validate_policy(ctx);
  else
#endif
    rc = kafs_ctx_v6_validate_worker_policy(ctx);
  if (rc != 0)
    kafs_log(
        KAFS_LOG_ERR,
        "kafs: invalid format v%u worker policy in FUSE init rc=%d; delayed/background workers "
        "remain suppressed\n",
        kafs_ctx_inode_format(ctx), rc);
  return 1;
}
