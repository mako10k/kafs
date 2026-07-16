#pragma once

#include "kafs_context.h"
#include "kafs_offline_summary.h"
#include "kafs_v7_runtime_view.h"

#include <errno.h>
#include <stdint.h>
#include <string.h>

static inline int kafs_v7_admission_preflight_core(int fd, const kafs_ssuperblock_t *sbdisk)
{
  if (fd < 0 || !sbdisk)
    return -EINVAL;

  uint64_t file_size = 0;
  int rc = kafs_offline_detect_file_size(fd, &file_size);

  kafs_context_t preflight_ctx;
  memset(&preflight_ctx, 0, sizeof(preflight_ctx));
  preflight_ctx.c_fd = fd;
  preflight_ctx.c_superblock = (kafs_ssuperblock_t *)sbdisk;

  if (rc == 0)
    rc = kafs_v7_runtime_view_admit_fd(&preflight_ctx, fd, sbdisk, file_size);

  kafs_ctx_v7_runtime_view_clear(&preflight_ctx);
  return rc;
}

static inline int kafs_v7_admission_runtime_context(kafs_context_t *ctx,
                                                    const kafs_ssuperblock_t *sbdisk, int prot)
{
  if (!ctx || ctx->c_fd < 0 || !sbdisk)
    return -EINVAL;
  if (prot != PROT_READ)
    return -EPROTONOSUPPORT;

  uint64_t file_size = 0;
  int rc = kafs_offline_detect_file_size(ctx->c_fd, &file_size);
  if (rc == 0)
    rc = kafs_ctx_map_descriptor_runtime_admission_memory(ctx, sbdisk, file_size, prot);
  if (rc == 0)
    rc = kafs_v7_runtime_view_admit_fd(ctx, ctx->c_fd, ctx->c_superblock, file_size);
  if (rc == 0)
    rc = kafs_v7_runtime_view_validate(ctx);
  if (rc == 0)
  {
    kafs_v7_runtime_view_seal_mutations(ctx);
    rc = kafs_v7_runtime_view_validate_policy(ctx);
  }

  if (rc != 0)
    kafs_ctx_unmap_image(ctx);
  return rc;
}
