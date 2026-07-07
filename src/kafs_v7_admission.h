#pragma once

#include "kafs_block.h"
#include "kafs_context.h"
#include "kafs_offline_summary.h"
#include "kafs_v6_layout.h"

#include <errno.h>
#include <stdint.h>
#include <string.h>

/*
 * v7 admission owns the v7 runtime boundary.  The descriptor scaffold parser is
 * still the historical v6 layout helper until the next neutral-name extraction.
 */

static inline int kafs_v7_admission_validate_descriptor_segments_fd(
    kafs_context_t *ctx, int fd, const kafs_ssuperblock_t *sbdisk, uint64_t file_size)
{
  if (!ctx || fd < 0 || !sbdisk)
    return -EINVAL;

  int rc = kafs_v6_descriptor_mapping_admit_fd(ctx, fd, file_size, NULL, NULL, NULL, NULL, NULL);
  if (rc != 0)
    return rc;

  kafs_v6_journal_segment_report_t journal_report;
  const kafs_ssuperblock_t *mapped_sb = ctx->c_superblock ? ctx->c_superblock : sbdisk;
  return kafs_v6_journal_validate_segments_fd(fd, ctx->c_v6_layout_desc,
                                              ctx->c_v6_layout_desc_bytes, mapped_sb, file_size,
                                              &journal_report);
}

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
    rc = kafs_v7_admission_validate_descriptor_segments_fd(&preflight_ctx, fd, sbdisk, file_size);

  kafs_bitmap_descriptor_mapping_clear(&preflight_ctx);
  return rc;
}

static inline int kafs_v7_admission_runtime_context(kafs_context_t *ctx,
                                                    const kafs_ssuperblock_t *sbdisk, int prot)
{
  if (!ctx || ctx->c_fd < 0 || !sbdisk)
    return -EINVAL;

  uint64_t file_size = 0;
  int rc = kafs_offline_detect_file_size(ctx->c_fd, &file_size);
  if (rc == 0)
    rc = kafs_ctx_map_v6_runtime_admission_memory(ctx, sbdisk, file_size, prot);
  if (rc == 0)
    rc = kafs_v7_admission_validate_descriptor_segments_fd(ctx, ctx->c_fd, sbdisk, file_size);
  if (rc == 0)
    rc = kafs_ctx_v6_validate_runtime_views(ctx);
  if (rc == 0)
  {
    kafs_ctx_v6_apply_delayed_mutation_policy(ctx);
    rc = kafs_ctx_v6_validate_worker_policy(ctx);
  }

  if (rc != 0)
    kafs_ctx_unmap_image(ctx);
  return rc;
}
