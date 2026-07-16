#include "kafs_v7_runtime_view.h"

#include "kafs_v7_layout.h"

#include <errno.h>
#include <stdlib.h>

static int kafs_v7_runtime_view_build(const kafs_v7_layout_report_t *report,
                                      kafs_v7_inode_runtime_shard_t **inode_shards_out,
                                      kafs_v7_data_runtime_group_t **data_groups_out)
{
  if (!report || !inode_shards_out || !data_groups_out || report->group_count == 0u)
    return -EINVAL;

  const kafs_v7_layout_header_t *header = kafs_v7_report_header(report);
  const kafs_v7_group_desc_t *groups = kafs_v7_report_groups(report);
  const kafs_v7_shard_desc_t *shards = kafs_v7_report_shards(report);
  if (!header || !groups || !shards || le32toh(header->group_count) != report->group_count ||
      le32toh(header->shard_count) != report->shard_count)
    return -EUCLEAN;

  kafs_v7_inode_runtime_shard_t *inode_shards = calloc(report->group_count, sizeof(*inode_shards));
  kafs_v7_data_runtime_group_t *data_groups = calloc(report->group_count, sizeof(*data_groups));
  if (!inode_shards || !data_groups)
  {
    free(inode_shards);
    free(data_groups);
    return -ENOMEM;
  }

  int rc = 0;
  for (uint32_t group_index = 0; group_index < report->group_count; ++group_index)
  {
    const kafs_v7_group_desc_t *group = &groups[group_index];
    uint32_t group_id = le32toh(group->group_id);
    uint32_t first = le32toh(group->first_shard_index);
    uint32_t count = le32toh(group->shard_count);
    uint64_t data_logical_start = le64toh(group->data_logical_start);
    uint64_t data_logical_count = le64toh(group->data_logical_count);
    uint64_t data_physical_off = le64toh(group->data_physical_off);
    uint64_t data_physical_bytes = le64toh(group->data_physical_bytes);
    if (group_id != group_index || count == 0u || first > report->shard_count ||
        count > report->shard_count - first || data_logical_count == 0u ||
        data_physical_off > UINT64_MAX - data_physical_bytes)
    {
      rc = -EUCLEAN;
      break;
    }

    data_groups[group_index] = (kafs_v7_data_runtime_group_t){
        .logical_start = data_logical_start,
        .logical_count = data_logical_count,
        .physical_off = data_physical_off,
        .physical_end = data_physical_off + data_physical_bytes,
        .group_id = group_id,
    };

    const kafs_v7_shard_desc_t *inode_shard = NULL;
    for (uint32_t local = 0; local < count; ++local)
    {
      const kafs_v7_shard_desc_t *candidate = &shards[first + local];
      if (le16toh(candidate->type) == KAFS_V7_SHARD_INODE_TABLE)
      {
        if (inode_shard)
        {
          rc = -EUCLEAN;
          break;
        }
        inode_shard = candidate;
      }
    }
    if (rc != 0)
      break;
    if (!inode_shard || le32toh(inode_shard->group_id) != group_id ||
        le32toh(inode_shard->record_bytes) != KAFS_V7_INODE_BYTES)
    {
      rc = -EUCLEAN;
      break;
    }

    uint64_t physical_off = le64toh(inode_shard->physical_off);
    uint64_t physical_bytes = le64toh(inode_shard->physical_bytes);
    if (physical_off > UINT64_MAX - physical_bytes)
    {
      rc = -EUCLEAN;
      break;
    }
    inode_shards[group_index] = (kafs_v7_inode_runtime_shard_t){
        .logical_start = le64toh(inode_shard->logical_start),
        .logical_count = le64toh(inode_shard->logical_count),
        .physical_off = physical_off,
        .physical_end = physical_off + physical_bytes,
        .record_bytes = le32toh(inode_shard->record_bytes),
        .group_id = group_id,
    };
  }

  if (rc != 0)
  {
    free(inode_shards);
    free(data_groups);
    return rc;
  }
  *inode_shards_out = inode_shards;
  *data_groups_out = data_groups;
  return 0;
}

int kafs_v7_runtime_view_admit_fd(kafs_context_t *ctx, int fd, const kafs_ssuperblock_t *sbdisk,
                                  uint64_t file_size)
{
  if (!ctx || fd < 0 || !sbdisk)
    return -EINVAL;

  kafs_v7_layout_report_t report;
  int rc = kafs_v7_validate_image_fd(fd, sbdisk, file_size, &report);
  if (rc != 0)
    return rc;
  if (report.checkpoint_sequence != 0u)
  {
    kafs_v7_layout_report_clear(&report);
    return -ENOTSUP;
  }

  kafs_v7_inode_runtime_shard_t *inode_shards = NULL;
  kafs_v7_data_runtime_group_t *data_groups = NULL;
  rc = kafs_v7_runtime_view_build(&report, &inode_shards, &data_groups);
  if (rc != 0)
  {
    kafs_v7_layout_report_clear(&report);
    return rc;
  }

  kafs_ctx_v7_runtime_view_clear(ctx);
  ctx->c_v7_layout_desc = report.descriptor;
  ctx->c_v7_layout_desc_owned = 1u;
  ctx->c_v7_layout_desc_bytes = report.descriptor_bytes;
  ctx->c_v7_inode_shards = inode_shards;
  ctx->c_v7_inode_shard_count = report.group_count;
  ctx->c_v7_data_groups = data_groups;
  ctx->c_v7_data_group_count = report.group_count;
  ctx->c_v7_block_size = report.block_size;
  ctx->c_v7_selected_replica = report.selected_replica;
  ctx->c_v7_selected_checkpoint = report.selected_checkpoint;
  ctx->c_v7_degraded = report.degraded ? 1u : 0u;
  ctx->c_v7_selected_generation = report.selected_generation;
  ctx->c_v7_checkpoint_generation = report.checkpoint_generation;
  ctx->c_v7_checkpoint_sequence = report.checkpoint_sequence;
  ctx->c_v7_recovered_free_blocks = report.free_blocks;
  ctx->c_v7_recovered_free_inodes = report.free_inodes;
  ctx->c_v7_runtime_view_enabled = 1u;
  report.descriptor = NULL;
  kafs_v7_layout_report_clear(&report);
  return 0;
}

int kafs_v7_runtime_view_validate(const kafs_context_t *ctx)
{
  if (!ctx || !ctx->c_superblock ||
      kafs_sb_format_version_get(ctx->c_superblock) != KAFS_FORMAT_VERSION_V7)
    return -EINVAL;
  if (!ctx->c_img_base || ctx->c_img_size == 0u || ctx->c_mapsize != 0u || ctx->c_blkmasktbl ||
      ctx->c_inotbl)
    return -EPROTO;
  if (!ctx->c_v7_runtime_view_enabled || !ctx->c_v7_layout_desc_owned || !ctx->c_v7_layout_desc ||
      ctx->c_v7_layout_desc_bytes == 0u || !ctx->c_v7_inode_shards ||
      ctx->c_v7_inode_shard_count == 0u || !ctx->c_v7_data_groups ||
      ctx->c_v7_data_group_count != ctx->c_v7_inode_shard_count || ctx->c_v7_block_size == 0u ||
      ctx->c_v7_checkpoint_sequence != 0u)
    return -EPROTO;
  if (ctx->c_v6_layout_desc || ctx->c_v6_bitmap_mapping_enabled ||
      ctx->c_v6_inode_mapping_enabled || ctx->c_v6_alloc_summary_mapping_enabled ||
      ctx->c_v6_hrl_mapping_enabled)
    return -EPROTO;
  return 0;
}

void kafs_v7_runtime_view_seal_mutations(kafs_context_t *ctx)
{
  if (!ctx || !ctx->c_superblock ||
      kafs_sb_format_version_get(ctx->c_superblock) != KAFS_FORMAT_VERSION_V7)
    return;
  ctx->c_pendinglog_enabled = 0u;
  ctx->c_pendinglog_base = NULL;
  ctx->c_pendinglog_size = 0u;
  ctx->c_pendinglog_capacity = 0u;
  ctx->c_pending_worker_stop = 1;
  ctx->c_tombstone_gc_worker_stop = 1;
  ctx->c_bg_dedup_enabled = 0u;
  ctx->c_bg_dedup_worker_stop = 1;
  ctx->c_v7_mutation_policy_applied = 1u;
}

int kafs_v7_runtime_view_validate_policy(const kafs_context_t *ctx)
{
  if (!ctx || !ctx->c_v7_mutation_policy_applied)
    return -EPROTO;
  if (ctx->c_pendinglog_enabled || ctx->c_pendinglog_base || ctx->c_pendinglog_size != 0u ||
      ctx->c_pendinglog_capacity != 0u || ctx->c_pending_worker_running ||
      ctx->c_pending_worker_lock_init || !ctx->c_pending_worker_stop)
    return -EPROTO;
  if (ctx->c_tombstone_gc_worker_running || ctx->c_tombstone_gc_worker_lock_init ||
      !ctx->c_tombstone_gc_worker_stop)
    return -EPROTO;
  if (ctx->c_bg_dedup_enabled || ctx->c_bg_dedup_worker_running ||
      ctx->c_bg_dedup_worker_lock_init || !ctx->c_bg_dedup_worker_stop)
    return -EPROTO;
  if (ctx->c_hotplug_active || ctx->c_hotplug_fd >= 0 ||
      ctx->c_hotplug_state != KAFS_HOTPLUG_STATE_DISABLED || ctx->c_hotplug_connecting ||
      ctx->c_hotplug_uds_path[0] != '\0')
    return -EPROTO;
  return 0;
}
