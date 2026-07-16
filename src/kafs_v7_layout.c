#include "kafs_v7_layout.h"

#include "kafs_tool_util.h"

#include <endian.h>
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <math.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

typedef struct kafs_v7_group_geometry
{
  uint64_t metadata_off;
  uint64_t metadata_bytes;
  uint64_t data_logical_start;
  uint64_t data_off;
  uint64_t data_bytes;
  uint64_t data_blocks;
  uint64_t bitmap_bytes;
  uint64_t inode_logical_start;
  uint64_t inode_count;
  uint64_t inode_bytes;
  uint64_t allocator_bytes;
  uint64_t hrl_bucket_start;
  uint64_t hrl_bucket_count;
  uint64_t hrl_index_bytes;
  uint64_t hrl_entry_start;
  uint64_t hrl_entry_count;
  uint64_t hrl_entry_bytes;
  uint64_t journal_segment_start;
  uint64_t journal_segment_count;
  uint64_t journal_header_bytes;
  uint64_t journal_data_bytes;
} kafs_v7_group_geometry_t;

typedef struct kafs_v7_geometry
{
  uint64_t descriptor_bytes;
  uint64_t primary_desc_off;
  uint64_t primary_checkpoint_off;
  uint64_t tail_checkpoint_off;
  uint64_t tail_desc_off;
  uint64_t midpoint_desc_off;
  uint64_t midpoint_checkpoint_off;
  uint64_t first_group_off;
  uint64_t groups_end;
  uint64_t data_blocks;
  uint64_t journal_segment_bytes;
  uint32_t hrl_entry_count;
  uint32_t journal_segment_count;
  uint32_t group_count;
  uint32_t replica_count;
  kafs_v7_group_geometry_t groups[KAFS_V7_GROUP_MAX_COUNT];
} kafs_v7_geometry_t;

typedef struct kafs_v7_descriptor_candidate
{
  void *bytes;
  uint64_t generation;
  int valid;
} kafs_v7_descriptor_candidate_t;

#define KAFS_V7_AUTO_GROUP_TARGET_BYTES (UINT64_C(64) * 1024u * 1024u)
#define KAFS_V7_HRL_BUCKET_COUNT 1024u

static int kafs_v7_add_u64(uint64_t a, uint64_t b, uint64_t *out)
{
  if (!out || b > UINT64_MAX - a)
    return -EOVERFLOW;
  *out = a + b;
  return 0;
}

static int kafs_v7_mul_u64(uint64_t a, uint64_t b, uint64_t *out)
{
  if (!out || (a != 0 && b > UINT64_MAX / a))
    return -EOVERFLOW;
  *out = a * b;
  return 0;
}

static int kafs_v7_align_up(uint64_t value, uint64_t alignment, uint64_t *out)
{
  uint64_t added;

  if (!out || alignment == 0 || (alignment & (alignment - 1u)) != 0)
    return -EINVAL;
  if (kafs_v7_add_u64(value, alignment - 1u, &added) != 0)
    return -EOVERFLOW;
  *out = added & ~(alignment - 1u);
  return 0;
}

static int kafs_v7_range_ok(uint64_t off, uint64_t bytes, uint64_t image_size)
{
  return off <= image_size && bytes <= image_size - off;
}

static int kafs_v7_all_bytes(const void *data, size_t bytes, uint8_t value)
{
  const uint8_t *p = (const uint8_t *)data;

  for (size_t i = 0; i < bytes; ++i)
  {
    if (p[i] != value)
      return 0;
  }
  return 1;
}

static int kafs_v7_validate_zero_range(int fd, uint64_t off, uint64_t bytes)
{
  uint8_t buf[64u * 1024u];

  while (bytes != 0)
  {
    size_t chunk = bytes < sizeof(buf) ? (size_t)bytes : sizeof(buf);
    int rc = kafs_pread_all(fd, buf, chunk, (off_t)off);
    if (rc != 0)
      return rc;
    if (!kafs_v7_all_bytes(buf, chunk, 0))
      return -EINVAL;
    off += chunk;
    bytes -= chunk;
  }
  return 0;
}

uint32_t kafs_v7_crc32(const void *buf, size_t bytes)
{
  const uint8_t *p = (const uint8_t *)buf;
  uint32_t crc = UINT32_MAX;

  for (size_t i = 0; i < bytes; ++i)
  {
    crc ^= p[i];
    for (unsigned bit = 0; bit < 8u; ++bit)
      crc = (crc >> 1u) ^ ((crc & 1u) ? UINT32_C(0xedb88320) : 0u);
  }
  return crc ^ UINT32_MAX;
}

const char *kafs_v7_replica_status_name(kafs_v7_replica_status_t status)
{
  switch (status)
  {
  case KAFS_V7_REPLICA_STATUS_VALID:
    return "valid";
  case KAFS_V7_REPLICA_STATUS_STALE:
    return "stale";
  case KAFS_V7_REPLICA_STATUS_DIVERGENT:
    return "divergent";
  case KAFS_V7_REPLICA_STATUS_INVALID:
  default:
    return "invalid";
  }
}

const char *kafs_v7_replica_role_name(uint16_t role)
{
  switch (role)
  {
  case KAFS_V7_REPLICA_PRIMARY:
    return "primary";
  case KAFS_V7_REPLICA_TAIL:
    return "tail";
  case KAFS_V7_REPLICA_MIDPOINT:
    return "midpoint";
  default:
    return "unknown";
  }
}

const char *kafs_v7_shard_type_name(uint16_t type)
{
  static const char *const names[] = {
      "superblock_checkpoint", "block_bitmap", "inode_table",
      "allocator_summary",     "hrl_index",    "hrl_entries",
      "journal_header",        "journal_data", "pending_log",
      "tail_metadata",         "unknown",      "layout_descriptor",
  };

  return type < sizeof(names) / sizeof(names[0]) ? names[type] : "unknown";
}

void kafs_v7_layout_report_clear(kafs_v7_layout_report_t *report)
{
  if (!report)
    return;
  free(report->descriptor);
  memset(report, 0, sizeof(*report));
}

static uint32_t kafs_v7_locator_crc(const kafs_v7_root_locator_t *locator)
{
  kafs_v7_root_locator_t copy = *locator;
  copy.anchor_crc32 = 0;
  return kafs_v7_crc32(&copy, sizeof(copy));
}

static uint32_t kafs_v7_checkpoint_crc(const kafs_v7_checkpoint_t *checkpoint)
{
  kafs_v7_checkpoint_t copy = *checkpoint;
  copy.crc32 = 0;
  return kafs_v7_crc32(&copy, sizeof(copy));
}

static uint32_t kafs_v7_journal_header_crc(const kafs_v7_journal_header_t *header)
{
  kafs_v7_journal_header_t copy = *header;
  copy.crc32 = 0;
  return kafs_v7_crc32(&copy, sizeof(copy));
}

static uint32_t kafs_v7_descriptor_crc(void *descriptor, uint32_t descriptor_bytes)
{
  kafs_v7_layout_header_t *header = (kafs_v7_layout_header_t *)descriptor;
  uint32_t saved = header->descriptor_crc32;
  header->descriptor_crc32 = 0;
  uint32_t crc = kafs_v7_crc32(descriptor, descriptor_bytes);
  header->descriptor_crc32 = saved;
  return crc;
}

static int kafs_v7_supported_block_size(uint32_t block_size)
{
  return block_size >= 1024u && block_size <= 65536u && (block_size & (block_size - 1u)) == 0;
}

static int kafs_v7_descriptor_size(uint32_t block_size, uint32_t group_count, uint32_t shard_count,
                                   uint32_t replica_count, uint32_t *out_bytes,
                                   uint32_t *out_group_off, uint32_t *out_shard_off,
                                   uint32_t *out_replica_off)
{
  uint64_t group_end;
  uint64_t shard_off;
  uint64_t shard_capacity;
  uint64_t replica_off;
  uint64_t descriptor_bytes;

  if (!out_bytes || !out_group_off || !out_shard_off || !out_replica_off || group_count == 0 ||
      shard_count == 0 || replica_count < 2u || replica_count > 3u ||
      !kafs_v7_supported_block_size(block_size))
    return -EINVAL;
  if (kafs_v7_mul_u64(group_count, KAFS_V7_GROUP_DESC_BYTES, &group_end) != 0 ||
      kafs_v7_add_u64(KAFS_V7_LAYOUT_HEADER_BYTES, group_end, &group_end) != 0 ||
      kafs_v7_align_up(group_end, 8u, &shard_off) != 0)
    return -EOVERFLOW;
  shard_capacity = (uint64_t)shard_count + 2u * (3u - replica_count);
  if (kafs_v7_mul_u64(shard_capacity, KAFS_V7_SHARD_DESC_BYTES, &replica_off) != 0 ||
      kafs_v7_add_u64(shard_off, replica_off, &replica_off) != 0 ||
      kafs_v7_align_up(replica_off, 8u, &replica_off) != 0 ||
      kafs_v7_add_u64(replica_off, 3u * KAFS_V7_REPLICA_DESC_BYTES, &descriptor_bytes) != 0 ||
      kafs_v7_align_up(descriptor_bytes, block_size, &descriptor_bytes) != 0)
    return -EOVERFLOW;
  if (descriptor_bytes > KAFS_V7_LAYOUT_MAX_BYTES || descriptor_bytes > UINT32_MAX ||
      shard_off > UINT32_MAX || replica_off > UINT32_MAX)
    return -EOVERFLOW;

  *out_bytes = (uint32_t)descriptor_bytes;
  *out_group_off = KAFS_V7_LAYOUT_HEADER_BYTES;
  *out_shard_off = (uint32_t)shard_off;
  *out_replica_off = (uint32_t)replica_off;
  return 0;
}

static uint32_t kafs_v7_hrl_entry_count(uint64_t blocks, double ratio)
{
  double scaled = (double)blocks * ratio;
  uint64_t count = scaled >= (double)UINT32_MAX ? UINT32_MAX : (uint64_t)scaled;

  if (count == 0)
    count = 1;
  if (count > blocks)
    count = blocks;
  return (uint32_t)count;
}

static int kafs_v7_group_count_supported(uint32_t group_count)
{
  return group_count != 0 && group_count <= KAFS_V7_GROUP_MAX_COUNT &&
         (group_count & (group_count - 1u)) == 0 && KAFS_V7_HRL_BUCKET_COUNT % group_count == 0;
}

static void kafs_v7_partition_even(uint64_t total, uint32_t count, uint32_t id, uint64_t *start,
                                   uint64_t *length)
{
  uint64_t base = total / count;
  uint64_t remainder = total % count;

  *start = (uint64_t)id * base + (id < remainder ? id : remainder);
  *length = base + (id < remainder ? 1u : 0u);
}

static int kafs_v7_partition_data(uint64_t total, uint32_t group_count, uint32_t group_id,
                                  uint64_t *start, uint64_t *length)
{
  uint64_t words = total / 64u;
  uint64_t tail = total % 64u;
  uint64_t word_start;
  uint64_t word_count;

  if (words < group_count)
    return -ERANGE;
  kafs_v7_partition_even(words, group_count, group_id, &word_start, &word_count);
  *start = word_start * 64u;
  *length = word_count * 64u;
  if (group_id + 1u == group_count)
    *length += tail;
  return *length == 0 ? -ERANGE : 0;
}

static int kafs_v7_compute_metadata(const kafs_v7_mkfs_options_t *options,
                                    kafs_v7_geometry_t *geometry, uint64_t data_blocks)
{
  uint64_t cursor = geometry->first_group_off;
  const uint32_t block_size = options->block_size;

  if (data_blocks == 0 || data_blocks > UINT32_MAX ||
      options->inode_count < geometry->group_count * 2u)
    return -ERANGE;
  geometry->data_blocks = data_blocks;
  geometry->hrl_entry_count = kafs_v7_hrl_entry_count(data_blocks, options->hrl_entry_ratio);
  if (geometry->hrl_entry_count < geometry->group_count)
    return -ERANGE;

  for (uint32_t id = 0; id < geometry->group_count; ++id)
  {
    kafs_v7_group_geometry_t *group = &geometry->groups[id];
    uint64_t bytes;
    uint64_t l0_bytes;
    uint64_t l1_bytes;
    uint64_t l2_bytes;
    uint64_t total = 0;

    memset(group, 0, sizeof(*group));
    if (kafs_v7_partition_data(data_blocks, geometry->group_count, id, &group->data_logical_start,
                               &group->data_blocks) != 0)
      return -ERANGE;
    kafs_v7_partition_even(options->inode_count, geometry->group_count, id,
                           &group->inode_logical_start, &group->inode_count);
    kafs_v7_partition_even(KAFS_V7_HRL_BUCKET_COUNT, geometry->group_count, id,
                           &group->hrl_bucket_start, &group->hrl_bucket_count);
    kafs_v7_partition_even(geometry->hrl_entry_count, geometry->group_count, id,
                           &group->hrl_entry_start, &group->hrl_entry_count);
    if (geometry->group_count == 1u)
      group->journal_segment_count = geometry->journal_segment_count;
    else
    {
      group->journal_segment_start = id;
      group->journal_segment_count = 1u;
    }

    bytes = ((group->data_blocks + 63u) / 64u) * 8u;
    if (kafs_v7_align_up(bytes, block_size, &group->bitmap_bytes) != 0 ||
        kafs_v7_mul_u64(group->inode_count, KAFS_V7_INODE_BYTES, &bytes) != 0 ||
        kafs_v7_align_up(bytes, block_size, &group->inode_bytes) != 0)
      return -EOVERFLOW;
    l0_bytes = (group->data_blocks + 7u) / 8u;
    l1_bytes = (l0_bytes + 7u) / 8u;
    l2_bytes = (l1_bytes + 7u) / 8u;
    if (kafs_v7_add_u64(l1_bytes, l2_bytes, &bytes) != 0 ||
        kafs_v7_align_up(bytes, block_size, &group->allocator_bytes) != 0 ||
        kafs_v7_mul_u64(group->hrl_bucket_count, 4u, &bytes) != 0 ||
        kafs_v7_align_up(bytes, block_size, &group->hrl_index_bytes) != 0 ||
        kafs_v7_mul_u64(group->hrl_entry_count, KAFS_V7_HRL_ENTRY_BYTES, &bytes) != 0 ||
        kafs_v7_align_up(bytes, block_size, &group->hrl_entry_bytes) != 0 ||
        kafs_v7_mul_u64(group->journal_segment_count, block_size, &group->journal_header_bytes) !=
            0 ||
        kafs_v7_mul_u64(group->journal_segment_count, geometry->journal_segment_bytes,
                        &group->journal_data_bytes) != 0)
      return -EOVERFLOW;

    const uint64_t spans[] = {group->bitmap_bytes,      group->inode_bytes,
                              group->allocator_bytes,   group->hrl_index_bytes,
                              group->hrl_entry_bytes,   group->journal_header_bytes,
                              group->journal_data_bytes};
    for (size_t span = 0; span < sizeof(spans) / sizeof(spans[0]); ++span)
    {
      if (kafs_v7_add_u64(total, spans[span], &total) != 0)
        return -EOVERFLOW;
    }
    group->metadata_off = cursor;
    group->metadata_bytes = total;
    if (kafs_v7_add_u64(cursor, total, &group->data_off) != 0 ||
        kafs_v7_mul_u64(group->data_blocks, block_size, &group->data_bytes) != 0 ||
        kafs_v7_add_u64(group->data_off, group->data_bytes, &cursor) != 0)
      return -EOVERFLOW;
  }
  geometry->groups_end = cursor;
  return 0;
}

static int kafs_v7_plan_group_count(const kafs_v7_mkfs_options_t *options, uint32_t group_count,
                                    kafs_v7_geometry_t *geometry)
{
  uint32_t descriptor_bytes;
  uint32_t unused_group_off;
  uint32_t unused_shard_off;
  uint32_t unused_replica_off;
  uint64_t shard_count;
  uint64_t low = (uint64_t)group_count * 64u;
  uint64_t high;
  uint64_t best = 0;

  if (!kafs_v7_group_count_supported(group_count) || options->inode_count < group_count * 2u ||
      kafs_v7_mul_u64(group_count, KAFS_V7_GROUP_LOCAL_SHARDS, &shard_count) != 0 ||
      kafs_v7_add_u64(shard_count, 4u, &shard_count) != 0 || shard_count > UINT32_MAX)
    return -EINVAL;

  memset(geometry, 0, sizeof(*geometry));
  geometry->group_count = group_count;
  geometry->replica_count = 2u;
  geometry->journal_segment_count = group_count == 1u ? 2u : group_count;
  if (kafs_v7_descriptor_size(options->block_size, group_count, (uint32_t)shard_count, 2u,
                              &descriptor_bytes, &unused_group_off, &unused_shard_off,
                              &unused_replica_off) != 0)
    return -EOVERFLOW;
  geometry->descriptor_bytes = descriptor_bytes;
  geometry->primary_desc_off = options->block_size;
  if (kafs_v7_add_u64(geometry->primary_desc_off, descriptor_bytes,
                      &geometry->primary_checkpoint_off) != 0 ||
      kafs_v7_add_u64(geometry->primary_checkpoint_off, options->block_size,
                      &geometry->first_group_off) != 0)
    return -EOVERFLOW;
  if (options->image_size_bytes <
      (uint64_t)options->block_size + descriptor_bytes + options->block_size)
    return -ENOSPC;
  geometry->tail_desc_off = options->image_size_bytes - options->block_size - descriptor_bytes;
  if (geometry->tail_desc_off < options->block_size)
    return -ENOSPC;
  geometry->tail_checkpoint_off = geometry->tail_desc_off - options->block_size;
  if (geometry->first_group_off >= geometry->tail_checkpoint_off)
    return -ENOSPC;

  geometry->journal_segment_bytes =
      ((options->journal_bytes / geometry->journal_segment_count) / options->block_size) *
      options->block_size;
  if (geometry->journal_segment_bytes < options->block_size)
    return -ENOSPC;

  high = (geometry->tail_checkpoint_off - geometry->first_group_off) / options->block_size;
  while (low <= high)
  {
    uint64_t mid = low + (high - low) / 2u;
    kafs_v7_geometry_t candidate = *geometry;
    int rc = kafs_v7_compute_metadata(options, &candidate, mid);
    if (rc == 0 && candidate.groups_end <= geometry->tail_checkpoint_off)
    {
      best = mid;
      *geometry = candidate;
      low = mid + 1u;
    }
    else
      high = mid - 1u;
  }
  if (best == 0)
    return -ENOSPC;

  geometry->midpoint_desc_off =
      (options->image_size_bytes / 2u) & ~((uint64_t)options->block_size - 1u);
  if (kafs_v7_add_u64(geometry->midpoint_desc_off, descriptor_bytes,
                      &geometry->midpoint_checkpoint_off) != 0)
    return -EOVERFLOW;
  uint64_t midpoint_end;
  if (kafs_v7_add_u64(geometry->midpoint_checkpoint_off, options->block_size, &midpoint_end) != 0)
    return -EOVERFLOW;
  if (geometry->midpoint_desc_off >= geometry->groups_end &&
      midpoint_end <= geometry->tail_checkpoint_off)
    geometry->replica_count = 3u;
  return 0;
}

static int kafs_v7_plan(const kafs_v7_mkfs_options_t *options, kafs_v7_geometry_t *geometry)
{
  if (!options || !geometry || !kafs_v7_supported_block_size(options->block_size) ||
      options->inode_count < 2u || !isfinite(options->hrl_entry_ratio) ||
      options->hrl_entry_ratio <= 0.0 || options->hrl_entry_ratio > 1.0 ||
      options->image_size_bytes == 0 || options->image_size_bytes % options->block_size != 0 ||
      options->image_size_bytes / options->block_size > UINT32_MAX ||
      (options->group_count != 0 && !kafs_v7_group_count_supported(options->group_count)))
    return -EINVAL;

  if (options->group_count != 0)
    return kafs_v7_plan_group_count(options, options->group_count, geometry);

  uint64_t target_units = options->image_size_bytes / KAFS_V7_AUTO_GROUP_TARGET_BYTES;
  uint32_t group_count = 1u;
  while (group_count < KAFS_V7_GROUP_MAX_COUNT && (uint64_t)group_count * 2u <= target_units)
    group_count *= 2u;
  while (group_count != 0)
  {
    int rc = kafs_v7_plan_group_count(options, group_count, geometry);
    if (rc == 0)
      return 0;
    group_count /= 2u;
  }
  return -ENOSPC;
}

static void kafs_v7_shard_set(kafs_v7_shard_desc_t *shard, uint16_t type, uint16_t storage_class,
                              uint32_t group_id, uint64_t physical_off, uint64_t physical_bytes,
                              uint64_t logical_start, uint64_t logical_count, uint32_t record_bytes)
{
  memset(shard, 0, sizeof(*shard));
  shard->type = htole16(type);
  shard->storage_class = htole16(storage_class);
  shard->group_id = htole32(group_id);
  shard->physical_off = htole64(physical_off);
  shard->physical_bytes = htole64(physical_bytes);
  shard->logical_start = htole64(logical_start);
  shard->logical_count = htole64(logical_count);
  shard->record_bytes = htole32(record_bytes);
}

static uint64_t kafs_v7_replica_offset(const kafs_v7_geometry_t *geometry, uint32_t id)
{
  if (id == KAFS_V7_REPLICA_PRIMARY)
    return geometry->primary_desc_off;
  if (id == KAFS_V7_REPLICA_TAIL)
    return geometry->tail_desc_off;
  return geometry->midpoint_desc_off;
}

static uint64_t kafs_v7_checkpoint_offset(const kafs_v7_geometry_t *geometry, uint32_t id)
{
  if (id == KAFS_V7_REPLICA_PRIMARY)
    return geometry->primary_checkpoint_off;
  if (id == KAFS_V7_REPLICA_TAIL)
    return geometry->tail_checkpoint_off;
  return geometry->midpoint_checkpoint_off;
}

static int kafs_v7_build_descriptor(const kafs_v7_mkfs_options_t *options,
                                    const kafs_v7_geometry_t *geometry, void **out_descriptor)
{
  uint32_t descriptor_bytes;
  uint32_t group_off;
  uint32_t shard_off;
  uint32_t replica_off;
  uint64_t shard_count_u64 =
      (uint64_t)KAFS_V7_GROUP_LOCAL_SHARDS * geometry->group_count + 2u * geometry->replica_count;
  if (shard_count_u64 > UINT32_MAX)
    return -EOVERFLOW;
  uint32_t shard_count = (uint32_t)shard_count_u64;
  int rc = kafs_v7_descriptor_size(options->block_size, geometry->group_count, shard_count,
                                   geometry->replica_count, &descriptor_bytes, &group_off,
                                   &shard_off, &replica_off);
  if (rc != 0 || descriptor_bytes != geometry->descriptor_bytes)
    return rc != 0 ? rc : -EINVAL;

  void *descriptor = calloc(1u, descriptor_bytes);
  if (!descriptor)
    return -ENOMEM;
  kafs_v7_layout_header_t *header = (kafs_v7_layout_header_t *)descriptor;
  kafs_v7_group_desc_t *groups = (kafs_v7_group_desc_t *)((uint8_t *)descriptor + group_off);
  kafs_v7_shard_desc_t *shards = (kafs_v7_shard_desc_t *)((uint8_t *)descriptor + shard_off);
  kafs_v7_replica_desc_t *replicas =
      (kafs_v7_replica_desc_t *)((uint8_t *)descriptor + replica_off);

  header->magic = htole32(KAFS_V7_LAYOUT_MAGIC);
  header->version = htole16(KAFS_V7_LAYOUT_VERSION);
  header->header_bytes = htole16(KAFS_V7_LAYOUT_HEADER_BYTES);
  header->descriptor_bytes = htole32(descriptor_bytes);
  header->generation = htole64(1u);
  header->image_size_bytes = htole64(options->image_size_bytes);
  header->block_size = htole32(options->block_size);
  header->group_count = htole32(geometry->group_count);
  header->group_desc_off = htole32(group_off);
  header->group_desc_bytes = htole16(KAFS_V7_GROUP_DESC_BYTES);
  header->mapping_policy = htole16(0u);
  header->shard_count = htole32(shard_count);
  header->shard_desc_off = htole32(shard_off);
  header->shard_desc_bytes = htole16(KAFS_V7_SHARD_DESC_BYTES);
  header->replica_count = htole32(geometry->replica_count);
  header->replica_desc_off = htole32(replica_off);
  header->replica_desc_bytes = htole16(KAFS_V7_REPLICA_DESC_BYTES);
  header->incompat_flags = htole64(KAFS_V7_REQUIRED_INCOMPAT_FLAGS);

  for (uint32_t id = 0; id < geometry->group_count; ++id)
  {
    const kafs_v7_group_geometry_t *group = &geometry->groups[id];
    uint32_t first_shard = id * KAFS_V7_GROUP_LOCAL_SHARDS;
    uint64_t off = group->metadata_off;

    groups[id].group_id = htole32(id);
    groups[id].first_shard_index = htole32(first_shard);
    groups[id].shard_count = htole32(KAFS_V7_GROUP_LOCAL_SHARDS);
    groups[id].metadata_physical_off = htole64(group->metadata_off);
    groups[id].metadata_physical_bytes = htole64(group->metadata_bytes);
    groups[id].data_logical_start = htole64(group->data_logical_start);
    groups[id].data_logical_count = htole64(group->data_blocks);
    groups[id].data_physical_off = htole64(group->data_off);
    groups[id].data_physical_bytes = htole64(group->data_bytes);

    kafs_v7_shard_set(&shards[first_shard], KAFS_V7_SHARD_BLOCK_BITMAP, KAFS_V7_STORAGE_BIT_PACKED,
                      id, off, group->bitmap_bytes, group->data_logical_start, group->data_blocks,
                      8u);
    off += group->bitmap_bytes;
    kafs_v7_shard_set(&shards[first_shard + 1u], KAFS_V7_SHARD_INODE_TABLE,
                      KAFS_V7_STORAGE_FIXED_RECORD, id, off, group->inode_bytes,
                      group->inode_logical_start, group->inode_count, KAFS_V7_INODE_BYTES);
    off += group->inode_bytes;
    kafs_v7_shard_set(&shards[first_shard + 2u], KAFS_V7_SHARD_ALLOCATOR_SUMMARY,
                      KAFS_V7_STORAGE_ALLOCATOR_SUMMARY, id, off, group->allocator_bytes,
                      group->data_logical_start, group->data_blocks, 0u);
    off += group->allocator_bytes;
    kafs_v7_shard_set(&shards[first_shard + 3u], KAFS_V7_SHARD_HRL_INDEX,
                      KAFS_V7_STORAGE_FIXED_RECORD, id, off, group->hrl_index_bytes,
                      group->hrl_bucket_start, group->hrl_bucket_count, 4u);
    off += group->hrl_index_bytes;
    kafs_v7_shard_set(&shards[first_shard + 4u], KAFS_V7_SHARD_HRL_ENTRIES,
                      KAFS_V7_STORAGE_FIXED_RECORD, id, off, group->hrl_entry_bytes,
                      group->hrl_entry_start, group->hrl_entry_count, KAFS_V7_HRL_ENTRY_BYTES);
    off += group->hrl_entry_bytes;
    kafs_v7_shard_set(&shards[first_shard + 5u], KAFS_V7_SHARD_JOURNAL_HEADER,
                      KAFS_V7_STORAGE_FIXED_RECORD, id, off, group->journal_header_bytes,
                      group->journal_segment_start, group->journal_segment_count,
                      options->block_size);
    off += group->journal_header_bytes;
    kafs_v7_shard_set(&shards[first_shard + 6u], KAFS_V7_SHARD_JOURNAL_DATA,
                      KAFS_V7_STORAGE_BYTE_SPAN, id, off, group->journal_data_bytes,
                      group->journal_segment_start, group->journal_segment_count, 0u);
  }

  uint32_t shard_index = geometry->group_count * KAFS_V7_GROUP_LOCAL_SHARDS;
  for (uint32_t id = 0; id < geometry->replica_count; ++id)
  {
    uint64_t descriptor_off = kafs_v7_replica_offset(geometry, id);
    uint64_t checkpoint_off = kafs_v7_checkpoint_offset(geometry, id);
    kafs_v7_shard_set(&shards[shard_index++], KAFS_V7_SHARD_LAYOUT_DESCRIPTOR,
                      KAFS_V7_STORAGE_BYTE_SPAN, UINT32_MAX, descriptor_off,
                      geometry->descriptor_bytes, id, 1u, 0u);
    kafs_v7_shard_set(&shards[shard_index++], KAFS_V7_SHARD_SUPERBLOCK_CHECKPOINT,
                      KAFS_V7_STORAGE_FIXED_RECORD, UINT32_MAX, checkpoint_off, options->block_size,
                      id, 1u, KAFS_V7_CHECKPOINT_BYTES);

    replicas[id].replica_id = htole32(id);
    replicas[id].role = htole16((uint16_t)id);
    replicas[id].physical_off = htole64(descriptor_off);
    replicas[id].descriptor_bytes = htole32((uint32_t)geometry->descriptor_bytes);
  }

  header->descriptor_crc32 = htole32(kafs_v7_descriptor_crc(descriptor, descriptor_bytes));
  *out_descriptor = descriptor;
  return 0;
}

static int kafs_v7_write_zeroes(int fd, uint64_t off, uint64_t bytes, uint32_t block_size)
{
  void *zero = calloc(1u, block_size);
  if (!zero)
    return -ENOMEM;
  int rc = 0;
  while (bytes != 0)
  {
    size_t chunk = bytes < block_size ? (size_t)bytes : block_size;
    rc = kafs_pwrite_all(fd, zero, chunk, (off_t)off);
    if (rc != 0)
      break;
    off += chunk;
    bytes -= chunk;
  }
  free(zero);
  return rc;
}

static int kafs_v7_sync_fd(int fd) { return fdatasync(fd) == 0 ? 0 : -errno; }

static int kafs_v7_write_group_metadata(int fd, const kafs_v7_mkfs_options_t *options,
                                        const kafs_v7_geometry_t *geometry,
                                        const kafs_v7_group_geometry_t *group)
{
  int rc;
  uint8_t *bitmap = (uint8_t *)malloc((size_t)group->bitmap_bytes);
  if (!bitmap)
    return -ENOMEM;
  memset(bitmap, 0xff, (size_t)group->bitmap_bytes);
  uint64_t bitmap_payload = ((group->data_blocks + 63u) / 64u) * 8u;
  memset(bitmap, 0, (size_t)bitmap_payload);
  uint32_t trailing = (uint32_t)(group->data_blocks & 63u);
  if (trailing != 0)
  {
    uint64_t valid_mask = (UINT64_C(1) << trailing) - 1u;
    uint64_t invalid_mask = htole64(~valid_mask);
    memcpy(bitmap + bitmap_payload - 8u, &invalid_mask, sizeof(invalid_mask));
  }
  rc = kafs_pwrite_all(fd, bitmap, (size_t)group->bitmap_bytes, (off_t)group->metadata_off);
  free(bitmap);
  if (rc != 0)
    return rc;

  uint64_t inode_off = group->metadata_off + group->bitmap_bytes;
  void *inode_area = calloc(1u, (size_t)group->inode_bytes);
  if (!inode_area)
    return -ENOMEM;
  if (group->inode_logical_start <= 1u && 1u < group->inode_logical_start + group->inode_count)
  {
    kafs_v7_inode_t *inodes = (kafs_v7_inode_t *)inode_area;
    uint64_t root_index = 1u - group->inode_logical_start;
    inodes[root_index].mode = htole16((uint16_t)(S_IFDIR | 0755));
    inodes[root_index].uid = htole16(options->root_uid);
    inodes[root_index].gid = htole16(options->root_gid);
    inodes[root_index].link_count = htole16(1u);
    inodes[root_index].size = htole64(KAFS_V7_KDIR_HEADER_BYTES);
    kafs_v7_kdir_header_t root_directory = {
        .magic = htole32(KAFS_V7_KDIR_MAGIC),
        .version = htole16(KAFS_V7_KDIR_VERSION),
    };
    memcpy(inodes[root_index].inline_or_block_refs, &root_directory, sizeof(root_directory));
  }
  rc = kafs_pwrite_all(fd, inode_area, (size_t)group->inode_bytes, (off_t)inode_off);
  free(inode_area);
  if (rc != 0)
    return rc;

  uint64_t allocator_off = inode_off + group->inode_bytes;
  uint8_t *allocator = (uint8_t *)calloc(1u, (size_t)group->allocator_bytes);
  if (!allocator)
    return -ENOMEM;
  uint64_t l0_bytes = (group->data_blocks + 7u) / 8u;
  uint64_t l1_bytes = (l0_bytes + 7u) / 8u;
  for (uint64_t i = 0; i < l0_bytes; ++i)
    allocator[i / 8u] |= (uint8_t)(1u << (i % 8u));
  for (uint64_t i = 0; i < l1_bytes; ++i)
  {
    if (allocator[i] != 0)
      allocator[l1_bytes + i / 8u] |= (uint8_t)(1u << (i % 8u));
  }
  rc = kafs_pwrite_all(fd, allocator, (size_t)group->allocator_bytes, (off_t)allocator_off);
  free(allocator);
  if (rc != 0)
    return rc;

  uint64_t hrl_index_off = allocator_off + group->allocator_bytes;
  rc = kafs_v7_write_zeroes(fd, hrl_index_off, group->hrl_index_bytes + group->hrl_entry_bytes,
                            options->block_size);
  if (rc != 0)
    return rc;

  uint64_t journal_header_off = hrl_index_off + group->hrl_index_bytes + group->hrl_entry_bytes;
  void *journal_headers = calloc(1u, (size_t)group->journal_header_bytes);
  if (!journal_headers)
    return -ENOMEM;
  for (uint32_t local = 0; local < group->journal_segment_count; ++local)
  {
    uint32_t segment = (uint32_t)group->journal_segment_start + local;
    kafs_v7_journal_header_t *header =
        (kafs_v7_journal_header_t *)((uint8_t *)journal_headers +
                                     (uint64_t)local * options->block_size);
    header->magic = htole32(KAFS_V7_JOURNAL_HEADER_MAGIC);
    header->version = htole16(KAFS_V7_JOURNAL_HEADER_VERSION);
    header->segment_id = htole32(segment);
    header->slot_bytes = htole32(KAFS_V7_JOURNAL_HEADER_BYTES);
    header->generation = htole64(1u);
    header->data_bytes = htole64(geometry->journal_segment_bytes);
    header->crc32 = htole32(kafs_v7_journal_header_crc(header));
  }
  rc = kafs_pwrite_all(fd, journal_headers, (size_t)group->journal_header_bytes,
                       (off_t)journal_header_off);
  free(journal_headers);
  if (rc != 0)
    return rc;
  return kafs_v7_write_zeroes(fd, journal_header_off + group->journal_header_bytes,
                              group->journal_data_bytes, options->block_size);
}

static int kafs_v7_write_metadata(int fd, const kafs_v7_mkfs_options_t *options,
                                  const kafs_v7_geometry_t *geometry)
{
  for (uint32_t id = 0; id < geometry->group_count; ++id)
  {
    int rc = kafs_v7_write_group_metadata(fd, options, geometry, &geometry->groups[id]);
    if (rc != 0)
      return rc;
  }
  return 0;
}

int kafs_v7_mkfs_fd(int fd, const kafs_v7_mkfs_options_t *options, kafs_v7_layout_report_t *report)
{
  kafs_v7_geometry_t geometry;
  void *descriptor = NULL;
  int rc;

  if (fd < 0 || !options)
    return -EINVAL;
  rc = kafs_v7_plan(options, &geometry);
  if (rc != 0)
    return rc;
  rc = kafs_v7_build_descriptor(options, &geometry, &descriptor);
  if (rc != 0)
    return rc;

  kafs_v7_root_locator_t locator;
  memset(&locator, 0, sizeof(locator));
  locator.magic = htole32(KAFS_V7_SUPERBLOCK_ANCHOR_MAGIC);
  locator.version = htole16(KAFS_V7_SUPERBLOCK_ANCHOR_VERSION);
  locator.primary_desc_off = htole64(geometry.primary_desc_off);
  locator.primary_desc_bytes = htole32((uint32_t)geometry.descriptor_bytes);
  locator.candidate_count = htole32(geometry.replica_count);
  locator.block_size = htole32(options->block_size);
  locator.anchor_crc32 = htole32(kafs_v7_locator_crc(&locator));

  kafs_ssuperblock_t superblock;
  memset(&superblock, 0, sizeof(superblock));
  kafs_sb_magic_set(&superblock, KAFS_MAGIC);
  kafs_sb_format_version_set(&superblock, KAFS_FORMAT_VERSION_V7);
  uint16_t log_value = 0;
  uint32_t size = options->block_size;
  while (size > 1024u)
  {
    size >>= 1u;
    log_value++;
  }
  superblock.s_log_blksize.value = htole16(log_value);
  superblock.s_inocnt = kafs_inocnt_htos(options->inode_count);
  superblock.s_blkcnt =
      kafs_blkcnt_htos((kafs_blkcnt_t)(options->image_size_bytes / options->block_size));
  superblock.s_r_blkcnt = kafs_blkcnt_htos((kafs_blkcnt_t)geometry.data_blocks);
  kafs_sb_hash_fast_set(&superblock, KAFS_V7_HASH_FAST_FNV1A64);
  kafs_sb_hash_strong_set(&superblock, KAFS_V7_HASH_STRONG_NONE);
  memcpy(superblock.s_reserved, &locator, sizeof(locator));

  kafs_v7_checkpoint_t checkpoint;
  memset(&checkpoint, 0, sizeof(checkpoint));
  checkpoint.magic = htole32(KAFS_V7_CHECKPOINT_MAGIC);
  checkpoint.version = htole16(KAFS_V7_CHECKPOINT_VERSION);
  checkpoint.record_bytes = htole16(KAFS_V7_CHECKPOINT_BYTES);
  checkpoint.generation = htole64(1u);
  checkpoint.descriptor_generation = htole64(1u);
  checkpoint.free_blocks = htole64(geometry.data_blocks);
  checkpoint.free_inodes = htole64(options->inode_count - 2u);
  checkpoint.crc32 = htole32(kafs_v7_checkpoint_crc(&checkpoint));

  rc = kafs_v7_write_zeroes(fd, 0, options->block_size, options->block_size);
  if (rc == 0)
    rc = kafs_v7_write_zeroes(fd, options->image_size_bytes - options->block_size,
                              options->block_size, options->block_size);
  if (rc == 0)
    rc = kafs_v7_sync_fd(fd);
  if (rc == 0)
    rc = kafs_v7_write_metadata(fd, options, &geometry);
  if (rc == 0 && geometry.groups_end < geometry.tail_checkpoint_off)
    rc = kafs_v7_write_zeroes(fd, geometry.groups_end,
                              geometry.tail_checkpoint_off - geometry.groups_end,
                              options->block_size);
  if (rc == 0)
    rc = kafs_v7_sync_fd(fd);

  void *checkpoint_block = calloc(1u, options->block_size);
  if (!checkpoint_block && rc == 0)
    rc = -ENOMEM;
  if (checkpoint_block)
    memcpy(checkpoint_block, &checkpoint, sizeof(checkpoint));

  for (uint32_t id = geometry.replica_count; rc == 0 && id-- > 1u;)
  {
    rc = kafs_pwrite_all(fd, descriptor, (size_t)geometry.descriptor_bytes,
                         (off_t)kafs_v7_replica_offset(&geometry, id));
    if (rc == 0)
      rc = kafs_pwrite_all(fd, checkpoint_block, options->block_size,
                           (off_t)kafs_v7_checkpoint_offset(&geometry, id));
    if (rc == 0)
      rc = kafs_v7_sync_fd(fd);
  }
  if (rc == 0)
    rc = kafs_pwrite_all(fd, descriptor, (size_t)geometry.descriptor_bytes,
                         (off_t)geometry.primary_desc_off);
  if (rc == 0)
    rc = kafs_pwrite_all(fd, checkpoint_block, options->block_size,
                         (off_t)geometry.primary_checkpoint_off);
  if (rc == 0)
    rc = kafs_v7_sync_fd(fd);
  if (rc == 0)
    rc = kafs_pwrite_all(fd, &locator, sizeof(locator),
                         (off_t)(options->image_size_bytes - sizeof(locator)));
  if (rc == 0)
    rc = kafs_v7_sync_fd(fd);
  if (rc == 0)
    rc = kafs_pwrite_all(fd, &superblock, sizeof(superblock), 0);
  if (rc == 0)
    rc = kafs_v7_sync_fd(fd);

  free(checkpoint_block);
  free(descriptor);
  if (rc != 0)
    return rc;
  if (report)
    return kafs_v7_validate_image_fd(fd, &superblock, options->image_size_bytes, report);
  return 0;
}

static int kafs_v7_validate_locator(const kafs_v7_root_locator_t *locator, uint64_t file_size)
{
  uint32_t block_size = le32toh(locator->block_size);
  uint32_t descriptor_bytes = le32toh(locator->primary_desc_bytes);
  uint32_t count = le32toh(locator->candidate_count);

  if (le32toh(locator->magic) != KAFS_V7_SUPERBLOCK_ANCHOR_MAGIC ||
      le16toh(locator->version) != KAFS_V7_SUPERBLOCK_ANCHOR_VERSION ||
      le16toh(locator->flags) != 0 ||
      le32toh(locator->anchor_crc32) != kafs_v7_locator_crc(locator) ||
      !kafs_v7_supported_block_size(block_size) ||
      le64toh(locator->primary_desc_off) != block_size || count < 2u || count > 3u ||
      descriptor_bytes == 0 || descriptor_bytes > KAFS_V7_LAYOUT_MAX_BYTES ||
      descriptor_bytes % block_size != 0 || file_size % block_size != 0)
    return -EINVAL;
  return 0;
}

static int kafs_v7_primary_identity(const kafs_ssuperblock_t *sb, uint64_t file_size,
                                    uint32_t block_size)
{
  uint16_t log_value = le16toh(sb->s_log_blksize.value);
  if (kafs_sb_magic_get(sb) != KAFS_MAGIC ||
      kafs_sb_format_version_get(sb) != KAFS_FORMAT_VERSION_V7 || log_value > 6u ||
      (1024u << log_value) != block_size ||
      kafs_sb_hash_fast_get(sb) != KAFS_V7_HASH_FAST_FNV1A64 ||
      kafs_sb_hash_strong_get(sb) != KAFS_V7_HASH_STRONG_NONE || kafs_sb_inocnt_get(sb) < 2u ||
      file_size / block_size > UINT32_MAX || kafs_sb_blkcnt_get(sb) != file_size / block_size ||
      kafs_sb_r_blkcnt_get(sb) > UINT32_MAX ||
      !kafs_v7_all_bytes(sb->s_reserved + KAFS_V7_ROOT_LOCATOR_BYTES,
                         sizeof(sb->s_reserved) - KAFS_V7_ROOT_LOCATOR_BYTES, 0))
    return -EINVAL;
  return 0;
}

static int kafs_v7_table_bounds(uint32_t off, uint32_t count, uint32_t entry_bytes,
                                uint32_t container_bytes)
{
  if ((off & 7u) != 0 || off > container_bytes || entry_bytes == 0 ||
      count > (container_bytes - off) / entry_bytes)
    return -ERANGE;
  return 0;
}

static int kafs_v7_expected_replica_offsets(uint64_t file_size, uint32_t block_size,
                                            uint32_t descriptor_bytes, uint64_t offsets[3],
                                            uint64_t checkpoints[3])
{
  if (file_size < (uint64_t)block_size + descriptor_bytes + block_size)
    return -ERANGE;
  offsets[0] = block_size;
  checkpoints[0] = offsets[0] + descriptor_bytes;
  offsets[1] = file_size - block_size - descriptor_bytes;
  if (offsets[1] < block_size)
    return -ERANGE;
  checkpoints[1] = offsets[1] - block_size;
  offsets[2] = (file_size / 2u) & ~((uint64_t)block_size - 1u);
  if (offsets[2] > file_size || descriptor_bytes > file_size - offsets[2])
    return -ERANGE;
  checkpoints[2] = offsets[2] + descriptor_bytes;
  return 0;
}

typedef struct kafs_v7_descriptor_view
{
  const uint8_t *bytes;
  const kafs_v7_layout_header_t *header;
  const kafs_v7_group_desc_t *groups;
  const kafs_v7_shard_desc_t *shards;
  const kafs_v7_replica_desc_t *replicas;
  uint32_t group_count;
  uint32_t shard_count;
  uint32_t replica_count;
  uint32_t block_size;
  uint32_t group_off;
  uint32_t shard_off;
  uint32_t replica_off;
} kafs_v7_descriptor_view_t;

static int kafs_v7_descriptor_view_init(const void *descriptor, uint32_t descriptor_bytes,
                                        uint64_t file_size, const kafs_v7_root_locator_t *locator,
                                        kafs_v7_descriptor_view_t *view)
{
  if (!descriptor || descriptor_bytes < sizeof(kafs_v7_layout_header_t) || !locator || !view)
    return -EINVAL;
  memset(view, 0, sizeof(*view));
  view->bytes = (const uint8_t *)descriptor;
  view->header = (const kafs_v7_layout_header_t *)descriptor;
  view->group_count = le32toh(view->header->group_count);
  view->shard_count = le32toh(view->header->shard_count);
  view->replica_count = le32toh(view->header->replica_count);
  view->block_size = le32toh(view->header->block_size);
  uint64_t expected_shards =
      (uint64_t)view->group_count * KAFS_V7_GROUP_LOCAL_SHARDS + 2u * view->replica_count;

  if (!kafs_v7_group_count_supported(view->group_count) || expected_shards > UINT32_MAX ||
      view->shard_count != expected_shards ||
      le32toh(view->header->magic) != KAFS_V7_LAYOUT_MAGIC ||
      le16toh(view->header->version) != KAFS_V7_LAYOUT_VERSION ||
      le16toh(view->header->header_bytes) != KAFS_V7_LAYOUT_HEADER_BYTES ||
      le32toh(view->header->descriptor_bytes) != descriptor_bytes ||
      le32toh(view->header->flags) != 0 || le64toh(view->header->generation) == 0 ||
      le64toh(view->header->image_size_bytes) != file_size ||
      view->block_size != le32toh(locator->block_size) ||
      view->replica_count != le32toh(locator->candidate_count) ||
      le16toh(view->header->group_desc_bytes) != KAFS_V7_GROUP_DESC_BYTES ||
      le16toh(view->header->mapping_policy) != 0 ||
      le16toh(view->header->shard_desc_bytes) != KAFS_V7_SHARD_DESC_BYTES ||
      le16toh(view->header->replica_desc_bytes) != KAFS_V7_REPLICA_DESC_BYTES ||
      le16toh(view->header->reserved0) != 0 || le16toh(view->header->reserved1) != 0 ||
      le64toh(view->header->feature_flags) != 0 ||
      le64toh(view->header->incompat_flags) != KAFS_V7_REQUIRED_INCOMPAT_FLAGS ||
      le64toh(view->header->ro_compat_flags) != 0 || le64toh(view->header->mapping_seed) != 0 ||
      le32toh(view->header->reserved2) != 0 || le64toh(view->header->reserved3) != 0 ||
      le64toh(view->header->reserved4) != 0 ||
      le32toh(view->header->descriptor_crc32) !=
          kafs_v7_descriptor_crc((void *)descriptor, descriptor_bytes))
    return -EINVAL;

  uint32_t expected_bytes;
  int rc = kafs_v7_descriptor_size(view->block_size, view->group_count, view->shard_count,
                                   view->replica_count, &expected_bytes, &view->group_off,
                                   &view->shard_off, &view->replica_off);
  if (rc != 0 || expected_bytes != descriptor_bytes ||
      le32toh(view->header->group_desc_off) != view->group_off ||
      le32toh(view->header->shard_desc_off) != view->shard_off ||
      le32toh(view->header->replica_desc_off) != view->replica_off ||
      kafs_v7_table_bounds(view->group_off, view->group_count, KAFS_V7_GROUP_DESC_BYTES,
                           descriptor_bytes) != 0 ||
      kafs_v7_table_bounds(view->shard_off, view->shard_count, KAFS_V7_SHARD_DESC_BYTES,
                           descriptor_bytes) != 0 ||
      kafs_v7_table_bounds(view->replica_off, view->replica_count, KAFS_V7_REPLICA_DESC_BYTES,
                           descriptor_bytes) != 0)
    return rc != 0 ? rc : -ERANGE;

  view->groups = (const kafs_v7_group_desc_t *)(view->bytes + view->group_off);
  view->shards = (const kafs_v7_shard_desc_t *)(view->bytes + view->shard_off);
  view->replicas = (const kafs_v7_replica_desc_t *)(view->bytes + view->replica_off);
  return 0;
}

static int kafs_v7_validate_recovery_records(const kafs_v7_descriptor_view_t *view,
                                             uint32_t descriptor_bytes, uint64_t file_size,
                                             uint64_t groups_end)
{
  uint64_t replica_offsets[3];
  uint64_t checkpoint_offsets[3];
  int rc = kafs_v7_expected_replica_offsets(file_size, view->block_size, descriptor_bytes,
                                            replica_offsets, checkpoint_offsets);
  if (rc != 0 || groups_end > checkpoint_offsets[1])
    return -ERANGE;

  uint32_t recovery_start = view->group_count * KAFS_V7_GROUP_LOCAL_SHARDS;
  for (uint32_t id = 0; id < view->replica_count; ++id)
  {
    const kafs_v7_replica_desc_t *replica = &view->replicas[id];
    const kafs_v7_shard_desc_t *layout_shard = &view->shards[recovery_start + id * 2u];
    const kafs_v7_shard_desc_t *checkpoint_shard = &view->shards[recovery_start + id * 2u + 1u];
    if (le32toh(replica->replica_id) != id || le16toh(replica->role) != id ||
        le16toh(replica->flags) != 0 || le64toh(replica->physical_off) != replica_offsets[id] ||
        le32toh(replica->descriptor_bytes) != descriptor_bytes ||
        le32toh(replica->reserved0) != 0 || le64toh(replica->reserved1) != 0 ||
        le16toh(layout_shard->type) != KAFS_V7_SHARD_LAYOUT_DESCRIPTOR ||
        le16toh(layout_shard->storage_class) != KAFS_V7_STORAGE_BYTE_SPAN ||
        le32toh(layout_shard->flags) != 0 || le32toh(layout_shard->group_id) != UINT32_MAX ||
        le32toh(layout_shard->reserved0) != 0 ||
        le64toh(layout_shard->physical_off) != replica_offsets[id] ||
        le64toh(layout_shard->physical_bytes) != descriptor_bytes ||
        le64toh(layout_shard->logical_start) != id || le64toh(layout_shard->logical_count) != 1u ||
        le32toh(layout_shard->record_bytes) != 0 || le32toh(layout_shard->header_bytes) != 0 ||
        le64toh(layout_shard->generation_floor) != 0 || le64toh(layout_shard->mapping_seed) != 0 ||
        le64toh(layout_shard->reserved1) != 0 || le64toh(layout_shard->reserved2) != 0 ||
        le64toh(layout_shard->reserved3) != 0 ||
        le16toh(checkpoint_shard->type) != KAFS_V7_SHARD_SUPERBLOCK_CHECKPOINT ||
        le16toh(checkpoint_shard->storage_class) != KAFS_V7_STORAGE_FIXED_RECORD ||
        le32toh(checkpoint_shard->flags) != 0 ||
        le32toh(checkpoint_shard->group_id) != UINT32_MAX ||
        le32toh(checkpoint_shard->reserved0) != 0 ||
        le64toh(checkpoint_shard->physical_off) != checkpoint_offsets[id] ||
        le64toh(checkpoint_shard->physical_bytes) != view->block_size ||
        le64toh(checkpoint_shard->logical_start) != id ||
        le64toh(checkpoint_shard->logical_count) != 1u ||
        le32toh(checkpoint_shard->record_bytes) != KAFS_V7_CHECKPOINT_BYTES ||
        le32toh(checkpoint_shard->header_bytes) != 0 ||
        le64toh(checkpoint_shard->generation_floor) != 0 ||
        le64toh(checkpoint_shard->mapping_seed) != 0 || le64toh(checkpoint_shard->reserved1) != 0 ||
        le64toh(checkpoint_shard->reserved2) != 0 || le64toh(checkpoint_shard->reserved3) != 0)
      return -EINVAL;
  }

  uint64_t midpoint_end;
  if (kafs_v7_add_u64(checkpoint_offsets[2], view->block_size, &midpoint_end) != 0)
    return -EOVERFLOW;
  int midpoint_fits = replica_offsets[2] >= groups_end && midpoint_end <= checkpoint_offsets[1];
  if ((view->replica_count == 3u) != midpoint_fits)
    return -EINVAL;

  uint32_t live_shard_end = view->shard_off + view->shard_count * KAFS_V7_SHARD_DESC_BYTES;
  uint32_t live_replica_end = view->replica_off + view->replica_count * KAFS_V7_REPLICA_DESC_BYTES;
  if (!kafs_v7_all_bytes(view->bytes + live_shard_end, view->replica_off - live_shard_end, 0) ||
      !kafs_v7_all_bytes(view->bytes + live_replica_end, descriptor_bytes - live_replica_end, 0))
    return -EINVAL;
  return 0;
}

static int kafs_v7_validate_single_group_view(const kafs_v7_descriptor_view_t *view,
                                              uint32_t descriptor_bytes,
                                              const kafs_ssuperblock_t *sb, uint64_t file_size)
{
  const kafs_v7_group_desc_t *group = view->groups;
  const kafs_v7_shard_desc_t *shards = view->shards;
  const uint32_t block_size = view->block_size;
  const uint64_t data_blocks = le64toh(group->data_logical_count);

  if (le32toh(group->group_id) != 0 || le32toh(group->flags) != 0 ||
      le32toh(group->first_shard_index) != 0 ||
      le32toh(group->shard_count) != KAFS_V7_SINGLE_GROUP_LOCAL_SHARDS ||
      le64toh(group->metadata_physical_off) !=
          (uint64_t)block_size + descriptor_bytes + block_size ||
      le64toh(group->metadata_physical_bytes) == 0 || data_blocks == 0 ||
      data_blocks != kafs_sb_r_blkcnt_get(sb) || le64toh(group->data_logical_start) != 0 ||
      le64toh(group->data_physical_bytes) != data_blocks * (uint64_t)block_size ||
      le64toh(group->generation_floor) != 0 || le64toh(group->reserved0) != 0 ||
      le64toh(group->reserved1) != 0 || le64toh(group->reserved2) != 0)
    return -EINVAL;

  static const uint16_t types[7] = {KAFS_V7_SHARD_BLOCK_BITMAP,      KAFS_V7_SHARD_INODE_TABLE,
                                    KAFS_V7_SHARD_ALLOCATOR_SUMMARY, KAFS_V7_SHARD_HRL_INDEX,
                                    KAFS_V7_SHARD_HRL_ENTRIES,       KAFS_V7_SHARD_JOURNAL_HEADER,
                                    KAFS_V7_SHARD_JOURNAL_DATA};
  static const uint16_t classes[7] = {
      KAFS_V7_STORAGE_BIT_PACKED,   KAFS_V7_STORAGE_FIXED_RECORD, KAFS_V7_STORAGE_ALLOCATOR_SUMMARY,
      KAFS_V7_STORAGE_FIXED_RECORD, KAFS_V7_STORAGE_FIXED_RECORD, KAFS_V7_STORAGE_FIXED_RECORD,
      KAFS_V7_STORAGE_BYTE_SPAN};
  uint64_t expected_off = le64toh(group->metadata_physical_off);
  uint64_t l0_bytes = (data_blocks + 7u) / 8u;
  uint64_t l1_bytes = (l0_bytes + 7u) / 8u;
  uint64_t l2_bytes = (l1_bytes + 7u) / 8u;
  uint64_t expected_sizes[7];
  uint64_t temp;
  if (kafs_v7_align_up(((data_blocks + 63u) / 64u) * 8u, block_size, &expected_sizes[0]) != 0 ||
      kafs_v7_align_up((uint64_t)kafs_sb_inocnt_get(sb) * KAFS_V7_INODE_BYTES, block_size,
                       &expected_sizes[1]) != 0 ||
      kafs_v7_align_up(l1_bytes + l2_bytes, block_size, &expected_sizes[2]) != 0)
    return -EOVERFLOW;
  temp = le64toh(shards[3].logical_count);
  if (temp == 0 || temp > UINT32_MAX || (temp & (temp - 1u)) != 0 ||
      kafs_v7_align_up(temp * 4u, block_size, &expected_sizes[3]) != 0)
    return -EINVAL;
  temp = le64toh(shards[4].logical_count);
  if (temp == 0 || temp > UINT32_MAX ||
      kafs_v7_align_up(temp * KAFS_V7_HRL_ENTRY_BYTES, block_size, &expected_sizes[4]) != 0)
    return -EINVAL;
  expected_sizes[5] = 2u * (uint64_t)block_size;
  expected_sizes[6] = le64toh(shards[6].physical_bytes);
  if (expected_sizes[6] < 2u * (uint64_t)block_size || expected_sizes[6] % 2u != 0 ||
      (expected_sizes[6] / 2u) % block_size != 0)
    return -EINVAL;

  const uint64_t logical_counts[7] = {data_blocks,
                                      kafs_sb_inocnt_get(sb),
                                      data_blocks,
                                      le64toh(shards[3].logical_count),
                                      le64toh(shards[4].logical_count),
                                      2u,
                                      2u};
  const uint32_t record_bytes[7] = {
      8u, KAFS_V7_INODE_BYTES, 0u, 4u, KAFS_V7_HRL_ENTRY_BYTES, block_size, 0u};
  for (uint32_t i = 0; i < 7u; ++i)
  {
    const kafs_v7_shard_desc_t *shard = &shards[i];
    if (le16toh(shard->type) != types[i] || le16toh(shard->storage_class) != classes[i] ||
        le32toh(shard->flags) != 0 || le32toh(shard->group_id) != 0 ||
        le32toh(shard->reserved0) != 0 || le64toh(shard->physical_off) != expected_off ||
        le64toh(shard->physical_bytes) != expected_sizes[i] || le64toh(shard->logical_start) != 0 ||
        le64toh(shard->logical_count) != logical_counts[i] ||
        le32toh(shard->record_bytes) != record_bytes[i] || le32toh(shard->header_bytes) != 0 ||
        le64toh(shard->generation_floor) != 0 || le64toh(shard->mapping_seed) != 0 ||
        le64toh(shard->reserved1) != 0 || le64toh(shard->reserved2) != 0 ||
        le64toh(shard->reserved3) != 0 ||
        !kafs_v7_range_ok(expected_off, expected_sizes[i], file_size))
      return -EINVAL;
    expected_off += expected_sizes[i];
  }
  if (expected_off !=
          le64toh(group->metadata_physical_off) + le64toh(group->metadata_physical_bytes) ||
      expected_off != le64toh(group->data_physical_off))
    return -EINVAL;
  uint64_t data_end;
  if (kafs_v7_add_u64(expected_off, le64toh(group->data_physical_bytes), &data_end) != 0)
    return -ERANGE;
  return kafs_v7_validate_recovery_records(view, descriptor_bytes, file_size, data_end);
}

typedef struct kafs_v7_group_coverage
{
  uint64_t physical;
  uint64_t data_blocks;
  uint64_t inodes;
  uint64_t hrl_buckets;
  uint64_t hrl_entries;
  uint64_t journal_segments;
} kafs_v7_group_coverage_t;

static int kafs_v7_validate_group_descriptor(uint32_t group_id, const kafs_v7_group_desc_t *group,
                                             const kafs_v7_shard_desc_t *shards,
                                             uint32_t block_size, uint64_t file_size,
                                             kafs_v7_group_coverage_t *coverage)
{
  static const uint16_t types[KAFS_V7_GROUP_LOCAL_SHARDS] = {
      KAFS_V7_SHARD_BLOCK_BITMAP, KAFS_V7_SHARD_INODE_TABLE, KAFS_V7_SHARD_ALLOCATOR_SUMMARY,
      KAFS_V7_SHARD_HRL_INDEX,    KAFS_V7_SHARD_HRL_ENTRIES, KAFS_V7_SHARD_JOURNAL_HEADER,
      KAFS_V7_SHARD_JOURNAL_DATA};
  static const uint16_t classes[KAFS_V7_GROUP_LOCAL_SHARDS] = {
      KAFS_V7_STORAGE_BIT_PACKED,   KAFS_V7_STORAGE_FIXED_RECORD, KAFS_V7_STORAGE_ALLOCATOR_SUMMARY,
      KAFS_V7_STORAGE_FIXED_RECORD, KAFS_V7_STORAGE_FIXED_RECORD, KAFS_V7_STORAGE_FIXED_RECORD,
      KAFS_V7_STORAGE_BYTE_SPAN};
  uint64_t expected_sizes[KAFS_V7_GROUP_LOCAL_SHARDS];
  uint64_t logical_starts[KAFS_V7_GROUP_LOCAL_SHARDS];
  uint64_t logical_counts[KAFS_V7_GROUP_LOCAL_SHARDS];
  const uint32_t record_bytes[KAFS_V7_GROUP_LOCAL_SHARDS] = {
      8u, KAFS_V7_INODE_BYTES, 0u, 4u, KAFS_V7_HRL_ENTRY_BYTES, block_size, 0u};
  uint64_t data_start = le64toh(group->data_logical_start);
  uint64_t data_count = le64toh(group->data_logical_count);
  uint64_t inode_count = le64toh(shards[1].logical_count);
  uint64_t bucket_count = le64toh(shards[3].logical_count);
  uint64_t entry_count = le64toh(shards[4].logical_count);
  uint64_t segment_count = le64toh(shards[5].logical_count);
  uint64_t l0_bytes;
  uint64_t l1_bytes;
  uint64_t l2_bytes;
  uint64_t bytes;
  uint64_t expected_off = coverage->physical;

  if (le32toh(group->group_id) != group_id || le32toh(group->flags) != 0 ||
      le32toh(group->first_shard_index) != group_id * KAFS_V7_GROUP_LOCAL_SHARDS ||
      le32toh(group->shard_count) != KAFS_V7_GROUP_LOCAL_SHARDS ||
      le64toh(group->metadata_physical_off) != expected_off ||
      le64toh(group->metadata_physical_bytes) == 0 || data_start != coverage->data_blocks ||
      data_count == 0 || (group_id != 0 && (data_start & 63u) != 0) ||
      le64toh(group->data_physical_bytes) != data_count * (uint64_t)block_size ||
      le64toh(group->generation_floor) != 0 || le64toh(group->reserved0) != 0 ||
      le64toh(group->reserved1) != 0 || le64toh(group->reserved2) != 0 || inode_count == 0 ||
      bucket_count == 0 || entry_count == 0 || segment_count == 0 ||
      le64toh(shards[6].logical_start) != coverage->journal_segments ||
      le64toh(shards[6].logical_count) != segment_count)
    return -EINVAL;
  if (group_id == 0 && inode_count < 2u)
    return -EINVAL;

  if (kafs_v7_align_up(((data_count + 63u) / 64u) * 8u, block_size, &expected_sizes[0]) != 0 ||
      kafs_v7_mul_u64(inode_count, KAFS_V7_INODE_BYTES, &bytes) != 0 ||
      kafs_v7_align_up(bytes, block_size, &expected_sizes[1]) != 0)
    return -EOVERFLOW;
  l0_bytes = (data_count + 7u) / 8u;
  l1_bytes = (l0_bytes + 7u) / 8u;
  l2_bytes = (l1_bytes + 7u) / 8u;
  if (kafs_v7_add_u64(l1_bytes, l2_bytes, &bytes) != 0 ||
      kafs_v7_align_up(bytes, block_size, &expected_sizes[2]) != 0 ||
      kafs_v7_mul_u64(bucket_count, 4u, &bytes) != 0 ||
      kafs_v7_align_up(bytes, block_size, &expected_sizes[3]) != 0 ||
      kafs_v7_mul_u64(entry_count, KAFS_V7_HRL_ENTRY_BYTES, &bytes) != 0 ||
      kafs_v7_align_up(bytes, block_size, &expected_sizes[4]) != 0 ||
      kafs_v7_mul_u64(segment_count, block_size, &expected_sizes[5]) != 0)
    return -EOVERFLOW;
  expected_sizes[6] = le64toh(shards[6].physical_bytes);
  if (expected_sizes[6] < expected_sizes[5] || expected_sizes[6] % segment_count != 0 ||
      (expected_sizes[6] / segment_count) % block_size != 0)
    return -EINVAL;

  logical_starts[0] = data_start;
  logical_starts[1] = coverage->inodes;
  logical_starts[2] = data_start;
  logical_starts[3] = coverage->hrl_buckets;
  logical_starts[4] = coverage->hrl_entries;
  logical_starts[5] = coverage->journal_segments;
  logical_starts[6] = coverage->journal_segments;
  logical_counts[0] = data_count;
  logical_counts[1] = inode_count;
  logical_counts[2] = data_count;
  logical_counts[3] = bucket_count;
  logical_counts[4] = entry_count;
  logical_counts[5] = segment_count;
  logical_counts[6] = segment_count;

  for (uint32_t index = 0; index < KAFS_V7_GROUP_LOCAL_SHARDS; ++index)
  {
    const kafs_v7_shard_desc_t *shard = &shards[index];
    if (le16toh(shard->type) != types[index] || le16toh(shard->storage_class) != classes[index] ||
        le32toh(shard->flags) != 0 || le32toh(shard->group_id) != group_id ||
        le32toh(shard->reserved0) != 0 || le64toh(shard->physical_off) != expected_off ||
        le64toh(shard->physical_bytes) != expected_sizes[index] ||
        le64toh(shard->logical_start) != logical_starts[index] ||
        le64toh(shard->logical_count) != logical_counts[index] ||
        le32toh(shard->record_bytes) != record_bytes[index] || le32toh(shard->header_bytes) != 0 ||
        le64toh(shard->generation_floor) != 0 || le64toh(shard->mapping_seed) != 0 ||
        le64toh(shard->reserved1) != 0 || le64toh(shard->reserved2) != 0 ||
        le64toh(shard->reserved3) != 0 ||
        !kafs_v7_range_ok(expected_off, expected_sizes[index], file_size))
      return -EINVAL;
    expected_off += expected_sizes[index];
  }

  uint64_t metadata_end;
  uint64_t data_end;
  if (kafs_v7_add_u64(le64toh(group->metadata_physical_off),
                      le64toh(group->metadata_physical_bytes), &metadata_end) != 0 ||
      metadata_end != expected_off || le64toh(group->data_physical_off) != expected_off ||
      kafs_v7_add_u64(expected_off, le64toh(group->data_physical_bytes), &data_end) != 0 ||
      !kafs_v7_range_ok(expected_off, le64toh(group->data_physical_bytes), file_size) ||
      kafs_v7_add_u64(coverage->data_blocks, data_count, &coverage->data_blocks) != 0 ||
      kafs_v7_add_u64(coverage->inodes, inode_count, &coverage->inodes) != 0 ||
      kafs_v7_add_u64(coverage->hrl_buckets, bucket_count, &coverage->hrl_buckets) != 0 ||
      kafs_v7_add_u64(coverage->hrl_entries, entry_count, &coverage->hrl_entries) != 0 ||
      kafs_v7_add_u64(coverage->journal_segments, segment_count, &coverage->journal_segments) != 0)
    return -EOVERFLOW;
  coverage->physical = data_end;
  return 0;
}

static int kafs_v7_validate_multi_group_view(const kafs_v7_descriptor_view_t *view,
                                             uint32_t descriptor_bytes,
                                             const kafs_ssuperblock_t *sb, uint64_t file_size)
{
  kafs_v7_group_coverage_t coverage = {
      .physical = (uint64_t)view->block_size + descriptor_bytes + view->block_size,
  };
  for (uint32_t id = 0; id < view->group_count; ++id)
  {
    int rc = kafs_v7_validate_group_descriptor(id, &view->groups[id],
                                               &view->shards[id * KAFS_V7_GROUP_LOCAL_SHARDS],
                                               view->block_size, file_size, &coverage);
    if (rc != 0)
      return rc;
  }
  if (coverage.data_blocks != kafs_sb_r_blkcnt_get(sb) ||
      coverage.inodes != kafs_sb_inocnt_get(sb) || coverage.hrl_buckets == 0 ||
      coverage.hrl_buckets > (UINT64_C(1) << 32u) ||
      (coverage.hrl_buckets & (coverage.hrl_buckets - 1u)) != 0 || coverage.hrl_entries == 0 ||
      coverage.hrl_entries > UINT32_MAX || coverage.journal_segments < view->group_count ||
      coverage.journal_segments < 2u)
    return -EINVAL;
  return kafs_v7_validate_recovery_records(view, descriptor_bytes, file_size, coverage.physical);
}

static int kafs_v7_validate_descriptor_shape(const void *descriptor, uint32_t descriptor_bytes,
                                             const kafs_ssuperblock_t *sb, uint64_t file_size,
                                             const kafs_v7_root_locator_t *locator)
{
  kafs_v7_descriptor_view_t view;
  int rc = kafs_v7_descriptor_view_init(descriptor, descriptor_bytes, file_size, locator, &view);
  if (rc != 0)
    return rc;
  if (view.group_count == 1u)
    return kafs_v7_validate_single_group_view(&view, descriptor_bytes, sb, file_size);
  return kafs_v7_validate_multi_group_view(&view, descriptor_bytes, sb, file_size);
}

static int kafs_v7_validate_recovery_roots(int fd, const kafs_ssuperblock_t *sb, uint64_t file_size,
                                           uint32_t block_size, kafs_v7_layout_report_t *report)
{
  void *block = malloc(block_size);
  if (!block)
    return -ENOMEM;
  int rc = kafs_pread_all(fd, block, block_size, 0);
  if (rc == 0 && !kafs_v7_all_bytes((uint8_t *)block + sizeof(*sb), block_size - sizeof(*sb), 0))
    rc = -EINVAL;
  free(block);
  if (rc != 0)
    return rc;

  block = malloc(block_size);
  if (!block)
    return -ENOMEM;
  rc = kafs_pread_all(fd, block, block_size, (off_t)(file_size - block_size));
  if (rc == 0 && !kafs_v7_all_bytes(block, block_size - KAFS_V7_ROOT_LOCATOR_BYTES, 0))
    rc = -EINVAL;
  if (rc == 0)
  {
    kafs_v7_root_locator_t tail;
    memcpy(&tail, (uint8_t *)block + block_size - sizeof(tail), sizeof(tail));
    report->tail_locator_valid = kafs_v7_validate_locator(&tail, file_size) == 0;
    if (report->tail_locator_valid && report->primary_locator_valid &&
        memcmp(&tail, &report->locator, sizeof(tail)) != 0)
      rc = -EUCLEAN;
    else if (report->tail_locator_valid && !report->primary_locator_valid)
      report->locator = tail;
  }
  free(block);
  return rc;
}

static int kafs_v7_select_descriptors(int fd, const kafs_ssuperblock_t *sb, uint64_t file_size,
                                      kafs_v7_layout_report_t *report)
{
  uint32_t block_size = le32toh(report->locator.block_size);
  uint32_t descriptor_bytes = le32toh(report->locator.primary_desc_bytes);
  uint32_t count = le32toh(report->locator.candidate_count);
  uint64_t offsets[3];
  uint64_t checkpoints[3];
  kafs_v7_descriptor_candidate_t candidates[3];
  memset(candidates, 0, sizeof(candidates));
  if (kafs_v7_expected_replica_offsets(file_size, block_size, descriptor_bytes, offsets,
                                       checkpoints) != 0)
    return -ERANGE;

  uint64_t highest = 0;
  for (uint32_t id = 0; id < count; ++id)
  {
    report->descriptors[id].id = id;
    report->descriptors[id].role = (uint16_t)id;
    report->descriptors[id].offset = offsets[id];
    report->descriptors[id].bytes = descriptor_bytes;
    report->descriptors[id].status = KAFS_V7_REPLICA_STATUS_INVALID;
    candidates[id].bytes = malloc(descriptor_bytes);
    if (!candidates[id].bytes)
    {
      for (uint32_t allocated = 0; allocated < id; ++allocated)
        free(candidates[allocated].bytes);
      return -ENOMEM;
    }
    int rc = kafs_pread_all(fd, candidates[id].bytes, descriptor_bytes, (off_t)offsets[id]);
    if (rc == 0)
      rc = kafs_v7_validate_descriptor_shape(candidates[id].bytes, descriptor_bytes, sb, file_size,
                                             &report->locator);
    if (rc == 0)
    {
      const kafs_v7_layout_header_t *header = (const kafs_v7_layout_header_t *)candidates[id].bytes;
      candidates[id].valid = 1;
      candidates[id].generation = le64toh(header->generation);
      report->descriptors[id].generation = candidates[id].generation;
      report->descriptors[id].crc_ok = 1;
      report->descriptors[id].status = KAFS_V7_REPLICA_STATUS_VALID;
      if (candidates[id].generation > highest)
        highest = candidates[id].generation;
    }
  }
  if (highest == 0)
  {
    for (uint32_t id = 0; id < count; ++id)
      free(candidates[id].bytes);
    return -EINVAL;
  }

  uint32_t selected = UINT32_MAX;
  uint32_t equivalent = 0;
  for (uint32_t id = 0; id < count; ++id)
  {
    if (!candidates[id].valid)
      continue;
    if (candidates[id].generation < highest)
    {
      report->descriptors[id].status = KAFS_V7_REPLICA_STATUS_STALE;
      continue;
    }
    if (selected == UINT32_MAX)
    {
      selected = id;
      equivalent = 1u;
    }
    else if (memcmp(candidates[selected].bytes, candidates[id].bytes, descriptor_bytes) != 0)
    {
      report->descriptors[selected].status = KAFS_V7_REPLICA_STATUS_DIVERGENT;
      report->descriptors[id].status = KAFS_V7_REPLICA_STATUS_DIVERGENT;
      report->descriptor_divergent = 1;
    }
    else
      equivalent++;
  }
  if (report->descriptor_divergent)
  {
    for (uint32_t id = 0; id < count; ++id)
      free(candidates[id].bytes);
    return -EUCLEAN;
  }

  report->descriptor = candidates[selected].bytes;
  candidates[selected].bytes = NULL;
  report->descriptors[selected].selected = 1;
  report->selected_replica = selected;
  report->selected_generation = highest;
  report->selected_found = 1;
  report->block_size = block_size;
  report->descriptor_bytes = descriptor_bytes;
  report->replica_count = count;
  const kafs_v7_layout_header_t *header = (const kafs_v7_layout_header_t *)report->descriptor;
  report->group_count = le32toh(header->group_count);
  report->shard_count = le32toh(header->shard_count);
  if (equivalent < count)
    report->degraded = 1;
  for (uint32_t id = 0; id < count; ++id)
    free(candidates[id].bytes);
  return 0;
}

const kafs_v7_layout_header_t *kafs_v7_report_header(const kafs_v7_layout_report_t *report)
{
  return report && report->descriptor ? (const kafs_v7_layout_header_t *)report->descriptor : NULL;
}

const kafs_v7_group_desc_t *kafs_v7_report_groups(const kafs_v7_layout_report_t *report)
{
  const kafs_v7_layout_header_t *header = kafs_v7_report_header(report);
  return header ? (const kafs_v7_group_desc_t *)((const uint8_t *)report->descriptor +
                                                 le32toh(header->group_desc_off))
                : NULL;
}

const kafs_v7_shard_desc_t *kafs_v7_report_shards(const kafs_v7_layout_report_t *report)
{
  const kafs_v7_layout_header_t *header = kafs_v7_report_header(report);
  return header ? (const kafs_v7_shard_desc_t *)((const uint8_t *)report->descriptor +
                                                 le32toh(header->shard_desc_off))
                : NULL;
}

const kafs_v7_replica_desc_t *kafs_v7_report_replicas(const kafs_v7_layout_report_t *report)
{
  const kafs_v7_layout_header_t *header = kafs_v7_report_header(report);
  return header ? (const kafs_v7_replica_desc_t *)((const uint8_t *)report->descriptor +
                                                   le32toh(header->replica_desc_off))
                : NULL;
}

static int kafs_v7_select_checkpoints(int fd, const kafs_ssuperblock_t *sb, uint64_t file_size,
                                      kafs_v7_layout_report_t *report)
{
  uint64_t offsets[3];
  uint64_t checkpoint_offsets[3];
  if (kafs_v7_expected_replica_offsets(file_size, report->block_size, report->descriptor_bytes,
                                       offsets, checkpoint_offsets) != 0)
    return -ERANGE;
  kafs_v7_checkpoint_t records[3];
  int valid[3] = {0, 0, 0};
  uint64_t highest = 0;
  for (uint32_t id = 0; id < report->replica_count; ++id)
  {
    kafs_v7_copy_report_t *copy = &report->checkpoints[id];
    copy->id = id;
    copy->role = (uint16_t)id;
    copy->offset = checkpoint_offsets[id];
    copy->bytes = KAFS_V7_CHECKPOINT_BYTES;
    copy->status = KAFS_V7_REPLICA_STATUS_INVALID;
    void *block = malloc(report->block_size);
    if (!block)
      return -ENOMEM;
    int rc = kafs_pread_all(fd, block, report->block_size, (off_t)checkpoint_offsets[id]);
    if (rc == 0)
      memcpy(&records[id], block, sizeof(records[id]));
    if (rc == 0 && !kafs_v7_all_bytes((uint8_t *)block + sizeof(records[id]),
                                      report->block_size - sizeof(records[id]), 0))
      rc = -EINVAL;
    free(block);
    if (rc == 0 && (le32toh(records[id].magic) != KAFS_V7_CHECKPOINT_MAGIC ||
                    le16toh(records[id].version) != KAFS_V7_CHECKPOINT_VERSION ||
                    le16toh(records[id].record_bytes) != KAFS_V7_CHECKPOINT_BYTES ||
                    le32toh(records[id].flags) != 0 || le32toh(records[id].reserved0) != 0 ||
                    le64toh(records[id].generation) == 0 ||
                    le64toh(records[id].free_blocks) > kafs_sb_r_blkcnt_get(sb) ||
                    le64toh(records[id].free_inodes) > kafs_sb_inocnt_get(sb) - 2u ||
                    le32toh(records[id].reserved1) != 0 ||
                    le32toh(records[id].crc32) != kafs_v7_checkpoint_crc(&records[id])))
      rc = -EINVAL;
    if (rc == 0)
    {
      copy->crc_ok = 1;
      copy->generation = le64toh(records[id].generation);
      if (le64toh(records[id].descriptor_generation) != report->selected_generation)
      {
        copy->status = KAFS_V7_REPLICA_STATUS_STALE;
        report->degraded = 1;
        continue;
      }
      valid[id] = 1;
      copy->status = KAFS_V7_REPLICA_STATUS_VALID;
      if (copy->generation > highest)
        highest = copy->generation;
    }
  }
  if (highest == 0)
    return -EINVAL;

  uint32_t selected = UINT32_MAX;
  uint32_t equivalent = 0;
  for (uint32_t id = 0; id < report->replica_count; ++id)
  {
    if (!valid[id])
      continue;
    if (report->checkpoints[id].generation < highest)
    {
      report->checkpoints[id].status = KAFS_V7_REPLICA_STATUS_STALE;
      continue;
    }
    if (selected == UINT32_MAX)
    {
      selected = id;
      equivalent = 1;
    }
    else if (memcmp(&records[selected], &records[id], sizeof(records[id])) != 0)
    {
      report->checkpoints[selected].status = KAFS_V7_REPLICA_STATUS_DIVERGENT;
      report->checkpoints[id].status = KAFS_V7_REPLICA_STATUS_DIVERGENT;
      report->checkpoint_divergent = 1;
    }
    else
      equivalent++;
  }
  if (report->checkpoint_divergent)
    return -EUCLEAN;
  report->checkpoints[selected].selected = 1;
  report->selected_checkpoint = selected;
  report->checkpoint_generation = highest;
  report->checkpoint_sequence = le64toh(records[selected].checkpoint_seq);
  report->free_blocks = le64toh(records[selected].free_blocks);
  report->free_inodes = le64toh(records[selected].free_inodes);
  if (equivalent < report->replica_count)
    report->degraded = 1;
  return 0;
}

static int kafs_v7_read_shard(int fd, const kafs_v7_shard_desc_t *shard, void **out)
{
  uint64_t bytes = le64toh(shard->physical_bytes);
  if (!out || bytes == 0 || bytes > SIZE_MAX)
    return -ERANGE;
  void *buf = malloc((size_t)bytes);
  if (!buf)
    return -ENOMEM;
  int rc = kafs_pread_all(fd, buf, (size_t)bytes, (off_t)le64toh(shard->physical_off));
  if (rc != 0)
  {
    free(buf);
    return rc;
  }
  *out = buf;
  return 0;
}

static int kafs_v7_validate_bitmap_allocator(int fd, const kafs_v7_shard_desc_t *shards,
                                             uint64_t *free_blocks)
{
  void *bitmap = NULL;
  void *allocator = NULL;
  int rc = kafs_v7_read_shard(fd, &shards[0], &bitmap);
  if (rc != 0)
    return rc;
  uint64_t blocks = le64toh(shards[0].logical_count);
  uint64_t bitmap_bytes = le64toh(shards[0].physical_bytes);
  uint64_t free_count = 0;
  const uint8_t *bits = (const uint8_t *)bitmap;
  for (uint64_t block = 0; block < blocks; ++block)
  {
    if ((bits[block / 8u] & (uint8_t)(1u << (block % 8u))) == 0)
      free_count++;
  }
  for (uint64_t bit = blocks; bit < bitmap_bytes * 8u; ++bit)
  {
    if ((bits[bit / 8u] & (uint8_t)(1u << (bit % 8u))) == 0)
    {
      free(bitmap);
      return -EINVAL;
    }
  }
  rc = kafs_v7_read_shard(fd, &shards[2], &allocator);
  if (rc != 0)
  {
    free(bitmap);
    return rc;
  }
  uint64_t l0_bytes = (blocks + 7u) / 8u;
  uint64_t l1_bytes = (l0_bytes + 7u) / 8u;
  uint64_t l2_bytes = (l1_bytes + 7u) / 8u;
  uint8_t *expected = (uint8_t *)calloc(1u, (size_t)le64toh(shards[2].physical_bytes));
  if (!expected)
  {
    free(bitmap);
    free(allocator);
    return -ENOMEM;
  }
  for (uint64_t i = 0; i < l0_bytes; ++i)
  {
    uint8_t valid_mask = 0xffu;
    if (i + 1u == l0_bytes && (blocks & 7u) != 0)
      valid_mask = (uint8_t)((1u << (blocks & 7u)) - 1u);
    if ((((uint8_t *)bitmap)[i] & valid_mask) != valid_mask)
      expected[i / 8u] |= (uint8_t)(1u << (i % 8u));
  }
  for (uint64_t i = 0; i < l1_bytes; ++i)
  {
    if (expected[i] != 0)
      expected[l1_bytes + i / 8u] |= (uint8_t)(1u << (i % 8u));
  }
  rc = memcmp(expected, allocator, (size_t)le64toh(shards[2].physical_bytes)) == 0 ? 0 : -EINVAL;
  free(expected);
  free(bitmap);
  free(allocator);
  if (rc == 0)
    rc = kafs_v7_add_u64(*free_blocks, free_count, free_blocks);
  (void)l2_bytes;
  return rc;
}

static int kafs_v7_validate_inodes(int fd, const kafs_v7_shard_desc_t *shard, uint64_t *free_inodes)
{
  void *area = NULL;
  int rc = kafs_v7_read_shard(fd, shard, &area);
  if (rc != 0)
    return rc;
  uint64_t count = le64toh(shard->logical_count);
  uint64_t start = le64toh(shard->logical_start);
  const kafs_v7_inode_t *inodes = (const kafs_v7_inode_t *)area;
  uint64_t free_count = 0;
  for (uint64_t i = 0; rc == 0 && i < count; ++i)
  {
    uint64_t inode = start + i;
    int zero = kafs_v7_all_bytes(&inodes[i], sizeof(inodes[i]), 0);
    if (inode == 0)
    {
      if (!zero)
        rc = -EINVAL;
    }
    else if (inode == 1)
    {
      if (le16toh(inodes[i].mode) == 0)
        rc = -EINVAL;
    }
    else if (le16toh(inodes[i].mode) == 0)
    {
      if (!zero)
        rc = -EINVAL;
      else
        free_count++;
    }
    else if (!kafs_v7_all_bytes(inodes[i].disabled_tail_bytes,
                                sizeof(inodes[i].disabled_tail_bytes), 0))
      rc = -EINVAL;
  }
  uint64_t payload = count * KAFS_V7_INODE_BYTES;
  if (rc == 0 && !kafs_v7_all_bytes((uint8_t *)area + payload,
                                    (size_t)(le64toh(shard->physical_bytes) - payload), 0))
    rc = -EINVAL;
  free(area);
  if (rc == 0)
    rc = kafs_v7_add_u64(*free_inodes, free_count, free_inodes);
  return rc;
}

static uint64_t kafs_v7_fnv1a64(const void *data, size_t bytes)
{
  const uint8_t *p = (const uint8_t *)data;
  uint64_t hash = UINT64_C(14695981039346656037);
  for (size_t i = 0; i < bytes; ++i)
  {
    hash ^= p[i];
    hash *= UINT64_C(1099511628211);
  }
  return hash;
}

static int kafs_v7_validate_hrl(int fd, const kafs_v7_shard_desc_t *shards,
                                const kafs_v7_group_desc_t *group, uint32_t block_size,
                                uint64_t total_bucket_count)
{
  void *index_area = NULL;
  void *entry_area = NULL;
  int rc = kafs_v7_read_shard(fd, &shards[3], &index_area);
  if (rc == 0)
    rc = kafs_v7_read_shard(fd, &shards[4], &entry_area);
  if (rc != 0)
  {
    free(index_area);
    return rc;
  }
  uint64_t bucket_count = le64toh(shards[3].logical_count);
  uint64_t bucket_start = le64toh(shards[3].logical_start);
  uint64_t entry_count = le64toh(shards[4].logical_count);
  uint64_t entry_start = le64toh(shards[4].logical_start);
  uint64_t data_start = le64toh(group->data_logical_start);
  uint64_t data_count = le64toh(group->data_logical_count);
  uint8_t *seen = (uint8_t *)calloc(1u, (size_t)entry_count);
  void *data = malloc(block_size);
  if (!seen || !data)
  {
    free(seen);
    free(data);
    free(index_area);
    free(entry_area);
    return -ENOMEM;
  }
  const uint32_t *index = (const uint32_t *)index_area;
  const kafs_v7_hrl_entry_t *entries = (const kafs_v7_hrl_entry_t *)entry_area;
  for (uint64_t local_bucket = 0; rc == 0 && local_bucket < bucket_count; ++local_bucket)
  {
    uint64_t bucket = bucket_start + local_bucket;
    uint32_t next = le32toh(index[local_bucket]);
    uint64_t steps = 0;
    while (rc == 0 && next != 0)
    {
      uint64_t id = (uint64_t)next - 1u;
      if (id < entry_start || id - entry_start >= entry_count || seen[id - entry_start] ||
          ++steps > entry_count)
      {
        rc = -EINVAL;
        break;
      }
      uint64_t local_id = id - entry_start;
      seen[local_id] = 1;
      uint32_t logical_plus1 = le32toh(entries[local_id].logical_block_plus1);
      uint64_t logical = logical_plus1 == 0 ? UINT64_MAX : (uint64_t)logical_plus1 - 1u;
      if (le32toh(entries[local_id].ref_count) == 0 || logical_plus1 == 0 || logical < data_start ||
          logical - data_start >= data_count || le32toh(entries[local_id].reserved) != 0)
      {
        rc = -EINVAL;
        break;
      }
      uint64_t data_off =
          le64toh(group->data_physical_off) + (logical - data_start) * (uint64_t)block_size;
      rc = kafs_pread_all(fd, data, block_size, (off_t)data_off);
      uint64_t hash = rc == 0 ? kafs_v7_fnv1a64(data, block_size) : 0;
      if (rc == 0 && (hash != le64toh(entries[local_id].fast_hash) ||
                      (hash & (total_bucket_count - 1u)) != bucket))
        rc = -EINVAL;
      next = le32toh(entries[local_id].next_entry_id_plus1);
    }
  }
  for (uint64_t id = 0; rc == 0 && id < entry_count; ++id)
  {
    if (le32toh(entries[id].ref_count) == 0)
    {
      if (!kafs_v7_all_bytes(&entries[id], sizeof(entries[id]), 0))
        rc = -EINVAL;
    }
    else if (!seen[id])
      rc = -EINVAL;
  }
  uint64_t index_payload = bucket_count * 4u;
  uint64_t entry_payload = entry_count * KAFS_V7_HRL_ENTRY_BYTES;
  if (rc == 0 &&
      (!kafs_v7_all_bytes((uint8_t *)index_area + index_payload,
                          (size_t)(le64toh(shards[3].physical_bytes) - index_payload), 0) ||
       !kafs_v7_all_bytes((uint8_t *)entry_area + entry_payload,
                          (size_t)(le64toh(shards[4].physical_bytes) - entry_payload), 0)))
    rc = -EINVAL;
  free(seen);
  free(data);
  free(index_area);
  free(entry_area);
  return rc;
}

static int kafs_v7_validate_journal(int fd, const kafs_v7_shard_desc_t *shards, uint32_t block_size,
                                    uint64_t checkpoint_sequence, uint32_t *segment_count)
{
  void *headers = NULL;
  int rc = kafs_v7_read_shard(fd, &shards[5], &headers);
  if (rc != 0)
    return rc;
  uint64_t count_u64 = le64toh(shards[5].logical_count);
  uint64_t start_u64 = le64toh(shards[5].logical_start);
  if (count_u64 > UINT32_MAX || start_u64 > UINT32_MAX || count_u64 > UINT32_MAX - start_u64 ||
      count_u64 > UINT32_MAX - *segment_count)
  {
    free(headers);
    return -ERANGE;
  }
  uint32_t count = (uint32_t)count_u64;
  uint32_t start = (uint32_t)start_u64;
  uint64_t segment_bytes = le64toh(shards[6].physical_bytes) / count;
  for (uint32_t local = 0; rc == 0 && local < count; ++local)
  {
    uint32_t segment = start + local;
    const uint8_t *block = (const uint8_t *)headers + (uint64_t)local * block_size;
    uint64_t highest = 0;
    const kafs_v7_journal_header_t *selected = NULL;
    for (uint32_t slot = 0; slot < block_size / KAFS_V7_JOURNAL_HEADER_BYTES; ++slot)
    {
      const kafs_v7_journal_header_t *header =
          (const kafs_v7_journal_header_t *)(block + slot * KAFS_V7_JOURNAL_HEADER_BYTES);
      if (kafs_v7_all_bytes(header, sizeof(*header), 0))
        continue;
      if (le32toh(header->magic) != KAFS_V7_JOURNAL_HEADER_MAGIC ||
          le16toh(header->version) != KAFS_V7_JOURNAL_HEADER_VERSION ||
          le16toh(header->flags) != 0 || le32toh(header->segment_id) != segment ||
          le32toh(header->slot_bytes) != KAFS_V7_JOURNAL_HEADER_BYTES ||
          le64toh(header->generation) == 0 || le64toh(header->data_bytes) != segment_bytes ||
          le64toh(header->write_bytes) > segment_bytes || le32toh(header->reserved) != 0 ||
          le32toh(header->crc32) != kafs_v7_journal_header_crc(header) ||
          (le64toh(header->generation) - 1u) % (block_size / KAFS_V7_JOURNAL_HEADER_BYTES) != slot)
        continue;
      uint64_t generation = le64toh(header->generation);
      if (generation > highest)
      {
        highest = generation;
        selected = header;
      }
      else if (generation == highest && selected && memcmp(selected, header, sizeof(*header)) != 0)
      {
        rc = -EUCLEAN;
        break;
      }
    }
    if (rc == 0 && !selected)
      rc = -EINVAL;
    if (rc == 0 && (le64toh(selected->write_bytes) != 0 || le64toh(selected->first_sequence) != 0 ||
                    le64toh(selected->last_sequence) != 0 || checkpoint_sequence != 0))
      rc = -ENOTSUP;
  }
  free(headers);
  if (rc == 0)
    *segment_count += count;
  return rc;
}

static int kafs_v7_validate_payloads(int fd, const kafs_ssuperblock_t *sb,
                                     kafs_v7_layout_report_t *report)
{
  const kafs_v7_group_desc_t *groups = kafs_v7_report_groups(report);
  const kafs_v7_shard_desc_t *shards = kafs_v7_report_shards(report);
  uint64_t replica_offsets[3];
  uint64_t checkpoint_offsets[3];
  uint64_t data_end;
  uint64_t free_blocks = 0;
  uint64_t free_inodes = 0;
  uint64_t total_bucket_count = 0;
  uint64_t first_metadata_off = le64toh(groups[0].metadata_physical_off);
  uint64_t last_metadata_off = le64toh(groups[report->group_count - 1u].metadata_physical_off);
  uint64_t min_data_blocks = UINT64_MAX;
  uint64_t max_data_blocks = 0;
  int rc =
      kafs_v7_add_u64(le64toh(groups[report->group_count - 1u].data_physical_off),
                      le64toh(groups[report->group_count - 1u].data_physical_bytes), &data_end);
  for (uint32_t group_id = 0; rc == 0 && group_id < report->group_count; ++group_id)
  {
    const kafs_v7_shard_desc_t *local = &shards[(uint64_t)group_id * KAFS_V7_GROUP_LOCAL_SHARDS];
    rc = kafs_v7_add_u64(total_bucket_count, le64toh(local[3].logical_count), &total_bucket_count);
    uint64_t data_blocks = le64toh(groups[group_id].data_logical_count);
    if (data_blocks < min_data_blocks)
      min_data_blocks = data_blocks;
    if (data_blocks > max_data_blocks)
      max_data_blocks = data_blocks;
  }
  if (rc == 0)
    rc = kafs_v7_expected_replica_offsets(kafs_sb_blkcnt_get(sb) * (uint64_t)report->block_size,
                                          report->block_size, report->descriptor_bytes,
                                          replica_offsets, checkpoint_offsets);
  if (rc == 0 && report->replica_count == 3u)
  {
    rc = kafs_v7_validate_zero_range(fd, data_end, replica_offsets[2] - data_end);
    if (rc == 0)
      rc = kafs_v7_validate_zero_range(fd, checkpoint_offsets[2] + report->block_size,
                                       checkpoint_offsets[1] - checkpoint_offsets[2] -
                                           report->block_size);
  }
  else if (rc == 0)
    rc = kafs_v7_validate_zero_range(fd, data_end, checkpoint_offsets[1] - data_end);
  report->journal_segment_count = 0;
  for (uint32_t group_id = 0; rc == 0 && group_id < report->group_count; ++group_id)
  {
    const kafs_v7_group_desc_t *group = &groups[group_id];
    const kafs_v7_shard_desc_t *local = &shards[(uint64_t)group_id * KAFS_V7_GROUP_LOCAL_SHARDS];
    rc = kafs_v7_validate_bitmap_allocator(fd, local, &free_blocks);
    if (rc == 0)
      rc = kafs_v7_validate_inodes(fd, &local[1], &free_inodes);
    if (rc == 0)
      rc = kafs_v7_validate_hrl(fd, local, group, report->block_size, total_bucket_count);
    if (rc == 0)
      rc = kafs_v7_validate_journal(fd, local, report->block_size, report->checkpoint_sequence,
                                    &report->journal_segment_count);
  }
  if (rc == 0 &&
      (free_blocks != report->free_blocks || free_inodes != report->free_inodes ||
       free_blocks > kafs_sb_r_blkcnt_get(sb) || free_inodes > kafs_sb_inocnt_get(sb) - 2u))
    rc = -EUCLEAN;
  if (rc == 0)
  {
    report->placement_span_bytes = last_metadata_off - first_metadata_off;
    report->placement_arena_bytes = data_end - first_metadata_off;
    report->min_group_data_blocks = min_data_blocks;
    report->max_group_data_blocks = max_data_blocks;
  }
  return rc;
}

int kafs_v7_validate_image_fd(int fd, const kafs_ssuperblock_t *sb, uint64_t file_size,
                              kafs_v7_layout_report_t *report)
{
  if (fd < 0 || !sb || !report || file_size < 2048u)
    return -EINVAL;
  memset(report, 0, sizeof(*report));

  memcpy(&report->locator, sb->s_reserved, sizeof(report->locator));
  report->primary_locator_valid = kafs_v7_validate_locator(&report->locator, file_size) == 0;

  uint32_t block_size = report->primary_locator_valid ? le32toh(report->locator.block_size) : 0;
  if (block_size == 0)
  {
    uint8_t tail_bytes[KAFS_V7_ROOT_LOCATOR_BYTES];
    int rc =
        kafs_pread_all(fd, tail_bytes, sizeof(tail_bytes), (off_t)(file_size - sizeof(tail_bytes)));
    if (rc != 0)
      return rc;
    kafs_v7_root_locator_t tail;
    memcpy(&tail, tail_bytes, sizeof(tail));
    if (kafs_v7_validate_locator(&tail, file_size) != 0)
      return -EINVAL;
    report->locator = tail;
    block_size = le32toh(tail.block_size);
  }
  report->primary_identity_valid = kafs_v7_primary_identity(sb, file_size, block_size) == 0;
  if (!report->primary_identity_valid)
    return -EINVAL;
  int rc = kafs_v7_validate_recovery_roots(fd, sb, file_size, block_size, report);
  if (rc != 0 || (!report->primary_locator_valid && !report->tail_locator_valid))
    return rc != 0 ? rc : -EINVAL;
  if (!report->primary_locator_valid || !report->tail_locator_valid)
    report->degraded = 1;

  rc = kafs_v7_select_descriptors(fd, sb, file_size, report);
  if (rc == 0)
    rc = kafs_v7_select_checkpoints(fd, sb, file_size, report);
  if (rc == 0)
    rc = kafs_v7_validate_payloads(fd, sb, report);
  if (rc != 0)
  {
    free(report->descriptor);
    report->descriptor = NULL;
  }
  return rc;
}
