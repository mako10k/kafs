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

typedef struct kafs_v7_geometry
{
  uint64_t descriptor_bytes;
  uint64_t primary_desc_off;
  uint64_t primary_checkpoint_off;
  uint64_t tail_checkpoint_off;
  uint64_t tail_desc_off;
  uint64_t midpoint_desc_off;
  uint64_t midpoint_checkpoint_off;
  uint64_t metadata_off;
  uint64_t metadata_bytes;
  uint64_t data_off;
  uint64_t data_bytes;
  uint64_t data_blocks;
  uint64_t bitmap_bytes;
  uint64_t inode_bytes;
  uint64_t allocator_bytes;
  uint64_t hrl_index_bytes;
  uint64_t hrl_entry_bytes;
  uint64_t journal_header_bytes;
  uint64_t journal_data_bytes;
  uint64_t journal_segment_bytes;
  uint32_t hrl_bucket_count;
  uint32_t hrl_entry_count;
  uint32_t replica_count;
} kafs_v7_geometry_t;

typedef struct kafs_v7_descriptor_candidate
{
  void *bytes;
  uint64_t generation;
  int valid;
} kafs_v7_descriptor_candidate_t;

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

static int kafs_v7_compute_metadata(const kafs_v7_mkfs_options_t *options,
                                    kafs_v7_geometry_t *geometry, uint64_t data_blocks)
{
  uint64_t bytes;
  uint64_t l0_bytes;
  uint64_t l1_bytes;
  uint64_t l2_bytes;
  uint64_t total = 0;
  const uint32_t block_size = options->block_size;

  if (data_blocks == 0 || data_blocks > UINT32_MAX)
    return -ERANGE;
  geometry->data_blocks = data_blocks;
  geometry->hrl_bucket_count = 1024u;
  geometry->hrl_entry_count = kafs_v7_hrl_entry_count(data_blocks, options->hrl_entry_ratio);

  bytes = ((data_blocks + 63u) / 64u) * 8u;
  if (kafs_v7_align_up(bytes, block_size, &geometry->bitmap_bytes) != 0)
    return -EOVERFLOW;
  if (kafs_v7_mul_u64(options->inode_count, KAFS_V7_INODE_BYTES, &bytes) != 0 ||
      kafs_v7_align_up(bytes, block_size, &geometry->inode_bytes) != 0)
    return -EOVERFLOW;
  l0_bytes = (data_blocks + 7u) / 8u;
  l1_bytes = (l0_bytes + 7u) / 8u;
  l2_bytes = (l1_bytes + 7u) / 8u;
  if (kafs_v7_add_u64(l1_bytes, l2_bytes, &bytes) != 0 ||
      kafs_v7_align_up(bytes, block_size, &geometry->allocator_bytes) != 0)
    return -EOVERFLOW;
  if (kafs_v7_mul_u64(geometry->hrl_bucket_count, 4u, &bytes) != 0 ||
      kafs_v7_align_up(bytes, block_size, &geometry->hrl_index_bytes) != 0)
    return -EOVERFLOW;
  if (kafs_v7_mul_u64(geometry->hrl_entry_count, KAFS_V7_HRL_ENTRY_BYTES, &bytes) != 0 ||
      kafs_v7_align_up(bytes, block_size, &geometry->hrl_entry_bytes) != 0)
    return -EOVERFLOW;
  geometry->journal_header_bytes = 2u * (uint64_t)block_size;
  if (kafs_v7_mul_u64(2u, geometry->journal_segment_bytes, &geometry->journal_data_bytes) != 0)
    return -EOVERFLOW;

  const uint64_t spans[] = {geometry->bitmap_bytes,      geometry->inode_bytes,
                            geometry->allocator_bytes,   geometry->hrl_index_bytes,
                            geometry->hrl_entry_bytes,   geometry->journal_header_bytes,
                            geometry->journal_data_bytes};
  for (size_t i = 0; i < sizeof(spans) / sizeof(spans[0]); ++i)
  {
    if (kafs_v7_add_u64(total, spans[i], &total) != 0)
      return -EOVERFLOW;
  }
  geometry->metadata_bytes = total;
  if (kafs_v7_add_u64(geometry->metadata_off, total, &geometry->data_off) != 0 ||
      kafs_v7_mul_u64(data_blocks, block_size, &geometry->data_bytes) != 0)
    return -EOVERFLOW;
  return 0;
}

static int kafs_v7_plan(const kafs_v7_mkfs_options_t *options, kafs_v7_geometry_t *geometry)
{
  uint32_t descriptor_bytes;
  uint32_t unused_group_off;
  uint32_t unused_shard_off;
  uint32_t unused_replica_off;
  uint64_t low = 1;
  uint64_t high;
  uint64_t best = 0;

  if (!options || !geometry || !kafs_v7_supported_block_size(options->block_size) ||
      options->inode_count < 2u || !isfinite(options->hrl_entry_ratio) ||
      options->hrl_entry_ratio <= 0.0 || options->hrl_entry_ratio > 1.0 ||
      options->image_size_bytes == 0 || options->image_size_bytes % options->block_size != 0 ||
      options->image_size_bytes / options->block_size > UINT32_MAX)
    return -EINVAL;

  memset(geometry, 0, sizeof(*geometry));
  geometry->replica_count = 2u;
  if (kafs_v7_descriptor_size(options->block_size, 1u, 11u, 2u, &descriptor_bytes,
                              &unused_group_off, &unused_shard_off, &unused_replica_off) != 0)
    return -EOVERFLOW;
  geometry->descriptor_bytes = descriptor_bytes;
  geometry->primary_desc_off = options->block_size;
  if (kafs_v7_add_u64(geometry->primary_desc_off, descriptor_bytes,
                      &geometry->primary_checkpoint_off) != 0 ||
      kafs_v7_add_u64(geometry->primary_checkpoint_off, options->block_size,
                      &geometry->metadata_off) != 0)
    return -EOVERFLOW;
  if (options->image_size_bytes <
      (uint64_t)options->block_size + descriptor_bytes + options->block_size)
    return -ENOSPC;
  geometry->tail_desc_off = options->image_size_bytes - options->block_size - descriptor_bytes;
  if (geometry->tail_desc_off < options->block_size)
    return -ENOSPC;
  geometry->tail_checkpoint_off = geometry->tail_desc_off - options->block_size;
  if (geometry->metadata_off >= geometry->tail_checkpoint_off)
    return -ENOSPC;

  geometry->journal_segment_bytes =
      ((options->journal_bytes / 2u) / options->block_size) * options->block_size;
  if (geometry->journal_segment_bytes < options->block_size)
    return -ENOSPC;

  high = (geometry->tail_checkpoint_off - geometry->metadata_off) / options->block_size;
  while (low <= high)
  {
    uint64_t mid = low + (high - low) / 2u;
    kafs_v7_geometry_t candidate = *geometry;
    uint64_t end;
    int rc = kafs_v7_compute_metadata(options, &candidate, mid);
    if (rc == 0)
      rc = kafs_v7_add_u64(candidate.data_off, candidate.data_bytes, &end);
    if (rc == 0 && end <= geometry->tail_checkpoint_off)
    {
      best = mid;
      *geometry = candidate;
      low = mid + 1u;
    }
    else
    {
      if (mid == 0)
        break;
      high = mid - 1u;
    }
  }
  if (best == 0)
    return -ENOSPC;

  geometry->midpoint_desc_off =
      (options->image_size_bytes / 2u) & ~((uint64_t)options->block_size - 1u);
  if (kafs_v7_add_u64(geometry->midpoint_desc_off, descriptor_bytes,
                      &geometry->midpoint_checkpoint_off) != 0)
    return -EOVERFLOW;
  uint64_t midpoint_end;
  uint64_t data_end;
  if (kafs_v7_add_u64(geometry->midpoint_checkpoint_off, options->block_size, &midpoint_end) != 0 ||
      kafs_v7_add_u64(geometry->data_off, geometry->data_bytes, &data_end) != 0)
    return -EOVERFLOW;
  if (geometry->midpoint_desc_off >= data_end && midpoint_end <= geometry->tail_checkpoint_off)
    geometry->replica_count = 3u;
  return 0;
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
  uint32_t shard_count = 7u + 2u * geometry->replica_count;
  int rc = kafs_v7_descriptor_size(options->block_size, 1u, shard_count, geometry->replica_count,
                                   &descriptor_bytes, &group_off, &shard_off, &replica_off);
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
  header->group_count = htole32(1u);
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

  groups[0].group_id = htole32(0u);
  groups[0].first_shard_index = htole32(0u);
  groups[0].shard_count = htole32(KAFS_V7_SINGLE_GROUP_LOCAL_SHARDS);
  groups[0].metadata_physical_off = htole64(geometry->metadata_off);
  groups[0].metadata_physical_bytes = htole64(geometry->metadata_bytes);
  groups[0].data_logical_start = htole64(0u);
  groups[0].data_logical_count = htole64(geometry->data_blocks);
  groups[0].data_physical_off = htole64(geometry->data_off);
  groups[0].data_physical_bytes = htole64(geometry->data_bytes);

  uint64_t off = geometry->metadata_off;
  kafs_v7_shard_set(&shards[0], KAFS_V7_SHARD_BLOCK_BITMAP, KAFS_V7_STORAGE_BIT_PACKED, 0u, off,
                    geometry->bitmap_bytes, 0u, geometry->data_blocks, 8u);
  off += geometry->bitmap_bytes;
  kafs_v7_shard_set(&shards[1], KAFS_V7_SHARD_INODE_TABLE, KAFS_V7_STORAGE_FIXED_RECORD, 0u, off,
                    geometry->inode_bytes, 0u, options->inode_count, KAFS_V7_INODE_BYTES);
  off += geometry->inode_bytes;
  kafs_v7_shard_set(&shards[2], KAFS_V7_SHARD_ALLOCATOR_SUMMARY, KAFS_V7_STORAGE_ALLOCATOR_SUMMARY,
                    0u, off, geometry->allocator_bytes, 0u, geometry->data_blocks, 0u);
  off += geometry->allocator_bytes;
  kafs_v7_shard_set(&shards[3], KAFS_V7_SHARD_HRL_INDEX, KAFS_V7_STORAGE_FIXED_RECORD, 0u, off,
                    geometry->hrl_index_bytes, 0u, geometry->hrl_bucket_count, 4u);
  off += geometry->hrl_index_bytes;
  kafs_v7_shard_set(&shards[4], KAFS_V7_SHARD_HRL_ENTRIES, KAFS_V7_STORAGE_FIXED_RECORD, 0u, off,
                    geometry->hrl_entry_bytes, 0u, geometry->hrl_entry_count,
                    KAFS_V7_HRL_ENTRY_BYTES);
  off += geometry->hrl_entry_bytes;
  kafs_v7_shard_set(&shards[5], KAFS_V7_SHARD_JOURNAL_HEADER, KAFS_V7_STORAGE_FIXED_RECORD, 0u, off,
                    geometry->journal_header_bytes, 0u, 2u, options->block_size);
  off += geometry->journal_header_bytes;
  kafs_v7_shard_set(&shards[6], KAFS_V7_SHARD_JOURNAL_DATA, KAFS_V7_STORAGE_BYTE_SPAN, 0u, off,
                    geometry->journal_data_bytes, 0u, 2u, 0u);

  uint32_t shard_index = KAFS_V7_SINGLE_GROUP_LOCAL_SHARDS;
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

static int kafs_v7_write_metadata(int fd, const kafs_v7_mkfs_options_t *options,
                                  const kafs_v7_geometry_t *geometry)
{
  int rc;
  uint8_t *bitmap = (uint8_t *)malloc((size_t)geometry->bitmap_bytes);
  if (!bitmap)
    return -ENOMEM;
  memset(bitmap, 0xff, (size_t)geometry->bitmap_bytes);
  uint64_t bitmap_payload = ((geometry->data_blocks + 63u) / 64u) * 8u;
  memset(bitmap, 0, (size_t)bitmap_payload);
  uint32_t trailing = (uint32_t)(geometry->data_blocks & 63u);
  if (trailing != 0)
  {
    uint64_t valid_mask = (UINT64_C(1) << trailing) - 1u;
    uint64_t invalid_mask = htole64(~valid_mask);
    memcpy(bitmap + bitmap_payload - 8u, &invalid_mask, sizeof(invalid_mask));
  }
  rc = kafs_pwrite_all(fd, bitmap, (size_t)geometry->bitmap_bytes, (off_t)geometry->metadata_off);
  free(bitmap);
  if (rc != 0)
    return rc;

  uint64_t inode_off = geometry->metadata_off + geometry->bitmap_bytes;
  void *inode_area = calloc(1u, (size_t)geometry->inode_bytes);
  if (!inode_area)
    return -ENOMEM;
  kafs_v7_inode_t *inodes = (kafs_v7_inode_t *)inode_area;
  inodes[1].mode = htole16((uint16_t)(S_IFDIR | 0755));
  inodes[1].uid = htole16(options->root_uid);
  inodes[1].gid = htole16(options->root_gid);
  inodes[1].link_count = htole16(1u);
  rc = kafs_pwrite_all(fd, inode_area, (size_t)geometry->inode_bytes, (off_t)inode_off);
  free(inode_area);
  if (rc != 0)
    return rc;

  uint64_t allocator_off = inode_off + geometry->inode_bytes;
  uint8_t *allocator = (uint8_t *)calloc(1u, (size_t)geometry->allocator_bytes);
  if (!allocator)
    return -ENOMEM;
  uint64_t l0_bytes = (geometry->data_blocks + 7u) / 8u;
  uint64_t l1_bytes = (l0_bytes + 7u) / 8u;
  uint64_t l2_bytes = (l1_bytes + 7u) / 8u;
  for (uint64_t i = 0; i < l0_bytes; ++i)
    allocator[i / 8u] |= (uint8_t)(1u << (i % 8u));
  for (uint64_t i = 0; i < l1_bytes; ++i)
  {
    if (allocator[i] != 0)
      allocator[l1_bytes + i / 8u] |= (uint8_t)(1u << (i % 8u));
  }
  (void)l2_bytes;
  rc = kafs_pwrite_all(fd, allocator, (size_t)geometry->allocator_bytes, (off_t)allocator_off);
  free(allocator);
  if (rc != 0)
    return rc;

  uint64_t hrl_index_off = allocator_off + geometry->allocator_bytes;
  rc =
      kafs_v7_write_zeroes(fd, hrl_index_off, geometry->hrl_index_bytes + geometry->hrl_entry_bytes,
                           options->block_size);
  if (rc != 0)
    return rc;

  uint64_t journal_header_off =
      hrl_index_off + geometry->hrl_index_bytes + geometry->hrl_entry_bytes;
  void *journal_headers = calloc(1u, (size_t)geometry->journal_header_bytes);
  if (!journal_headers)
    return -ENOMEM;
  for (uint32_t segment = 0; segment < 2u; ++segment)
  {
    kafs_v7_journal_header_t *header =
        (kafs_v7_journal_header_t *)((uint8_t *)journal_headers +
                                     (uint64_t)segment * options->block_size);
    header->magic = htole32(KAFS_V7_JOURNAL_HEADER_MAGIC);
    header->version = htole16(KAFS_V7_JOURNAL_HEADER_VERSION);
    header->segment_id = htole32(segment);
    header->slot_bytes = htole32(KAFS_V7_JOURNAL_HEADER_BYTES);
    header->generation = htole64(1u);
    header->data_bytes = htole64(geometry->journal_segment_bytes);
    header->crc32 = htole32(kafs_v7_journal_header_crc(header));
  }
  rc = kafs_pwrite_all(fd, journal_headers, (size_t)geometry->journal_header_bytes,
                       (off_t)journal_header_off);
  free(journal_headers);
  if (rc != 0)
    return rc;
  return kafs_v7_write_zeroes(fd, journal_header_off + geometry->journal_header_bytes,
                              geometry->journal_data_bytes, options->block_size);
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
  uint64_t data_end = geometry.data_off + geometry.data_bytes;
  if (rc == 0 && data_end < geometry.tail_checkpoint_off)
    rc = kafs_v7_write_zeroes(fd, data_end, geometry.tail_checkpoint_off - data_end,
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

static int kafs_v7_validate_descriptor_shape(const void *descriptor, uint32_t descriptor_bytes,
                                             const kafs_ssuperblock_t *sb, uint64_t file_size,
                                             const kafs_v7_root_locator_t *locator)
{
  if (!descriptor || descriptor_bytes < sizeof(kafs_v7_layout_header_t))
    return -EINVAL;
  const kafs_v7_layout_header_t *header = (const kafs_v7_layout_header_t *)descriptor;
  uint32_t group_count = le32toh(header->group_count);
  uint32_t shard_count = le32toh(header->shard_count);
  uint32_t replica_count = le32toh(header->replica_count);
  uint32_t expected_bytes;
  uint32_t expected_group_off;
  uint32_t expected_shard_off;
  uint32_t expected_replica_off;

  if (le32toh(header->magic) != KAFS_V7_LAYOUT_MAGIC ||
      le16toh(header->version) != KAFS_V7_LAYOUT_VERSION ||
      le16toh(header->header_bytes) != KAFS_V7_LAYOUT_HEADER_BYTES ||
      le32toh(header->descriptor_bytes) != descriptor_bytes || le32toh(header->flags) != 0 ||
      le64toh(header->generation) == 0 || le64toh(header->image_size_bytes) != file_size ||
      le32toh(header->block_size) != le32toh(locator->block_size) || group_count != 1u ||
      shard_count != 7u + 2u * replica_count ||
      replica_count != le32toh(locator->candidate_count) ||
      le16toh(header->group_desc_bytes) != KAFS_V7_GROUP_DESC_BYTES ||
      le16toh(header->mapping_policy) != 0 ||
      le16toh(header->shard_desc_bytes) != KAFS_V7_SHARD_DESC_BYTES ||
      le16toh(header->replica_desc_bytes) != KAFS_V7_REPLICA_DESC_BYTES ||
      le16toh(header->reserved0) != 0 || le16toh(header->reserved1) != 0 ||
      le64toh(header->feature_flags) != 0 ||
      le64toh(header->incompat_flags) != KAFS_V7_REQUIRED_INCOMPAT_FLAGS ||
      le64toh(header->ro_compat_flags) != 0 || le64toh(header->mapping_seed) != 0 ||
      le32toh(header->reserved2) != 0 || le64toh(header->reserved3) != 0 ||
      le64toh(header->reserved4) != 0 ||
      le32toh(header->descriptor_crc32) !=
          kafs_v7_descriptor_crc((void *)descriptor, descriptor_bytes))
    return -EINVAL;
  if (kafs_v7_descriptor_size(le32toh(header->block_size), group_count, shard_count, replica_count,
                              &expected_bytes, &expected_group_off, &expected_shard_off,
                              &expected_replica_off) != 0 ||
      expected_bytes != descriptor_bytes || le32toh(header->group_desc_off) != expected_group_off ||
      le32toh(header->shard_desc_off) != expected_shard_off ||
      le32toh(header->replica_desc_off) != expected_replica_off ||
      kafs_v7_table_bounds(expected_group_off, group_count, KAFS_V7_GROUP_DESC_BYTES,
                           descriptor_bytes) != 0 ||
      kafs_v7_table_bounds(expected_shard_off, shard_count, KAFS_V7_SHARD_DESC_BYTES,
                           descriptor_bytes) != 0 ||
      kafs_v7_table_bounds(expected_replica_off, replica_count, KAFS_V7_REPLICA_DESC_BYTES,
                           descriptor_bytes) != 0)
    return -ERANGE;

  const uint8_t *bytes = (const uint8_t *)descriptor;
  const kafs_v7_group_desc_t *group = (const kafs_v7_group_desc_t *)(bytes + expected_group_off);
  const kafs_v7_shard_desc_t *shards = (const kafs_v7_shard_desc_t *)(bytes + expected_shard_off);
  const kafs_v7_replica_desc_t *replicas =
      (const kafs_v7_replica_desc_t *)(bytes + expected_replica_off);
  const uint32_t block_size = le32toh(header->block_size);
  const uint64_t data_blocks = le64toh(group->data_logical_count);
  uint64_t replica_offsets[3];
  uint64_t checkpoint_offsets[3];
  if (kafs_v7_expected_replica_offsets(file_size, block_size, descriptor_bytes, replica_offsets,
                                       checkpoint_offsets) != 0)
    return -ERANGE;

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
  if (kafs_v7_add_u64(expected_off, le64toh(group->data_physical_bytes), &data_end) != 0 ||
      data_end > checkpoint_offsets[1])
    return -ERANGE;

  for (uint32_t id = 0; id < replica_count; ++id)
  {
    if (le32toh(replicas[id].replica_id) != id || le16toh(replicas[id].role) != id ||
        le16toh(replicas[id].flags) != 0 ||
        le64toh(replicas[id].physical_off) != replica_offsets[id] ||
        le32toh(replicas[id].descriptor_bytes) != descriptor_bytes ||
        le32toh(replicas[id].reserved0) != 0 || le64toh(replicas[id].reserved1) != 0)
      return -EINVAL;
    const kafs_v7_shard_desc_t *layout_shard = &shards[7u + id * 2u];
    const kafs_v7_shard_desc_t *checkpoint_shard = &shards[8u + id * 2u];
    if (le16toh(layout_shard->type) != KAFS_V7_SHARD_LAYOUT_DESCRIPTOR ||
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
        le64toh(checkpoint_shard->physical_bytes) != block_size ||
        le64toh(checkpoint_shard->logical_start) != id ||
        le64toh(checkpoint_shard->logical_count) != 1u ||
        le32toh(checkpoint_shard->record_bytes) != KAFS_V7_CHECKPOINT_BYTES ||
        le32toh(checkpoint_shard->header_bytes) != 0 ||
        le64toh(checkpoint_shard->generation_floor) != 0 ||
        le64toh(checkpoint_shard->mapping_seed) != 0 || le64toh(checkpoint_shard->reserved1) != 0 ||
        le64toh(checkpoint_shard->reserved2) != 0 || le64toh(checkpoint_shard->reserved3) != 0)
      return -EINVAL;
  }
  uint64_t midpoint_end = checkpoint_offsets[2] + block_size;
  int midpoint_fits = replica_offsets[2] >= data_end && midpoint_end <= checkpoint_offsets[1];
  if ((replica_count == 3u) != midpoint_fits)
    return -EINVAL;

  uint32_t live_shard_end = expected_shard_off + shard_count * KAFS_V7_SHARD_DESC_BYTES;
  if (!kafs_v7_all_bytes(bytes + live_shard_end, expected_replica_off - live_shard_end, 0) ||
      !kafs_v7_all_bytes(
          bytes + expected_replica_off + replica_count * KAFS_V7_REPLICA_DESC_BYTES,
          descriptor_bytes - expected_replica_off - replica_count * KAFS_V7_REPLICA_DESC_BYTES, 0))
    return -EINVAL;
  return 0;
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
    *free_blocks = free_count;
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
  const kafs_v7_inode_t *inodes = (const kafs_v7_inode_t *)area;
  uint64_t free_count = 0;
  if (!kafs_v7_all_bytes(&inodes[0], sizeof(inodes[0]), 0) || le16toh(inodes[1].mode) == 0)
    rc = -EINVAL;
  for (uint64_t i = 1; rc == 0 && i < count; ++i)
  {
    int zero = kafs_v7_all_bytes(&inodes[i], sizeof(inodes[i]), 0);
    if (le16toh(inodes[i].mode) == 0)
    {
      if (!zero)
        rc = -EINVAL;
      else if (i >= 2u)
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
    *free_inodes = free_count;
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
                                const kafs_v7_group_desc_t *group, uint32_t block_size)
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
  uint64_t entry_count = le64toh(shards[4].logical_count);
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
  for (uint64_t bucket = 0; rc == 0 && bucket < bucket_count; ++bucket)
  {
    uint32_t next = le32toh(index[bucket]);
    uint64_t steps = 0;
    while (next != 0)
    {
      uint64_t id = (uint64_t)next - 1u;
      if (id >= entry_count || seen[id] || ++steps > entry_count)
      {
        rc = -EINVAL;
        break;
      }
      seen[id] = 1;
      uint32_t logical_plus1 = le32toh(entries[id].logical_block_plus1);
      if (le32toh(entries[id].ref_count) == 0 || logical_plus1 == 0 ||
          logical_plus1 - 1u >= le64toh(group->data_logical_count) ||
          le32toh(entries[id].reserved) != 0)
      {
        rc = -EINVAL;
        break;
      }
      uint64_t data_off =
          le64toh(group->data_physical_off) + (uint64_t)(logical_plus1 - 1u) * block_size;
      rc = kafs_pread_all(fd, data, block_size, (off_t)data_off);
      uint64_t hash = rc == 0 ? kafs_v7_fnv1a64(data, block_size) : 0;
      if (rc == 0 &&
          (hash != le64toh(entries[id].fast_hash) || (hash & (bucket_count - 1u)) != bucket))
        rc = -EINVAL;
      next = le32toh(entries[id].next_entry_id_plus1);
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
  uint32_t count = (uint32_t)le64toh(shards[5].logical_count);
  uint64_t segment_bytes = le64toh(shards[6].physical_bytes) / count;
  for (uint32_t segment = 0; rc == 0 && segment < count; ++segment)
  {
    const uint8_t *block = (const uint8_t *)headers + (uint64_t)segment * block_size;
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
    *segment_count = count;
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
  int rc = kafs_v7_add_u64(le64toh(groups[0].data_physical_off),
                           le64toh(groups[0].data_physical_bytes), &data_end);
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
  if (rc == 0)
    rc = kafs_v7_validate_bitmap_allocator(fd, shards, &free_blocks);
  if (rc == 0)
    rc = kafs_v7_validate_inodes(fd, &shards[1], &free_inodes);
  if (rc == 0)
    rc = kafs_v7_validate_hrl(fd, shards, groups, report->block_size);
  if (rc == 0)
    rc = kafs_v7_validate_journal(fd, shards, report->block_size, report->checkpoint_sequence,
                                  &report->journal_segment_count);
  if (rc == 0 &&
      (free_blocks != report->free_blocks || free_inodes != report->free_inodes ||
       free_blocks > kafs_sb_r_blkcnt_get(sb) || free_inodes > kafs_sb_inocnt_get(sb) - 2u))
    rc = -EUCLEAN;
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
