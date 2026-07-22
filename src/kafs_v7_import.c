#include "kafs_v7_import.h"

#include <stdio.h>

#include "kafs_dirent.h"
#include "kafs_inode.h"
#include "kafs_superblock.h"
#include "kafs_tailmeta.h"
#include "kafs_tool_util.h"
#include "kafs_v7_layout.h"

#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <limits.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <sys/file.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#ifndef O_CLOEXEC
#define O_CLOEXEC 0
#endif
#ifndef O_NOFOLLOW
#define O_NOFOLLOW 0
#endif

#define KAFS_V5_PENDING_REF_FLAG UINT32_C(0x80000000)
#define KAFS_V7_IMPORT_DEFAULT_JOURNAL_BYTES (UINT64_C(1) << 20)
#define KAFS_V7_IMPORT_DEFAULT_HRL_RATIO 0.75

typedef struct kafs_v7_import_source_object
{
  const kafs_sinode_v5_t *inode;
  uint8_t *directory_payload;
  size_t directory_payload_bytes;
  uint32_t incoming_links;
  uint32_t namespace_parent;
  uint32_t dotdot_parent;
  uint8_t allocated;
  uint8_t reachable;
  uint8_t dotdot_seen;
} kafs_v7_import_source_object_t;

typedef struct kafs_v7_import_source_edge
{
  uint32_t parent;
  uint32_t target;
} kafs_v7_import_source_edge_t;

typedef struct kafs_v7_import_source
{
  int fd;
  struct stat initial_stat;
  uint8_t *image;
  size_t image_bytes;
  const kafs_ssuperblock_t *superblock;
  const uint8_t *bitmap;
  const uint8_t *inode_table;
  uint64_t bitmap_off;
  uint64_t inode_table_off;
  uint32_t block_size;
  uint32_t log_block_size;
  uint32_t block_count;
  uint32_t first_data_block;
  uint32_t inode_count;
  uint32_t used_inode_count;
  uint32_t directory_count;
  uint32_t regular_count;
  uint32_t symlink_count;
  uint32_t source_crc32;
  uint64_t payload_bytes;
  kafs_v7_import_source_object_t *objects;
  kafs_v7_import_source_edge_t *edges;
  size_t edge_count;
  size_t edge_capacity;
} kafs_v7_import_source_t;

typedef struct kafs_v7_import_destination_group
{
  const kafs_v7_group_desc_t *group;
  const kafs_v7_shard_desc_t *bitmap_shard;
  const kafs_v7_shard_desc_t *allocator_shard;
  uint8_t *bitmap;
  uint64_t bitmap_bytes;
  uint64_t allocator_bytes;
  uint64_t cursor;
} kafs_v7_import_destination_group_t;

typedef struct kafs_v7_import_destination
{
  int fd;
  const kafs_v7_layout_report_t *layout;
  kafs_v7_import_destination_group_t *groups;
  uint64_t allocated_blocks;
} kafs_v7_import_destination_t;

static uint64_t kafs_v7_import_align_up(uint64_t value, uint64_t alignment)
{
  if (alignment == 0u || value > UINT64_MAX - (alignment - 1u))
    return UINT64_MAX;
  return (value + alignment - 1u) & ~(alignment - 1u);
}

static int kafs_v7_import_range_valid(uint64_t off, uint64_t bytes, uint64_t extent)
{
  return off <= extent && bytes <= extent - off;
}

static int kafs_v7_import_stat_equal(const struct stat *left, const struct stat *right)
{
  return left->st_dev == right->st_dev && left->st_ino == right->st_ino &&
         left->st_mode == right->st_mode && left->st_size == right->st_size &&
         left->st_mtim.tv_sec == right->st_mtim.tv_sec &&
         left->st_mtim.tv_nsec == right->st_mtim.tv_nsec &&
         left->st_ctim.tv_sec == right->st_ctim.tv_sec &&
         left->st_ctim.tv_nsec == right->st_ctim.tv_nsec;
}

static int kafs_v7_import_crc_fd(int fd, uint64_t bytes, uint32_t *crc_out)
{
  uint8_t buffer[64u * 1024u];
  uint32_t crc = UINT32_MAX;
  uint64_t off = 0u;

  if (fd < 0 || !crc_out)
    return -EINVAL;
  while (off < bytes)
  {
    size_t chunk = bytes - off < sizeof(buffer) ? (size_t)(bytes - off) : sizeof(buffer);
    ssize_t got = pread(fd, buffer, chunk, (off_t)off);
    if (got < 0)
      return -errno;
    if ((size_t)got != chunk)
      return -EIO;
    crc = kafs_v7_crc32_update(crc, buffer, chunk);
    off += chunk;
  }
  *crc_out = crc ^ UINT32_MAX;
  return 0;
}

static int kafs_v7_import_source_block_used(const kafs_v7_import_source_t *source, uint32_t block)
{
  return block < source->block_count &&
         (source->bitmap[block / 8u] & (uint8_t)(1u << (block % 8u))) != 0u;
}

static int kafs_v7_import_source_decode_ref(const kafs_v7_import_source_t *source, uint32_t raw,
                                            uint32_t *block_out)
{
  if (!source || !block_out)
    return -EINVAL;
  if (raw == 0u)
  {
    *block_out = 0u;
    return 0;
  }
  if ((raw & KAFS_V5_PENDING_REF_FLAG) != 0u)
    return -EAGAIN;
  if (raw < source->first_data_block || raw >= source->block_count ||
      !kafs_v7_import_source_block_used(source, raw))
    return -EUCLEAN;
  if (!kafs_v7_import_range_valid((uint64_t)raw * source->block_size, source->block_size,
                                  source->image_bytes))
    return -ERANGE;
  *block_out = raw;
  return 0;
}

static int kafs_v7_import_source_ref_table(const kafs_v7_import_source_t *source, uint32_t raw,
                                           const uint32_t **table_out)
{
  uint32_t block = 0u;
  int rc = kafs_v7_import_source_decode_ref(source, raw, &block);
  if (rc != 0)
    return rc;
  if (block == 0u)
  {
    *table_out = NULL;
    return 0;
  }
  *table_out = (const uint32_t *)(source->image + (uint64_t)block * source->block_size);
  return 0;
}

static int kafs_v7_import_source_ref_path(const kafs_v7_import_source_t *source, uint32_t raw,
                                          const uint64_t *indices, size_t depth,
                                          const uint32_t **table_out)
{
  const uint32_t *table = NULL;
  int rc = kafs_v7_import_source_ref_table(source, raw, &table);
  for (size_t level = 0u; rc == 0 && table && level < depth; ++level)
    rc = kafs_v7_import_source_ref_table(source, le32toh(table[indices[level]]), &table);
  if (rc == 0)
    *table_out = table;
  return rc;
}

static int kafs_v7_import_source_data_block(const kafs_v7_import_source_t *source,
                                            const kafs_sinode_v5_t *inode, uint64_t logical,
                                            uint32_t *block_out)
{
  uint64_t refs_per_block = source->block_size / sizeof(uint32_t);
  const uint32_t *first = NULL;
  const uint32_t *second = NULL;
  uint32_t raw = 0u;
  int rc;

  if (logical < 12u)
  {
    raw = le32toh(inode->i_blkreftbl[logical].value);
    return kafs_v7_import_source_decode_ref(source, raw, block_out);
  }
  logical -= 12u;
  if (logical < refs_per_block)
  {
    rc = kafs_v7_import_source_ref_table(source, le32toh(inode->i_blkreftbl[12].value), &first);
    if (rc != 0)
      return rc;
    if (!first)
    {
      *block_out = 0u;
      return 0;
    }
    return kafs_v7_import_source_decode_ref(source, le32toh(first[logical]), block_out);
  }
  logical -= refs_per_block;
  if (logical < refs_per_block * refs_per_block)
  {
    uint64_t outer = logical / refs_per_block;
    uint64_t inner = logical % refs_per_block;
    rc = kafs_v7_import_source_ref_path(source, le32toh(inode->i_blkreftbl[13].value), &outer, 1u,
                                        &second);
    if (rc != 0)
      return rc;
    if (!second)
    {
      *block_out = 0u;
      return 0;
    }
    return kafs_v7_import_source_decode_ref(source, le32toh(second[inner]), block_out);
  }
  logical -= refs_per_block * refs_per_block;
  if (logical >= refs_per_block * refs_per_block * refs_per_block)
    return -EFBIG;
  uint64_t outer = logical / (refs_per_block * refs_per_block);
  uint64_t remainder = logical % (refs_per_block * refs_per_block);
  uint64_t middle = remainder / refs_per_block;
  uint64_t inner = remainder % refs_per_block;
  uint64_t indices[] = {outer, middle};
  rc = kafs_v7_import_source_ref_path(source, le32toh(inode->i_blkreftbl[14].value), indices, 2u,
                                      &first);
  if (rc != 0)
    return rc;
  if (!first)
  {
    *block_out = 0u;
    return 0;
  }
  return kafs_v7_import_source_decode_ref(source, le32toh(first[inner]), block_out);
}

static int kafs_v7_import_source_read_full(const kafs_v7_import_source_t *source,
                                           const kafs_sinode_v5_t *inode, void *buffer,
                                           uint64_t bytes, uint64_t offset)
{
  uint8_t *out = (uint8_t *)buffer;
  uint64_t done = 0u;

  while (done < bytes)
  {
    uint64_t position = offset + done;
    uint64_t logical = position / source->block_size;
    size_t in_block = (size_t)(position % source->block_size);
    size_t chunk = source->block_size - in_block;
    if ((uint64_t)chunk > bytes - done)
      chunk = (size_t)(bytes - done);
    uint32_t block = 0u;
    int rc = kafs_v7_import_source_data_block(source, inode, logical, &block);
    if (rc != 0)
      return rc;
    if (block == 0u)
      return -ENOTSUP;
    memcpy(out + done, source->image + (uint64_t)block * source->block_size + in_block, chunk);
    done += chunk;
  }
  return 0;
}

static int kafs_v7_import_source_tail_payload(const kafs_v7_import_source_t *source, uint32_t ino,
                                              const kafs_sinode_v5_t *inode,
                                              const uint8_t **payload_out, uint16_t *bytes_out)
{
  const kafs_sinode_taildesc_v5_t *tail = &inode->i_taildesc;
  uint64_t region_off = kafs_sb_tailmeta_offset_get(source->superblock);
  uint64_t region_bytes = kafs_sb_tailmeta_size_get(source->superblock);
  if (!kafs_v7_import_range_valid(region_off, region_bytes, source->image_bytes) ||
      region_bytes < sizeof(kafs_tailmeta_region_hdr_t))
    return -ERANGE;
  const uint8_t *base = source->image + region_off;
  const kafs_tailmeta_region_hdr_t *header = (const kafs_tailmeta_region_hdr_t *)base;
  int rc = kafs_tailmeta_region_hdr_validate(header, region_bytes);
  if (rc != 0)
    return rc;
  uint32_t count = kafs_tailmeta_region_hdr_container_count_get(header);
  uint32_t table_off = kafs_tailmeta_region_hdr_container_table_off_get(header);
  const kafs_tailmeta_container_hdr_t *containers =
      (const kafs_tailmeta_container_hdr_t *)(base + table_off);
  uint32_t base_block = (uint32_t)(region_off / source->block_size);
  uint32_t container_block = kafs_ino_taildesc_v5_container_blo_get(tail);
  if (container_block <= base_block || container_block - base_block - 1u >= count)
    return -EPROTO;
  uint32_t index = container_block - base_block - 1u;
  const kafs_tailmeta_container_hdr_t *container = &containers[index];
  rc = kafs_tailmeta_container_hdr_validate(container, region_bytes,
                                            kafs_tailmeta_region_hdr_slot_desc_bytes_get(header));
  if (rc != 0)
    return rc;
  uint16_t class_bytes = kafs_tailmeta_container_hdr_class_bytes_get(container);
  uint16_t fragment_off = kafs_ino_taildesc_v5_fragment_off_get(tail);
  if (class_bytes == 0u || fragment_off % class_bytes != 0u)
    return -EPROTO;
  uint16_t slot_index = fragment_off / class_bytes;
  if (slot_index >= kafs_tailmeta_container_hdr_slot_count_get(container))
    return -ERANGE;
  uint32_t slot_table_off = kafs_tailmeta_container_hdr_slot_table_off_get(container);
  uint32_t slot_table_bytes = kafs_tailmeta_container_hdr_slot_table_bytes_get(container);
  uint64_t slot_off =
      (uint64_t)slot_table_off + (uint64_t)slot_index * sizeof(kafs_tailmeta_slot_desc_t);
  if (!kafs_v7_import_range_valid(slot_off, sizeof(kafs_tailmeta_slot_desc_t), region_bytes))
    return -ERANGE;
  const kafs_tailmeta_slot_desc_t *slot = (const kafs_tailmeta_slot_desc_t *)(base + slot_off);
  rc = kafs_tailmeta_inode_desc_matches_slot_for_inode(
      (const kafs_tailmeta_inode_desc_t *)tail, slot, class_bytes, ino,
      kafs_off_stoh(inode->i_size), source->block_size);
  if (rc != 0)
    return rc;
  if ((kafs_ino_taildesc_v5_flags_get(tail) & KAFS_TAILDESC_FLAG_NEEDS_FSCK_REVIEW) != 0u)
    return -EUCLEAN;
  uint64_t payload_off =
      (uint64_t)slot_table_off + slot_table_bytes + (uint64_t)slot_index * class_bytes;
  if (!kafs_v7_import_range_valid(payload_off, class_bytes, region_bytes))
    return -ERANGE;
  *payload_out = base + payload_off;
  *bytes_out = kafs_tailmeta_slot_len_get(slot);
  return 0;
}

static int kafs_v7_import_source_read(const kafs_v7_import_source_t *source, uint32_t ino,
                                      void *buffer, uint64_t bytes, uint64_t offset)
{
  const kafs_sinode_v5_t *inode = source->objects[ino].inode;
  uint64_t size = kafs_off_stoh(inode->i_size);
  if (offset > size || bytes > size - offset)
    return -ERANGE;
  if (bytes == 0u)
    return 0;
  uint8_t kind = kafs_ino_taildesc_v5_layout_kind_get(&inode->i_taildesc);
  if (kind == KAFS_TAIL_LAYOUT_INLINE)
  {
    if (size <= KAFS_INODE_DIRECT_BYTES)
    {
      memcpy(buffer, (const uint8_t *)inode->i_blkreftbl + offset, (size_t)bytes);
      return 0;
    }
    return kafs_v7_import_source_read_full(source, inode, buffer, bytes, offset);
  }
  if (kind == KAFS_TAIL_LAYOUT_FULL_BLOCK)
    return kafs_v7_import_source_read_full(source, inode, buffer, bytes, offset);
  if (kind != KAFS_TAIL_LAYOUT_TAIL_ONLY && kind != KAFS_TAIL_LAYOUT_MIXED_FULL_TAIL)
    return -EPROTO;
  if (!S_ISREG(kafs_mode_stoh(inode->i_mode)))
    return -ENOTSUP;
  const uint8_t *tail_payload = NULL;
  uint16_t tail_bytes = 0u;
  int rc = kafs_v7_import_source_tail_payload(source, ino, inode, &tail_payload, &tail_bytes);
  if (rc != 0)
    return rc;
  if (kind == KAFS_TAIL_LAYOUT_TAIL_ONLY)
  {
    if (offset > tail_bytes || bytes > (uint64_t)tail_bytes - offset)
      return -EPROTO;
    memcpy(buffer, tail_payload + offset, (size_t)bytes);
    return 0;
  }
  uint64_t full_bytes = size - tail_bytes;
  uint64_t copied = 0u;
  if (offset < full_bytes)
  {
    uint64_t prefix = bytes;
    if (prefix > full_bytes - offset)
      prefix = full_bytes - offset;
    rc = kafs_v7_import_source_read_full(source, inode, buffer, prefix, offset);
    if (rc != 0)
      return rc;
    copied = prefix;
  }
  if (copied < bytes)
  {
    uint64_t tail_off = offset + copied - full_bytes;
    if (tail_off > tail_bytes || bytes - copied > (uint64_t)tail_bytes - tail_off)
      return -EPROTO;
    memcpy((uint8_t *)buffer + copied, tail_payload + tail_off, (size_t)(bytes - copied));
  }
  return 0;
}

static uint32_t kafs_v7_import_name_hash(const uint8_t *name, size_t bytes)
{
  uint32_t hash = UINT32_C(2166136261);
  for (size_t i = 0u; i < bytes; ++i)
  {
    hash ^= name[i];
    hash *= UINT32_C(16777619);
  }
  return hash;
}

static int kafs_v7_import_source_add_edge(kafs_v7_import_source_t *source, uint32_t parent,
                                          uint32_t target)
{
  if (source->edge_count == source->edge_capacity)
  {
    size_t next = source->edge_capacity == 0u ? 32u : source->edge_capacity * 2u;
    if (next < source->edge_capacity || next > SIZE_MAX / sizeof(*source->edges))
      return -EOVERFLOW;
    void *grown = realloc(source->edges, next * sizeof(*source->edges));
    if (!grown)
      return -ENOMEM;
    source->edges = (kafs_v7_import_source_edge_t *)grown;
    source->edge_capacity = next;
  }
  source->edges[source->edge_count++] =
      (kafs_v7_import_source_edge_t){.parent = parent, .target = target};
  return 0;
}

static int kafs_v7_import_directory_name_seen(const uint8_t *payload, size_t bytes,
                                              const uint8_t *name, size_t name_bytes)
{
  size_t off = KAFS_V7_KDIR_HEADER_BYTES;
  while (off < bytes)
  {
    const kafs_v7_kdir_record_t *record = (const kafs_v7_kdir_record_t *)(payload + off);
    size_t record_bytes = le16toh(record->record_length);
    size_t current_bytes = le16toh(record->name_bytes);
    if (current_bytes == name_bytes && memcmp(record->name, name, name_bytes) == 0)
      return 1;
    if (record_bytes == 0u || record_bytes > bytes - off)
      return 1;
    off += record_bytes;
  }
  return 0;
}

static int kafs_v7_import_directory_append(uint8_t *payload, size_t capacity, size_t *used,
                                           uint32_t target, const uint8_t *name, size_t name_bytes)
{
  size_t record_bytes = KAFS_V7_KDIR_RECORD_PREFIX_BYTES + name_bytes;
  if (name_bytes == 0u || name_bytes > 255u || record_bytes > UINT16_MAX || *used > capacity ||
      record_bytes > capacity - *used)
    return -ERANGE;
  kafs_v7_kdir_record_t *record = (kafs_v7_kdir_record_t *)(payload + *used);
  memset(record, 0, record_bytes);
  record->record_length = htole16((uint16_t)record_bytes);
  record->inode = htole32(target);
  record->name_bytes = htole16((uint16_t)name_bytes);
  record->name_hash = htole32(kafs_v7_import_name_hash(name, name_bytes));
  memcpy(record->name, name, name_bytes);
  *used += record_bytes;
  return 0;
}

static int kafs_v7_import_validate_name(const uint8_t *name, size_t bytes)
{
  if (bytes == 0u || bytes > 255u)
    return -EINVAL;
  for (size_t i = 0u; i < bytes; ++i)
    if (name[i] == '\0' || name[i] == '/')
      return -EINVAL;
  return 0;
}

static int kafs_v7_import_source_parse_directory(kafs_v7_import_source_t *source, uint32_t ino)
{
  kafs_v7_import_source_object_t *object = &source->objects[ino];
  uint64_t source_bytes = kafs_off_stoh(object->inode->i_size);
  if (source_bytes < sizeof(kafs_sdir_v4_hdr_t) || source_bytes > SIZE_MAX)
    return -EINVAL;
  uint8_t *input = (uint8_t *)malloc((size_t)source_bytes);
  uint8_t *output = (uint8_t *)calloc(1u, (size_t)source_bytes);
  if (!input || !output)
  {
    free(input);
    free(output);
    return -ENOMEM;
  }
  int rc = kafs_v7_import_source_read(source, ino, input, source_bytes, 0u);
  if (rc != 0)
    goto out;
  const kafs_sdir_v4_hdr_t *header = (const kafs_sdir_v4_hdr_t *)input;
  uint32_t record_bytes = kafs_dir_v4_hdr_record_bytes_get(header);
  if (kafs_u32_stoh(header->dh_magic) != KAFS_DIRENT_V4_MAGIC ||
      kafs_dir_v4_hdr_format_get(header) != KAFS_DIRENT_V4_FORMAT_VERSION ||
      kafs_dir_v4_hdr_flags_get(header) != 0u || kafs_u32_stoh(header->dh_reserved0) != 0u ||
      record_bytes != source_bytes - sizeof(*header))
  {
    rc = -EINVAL;
    goto out;
  }
  size_t input_off = sizeof(*header);
  size_t output_used = sizeof(kafs_v7_kdir_header_t);
  uint32_t live_count = 0u;
  uint32_t tombstone_count = 0u;
  while (input_off < source_bytes)
  {
    if (source_bytes - input_off < sizeof(kafs_sdirent_v4_t))
    {
      rc = -EINVAL;
      goto out;
    }
    const kafs_sdirent_v4_t *record = (const kafs_sdirent_v4_t *)(input + input_off);
    size_t length = kafs_dirent_v4_rec_len_get(record);
    size_t name_bytes = kafs_dirent_v4_filenamelen_get(record);
    uint16_t flags = kafs_dirent_v4_flags_get(record);
    uint32_t target = kafs_dirent_v4_ino_get(record);
    if (length != sizeof(*record) + name_bytes || length > source_bytes - input_off ||
        (flags & ~KAFS_DIRENT_FLAG_TOMBSTONE) != 0u || target == 0u ||
        target >= source->inode_count ||
        kafs_v7_import_validate_name((const uint8_t *)record->de_filename, name_bytes) != 0 ||
        kafs_dirent_v4_name_hash_get(record) !=
            kafs_v7_import_name_hash((const uint8_t *)record->de_filename, name_bytes))
    {
      rc = -EINVAL;
      goto out;
    }
    if ((flags & KAFS_DIRENT_FLAG_TOMBSTONE) != 0u)
    {
      tombstone_count++;
      input_off += length;
      continue;
    }
    if (!source->objects[target].allocated ||
        kafs_v7_import_directory_name_seen(output, output_used,
                                           (const uint8_t *)record->de_filename, name_bytes))
    {
      rc = -EINVAL;
      goto out;
    }
    int is_dot = name_bytes == 1u && record->de_filename[0] == '.';
    int is_dotdot =
        name_bytes == 2u && record->de_filename[0] == '.' && record->de_filename[1] == '.';
    if (is_dot || (ino == KAFS_INO_ROOTDIR && is_dotdot))
    {
      rc = -EINVAL;
      goto out;
    }
    if (is_dotdot)
    {
      if (object->dotdot_seen || !S_ISDIR(kafs_mode_stoh(source->objects[target].inode->i_mode)))
      {
        rc = -EINVAL;
        goto out;
      }
      object->dotdot_seen = 1u;
      object->dotdot_parent = target;
    }
    else
    {
      if (source->objects[target].incoming_links == UINT32_MAX)
      {
        rc = -EOVERFLOW;
        goto out;
      }
      source->objects[target].incoming_links++;
      if (S_ISDIR(kafs_mode_stoh(source->objects[target].inode->i_mode)))
      {
        if (source->objects[target].namespace_parent != 0u)
        {
          rc = -EMLINK;
          goto out;
        }
        source->objects[target].namespace_parent = ino;
      }
      rc = kafs_v7_import_source_add_edge(source, ino, target);
      if (rc != 0)
        goto out;
    }
    rc = kafs_v7_import_directory_append(output, (size_t)source_bytes, &output_used, target,
                                         (const uint8_t *)record->de_filename, name_bytes);
    if (rc != 0)
      goto out;
    live_count++;
    input_off += length;
  }
  if (input_off != source_bytes || live_count != kafs_dir_v4_hdr_live_count_get(header) ||
      tombstone_count != kafs_dir_v4_hdr_tombstone_count_get(header) ||
      (ino != KAFS_INO_ROOTDIR && !object->dotdot_seen))
  {
    rc = -EINVAL;
    goto out;
  }
  kafs_v7_kdir_header_t *destination_header = (kafs_v7_kdir_header_t *)output;
  destination_header->magic = htole32(KAFS_V7_KDIR_MAGIC);
  destination_header->version = htole16(KAFS_V7_KDIR_VERSION);
  destination_header->live_count = htole32(live_count);
  destination_header->record_bytes = htole32((uint32_t)(output_used - KAFS_V7_KDIR_HEADER_BYTES));
  object->directory_payload = output;
  object->directory_payload_bytes = output_used;
  output = NULL;
out:
  free(input);
  free(output);
  return rc;
}

static int kafs_v7_import_time_valid(kafs_stime_t value)
{
  uint64_t raw = le64toh(value.value);
  return (raw & UINT32_MAX) < UINT64_C(1000000000);
}

static int kafs_v7_import_source_validate_inode(kafs_v7_import_source_t *source, uint32_t ino)
{
  kafs_v7_import_source_object_t *object = &source->objects[ino];
  const kafs_sinode_v5_t *inode = object->inode;
  uint16_t mode = kafs_mode_stoh(inode->i_mode);
  uint64_t size = kafs_off_stoh(inode->i_size);
  uint8_t kind = kafs_ino_taildesc_v5_layout_kind_get(&inode->i_taildesc);
  if (!S_ISDIR(mode) && !S_ISREG(mode) && !S_ISLNK(mode))
    return -ENOTSUP;
  if (!kafs_v7_import_time_valid(inode->i_atime) || !kafs_v7_import_time_valid(inode->i_ctime) ||
      !kafs_v7_import_time_valid(inode->i_mtime) || !kafs_v7_import_time_valid(inode->i_dtime))
    return -ERANGE;
  if (kafs_linkcnt_stoh(inode->i_linkcnt) == 0u)
    return -EUCLEAN;
  if (!kafs_tail_layout_is_known(kind))
    return -EPROTONOSUPPORT;
  int rc = kind == KAFS_TAIL_LAYOUT_INLINE && size > KAFS_INODE_DIRECT_BYTES
               ? kafs_tailmeta_inode_desc_validate(
                     (const kafs_tailmeta_inode_desc_t *)&inode->i_taildesc, 0u)
               : kafs_tailmeta_inode_desc_validate_for_inode(
                     (const kafs_tailmeta_inode_desc_t *)&inode->i_taildesc, size, 0u,
                     source->block_size);
  if (kafs_tail_layout_uses_tail_storage(kind))
  {
    if (!S_ISREG(mode))
      return -ENOTSUP;
    const uint8_t *tail_payload = NULL;
    uint16_t tail_bytes = 0u;
    rc = kafs_v7_import_source_tail_payload(source, ino, inode, &tail_payload, &tail_bytes);
    (void)tail_payload;
    (void)tail_bytes;
  }
  if (rc != 0)
    return rc;
  uint64_t full_bytes = size;
  if ((kind == KAFS_TAIL_LAYOUT_INLINE && size <= KAFS_INODE_DIRECT_BYTES) ||
      kind == KAFS_TAIL_LAYOUT_TAIL_ONLY)
    full_bytes = 0u;
  else if (kind == KAFS_TAIL_LAYOUT_MIXED_FULL_TAIL)
    full_bytes -= kafs_ino_taildesc_v5_fragment_len_get(&inode->i_taildesc);
  uint64_t full_blocks =
      full_bytes == 0u ? 0u : (full_bytes + source->block_size - 1u) / source->block_size;
  for (uint64_t logical = 0u; logical < full_blocks; ++logical)
  {
    uint32_t block = 0u;
    rc = kafs_v7_import_source_data_block(source, inode, logical, &block);
    if (rc != 0)
      return rc;
    if (block == 0u)
      return -ENOTSUP;
  }
  if (source->payload_bytes > UINT64_MAX - size)
    return -EOVERFLOW;
  source->payload_bytes += size;
  if (S_ISDIR(mode))
    source->directory_count++;
  else if (S_ISREG(mode))
    source->regular_count++;
  else
  {
    uint8_t *payload;
    if (size == 0u || size > SIZE_MAX)
      return -EINVAL;
    payload = (uint8_t *)malloc((size_t)size);
    if (!payload)
      return -ENOMEM;
    rc = kafs_v7_import_source_read(source, ino, payload, size, 0u);
    if (rc == 0 && memchr(payload, '\0', (size_t)size) != NULL)
      rc = -EINVAL;
    free(payload);
    if (rc != 0)
      return rc;
    source->symlink_count++;
  }
  return 0;
}

static int kafs_v7_import_source_reachability(kafs_v7_import_source_t *source)
{
  uint32_t *queue = (uint32_t *)malloc((size_t)source->used_inode_count * sizeof(*queue));
  if (!queue)
    return -ENOMEM;
  size_t head = 0u;
  size_t tail = 0u;
  source->objects[KAFS_INO_ROOTDIR].reachable = 1u;
  queue[tail++] = KAFS_INO_ROOTDIR;
  while (head < tail)
  {
    uint32_t parent = queue[head++];
    for (size_t edge = 0u; edge < source->edge_count; ++edge)
    {
      if (source->edges[edge].parent != parent)
        continue;
      uint32_t target = source->edges[edge].target;
      if (!source->objects[target].reachable)
      {
        source->objects[target].reachable = 1u;
        if (tail >= source->used_inode_count)
        {
          free(queue);
          return -ELOOP;
        }
        queue[tail++] = target;
      }
    }
  }
  free(queue);
  return 0;
}

static int kafs_v7_import_source_validate_namespace(kafs_v7_import_source_t *source)
{
  int rc;
  for (uint32_t ino = 1u; ino < source->inode_count; ++ino)
    if (source->objects[ino].allocated &&
        S_ISDIR(kafs_mode_stoh(source->objects[ino].inode->i_mode)))
    {
      rc = kafs_v7_import_source_parse_directory(source, ino);
      if (rc != 0)
      {
        fprintf(stderr, "v5 source directory inode=%" PRIu32 " is invalid: %s\n", ino,
                strerror(-rc));
        return rc;
      }
    }
  rc = kafs_v7_import_source_reachability(source);
  if (rc != 0)
    return rc;
  for (uint32_t ino = 1u; ino < source->inode_count; ++ino)
  {
    kafs_v7_import_source_object_t *object = &source->objects[ino];
    if (!object->allocated)
      continue;
    uint16_t mode = kafs_mode_stoh(object->inode->i_mode);
    uint32_t links = kafs_linkcnt_stoh(object->inode->i_linkcnt);
    if (!object->reachable || (ino == KAFS_INO_ROOTDIR && object->incoming_links != 0u) ||
        (ino != KAFS_INO_ROOTDIR && object->incoming_links == 0u))
      return -EUCLEAN;
    if (S_ISDIR(mode))
    {
      if ((ino != KAFS_INO_ROOTDIR && object->incoming_links != 1u) ||
          (ino != KAFS_INO_ROOTDIR && object->dotdot_parent != object->namespace_parent))
        return -EMLINK;
    }
    else if (S_ISLNK(mode))
    {
      if (object->incoming_links != 1u || links != 1u)
        return -EMLINK;
    }
    else if (object->incoming_links != links)
      return -EMLINK;
  }
  return 0;
}

static int kafs_v7_import_source_open(const char *path, kafs_v7_import_source_t *source)
{
  memset(source, 0, sizeof(*source));
  source->fd = -1;
  source->fd = open(path, O_RDONLY | O_CLOEXEC | O_NOFOLLOW);
  if (source->fd < 0)
    return -errno;
  if (flock(source->fd, LOCK_SH | LOCK_NB) != 0)
    return -errno;
  if (fstat(source->fd, &source->initial_stat) != 0)
    return -errno;
  if (!S_ISREG(source->initial_stat.st_mode) || source->initial_stat.st_size <= 0 ||
      (uint64_t)source->initial_stat.st_size > SIZE_MAX)
    return -ENOTSUP;
  source->image_bytes = (size_t)source->initial_stat.st_size;
  int rc = kafs_v7_import_crc_fd(source->fd, source->image_bytes, &source->source_crc32);
  if (rc != 0)
    return rc;
  source->image = mmap(NULL, source->image_bytes, PROT_READ, MAP_PRIVATE, source->fd, 0);
  if (source->image == MAP_FAILED)
  {
    source->image = NULL;
    return -errno;
  }
  if (source->image_bytes < sizeof(kafs_ssuperblock_t))
    return -EINVAL;
  source->superblock = (const kafs_ssuperblock_t *)source->image;
  if (kafs_sb_magic_get(source->superblock) != KAFS_MAGIC ||
      kafs_sb_format_version_get(source->superblock) != KAFS_FORMAT_VERSION_V5)
    return -EPROTONOSUPPORT;
  if (kafs_sb_checkpoint_seq_get(source->superblock) != kafs_sb_commit_seq_get(source->superblock))
    return -EUCLEAN;
  source->log_block_size = (uint32_t)kafs_sb_log_blksize_get(source->superblock);
  if (source->log_block_size < 10u || source->log_block_size > 16u)
    return -EINVAL;
  source->block_size = 1u << source->log_block_size;
  source->block_count = (uint32_t)kafs_sb_blkcnt_get(source->superblock);
  source->first_data_block = (uint32_t)kafs_sb_first_data_block_get(source->superblock);
  source->inode_count = (uint32_t)kafs_sb_inocnt_get(source->superblock);
  if (source->block_count == 0u || source->first_data_block >= source->block_count ||
      source->inode_count < 2u)
    return -EINVAL;
  source->bitmap_off = kafs_v7_import_align_up(sizeof(kafs_ssuperblock_t), source->block_size);
  uint64_t bitmap_bytes = ((uint64_t)source->block_count + 7u) / 8u;
  source->inode_table_off = kafs_v7_import_align_up(
      kafs_v7_import_align_up(source->bitmap_off + bitmap_bytes, 8u), source->block_size);
  uint64_t inode_bytes = (uint64_t)source->inode_count * sizeof(kafs_sinode_v5_t);
  if (!kafs_v7_import_range_valid(source->bitmap_off, bitmap_bytes, source->image_bytes) ||
      !kafs_v7_import_range_valid(source->inode_table_off, inode_bytes, source->image_bytes) ||
      source->first_data_block !=
          kafs_v7_import_align_up(kafs_sb_tailmeta_offset_get(source->superblock) +
                                      kafs_sb_tailmeta_size_get(source->superblock),
                                  source->block_size) /
              source->block_size)
    return -EINVAL;
  source->bitmap = source->image + source->bitmap_off;
  source->inode_table = source->image + source->inode_table_off;
  source->objects =
      (kafs_v7_import_source_object_t *)calloc(source->inode_count, sizeof(*source->objects));
  if (!source->objects)
    return -ENOMEM;
  for (uint32_t ino = 0u; ino < source->inode_count; ++ino)
  {
    const kafs_sinode_v5_t *inode =
        (const kafs_sinode_v5_t *)(source->inode_table + (uint64_t)ino * sizeof(*inode));
    source->objects[ino].inode = inode;
    if (kafs_mode_stoh(inode->i_mode) == 0u)
    {
      const uint8_t *bytes = (const uint8_t *)inode;
      for (size_t i = 0u; i < sizeof(*inode); ++i)
        if (bytes[i] != 0u)
          return -EUCLEAN;
      continue;
    }
    if (ino == 0u)
      return -EUCLEAN;
    source->objects[ino].allocated = 1u;
    source->used_inode_count++;
  }
  if (!source->objects[KAFS_INO_ROOTDIR].allocated ||
      !S_ISDIR(kafs_mode_stoh(source->objects[KAFS_INO_ROOTDIR].inode->i_mode)) ||
      source->used_inode_count != source->inode_count - kafs_sb_inocnt_free_get(source->superblock))
    return -EUCLEAN;
  for (uint32_t ino = 1u; ino < source->inode_count; ++ino)
    if (source->objects[ino].allocated)
    {
      rc = kafs_v7_import_source_validate_inode(source, ino);
      if (rc != 0)
      {
        fprintf(stderr, "v5 source inode=%" PRIu32 " is not importable: %s\n", ino, strerror(-rc));
        return rc;
      }
    }
  return kafs_v7_import_source_validate_namespace(source);
}

static void kafs_v7_import_source_close(kafs_v7_import_source_t *source)
{
  if (!source)
    return;
  if (source->objects)
    for (uint32_t ino = 0u; ino < source->inode_count; ++ino)
      free(source->objects[ino].directory_payload);
  free(source->objects);
  free(source->edges);
  if (source->image)
    munmap(source->image, source->image_bytes);
  if (source->fd >= 0)
    close(source->fd);
  memset(source, 0, sizeof(*source));
  source->fd = -1;
}

static int kafs_v7_import_source_unchanged(const kafs_v7_import_source_t *source)
{
  struct stat current;
  uint32_t crc = 0u;
  if (fstat(source->fd, &current) != 0)
    return -errno;
  if (!kafs_v7_import_stat_equal(&source->initial_stat, &current))
    return -ESTALE;
  int rc = kafs_v7_import_crc_fd(source->fd, source->image_bytes, &crc);
  if (rc != 0)
    return rc;
  return crc == source->source_crc32 ? 0 : -ESTALE;
}

static uint64_t kafs_v7_import_index_blocks(uint64_t data_blocks, uint32_t block_size,
                                            int *error_out)
{
  uint64_t refs = block_size / sizeof(uint32_t);
  uint64_t remaining = data_blocks;
  uint64_t index_blocks = 0u;
  *error_out = 0;
  if (remaining <= KAFS_V7_INODE_DIRECT_REFERENCE_COUNT)
    return 0u;
  remaining -= KAFS_V7_INODE_DIRECT_REFERENCE_COUNT;
  uint64_t single_data = remaining < refs ? remaining : refs;
  if (single_data != 0u)
  {
    index_blocks++;
    remaining -= single_data;
  }
  uint64_t double_capacity = refs * refs;
  uint64_t double_data = remaining < double_capacity ? remaining : double_capacity;
  if (double_data != 0u)
  {
    index_blocks += 1u + (double_data + refs - 1u) / refs;
    remaining -= double_data;
  }
  uint64_t triple_capacity = double_capacity * refs;
  if (remaining > triple_capacity)
  {
    *error_out = -EFBIG;
    return 0u;
  }
  if (remaining != 0u)
  {
    uint64_t leaves = (remaining + refs - 1u) / refs;
    uint64_t middles = (leaves + refs - 1u) / refs;
    index_blocks += 1u + leaves + middles;
  }
  return index_blocks;
}

static int kafs_v7_import_required_blocks(const kafs_v7_import_source_t *source,
                                          uint32_t destination_block_size, uint64_t *blocks_out)
{
  uint64_t total = 0u;
  for (uint32_t ino = 1u; ino < source->inode_count; ++ino)
  {
    const kafs_v7_import_source_object_t *object = &source->objects[ino];
    if (!object->allocated)
      continue;
    uint64_t bytes = S_ISDIR(kafs_mode_stoh(object->inode->i_mode))
                         ? object->directory_payload_bytes
                         : kafs_off_stoh(object->inode->i_size);
    if (bytes <= KAFS_INODE_DIRECT_BYTES)
      continue;
    uint64_t data = (bytes + destination_block_size - 1u) / destination_block_size;
    int error = 0;
    uint64_t index = kafs_v7_import_index_blocks(data, destination_block_size, &error);
    if (error != 0)
      return error;
    if (data > UINT64_MAX - index || total > UINT64_MAX - data - index)
      return -EOVERFLOW;
    total += data + index;
  }
  *blocks_out = total;
  return 0;
}

static const kafs_v7_shard_desc_t *kafs_v7_import_group_shard(const kafs_v7_layout_report_t *layout,
                                                              uint32_t group_id, uint16_t type)
{
  const kafs_v7_group_desc_t *groups = kafs_v7_report_groups(layout);
  const kafs_v7_shard_desc_t *shards = kafs_v7_report_shards(layout);
  uint32_t first = le32toh(groups[group_id].first_shard_index);
  uint32_t count = le32toh(groups[group_id].shard_count);
  for (uint32_t index = 0u; index < count; ++index)
    if (le16toh(shards[first + index].type) == type)
      return &shards[first + index];
  return NULL;
}

static int kafs_v7_import_destination_init(kafs_v7_import_destination_t *destination, int fd,
                                           const kafs_v7_layout_report_t *layout)
{
  memset(destination, 0, sizeof(*destination));
  destination->fd = fd;
  destination->layout = layout;
  destination->groups = (kafs_v7_import_destination_group_t *)calloc(layout->group_count,
                                                                     sizeof(*destination->groups));
  if (!destination->groups)
    return -ENOMEM;
  const kafs_v7_group_desc_t *groups = kafs_v7_report_groups(layout);
  for (uint32_t group_id = 0u; group_id < layout->group_count; ++group_id)
  {
    kafs_v7_import_destination_group_t *state = &destination->groups[group_id];
    state->group = &groups[group_id];
    state->bitmap_shard = kafs_v7_import_group_shard(layout, group_id, KAFS_V7_SHARD_BLOCK_BITMAP);
    state->allocator_shard =
        kafs_v7_import_group_shard(layout, group_id, KAFS_V7_SHARD_ALLOCATOR_SUMMARY);
    if (!state->bitmap_shard || !state->allocator_shard)
      return -EINVAL;
    state->bitmap_bytes = le64toh(state->bitmap_shard->physical_bytes);
    state->allocator_bytes = le64toh(state->allocator_shard->physical_bytes);
    if (state->bitmap_bytes > SIZE_MAX || state->allocator_bytes > SIZE_MAX)
      return -EOVERFLOW;
    state->bitmap = (uint8_t *)malloc((size_t)state->bitmap_bytes);
    if (!state->bitmap)
      return -ENOMEM;
    int rc = kafs_pread_all(fd, state->bitmap, (size_t)state->bitmap_bytes,
                            (off_t)le64toh(state->bitmap_shard->physical_off));
    if (rc != 0)
      return rc;
  }
  return 0;
}

static void kafs_v7_import_destination_clear(kafs_v7_import_destination_t *destination)
{
  if (!destination)
    return;
  if (destination->groups && destination->layout)
    for (uint32_t group_id = 0u; group_id < destination->layout->group_count; ++group_id)
      free(destination->groups[group_id].bitmap);
  free(destination->groups);
  memset(destination, 0, sizeof(*destination));
}

static int kafs_v7_import_inode_group(const kafs_v7_layout_report_t *layout, uint32_t ino,
                                      uint32_t *group_out)
{
  for (uint32_t group_id = 0u; group_id < layout->group_count; ++group_id)
  {
    const kafs_v7_shard_desc_t *shard =
        kafs_v7_import_group_shard(layout, group_id, KAFS_V7_SHARD_INODE_TABLE);
    if (!shard)
      return -EINVAL;
    uint64_t start = le64toh(shard->logical_start);
    uint64_t count = le64toh(shard->logical_count);
    if ((uint64_t)ino >= start && (uint64_t)ino - start < count)
    {
      *group_out = group_id;
      return 0;
    }
  }
  return -ERANGE;
}

static int kafs_v7_import_allocate_block(kafs_v7_import_destination_t *destination,
                                         uint32_t preferred_group, uint32_t *logical_out)
{
  for (uint32_t attempt = 0u; attempt < destination->layout->group_count; ++attempt)
  {
    uint32_t group_id = (preferred_group + attempt) % destination->layout->group_count;
    kafs_v7_import_destination_group_t *state = &destination->groups[group_id];
    uint64_t count = le64toh(state->bitmap_shard->logical_count);
    while (state->cursor < count)
    {
      uint64_t local = state->cursor++;
      uint8_t mask = (uint8_t)(1u << (local % 8u));
      if ((state->bitmap[local / 8u] & mask) != 0u)
        continue;
      uint64_t logical = le64toh(state->bitmap_shard->logical_start) + local;
      if (logical >= UINT32_MAX)
        return -EOVERFLOW;
      state->bitmap[local / 8u] |= mask;
      destination->allocated_blocks++;
      *logical_out = (uint32_t)logical;
      return 0;
    }
  }
  return -ENOSPC;
}

static int kafs_v7_import_write_block(kafs_v7_import_destination_t *destination, uint32_t logical,
                                      const void *block)
{
  for (uint32_t group_id = 0u; group_id < destination->layout->group_count; ++group_id)
  {
    const kafs_v7_group_desc_t *group = destination->groups[group_id].group;
    uint64_t start = le64toh(group->data_logical_start);
    uint64_t count = le64toh(group->data_logical_count);
    if ((uint64_t)logical < start || (uint64_t)logical - start >= count)
      continue;
    uint64_t off = le64toh(group->data_physical_off) +
                   ((uint64_t)logical - start) * destination->layout->block_size;
    return kafs_pwrite_all(destination->fd, block, destination->layout->block_size, (off_t)off);
  }
  return -ERANGE;
}

static int kafs_v7_import_write_inode(kafs_v7_import_destination_t *destination, uint32_t ino,
                                      const kafs_v7_inode_t *inode)
{
  uint32_t group_id = 0u;
  int rc = kafs_v7_import_inode_group(destination->layout, ino, &group_id);
  if (rc != 0)
    return rc;
  const kafs_v7_shard_desc_t *shard =
      kafs_v7_import_group_shard(destination->layout, group_id, KAFS_V7_SHARD_INODE_TABLE);
  uint64_t start = le64toh(shard->logical_start);
  uint64_t off = le64toh(shard->physical_off) + ((uint64_t)ino - start) * sizeof(*inode);
  return kafs_pwrite_all(destination->fd, inode, sizeof(*inode), (off_t)off);
}

static int kafs_v7_import_build_reference_layer(kafs_v7_import_destination_t *destination,
                                                uint32_t preferred_group,
                                                const uint32_t *input_refs, uint64_t input_count,
                                                uint32_t *output_refs, uint64_t output_count,
                                                uint8_t *block)
{
  uint64_t refs_per_block = destination->layout->block_size / sizeof(uint32_t);
  uint64_t expected_outputs = input_count == 0u ? 0u : 1u + (input_count - 1u) / refs_per_block;
  if (!input_refs || !output_refs || !block || output_count != expected_outputs)
    return -EINVAL;
  int rc = 0;
  for (uint64_t output = 0u; rc == 0 && output < output_count; ++output)
  {
    memset(block, 0, destination->layout->block_size);
    uint64_t first = output * refs_per_block;
    uint64_t entries = input_count - first;
    if (entries > refs_per_block)
      entries = refs_per_block;
    rc = kafs_v7_import_allocate_block(destination, preferred_group, &output_refs[output]);
    for (uint64_t entry = 0u; rc == 0 && entry < entries; ++entry)
      ((uint32_t *)block)[entry] = htole32(input_refs[first + entry] + 1u);
    if (rc == 0)
      rc = kafs_v7_import_write_block(destination, output_refs[output], block);
  }
  return rc;
}

static int kafs_v7_import_build_indirect(kafs_v7_import_destination_t *destination,
                                         uint32_t preferred_group, const uint32_t *data_blocks,
                                         uint64_t data_count, kafs_v7_inode_t *inode)
{
  uint64_t refs_per_block = destination->layout->block_size / sizeof(uint32_t);
  uint8_t *block = (uint8_t *)calloc(1u, destination->layout->block_size);
  uint32_t *leaf_refs = NULL;
  uint32_t *middle_refs = NULL;
  uint64_t offset = data_count < KAFS_V7_INODE_DIRECT_REFERENCE_COUNT
                        ? data_count
                        : KAFS_V7_INODE_DIRECT_REFERENCE_COUNT;
  int rc = block ? 0 : -ENOMEM;
  if (rc == 0 && data_count > offset)
  {
    uint64_t count = data_count - offset;
    if (count > refs_per_block)
      count = refs_per_block;
    uint32_t single = 0u;
    rc = kafs_v7_import_build_reference_layer(destination, preferred_group, data_blocks + offset,
                                              count, &single, 1u, block);
    if (rc == 0)
    {
      uint32_t reference = htole32(single + 1u);
      memcpy(inode->inline_or_block_refs + 12u * sizeof(reference), &reference, sizeof(reference));
      offset += count;
    }
  }
  if (rc == 0 && data_count > offset)
  {
    uint64_t count = data_count - offset;
    uint64_t capacity = refs_per_block * refs_per_block;
    if (count > capacity)
      count = capacity;
    uint64_t leaves = (count + refs_per_block - 1u) / refs_per_block;
    leaf_refs = (uint32_t *)calloc((size_t)leaves, sizeof(*leaf_refs));
    if (!leaf_refs)
      rc = -ENOMEM;
    if (rc == 0)
      rc = kafs_v7_import_build_reference_layer(destination, preferred_group, data_blocks + offset,
                                                count, leaf_refs, leaves, block);
    uint32_t root = 0u;
    if (rc == 0)
      rc = kafs_v7_import_build_reference_layer(destination, preferred_group, leaf_refs, leaves,
                                                &root, 1u, block);
    if (rc == 0)
    {
      uint32_t reference = htole32(root + 1u);
      memcpy(inode->inline_or_block_refs + 13u * sizeof(reference), &reference, sizeof(reference));
      offset += count;
    }
    free(leaf_refs);
    leaf_refs = NULL;
  }
  if (rc == 0 && data_count > offset)
  {
    uint64_t count = data_count - offset;
    uint64_t leaves = (count + refs_per_block - 1u) / refs_per_block;
    uint64_t middles = (leaves + refs_per_block - 1u) / refs_per_block;
    if (leaves > SIZE_MAX / sizeof(*leaf_refs) || middles > SIZE_MAX / sizeof(*middle_refs))
      rc = -EOVERFLOW;
    if (rc == 0)
    {
      leaf_refs = (uint32_t *)calloc((size_t)leaves, sizeof(*leaf_refs));
      middle_refs = (uint32_t *)calloc((size_t)middles, sizeof(*middle_refs));
      if (!leaf_refs || !middle_refs)
        rc = -ENOMEM;
    }
    if (rc == 0)
      rc = kafs_v7_import_build_reference_layer(destination, preferred_group, data_blocks + offset,
                                                count, leaf_refs, leaves, block);
    if (rc == 0)
      rc = kafs_v7_import_build_reference_layer(destination, preferred_group, leaf_refs, leaves,
                                                middle_refs, middles, block);
    uint32_t root = 0u;
    if (rc == 0)
      rc = kafs_v7_import_build_reference_layer(destination, preferred_group, middle_refs, middles,
                                                &root, 1u, block);
    if (rc == 0)
    {
      uint32_t reference = htole32(root + 1u);
      memcpy(inode->inline_or_block_refs + 14u * sizeof(reference), &reference, sizeof(reference));
      offset += count;
    }
  }
  if (rc == 0 && offset != data_count)
    rc = -EFBIG;
  free(leaf_refs);
  free(middle_refs);
  free(block);
  return rc;
}

static int kafs_v7_import_copy_object(kafs_v7_import_destination_t *destination,
                                      const kafs_v7_import_source_t *source, uint32_t ino)
{
  const kafs_v7_import_source_object_t *object = &source->objects[ino];
  const kafs_sinode_v5_t *source_inode = object->inode;
  uint16_t mode = kafs_mode_stoh(source_inode->i_mode);
  const uint8_t *fixed_payload = S_ISDIR(mode) ? object->directory_payload : NULL;
  uint64_t bytes =
      S_ISDIR(mode) ? object->directory_payload_bytes : kafs_off_stoh(source_inode->i_size);
  kafs_v7_inode_t inode;
  memset(&inode, 0, sizeof(inode));
  inode.mode = source_inode->i_mode.value;
  inode.uid = source_inode->i_uid.value;
  inode.size = htole64(bytes);
  inode.atime = source_inode->i_atime.value;
  inode.ctime = source_inode->i_ctime.value;
  inode.mtime = source_inode->i_mtime.value;
  inode.dtime = source_inode->i_dtime.value;
  inode.gid = source_inode->i_gid.value;
  inode.link_count = source_inode->i_linkcnt.value;
  inode.rdev = source_inode->i_rdev.value;
  if (bytes <= sizeof(inode.inline_or_block_refs))
  {
    if (fixed_payload)
      memcpy(inode.inline_or_block_refs, fixed_payload, (size_t)bytes);
    else
    {
      int rc = kafs_v7_import_source_read(source, ino, inode.inline_or_block_refs, bytes, 0u);
      if (rc != 0)
        return rc;
    }
    return kafs_v7_import_write_inode(destination, ino, &inode);
  }
  uint64_t data_count =
      (bytes + destination->layout->block_size - 1u) / destination->layout->block_size;
  if (data_count > SIZE_MAX / sizeof(uint32_t))
    return -EOVERFLOW;
  uint32_t *data_blocks = (uint32_t *)calloc((size_t)data_count, sizeof(*data_blocks));
  uint8_t *block = (uint8_t *)calloc(1u, destination->layout->block_size);
  if (!data_blocks || !block)
  {
    free(data_blocks);
    free(block);
    return -ENOMEM;
  }
  uint32_t preferred_group = 0u;
  int rc = kafs_v7_import_inode_group(destination->layout, ino, &preferred_group);
  for (uint64_t logical = 0u; rc == 0 && logical < data_count; ++logical)
  {
    memset(block, 0, destination->layout->block_size);
    uint64_t offset = logical * destination->layout->block_size;
    size_t chunk = bytes - offset < destination->layout->block_size
                       ? (size_t)(bytes - offset)
                       : destination->layout->block_size;
    if (fixed_payload)
      memcpy(block, fixed_payload + offset, chunk);
    else
      rc = kafs_v7_import_source_read(source, ino, block, chunk, offset);
    if (rc == 0)
      rc = kafs_v7_import_allocate_block(destination, preferred_group, &data_blocks[logical]);
    if (rc == 0)
      rc = kafs_v7_import_write_block(destination, data_blocks[logical], block);
  }
  for (uint32_t direct = 0u;
       rc == 0 && direct < KAFS_V7_INODE_DIRECT_REFERENCE_COUNT && direct < data_count; ++direct)
  {
    uint32_t reference = htole32(data_blocks[direct] + 1u);
    memcpy(inode.inline_or_block_refs + direct * sizeof(reference), &reference, sizeof(reference));
  }
  uint64_t before_index = destination->allocated_blocks;
  if (rc == 0)
    rc = kafs_v7_import_build_indirect(destination, preferred_group, data_blocks, data_count,
                                       &inode);
  if (rc == 0)
  {
    uint64_t blocks = data_count + destination->allocated_blocks - before_index;
    if (blocks > UINT32_MAX)
      rc = -EOVERFLOW;
    else
      inode.blocks = htole32((uint32_t)blocks);
  }
  if (rc == 0)
    rc = kafs_v7_import_write_inode(destination, ino, &inode);
  free(data_blocks);
  free(block);
  return rc;
}

static int kafs_v7_import_destination_flush_allocators(kafs_v7_import_destination_t *destination)
{
  for (uint32_t group_id = 0u; group_id < destination->layout->group_count; ++group_id)
  {
    kafs_v7_import_destination_group_t *state = &destination->groups[group_id];
    uint64_t blocks = le64toh(state->bitmap_shard->logical_count);
    uint8_t *allocator = (uint8_t *)calloc(1u, (size_t)state->allocator_bytes);
    if (!allocator)
      return -ENOMEM;
    uint64_t l0_bytes = (blocks + 7u) / 8u;
    uint64_t l1_bytes = (l0_bytes + 7u) / 8u;
    for (uint64_t i = 0u; i < l0_bytes; ++i)
    {
      uint8_t valid_mask = 0xffu;
      if (i + 1u == l0_bytes && (blocks & 7u) != 0u)
        valid_mask = (uint8_t)((1u << (blocks & 7u)) - 1u);
      if ((state->bitmap[i] & valid_mask) != valid_mask)
        allocator[i / 8u] |= (uint8_t)(1u << (i % 8u));
    }
    for (uint64_t i = 0u; i < l1_bytes; ++i)
      if (allocator[i] != 0u)
        allocator[l1_bytes + i / 8u] |= (uint8_t)(1u << (i % 8u));
    int rc = kafs_pwrite_all(destination->fd, state->bitmap, (size_t)state->bitmap_bytes,
                             (off_t)le64toh(state->bitmap_shard->physical_off));
    if (rc == 0)
      rc = kafs_pwrite_all(destination->fd, allocator, (size_t)state->allocator_bytes,
                           (off_t)le64toh(state->allocator_shard->physical_off));
    free(allocator);
    if (rc != 0)
      return rc;
  }
  return 0;
}

static int kafs_v7_import_destination_update_checkpoints(kafs_v7_import_destination_t *destination,
                                                         uint64_t free_inodes)
{
  uint64_t free_blocks = destination->layout->free_blocks - destination->allocated_blocks;
  for (uint32_t copy = 0u; copy < destination->layout->replica_count; ++copy)
  {
    kafs_v7_checkpoint_t checkpoint;
    int rc = kafs_pread_all(destination->fd, &checkpoint, sizeof(checkpoint),
                            (off_t)destination->layout->checkpoints[copy].offset);
    if (rc != 0)
      return rc;
    checkpoint.free_blocks = htole64(free_blocks);
    checkpoint.free_inodes = htole64(free_inodes);
    checkpoint.crc32 = 0u;
    checkpoint.crc32 = htole32(kafs_v7_crc32(&checkpoint, sizeof(checkpoint)));
    rc = kafs_pwrite_all(destination->fd, &checkpoint, sizeof(checkpoint),
                         (off_t)destination->layout->checkpoints[copy].offset);
    if (rc != 0)
      return rc;
  }
  return fdatasync(destination->fd) == 0 ? 0 : -errno;
}

static int kafs_v7_import_destination_build(int fd, const kafs_v7_layout_report_t *layout,
                                            const kafs_v7_import_source_t *source,
                                            uint32_t destination_inode_count,
                                            uint64_t *allocated_blocks_out)
{
  kafs_v7_import_destination_t destination;
  int rc = kafs_v7_import_destination_init(&destination, fd, layout);
  if (rc != 0)
  {
    kafs_v7_import_destination_clear(&destination);
    return rc;
  }
  const char *fault = getenv("KAFS_V7_IMPORT_FAIL_AFTER_OBJECTS");
  uint64_t fail_after = fault && *fault ? strtoull(fault, NULL, 10) : 0u;
  uint64_t completed = 0u;
  for (uint32_t ino = 1u; rc == 0 && ino < source->inode_count; ++ino)
  {
    if (!source->objects[ino].allocated)
      continue;
    rc = kafs_v7_import_copy_object(&destination, source, ino);
    if (rc == 0 && fail_after != 0u && ++completed >= fail_after)
      rc = -EINTR;
  }
  if (rc == 0)
    rc = kafs_v7_import_destination_flush_allocators(&destination);
  uint64_t free_inodes = (uint64_t)destination_inode_count - 1u - source->used_inode_count;
  if (rc == 0)
    rc = kafs_v7_import_destination_update_checkpoints(&destination, free_inodes);
  if (rc == 0)
    *allocated_blocks_out = destination.allocated_blocks;
  kafs_v7_import_destination_clear(&destination);
  return rc;
}

static int kafs_v7_import_fsync_parent(const char *path)
{
  char directory[PATH_MAX];
  size_t bytes = strlen(path);
  if (bytes == 0u || bytes >= sizeof(directory))
    return -ENAMETOOLONG;
  memcpy(directory, path, bytes + 1u);
  char *slash = strrchr(directory, '/');
  if (!slash)
    strcpy(directory, ".");
  else if (slash == directory)
    slash[1] = '\0';
  else
    *slash = '\0';
  int fd = open(directory, O_RDONLY | O_DIRECTORY | O_CLOEXEC);
  if (fd < 0)
    return -errno;
  int rc = fsync(fd) == 0 ? 0 : -errno;
  close(fd);
  return rc;
}

static int kafs_v7_import_publish(int fd, const char *work_path, const char *destination_path)
{
  if (fchmod(fd, 0644) != 0)
    return -errno;
  if (fdatasync(fd) != 0)
    return -errno;
  if (link(work_path, destination_path) != 0)
    return -errno;
  int rc = kafs_v7_import_fsync_parent(destination_path);
  if (rc != 0)
  {
    if (unlink(destination_path) != 0)
      fprintf(stderr,
              "v7 import publication sync failed; rollback unlink also failed; inspect final "
              "path %s: %s\n",
              destination_path, strerror(errno));
    else
    {
      int rollback_rc = kafs_v7_import_fsync_parent(destination_path);
      if (rollback_rc != 0)
        fprintf(stderr,
                "v7 import publication sync failed; rollback unlink sync also failed; inspect "
                "final path %s: %s\n",
                destination_path, strerror(-rollback_rc));
    }
    return rc;
  }
  if (unlink(work_path) != 0)
  {
    fprintf(stderr, "v7 import published; warning: retained work-image link %s: %s\n", work_path,
            strerror(errno));
    return 0;
  }
  rc = kafs_v7_import_fsync_parent(destination_path);
  if (rc != 0)
    fprintf(stderr, "v7 import published; warning: work-image unlink sync failed: %s\n",
            strerror(-rc));
  return 0;
}

static int kafs_v7_import_prepare_options(const kafs_v7_import_options_t *input,
                                          const kafs_v7_import_source_t *source,
                                          kafs_v7_mkfs_options_t *mkfs, kafs_v7_mkfs_plan_t *plan,
                                          uint64_t *required_blocks)
{
  uint64_t destination_size = input->destination_size_bytes;
  uint32_t destination_inodes = input->destination_inode_count;
  uint32_t destination_block_size = input->destination_block_size;
  if (destination_size == 0u)
    destination_size = source->image_bytes;
  if (destination_inodes == 0u)
    destination_inodes = source->inode_count;
  if (destination_block_size == 0u)
    destination_block_size = source->block_size;
  if (destination_inodes < source->inode_count)
    return -ENOSPC;
  *mkfs = (kafs_v7_mkfs_options_t){
      .image_size_bytes = destination_size,
      .block_size = destination_block_size,
      .inode_count = destination_inodes,
      .journal_bytes =
          input->journal_bytes != 0u ? input->journal_bytes : KAFS_V7_IMPORT_DEFAULT_JOURNAL_BYTES,
      .hrl_entry_ratio =
          input->hrl_entry_ratio > 0.0 ? input->hrl_entry_ratio : KAFS_V7_IMPORT_DEFAULT_HRL_RATIO,
      .root_uid = (uint16_t)kafs_uid_stoh(source->objects[KAFS_INO_ROOTDIR].inode->i_uid),
      .root_gid = (uint16_t)kafs_gid_stoh(source->objects[KAFS_INO_ROOTDIR].inode->i_gid),
      .group_count = input->group_count,
  };
  int rc = kafs_v7_mkfs_plan(mkfs, plan);
  if (rc != 0)
    return rc;
  rc = kafs_v7_import_required_blocks(source, destination_block_size, required_blocks);
  if (rc != 0)
    return rc;
  return *required_blocks <= plan->data_blocks ? 0 : -ENOSPC;
}

int kafs_v7_import_image(const kafs_v7_import_options_t *options, kafs_v7_import_report_t *report)
{
  if (!options || !options->source_path || !*options->source_path || !options->destination_path ||
      !*options->destination_path || !report)
    return -EINVAL;
  memset(report, 0, sizeof(*report));
  kafs_v7_import_source_t source;
  int rc = kafs_v7_import_source_open(options->source_path, &source);
  if (rc != 0)
  {
    fprintf(stderr, "v5 import preflight failed: %s\n", strerror(-rc));
    kafs_v7_import_source_close(&source);
    return rc;
  }
  kafs_v7_mkfs_options_t mkfs;
  kafs_v7_mkfs_plan_t plan;
  uint64_t required_blocks = 0u;
  rc = kafs_v7_import_prepare_options(options, &source, &mkfs, &plan, &required_blocks);
  if (rc != 0)
  {
    fprintf(stderr, "v7 destination preflight failed: %s\n", strerror(-rc));
    kafs_v7_import_source_close(&source);
    return rc;
  }
  report->source_inode_count = source.inode_count;
  report->imported_inode_count = source.used_inode_count;
  report->imported_directory_count = source.directory_count;
  report->imported_regular_count = source.regular_count;
  report->imported_symlink_count = source.symlink_count;
  report->destination_block_size = mkfs.block_size;
  report->destination_group_count = plan.group_count;
  report->destination_size_bytes = mkfs.image_size_bytes;
  report->payload_bytes = source.payload_bytes;
  report->allocated_blocks = required_blocks;
  report->source_crc32 = source.source_crc32;
  if (options->dry_run)
  {
    kafs_v7_import_source_close(&source);
    return 0;
  }
  struct stat existing;
  if (lstat(options->destination_path, &existing) == 0)
  {
    kafs_v7_import_source_close(&source);
    return -EEXIST;
  }
  if (errno != ENOENT)
  {
    rc = -errno;
    kafs_v7_import_source_close(&source);
    return rc;
  }
  int path_bytes = snprintf(report->work_path, sizeof(report->work_path), "%s.kafs-import-partial",
                            options->destination_path);
  if (path_bytes < 0 || (size_t)path_bytes >= sizeof(report->work_path))
  {
    kafs_v7_import_source_close(&source);
    return -ENAMETOOLONG;
  }
  int fd = open(report->work_path, O_RDWR | O_CREAT | O_EXCL | O_CLOEXEC | O_NOFOLLOW, 0600);
  if (fd < 0)
  {
    rc = -errno;
    kafs_v7_import_source_close(&source);
    return rc;
  }
  if (ftruncate(fd, (off_t)mkfs.image_size_bytes) != 0)
    rc = -errno;
  kafs_v7_layout_report_t layout = {0};
  if (rc == 0)
    rc = kafs_v7_mkfs_fd(fd, &mkfs, &layout);
  uint64_t allocated_blocks = 0u;
  if (rc == 0)
    rc =
        kafs_v7_import_destination_build(fd, &layout, &source, mkfs.inode_count, &allocated_blocks);
  if (rc == 0 && allocated_blocks != required_blocks)
    rc = -EUCLEAN;
  if (rc == 0)
    rc = kafs_v7_import_source_unchanged(&source);
  if (rc == 0)
  {
    kafs_ssuperblock_t superblock;
    rc = kafs_pread_all(fd, &superblock, sizeof(superblock), 0);
    kafs_v7_layout_report_clear(&layout);
    if (rc == 0)
      rc = kafs_v7_validate_image_fd(fd, &superblock, mkfs.image_size_bytes, &layout);
  }
  if (rc == 0)
    rc = kafs_v7_import_publish(fd, report->work_path, options->destination_path);
  if (rc == 0)
    report->allocated_blocks = allocated_blocks;
  else
    fprintf(stderr,
            "v7 import failed before committed publication; inspect final and work paths (%s): "
            "%s\n",
            report->work_path, strerror(-rc));
  kafs_v7_layout_report_clear(&layout);
  close(fd);
  kafs_v7_import_source_close(&source);
  return rc;
}
