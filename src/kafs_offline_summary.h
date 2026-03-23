#pragma once

#include "kafs_inode.h"
#include "kafs_superblock.h"
#include "kafs_tailmeta.h"
#include "kafs_tool_util.h"

#include <errno.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <sys/ioctl.h>
#include <sys/stat.h>
#ifdef __linux__
#include <linux/fs.h>
#endif

struct inode_summary
{
  uint64_t total;
  uint64_t used;
  uint64_t free;
  uint64_t linkcnt_zero_used;
  uint64_t regular_files;
  uint64_t tail_only_regular;
  uint64_t mixed_full_tail_regular;
};

struct tailmeta_class_summary
{
  uint16_t class_bytes;
  uint64_t valid_containers;
  uint64_t invalid_containers;
  uint64_t slots;
  uint64_t live_slots;
  uint64_t live_bytes;
  uint64_t free_bytes;
};

struct tailmeta_summary
{
  int available;
  uint64_t valid_containers;
  uint64_t invalid_containers;
  uint64_t live_slots;
  uint64_t invalid_slots;
  uint64_t live_bytes;
  uint64_t free_bytes;
  uint16_t class_summary_count;
  struct tailmeta_class_summary classes[KAFS_TAILMETA_DEFAULT_CLASS_COUNT];
  kafs_tailmeta_region_hdr_t header;
};

struct sb_geometry
{
  uint64_t blksize;
  uint64_t r_blkcnt;
  uint64_t first_data;
  uint64_t data_blocks;
};

static inline uint64_t kafs_offline_align_up_u64(uint64_t v, uint64_t align)
{
  if (align == 0)
    return v;
  return (v + align - 1u) & ~(align - 1u);
}

static inline int kafs_offline_check_bounds(uint64_t off, uint64_t len, uint64_t file_size)
{
  if (off > file_size)
    return -ERANGE;
  if (len > file_size - off)
    return -ERANGE;
  return 0;
}

static inline int kafs_offline_detect_file_size(int fd, uint64_t *out_file_size)
{
  struct stat st;

  if (!out_file_size)
    return -EINVAL;

  *out_file_size = 0;
  if (fstat(fd, &st) != 0)
    return -errno;

  if (S_ISREG(st.st_mode))
  {
    *out_file_size = (uint64_t)st.st_size;
    return 0;
  }
  if (S_ISBLK(st.st_mode))
  {
#ifdef __linux__
    if (ioctl(fd, BLKGETSIZE64, out_file_size) != 0)
      return -errno;
    return 0;
#else
    return -EOPNOTSUPP;
#endif
  }

  return -EINVAL;
}

static inline struct sb_geometry kafs_offline_superblock_geometry(const kafs_ssuperblock_t *sb)
{
  struct sb_geometry g;

  g.blksize = kafs_sb_blksize_get(sb);
  g.r_blkcnt = kafs_sb_r_blkcnt_get(sb);
  g.first_data = kafs_sb_first_data_block_get(sb);
  g.data_blocks = (g.r_blkcnt > g.first_data) ? (g.r_blkcnt - g.first_data) : 0;
  return g;
}

static inline int kafs_offline_load_inode_table(int fd, const kafs_ssuperblock_t *sb,
                                                uint64_t file_size, void **out_inotbl,
                                                uint64_t *out_inocnt)
{
  const uint64_t inocnt = (uint64_t)kafs_sb_inocnt_get(sb);
  const uint64_t r_blkcnt = (uint64_t)kafs_sb_r_blkcnt_get(sb);
  const uint64_t blksize = (uint64_t)kafs_sb_blksize_get(sb);
  uint64_t layout = sizeof(kafs_ssuperblock_t);
  uint64_t inotbl_off;
  uint64_t inotbl_bytes;
  void *inotbl;
  int rc;

  if (!out_inotbl)
    return -EINVAL;

  *out_inotbl = NULL;
  if (out_inocnt)
    *out_inocnt = 0;

  layout = kafs_offline_align_up_u64(layout, blksize);
  layout += (r_blkcnt + 7u) >> 3;
  layout = kafs_offline_align_up_u64(layout, 8u);
  layout = kafs_offline_align_up_u64(layout, blksize);
  inotbl_off = layout;
  inotbl_bytes = kafs_inode_table_bytes_for_format(kafs_sb_format_version_get(sb), inocnt);
  if (inotbl_bytes == 0)
    return -EINVAL;
  rc = kafs_offline_check_bounds(inotbl_off, inotbl_bytes, file_size);
  if (rc != 0)
    return rc;

  inotbl = malloc((size_t)inotbl_bytes);
  if (!inotbl)
    return -ENOMEM;

  rc = kafs_pread_all(fd, inotbl, (size_t)inotbl_bytes, (off_t)inotbl_off);
  if (rc != 0)
  {
    free(inotbl);
    return rc;
  }

  *out_inotbl = inotbl;
  if (out_inocnt)
    *out_inocnt = inocnt;
  return 0;
}

static inline int collect_inode_summary(int fd, const kafs_ssuperblock_t *sb, uint64_t file_size,
                                        struct inode_summary *out)
{
  const uint32_t format_version = kafs_sb_format_version_get(sb);
  void *inotbl = NULL;
  uint64_t inocnt = 0;
  int rc;

  if (!out)
    return -EINVAL;
  memset(out, 0, sizeof(*out));

  rc = kafs_offline_load_inode_table(fd, sb, file_size, &inotbl, &inocnt);
  if (rc != 0)
    return rc;

  out->total = (inocnt > KAFS_INO_ROOTDIR) ? (inocnt - KAFS_INO_ROOTDIR) : 0;
  for (uint64_t ino = KAFS_INO_ROOTDIR; ino < inocnt; ++ino)
  {
    const kafs_sinode_t *e =
        (const kafs_sinode_t *)kafs_inode_ptr_const_in_table(inotbl, format_version, ino);
    if (!e || !kafs_ino_get_usage(e))
      continue;

    out->used++;
    if (kafs_ino_linkcnt_get(e) == 0)
      out->linkcnt_zero_used++;

    if (!S_ISREG(kafs_ino_mode_get(e)))
      continue;
    out->regular_files++;

    if (format_version == KAFS_FORMAT_VERSION_V5)
    {
      const kafs_sinode_taildesc_v5_t *taildesc = &((const kafs_sinode_v5_t *)e)->i_taildesc;

      switch (kafs_ino_taildesc_v5_layout_kind_get(taildesc))
      {
      case KAFS_TAIL_LAYOUT_TAIL_ONLY:
        out->tail_only_regular++;
        break;
      case KAFS_TAIL_LAYOUT_MIXED_FULL_TAIL:
        out->mixed_full_tail_regular++;
        break;
      default:
        break;
      }
    }
  }

  out->free = (out->total >= out->used) ? (out->total - out->used) : 0;
  free(inotbl);
  return 0;
}

static inline struct tailmeta_class_summary *
kafs_tailmeta_summary_class(struct tailmeta_summary *out, uint16_t class_bytes)
{
  uint16_t index;

  if (!out || class_bytes == 0u)
    return NULL;

  for (index = 0; index < out->class_summary_count; ++index)
  {
    if (out->classes[index].class_bytes == class_bytes)
      return &out->classes[index];
  }
  if (out->class_summary_count >= KAFS_TAILMETA_DEFAULT_CLASS_COUNT)
    return NULL;

  index = out->class_summary_count++;
  memset(&out->classes[index], 0, sizeof(out->classes[index]));
  out->classes[index].class_bytes = class_bytes;
  return &out->classes[index];
}

static inline int collect_tailmeta_summary(int fd, const kafs_ssuperblock_t *sb, uint64_t file_size,
                                           struct tailmeta_summary *out)
{
  uint64_t region_off;
  uint64_t region_size;
  uint32_t container_count;
  uint32_t table_off;
  uint32_t table_bytes;
  kafs_tailmeta_container_hdr_t *containers;
  int rc;

  if (!out)
    return -EINVAL;
  memset(out, 0, sizeof(*out));

  if (!kafs_tailmeta_region_present(sb))
    return 0;

  region_off = kafs_sb_tailmeta_offset_get(sb);
  region_size = kafs_sb_tailmeta_size_get(sb);
  if (region_size < sizeof(out->header))
    return -ERANGE;
  rc = kafs_offline_check_bounds(region_off, sizeof(out->header), file_size);
  if (rc != 0)
    return rc;

  rc = kafs_pread_all(fd, &out->header, sizeof(out->header), (off_t)region_off);
  if (rc != 0)
    return rc;

  out->available = 1;
  rc = kafs_tailmeta_region_hdr_validate(&out->header, region_size);
  if (rc != 0)
    return rc;

  container_count = kafs_tailmeta_region_hdr_container_count_get(&out->header);
  if (container_count == 0)
    return 0;

  table_off = kafs_tailmeta_region_hdr_container_table_off_get(&out->header);
  table_bytes = kafs_tailmeta_region_hdr_container_table_bytes_get(&out->header);
  containers = (kafs_tailmeta_container_hdr_t *)malloc((size_t)table_bytes);
  if (!containers)
    return -ENOMEM;

  rc = kafs_pread_all(fd, containers, (size_t)table_bytes, (off_t)(region_off + table_off));
  if (rc != 0)
  {
    free(containers);
    return rc;
  }

  for (uint32_t index = 0; index < container_count; ++index)
  {
    const kafs_tailmeta_container_hdr_t *container = &containers[index];
    struct tailmeta_class_summary *class_summary =
        kafs_tailmeta_summary_class(out, kafs_tailmeta_container_hdr_class_bytes_get(container));
    uint16_t slot_count;
    uint32_t slot_table_bytes;
    uint32_t slot_table_off;
    uint16_t class_bytes;
    uint64_t live_slots = 0;
    uint64_t live_bytes = 0;
    kafs_tailmeta_slot_desc_t *slots;

    rc = kafs_tailmeta_container_hdr_validate(
        container, region_size, kafs_tailmeta_region_hdr_slot_desc_bytes_get(&out->header));
    if (rc != 0)
    {
      out->invalid_containers++;
      if (class_summary)
        class_summary->invalid_containers++;
      continue;
    }

    out->valid_containers++;
    class_bytes = kafs_tailmeta_container_hdr_class_bytes_get(container);
    slot_count = kafs_tailmeta_container_hdr_slot_count_get(container);
    slot_table_bytes = kafs_tailmeta_container_hdr_slot_table_bytes_get(container);
    slot_table_off = kafs_tailmeta_container_hdr_slot_table_off_get(container);
    out->free_bytes += kafs_tailmeta_container_hdr_free_bytes_get(container);
    if (class_summary)
    {
      class_summary->valid_containers++;
      class_summary->slots += slot_count;
      class_summary->free_bytes += kafs_tailmeta_container_hdr_free_bytes_get(container);
    }
    if (slot_count == 0)
      continue;

    slots = (kafs_tailmeta_slot_desc_t *)malloc((size_t)slot_table_bytes);
    if (!slots)
    {
      free(containers);
      return -ENOMEM;
    }

    rc = kafs_pread_all(fd, slots, (size_t)slot_table_bytes, (off_t)(region_off + slot_table_off));
    if (rc != 0)
    {
      free(slots);
      free(containers);
      return rc;
    }

    for (uint16_t slot_index = 0; slot_index < slot_count; ++slot_index)
    {
      rc = kafs_tailmeta_slot_validate(&slots[slot_index], class_bytes);
      if (rc != 0)
      {
        out->invalid_slots++;
        continue;
      }
      if (kafs_tailmeta_slot_owner_ino_get(&slots[slot_index]) == KAFS_INO_NONE)
        continue;
      live_slots++;
      live_bytes += kafs_tailmeta_slot_len_get(&slots[slot_index]);
    }

    out->live_slots += live_slots;
    out->live_bytes += live_bytes;
    if (class_summary)
    {
      class_summary->live_slots += live_slots;
      class_summary->live_bytes += live_bytes;
    }
    if (live_slots != kafs_tailmeta_container_hdr_live_count_get(container))
    {
      out->invalid_containers++;
      if (class_summary)
        class_summary->invalid_containers++;
    }
    free(slots);
  }

  free(containers);
  return (out->invalid_containers == 0 && out->invalid_slots == 0) ? 0 : -EPROTO;
}