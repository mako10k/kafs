#include "test_utils.h"

#include "kafs_block.h"
#include "kafs_offline_summary.h"
#include "kafs_superblock.h"
#include "kafs_v6_layout.h"

#include <errno.h>
#include <fcntl.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

typedef struct v6_image_info
{
  int fd;
  kafs_ssuperblock_t sb;
  uint64_t file_size;
  uint32_t desc_bytes;
  uint32_t candidate_count;
  uint64_t candidates[KAFS_V6_LAYOUT_REPLICA_MAX_COUNT];
} v6_image_info_t;

static void tlogf(const char *fmt, ...)
{
  va_list ap;
  va_start(ap, fmt);
  vfprintf(stderr, fmt, ap);
  fputc('\n', stderr);
  va_end(ap);
}

static int run_cmd_capture(char *const argv[], int expected_exit, char *out, size_t out_sz)
{
  int pipefd[2];
  if (pipe(pipefd) != 0)
    return -errno;

  pid_t pid = fork();
  if (pid < 0)
  {
    close(pipefd[0]);
    close(pipefd[1]);
    return -errno;
  }
  if (pid == 0)
  {
    close(pipefd[0]);
    dup2(pipefd[1], STDOUT_FILENO);
    dup2(pipefd[1], STDERR_FILENO);
    close(pipefd[1]);
    execvp(argv[0], argv);
    _exit(127);
  }

  close(pipefd[1]);
  size_t used = 0;
  for (;;)
  {
    char buf[512];
    ssize_t n = read(pipefd[0], buf, sizeof(buf));
    if (n < 0)
    {
      if (errno == EINTR)
        continue;
      break;
    }
    if (n == 0)
      break;
    if (out && out_sz > 0 && used < out_sz - 1u)
    {
      size_t copy = (size_t)n;
      if (copy > out_sz - 1u - used)
        copy = out_sz - 1u - used;
      memcpy(out + used, buf, copy);
      used += copy;
    }
  }
  close(pipefd[0]);
  if (out && out_sz > 0)
    out[used] = '\0';

  int st = 0;
  if (waitpid(pid, &st, 0) != pid)
    return -errno;
  if (!WIFEXITED(st))
    return -1;
  return (WEXITSTATUS(st) == expected_exit) ? 0 : -1;
}

static int make_v6_image_size(const char *img, const char *size)
{
  char out[2048];
  char *argv[] = {(char *)kafs_test_mkfs_bin(), (char *)img, (char *)"--format-version",
                  (char *)"6", (char *)"--size-bytes", (char *)size, (char *)"--yes", NULL};
  int rc = run_cmd_capture(argv, 0, out, sizeof(out));
  if (rc != 0)
    tlogf("mkfs failed for %s: %s", img, out);
  return rc;
}

static int make_v6_image(const char *img)
{
  return make_v6_image_size(img, "64M");
}

static int open_v6_image(const char *img, int writable, v6_image_info_t *info)
{
  memset(info, 0, sizeof(*info));
  info->fd = open(img, writable ? O_RDWR : O_RDONLY);
  if (info->fd < 0)
    return -errno;

  int rc = kafs_pread_all(info->fd, &info->sb, sizeof(info->sb), 0);
  if (rc == 0)
    rc = kafs_offline_detect_file_size(info->fd, &info->file_size);
  if (rc == 0 && kafs_sb_format_version_get(&info->sb) != KAFS_FORMAT_VERSION_V6)
    rc = -EINVAL;
  if (rc != 0)
  {
    close(info->fd);
    info->fd = -1;
    return rc;
  }

  kafs_sv6_superblock_anchor_t anchor;
  memcpy(&anchor, info->sb.s_reserved, sizeof(anchor));
  info->desc_bytes = kafs_u32_stoh(anchor.va_primary_desc_bytes);
  uint64_t primary_off = kafs_u64_stoh(anchor.va_primary_desc_off);
  rc = kafs_v6_candidate_offsets(info->file_size, (uint32_t)kafs_sb_blksize_get(&info->sb),
                                 primary_off, info->desc_bytes, info->candidates,
                                 &info->candidate_count);
  if (rc != 0)
  {
    close(info->fd);
    info->fd = -1;
    return rc;
  }
  return 0;
}

static void close_v6_image(v6_image_info_t *info)
{
  if (info->fd >= 0)
    close(info->fd);
  info->fd = -1;
}

static int discover_v6_image(const char *img, kafs_v6_layout_report_t *report)
{
  v6_image_info_t info;
  int rc = open_v6_image(img, 0, &info);
  if (rc != 0)
    return rc;
  rc = kafs_v6_discover_layout(info.fd, &info.sb, info.file_size, report);
  close_v6_image(&info);
  return rc;
}

static int read_selected_descriptor_for_image(const char *img, void **out_desc,
                                              uint32_t *out_bytes,
                                              kafs_ssuperblock_t *out_sb,
                                              uint64_t *out_file_size,
                                              kafs_v6_layout_report_t *out_report)
{
  v6_image_info_t info;
  int rc = open_v6_image(img, 0, &info);
  if (rc != 0)
    return rc;

  kafs_v6_layout_report_t report;
  rc = kafs_v6_discover_layout(info.fd, &info.sb, info.file_size, &report);
  if (rc == 0)
    rc = kafs_v6_read_selected_descriptor(info.fd, &report, out_desc, out_bytes);
  if (rc == 0 && out_sb)
    *out_sb = info.sb;
  if (rc == 0 && out_file_size)
    *out_file_size = info.file_size;
  if (rc == 0 && out_report)
    *out_report = report;

  close_v6_image(&info);
  return rc;
}

static int read_descriptor(const v6_image_info_t *info, uint32_t candidate, void **out)
{
  if (candidate >= info->candidate_count)
    return -EINVAL;
  void *buf = malloc(info->desc_bytes);
  if (!buf)
    return -ENOMEM;
  int rc = kafs_pread_all(info->fd, buf, info->desc_bytes, (off_t)info->candidates[candidate]);
  if (rc != 0)
  {
    free(buf);
    return rc;
  }
  *out = buf;
  return 0;
}

static int write_descriptor(const v6_image_info_t *info, uint32_t candidate, const void *buf)
{
  if (candidate >= info->candidate_count)
    return -EINVAL;
  return kafs_pwrite_all(info->fd, buf, info->desc_bytes, (off_t)info->candidates[candidate]);
}

static void refresh_descriptor_crc(void *buf, uint32_t desc_bytes)
{
  kafs_sv6_layout_desc_header_t *hdr = (kafs_sv6_layout_desc_header_t *)buf;
  hdr->ld_descriptor_crc32 = kafs_u32_htos(kafs_v6_layout_crc_calc(buf, desc_bytes));
}

typedef void (*desc_mutator_t)(void *buf, uint32_t desc_bytes);

static int mutate_descriptor(const char *img, uint32_t candidate, desc_mutator_t mutate,
                             int refresh_crc)
{
  v6_image_info_t info;
  int rc = open_v6_image(img, 1, &info);
  if (rc != 0)
    return rc;

  void *buf = NULL;
  rc = read_descriptor(&info, candidate, &buf);
  if (rc == 0)
  {
    mutate(buf, info.desc_bytes);
    if (refresh_crc)
      refresh_descriptor_crc(buf, info.desc_bytes);
    rc = write_descriptor(&info, candidate, buf);
  }
  free(buf);
  close_v6_image(&info);
  return rc;
}

static int mutate_all_descriptors(const char *img, desc_mutator_t mutate, int refresh_crc)
{
  v6_image_info_t info;
  int rc = open_v6_image(img, 1, &info);
  if (rc != 0)
    return rc;

  void *buf = NULL;
  rc = read_descriptor(&info, 0, &buf);
  if (rc == 0)
  {
    mutate(buf, info.desc_bytes);
    if (refresh_crc)
      refresh_descriptor_crc(buf, info.desc_bytes);
    for (uint32_t i = 0; rc == 0 && i < info.candidate_count; ++i)
      rc = write_descriptor(&info, i, buf);
  }
  free(buf);
  close_v6_image(&info);
  return rc;
}

static void mutate_flip_padding_byte(void *buf, uint32_t desc_bytes)
{
  ((unsigned char *)buf)[desc_bytes - 1u] ^= 0x5au;
}

static void mutate_bad_table_bounds(void *buf, uint32_t desc_bytes)
{
  kafs_sv6_layout_desc_header_t *hdr = (kafs_sv6_layout_desc_header_t *)buf;
  hdr->ld_shard_desc_off = kafs_u32_htos(desc_bytes - 8u);
}

static void mutate_unsupported_version(void *buf, uint32_t desc_bytes)
{
  (void)desc_bytes;
  kafs_sv6_layout_desc_header_t *hdr = (kafs_sv6_layout_desc_header_t *)buf;
  hdr->ld_version = htole16(KAFS_V6_LAYOUT_VERSION + 1u);
}

static void mutate_incompat_flag(void *buf, uint32_t desc_bytes)
{
  (void)desc_bytes;
  kafs_sv6_layout_desc_header_t *hdr = (kafs_sv6_layout_desc_header_t *)buf;
  hdr->ld_incompat_flags = kafs_u64_htos(1u);
}

static void mutate_generation_2(void *buf, uint32_t desc_bytes)
{
  (void)desc_bytes;
  kafs_sv6_layout_desc_header_t *hdr = (kafs_sv6_layout_desc_header_t *)buf;
  hdr->ld_generation = kafs_u64_htos(2u);
}

static void mutate_ro_compat_flag(void *buf, uint32_t desc_bytes)
{
  (void)desc_bytes;
  kafs_sv6_layout_desc_header_t *hdr = (kafs_sv6_layout_desc_header_t *)buf;
  hdr->ld_ro_compat_flags = kafs_u64_htos(1u);
}

static kafs_sv6_shard_desc_t *mutable_shard_table(void *buf, uint32_t desc_bytes,
                                                  uint32_t *out_count)
{
  kafs_sv6_layout_desc_header_t *hdr = (kafs_sv6_layout_desc_header_t *)buf;
  uint32_t shard_count = kafs_u32_stoh(hdr->ld_shard_count);
  uint32_t shard_off = kafs_u32_stoh(hdr->ld_shard_desc_off);

  if (le16toh(hdr->ld_shard_desc_bytes) != KAFS_V6_SHARD_DESC_BYTES)
    return NULL;
  if (kafs_v6_table_bounds(shard_off, shard_count, KAFS_V6_SHARD_DESC_BYTES, desc_bytes) != 0)
    return NULL;
  if (out_count)
    *out_count = shard_count;
  return (kafs_sv6_shard_desc_t *)((char *)buf + shard_off);
}

static void mutate_bitmap_gap(void *buf, uint32_t desc_bytes)
{
  uint32_t shard_count = 0;
  kafs_sv6_shard_desc_t *shards = mutable_shard_table(buf, desc_bytes, &shard_count);
  if (!shards)
    return;

  for (uint32_t i = 0; i < shard_count; ++i)
  {
    if (le16toh(shards[i].sd_type) != KAFS_META_REGION_BLOCK_BITMAP)
      continue;
    uint64_t logical_count = kafs_u64_stoh(shards[i].sd_logical_count);
    if (logical_count > 1u)
      shards[i].sd_logical_count = kafs_u64_htos(logical_count - 1u);
    return;
  }
}

static void mutate_inode_gap(void *buf, uint32_t desc_bytes)
{
  uint32_t shard_count = 0;
  kafs_sv6_shard_desc_t *shards = mutable_shard_table(buf, desc_bytes, &shard_count);
  if (!shards)
    return;

  for (uint32_t i = 0; i < shard_count; ++i)
  {
    if (le16toh(shards[i].sd_type) != KAFS_META_REGION_INODE_TABLE)
      continue;
    uint64_t logical_count = kafs_u64_stoh(shards[i].sd_logical_count);
    if (logical_count > 1u)
      shards[i].sd_logical_count = kafs_u64_htos(logical_count - 1u);
    return;
  }
}

static void mutate_bitmap_duplicate_shard(void *buf, uint32_t desc_bytes)
{
  kafs_sv6_layout_desc_header_t *hdr = (kafs_sv6_layout_desc_header_t *)buf;
  uint32_t old_shard_count = kafs_u32_stoh(hdr->ld_shard_count);
  uint32_t replica_count = kafs_u32_stoh(hdr->ld_replica_count);
  uint32_t group_count = kafs_u32_stoh(hdr->ld_group_count);
  uint32_t group_off = kafs_u32_stoh(hdr->ld_group_desc_off);
  uint32_t shard_off = kafs_u32_stoh(hdr->ld_shard_desc_off);
  uint32_t old_replica_off = kafs_u32_stoh(hdr->ld_replica_desc_off);
  uint32_t new_shard_count = old_shard_count + 1u;
  uint32_t new_replica_off = shard_off + new_shard_count * KAFS_V6_SHARD_DESC_BYTES;
  uint64_t replica_bytes = (uint64_t)replica_count * KAFS_V6_REPLICA_DESC_BYTES;

  if (new_shard_count == 0u || group_count == 0u)
    return;
  if (kafs_v6_table_bounds(group_off, group_count, KAFS_V6_GROUP_DESC_BYTES, desc_bytes) != 0 ||
      kafs_v6_table_bounds(shard_off, new_shard_count, KAFS_V6_SHARD_DESC_BYTES, desc_bytes) != 0 ||
      kafs_v6_table_bounds(new_replica_off, replica_count, KAFS_V6_REPLICA_DESC_BYTES,
                           desc_bytes) != 0 ||
      old_replica_off > desc_bytes || replica_bytes > (uint64_t)desc_bytes - old_replica_off)
    return;

  kafs_sv6_shard_desc_t *shards = (kafs_sv6_shard_desc_t *)((char *)buf + shard_off);
  uint32_t bitmap_index = UINT32_MAX;
  for (uint32_t i = 0; i < old_shard_count; ++i)
  {
    if (le16toh(shards[i].sd_type) == KAFS_META_REGION_BLOCK_BITMAP)
    {
      bitmap_index = i;
      break;
    }
  }
  if (bitmap_index == UINT32_MAX)
    return;

  memmove((char *)buf + new_replica_off, (char *)buf + old_replica_off, (size_t)replica_bytes);
  shards[old_shard_count] = shards[bitmap_index];
  hdr->ld_shard_count = kafs_u32_htos(new_shard_count);
  hdr->ld_replica_desc_off = kafs_u32_htos(new_replica_off);

  kafs_sv6_group_desc_t *groups = (kafs_sv6_group_desc_t *)((char *)buf + group_off);
  groups[0].gd_shard_count = kafs_u32_htos(kafs_u32_stoh(groups[0].gd_shard_count) + 1u);
}

static void mutate_bitmap_split_two_shards(void *buf, uint32_t desc_bytes)
{
  kafs_sv6_layout_desc_header_t *hdr = (kafs_sv6_layout_desc_header_t *)buf;
  uint32_t old_shard_count = kafs_u32_stoh(hdr->ld_shard_count);
  uint32_t replica_count = kafs_u32_stoh(hdr->ld_replica_count);
  uint32_t group_count = kafs_u32_stoh(hdr->ld_group_count);
  uint32_t group_off = kafs_u32_stoh(hdr->ld_group_desc_off);
  uint32_t shard_off = kafs_u32_stoh(hdr->ld_shard_desc_off);
  uint32_t old_replica_off = kafs_u32_stoh(hdr->ld_replica_desc_off);
  uint32_t block_size = kafs_u32_stoh(hdr->ld_block_size);
  uint32_t new_shard_count = old_shard_count + 1u;
  uint32_t new_replica_off = shard_off + new_shard_count * KAFS_V6_SHARD_DESC_BYTES;
  uint64_t replica_bytes = (uint64_t)replica_count * KAFS_V6_REPLICA_DESC_BYTES;

  if (new_shard_count == 0u || group_count == 0u || block_size == 0u)
    return;
  if (kafs_v6_table_bounds(group_off, group_count, KAFS_V6_GROUP_DESC_BYTES, desc_bytes) != 0 ||
      kafs_v6_table_bounds(shard_off, new_shard_count, KAFS_V6_SHARD_DESC_BYTES, desc_bytes) != 0 ||
      kafs_v6_table_bounds(new_replica_off, replica_count, KAFS_V6_REPLICA_DESC_BYTES,
                           desc_bytes) != 0 ||
      old_replica_off > desc_bytes || replica_bytes > (uint64_t)desc_bytes - old_replica_off)
    return;

  kafs_sv6_shard_desc_t *shards = (kafs_sv6_shard_desc_t *)((char *)buf + shard_off);
  uint32_t bitmap_index = UINT32_MAX;
  for (uint32_t i = 0; i < old_shard_count; ++i)
  {
    if (le16toh(shards[i].sd_type) == KAFS_META_REGION_BLOCK_BITMAP)
    {
      bitmap_index = i;
      break;
    }
  }
  if (bitmap_index == UINT32_MAX)
    return;

  kafs_sv6_shard_desc_t bitmap = shards[bitmap_index];
  uint64_t logical_start = kafs_u64_stoh(bitmap.sd_logical_start);
  uint64_t logical_count = kafs_u64_stoh(bitmap.sd_logical_count);
  uint64_t physical_off = kafs_u64_stoh(bitmap.sd_physical_off);
  uint64_t physical_bytes = kafs_u64_stoh(bitmap.sd_physical_bytes);
  uint64_t first_bytes = (physical_bytes / 2u / block_size) * block_size;
  if (first_bytes == 0u || first_bytes >= physical_bytes)
    return;
  if (physical_off > UINT64_MAX - first_bytes)
    return;

  uint64_t first_count = first_bytes * 8u;
  if (first_count == 0u || first_count >= logical_count)
    return;
  if (logical_start > UINT64_MAX - first_count)
    return;
  uint64_t second_count = logical_count - first_count;
  uint64_t second_bytes = physical_bytes - first_bytes;
  if (((second_count + 7u) >> 3) > second_bytes)
    return;

  memmove((char *)buf + new_replica_off, (char *)buf + old_replica_off, (size_t)replica_bytes);
  shards[bitmap_index].sd_logical_count = kafs_u64_htos(first_count);
  shards[bitmap_index].sd_physical_bytes = kafs_u64_htos(first_bytes);
  shards[old_shard_count] = bitmap;
  shards[old_shard_count].sd_logical_start = kafs_u64_htos(logical_start + first_count);
  shards[old_shard_count].sd_logical_count = kafs_u64_htos(second_count);
  shards[old_shard_count].sd_physical_off = kafs_u64_htos(physical_off + first_bytes);
  shards[old_shard_count].sd_physical_bytes = kafs_u64_htos(second_bytes);
  hdr->ld_shard_count = kafs_u32_htos(new_shard_count);
  hdr->ld_replica_desc_off = kafs_u32_htos(new_replica_off);

  kafs_sv6_group_desc_t *groups = (kafs_sv6_group_desc_t *)((char *)buf + group_off);
  groups[0].gd_shard_count = kafs_u32_htos(kafs_u32_stoh(groups[0].gd_shard_count) + 1u);
}

static void mutate_inode_split_two_shards(void *buf, uint32_t desc_bytes)
{
  kafs_sv6_layout_desc_header_t *hdr = (kafs_sv6_layout_desc_header_t *)buf;
  uint32_t old_shard_count = kafs_u32_stoh(hdr->ld_shard_count);
  uint32_t replica_count = kafs_u32_stoh(hdr->ld_replica_count);
  uint32_t group_count = kafs_u32_stoh(hdr->ld_group_count);
  uint32_t group_off = kafs_u32_stoh(hdr->ld_group_desc_off);
  uint32_t shard_off = kafs_u32_stoh(hdr->ld_shard_desc_off);
  uint32_t old_replica_off = kafs_u32_stoh(hdr->ld_replica_desc_off);
  uint32_t block_size = kafs_u32_stoh(hdr->ld_block_size);
  uint32_t new_shard_count = old_shard_count + 1u;
  uint32_t new_replica_off = shard_off + new_shard_count * KAFS_V6_SHARD_DESC_BYTES;
  uint64_t replica_bytes = (uint64_t)replica_count * KAFS_V6_REPLICA_DESC_BYTES;

  if (new_shard_count == 0u || group_count == 0u || block_size == 0u)
    return;
  if (kafs_v6_table_bounds(group_off, group_count, KAFS_V6_GROUP_DESC_BYTES, desc_bytes) != 0 ||
      kafs_v6_table_bounds(shard_off, new_shard_count, KAFS_V6_SHARD_DESC_BYTES, desc_bytes) != 0 ||
      kafs_v6_table_bounds(new_replica_off, replica_count, KAFS_V6_REPLICA_DESC_BYTES,
                           desc_bytes) != 0 ||
      old_replica_off > desc_bytes || replica_bytes > (uint64_t)desc_bytes - old_replica_off)
    return;

  kafs_sv6_shard_desc_t *shards = (kafs_sv6_shard_desc_t *)((char *)buf + shard_off);
  uint32_t inode_index = UINT32_MAX;
  for (uint32_t i = 0; i < old_shard_count; ++i)
  {
    if (le16toh(shards[i].sd_type) == KAFS_META_REGION_INODE_TABLE)
    {
      inode_index = i;
      break;
    }
  }
  if (inode_index == UINT32_MAX)
    return;

  kafs_sv6_shard_desc_t inode = shards[inode_index];
  uint64_t logical_start = kafs_u64_stoh(inode.sd_logical_start);
  uint64_t logical_count = kafs_u64_stoh(inode.sd_logical_count);
  uint64_t physical_off = kafs_u64_stoh(inode.sd_physical_off);
  uint64_t physical_bytes = kafs_u64_stoh(inode.sd_physical_bytes);
  uint32_t record_bytes = kafs_u32_stoh(inode.sd_record_bytes);
  if (record_bytes == 0u || block_size < record_bytes || (block_size % record_bytes) != 0u)
    return;

  uint64_t records_per_block = block_size / record_bytes;
  uint64_t first_count = ((logical_count / 2u) / records_per_block) * records_per_block;
  if (first_count == 0u || first_count >= logical_count ||
      first_count > UINT64_MAX / (uint64_t)record_bytes)
    return;
  uint64_t first_bytes = first_count * (uint64_t)record_bytes;
  if (first_bytes == 0u || first_bytes >= physical_bytes || physical_off > UINT64_MAX - first_bytes)
    return;
  if (logical_start > UINT64_MAX - first_count)
    return;

  uint64_t second_count = logical_count - first_count;
  uint64_t second_bytes = physical_bytes - first_bytes;
  if (second_count == 0u || second_count > UINT64_MAX / (uint64_t)record_bytes ||
      second_count * (uint64_t)record_bytes > second_bytes)
    return;

  memmove((char *)buf + new_replica_off, (char *)buf + old_replica_off, (size_t)replica_bytes);
  shards[inode_index].sd_logical_count = kafs_u64_htos(first_count);
  shards[inode_index].sd_physical_bytes = kafs_u64_htos(first_bytes);
  shards[old_shard_count] = inode;
  shards[old_shard_count].sd_logical_start = kafs_u64_htos(logical_start + first_count);
  shards[old_shard_count].sd_logical_count = kafs_u64_htos(second_count);
  shards[old_shard_count].sd_physical_off = kafs_u64_htos(physical_off + first_bytes);
  shards[old_shard_count].sd_physical_bytes = kafs_u64_htos(second_bytes);
  hdr->ld_shard_count = kafs_u32_htos(new_shard_count);
  hdr->ld_replica_desc_off = kafs_u32_htos(new_replica_off);

  kafs_sv6_group_desc_t *groups = (kafs_sv6_group_desc_t *)((char *)buf + group_off);
  groups[0].gd_shard_count = kafs_u32_htos(kafs_u32_stoh(groups[0].gd_shard_count) + 1u);
}

static int find_mutable_shard_index(kafs_sv6_shard_desc_t *shards, uint32_t shard_count,
                                    uint16_t shard_type, uint32_t *out_index)
{
  if (!shards || !out_index)
    return -EINVAL;
  for (uint32_t i = 0; i < shard_count; ++i)
  {
    if (le16toh(shards[i].sd_type) == shard_type)
    {
      *out_index = i;
      return 0;
    }
  }
  return -ENOENT;
}

static kafs_sv6_shard_desc_t *append_mutable_shard_slot(void *buf, uint32_t desc_bytes,
                                                        uint32_t *out_old_shard_count)
{
  kafs_sv6_layout_desc_header_t *hdr = (kafs_sv6_layout_desc_header_t *)buf;
  uint32_t old_shard_count = kafs_u32_stoh(hdr->ld_shard_count);
  uint32_t replica_count = kafs_u32_stoh(hdr->ld_replica_count);
  uint32_t group_count = kafs_u32_stoh(hdr->ld_group_count);
  uint32_t group_off = kafs_u32_stoh(hdr->ld_group_desc_off);
  uint32_t shard_off = kafs_u32_stoh(hdr->ld_shard_desc_off);
  uint32_t old_replica_off = kafs_u32_stoh(hdr->ld_replica_desc_off);
  uint32_t new_shard_count = old_shard_count + 1u;
  uint32_t new_replica_off = shard_off + new_shard_count * KAFS_V6_SHARD_DESC_BYTES;
  uint64_t replica_bytes = (uint64_t)replica_count * KAFS_V6_REPLICA_DESC_BYTES;

  if (new_shard_count == 0u || group_count == 0u)
    return NULL;
  if (kafs_v6_table_bounds(group_off, group_count, KAFS_V6_GROUP_DESC_BYTES, desc_bytes) != 0 ||
      kafs_v6_table_bounds(shard_off, new_shard_count, KAFS_V6_SHARD_DESC_BYTES, desc_bytes) != 0 ||
      kafs_v6_table_bounds(new_replica_off, replica_count, KAFS_V6_REPLICA_DESC_BYTES,
                           desc_bytes) != 0 ||
      old_replica_off > desc_bytes || replica_bytes > (uint64_t)desc_bytes - old_replica_off)
    return NULL;

  kafs_sv6_shard_desc_t *shards = (kafs_sv6_shard_desc_t *)((char *)buf + shard_off);
  memmove((char *)buf + new_replica_off, (char *)buf + old_replica_off, (size_t)replica_bytes);
  hdr->ld_shard_count = kafs_u32_htos(new_shard_count);
  hdr->ld_replica_desc_off = kafs_u32_htos(new_replica_off);

  kafs_sv6_group_desc_t *groups = (kafs_sv6_group_desc_t *)((char *)buf + group_off);
  groups[0].gd_shard_count = kafs_u32_htos(kafs_u32_stoh(groups[0].gd_shard_count) + 1u);
  if (out_old_shard_count)
    *out_old_shard_count = old_shard_count;
  return shards;
}

static void mutate_allocator_gap(void *buf, uint32_t desc_bytes)
{
  uint32_t shard_count = 0;
  kafs_sv6_shard_desc_t *shards = mutable_shard_table(buf, desc_bytes, &shard_count);
  if (!shards)
    return;

  uint32_t index = UINT32_MAX;
  if (find_mutable_shard_index(shards, shard_count, KAFS_META_REGION_ALLOCATOR_SUMMARY, &index) !=
      0)
    return;
  uint64_t logical_count = kafs_u64_stoh(shards[index].sd_logical_count);
  if (logical_count > 1u)
    shards[index].sd_logical_count = kafs_u64_htos(logical_count - 1u);
}

static void mutate_allocator_split_two_shards(void *buf, uint32_t desc_bytes)
{
  uint32_t old_shard_count = 0;
  kafs_sv6_shard_desc_t *shards = mutable_shard_table(buf, desc_bytes, &old_shard_count);
  if (!shards)
    return;

  uint32_t allocator_index = UINT32_MAX;
  if (find_mutable_shard_index(shards, old_shard_count, KAFS_META_REGION_ALLOCATOR_SUMMARY,
                               &allocator_index) != 0)
    return;

  kafs_sv6_shard_desc_t allocator = shards[allocator_index];
  uint64_t logical_start = kafs_u64_stoh(allocator.sd_logical_start);
  uint64_t logical_count = kafs_u64_stoh(allocator.sd_logical_count);
  uint64_t physical_off = kafs_u64_stoh(allocator.sd_physical_off);
  uint64_t physical_bytes = kafs_u64_stoh(allocator.sd_physical_bytes);
  uint32_t block_size = kafs_u32_stoh(((kafs_sv6_layout_desc_header_t *)buf)->ld_block_size);
  if (block_size == 0u)
    return;

  uint64_t first_count = ((logical_count / 2u) / 8u) * 8u;
  if (first_count == 0u || first_count >= logical_count ||
      logical_start > UINT64_MAX - first_count)
    return;
  uint64_t second_count = logical_count - first_count;
  uint64_t l0_bytes = 0;
  uint64_t l1_bytes = 0;
  uint64_t l2_bytes = 0;
  uint64_t first_summary_bytes = 0;
  uint64_t second_summary_bytes = 0;
  if (kafs_v6_allocator_summary_shape(first_count, &l0_bytes, &l1_bytes, &l2_bytes,
                                      &first_summary_bytes) != 0 ||
      kafs_v6_allocator_summary_shape(second_count, &l0_bytes, &l1_bytes, &l2_bytes,
                                      &second_summary_bytes) != 0)
    return;
  if (first_summary_bytes > physical_bytes || second_summary_bytes > physical_bytes ||
      first_summary_bytes > UINT64_MAX - second_summary_bytes ||
      first_summary_bytes + second_summary_bytes > physical_bytes)
    return;

  uint64_t first_physical_bytes = (physical_bytes / 2u / block_size) * block_size;
  if (first_physical_bytes < first_summary_bytes)
    first_physical_bytes = kafs_offline_align_up_u64(first_summary_bytes, block_size);
  if (physical_bytes - first_physical_bytes < second_summary_bytes)
    first_physical_bytes =
        physical_bytes - kafs_offline_align_up_u64(second_summary_bytes, block_size);
  if (first_physical_bytes == 0u || first_physical_bytes >= physical_bytes ||
      (first_physical_bytes % block_size) != 0u ||
      ((physical_bytes - first_physical_bytes) % block_size) != 0u ||
      physical_off > UINT64_MAX - first_physical_bytes)
    return;

  uint32_t appended_old_shard_count = 0;
  shards = append_mutable_shard_slot(buf, desc_bytes, &appended_old_shard_count);
  if (!shards || appended_old_shard_count != old_shard_count)
    return;

  shards[allocator_index].sd_logical_count = kafs_u64_htos(first_count);
  shards[allocator_index].sd_physical_bytes = kafs_u64_htos(first_physical_bytes);
  shards[old_shard_count] = allocator;
  shards[old_shard_count].sd_logical_start = kafs_u64_htos(logical_start + first_count);
  shards[old_shard_count].sd_logical_count = kafs_u64_htos(second_count);
  shards[old_shard_count].sd_physical_off = kafs_u64_htos(physical_off + first_physical_bytes);
  shards[old_shard_count].sd_physical_bytes = kafs_u64_htos(physical_bytes - first_physical_bytes);
}

static uint64_t test_gcd_u64(uint64_t a, uint64_t b)
{
  while (b != 0u)
  {
    uint64_t r = a % b;
    a = b;
    b = r;
  }
  return a;
}

static void mutate_fixed_record_gap(void *buf, uint32_t desc_bytes, uint16_t shard_type)
{
  uint32_t shard_count = 0;
  kafs_sv6_shard_desc_t *shards = mutable_shard_table(buf, desc_bytes, &shard_count);
  if (!shards)
    return;

  uint32_t index = UINT32_MAX;
  if (find_mutable_shard_index(shards, shard_count, shard_type, &index) != 0)
    return;
  uint64_t logical_count = kafs_u64_stoh(shards[index].sd_logical_count);
  if (logical_count > 1u)
    shards[index].sd_logical_count = kafs_u64_htos(logical_count - 1u);
}

static void mutate_fixed_record_split_two_shards(void *buf, uint32_t desc_bytes,
                                                 uint16_t shard_type)
{
  uint32_t old_shard_count = 0;
  kafs_sv6_shard_desc_t *shards = mutable_shard_table(buf, desc_bytes, &old_shard_count);
  if (!shards)
    return;

  uint32_t shard_index = UINT32_MAX;
  if (find_mutable_shard_index(shards, old_shard_count, shard_type, &shard_index) != 0)
    return;

  kafs_sv6_shard_desc_t original = shards[shard_index];
  uint64_t logical_start = kafs_u64_stoh(original.sd_logical_start);
  uint64_t logical_count = kafs_u64_stoh(original.sd_logical_count);
  uint64_t physical_off = kafs_u64_stoh(original.sd_physical_off);
  uint64_t physical_bytes = kafs_u64_stoh(original.sd_physical_bytes);
  uint32_t record_bytes = kafs_u32_stoh(original.sd_record_bytes);
  uint32_t block_size = kafs_u32_stoh(((kafs_sv6_layout_desc_header_t *)buf)->ld_block_size);
  if (record_bytes == 0u || block_size == 0u)
    return;

  uint64_t gcd = test_gcd_u64((uint64_t)block_size, (uint64_t)record_bytes);
  uint64_t records_per_aligned_span = (uint64_t)block_size / gcd;
  uint64_t first_count = ((logical_count / 2u) / records_per_aligned_span) *
                         records_per_aligned_span;
  if (first_count == 0u || first_count >= logical_count ||
      first_count > UINT64_MAX / (uint64_t)record_bytes ||
      logical_start > UINT64_MAX - first_count)
    return;

  uint64_t first_bytes = first_count * (uint64_t)record_bytes;
  if (first_bytes == 0u || first_bytes >= physical_bytes ||
      (first_bytes % block_size) != 0u || physical_off > UINT64_MAX - first_bytes)
    return;
  uint64_t second_count = logical_count - first_count;
  if (second_count > UINT64_MAX / (uint64_t)record_bytes)
    return;
  uint64_t second_required_bytes = second_count * (uint64_t)record_bytes;
  uint64_t second_physical_bytes = physical_bytes - first_bytes;
  if (second_count == 0u || second_physical_bytes == 0u ||
      (second_physical_bytes % block_size) != 0u || second_required_bytes > second_physical_bytes)
    return;

  uint32_t appended_old_shard_count = 0;
  shards = append_mutable_shard_slot(buf, desc_bytes, &appended_old_shard_count);
  if (!shards || appended_old_shard_count != old_shard_count)
    return;

  shards[shard_index].sd_logical_count = kafs_u64_htos(first_count);
  shards[shard_index].sd_physical_bytes = kafs_u64_htos(first_bytes);
  shards[old_shard_count] = original;
  shards[old_shard_count].sd_logical_start = kafs_u64_htos(logical_start + first_count);
  shards[old_shard_count].sd_logical_count = kafs_u64_htos(second_count);
  shards[old_shard_count].sd_physical_off = kafs_u64_htos(physical_off + first_bytes);
  shards[old_shard_count].sd_physical_bytes = kafs_u64_htos(second_physical_bytes);
}

static void mutate_hrl_index_gap(void *buf, uint32_t desc_bytes)
{
  mutate_fixed_record_gap(buf, desc_bytes, KAFS_META_REGION_HRL_INDEX);
}

static void mutate_hrl_entries_gap(void *buf, uint32_t desc_bytes)
{
  mutate_fixed_record_gap(buf, desc_bytes, KAFS_META_REGION_HRL_ENTRIES);
}

static void mutate_hrl_index_split_two_shards(void *buf, uint32_t desc_bytes)
{
  mutate_fixed_record_split_two_shards(buf, desc_bytes, KAFS_META_REGION_HRL_INDEX);
}

static void mutate_hrl_entries_split_two_shards(void *buf, uint32_t desc_bytes)
{
  mutate_fixed_record_split_two_shards(buf, desc_bytes, KAFS_META_REGION_HRL_ENTRIES);
}

static void mutate_journal_header_gap(void *buf, uint32_t desc_bytes)
{
  uint32_t shard_count = 0;
  kafs_sv6_shard_desc_t *shards = mutable_shard_table(buf, desc_bytes, &shard_count);
  if (!shards)
    return;

  uint32_t index = UINT32_MAX;
  if (find_mutable_shard_index(shards, shard_count, KAFS_META_REGION_JOURNAL_HEADER, &index) != 0)
    return;
  shards[index].sd_logical_start = kafs_u64_htos(1);
}

static void mutate_journal_split_two_segments(void *buf, uint32_t desc_bytes)
{
  uint32_t old_shard_count = 0;
  kafs_sv6_shard_desc_t *shards = mutable_shard_table(buf, desc_bytes, &old_shard_count);
  if (!shards)
    return;

  uint32_t header_index = UINT32_MAX;
  uint32_t data_index = UINT32_MAX;
  if (find_mutable_shard_index(shards, old_shard_count, KAFS_META_REGION_JOURNAL_HEADER,
                               &header_index) != 0 ||
      find_mutable_shard_index(shards, old_shard_count, KAFS_META_REGION_JOURNAL_DATA, &data_index) !=
          0)
    return;

  uint32_t block_size = kafs_u32_stoh(((kafs_sv6_layout_desc_header_t *)buf)->ld_block_size);
  if (block_size == 0u)
    return;

  kafs_sv6_shard_desc_t header0 = shards[header_index];
  kafs_sv6_shard_desc_t data0 = shards[data_index];
  uint64_t data_off = kafs_u64_stoh(data0.sd_physical_off);
  uint64_t data_bytes = kafs_u64_stoh(data0.sd_physical_bytes);
  if (data_bytes <= (uint64_t)block_size * 3u || data_off > UINT64_MAX - block_size)
    return;

  uint64_t remaining_data = data_bytes - block_size;
  uint64_t first_data_bytes = (remaining_data / 2u / block_size) * block_size;
  if (first_data_bytes == 0u || first_data_bytes >= remaining_data)
    return;
  uint64_t second_data_bytes = remaining_data - first_data_bytes;
  if ((second_data_bytes % block_size) != 0u)
    return;

  uint32_t appended_old_shard_count = 0;
  shards = append_mutable_shard_slot(buf, desc_bytes, &appended_old_shard_count);
  if (!shards || appended_old_shard_count != old_shard_count)
    return;
  shards = append_mutable_shard_slot(buf, desc_bytes, &appended_old_shard_count);
  if (!shards || appended_old_shard_count != old_shard_count + 1u)
    return;

  shards[header_index].sd_logical_start = kafs_u64_htos(0);
  shards[header_index].sd_logical_count = kafs_u64_htos(1);
  shards[header_index].sd_physical_bytes = kafs_u64_htos(block_size);

  shards[data_index].sd_logical_start = kafs_u64_htos(0);
  shards[data_index].sd_logical_count = kafs_u64_htos(1);
  shards[data_index].sd_physical_off = kafs_u64_htos(data_off + block_size);
  shards[data_index].sd_physical_bytes = kafs_u64_htos(first_data_bytes);

  shards[old_shard_count] = header0;
  shards[old_shard_count].sd_logical_start = kafs_u64_htos(1);
  shards[old_shard_count].sd_logical_count = kafs_u64_htos(1);
  shards[old_shard_count].sd_physical_off = kafs_u64_htos(data_off);
  shards[old_shard_count].sd_physical_bytes = kafs_u64_htos(block_size);

  shards[old_shard_count + 1u] = data0;
  shards[old_shard_count + 1u].sd_logical_start = kafs_u64_htos(1);
  shards[old_shard_count + 1u].sd_logical_count = kafs_u64_htos(1);
  shards[old_shard_count + 1u].sd_physical_off = kafs_u64_htos(data_off + block_size + first_data_bytes);
  shards[old_shard_count + 1u].sd_physical_bytes = kafs_u64_htos(second_data_bytes);
}

static uint64_t test_hrl_hash64(const void *buf, size_t len)
{
  const unsigned char *p = (const unsigned char *)buf;
  uint64_t h = 1469598103934665603ull;
  const uint64_t prime = 1099511628211ull;
  for (size_t i = 0; i < len; ++i)
  {
    h ^= p[i];
    h *= prime;
  }
  return h;
}

static int test_anchor_crc_bad(void)
{
  const char *img = "anchor-crc.img";
  if (make_v6_image(img) != 0)
    return -1;

  v6_image_info_t info;
  int rc = open_v6_image(img, 1, &info);
  if (rc != 0)
    return rc;
  info.sb.s_reserved[24] ^= 0x01u;
  rc = kafs_pwrite_all(info.fd, &info.sb, sizeof(info.sb), 0);
  close_v6_image(&info);
  if (rc != 0)
    return rc;

  kafs_v6_layout_report_t report;
  rc = discover_v6_image(img, &report);
  if (rc != -EINVAL || report.anchor_valid || report.replica_count != 0u)
    return -1;
  return 0;
}

static int test_all_descriptor_crc_bad(void)
{
  const char *img = "descriptor-crc.img";
  if (make_v6_image(img) != 0)
    return -1;
  if (mutate_all_descriptors(img, mutate_flip_padding_byte, 0) != 0)
    return -1;

  kafs_v6_layout_report_t report;
  int rc = discover_v6_image(img, &report);
  if (rc != -EINVAL || !report.anchor_valid || report.replica_count != 3u)
    return -1;
  for (uint32_t i = 0; i < report.replica_count; ++i)
    if (report.replicas[i].status != KAFS_V6_REPLICA_STATUS_CORRUPT)
      return -1;
  return 0;
}

static int test_descriptor_bounds_bad(void)
{
  const char *img = "descriptor-bounds.img";
  if (make_v6_image(img) != 0)
    return -1;
  if (mutate_all_descriptors(img, mutate_bad_table_bounds, 1) != 0)
    return -1;

  kafs_v6_layout_report_t report;
  int rc = discover_v6_image(img, &report);
  if (rc != -EINVAL || report.replica_count != 3u)
    return -1;
  for (uint32_t i = 0; i < report.replica_count; ++i)
    if (report.replicas[i].status != KAFS_V6_REPLICA_STATUS_CORRUPT)
      return -1;
  return 0;
}

static int test_unsupported_version(void)
{
  const char *img = "unsupported-version.img";
  if (make_v6_image(img) != 0)
    return -1;
  if (mutate_all_descriptors(img, mutate_unsupported_version, 1) != 0)
    return -1;

  kafs_v6_layout_report_t report;
  int rc = discover_v6_image(img, &report);
  if (rc != -ENOTSUP || !report.unsupported_only || report.replica_count != 3u)
    return -1;
  for (uint32_t i = 0; i < report.replica_count; ++i)
    if (report.replicas[i].status != KAFS_V6_REPLICA_STATUS_UNSUPPORTED)
      return -1;
  return 0;
}

static int test_incompat_flag(void)
{
  const char *img = "incompat-flag.img";
  if (make_v6_image(img) != 0)
    return -1;
  if (mutate_all_descriptors(img, mutate_incompat_flag, 1) != 0)
    return -1;

  kafs_v6_layout_report_t report;
  int rc = discover_v6_image(img, &report);
  if (rc != -ENOTSUP || !report.unsupported_only || report.replica_count != 3u)
    return -1;
  for (uint32_t i = 0; i < report.replica_count; ++i)
    if (report.replicas[i].status != KAFS_V6_REPLICA_STATUS_UNSUPPORTED)
      return -1;
  return 0;
}

static int test_primary_corrupt_backup_selected(void)
{
  const char *img = "primary-corrupt.img";
  if (make_v6_image(img) != 0)
    return -1;
  if (mutate_descriptor(img, 0, mutate_flip_padding_byte, 0) != 0)
    return -1;

  kafs_v6_layout_report_t report;
  int rc = discover_v6_image(img, &report);
  if (rc != 0 || !report.selected_found || report.selected_replica != 1u)
    return -1;
  if (report.replicas[0].status != KAFS_V6_REPLICA_STATUS_CORRUPT ||
      report.replicas[1].status != KAFS_V6_REPLICA_STATUS_SELECTED ||
      report.replicas[2].status != KAFS_V6_REPLICA_STATUS_VALID)
    return -1;

  char out[8192];
  char *dump_argv[] = {(char *)kafs_test_kafsdump_bin(), (char *)"--json", (char *)img, NULL};
  if (run_cmd_capture(dump_argv, 0, out, sizeof(out)) != 0 ||
      !strstr(out, "\"selected_replica\": 1") || !strstr(out, "\"status\": \"corrupt\""))
    return -1;

  char *fsck_argv[] = {(char *)kafs_test_fsck_bin(), (char *)img, NULL};
  if (run_cmd_capture(fsck_argv, 0, out, sizeof(out)) != 0 || !strstr(out, "status=corrupt") ||
      !strstr(out, "selected_replica=1"))
    return -1;
  return 0;
}

static int test_stale_generation_reported(void)
{
  const char *img = "stale-generation.img";
  if (make_v6_image(img) != 0)
    return -1;
  if (mutate_descriptor(img, 0, mutate_generation_2, 1) != 0)
    return -1;

  kafs_v6_layout_report_t report;
  int rc = discover_v6_image(img, &report);
  if (rc != 0 || !report.selected_found || report.selected_replica != 0u ||
      report.selected_generation != 2u)
    return -1;
  if (report.replicas[0].status != KAFS_V6_REPLICA_STATUS_SELECTED ||
      report.replicas[1].status != KAFS_V6_REPLICA_STATUS_STALE ||
      report.replicas[2].status != KAFS_V6_REPLICA_STATUS_STALE)
    return -1;

  char out[8192];
  char *dump_argv[] = {(char *)kafs_test_kafsdump_bin(), (char *)"--json", (char *)img, NULL};
  if (run_cmd_capture(dump_argv, 0, out, sizeof(out)) != 0 ||
      !strstr(out, "\"generation\": 2") || !strstr(out, "\"status\": \"stale\""))
    return -1;
  return 0;
}

static int test_divergent_same_generation_rejected(void)
{
  const char *img = "divergent-generation.img";
  if (make_v6_image(img) != 0)
    return -1;
  if (mutate_descriptor(img, 1, mutate_ro_compat_flag, 1) != 0)
    return -1;

  kafs_v6_layout_report_t report;
  int rc = discover_v6_image(img, &report);
  if (rc != -EINVAL || !report.divergent || report.selected_found)
    return -1;
  if (report.replicas[1].status != KAFS_V6_REPLICA_STATUS_DIVERGENT)
    return -1;

  char out[8192];
  char *dump_argv[] = {(char *)kafs_test_kafsdump_bin(), (char *)"--json", (char *)img, NULL};
  if (run_cmd_capture(dump_argv, 1, out, sizeof(out)) != 0 ||
      !strstr(out, "\"status\": \"divergent\""))
    return -1;
  return 0;
}

static int test_bitmap_coverage_valid_and_lookup(void)
{
  const char *img = "bitmap-valid.img";
  if (make_v6_image(img) != 0)
    return -1;

  void *desc = NULL;
  uint32_t desc_bytes = 0;
  kafs_ssuperblock_t sb;
  uint64_t file_size = 0;
  if (read_selected_descriptor_for_image(img, &desc, &desc_bytes, &sb, &file_size, NULL) != 0)
    return -1;

  kafs_v6_bitmap_coverage_report_t report;
  int rc = kafs_v6_bitmap_validate_coverage(desc, desc_bytes, &sb, file_size, &report);
  if (rc != 0 || !report.available || report.shard_count != 1u ||
      report.expected_count != (uint64_t)kafs_sb_r_blkcnt_get(&sb) ||
      report.covered_blocks != report.expected_count || report.missing_coverage ||
      report.has_gap || report.has_overlap || report.has_physical_overlap ||
      !report.lookup_available)
  {
    free(desc);
    return -1;
  }

  kafs_v6_bitmap_lookup_t lookup;
  if (kafs_v6_bitmap_lookup(desc, desc_bytes, 0, &lookup) != 0 || lookup.shard_index != 1u ||
      lookup.bitmap_bit != 0u)
  {
    free(desc);
    return -1;
  }

  uint64_t first_data = kafs_sb_first_data_block_get(&sb);
  if (first_data < (uint64_t)kafs_sb_r_blkcnt_get(&sb) &&
      kafs_v6_bitmap_lookup(desc, desc_bytes, first_data, &lookup) != 0)
  {
    free(desc);
    return -1;
  }

  free(desc);
  return 0;
}

static int test_inode_coverage_valid_and_lookup(void)
{
  const char *img = "inode-valid.img";
  if (make_v6_image(img) != 0)
    return -1;

  void *desc = NULL;
  uint32_t desc_bytes = 0;
  kafs_ssuperblock_t sb;
  uint64_t file_size = 0;
  if (read_selected_descriptor_for_image(img, &desc, &desc_bytes, &sb, &file_size, NULL) != 0)
    return -1;

  kafs_v6_inode_coverage_report_t report;
  int rc = kafs_v6_inode_validate_coverage(desc, desc_bytes, &sb, file_size, &report);
  if (rc != 0 || !report.available || report.shard_count != 1u ||
      report.expected_count != (uint64_t)kafs_sb_inocnt_get(&sb) ||
      report.covered_inodes != report.expected_count || report.missing_coverage ||
      report.has_gap || report.has_overlap || report.has_physical_overlap ||
      !report.root_lookup_available ||
      report.root_lookup.record_bytes !=
          (uint32_t)kafs_inode_bytes_for_format(KAFS_FORMAT_VERSION_V6) ||
      report.root_lookup.inode_off >= file_size)
  {
    free(desc);
    return -1;
  }

  kafs_v6_inode_lookup_t lookup;
  if (kafs_v6_inode_lookup(desc, desc_bytes, KAFS_INO_ROOTDIR, &lookup) != 0 ||
      lookup.ino != KAFS_INO_ROOTDIR || lookup.inode_off != report.root_lookup.inode_off)
  {
    free(desc);
    return -1;
  }

  free(desc);
  return 0;
}

static int test_allocator_summary_coverage_valid_and_lookup(void)
{
  const char *img = "allocator-summary-valid.img";
  if (make_v6_image(img) != 0)
    return -1;

  void *desc = NULL;
  uint32_t desc_bytes = 0;
  kafs_ssuperblock_t sb;
  uint64_t file_size = 0;
  if (read_selected_descriptor_for_image(img, &desc, &desc_bytes, &sb, &file_size, NULL) != 0)
    return -1;

  kafs_v6_allocator_summary_coverage_report_t report;
  int rc = kafs_v6_allocator_summary_validate_coverage(desc, desc_bytes, &sb, file_size, &report);
  if (rc != 0 || !report.available || report.shard_count != 1u ||
      report.expected_count != (uint64_t)kafs_sb_r_blkcnt_get(&sb) ||
      report.covered_blocks != report.expected_count || report.missing_coverage ||
      report.has_gap || report.has_overlap || report.has_physical_overlap ||
      !report.lookup_available || report.lookup.shard_index != 3u ||
      report.lookup.l1_byte_off >= file_size || report.lookup.l2_byte_off >= file_size)
  {
    free(desc);
    return -1;
  }

  kafs_v6_allocator_summary_lookup_t lookup;
  if (kafs_v6_allocator_summary_lookup(desc, desc_bytes, 0, &lookup) != 0 ||
      lookup.l1_byte_off != report.lookup.l1_byte_off ||
      lookup.l2_byte_off != report.lookup.l2_byte_off)
  {
    free(desc);
    return -1;
  }

  free(desc);
  return 0;
}

static int test_allocator_summary_runtime_allocation_mapping(void)
{
  const char *img = "allocator-summary-runtime.img";
  if (make_v6_image_size(img, "1G") != 0)
    return -1;
  if (mutate_all_descriptors(img, mutate_allocator_split_two_shards, 1) != 0)
    return -1;

  v6_image_info_t info;
  if (open_v6_image(img, 1, &info) != 0)
    return -1;

  int failed = 0;
  void *map = MAP_FAILED;
  kafs_context_t ctx;
  memset(&ctx, 0, sizeof(ctx));

  map = mmap(NULL, (size_t)info.file_size, PROT_READ | PROT_WRITE, MAP_PRIVATE, info.fd, 0);
  if (map == MAP_FAILED)
    failed = 1;

  if (!failed)
  {
    ctx.c_img_base = map;
    ctx.c_img_size = (size_t)info.file_size;
    ctx.c_superblock = (kafs_ssuperblock_t *)map;
    ctx.c_fd = info.fd;

    kafs_v6_allocator_summary_coverage_report_t alloc_coverage;
    int admit_rc = kafs_v6_descriptor_mapping_admit_fd(&ctx, info.fd, info.file_size, NULL, NULL,
                                                       &alloc_coverage, NULL, NULL);
    if (admit_rc != 0 ||
        !ctx.c_v6_bitmap_mapping_enabled || !ctx.c_v6_inode_mapping_enabled ||
        !ctx.c_v6_alloc_summary_mapping_enabled || alloc_coverage.shard_count != 2u ||
        ctx.c_v6_alloc_summary_shard_count != 2u)
      failed = 1;
  }

  if (!failed)
  {
    const kafs_v6_alloc_summary_runtime_shard_t *second = &ctx.c_v6_alloc_summary_shards[1];
    kafs_blkcnt_t fdb = kafs_sb_first_data_block_get(ctx.c_superblock);
    kafs_blkcnt_t blocnt = kafs_sb_blkcnt_get(ctx.c_superblock);
    if (second->logical_start < (uint64_t)fdb || second->logical_start >= (uint64_t)blocnt ||
        second->physical_end > ctx.c_img_size)
      failed = 1;

    if (!failed)
    {
      memset((char *)map + (size_t)second->physical_off, 0,
             (size_t)(second->physical_end - second->physical_off));
      ctx.c_alloc_v3_summary_dirty = 0u;
      ctx.c_blo_search = (kafs_blkcnt_t)(second->logical_start - 1u);
      uint64_t calls_before = ctx.c_stat_blk_alloc_calls;
      uint64_t writes_before = ctx.c_meta_region_writes[KAFS_META_REGION_ALLOCATOR_SUMMARY];
      kafs_blkcnt_t blo = KAFS_BLO_NONE;
      int rc = kafs_blk_alloc(&ctx, &blo);
      if (rc != 0 || blo != (kafs_blkcnt_t)second->logical_start ||
          ctx.c_alloc_v3_summary_dirty != 0u || ctx.c_stat_blk_alloc_calls <= calls_before ||
          ctx.c_meta_region_writes[KAFS_META_REGION_ALLOCATOR_SUMMARY] <= writes_before ||
          !kafs_blk_get_usage(&ctx, blo))
        failed = 1;

      kafs_v6_allocator_summary_lookup_t lookup;
      if (!failed &&
          (kafs_v6_allocator_summary_lookup(ctx.c_v6_layout_desc, ctx.c_v6_layout_desc_bytes,
                                            (uint64_t)blo, &lookup) != 0 ||
           lookup.l1_byte_off < second->physical_off || lookup.l1_byte_off >= second->physical_end ||
           lookup.l2_byte_off < second->physical_off || lookup.l2_byte_off >= second->physical_end))
        failed = 1;
    }
  }

  kafs_bitmap_descriptor_mapping_clear(&ctx);
  if (map != MAP_FAILED)
    munmap(map, (size_t)info.file_size);
  close_v6_image(&info);
  return failed ? -1 : 0;
}

static int test_hrl_coverage_valid_lookup_and_chains(void)
{
  const char *img = "hrl-valid.img";
  if (make_v6_image(img) != 0)
    return -1;

  v6_image_info_t info;
  if (open_v6_image(img, 0, &info) != 0)
    return -1;

  void *desc = NULL;
  int failed = 0;
  if (read_descriptor(&info, 0, &desc) != 0)
    failed = 1;

  if (!failed)
  {
    kafs_v6_hrl_index_coverage_report_t index_report;
    kafs_v6_hrl_entries_coverage_report_t entries_report;
    kafs_v6_hrl_chain_report_t chain_report;
    uint64_t expected_buckets = kafs_sb_hrl_index_size_get(&info.sb) / sizeof(uint32_t);
    uint64_t expected_entries = kafs_sb_hrl_entry_cnt_get(&info.sb);

    int rc = kafs_v6_hrl_index_validate_coverage(desc, info.desc_bytes, &info.sb,
                                                 info.file_size, &index_report);
    if (rc != 0 || !index_report.available || index_report.shard_count != 1u ||
        index_report.expected_count != expected_buckets ||
        index_report.covered_buckets != expected_buckets || index_report.missing_coverage ||
        !index_report.lookup_available || index_report.lookup.index_off >= info.file_size)
      failed = 1;

    rc = kafs_v6_hrl_entries_validate_coverage(desc, info.desc_bytes, &info.sb,
                                               info.file_size, &entries_report);
    if (!failed &&
        (rc != 0 || !entries_report.available || entries_report.shard_count != 1u ||
         entries_report.expected_count != expected_entries ||
         entries_report.covered_entries != expected_entries || entries_report.missing_coverage ||
         !entries_report.lookup_available || entries_report.lookup.entry_off >= info.file_size))
      failed = 1;

    rc = kafs_v6_hrl_validate_chain_bounds_fd(info.fd, desc, info.desc_bytes, &info.sb,
                                             info.file_size, &chain_report);
    if (!failed &&
        (rc != 0 || !chain_report.available || chain_report.buckets_checked != expected_buckets ||
         chain_report.entries_checked != 0u || chain_report.has_out_of_range ||
         chain_report.has_loop || chain_report.has_wrong_entry_group || chain_report.has_read_error))
      failed = 1;
  }

  free(desc);
  close_v6_image(&info);
  return failed ? -1 : 0;
}

static int choose_block_for_bucket_range(unsigned char *block, size_t block_size,
                                         uint32_t bucket_count, uint64_t min_bucket,
                                         uint32_t *out_bucket)
{
  if (!block || bucket_count == 0u || !out_bucket)
    return -1;

  for (uint64_t seed = 0; seed < 200000u; ++seed)
  {
    memset(block, 0x5a, block_size);
    memcpy(block, &seed, sizeof(seed) < block_size ? sizeof(seed) : block_size);
    uint64_t fast = test_hrl_hash64(block, block_size);
    uint32_t bucket = (uint32_t)(fast & (uint64_t)(bucket_count - 1u));
    if ((uint64_t)bucket >= min_bucket)
    {
      *out_bucket = bucket;
      return 0;
    }
  }
  return -1;
}

static int write_journal_segment_header(const v6_image_info_t *info, const void *desc,
                                        uint64_t segment_id, uint64_t generation)
{
  kafs_v6_journal_segment_lookup_t lookup;
  if (kafs_v6_journal_segment_lookup(desc, info->desc_bytes, segment_id, &lookup) != 0)
    return -1;

  kj_header_t hdr;
  memset(&hdr, 0, sizeof(hdr));
  hdr.magic = KJ_MAGIC;
  hdr.version = KJ_VER;
  hdr.flags = 0;
  hdr.area_size = lookup.data.data_bytes;
  hdr.write_off = 0;
  hdr.seq = generation;
  hdr.reserved0 = generation;
  hdr.header_crc = kj_header_crc_calc(&hdr);
  return (kafs_pwrite_all(info->fd, &hdr, sizeof(hdr), (off_t)lookup.header.header_off) == 0) ? 0
                                                                                              : -1;
}

static int test_hrl_runtime_shard_mapping(void)
{
  const char *img = "hrl-runtime-shards.img";
  if (make_v6_image_size(img, "256M") != 0)
    return -1;
  if (mutate_all_descriptors(img, mutate_hrl_index_split_two_shards, 1) != 0)
    return -1;
  if (mutate_all_descriptors(img, mutate_hrl_entries_split_two_shards, 1) != 0)
    return -1;

  v6_image_info_t info;
  if (open_v6_image(img, 1, &info) != 0)
    return -1;

  int failed = 0;
  int hrl_opened = 0;
  void *map = MAP_FAILED;
  unsigned char *block = NULL;
  kafs_context_t ctx;
  memset(&ctx, 0, sizeof(ctx));

  map = mmap(NULL, (size_t)info.file_size, PROT_READ | PROT_WRITE, MAP_PRIVATE, info.fd, 0);
  if (map == MAP_FAILED)
    failed = 1;

  if (!failed)
  {
    ctx.c_img_base = map;
    ctx.c_img_size = (size_t)info.file_size;
    ctx.c_superblock = (kafs_ssuperblock_t *)map;
    ctx.c_fd = info.fd;

    kafs_v6_hrl_index_coverage_report_t index_report;
    kafs_v6_hrl_entries_coverage_report_t entries_report;
    int rc = kafs_v6_descriptor_mapping_admit_fd(&ctx, info.fd, info.file_size, NULL, NULL, NULL,
                                                 &index_report, &entries_report);
    if (rc != 0 || !ctx.c_v6_hrl_mapping_enabled || index_report.shard_count != 2u ||
        entries_report.shard_count != 2u || ctx.c_v6_hrl_index_shard_count != 2u ||
        ctx.c_v6_hrl_entry_shard_count != 2u)
      failed = 1;
  }

  if (!failed && kafs_hrl_open(&ctx) != 0)
    failed = 1;
  else if (!failed)
    hrl_opened = 1;

  uint32_t bucket = 0;
  uint32_t forced_hrid = 0;
  kafs_v6_hrl_index_lookup_t index_lookup;
  kafs_v6_hrl_entry_lookup_t entry_lookup;
  memset(&index_lookup, 0, sizeof(index_lookup));
  memset(&entry_lookup, 0, sizeof(entry_lookup));

  if (!failed)
  {
    uint32_t block_size = (uint32_t)kafs_sb_blksize_get(ctx.c_superblock);
    block = calloc(1, block_size);
    if (!block)
      failed = 1;
    uint64_t second_bucket_start = ctx.c_v6_hrl_index_shards[1].logical_start;
    if (!failed &&
        choose_block_for_bucket_range(block, block_size, ctx.c_hrl_bucket_cnt, second_bucket_start,
                                      &bucket) != 0)
      failed = 1;

    forced_hrid = (uint32_t)ctx.c_v6_hrl_entry_shards[1].logical_start;
    if (!failed &&
        (kafs_v6_hrl_index_lookup(ctx.c_v6_layout_desc, ctx.c_v6_layout_desc_bytes, bucket,
                                  &index_lookup) != 0 ||
         kafs_v6_hrl_entry_lookup(ctx.c_v6_layout_desc, ctx.c_v6_layout_desc_bytes, forced_hrid,
                                  &entry_lookup) != 0))
      failed = 1;
  }

  if (!failed)
  {
    kafs_hrl_entry_t *entry = (kafs_hrl_entry_t *)((char *)map + entry_lookup.entry_off);
    memset(entry, 0, sizeof(*entry));
    entry->blo = KAFS_BLO_NONE;
    ctx.c_hrl_free_head_plus1 = forced_hrid + 1u;
    ctx.c_hrl_free_slot_count = 1u;

    uint64_t index_writes_before = ctx.c_meta_region_writes[KAFS_META_REGION_HRL_INDEX];
    uint64_t entry_writes_before = ctx.c_meta_region_writes[KAFS_META_REGION_HRL_ENTRIES];
    kafs_hrid_t hrid = 0;
    int is_new = 0;
    kafs_blkcnt_t blo = KAFS_BLO_NONE;
    int rc = kafs_hrl_put(&ctx, block, &hrid, &is_new, &blo);
    uint32_t *bucket_head = (uint32_t *)((char *)map + index_lookup.index_off);
    if (rc != 0 || !is_new || hrid != forced_hrid || blo == KAFS_BLO_NONE ||
        *bucket_head != forced_hrid + 1u ||
        ctx.c_meta_region_writes[KAFS_META_REGION_HRL_INDEX] <= index_writes_before ||
        ctx.c_meta_region_writes[KAFS_META_REGION_HRL_ENTRIES] <= entry_writes_before)
      failed = 1;

    if (!failed && (kafs_hrl_inc_ref(&ctx, hrid) != 0 || entry->refcnt != 2u ||
                    kafs_hrl_dec_ref(&ctx, hrid) != 0 || entry->refcnt != 1u))
      failed = 1;
  }

  if (hrl_opened)
    (void)kafs_hrl_close(&ctx);
  free(block);
  kafs_bitmap_descriptor_mapping_clear(&ctx);
  if (map != MAP_FAILED)
    munmap(map, (size_t)info.file_size);
  close_v6_image(&info);
  return failed ? -1 : 0;
}

static int test_inode_runtime_descriptor_mapping(void)
{
  const char *img = "inode-runtime-mapping.img";
  if (make_v6_image(img) != 0)
    return -1;

  v6_image_info_t info;
  if (open_v6_image(img, 1, &info) != 0)
    return -1;

  int failed = 0;
  void *map = MAP_FAILED;
  kafs_context_t ctx;
  memset(&ctx, 0, sizeof(ctx));

  map = mmap(NULL, (size_t)info.file_size, PROT_READ | PROT_WRITE, MAP_PRIVATE, info.fd, 0);
  if (map == MAP_FAILED)
    failed = 1;

  if (!failed)
  {
    ctx.c_img_base = map;
    ctx.c_img_size = (size_t)info.file_size;
    ctx.c_superblock = (kafs_ssuperblock_t *)map;
    ctx.c_fd = info.fd;

    kafs_v6_inode_coverage_report_t inode_coverage;
    if (kafs_v6_descriptor_mapping_admit_fd(&ctx, info.fd, info.file_size, NULL,
                                            &inode_coverage, NULL, NULL, NULL) != 0 ||
        !ctx.c_v6_bitmap_mapping_enabled || !ctx.c_v6_inode_mapping_enabled ||
        !ctx.c_v6_layout_desc_owned || inode_coverage.shard_count != ctx.c_v6_inode_shard_count)
      failed = 1;
  }

  if (!failed)
  {
    kafs_v6_inode_lookup_t lookup;
    if (kafs_v6_inode_lookup(ctx.c_v6_layout_desc, ctx.c_v6_layout_desc_bytes, KAFS_INO_ROOTDIR,
                             &lookup) != 0)
      failed = 1;
    kafs_sinode_t *root = failed ? NULL : kafs_ctx_inode(&ctx, KAFS_INO_ROOTDIR);
    if (!failed && root != (kafs_sinode_t *)((char *)map + lookup.inode_off))
      failed = 1;
    if (!failed && kafs_ctx_inode_const(&ctx, KAFS_INO_ROOTDIR) != root)
      failed = 1;
    if (!failed && kafs_ctx_ino_no(&ctx, root) != KAFS_INO_ROOTDIR)
      failed = 1;
    if (!failed && !kafs_ino_get_usage(root))
      failed = 1;
  }

  kafs_bitmap_descriptor_mapping_clear(&ctx);
  if (map != MAP_FAILED)
    munmap(map, (size_t)info.file_size);
  close_v6_image(&info);
  return failed ? -1 : 0;
}

static int test_inode_runtime_allocation_mapping(void)
{
  const char *img = "inode-runtime-allocation.img";
  if (make_v6_image_size(img, "256M") != 0)
    return -1;
  if (mutate_all_descriptors(img, mutate_inode_split_two_shards, 1) != 0)
    return -1;

  v6_image_info_t info;
  if (open_v6_image(img, 1, &info) != 0)
    return -1;

  int failed = 0;
  void *map = MAP_FAILED;
  kafs_context_t ctx;
  memset(&ctx, 0, sizeof(ctx));

  map = mmap(NULL, (size_t)info.file_size, PROT_READ | PROT_WRITE, MAP_PRIVATE, info.fd, 0);
  if (map == MAP_FAILED)
    failed = 1;

  if (!failed)
  {
    ctx.c_img_base = map;
    ctx.c_img_size = (size_t)info.file_size;
    ctx.c_superblock = (kafs_ssuperblock_t *)map;
    ctx.c_fd = info.fd;
    kafs_v6_inode_coverage_report_t inode_coverage;
    if (kafs_v6_descriptor_mapping_admit_fd(&ctx, info.fd, info.file_size, NULL,
                                            &inode_coverage, NULL, NULL, NULL) != 0 ||
        !ctx.c_v6_inode_mapping_enabled || inode_coverage.shard_count != 2u ||
        ctx.c_v6_inode_shard_count != 2u)
      failed = 1;
  }

  if (!failed)
  {
    kafs_inocnt_t inocnt = kafs_sb_inocnt_get(ctx.c_superblock);
    uint64_t second_start = ctx.c_v6_inode_shards[1].logical_start;
    if (second_start == 0u || second_start >= inocnt)
      failed = 1;
    if (!failed)
    {
      ctx.c_ino_search = (kafs_inocnt_t)(second_start - 1u);
      kafs_inocnt_t ino = KAFS_INO_NONE;
      int rc = kafs_ctx_ino_find_free(&ctx, &ino, &ctx.c_ino_search, inocnt);
      if (rc != 0 || ino != (kafs_inocnt_t)second_start)
        failed = 1;
      kafs_sinode_t *inoent = failed ? NULL : kafs_ctx_inode(&ctx, ino);
      kafs_v6_inode_lookup_t lookup;
      if (!failed &&
          kafs_v6_inode_lookup(ctx.c_v6_layout_desc, ctx.c_v6_layout_desc_bytes, ino, &lookup) != 0)
        failed = 1;
      if (!failed && inoent != (kafs_sinode_t *)((char *)map + lookup.inode_off))
        failed = 1;
      if (!failed)
      {
        kafs_ctx_inode_zero(&ctx, inoent);
        kafs_ino_mode_set(inoent, S_IFREG | 0600);
        if (!kafs_ino_get_usage(kafs_ctx_inode_const(&ctx, ino)) ||
            kafs_ctx_ino_no(&ctx, inoent) != ino)
          failed = 1;
      }
    }
  }

  kafs_bitmap_descriptor_mapping_clear(&ctx);
  if (map != MAP_FAILED)
    munmap(map, (size_t)info.file_size);
  close_v6_image(&info);
  return failed ? -1 : 0;
}

static int exercise_bitmap_mapping_blo(kafs_context_t *ctx, kafs_blkcnt_t blo, uint32_t *out_shard)
{
  kafs_v6_bitmap_lookup_t lookup;
  if (!ctx || !ctx->c_v6_layout_desc ||
      kafs_v6_bitmap_lookup(ctx->c_v6_layout_desc, ctx->c_v6_layout_desc_bytes, (uint64_t)blo,
                            &lookup) != 0)
    return -1;
  if (lookup.bitmap_byte_off >= ctx->c_img_size)
    return -1;
  if (out_shard)
    *out_shard = lookup.shard_index;

  uint8_t *bitmap_byte = (uint8_t *)ctx->c_img_base + lookup.bitmap_byte_off;
  uint8_t bitmap_mask = (uint8_t)(1u << lookup.bitmap_bit);
  *bitmap_byte &= (uint8_t)~bitmap_mask;

  uint64_t writes_before = ctx->c_meta_region_writes[KAFS_META_REGION_BLOCK_BITMAP];
  uint64_t bytes_before = ctx->c_meta_region_bytes[KAFS_META_REGION_BLOCK_BITMAP];
  if (kafs_blk_get_usage(ctx, blo) != 0)
    return -1;
  if (kafs_blk_set_usage_nolock(ctx, blo, KAFS_TRUE) != 0)
    return -1;
  if ((*bitmap_byte & bitmap_mask) == 0u || !kafs_blk_get_usage(ctx, blo))
    return -1;
  if (ctx->c_meta_region_writes[KAFS_META_REGION_BLOCK_BITMAP] <= writes_before ||
      ctx->c_meta_region_bytes[KAFS_META_REGION_BLOCK_BITMAP] <= bytes_before)
    return -1;
  if (kafs_blk_set_usage_nolock(ctx, blo, KAFS_FALSE) != 0)
    return -1;
  if ((*bitmap_byte & bitmap_mask) != 0u || kafs_blk_get_usage(ctx, blo) != 0)
    return -1;
  return 0;
}

static int test_bitmap_runtime_descriptor_mapping(void)
{
  const char *img = "bitmap-runtime-mapping.img";
  if (make_v6_image(img) != 0)
    return -1;

  v6_image_info_t info;
  if (open_v6_image(img, 1, &info) != 0)
    return -1;

  int failed = 0;
  void *map = MAP_FAILED;
  kafs_context_t ctx;
  memset(&ctx, 0, sizeof(ctx));

  map = mmap(NULL, (size_t)info.file_size, PROT_READ | PROT_WRITE, MAP_PRIVATE, info.fd, 0);
  if (map == MAP_FAILED)
    failed = 1;

  if (!failed)
  {
    ctx.c_img_base = map;
    ctx.c_img_size = (size_t)info.file_size;
    ctx.c_superblock = (kafs_ssuperblock_t *)map;
    ctx.c_fd = info.fd;
    if (kafs_bitmap_descriptor_mapping_admit_fd(&ctx, info.fd, info.file_size, NULL) != 0 ||
        !ctx.c_v6_bitmap_mapping_enabled || !ctx.c_v6_layout_desc_owned)
      failed = 1;
  }

  if (!failed)
  {
    kafs_blkcnt_t blo = kafs_sb_first_data_block_get(ctx.c_superblock);
    if (blo >= kafs_sb_blkcnt_get(ctx.c_superblock))
      failed = 1;
    if (!failed && exercise_bitmap_mapping_blo(&ctx, blo, NULL) != 0)
      failed = 1;
  }

  kafs_bitmap_descriptor_mapping_clear(&ctx);
  if (map != MAP_FAILED)
    munmap(map, (size_t)info.file_size);
  close_v6_image(&info);
  return failed ? -1 : 0;
}

static int test_bitmap_multi_shard_runtime_mapping(void)
{
  const char *img = "bitmap-runtime-multishard.img";
  if (make_v6_image_size(img, "256M") != 0)
    return -1;
  if (mutate_all_descriptors(img, mutate_bitmap_split_two_shards, 1) != 0)
    return -1;

  v6_image_info_t info;
  if (open_v6_image(img, 1, &info) != 0)
    return -1;

  int failed = 0;
  void *map = MAP_FAILED;
  kafs_context_t ctx;
  memset(&ctx, 0, sizeof(ctx));

  map = mmap(NULL, (size_t)info.file_size, PROT_READ | PROT_WRITE, MAP_PRIVATE, info.fd, 0);
  if (map == MAP_FAILED)
    failed = 1;
  if (!failed)
  {
    ctx.c_img_base = map;
    ctx.c_img_size = (size_t)info.file_size;
    ctx.c_superblock = (kafs_ssuperblock_t *)map;
    ctx.c_fd = info.fd;
    kafs_v6_bitmap_coverage_report_t coverage;
    if (kafs_bitmap_descriptor_mapping_admit_fd(&ctx, info.fd, info.file_size, &coverage) != 0 ||
        !ctx.c_v6_bitmap_mapping_enabled || coverage.shard_count != 2u)
      failed = 1;
  }

  if (!failed)
  {
    kafs_v6_bitmap_lookup_t first;
    kafs_v6_bitmap_lookup_t second;
    if (kafs_v6_bitmap_lookup(ctx.c_v6_layout_desc, ctx.c_v6_layout_desc_bytes, 0, &first) != 0)
      failed = 1;
    if (!failed &&
        kafs_v6_bitmap_lookup(ctx.c_v6_layout_desc, ctx.c_v6_layout_desc_bytes,
                              first.logical_start + first.logical_count, &second) != 0)
      failed = 1;
    if (!failed && second.shard_index == first.shard_index)
      failed = 1;

    uint32_t used_shard = UINT32_MAX;
    if (!failed && exercise_bitmap_mapping_blo(&ctx, (kafs_blkcnt_t)second.blo, &used_shard) != 0)
      failed = 1;
    if (!failed && used_shard != second.shard_index)
      failed = 1;
  }

  kafs_bitmap_descriptor_mapping_clear(&ctx);
  if (map != MAP_FAILED)
    munmap(map, (size_t)info.file_size);
  close_v6_image(&info);
  return failed ? -1 : 0;
}

static int test_bitmap_gap_rejected(void)
{
  const char *img = "bitmap-gap.img";
  if (make_v6_image(img) != 0)
    return -1;
  if (mutate_all_descriptors(img, mutate_bitmap_gap, 1) != 0)
    return -1;

  void *desc = NULL;
  uint32_t desc_bytes = 0;
  kafs_ssuperblock_t sb;
  uint64_t file_size = 0;
  if (read_selected_descriptor_for_image(img, &desc, &desc_bytes, &sb, &file_size, NULL) != 0)
    return -1;

  kafs_v6_bitmap_coverage_report_t report;
  int rc = kafs_v6_bitmap_validate_coverage(desc, desc_bytes, &sb, file_size, &report);
  free(desc);
  if (rc == 0 || !report.available || !report.has_gap || !report.missing_coverage)
    return -1;

  char out[8192];
  char *fsck_argv[] = {(char *)kafs_test_fsck_bin(), (char *)img, NULL};
  if (run_cmd_capture(fsck_argv, 13, out, sizeof(out)) != 0 ||
      !strstr(out, "v6 bitmap shards:") || !strstr(out, "has_gap=true"))
    return -1;

  char *dump_argv[] = {(char *)kafs_test_kafsdump_bin(), (char *)"--json", (char *)img, NULL};
  if (run_cmd_capture(dump_argv, 1, out, sizeof(out)) != 0 ||
      !strstr(out, "\"missing_coverage\": true"))
    return -1;
  return 0;
}

static int test_inode_gap_rejected(void)
{
  const char *img = "inode-gap.img";
  if (make_v6_image(img) != 0)
    return -1;
  if (mutate_all_descriptors(img, mutate_inode_gap, 1) != 0)
    return -1;

  void *desc = NULL;
  uint32_t desc_bytes = 0;
  kafs_ssuperblock_t sb;
  uint64_t file_size = 0;
  if (read_selected_descriptor_for_image(img, &desc, &desc_bytes, &sb, &file_size, NULL) != 0)
    return -1;

  kafs_v6_inode_coverage_report_t report;
  int rc = kafs_v6_inode_validate_coverage(desc, desc_bytes, &sb, file_size, &report);
  free(desc);
  if (rc == 0 || !report.available || !report.has_gap || !report.missing_coverage)
    return -1;

  char out[8192];
  char *fsck_argv[] = {(char *)kafs_test_fsck_bin(), (char *)img, NULL};
  if (run_cmd_capture(fsck_argv, 13, out, sizeof(out)) != 0 ||
      !strstr(out, "v6 inode shards:") || !strstr(out, "has_gap=true"))
    return -1;

  return 0;
}

static int test_allocator_summary_gap_rejected(void)
{
  const char *img = "allocator-summary-gap.img";
  if (make_v6_image(img) != 0)
    return -1;
  if (mutate_all_descriptors(img, mutate_allocator_gap, 1) != 0)
    return -1;

  void *desc = NULL;
  uint32_t desc_bytes = 0;
  kafs_ssuperblock_t sb;
  uint64_t file_size = 0;
  if (read_selected_descriptor_for_image(img, &desc, &desc_bytes, &sb, &file_size, NULL) != 0)
    return -1;

  kafs_v6_allocator_summary_coverage_report_t report;
  int rc = kafs_v6_allocator_summary_validate_coverage(desc, desc_bytes, &sb, file_size, &report);
  free(desc);
  if (rc == 0 || !report.available || !report.has_gap || !report.missing_coverage)
    return -1;

  char out[8192];
  char *fsck_argv[] = {(char *)kafs_test_fsck_bin(), (char *)img, NULL};
  if (run_cmd_capture(fsck_argv, 13, out, sizeof(out)) != 0 ||
      !strstr(out, "v6 allocator summary shards:") || !strstr(out, "has_gap=true"))
    return -1;

  return 0;
}

static int test_hrl_index_gap_rejected(void)
{
  const char *img = "hrl-index-gap.img";
  if (make_v6_image(img) != 0)
    return -1;
  if (mutate_all_descriptors(img, mutate_hrl_index_gap, 1) != 0)
    return -1;

  void *desc = NULL;
  uint32_t desc_bytes = 0;
  kafs_ssuperblock_t sb;
  uint64_t file_size = 0;
  if (read_selected_descriptor_for_image(img, &desc, &desc_bytes, &sb, &file_size, NULL) != 0)
    return -1;

  kafs_v6_hrl_index_coverage_report_t report;
  int rc = kafs_v6_hrl_index_validate_coverage(desc, desc_bytes, &sb, file_size, &report);
  free(desc);
  if (rc == 0 || !report.available || !report.has_gap || !report.missing_coverage)
    return -1;

  char out[8192];
  char *fsck_argv[] = {(char *)kafs_test_fsck_bin(), (char *)img, NULL};
  if (run_cmd_capture(fsck_argv, 13, out, sizeof(out)) != 0 ||
      !strstr(out, "v6 HRL index shards:") || !strstr(out, "has_gap=true"))
    return -1;

  return 0;
}

static int test_hrl_entries_gap_rejected(void)
{
  const char *img = "hrl-entries-gap.img";
  if (make_v6_image(img) != 0)
    return -1;
  if (mutate_all_descriptors(img, mutate_hrl_entries_gap, 1) != 0)
    return -1;

  void *desc = NULL;
  uint32_t desc_bytes = 0;
  kafs_ssuperblock_t sb;
  uint64_t file_size = 0;
  if (read_selected_descriptor_for_image(img, &desc, &desc_bytes, &sb, &file_size, NULL) != 0)
    return -1;

  kafs_v6_hrl_entries_coverage_report_t report;
  int rc = kafs_v6_hrl_entries_validate_coverage(desc, desc_bytes, &sb, file_size, &report);
  free(desc);
  if (rc == 0 || !report.available || !report.has_gap || !report.missing_coverage)
    return -1;

  char out[8192];
  char *fsck_argv[] = {(char *)kafs_test_fsck_bin(), (char *)img, NULL};
  if (run_cmd_capture(fsck_argv, 13, out, sizeof(out)) != 0 ||
      !strstr(out, "v6 HRL entry shards:") || !strstr(out, "has_gap=true"))
    return -1;

  return 0;
}

static int test_hrl_chain_out_of_range_rejected(void)
{
  const char *img = "hrl-chain-out-of-range.img";
  if (make_v6_image(img) != 0)
    return -1;

  v6_image_info_t info;
  if (open_v6_image(img, 1, &info) != 0)
    return -1;

  void *desc = NULL;
  int failed = 0;
  if (read_descriptor(&info, 0, &desc) != 0)
    failed = 1;

  if (!failed)
  {
    kafs_v6_hrl_index_lookup_t lookup;
    uint32_t bad_head = kafs_sb_hrl_entry_cnt_get(&info.sb) + 1u;
    if (kafs_v6_hrl_index_lookup(desc, info.desc_bytes, 0, &lookup) != 0 ||
        kafs_pwrite_all(info.fd, &bad_head, sizeof(bad_head), (off_t)lookup.index_off) != 0)
      failed = 1;
  }

  if (!failed)
  {
    kafs_v6_hrl_chain_report_t report;
    int rc = kafs_v6_hrl_validate_chain_bounds_fd(info.fd, desc, info.desc_bytes, &info.sb,
                                                 info.file_size, &report);
    if (rc == 0 || !report.available || !report.has_out_of_range ||
        report.first_bad_bucket != 0u)
      failed = 1;
  }

  free(desc);
  close_v6_image(&info);
  if (failed)
    return -1;

  char out[8192];
  char *fsck_argv[] = {(char *)kafs_test_fsck_bin(), (char *)img, NULL};
  if (run_cmd_capture(fsck_argv, 13, out, sizeof(out)) != 0 ||
      !strstr(out, "v6 HRL chains:") || !strstr(out, "has_out_of_range=true"))
    return -1;

  return 0;
}

static int test_journal_coverage_valid_lookup_and_segments(void)
{
  const char *img = "journal-valid.img";
  if (make_v6_image(img) != 0)
    return -1;

  v6_image_info_t info;
  if (open_v6_image(img, 0, &info) != 0)
    return -1;

  void *desc = NULL;
  int failed = 0;
  if (read_descriptor(&info, 0, &desc) != 0)
    failed = 1;

  kafs_v6_journal_header_coverage_report_t header_report;
  kafs_v6_journal_data_coverage_report_t data_report;
  kafs_v6_journal_segment_report_t segment_report;
  int rc = 0;
  if (!failed)
    rc = kafs_v6_journal_header_validate_coverage(desc, info.desc_bytes, &info.sb,
                                                  info.file_size, &header_report);
  if (!failed &&
      (rc != 0 || !header_report.available || header_report.shard_count != 1u ||
       header_report.expected_count != 1u || header_report.covered_segments != 1u ||
       !header_report.lookup_available))
    failed = 1;

  if (!failed)
    rc = kafs_v6_journal_data_validate_coverage(desc, info.desc_bytes, &info.sb, info.file_size,
                                                &data_report);
  if (!failed &&
      (rc != 0 || !data_report.available || data_report.shard_count != 1u ||
       data_report.expected_count != 1u || data_report.covered_segments != 1u ||
       !data_report.lookup_available || data_report.lookup.data_bytes == 0u))
    failed = 1;

  if (!failed)
    rc = kafs_v6_journal_validate_segments_fd(info.fd, desc, info.desc_bytes, &info.sb,
                                              info.file_size, &segment_report);
  if (!failed &&
      (rc != 0 || !segment_report.available || segment_report.segment_count != 1u ||
       segment_report.valid_segments != 1u || segment_report.selected_segment_id != 0u ||
       segment_report.selected_generation != 1u || segment_report.has_invalid_header ||
       segment_report.has_torn_data || segment_report.has_read_error))
    failed = 1;

  free(desc);
  close_v6_image(&info);
  if (failed)
    return -1;

  char out[8192];
  char *fsck_argv[] = {(char *)kafs_test_fsck_bin(), (char *)img, NULL};
  if (run_cmd_capture(fsck_argv, 0, out, sizeof(out)) != 0 ||
      !strstr(out, "v6 journal segments:") || !strstr(out, "selected_generation=1"))
    return -1;

  char *dump_argv[] = {(char *)kafs_test_kafsdump_bin(), (char *)"--json", (char *)img, NULL};
  if (run_cmd_capture(dump_argv, 0, out, sizeof(out)) != 0 ||
      !strstr(out, "\"v6_journal_segments\"") || !strstr(out, "\"selected_generation\": 1"))
    return -1;
  return 0;
}

static int test_journal_header_gap_rejected(void)
{
  const char *img = "journal-header-gap.img";
  if (make_v6_image(img) != 0)
    return -1;
  if (mutate_all_descriptors(img, mutate_journal_header_gap, 1) != 0)
    return -1;

  void *desc = NULL;
  uint32_t desc_bytes = 0;
  kafs_ssuperblock_t sb;
  uint64_t file_size = 0;
  if (read_selected_descriptor_for_image(img, &desc, &desc_bytes, &sb, &file_size, NULL) != 0)
    return -1;

  kafs_v6_journal_header_coverage_report_t report;
  int rc = kafs_v6_journal_header_validate_coverage(desc, desc_bytes, &sb, file_size, &report);
  free(desc);
  if (rc == 0 || !report.available || !report.has_gap || !report.missing_coverage)
    return -1;

  char out[8192];
  char *fsck_argv[] = {(char *)kafs_test_fsck_bin(), (char *)img, NULL};
  if (run_cmd_capture(fsck_argv, 13, out, sizeof(out)) != 0 ||
      !strstr(out, "v6 journal header shards:") || !strstr(out, "has_gap=true"))
    return -1;
  return 0;
}

static int test_journal_torn_latest_segment_recovers(void)
{
  const char *img = "journal-torn-latest.img";
  if (make_v6_image(img) != 0)
    return -1;
  if (mutate_all_descriptors(img, mutate_journal_split_two_segments, 1) != 0)
    return -1;

  v6_image_info_t info;
  if (open_v6_image(img, 1, &info) != 0)
    return -1;

  void *desc = NULL;
  int failed = 0;
  if (read_descriptor(&info, 0, &desc) != 0)
    failed = 1;

  if (!failed &&
      (write_journal_segment_header(&info, desc, 0, 1) != 0 ||
       write_journal_segment_header(&info, desc, 1, 2) != 0))
    failed = 1;

  kafs_v6_journal_segment_lookup_t lookup;
  if (!failed && kafs_v6_journal_segment_lookup(desc, info.desc_bytes, 1, &lookup) != 0)
    failed = 1;
  if (!failed)
  {
    kj_header_t hdr;
    if (kafs_pread_all(info.fd, &hdr, sizeof(hdr), (off_t)lookup.header.header_off) != 0)
      failed = 1;
    else
    {
      hdr.header_crc ^= 1u;
      if (kafs_pwrite_all(info.fd, &hdr, sizeof(hdr), (off_t)lookup.header.header_off) != 0)
        failed = 1;
    }
  }

  if (!failed)
  {
    kafs_v6_journal_header_coverage_report_t header_report;
    kafs_v6_journal_data_coverage_report_t data_report;
    kafs_v6_journal_segment_report_t segment_report;
    int rc = kafs_v6_journal_header_validate_coverage(desc, info.desc_bytes, &info.sb,
                                                      info.file_size, &header_report);
    if (rc != 0 || header_report.expected_count != 2u)
      failed = 1;
    rc = kafs_v6_journal_data_validate_coverage(desc, info.desc_bytes, &info.sb, info.file_size,
                                                &data_report);
    if (!failed && (rc != 0 || data_report.expected_count != 2u))
      failed = 1;
    rc = kafs_v6_journal_validate_segments_fd(info.fd, desc, info.desc_bytes, &info.sb,
                                              info.file_size, &segment_report);
    if (!failed &&
        (rc != 0 || !segment_report.has_invalid_header || segment_report.valid_segments != 1u ||
         segment_report.selected_segment_id != 0u || segment_report.selected_generation != 1u))
      failed = 1;
  }

  free(desc);
  close_v6_image(&info);
  if (failed)
    return -1;

  char out[8192];
  char *fsck_argv[] = {(char *)kafs_test_fsck_bin(), (char *)img, NULL};
  if (run_cmd_capture(fsck_argv, 0, out, sizeof(out)) != 0 ||
      !strstr(out, "v6 journal segments:") || !strstr(out, "has_invalid_header=true") ||
      !strstr(out, "selected_segment=0"))
    return -1;

  char *dump_argv[] = {(char *)kafs_test_kafsdump_bin(), (char *)"--json", (char *)img, NULL};
  if (run_cmd_capture(dump_argv, 0, out, sizeof(out)) != 0 ||
      !strstr(out, "\"has_invalid_header\": true") || !strstr(out, "\"selected_segment\": 0"))
    return -1;
  return 0;
}

static int test_bitmap_overlap_rejected(void)
{
  const char *img = "bitmap-overlap.img";
  if (make_v6_image(img) != 0)
    return -1;
  if (mutate_all_descriptors(img, mutate_bitmap_duplicate_shard, 1) != 0)
    return -1;

  void *desc = NULL;
  uint32_t desc_bytes = 0;
  kafs_ssuperblock_t sb;
  uint64_t file_size = 0;
  if (read_selected_descriptor_for_image(img, &desc, &desc_bytes, &sb, &file_size, NULL) != 0)
    return -1;

  kafs_v6_bitmap_coverage_report_t report;
  int rc = kafs_v6_bitmap_validate_coverage(desc, desc_bytes, &sb, file_size, &report);
  free(desc);
  if (rc == 0 || !report.available || !report.has_overlap || !report.has_physical_overlap)
    return -1;

  char out[8192];
  char *dump_argv[] = {(char *)kafs_test_kafsdump_bin(), (char *)"--json", (char *)img, NULL};
  if (run_cmd_capture(dump_argv, 1, out, sizeof(out)) != 0 ||
      !strstr(out, "\"has_overlap\": true") ||
      !strstr(out, "\"has_physical_overlap\": true"))
    return -1;
  return 0;
}

static int test_allocator_summary_admission_rejects_gap(void)
{
  const char *img = "allocator-summary-admission-gap.img";
  if (make_v6_image(img) != 0)
    return -1;
  if (mutate_all_descriptors(img, mutate_allocator_gap, 1) != 0)
    return -1;

  v6_image_info_t info;
  if (open_v6_image(img, 1, &info) != 0)
    return -1;

  int failed = 0;
  void *map = MAP_FAILED;
  kafs_context_t ctx;
  memset(&ctx, 0, sizeof(ctx));

  map = mmap(NULL, (size_t)info.file_size, PROT_READ | PROT_WRITE, MAP_PRIVATE, info.fd, 0);
  if (map == MAP_FAILED)
    failed = 1;
  if (!failed)
  {
    ctx.c_img_base = map;
    ctx.c_img_size = (size_t)info.file_size;
    ctx.c_superblock = (kafs_ssuperblock_t *)map;
    ctx.c_fd = info.fd;

    kafs_v6_allocator_summary_coverage_report_t alloc_coverage;
    int rc = kafs_v6_descriptor_mapping_admit_fd(&ctx, info.fd, info.file_size, NULL, NULL,
                                                 &alloc_coverage, NULL, NULL);
    if (rc == 0 || !alloc_coverage.has_gap || !alloc_coverage.missing_coverage ||
        ctx.c_v6_bitmap_mapping_enabled || ctx.c_v6_inode_mapping_enabled ||
        ctx.c_v6_alloc_summary_mapping_enabled || ctx.c_v6_layout_desc ||
        ctx.c_v6_layout_desc_owned || ctx.c_v6_inode_shards || ctx.c_v6_alloc_summary_shards)
      failed = 1;
  }

  kafs_bitmap_descriptor_mapping_clear(&ctx);
  if (map != MAP_FAILED)
    munmap(map, (size_t)info.file_size);
  close_v6_image(&info);
  return failed ? -1 : 0;
}

static int test_hrl_admission_rejects_gap(void)
{
  const char *img = "hrl-admission-gap.img";
  if (make_v6_image(img) != 0)
    return -1;
  if (mutate_all_descriptors(img, mutate_hrl_index_gap, 1) != 0)
    return -1;

  v6_image_info_t info;
  if (open_v6_image(img, 1, &info) != 0)
    return -1;

  int failed = 0;
  void *map = MAP_FAILED;
  kafs_context_t ctx;
  memset(&ctx, 0, sizeof(ctx));

  map = mmap(NULL, (size_t)info.file_size, PROT_READ | PROT_WRITE, MAP_PRIVATE, info.fd, 0);
  if (map == MAP_FAILED)
    failed = 1;
  if (!failed)
  {
    ctx.c_img_base = map;
    ctx.c_img_size = (size_t)info.file_size;
    ctx.c_superblock = (kafs_ssuperblock_t *)map;
    ctx.c_fd = info.fd;

    kafs_v6_hrl_index_coverage_report_t hrl_index_coverage;
    int rc = kafs_v6_descriptor_mapping_admit_fd(&ctx, info.fd, info.file_size, NULL, NULL, NULL,
                                                 &hrl_index_coverage, NULL);
    if (rc == 0 || !hrl_index_coverage.has_gap || !hrl_index_coverage.missing_coverage ||
        ctx.c_v6_bitmap_mapping_enabled || ctx.c_v6_inode_mapping_enabled ||
        ctx.c_v6_alloc_summary_mapping_enabled || ctx.c_v6_hrl_mapping_enabled ||
        ctx.c_v6_layout_desc || ctx.c_v6_layout_desc_owned || ctx.c_v6_inode_shards ||
        ctx.c_v6_alloc_summary_shards || ctx.c_v6_hrl_index_shards ||
        ctx.c_v6_hrl_entry_shards)
      failed = 1;
  }

  kafs_bitmap_descriptor_mapping_clear(&ctx);
  if (map != MAP_FAILED)
    munmap(map, (size_t)info.file_size);
  close_v6_image(&info);
  return failed ? -1 : 0;
}

static int test_bitmap_admission_rejects_gap(void)
{
  const char *img = "bitmap-admission-gap.img";
  if (make_v6_image(img) != 0)
    return -1;
  if (mutate_all_descriptors(img, mutate_bitmap_gap, 1) != 0)
    return -1;

  v6_image_info_t info;
  if (open_v6_image(img, 1, &info) != 0)
    return -1;

  int failed = 0;
  void *map = MAP_FAILED;
  kafs_context_t ctx;
  memset(&ctx, 0, sizeof(ctx));

  map = mmap(NULL, (size_t)info.file_size, PROT_READ | PROT_WRITE, MAP_PRIVATE, info.fd, 0);
  if (map == MAP_FAILED)
    failed = 1;
  if (!failed)
  {
    ctx.c_img_base = map;
    ctx.c_img_size = (size_t)info.file_size;
    ctx.c_superblock = (kafs_ssuperblock_t *)map;
    ctx.c_fd = info.fd;

    kafs_v6_bitmap_coverage_report_t coverage;
    int rc = kafs_bitmap_descriptor_mapping_admit_fd(&ctx, info.fd, info.file_size, &coverage);
    if (rc == 0 || !coverage.has_gap || !coverage.missing_coverage ||
        ctx.c_v6_bitmap_mapping_enabled || ctx.c_v6_layout_desc || ctx.c_v6_layout_desc_owned)
      failed = 1;
  }

  kafs_bitmap_descriptor_mapping_clear(&ctx);
  if (map != MAP_FAILED)
    munmap(map, (size_t)info.file_size);
  close_v6_image(&info);
  return failed ? -1 : 0;
}

static int test_inode_admission_rejects_gap(void)
{
  const char *img = "inode-admission-gap.img";
  if (make_v6_image(img) != 0)
    return -1;
  if (mutate_all_descriptors(img, mutate_inode_gap, 1) != 0)
    return -1;

  v6_image_info_t info;
  if (open_v6_image(img, 1, &info) != 0)
    return -1;

  int failed = 0;
  void *map = MAP_FAILED;
  kafs_context_t ctx;
  memset(&ctx, 0, sizeof(ctx));

  map = mmap(NULL, (size_t)info.file_size, PROT_READ | PROT_WRITE, MAP_PRIVATE, info.fd, 0);
  if (map == MAP_FAILED)
    failed = 1;
  if (!failed)
  {
    ctx.c_img_base = map;
    ctx.c_img_size = (size_t)info.file_size;
    ctx.c_superblock = (kafs_ssuperblock_t *)map;
    ctx.c_fd = info.fd;

    kafs_v6_inode_coverage_report_t inode_coverage;
    int rc = kafs_v6_descriptor_mapping_admit_fd(&ctx, info.fd, info.file_size, NULL,
                                                 &inode_coverage, NULL, NULL, NULL);
    if (rc == 0 || !inode_coverage.has_gap || !inode_coverage.missing_coverage ||
        ctx.c_v6_bitmap_mapping_enabled || ctx.c_v6_inode_mapping_enabled ||
        ctx.c_v6_layout_desc || ctx.c_v6_layout_desc_owned || ctx.c_v6_inode_shards)
      failed = 1;
  }

  kafs_bitmap_descriptor_mapping_clear(&ctx);
  if (map != MAP_FAILED)
    munmap(map, (size_t)info.file_size);
  close_v6_image(&info);
  return failed ? -1 : 0;
}

typedef int (*test_fn_t)(void);

typedef struct validation_case
{
  const char *name;
  test_fn_t fn;
} validation_case_t;

int main(void)
{
  if (kafs_test_enter_tmpdir("v6_descriptor_validation") != 0)
    return 77;

  const validation_case_t cases[] = {
      {"anchor_crc_bad", test_anchor_crc_bad},
      {"all_descriptor_crc_bad", test_all_descriptor_crc_bad},
      {"descriptor_bounds_bad", test_descriptor_bounds_bad},
      {"unsupported_version", test_unsupported_version},
      {"incompat_flag", test_incompat_flag},
      {"primary_corrupt_backup_selected", test_primary_corrupt_backup_selected},
      {"stale_generation_reported", test_stale_generation_reported},
      {"divergent_same_generation_rejected", test_divergent_same_generation_rejected},
      {"bitmap_coverage_valid_and_lookup", test_bitmap_coverage_valid_and_lookup},
      {"inode_coverage_valid_and_lookup", test_inode_coverage_valid_and_lookup},
      {"allocator_summary_coverage_valid_and_lookup",
       test_allocator_summary_coverage_valid_and_lookup},
      {"allocator_summary_runtime_allocation_mapping",
       test_allocator_summary_runtime_allocation_mapping},
      {"hrl_coverage_valid_lookup_and_chains", test_hrl_coverage_valid_lookup_and_chains},
      {"hrl_runtime_shard_mapping", test_hrl_runtime_shard_mapping},
      {"inode_runtime_descriptor_mapping", test_inode_runtime_descriptor_mapping},
      {"inode_runtime_allocation_mapping", test_inode_runtime_allocation_mapping},
      {"bitmap_runtime_descriptor_mapping", test_bitmap_runtime_descriptor_mapping},
      {"bitmap_multi_shard_runtime_mapping", test_bitmap_multi_shard_runtime_mapping},
      {"bitmap_gap_rejected", test_bitmap_gap_rejected},
      {"inode_gap_rejected", test_inode_gap_rejected},
      {"allocator_summary_gap_rejected", test_allocator_summary_gap_rejected},
      {"hrl_index_gap_rejected", test_hrl_index_gap_rejected},
      {"hrl_entries_gap_rejected", test_hrl_entries_gap_rejected},
      {"hrl_chain_out_of_range_rejected", test_hrl_chain_out_of_range_rejected},
      {"journal_coverage_valid_lookup_and_segments", test_journal_coverage_valid_lookup_and_segments},
      {"journal_header_gap_rejected", test_journal_header_gap_rejected},
      {"journal_torn_latest_segment_recovers", test_journal_torn_latest_segment_recovers},
      {"bitmap_overlap_rejected", test_bitmap_overlap_rejected},
      {"allocator_summary_admission_rejects_gap", test_allocator_summary_admission_rejects_gap},
      {"hrl_admission_rejects_gap", test_hrl_admission_rejects_gap},
      {"bitmap_admission_rejects_gap", test_bitmap_admission_rejects_gap},
      {"inode_admission_rejects_gap", test_inode_admission_rejects_gap},
  };

  for (size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); ++i)
  {
    if (cases[i].fn() != 0)
    {
      tlogf("case failed: %s", cases[i].name);
      return 1;
    }
  }

  return 0;
}
