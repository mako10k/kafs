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

static int make_v6_image(const char *img)
{
  char out[2048];
  char *argv[] = {(char *)kafs_test_mkfs_bin(), (char *)img, (char *)"--format-version",
                  (char *)"6", (char *)"--size-bytes", (char *)"64M", (char *)"--yes", NULL};
  int rc = run_cmd_capture(argv, 0, out, sizeof(out));
  if (rc != 0)
    tlogf("mkfs failed for %s: %s", img, out);
  return rc;
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

static int test_bitmap_runtime_descriptor_mapping(void)
{
  const char *img = "bitmap-runtime-mapping.img";
  if (make_v6_image(img) != 0)
    return -1;

  v6_image_info_t info;
  if (open_v6_image(img, 1, &info) != 0)
    return -1;

  int failed = 0;
  void *desc = NULL;
  uint32_t desc_bytes = 0;
  void *map = MAP_FAILED;
  kafs_v6_layout_report_t layout;
  int rc = kafs_v6_discover_layout(info.fd, &info.sb, info.file_size, &layout);
  if (rc == 0)
    rc = kafs_v6_read_selected_descriptor(info.fd, &layout, &desc, &desc_bytes);
  if (rc != 0)
    failed = 1;

  if (!failed)
  {
    map = mmap(NULL, (size_t)info.file_size, PROT_READ | PROT_WRITE, MAP_PRIVATE, info.fd, 0);
    if (map == MAP_FAILED)
      failed = 1;
  }

  if (!failed)
  {
    kafs_context_t ctx;
    memset(&ctx, 0, sizeof(ctx));
    ctx.c_img_base = map;
    ctx.c_img_size = (size_t)info.file_size;
    ctx.c_superblock = (kafs_ssuperblock_t *)map;
    ctx.c_fd = info.fd;
    ctx.c_v6_bitmap_mapping_enabled = 1u;
    ctx.c_v6_layout_desc = desc;
    ctx.c_v6_layout_desc_bytes = desc_bytes;

    kafs_blkcnt_t blo = kafs_sb_first_data_block_get(ctx.c_superblock);
    if (blo >= kafs_sb_blkcnt_get(ctx.c_superblock))
      failed = 1;

    kafs_v6_bitmap_lookup_t lookup;
    if (!failed && kafs_v6_bitmap_lookup(desc, desc_bytes, (uint64_t)blo, &lookup) != 0)
      failed = 1;
    if (!failed && lookup.bitmap_byte_off >= info.file_size)
      failed = 1;

    if (!failed)
    {
      uint8_t *bitmap_byte = (uint8_t *)map + lookup.bitmap_byte_off;
      uint8_t bitmap_mask = (uint8_t)(1u << lookup.bitmap_bit);
      *bitmap_byte &= (uint8_t)~bitmap_mask;

      uint64_t writes_before = ctx.c_meta_region_writes[KAFS_META_REGION_BLOCK_BITMAP];
      uint64_t bytes_before = ctx.c_meta_region_bytes[KAFS_META_REGION_BLOCK_BITMAP];
      if (kafs_blk_get_usage(&ctx, blo) != 0)
        failed = 1;
      if (!failed && kafs_blk_set_usage_nolock(&ctx, blo, KAFS_TRUE) != 0)
        failed = 1;
      if (!failed && ((*bitmap_byte & bitmap_mask) == 0u || !kafs_blk_get_usage(&ctx, blo)))
        failed = 1;
      if (!failed &&
          (ctx.c_meta_region_writes[KAFS_META_REGION_BLOCK_BITMAP] <= writes_before ||
           ctx.c_meta_region_bytes[KAFS_META_REGION_BLOCK_BITMAP] <= bytes_before))
        failed = 1;
      if (!failed && kafs_blk_set_usage_nolock(&ctx, blo, KAFS_FALSE) != 0)
        failed = 1;
      if (!failed && ((*bitmap_byte & bitmap_mask) != 0u || kafs_blk_get_usage(&ctx, blo) != 0))
        failed = 1;
    }
  }

  if (map != MAP_FAILED)
    munmap(map, (size_t)info.file_size);
  free(desc);
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
      {"bitmap_runtime_descriptor_mapping", test_bitmap_runtime_descriptor_mapping},
      {"bitmap_gap_rejected", test_bitmap_gap_rejected},
      {"bitmap_overlap_rejected", test_bitmap_overlap_rejected},
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
