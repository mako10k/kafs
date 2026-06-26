#include "kafs.h"
#include "kafs_cli_opts.h"
#include "kafs_hash.h"
#include "kafs_inode.h"
#include "kafs_superblock.h"
#include "kafs_tailmeta.h"
#include "kafs_tool_util.h"
#include "kafs_v6_layout.h"

#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <limits.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/ioctl.h>
#include <sys/wait.h>
#include <unistd.h>
#ifdef __linux__
#include <linux/fs.h>
#endif

#define KAFSRESIZE_DEFAULT_BLKSIZE_LOG 12
#define KAFSRESIZE_DEFAULT_JOURNAL_BYTES (1u << 20)
#define KAFSRESIZE_DEFAULT_HRL_ENTRY_RATIO 0.75

typedef struct kafsresize_mkfs_layout
{
  uint64_t mapsize;
  uint64_t first_data_block;
  uint64_t block_count;
  uint64_t data_block_capacity;
  uint64_t v6_desc_off;
  uint32_t v6_desc_bytes;
  uint64_t v6_candidates[KAFS_V6_LAYOUT_REPLICA_MAX_COUNT];
  uint32_t v6_candidate_count;
  uint64_t hrl_entry_count;
} kafsresize_mkfs_layout_t;

typedef struct kafsresize_source_info
{
  uint32_t format_version;
  uint64_t file_size;
  uint64_t block_size;
  uint64_t block_count;
  uint64_t first_data_block;
  uint64_t free_blocks;
  uint64_t used_data_blocks;
  uint64_t inode_count;
  uint64_t free_inodes;
  uint64_t used_inodes;
  uint64_t checkpoint_seq;
  uint64_t commit_seq;
} kafsresize_source_info_t;

typedef struct kafsresize_migrate_create_plan
{
  uint32_t target_format;
  uint64_t size_bytes;
  uint32_t inodes;
  int blksize_log;
  uint64_t journal_bytes;
  double hrl_entry_ratio;
  int has_source;
  kafsresize_source_info_t source;
  kafsresize_mkfs_layout_t layout;
} kafsresize_migrate_create_plan_t;

static void usage(const char *prog)
{
  fprintf(
      stderr,
      "Usage: %s --grow --size-bytes N <image>\n"
      "       %s --migrate-create --dst-image <image> [--size-bytes N] --inodes I [options]\n"
      "  notes:\n"
      "    - grow-only (no shrink)\n"
      "    - v0 grow supports only preallocated headroom (s_blkcnt < s_r_blkcnt)\n"
      "    - migrate-create builds a new image with target size/inodes via mkfs.kafs\n"
      "    - migrate-create without --size-bytes auto-detects size from --dst-image\n"
      "  options for --migrate-create:\n"
      "    --src-image IMAGE       source image used by v6 migration precheck/dry-run\n"
      "    --format-version V      on-disk format version passed to mkfs.kafs\n"
      "    --journal-size-bytes N   journal size passed to mkfs.kafs\n"
      "    --blksize-log L          block-size log2 passed to mkfs.kafs\n"
      "    --hrl-entry-ratio R      HRL entries/data-block ratio passed to mkfs.kafs\n"
      "    --src-mount PATH         print suggested rsync source mount\n"
      "    --dst-mount PATH         print suggested destination mount\n"
      "    --dry-run                validate migration-create inputs without writing dst-image\n"
      "    --yes                    skip confirmation prompt\n"
      "    --force                  overwrite existing --dst-image\n",
      prog, prog);
}

static int confirm_yes_stdin(void)
{
  fprintf(stderr,
          "WARNING: this operation may overwrite destination image. Type 'YES' to continue: ");
  fflush(stderr);
  char buf[32];
  if (!fgets(buf, sizeof(buf), stdin))
    return 0;
  buf[strcspn(buf, "\r\n")] = '\0';
  return strcmp(buf, "YES") == 0;
}

static const char *resolve_mkfs_prog(void)
{
  const char *envv = getenv("KAFS_MKFS");
  if (envv && *envv)
    return envv;

#ifdef __linux__
  char exe_path[PATH_MAX];
  static char sibling[PATH_MAX];
  ssize_t n = readlink("/proc/self/exe", exe_path, sizeof(exe_path) - 1u);
  if (n > 0)
  {
    exe_path[n] = '\0';
    char *slash = strrchr(exe_path, '/');
    if (slash)
    {
      size_t dir_len = (size_t)(slash - exe_path) + 1u;
      if (snprintf(sibling, sizeof(sibling), "%.*smkfs.kafs", (int)dir_len, exe_path) <
              (int)sizeof(sibling) &&
          access(sibling, X_OK) == 0)
        return sibling;
    }
  }
#endif

  if (access("./src/mkfs.kafs", X_OK) == 0)
    return "./src/mkfs.kafs";
  return "mkfs.kafs";
}

static int run_command(const char *prog, char *const argv[])
{
  pid_t pid = fork();
  if (pid < 0)
  {
    perror("fork");
    return 1;
  }
  if (pid == 0)
  {
    execvp(prog, argv);
    perror("execvp");
    _exit(127);
  }

  int status = 0;
  if (waitpid(pid, &status, 0) < 0)
  {
    perror("waitpid");
    return 1;
  }
  if (WIFEXITED(status))
    return WEXITSTATUS(status);
  return 1;
}

static void print_migrate_next_steps(const char *dst_image, const char *src_mount,
                                     const char *dst_mount)
{
  printf("\nnext steps (manual cutover):\n");
  if (dst_mount && *dst_mount)
  {
    printf("  1) sudo mkdir -p %s\n", dst_mount);
    printf("  2) sudo kafs %s %s\n", dst_image, dst_mount);
    if (src_mount && *src_mount)
    {
      printf("  3) initial seed copy:\n");
      printf("     sudo rsync -aHAX --numeric-ids --delete %s/ %s/\n", src_mount, dst_mount);
      printf("  4) final low-transfer sync before cutover:\n");
      printf("     sudo rsync -aHAX --numeric-ids --delete --inplace --no-whole-file %s/ %s/\n",
             src_mount, dst_mount);
      printf("  5) verify then switch mountpoint\n");
    }
    else
    {
      printf("  3) copy data from source mount to %s\n", dst_mount);
      printf("  4) re-run a low-transfer rsync before cutover if source changed\n");
      printf("  5) verify then switch mountpoint\n");
    }
  }
  else
  {
    printf("  - mount destination image and copy data from source filesystem\n");
    printf(
        "  - before cutover, re-run rsync with --inplace --no-whole-file to minimize transfer\n");
  }
}

static void bitmap_clear_range(uint8_t *bm, uint32_t from_blo, uint32_t to_blo)
{
  for (uint32_t b = from_blo; b < to_blo; ++b)
  {
    uint32_t byte = b >> 3;
    uint32_t bit = b & 7u;
    bm[byte] &= (uint8_t) ~(1u << bit);
  }
}

static int load_superblock_checked(int fd, kafs_ssuperblock_t *sb)
{
  int rc = kafs_pread_all(fd, sb, sizeof(*sb), 0);
  if (rc != 0)
    return rc;
  if (kafs_sb_magic_get(sb) != KAFS_MAGIC)
    return -EINVAL;
  uint32_t format_version = kafs_sb_format_version_get(sb);
  if (format_version != KAFS_FORMAT_VERSION && format_version != KAFS_FORMAT_VERSION_V5)
    return -EINVAL;
  return 0;
}

static int detect_dst_size_bytes(const char *path, uint64_t *out_size)
{
  if (!path || !out_size)
    return -EINVAL;

  struct stat st;
  if (stat(path, &st) != 0)
    return -errno;

  if (S_ISREG(st.st_mode))
  {
    if (st.st_size <= 0)
      return -EINVAL;
    *out_size = (uint64_t)st.st_size;
    return 0;
  }

  if (S_ISBLK(st.st_mode))
  {
#ifdef __linux__
    int fd = open(path, O_RDONLY);
    if (fd < 0)
      return -errno;
    uint64_t dev_bytes = 0;
    if (ioctl(fd, BLKGETSIZE64, &dev_bytes) != 0)
    {
      int e = errno;
      close(fd);
      return -e;
    }
    close(fd);
    if (dev_bytes == 0)
      return -EINVAL;
    *out_size = dev_bytes;
    return 0;
#else
    return -ENOTSUP;
#endif
  }

  return -EINVAL;
}

static int resolve_migrate_create_size(const char *dst_image, uint64_t *size_bytes)
{
  if (!size_bytes || *size_bytes != 0)
    return 0;

  uint64_t auto_size = 0;
  int drc = detect_dst_size_bytes(dst_image, &auto_size);
  if (drc != 0)
  {
    fprintf(stderr, "failed to auto-detect size from --dst-image '%s' (use --size-bytes): %s\n",
            dst_image, strerror(-drc));
    return 2;
  }
  *size_bytes = auto_size;
  return 0;
}

static int kafsresize_format_version_is_supported(uint32_t format_version)
{
  return format_version == KAFS_FORMAT_VERSION || format_version == KAFS_FORMAT_VERSION_V5 ||
         format_version == KAFS_FORMAT_VERSION_V6;
}

static uint32_t kafsresize_resolve_target_format(uint32_t format_version)
{
  return (format_version > 0) ? format_version : (uint32_t)KAFS_FORMAT_VERSION_V5;
}

static uint64_t kafsresize_align_up_u64(uint64_t value, uint64_t align_mask)
{
  return (value + align_mask) & ~align_mask;
}

static int kafsresize_collect_source_info(const char *src_image, kafsresize_source_info_t *out)
{
  if (!src_image || !*src_image || !out)
    return -EINVAL;

  int fd = open(src_image, O_RDONLY);
  if (fd < 0)
  {
    fprintf(stderr, "failed to open source image '%s': %s\n", src_image, strerror(errno));
    return 1;
  }

  struct stat st;
  if (fstat(fd, &st) != 0)
  {
    fprintf(stderr, "failed to stat source image '%s': %s\n", src_image, strerror(errno));
    close(fd);
    return 1;
  }

  kafs_ssuperblock_t sb;
  int rc = kafs_pread_all(fd, &sb, sizeof(sb), 0);
  close(fd);
  if (rc != 0)
  {
    fprintf(stderr, "failed to read source superblock: %s\n", strerror(-rc));
    return 1;
  }
  if (kafs_sb_magic_get(&sb) != KAFS_MAGIC)
  {
    fprintf(stderr, "source image is not a KAFS image\n");
    return 1;
  }

  memset(out, 0, sizeof(*out));
  out->format_version = kafs_sb_format_version_get(&sb);
  if (out->format_version != KAFS_FORMAT_VERSION_V5)
  {
    fprintf(stderr, "unsupported source format_version=%" PRIu32 " (expected v5)\n",
            out->format_version);
    return 1;
  }

  out->file_size = S_ISREG(st.st_mode) ? (uint64_t)st.st_size : 0u;
  out->block_size = 1ull << kafs_sb_log_blksize_get(&sb);
  out->block_count = kafs_sb_blkcnt_get(&sb);
  out->first_data_block = kafs_sb_first_data_block_get(&sb);
  out->free_blocks = kafs_sb_blkcnt_free_get(&sb);
  out->inode_count = kafs_sb_inocnt_get(&sb);
  out->free_inodes = kafs_sb_inocnt_free_get(&sb);
  out->checkpoint_seq = kafs_sb_checkpoint_seq_get(&sb);
  out->commit_seq = kafs_sb_commit_seq_get(&sb);

  if (out->commit_seq != out->checkpoint_seq)
  {
    fprintf(stderr,
            "dirty source image: checkpoint_seq=%" PRIu64 ", commit_seq=%" PRIu64
            " (run fsck/replay before migration)\n",
            out->checkpoint_seq, out->commit_seq);
    return 1;
  }
  if (out->block_count == 0 || out->free_blocks > out->block_count ||
      out->first_data_block > out->block_count)
  {
    fprintf(stderr, "source image has invalid block counters\n");
    return 1;
  }
  if (out->inode_count == 0 || out->free_inodes > out->inode_count)
  {
    fprintf(stderr, "source image has invalid inode counters\n");
    return 1;
  }

  uint64_t used_blocks = out->block_count - out->free_blocks;
  if (used_blocks < out->first_data_block)
  {
    fprintf(stderr, "source image has inconsistent metadata/free block counters\n");
    return 1;
  }
  out->used_data_blocks = used_blocks - out->first_data_block;
  out->used_inodes = out->inode_count - out->free_inodes;
  return 0;
}

static void kafsresize_compute_mkfs_layout_once(uint32_t format_version, uint64_t block_count,
                                                uint64_t block_mask, uint64_t block_size,
                                                uint64_t inode_count, uint64_t journal_bytes,
                                                double hrl_entry_ratio,
                                                kafsresize_mkfs_layout_t *out)
{
  memset(out, 0, sizeof(*out));

  uint64_t mapsize = sizeof(kafs_ssuperblock_t);
  mapsize = kafsresize_align_up_u64(mapsize, block_mask);

  if (format_version == KAFS_FORMAT_VERSION_V6)
  {
    out->v6_desc_off = mapsize;
    out->v6_desc_bytes = kafs_v6_descriptor_bytes_for_block((uint32_t)block_size);
    mapsize += out->v6_desc_bytes;
    mapsize = kafsresize_align_up_u64(mapsize, block_mask);
  }

  mapsize += (block_count + 7u) >> 3;
  mapsize = kafsresize_align_up_u64(mapsize, 7u);
  mapsize = kafsresize_align_up_u64(mapsize, block_mask);

  mapsize += kafs_inode_table_bytes_for_format(format_version, inode_count);
  mapsize = kafsresize_align_up_u64(mapsize, block_mask);

  uint64_t allocator_size = 0;
  if (kafs_v6_allocator_summary_shape(block_count, &(uint64_t){0}, &(uint64_t){0}, &(uint64_t){0},
                                      &allocator_size) != 0 ||
      allocator_size < 4096u)
    allocator_size = 4096u;
  allocator_size = kafsresize_align_up_u64(allocator_size, block_mask);
  mapsize += allocator_size;
  mapsize = kafsresize_align_up_u64(mapsize, block_mask);

  uint32_t bucket_count = 1024u;
  while ((bucket_count << 1u) <= (uint32_t)(block_count / 4u))
    bucket_count <<= 1u;
  mapsize += (uint64_t)bucket_count * sizeof(uint32_t);
  mapsize = kafsresize_align_up_u64(mapsize, 7u);

  uint64_t hrl_entry_count = (uint64_t)((double)block_count * hrl_entry_ratio);
  if (hrl_entry_count == 0 && block_count > 0)
    hrl_entry_count = 1;
  if (hrl_entry_count > block_count)
    hrl_entry_count = block_count;
  out->hrl_entry_count = hrl_entry_count;
  mapsize += hrl_entry_count * (uint64_t)sizeof(kafs_hrl_entry_t);
  mapsize = kafsresize_align_up_u64(mapsize, block_mask);

  mapsize += journal_bytes;
  mapsize = kafsresize_align_up_u64(mapsize, block_mask);

  mapsize += kafsresize_align_up_u64(1u << 20, block_mask);
  mapsize = kafsresize_align_up_u64(mapsize, block_mask);

  if (format_version == KAFS_FORMAT_VERSION_V5)
    mapsize += kafs_tailmeta_default_region_bytes((kafs_blksize_t)block_size);
  mapsize = kafsresize_align_up_u64(mapsize, block_mask);

  out->mapsize = mapsize;
  out->first_data_block = mapsize >> __builtin_ctzll(block_size);
  out->block_count = block_count;
  out->data_block_capacity =
      (block_count > out->first_data_block) ? (block_count - out->first_data_block) : 0u;
}

static int kafsresize_compute_mkfs_layout(uint32_t format_version, uint64_t total_bytes,
                                          int blksize_log, uint64_t inode_count,
                                          uint64_t journal_bytes, double hrl_entry_ratio,
                                          kafsresize_mkfs_layout_t *out)
{
  if (!out || total_bytes == 0 || inode_count == 0 || blksize_log <= 0 || blksize_log >= 63)
    return -EINVAL;

  uint64_t block_size = 1ull << blksize_log;
  uint64_t block_mask = block_size - 1u;
  uint64_t block_count = total_bytes >> blksize_log;
  if (block_count == 0 || block_count > UINT32_MAX)
    return -ERANGE;

  kafsresize_mkfs_layout_t layout;
  for (int i = 0; i < 16; ++i)
  {
    kafsresize_compute_mkfs_layout_once(format_version, block_count, block_mask, block_size,
                                        inode_count, journal_bytes, hrl_entry_ratio, &layout);
    if (total_bytes <= layout.mapsize)
      return -ERANGE;
    uint64_t next = (total_bytes - layout.mapsize) >> blksize_log;
    if (next == block_count)
      break;
    if (next == 0 || next > UINT32_MAX)
      return -ERANGE;
    block_count = next;
  }

  for (;;)
  {
    kafsresize_compute_mkfs_layout_once(format_version, block_count, block_mask, block_size,
                                        inode_count, journal_bytes, hrl_entry_ratio, &layout);
    if (block_count > (UINT64_MAX >> blksize_log))
      return -ERANGE;
    uint64_t imgsize = layout.mapsize + (block_count << blksize_log);
    if (imgsize <= total_bytes)
      break;
    if (block_count == 0)
      return -ERANGE;
    block_count--;
  }

  if (format_version == KAFS_FORMAT_VERSION_V6)
  {
    if (journal_bytes <= block_size)
      return -EINVAL;
    int rc = kafs_v6_candidate_offsets(total_bytes, (uint32_t)block_size, layout.v6_desc_off,
                                       layout.v6_desc_bytes, layout.v6_candidates,
                                       &layout.v6_candidate_count);
    if (rc != 0)
      return rc;
    for (uint32_t i = 1; i < layout.v6_candidate_count; ++i)
      if (layout.v6_candidates[i] < layout.mapsize)
        return -ERANGE;
  }

  *out = layout;
  return 0;
}

static int kafsresize_prepare_migrate_create_plan(const char *src_image, const char *dst_image,
                                                  uint64_t size_bytes, uint32_t inodes,
                                                  uint32_t format_version, uint64_t journal_bytes,
                                                  int blksize_log, double hrl_entry_ratio,
                                                  kafsresize_migrate_create_plan_t *plan)
{
  if (!plan)
    return 2;

  uint32_t target_format = kafsresize_resolve_target_format(format_version);
  if (!kafsresize_format_version_is_supported(target_format))
  {
    fprintf(stderr, "unsupported format version: %" PRIu32 "\n", target_format);
    return 2;
  }
  if (target_format == KAFS_FORMAT_VERSION_V6 && (!src_image || !*src_image))
  {
    fprintf(stderr, "--format-version 6 requires --src-image\n");
    return 2;
  }

  int size_rc = resolve_migrate_create_size(dst_image, &size_bytes);
  if (size_rc != 0)
    return size_rc;

  int resolved_blksize_log = (blksize_log > 0) ? blksize_log : KAFSRESIZE_DEFAULT_BLKSIZE_LOG;
  uint64_t resolved_journal_bytes =
      (journal_bytes > 0) ? journal_bytes : (uint64_t)KAFSRESIZE_DEFAULT_JOURNAL_BYTES;
  if (resolved_journal_bytes < 4096u)
    resolved_journal_bytes = 4096u;
  double resolved_hrl_entry_ratio =
      (hrl_entry_ratio > 0.0) ? hrl_entry_ratio : KAFSRESIZE_DEFAULT_HRL_ENTRY_RATIO;

  kafsresize_source_info_t source = {0};
  if (src_image && *src_image)
  {
    int src_rc = kafsresize_collect_source_info(src_image, &source);
    if (src_rc != 0)
      return src_rc;
  }

  kafsresize_mkfs_layout_t layout;
  int rc =
      kafsresize_compute_mkfs_layout(target_format, size_bytes, resolved_blksize_log, inodes,
                                     resolved_journal_bytes, resolved_hrl_entry_ratio, &layout);
  if (rc != 0)
  {
    if (target_format == KAFS_FORMAT_VERSION_V6 && rc == -EINVAL)
      fprintf(stderr, "format v6 requires journal size greater than one filesystem block\n");
    else if (target_format == KAFS_FORMAT_VERSION_V6)
      fprintf(stderr, "image too small for format v6 descriptor replicas\n");
    else
      fprintf(stderr, "invalid total size: %" PRIu64 "\n", size_bytes);
    return 1;
  }

  if (src_image && *src_image)
  {
    if ((uint64_t)inodes < source.used_inodes)
    {
      fprintf(stderr,
              "destination inode count too small: inodes=%" PRIu32 ", source_used=%" PRIu64 "\n",
              inodes, source.used_inodes);
      return 1;
    }
    if (layout.data_block_capacity < source.used_data_blocks)
    {
      fprintf(stderr,
              "destination data capacity too small: capacity_blocks=%" PRIu64
              ", source_used_data_blocks=%" PRIu64 "\n",
              layout.data_block_capacity, source.used_data_blocks);
      return 1;
    }
  }

  memset(plan, 0, sizeof(*plan));
  plan->target_format = target_format;
  plan->size_bytes = size_bytes;
  plan->inodes = inodes;
  plan->blksize_log = resolved_blksize_log;
  plan->journal_bytes = resolved_journal_bytes;
  plan->hrl_entry_ratio = resolved_hrl_entry_ratio;
  plan->has_source = (src_image && *src_image);
  plan->source = source;
  plan->layout = layout;
  return 0;
}

static void kafsresize_print_migrate_create_dry_run(const char *src_image, const char *dst_image,
                                                    const kafsresize_migrate_create_plan_t *plan)
{
  printf("kafsresize: migrate-create dry-run PASS\n");
  if (plan->has_source)
  {
    printf("  src_image: %s\n", src_image);
    printf("  source_format_version: %" PRIu32 "\n", plan->source.format_version);
    printf("  source_used_data_blocks: %" PRIu64 "\n", plan->source.used_data_blocks);
    printf("  source_used_inodes: %" PRIu64 "\n", plan->source.used_inodes);
  }
  printf("  dst_image: %s\n", dst_image);
  printf("  size_bytes: %" PRIu64 "\n", plan->size_bytes);
  printf("  inodes: %" PRIu32 "\n", plan->inodes);
  printf("  format_version: %" PRIu32 "\n", plan->target_format);
  printf("  block_size: %" PRIu64 "\n", (uint64_t)(1ull << plan->blksize_log));
  printf("  journal_bytes: %" PRIu64 "\n", plan->journal_bytes);
  printf("  hrl_entry_ratio: %.6f\n", plan->hrl_entry_ratio);
  printf("  metadata_bytes: %" PRIu64 "\n", plan->layout.mapsize);
  printf("  first_data_block: %" PRIu64 "\n", plan->layout.first_data_block);
  printf("  data_block_capacity: %" PRIu64 "\n", plan->layout.data_block_capacity);
  if (plan->target_format == KAFS_FORMAT_VERSION_V6)
  {
    printf("  v6_descriptor_bytes: %" PRIu32 "\n", plan->layout.v6_desc_bytes);
    printf("  v6_descriptor_replicas: %" PRIu32 "\n", plan->layout.v6_candidate_count);
    printf("  v6_descriptor_offsets:");
    for (uint32_t i = 0; i < plan->layout.v6_candidate_count; ++i)
      printf(" %" PRIu64, plan->layout.v6_candidates[i]);
    printf("\n");
    printf("  v6_group_policy: descriptor-backed metadata groups\n");
  }
  printf("  writes_performed: no\n");
}

static int cmd_migrate_create_dry_run(const char *src_image, const char *dst_image,
                                      uint64_t size_bytes, uint32_t inodes, uint32_t format_version,
                                      uint64_t journal_bytes, int blksize_log,
                                      double hrl_entry_ratio)
{
  kafsresize_migrate_create_plan_t plan;
  int rc = kafsresize_prepare_migrate_create_plan(src_image, dst_image, size_bytes, inodes,
                                                  format_version, journal_bytes, blksize_log,
                                                  hrl_entry_ratio, &plan);
  if (rc != 0)
    return rc;

  kafsresize_print_migrate_create_dry_run(src_image, dst_image, &plan);
  return 0;
}

static int cmd_grow(const char *image, uint64_t target_bytes)
{
  int fd = open(image, O_RDWR);
  if (fd < 0)
  {
    perror("open");
    return 1;
  }

  kafs_ssuperblock_t sb;
  int rc = load_superblock_checked(fd, &sb);
  if (rc != 0)
  {
    if (rc == -EINVAL)
      fprintf(stderr, "unsupported or invalid image format\n");
    else
      fprintf(stderr, "failed to read superblock: %s\n", strerror(-rc));
    close(fd);
    return 1;
  }

  uint32_t log_blksize = kafs_sb_log_blksize_get(&sb);
  uint64_t blksize = 1ull << log_blksize;
  uint32_t old_blkcnt = (uint32_t)kafs_sb_blkcnt_get(&sb);
  uint32_t max_blkcnt = (uint32_t)kafs_sb_r_blkcnt_get(&sb);
  uint32_t old_free = (uint32_t)kafs_sb_blkcnt_free_get(&sb);

  if (max_blkcnt <= old_blkcnt)
  {
    fprintf(stderr,
            "no grow headroom: s_blkcnt=%" PRIu32 ", s_r_blkcnt=%" PRIu32
            " (v0 only supports grow within preallocated headroom)\n",
            old_blkcnt, max_blkcnt);
    close(fd);
    return 1;
  }

  uint64_t target_blkcnt_u64 = target_bytes / blksize;
  if (target_blkcnt_u64 > UINT32_MAX)
  {
    fprintf(stderr, "target size is too large\n");
    close(fd);
    return 1;
  }
  uint32_t target_blkcnt = (uint32_t)target_blkcnt_u64;

  if (target_blkcnt <= old_blkcnt)
  {
    fprintf(stderr, "target is not larger than current size (%" PRIu32 " blocks)\n", old_blkcnt);
    close(fd);
    return 1;
  }
  if (target_blkcnt > max_blkcnt)
  {
    fprintf(stderr,
            "target exceeds preallocated headroom: target=%" PRIu32 ", max=%" PRIu32 " blocks\n",
            target_blkcnt, max_blkcnt);
    close(fd);
    return 1;
  }

  uint64_t need_bytes = (uint64_t)target_blkcnt * blksize;
  struct stat st;
  if (fstat(fd, &st) != 0)
  {
    perror("fstat");
    close(fd);
    return 1;
  }
  if ((uint64_t)st.st_size < need_bytes)
  {
    if (ftruncate(fd, (off_t)need_bytes) != 0)
    {
      perror("ftruncate");
      close(fd);
      return 1;
    }
  }

  uint64_t mapsize = sizeof(kafs_ssuperblock_t);
  uint64_t align = blksize - 1u;
  mapsize = (mapsize + align) & ~align;
  uint64_t blkmask_off = mapsize;
  uint64_t blkmask_bytes = ((uint64_t)max_blkcnt + 7u) >> 3;

  uint8_t *bm = (uint8_t *)malloc((size_t)blkmask_bytes);
  if (!bm)
  {
    fprintf(stderr, "malloc failed\n");
    close(fd);
    return 1;
  }

  rc = kafs_pread_all(fd, bm, (size_t)blkmask_bytes, (off_t)blkmask_off);
  if (rc != 0)
  {
    fprintf(stderr, "failed to read block bitmap: %s\n", strerror(-rc));
    free(bm);
    close(fd);
    return 1;
  }

  bitmap_clear_range(bm, old_blkcnt, target_blkcnt);

  rc = kafs_pwrite_all(fd, bm, (size_t)blkmask_bytes, (off_t)blkmask_off);
  if (rc != 0)
  {
    fprintf(stderr, "failed to write block bitmap: %s\n", strerror(-rc));
    free(bm);
    close(fd);
    return 1;
  }

  free(bm);

  uint32_t delta = target_blkcnt - old_blkcnt;
  if (UINT32_MAX - old_free < delta)
  {
    fprintf(stderr, "free block counter overflow\n");
    close(fd);
    return 1;
  }

  sb.s_blkcnt = kafs_blkcnt_htos((kafs_blkcnt_t)target_blkcnt);
  sb.s_blkcnt_free = kafs_blkcnt_htos((kafs_blkcnt_t)(old_free + delta));
  kafs_sb_wtime_set(&sb, kafs_now());

  rc = kafs_pwrite_all(fd, &sb, sizeof(sb), 0);
  if (rc != 0)
  {
    fprintf(stderr, "failed to write superblock: %s\n", strerror(-rc));
    close(fd);
    return 1;
  }

  if (fsync(fd) != 0)
  {
    perror("fsync");
    close(fd);
    return 1;
  }

  printf("kafsresize: grow completed\n");
  printf("  image: %s\n", image);
  printf("  block_size: %" PRIu64 "\n", blksize);
  printf("  old_blocks: %" PRIu32 "\n", old_blkcnt);
  printf("  new_blocks: %" PRIu32 "\n", target_blkcnt);
  printf("  old_free_blocks: %" PRIu32 "\n", old_free);
  printf("  new_free_blocks: %" PRIu32 "\n", old_free + delta);
  printf("  logical_bytes: %" PRIu64 "\n", need_bytes);

  close(fd);
  return 0;
}

static int cmd_migrate_create(const char *src_image, const char *dst_image, uint64_t size_bytes,
                              uint32_t inodes, uint32_t format_version, uint64_t journal_bytes,
                              int blksize_log, double hrl_entry_ratio, const char *src_mount,
                              const char *dst_mount, int assume_yes, int force, int dry_run)
{
  if (!dst_image || !*dst_image)
  {
    fprintf(stderr, "invalid --dst-image\n");
    return 2;
  }
  if (dry_run)
    return cmd_migrate_create_dry_run(src_image, dst_image, size_bytes, inodes, format_version,
                                      journal_bytes, blksize_log, hrl_entry_ratio);

  uint32_t target_format = kafsresize_resolve_target_format(format_version);
  if (!kafsresize_format_version_is_supported(target_format))
  {
    fprintf(stderr, "unsupported format version: %" PRIu32 "\n", target_format);
    return 2;
  }

  struct stat dst_st;
  int dst_exists = (stat(dst_image, &dst_st) == 0);
  int dst_is_reg = dst_exists && S_ISREG(dst_st.st_mode);
  int dst_is_blk = dst_exists && S_ISBLK(dst_st.st_mode);

  if (target_format == KAFS_FORMAT_VERSION_V6)
  {
    kafsresize_migrate_create_plan_t plan;
    int precheck_rc = kafsresize_prepare_migrate_create_plan(src_image, dst_image, size_bytes,
                                                             inodes, format_version, journal_bytes,
                                                             blksize_log, hrl_entry_ratio, &plan);
    if (precheck_rc != 0)
      return precheck_rc;
    size_bytes = plan.size_bytes;
  }
  else
  {
    int size_rc = resolve_migrate_create_size(dst_image, &size_bytes);
    if (size_rc != 0)
      return size_rc;
  }

  if (!assume_yes && !confirm_yes_stdin())
  {
    fprintf(stderr, "aborted\n");
    return 2;
  }

  if (dst_exists)
  {
    if (dst_is_blk)
    {
      // block device is expected to exist; force flag is not required.
    }
    else if (dst_is_reg)
    {
      if (!force)
      {
        fprintf(stderr, "destination image exists: %s (use --force to overwrite)\n", dst_image);
        return 1;
      }
      if (unlink(dst_image) != 0)
      {
        perror("unlink(dst-image)");
        return 1;
      }
    }
    else
    {
      fprintf(stderr, "unsupported destination type: %s\n", dst_image);
      return 1;
    }
  }

  char size_buf[32];
  char inode_buf[32];
  char format_buf[32];
  char jbuf[32];
  char lbuf[32];
  char rbuf[32];
  snprintf(size_buf, sizeof(size_buf), "%" PRIu64, size_bytes);
  snprintf(inode_buf, sizeof(inode_buf), "%" PRIu32, inodes);
  snprintf(format_buf, sizeof(format_buf), "%" PRIu32, format_version);
  snprintf(jbuf, sizeof(jbuf), "%" PRIu64, journal_bytes);
  snprintf(lbuf, sizeof(lbuf), "%d", blksize_log);
  snprintf(rbuf, sizeof(rbuf), "%.6f", hrl_entry_ratio);

  const char *mkfs = resolve_mkfs_prog();
  char *argv[24];
  int ai = 0;
  argv[ai++] = (char *)mkfs;
  argv[ai++] = (char *)dst_image;
  argv[ai++] = "--size-bytes";
  argv[ai++] = size_buf;
  argv[ai++] = "--inodes";
  argv[ai++] = inode_buf;
  if (format_version > 0)
  {
    argv[ai++] = "--format-version";
    argv[ai++] = format_buf;
  }
  if (journal_bytes > 0)
  {
    argv[ai++] = "--journal-size-bytes";
    argv[ai++] = jbuf;
  }
  if (blksize_log > 0)
  {
    argv[ai++] = "--blksize-log";
    argv[ai++] = lbuf;
  }
  if (hrl_entry_ratio > 0.0)
  {
    argv[ai++] = "--hrl-entry-ratio";
    argv[ai++] = rbuf;
  }
  argv[ai] = NULL;

  int rc = run_command(mkfs, argv);
  if (rc != 0)
  {
    fprintf(stderr, "mkfs.kafs failed with exit code %d\n", rc);
    return 1;
  }

  printf("kafsresize: migrate-create completed\n");
  printf("  dst_image: %s\n", dst_image);
  printf("  size_bytes: %" PRIu64 "\n", size_bytes);
  printf("  inodes: %" PRIu32 "\n", inodes);
  if (format_version > 0)
    printf("  format_version: %" PRIu32 "\n", format_version);
  if (journal_bytes > 0)
    printf("  journal_bytes: %" PRIu64 "\n", journal_bytes);
  if (blksize_log > 0)
    printf("  blksize_log: %d\n", blksize_log);
  if (hrl_entry_ratio > 0.0)
    printf("  hrl_entry_ratio: %.6f\n", hrl_entry_ratio);

  print_migrate_next_steps(dst_image, src_mount, dst_mount);

  return 0;
}

typedef struct kafsresize_options
{
  int do_grow;
  int do_migrate_create;
  int assume_yes;
  int force;
  int dry_run;
  uint64_t target_bytes;
  uint32_t format_version;
  uint64_t journal_bytes;
  int blksize_log;
  double hrl_entry_ratio;
  uint32_t inodes;
  const char *image;
  const char *src_image;
  const char *dst_image;
  const char *src_mount;
  const char *dst_mount;
} kafsresize_options_t;

static int kafsresize_parse_u32_arg(const char *name, const char *value, uint32_t *out)
{
  unsigned long long parsed = strtoull(value, NULL, 0);
  if (parsed == 0 || parsed > UINT32_MAX)
  {
    fprintf(stderr, "invalid %s: %s\n", name, value);
    return 2;
  }
  *out = (uint32_t)parsed;
  return 0;
}

static int kafsresize_parse_flag_arg(const char *arg, kafsresize_options_t *opts)
{
  if (strcmp(arg, "--grow") == 0)
  {
    opts->do_grow = 1;
    return 1;
  }
  if (strcmp(arg, "--migrate-create") == 0)
  {
    opts->do_migrate_create = 1;
    return 1;
  }
  if (strcmp(arg, "--yes") == 0)
  {
    opts->assume_yes = 1;
    return 1;
  }
  if (strcmp(arg, "--force") == 0)
  {
    opts->force = 1;
    return 1;
  }
  if (strcmp(arg, "--dry-run") == 0)
  {
    opts->dry_run = 1;
    return 1;
  }
  return 0;
}

static int kafsresize_parse_size_value_arg(int argc, char **argv, int *index,
                                           kafsresize_options_t *opts)
{
  const char *arg = argv[*index];
  if (strcmp(arg, "--size-bytes") == 0 && *index + 1 < argc)
  {
    if (kafs_parse_size_bytes_u64(argv[++(*index)], &opts->target_bytes) != 0)
    {
      fprintf(stderr, "invalid size-bytes: %s\n", argv[*index]);
      return 2;
    }
    return 1;
  }
  if (strcmp(arg, "--journal-size-bytes") == 0 && *index + 1 < argc)
  {
    if (kafs_parse_size_bytes_u64(argv[++(*index)], &opts->journal_bytes) != 0)
    {
      fprintf(stderr, "invalid journal-size-bytes: %s\n", argv[*index]);
      return 2;
    }
    return 1;
  }
  return 0;
}

static int kafsresize_parse_setting_value_arg(int argc, char **argv, int *index,
                                              kafsresize_options_t *opts)
{
  const char *arg = argv[*index];
  if (strcmp(arg, "--format-version") == 0 && *index + 1 < argc)
  {
    if (kafsresize_parse_u32_arg("format-version", argv[++(*index)], &opts->format_version) != 0)
      return 2;
    return 1;
  }
  if (strcmp(arg, "--blksize-log") == 0 && *index + 1 < argc)
  {
    opts->blksize_log = atoi(argv[++(*index)]);
    if (opts->blksize_log <= 0)
    {
      fprintf(stderr, "invalid blksize-log: %s\n", argv[*index]);
      return 2;
    }
    return 1;
  }
  if (strcmp(arg, "--hrl-entry-ratio") == 0 && *index + 1 < argc)
  {
    if (kafs_parse_ratio_0_to_1(argv[++(*index)], &opts->hrl_entry_ratio) != 0)
    {
      fprintf(stderr, "invalid hrl-entry-ratio (expected 0<R<=1): %s\n", argv[*index]);
      return 2;
    }
    return 1;
  }
  if (strcmp(arg, "--inodes") == 0 && *index + 1 < argc)
  {
    if (kafsresize_parse_u32_arg("inodes", argv[++(*index)], &opts->inodes) != 0)
      return 2;
    return 1;
  }
  return 0;
}

static int kafsresize_parse_path_value_arg(int argc, char **argv, int *index,
                                           kafsresize_options_t *opts)
{
  const char *arg = argv[*index];
  if (strcmp(arg, "--dst-image") == 0 && *index + 1 < argc)
  {
    opts->dst_image = argv[++(*index)];
    return 1;
  }
  if (strcmp(arg, "--src-image") == 0 && *index + 1 < argc)
  {
    opts->src_image = argv[++(*index)];
    return 1;
  }
  if (strcmp(arg, "--src-mount") == 0 && *index + 1 < argc)
  {
    opts->src_mount = argv[++(*index)];
    return 1;
  }
  if (strcmp(arg, "--dst-mount") == 0 && *index + 1 < argc)
  {
    opts->dst_mount = argv[++(*index)];
    return 1;
  }
  return 0;
}

static int kafsresize_parse_value_arg(int argc, char **argv, int *index, kafsresize_options_t *opts)
{
  int rc = kafsresize_parse_size_value_arg(argc, argv, index, opts);
  if (rc != 0)
    return rc;

  rc = kafsresize_parse_setting_value_arg(argc, argv, index, opts);
  if (rc != 0)
    return rc;

  return kafsresize_parse_path_value_arg(argc, argv, index, opts);
}

static int kafsresize_parse_args(int argc, char **argv, kafsresize_options_t *opts)
{
  for (int i = 1; i < argc; ++i)
  {
    const char *arg = argv[i];
    if (kafsresize_parse_flag_arg(arg, opts) != 0)
      continue;

    int rc = kafsresize_parse_value_arg(argc, argv, &i, opts);
    if (rc == 2)
      return 2;
    if (rc == 1)
      continue;

    if (arg[0] == '-')
    {
      usage(argv[0]);
      return 2;
    }
    opts->image = arg;
  }

  return 0;
}

static int kafsresize_run(const char *prog, const kafsresize_options_t *opts)
{
  if (opts->do_grow && opts->do_migrate_create)
  {
    fprintf(stderr, "--grow and --migrate-create are mutually exclusive\n");
    return 2;
  }

  if (opts->do_grow)
  {
    if (opts->target_bytes == 0 || !opts->image)
    {
      usage(prog);
      return 2;
    }
    return cmd_grow(opts->image, opts->target_bytes);
  }

  if (opts->do_migrate_create)
  {
    if (opts->inodes == 0 || !opts->dst_image)
    {
      usage(prog);
      return 2;
    }
    return cmd_migrate_create(opts->src_image, opts->dst_image, opts->target_bytes, opts->inodes,
                              opts->format_version, opts->journal_bytes, opts->blksize_log,
                              opts->hrl_entry_ratio, opts->src_mount, opts->dst_mount,
                              opts->assume_yes, opts->force, opts->dry_run);
  }

  usage(prog);
  return 2;
}

int main(int argc, char **argv)
{
  kafsresize_options_t opts = {0};

  if (kafs_cli_exit_if_help(argc, argv, usage, argv[0]) == 0)
    return 0;

  if (kafsresize_parse_args(argc, argv, &opts) != 0)
    return 2;

  return kafsresize_run(argv[0], &opts);
}
