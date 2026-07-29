#include "kafs.h"
#include "kafs_cli_opts.h"
#include "kafs_hash.h"
#include "kafs_inode.h"
#include "kafs_superblock.h"
#include "kafs_tailmeta.h"
#include "kafs_tool_util.h"
#include "kafs_v7_import.h"
#include "kafs_v7_layout.h"

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
  uint32_t v7_group_count;
  uint32_t v7_replica_count;
  uint32_t v7_descriptor_bytes;
  uint32_t v7_journal_segment_count;
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
      "       %s --migrate-import-v7 --src-image <v5> --dst-image <v7> [options]\n"
      "  notes:\n"
      "    - grow-only (no shrink)\n"
      "    - v0 grow supports only preallocated headroom (s_blkcnt < s_r_blkcnt)\n"
      "    - migrate-create builds a new image with target size/inodes via mkfs.kafs\n"
      "    - migrate-create without --size-bytes auto-detects size from --dst-image\n"
      "    - migrate-import-v7 publishes dst-image only after full offline validation\n"
      "    - options are mode-specific; malformed or inapplicable input exits with status 2\n"
      "    - prefix path values beginning with '-' with './'\n"
      "  options for --grow:\n"
      "    --size-bytes N           required target size; accepts K/M/G suffixes\n"
      "    <image>                  exactly one image operand is required\n"
      "  options for --migrate-create:\n"
      "    --src-image IMAGE       source image for --format-version 7 precheck/dry-run\n"
      "    --format-version V      on-disk format version passed to mkfs.kafs\n"
      "    --journal-size-bytes N   journal size passed to mkfs.kafs\n"
      "    --blksize-log L          block-size log2 passed to mkfs.kafs\n"
      "    --hrl-entry-ratio R      HRL entries/data-block ratio passed to mkfs.kafs\n"
      "    --src-mount PATH         print suggested rsync source mount\n"
      "    --dst-mount PATH         print suggested destination mount\n"
      "    --dry-run                validate migration-create inputs without writing dst-image\n"
      "    --yes                    skip confirmation prompt\n"
      "    --force                  overwrite existing --dst-image\n"
      "  options for --migrate-import-v7:\n"
      "    --size-bytes N           destination size (default: source file size)\n"
      "    --inodes I               destination inode count (default: source count)\n"
      "    --blksize-log L          destination block-size log2 (default: source)\n"
      "    --journal-size-bytes N   destination journal size\n"
      "    --hrl-entry-ratio R      destination HRL entries/data-block ratio\n"
      "    --v7-group-count N       destination group count (default: automatic)\n"
      "    --format-version 7       optional explicit import format\n"
      "    --dry-run                validate source and exact destination capacity only\n"
      "    --json                   print one versioned JSON result; diagnostics stay on stderr\n",
      prog, prog, prog);
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
         format_version == KAFS_FORMAT_VERSION_V7;
}

static uint32_t kafsresize_resolve_target_format(uint32_t format_version)
{
  return (format_version > 0) ? format_version : (uint32_t)KAFS_FORMAT_VERSION_V5;
}

static uint64_t kafsresize_align_up_u64(uint64_t value, uint64_t align_mask)
{
  return (value + align_mask) & ~align_mask;
}

static int kafsresize_allocator_summary_size(uint64_t block_count, uint64_t *out_size)
{
  if (!out_size || block_count > UINT64_MAX - 7u)
    return -ERANGE;
  uint64_t l0_bytes = (block_count + 7u) >> 3;
  if (l0_bytes == 0u || l0_bytes > UINT64_MAX - 7u)
    return -ERANGE;
  uint64_t l1_bytes = (l0_bytes + 7u) >> 3;
  if (l1_bytes == 0u || l1_bytes > UINT64_MAX - 7u)
    return -ERANGE;
  uint64_t l2_bytes = (l1_bytes + 7u) >> 3;
  if (l2_bytes == 0u || l1_bytes > UINT64_MAX - l2_bytes)
    return -ERANGE;
  *out_size = l1_bytes + l2_bytes;
  return 0;
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

  mapsize += (block_count + 7u) >> 3;
  mapsize = kafsresize_align_up_u64(mapsize, 7u);
  mapsize = kafsresize_align_up_u64(mapsize, block_mask);

  mapsize += kafs_inode_table_bytes_for_format(format_version, inode_count);
  mapsize = kafsresize_align_up_u64(mapsize, block_mask);

  uint64_t allocator_size = 0;
  if (kafsresize_allocator_summary_size(block_count, &allocator_size) != 0 ||
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
  if (format_version == KAFS_FORMAT_VERSION_V7)
  {
    if (block_size > UINT32_MAX || inode_count > UINT32_MAX)
      return -ERANGE;
    kafs_v7_mkfs_options_t options = {
        .image_size_bytes = total_bytes,
        .block_size = (uint32_t)block_size,
        .inode_count = (uint32_t)inode_count,
        .journal_bytes = journal_bytes,
        .hrl_entry_ratio = hrl_entry_ratio,
    };
    kafs_v7_mkfs_plan_t plan;
    int rc = kafs_v7_mkfs_plan(&options, &plan);
    if (rc != 0)
      return rc;
    *out = (kafsresize_mkfs_layout_t){
        .mapsize = plan.metadata_bytes,
        .first_data_block = plan.first_data_block,
        .block_count = total_bytes / block_size,
        .data_block_capacity = plan.data_blocks,
        .v7_group_count = plan.group_count,
        .v7_replica_count = plan.replica_count,
        .v7_descriptor_bytes = plan.descriptor_bytes,
        .v7_journal_segment_count = plan.journal_segment_count,
    };
    return 0;
  }
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
  if (target_format == KAFS_FORMAT_VERSION_V7 && (!src_image || !*src_image))
  {
    fprintf(stderr, "--format-version %" PRIu32 " requires --src-image\n", target_format);
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
    if (target_format == KAFS_FORMAT_VERSION_V7 && rc == -EINVAL)
      fprintf(stderr, "invalid format v%" PRIu32 " migration geometry\n", target_format);
    else if (target_format == KAFS_FORMAT_VERSION_V7)
      fprintf(stderr, "image too small for format v%" PRIu32 " descriptor layout\n", target_format);
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
    if (source.used_data_blocks > UINT64_MAX / source.block_size ||
        layout.data_block_capacity > UINT64_MAX / (1ull << resolved_blksize_log))
    {
      fprintf(stderr, "source or destination data capacity overflow\n");
      return 1;
    }
    uint64_t source_used_data_bytes = source.used_data_blocks * source.block_size;
    uint64_t destination_data_capacity_bytes =
        layout.data_block_capacity * (1ull << resolved_blksize_log);
    if (destination_data_capacity_bytes < source_used_data_bytes)
    {
      fprintf(stderr,
              "destination data capacity too small: capacity_bytes=%" PRIu64
              ", source_used_data_bytes=%" PRIu64 "\n",
              destination_data_capacity_bytes, source_used_data_bytes);
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
  if (plan->target_format == KAFS_FORMAT_VERSION_V7)
  {
    printf("  v7_descriptor_bytes: %" PRIu32 "\n", plan->layout.v7_descriptor_bytes);
    printf("  v7_descriptor_replicas: %" PRIu32 "\n", plan->layout.v7_replica_count);
    printf("  v7_group_count: %" PRIu32 "\n", plan->layout.v7_group_count);
    printf("  v7_journal_segments: %" PRIu32 "\n", plan->layout.v7_journal_segment_count);
    printf("  v7_group_policy: group-local-linear\n");
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

  if (target_format == KAFS_FORMAT_VERSION_V7)
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

static void kafsresize_print_json_string(const char *value)
{
  putchar('"');
  for (const unsigned char *cursor = (const unsigned char *)(value ? value : ""); *cursor; ++cursor)
  {
    switch (*cursor)
    {
    case '"':
      fputs("\\\"", stdout);
      break;
    case '\\':
      fputs("\\\\", stdout);
      break;
    case '\b':
      fputs("\\b", stdout);
      break;
    case '\f':
      fputs("\\f", stdout);
      break;
    case '\n':
      fputs("\\n", stdout);
      break;
    case '\r':
      fputs("\\r", stdout);
      break;
    case '\t':
      fputs("\\t", stdout);
      break;
    default:
      if (*cursor < 0x20u)
        printf("\\u%04x", (unsigned int)*cursor);
      else
        putchar((int)*cursor);
      break;
    }
  }
  putchar('"');
}

static void kafsresize_print_import_result_json(const char *src_image, const char *dst_image,
                                                int dry_run, int import_rc,
                                                const kafs_v7_import_report_t *report)
{
  int exit_status = import_rc != 0;
  printf("{\"schema\":\"KAFS.V5V7MigrationImportResult.v1\","
         "\"operation\":\"migrate-import-v7\",\"status\":\"%s\","
         "\"exit_status\":%d,\"mode\":\"%s\",\"source_image\":",
         exit_status == 0 ? "PASS" : "FAIL", exit_status, dry_run ? "dry-run" : "import");
  kafsresize_print_json_string(src_image);
  fputs(",\"destination_image\":", stdout);
  kafsresize_print_json_string(dst_image);
  fputs(",\"result\":", stdout);
  if (import_rc == 0)
  {
    printf("{\"source_crc32\":\"%08" PRIx32 "\",\"source_inode_count\":%" PRIu32
           ",\"imported_inodes\":%" PRIu32 ",\"imported_directories\":%" PRIu32
           ",\"imported_regular_files\":%" PRIu32 ",\"imported_symlinks\":%" PRIu32
           ",\"payload_bytes\":%" PRIu64 ",\"destination_size_bytes\":%" PRIu64
           ",\"destination_block_size\":%" PRIu32 ",\"destination_group_count\":%" PRIu32
           ",\"allocated_blocks\":%" PRIu64
           ",\"writes_performed\":%s,\"destination_admission_ready\":%s}",
           report->source_crc32, report->source_inode_count, report->imported_inode_count,
           report->imported_directory_count, report->imported_regular_count,
           report->imported_symlink_count, report->payload_bytes, report->destination_size_bytes,
           report->destination_block_size, report->destination_group_count,
           report->allocated_blocks, dry_run ? "false" : "true", dry_run ? "false" : "true");
  }
  else
  {
    fputs("null", stdout);
  }
  fputs(",\"claims\":{\"production_cutover_authorized\":false,"
        "\"migration_lifecycle_accepted\":false}}\n",
        stdout);
}

static int cmd_migrate_import_v7(const char *src_image, const char *dst_image, uint64_t size_bytes,
                                 uint32_t inodes, uint64_t journal_bytes, int blksize_log,
                                 double hrl_entry_ratio, uint32_t group_count,
                                 uint32_t format_version, int force, int dry_run, int json_output)
{
  if (!src_image || !*src_image || !dst_image || !*dst_image)
  {
    fprintf(stderr, "--migrate-import-v7 requires --src-image and --dst-image\n");
    return 2;
  }
  if (format_version != 0u && format_version != KAFS_FORMAT_VERSION_V7)
  {
    fprintf(stderr, "--migrate-import-v7 supports only --format-version 7\n");
    return 2;
  }
  if (force)
  {
    fprintf(stderr,
            "--force is not supported by --migrate-import-v7; the final destination must not "
            "exist\n");
    return 2;
  }
  uint32_t block_size = 0u;
  if (blksize_log > 0)
  {
    if (blksize_log >= 32)
    {
      fprintf(stderr, "invalid blksize-log: %d\n", blksize_log);
      return 2;
    }
    block_size = 1u << blksize_log;
  }
  kafs_v7_import_options_t options = {
      .source_path = src_image,
      .destination_path = dst_image,
      .destination_size_bytes = size_bytes,
      .destination_inode_count = inodes,
      .destination_block_size = block_size,
      .journal_bytes = journal_bytes,
      .hrl_entry_ratio = hrl_entry_ratio,
      .group_count = group_count,
      .dry_run = dry_run,
  };
  kafs_v7_import_report_t report = {0};
  int rc = kafs_v7_import_image(&options, &report);
  if (json_output)
  {
    kafsresize_print_import_result_json(src_image, dst_image, dry_run, rc, &report);
    return rc != 0;
  }
  if (rc != 0)
    return 1;
  printf("kafsresize: migrate-import-v7 %s PASS\n", dry_run ? "dry-run" : "completed");
  printf("  src_image: %s\n", src_image);
  printf("  dst_image: %s\n", dst_image);
  printf("  source_crc32: %08" PRIx32 "\n", report.source_crc32);
  printf("  source_inode_count: %" PRIu32 "\n", report.source_inode_count);
  printf("  imported_inodes: %" PRIu32 "\n", report.imported_inode_count);
  printf("  imported_directories: %" PRIu32 "\n", report.imported_directory_count);
  printf("  imported_regular_files: %" PRIu32 "\n", report.imported_regular_count);
  printf("  imported_symlinks: %" PRIu32 "\n", report.imported_symlink_count);
  printf("  payload_bytes: %" PRIu64 "\n", report.payload_bytes);
  printf("  destination_size_bytes: %" PRIu64 "\n", report.destination_size_bytes);
  printf("  destination_block_size: %" PRIu32 "\n", report.destination_block_size);
  printf("  destination_group_count: %" PRIu32 "\n", report.destination_group_count);
  printf("  allocated_blocks: %" PRIu64 "\n", report.allocated_blocks);
  printf("  writes_performed: %s\n", dry_run ? "no" : "yes");
  printf("  destination_admission_ready: %s\n", dry_run ? "no" : "yes");
  return 0;
}

typedef struct kafsresize_options
{
  int do_grow;
  int do_migrate_create;
  int do_migrate_import_v7;
  int assume_yes;
  int force;
  int dry_run;
  int json_output;
  uint64_t target_bytes;
  uint32_t format_version;
  uint64_t journal_bytes;
  int blksize_log;
  double hrl_entry_ratio;
  uint32_t inodes;
  uint32_t v7_group_count;
  const char *image;
  const char *src_image;
  const char *dst_image;
  const char *src_mount;
  const char *dst_mount;
  uint32_t provided_options;
} kafsresize_options_t;

enum kafsresize_option_bit
{
  KAFSRESIZE_OPT_SIZE = 1u << 0,
  KAFSRESIZE_OPT_FORMAT = 1u << 1,
  KAFSRESIZE_OPT_JOURNAL = 1u << 2,
  KAFSRESIZE_OPT_BLKSIZE = 1u << 3,
  KAFSRESIZE_OPT_HRL_RATIO = 1u << 4,
  KAFSRESIZE_OPT_INODES = 1u << 5,
  KAFSRESIZE_OPT_V7_GROUPS = 1u << 6,
  KAFSRESIZE_OPT_IMAGE = 1u << 7,
  KAFSRESIZE_OPT_SRC_IMAGE = 1u << 8,
  KAFSRESIZE_OPT_DST_IMAGE = 1u << 9,
  KAFSRESIZE_OPT_SRC_MOUNT = 1u << 10,
  KAFSRESIZE_OPT_DST_MOUNT = 1u << 11,
  KAFSRESIZE_OPT_YES = 1u << 12,
  KAFSRESIZE_OPT_FORCE = 1u << 13,
  KAFSRESIZE_OPT_DRY_RUN = 1u << 14,
  KAFSRESIZE_OPT_JSON = 1u << 15,
};

typedef struct kafsresize_option_name
{
  uint32_t bit;
  const char *name;
} kafsresize_option_name_t;

static const kafsresize_option_name_t kafsresize_option_names[] = {
    {KAFSRESIZE_OPT_SIZE, "--size-bytes"},
    {KAFSRESIZE_OPT_FORMAT, "--format-version"},
    {KAFSRESIZE_OPT_JOURNAL, "--journal-size-bytes"},
    {KAFSRESIZE_OPT_BLKSIZE, "--blksize-log"},
    {KAFSRESIZE_OPT_HRL_RATIO, "--hrl-entry-ratio"},
    {KAFSRESIZE_OPT_INODES, "--inodes"},
    {KAFSRESIZE_OPT_V7_GROUPS, "--v7-group-count"},
    {KAFSRESIZE_OPT_IMAGE, "<image>"},
    {KAFSRESIZE_OPT_SRC_IMAGE, "--src-image"},
    {KAFSRESIZE_OPT_DST_IMAGE, "--dst-image"},
    {KAFSRESIZE_OPT_SRC_MOUNT, "--src-mount"},
    {KAFSRESIZE_OPT_DST_MOUNT, "--dst-mount"},
    {KAFSRESIZE_OPT_YES, "--yes"},
    {KAFSRESIZE_OPT_FORCE, "--force"},
    {KAFSRESIZE_OPT_DRY_RUN, "--dry-run"},
    {KAFSRESIZE_OPT_JSON, "--json"},
};

static int kafsresize_parse_u32_arg(const char *name, const char *value, uint32_t *out)
{
  if (!value || !*value || *value == '-' || isspace((unsigned char)*value))
  {
    fprintf(stderr, "invalid %s: %s\n", name, value ? value : "<missing>");
    return 2;
  }

  char *end = NULL;
  errno = 0;
  unsigned long long parsed = strtoull(value, &end, 0);
  if (errno != 0 || !end || end == value || *end != '\0' || parsed == 0 || parsed > UINT32_MAX)
  {
    fprintf(stderr, "invalid %s: %s\n", name, value);
    return 2;
  }
  *out = (uint32_t)parsed;
  return 0;
}

static int kafsresize_take_value(int argc, char **argv, int *index, const char **value)
{
  const char *option = argv[*index];
  if (*index + 1 >= argc || argv[*index + 1][0] == '\0' || argv[*index + 1][0] == '-')
  {
    fprintf(stderr, "missing value for %s\n", option);
    return 2;
  }
  *value = argv[++(*index)];
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
  if (strcmp(arg, "--migrate-import-v7") == 0)
  {
    opts->do_migrate_import_v7 = 1;
    return 1;
  }
  if (strcmp(arg, "--yes") == 0)
  {
    opts->assume_yes = 1;
    opts->provided_options |= KAFSRESIZE_OPT_YES;
    return 1;
  }
  if (strcmp(arg, "--force") == 0)
  {
    opts->force = 1;
    opts->provided_options |= KAFSRESIZE_OPT_FORCE;
    return 1;
  }
  if (strcmp(arg, "--dry-run") == 0)
  {
    opts->dry_run = 1;
    opts->provided_options |= KAFSRESIZE_OPT_DRY_RUN;
    return 1;
  }
  if (strcmp(arg, "--json") == 0)
  {
    opts->json_output = 1;
    opts->provided_options |= KAFSRESIZE_OPT_JSON;
    return 1;
  }
  return 0;
}

static int kafsresize_parse_size_value_arg(int argc, char **argv, int *index,
                                           kafsresize_options_t *opts)
{
  const char *arg = argv[*index];
  if (strcmp(arg, "--size-bytes") == 0)
  {
    const char *value = NULL;
    if (kafsresize_take_value(argc, argv, index, &value) != 0)
      return 2;
    if (kafs_parse_size_bytes_u64(value, &opts->target_bytes) != 0 || opts->target_bytes == 0u)
    {
      fprintf(stderr, "invalid size-bytes: %s\n", value);
      return 2;
    }
    opts->provided_options |= KAFSRESIZE_OPT_SIZE;
    return 1;
  }
  if (strcmp(arg, "--journal-size-bytes") == 0)
  {
    const char *value = NULL;
    if (kafsresize_take_value(argc, argv, index, &value) != 0)
      return 2;
    if (kafs_parse_size_bytes_u64(value, &opts->journal_bytes) != 0 || opts->journal_bytes == 0u)
    {
      fprintf(stderr, "invalid journal-size-bytes: %s\n", value);
      return 2;
    }
    opts->provided_options |= KAFSRESIZE_OPT_JOURNAL;
    return 1;
  }
  return 0;
}

static int kafsresize_parse_setting_value_arg(int argc, char **argv, int *index,
                                              kafsresize_options_t *opts)
{
  const char *arg = argv[*index];
  if (strcmp(arg, "--format-version") == 0)
  {
    const char *value = NULL;
    if (kafsresize_take_value(argc, argv, index, &value) != 0 ||
        kafsresize_parse_u32_arg("format-version", value, &opts->format_version) != 0)
      return 2;
    opts->provided_options |= KAFSRESIZE_OPT_FORMAT;
    return 1;
  }
  if (strcmp(arg, "--blksize-log") == 0)
  {
    const char *value = NULL;
    uint32_t parsed = 0u;
    if (kafsresize_take_value(argc, argv, index, &value) != 0 ||
        kafsresize_parse_u32_arg("blksize-log", value, &parsed) != 0 || parsed >= 32u)
    {
      if (parsed >= 32u)
        fprintf(stderr, "invalid blksize-log: %s\n", value);
      return 2;
    }
    opts->blksize_log = (int)parsed;
    opts->provided_options |= KAFSRESIZE_OPT_BLKSIZE;
    return 1;
  }
  if (strcmp(arg, "--hrl-entry-ratio") == 0)
  {
    const char *value = NULL;
    if (kafsresize_take_value(argc, argv, index, &value) != 0)
      return 2;
    if (kafs_parse_ratio_0_to_1(value, &opts->hrl_entry_ratio) != 0)
    {
      fprintf(stderr, "invalid hrl-entry-ratio (expected 0<R<=1): %s\n", value);
      return 2;
    }
    opts->provided_options |= KAFSRESIZE_OPT_HRL_RATIO;
    return 1;
  }
  if (strcmp(arg, "--inodes") == 0)
  {
    const char *value = NULL;
    if (kafsresize_take_value(argc, argv, index, &value) != 0 ||
        kafsresize_parse_u32_arg("inodes", value, &opts->inodes) != 0)
      return 2;
    opts->provided_options |= KAFSRESIZE_OPT_INODES;
    return 1;
  }
  if (strcmp(arg, "--v7-group-count") == 0)
  {
    const char *value = NULL;
    if (kafsresize_take_value(argc, argv, index, &value) != 0 ||
        kafsresize_parse_u32_arg("v7-group-count", value, &opts->v7_group_count) != 0)
      return 2;
    opts->provided_options |= KAFSRESIZE_OPT_V7_GROUPS;
    return 1;
  }
  return 0;
}

static int kafsresize_parse_path_value_arg(int argc, char **argv, int *index,
                                           kafsresize_options_t *opts)
{
  const char *arg = argv[*index];
  if (strcmp(arg, "--dst-image") == 0)
  {
    if (kafsresize_take_value(argc, argv, index, &opts->dst_image) != 0)
      return 2;
    opts->provided_options |= KAFSRESIZE_OPT_DST_IMAGE;
    return 1;
  }
  if (strcmp(arg, "--src-image") == 0)
  {
    if (kafsresize_take_value(argc, argv, index, &opts->src_image) != 0)
      return 2;
    opts->provided_options |= KAFSRESIZE_OPT_SRC_IMAGE;
    return 1;
  }
  if (strcmp(arg, "--src-mount") == 0)
  {
    if (kafsresize_take_value(argc, argv, index, &opts->src_mount) != 0)
      return 2;
    opts->provided_options |= KAFSRESIZE_OPT_SRC_MOUNT;
    return 1;
  }
  if (strcmp(arg, "--dst-mount") == 0)
  {
    if (kafsresize_take_value(argc, argv, index, &opts->dst_mount) != 0)
      return 2;
    opts->provided_options |= KAFSRESIZE_OPT_DST_MOUNT;
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
    if ((opts->provided_options & KAFSRESIZE_OPT_IMAGE) != 0u)
    {
      fprintf(stderr, "unexpected operand: %s\n", arg);
      return 2;
    }
    opts->image = arg;
    opts->provided_options |= KAFSRESIZE_OPT_IMAGE;
  }

  return 0;
}

static const char *kafsresize_option_name(uint32_t bit)
{
  for (size_t i = 0; i < sizeof(kafsresize_option_names) / sizeof(kafsresize_option_names[0]); ++i)
  {
    if (kafsresize_option_names[i].bit == bit)
      return kafsresize_option_names[i].name;
  }
  return "<unknown>";
}

static uint32_t kafsresize_first_option_bit(uint32_t options)
{
  for (uint32_t bit = 1u; bit != 0u; bit <<= 1u)
  {
    if ((options & bit) != 0u)
      return bit;
  }
  return 0u;
}

static int kafsresize_validate_mode_options(const kafsresize_options_t *opts, const char *mode,
                                            uint32_t allowed, uint32_t required)
{
  uint32_t invalid_bit = kafsresize_first_option_bit(opts->provided_options & ~allowed);
  if (invalid_bit != 0u)
  {
    fprintf(stderr, "%s does not apply to %s\n", kafsresize_option_name(invalid_bit), mode);
    return 2;
  }

  uint32_t missing_bit = kafsresize_first_option_bit(required & ~opts->provided_options);
  if (missing_bit != 0u)
  {
    fprintf(stderr, "%s is required for %s\n", kafsresize_option_name(missing_bit), mode);
    return 2;
  }
  return 0;
}

static int kafsresize_run(const kafsresize_options_t *opts)
{
  int mode_count = opts->do_grow + opts->do_migrate_create + opts->do_migrate_import_v7;
  if (mode_count != 1)
  {
    fprintf(stderr,
            "exactly one of --grow, --migrate-create, and --migrate-import-v7 is required\n");
    return 2;
  }

  if (opts->do_grow)
  {
    const uint32_t allowed = KAFSRESIZE_OPT_SIZE | KAFSRESIZE_OPT_IMAGE;
    if (kafsresize_validate_mode_options(opts, "--grow", allowed, allowed) != 0)
      return 2;
    return cmd_grow(opts->image, opts->target_bytes);
  }

  if (opts->do_migrate_create)
  {
    const uint32_t allowed = KAFSRESIZE_OPT_SIZE | KAFSRESIZE_OPT_FORMAT | KAFSRESIZE_OPT_JOURNAL |
                             KAFSRESIZE_OPT_BLKSIZE | KAFSRESIZE_OPT_HRL_RATIO |
                             KAFSRESIZE_OPT_INODES | KAFSRESIZE_OPT_SRC_IMAGE |
                             KAFSRESIZE_OPT_DST_IMAGE | KAFSRESIZE_OPT_SRC_MOUNT |
                             KAFSRESIZE_OPT_DST_MOUNT | KAFSRESIZE_OPT_YES | KAFSRESIZE_OPT_FORCE |
                             KAFSRESIZE_OPT_DRY_RUN;
    const uint32_t required = KAFSRESIZE_OPT_INODES | KAFSRESIZE_OPT_DST_IMAGE;
    if (kafsresize_validate_mode_options(opts, "--migrate-create", allowed, required) != 0)
      return 2;
    if ((opts->provided_options & KAFSRESIZE_OPT_SRC_IMAGE) != 0u &&
        kafsresize_resolve_target_format(opts->format_version) != KAFS_FORMAT_VERSION_V7)
    {
      fprintf(stderr, "--src-image applies to --migrate-create only with --format-version 7\n");
      return 2;
    }
    return cmd_migrate_create(opts->src_image, opts->dst_image, opts->target_bytes, opts->inodes,
                              opts->format_version, opts->journal_bytes, opts->blksize_log,
                              opts->hrl_entry_ratio, opts->src_mount, opts->dst_mount,
                              opts->assume_yes, opts->force, opts->dry_run);
  }

  if (opts->do_migrate_import_v7)
  {
    const uint32_t allowed = KAFSRESIZE_OPT_SIZE | KAFSRESIZE_OPT_FORMAT | KAFSRESIZE_OPT_JOURNAL |
                             KAFSRESIZE_OPT_BLKSIZE | KAFSRESIZE_OPT_HRL_RATIO |
                             KAFSRESIZE_OPT_INODES | KAFSRESIZE_OPT_V7_GROUPS |
                             KAFSRESIZE_OPT_SRC_IMAGE | KAFSRESIZE_OPT_DST_IMAGE |
                             KAFSRESIZE_OPT_DRY_RUN | KAFSRESIZE_OPT_JSON;
    const uint32_t required = KAFSRESIZE_OPT_SRC_IMAGE | KAFSRESIZE_OPT_DST_IMAGE;
    if (kafsresize_validate_mode_options(opts, "--migrate-import-v7", allowed, required) != 0)
      return 2;
    return cmd_migrate_import_v7(opts->src_image, opts->dst_image, opts->target_bytes, opts->inodes,
                                 opts->journal_bytes, opts->blksize_log, opts->hrl_entry_ratio,
                                 opts->v7_group_count, opts->format_version, opts->force,
                                 opts->dry_run, opts->json_output);
  }

  return 2;
}

int main(int argc, char **argv)
{
  kafsresize_options_t opts = {0};

  if (kafs_cli_exit_if_help(argc, argv, usage, argv[0]) == 0)
    return 0;

  if (kafsresize_parse_args(argc, argv, &opts) != 0)
    return 2;

  return kafsresize_run(&opts);
}
