#include "kafs.h"
#include "kafs_superblock.h"
#include "kafs_tool_util.h"

#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
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

static void usage(const char *prog)
{
  fprintf(stderr,
          "Usage: %s --grow --size-bytes N <image>\n"
          "       %s --migrate-create --dst-image <image> [--size-bytes N] --inodes I [options]\n"
          "  notes:\n"
          "    - grow-only (no shrink)\n"
          "    - v0 grow supports only preallocated headroom (s_blkcnt < s_r_blkcnt)\n"
          "    - migrate-create builds a new image with target size/inodes via mkfs.kafs\n"
          "    - migrate-create without --size-bytes auto-detects size from --dst-image\n"
          "  options for --migrate-create:\n"
          "    --journal-size-bytes N   journal size passed to mkfs.kafs\n"
          "    --blksize-log L          block-size log2 passed to mkfs.kafs\n"
          "    --hrl-entry-ratio R      HRL entries/data-block ratio passed to mkfs.kafs\n"
          "    --src-mount PATH         print suggested rsync source mount\n"
          "    --dst-mount PATH         print suggested destination mount\n"
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
  if (kafs_sb_magic_get(sb) != KAFS_MAGIC || kafs_sb_format_version_get(sb) != KAFS_FORMAT_VERSION)
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

static int cmd_migrate_create(const char *dst_image, uint64_t size_bytes, uint32_t inodes,
                              uint64_t journal_bytes, int blksize_log, double hrl_entry_ratio,
                              const char *src_mount, const char *dst_mount, int assume_yes,
                              int force)
{
  if (!dst_image || !*dst_image)
  {
    fprintf(stderr, "invalid --dst-image\n");
    return 2;
  }

  struct stat dst_st;
  int dst_exists = (stat(dst_image, &dst_st) == 0);
  int dst_is_reg = dst_exists && S_ISREG(dst_st.st_mode);
  int dst_is_blk = dst_exists && S_ISBLK(dst_st.st_mode);

  if (size_bytes == 0)
  {
    uint64_t auto_size = 0;
    int drc = detect_dst_size_bytes(dst_image, &auto_size);
    if (drc != 0)
    {
      fprintf(stderr, "failed to auto-detect size from --dst-image '%s' (use --size-bytes): %s\n",
              dst_image, strerror(-drc));
      return 2;
    }
    size_bytes = auto_size;
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
  char jbuf[32];
  char lbuf[32];
  char rbuf[32];
  snprintf(size_buf, sizeof(size_buf), "%" PRIu64, size_bytes);
  snprintf(inode_buf, sizeof(inode_buf), "%" PRIu32, inodes);
  snprintf(jbuf, sizeof(jbuf), "%" PRIu64, journal_bytes);
  snprintf(lbuf, sizeof(lbuf), "%d", blksize_log);
  snprintf(rbuf, sizeof(rbuf), "%.6f", hrl_entry_ratio);

  const char *mkfs = resolve_mkfs_prog();
  char *argv[20];
  int ai = 0;
  argv[ai++] = (char *)mkfs;
  argv[ai++] = (char *)dst_image;
  argv[ai++] = "--size-bytes";
  argv[ai++] = size_buf;
  argv[ai++] = "--inodes";
  argv[ai++] = inode_buf;
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
  if (journal_bytes > 0)
    printf("  journal_bytes: %" PRIu64 "\n", journal_bytes);
  if (blksize_log > 0)
    printf("  blksize_log: %d\n", blksize_log);
  if (hrl_entry_ratio > 0.0)
    printf("  hrl_entry_ratio: %.6f\n", hrl_entry_ratio);

  print_migrate_next_steps(dst_image, src_mount, dst_mount);

  return 0;
}

int main(int argc, char **argv)
{
  int do_grow = 0;
  int do_migrate_create = 0;
  int assume_yes = 0;
  int force = 0;
  uint64_t target_bytes = 0;
  uint64_t journal_bytes = 0;
  int blksize_log = 0;
  double hrl_entry_ratio = 0.0;
  uint32_t inodes = 0;
  const char *image = NULL;
  const char *dst_image = NULL;
  const char *src_mount = NULL;
  const char *dst_mount = NULL;

  for (int i = 1; i < argc; ++i)
  {
    if (strcmp(argv[i], "--grow") == 0)
    {
      do_grow = 1;
      continue;
    }
    if (strcmp(argv[i], "--migrate-create") == 0)
    {
      do_migrate_create = 1;
      continue;
    }
    if (strcmp(argv[i], "--size-bytes") == 0 && i + 1 < argc)
    {
      if (kafs_parse_size_bytes_u64(argv[++i], &target_bytes) != 0)
      {
        fprintf(stderr, "invalid size-bytes: %s\n", argv[i]);
        return 2;
      }
      continue;
    }
    if (strcmp(argv[i], "--journal-size-bytes") == 0 && i + 1 < argc)
    {
      if (kafs_parse_size_bytes_u64(argv[++i], &journal_bytes) != 0)
      {
        fprintf(stderr, "invalid journal-size-bytes: %s\n", argv[i]);
        return 2;
      }
      continue;
    }
    if (strcmp(argv[i], "--blksize-log") == 0 && i + 1 < argc)
    {
      blksize_log = atoi(argv[++i]);
      if (blksize_log <= 0)
      {
        fprintf(stderr, "invalid blksize-log: %s\n", argv[i]);
        return 2;
      }
      continue;
    }
    if (strcmp(argv[i], "--hrl-entry-ratio") == 0 && i + 1 < argc)
    {
      if (kafs_parse_ratio_0_to_1(argv[++i], &hrl_entry_ratio) != 0)
      {
        fprintf(stderr, "invalid hrl-entry-ratio (expected 0<R<=1): %s\n", argv[i]);
        return 2;
      }
      continue;
    }
    if (strcmp(argv[i], "--inodes") == 0 && i + 1 < argc)
    {
      unsigned long long v = strtoull(argv[++i], NULL, 0);
      if (v == 0 || v > UINT32_MAX)
      {
        fprintf(stderr, "invalid inodes: %s\n", argv[i]);
        return 2;
      }
      inodes = (uint32_t)v;
      continue;
    }
    if (strcmp(argv[i], "--dst-image") == 0 && i + 1 < argc)
    {
      dst_image = argv[++i];
      continue;
    }
    if (strcmp(argv[i], "--src-mount") == 0 && i + 1 < argc)
    {
      src_mount = argv[++i];
      continue;
    }
    if (strcmp(argv[i], "--dst-mount") == 0 && i + 1 < argc)
    {
      dst_mount = argv[++i];
      continue;
    }
    if (strcmp(argv[i], "--yes") == 0)
    {
      assume_yes = 1;
      continue;
    }
    if (strcmp(argv[i], "--force") == 0)
    {
      force = 1;
      continue;
    }
    if (argv[i][0] == '-')
    {
      usage(argv[0]);
      return 2;
    }
    image = argv[i];
  }

  if (do_grow && do_migrate_create)
  {
    fprintf(stderr, "--grow and --migrate-create are mutually exclusive\n");
    return 2;
  }

  if (do_grow)
  {
    if (target_bytes == 0 || !image)
    {
      usage(argv[0]);
      return 2;
    }
    return cmd_grow(image, target_bytes);
  }

  if (do_migrate_create)
  {
    if (inodes == 0 || !dst_image)
    {
      usage(argv[0]);
      return 2;
    }
    return cmd_migrate_create(dst_image, target_bytes, inodes, journal_bytes, blksize_log,
                              hrl_entry_ratio, src_mount, dst_mount, assume_yes, force);
  }

  usage(argv[0]);
  return 2;
}
