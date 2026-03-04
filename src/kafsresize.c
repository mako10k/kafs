#include "kafs.h"
#include "kafs_superblock.h"

#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

static void usage(const char *prog)
{
  fprintf(stderr,
          "Usage: %s --grow --size-bytes N <image>\n"
          "  notes:\n"
          "    - grow-only (no shrink)\n"
          "    - v0 supports growth only within preallocated headroom (s_blkcnt < s_r_blkcnt)\n",
          prog);
}

static int parse_size_bytes(const char *arg, uint64_t *out)
{
  if (!arg || !out || *arg == '\0')
    return -1;
  char *endp = NULL;
  errno = 0;
  unsigned long long v = strtoull(arg, &endp, 0);
  if (errno != 0 || endp == arg)
    return -1;
  if (*endp == '\0')
  {
    *out = (uint64_t)v;
    return 0;
  }
  if (endp[1] != '\0')
    return -1;
  switch (*endp)
  {
  case 'K':
  case 'k':
    v <<= 10;
    break;
  case 'M':
  case 'm':
    v <<= 20;
    break;
  case 'G':
  case 'g':
    v <<= 30;
    break;
  default:
    return -1;
  }
  *out = (uint64_t)v;
  return 0;
}

static int pread_all(int fd, void *buf, size_t sz, off_t off)
{
  char *p = (char *)buf;
  size_t done = 0;
  while (done < sz)
  {
    ssize_t r = pread(fd, p + done, sz - done, off + (off_t)done);
    if (r == 0)
      return -EIO;
    if (r < 0)
    {
      if (errno == EINTR)
        continue;
      return -errno;
    }
    done += (size_t)r;
  }
  return 0;
}

static int pwrite_all(int fd, const void *buf, size_t sz, off_t off)
{
  const char *p = (const char *)buf;
  size_t done = 0;
  while (done < sz)
  {
    ssize_t w = pwrite(fd, p + done, sz - done, off + (off_t)done);
    if (w < 0)
    {
      if (errno == EINTR)
        continue;
      return -errno;
    }
    done += (size_t)w;
  }
  return 0;
}

static void bitmap_clear_range(uint8_t *bm, uint32_t from_blo, uint32_t to_blo)
{
  for (uint32_t b = from_blo; b < to_blo; ++b)
  {
    uint32_t byte = b >> 3;
    uint32_t bit = b & 7u;
    bm[byte] &= (uint8_t)~(1u << bit);
  }
}

int main(int argc, char **argv)
{
  int do_grow = 0;
  uint64_t target_bytes = 0;
  const char *image = NULL;

  for (int i = 1; i < argc; ++i)
  {
    if (strcmp(argv[i], "--grow") == 0)
    {
      do_grow = 1;
      continue;
    }
    if (strcmp(argv[i], "--size-bytes") == 0 && i + 1 < argc)
    {
      if (parse_size_bytes(argv[++i], &target_bytes) != 0)
      {
        fprintf(stderr, "invalid size-bytes: %s\n", argv[i]);
        return 2;
      }
      continue;
    }
    if (argv[i][0] == '-')
    {
      usage(argv[0]);
      return 2;
    }
    image = argv[i];
  }

  if (!do_grow || target_bytes == 0 || !image)
  {
    usage(argv[0]);
    return 2;
  }

  int fd = open(image, O_RDWR);
  if (fd < 0)
  {
    perror("open");
    return 1;
  }

  kafs_ssuperblock_t sb;
  int rc = pread_all(fd, &sb, sizeof(sb), 0);
  if (rc != 0)
  {
    fprintf(stderr, "failed to read superblock: %s\n", strerror(-rc));
    close(fd);
    return 1;
  }

  if (kafs_sb_magic_get(&sb) != KAFS_MAGIC ||
      kafs_sb_format_version_get(&sb) != KAFS_FORMAT_VERSION)
  {
    fprintf(stderr, "unsupported or invalid image format\n");
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

  // Bitmap layout is fixed by s_r_blkcnt at format time.
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

  rc = pread_all(fd, bm, (size_t)blkmask_bytes, (off_t)blkmask_off);
  if (rc != 0)
  {
    fprintf(stderr, "failed to read block bitmap: %s\n", strerror(-rc));
    free(bm);
    close(fd);
    return 1;
  }

  bitmap_clear_range(bm, old_blkcnt, target_blkcnt);

  rc = pwrite_all(fd, bm, (size_t)blkmask_bytes, (off_t)blkmask_off);
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

  rc = pwrite_all(fd, &sb, sizeof(sb), 0);
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
