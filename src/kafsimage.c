#include "kafs.h"
#include "kafs_superblock.h"

#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#define COPY_BUF_SIZE (1u << 20)

static void usage(const char *prog)
{
  fprintf(stderr, "Usage: %s --metadata-only [--verify] <src-image> <dst-file>\n", prog);
}

static int pread_all(int fd, void *buf, size_t size, off_t off)
{
  char *p = (char *)buf;
  size_t done = 0;
  while (done < size)
  {
    ssize_t r = pread(fd, p + done, size - done, off + (off_t)done);
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

static int copy_nbytes(int fd_src, int fd_dst, uint64_t bytes)
{
  char *buf = (char *)malloc(COPY_BUF_SIZE);
  if (!buf)
    return -ENOMEM;

  uint64_t left = bytes;
  while (left > 0)
  {
    size_t chunk = (left > COPY_BUF_SIZE) ? COPY_BUF_SIZE : (size_t)left;
    ssize_t r = read(fd_src, buf, chunk);
    if (r == 0)
    {
      free(buf);
      return -EIO;
    }
    if (r < 0)
    {
      if (errno == EINTR)
        continue;
      free(buf);
      return -errno;
    }

    size_t wr_done = 0;
    while (wr_done < (size_t)r)
    {
      ssize_t w = write(fd_dst, buf + wr_done, (size_t)r - wr_done);
      if (w < 0)
      {
        if (errno == EINTR)
          continue;
        free(buf);
        return -errno;
      }
      wr_done += (size_t)w;
    }

    left -= (uint64_t)r;
  }

  free(buf);
  return 0;
}

static int verify_prefix_equal(int fd_src, int fd_dst, uint64_t bytes)
{
  char *a = (char *)malloc(COPY_BUF_SIZE);
  char *b = (char *)malloc(COPY_BUF_SIZE);
  if (!a || !b)
  {
    free(a);
    free(b);
    return -ENOMEM;
  }

  uint64_t checked = 0;
  while (checked < bytes)
  {
    size_t chunk = (bytes - checked > COPY_BUF_SIZE) ? COPY_BUF_SIZE : (size_t)(bytes - checked);
    int rc = pread_all(fd_src, a, chunk, (off_t)checked);
    if (rc != 0)
    {
      free(a);
      free(b);
      return rc;
    }
    rc = pread_all(fd_dst, b, chunk, (off_t)checked);
    if (rc != 0)
    {
      free(a);
      free(b);
      return rc;
    }
    if (memcmp(a, b, chunk) != 0)
    {
      free(a);
      free(b);
      return -EUCLEAN;
    }
    checked += chunk;
  }

  free(a);
  free(b);
  return 0;
}

int main(int argc, char **argv)
{
  int metadata_only = 0;
  int verify = 0;
  const char *src_path = NULL;
  const char *dst_path = NULL;

  for (int i = 1; i < argc; ++i)
  {
    if (strcmp(argv[i], "--metadata-only") == 0)
    {
      metadata_only = 1;
      continue;
    }
    if (strcmp(argv[i], "--verify") == 0)
    {
      verify = 1;
      continue;
    }
    if (argv[i][0] == '-')
    {
      usage(argv[0]);
      return 2;
    }
    if (!src_path)
      src_path = argv[i];
    else if (!dst_path)
      dst_path = argv[i];
    else
    {
      usage(argv[0]);
      return 2;
    }
  }

  if (!metadata_only || !src_path || !dst_path)
  {
    usage(argv[0]);
    return 2;
  }

  int fd_src = open(src_path, O_RDONLY);
  if (fd_src < 0)
  {
    perror("open src");
    return 1;
  }

  struct stat st_src;
  if (fstat(fd_src, &st_src) != 0)
  {
    perror("fstat src");
    close(fd_src);
    return 1;
  }

  kafs_ssuperblock_t sb;
  int rc = pread_all(fd_src, &sb, sizeof(sb), 0);
  if (rc != 0)
  {
    fprintf(stderr, "failed to read superblock: %s\n", strerror(-rc));
    close(fd_src);
    return 1;
  }

  uint64_t blksize = kafs_sb_blksize_get(&sb);
  uint64_t first_data_block = kafs_sb_first_data_block_get(&sb);
  if (blksize == 0 || first_data_block > (UINT64_MAX / blksize))
  {
    fprintf(stderr, "invalid superblock geometry\n");
    close(fd_src);
    return 1;
  }

  uint64_t metadata_bytes = first_data_block * blksize;
  if (metadata_bytes == 0 || metadata_bytes > (uint64_t)st_src.st_size)
  {
    fprintf(stderr, "metadata range is out of source image bounds\n");
    close(fd_src);
    return 1;
  }

  int fd_dst = open(dst_path, O_WRONLY | O_CREAT | O_TRUNC, 0644);
  if (fd_dst < 0)
  {
    perror("open dst");
    close(fd_src);
    return 1;
  }

  if (lseek(fd_src, 0, SEEK_SET) < 0)
  {
    perror("lseek src");
    close(fd_dst);
    close(fd_src);
    return 1;
  }

  rc = copy_nbytes(fd_src, fd_dst, metadata_bytes);
  if (rc != 0)
  {
    fprintf(stderr, "copy failed: %s\n", strerror(-rc));
    close(fd_dst);
    close(fd_src);
    return 1;
  }

  if (fsync(fd_dst) != 0)
  {
    perror("fsync dst");
    close(fd_dst);
    close(fd_src);
    return 1;
  }

  if (close(fd_dst) != 0)
  {
    perror("close dst");
    close(fd_src);
    return 1;
  }

  if (verify)
  {
    int fd_verify = open(dst_path, O_RDONLY);
    if (fd_verify < 0)
    {
      perror("open verify dst");
      close(fd_src);
      return 1;
    }

    struct stat st_dst;
    if (fstat(fd_verify, &st_dst) != 0)
    {
      perror("fstat verify dst");
      close(fd_verify);
      close(fd_src);
      return 1;
    }

    if ((uint64_t)st_dst.st_size != metadata_bytes)
    {
      fprintf(stderr, "verify failed: size mismatch (%" PRIu64 " != %" PRIu64 ")\n",
              (uint64_t)st_dst.st_size, metadata_bytes);
      close(fd_verify);
      close(fd_src);
      return 1;
    }

    rc = verify_prefix_equal(fd_src, fd_verify, metadata_bytes);
    if (rc != 0)
    {
      fprintf(stderr, "verify failed: %s\n", strerror(-rc));
      close(fd_verify);
      close(fd_src);
      return 1;
    }

    close(fd_verify);
  }

  printf("kafsimage: metadata-only export completed\n");
  printf("  src: %s\n", src_path);
  printf("  dst: %s\n", dst_path);
  printf("  bytes: %" PRIu64 "\n", metadata_bytes);
  printf("  verified: %s\n", verify ? "true" : "false");

  close(fd_src);
  return 0;
}
