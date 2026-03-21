#include "kafs_context.h"
#include "test_utils.h"

#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <stdarg.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

static void tlogf(const char *fmt, ...)
{
  va_list ap;
  va_start(ap, fmt);
  vfprintf(stderr, fmt, ap);
  fputc('\n', stderr);
  va_end(ap);
}

static const kafs_test_mount_options_t k_mount_options = {
    .debug = "2",
    .log_path = "minisrv.log",
    .multithread = 1,
    .timeout_ms = 5000,
};

static int write_pattern_file(const char *path, size_t size, unsigned seed)
{
  int fd = open(path, O_CREAT | O_TRUNC | O_WRONLY, 0644);
  if (fd < 0)
    return -errno;

  unsigned state = seed ? seed : 1u;
  char buf[8192];
  size_t left = size;
  while (left > 0)
  {
    size_t chunk = left < sizeof(buf) ? left : sizeof(buf);
    for (size_t i = 0; i < chunk; ++i)
    {
      state = 1664525u * state + 1013904223u;
      buf[i] = (char)(state >> 24);
    }
    ssize_t written = write(fd, buf, chunk);
    if (written < 0)
    {
      int err = -errno;
      close(fd);
      return err;
    }
    left -= (size_t)written;
  }

  close(fd);
  return 0;
}

static int copy_file(const char *src, const char *dst)
{
  int src_fd = open(src, O_RDONLY);
  if (src_fd < 0)
    return -errno;
  int dst_fd = open(dst, O_CREAT | O_TRUNC | O_WRONLY, 0644);
  if (dst_fd < 0)
  {
    int err = -errno;
    close(src_fd);
    return err;
  }

  char buf[8192];
  ssize_t read_bytes;
  while ((read_bytes = read(src_fd, buf, sizeof(buf))) > 0)
  {
    ssize_t off = 0;
    while (off < read_bytes)
    {
      ssize_t written = write(dst_fd, buf + off, (size_t)(read_bytes - off));
      if (written < 0)
      {
        int err = -errno;
        close(src_fd);
        close(dst_fd);
        return err;
      }
      off += written;
    }
  }

  int rc = (read_bytes < 0) ? -errno : 0;
  close(src_fd);
  close(dst_fd);
  return rc;
}

static int checksum_file(const char *path, unsigned *out_sum)
{
  int fd = open(path, O_RDONLY);
  if (fd < 0)
    return -errno;

  unsigned sum = 0;
  char buf[8192];
  ssize_t read_bytes;
  while ((read_bytes = read(fd, buf, sizeof(buf))) > 0)
  {
    for (ssize_t i = 0; i < read_bytes; ++i)
      sum = (sum * 131u) + (unsigned)(unsigned char)buf[i];
  }

  int rc = (read_bytes < 0) ? -errno : 0;
  close(fd);
  if (rc == 0)
    *out_sum = sum;
  return rc;
}

int main(void)
{
  if (kafs_test_enter_tmpdir("git_template_copy_mt") != 0)
    return 77;

  const char *img = "clone-mt.img";
  const char *mnt = "mnt-clone-mt";
  const char *host_tmpl = "host-tmpl-mt";

  mkdir(host_tmpl, 0700);
  const int file_count = 32;
  unsigned expect_sum[file_count];
  for (int index = 0; index < file_count; ++index)
  {
    char path[PATH_MAX];
    int path_len = snprintf(path, sizeof(path), "%s/f%02d.sample", host_tmpl, index);
    if (path_len < 0 || path_len >= (int)sizeof(path))
    {
      tlogf("src path too long");
      return 77;
    }

    size_t size;
    switch (index % 5)
    {
    case 0:
      size = 0;
      break;
    case 1:
      size = 17;
      break;
    case 2:
      size = 8192;
      break;
    case 3:
      size = 65536;
      break;
    default:
      size = 12345;
      break;
    }

    if (write_pattern_file(path, size, (unsigned)(0xC001u + index)) != 0)
    {
      tlogf("prepare src failed: %d", errno);
      return 77;
    }
    if (checksum_file(path, &expect_sum[index]) != 0)
    {
      tlogf("src checksum failed");
      return 77;
    }
  }

  kafs_context_t ctx;
  off_t mapsize;
  if (kafs_test_mkimg_with_hrl(img, 64u * 1024u * 1024u, 12, 4096, &ctx, &mapsize) != 0)
  {
    tlogf("mkimg failed");
    return 77;
  }
  munmap(ctx.c_superblock, mapsize);
  close(ctx.c_fd);

  pid_t srv = kafs_test_start_kafs(img, mnt, &k_mount_options);
  if (srv <= 0)
  {
    tlogf("mount failed");
    return 77;
  }

  char target[PATH_MAX];
  snprintf(target, sizeof(target), "%s/clone/.git/hooks", mnt);
  if (mkdir("mnt-clone-mt/clone", 0777) != 0 && errno != EEXIST)
  {
    tlogf("mkdir clone failed:%s", strerror(errno));
    kafs_test_stop_kafs(mnt, srv);
    return 1;
  }
  if (mkdir("mnt-clone-mt/clone/.git", 0777) != 0 && errno != EEXIST)
  {
    tlogf("mkdir .git failed:%s", strerror(errno));
    kafs_test_stop_kafs(mnt, srv);
    return 1;
  }
  if (mkdir(target, 0777) != 0 && errno != EEXIST)
  {
    tlogf("mkdir hooks failed:%s", strerror(errno));
    kafs_test_stop_kafs(mnt, srv);
    return 1;
  }

  const int worker_count = 4;
  pid_t workers[worker_count];
  memset(workers, 0, sizeof(workers));
  for (int worker = 0; worker < worker_count; ++worker)
  {
    pid_t child = fork();
    if (child == 0)
    {
      for (int index = worker; index < file_count; index += worker_count)
      {
        char src[PATH_MAX];
        int src_len = snprintf(src, sizeof(src), "%s/f%02d.sample", host_tmpl, index);
        if (src_len < 0 || src_len >= (int)sizeof(src))
        {
          tlogf("src path too long %d", index);
          _exit(11);
        }

        char dst[PATH_MAX];
        int dst_len = snprintf(dst, sizeof(dst), "%s/f%02d.sample", target, index);
        if (dst_len < 0 || dst_len >= (int)sizeof(dst))
        {
          tlogf("dst path too long %d", index);
          _exit(12);
        }

        int rc = copy_file(src, dst);
        if (rc != 0)
        {
          tlogf("copy %d failed:%s", index, strerror(-rc));
          _exit(10);
        }
      }
      _exit(0);
    }
    workers[worker] = child;
  }

  int ok = 1;
  for (int worker = 0; worker < worker_count; ++worker)
  {
    int status = 0;
    waitpid(workers[worker], &status, 0);
    if (!WIFEXITED(status) || WEXITSTATUS(status) != 0)
      ok = 0;
  }

  if (ok)
  {
    for (int index = 0; index < file_count; ++index)
    {
      char dst[PATH_MAX];
      int dst_len = snprintf(dst, sizeof(dst), "%s/f%02d.sample", target, index);
      if (dst_len < 0 || dst_len >= (int)sizeof(dst))
      {
        tlogf("dst path too long %d", index);
        ok = 0;
        break;
      }

      unsigned got = 0;
      int rc = checksum_file(dst, &got);
      if (rc != 0)
      {
        tlogf("verify open failed %d:%s", index, strerror(-rc));
        ok = 0;
        break;
      }
      if (got != expect_sum[index])
      {
        tlogf("checksum mismatch %d", index);
        ok = 0;
        break;
      }
    }
  }

  if (!ok)
  {
    kafs_test_dump_log("minisrv.log", "copy verification failed");
    kafs_test_stop_kafs(mnt, srv);
    return 1;
  }

  kafs_test_stop_kafs(mnt, srv);
  tlogf("git_template_copy_mt OK");
  return 0;
}
