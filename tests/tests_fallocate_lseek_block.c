#include "test_utils.h"

#include <errno.h>
#include <fcntl.h>
#include <linux/falloc.h>
#include <signal.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

static int is_mounted_fuse(const char *mnt)
{
  FILE *fp = fopen("/proc/mounts", "r");
  if (!fp)
    return 0;
  char dev[256], dir[256], type[64];
  int mounted = 0;
  while (fscanf(fp, "%255s %255s %63s %*[^\n]\n", dev, dir, type) == 3)
  {
    if (strcmp(dir, mnt) == 0 && strncmp(type, "fuse", 4) == 0)
    {
      mounted = 1;
      break;
    }
  }
  fclose(fp);
  return mounted;
}

static pid_t spawn_kafs(const char *img, const char *mnt)
{
  mkdir(mnt, 0700);
  pid_t pid = fork();
  if (pid < 0)
    return -1;
  if (pid == 0)
  {
    setenv("KAFS_IMAGE", img, 1);
    int lfd = open("minisrv.log", O_CREAT | O_TRUNC | O_WRONLY, 0644);
    if (lfd >= 0)
    {
      dup2(lfd, STDERR_FILENO);
      dup2(lfd, STDOUT_FILENO);
      close(lfd);
    }
    const char *kafs = kafs_test_kafs_bin();
    char *args[] = {(char *)kafs, (char *)mnt, "-f", NULL};
    execvp(args[0], args);
    _exit(127);
  }

  for (int i = 0; i < 100; ++i)
  {
    if (is_mounted_fuse(mnt))
      return pid;
    struct timespec ts = {0, 100 * 1000 * 1000};
    nanosleep(&ts, NULL);
  }

  kill(pid, SIGTERM);
  waitpid(pid, NULL, 0);
  return -1;
}

static int verify_region(const uint8_t *buf, size_t start, size_t end, uint8_t v)
{
  for (size_t i = start; i < end; ++i)
    if (buf[i] != v)
      return -1;
  return 0;
}

int main(void)
{
  const char *img = "fallocate_lseek.img";
  const char *mnt = "mnt-fallocate-lseek";
  const char *file = "mnt-fallocate-lseek/file";
  const size_t bs = 4096;

  if (kafs_test_enter_tmpdir("fallocate_lseek_block") != 0)
    return 77;

  kafs_context_t ctx;
  off_t mapsize = 0;
  if (kafs_test_mkimg_with_hrl(img, 64u * 1024u * 1024u, 12, 4096, &ctx, &mapsize) != 0)
    return 77;
  munmap(ctx.c_superblock, mapsize);
  close(ctx.c_fd);

  pid_t srv = spawn_kafs(img, mnt);
  if (srv <= 0)
    return 77;

  int rc = 0;
  int fd = open(file, O_CREAT | O_RDWR, 0644);
  if (fd < 0)
  {
    rc = 1;
    goto out;
  }

  uint8_t *w = (uint8_t *)malloc(bs * 3);
  uint8_t *r = (uint8_t *)malloc(bs * 3);
  if (!w || !r)
  {
    rc = 1;
    goto out_fd;
  }

  memset(w + 0 * bs, 0x11, bs);
  memset(w + 1 * bs, 0x22, bs);
  memset(w + 2 * bs, 0x33, bs);
  if (pwrite(fd, w, bs * 3, 0) != (ssize_t)(bs * 3))
  {
    rc = 1;
    goto out_buf;
  }

  // [bs/2, bs/2 + 2*bs) を punch: 中央1ブロックのみ穴化、端はゼロ埋めを期待。
  if (fallocate(fd, FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE, (off_t)(bs / 2),
                (off_t)(bs * 2)) != 0)
  {
    rc = 1;
    goto out_buf;
  }

  struct stat st;
  if (fstat(fd, &st) != 0 || st.st_size != (off_t)(bs * 3))
  {
    rc = 1;
    goto out_buf;
  }

  if (pread(fd, r, bs * 3, 0) != (ssize_t)(bs * 3))
  {
    rc = 1;
    goto out_buf;
  }

  if (verify_region(r, 0, bs / 2, 0x11) != 0 || verify_region(r, bs / 2, bs, 0x00) != 0)
    rc = 1;
  if (verify_region(r, bs, 2 * bs, 0x00) != 0)
    rc = 1;
  if (verify_region(r, 2 * bs, 2 * bs + bs / 2, 0x00) != 0 ||
      verify_region(r, 2 * bs + bs / 2, 3 * bs, 0x33) != 0)
    rc = 1;

  if (lseek(fd, 0, SEEK_DATA) != 0)
    rc = 1;
  if (lseek(fd, 0, SEEK_HOLE) != (off_t)bs)
    rc = 1;
  if (lseek(fd, (off_t)bs, SEEK_HOLE) != (off_t)bs)
    rc = 1;
  if (lseek(fd, (off_t)bs, SEEK_DATA) != (off_t)(2 * bs))
    rc = 1;

  // 拡大方向: 元 EOF 以降は hole として疎拡張されることを確認。
  off_t old_eof = (off_t)(3 * bs);
  off_t grow_off = old_eof + (off_t)123;
  off_t grow_len = (off_t)(2 * bs);
  if (fallocate(fd, 0, grow_off, grow_len) != 0)
  {
    rc = 1;
    goto out_buf;
  }
  if (fstat(fd, &st) != 0 || st.st_size != grow_off + grow_len)
    rc = 1;

  uint8_t tail[128];
  if (pread(fd, tail, sizeof(tail), old_eof) != (ssize_t)sizeof(tail))
    rc = 1;
  for (size_t i = 0; i < sizeof(tail); ++i)
    if (tail[i] != 0)
      rc = 1;

  errno = 0;
  if (lseek(fd, old_eof, SEEK_DATA) != -1 || errno != ENXIO)
    rc = 1;
  if (lseek(fd, old_eof, SEEK_HOLE) != old_eof)
    rc = 1;

out_buf:
  free(w);
  free(r);
out_fd:
  close(fd);
out:
  kafs_test_stop_kafs(mnt, srv);
  if (rc == 0)
    fprintf(stderr, "fallocate_lseek_block OK\n");
  return rc;
}
