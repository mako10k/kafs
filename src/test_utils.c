#include "kafs.h"
#include "kafs_context.h"
#include "kafs_superblock.h"
#include "kafs_block.h"
#include "kafs_inode.h"
#include "kafs_hash.h"

#include <errno.h>
#include <fcntl.h>
#include <limits.h>
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

static const char *abspath_no_fs(const char *path, char out[PATH_MAX])
{
  if (!path || !*path)
    return path;
  if (path[0] == '/')
  {
    strncpy(out, path, PATH_MAX - 1);
    out[PATH_MAX - 1] = '\0';
    return out;
  }
  char cwd[PATH_MAX];
  if (!getcwd(cwd, sizeof(cwd)))
    return path;
  if ((size_t)snprintf(out, PATH_MAX, "%s/%s", cwd, path) >= PATH_MAX)
    return path;
  return out;
}

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

static int run_cmd_timeout(char *const argv[], int timeout_ms)
{
  pid_t p = fork();
  if (p < 0)
    return -errno;
  if (p == 0)
  {
    execvp(argv[0], argv);
    _exit(127);
  }
  const int step_ms = 50;
  int waited = 0;
  for (;;)
  {
    int st = 0;
    pid_t w = waitpid(p, &st, WNOHANG);
    if (w == p)
    {
      if (WIFEXITED(st) && WEXITSTATUS(st) == 0)
        return 0;
      return -1;
    }
    if (w < 0)
      return -errno;

    if (timeout_ms >= 0 && waited >= timeout_ms)
    {
      (void)kill(p, SIGKILL);
      // Never block here: fusermount can get stuck in uninterruptible sleep.
      for (int i = 0; i < 100; ++i)
      { // best-effort reap (~1s)
        if (waitpid(p, NULL, WNOHANG) == p)
          return -ETIMEDOUT;
        struct timespec ts2 = {0, 10 * 1000 * 1000};
        nanosleep(&ts2, NULL);
      }
      (void)waitpid(p, NULL, WNOHANG);
      return -ETIMEDOUT;
    }

    struct timespec ts = {0, step_ms * 1000 * 1000};
    nanosleep(&ts, NULL);
    waited += step_ms;
  }
}

static void kill_wait_timeout(pid_t pid, int timeout_ms)
{
  if (pid <= 0)
    return;

  // already exited?
  {
    int st = 0;
    pid_t w = waitpid(pid, &st, WNOHANG);
    if (w == pid)
      return;
    if (w < 0 && errno == ECHILD)
      return;
  }

  (void)kill(pid, SIGTERM);
  const int step_ms = 50;
  int waited = 0;
  for (;;)
  {
    int st = 0;
    pid_t w = waitpid(pid, &st, WNOHANG);
    if (w == pid)
      return;
    if (w < 0)
    {
      if (errno == ECHILD)
        return;
      break;
    }
    if (timeout_ms >= 0 && waited >= timeout_ms)
      break;
    struct timespec ts = {0, step_ms * 1000 * 1000};
    nanosleep(&ts, NULL);
    waited += step_ms;
  }

  (void)kill(pid, SIGKILL);
  // Best-effort reap with a small bounded wait to avoid zombies.
  for (int i = 0; i < 100; ++i)
  { // ~1s
    if (waitpid(pid, NULL, WNOHANG) == pid)
      return;
    struct timespec ts2 = {0, 10 * 1000 * 1000};
    nanosleep(&ts2, NULL);
  }
  (void)waitpid(pid, NULL, WNOHANG);
}

void kafs_test_stop_kafs(const char *mnt, pid_t kafs_pid)
{
  char absbuf[PATH_MAX];
  const char *mp = abspath_no_fs(mnt, absbuf);

  // Try to unmount without hanging. Keep timeouts short to avoid blocking make check.
  if (mp)
  {
    char *um1[] = {"fusermount3", "-u", (char *)mp, NULL};
    (void)run_cmd_timeout(um1, 2000);

    if (is_mounted_fuse(mp))
    {
      char *um2[] = {"fusermount", "-u", (char *)mp, NULL};
      (void)run_cmd_timeout(um2, 2000);
    }
  }

  // Ensure server is not left running.
  kill_wait_timeout(kafs_pid, 2000);

  // If still mounted, force lazy unmount again after killing server.
  if (mp && is_mounted_fuse(mp))
  {
    char *um3[] = {"fusermount3", "-u", "-z", (char *)mp, NULL};
    (void)run_cmd_timeout(um3, 2000);
  }
}

int kafs_test_mkimg(const char *path, size_t bytes, unsigned log_bs, unsigned inodes,
                    int enable_hrl, kafs_context_t *out_ctx, off_t *out_mapsize)
{
  int fd = open(path, O_RDWR | O_CREAT | O_TRUNC, 0600);
  if (fd < 0)
    return -errno;

  kafs_blkcnt_t blkcnt = (kafs_blkcnt_t)(bytes >> log_bs);
  kafs_blksize_t bs = 1u << log_bs;
  kafs_blksize_t bmask = bs - 1u;

  off_t mapsize = 0;
  mapsize += sizeof(kafs_ssuperblock_t);
  mapsize = (mapsize + bmask) & ~bmask;
  void *blkmask_off = (void *)mapsize;
  mapsize += (blkcnt + 7) >> 3;
  mapsize = (mapsize + 7) & ~7;
  mapsize = (mapsize + bmask) & ~bmask;
  void *inotbl_off = (void *)mapsize;
  mapsize += sizeof(kafs_sinode_t) * inodes;
  mapsize = (mapsize + bmask) & ~bmask;

  // HRL 領域（任意）
  uint32_t bucket_cnt = 0;
  size_t hrl_index_size = 0;
  off_t hrl_index_off = 0;
  uint32_t entry_cnt = 0;
  off_t hrl_entry_off = 0;
  if (enable_hrl)
  {
    bucket_cnt = 1024;
    while ((bucket_cnt << 1) <= (uint32_t)(blkcnt / 4))
      bucket_cnt <<= 1;
    hrl_index_size = (size_t)bucket_cnt * sizeof(uint32_t);
    hrl_index_off = mapsize;
    mapsize += hrl_index_size;
    mapsize = (mapsize + 7) & ~7;
    entry_cnt = (uint32_t)(blkcnt / 2);
    hrl_entry_off = mapsize;
    mapsize += (off_t)entry_cnt * (off_t)sizeof(kafs_hrl_entry_t);
    mapsize = (mapsize + bmask) & ~bmask;
  }

  // In-image journal region (fixed 1MiB for tests)
  size_t journal_bytes = 1u << 20;
  off_t journal_off = mapsize;
  mapsize += (off_t)journal_bytes;
  mapsize = (mapsize + bmask) & ~bmask;

  if (ftruncate(fd, mapsize + (off_t)bs * blkcnt) < 0)
  {
    int err = -errno;
    close(fd);
    return err;
  }

  kafs_ssuperblock_t *sb = mmap(NULL, mapsize, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
  if (sb == MAP_FAILED)
  {
    int err = -errno;
    close(fd);
    return err;
  }
  memset((char *)sb + (intptr_t)blkmask_off, 0, ((size_t)blkcnt + 7) >> 3);
  memset((char *)sb + (intptr_t)inotbl_off, 0, sizeof(kafs_sinode_t) * inodes);

  kafs_sb_log_blksize_set(sb, log_bs);
  kafs_sb_magic_set(sb, KAFS_MAGIC);
  kafs_sb_format_version_set(sb, KAFS_FORMAT_VERSION);
  kafs_sb_hash_fast_set(sb, KAFS_HASH_FAST_XXH64);
  kafs_sb_hash_strong_set(sb, KAFS_HASH_STRONG_BLAKE3_256);
  if (enable_hrl)
  {
    kafs_sb_hrl_index_offset_set(sb, (uint64_t)hrl_index_off);
    kafs_sb_hrl_index_size_set(sb, (uint64_t)hrl_index_size);
    kafs_sb_hrl_entry_offset_set(sb, (uint64_t)hrl_entry_off);
    kafs_sb_hrl_entry_cnt_set(sb, (uint32_t)entry_cnt);
  }
  kafs_sb_journal_offset_set(sb, (uint64_t)journal_off);
  kafs_sb_journal_size_set(sb, (uint64_t)journal_bytes);
  kafs_sb_journal_flags_set(sb, 0);

  sb->s_inocnt = kafs_inocnt_htos(inodes);
  kafs_sb_inocnt_free_set(sb, (inodes > (kafs_inocnt_t)KAFS_INO_ROOTDIR) ? (inodes - 1) : 0);
  sb->s_blkcnt = kafs_blkcnt_htos(blkcnt);
  sb->s_r_blkcnt = kafs_blkcnt_htos(blkcnt);
  kafs_blkcnt_t fdb = (kafs_blkcnt_t)(mapsize >> log_bs);
  sb->s_first_data_block = kafs_blkcnt_htos(fdb);
  kafs_sb_blkcnt_free_set(sb, blkcnt - fdb);

  kafs_context_t c = {0};
  c.c_superblock = sb;
  c.c_fd = fd;
  c.c_blkmasktbl = (kafs_blkmask_t *)((char *)sb + (intptr_t)blkmask_off);
  c.c_inotbl = (kafs_sinode_t *)((char *)sb + (intptr_t)inotbl_off);
  c.c_blo_search = 0;
  c.c_ino_search = 0;
  c.c_hrl_index = enable_hrl ? (void *)((char *)sb + hrl_index_off) : NULL;
  c.c_hrl_bucket_cnt = enable_hrl ? bucket_cnt : 0u;

  for (kafs_blkcnt_t blo = 0; blo < fdb; ++blo)
    kafs_blk_set_usage(&c, blo, KAFS_TRUE);
  if (enable_hrl)
    kafs_hrl_format(&c);

  // Initialize root inode to a usable directory for tests
  kafs_sinode_t *root = &c.c_inotbl[KAFS_INO_ROOTDIR];
  {
    // set mode/uid/gid/size/times
    kafs_ino_mode_set(root, S_IFDIR | 0777);
    kafs_ino_uid_set(root, (kafs_uid_t)getuid());
    kafs_ino_gid_set(root, (kafs_gid_t)getgid());
    kafs_ino_size_set(root, 0);
    kafs_time_t now = kafs_now();
    kafs_time_t nulltime = (kafs_time_t){0, 0};
    kafs_ino_atime_set(root, now);
    kafs_ino_ctime_set(root, now);
    kafs_ino_mtime_set(root, now);
    kafs_ino_dtime_set(root, nulltime);
    kafs_ino_linkcnt_set(root, 1);
    kafs_ino_blocks_set(root, 0);
    kafs_ino_dev_set(root, 0);
    memset(root->i_blkreftbl, 0, sizeof(root->i_blkreftbl));
  }

  *out_ctx = c;
  *out_mapsize = mapsize;
  return 0;
}
