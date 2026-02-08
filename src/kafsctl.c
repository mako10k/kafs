#include "kafs_ioctl.h"
#include "kafs_rpc.h"

#include <errno.h>
#include <inttypes.h>
#include <stdio.h>
#include <string.h>
#include <fcntl.h>
#include <unistd.h>
#include <stdlib.h>
#include <limits.h>
#include <sys/stat.h>
#include <time.h>

typedef enum
{
  KAFS_UNIT_KIB = 0,
  KAFS_UNIT_BYTES,
  KAFS_UNIT_MIB,
  KAFS_UNIT_GIB,
} kafs_unit_t;

static double unit_divisor(kafs_unit_t u)
{
  switch (u)
  {
  case KAFS_UNIT_BYTES:
    return 1.0;
  case KAFS_UNIT_MIB:
    return 1024.0 * 1024.0;
  case KAFS_UNIT_GIB:
    return 1024.0 * 1024.0 * 1024.0;
  case KAFS_UNIT_KIB:
  default:
    return 1024.0;
  }
}

static const char *unit_suffix(kafs_unit_t u)
{
  switch (u)
  {
  case KAFS_UNIT_BYTES:
    return "B";
  case KAFS_UNIT_MIB:
    return "MiB";
  case KAFS_UNIT_GIB:
    return "GiB";
  case KAFS_UNIT_KIB:
  default:
    return "KiB";
  }
}

static void print_bytes(uint64_t bytes, kafs_unit_t unit)
{
  if (unit == KAFS_UNIT_BYTES)
  {
    printf("%" PRIu64 "B", bytes);
    return;
  }

  const double v = (double)bytes / unit_divisor(unit);
  if (v >= 100.0)
    printf("%.0f%s", v, unit_suffix(unit));
  else
    printf("%.1f%s", v, unit_suffix(unit));
}

static void usage(const char *prog)
{
  fprintf(stderr,
          "Usage:\n"
          "  %s fsstat <mountpoint> [--json] [--bytes|--mib|--gib]   (alias: stats)\n"
          "  %s hotplug status <mountpoint> [--json]\n"
          "  %s stat <mountpoint> <path>\n"
          "  %s cat <mountpoint> <path>\n"
          "  %s write <mountpoint> <path>   (stdin -> file, trunc)\n"
          "  %s cp <mountpoint> <src> <dst> [--reflink]\n"
          "  %s mv <mountpoint> <src> <dst>\n"
          "  %s rm <mountpoint> <path>\n"
          "  %s mkdir <mountpoint> <path>\n"
          "  %s rmdir <mountpoint> <path>\n"
          "  %s ln <mountpoint> <src> <dst>\n"
          "  %s symlink <mountpoint> <target> <linkpath>\n"
          "  %s readlink <mountpoint> <path>\n"
          "  %s chmod <mountpoint> <octal_mode> <path>\n"
          "  %s touch <mountpoint> <path>\n",
          prog, prog, prog, prog, prog, prog, prog, prog, prog, prog, prog, prog, prog, prog,
          prog);
}

static const char *hotplug_state_str(uint32_t state)
{
  switch (state)
  {
  case KAFS_HOTPLUG_STATE_DISABLED:
    return "disabled";
  case KAFS_HOTPLUG_STATE_WAITING:
    return "waiting";
  case KAFS_HOTPLUG_STATE_CONNECTED:
    return "connected";
  case KAFS_HOTPLUG_STATE_ERROR:
    return "error";
  default:
    return "unknown";
  }
}

static const char *hotplug_data_mode_str(uint32_t mode)
{
  switch (mode)
  {
  case KAFS_RPC_DATA_INLINE:
    return "inline";
  case KAFS_RPC_DATA_PLAN_ONLY:
    return "plan_only";
  case KAFS_RPC_DATA_SHM:
    return "shm";
  default:
    return "unknown";
  }
}

static const char *hotplug_compat_str(uint32_t result)
{
  switch (result)
  {
  case KAFS_HOTPLUG_COMPAT_OK:
    return "ok";
  case KAFS_HOTPLUG_COMPAT_WARN:
    return "warn";
  case KAFS_HOTPLUG_COMPAT_REJECT:
    return "reject";
  case KAFS_HOTPLUG_COMPAT_UNKNOWN:
  default:
    return "unknown";
  }
}

static const char *hotplug_compat_reason_str(int32_t reason)
{
  switch (reason)
  {
  case 0:
    return "ok";
  case -EPROTONOSUPPORT:
    return "protocol_mismatch";
  case -EBADMSG:
    return "bad_message";
  default:
    return "unknown";
  }
}

static int cmd_hotplug_status(const char *mnt, int json)
{
  int fd = open(mnt, O_RDONLY | O_DIRECTORY);
  if (fd < 0)
  {
    perror("open");
    return 1;
  }

  kafs_hotplug_status_t st;
  memset(&st, 0, sizeof(st));
  if (ioctl(fd, KAFS_IOCTL_GET_HOTPLUG_STATUS, &st) != 0)
  {
    perror("ioctl(KAFS_IOCTL_GET_HOTPLUG_STATUS)");
    close(fd);
    return 1;
  }
  close(fd);

  if (json)
  {
    printf("{\n");
    printf("  \"version\": %u,\n", st.version);
    printf("  \"state\": %u,\n", st.state);
    printf("  \"state_str\": \"%s\",\n", hotplug_state_str(st.state));
    printf("  \"data_mode\": %u,\n", st.data_mode);
    printf("  \"data_mode_str\": \"%s\",\n", hotplug_data_mode_str(st.data_mode));
    printf("  \"session_id\": %" PRIu64 ",\n", st.session_id);
    printf("  \"epoch\": %u,\n", st.epoch);
    printf("  \"last_error\": %d,\n", st.last_error);
    printf("  \"wait_queue_len\": %u,\n", st.wait_queue_len);
    printf("  \"wait_timeout_ms\": %u,\n", st.wait_timeout_ms);
    printf("  \"wait_queue_limit\": %u,\n", st.wait_queue_limit);
    printf("  \"front_major\": %u,\n", st.front_major);
    printf("  \"front_minor\": %u,\n", st.front_minor);
    printf("  \"front_features\": %u,\n", st.front_features);
    printf("  \"back_major\": %u,\n", st.back_major);
    printf("  \"back_minor\": %u,\n", st.back_minor);
    printf("  \"back_features\": %u,\n", st.back_features);
    printf("  \"compat_result\": %u,\n", st.compat_result);
    printf("  \"compat_result_str\": \"%s\",\n", hotplug_compat_str(st.compat_result));
    printf("  \"compat_reason\": %d,\n", st.compat_reason);
    printf("  \"compat_reason_str\": \"%s\"\n", hotplug_compat_reason_str(st.compat_reason));
    printf("}\n");
    return 0;
  }

  printf("kafs hotplug status v%u\n", st.version);
  printf("  state: %u (%s)\n", st.state, hotplug_state_str(st.state));
  printf("  data_mode: %u (%s)\n", st.data_mode, hotplug_data_mode_str(st.data_mode));
  printf("  session_id: %" PRIu64 "\n", st.session_id);
  printf("  epoch: %u\n", st.epoch);
  printf("  last_error: %d\n", st.last_error);
  printf("  wait_queue_len: %u\n", st.wait_queue_len);
  printf("  wait_timeout_ms: %u\n", st.wait_timeout_ms);
  printf("  wait_queue_limit: %u\n", st.wait_queue_limit);
  printf("  front_version: %u.%u\n", st.front_major, st.front_minor);
  printf("  front_features: %u\n", st.front_features);
  printf("  back_version: %u.%u\n", st.back_major, st.back_minor);
  printf("  back_features: %u\n", st.back_features);
  printf("  compat_result: %u (%s)\n", st.compat_result, hotplug_compat_str(st.compat_result));
  printf("  compat_reason: %d (%s)\n", st.compat_reason,
         hotplug_compat_reason_str(st.compat_reason));
  return 0;
}

static int path_has_dotdot_component(const char *p)
{
  if (!p)
    return 0;
  const char *s = p;
  while (*s)
  {
    while (*s == '/')
      ++s;
    const char *c = s;
    while (*s && *s != '/')
      ++s;
    size_t len = (size_t)(s - c);
    if (len == 2 && c[0] == '.' && c[1] == '.')
      return 1;
  }
  return 0;
}

static const char *to_kafs_path(const char *mnt_abs, const char *p, char out[KAFS_IOCTL_PATH_MAX])
{
  if (!p || !*p)
    return NULL;
  if (p[0] == '/')
  {
    size_t ml = mnt_abs ? strlen(mnt_abs) : 0;
    if (ml && strncmp(p, mnt_abs, ml) == 0 && (p[ml] == '/' || p[ml] == '\0'))
    {
      const char *suf = p + ml;
      if (*suf == '\0')
        suf = "/";
      snprintf(out, KAFS_IOCTL_PATH_MAX, "%s", suf);
      return out;
    }
    snprintf(out, KAFS_IOCTL_PATH_MAX, "%s", p);
    return out;
  }
  snprintf(out, KAFS_IOCTL_PATH_MAX, "/%s", p);
  return out;
}

static const char *to_mount_rel_path(const char *mnt_abs, const char *p,
                                     char out[KAFS_IOCTL_PATH_MAX])
{
  if (!p || !*p)
    return NULL;

  const char *suf = p;
  if (p[0] == '/')
  {
    size_t ml = mnt_abs ? strlen(mnt_abs) : 0;
    if (ml && strncmp(p, mnt_abs, ml) == 0 && (p[ml] == '/' || p[ml] == '\0'))
      suf = p + ml;
    if (*suf == '/')
      ++suf;
  }

  if (*suf == '\0')
    return NULL;

  snprintf(out, KAFS_IOCTL_PATH_MAX, "%s", suf);
  if (out[0] == '/' || path_has_dotdot_component(out))
    return NULL;
  return out;
}

static int cmd_stats(const char *mnt, int json, kafs_unit_t unit)
{
  int fd = open(mnt, O_RDONLY | O_DIRECTORY);
  if (fd < 0)
  {
    perror("open");
    return 1;
  }

  kafs_stats_t st;
  memset(&st, 0, sizeof(st));
  if (ioctl(fd, KAFS_IOCTL_GET_STATS, &st) != 0)
  {
    perror("ioctl(KAFS_IOCTL_GET_STATS)");
    close(fd);
    return 1;
  }
  close(fd);

  uint64_t logical_bytes = st.hrl_refcnt_sum * (uint64_t)st.blksize;
  uint64_t unique_bytes = st.hrl_entries_used * (uint64_t)st.blksize;
  uint64_t saved_bytes = (st.hrl_refcnt_sum > st.hrl_entries_used)
                             ? (st.hrl_refcnt_sum - st.hrl_entries_used) * (uint64_t)st.blksize
                             : 0;

  double dedup_ratio = 1.0;
  if (unique_bytes > 0)
    dedup_ratio = (double)logical_bytes / (double)unique_bytes;

  double hit_rate = 0.0;
  if (st.hrl_put_calls > 0)
    hit_rate = (double)st.hrl_put_hits / (double)st.hrl_put_calls;

  if (json)
  {
    printf("{\n");
    printf("  \"version\": %" PRIu32 ",\n", st.version);
    printf("  \"blksize\": %" PRIu32 ",\n", st.blksize);
    printf("  \"fs_blocks_total\": %" PRIu64 ",\n", st.fs_blocks_total);
    printf("  \"fs_blocks_free\": %" PRIu64 ",\n", st.fs_blocks_free);
    printf("  \"fs_inodes_total\": %" PRIu64 ",\n", st.fs_inodes_total);
    printf("  \"fs_inodes_free\": %" PRIu64 ",\n", st.fs_inodes_free);
    printf("  \"hrl_entries_total\": %" PRIu64 ",\n", st.hrl_entries_total);
    printf("  \"hrl_entries_used\": %" PRIu64 ",\n", st.hrl_entries_used);
    printf("  \"hrl_entries_duplicated\": %" PRIu64 ",\n", st.hrl_entries_duplicated);
    printf("  \"hrl_refcnt_sum\": %" PRIu64 ",\n", st.hrl_refcnt_sum);
    printf("  \"logical_bytes\": %" PRIu64 ",\n", logical_bytes);
    printf("  \"unique_bytes\": %" PRIu64 ",\n", unique_bytes);
    printf("  \"saved_bytes\": %" PRIu64 ",\n", saved_bytes);
    printf("  \"dedup_ratio\": %.6f,\n", dedup_ratio);
    printf("  \"hrl_put_calls\": %" PRIu64 ",\n", st.hrl_put_calls);
    printf("  \"hrl_put_hits\": %" PRIu64 ",\n", st.hrl_put_hits);
    printf("  \"hrl_put_misses\": %" PRIu64 ",\n", st.hrl_put_misses);
    printf("  \"hrl_put_fallback_legacy\": %" PRIu64 ",\n", st.hrl_put_fallback_legacy);
    printf("  \"hrl_put_hit_rate\": %.6f\n", hit_rate);
    printf("}\n");
    return 0;
  }

  printf("kafs fsstat v%" PRIu32 "\n", st.version);
  printf("  blksize: ");
  print_bytes(st.blksize, unit);
  printf("\n");

  printf("  fs: blocks total=%" PRIu64 " (", st.fs_blocks_total);
  print_bytes(st.fs_blocks_total * (uint64_t)st.blksize, unit);
  printf(") free=%" PRIu64 " (", st.fs_blocks_free);
  print_bytes(st.fs_blocks_free * (uint64_t)st.blksize, unit);
  printf(")\n");
  printf("      inodes total=%" PRIu64 " free=%" PRIu64 "\n", st.fs_inodes_total,
         st.fs_inodes_free);

  printf("  hrl: entries used=%" PRIu64 "/%" PRIu64 " duplicated=%" PRIu64 " refsum=%" PRIu64 "\n",
         st.hrl_entries_used, st.hrl_entries_total, st.hrl_entries_duplicated, st.hrl_refcnt_sum);

  printf("  dedup: logical=");
  print_bytes(logical_bytes, unit);
  printf(" unique=");
  print_bytes(unique_bytes, unit);
  printf(" saved=");
  print_bytes(saved_bytes, unit);
  printf(" ratio=%.3f\n", dedup_ratio);
  printf("  hrl_put: calls=%" PRIu64 " hits=%" PRIu64 " misses=%" PRIu64 " fallback_legacy=%" PRIu64
         " hit_rate=%.3f\n",
         st.hrl_put_calls, st.hrl_put_hits, st.hrl_put_misses, st.hrl_put_fallback_legacy,
         hit_rate);
  return 0;
}

static void fmt_time(char out[64], const struct timespec *ts)
{
  if (!out)
    return;
  if (!ts)
  {
    out[0] = '\0';
    return;
  }
  time_t t = ts->tv_sec;
  struct tm tm;
  if (localtime_r(&t, &tm) == NULL)
  {
    snprintf(out, 64, "%lld", (long long)ts->tv_sec);
    return;
  }
  strftime(out, 64, "%Y-%m-%d %H:%M:%S", &tm);
}

static int cmd_stat(const char *mnt, const char *path)
{
  char mabs[KAFS_IOCTL_PATH_MAX];
  const char *mnt_abs = mnt;
  if (realpath(mnt, mabs) != NULL)
    mnt_abs = mabs;

  int dfd = open(mnt, O_RDONLY | O_DIRECTORY);
  if (dfd < 0)
  {
    perror("open");
    return 1;
  }

  char rel[KAFS_IOCTL_PATH_MAX];
  const char *p = to_mount_rel_path(mnt_abs, path, rel);
  if (!p)
  {
    fprintf(stderr, "invalid path\n");
    close(dfd);
    return 2;
  }

  struct stat st;
  if (fstatat(dfd, p, &st, AT_SYMLINK_NOFOLLOW) != 0)
  {
    perror("fstatat");
    close(dfd);
    return 1;
  }

  const char *t = "unknown";
  if (S_ISREG(st.st_mode))
    t = "file";
  else if (S_ISDIR(st.st_mode))
    t = "dir";
  else if (S_ISLNK(st.st_mode))
    t = "symlink";
  else if (S_ISCHR(st.st_mode))
    t = "char";
  else if (S_ISBLK(st.st_mode))
    t = "block";
  else if (S_ISFIFO(st.st_mode))
    t = "fifo";
  else if (S_ISSOCK(st.st_mode))
    t = "sock";

  char at[64], mt[64], ct[64];
#if defined(__APPLE__)
  (void)at;
  (void)mt;
  (void)ct;
#else
  fmt_time(at, &st.st_atim);
  fmt_time(mt, &st.st_mtim);
  fmt_time(ct, &st.st_ctim);
#endif

  printf("path: %s\n", path);
  printf("type: %s\n", t);
  printf("mode: %04o\n", (unsigned int)(st.st_mode & 07777));
  printf("uid: %u\n", (unsigned int)st.st_uid);
  printf("gid: %u\n", (unsigned int)st.st_gid);
  printf("size: %lld\n", (long long)st.st_size);
  printf("nlink: %llu\n", (unsigned long long)st.st_nlink);
  printf("ino: %llu\n", (unsigned long long)st.st_ino);
#if !defined(__APPLE__)
  printf("atime: %s\n", at);
  printf("mtime: %s\n", mt);
  printf("ctime: %s\n", ct);
#endif

  close(dfd);
  return 0;
}

static int cmd_cat(const char *mnt, const char *path)
{
  char mabs[KAFS_IOCTL_PATH_MAX];
  const char *mnt_abs = mnt;
  if (realpath(mnt, mabs) != NULL)
    mnt_abs = mabs;

  int dfd = open(mnt, O_RDONLY | O_DIRECTORY);
  if (dfd < 0)
  {
    perror("open");
    return 1;
  }

  char rel[KAFS_IOCTL_PATH_MAX];
  const char *p = to_mount_rel_path(mnt_abs, path, rel);
  if (!p)
  {
    fprintf(stderr, "invalid path\n");
    close(dfd);
    return 2;
  }

  int fd = openat(dfd, p, O_RDONLY);
  if (fd < 0)
  {
    perror("openat");
    close(dfd);
    return 1;
  }

  char buf[64 * 1024];
  while (1)
  {
    ssize_t r = read(fd, buf, sizeof(buf));
    if (r < 0)
    {
      perror("read");
      close(fd);
      close(dfd);
      return 1;
    }
    if (r == 0)
      break;
    ssize_t off = 0;
    while (off < r)
    {
      ssize_t w = write(STDOUT_FILENO, buf + off, (size_t)(r - off));
      if (w < 0)
      {
        perror("write");
        close(fd);
        close(dfd);
        return 1;
      }
      off += w;
    }
  }

  close(fd);
  close(dfd);
  return 0;
}

static int cmd_write(const char *mnt, const char *path)
{
  char mabs[KAFS_IOCTL_PATH_MAX];
  const char *mnt_abs = mnt;
  if (realpath(mnt, mabs) != NULL)
    mnt_abs = mabs;

  int dfd = open(mnt, O_RDONLY | O_DIRECTORY);
  if (dfd < 0)
  {
    perror("open");
    return 1;
  }

  char rel[KAFS_IOCTL_PATH_MAX];
  const char *p = to_mount_rel_path(mnt_abs, path, rel);
  if (!p)
  {
    fprintf(stderr, "invalid path\n");
    close(dfd);
    return 2;
  }

  int fd = openat(dfd, p, O_CREAT | O_TRUNC | O_WRONLY, 0644);
  if (fd < 0)
  {
    perror("openat");
    close(dfd);
    return 1;
  }

  char buf[64 * 1024];
  while (1)
  {
    ssize_t r = read(STDIN_FILENO, buf, sizeof(buf));
    if (r < 0)
    {
      perror("read(stdin)");
      close(fd);
      close(dfd);
      return 1;
    }
    if (r == 0)
      break;
    ssize_t off = 0;
    while (off < r)
    {
      ssize_t w = write(fd, buf + off, (size_t)(r - off));
      if (w < 0)
      {
        perror("write(file)");
        close(fd);
        close(dfd);
        return 1;
      }
      off += w;
    }
  }

  close(fd);
  close(dfd);
  return 0;
}

static int cmd_cp(const char *mnt, const char *src, const char *dst, int reflink)
{
  char mabs[KAFS_IOCTL_PATH_MAX];
  const char *mnt_abs = mnt;
  if (realpath(mnt, mabs) != NULL)
    mnt_abs = mabs;

  int fd = open(mnt, O_RDONLY | O_DIRECTORY);
  if (fd < 0)
  {
    perror("open");
    return 1;
  }

  kafs_ioctl_copy_t req;
  memset(&req, 0, sizeof(req));
  req.struct_size = (uint32_t)sizeof(req);
  req.flags = reflink ? KAFS_IOCTL_COPY_F_REFLINK : 0;

  char srcbuf[KAFS_IOCTL_PATH_MAX];
  char dstbuf[KAFS_IOCTL_PATH_MAX];
  const char *s = to_kafs_path(mnt_abs, src, srcbuf);
  const char *d = to_kafs_path(mnt_abs, dst, dstbuf);
  if (!s || !d)
  {
    fprintf(stderr, "invalid path\n");
    close(fd);
    return 2;
  }
  snprintf(req.src, sizeof(req.src), "%s", s);
  snprintf(req.dst, sizeof(req.dst), "%s", d);

  if (ioctl(fd, KAFS_IOCTL_COPY, &req) != 0)
  {
    perror("ioctl(KAFS_IOCTL_COPY)");
    close(fd);
    return 1;
  }
  close(fd);
  return 0;
}

static int cmd_rm(const char *mnt, const char *path)
{
  char mabs[KAFS_IOCTL_PATH_MAX];
  const char *mnt_abs = mnt;
  if (realpath(mnt, mabs) != NULL)
    mnt_abs = mabs;

  int dfd = open(mnt, O_RDONLY | O_DIRECTORY);
  if (dfd < 0)
  {
    perror("open");
    return 1;
  }

  char rel[KAFS_IOCTL_PATH_MAX];
  const char *p = to_mount_rel_path(mnt_abs, path, rel);
  if (!p)
  {
    fprintf(stderr, "invalid path\n");
    close(dfd);
    return 2;
  }

  if (unlinkat(dfd, p, 0) != 0)
  {
    perror("unlinkat");
    close(dfd);
    return 1;
  }
  close(dfd);
  return 0;
}

static int cmd_rmdir(const char *mnt, const char *path)
{
  char mabs[KAFS_IOCTL_PATH_MAX];
  const char *mnt_abs = mnt;
  if (realpath(mnt, mabs) != NULL)
    mnt_abs = mabs;

  int dfd = open(mnt, O_RDONLY | O_DIRECTORY);
  if (dfd < 0)
  {
    perror("open");
    return 1;
  }

  char rel[KAFS_IOCTL_PATH_MAX];
  const char *p = to_mount_rel_path(mnt_abs, path, rel);
  if (!p)
  {
    fprintf(stderr, "invalid path\n");
    close(dfd);
    return 2;
  }

  if (unlinkat(dfd, p, AT_REMOVEDIR) != 0)
  {
    perror("unlinkat(AT_REMOVEDIR)");
    close(dfd);
    return 1;
  }
  close(dfd);
  return 0;
}

static int cmd_mkdir(const char *mnt, const char *path)
{
  char mabs[KAFS_IOCTL_PATH_MAX];
  const char *mnt_abs = mnt;
  if (realpath(mnt, mabs) != NULL)
    mnt_abs = mabs;

  int dfd = open(mnt, O_RDONLY | O_DIRECTORY);
  if (dfd < 0)
  {
    perror("open");
    return 1;
  }

  char rel[KAFS_IOCTL_PATH_MAX];
  const char *p = to_mount_rel_path(mnt_abs, path, rel);
  if (!p)
  {
    fprintf(stderr, "invalid path\n");
    close(dfd);
    return 2;
  }

  if (mkdirat(dfd, p, 0755) != 0)
  {
    perror("mkdirat");
    close(dfd);
    return 1;
  }
  close(dfd);
  return 0;
}

static int cmd_mv(const char *mnt, const char *src, const char *dst)
{
  char mabs[KAFS_IOCTL_PATH_MAX];
  const char *mnt_abs = mnt;
  if (realpath(mnt, mabs) != NULL)
    mnt_abs = mabs;

  int dfd = open(mnt, O_RDONLY | O_DIRECTORY);
  if (dfd < 0)
  {
    perror("open");
    return 1;
  }

  char srel[KAFS_IOCTL_PATH_MAX];
  char drel[KAFS_IOCTL_PATH_MAX];
  const char *s = to_mount_rel_path(mnt_abs, src, srel);
  const char *d = to_mount_rel_path(mnt_abs, dst, drel);
  if (!s || !d)
  {
    fprintf(stderr, "invalid path\n");
    close(dfd);
    return 2;
  }

  if (renameat(dfd, s, dfd, d) != 0)
  {
    perror("renameat");
    close(dfd);
    return 1;
  }

  close(dfd);
  return 0;
}

static int cmd_ln(const char *mnt, const char *src, const char *dst)
{
  char mabs[KAFS_IOCTL_PATH_MAX];
  const char *mnt_abs = mnt;
  if (realpath(mnt, mabs) != NULL)
    mnt_abs = mabs;

  int dfd = open(mnt, O_RDONLY | O_DIRECTORY);
  if (dfd < 0)
  {
    perror("open");
    return 1;
  }

  char srel[KAFS_IOCTL_PATH_MAX];
  char drel[KAFS_IOCTL_PATH_MAX];
  const char *s = to_mount_rel_path(mnt_abs, src, srel);
  const char *d = to_mount_rel_path(mnt_abs, dst, drel);
  if (!s || !d)
  {
    fprintf(stderr, "invalid path\n");
    close(dfd);
    return 2;
  }

  if (linkat(dfd, s, dfd, d, 0) != 0)
  {
    perror("linkat");
    close(dfd);
    return 1;
  }

  close(dfd);
  return 0;
}

static int cmd_symlink(const char *mnt, const char *target, const char *linkpath)
{
  char mabs[KAFS_IOCTL_PATH_MAX];
  const char *mnt_abs = mnt;
  if (realpath(mnt, mabs) != NULL)
    mnt_abs = mabs;

  int dfd = open(mnt, O_RDONLY | O_DIRECTORY);
  if (dfd < 0)
  {
    perror("open");
    return 1;
  }

  char lrel[KAFS_IOCTL_PATH_MAX];
  const char *l = to_mount_rel_path(mnt_abs, linkpath, lrel);
  if (!l)
  {
    fprintf(stderr, "invalid path\n");
    close(dfd);
    return 2;
  }

  if (symlinkat(target, dfd, l) != 0)
  {
    perror("symlinkat");
    close(dfd);
    return 1;
  }

  close(dfd);
  return 0;
}

static int cmd_readlink(const char *mnt, const char *path)
{
  char mabs[KAFS_IOCTL_PATH_MAX];
  const char *mnt_abs = mnt;
  if (realpath(mnt, mabs) != NULL)
    mnt_abs = mabs;

  int dfd = open(mnt, O_RDONLY | O_DIRECTORY);
  if (dfd < 0)
  {
    perror("open");
    return 1;
  }

  char rel[KAFS_IOCTL_PATH_MAX];
  const char *p = to_mount_rel_path(mnt_abs, path, rel);
  if (!p)
  {
    fprintf(stderr, "invalid path\n");
    close(dfd);
    return 2;
  }

  char buf[KAFS_IOCTL_PATH_MAX];
  ssize_t n = readlinkat(dfd, p, buf, sizeof(buf) - 1);
  if (n < 0)
  {
    perror("readlinkat");
    close(dfd);
    return 1;
  }
  buf[n] = '\0';
  printf("%s\n", buf);

  close(dfd);
  return 0;
}

static int parse_octal_mode(const char *s, mode_t *out)
{
  if (!s || !*s || !out)
    return -EINVAL;
  char *end = NULL;
  errno = 0;
  long v = strtol(s, &end, 8);
  if (errno != 0 || !end || *end != '\0' || v < 0)
    return -EINVAL;
  *out = (mode_t)(v & 07777);
  return 0;
}

static int cmd_chmod(const char *mnt, const char *mode_str, const char *path)
{
  mode_t mode;
  if (parse_octal_mode(mode_str, &mode) != 0)
  {
    fprintf(stderr, "invalid mode\n");
    return 2;
  }

  char mabs[KAFS_IOCTL_PATH_MAX];
  const char *mnt_abs = mnt;
  if (realpath(mnt, mabs) != NULL)
    mnt_abs = mabs;

  int dfd = open(mnt, O_RDONLY | O_DIRECTORY);
  if (dfd < 0)
  {
    perror("open");
    return 1;
  }

  char rel[KAFS_IOCTL_PATH_MAX];
  const char *p = to_mount_rel_path(mnt_abs, path, rel);
  if (!p)
  {
    fprintf(stderr, "invalid path\n");
    close(dfd);
    return 2;
  }

  if (fchmodat(dfd, p, mode, 0) != 0)
  {
    perror("fchmodat");
    close(dfd);
    return 1;
  }

  close(dfd);
  return 0;
}

static int cmd_touch(const char *mnt, const char *path)
{
  char mabs[KAFS_IOCTL_PATH_MAX];
  const char *mnt_abs = mnt;
  if (realpath(mnt, mabs) != NULL)
    mnt_abs = mabs;

  int dfd = open(mnt, O_RDONLY | O_DIRECTORY);
  if (dfd < 0)
  {
    perror("open");
    return 1;
  }

  char rel[KAFS_IOCTL_PATH_MAX];
  const char *p = to_mount_rel_path(mnt_abs, path, rel);
  if (!p)
  {
    fprintf(stderr, "invalid path\n");
    close(dfd);
    return 2;
  }

  int fd = openat(dfd, p, O_CREAT | O_WRONLY, 0644);
  if (fd >= 0)
    close(fd);
  else if (errno != EISDIR)
  {
    perror("openat");
    close(dfd);
    return 1;
  }

  if (utimensat(dfd, p, NULL, 0) != 0)
  {
    perror("utimensat");
    close(dfd);
    return 1;
  }

  close(dfd);
  return 0;
}

int main(int argc, char **argv)
{
  if (argc < 3)
  {
    usage(argv[0]);
    return 2;
  }

  if (strcmp(argv[1], "fsstat") == 0 || strcmp(argv[1], "stats") == 0)
  {
    int json = 0;
    kafs_unit_t unit = KAFS_UNIT_KIB;
    for (int i = 3; i < argc; ++i)
    {
      if (strcmp(argv[i], "--json") == 0)
        json = 1;
      else if (strcmp(argv[i], "--bytes") == 0)
        unit = KAFS_UNIT_BYTES;
      else if (strcmp(argv[i], "--mib") == 0)
        unit = KAFS_UNIT_MIB;
      else if (strcmp(argv[i], "--gib") == 0)
        unit = KAFS_UNIT_GIB;
      else
      {
        usage(argv[0]);
        return 2;
      }
    }
    return cmd_stats(argv[2], json, unit);
  }

  if (strcmp(argv[1], "hotplug") == 0)
  {
    if (argc < 4)
    {
      usage(argv[0]);
      return 2;
    }
    if (strcmp(argv[2], "status") == 0)
    {
      int json = 0;
      for (int i = 4; i < argc; ++i)
      {
        if (strcmp(argv[i], "--json") == 0)
          json = 1;
        else
        {
          usage(argv[0]);
          return 2;
        }
      }
      return cmd_hotplug_status(argv[3], json);
    }
    usage(argv[0]);
    return 2;
  }

  if (strcmp(argv[1], "stat") == 0)
  {
    if (argc != 4)
    {
      usage(argv[0]);
      return 2;
    }
    return cmd_stat(argv[2], argv[3]);
  }

  if (strcmp(argv[1], "cat") == 0)
  {
    if (argc != 4)
    {
      usage(argv[0]);
      return 2;
    }
    return cmd_cat(argv[2], argv[3]);
  }

  if (strcmp(argv[1], "write") == 0)
  {
    if (argc != 4)
    {
      usage(argv[0]);
      return 2;
    }
    return cmd_write(argv[2], argv[3]);
  }

  if (strcmp(argv[1], "cp") == 0)
  {
    if (argc < 5)
    {
      usage(argv[0]);
      return 2;
    }
    int reflink = 0;
    for (int i = 5; i < argc; ++i)
    {
      if (strcmp(argv[i], "--reflink") == 0)
        reflink = 1;
      else
      {
        usage(argv[0]);
        return 2;
      }
    }
    return cmd_cp(argv[2], argv[3], argv[4], reflink);
  }

  if (strcmp(argv[1], "mv") == 0)
  {
    if (argc != 5)
    {
      usage(argv[0]);
      return 2;
    }
    return cmd_mv(argv[2], argv[3], argv[4]);
  }

  if (strcmp(argv[1], "rm") == 0)
  {
    if (argc != 4)
    {
      usage(argv[0]);
      return 2;
    }
    return cmd_rm(argv[2], argv[3]);
  }

  if (strcmp(argv[1], "mkdir") == 0)
  {
    if (argc != 4)
    {
      usage(argv[0]);
      return 2;
    }
    return cmd_mkdir(argv[2], argv[3]);
  }

  if (strcmp(argv[1], "rmdir") == 0)
  {
    if (argc != 4)
    {
      usage(argv[0]);
      return 2;
    }
    return cmd_rmdir(argv[2], argv[3]);
  }

  if (strcmp(argv[1], "ln") == 0)
  {
    if (argc != 5)
    {
      usage(argv[0]);
      return 2;
    }
    return cmd_ln(argv[2], argv[3], argv[4]);
  }

  if (strcmp(argv[1], "symlink") == 0)
  {
    if (argc != 5)
    {
      usage(argv[0]);
      return 2;
    }
    return cmd_symlink(argv[2], argv[3], argv[4]);
  }

  if (strcmp(argv[1], "readlink") == 0)
  {
    if (argc != 4)
    {
      usage(argv[0]);
      return 2;
    }
    return cmd_readlink(argv[2], argv[3]);
  }

  if (strcmp(argv[1], "chmod") == 0)
  {
    if (argc != 5)
    {
      usage(argv[0]);
      return 2;
    }
    return cmd_chmod(argv[2], argv[3], argv[4]);
  }

  if (strcmp(argv[1], "touch") == 0)
  {
    if (argc != 4)
    {
      usage(argv[0]);
      return 2;
    }
    return cmd_touch(argv[2], argv[3]);
  }

  usage(argv[0]);
  return 2;
}
