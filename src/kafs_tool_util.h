#pragma once

#include "kafs_config.h"
#include <ctype.h>
#include <errno.h>
#include <stdint.h>
#include <stdlib.h>
#include <sys/types.h>
#include <unistd.h>
#ifdef __linux__
#include <linux/falloc.h>
#include <sys/syscall.h>
#endif

static inline int kafs_parse_size_bytes_u64(const char *arg, uint64_t *out)
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

  switch ((int)tolower((unsigned char)endp[0]))
  {
  case 'k':
    v <<= 10;
    break;
  case 'm':
    v <<= 20;
    break;
  case 'g':
    v <<= 30;
    break;
  default:
    return -1;
  }

  *out = (uint64_t)v;
  return 0;
}

static inline int kafs_parse_ratio_0_to_1(const char *arg, double *out)
{
  if (!arg || !out || *arg == '\0')
    return -1;

  char *endp = NULL;
  errno = 0;
  double v = strtod(arg, &endp);
  if (errno != 0 || endp == arg || *endp != '\0' || v <= 0.0 || v > 1.0)
    return -1;

  *out = v;
  return 0;
}

static inline int kafs_pread_all(int fd, void *buf, size_t sz, off_t off)
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

static inline int kafs_pwrite_all(int fd, const void *buf, size_t sz, off_t off)
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

static inline int kafs_punch_hole_keep_size(int fd, off_t off, off_t len)
{
  if (len <= 0)
    return 0;
#ifdef __linux__
#ifdef SYS_fallocate
  if (syscall(SYS_fallocate, fd, FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE, off, len) == 0)
    return 0;
  return -errno;
#else
  (void)fd;
  (void)off;
  (void)len;
  return -ENOSYS;
#endif
#else
  (void)fd;
  (void)off;
  (void)len;
  return -ENOTSUP;
#endif
}
