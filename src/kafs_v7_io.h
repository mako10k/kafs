#pragma once

#include <errno.h>
#include <fcntl.h>

/* Positional v7 durability writes require a writable, non-append descriptor. */
static inline int kafs_v7_io_require_positional_writes(int fd)
{
  if (fd < 0)
    return -EINVAL;
  int flags = fcntl(fd, F_GETFL);
  if (flags < 0)
    return -errno;
  return (flags & O_ACCMODE) == O_RDONLY || (flags & O_APPEND) != 0 ? -EBADF : 0;
}
