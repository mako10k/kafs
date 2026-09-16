#pragma once

#include <errno.h>
#include <stddef.h>
#include <stdint.h>
#include <sys/types.h>

static inline int kafs_u64_to_size(uint64_t value, size_t *out)
{
  if (!out || value > (uint64_t)SIZE_MAX)
    return -EOVERFLOW;
  *out = (size_t)value;
  return 0;
}

static inline int kafs_off_to_size(off_t value, size_t *out)
{
  if (!out || value < 0 || (uint64_t)value > (uint64_t)SIZE_MAX)
    return -EOVERFLOW;
  *out = (size_t)value;
  return 0;
}

static inline int kafs_u64_add(uint64_t a, uint64_t b, uint64_t *out)
{
  if (!out || b > UINT64_MAX - a)
    return -EOVERFLOW;
  *out = a + b;
  return 0;
}

static inline int kafs_u64_align_up(uint64_t value, uint64_t alignment, uint64_t *out)
{
  uint64_t mask;

  if (!out || alignment == 0u || (alignment & (alignment - 1u)) != 0u)
    return -EINVAL;
  mask = alignment - 1u;
  if (value > UINT64_MAX - mask)
    return -EOVERFLOW;
  *out = (value + mask) & ~mask;
  return 0;
}

static inline void *kafs_mapped_region(void *base, size_t mapped_size, uint64_t offset,
                                       uint64_t length)
{
  size_t local_offset;
  size_t local_length;

  if (!base || kafs_u64_to_size(offset, &local_offset) != 0 ||
      kafs_u64_to_size(length, &local_length) != 0 || local_offset > mapped_size ||
      local_length > mapped_size - local_offset)
    return NULL;
  return (char *)base + local_offset;
}
