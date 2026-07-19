#pragma once

#include "kafs_context.h"

#include <stddef.h>
#include <stdint.h>

typedef struct kafs_v7_fuse_write_result
{
  uint64_t new_logical_block;
  uint64_t retained_logical_block;
  int retirement_rc;
} kafs_v7_fuse_write_result_t;

int kafs_v7_fuse_write_direct(kafs_context_t *ctx, kafs_inocnt_t ino, const void *buf, size_t size,
                              uint64_t offset, kafs_v7_fuse_write_result_t *result);
