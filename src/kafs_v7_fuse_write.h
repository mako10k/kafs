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

typedef struct kafs_v7_fuse_truncate_result
{
  uint64_t retained_logical_block;
  int retirement_rc;
} kafs_v7_fuse_truncate_result_t;

int kafs_v7_fuse_write_direct(kafs_context_t *ctx, kafs_inocnt_t ino, const void *buf, size_t size,
                              uint64_t offset, kafs_v7_fuse_write_result_t *result);
int kafs_v7_fuse_truncate_direct(kafs_context_t *ctx, kafs_inocnt_t ino, uint64_t size,
                                 kafs_v7_fuse_truncate_result_t *result);
int kafs_v7_fuse_create_in_direct_directory(kafs_context_t *ctx, kafs_inocnt_t parent_ino,
                                            const char *name, uint16_t mode, uint16_t uid,
                                            uint16_t gid, kafs_inocnt_t *ino_out,
                                            int *retirement_rc_out);
