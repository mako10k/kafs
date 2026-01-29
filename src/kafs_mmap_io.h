#pragma once
#include "kafs.h"
#include "kafs_context.h"
#include "kafs_superblock.h"
#include <assert.h>
#include <string.h>

static inline void *kafs_img_ptr(struct kafs_context *ctx, off_t off, size_t len)
{
  assert(ctx != NULL);
  assert(ctx->c_img_base != NULL);
  assert((size_t)off + len <= ctx->c_img_size);
  return (void *)((char *)ctx->c_img_base + off);
}

static inline int kafs_img_read(struct kafs_context *ctx, void *dst, size_t len, off_t off)
{
  memcpy(dst, kafs_img_ptr(ctx, off, len), len);
  return 0;
}

static inline int kafs_img_write(struct kafs_context *ctx, const void *src, size_t len, off_t off)
{
  memcpy(kafs_img_ptr(ctx, off, len), src, len);
  return 0;
}
