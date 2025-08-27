#pragma once
#include "kafs_context.h"

#ifdef __has_include
#if __has_include(<pthread.h>)
#define KAFS_HAS_PTHREAD 1
#else
#define KAFS_HAS_PTHREAD 0
#endif
#else
#define KAFS_HAS_PTHREAD 1
#endif

int kafs_ctx_locks_init(struct kafs_context *ctx);
void kafs_ctx_locks_destroy(struct kafs_context *ctx);
void kafs_hrl_bucket_lock(struct kafs_context *ctx, uint32_t bucket);
void kafs_hrl_bucket_unlock(struct kafs_context *ctx, uint32_t bucket);
void kafs_hrl_global_lock(struct kafs_context *ctx);
void kafs_hrl_global_unlock(struct kafs_context *ctx);

// Bitmap/allocator lock (must be acquired after HRL bucket lock when both are needed)
void kafs_bitmap_lock(struct kafs_context *ctx);
void kafs_bitmap_unlock(struct kafs_context *ctx);

// Inode locking: per-inode mutex array and an allocation mutex
void kafs_inode_lock(struct kafs_context *ctx, uint32_t ino);
void kafs_inode_unlock(struct kafs_context *ctx, uint32_t ino);
void kafs_inode_alloc_lock(struct kafs_context *ctx);
void kafs_inode_alloc_unlock(struct kafs_context *ctx);
