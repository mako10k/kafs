#pragma once
#include "kafs.h"
#include "kafs_superblock.h"
#include "kafs_inode.h"

/// @brief コンテキスト
struct kafs_context
{
  /// @brief スーパーブロック情報
  kafs_ssuperblock_t *c_superblock;
  /// @brief inode 情報
  kafs_sinode_t *c_inotbl;
  /// @brief ブロックマスク
  kafs_blkmask_t *c_blkmasktbl;
  /// @brief 前回の inode 検索情報
  kafs_inocnt_t c_ino_search;
  /// @brief 前回のブロック検索情報
  kafs_blkcnt_t c_blo_search;
  /// @brief ファイル記述子
  int c_fd;
  /// @brief HRL バケットテーブル先頭（メタデータ mmap 内）
  void *c_hrl_index;
  /// @brief HRL バケット数（テーブルサイズ / バケットサイズ）
  uint32_t c_hrl_bucket_cnt;
  // --- Concurrency (optional locks) ---
  void *c_lock_hrl_buckets; // opaque pointer to mutex array
  void *c_lock_hrl_global;  // opaque pointer to global HRL mutex
  void *c_lock_bitmap;      // opaque pointer to bitmap/allocator mutex
};

// Lock helpers (no-op when locks not enabled in build)
int kafs_ctx_locks_init(struct kafs_context *ctx);
void kafs_ctx_locks_destroy(struct kafs_context *ctx);

typedef struct kafs_context kafs_context_t;
