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
};

typedef struct kafs_context kafs_context_t;
