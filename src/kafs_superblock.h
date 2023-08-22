#pragma once
#include "kafs.h"
#include <assert.h>
#include <stdio.h>

/// @brief スーパーブロック情報
struct kafs_ssuperblock
{
  /// @brief inode 番号の数
  kafs_sinocnt_t s_inocnt;
  /// @brief block 番号の数(一般ユーザー)
  kafs_sblkcnt_t s_blkcnt;
  /// @brief block 番号の数(ルートユーザー)
  kafs_sblkcnt_t s_r_blkcnt;
  /// @brief 空き block 番号の数
  kafs_sblkcnt_t s_blkcnt_free;
  /// @brief 空き inode 番号の数
  kafs_sinocnt_t s_inocnt_free;
  /// @brief 最初のデータのブロック番号
  kafs_sblkcnt_t s_first_data_block;
  /// @brief ブロックサイズ(ただし、サイズ=2^(10 + s_log_block_size))
  kafs_slogblksize_t s_log_blksize;
  /// @brief マウント日時
  kafs_stime_t s_mtime;
  /// @brief 書き込み日時
  kafs_stime_t s_wtime;
  /// @brief マウント数
  kafs_smntcnt_t s_mntcnt;
} __attribute__((packed));

typedef struct kafs_ssuperblock kafs_ssuperblock_t;

#include "kafs_context.h"

static kafs_blkcnt_t
kafs_sb_blkcnt_get (const kafs_ssuperblock_t * sb)
{
  assert (sb != NULL);
  return kafs_blkcnt_stoh (sb->s_blkcnt);
}

__attribute_maybe_unused__ static int
kafs_sb_inotbl_is_full (const kafs_ssuperblock_t * sb)
{
  assert (sb != NULL);
  // 下記は 0 のビット表現がエンディアンによって変わらない前提
  return sb->s_inocnt_free.value == 0;
}

/// @brief inodeの最大数を読み出す
/// @param sb スーパーブロック
/// @return inodeの最大数
static kafs_inocnt_t
kafs_sb_inocnt_get (const kafs_ssuperblock_t * sb)
{
  assert (sb != NULL);
  return kafs_inocnt_stoh (sb->s_inocnt);
}

__attribute_maybe_unused__ static int
kafs_sb_inocnt_free_get (const kafs_ssuperblock_t * sb)
{
  assert (sb != NULL);
  return kafs_inocnt_stoh (sb->s_inocnt_free);
}

static kafs_blkcnt_t
kafs_sb_r_blkcnt_get (const struct kafs_ssuperblock *sb)
{
  assert (sb != NULL);
  return kafs_blkcnt_stoh (sb->s_blkcnt);
}

static kafs_blkcnt_t
kafs_sb_blkcnt_free_get (const struct kafs_ssuperblock *sb)
{
  assert (sb != NULL);
  return kafs_blkcnt_stoh (sb->s_blkcnt_free);
}

static void
kafs_sb_blkcnt_free_set (struct kafs_ssuperblock *sb, kafs_blkcnt_t blkcnt_free)
{
  assert (sb != NULL);
  sb->s_blkcnt_free = kafs_blkcnt_htos (blkcnt_free);
}

static kafs_blkcnt_t
kafs_sb_blkcnt_free_incr (struct kafs_ssuperblock *sb)
{
  assert (sb != NULL);
  kafs_blkcnt_t blkcnt_free = kafs_sb_blkcnt_free_get (sb);
  kafs_sb_blkcnt_free_set (sb, blkcnt_free + 1);
  return blkcnt_free + 1;
}

static void
kafs_sb_wtime_set (struct kafs_ssuperblock *sb, kafs_time_t wtime)
{
  assert (sb != NULL);
  sb->s_wtime = kafs_time_htos (wtime);
}

static kafs_logblksize_t
kafs_sb_log_blksize_get (const struct kafs_ssuperblock *sb)
{
  assert (sb != NULL);
  return kafs_logblksize_stoh (sb->s_log_blksize) + 10;
}

static kafs_blksize_t
kafs_sb_blksize_get (const struct kafs_ssuperblock *sb)
{
  assert (sb != NULL);
  return 1 << kafs_sb_log_blksize_get (sb);
}

static kafs_logblksize_t
kafs_sb_log_blkref_pb_get (const struct kafs_ssuperblock *sb)
{
  return kafs_sb_log_blksize_get (sb) + 3;
}

static kafs_blksize_t
kafs_sb_blkref_pb_get (const struct kafs_ssuperblock *sb)
{
  return 1 << kafs_sb_log_blkref_pb_get (sb);
}
