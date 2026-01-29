#pragma once
#include "kafs.h"
#include <assert.h>
#include <stdio.h>

/// @brief スーパーブロック情報（固定長 128 bytes）
struct kafs_ssuperblock
{
  // --- Header ---
  /// @brief フォーマット識別子 'KAFS'
  kafs_su32_t s_magic; // +0  (4)
  /// @brief フォーマットバージョン（2=HRL採用）
  kafs_su32_t s_format_version; // +4  (4)
  /// @brief ブロックサイズ(サイズ=2^(10 + s_log_blksize))
  kafs_slogblksize_t s_log_blksize; // +8  (2)
  uint16_t s_pad0;                  // +10 (2) 8B境界調整

  /// @brief マウント日時
  kafs_stime_t s_mtime; // +12 (8)
  /// @brief 書き込み日時
  kafs_stime_t s_wtime; // +20 (8)
  /// @brief マウント数
  kafs_smntcnt_t s_mntcnt; // +28 (2)
  uint16_t s_pad1;         // +30 (2)

  // --- Geometry / counts ---
  /// @brief inode 番号の数
  kafs_sinocnt_t s_inocnt; // +32 (4)
  /// @brief block 番号の数(一般ユーザー)
  kafs_sblkcnt_t s_blkcnt; // +36 (4)
  /// @brief block 番号の数(ルートユーザー)
  kafs_sblkcnt_t s_r_blkcnt; // +40 (4)
  /// @brief 空き block 番号の数
  kafs_sblkcnt_t s_blkcnt_free; // +44 (4)
  /// @brief 空き inode 番号の数
  kafs_sinocnt_t s_inocnt_free; // +48 (4)
  /// @brief 最初のデータのブロック番号
  kafs_sblkcnt_t s_first_data_block; // +52 (4)

  // --- HRL config ---
  /// @brief 高速ハッシュ識別子（例: 1=xxh64）
  kafs_su32_t s_hash_algo_fast; // +56 (4)
  /// @brief 強ハッシュ識別子（例: 1=BLAKE3-256）
  kafs_su32_t s_hash_algo_strong; // +60 (4)
  /// @brief HRL インデックス領域の先頭オフセット（バイト）
  kafs_su64_t s_hrl_index_offset; // +64 (8)
  /// @brief HRL インデックス領域のサイズ（バイト）
  kafs_su64_t s_hrl_index_size; // +72 (8)
  /// @brief HR エントリ表の先頭オフセット（バイト）
  kafs_su64_t s_hrl_entry_offset; // +80 (8)
  /// @brief HR エントリ数
  kafs_su32_t s_hrl_entry_cnt; // +88 (4)
  uint32_t s_pad2;             // +92 (4)

  // --- Journal (in-image) ---
  /// @brief ジャーナル領域の先頭オフセット（バイト、メタ領域内）
  kafs_su64_t s_journal_offset; // +96 (8)
  /// @brief ジャーナル領域の総サイズ（ヘッダ含む、バイト）
  kafs_su64_t s_journal_size; // +104 (8)
  /// @brief ジャーナル設定フラグ（将来用）
  kafs_su32_t s_journal_flags;   // +112 (4)
  uint32_t s_pad3;               // +116 (4)
  uint8_t s_reserved[128 - 120]; // +120 .. +127
} __attribute__((packed));

typedef struct kafs_ssuperblock kafs_ssuperblock_t;

// 固定長チェック
_Static_assert(sizeof(struct kafs_ssuperblock) == 128, "kafs_ssuperblock must be 128 bytes");

#include "kafs_context.h"

static kafs_blkcnt_t kafs_sb_blkcnt_get(const kafs_ssuperblock_t *sb)
{
  assert(sb != NULL);
  return kafs_blkcnt_stoh(sb->s_blkcnt);
}

__attribute_maybe_unused__ static int kafs_sb_inotbl_is_full(const kafs_ssuperblock_t *sb)
{
  assert(sb != NULL);
  // 下記は 0 のビット表現がエンディアンによって変わらない前提
  return sb->s_inocnt_free.value == 0;
}

/// @brief inodeの最大数を読み出す
/// @param sb スーパーブロック
/// @return inodeの最大数
static kafs_inocnt_t kafs_sb_inocnt_get(const kafs_ssuperblock_t *sb)
{
  assert(sb != NULL);
  return kafs_inocnt_stoh(sb->s_inocnt);
}

__attribute_maybe_unused__ static int kafs_sb_inocnt_free_get(const kafs_ssuperblock_t *sb)
{
  assert(sb != NULL);
  return kafs_inocnt_stoh(sb->s_inocnt_free);
}

static kafs_blkcnt_t kafs_sb_r_blkcnt_get(const struct kafs_ssuperblock *sb)
{
  assert(sb != NULL);
  return kafs_blkcnt_stoh(sb->s_r_blkcnt);
}

static inline kafs_blkcnt_t kafs_sb_first_data_block_get(const struct kafs_ssuperblock *sb)
{
  assert(sb != NULL);
  return kafs_blkcnt_stoh(sb->s_first_data_block);
}

static kafs_blkcnt_t kafs_sb_blkcnt_free_get(const struct kafs_ssuperblock *sb)
{
  assert(sb != NULL);
  return kafs_blkcnt_stoh(sb->s_blkcnt_free);
}

static void kafs_sb_blkcnt_free_set(struct kafs_ssuperblock *sb, kafs_blkcnt_t blkcnt_free)
{
  assert(sb != NULL);
  sb->s_blkcnt_free = kafs_blkcnt_htos(blkcnt_free);
}

static kafs_blkcnt_t kafs_sb_blkcnt_free_incr(struct kafs_ssuperblock *sb)
{
  assert(sb != NULL);
  kafs_blkcnt_t blkcnt_free = kafs_sb_blkcnt_free_get(sb);
  kafs_sb_blkcnt_free_set(sb, blkcnt_free + 1);
  return blkcnt_free + 1;
}

static void kafs_sb_wtime_set(struct kafs_ssuperblock *sb, kafs_time_t wtime)
{
  assert(sb != NULL);
  sb->s_wtime = kafs_time_htos(wtime);
}

static kafs_logblksize_t kafs_sb_log_blksize_get(const struct kafs_ssuperblock *sb)
{
  assert(sb != NULL);
  return kafs_logblksize_stoh(sb->s_log_blksize) + 10;
}

__attribute_maybe_unused__ static void kafs_sb_log_blksize_set(struct kafs_ssuperblock *sb,
                                                               kafs_logblksize_t log2_blksize)
{
  assert(sb != NULL);
  // 引数は実ブロックサイズの log2 値（例: 4096=2^12 → 12）
  sb->s_log_blksize = (kafs_slogblksize_t){.value = htole16((uint16_t)(log2_blksize - 10))};
}

// --- HRL 追加フィールドの get/set ---
static inline uint32_t kafs_sb_magic_get(const struct kafs_ssuperblock *sb)
{
  return kafs_u32_stoh(sb->s_magic);
}
static inline void kafs_sb_magic_set(struct kafs_ssuperblock *sb, uint32_t v)
{
  sb->s_magic = kafs_u32_htos(v);
}
static inline uint32_t kafs_sb_format_version_get(const struct kafs_ssuperblock *sb)
{
  return kafs_u32_stoh(sb->s_format_version);
}
static inline void kafs_sb_format_version_set(struct kafs_ssuperblock *sb, uint32_t v)
{
  sb->s_format_version = kafs_u32_htos(v);
}
static inline uint32_t kafs_sb_hash_fast_get(const struct kafs_ssuperblock *sb)
{
  return kafs_u32_stoh(sb->s_hash_algo_fast);
}
static inline void kafs_sb_hash_fast_set(struct kafs_ssuperblock *sb, uint32_t v)
{
  sb->s_hash_algo_fast = kafs_u32_htos(v);
}
static inline uint32_t kafs_sb_hash_strong_get(const struct kafs_ssuperblock *sb)
{
  return kafs_u32_stoh(sb->s_hash_algo_strong);
}
static inline void kafs_sb_hash_strong_set(struct kafs_ssuperblock *sb, uint32_t v)
{
  sb->s_hash_algo_strong = kafs_u32_htos(v);
}

static inline uint64_t kafs_sb_hrl_index_offset_get(const struct kafs_ssuperblock *sb)
{
  return kafs_u64_stoh(sb->s_hrl_index_offset);
}
static inline void kafs_sb_hrl_index_offset_set(struct kafs_ssuperblock *sb, uint64_t v)
{
  sb->s_hrl_index_offset = kafs_u64_htos(v);
}
static inline uint64_t kafs_sb_hrl_index_size_get(const struct kafs_ssuperblock *sb)
{
  return kafs_u64_stoh(sb->s_hrl_index_size);
}
static inline void kafs_sb_hrl_index_size_set(struct kafs_ssuperblock *sb, uint64_t v)
{
  sb->s_hrl_index_size = kafs_u64_htos(v);
}
static inline uint64_t kafs_sb_hrl_entry_offset_get(const struct kafs_ssuperblock *sb)
{
  return kafs_u64_stoh(sb->s_hrl_entry_offset);
}
static inline void kafs_sb_hrl_entry_offset_set(struct kafs_ssuperblock *sb, uint64_t v)
{
  sb->s_hrl_entry_offset = kafs_u64_htos(v);
}
static inline uint32_t kafs_sb_hrl_entry_cnt_get(const struct kafs_ssuperblock *sb)
{
  return kafs_u32_stoh(sb->s_hrl_entry_cnt);
}
static inline void kafs_sb_hrl_entry_cnt_set(struct kafs_ssuperblock *sb, uint32_t v)
{
  sb->s_hrl_entry_cnt = kafs_u32_htos(v);
}

// --- Journal get/set ---
static inline uint64_t kafs_sb_journal_offset_get(const struct kafs_ssuperblock *sb)
{
  return kafs_u64_stoh(sb->s_journal_offset);
}
static inline void kafs_sb_journal_offset_set(struct kafs_ssuperblock *sb, uint64_t v)
{
  sb->s_journal_offset = kafs_u64_htos(v);
}
static inline uint64_t kafs_sb_journal_size_get(const struct kafs_ssuperblock *sb)
{
  return kafs_u64_stoh(sb->s_journal_size);
}
static inline void kafs_sb_journal_size_set(struct kafs_ssuperblock *sb, uint64_t v)
{
  sb->s_journal_size = kafs_u64_htos(v);
}
static inline uint32_t kafs_sb_journal_flags_get(const struct kafs_ssuperblock *sb)
{
  return kafs_u32_stoh(sb->s_journal_flags);
}
static inline void kafs_sb_journal_flags_set(struct kafs_ssuperblock *sb, uint32_t v)
{
  sb->s_journal_flags = kafs_u32_htos(v);
}

static kafs_blksize_t kafs_sb_blksize_get(const struct kafs_ssuperblock *sb)
{
  assert(sb != NULL);
  return 1 << kafs_sb_log_blksize_get(sb);
}

static kafs_logblksize_t kafs_sb_log_blkref_pb_get(const struct kafs_ssuperblock *sb)
{
  // 1ブロックに格納できる参照数: ブロックサイズ[byte] / 参照サイズ(4byte)
  // したがって log2 = log_blksize - 2
  return kafs_sb_log_blksize_get(sb) - 2;
}

static kafs_blksize_t kafs_sb_blkref_pb_get(const struct kafs_ssuperblock *sb)
{
  return 1 << kafs_sb_log_blkref_pb_get(sb);
}
