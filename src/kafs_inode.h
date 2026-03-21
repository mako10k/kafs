#pragma once
#include "kafs_config.h"
#include "kafs.h"
#include <assert.h>
#include <errno.h>
#include <string.h>

/// 存在しないことを表す inode 番号
#define KAFS_INO_NONE 0
/// ルートディレクトリの inode 番号
#define KAFS_INO_ROOTDIR 1

#define KAFS_INODE_BLOCKREF_SLOTS 15u
#define KAFS_INODE_DIRECT_BYTES 60u
#define KAFS_INODE_TAILDESC_V5_BYTES 14u
#define KAFS_INODE_V4_BYTES 114u
#define KAFS_INODE_V5_BYTES (KAFS_INODE_V4_BYTES + KAFS_INODE_TAILDESC_V5_BYTES)

#define KAFS_TAIL_LAYOUT_INLINE 0u
#define KAFS_TAIL_LAYOUT_FULL_BLOCK 1u
#define KAFS_TAIL_LAYOUT_TAIL_ONLY 2u
#define KAFS_TAIL_LAYOUT_MIXED_FULL_TAIL 3u

#define KAFS_TAILDESC_FLAG_PACKED_SMALL_FILE (1u << 0)
#define KAFS_TAILDESC_FLAG_FINAL_TAIL (1u << 1)
#define KAFS_TAILDESC_FLAG_NEEDS_FSCK_REVIEW (1u << 2)
#define KAFS_TAILDESC_KNOWN_FLAGS                                                                  \
  (KAFS_TAILDESC_FLAG_PACKED_SMALL_FILE | KAFS_TAILDESC_FLAG_FINAL_TAIL |                          \
   KAFS_TAILDESC_FLAG_NEEDS_FSCK_REVIEW)

/// @brief 将来の v5 inode 内 tail descriptor
struct kafs_sinode_taildesc_v5
{
  uint8_t it_layout_kind;
  uint8_t it_flags;
  uint16_t it_fragment_len;
  kafs_sblkcnt_t it_container_blo;
  uint16_t it_fragment_off;
  kafs_su32_t it_generation;
} __attribute__((packed));

typedef struct kafs_sinode_taildesc_v5 kafs_sinode_taildesc_v5_t;

/// @brief inode 情報
struct kafs_sinode
{
  /// @brief モード
  kafs_smode_t i_mode;
  /// @brief UID
  kafs_suid_t i_uid;
  /// @brief ファイルサイズ
  kafs_soff_t i_size;
  /// @brief アクセスタイム
  kafs_stime_t i_atime;
  /// @brief inode の更新時間
  kafs_stime_t i_ctime;
  /// @brief データ の更新時間
  kafs_stime_t i_mtime;
  /// @brief 削除時間
  kafs_stime_t i_dtime;
  /// @brief GID
  kafs_sgid_t i_gid;
  /// @brief リンク数
  kafs_slinkcnt_t i_linkcnt;
  /// @brief ブロック数
  kafs_sblkcnt_t i_blocks;
  /// @brief デバイス番号
  kafs_sdev_t i_rdev;
  /// @brief ブロックデータ
  kafs_sblkcnt_t i_blkreftbl[KAFS_INODE_BLOCKREF_SLOTS];
} __attribute__((packed));

typedef struct kafs_sinode kafs_sinode_t;

/// @brief 将来の v5 inode 情報
struct kafs_sinode_v5
{
  kafs_smode_t i_mode;
  kafs_suid_t i_uid;
  kafs_soff_t i_size;
  kafs_stime_t i_atime;
  kafs_stime_t i_ctime;
  kafs_stime_t i_mtime;
  kafs_stime_t i_dtime;
  kafs_sgid_t i_gid;
  kafs_slinkcnt_t i_linkcnt;
  kafs_sblkcnt_t i_blocks;
  kafs_sdev_t i_rdev;
  kafs_sblkcnt_t i_blkreftbl[KAFS_INODE_BLOCKREF_SLOTS];
  kafs_sinode_taildesc_v5_t i_taildesc;
} __attribute__((packed));

typedef struct kafs_sinode_v5 kafs_sinode_v5_t;

_Static_assert(sizeof(kafs_sinode_taildesc_v5_t) == KAFS_INODE_TAILDESC_V5_BYTES,
               "kafs_sinode_taildesc_v5_t must be 14 bytes");
_Static_assert(sizeof(((struct kafs_sinode *)NULL)->i_blkreftbl) == KAFS_INODE_DIRECT_BYTES,
               "kafs_sinode direct payload must remain 60 bytes");
_Static_assert(sizeof(kafs_sinode_t) == KAFS_INODE_V4_BYTES,
               "kafs_sinode must remain 114 bytes in v4");
_Static_assert(sizeof(((struct kafs_sinode_v5 *)NULL)->i_blkreftbl) == KAFS_INODE_DIRECT_BYTES,
               "kafs_sinode_v5 direct payload must preserve 60 bytes");
_Static_assert(sizeof(kafs_sinode_v5_t) == KAFS_INODE_V5_BYTES, "kafs_sinode_v5 must be 128 bytes");

static inline uint8_t
kafs_ino_taildesc_v5_layout_kind_get(const kafs_sinode_taildesc_v5_t *taildesc)
{
  return taildesc->it_layout_kind;
}

static inline void kafs_ino_taildesc_v5_layout_kind_set(kafs_sinode_taildesc_v5_t *taildesc,
                                                        uint8_t v)
{
  taildesc->it_layout_kind = v;
}

static inline uint8_t kafs_ino_taildesc_v5_flags_get(const kafs_sinode_taildesc_v5_t *taildesc)
{
  return taildesc->it_flags;
}

static inline void kafs_ino_taildesc_v5_flags_set(kafs_sinode_taildesc_v5_t *taildesc, uint8_t v)
{
  taildesc->it_flags = v;
}

static inline uint16_t
kafs_ino_taildesc_v5_fragment_len_get(const kafs_sinode_taildesc_v5_t *taildesc)
{
  return le16toh(taildesc->it_fragment_len);
}

static inline void kafs_ino_taildesc_v5_fragment_len_set(kafs_sinode_taildesc_v5_t *taildesc,
                                                         uint16_t v)
{
  taildesc->it_fragment_len = htole16(v);
}

static inline kafs_blkcnt_t
kafs_ino_taildesc_v5_container_blo_get(const kafs_sinode_taildesc_v5_t *taildesc)
{
  return kafs_blkcnt_stoh(taildesc->it_container_blo);
}

static inline void kafs_ino_taildesc_v5_container_blo_set(kafs_sinode_taildesc_v5_t *taildesc,
                                                          kafs_blkcnt_t v)
{
  taildesc->it_container_blo = kafs_blkcnt_htos(v);
}

static inline uint16_t
kafs_ino_taildesc_v5_fragment_off_get(const kafs_sinode_taildesc_v5_t *taildesc)
{
  return le16toh(taildesc->it_fragment_off);
}

static inline void kafs_ino_taildesc_v5_fragment_off_set(kafs_sinode_taildesc_v5_t *taildesc,
                                                         uint16_t v)
{
  taildesc->it_fragment_off = htole16(v);
}

static inline uint32_t
kafs_ino_taildesc_v5_generation_get(const kafs_sinode_taildesc_v5_t *taildesc)
{
  return kafs_u32_stoh(taildesc->it_generation);
}

static inline void kafs_ino_taildesc_v5_generation_set(kafs_sinode_taildesc_v5_t *taildesc,
                                                       uint32_t v)
{
  taildesc->it_generation = kafs_u32_htos(v);
}

static inline void kafs_ino_taildesc_v5_init(kafs_sinode_taildesc_v5_t *taildesc)
{
  memset(taildesc, 0, sizeof(*taildesc));
  kafs_ino_taildesc_v5_layout_kind_set(taildesc, KAFS_TAIL_LAYOUT_INLINE);
}

static inline int kafs_tail_layout_is_known(uint8_t kind)
{
  return kind <= KAFS_TAIL_LAYOUT_MIXED_FULL_TAIL;
}

static inline int kafs_tail_layout_uses_tail_storage(uint8_t kind)
{
  return kind == KAFS_TAIL_LAYOUT_TAIL_ONLY || kind == KAFS_TAIL_LAYOUT_MIXED_FULL_TAIL;
}

static inline int kafs_ino_taildesc_v5_uses_tail_storage(const kafs_sinode_taildesc_v5_t *taildesc)
{
  uint8_t kind = kafs_ino_taildesc_v5_layout_kind_get(taildesc);
  return kafs_tail_layout_uses_tail_storage(kind);
}

static inline size_t kafs_inode_inline_bytes(void) { return KAFS_INODE_DIRECT_BYTES; }

static inline int kafs_inode_size_is_inline(kafs_off_t inode_size)
{
  return inode_size <= (kafs_off_t)kafs_inode_inline_bytes();
}

static inline int kafs_inode_size_uses_blocks(kafs_off_t inode_size)
{
  return inode_size > (kafs_off_t)kafs_inode_inline_bytes();
}

static inline size_t kafs_inode_bytes_for_format(uint32_t format_version)
{
  switch (format_version)
  {
  case KAFS_FORMAT_VERSION:
    return sizeof(kafs_sinode_t);
  case KAFS_FORMAT_VERSION_V5:
    return sizeof(kafs_sinode_v5_t);
  default:
    return 0;
  }
}

static inline uint64_t kafs_inode_table_bytes_for_format(uint32_t format_version, uint64_t inocnt)
{
  size_t inode_bytes = kafs_inode_bytes_for_format(format_version);

  if (inode_bytes == 0)
    return 0;

  return (uint64_t)inode_bytes * inocnt;
}

static kafs_mode_t kafs_ino_mode_get(const kafs_sinode_t *inoent)
{
  return kafs_mode_stoh(inoent->i_mode);
}

static void kafs_ino_mode_set(kafs_sinode_t *inoent, kafs_mode_t mode)
{
  inoent->i_mode = kafs_mode_htos(mode);
}

__attribute_maybe_unused__ static kafs_uid_t kafs_ino_uid_get(const kafs_sinode_t *inoent)
{
  return kafs_uid_stoh(inoent->i_uid);
}

__attribute_maybe_unused__ static void kafs_ino_uid_set(kafs_sinode_t *inode, kafs_uid_t uid)
{
  inode->i_uid = kafs_uid_htos(uid);
}

__attribute_maybe_unused__ static kafs_gid_t kafs_ino_gid_get(const kafs_sinode_t *inoent)
{
  return kafs_gid_stoh(inoent->i_gid);
}

__attribute_maybe_unused__ static void kafs_ino_gid_set(kafs_sinode_t *inode, kafs_gid_t gid)
{
  inode->i_gid = kafs_gid_htos(gid);
}

__attribute_maybe_unused__ static kafs_dev_t kafs_ino_dev_get(const kafs_sinode_t *inoent)
{
  return kafs_dev_stoh(inoent->i_rdev);
}

__attribute_maybe_unused__ static void kafs_ino_dev_set(kafs_sinode_t *inode, kafs_dev_t dev)
{
  inode->i_rdev = kafs_dev_htos(dev);
}

__attribute_maybe_unused__ static kafs_blkcnt_t kafs_ino_blocks_get(const kafs_sinode_t *inoent)
{
  return kafs_blkcnt_stoh(inoent->i_blocks);
}

__attribute_maybe_unused__ static void kafs_ino_blocks_set(kafs_sinode_t *inode,
                                                           kafs_blkcnt_t blkcnt)
{
  inode->i_blocks = kafs_blkcnt_htos(blkcnt);
}

__attribute_maybe_unused__ static kafs_off_t kafs_ino_size_get(const kafs_sinode_t *inoent)
{
  assert(inoent != NULL);
  return kafs_off_stoh(inoent->i_size);
}

__attribute_maybe_unused__ static void kafs_ino_size_set(kafs_sinode_t *inoent, kafs_off_t size)
{
  assert(inoent != NULL);
  inoent->i_size = kafs_off_htos(size);
}

__attribute_maybe_unused__ static kafs_time_t kafs_ino_atime_get(const kafs_sinode_t *inoent)
{
  assert(inoent != NULL);
  return kafs_time_stoh(inoent->i_atime);
}

__attribute_maybe_unused__ static void kafs_ino_atime_set(kafs_sinode_t *inoent, kafs_time_t atime)
{
  assert(inoent != NULL);
  inoent->i_atime = kafs_time_htos(atime);
}

static kafs_time_t kafs_ino_ctime_get(const kafs_sinode_t *inoent)
{
  assert(inoent != NULL);
  return kafs_time_stoh(inoent->i_ctime);
}

static void kafs_ino_ctime_set(kafs_sinode_t *inoent, kafs_time_t ctime)
{
  assert(inoent != NULL);
  inoent->i_ctime = kafs_time_htos(ctime);
}

static kafs_time_t kafs_ino_mtime_get(const kafs_sinode_t *inoent)
{
  assert(inoent != NULL);
  return kafs_time_stoh(inoent->i_mtime);
}

static void kafs_ino_mtime_set(kafs_sinode_t *inoent, kafs_time_t mtime)
{
  assert(inoent != NULL);
  inoent->i_mtime = kafs_time_htos(mtime);
}

__attribute_maybe_unused__ static kafs_time_t kafs_ino_dtime_get(const kafs_sinode_t *inoent)
{
  assert(inoent != NULL);
  return kafs_time_stoh(inoent->i_dtime);
}

static void kafs_ino_dtime_set(kafs_sinode_t *inoent, kafs_time_t dtime)
{
  assert(inoent != NULL);
  inoent->i_dtime = kafs_time_htos(dtime);
}

/// @brief inode 番号の利用状況を返す
/// @param inoent inodeテーブルのエントリ
/// @return 0: 未使用, != 0: 使用中
static int kafs_ino_get_usage(const kafs_sinode_t *inoent)
{
  assert(inoent != NULL);
  // 下記は 0 のビット表現がエンディアンによって変わらない前提
  return inoent->i_mode.value != 0;
}

static kafs_linkcnt_t kafs_ino_linkcnt_get(const kafs_sinode_t *inoent)
{
  kafs_linkcnt_t linkcnt = kafs_linkcnt_stoh(inoent->i_linkcnt);
  kafs_log(KAFS_LOG_DEBUG, "%s(inoent = %p) returns linkcnt = %" PRIuFAST16 "\n", __func__, inoent,
           linkcnt);
  return linkcnt;
}

static void kafs_ino_linkcnt_set(kafs_sinode_t *inoent, kafs_linkcnt_t linkcnt)
{
  kafs_log(KAFS_LOG_DEBUG, "%s(inoent = %p, linkcnt = %" PRIuFAST16 ")\n", __func__, inoent,
           linkcnt);
  inoent->i_linkcnt = kafs_linkcnt_htos(linkcnt);
}

static kafs_linkcnt_t kafs_ino_linkcnt_incr(kafs_sinode_t *inoent)
{
  kafs_linkcnt_t linkcnt = kafs_ino_linkcnt_get(inoent);
  // linkcnt is stored as u16 on disk; clamp to avoid wraparound.
  if (linkcnt == UINT16_MAX)
    return linkcnt;
  kafs_ino_linkcnt_set(inoent, linkcnt + 1);
  return linkcnt + 1;
}

static kafs_linkcnt_t kafs_ino_linkcnt_decr(kafs_sinode_t *inoent)
{
  kafs_linkcnt_t linkcnt = kafs_ino_linkcnt_get(inoent);
  assert(linkcnt > 0);
  kafs_ino_linkcnt_set(inoent, linkcnt - 1);
  return linkcnt - 1;
}

/// @brief 未使用の inode 番号を見つける
/// @param inotbl inode テーブルの先頭アドレス
/// @param pino 見つかった inode 番号の格納領域
/// @param pino_search 前回見つかった inode 番号の格納領域
/// @param inocnt inode 番号の最大数
/// @return 0: 成功、 < 0: 失敗 (-errno)
static int kafs_ino_find_free(const kafs_sinode_t *inotbl, kafs_inocnt_t *pino,
                              kafs_inocnt_t *pino_search, kafs_inocnt_t inocnt)
{
  assert(inotbl != NULL);
  assert(pino != NULL);
  kafs_inocnt_t ino_search = *pino_search;
  kafs_inocnt_t ino = ino_search + 1;
  while (ino_search != ino)
  {
    if (ino >= inocnt)
      ino = KAFS_INO_ROOTDIR;
    if (!kafs_ino_get_usage(&inotbl[ino]))
    {
      *pino_search = ino;
      *pino = ino;
      return KAFS_SUCCESS;
    }
    ino++;
  }
  return -ENOSPC;
}
