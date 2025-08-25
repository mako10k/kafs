#pragma once
#define FUSE_USE_VERSION 30
#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include <inttypes.h>
#include <time.h>
#include <endian.h>
#include <fuse.h>

/// 標準関数の正常戻り値
#define KAFS_SUCCESS 0

/// @brief bool 型
typedef enum
{
  /// @brief 偽
  KAFS_FALSE = 0,
  /// @brief 真
  KAFS_TRUE
} kafs_bool_t;

typedef enum
{
  KAFS_LOG_EMERG = FUSE_LOG_EMERG,
  KAFS_LOG_ALERT = FUSE_LOG_ALERT,
  KAFS_LOG_CRIT = FUSE_LOG_CRIT,
  KAFS_LOG_ERR = FUSE_LOG_ERR,
  KAFS_LOG_WARNING = FUSE_LOG_WARNING,
  KAFS_LOG_NOTICE = FUSE_LOG_NOTICE,
  KAFS_LOG_INFO = FUSE_LOG_INFO,
  KAFS_LOG_DEBUG = FUSE_LOG_DEBUG,
} kafs_loglevel_t;

#define kafs_log(level, fmt, ...) fuse_log ((enum fuse_log_level)(level), (fmt), __VA_ARGS__)

/// 標準呼び出し (戻り値が < 0 であれば戻り値を返す)
#define KAFS_CALL(func, ...) ({ \
  __auto_type _ret = (func) (__VA_ARGS__); \
  if (_ret < 0) \
    return _ret; \
  _ret; \
})

/// システムIOコール系呼び出し (戻り値が -1 であれば -errno を返す)
#define KAFS_IOCALL(func, ...) ({ \
  __auto_type _ret = (func) (__VA_ARGS__); \
  if (_ret == -1) \
    return -errno; \
  _ret; \
})

// ------------------------------------
// 処理で使う型
// ------------------------------------

/// @brief ブロックマスク型
typedef uint_fast16_t kafs_blkmask_t;
/// @brief ブロックカウント型
typedef uint_fast32_t kafs_blkcnt_t;
/// @brief inode カウント型
typedef uint_fast32_t kafs_inocnt_t;
/// @brief uid 型
typedef uint_fast16_t kafs_uid_t;
/// @brief ファイルオフセット型
typedef uint_fast64_t kafs_off_t;
/// @brief タイムスタンプ型
typedef struct timespec kafs_time_t;
/// @brief gid 型
typedef uint_fast16_t kafs_gid_t;
/// @brief リンクカウント型
typedef uint_fast16_t kafs_linkcnt_t;
/// @brief log2 のブロックサイズ型
typedef uint_fast16_t kafs_logblksize_t;
/// @brief マウントカウント型
typedef uint_fast16_t kafs_mntcnt_t;
/// @brief mode 型
typedef uint_fast16_t kafs_mode_t;
/// @brief デバイス型
typedef uint_fast16_t kafs_dev_t;
/// @brief ファイル名長さ型
typedef uint_fast16_t kafs_filenamelen_t;
/// @brief ブロックサイズ型
typedef uint_fast32_t kafs_blksize_t;
/// @brief inode ブロックカウント型
typedef uint_fast32_t kafs_iblkcnt_t;
/// @brief ハッシュ参照ID型 (Hash Reference ID)
typedef uint_fast32_t kafs_hrid_t;

// --- 定数（新フォーマット用） ---
#define KAFS_MAGIC 0x4B414653u /* 'KAFS' */
#define KAFS_FORMAT_VERSION 2u  /* HRL 採用版 */
#define KAFS_HASH_FAST_XXH64 1u
#define KAFS_HASH_STRONG_BLAKE3_256 1u

// ------------------------------------
// 記録表現で使う型
// ------------------------------------

/// @brief ブロックカウント型
struct kafs_sblkcnt
{
  uint32_t value;
} __attribute__((packed));
/// @brief ブロックカウント型
typedef struct kafs_sblkcnt kafs_sblkcnt_t;

/// @brief inode カウント型
struct kafs_sinocnt
{
  uint32_t value;
} __attribute__((packed));
/// @brief inode カウント型
typedef struct kafs_sinocnt kafs_sinocnt_t;

/// @brief uid 型
struct kafs_suid
{
  uint16_t value;
} __attribute__((packed));
/// @brief uid 型
typedef struct kafs_suid kafs_suid_t;

/// @brief ファイルオフセット型
struct kafs_soff
{
  uint64_t value;
} __attribute__((packed));
/// @brief ファイルオフセット型
typedef struct kafs_soff kafs_soff_t;

/// @brief タイムスタンプ型
struct kafs_stime
{
  uint64_t value;
} __attribute__((packed));
/// @brief ファイルオフセット型
typedef struct kafs_stime kafs_stime_t;

/// @brief gid 型
struct kafs_sgid
{
  uint16_t value;
} __attribute__((packed));
/// @brief gid 型
typedef struct kafs_sgid kafs_sgid_t;

/// @brief リンクカウント型
struct kafs_slinkcnt
{
  uint16_t value;
} __attribute ((packed));
/// @brief リンクカウント型
typedef struct kafs_slinkcnt kafs_slinkcnt_t;

/// @brief log2 のブロックサイズ型
struct kafs_slogblksize
{
  uint16_t value;
} __attribute__((packed));
/// @brief log2 のブロックサイズ型
typedef struct kafs_slogblksize kafs_slogblksize_t;

/// @brief マウントカウント型
struct kafs_smntcnt
{
  uint16_t value;
} __attribute__((packed));
/// @brief マウントカウント型
typedef struct kafs_smntcnt kafs_smntcnt_t;

/// @brief mode 型
struct kafs_smode
{
  uint16_t value;
} __attribute__((packed));
/// @brief mode 型
typedef struct kafs_smode kafs_smode_t;

/// @brief デバイス型
struct kafs_sdev
{
  uint16_t value;
} __attribute__((packed));
/// @brief デバイス型
typedef struct kafs_sdev kafs_sdev_t;

/// @brief ファイル名長さ型
struct kafs_sfilenamelen
{
  uint16_t value;
} __attribute__((packed));
/// @brief ファイル名長さ型
typedef struct kafs_sfilenamelen kafs_sfilenamelen_t;

// 一般用途の 32bit 保管型
struct kafs_su32
{
  uint32_t value;
} __attribute__((packed));
typedef struct kafs_su32 kafs_su32_t;

// 一般用途の 64bit 保管型
struct kafs_su64
{
  uint64_t value;
} __attribute__((packed));
typedef struct kafs_su64 kafs_su64_t;

// ------------------------------------
// 型変換
// ------------------------------------

static kafs_blkcnt_t
kafs_blkcnt_stoh (kafs_sblkcnt_t s)
{
  return le32toh (s.value);
}

static kafs_sblkcnt_t
kafs_blkcnt_htos (kafs_blkcnt_t h)
{
  kafs_sblkcnt_t s = {.value = htole32 (h) };
  return s;
}

static kafs_uid_t
kafs_uid_stoh (kafs_suid_t s)
{
  return le16toh (s.value);
}

static kafs_suid_t
kafs_uid_htos (kafs_uid_t h)
{
  kafs_suid_t s = {.value = htole16 (h) };
  return s;
}

static kafs_gid_t
kafs_gid_stoh (kafs_sgid_t s)
{
  return le16toh (s.value);
}

static kafs_sgid_t
kafs_gid_htos (kafs_gid_t h)
{
  kafs_sgid_t s = {.value = htole16 (h) };
  return s;
}

static kafs_dev_t
kafs_dev_stoh (kafs_sdev_t s)
{
  return le16toh (s.value);
}

static kafs_sdev_t
kafs_dev_htos (kafs_dev_t h)
{
  kafs_sdev_t s = {.value = htole16 (h) };
  return s;
}

static kafs_stime_t
kafs_time_htos (kafs_time_t h)
{
  uint64_t v = ((uint64_t) h.tv_sec << 32) | h.tv_nsec;
  kafs_stime_t s = {.value = htole64 (v) };
  return s;
}

static kafs_time_t
kafs_time_stoh (kafs_stime_t s)
{
  uint64_t v = le64toh (s.value);
  kafs_time_t h = {.tv_sec = (time_t) (v >> 32),.tv_nsec = (long) (v & 0xffffffff) };
  return h;
}

static kafs_logblksize_t
kafs_logblksize_stoh (kafs_slogblksize_t s)
{
  return le16toh (s.value);
}

static kafs_off_t
kafs_off_stoh (kafs_soff_t s)
{
  return le64toh (s.value);
}

static kafs_soff_t
kafs_off_htos (kafs_off_t h)
{
  kafs_soff_t s = {.value = htole64 (h) };
  return s;
}

/// @brief inodeカウント型の記録表現から処理表現へ変更する
/// @param s inodeカウント型の記録表現
/// @return inodeカウント型の処理表現
static kafs_inocnt_t
kafs_inocnt_stoh (kafs_sinocnt_t s)
{
  return le32toh (s.value);
}

static kafs_sinocnt_t
kafs_inocnt_htos (kafs_inocnt_t h)
{
  kafs_sinocnt_t s = {.value = htole32 (h) };
  return s;
}

static kafs_mode_t
kafs_mode_stoh (kafs_smode_t s)
{
  return le16toh (s.value);
}

static kafs_smode_t
kafs_mode_htos (kafs_mode_t h)
{
  kafs_smode_t s = {.value = htole16 (h) };
  return s;
}

static kafs_linkcnt_t
kafs_linkcnt_stoh (kafs_slinkcnt_t s)
{
  return le16toh (s.value);
}

static kafs_slinkcnt_t
kafs_linkcnt_htos (kafs_linkcnt_t h)
{
  kafs_slinkcnt_t s = {.value = htole16 (h) };
  return s;
}

static kafs_filenamelen_t
kafs_filenamelen_stoh (kafs_sfilenamelen_t s)
{
  return le16toh (s.value);
}

static kafs_sfilenamelen_t
kafs_filenamelen_htos (kafs_filenamelen_t h)
{
  kafs_sfilenamelen_t s = {.value = htole16 (h) };
  return s;
}

static inline uint32_t
kafs_u32_stoh (kafs_su32_t s)
{
  return le32toh (s.value);
}

static inline kafs_su32_t
kafs_u32_htos (uint32_t h)
{
  kafs_su32_t s = {.value = htole32 (h) };
  return s;
}

static inline uint64_t
kafs_u64_stoh (kafs_su64_t s)
{
  return le64toh (s.value);
}

static inline kafs_su64_t
kafs_u64_htos (uint64_t h)
{
  kafs_su64_t s = {.value = htole64 (h) };
  return s;
}

// ------------------------------------
// その他
// ------------------------------------

#define kafs_now() ({ \
  struct timespec _now; \
  KAFS_IOCALL (clock_gettime, CLOCK_REALTIME, &_now); \
  _now; \
})
