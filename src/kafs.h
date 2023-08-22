#pragma once
#define FUSE_USE_VERSION 30
#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include <inttypes.h>
#include <time.h>
#include <endian.h>

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
/// @param sinocnt inodeカウント型の記録表現
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

// ------------------------------------
// その他
// ------------------------------------

#define kafs_now() ({ \
  struct timespec _now; \
  KAFS_IOCALL (clock_gettime, CLOCK_REALTIME, &_now); \
  _now; \
})
