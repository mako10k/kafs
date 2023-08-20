#define FUSE_USE_VERSION 30

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include <fuse.h>
#include <errno.h>
#include <string.h>
#include <unistd.h>
#include <assert.h>
#include <endian.h>

typedef enum
{
  kafs_false = 0,
  kafs_true
} kafs_hbool_t;

/// 標準呼び出し (戻り値が < 0 であれば戻り値を返す)
#define kafs_call(func, ...) ({ \
  __auto_type _ret = (func) (__VA_ARGS__); \
  if (_ret < 0) \
    return _ret; \
  _ret; \
})

/// システムIOコール系呼び出し (戻り値が -1 であれば -errno を返す)
#define kafs_iocall(func, ...) ({ \
  __auto_type _ret = (func) (__VA_ARGS__); \
  if (_ret == -1) \
    return -errno; \
  _ret; \
})

/// @brief ブロックマスク型
typedef uint_fast16_t kafs_blkmask_t;
/// @brief ブロックマスク型
typedef struct kafs_sblkmask kafs_sblkmask_t;

/// @brief ブロックカウント型
typedef uint_fast32_t kafs_hblkcnt_t;
/// @brief ブロックカウント型
struct kafs_sblkcnt
{
  uint32_t value;
} __attribute__((packed));
/// @brief ブロックカウント型
typedef struct kafs_sblkcnt kafs_sblkcnt_t;

/// @brief inode カウント型
typedef uint_fast32_t kafs_hinocnt_t;
/// @brief inode カウント型
struct kafs_sinocnt
{
  uint32_t value;
} __attribute__((packed));
/// @brief inode カウント型
typedef struct kafs_sinocnt kafs_sinocnt_t;

/// @brief uid 型
typedef uint_fast16_t kafs_huid_t;
/// @brief uid 型
struct kafs_suid
{
  uint16_t value;
} __attribute__((packed));
/// @brief uid 型
typedef struct kafs_suid kafs_suid_t;

/// @brief ファイルオフセット型
typedef uint_fast64_t kafs_hoff_t;
/// @brief ファイルオフセット型
struct kafs_soff
{
  uint64_t value;
} __attribute__((packed));
/// @brief ファイルオフセット型
typedef struct kafs_soff kafs_soff_t;

/// @brief タイムスタンプ型
typedef struct timespec kafs_htime_t;
/// @brief タイムスタンプ型
struct kafs_stime
{
  uint64_t value;
} __attribute__((packed));
/// @brief ファイルオフセット型
typedef struct kafs_stime kafs_stime_t;

/// @brief gid 型
typedef uint_fast16_t kafs_hgid_t;
/// @brief gid 型
struct kafs_sgid
{
  uint16_t value;
} __attribute__((packed));
/// @brief gid 型
typedef struct kafs_sgid kafs_sgid_t;

/// @brief リンクカウント型
typedef uint_fast16_t kafs_hlinkcnt_t;
/// @brief リンクカウント型
struct kafs_slinkcnt
{
  uint16_t value;
} __attribute ((packed));
/// @brief リンクカウント型
typedef struct kafs_slinkcnt kafs_slinkcnt_t;

/// @brief log2 のブロックサイズ型
typedef uint_fast16_t kafs_hlogblksize_t;
/// @brief log2 のブロックサイズ型
struct kafs_slogblksize
{
  uint16_t value;
} __attribute__((packed));
/// @brief log2 のブロックサイズ型
typedef struct kafs_slogblksize kafs_slogblksize_t;

/// @brief マウントカウント型
typedef uint_fast16_t kafs_hmntcnt_t;
/// @brief マウントカウント型
struct kafs_smntcnt
{
  uint16_t value;
} __attribute__((packed));
/// @brief マウントカウント型
typedef struct kafs_smntcnt kafs_smntcnt_t;

/// @brief mode 型
typedef uint_fast16_t kafs_hmode_t;
/// @brief mode 型
struct kafs_smode
{
  uint16_t value;
} __attribute__((packed));
/// @brief mode 型
typedef struct kafs_smode kafs_smode_t;

/// @brief デバイス型
typedef uint_fast16_t kafs_hdev_t;
/// @brief デバイス型
struct kafs_sdev
{
  uint16_t value;
} __attribute__((packed));
/// @brief デバイス型
typedef struct kafs_sdev kafs_sdev_t;

/// @brief ファイル名長さ型
typedef uint_fast16_t kafs_hfilenamelen_t;
/// @brief ファイル名長さ型
struct kafs_sfilenamelen
{
  uint16_t value;
} __attribute__((packed));
/// @brief ファイル名長さ型
typedef struct kafs_sfilenamelen kafs_sfilenamelen_t;

/// @brief ブロックサイズ型
typedef uint_fast32_t kafs_hblksize_t;

/// @brief inode ブロックカウント型
typedef uint_fast32_t kafs_hiblkcnt_t;

/// 標準関数の正常戻り値
#define KAFS_SUCCESS 0
#define KAFS_SUCCESS_ALLOC 1
#define KAFS_SUCCESS_RELEASE 2
#define KAFS_SUCCESS_NONE 3

/// inode番号のうち存在しないことを表す値
#define KAFS_INO_NONE 0

/// ブロック番号のうち存在しないことを表す値
#define KAFS_BLO_NONE 0

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
  kafs_slinkcnt_t i_links_count;
  /// @brief ブロック数
  kafs_sblkcnt_t i_blocks;
  /// @brief デバイス番号
  kafs_sdev_t i_rdev;
  /// @brief ブロックデータ
  kafs_sblkcnt_t i_blkreftbl[15];
} __attribute__((packed));

/// @brief コンテキスト
struct kafs_context
{
  /// @brief スーパーブロック情報
  struct kafs_ssuperblock *c_superblock;
  /// @brief inode 情報
  struct kafs_sinode *c_inotbl;
  /// @brief ブロックマスク
  kafs_blkmask_t *c_blkmasktbl;
  /// @brief 前回の inode 検索情報
  kafs_hinocnt_t c_ino_search;
  /// @brief 前回のブロック検索情報
  kafs_hblkcnt_t c_blo_search;
  /// @brief ファイル記述子
  int c_fd;
};

/// @brief ディレクトリエントリ
struct kafs_sdirent
{
  /// @brief inode 番号
  kafs_sinocnt_t d_ino;
  /// @brief ファイル名の長さ
  kafs_sfilenamelen_t d_filenamelen;
  /// @brief ファイル名
  char d_filename[0];
} __attribute__((packed));

/// ブロックマスクのビット数
#define KAFS_BLKMASK_BITS (sizeof(kafs_blkmask_t) << 3)
/// ブロックマスクのビット数の log2 の値
#define KAFS_BLKMASK_LOG_BITS (sizeof(kafs_blkmask_t) + 3)
/// ブロックマスクのビット数のマスク値
#define KAFS_BLKMASK_MASK_BITS (KAFS_BLKMASK_BITS - 1)

/// @brief inodeカウント型の記録表現から処理表現へ変更する
/// @param sinocnt inodeカウント型の記録表現
/// @return inodeカウント型の処理表現
static kafs_hinocnt_t
kafs_inocnt_stoh (kafs_sinocnt_t s)
{
  return le32toh (s.value);
}

/// @brief スーパーブロックを取得する
/// @param ctx コンテキスト
/// @return スーパーブロック
static struct kafs_ssuperblock *
kafs_get_superblock (struct kafs_context *ctx)
{
  assert (ctx != NULL);
  return ctx->c_superblock;
}

/// @brief スーパーブロックを取得する
/// @param ctx コンテキスト
/// @return スーパーブロック
static const struct kafs_ssuperblock *
kafs_get_const_superblock (const struct kafs_context *ctx)
{
  assert (ctx != NULL);
  return ctx->c_superblock;
}

/// @brief inodeの最大数を読み出す
/// @param sb スーパーブロック
/// @return inodeの最大数
static kafs_hinocnt_t
kafs_sbload_inocnt (const struct kafs_ssuperblock *sb)
{
  assert (sb != NULL);
  return kafs_inocnt_stoh (sb->s_inocnt);
}

/// @brief inodeの最大数を読み出す
/// @param ctx コンテキスト
/// @return inodeの最大数
static kafs_hinocnt_t
kafs_load_inode_max (const struct kafs_context *ctx)
{
  assert (ctx != NULL);
  return kafs_sbload_inocnt (kafs_get_const_superblock (ctx));
}

/// @brief inode 構造体の参照を取得
/// @param ctx コンテキスト
/// @param ino inode 番号
/// @return inode 構造体の参照
static struct kafs_sinode *
kafs_get_inode (struct kafs_context *ctx, kafs_hinocnt_t ino)
{
  assert (ctx != NULL);
  assert (ino < kafs_load_inode_max (ctx));
  return ctx->c_inotbl + ino;
}

/// @brief inode 構造体の参照を取得
/// @param ctx コンテキスト
/// @param ino inode 番号
/// @return inode 構造体の参照
static const struct kafs_sinode *
kafs_get_const_inode (const struct kafs_context *ctx, kafs_hinocnt_t ino)
{
  assert (ctx != NULL);
  assert (ino < kafs_load_inode_max (ctx));
  return ctx->c_inotbl + ino;
}

/// @brief inode 番号の利用状況を返す
/// @param ctx コンテキスト
/// @param ino inode 番号
/// @return 0: 未使用, != 0: 使用中
static int
kafs_load_inode_usage (const struct kafs_context *ctx, kafs_hinocnt_t ino)
{
  assert (ctx != NULL);
  assert (ino < kafs_load_inode_max (ctx));
  // 下記は 0 のビット表現がエンディアンによって変わらない前提
  return kafs_get_const_inode (ctx, ino)->i_mode.value != 0;
}

static int
kafs_is_zero_inocnt_free (const struct kafs_context *ctx)
{
  assert (ctx != NULL);
  // 下記は 0 のビット表現がエンディアンによって変わらない前提
  return kafs_get_const_superblock (ctx)->s_inocnt_free.value == 0;
}

static int
kafs_sbload_inocnt_free (const struct kafs_ssuperblock *sb)
{
  assert (sb != NULL);
  return kafs_inocnt_stoh (sb->s_inocnt_free);
}

/// @brief 未使用の inode 番号を見つける
/// @param ctx コンテキスト
/// @param pino 見つかった inode 番号
/// @return 0: 成功、 < 0: 失敗 (-errno)
static int
kafs_find_free_inode (struct kafs_context *ctx, kafs_hinocnt_t * pino)
{
  assert (ctx != NULL);
  assert (pino != NULL);
  if (kafs_is_zero_inocnt_free (ctx))
    return -ENOSPC;
  kafs_hinocnt_t inocnt = kafs_load_inode_max (ctx);
  kafs_hinocnt_t ino_search = ctx->c_ino_search;
  kafs_hinocnt_t ino = ino_search + 1;
  while (ino_search != ino)
    {
      if (ino >= inocnt)
	ino = 0;
      if (!kafs_load_inode_usage (ctx, ino))
	{
	  ctx->c_ino_search = ino;
	  *pino = ino;
	  return KAFS_SUCCESS;
	}
      ino++;
    }
  return -ENOSPC;
}

static kafs_hblkcnt_t
kafs_blkcnt_stoh (kafs_sblkcnt_t s)
{
  return le32toh (s.value);
}

static kafs_sblkcnt_t
kafs_blkcnt_htos (kafs_hblkcnt_t h)
{
  kafs_sblkcnt_t s = {.value = htole32 (h) };
  return s;
}

static kafs_hblkcnt_t
kafs_sbload_blkcnt (const struct kafs_ssuperblock *sb)
{
  assert (sb != NULL);
  return kafs_blkcnt_stoh (sb->s_blkcnt);
}

static kafs_hblkcnt_t
kafs_load_blkcnt (const struct kafs_context *ctx)
{
  assert (ctx != NULL);
  return kafs_sbload_blkcnt (kafs_get_const_superblock (ctx));
}

static kafs_hblkcnt_t
kafs_sbload_r_blkcnt (const struct kafs_ssuperblock *sb)
{
  assert (sb != NULL);
  return kafs_blkcnt_stoh (sb->s_blkcnt);
}

static kafs_hblkcnt_t
kafs_load_r_blkcnt (const struct kafs_context *ctx)
{
  assert (ctx != NULL);
  return kafs_sbload_r_blkcnt (kafs_get_const_superblock (ctx));
}

static kafs_hblkcnt_t
kafs_sbload_blkcnt_free (const struct kafs_ssuperblock *sb)
{
  assert (sb != NULL);
  return kafs_blkcnt_stoh (sb->s_blkcnt_free);
}

static kafs_hblkcnt_t
kafs_load_blkcnt_free (const struct kafs_context *ctx)
{
  assert (ctx != NULL);
  return kafs_sbload_blkcnt_free (kafs_get_const_superblock (ctx));
}

static void
kafs_sbsave_blkcnt_free (struct kafs_ssuperblock *sb, kafs_hblkcnt_t blkcnt_free)
{
  assert (sb != NULL);
  sb->s_blkcnt_free = kafs_blkcnt_htos (blkcnt_free);
}

static void
kafs_save_blkcnt_free (struct kafs_context *ctx, kafs_hblkcnt_t blkcnt_free)
{
  assert (ctx != NULL);
  kafs_sbsave_blkcnt_free (kafs_get_superblock (ctx), blkcnt_free);
}

#define kafs_now() ({ \
  struct timespec _now; \
  kafs_iocall (clock_gettime, CLOCK_REALTIME, &_now); \
  _now; \
})

/// @brief 指定されたブロック番号の利用状況を取得する
/// @param ctx コンテキスト
/// @param blo ブロック番号
/// @return 0: 未使用, != 0: 使用中
static int
kafs_get_blkmask (const struct kafs_context *ctx, kafs_hblkcnt_t blo)
{
  assert (ctx != NULL);
  assert (blo < kafs_load_blkcnt (ctx));
  kafs_hblkcnt_t blod = blo >> KAFS_BLKMASK_LOG_BITS;
  kafs_hblkcnt_t blor = blo & KAFS_BLKMASK_MASK_BITS;
  return (ctx->c_blkmasktbl[blod] & (1 << blor)) != 0;
}

static kafs_stime_t
kafs_time_htos (kafs_htime_t h)
{
  uint64_t v = ((uint64_t) h.tv_sec << 32) | h.tv_nsec;
  kafs_stime_t s = {.value = htole64 (v) };
  return s;
}

static kafs_htime_t
kafs_time_stoh (kafs_stime_t s)
{
  uint64_t v = le64toh (s.value);
  kafs_htime_t h = {.tv_sec = v >> 32,.tv_nsec = v & 0xffffffff };
  return h;
}

static void
kafs_sbsave_wtime (struct kafs_ssuperblock *sb, kafs_htime_t wtime)
{
  assert (sb != NULL);
  sb->s_wtime = kafs_time_htos (wtime);
}

static void
kafs_save_wtime (struct kafs_context *ctx, kafs_htime_t wtime)
{
  assert (ctx != NULL);
  kafs_sbsave_wtime (kafs_get_superblock (ctx), wtime);
}

/// @brief 指定されたブロックの利用状況をセットする
/// @param ctx コンテキスト
/// @param blo ブロック番号
/// @param val 0: フラグをクリア, != 0: フラグをセット
/// @return 0: 成功, < 0: 失敗 (-errno)
static int
kafs_set_blkmask (struct kafs_context *ctx, kafs_hblkcnt_t blo, kafs_hbool_t val)
{
  assert (ctx != NULL);
  assert (blo < kafs_load_blkcnt (ctx));
  kafs_hblkcnt_t blod = blo >> KAFS_BLKMASK_LOG_BITS;
  kafs_hblkcnt_t blor = blo & KAFS_BLKMASK_MASK_BITS;
  if (val == kafs_true)
    {
      assert (!kafs_get_blkmask (ctx, blo));
      ctx->c_blkmasktbl[blod] |= 1 << blor;
      kafs_hblkcnt_t blkcnt_free = kafs_load_blkcnt_free (ctx);
      assert (blkcnt_free > 0);
      kafs_save_blkcnt_free (ctx, blkcnt_free - 1);
      kafs_save_wtime (ctx, kafs_now ());
    }
  else
    {
      assert (kafs_get_blkmask (ctx, blo));
      ctx->c_blkmasktbl[blod] &= ~(1 << blor);
      kafs_hblkcnt_t blkcnt_free = kafs_load_blkcnt_free (ctx);
      kafs_hblkcnt_t r_blkcnt = kafs_load_r_blkcnt (ctx);
      assert (blkcnt_free < r_blkcnt - 1);
      kafs_save_blkcnt_free (ctx, blkcnt_free + 1);
      kafs_save_wtime (ctx, kafs_now ());
    }
  return KAFS_SUCCESS;
}

static kafs_hblkcnt_t
kafs_get_free_blkmask (kafs_blkmask_t bm)
{
  if (sizeof (kafs_blkmask_t) <= sizeof (unsigned int))
    return __builtin_ctz (bm);
  if (sizeof (kafs_blkmask_t) <= sizeof (unsigned long))
    return __builtin_ctzl (bm);
  return __builtin_ctzll (bm);
}

static kafs_hlogblksize_t
kafs_logblksize_stoh (kafs_slogblksize_t s)
{
  return le16toh (s.value);
}

static kafs_hlogblksize_t
kafs_sbload_log_blksize (const struct kafs_ssuperblock *sb)
{
  assert (sb != NULL);
  return kafs_logblksize_stoh (sb->s_log_blksize) + 10;
}

static kafs_hlogblksize_t
kafs_load_log_blksize (const struct kafs_context *ctx)
{
  assert (ctx != NULL);
  return kafs_sbload_log_blksize (kafs_get_const_superblock (ctx));
}

static kafs_hblksize_t
kafs_sbload_blksize (const struct kafs_ssuperblock *sb)
{
  assert (sb != NULL);
  return 1 << kafs_sbload_log_blksize (sb);
}

static kafs_hblksize_t
kafs_load_blksize (const struct kafs_context *ctx)
{
  assert (ctx != NULL);
  return kafs_sbload_blksize (kafs_get_const_superblock (ctx));
}

// ---------------------------------------------------------
// BLOCK OPERATIONS
// ---------------------------------------------------------

/// @brief 未使用のブロック番号を取得し、使用中フラグをつける
/// @param ctx コンテキスト
/// @param pblo ブロック番号
/// @return 0: 成功, < 0: 失敗 (-errno)
static int
kafs_alloc_blk (struct kafs_context *ctx, kafs_hblkcnt_t * pblo)
{
  assert (ctx != NULL);
  assert (pblo != NULL);
  assert (*pblo == KAFS_BLO_NONE);
  kafs_hblkcnt_t blo_search = ctx->c_blo_search;
  kafs_hblkcnt_t blo = blo_search + 1;
  kafs_blkmask_t *blkmasktbl = ctx->c_blkmasktbl;
  kafs_blkmask_t blocnt = kafs_load_blkcnt (ctx);
  while (blo_search != blo)
    {
      if (blo >= blocnt)
	blo = 0;
      kafs_hblkcnt_t blod = blo >> KAFS_BLKMASK_LOG_BITS;
      kafs_hblkcnt_t blor = blo & KAFS_BLKMASK_MASK_BITS;	// ToDo: 2周目以降は常に0
      kafs_blkmask_t blkmask = ~blkmasktbl[blod];
      if (blkmask != 0)
	{
	  kafs_hblkcnt_t blor_found = kafs_get_free_blkmask (blkmask);
	  kafs_hblkcnt_t blo_found = (blod << KAFS_BLKMASK_LOG_BITS) + blor_found;
	  if (blo_found < blocnt)
	    {
	      ctx->c_blo_search = blo_found;
	      *pblo = blo_found;
	      kafs_call (kafs_set_blkmask, ctx, blo_found, 1);
	      return KAFS_SUCCESS_ALLOC;
	    }
	}
      blo += KAFS_BLKMASK_BITS - blor;
    }
  return -ENOSPC;
}

/// @brief ブロック単位でデータを読み出す
/// @param ctx コンテキスト
/// @param blo ブロック番号
/// @param buf 読み出すバッファ
/// @return 0: 成功, < 0: 失敗 (-errno)
static int
kafs_read_blk (struct kafs_context *ctx, kafs_hblkcnt_t blo, void *buf)
{
  assert (ctx != NULL);
  assert (buf != NULL);
  assert (blo != KAFS_INO_NONE);
  assert (blo < kafs_load_r_blkcnt (ctx));
  assert (kafs_get_blkmask (ctx, blo));
  kafs_hlogblksize_t log_blksize = kafs_load_log_blksize (ctx);
  kafs_hlogblksize_t blksize = kafs_load_blksize (ctx);
  ssize_t r = kafs_iocall (pread, ctx->c_fd, buf, blksize, blo << log_blksize);
  assert (r == blksize);
  return KAFS_SUCCESS;
}

/// @brief ブロック単位でデータを書き込む
/// @param ctx コンテキスト
/// @param blo ブロック番号へのポインタ
/// @param buf 書き込むバッファ
/// @return 0: 成功, < 0: 失敗 (-errno)
static int
kafs_write_blk (struct kafs_context *ctx, kafs_hblkcnt_t blo, const void *buf)
{
  assert (ctx != NULL);
  assert (buf != NULL);
  assert (blo != KAFS_INO_NONE);
  assert (blo < kafs_load_blkcnt (ctx));
  assert (kafs_get_blkmask (ctx, blo));
  kafs_hlogblksize_t log_blksize = kafs_load_log_blksize (ctx);
  kafs_hblksize_t blksize = kafs_load_blksize (ctx);
  ssize_t w = kafs_iocall (pwrite, ctx->c_fd, buf, blksize, blo << log_blksize);
  assert (w == blksize);
  return KAFS_SUCCESS;
}

/// @brief ブロックデータを削除する
/// @param ctx コンテキスト
/// @param sblo ブロック番号
/// @return 0: 成功, < 0: 失敗 (-errno)
static int
kafs_release_blk (struct kafs_context *ctx, kafs_hblkcnt_t * pblo)
{
  assert (ctx != NULL);
  assert (pblo != NULL);
  assert (*pblo != KAFS_INO_NONE);
  assert (*pblo < kafs_load_r_blkcnt (ctx));
  assert (kafs_get_blkmask (ctx, *pblo));
  kafs_hblksize_t blksize = kafs_load_blksize (ctx);
  char zbuf[blksize];
  memset (zbuf, 0, blksize);
  kafs_call (kafs_write_blk, ctx, *pblo, zbuf);
  kafs_call (kafs_set_blkmask, ctx, *pblo, kafs_false);
  *pblo = KAFS_BLO_NONE;
  return KAFS_SUCCESS;
}

// ---------------------------------------------------------
// INODE BLOCK OPERATIONS
// ---------------------------------------------------------

typedef enum
{
  KAFS_IBLKREF_FUNC_GET,
  KAFS_IBLKREF_FUNC_PUT,
  KAFS_IBLKREF_FUNC_DEL
} kafs_iblkref_func_t;

static kafs_hlogblksize_t
kafs_load_log_blkref_pb (const struct kafs_context *ctx)
{
  return kafs_load_log_blksize (ctx) + 3;
}

static kafs_hblksize_t
kafs_load_blkref_pb (const struct kafs_context *ctx)
{
  return 1 << kafs_load_log_blkref_pb (ctx);
}

#define KAFS_DIRECT_SIZE sizeof(((struct kafs_sinode *)NULL)->i_blkreftbl)

static kafs_hoff_t
kafs_off_stoh (kafs_soff_t s)
{
  return le64toh (s.value);
}

static kafs_hoff_t
kafs_load_filesize (const struct kafs_context *ctx, kafs_hinocnt_t ino)
{
  assert (ctx != NULL);
  assert (ino != KAFS_INO_NONE);
  assert (ino < kafs_load_inode_max (ctx));
  const struct kafs_sinode *inoent = kafs_get_const_inode (ctx, ino);
  return kafs_off_stoh (inoent->i_size);
}

static int
kafs_is_zero_blk (const void *buf, size_t len)
{
  const char *c = buf;
  while (len--)
    if (*c++)
      return 1;
  return 0;
}

static int
kafs_do_ibrk (struct kafs_context *ctx, kafs_hinocnt_t ino, kafs_hiblkcnt_t iblo, kafs_hblkcnt_t * pblo,
	      kafs_iblkref_func_t ifunc)
{
  assert (ctx != NULL);
  assert (pblo != NULL);
  assert (ino != KAFS_INO_NONE);
  assert (ino < kafs_load_inode_max (ctx));
  struct kafs_sinode *inoent = kafs_get_inode (ctx, ino);

  if (iblo < 12)
    {
      kafs_hblkcnt_t blo_data = kafs_blkcnt_stoh (inoent->i_blkreftbl[iblo]);
      switch (ifunc)
	{
	case KAFS_IBLKREF_FUNC_GET:
	  *pblo = blo_data;
	  return KAFS_SUCCESS;

	case KAFS_IBLKREF_FUNC_PUT:
	  if (blo_data == KAFS_BLO_NONE)
	    {
	      kafs_call (kafs_alloc_blk, ctx, &blo_data);
	      inoent->i_blkreftbl[iblo] = kafs_blkcnt_htos (blo_data);
	    }
	  *pblo = blo_data;
	  return KAFS_SUCCESS;

	case KAFS_IBLKREF_FUNC_DEL:
	  if (blo_data == KAFS_BLO_NONE)
	    {
	      *pblo = KAFS_BLO_NONE;
	      return KAFS_SUCCESS;
	    }
	  kafs_call (kafs_release_blk, ctx, &blo_data);
	  inoent->i_blkreftbl[iblo] = kafs_blkcnt_htos (KAFS_BLO_NONE);
	  *pblo = KAFS_BLO_NONE;
	  return KAFS_SUCCESS;
	}
    }

  iblo -= 12;
  kafs_hblksize_t blksize = kafs_load_blksize (ctx);
  kafs_hlogblksize_t log_blkrefs_pb = kafs_load_log_blkref_pb (ctx);
  kafs_hblkcnt_t blkrefs_pb = kafs_load_blkref_pb (ctx);
  if (iblo < blkrefs_pb)
    {
      kafs_sblkcnt_t blkreftbl[blkrefs_pb];
      kafs_hblkcnt_t blo_blkreftbl = kafs_blkcnt_stoh (inoent->i_blkreftbl[12]);
      kafs_hblkcnt_t blo_data;

      switch (ifunc)
	{
	case KAFS_IBLKREF_FUNC_GET:
	  if (blo_blkreftbl == KAFS_BLO_NONE)
	    {
	      *pblo = KAFS_BLO_NONE;
	      return KAFS_SUCCESS;
	    }
	  kafs_call (kafs_read_blk, ctx, blo_blkreftbl, blkreftbl);
	  *pblo = kafs_blkcnt_stoh (blkreftbl[iblo]);
	  return KAFS_SUCCESS;

	case KAFS_IBLKREF_FUNC_PUT:
	  if (blo_blkreftbl == KAFS_BLO_NONE)
	    {
	      kafs_call (kafs_alloc_blk, ctx, &blo_blkreftbl);
	      inoent->i_blkreftbl[12] = kafs_blkcnt_htos (blo_blkreftbl);
	      blo_data = KAFS_BLO_NONE;
	    }
	  else
	    {
	      kafs_call (kafs_read_blk, ctx, blo_blkreftbl, blkreftbl);
	      blo_data = kafs_blkcnt_stoh (blkreftbl[iblo]);
	    }
	  if (blo_data == KAFS_BLO_NONE)
	    {
	      kafs_call (kafs_alloc_blk, ctx, &blo_data);
	      blkreftbl[iblo] = kafs_blkcnt_htos (blo_data);
	      kafs_call (kafs_write_blk, ctx, blo_blkreftbl, blkreftbl);
	    }
	  *pblo = blo_data;
	  return KAFS_SUCCESS;

	case KAFS_IBLKREF_FUNC_DEL:
	  if (blo_blkreftbl == KAFS_BLO_NONE)
	    {
	      *pblo = KAFS_BLO_NONE;
	      return KAFS_SUCCESS;
	    }
	  kafs_call (kafs_read_blk, ctx, blo_blkreftbl, blkreftbl);
	  blo_data = kafs_blkcnt_stoh (blkreftbl[iblo]);
	  if (blo_data != KAFS_BLO_NONE)
	    {
	      kafs_call (kafs_release_blk, ctx, &blo_data);
	      blkreftbl[iblo] = kafs_blkcnt_htos (blo_data);
	    }
	  if (!kafs_is_zero_blk (blkreftbl, blksize))
	    {
	      kafs_call (kafs_write_blk, ctx, blo_blkreftbl, blkreftbl);
	      *pblo = KAFS_BLO_NONE;
	      return KAFS_SUCCESS;
	    }
	  kafs_call (kafs_release_blk, ctx, &blo_blkreftbl);
	  inoent->i_blkreftbl[12] = kafs_blkcnt_htos (blo_blkreftbl);
	  *pblo = KAFS_BLO_NONE;
	  return KAFS_SUCCESS;
	}
    }

  iblo -= blkrefs_pb;
  kafs_hlogblksize_t log_blkrefs_pb_sq = log_blkrefs_pb << 1;
  kafs_hblksize_t blkrefs_pb_sq = 1 << log_blkrefs_pb_sq;
  if (iblo < blkrefs_pb_sq)
    {
      kafs_sblkcnt_t blkreftbl1[blkrefs_pb];
      kafs_sblkcnt_t blkreftbl2[blkrefs_pb];
      kafs_hblkcnt_t blo_blkreftbl1 = kafs_blkcnt_stoh (inoent->i_blkreftbl[13]);
      kafs_hblkcnt_t blo_blkreftbl2;
      kafs_hblkcnt_t blo_data;
      kafs_hblkcnt_t iblo1 = iblo >> log_blkrefs_pb;
      kafs_hblkcnt_t iblo2 = iblo & (blkrefs_pb - 1);

      switch (ifunc)
	{
	case KAFS_IBLKREF_FUNC_GET:
	  if (blo_blkreftbl1 == KAFS_BLO_NONE)
	    {
	      *pblo = KAFS_BLO_NONE;
	      return KAFS_SUCCESS;
	    }
	  kafs_call (kafs_read_blk, ctx, blo_blkreftbl1, blkreftbl1);
	  blo_blkreftbl2 = kafs_blkcnt_stoh (blkreftbl1[iblo1]);
	  if (blo_blkreftbl2 == KAFS_BLO_NONE)
	    {
	      *pblo = KAFS_BLO_NONE;
	      return KAFS_SUCCESS;
	    }
	  kafs_call (kafs_read_blk, ctx, blo_blkreftbl2, blkreftbl2);
	  *pblo = kafs_blkcnt_stoh (blkreftbl2[iblo2]);
	  return KAFS_SUCCESS;

	case KAFS_IBLKREF_FUNC_PUT:
	  if (blo_blkreftbl1 == KAFS_BLO_NONE)
	    {
	      kafs_call (kafs_alloc_blk, ctx, &blo_blkreftbl1);
	      inoent->i_blkreftbl[13] = kafs_blkcnt_htos (blo_blkreftbl1);
	      memset (blkreftbl1, 0, blksize);
	      blo_blkreftbl2 = KAFS_BLO_NONE;
	    }
	  else
	    {
	      kafs_call (kafs_read_blk, ctx, blo_blkreftbl1, blkreftbl1);
	      blo_blkreftbl2 = kafs_blkcnt_stoh (blkreftbl1[iblo1]);
	    }
	  if (blo_blkreftbl2 == KAFS_BLO_NONE)
	    {
	      kafs_call (kafs_alloc_blk, ctx, &blo_blkreftbl2);
	      blkreftbl1[iblo1] = kafs_blkcnt_htos (blo_blkreftbl2);
	      kafs_call (kafs_write_blk, ctx, blo_blkreftbl1, blkreftbl1);
	      memset (blkreftbl2, 0, blksize);
	      blo_data = KAFS_BLO_NONE;
	    }
	  else
	    {
	      kafs_call (kafs_read_blk, ctx, blo_blkreftbl2, blkreftbl2);
	      blo_data = kafs_blkcnt_stoh (blkreftbl2[iblo2]);
	    }
	  if (blo_data == KAFS_BLO_NONE)
	    {
	      kafs_call (kafs_alloc_blk, ctx, &blo_data);
	      blkreftbl2[iblo2] = kafs_blkcnt_htos (blo_data);
	      kafs_call (kafs_write_blk, ctx, blo_blkreftbl2, blkreftbl2);
	    }
	  *pblo = blo_data;
	  return KAFS_SUCCESS;

	case KAFS_IBLKREF_FUNC_DEL:
	  if (blo_blkreftbl1 == KAFS_BLO_NONE)
	    {
	      *pblo = KAFS_BLO_NONE;
	      return KAFS_SUCCESS;
	    }
	  kafs_call (kafs_read_blk, ctx, blo_blkreftbl1, blkreftbl1);
	  blo_blkreftbl2 = kafs_blkcnt_stoh (blkreftbl1[iblo1]);
	  if (blo_blkreftbl2 == KAFS_BLO_NONE)
	    {
	      *pblo = KAFS_BLO_NONE;
	      return KAFS_SUCCESS;
	    }
	  kafs_call (kafs_read_blk, ctx, blo_blkreftbl2, blkreftbl2);
	  blo_data = kafs_blkcnt_stoh (blkreftbl2[iblo2]);
	  if (blo_data == KAFS_BLO_NONE)
	    {
	      *pblo = KAFS_BLO_NONE;
	      return KAFS_SUCCESS;
	    }
	  kafs_call (kafs_release_blk, ctx, &blo_data);
	  blkreftbl2[iblo2] = kafs_blkcnt_htos (KAFS_BLO_NONE);
	  if (!kafs_is_zero_blk (blkreftbl2, blksize))
	    {
	      kafs_call (kafs_write_blk, ctx, blo_blkreftbl2, blkreftbl2);
	      *pblo = KAFS_BLO_NONE;
	      return KAFS_SUCCESS;
	    }
	  kafs_call (kafs_release_blk, ctx, &blo_blkreftbl2);
	  blkreftbl1[iblo1] = kafs_blkcnt_htos (KAFS_BLO_NONE);
	  if (!kafs_is_zero_blk (blkreftbl1, blksize))
	    {
	      kafs_call (kafs_write_blk, ctx, blo_blkreftbl1, blkreftbl1);
	      *pblo = KAFS_BLO_NONE;
	      return KAFS_SUCCESS;
	    }
	  kafs_call (kafs_release_blk, ctx, &blo_blkreftbl1);
	  inoent->i_blkreftbl[13] = kafs_blkcnt_htos (KAFS_BLO_NONE);
	  *pblo = KAFS_BLO_NONE;
	  return KAFS_SUCCESS;
	}
    }

  iblo -= blkrefs_pb_sq;
  kafs_sblkcnt_t blkreftbl1[blkrefs_pb];
  kafs_sblkcnt_t blkreftbl2[blkrefs_pb];
  kafs_sblkcnt_t blkreftbl3[blkrefs_pb];

  kafs_hblkcnt_t blo_blkreftbl1 = kafs_blkcnt_stoh (inoent->i_blkreftbl[14]);
  kafs_hblkcnt_t blo_blkreftbl2;
  kafs_hblkcnt_t blo_blkreftbl3;
  kafs_hblkcnt_t blo_data;

  kafs_hblkcnt_t iblo1 = iblo >> log_blkrefs_pb_sq;
  kafs_hblkcnt_t iblo2 = (iblo >> log_blkrefs_pb) & (blkrefs_pb - 1);
  kafs_hblkcnt_t iblo3 = iblo & (blkrefs_pb - 1);

  switch (ifunc)
    {
    case KAFS_IBLKREF_FUNC_GET:
      if (blo_blkreftbl1 == KAFS_BLO_NONE)
	{
	  *pblo = KAFS_BLO_NONE;
	  return KAFS_SUCCESS;
	}
      kafs_call (kafs_read_blk, ctx, blo_blkreftbl1, blkreftbl1);
      blo_blkreftbl2 = kafs_blkcnt_stoh (blkreftbl1[iblo1]);
      if (blo_blkreftbl2 == KAFS_BLO_NONE)
	{
	  *pblo = KAFS_BLO_NONE;
	  return KAFS_SUCCESS;
	}
      kafs_call (kafs_read_blk, ctx, blo_blkreftbl2, blkreftbl2);
      blo_blkreftbl3 = kafs_blkcnt_stoh (blkreftbl2[iblo2]);
      if (blo_blkreftbl3 == KAFS_BLO_NONE)
	{
	  *pblo = KAFS_BLO_NONE;
	  return KAFS_SUCCESS;
	}
      kafs_call (kafs_read_blk, ctx, blo_blkreftbl3, blkreftbl3);
      *pblo = kafs_blkcnt_stoh (blkreftbl3[iblo3]);
      return KAFS_SUCCESS;

    case KAFS_IBLKREF_FUNC_PUT:
      if (blo_blkreftbl1 == KAFS_BLO_NONE)
	{
	  kafs_call (kafs_alloc_blk, ctx, &blo_blkreftbl1);
	  inoent->i_blkreftbl[14] = kafs_blkcnt_htos (blo_blkreftbl1);
	  memset (blkreftbl1, 0, blksize);
	  blo_blkreftbl2 = KAFS_BLO_NONE;
	}
      else
	{
	  kafs_call (kafs_read_blk, ctx, blo_blkreftbl1, blkreftbl1);
	  blo_blkreftbl2 = kafs_blkcnt_stoh (blkreftbl1[iblo1]);
	}
      if (blo_blkreftbl2 == KAFS_BLO_NONE)
	{
	  kafs_call (kafs_alloc_blk, ctx, &blo_blkreftbl2);
	  blkreftbl1[iblo1] = kafs_blkcnt_htos (blo_blkreftbl2);
	  kafs_call (kafs_write_blk, ctx, blo_blkreftbl1, blkreftbl1);
	  memset (blkreftbl2, 0, blksize);
	  blo_blkreftbl3 = KAFS_BLO_NONE;
	}
      else
	{
	  kafs_call (kafs_read_blk, ctx, blo_blkreftbl2, blkreftbl2);
	  blo_blkreftbl3 = kafs_blkcnt_stoh (blkreftbl2[iblo2]);
	}
      if (blo_blkreftbl3 == KAFS_BLO_NONE)
	{
	  kafs_call (kafs_alloc_blk, ctx, &blo_blkreftbl3);
	  blkreftbl2[iblo2] = kafs_blkcnt_htos (blo_blkreftbl3);
	  kafs_call (kafs_write_blk, ctx, blo_blkreftbl2, blkreftbl2);
	  memset (blkreftbl3, 0, blksize);
	  blo_data = KAFS_BLO_NONE;
	}
      else
	{
	  kafs_call (kafs_read_blk, ctx, blo_blkreftbl3, blkreftbl3);
	  blo_data = kafs_blkcnt_stoh (blkreftbl3[iblo3]);
	}
      if (blo_data == KAFS_BLO_NONE)
	{
	  kafs_call (kafs_alloc_blk, ctx, &blo_data);
	  blkreftbl3[iblo3] = kafs_blkcnt_htos (blo_data);
	  kafs_call (kafs_write_blk, ctx, blo_blkreftbl3, blkreftbl3);
	}
      *pblo = blo_data;
      return KAFS_SUCCESS;

    case KAFS_IBLKREF_FUNC_DEL:
      if (blo_blkreftbl1 == KAFS_BLO_NONE)
	{
	  *pblo = KAFS_BLO_NONE;
	  return KAFS_SUCCESS;
	}
      kafs_call (kafs_read_blk, ctx, blo_blkreftbl1, blkreftbl1);
      blo_blkreftbl2 = kafs_blkcnt_stoh (blkreftbl1[iblo1]);
      if (blo_blkreftbl2 == KAFS_BLO_NONE)
	{
	  *pblo = KAFS_BLO_NONE;
	  return KAFS_SUCCESS;
	}
      kafs_call (kafs_read_blk, ctx, blo_blkreftbl2, blkreftbl2);
      blo_blkreftbl3 = kafs_blkcnt_stoh (blkreftbl2[iblo2]);
      if (blo_blkreftbl3 == KAFS_BLO_NONE)
	{
	  *pblo = KAFS_BLO_NONE;
	  return KAFS_SUCCESS;
	}
      kafs_call (kafs_read_blk, ctx, blo_blkreftbl3, blkreftbl3);
      blo_data = kafs_blkcnt_stoh (blkreftbl3[iblo3]);
      if (blo_data == KAFS_BLO_NONE)
	{
	  *pblo = KAFS_BLO_NONE;
	  return KAFS_SUCCESS;
	}
      kafs_call (kafs_release_blk, ctx, &blo_data);
      blkreftbl3[iblo3] = kafs_blkcnt_htos (KAFS_BLO_NONE);
      if (!kafs_is_zero_blk (blkreftbl3, blksize))
	{
	  kafs_call (kafs_write_blk, ctx, blo_blkreftbl3, blkreftbl3);
	  *pblo = KAFS_BLO_NONE;
	  return KAFS_SUCCESS;
	}
      kafs_call (kafs_release_blk, ctx, &blo_blkreftbl3);
      blkreftbl2[iblo2] = kafs_blkcnt_htos (KAFS_BLO_NONE);
      if (!kafs_is_zero_blk (blkreftbl2, blksize))
	{
	  kafs_call (kafs_write_blk, ctx, blo_blkreftbl2, blkreftbl2);
	  *pblo = KAFS_BLO_NONE;
	  return KAFS_SUCCESS;
	}
      kafs_call (kafs_release_blk, ctx, &blo_blkreftbl2);
      blkreftbl1[iblo1] = kafs_blkcnt_htos (KAFS_BLO_NONE);
      if (!kafs_is_zero_blk (blkreftbl1, blksize))
	{
	  kafs_call (kafs_write_blk, ctx, blo_blkreftbl1, blkreftbl1);
	  *pblo = KAFS_BLO_NONE;
	  return KAFS_SUCCESS;
	}
      kafs_call (kafs_release_blk, ctx, &blo_blkreftbl1);
      inoent->i_blkreftbl[14] = kafs_blkcnt_htos (KAFS_BLO_NONE);
      *pblo = KAFS_BLO_NONE;
      return KAFS_SUCCESS;
    }
}

/// @brief inode毎のデータを読み出す（ブロック単位）
/// @param ctx コンテキスト
/// @param ino inode番号
/// @param buf バッファ
/// @param iblo ブロック番号
/// @return 0: 成功, < 0: 失敗 (-errno)
static int
kafs_read_iblk (struct kafs_context *ctx, kafs_hinocnt_t ino, kafs_hiblkcnt_t iblo, void *buf)
{
  assert (ctx != NULL);
  assert (buf != NULL);
  assert (ino != KAFS_INO_NONE);
  assert (ino < kafs_load_inode_max (ctx));
  assert (kafs_load_inode_usage (ctx, ino));
  assert (kafs_load_filesize (ctx, ino) > KAFS_DIRECT_SIZE);

  kafs_hblkcnt_t blo;
  kafs_call (kafs_do_ibrk, ctx, ino, iblo, &blo, KAFS_IBLKREF_FUNC_GET);
  kafs_call (kafs_read_blk, ctx, blo, buf);
  return KAFS_SUCCESS;
}

/// @brief inode毎のデータを書き込む（ブロック単位）
/// @param ctx コンテキスト
/// @param ino inode番号
/// @param iblo ブロック番号
/// @param buf バッファ
/// @return 0: 成功, < 0: 失敗 (-errno)
static int
kafs_write_iblk (struct kafs_context *ctx, kafs_hinocnt_t ino, kafs_hiblkcnt_t iblo, const void *buf)
{
  assert (ctx != NULL);
  assert (buf != NULL);
  assert (ino != KAFS_INO_NONE);
  assert (ino < kafs_load_inode_max (ctx));
  assert (kafs_load_inode_usage (ctx, ino));
  assert (kafs_load_filesize (ctx, ino) > KAFS_DIRECT_SIZE);	// TODO: 拡張時の考慮

  kafs_hblkcnt_t blo;
  kafs_call (kafs_do_ibrk, ctx, ino, iblo, &blo, KAFS_IBLKREF_FUNC_PUT);
  assert (blo != KAFS_BLO_NONE);
  kafs_call (kafs_write_blk, ctx, blo, buf);
  return KAFS_SUCCESS;
}

static int
kafs_release_iblk (struct kafs_context *ctx, kafs_hinocnt_t ino, kafs_hiblkcnt_t iblo)
{
  assert (ctx != NULL);
  assert (ino != KAFS_INO_NONE);
  assert (ino < kafs_load_inode_max (ctx));
  assert (kafs_load_inode_usage (ctx, ino));
  assert (kafs_load_filesize (ctx, ino) > KAFS_DIRECT_SIZE);	// TODO: 縮小時の考慮

  kafs_hblkcnt_t blo;
  kafs_call (kafs_do_ibrk, ctx, ino, iblo, &blo, KAFS_IBLKREF_FUNC_DEL);
  assert (blo == KAFS_BLO_NONE);
  return KAFS_SUCCESS;
}

/// @brief inode 毎にデータを読み出す
/// @param ctx コンテキスト
/// @param ino inode 番号
/// @param buf バッファ
/// @param size バッファサイズ
/// @param offset オフセット
/// @return > 0: 読み出しサイズ, 0: EOF, < 0: エラー(-errno)
static ssize_t
kafs_pread_inode (struct kafs_context *ctx, kafs_inocnt_t ino, void *buf, size_t size, off_t offset)
{
  assert (ctx != NULL);
  assert (ino < ctx->c_superblock->s_inocnt_free);
  assert (buf != NULL);

  struct kafs_sinode *inotbl = ctx->c_inotbl + ino;
  kafs_off_t i_size = inotbl->i_size;

  // 60バイト以下は直接
  if (i_size <= sizeof (inotbl->i_blkreftbl))
    {
      if (offset >= sizeof (inotbl->i_blkreftbl))
	return KAFS_SUCCESS;
      if (offset + size > i_size)
	size = i_size - offset;
      memcpy (buf, (void *) inotbl->i_blkreftbl + offset, size);
      return size;
    }

  size_t size_read = 0;
  if (offset >= i_size)
    return 0;
  if (offset + size > i_size)
    size = i_size - offset;
  if (size == 0)
    return 0;

  kafs_logblksize_t log_blksize = 10 + ctx->c_superblock->s_log_blksize;
  kafs_blksize_t blksize = 1 << log_blksize;
  kafs_blksize_t offset_blksize = offset & (blksize - 1);
  if (offset_blksize > 0 || size - size_read < blksize)
    {
      char rbuf[blksize];
      kafs_iblkcnt_t iblo = offset >> log_blksize;
      kafs_call (kafs_read_iblk, ctx, inotbl, iblo, rbuf);
      if (size < blksize - offset_blksize)
	{
	  memcpy (buf, rbuf + offset_blksize, size);
	  return size;
	}
      memcpy (buf, rbuf + offset_blksize, blksize - offset_blksize);
      size_read += blksize - offset_blksize;
    }
  while (size_read < size)
    {
      kafs_iblkcnt_t iblo = (offset + size_read) >> log_blksize;
      if (size - size_read <= blksize)
	{
	  char rbuf[blksize];
	  kafs_call (kafs_read_iblk, ctx, inotbl, iblo, rbuf);
	  memcpy (buf + size_read, rbuf, size - size_read);
	  return size;
	}
      kafs_call (kafs_read_iblk, ctx, inotbl, iblo, buf + size_read);
      size_read += blksize;
    }
  return size;
}

/// @brief inode 毎にデータを読み出す
/// @param ctx コンテキスト
/// @param ino inode 番号
/// @param buf バッファ
/// @param size バッファサイズ
/// @param offset オフセット
/// @return > 0: 読み出しサイズ, 0: EOF, < 0: エラー(-errno)
static ssize_t
kafs_pwrite_inode (struct kafs_context *ctx, kafs_inocnt_t ino, const void *buf, size_t size, off_t offset)
{
  assert (ctx != NULL);
  assert (ino < ctx->c_superblock->s_inocnt_free);
  assert (buf != NULL);

  struct kafs_sinode *inotbl = ctx->c_inotbl + ino;
  kafs_off_t i_size = inotbl->i_size;
  kafs_off_t i_size_new = offset + size;
  kafs_logblksize_t log_blksize = 10 + ctx->c_superblock->s_log_blksize;
  kafs_blksize_t blksize = 1 << log_blksize;

  if (i_size < i_size_new)
    {
      // サイズ拡大時
      inotbl->i_size = i_size_new;
      if (i_size_new > sizeof (inotbl->i_blkreftbl))
	{
	  char wbuf[blksize];
	  memset (wbuf, 0, blksize);
	  memcpy (wbuf, inotbl->i_blkreftbl, i_size);
	  memset (inotbl->i_blkreftbl, 0, sizeof (inotbl->i_blkreftbl));
	  kafs_call (kafs_write_iblk, ctx, inotbl, 0, wbuf);
	}
      i_size = i_size_new;
    }

  size_t size_written = 0;
  if (size == 0)
    return size_written;

  // 60バイト以下は直接
  if (i_size <= sizeof (inotbl->i_blkreftbl))
    {
      memcpy ((void *) inotbl->i_blkreftbl + offset, buf, size);
      return size;
    }

  kafs_blksize_t offset_blksize = offset & (blksize - 1);
  if (offset_blksize > 0 || size - size_written < blksize)
    {
      // 1ブロック目で端数が出る場合
      kafs_iblkcnt_t iblo = offset >> log_blksize;
      // 書き戻しバッファ
      char wbuf[blksize];
      kafs_call (kafs_read_iblk, ctx, inotbl, iblo, wbuf);
      if (size < blksize - offset_blksize)
	{
	  // 1ブロックのみの場合
	  memcpy (wbuf + offset_blksize, buf, size);
	  kafs_call (kafs_write_iblk, ctx, inotbl, iblo, wbuf);
	  return size;
	}
      // ブロックの残り分を書き込む
      memcpy (wbuf + offset_blksize, buf, blksize - offset_blksize);
      kafs_call (kafs_write_iblk, ctx, inotbl, iblo, wbuf);
      size_written += blksize - offset_blksize;
    }

  while (size_written < size)
    {
      kafs_iblkcnt_t iblo = (offset + size_written) >> log_blksize;
      if (size - size_written < blksize)
	{
	  char wbuf[blksize];
	  kafs_call (kafs_read_iblk, ctx, inotbl, iblo, wbuf);
	  memcpy (wbuf, buf + size_written, size - size_written);
	  kafs_call (kafs_write_iblk, ctx, inotbl, iblo, wbuf);
	  return size;
	}
      kafs_call (kafs_write_iblk, ctx, inotbl, iblo, buf + size_written);
      size_written += blksize;
    }
  return size;
}

/// @brief ディレクトリエントリを読み出す
/// @param ctx コンテキスト
/// @param ino_dir ディレクトリのinode番号
/// @param dirent 読み出すディレクトリエントリのバッファ（sizeof(struct kafs_dirent) 以上）
/// @param direntlen バッファの長さ（全体がこれより長い場合はファイル名部分が読みだされない）
/// @param offset オフセット
/// @return > 0: サイズ, 0: EOF, < 0: 失敗 (-errno)
static ssize_t
kafs_read_dirent_inode (struct kafs_context *ctx, kafs_inocnt_t ino_dir,
			struct kafs_sdirent *dirent, size_t direntlen, off_t offset)
{
  assert (ctx != NULL);
  assert (kafs_load_inode_usage (ctx, ino_dir));
  assert (dirent < 0);
  assert (direntlen > sizeof (struct kafs_sdirent));
  ssize_t r = kafs_call (kafs_pread_inode, ctx, ino_dir, dirent,
			 sizeof (struct kafs_sdirent),
			 offset);
  if (r == 0)
    return 0;
  if (r < sizeof (struct kafs_sdirent))
    return -EIO;
  if (direntlen - sizeof (struct kafs_sdirent) < dirent->d_filenamelen)
    return sizeof (struct kafs_sdirent) + dirent->d_filenamelen;
  r =
    kafs_call (kafs_pread_inode, ctx, ino_dir, dirent->d_filename, dirent->d_filenamelen,
	       offset + dirent->d_filenamelen);
  if (r < dirent->d_filenamelen)
    return -EIO;
  return sizeof (struct kafs_sdirent) + dirent->d_filenamelen;
}

/// @brief ディレクトリエントリから対象のファイル名を探す
/// @param ctx コンテキスト
/// @param name ファイル名
/// @param namelen ファイル名の長さ
/// @param ino 対象のディレクトリ
/// @param pino_found 見つかったエントリ
/// @return 0: 成功, < 0: 失敗 (-errno)
static int
kafs_find_dirent_inode (struct kafs_context *ctx, kafs_inocnt_t ino_dir,
			const char *filename, size_t filenamelen, kafs_inocnt_t * pino_found)
{
  assert (ctx != NULL);
  assert (kafs_load_inode_usage (ctx, ino_dir));
  assert (filename != NULL);
  assert (filenamelen > 0);
  const struct kafs_sinode *inotbl = kafs_get_const_inode (ctx, ino_dir);
  if (!S_ISDIR (inotbl->i_mode))
    return -ENOTDIR;
  char buf[sizeof (struct kafs_sdirent) + filenamelen];
  struct kafs_sdirent *dirent = (struct kafs_sdirent *) buf;
  off_t offset = 0;
  while (1)
    {
      ssize_t r = kafs_call (kafs_read_dirent_inode, ctx, ino_dir, dirent, sizeof (struct kafs_sdirent) + filenamelen,
			     offset);
      assert (r == sizeof (struct kafs_sdirent) + filenamelen);
      if (dirent->d_filenamelen == filenamelen && memcmp (filename, dirent->d_filename, filenamelen) == 0)
	{
	  *pino_found = dirent->d_ino;
	  return 0;
	}
      offset += r;
    }
}

static int
kafs_truncate_inode (struct kafs_context *ctx, kafs_inocnt_t ino, off_t size)
{
  assert (ctx != NULL);
  assert (kafs_load_inode_usage (ctx, ino));
  struct kafs_sinode *inotbl = kafs_get_inode (ctx, ino);
  kafs_logblksize_t log_blksize = 10 + ctx->c_superblock->s_log_blksize;
  kafs_blksize_t blksize = 1 << log_blksize;
  kafs_off_t i_size = inotbl->i_size;
  if (i_size == size)
    return 0;
  inotbl->i_size = size;
  if (i_size < size)
    return 0;
  kafs_blksize_t off = i_size & (blksize - 1);
  off_t offset = size;
  if (off > 0)
    {
      char zbuf[blksize];
      memset (zbuf, 0, blksize);
      ssize_t w = kafs_call (kafs_pwrite_inode, ctx, ino, zbuf, blksize - off, offset);
      assert (w == blksize - off);
      offset += blksize - off;
    }
  while (offset < i_size)
    {
      kafs_call (kafs_release_iblk, ctx, inotbl, offset >> log_blksize);
      offset += blksize;
    }
  return KAFS_SUCCESS;
}

static int
kafs_add_dirent_inode (struct kafs_context *ctx, kafs_inocnt_t ino_dir,
		       const char *name, size_t namelen, kafs_inocnt_t ino)
{
  const struct kafs_sinode *inode_dir = kafs_get_const_inode (ctx, ino_dir);
  if (inode_dir == NULL)
    return -ENOENT;
  if (!S_ISDIR (inode_dir->i_mode))
    return -ENOTDIR;
  char buf[sizeof (struct kafs_sdirent) + namelen];
  struct kafs_sdirent *dirent = (struct kafs_sdirent *) buf;
  off_t offset = 0;
  while (1)
    {
      ssize_t r = kafs_call (kafs_pread_inode, ctx, ino_dir, &dirent,
			     sizeof (struct kafs_sdirent),
			     offset);
      if (r < sizeof (struct kafs_sdirent))
	{
	addent:
	  dirent->d_ino = ino;
	  dirent->d_filenamelen = namelen;
	  memcpy (dirent->d_filename, name, namelen);
	  kafs_call (kafs_truncate_inode, ctx, ino_dir, offset);
	  ssize_t w = kafs_call (kafs_pwrite_inode, ctx, ino_dir, dirent,
				 sizeof (struct kafs_sdirent) + namelen, offset);
	  if (w < sizeof (struct kafs_sdirent) + namelen)
	    return -EIO;
	  struct kafs_sinode *inode = kafs_get_inode (ctx, ino);
	  inode->i_links_count++;
	  return 0;
	}
      if (dirent->d_filenamelen == namelen)
	{
	  ssize_t r2 = kafs_call (kafs_pread_inode, ctx, ino_dir, dirent->d_filename, namelen,
				  offset + r);
	  if (r2 < namelen)
	    goto addent;
	  if (memcmp (name, dirent->d_filename, namelen) == 0)
	    return -EEXIST;
	}
      offset += r + dirent->d_filenamelen;
    }
}

static int
kafs_delete_dirent_inode (struct kafs_context *ctx, kafs_inocnt_t ino_dir, const char *name, size_t namelen)
{
  const struct kafs_sinode *inode = kafs_get_const_inode (ctx, ino_dir);
  if (inode == NULL)
    return -ENOENT;
  if (!S_ISDIR (inode->i_mode))
    return -ENOTDIR;
  char buf[sizeof (struct kafs_sdirent) + namelen];
  struct kafs_sdirent *dirent = (struct kafs_sdirent *) buf;
  off_t offset = 0;
  while (1)
    {
      ssize_t r = kafs_call (kafs_pread_inode, ctx, ino_dir, &dirent,
			     sizeof (struct kafs_sdirent),
			     offset);
      if (r < sizeof (struct kafs_sdirent))
	return -ENOENT;
      if (dirent->d_filenamelen == namelen)
	{
	  ssize_t r2 = kafs_call (kafs_pread_inode, ctx, ino_dir, dirent->d_filename, namelen,
				  offset + r);
	  if (r2 < namelen)
	    return -ENOENT;
	  if (memcmp (name, dirent->d_filename, namelen) == 0)
	    while (1)
	      {


	      }
	}
      offset += r + dirent->d_filenamelen;
    }
}

static int
kafs_get_from_path_inode (struct kafs_context *ctx, const char *path, kafs_inocnt_t * pino)
{
  kafs_inocnt_t i = *pino;
  const char *p = path;
  if (*p == '/')
    {
      i = 0;			// ROOT DIR
      path++;
    }
  while (*p)
    {
      char *frag = strchrnul (p, '/');
      kafs_inocnt_t j;
      int ret = kafs_find_dirent_inode (ctx, p, frag - p, i, &j);
      if (ret < 0)
	return ret;
      i = j;
      p = frag;
      if (*p == '/')
	p++;
    }
  *pino = i;
  return 0;
}

static int
kafs_op_getattr (const char *path, struct stat *st, struct fuse_file_info *fi)
{
  struct fuse_context *fctx = fuse_get_context ();
  struct kafs_context *ctx = fctx->private_data;
  kafs_inocnt_t ino = fi->fh;
  struct kafs_sinode *inode = kafs_get_inode (ctx, ino);
  st->st_dev = 0;
  st->st_ino = ino + 1;
  st->st_mode = inode->i_mode;
  st->st_nlink = inode->i_links_count;
  st->st_uid = inode->i_uid;
  st->st_gid = inode->i_gid;
  st->st_rdev = inode->i_rdev;
  st->st_size = inode->i_size;
  st->st_blksize = 1 << (10 + ctx->c_superblock->s_log_blksize);
  st->st_blocks = inode->i_links_count;
  return 0;
}

static int
kafs_op_open (const char *path, struct fuse_file_info *fi)
{
  struct fuse_context *fctx = fuse_get_context ();
  struct kafs_context *ctx = fctx->private_data;
  kafs_inocnt_t ino = 0;
  int ret = kafs_get_from_path_inode (ctx, path, &ino);
  if (ret < 0)
    return ret;
  fi->fh = ino;
  return 0;
}

static int
kafs_op_mknod (const char *path, mode_t mode, dev_t dev)
{
  struct fuse_context *fctx = fuse_get_context ();
  struct kafs_context *ctx = fctx->private_data;
  char dirpath[strlen (path) + 1];
  strcpy (dirpath, path);
  char *filename = strrchr (dirpath, '/');
  if (filename == NULL)
    return -EIO;
  *(filename++) = '\0';
  kafs_inocnt_t ino_dir = 0;
  int ret = kafs_get_from_path_inode (ctx, dirpath, &ino_dir);
  if (ret < 0)
    return -ret;
  struct kafs_sinode *inode_dir = kafs_get_inode (ctx, ino_dir);
  if (!S_ISDIR (inode_dir->i_mode))
    return -EIO;
  kafs_inocnt_t ino;
  ret = kafs_find_free_inode (ctx, &ino);
  if (ret < 0)
    return ret;
  struct kafs_sinode *inode = kafs_get_inode (ctx, ino);
  inode->i_mode = mode;
  inode->i_uid = fctx->uid;
  inode->i_size = 0;
  time_t now = time (NULL);
  inode->i_atime = now;
  inode->i_ctime = now;
  inode->i_mtime = now;
  inode->i_dtime = 0;
  inode->i_gid = fctx->gid;
  inode->i_links_count = 0;
  inode->i_blocks = 0;
  inode->i_rdev = dev;
  memset (inode->i_blkreftbl, 0, sizeof (inode->i_blkreftbl));
  ret = kafs_add_dirent_inode (ctx, ino_dir, filename, ino);
  if (ret < 0)
    {
      int ret2 = kafs_irelease (ctx, ino);
      if (ret2 < 0)
	return -EIO;
      return ret;
    }
  return 0;
}

static int
kafs_op_readlink (const char *path, char *buf, size_t buflen)
{
  struct fuse_context *fctx = fuse_get_context ();
  struct kafs_context *ctx = fctx->private_data;
  kafs_inocnt_t ino = 0;
  int ret = kafs_get_from_path_inode (ctx, path, &ino);
  if (ret < 0)
    return ret;
  ssize_t r = kafs_pread_inode (ctx, ino, buf, buflen - 1, 0);
  if (r < 0)
    return r;
  buf[r] = '\0';
  return 0;
}

static int
kafs_op_read (const char *path, char *buf, size_t size, off_t offset, struct fuse_file_info *fi)
{
  struct fuse_context *fctx = fuse_get_context ();
  struct kafs_context *ctx = fctx->private_data;
  kafs_inocnt_t ino = fi->fh;
  return kafs_pread_inode (ctx, ino, buf, size, offset);
}

static int
kafs_op_write (const char *path, const char *buf, size_t size, off_t offset, struct fuse_file_info *fi)
{
  struct fuse_context *fctx = fuse_get_context ();
  struct kafs_context *ctx = fctx->private_data;
  kafs_inocnt_t ino = fi->fh;
  return kafs_pwrite_inode (ctx, ino, buf, size, offset);
}

static struct fuse_operations kafs_operations = {
  .getattr = kafs_op_getattr,
  .open = kafs_op_open,
  .mknod = kafs_op_mknod,
  .readlink = kafs_op_readlink,
  .read = kafs_op_read,
  .write = kafs_op_write,
};
