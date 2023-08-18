#define FUSE_USE_VERSION 30

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include <fuse.h>
#include <errno.h>
#include <string.h>
#include <unistd.h>
#include <assert.h>

#define kafs_call(func, ...) ({ \
  int _ret = (func) (__VA_ARGS__); \
  if (_ret < 0) \
    return _ret; \
  _ret; \
})

#define kafs_iocall(func, ...) ({ \
  __auto_type _ret = (func) (__VA_ARGS__); \
  if (_ret < 0) \
    return -errno; \
  _ret; \
})

typedef unsigned int kafs_blkmask_t;
typedef uint32_t kafs_blkcnt_t;
typedef uint32_t kafs_inocnt_t;
typedef uint16_t kafs_uid_t;
typedef uint64_t kafs_off_t;
typedef struct timespec kafs_time_t;
typedef uint16_t kafs_gid_t;
typedef uint16_t kafs_linkcnt_t;
typedef uint32_t kafs_blksize_t;
typedef uint16_t kafs_logblksize_t;
typedef uint16_t kafs_mntcnt_t;
typedef uint16_t kafs_mode_t;
typedef uint16_t kafs_dev_t;
typedef uint32_t kafs_iblkcnt_t;
typedef uint16_t kafs_filenamelen_t;

#define KAFS_SUCCESS 0
#define KAFS_INO_NONE 0
#define KAFS_BLO_NONE 0

/// @brief スーパーブロック情報
struct kafs_superblock_layout
{
  /// @brief inode 番号の数
  kafs_inocnt_t s_inocnt;
  /// @brief block 番号の数(一般ユーザー)
  kafs_blkcnt_t s_blkcnt;
  /// @brief block 番号の数(ルートユーザー)
  kafs_blkcnt_t s_r_blkcnt;
  /// @brief 空き block 番号の数
  kafs_blkcnt_t s_blkcnt_free;
  /// @brief 空き inode 番号の数
  kafs_inocnt_t s_inocnt_free;
  /// @brief 最初のデータのブロック番号
  kafs_blkcnt_t s_first_data_block;
  /// @brief ブロックサイズ(ただし、サイズ=2^(10 + s_log_block_size))
  kafs_logblksize_t s_log_blksize;
  /// @brief マウント日時
  kafs_time_t s_mtime;
  /// @brief 書き込み日時
  kafs_time_t s_wtime;
  /// @brief マウント数
  kafs_mntcnt_t s_mnt_count;
};

/// @brief inode 情報
struct kafs_inode_layout
{
  /// @brief モード
  kafs_mode_t i_mode;
  /// @brief UID
  kafs_uid_t i_uid;
  /// @brief ファイルサイズ
  kafs_off_t i_size;
  /// @brief アクセスタイム
  kafs_time_t i_atime;
  /// @brief inode の更新時間
  kafs_time_t i_ctime;
  /// @brief データ の更新時間
  kafs_time_t i_mtime;
  /// @brief 削除時間
  kafs_time_t i_dtime;
  /// @brief GID
  kafs_gid_t i_gid;
  /// @brief リンク数
  kafs_linkcnt_t i_links_count;
  /// @brief ブロック数
  kafs_blkcnt_t i_blocks;
  /// @brief デバイス番号
  kafs_dev_t i_rdev;
  /// @brief ブロックデータ
  kafs_blkcnt_t i_blkreftbl[15];
};

/// @brief コンテキスト
struct kafs_context
{
  /// @brief スーパーブロック情報
  struct kafs_superblock_layout *c_superblock;
  /// @brief inode 情報
  struct kafs_inode_layout *c_inotbl;
  /// @brief ブロックマスク
  kafs_blkmask_t *c_blkmasktbl;
  /// @brief 前回の inode 検索情報
  kafs_inocnt_t c_ino_search;
  /// @brief 前回のブロック検索情報
  kafs_blkcnt_t c_blo_search;
  /// @brief ファイル記述子
  int c_fd;
};

/// @brief ディレクトリエントリ
struct kafs_dirent_layout
{
  /// @brief inode 番号
  kafs_inocnt_t d_ino;
  /// @brief ファイル名の長さ
  kafs_filenamelen_t d_filenamelen;
  /// @brief ファイル名
  char d_filename[0];
};

/// ブロックマスクのビット数
#define KAFS_BLKMASK_BITS (sizeof(kafs_blkmask_t) << 3)
/// ブロックマスクのビット数の log2 の値
#define KAFS_BLKMASK_LOG_BITS (sizeof(kafs_blkmask_t) + 3)
/// ブロックマスクのビット数のマスク値
#define KAFS_BLKMASK_MASK_BITS (KAFS_BLKMASK_BITS - 1)

/// @brief inode 構造体の参照を取得
/// @param ctx コンテキスト
/// @param ino inode 番号
/// @return inode 構造体の参照
static struct kafs_inode_layout *
kafs_get_inode (struct kafs_context *ctx, kafs_inocnt_t ino)
{
  assert (ctx != NULL);
  assert (ino < ctx->c_superblock->s_inocnt);
  return ctx->c_inotbl + ino;
}

/// @brief inode 構造体の参照を取得
/// @param ctx コンテキスト
/// @param ino inode 番号
/// @return inode 構造体の参照
static const struct kafs_inode_layout *
kafs_get_const_inode (const struct kafs_context *ctx, kafs_inocnt_t ino)
{
  assert (ctx != NULL);
  assert (ino < ctx->c_superblock->s_inocnt);
  return ctx->c_inotbl + ino;
}

/// @brief inode 番号の利用状況を返す
/// @param ctx コンテキスト
/// @param ino inode 番号
/// @return 0: 未使用, != 0: 使用中
static int
kafs_get_usage_inode (const struct kafs_context *ctx, kafs_inocnt_t ino)
{
  assert (ctx != NULL);
  assert (ino < ctx->c_superblock->s_inocnt);
  return ctx->c_inotbl[ino].i_mode != 0;
}

/// @brief 未使用の inode 番号を見つける
/// @param ctx コンテキスト
/// @param pino 見つかった inode 番号
/// @return 0: 成功、 < 0: 失敗 (-errno)
static int
kafs_find_free_inode (struct kafs_context *ctx, kafs_inocnt_t * pino)
{
  assert (ctx != NULL);
  assert (pino != NULL);
  if (ctx->c_superblock->s_inocnt_free == 0)
    return -ENOSPC;
  kafs_inocnt_t inocnt = ctx->c_superblock->s_inocnt;
  kafs_inocnt_t ino_search = ctx->c_ino_search;
  kafs_inocnt_t ino = ino_search + 1;
  struct kafs_inode_layout *inotbl = ctx->c_inotbl;
  while (ino_search != ino)
    {
      if (ino >= inocnt)
	ino = 0;
      if (!kafs_get_usage_inode (ctx, ino))
	{
	  ctx->c_ino_search = ino;
	  *pino = ino;
	  return KAFS_SUCCESS;
	}
      ino++;
    }
  return -ENOSPC;
}

/// @brief 指定されたブロック番号の利用状況を取得する
/// @param ctx コンテキスト
/// @param blo ブロック番号
/// @return 0: 未使用, != 0: 使用中
static int
kafs_get_blkmask (const struct kafs_context *ctx, kafs_blkcnt_t blo)
{
  assert (ctx != NULL);
  assert (blo < ctx->c_superblock->s_blkcnt);
  kafs_blkcnt_t blod = blo >> KAFS_BLKMASK_LOG_BITS;
  kafs_blkcnt_t blor = blo & KAFS_BLKMASK_MASK_BITS;
  return (ctx->c_blkmasktbl[blod] & (1 << blor)) != 0;
}

/// @brief 指定されたブロックの利用状況をセットする
/// @param ctx コンテキスト
/// @param blo ブロック番号
/// @param val 0: フラグをクリア, != 0: フラグをセット
/// @return 0: 成功, < 0: 失敗 (-errno)
static int
kafs_set_blkmask (struct kafs_context *ctx, kafs_blkcnt_t blo, int val)
{
  assert (ctx != NULL);
  assert (blo < ctx->c_superblock->s_blkcnt);
  kafs_blkcnt_t blod = blo >> KAFS_BLKMASK_LOG_BITS;
  kafs_blkcnt_t blor = blo & KAFS_BLKMASK_MASK_BITS;
  if (val)
    {
      assert (!kafs_get_blkmask (ctx, blo));
      ctx->c_blkmasktbl[blod] |= 1 << blor;
      assert (ctx->c_superblock->s_blkcnt_free > 0);
      ctx->c_superblock->s_blkcnt_free--;
      kafs_iocall (clock_settime, CLOCK_REALTIME, &ctx->c_superblock->s_wtime);
    }
  else
    {
      assert (kafs_get_blkmask (ctx, blo));
      ctx->c_blkmasktbl[blod] &= ~(1 << blor);
      assert (ctx->c_superblock->s_blkcnt_free < ctx->c_superblock->s_r_blkcnt - 1);
      ctx->c_superblock->s_blkcnt_free++;
      kafs_iocall (clock_settime, CLOCK_REALTIME, &ctx->c_superblock->s_wtime);
    }
  return KAFS_SUCCESS;
}

static kafs_blkcnt_t
kafs_get_free_blkmask (kafs_blkmask_t bm)
{
  if (sizeof (kafs_blkmask_t) <= sizeof (unsigned int))
    return __builtin_ctz (bm);
  if (sizeof (kafs_blkmask_t) <= sizeof (unsigned long))
    return __builtin_ctzl (bm);
  return __builtin_ctzll (bm);
}

/// @brief 未使用のブロック番号を取得し、使用中フラグをつける
/// @param ctx コンテキスト
/// @param pblo ブロック番号
/// @return 0: 成功, < 0: 失敗 (-errno)
static int
kafs_alloc_blk (struct kafs_context *ctx, kafs_blkcnt_t * pblo)
{
  assert (ctx != NULL);
  assert (pblo != NULL);
  kafs_blkcnt_t blo_search = ctx->c_blo_search;
  kafs_blkcnt_t blo = blo_search + 1;
  kafs_blkmask_t *blkmasktbl = ctx->c_blkmasktbl;
  kafs_blkcnt_t blocnt = ctx->c_superblock->s_blkcnt;
  while (blo_search != blo)
    {
      if (blo >= blocnt)
	blo = 0;
      kafs_blkcnt_t blod = blo >> KAFS_BLKMASK_LOG_BITS;
      kafs_blkcnt_t blor = blo & KAFS_BLKMASK_MASK_BITS;	// ToDo: 2周目以降は常に0
      kafs_blkmask_t blkmask = ~blkmasktbl[blod];
      if (blkmask != 0)
	{
	  kafs_blkcnt_t blor_found = kafs_get_free_blkmask (blkmask);	// TODO: エラー処理
	  kafs_blkcnt_t blo_found = (blod << KAFS_BLKMASK_LOG_BITS) + blor_found;
	  if (blo_found < blocnt)
	    {
	      ctx->c_blo_search = blo_found;
	      *pblo = blo_found;
	      kafs_call (kafs_set_blkmask, ctx, blo_found, 1);
	      return KAFS_SUCCESS;
	    }
	}
      blo += KAFS_BLKMASK_BITS - blor;
    }
  return -ENOSPC;
}

/// @brief ブロックデータを削除する
/// @param ctx コンテキスト
/// @param pblo ブロック番号へのポインタ
/// @return 0: 成功, < 0: 失敗 (-errno)
static int
kafs_release_blk (struct kafs_context *ctx, kafs_blkcnt_t * pblo)
{
  assert (ctx != NULL);
  assert (pblo != NULL);
  assert (*pblo != KAFS_BLO_NONE);
  assert (*pblo < ctx->c_superblock->s_r_blkcnt);
  assert (kafs_get_blkmask (ctx, *pblo));

  kafs_call (kafs_set_blkmask, ctx, *pblo, 0);
  kafs_logblksize_t log_blksize = 10 + ctx->c_superblock->s_log_blksize;
  kafs_blksize_t blksize = 1 << log_blksize;
  char zbuf[blksize];
  memset (zbuf, 0, blksize);
  ssize_t w = kafs_iocall (pwrite, ctx->c_fd, zbuf, blksize, blksize * *pblo);
  assert (w == blksize);
  *pblo = KAFS_BLO_NONE;
  return KAFS_SUCCESS;
}

/// @brief ブロック単位でデータを読み出す
/// @param ctx コンテキスト
/// @param blo ブロック番号
/// @param buf 読み出すバッファ
/// @return 0: 成功, < 0: 失敗 (-errno)
static int
kafs_read_blk (struct kafs_context *ctx, kafs_blkcnt_t blo, void *buf)
{
  assert (ctx != NULL);
  assert (buf != NULL);
  assert (blo < ctx->c_superblock->s_r_blkcnt);
  assert (kafs_get_blkmask (ctx, blo));
  kafs_logblksize_t log_blksize = 10 + ctx->c_superblock->s_log_blksize;
  kafs_blksize_t blksize = 1 << log_blksize;
  if (blo == KAFS_BLO_NONE)
    {
      memset (buf, 0, blksize);
      return KAFS_SUCCESS;
    }
  ssize_t r = kafs_iocall (pread, ctx->c_fd, buf, blksize, blksize * blo);
  assert (r == blksize);
  return KAFS_SUCCESS;
}

/// @brief ブロック単位でデータを読み出す
/// @param ctx コンテキスト
/// @param pblo ブロック番号へのポインタ、0の場合はブロックを確保し、その値が入る
/// @param buf 読み出すバッファ
/// @return 0: 成功, < 0: 失敗 (-errno)
static int
kafs_reada_blk (struct kafs_context *ctx, kafs_blkcnt_t * pblo, void *buf)
{
  assert (ctx != NULL);
  assert (buf != NULL);
  assert (pblo != NULL);
  assert (*pblo < ctx->c_superblock->s_r_blkcnt);
  kafs_logblksize_t log_blksize = 10 + ctx->c_superblock->s_log_blksize;
  kafs_blksize_t blksize = 1 << log_blksize;
  if (*pblo == KAFS_BLO_NONE)
    {
      kafs_call (kafs_alloc_blk, ctx, pblo);
      memset (buf, 0, blksize);
      return KAFS_SUCCESS;
    }
  ssize_t r = kafs_iocall (pread, ctx->c_fd, buf, blksize, blksize * *pblo);
  assert (r == blksize);
  return KAFS_SUCCESS;
}

/// @brief ブロック単位でデータを書き込む
/// @param ctx コンテキスト
/// @param pblo ブロック番号へのポインタ、0の場合はブロックを確保し、その値が入る
/// @param buf 書き込むバッファ
/// @return 0: 成功, < 0: 失敗 (-errno)
static int
kafs_writea_blk (struct kafs_context *ctx, kafs_blkcnt_t * pblo, const void *buf)
{
  assert (ctx != NULL);
  assert (buf != NULL);
  assert (pblo != NULL);
  assert (*pblo < ctx->c_superblock->s_r_blkcnt);
  kafs_logblksize_t log_blksize = 10 + ctx->c_superblock->s_log_blksize;
  kafs_blksize_t blksize = 1 << log_blksize;
  if (*pblo == KAFS_BLO_NONE)
    kafs_call (kafs_alloc_blk, ctx, pblo);
  ssize_t w = kafs_iocall (pwrite, ctx->c_fd, buf, blksize, blksize * *pblo);
  assert (w == blksize);
  return KAFS_SUCCESS;
}

/// @brief inode毎のデータを読み出す（ブロック単位）
/// @param ctx コンテキスト
/// @param inotbl inode情報
/// @param buf バッファ
/// @param iblo ブロック番号
/// @return 0: 成功, < 0: 失敗 (-errno)
static int
kafs_read_iblk (struct kafs_context *ctx, struct kafs_inode_layout *inotbl, kafs_iblkcnt_t iblo, void *buf)
{
  assert (ctx != NULL);
  assert (inotbl != NULL);
  assert (buf != NULL);
  kafs_logblksize_t log_blksize = 10 + ctx->c_superblock->s_log_blksize;
  kafs_blksize_t blksize = 1 << log_blksize;

  // 0..11 は 直接
  if (iblo < 12)
    {
      kafs_call (kafs_read_blk, ctx, inotbl->i_blkreftbl[iblo], buf);
      return KAFS_SUCCESS;
    }
  iblo -= 12;
  // 12 .. ブロックサイズ / 4 + 11 は、間接（１段階） 
  kafs_logblksize_t log_blkrefs_pb = log_blksize - (sizeof (*inotbl->i_blkreftbl) + 3);
  kafs_blksize_t blkrefs_pb = 1 << log_blkrefs_pb;
  kafs_blkcnt_t blkreftbl[blkrefs_pb];
  if (iblo < blkrefs_pb)
    {
      kafs_call (kafs_read_blk, ctx, inotbl->i_blkreftbl[12], blkreftbl);
      kafs_call (kafs_read_blk, ctx, blkreftbl[iblo], buf);
      return KAFS_SUCCESS;
    }

  // ブロックサイズ/4 + 12  .. (ブロックサイズ/4)^2 + ブロックサイズ / 4 + 11 は、間接（２段階） 
  iblo -= blkrefs_pb;
  kafs_blkcnt_t blkrefs_pb_sq = 1 << (log_blkrefs_pb * 2);
  if (iblo < blkrefs_pb_sq)
    {
      kafs_call (kafs_read_blk, ctx, inotbl->i_blkreftbl[13], blkreftbl);
      kafs_call (kafs_read_blk, ctx, blkreftbl[iblo >> log_blkrefs_pb], blkreftbl);
      kafs_call (kafs_read_blk, ctx, blkreftbl[iblo & (blkrefs_pb - 1)], blkreftbl);
      return KAFS_SUCCESS;
    }

  // (ブロックサイズ/4)^2 + ブロックサイズ / 4 + 12 .. Inf は、間接（３段階） 
  iblo -= blkrefs_pb_sq;
  kafs_iblkcnt_t iblo2 = iblo >> log_blkrefs_pb;
  kafs_call (kafs_read_blk, ctx, inotbl->i_blkreftbl[14], blkreftbl);
  kafs_call (kafs_read_blk, ctx, blkreftbl[iblo2 >> log_blkrefs_pb], blkreftbl);
  kafs_call (kafs_read_blk, ctx, blkreftbl[iblo2 & (blkrefs_pb - 1)], blkreftbl);
  kafs_call (kafs_read_blk, ctx, blkreftbl[iblo & (blkrefs_pb - 1)], blkreftbl);
  return KAFS_SUCCESS;
}

/// @brief inode毎のデータを書き込む（ブロック単位）
/// @param ctx コンテキスト
/// @param inotbl inode情報
/// @param buf バッファ
/// @param iblo ブロック番号
/// @return 0: 成功, < 0: 失敗 (-errno)
static int
kafs_write_iblk (struct kafs_context *ctx, struct kafs_inode_layout *inotbl, kafs_iblkcnt_t iblo, const void *buf)
{
  assert (ctx != NULL);
  assert (inotbl != NULL);
  assert (buf != NULL);
  kafs_logblksize_t log_blksize = 10 + ctx->c_superblock->s_log_blksize;
  kafs_blkcnt_t blksize = 1 << log_blksize;

  // 0..11 は 直接
  if (iblo < 12)
    {
      kafs_call (kafs_writea_blk, ctx, inotbl->i_blkreftbl + iblo, buf);
      return KAFS_SUCCESS;
    }

  // 12 .. ブロックサイズ / 4 + 11 は、間接（１段階） 
  iblo -= 12;
  kafs_logblksize_t log_blkrefs_pb = log_blksize - (sizeof (*inotbl->i_blkreftbl) + 3);
  kafs_blkcnt_t blkrefs_pb = 1 << log_blkrefs_pb;
  kafs_blkcnt_t blkreftbl[blkrefs_pb];
  if (iblo < blkrefs_pb)
    {
      kafs_call (kafs_reada_blk, ctx, inotbl->i_blkreftbl + 12, blkreftbl);
      kafs_blkcnt_t blo2 = blkreftbl[iblo];
      kafs_call (kafs_writea_blk, ctx, blkreftbl + iblo, buf);
      if (blo2 == KAFS_BLO_NONE)
	kafs_call (kafs_writea_blk, ctx, inotbl->i_blkreftbl + 12, blkreftbl);
      return KAFS_SUCCESS;
    }

  // ブロックサイズ/4 + 12  .. (ブロックサイズ/4)^2 + ブロックサイズ / 4 + 11 は、間接（２段階） 
  iblo -= blkrefs_pb;
  kafs_blkcnt_t blkrefs_pb_sq = 1 << (log_blkrefs_pb * 2);
  kafs_blkcnt_t blkreftbl2[blkrefs_pb];
  if (iblo < blkrefs_pb_sq)
    {
      kafs_call (kafs_reada_blk, ctx, inotbl->i_blkreftbl + 13, blkreftbl);
      kafs_blkcnt_t blo2 = blkreftbl[iblo >> log_blkrefs_pb];
      kafs_call (kafs_reada_blk, ctx, blkreftbl + (iblo >> log_blkrefs_pb), blkreftbl2);
      if (blo2 == KAFS_BLO_NONE)
	kafs_call (kafs_writea_blk, ctx, inotbl->i_blkreftbl + 13, blkreftbl);
      blo2 = blkreftbl2[iblo & (blkrefs_pb - 1)];
      kafs_call (kafs_writea_blk, ctx, blkreftbl2 + (iblo & (blkrefs_pb - 1)), buf);
      if (blo2 == KAFS_BLO_NONE)
	kafs_call (kafs_writea_blk, ctx, blkreftbl + (iblo >> log_blkrefs_pb), blkreftbl2);
      return KAFS_SUCCESS;
    }

  // (ブロックサイズ/4)^2 + ブロックサイズ / 4 + 12 .. Inf は、間接（３段階） 
  iblo -= blkrefs_pb_sq;
  kafs_iblkcnt_t iblo2 = iblo >> log_blkrefs_pb;

  kafs_call (kafs_reada_blk, ctx, inotbl->i_blkreftbl + 14, blkreftbl);
  kafs_blkcnt_t blo2 = blkreftbl[iblo2 >> log_blkrefs_pb];
  kafs_call (kafs_reada_blk, ctx, blkreftbl + (iblo2 >> log_blkrefs_pb), blkreftbl2);
  if (blo2 == KAFS_BLO_NONE)
    kafs_call (kafs_writea_blk, ctx, inotbl->i_blkreftbl + 14, blkreftbl);
  blo2 = blkreftbl2[iblo2 & (blkrefs_pb - 1)];
  kafs_call (kafs_reada_blk, ctx, blkreftbl2 + (iblo2 & (blkrefs_pb - 1)), blkreftbl);
  if (blo2 == KAFS_BLO_NONE)
    kafs_call (kafs_writea_blk, ctx, blkreftbl + (iblo2 >> log_blkrefs_pb), blkreftbl2);

  blo2 = blkreftbl[iblo & (blkrefs_pb - 1)];
  kafs_call (kafs_reada_blk, ctx, blkreftbl + (iblo & (blkrefs_pb - 1)), blkreftbl2);
  if (blo2 == KAFS_BLO_NONE)
    kafs_call (kafs_writea_blk, ctx, blkreftbl2 + (iblo2 & (blkrefs_pb - 1)), blkreftbl);
  return KAFS_SUCCESS;
}

static int
kafs_release_iblk2 (struct kafs_context *ctx, kafs_blkcnt_t * blkreftbl, kafs_blkcnt_t * pblo)
{
  assert (ctx != NULL);
  assert (blkreftbl != NULL);
  assert (pblo != NULL);
  assert (*pblo != KAFS_BLO_NONE);
  assert (*pblo < ctx->c_superblock->s_blkcnt);

  kafs_logblksize_t log_blksize = 10 + ctx->c_superblock->s_log_blksize;
  kafs_blkcnt_t blksize = 1 << log_blksize;
  kafs_logblksize_t log_blkrefs_pb = log_blksize - (sizeof (kafs_blkcnt_t) + 3);
  kafs_blksize_t blkrefs_pb = 1 << log_blkrefs_pb;

  for (kafs_blksize_t i = 0; i < blkrefs_pb; i++)
    if (blkreftbl[i] != KAFS_BLO_NONE)
      return KAFS_SUCCESS;
  kafs_call (kafs_release_blk, ctx, pblo);
  return KAFS_SUCCESS;
}

#define kafs_get_blkcnt(p, i) ({ \
  __auto_type _p = (p) + (i); \
  if (*_p == KAFS_BLO_NONE) \
    return KAFS_SUCCESS; \
  _p; \
})

static int
kafs_release_iblk (struct kafs_context *ctx, struct kafs_inode_layout *inotbl, kafs_iblkcnt_t iblo)
{
  assert (ctx != NULL);
  assert (inotbl != NULL);
  kafs_logblksize_t log_blksize = 10 + ctx->c_superblock->s_log_blksize;
  kafs_blkcnt_t blksize = 1 << log_blksize;

  // 0..11 は 直接
  if (iblo < 12)
    {
      kafs_blkcnt_t *pblo = kafs_get_blkcnt (inotbl->i_blkreftbl, iblo);
      kafs_call (kafs_release_blk, ctx, pblo);
      return KAFS_SUCCESS;
    }

  // 12 .. ブロックサイズ / 4 + 11 は、間接（１段階） 
  iblo -= 12;
  kafs_logblksize_t log_blkrefs_pb = log_blksize - (sizeof (*inotbl->i_blkreftbl) + 3);
  kafs_blksize_t blkrefs_pb = 1 << log_blkrefs_pb;
  kafs_blkcnt_t blkreftbl[blkrefs_pb];
  if (iblo < blkrefs_pb)
    {
      kafs_blkcnt_t *pblo = kafs_get_blkcnt (inotbl->i_blkreftbl, 12);
      kafs_call (kafs_read_blk, ctx, *pblo, blkreftbl);
      kafs_blkcnt_t *pblo2 = kafs_get_blkcnt (blkreftbl, iblo);
      kafs_call (kafs_release_blk, ctx, pblo2);
      kafs_call (kafs_release_iblk2, ctx, blkreftbl, pblo);
      return KAFS_SUCCESS;
    }

  // ブロックサイズ/4 + 12  .. (ブロックサイズ/4)^2 + ブロックサイズ / 4 + 11 は、間接（２段階） 
  iblo -= blkrefs_pb;
  kafs_blkcnt_t blkrefs_pb_sq = 1 << (log_blkrefs_pb * 2);
  kafs_blkcnt_t blkreftbl2[blkrefs_pb];
  if (iblo < blkrefs_pb_sq)
    {
      kafs_blkcnt_t *pblo = kafs_get_blkcnt (inotbl->i_blkreftbl, 13);
      kafs_call (kafs_read_blk, ctx, *pblo, blkreftbl);
      kafs_blkcnt_t bloi = iblo >> log_blkrefs_pb;
      kafs_blkcnt_t *pblo2 = kafs_get_blkcnt (blkreftbl, bloi);
      kafs_call (kafs_read_blk, ctx, *pblo2, blkreftbl2);
      kafs_blkcnt_t bloi2 = iblo & (blkrefs_pb - 1);
      kafs_blkcnt_t *pblo3 = kafs_get_blkcnt (blkreftbl2, bloi2);
      kafs_call (kafs_release_blk, ctx, pblo3);
      kafs_call (kafs_release_iblk2, ctx, blkreftbl2, pblo2);
      kafs_call (kafs_release_iblk2, ctx, blkreftbl, pblo);
      return KAFS_SUCCESS;
    }

  // (ブロックサイズ/4)^2 + ブロックサイズ / 4 + 12 .. Inf は、間接（３段階） 
  iblo -= blkrefs_pb_sq;
  kafs_call (kafs_reada_blk, ctx, inotbl->i_blkreftbl + 14, blkreftbl);
  kafs_iblkcnt_t iblo2 = iblo >> log_blkrefs_pb;
  kafs_blkcnt_t blo2 = blkreftbl[iblo2 >> log_blkrefs_pb];
  kafs_call (kafs_reada_blk, ctx, blkreftbl + (iblo2 >> log_blkrefs_pb), blkreftbl2);
  if (blo2 == KAFS_BLO_NONE)
    {
      ret = kafs_writea_blk (ctx, inotbl->i_blkreftbl + 14, blkreftbl);
      if (ret < 0)
	return ret;
    }

  blo2 = blkreftbl2[iblo2 & (blkrefs_pb - 1)];
  ret = kafs_reada_blk (ctx, blkreftbl2 + (iblo2 & (blkrefs_pb - 1)), blkreftbl);
  if (ret < 0)
    return ret;
  if (blo2 == KAFS_BLO_NONE)
    return kafs_writea_blk (ctx, blkreftbl + (iblo2 >> log_blkrefs_pb), blkreftbl2);

  blo2 = blkreftbl[iblo & (blkrefs_pb - 1)];
  ret = kafs_reada_blk (ctx, blkreftbl + (iblo & (blkrefs_pb - 1)), blkreftbl2);
  if (ret < 0)
    return ret;
  if (blo2 == KAFS_BLO_NONE)
    return kafs_writea_blk (ctx, blkreftbl2 + (iblo2 & (blkrefs_pb - 1)), blkreftbl);
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

  struct kafs_inode_layout *inotbl = ctx->c_inotbl + ino;
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
      int ret = kafs_read_iblk (ctx, inotbl, iblo, rbuf);
      if (ret < 0)
	return ret;
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
	  int ret = kafs_read_iblk (ctx, inotbl, iblo, rbuf);
	  if (ret < 0)
	    return ret;
	  memcpy (buf + size_read, rbuf, size - size_read);
	  return size;
	}
      int ret = kafs_read_iblk (ctx, inotbl, iblo, buf + size_read);
      if (ret < 0)
	return ret;
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

  struct kafs_inode_layout *inotbl = ctx->c_inotbl + ino;
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
	  int ret = kafs_write_iblk (ctx, inotbl, 0, wbuf);
	  if (ret < 0)
	    return ret;
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
      int ret = kafs_read_iblk (ctx, inotbl, iblo, wbuf);
      if (ret < 0)
	return ret;
      if (size < blksize - offset_blksize)
	{
	  // 1ブロックのみの場合
	  memcpy (wbuf + offset_blksize, buf, size);
	  ret = kafs_write_iblk (ctx, inotbl, iblo, wbuf);
	  if (ret < 0)
	    return ret;
	  return size;
	}
      // ブロックの残り分を書き込む
      memcpy (wbuf + offset_blksize, buf, blksize - offset_blksize);
      ret = kafs_write_iblk (ctx, inotbl, iblo, wbuf);
      if (ret < 0)
	return ret;
      size_written += blksize - offset_blksize;
    }

  while (size_written < size)
    {
      kafs_iblkcnt_t iblo = (offset + size_written) >> log_blksize;
      if (size - size_written < blksize)
	{
	  char wbuf[blksize];
	  int ret = kafs_read_iblk (ctx, inotbl, iblo, wbuf);
	  if (ret < 0)
	    return ret;
	  memcpy (wbuf, buf + size_written, size - size_written);
	  ret = kafs_write_iblk (ctx, inotbl, iblo, wbuf);
	  if (ret < 0)
	    return ret;
	  return size;
	}
      int ret = kafs_write_iblk (ctx, inotbl, iblo, buf + size_written);
      if (ret < 0)
	return ret;
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
			struct kafs_dirent_layout *dirent, size_t direntlen, off_t offset)
{
  assert (ctx != NULL);
  assert (kafs_get_usage_inode (ctx, ino_dir));
  assert (dirent < 0);
  assert (direntlen > sizeof (struct kafs_dirent_layout));
  int r = kafs_pread_inode (ctx, ino_dir, dirent,
			    sizeof (struct kafs_dirent_layout),
			    offset);
  if (r < 0)
    return r;
  if (r == 0)
    return 0;
  if (r < sizeof (struct kafs_dirent_layout))
    return -EIO;
  if (direntlen - sizeof (struct kafs_dirent_layout) < dirent->d_filenamelen)
    return sizeof (struct kafs_dirent_layout) + dirent->d_filenamelen;
  r = kafs_pread_inode (ctx, ino_dir, dirent->d_filename, dirent->d_filenamelen, offset + dirent->d_filenamelen);
  if (r < 0)
    return r;
  if (r < dirent->d_filenamelen)
    return -EIO;
  return sizeof (struct kafs_dirent_layout) + dirent->d_filenamelen;
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
  assert (kafs_get_usage_inode (ctx, ino_dir));
  assert (filename != NULL);
  assert (filenamelen > 0);
  const struct kafs_inode_layout *inotbl = kafs_get_const_inode (ctx, ino_dir);
  if (!S_ISDIR (inotbl->i_mode))
    return -ENOTDIR;
  char buf[sizeof (struct kafs_dirent_layout) + filenamelen];
  struct kafs_dirent_layout *dirent = (struct kafs_dirent_layout *) buf;
  off_t offset = 0;
  while (1)
    {
      ssize_t r = kafs_read_dirent_inode (ctx, ino_dir, dirent,
					  sizeof (struct kafs_dirent_layout) + filenamelen,
					  offset);
      if (r < 0)
	return r;
      assert (r == sizeof (struct kafs_dirent_layout) + filenamelen);
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
  assert (kafs_get_usage_inode (ctx, ino));
  struct kafs_inode_layout *inotbl = kafs_get_inode (ctx, ino);
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
      ssize_t w = kafs_pwrite_inode (ctx, ino, zbuf, blksize - off, offset);
      if (w < 0)
	return w;
      assert (w == blksize - off);
      offset += blksize - off;
    }
  while (offset < i_size)
    {
      int ret = kafs_release_iblk (ctx, ino, offset >> log_blksize);
      if (ret < 0)
	return ret;
      offset += blksize;
    }
  return KAFS_SUCCESS;
}

static int
kafs_add_dirent_inode (struct kafs_context *ctx, kafs_inocnt_t ino_dir,
		       const char *name, size_t namelen, kafs_inocnt_t ino)
{
  const struct kafs_inode_layout *inode_dir = kafs_get_const_inode (ctx, ino_dir);
  if (inode_dir == NULL)
    return -ENOENT;
  if (!S_ISDIR (inode_dir->i_mode))
    return -ENOTDIR;
  char buf[sizeof (struct kafs_dirent_layout) + namelen];
  struct kafs_dirent_layout *dirent = (struct kafs_dirent_layout *) buf;
  off_t offset = 0;
  while (1)
    {
      ssize_t r = kafs_pread_inode (ctx, ino_dir, &dirent,
				    sizeof (struct kafs_dirent_layout),
				    offset);
      if (r < 0)
	return r;
      if (r < sizeof (struct kafs_dirent_layout))
	{
	addent:
	  dirent->d_ino = ino;
	  dirent->d_filenamelen = namelen;
	  memcpy (dirent->d_filename, name, namelen);
	  int ret = kafs_truncate_inode (ctx, ino_dir, offset);
	  ssize_t w = kafs_pwrite_inode (ctx, ino_dir, dirent,
					 sizeof (struct kafs_dirent_layout) + namelen, offset);
	  if (w < 0)
	    return w;
	  if (w < sizeof (struct kafs_dirent_layout) + namelen)
	    return -EIO;
	  struct kafs_inode_layout *inode = kafs_get_inode (ctx, ino);
	  inode->i_links_count++;
	  return 0;
	}
      if (dirent->d_filenamelen == namelen)
	{
	  ssize_t r2 = kafs_pread_inode (ctx, ino_dir, dirent->d_filename, namelen,
					 offset + r);
	  if (r2 < 0)
	    return r2;
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
  const struct kafs_inode_layout *inode = kafs_get_const_inode (ctx, ino_dir);
  if (inode == NULL)
    return -ENOENT;
  if (!S_ISDIR (inode->i_mode))
    return -ENOTDIR;
  char buf[sizeof (struct kafs_dirent_layout) + namelen];
  struct kafs_dirent_layout *dirent = (struct kafs_dirent_layout *) buf;
  off_t offset = 0;
  while (1)
    {
      ssize_t r = kafs_pread_inode (ctx, ino_dir, &dirent,
				    sizeof (struct kafs_dirent_layout),
				    offset);
      if (r < 0)
	return r;
      if (r < sizeof (struct kafs_dirent_layout))
	return -ENOENT;
      if (dirent->d_filenamelen == namelen)
	{
	  ssize_t r2 = kafs_pread_inode (ctx, ino_dir, dirent->d_filename, namelen,
					 offset + r);
	  if (r2 < 0)
	    return r2;
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
  struct kafs_inode_layout *inode = kafs_get_inode (ctx, ino);
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
  struct kafs_inode_layout *inode_dir = kafs_get_inode (ctx, ino_dir);
  if (!S_ISDIR (inode_dir->i_mode))
    return -EIO;
  kafs_inocnt_t ino;
  ret = kafs_find_free_inode (ctx, &ino);
  if (ret < 0)
    return ret;
  struct kafs_inode_layout *inode = kafs_get_inode (ctx, ino);
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
