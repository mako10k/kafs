#include "kafs.h"
#include "kafs_context.h"
#include "kafs_superblock.h"
#include "kafs_block.h"
#include "kafs_inode.h"
#include "kafs_dirent.h"
#include "kafs_hash.h"
#include "kafs_journal.h"
#include "kafs_ioctl.h"
#include "kafs_mmap_io.h"

#define KAFS_DIRECT_SIZE (sizeof(((struct kafs_sinode *)NULL)->i_blkreftbl))

#include <fuse.h>
#include <errno.h>
#include <string.h>
#include <unistd.h>
#include <assert.h>
#include <endian.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/mman.h>
#include <sys/statvfs.h>
#include <stdlib.h>
#include <stddef.h>
#include <signal.h>
#ifdef __linux__
#include <execinfo.h>
#endif

#ifdef DEBUG
static void *memset_orig(void *d, int x, size_t l) { return memset(d, x, l); }

#define memset(d, x, l)                                                                            \
  ({                                                                                               \
    void *_d = (d);                                                                                \
    int _x = (x);                                                                                  \
    size_t _l = (l);                                                                               \
    kafs_log(KAFS_LOG_DEBUG, "%s:%d: memset(%p, %d, %zu)\n", __FILE__, __LINE__, _d, _x, _l);      \
    memset_orig(_d, _x, _l);                                                                       \
  })

static void *memcpy_orig(void *d, const void *s, size_t l)
{
  assert(d + l <= s || s + l <= d);
  return memcpy(d, s, l);
}

#define memcpy(d, s, l)                                                                            \
  ({                                                                                               \
    void *_d = (d);                                                                                \
    const void *_s = (s);                                                                          \
    size_t _l = (l);                                                                               \
    kafs_log(KAFS_LOG_DEBUG, "%s:%d: memcpy(%p, %p, %zu)\n", __FILE__, __LINE__, _d, _s, _l);      \
    memcpy_orig(_d, _s, _l);                                                                       \
  })
#endif

// ---------------------------------------------------------
// BLOCK OPERATIONS
// ---------------------------------------------------------

/// @brief ブロック単位でデータを読み出す
/// @param ctx コンテキスト
/// @param blo ブロック番号
/// @param buf 読み出すバッファ
/// @return 0: 成功, < 0: 失敗 (-errno)
// cppcheck-suppress constParameterCallback
static int kafs_blk_read(struct kafs_context *ctx, kafs_blkcnt_t blo, void *buf)
{
  kafs_dlog(3, "%s(blo = %" PRIuFAST32 ")\n", __func__, blo);
  assert(ctx != NULL);
  assert(buf != NULL);
  assert(blo < kafs_sb_r_blkcnt_get(ctx->c_superblock));
  kafs_logblksize_t log_blksize = kafs_sb_log_blksize_get(ctx->c_superblock);
  kafs_blksize_t blksize = kafs_sb_blksize_get(ctx->c_superblock);
  if (blo == KAFS_BLO_NONE)
  {
    memset(buf, 0, blksize);
    return KAFS_SUCCESS;
  }
  off_t off = (off_t)blo << log_blksize;
  if ((size_t)off + (size_t)blksize > ctx->c_img_size)
    return -EIO;
  memcpy(buf, kafs_img_ptr(ctx, off, (size_t)blksize), (size_t)blksize);
  return KAFS_SUCCESS;
}

/// @brief ブロック単位でデータを書き込む
/// @param ctx コンテキスト
/// @param blo ブロック番号へのポインタ
/// @param buf 書き込むバッファ
/// @return 0: 成功, < 0: 失敗 (-errno)
// cppcheck-suppress constParameterCallback
static int kafs_blk_write(struct kafs_context *ctx, kafs_blkcnt_t blo, const void *buf)
{
  kafs_dlog(3, "%s(blo = %" PRIuFAST32 ")\n", __func__, blo);
  assert(ctx != NULL);
  assert(buf != NULL);
  assert(blo != KAFS_INO_NONE);
  assert(blo < kafs_sb_r_blkcnt_get(ctx->c_superblock));
  kafs_logblksize_t log_blksize = kafs_sb_log_blksize_get(ctx->c_superblock);
  kafs_blksize_t blksize = kafs_sb_blksize_get(ctx->c_superblock);
  if (blo == KAFS_BLO_NONE)
    return KAFS_SUCCESS;
  off_t off = (off_t)blo << log_blksize;
  if ((size_t)off + (size_t)blksize > ctx->c_img_size)
    return -EIO;
  memcpy(kafs_img_ptr(ctx, off, (size_t)blksize), buf, (size_t)blksize);
  return KAFS_SUCCESS;
}

/// @brief ブロックデータを未使用に変更する
/// @param ctx コンテキスト
/// @param pblo ブロック番号へのポインタ
/// @return 0: 成功, < 0: 失敗 (-errno)
// kafs_blk_release は DEL 経路廃止に伴い不要になったため削除しました

// ---------------------------------------------------------
// INODE BLOCK OPERATIONS
// ---------------------------------------------------------

typedef enum
{
  KAFS_IBLKREF_FUNC_GET,
  KAFS_IBLKREF_FUNC_PUT,
  KAFS_IBLKREF_FUNC_SET
} kafs_iblkref_func_t;

static int kafs_blk_is_zero(const void *buf, size_t len)
{
  const char *c = buf;
  while (len--)
    if (*c++)
      return 1;
  return 0;
}

static int kafs_ino_ibrk_run(struct kafs_context *ctx, kafs_sinode_t *inoent, kafs_iblkcnt_t iblo,
                             kafs_blkcnt_t *pblo, kafs_iblkref_func_t ifunc)
{
  assert(ctx != NULL);
  assert(pblo != NULL);
  assert(inoent != NULL);
  kafs_dlog(3, "ibrk_run: iblo=%" PRIuFAST32 " ifunc=%d (size=%" PRIuFAST64 ")\n", iblo, (int)ifunc,
            kafs_ino_size_get(inoent));

  if (iblo < 12)
  {
    kafs_blkcnt_t blo_data = kafs_blkcnt_stoh(inoent->i_blkreftbl[iblo]);
    switch (ifunc)
    {
    case KAFS_IBLKREF_FUNC_GET:
      *pblo = blo_data;
      return KAFS_SUCCESS;

    case KAFS_IBLKREF_FUNC_PUT:
      if (blo_data == KAFS_BLO_NONE)
      {
        KAFS_CALL(kafs_blk_alloc, ctx, &blo_data);
        inoent->i_blkreftbl[iblo] = kafs_blkcnt_htos(blo_data);
      }
      *pblo = blo_data;
      return KAFS_SUCCESS;

    case KAFS_IBLKREF_FUNC_SET:
      // 直接参照に new blo を設定（中間テーブル不要）
      inoent->i_blkreftbl[iblo] = kafs_blkcnt_htos(*pblo);
      return KAFS_SUCCESS;
    }
  }

  iblo -= 12;
  kafs_blksize_t blksize = kafs_sb_blksize_get(ctx->c_superblock);
  kafs_logblksize_t log_blkrefs_pb = kafs_sb_log_blkref_pb_get(ctx->c_superblock);
  kafs_blkcnt_t blkrefs_pb = kafs_sb_blkref_pb_get(ctx->c_superblock);
  if (iblo < blkrefs_pb)
  {
    kafs_sblkcnt_t blkreftbl[blkrefs_pb];
    kafs_blkcnt_t blo_blkreftbl = kafs_blkcnt_stoh(inoent->i_blkreftbl[12]);
    kafs_blkcnt_t blo_data;
    kafs_dlog(3,
              "ibrk_run: single-indirect idx=%" PRIuFAST32 ", blkrefs_pb=%" PRIuFAST32
              ", tbl_blo=%" PRIuFAST32 "\n",
              iblo, blkrefs_pb, blo_blkreftbl);

    switch (ifunc)
    {
    case KAFS_IBLKREF_FUNC_GET:
      if (blo_blkreftbl == KAFS_BLO_NONE)
      {
        *pblo = KAFS_BLO_NONE;
        return KAFS_SUCCESS;
      }
      KAFS_CALL(kafs_blk_read, ctx, blo_blkreftbl, blkreftbl);
      *pblo = kafs_blkcnt_stoh(blkreftbl[iblo]);
      return KAFS_SUCCESS;

    case KAFS_IBLKREF_FUNC_PUT:
      if (blo_blkreftbl == KAFS_BLO_NONE)
      {
        KAFS_CALL(kafs_blk_alloc, ctx, &blo_blkreftbl);
        inoent->i_blkreftbl[12] = kafs_blkcnt_htos(blo_blkreftbl);
        // 新規に間接テーブルを割り当てた場合は全エントリを 0 で初期化する
        memset(blkreftbl, 0, blksize);
        blo_data = KAFS_BLO_NONE;
      }
      else
      {
        KAFS_CALL(kafs_blk_read, ctx, blo_blkreftbl, blkreftbl);
        blo_data = kafs_blkcnt_stoh(blkreftbl[iblo]);
      }
      if (blo_data == KAFS_BLO_NONE)
      {
        KAFS_CALL(kafs_blk_alloc, ctx, &blo_data);
        blkreftbl[iblo] = kafs_blkcnt_htos(blo_data);
        KAFS_CALL(kafs_blk_write, ctx, blo_blkreftbl, blkreftbl);
      }
      *pblo = blo_data;
      return KAFS_SUCCESS;

    case KAFS_IBLKREF_FUNC_SET:
      if (blo_blkreftbl == KAFS_BLO_NONE)
      {
        // 中間テーブルを割り当て
        KAFS_CALL(kafs_blk_alloc, ctx, &blo_blkreftbl);
        inoent->i_blkreftbl[12] = kafs_blkcnt_htos(blo_blkreftbl);
        memset(blkreftbl, 0, blksize);
      }
      else
      {
        KAFS_CALL(kafs_blk_read, ctx, blo_blkreftbl, blkreftbl);
      }
      blkreftbl[iblo] = kafs_blkcnt_htos(*pblo);
      KAFS_CALL(kafs_blk_write, ctx, blo_blkreftbl, blkreftbl);
      return KAFS_SUCCESS;
    }
  }

  iblo -= blkrefs_pb;
  kafs_logblksize_t log_blkrefs_pb_sq = log_blkrefs_pb << 1;
  kafs_blksize_t blkrefs_pb_sq = 1 << log_blkrefs_pb_sq;
  if (iblo < blkrefs_pb_sq)
  {
    kafs_sblkcnt_t blkreftbl1[blkrefs_pb];
    kafs_sblkcnt_t blkreftbl2[blkrefs_pb];
    kafs_blkcnt_t blo_blkreftbl1 = kafs_blkcnt_stoh(inoent->i_blkreftbl[13]);
    kafs_blkcnt_t blo_blkreftbl2;
    kafs_blkcnt_t blo_data;
    kafs_blkcnt_t iblo1 = iblo >> log_blkrefs_pb;
    kafs_blkcnt_t iblo2 = iblo & (blkrefs_pb - 1);

    switch (ifunc)
    {
    case KAFS_IBLKREF_FUNC_GET:
      if (blo_blkreftbl1 == KAFS_BLO_NONE)
      {
        *pblo = KAFS_BLO_NONE;
        return KAFS_SUCCESS;
      }
      KAFS_CALL(kafs_blk_read, ctx, blo_blkreftbl1, blkreftbl1);
      blo_blkreftbl2 = kafs_blkcnt_stoh(blkreftbl1[iblo1]);
      if (blo_blkreftbl2 == KAFS_BLO_NONE)
      {
        *pblo = KAFS_BLO_NONE;
        return KAFS_SUCCESS;
      }
      KAFS_CALL(kafs_blk_read, ctx, blo_blkreftbl2, blkreftbl2);
      *pblo = kafs_blkcnt_stoh(blkreftbl2[iblo2]);
      return KAFS_SUCCESS;

    case KAFS_IBLKREF_FUNC_PUT:
      if (blo_blkreftbl1 == KAFS_BLO_NONE)
      {
        KAFS_CALL(kafs_blk_alloc, ctx, &blo_blkreftbl1);
        inoent->i_blkreftbl[13] = kafs_blkcnt_htos(blo_blkreftbl1);
        memset(blkreftbl1, 0, blksize);
        blo_blkreftbl2 = KAFS_BLO_NONE;
      }
      else
      {
        KAFS_CALL(kafs_blk_read, ctx, blo_blkreftbl1, blkreftbl1);
        blo_blkreftbl2 = kafs_blkcnt_stoh(blkreftbl1[iblo1]);
      }
      if (blo_blkreftbl2 == KAFS_BLO_NONE)
      {
        KAFS_CALL(kafs_blk_alloc, ctx, &blo_blkreftbl2);
        blkreftbl1[iblo1] = kafs_blkcnt_htos(blo_blkreftbl2);
        KAFS_CALL(kafs_blk_write, ctx, blo_blkreftbl1, blkreftbl1);
        memset(blkreftbl2, 0, blksize);
        blo_data = KAFS_BLO_NONE;
      }
      else
      {
        KAFS_CALL(kafs_blk_read, ctx, blo_blkreftbl2, blkreftbl2);
        blo_data = kafs_blkcnt_stoh(blkreftbl2[iblo2]);
      }
      if (blo_data == KAFS_BLO_NONE)
      {
        KAFS_CALL(kafs_blk_alloc, ctx, &blo_data);
        blkreftbl2[iblo2] = kafs_blkcnt_htos(blo_data);
        KAFS_CALL(kafs_blk_write, ctx, blo_blkreftbl2, blkreftbl2);
      }
      *pblo = blo_data;
      return KAFS_SUCCESS;
    case KAFS_IBLKREF_FUNC_SET:
      if (blo_blkreftbl1 == KAFS_BLO_NONE)
      {
        KAFS_CALL(kafs_blk_alloc, ctx, &blo_blkreftbl1);
        inoent->i_blkreftbl[13] = kafs_blkcnt_htos(blo_blkreftbl1);
        memset(blkreftbl1, 0, blksize);
        blo_blkreftbl2 = KAFS_BLO_NONE;
      }
      else
      {
        KAFS_CALL(kafs_blk_read, ctx, blo_blkreftbl1, blkreftbl1);
        blo_blkreftbl2 = kafs_blkcnt_stoh(blkreftbl1[iblo1]);
      }
      if (blo_blkreftbl2 == KAFS_BLO_NONE)
      {
        KAFS_CALL(kafs_blk_alloc, ctx, &blo_blkreftbl2);
        blkreftbl1[iblo1] = kafs_blkcnt_htos(blo_blkreftbl2);
        KAFS_CALL(kafs_blk_write, ctx, blo_blkreftbl1, blkreftbl1);
        memset(blkreftbl2, 0, blksize);
      }
      else
      {
        KAFS_CALL(kafs_blk_read, ctx, blo_blkreftbl2, blkreftbl2);
      }
      blkreftbl2[iblo2] = kafs_blkcnt_htos(*pblo);
      KAFS_CALL(kafs_blk_write, ctx, blo_blkreftbl2, blkreftbl2);
      return KAFS_SUCCESS;
    }
  }

  iblo -= blkrefs_pb_sq;
  kafs_sblkcnt_t blkreftbl1[blkrefs_pb];
  kafs_sblkcnt_t blkreftbl2[blkrefs_pb];
  kafs_sblkcnt_t blkreftbl3[blkrefs_pb];

  kafs_blkcnt_t blo_blkreftbl1 = kafs_blkcnt_stoh(inoent->i_blkreftbl[14]);
  kafs_blkcnt_t blo_blkreftbl2;
  kafs_blkcnt_t blo_blkreftbl3;
  kafs_blkcnt_t blo_data;

  kafs_blkcnt_t iblo1 = iblo >> log_blkrefs_pb_sq;
  kafs_blkcnt_t iblo2 = (iblo >> log_blkrefs_pb) & (blkrefs_pb - 1);
  kafs_blkcnt_t iblo3 = iblo & (blkrefs_pb - 1);

  switch (ifunc)
  {
  case KAFS_IBLKREF_FUNC_GET:
    if (blo_blkreftbl1 == KAFS_BLO_NONE)
    {
      *pblo = KAFS_BLO_NONE;
      return KAFS_SUCCESS;
    }
    KAFS_CALL(kafs_blk_read, ctx, blo_blkreftbl1, blkreftbl1);
    blo_blkreftbl2 = kafs_blkcnt_stoh(blkreftbl1[iblo1]);
    if (blo_blkreftbl2 == KAFS_BLO_NONE)
    {
      *pblo = KAFS_BLO_NONE;
      return KAFS_SUCCESS;
    }
    KAFS_CALL(kafs_blk_read, ctx, blo_blkreftbl2, blkreftbl2);
    blo_blkreftbl3 = kafs_blkcnt_stoh(blkreftbl2[iblo2]);
    if (blo_blkreftbl3 == KAFS_BLO_NONE)
    {
      *pblo = KAFS_BLO_NONE;
      return KAFS_SUCCESS;
    }
    KAFS_CALL(kafs_blk_read, ctx, blo_blkreftbl3, blkreftbl3);
    *pblo = kafs_blkcnt_stoh(blkreftbl3[iblo3]);
    return KAFS_SUCCESS;

  case KAFS_IBLKREF_FUNC_PUT:
    if (blo_blkreftbl1 == KAFS_BLO_NONE)
    {
      KAFS_CALL(kafs_blk_alloc, ctx, &blo_blkreftbl1);
      inoent->i_blkreftbl[14] = kafs_blkcnt_htos(blo_blkreftbl1);
      memset(blkreftbl1, 0, blksize);
      blo_blkreftbl2 = KAFS_BLO_NONE;
    }
    else
    {
      KAFS_CALL(kafs_blk_read, ctx, blo_blkreftbl1, blkreftbl1);
      blo_blkreftbl2 = kafs_blkcnt_stoh(blkreftbl1[iblo1]);
    }
    if (blo_blkreftbl2 == KAFS_BLO_NONE)
    {
      KAFS_CALL(kafs_blk_alloc, ctx, &blo_blkreftbl2);
      blkreftbl1[iblo1] = kafs_blkcnt_htos(blo_blkreftbl2);
      KAFS_CALL(kafs_blk_write, ctx, blo_blkreftbl1, blkreftbl1);
      memset(blkreftbl2, 0, blksize);
      blo_blkreftbl3 = KAFS_BLO_NONE;
    }
    else
    {
      KAFS_CALL(kafs_blk_read, ctx, blo_blkreftbl2, blkreftbl2);
      blo_blkreftbl3 = kafs_blkcnt_stoh(blkreftbl2[iblo2]);
    }
    if (blo_blkreftbl3 == KAFS_BLO_NONE)
    {
      KAFS_CALL(kafs_blk_alloc, ctx, &blo_blkreftbl3);
      blkreftbl2[iblo2] = kafs_blkcnt_htos(blo_blkreftbl3);
      KAFS_CALL(kafs_blk_write, ctx, blo_blkreftbl2, blkreftbl2);
      memset(blkreftbl3, 0, blksize);
      blo_data = KAFS_BLO_NONE;
    }
    else
    {
      KAFS_CALL(kafs_blk_read, ctx, blo_blkreftbl3, blkreftbl3);
      blo_data = kafs_blkcnt_stoh(blkreftbl3[iblo3]);
    }
    if (blo_data == KAFS_BLO_NONE)
    {
      KAFS_CALL(kafs_blk_alloc, ctx, &blo_data);
      blkreftbl3[iblo3] = kafs_blkcnt_htos(blo_data);
      KAFS_CALL(kafs_blk_write, ctx, blo_blkreftbl3, blkreftbl3);
    }
    *pblo = blo_data;
    return KAFS_SUCCESS;

  case KAFS_IBLKREF_FUNC_SET:
    if (blo_blkreftbl1 == KAFS_BLO_NONE)
    {
      KAFS_CALL(kafs_blk_alloc, ctx, &blo_blkreftbl1);
      inoent->i_blkreftbl[14] = kafs_blkcnt_htos(blo_blkreftbl1);
      memset(blkreftbl1, 0, blksize);
      blo_blkreftbl2 = KAFS_BLO_NONE;
    }
    else
    {
      KAFS_CALL(kafs_blk_read, ctx, blo_blkreftbl1, blkreftbl1);
      blo_blkreftbl2 = kafs_blkcnt_stoh(blkreftbl1[iblo1]);
    }
    if (blo_blkreftbl2 == KAFS_BLO_NONE)
    {
      KAFS_CALL(kafs_blk_alloc, ctx, &blo_blkreftbl2);
      blkreftbl1[iblo1] = kafs_blkcnt_htos(blo_blkreftbl2);
      KAFS_CALL(kafs_blk_write, ctx, blo_blkreftbl1, blkreftbl1);
      memset(blkreftbl2, 0, blksize);
      blo_blkreftbl3 = KAFS_BLO_NONE;
    }
    else
    {
      KAFS_CALL(kafs_blk_read, ctx, blo_blkreftbl2, blkreftbl2);
      blo_blkreftbl3 = kafs_blkcnt_stoh(blkreftbl2[iblo2]);
    }
    if (blo_blkreftbl3 == KAFS_BLO_NONE)
    {
      KAFS_CALL(kafs_blk_alloc, ctx, &blo_blkreftbl3);
      blkreftbl2[iblo2] = kafs_blkcnt_htos(blo_blkreftbl3);
      KAFS_CALL(kafs_blk_write, ctx, blo_blkreftbl2, blkreftbl2);
      memset(blkreftbl3, 0, blksize);
    }
    else
    {
      KAFS_CALL(kafs_blk_read, ctx, blo_blkreftbl3, blkreftbl3);
    }
    blkreftbl3[iblo3] = kafs_blkcnt_htos(*pblo);
    KAFS_CALL(kafs_blk_write, ctx, blo_blkreftbl3, blkreftbl3);
    return KAFS_SUCCESS;
  }
  return KAFS_SUCCESS;
}

// SET(NONE) 実施後に、空になった間接テーブルを親から切り離す。
// - 呼び出しは inode ロック内で行うこと
// - 解放すべきブロック番号（最大3段）を返し、物理解放は呼び出し側がロック外で行う
static int kafs_ino_prune_empty_indirects(struct kafs_context *ctx, kafs_sinode_t *inoent,
                                          kafs_iblkcnt_t iblo, kafs_blkcnt_t *free_blo1,
                                          kafs_blkcnt_t *free_blo2, kafs_blkcnt_t *free_blo3)
{
  assert(ctx != NULL);
  assert(inoent != NULL);
  assert(kafs_ino_get_usage(inoent));
  assert(free_blo1 && free_blo2 && free_blo3);
  *free_blo1 = *free_blo2 = *free_blo3 = KAFS_BLO_NONE;

  if (iblo < 12)
    return KAFS_SUCCESS; // 直接参照は親テーブルなし

  kafs_blksize_t blksize = kafs_sb_blksize_get(ctx->c_superblock);
  kafs_logblksize_t log_blkrefs_pb = kafs_sb_log_blkref_pb_get(ctx->c_superblock);
  kafs_blkcnt_t blkrefs_pb = kafs_sb_blkref_pb_get(ctx->c_superblock);

  kafs_iblkcnt_t rem = iblo - 12;
  if (rem < blkrefs_pb)
  {
    // 単間接
    kafs_blkcnt_t blo_tbl = kafs_blkcnt_stoh(inoent->i_blkreftbl[12]);
    if (blo_tbl == KAFS_BLO_NONE)
      return KAFS_SUCCESS;
    kafs_sblkcnt_t tbl[blkrefs_pb];
    KAFS_CALL(kafs_blk_read, ctx, blo_tbl, tbl);
    if (!kafs_blk_is_zero(tbl, blksize))
    {
      *free_blo1 = blo_tbl;
      inoent->i_blkreftbl[12] = kafs_blkcnt_htos(KAFS_BLO_NONE);
    }
    return KAFS_SUCCESS;
  }

  rem -= blkrefs_pb;
  kafs_logblksize_t log_blkrefs_pb_sq = log_blkrefs_pb << 1;
  kafs_blkcnt_t blkrefs_pb_sq = 1u << log_blkrefs_pb_sq;
  if (rem < blkrefs_pb_sq)
  {
    // 二重間接
    kafs_iblkcnt_t ib1 = rem >> log_blkrefs_pb;
    kafs_blkcnt_t blo_tbl1 = kafs_blkcnt_stoh(inoent->i_blkreftbl[13]);
    if (blo_tbl1 == KAFS_BLO_NONE)
      return KAFS_SUCCESS;
    kafs_sblkcnt_t tbl1[blkrefs_pb];
    KAFS_CALL(kafs_blk_read, ctx, blo_tbl1, tbl1);
    kafs_blkcnt_t blo_tbl2 = kafs_blkcnt_stoh(tbl1[ib1]);
    if (blo_tbl2 == KAFS_BLO_NONE)
      return KAFS_SUCCESS;
    kafs_sblkcnt_t tbl2[blkrefs_pb];
    KAFS_CALL(kafs_blk_read, ctx, blo_tbl2, tbl2);
    if (!kafs_blk_is_zero(tbl2, blksize))
    {
      *free_blo1 = blo_tbl2;
      tbl1[ib1] = kafs_blkcnt_htos(KAFS_BLO_NONE);
      KAFS_CALL(kafs_blk_write, ctx, blo_tbl1, tbl1);
      // 親も空なら切り離し
      if (!kafs_blk_is_zero(tbl1, blksize))
      {
        *free_blo2 = blo_tbl1;
        inoent->i_blkreftbl[13] = kafs_blkcnt_htos(KAFS_BLO_NONE);
      }
    }
    return KAFS_SUCCESS;
  }

  // 三重間接
  rem -= blkrefs_pb_sq;
  kafs_iblkcnt_t ib1 = rem >> log_blkrefs_pb_sq;
  kafs_iblkcnt_t ib2 = (rem >> log_blkrefs_pb) & (blkrefs_pb - 1);
  kafs_blkcnt_t blo_tbl1 = kafs_blkcnt_stoh(inoent->i_blkreftbl[14]);
  if (blo_tbl1 == KAFS_BLO_NONE)
    return KAFS_SUCCESS;
  kafs_sblkcnt_t tbl1[blkrefs_pb];
  KAFS_CALL(kafs_blk_read, ctx, blo_tbl1, tbl1);
  kafs_blkcnt_t blo_tbl2 = kafs_blkcnt_stoh(tbl1[ib1]);
  if (blo_tbl2 == KAFS_BLO_NONE)
    return KAFS_SUCCESS;
  kafs_sblkcnt_t tbl2[blkrefs_pb];
  KAFS_CALL(kafs_blk_read, ctx, blo_tbl2, tbl2);
  kafs_blkcnt_t blo_tbl3 = kafs_blkcnt_stoh(tbl2[ib2]);
  if (blo_tbl3 == KAFS_BLO_NONE)
    return KAFS_SUCCESS;
  kafs_sblkcnt_t tbl3[blkrefs_pb];
  KAFS_CALL(kafs_blk_read, ctx, blo_tbl3, tbl3);
  if (!kafs_blk_is_zero(tbl3, blksize))
  {
    *free_blo1 = blo_tbl3;
    tbl2[ib2] = kafs_blkcnt_htos(KAFS_BLO_NONE);
    KAFS_CALL(kafs_blk_write, ctx, blo_tbl2, tbl2);
    if (!kafs_blk_is_zero(tbl2, blksize))
    {
      *free_blo2 = blo_tbl2;
      tbl1[ib1] = kafs_blkcnt_htos(KAFS_BLO_NONE);
      KAFS_CALL(kafs_blk_write, ctx, blo_tbl1, tbl1);
      if (!kafs_blk_is_zero(tbl1, blksize))
      {
        *free_blo3 = blo_tbl1;
        inoent->i_blkreftbl[14] = kafs_blkcnt_htos(KAFS_BLO_NONE);
      }
    }
  }
  return KAFS_SUCCESS;
}

/// @brief inode毎のデータを読み出す（ブロック単位）
/// @param ctx コンテキスト
/// @param inoent inode テーブルエントリ
/// @param buf バッファ
/// @param iblo ブロック番号
/// @return 0: 成功, < 0: 失敗 (-errno)
static int kafs_ino_iblk_read(struct kafs_context *ctx, kafs_sinode_t *inoent, kafs_iblkcnt_t iblo,
                              void *buf)
{
  kafs_dlog(3, "%s(ino = %d, iblo = %" PRIuFAST32 ")\n", __func__, inoent - ctx->c_inotbl, iblo);
  assert(ctx != NULL);
  assert(buf != NULL);
  assert(inoent != NULL);
  assert(kafs_ino_get_usage(inoent));
  assert(kafs_ino_size_get(inoent) > KAFS_DIRECT_SIZE);
  kafs_blkcnt_t blo;
  KAFS_CALL(kafs_ino_ibrk_run, ctx, inoent, iblo, &blo, KAFS_IBLKREF_FUNC_GET);
  KAFS_CALL(kafs_blk_read, ctx, blo, buf);
  return KAFS_SUCCESS;
}

/// @brief inode毎のデータを書き込む（ブロック単位）
/// @param ctx コンテキスト
/// @param inoent inode テーブルエントリ
/// @param iblo ブロック番号
/// @param buf バッファ
/// @return 0: 成功, < 0: 失敗 (-errno)
static int kafs_ino_iblk_write(struct kafs_context *ctx, kafs_sinode_t *inoent, kafs_iblkcnt_t iblo,
                               const void *buf)
{
  kafs_dlog(3, "%s(ino = %d, iblo = %" PRIuFAST32 ")\n", __func__, inoent - ctx->c_inotbl, iblo);
  assert(ctx != NULL);
  assert(buf != NULL);
  assert(inoent != NULL);
  assert(kafs_ino_get_usage(inoent));
  assert(kafs_ino_size_get(inoent) > KAFS_DIRECT_SIZE);
  kafs_blksize_t blksize = kafs_sb_blksize_get(ctx->c_superblock);
  // 注意: kafs_blk_is_zero は「非ゼロを含むと1、全ゼロで0」を返す
  if (kafs_blk_is_zero(buf, blksize))
  {
    // 非ゼロ: HRL 経路（失敗時は従来経路）。
    kafs_hrid_t hrid;
    int is_new = 0;
    kafs_blkcnt_t new_blo = KAFS_BLO_NONE;
    ctx->c_stat_hrl_put_calls++;
    int rc = kafs_hrl_put(ctx, buf, &hrid, &is_new, &new_blo);
    if (rc == 0)
    {
      if (is_new)
        ctx->c_stat_hrl_put_misses++;
      else
        ctx->c_stat_hrl_put_hits++;
      // kafs_hrl_put() already takes one reference for the returned hrid.
      kafs_blkcnt_t old_blo;
      KAFS_CALL(kafs_ino_ibrk_run, ctx, inoent, iblo, &old_blo, KAFS_IBLKREF_FUNC_GET);
      KAFS_CALL(kafs_ino_ibrk_run, ctx, inoent, iblo, &new_blo, KAFS_IBLKREF_FUNC_SET);
      // HRLバケットロック順序のため、参照減算は inode ロック外で行う
      uint32_t ino_idx = (uint32_t)(inoent - ctx->c_inotbl);
      if (old_blo != KAFS_BLO_NONE && old_blo != new_blo)
      {
        kafs_inode_unlock(ctx, ino_idx);
        (void)kafs_hrl_dec_ref_by_blo(ctx, old_blo);
        kafs_inode_lock(ctx, ino_idx);
      }
      return KAFS_SUCCESS;
    }
    // HRL 失敗時: レガシー経路
    ctx->c_stat_hrl_put_fallback_legacy++;
    // 先に物理ブロックを割り当ててデータを書き込み、その後参照を更新する
    kafs_blkcnt_t new_blo2 = KAFS_BLO_NONE;
    KAFS_CALL(kafs_blk_alloc, ctx, &new_blo2);
    // 先にデータを書き込む（読取り側はまだ旧参照を見続ける）
    KAFS_CALL(kafs_blk_write, ctx, new_blo2, buf);
    kafs_blkcnt_t old_blo2;
    KAFS_CALL(kafs_ino_ibrk_run, ctx, inoent, iblo, &old_blo2, KAFS_IBLKREF_FUNC_GET);
    KAFS_CALL(kafs_ino_ibrk_run, ctx, inoent, iblo, &new_blo2, KAFS_IBLKREF_FUNC_SET);
    // 旧ブロックの参照を解放（inode ロック外）
    if (old_blo2 != KAFS_BLO_NONE && old_blo2 != new_blo2)
    {
      uint32_t ino_idx = (uint32_t)(inoent - ctx->c_inotbl);
      kafs_inode_unlock(ctx, ino_idx);
      (void)kafs_hrl_dec_ref_by_blo(ctx, old_blo2);
      kafs_inode_lock(ctx, ino_idx);
    }
    return KAFS_SUCCESS;
  }
  // 全ゼロ: スパース化（参照削除）
  {
    kafs_blkcnt_t old;
    KAFS_CALL(kafs_ino_ibrk_run, ctx, inoent, iblo, &old, KAFS_IBLKREF_FUNC_GET);
    if (old != KAFS_BLO_NONE)
    {
      kafs_blkcnt_t none = KAFS_BLO_NONE;
      // 先に参照を NONE に更新（inode ロック内）
      KAFS_CALL(kafs_ino_ibrk_run, ctx, inoent, iblo, &none, KAFS_IBLKREF_FUNC_SET);
      // 空になった中間テーブルを切り離し（inode ロック内）
      kafs_blkcnt_t f1, f2, f3;
      KAFS_CALL(kafs_ino_prune_empty_indirects, ctx, inoent, iblo, &f1, &f2, &f3);
      // 参照減算は inode ロック外
      uint32_t ino_idx = (uint32_t)(inoent - ctx->c_inotbl);
      kafs_inode_unlock(ctx, ino_idx);
      (void)kafs_hrl_dec_ref_by_blo(ctx, old);
      if (f1 != KAFS_BLO_NONE)
        (void)kafs_hrl_dec_ref_by_blo(ctx, f1);
      if (f2 != KAFS_BLO_NONE)
        (void)kafs_hrl_dec_ref_by_blo(ctx, f2);
      if (f3 != KAFS_BLO_NONE)
        (void)kafs_hrl_dec_ref_by_blo(ctx, f3);
      kafs_inode_lock(ctx, ino_idx);
    }
  }
  return KAFS_SUCCESS;
}

__attribute_maybe_unused__ static int
kafs_ino_iblk_release(struct kafs_context *ctx, kafs_sinode_t *inoent, kafs_iblkcnt_t iblo)
{
  kafs_dlog(3, "%s(ino = %d, iblo = %" PRIuFAST32 ")\n", __func__, inoent - ctx->c_inotbl, iblo);
  assert(ctx != NULL);
  assert(inoent != NULL);
  assert(kafs_ino_get_usage(inoent));
  assert(kafs_ino_size_get(inoent) > KAFS_DIRECT_SIZE);
  kafs_blkcnt_t old;
  KAFS_CALL(kafs_ino_ibrk_run, ctx, inoent, iblo, &old, KAFS_IBLKREF_FUNC_GET);
  if (old != KAFS_BLO_NONE)
  {
    kafs_blkcnt_t none = KAFS_BLO_NONE;
    KAFS_CALL(kafs_ino_ibrk_run, ctx, inoent, iblo, &none, KAFS_IBLKREF_FUNC_SET);
    // 空になった中間テーブルを切り離し（inode ロック内）
    kafs_blkcnt_t f1, f2, f3;
    KAFS_CALL(kafs_ino_prune_empty_indirects, ctx, inoent, iblo, &f1, &f2, &f3);
    // dec_ref は inode ロック外で実施
    uint32_t ino_idx = (uint32_t)(inoent - ctx->c_inotbl);
    kafs_inode_unlock(ctx, ino_idx);
    (void)kafs_hrl_dec_ref_by_blo(ctx, old);
    if (f1 != KAFS_BLO_NONE)
      (void)kafs_hrl_dec_ref_by_blo(ctx, f1);
    if (f2 != KAFS_BLO_NONE)
      (void)kafs_hrl_dec_ref_by_blo(ctx, f2);
    if (f3 != KAFS_BLO_NONE)
      (void)kafs_hrl_dec_ref_by_blo(ctx, f3);
    kafs_inode_lock(ctx, ino_idx);
  }
  return KAFS_SUCCESS;
}

static ssize_t kafs_pread(struct kafs_context *ctx, kafs_sinode_t *inoent, void *buf,
                          kafs_off_t size, kafs_off_t offset)
{
  assert(inoent != NULL);
  assert(kafs_ino_get_usage(inoent));
  kafs_off_t filesize = kafs_ino_size_get(inoent);
  if (offset >= filesize)
    return 0;
  if (offset + size > filesize)
    size = filesize - offset;
  if (size == 0)
    return 0;
  // 60バイト以下は直接
  if (filesize <= KAFS_DIRECT_SIZE)
  {
    memcpy(buf, (void *)inoent->i_blkreftbl + offset, size);
    return size;
  }
  size_t size_read = 0;
  kafs_logblksize_t log_blksize = kafs_sb_log_blksize_get(ctx->c_superblock);
  kafs_blksize_t blksize = kafs_sb_blksize_get(ctx->c_superblock);
  kafs_blksize_t offset_blksize = offset & (blksize - 1);
  if (offset_blksize > 0 || size - size_read < blksize)
  {
    char rbuf[blksize];
    kafs_iblkcnt_t iblo = offset >> log_blksize;
    KAFS_CALL(kafs_ino_iblk_read, ctx, inoent, iblo, rbuf);
    if (size < blksize - offset_blksize)
    {
      memcpy(buf, rbuf + offset_blksize, size);
      return size;
    }
    memcpy(buf, rbuf + offset_blksize, blksize - offset_blksize);
    size_read += blksize - offset_blksize;
  }
  while (size_read < size)
  {
    kafs_iblkcnt_t iblo = (offset + size_read) >> log_blksize;
    if (size - size_read <= blksize)
    {
      char rbuf[blksize];
      KAFS_CALL(kafs_ino_iblk_read, ctx, inoent, iblo, rbuf);
      memcpy(buf + size_read, rbuf, size - size_read);
      return size;
    }
    KAFS_CALL(kafs_ino_iblk_read, ctx, inoent, iblo, buf + size_read);
    size_read += blksize;
  }
  return size;
}

/// @brief inode 毎にデータを読み出す
/// @param ctx コンテキスト
/// @param inoent inode テーブルエントリ
/// @param buf バッファ
/// @param size バッファサイズ
/// @param offset オフセット
/// @return > 0: 読み出しサイズ, 0: EOF, < 0: エラー(-errno)
static ssize_t kafs_pwrite(struct kafs_context *ctx, kafs_sinode_t *inoent, const void *buf,
                           kafs_off_t size, kafs_off_t offset)
{
  kafs_dlog(3, "%s(ino = %d, size = %" PRIuFAST64 ", offset = %" PRIuFAST64 ")\n", __func__,
            inoent - ctx->c_inotbl, size, offset);
  assert(ctx != NULL);
  assert(buf != NULL);
  assert(inoent != NULL);
  assert(kafs_ino_get_usage(inoent));

  kafs_off_t filesize = kafs_ino_size_get(inoent);
  kafs_logblksize_t log_blksize = kafs_sb_log_blksize_get(ctx->c_superblock);
  kafs_blksize_t blksize = kafs_sb_blksize_get(ctx->c_superblock);
  kafs_off_t filesize_new = offset + size;

  if (size == 0)
    return 0;

  if (filesize < filesize_new)
  {
    // サイズ拡大時
    kafs_ino_size_set(inoent, filesize_new);
    if (filesize != 0 && filesize <= KAFS_DIRECT_SIZE && filesize_new > KAFS_DIRECT_SIZE)
    {
      char wbuf[blksize];
      memset(wbuf, 0, blksize);
      memcpy(wbuf, inoent->i_blkreftbl, filesize);
      memset(inoent->i_blkreftbl, 0, sizeof(inoent->i_blkreftbl));
      KAFS_CALL(kafs_ino_iblk_write, ctx, inoent, 0, wbuf);
    }
    filesize = filesize_new;
  }

  size_t size_written = 0;

  // 60バイト以下は直接
  if (filesize <= sizeof(inoent->i_blkreftbl))
  {
    memcpy((void *)inoent->i_blkreftbl + offset, buf, size);
    return size;
  }

  kafs_blksize_t offset_blksize = offset & (blksize - 1);
  if (offset_blksize > 0 || size - size_written < blksize)
  {
    // 1ブロック目で端数が出る場合
    kafs_iblkcnt_t iblo = offset >> log_blksize;
    // 書き戻しバッファ
    char wbuf[blksize];
    KAFS_CALL(kafs_ino_iblk_read, ctx, inoent, iblo, wbuf);
    if (size < blksize - offset_blksize)
    {
      // 1ブロックのみの場合
      memcpy(wbuf + offset_blksize, buf, size);
      KAFS_CALL(kafs_ino_iblk_write, ctx, inoent, iblo, wbuf);
      return size;
    }
    // ブロックの残り分を書き込む
    memcpy(wbuf + offset_blksize, buf, blksize - offset_blksize);
    KAFS_CALL(kafs_ino_iblk_write, ctx, inoent, iblo, wbuf);
    size_written += blksize - offset_blksize;
  }

  while (size_written < size)
  {
    kafs_iblkcnt_t iblo = (offset + size_written) >> log_blksize;
    if (size - size_written < blksize)
    {
      char wbuf[blksize];
      KAFS_CALL(kafs_ino_iblk_read, ctx, inoent, iblo, wbuf);
      memcpy(wbuf, buf + size_written, size - size_written);
      KAFS_CALL(kafs_ino_iblk_write, ctx, inoent, iblo, wbuf);
      return size;
    }
    KAFS_CALL(kafs_ino_iblk_write, ctx, inoent, iblo, buf + size_written);
    size_written += blksize;
  }
  return size;
}

static int kafs_truncate(struct kafs_context *ctx, kafs_sinode_t *inoent, kafs_off_t filesize_new)
{
  kafs_dlog(2, "%s(ino = %d, filesize_new = %" PRIuFAST64 ")\n", __func__, inoent - ctx->c_inotbl,
            filesize_new);
  assert(ctx != NULL);
  assert(inoent != NULL);
  assert(kafs_ino_get_usage(inoent));
  kafs_logblksize_t log_blksize = kafs_sb_log_blksize_get(ctx->c_superblock);
  kafs_blksize_t blksize = kafs_sb_blksize_get(ctx->c_superblock);
  kafs_off_t filesize_orig = kafs_ino_size_get(inoent);
  if (filesize_orig == filesize_new)
    return KAFS_SUCCESS;
  if (filesize_new > filesize_orig)
  {
    if (filesize_orig <= KAFS_DIRECT_SIZE && filesize_new > KAFS_DIRECT_SIZE)
    {
      char buf[blksize];
      memcpy(buf, inoent->i_blkreftbl, filesize_orig);
      memset(buf + filesize_orig, 0, blksize - filesize_orig);
      KAFS_CALL(kafs_ino_iblk_write, ctx, inoent, 0, buf);
    }
    kafs_ino_size_set(inoent, filesize_new);
    return KAFS_SUCCESS;
  }
  assert(filesize_new < filesize_orig);
  uint32_t ino_idx = (uint32_t)(inoent - ctx->c_inotbl);
  kafs_iblkcnt_t iblooff = filesize_new >> log_blksize;
  kafs_iblkcnt_t iblocnt = (filesize_orig + blksize - 1) >> log_blksize;
  kafs_blksize_t off = (kafs_blksize_t)(filesize_new & (blksize - 1));

  if (filesize_orig <= KAFS_DIRECT_SIZE)
  {
    memset((void *)inoent->i_blkreftbl + filesize_new, 0, filesize_orig - filesize_new);
    kafs_ino_size_set(inoent, filesize_new);
    return KAFS_SUCCESS;
  }

  // Indirect -> direct: copy first block data to inode, then release all blocks.
  if (filesize_new <= KAFS_DIRECT_SIZE)
  {
    char buf[blksize];
    KAFS_CALL(kafs_ino_iblk_read, ctx, inoent, 0, buf);

    // Update size first so readers won't access blocks being freed.
    kafs_ino_size_set(inoent, filesize_new);

    // Release blocks in batches: capture + clear refs under inode lock, dec_ref outside.
    const kafs_iblkcnt_t TRUNC_BATCH = 64;
    kafs_iblkcnt_t cur = 0;
    while (cur < iblocnt)
    {
      kafs_iblkcnt_t end = cur + TRUNC_BATCH;
      if (end > iblocnt)
        end = iblocnt;

      kafs_blkcnt_t to_free[TRUNC_BATCH * 4];
      size_t to_free_cnt = 0;
      for (kafs_iblkcnt_t iblo = cur; iblo < end; iblo++)
      {
        kafs_blkcnt_t old;
        KAFS_CALL(kafs_ino_ibrk_run, ctx, inoent, iblo, &old, KAFS_IBLKREF_FUNC_GET);
        if (old == KAFS_BLO_NONE)
          continue;
        kafs_blkcnt_t none = KAFS_BLO_NONE;
        KAFS_CALL(kafs_ino_ibrk_run, ctx, inoent, iblo, &none, KAFS_IBLKREF_FUNC_SET);
        kafs_blkcnt_t f1, f2, f3;
        KAFS_CALL(kafs_ino_prune_empty_indirects, ctx, inoent, iblo, &f1, &f2, &f3);
        to_free[to_free_cnt++] = old;
        if (f1 != KAFS_BLO_NONE)
          to_free[to_free_cnt++] = f1;
        if (f2 != KAFS_BLO_NONE)
          to_free[to_free_cnt++] = f2;
        if (f3 != KAFS_BLO_NONE)
          to_free[to_free_cnt++] = f3;
      }

      if (to_free_cnt)
      {
        kafs_inode_unlock(ctx, ino_idx);
        for (size_t i = 0; i < to_free_cnt; i++)
          (void)kafs_hrl_dec_ref_by_blo(ctx, to_free[i]);
        kafs_inode_lock(ctx, ino_idx);
      }
      cur = end;
    }

    memcpy(inoent->i_blkreftbl, buf, (size_t)filesize_new);
    if (filesize_new < KAFS_DIRECT_SIZE)
      memset((void *)inoent->i_blkreftbl + filesize_new, 0, KAFS_DIRECT_SIZE - filesize_new);
    return KAFS_SUCCESS;
  }

  // Shrink within indirect mode: update size first so readers won't access freed blocks.
  kafs_ino_size_set(inoent, filesize_new);

  if (off > 0)
  {
    char buf[blksize];
    KAFS_CALL(kafs_ino_iblk_read, ctx, inoent, iblooff, buf);
    memset(buf + off, 0, blksize - off);
    KAFS_CALL(kafs_ino_iblk_write, ctx, inoent, iblooff, buf);
    iblooff++;
  }

  const kafs_iblkcnt_t TRUNC_BATCH = 64;
  while (iblooff < iblocnt)
  {
    kafs_iblkcnt_t end = iblooff + TRUNC_BATCH;
    if (end > iblocnt)
      end = iblocnt;

    kafs_blkcnt_t to_free[TRUNC_BATCH * 4];
    size_t to_free_cnt = 0;
    for (kafs_iblkcnt_t iblo = iblooff; iblo < end; iblo++)
    {
      kafs_blkcnt_t old;
      KAFS_CALL(kafs_ino_ibrk_run, ctx, inoent, iblo, &old, KAFS_IBLKREF_FUNC_GET);
      if (old == KAFS_BLO_NONE)
        continue;
      kafs_blkcnt_t none = KAFS_BLO_NONE;
      KAFS_CALL(kafs_ino_ibrk_run, ctx, inoent, iblo, &none, KAFS_IBLKREF_FUNC_SET);
      kafs_blkcnt_t f1, f2, f3;
      KAFS_CALL(kafs_ino_prune_empty_indirects, ctx, inoent, iblo, &f1, &f2, &f3);
      to_free[to_free_cnt++] = old;
      if (f1 != KAFS_BLO_NONE)
        to_free[to_free_cnt++] = f1;
      if (f2 != KAFS_BLO_NONE)
        to_free[to_free_cnt++] = f2;
      if (f3 != KAFS_BLO_NONE)
        to_free[to_free_cnt++] = f3;
    }

    if (to_free_cnt)
    {
      kafs_inode_unlock(ctx, ino_idx);
      for (size_t i = 0; i < to_free_cnt; i++)
        (void)kafs_hrl_dec_ref_by_blo(ctx, to_free[i]);
      kafs_inode_lock(ctx, ino_idx);
    }
    iblooff = end;
  }

  return KAFS_SUCCESS;
}

__attribute_maybe_unused__ static int kafs_trim(struct kafs_context *ctx, kafs_sinode_t *inoent,
                                                kafs_off_t off, kafs_off_t size)
{
  kafs_dlog(2, "%s(ino = %d, off = %" PRIuFAST64 ", size = %" PRIuFAST64 ")\n", __func__,
            inoent - ctx->c_inotbl, off, size);
  assert(ctx != NULL);
  assert(inoent != NULL);
  assert(kafs_ino_get_usage(inoent));
  if (size == 0)
    return size;
  kafs_off_t size_orig = kafs_ino_size_get(inoent);
  if (off >= size_orig)
    return 0;
  if (off + size >= size_orig)
  {
    KAFS_CALL(kafs_truncate, ctx, inoent, off);
    return size_orig - off;
  }
  // Slow but correct implementation: shift tail data left in bounded chunks.
  kafs_off_t src = off + size;
  kafs_off_t dst = off;
  kafs_off_t tail = size_orig - src;
  kafs_blksize_t blksize = kafs_sb_blksize_get(ctx->c_superblock);
  const size_t CHUNK_MAX = (size_t)blksize * 4u;
  char *buf = (char *)malloc(CHUNK_MAX);
  if (buf == NULL)
    return -ENOMEM;
  while (tail > 0)
  {
    size_t chunk = (tail > (kafs_off_t)CHUNK_MAX) ? CHUNK_MAX : (size_t)tail;
    ssize_t r = KAFS_CALL(kafs_pread, ctx, inoent, buf, (kafs_off_t)chunk, src);
    if (r < 0)
    {
      free(buf);
      return (int)r;
    }
    if (r == 0)
      break;
    ssize_t w = KAFS_CALL(kafs_pwrite, ctx, inoent, buf, (kafs_off_t)r, dst);
    if (w != r)
    {
      free(buf);
      return (w < 0) ? (int)w : -EIO;
    }
    src += r;
    dst += r;
    tail -= r;
  }
  free(buf);
  KAFS_CALL(kafs_truncate, ctx, inoent, dst);
  return KAFS_SUCCESS;
}

__attribute_maybe_unused__ static int kafs_release(struct kafs_context *ctx, kafs_sinode_t *inoent)
{
  // Requires: caller holds inode lock for inoent.
  if (kafs_ino_linkcnt_decr(inoent) == 0)
  {
    KAFS_CALL(kafs_truncate, ctx, inoent, 0);
    memset(inoent, 0, sizeof(struct kafs_sinode));
    // Best-effort accounting (avoid taking inode_alloc_lock here to prevent lock inversion).
    kafs_sb_inocnt_free_incr(ctx->c_superblock);
    kafs_sb_wtime_set(ctx->c_superblock, kafs_now());
  }
  return KAFS_SUCCESS;
}

/// @brief ディレクトリエントリを読み出す
/// @param ctx コンテキスト
/// @param inoent_dir ディレクトリの inodeテーブルのエントリ
/// @param dirent 読み出すディレクトリエントリのバッファ（sizeof(struct kafs_dirent) 以上）
/// @param offset オフセット
/// @return > 0: サイズ, 0: EOF, < 0: 失敗 (-errno)
static ssize_t kafs_dirent_read(struct kafs_context *ctx, kafs_sinode_t *inoent_dir,
                                struct kafs_sdirent *dirent, kafs_off_t offset)
{
  kafs_dlog(3, "%s(ino_dir = %d, offset = %" PRIuFAST64 ")\n", __func__,
            (inoent_dir - ctx->c_inotbl), offset);
  assert(ctx != NULL);
  assert(inoent_dir != NULL);
  assert(dirent != NULL);
  assert(kafs_ino_get_usage(inoent_dir));
  ssize_t r1 =
      KAFS_CALL(kafs_pread, ctx, inoent_dir, dirent, offsetof(kafs_sdirent_t, d_filename), offset);
  if (r1 == 0)
    return 0;
  if (r1 < (ssize_t)offsetof(kafs_sdirent_t, d_filename))
  {
    kafs_dlog(1, "%s(short read dirent header)\n", __func__);
    return -EIO;
  }
  kafs_inocnt_t d_ino = kafs_dirent_ino_get(dirent);
  kafs_filenamelen_t filenamelen = kafs_dirent_filenamelen_get(dirent);
  // Treat zeroed/uninitialized entries as EOF (allows sparse tail within directory inode size).
  if (d_ino == 0 || filenamelen == 0)
    return 0;
  if (filenamelen >= FILENAME_MAX)
  {
    kafs_log(KAFS_LOG_ERR,
             "%s: invalid filenamelen=%" PRIuFAST32 " ino_dir=%d off=%" PRIuFAST64 "\n", __func__,
             (uint_fast32_t)filenamelen, (int)(inoent_dir - ctx->c_inotbl), offset);
    return -EIO;
  }
  ssize_t r2 = KAFS_CALL(kafs_pread, ctx, inoent_dir, dirent->d_filename, filenamelen, offset + r1);
  if (r2 < (ssize_t)filenamelen)
  {
    kafs_log(KAFS_LOG_ERR,
             "%s: short read dirent name ino_dir=%d off=%" PRIuFAST64 " need=%" PRIuFAST32
             " got=%zd\n",
             __func__, (int)(inoent_dir - ctx->c_inotbl), offset, (uint_fast32_t)filenamelen, r2);
    return -EIO;
  }
  dirent->d_filename[r2] = '\0';
  kafs_dlog(3,
            "%s(ino_dir = %d, offset = %" PRIuFAST64 ") return {.d_ino = %" PRIuFAST32
            ", .d_filename = %s, .d_filenamelen = %" PRIuFAST32 "}\n",
            __func__, (inoent_dir - ctx->c_inotbl), offset, kafs_dirent_ino_get(dirent),
            dirent->d_filename, kafs_dirent_filenamelen_get(dirent));
  return r1 + r2;
}

/// @brief ディレクトリエントリから対象のファイル名を探す
/// @param ctx コンテキスト
/// @param name ファイル名
/// @param namelen ファイル名の長さ
/// @param ino 対象のディレクトリ
/// @param pino_found 見つかったエントリ
/// @return 0: 成功, < 0: 失敗 (-errno)
static int kafs_dirent_search(struct kafs_context *ctx, kafs_sinode_t *inoent, const char *filename,
                              kafs_filenamelen_t filenamelen, kafs_sinode_t **pinoent_found)
{
  kafs_dlog(3, "%s(ino = %d, filename = %.*s, filenamelen = %" PRIuFAST16 ")\n", __func__,
            (inoent - ctx->c_inotbl), (int)filenamelen, filename, filenamelen);
  assert(ctx != NULL);
  assert(inoent != NULL);
  assert(filename != NULL);
  assert(filenamelen > 0);
  assert(pinoent_found != NULL);
  assert(kafs_ino_get_usage(inoent));
  kafs_mode_t mode = kafs_ino_mode_get(inoent);
  if (!S_ISDIR(mode))
    return -ENOTDIR;
  struct kafs_sdirent dirent;
  off_t offset = 0;
  while (1)
  {
    ssize_t r = KAFS_CALL(kafs_dirent_read, ctx, inoent, &dirent, offset);
    if (r == 0)
      break;
    kafs_inocnt_t d_ino = kafs_dirent_ino_get(&dirent);
    kafs_filenamelen_t d_filenamelen = kafs_dirent_filenamelen_get(&dirent);
    const char *d_filename = dirent.d_filename;
    if (r != (ssize_t)(offsetof(kafs_sdirent_t, d_filename) + d_filenamelen))
      return -EIO;
    if (d_filenamelen == filenamelen && memcmp(d_filename, filename, filenamelen) == 0)
    {
      *pinoent_found = &ctx->c_inotbl[d_ino];
      return KAFS_SUCCESS;
    }
    offset += r;
  }
  return -ENOENT;
}

// Directory entry format on disk: {ino,u16 namelen, name[namelen]} repeated.
typedef struct __attribute__((packed))
{
  kafs_sinocnt_t d_ino;
  kafs_sfilenamelen_t d_filenamelen;
} kafs_dirent_hdr_t;

static int kafs_dir_snapshot(struct kafs_context *ctx, kafs_sinode_t *inoent_dir, char **out,
                             size_t *out_len)
{
  *out = NULL;
  *out_len = 0;
  size_t len = (size_t)kafs_ino_size_get(inoent_dir);
  if (len == 0)
    return 0;
  char *buf = (char *)malloc(len);
  if (!buf)
    return -ENOMEM;
  ssize_t r = kafs_pread(ctx, inoent_dir, buf, (kafs_off_t)len, 0);
  if (r < 0 || (size_t)r != len)
  {
    free(buf);
    return -EIO;
  }
  *out = buf;
  *out_len = len;
  return 0;
}

static int kafs_dir_writeback(struct kafs_context *ctx, kafs_sinode_t *inoent_dir, const char *buf,
                              size_t len)
{
  size_t old = (size_t)kafs_ino_size_get(inoent_dir);
  if (len)
  {
    ssize_t w = kafs_pwrite(ctx, inoent_dir, buf, (kafs_off_t)len, 0);
    if (w < 0 || (size_t)w != len)
      return -EIO;
  }
  if (len < old)
    return kafs_truncate(ctx, inoent_dir, (kafs_off_t)len);
  if (len == 0 && old)
    return kafs_truncate(ctx, inoent_dir, 0);
  return 0;
}

static int kafs_dirent_iter_next(const char *buf, size_t len, size_t off, kafs_inocnt_t *out_ino,
                                 kafs_filenamelen_t *out_namelen, const char **out_name,
                                 size_t *out_rec_len)
{
  const size_t hdr_sz = sizeof(kafs_dirent_hdr_t);
  if (off >= len)
    return 0;
  if (len - off < hdr_sz)
    return -EIO;
  kafs_dirent_hdr_t hdr;
  memcpy(&hdr, buf + off, hdr_sz);
  kafs_inocnt_t ino = kafs_inocnt_stoh(hdr.d_ino);
  kafs_filenamelen_t namelen = kafs_filenamelen_stoh(hdr.d_filenamelen);
  if (ino == 0 || namelen == 0)
    return 0;
  if (namelen >= FILENAME_MAX)
    return -EIO;
  if (len - off < hdr_sz + (size_t)namelen)
    return -EIO;
  *out_ino = ino;
  *out_namelen = namelen;
  *out_name = buf + off + hdr_sz;
  *out_rec_len = hdr_sz + (size_t)namelen;
  return 1;
}

// NOTE: caller holds dir inode lock. For rename(2) we must not change linkcount of the moved inode.
static int kafs_dirent_add_nolink(struct kafs_context *ctx, kafs_sinode_t *inoent_dir,
                                  kafs_inocnt_t ino, const char *filename)
{
  assert(ctx != NULL);
  assert(inoent_dir != NULL);
  assert(filename != NULL);
  assert(ino != KAFS_INO_NONE);
  if (!S_ISDIR(kafs_ino_mode_get(inoent_dir)))
    return -ENOTDIR;

  kafs_filenamelen_t filenamelen = (kafs_filenamelen_t)strlen(filename);
  if (filenamelen == 0 || filenamelen >= FILENAME_MAX)
    return -EINVAL;

  char *old = NULL;
  size_t old_len = 0;
  int rc = kafs_dir_snapshot(ctx, inoent_dir, &old, &old_len);
  if (rc < 0)
    return rc;

  // scan for duplicates and compute append offset
  size_t off = 0;
  while (1)
  {
    kafs_inocnt_t dino;
    kafs_filenamelen_t dlen;
    const char *dname;
    size_t rec_len;
    int step = kafs_dirent_iter_next(old, old_len, off, &dino, &dlen, &dname, &rec_len);
    if (step == 0)
      break;
    if (step < 0)
    {
      free(old);
      return -EIO;
    }
    if (dlen == filenamelen && memcmp(dname, filename, filenamelen) == 0)
    {
      free(old);
      return -EEXIST;
    }
    off += rec_len;
  }

  const size_t hdr_sz = sizeof(kafs_dirent_hdr_t);
  size_t new_len = off + hdr_sz + (size_t)filenamelen;
  char *nw = (char *)malloc(new_len);
  if (!nw)
  {
    free(old);
    return -ENOMEM;
  }
  if (off)
    memcpy(nw, old, off);
  kafs_dirent_hdr_t hdr;
  hdr.d_ino = kafs_inocnt_htos(ino);
  hdr.d_filenamelen = kafs_filenamelen_htos(filenamelen);
  memcpy(nw + off, &hdr, hdr_sz);
  memcpy(nw + off + hdr_sz, filename, filenamelen);

  rc = kafs_dir_writeback(ctx, inoent_dir, nw, new_len);
  free(nw);
  free(old);
  return rc;
}

static int kafs_dirent_add(struct kafs_context *ctx, kafs_sinode_t *inoent_dir, kafs_inocnt_t ino,
                           const char *filename)
{
  int rc = kafs_dirent_add_nolink(ctx, inoent_dir, ino, filename);
  if (rc == 0)
    kafs_ino_linkcnt_incr(&ctx->c_inotbl[ino]);
  return rc;
}

// NOTE: caller holds dir inode lock.
static int kafs_dirent_remove_nolink(struct kafs_context *ctx, kafs_sinode_t *inoent_dir,
                                     const char *filename, kafs_inocnt_t *out_ino)
{
  assert(ctx != NULL);
  assert(inoent_dir != NULL);
  assert(filename != NULL);
  if (out_ino)
    *out_ino = KAFS_INO_NONE;
  if (!S_ISDIR(kafs_ino_mode_get(inoent_dir)))
    return -ENOTDIR;

  kafs_filenamelen_t filenamelen = (kafs_filenamelen_t)strlen(filename);
  if (filenamelen == 0 || filenamelen >= FILENAME_MAX)
    return -EINVAL;

  char *old = NULL;
  size_t old_len = 0;
  int rc = kafs_dir_snapshot(ctx, inoent_dir, &old, &old_len);
  if (rc < 0)
    return rc;

  size_t off = 0;
  while (1)
  {
    kafs_inocnt_t dino;
    kafs_filenamelen_t dlen;
    const char *dname;
    size_t rec_len;
    int step = kafs_dirent_iter_next(old, old_len, off, &dino, &dlen, &dname, &rec_len);
    if (step == 0)
      break;
    if (step < 0)
    {
      free(old);
      return -EIO;
    }
    if (dlen == filenamelen && memcmp(dname, filename, filenamelen) == 0)
    {
      size_t new_len = old_len - rec_len;
      char *nw = (char *)malloc(new_len);
      if (!nw)
      {
        free(old);
        return -ENOMEM;
      }
      if (off)
        memcpy(nw, old, off);
      if (off + rec_len < old_len)
        memcpy(nw + off, old + off + rec_len, old_len - (off + rec_len));

      rc = kafs_dir_writeback(ctx, inoent_dir, nw, new_len);
      if (rc == 0 && out_ino)
        *out_ino = dino;
      free(nw);
      free(old);
      return rc;
    }
    off += rec_len;
  }

  free(old);
  return -ENOENT;
}

static int kafs_dirent_remove(struct kafs_context *ctx, kafs_sinode_t *inoent_dir,
                              const char *filename)
{
  kafs_inocnt_t d_ino;
  int rc = kafs_dirent_remove_nolink(ctx, inoent_dir, filename, &d_ino);
  if (rc == 0 && d_ino != KAFS_INO_NONE)
    kafs_ino_linkcnt_decr(&ctx->c_inotbl[d_ino]);
  return rc;
}

// cppcheck-suppress constParameterCallback
static int kafs_access_check(int ok, kafs_sinode_t *inoent, kafs_bool_t is_dir, uid_t uid,
                             gid_t gid, size_t ngroups,
                             /* cppcheck-suppress constParameterCallback */ gid_t groups[])
{
  mode_t mode = kafs_ino_mode_get(inoent);
  uid_t fuid = kafs_ino_uid_get(inoent);
  gid_t fgid = kafs_ino_gid_get(inoent);
  if (is_dir)
  {
    if (!S_ISDIR(mode))
    {
      kafs_dlog(1, "%s: ENOTDIR (mode=%o uid=%u gid=%u)\n", __func__, (unsigned)mode,
                (unsigned)fuid, (unsigned)fgid);
      return -ENOTDIR;
    }
    if (ok == F_OK)
      ok = X_OK;
  }
  if (ok == F_OK)
    return KAFS_SUCCESS;
  if (ok & R_OK)
  {
    kafs_bool_t result = KAFS_FALSE;
    if (mode & S_IROTH)
      result = KAFS_TRUE;
    else if (mode & S_IRUSR && uid == fuid)
      result = KAFS_TRUE;
    else if (mode & S_IRGRP)
    {
      if (gid == fgid)
        result = KAFS_TRUE;
      else
        for (size_t i = 0; i < ngroups; i++)
          if (fgid == groups[i])
          {
            result = KAFS_TRUE;
            break;
          }
    }
    if (!result)
      return -EACCES;
  }
  if (ok & W_OK)
  {
    kafs_bool_t result = KAFS_FALSE;
    if (mode & S_IWOTH)
      result = KAFS_TRUE;
    else if (mode & S_IWUSR && uid == fuid)
      result = KAFS_TRUE;
    else if (mode & S_IWGRP)
    {
      if (gid == fgid)
        result = KAFS_TRUE;
      else
        for (size_t i = 0; i < ngroups; i++)
          if (fgid == groups[i])
          {
            result = KAFS_TRUE;
            break;
          }
    }
    if (!result)
      return -EACCES;
  }
  if (ok & X_OK)
  {
    kafs_bool_t result = KAFS_FALSE;
    if (mode & S_IXOTH)
      result = KAFS_TRUE;
    else if (mode & S_IXUSR && uid == fuid)
      result = KAFS_TRUE;
    else if (mode & S_IXGRP)
    {
      if (gid == fgid)
        result = KAFS_TRUE;
      else
        for (size_t i = 0; i < ngroups; i++)
          if (fgid == groups[i])
          {
            result = KAFS_TRUE;
            break;
          }
    }
    if (!result)
      return -EACCES;
  }
  return KAFS_SUCCESS;
}

// cppcheck-suppress constParameterCallback
static int kafs_access(struct fuse_context *fctx, kafs_context_t *ctx, const char *path,
                       struct fuse_file_info *fi, int ok, kafs_sinode_t **pinoent)
{
  assert(fctx != NULL);
  assert(ctx != NULL);
  assert(path == NULL || *path == '/' || *path == '\0');

  kafs_dlog(2, "%s(path=%s, ok=%d, fi=%p)\n", __func__, path ? path : "(null)", ok, (void *)fi);

  uid_t uid = fctx->uid;
  gid_t gid = fctx->gid;
  // fuse_getgroups(0,NULL) may return 0 or -1; don't use KAFS_IOCALL here and avoid zero-length VLA
  ssize_t ng0 = fuse_getgroups(0, NULL);
  size_t ngroups = (ng0 > 0) ? (size_t)ng0 : 0;
  gid_t groups[(ngroups > 0) ? ngroups : 1];
  if (ngroups > 0)
    (void)fuse_getgroups(ngroups, groups);

  kafs_sinode_t *inoent;
  const char *p;
  if (path == NULL || path[0] == '\0')
  {
    // Path-less operation (read/write etc.): start from file handle
    assert(fi != NULL);
    inoent = &ctx->c_inotbl[(kafs_inocnt_t)fi->fh];
    p = "";
  }
  else
  {
    // Path-based operation: always resolve from root regardless of fi
    inoent = &ctx->c_inotbl[KAFS_INO_ROOTDIR];
    p = path + 1; // skip leading '/'
  }
  int ok_final = ok; // keep original request for the final node
  while (*p != '\0')
  {
    const char *n = strchrnul(p, '/');
    kafs_mode_t cur_mode = kafs_ino_mode_get(inoent);
    kafs_dlog(2, "%s: component='%.*s' checking dir ino=%u mode=%o\n", __func__, (int)(n - p), p,
              (unsigned)(inoent - ctx->c_inotbl), (unsigned)cur_mode);
    // For intermediate directories, require execute (search) permission only
    int ok_dirs = X_OK;
    int rc_ac = kafs_access_check(ok_dirs, inoent, KAFS_TRUE, uid, gid, ngroups, groups);
    if (rc_ac < 0)
      return rc_ac;
    uint32_t ino_dir = (uint32_t)(inoent - ctx->c_inotbl);
    kafs_inode_lock(ctx, ino_dir);
    int rc = kafs_dirent_search(ctx, inoent, p, n - p, &inoent);
    kafs_inode_unlock(ctx, ino_dir);
    if (rc < 0)
    {
      kafs_dlog(2, "%s: dirent_search('%.*s') rc=%d\n", __func__, (int)(n - p), p, rc);
      return rc;
    }
    if (*n == '\0')
      break;
    p = n + 1;
  }
  kafs_mode_t final_mode = kafs_ino_mode_get(inoent);
  kafs_dlog(2, "%s: final node ino=%u mode=%o ok=%d\n", __func__,
            (unsigned)(inoent - ctx->c_inotbl), (unsigned)final_mode, ok_final);
  KAFS_CALL(kafs_access_check, ok_final, inoent, KAFS_FALSE, uid, gid, ngroups, groups);
  if (pinoent != NULL)
    *pinoent = inoent;
  return KAFS_SUCCESS;
}

static int kafs_op_getattr(const char *path, struct stat *st, struct fuse_file_info *fi)
{
  struct fuse_context *fctx = fuse_get_context();
  struct kafs_context *ctx = fctx->private_data;
  struct kafs_sinode *inoent;
  KAFS_CALL(kafs_access, fctx, ctx, path, fi, F_OK, &inoent);
  st->st_dev = 0;
  st->st_ino = inoent - ctx->c_inotbl;
  st->st_mode = kafs_ino_mode_get(inoent);
  st->st_nlink = kafs_ino_linkcnt_get(inoent);
  st->st_uid = kafs_ino_uid_get(inoent);
  st->st_gid = kafs_ino_gid_get(inoent);
  st->st_rdev = kafs_ino_dev_get(inoent);
  st->st_size = kafs_ino_size_get(inoent);
  st->st_blksize = kafs_sb_blksize_get(ctx->c_superblock);

  /* st_blocks is in 512-byte units; approximate allocation by rounding st_size to fs block size. */
  const unsigned blksz = (unsigned)st->st_blksize;
  const unsigned long long alloc = blksz ? ((unsigned long long)st->st_size + blksz - 1) / blksz * blksz : 0;
  st->st_blocks = (blkcnt_t)(alloc / 512ull);
  st->st_atim = kafs_ino_atime_get(inoent);
  st->st_mtim = kafs_ino_mtime_get(inoent);
  st->st_ctim = kafs_ino_ctime_get(inoent);
  return 0;
}

static int kafs_op_statfs(const char *path, struct statvfs *st)
{
  (void)path;
  struct fuse_context *fctx = fuse_get_context();
  struct kafs_context *ctx = fctx->private_data;
  memset(st, 0, sizeof(*st));

  const unsigned blksize = (unsigned)kafs_sb_blksize_get(ctx->c_superblock);
  const kafs_blkcnt_t blocks = kafs_sb_blkcnt_get(ctx->c_superblock);
  kafs_bitmap_lock(ctx);
  const kafs_blkcnt_t bfree = kafs_sb_blkcnt_free_get(ctx->c_superblock);
  kafs_bitmap_unlock(ctx);
  const kafs_inocnt_t files = kafs_sb_inocnt_get(ctx->c_superblock);
  const kafs_inocnt_t ffree = (kafs_inocnt_t)kafs_sb_inocnt_free_get(ctx->c_superblock);

  st->f_bsize = blksize;
  st->f_frsize = blksize;
  st->f_blocks = (fsblkcnt_t)blocks;
  st->f_bfree = (fsblkcnt_t)bfree;
  st->f_bavail = (fsblkcnt_t)bfree;
  st->f_files = (fsfilcnt_t)files;
  st->f_ffree = (fsfilcnt_t)ffree;
  st->f_favail = (fsfilcnt_t)ffree;
  st->f_namemax = 255;

  (void)fctx;
  return 0;
}

#define KAFS_STATS_VERSION 1u

static inline kafs_hrl_entry_t *kafs_hrl_entries_tbl(kafs_context_t *ctx)
{
  uintptr_t base = (uintptr_t)ctx->c_superblock;
  uint64_t off = kafs_sb_hrl_entry_offset_get(ctx->c_superblock);
  if (!off)
    return NULL;
  return (kafs_hrl_entry_t *)(base + (uintptr_t)off);
}

static void kafs_stats_snapshot(kafs_context_t *ctx, kafs_stats_t *out)
{
  memset(out, 0, sizeof(*out));
  out->struct_size = (uint32_t)sizeof(*out);
  out->version = KAFS_STATS_VERSION;

  out->blksize = (uint32_t)kafs_sb_blksize_get(ctx->c_superblock);
  out->fs_blocks_total = (uint64_t)kafs_sb_blkcnt_get(ctx->c_superblock);
  kafs_bitmap_lock(ctx);
  out->fs_blocks_free = (uint64_t)kafs_sb_blkcnt_free_get(ctx->c_superblock);
  kafs_bitmap_unlock(ctx);
  out->fs_inodes_total = (uint64_t)kafs_sb_inocnt_get(ctx->c_superblock);
  out->fs_inodes_free = (uint64_t)(kafs_inocnt_t)kafs_sb_inocnt_free_get(ctx->c_superblock);

  uint32_t entry_cnt = kafs_sb_hrl_entry_cnt_get(ctx->c_superblock);
  out->hrl_entries_total = (uint64_t)entry_cnt;

  kafs_hrl_entry_t *ents = kafs_hrl_entries_tbl(ctx);
  if (ents)
  {
    uint64_t used = 0, dup = 0, refsum = 0;
    for (uint32_t i = 0; i < entry_cnt; ++i)
    {
      uint32_t r = ents[i].refcnt;
      if (r)
      {
        used++;
        refsum += r;
        if (r > 1)
          dup++;
      }
    }
    out->hrl_entries_used = used;
    out->hrl_entries_duplicated = dup;
    out->hrl_refcnt_sum = refsum;
  }

  out->hrl_put_calls = ctx->c_stat_hrl_put_calls;
  out->hrl_put_hits = ctx->c_stat_hrl_put_hits;
  out->hrl_put_misses = ctx->c_stat_hrl_put_misses;
  out->hrl_put_fallback_legacy = ctx->c_stat_hrl_put_fallback_legacy;
}

static int kafs_op_ioctl(const char *path, int cmd, void *arg, struct fuse_file_info *fi,
                         unsigned int flags, void *data)
{
  (void)path;
  (void)fi;
  (void)flags;

  struct fuse_context *fctx = fuse_get_context();
  kafs_context_t *ctx = (kafs_context_t *)fctx->private_data;

  if ((unsigned int)cmd == (unsigned int)KAFS_IOCTL_GET_STATS)
  {
    void *buf = data ? data : arg;
    if (!buf)
      return -EINVAL;
    if (_IOC_SIZE((unsigned int)cmd) < sizeof(kafs_stats_t))
      return -EINVAL;
    kafs_stats_t out;
    kafs_stats_snapshot(ctx, &out);
    memcpy(buf, &out, sizeof(out));
    return 0;
  }
  return -ENOTTY;
}

#undef KAFS_STATS_VERSION

static int kafs_op_open(const char *path, struct fuse_file_info *fi)
{
  struct fuse_context *fctx = fuse_get_context();
  struct kafs_context *ctx = fctx->private_data;
  int ok = 0;
  int accmode = fi->flags & O_ACCMODE;
  if (accmode == O_RDONLY || accmode == O_RDWR)
    ok |= R_OK;
  if (accmode == O_WRONLY || accmode == O_RDWR)
    ok |= W_OK;
  kafs_sinode_t *inoent;
  KAFS_CALL(kafs_access, fctx, ctx, path, NULL, ok, &inoent);
  fi->fh = inoent - ctx->c_inotbl;
  // Handle O_TRUNC on open for existing files to match POSIX semantics
  if ((fi->flags & O_TRUNC) && (accmode == O_WRONLY || accmode == O_RDWR))
  {
    kafs_inode_lock(ctx, (uint32_t)fi->fh);
    (void)kafs_truncate(ctx, &ctx->c_inotbl[fi->fh], 0);
    kafs_inode_unlock(ctx, (uint32_t)fi->fh);
  }
  return 0;
}

static int kafs_op_opendir(const char *path, struct fuse_file_info *fi)
{
  struct fuse_context *fctx = fuse_get_context();
  struct kafs_context *ctx = fctx->private_data;
  kafs_sinode_t *inoent;
  KAFS_CALL(kafs_access, fctx, ctx, path, NULL, R_OK, &inoent);
  fi->fh = inoent - ctx->c_inotbl;
  return 0;
}

static int kafs_op_readdir(const char *path, void *buf, fuse_fill_dir_t filler, off_t offset,
                           struct fuse_file_info *fi, enum fuse_readdir_flags flags)
{
  (void)offset;
  (void)fi;
  (void)flags;
  struct fuse_context *fctx = fuse_get_context();
  struct kafs_context *ctx = fctx->private_data;
  kafs_sinode_t *inoent_dir;
  KAFS_CALL(kafs_access, fctx, ctx, path, NULL, R_OK, &inoent_dir);
  uint32_t ino_dir = (uint32_t)(inoent_dir - ctx->c_inotbl);

  kafs_inode_lock(ctx, ino_dir);
  off_t filesize = kafs_ino_size_get(inoent_dir);
  off_t o = 0;
  kafs_sdirent_t dirent;
  if (filler(buf, ".", NULL, 0, 0))
  {
    kafs_inode_unlock(ctx, ino_dir);
    return -ENOENT;
  }
  while (o < filesize)
  {
    ssize_t r = KAFS_CALL(kafs_dirent_read, ctx, inoent_dir, &dirent, o);
    if (r == 0)
      break;
    if (filler(buf, dirent.d_filename, NULL, 0, 0))
    {
      kafs_inode_unlock(ctx, ino_dir);
      return -ENOENT;
    }
    o += r;
  }
  kafs_inode_unlock(ctx, ino_dir);
  return 0;
}

static int kafs_create(const char *path, kafs_mode_t mode, kafs_dev_t dev, kafs_inocnt_t *pino_dir,
                       kafs_inocnt_t *pino_new)
{
  (void)dev;
  assert(path != NULL);
  assert(path[0] == '/');
  assert(path[1] != '\0');
  struct fuse_context *fctx = fuse_get_context();
  struct kafs_context *ctx = fctx->private_data;
  char path_copy[strlen(path) + 1];
  strcpy(path_copy, path);
  const char *dirpath = path_copy;
  char *basepath = strrchr(path_copy, '/');
  if (dirpath == basepath)
    dirpath = "/";
  *basepath = '\0';
  basepath++;

  uint64_t jseq = kafs_journal_begin(ctx, "CREATE", "path=%s mode=%o", path, (unsigned)mode);
  kafs_dlog(2, "%s: dirpath='%s' base='%s'\n", __func__, dirpath, basepath);
  int ret = kafs_access(fctx, ctx, path, NULL, F_OK, NULL);
  kafs_dlog(2, "%s: access(path) rc=%d\n", __func__, ret);
  if (ret == KAFS_SUCCESS)
  {
    kafs_journal_abort(ctx, jseq, "EEXIST");
    return -EEXIST;
  }
  if (ret != -ENOENT)
  {
    kafs_journal_abort(ctx, jseq, "access=%d", ret);
    return ret;
  }

  kafs_sinode_t *inoent_dir;
  ret = kafs_access(fctx, ctx, dirpath, NULL, W_OK, &inoent_dir);
  kafs_dlog(2, "%s: access(dirpath='%s') rc=%d ino_dir=%u\n", __func__, dirpath, ret,
            (unsigned)(inoent_dir ? (inoent_dir - ctx->c_inotbl) : 0));
  if (ret < 0)
  {
    kafs_journal_abort(ctx, jseq, "parent access=%d", ret);
    return ret;
  }
  kafs_mode_t mode_dir = kafs_ino_mode_get(inoent_dir);
  if (!S_ISDIR(mode_dir))
  {
    kafs_journal_abort(ctx, jseq, "ENOTDIR");
    return -ENOTDIR;
  }
  kafs_inocnt_t ino_new;
  kafs_inode_alloc_lock(ctx);
  ret = kafs_ino_find_free(ctx->c_inotbl, &ino_new, &ctx->c_ino_search,
                           kafs_sb_inocnt_get(ctx->c_superblock));
  if (ret < 0)
  {
    kafs_inode_alloc_unlock(ctx);
    kafs_journal_abort(ctx, jseq, "ino_find_free=%d", ret);
    return ret;
  }

  kafs_dlog(2, "%s: alloc ino=%u\n", __func__, (unsigned)ino_new);
  struct kafs_sinode *inoent_new = &ctx->c_inotbl[ino_new];

  // Reserve/initialize the inode while holding alloc lock so no other thread can allocate it.
  kafs_ino_mode_set(inoent_new, mode);
  kafs_ino_uid_set(inoent_new, fctx->uid);
  kafs_ino_size_set(inoent_new, 0);
  kafs_time_t now = kafs_now();
  kafs_time_t nulltime = {0, 0};
  kafs_ino_atime_set(inoent_new, now);
  kafs_ino_ctime_set(inoent_new, now);
  kafs_ino_mtime_set(inoent_new, now);
  kafs_ino_dtime_set(inoent_new, nulltime);
  kafs_ino_gid_set(inoent_new, fctx->gid);
  kafs_ino_linkcnt_set(inoent_new, 0);
  kafs_ino_blocks_set(inoent_new, 0);
  kafs_ino_dev_set(inoent_new, 0);
  memset(inoent_new->i_blkreftbl, 0, sizeof(inoent_new->i_blkreftbl));

  kafs_inode_alloc_unlock(ctx);

  uint32_t ino_dir_u32 = (uint32_t)(inoent_dir - ctx->c_inotbl);
  uint32_t ino_new_u32 = (uint32_t)ino_new;
  // lock ordering: always by inode number to avoid deadlock with other ops
  if (ino_dir_u32 < ino_new_u32)
  {
    kafs_inode_lock(ctx, ino_dir_u32);
    kafs_inode_lock(ctx, ino_new_u32);
  }
  else
  {
    kafs_inode_lock(ctx, ino_new_u32);
    if (ino_dir_u32 != ino_new_u32)
      kafs_inode_lock(ctx, ino_dir_u32);
  }

  kafs_dlog(2, "%s: dirent_add start dir=%u name='%s'\n", __func__, (unsigned)ino_dir_u32,
            basepath);
  ret = kafs_dirent_add(ctx, inoent_dir, ino_new, basepath);
  kafs_dlog(2, "%s: dirent_add done rc=%d\n", __func__, ret);
  if (ret < 0)
  {
    memset(inoent_new, 0, sizeof(*inoent_new));
    if (ino_dir_u32 < ino_new_u32)
    {
      kafs_inode_unlock(ctx, ino_new_u32);
      kafs_inode_unlock(ctx, ino_dir_u32);
    }
    else
    {
      if (ino_dir_u32 != ino_new_u32)
        kafs_inode_unlock(ctx, ino_dir_u32);
      kafs_inode_unlock(ctx, ino_new_u32);
    }
    kafs_journal_abort(ctx, jseq, "dirent_add=%d", ret);
    return ret;
  }

  if (pino_dir != NULL)
    *pino_dir = inoent_dir - ctx->c_inotbl;
  if (pino_new != NULL)
    *pino_new = ino_new;

  // unlock in reverse order
  if (ino_dir_u32 < ino_new_u32)
  {
    kafs_inode_unlock(ctx, ino_new_u32);
    kafs_inode_unlock(ctx, ino_dir_u32);
  }
  else
  {
    if (ino_dir_u32 != ino_new_u32)
      kafs_inode_unlock(ctx, ino_dir_u32);
    kafs_inode_unlock(ctx, ino_new_u32);
  }

  // Update free inode accounting after allocation (no inode locks held).
  kafs_inode_alloc_lock(ctx);
  (void)kafs_sb_inocnt_free_decr(ctx->c_superblock);
  kafs_sb_wtime_set(ctx->c_superblock, kafs_now());
  kafs_inode_alloc_unlock(ctx);

  kafs_dlog(2, "%s: success ino=%u added to dir ino=%u\n", __func__, (unsigned)ino_new,
            (unsigned)(pino_dir ? *pino_dir : 0));
  kafs_journal_commit(ctx, jseq);
  return KAFS_SUCCESS;
}

static int kafs_op_create(const char *path, mode_t mode, struct fuse_file_info *fi)
{
  kafs_inocnt_t ino_new;
  KAFS_CALL(kafs_create, path, mode | S_IFREG, 0, NULL, &ino_new);
  fi->fh = ino_new;
  return 0;
}

static int kafs_op_mknod(const char *path, mode_t mode, dev_t dev)
{
  KAFS_CALL(kafs_create, path, mode, dev, NULL, NULL);
  return 0;
}

static int kafs_op_truncate(const char *path, off_t size, struct fuse_file_info *fi)
{
  struct fuse_context *fctx = fuse_get_context();
  struct kafs_context *ctx = fctx->private_data;
  kafs_sinode_t *inoent;
  KAFS_CALL(kafs_access, fctx, ctx, path, fi, F_OK, &inoent);
  kafs_inode_lock(ctx, (uint32_t)(inoent - ctx->c_inotbl));
  int rc = kafs_truncate(ctx, inoent, (kafs_off_t)size);
  kafs_inode_unlock(ctx, (uint32_t)(inoent - ctx->c_inotbl));
  return rc == 0 ? 0 : rc;
}

static int kafs_op_mkdir(const char *path, mode_t mode)
{
  struct fuse_context *fctx = fuse_get_context();
  struct kafs_context *ctx = fctx->private_data;
  uint64_t jseq = kafs_journal_begin(ctx, "MKDIR", "path=%s mode=%o", path, (unsigned)mode);
  kafs_inocnt_t ino_dir;
  kafs_inocnt_t ino_new;
  KAFS_CALL(kafs_create, path, mode | S_IFDIR, 0, &ino_dir, &ino_new);
  kafs_sinode_t *inoent_new = &ctx->c_inotbl[ino_new];
  kafs_dlog(2, "%s: created ino=%u mode=%o\n", __func__, (unsigned)ino_new,
            (unsigned)kafs_ino_mode_get(inoent_new));
  // Lock parent + new dir in stable order (dirent_add("..") increments parent linkcnt)
  uint32_t ino_parent = (uint32_t)ino_dir;
  uint32_t ino_new_u32 = (uint32_t)ino_new;
  if (ino_parent < ino_new_u32)
  {
    kafs_inode_lock(ctx, ino_parent);
    kafs_inode_lock(ctx, ino_new_u32);
  }
  else
  {
    kafs_inode_lock(ctx, ino_new_u32);
    kafs_inode_lock(ctx, ino_parent);
  }

  int rc = kafs_dirent_add(ctx, inoent_new, ino_dir, "..");
  if (rc < 0)
  {
    if (ino_parent < ino_new_u32)
    {
      kafs_inode_unlock(ctx, ino_new_u32);
      kafs_inode_unlock(ctx, ino_parent);
    }
    else
    {
      kafs_inode_unlock(ctx, ino_parent);
      kafs_inode_unlock(ctx, ino_new_u32);
    }
    kafs_journal_abort(ctx, jseq, "dirent_add=%d", rc);
    return rc;
  }

  // ".." counts as a link for the new directory too.
  kafs_ino_linkcnt_incr(inoent_new);
  if (ino_parent < ino_new_u32)
  {
    kafs_inode_unlock(ctx, ino_new_u32);
    kafs_inode_unlock(ctx, ino_parent);
  }
  else
  {
    kafs_inode_unlock(ctx, ino_parent);
    kafs_inode_unlock(ctx, ino_new_u32);
  }
  kafs_journal_commit(ctx, jseq);
  return 0;
}

static int kafs_op_rmdir(const char *path)
{
  struct fuse_context *fctx = fuse_get_context();
  struct kafs_context *ctx = fctx->private_data;
  uint64_t jseq = kafs_journal_begin(ctx, "RMDIR", "path=%s", path);
  char path_copy[strlen(path) + 1];
  strcpy(path_copy, path);
  const char *dirpath = path_copy;
  char *basepath = strrchr(path_copy, '/');
  if (dirpath == basepath)
    dirpath = "/";
  *basepath = '\0';
  basepath++;

  kafs_sinode_t *inoent;
  KAFS_CALL(kafs_access, fctx, ctx, path, NULL, F_OK, &inoent);
  kafs_mode_t mode = kafs_ino_mode_get(inoent);
  if (!S_ISDIR(mode))
  {
    kafs_journal_abort(ctx, jseq, "ENOTDIR");
    return -ENOTDIR;
  }
  kafs_sinode_t *inoent_dir;
  KAFS_CALL(kafs_access, fctx, ctx, dirpath, NULL, W_OK, &inoent_dir);

  // lock parent then target dir in stable order by inode number to avoid deadlock
  uint32_t ino_parent = (uint32_t)(inoent_dir - ctx->c_inotbl);
  uint32_t ino_target = (uint32_t)(inoent - ctx->c_inotbl);
  if (ino_parent < ino_target)
  {
    kafs_inode_lock(ctx, ino_parent);
    kafs_inode_lock(ctx, ino_target);
  }
  else
  {
    kafs_inode_lock(ctx, ino_target);
    kafs_inode_lock(ctx, ino_parent);
  }

  // Verify directory emptiness under lock (TOCTOU-safe)
  struct kafs_sdirent dirent;
  off_t offset = 0;
  while (1)
  {
    ssize_t r = kafs_dirent_read(ctx, inoent, &dirent, offset);
    if (r < 0)
    {
      kafs_inode_unlock(ctx, ino_target);
      kafs_inode_unlock(ctx, ino_parent);
      kafs_journal_abort(ctx, jseq, "dirent_read=%zd", r);
      return (int)r;
    }
    if (r == 0)
      break;
    if (strcmp(dirent.d_filename, "..") != 0)
    {
      kafs_inode_unlock(ctx, ino_target);
      kafs_inode_unlock(ctx, ino_parent);
      kafs_journal_abort(ctx, jseq, "ENOTEMPTY");
      return -ENOTEMPTY;
    }
    offset += r;
  }

  int rc = kafs_dirent_remove(ctx, inoent_dir, basepath);
  if (rc < 0)
  {
    kafs_inode_unlock(ctx, ino_target);
    kafs_inode_unlock(ctx, ino_parent);
    kafs_journal_abort(ctx, jseq, "dirent_remove(parent)=%d", rc);
    return rc;
  }
  rc = kafs_dirent_remove(ctx, inoent, "..");
  if (rc < 0)
  {
    kafs_inode_unlock(ctx, ino_target);
    kafs_inode_unlock(ctx, ino_parent);
    kafs_journal_abort(ctx, jseq, "dirent_remove(dotdot)=%d", rc);
    return rc;
  }

  // Unlock in reverse order of acquisition
  if (ino_parent < ino_target)
  {
    kafs_inode_unlock(ctx, ino_target);
    kafs_inode_unlock(ctx, ino_parent);
  }
  else
  {
    kafs_inode_unlock(ctx, ino_parent);
    kafs_inode_unlock(ctx, ino_target);
  }
  kafs_journal_commit(ctx, jseq);
  return 0;
}

static int kafs_op_readlink(const char *path, char *buf, size_t buflen)
{
  struct fuse_context *fctx = fuse_get_context();
  struct kafs_context *ctx = fctx->private_data;
  kafs_sinode_t *inoent;
  KAFS_CALL(kafs_access, fctx, ctx, path, NULL, F_OK, &inoent);
  uint32_t ino = (uint32_t)(inoent - ctx->c_inotbl);
  kafs_inode_lock(ctx, ino);
  ssize_t r = KAFS_CALL(kafs_pread, ctx, inoent, buf, buflen - 1, 0);
  kafs_inode_unlock(ctx, ino);
  buf[r] = '\0';
  return 0;
}

static int kafs_op_read(const char *path, char *buf, size_t size, off_t offset,
                        struct fuse_file_info *fi)
{
  (void)path;
  struct fuse_context *fctx = fuse_get_context();
  struct kafs_context *ctx = fctx->private_data;
  kafs_inocnt_t ino = fi->fh;
  kafs_inode_lock(ctx, (uint32_t)ino);
  ssize_t rr = kafs_pread(ctx, &ctx->c_inotbl[ino], buf, size, offset);
  kafs_inode_unlock(ctx, (uint32_t)ino);
  return rr;
}

static int kafs_op_write(const char *path, const char *buf, size_t size, off_t offset,
                         struct fuse_file_info *fi)
{
  (void)path;
  struct fuse_context *fctx = fuse_get_context();
  struct kafs_context *ctx = fctx->private_data;
  kafs_inocnt_t ino = fi->fh;
  if ((fi->flags & O_ACCMODE) == O_RDONLY)
    return -EACCES;
  kafs_inode_lock(ctx, (uint32_t)ino);
  int rc_write = kafs_pwrite(ctx, &ctx->c_inotbl[ino], buf, size, offset);
  kafs_inode_unlock(ctx, (uint32_t)ino);
  return rc_write;
}

static int kafs_op_utimens(const char *path, const struct timespec tv[2], struct fuse_file_info *fi)
{
  struct fuse_context *fctx = fuse_get_context();
  struct kafs_context *ctx = fctx->private_data;
  kafs_sinode_t *inoent;
  KAFS_CALL(kafs_access, fctx, ctx, path, fi, F_OK, &inoent);
  uint32_t ino = (uint32_t)(inoent - ctx->c_inotbl);
  kafs_inode_lock(ctx, ino);

  kafs_time_t now = {0, 0};
  if (tv[0].tv_nsec == UTIME_NOW || tv[1].tv_nsec == UTIME_NOW)
    now = kafs_now();
  switch (tv[0].tv_nsec)
  {
  case UTIME_NOW:
    kafs_ino_atime_set(inoent, now);
    break;
  case UTIME_OMIT:
    break;
  default:
    kafs_ino_atime_set(inoent, tv[0]);
    break;
  }
  switch (tv[1].tv_nsec)
  {
  case UTIME_NOW:
    kafs_ino_mtime_set(inoent, now);
    break;
  case UTIME_OMIT:
    break;
  default:
    kafs_ino_mtime_set(inoent, tv[1]);
    break;
  }

  kafs_inode_unlock(ctx, ino);
  return 0;
}

static int kafs_op_unlink(const char *path)
{
  assert(path != NULL);
  assert(path[0] == '/');
  assert(path[1] != '\0');
  struct fuse_context *fctx = fuse_get_context();
  struct kafs_context *ctx = fctx->private_data;
  uint64_t jseq = kafs_journal_begin(ctx, "UNLINK", "path=%s", path);
  char path_copy[strlen(path) + 1];
  strcpy(path_copy, path);
  const char *dirpath = path_copy;
  char *basepath = strrchr(path_copy, '/');
  if (dirpath == basepath)
    dirpath = "/";
  *basepath = '\0';
  basepath++;

  if (strcmp(basepath, ".") == 0 || strcmp(basepath, "..") == 0)
  {
    kafs_journal_abort(ctx, jseq, "EINVAL");
    return -EINVAL;
  }

  kafs_sinode_t *inoent_dir;
  KAFS_CALL(kafs_access, fctx, ctx, dirpath, NULL, W_OK, &inoent_dir);
  uint32_t ino_dir = (uint32_t)(inoent_dir - ctx->c_inotbl);

  kafs_inocnt_t target_ino = KAFS_INO_NONE;
  kafs_sinode_t *inoent_target = NULL;

  kafs_inode_lock(ctx, ino_dir);
  int s = kafs_dirent_search(ctx, inoent_dir, basepath, (kafs_filenamelen_t)strlen(basepath),
                             &inoent_target);
  if (s < 0)
  {
    kafs_inode_unlock(ctx, ino_dir);
    kafs_journal_abort(ctx, jseq, "ENOENT");
    return s;
  }
  target_ino = (kafs_inocnt_t)(inoent_target - ctx->c_inotbl);
  // unlink(2) should not remove directories
  if (S_ISDIR(kafs_ino_mode_get(inoent_target)))
  {
    kafs_inode_unlock(ctx, ino_dir);
    kafs_journal_abort(ctx, jseq, "EISDIR");
    return -EISDIR;
  }

  kafs_inocnt_t removed_ino = KAFS_INO_NONE;
  KAFS_CALL(kafs_dirent_remove_nolink, ctx, inoent_dir, basepath, &removed_ino);
  kafs_inode_unlock(ctx, ino_dir);

  if (removed_ino != target_ino)
    return -ESTALE;

  // Decrement link count under target inode lock (keep dir lock hold time short)
  kafs_inode_lock(ctx, (uint32_t)removed_ino);
  (void)kafs_ino_linkcnt_decr(&ctx->c_inotbl[removed_ino]);
  kafs_inode_unlock(ctx, (uint32_t)removed_ino);

  kafs_journal_commit(ctx, jseq);
  return 0;
}

static int kafs_op_access(const char *path, int mode)
{
  struct fuse_context *fctx = fuse_get_context();
  struct kafs_context *ctx = fctx->private_data;
  KAFS_CALL(kafs_access, fctx, ctx, path, NULL, mode, NULL);
  return 0;
}

static int kafs_op_rename(const char *from, const char *to, unsigned int flags)
{
  // 最小実装: 通常ファイルのみ対応。RENAME_NOREPLACE は尊重。その他のフラグは未対応。
  struct fuse_context *fctx = fuse_get_context();
  struct kafs_context *ctx = fctx->private_data;
  if (from == NULL || to == NULL || from[0] != '/' || to[0] != '/')
    return -EINVAL;
  if (strcmp(from, to) == 0)
    return 0;

  const unsigned int supported = RENAME_NOREPLACE;
  if (flags & ~supported)
    return -EOPNOTSUPP;

  // 事前に対象 inode を確認（ディレクトリは未対応）
  kafs_sinode_t *inoent_src;
  int rc = kafs_access(fctx, ctx, from, NULL, F_OK, &inoent_src);
  if (rc < 0)
    return rc;
  kafs_mode_t src_mode = kafs_ino_mode_get(inoent_src);
  if (!S_ISREG(src_mode) && !S_ISLNK(src_mode))
    return -EOPNOTSUPP; // ディレクトリ等は未対応

  // パス分解
  char from_copy[strlen(from) + 1];
  strcpy(from_copy, from);
  const char *from_dir = from_copy;
  char *from_base = strrchr(from_copy, '/');
  if (from_dir == from_base)
    from_dir = "/";
  *from_base = '\0';
  from_base++;

  char to_copy[strlen(to) + 1];
  strcpy(to_copy, to);
  const char *to_dir = to_copy;
  char *to_base = strrchr(to_copy, '/');
  if (to_dir == to_base)
    to_dir = "/";
  *to_base = '\0';
  to_base++;

  uint64_t jseq = kafs_journal_begin(ctx, "RENAME", "from=%s to=%s flags=%u", from, to, flags);

  kafs_sinode_t *inoent_dir_from;
  KAFS_CALL(kafs_access, fctx, ctx, from_dir, NULL, W_OK, &inoent_dir_from);
  kafs_sinode_t *inoent_dir_to;
  KAFS_CALL(kafs_access, fctx, ctx, to_dir, NULL, W_OK, &inoent_dir_to);

  // RENAME_NOREPLACE: 既存なら EEXIST
  if (flags & RENAME_NOREPLACE)
  {
    kafs_sinode_t *inoent_tmp;
    int ex = kafs_access(fctx, ctx, to, NULL, F_OK, &inoent_tmp);
    if (ex == 0)
    {
      kafs_journal_abort(ctx, jseq, "EEXIST");
      return -EEXIST;
    }
    if (ex != -ENOENT)
    {
      kafs_journal_abort(ctx, jseq, "access(to)=%d", ex);
      return ex;
    }
  }

  // 取得しておく（from の inode番号）
  kafs_inocnt_t ino_src = (kafs_inocnt_t)(inoent_src - ctx->c_inotbl);

  // 置換先が存在する場合は削除（通常ファイルのみ）
  kafs_sinode_t *inoent_to_exist = NULL;
  int exists_to = kafs_access(fctx, ctx, to, NULL, F_OK, &inoent_to_exist);
  if (exists_to == 0)
  {
    kafs_mode_t dst_mode = kafs_ino_mode_get(inoent_to_exist);
    if (!S_ISREG(dst_mode) && !S_ISLNK(dst_mode))
    {
      kafs_journal_abort(ctx, jseq, "DST_NOT_FILE");
      return -EOPNOTSUPP;
    }
  }

  // ロック順序: 親ディレクトリ２つを inode番号で昇順に、最後に対象 inode
  uint32_t ino_from_dir = (uint32_t)(inoent_dir_from - ctx->c_inotbl);
  uint32_t ino_to_dir = (uint32_t)(inoent_dir_to - ctx->c_inotbl);
  if (ino_from_dir < ino_to_dir)
  {
    kafs_inode_lock(ctx, ino_from_dir);
    if (ino_to_dir != ino_from_dir)
      kafs_inode_lock(ctx, ino_to_dir);
  }
  else
  {
    kafs_inode_lock(ctx, ino_to_dir);
    if (ino_to_dir != ino_from_dir)
      kafs_inode_lock(ctx, ino_from_dir);
  }

  // 置換先が存在していればエントリを除去（rename では moved inode の linkcount は変えない）
  kafs_inocnt_t removed_dst_ino = KAFS_INO_NONE;
  if (exists_to == 0)
  {
    // Remove dirent only; decrement linkcount later under inode lock.
    KAFS_CALL(kafs_dirent_remove_nolink, ctx, inoent_dir_to, to_base, &removed_dst_ino);
  }
  // from から削除（rename ではリンク数を変更しない）
  kafs_inocnt_t moved_ino = KAFS_INO_NONE;
  KAFS_CALL(kafs_dirent_remove_nolink, ctx, inoent_dir_from, from_base, &moved_ino);
  if (moved_ino != ino_src)
    return -ESTALE;
  // to に追加（rename ではリンク数を変更しない）
  KAFS_CALL(kafs_dirent_add_nolink, ctx, inoent_dir_to, ino_src, to_base);

  // ロック解除
  if (ino_from_dir < ino_to_dir)
  {
    if (ino_to_dir != ino_from_dir)
      kafs_inode_unlock(ctx, ino_to_dir);
    kafs_inode_unlock(ctx, ino_from_dir);
  }
  else
  {
    if (ino_to_dir != ino_from_dir)
      kafs_inode_unlock(ctx, ino_from_dir);
    kafs_inode_unlock(ctx, ino_to_dir);
  }

  // If we replaced an existing dst, decrement its linkcount under inode lock.
  if (removed_dst_ino != KAFS_INO_NONE)
  {
    kafs_inode_lock(ctx, (uint32_t)removed_dst_ino);
    (void)kafs_ino_linkcnt_decr(&ctx->c_inotbl[removed_dst_ino]);
    kafs_inode_unlock(ctx, (uint32_t)removed_dst_ino);
  }

  kafs_journal_commit(ctx, jseq);
  return 0;
}

static int kafs_op_chmod(const char *path, mode_t mode, struct fuse_file_info *fi)
{
  struct fuse_context *fctx = fuse_get_context();
  struct kafs_context *ctx = fctx->private_data;
  uint64_t jseq = kafs_journal_begin(ctx, "CHMOD", "path=%s mode=%o", path, (unsigned)mode);
  kafs_sinode_t *inoent;
  KAFS_CALL(kafs_access, fctx, ctx, path, fi, F_OK, &inoent);
  uint32_t ino = (uint32_t)(inoent - ctx->c_inotbl);
  kafs_inode_lock(ctx, ino);
  kafs_mode_t m = kafs_ino_mode_get(inoent);
  kafs_ino_mode_set(inoent, (m & S_IFMT) | mode);
  kafs_inode_unlock(ctx, ino);
  kafs_journal_commit(ctx, jseq);
  return 0;
}

static int kafs_op_chown(const char *path, uid_t uid, gid_t gid, struct fuse_file_info *fi)
{
  struct fuse_context *fctx = fuse_get_context();
  struct kafs_context *ctx = fctx->private_data;
  uint64_t jseq =
      kafs_journal_begin(ctx, "CHOWN", "path=%s uid=%u gid=%u", path, (unsigned)uid, (unsigned)gid);
  kafs_sinode_t *inoent;
  KAFS_CALL(kafs_access, fctx, ctx, path, fi, F_OK, &inoent);
  uint32_t ino = (uint32_t)(inoent - ctx->c_inotbl);
  kafs_inode_lock(ctx, ino);
  kafs_ino_uid_set(inoent, uid);
  kafs_ino_gid_set(inoent, gid);
  kafs_inode_unlock(ctx, ino);
  kafs_journal_commit(ctx, jseq);
  return 0;
}

static int kafs_op_symlink(const char *target, const char *linkpath)
{
  struct fuse_context *fctx = fuse_get_context();
  struct kafs_context *ctx = fctx->private_data;
  uint64_t jseq = kafs_journal_begin(ctx, "SYMLINK", "target=%s linkpath=%s", target, linkpath);
  kafs_inocnt_t ino;
  KAFS_CALL(kafs_create, linkpath, 0777 | S_IFLNK, 0, NULL, &ino);
  kafs_sinode_t *inoent = &ctx->c_inotbl[ino];
  ssize_t w = KAFS_CALL(kafs_pwrite, ctx, inoent, target, strlen(target), 0);
  assert(w == (ssize_t)strlen(target));
  kafs_journal_commit(ctx, jseq);
  return 0;
}

static int kafs_op_flush(const char *path, struct fuse_file_info *fi)
{
  (void)path;
  (void)fi;
  struct fuse_context *fctx = fuse_get_context();
  struct kafs_context *ctx = fctx->private_data;
  if (!ctx || ctx->c_fd < 0)
    return 0;
  // Ensure mmap'd metadata (superblock/inodes/bitmap/HRL) is persisted.
  if (ctx->c_img_base && ctx->c_img_size)
  {
    // Best-effort: ignore msync errors and rely on fsync fallback.
    (void)msync((void *)ctx->c_img_base, ctx->c_img_size, MS_SYNC);
  }
  return (fsync(ctx->c_fd) == 0) ? 0 : -errno;
}

static int kafs_op_fsync(const char *path, int isdatasync, struct fuse_file_info *fi)
{
  (void)path;
  (void)isdatasync;
  (void)fi;
  struct fuse_context *fctx = fuse_get_context();
  struct kafs_context *ctx = fctx->private_data;
  if (!ctx || ctx->c_fd < 0)
    return 0;
  if (ctx->c_img_base && ctx->c_img_size)
  {
    (void)msync((void *)ctx->c_img_base, ctx->c_img_size, MS_SYNC);
  }
  return (fsync(ctx->c_fd) == 0) ? 0 : -errno;
}

static int kafs_op_fsyncdir(const char *path, int isdatasync, struct fuse_file_info *fi)
{
  return kafs_op_fsync(path, isdatasync, fi);
}

static int kafs_op_release(const char *path, struct fuse_file_info *fi)
{
  return kafs_op_flush(path, fi);
}

static struct fuse_operations kafs_operations = {
    .getattr = kafs_op_getattr,
    .statfs = kafs_op_statfs,
    .open = kafs_op_open,
    .create = kafs_op_create,
    .mknod = kafs_op_mknod,
    .readlink = kafs_op_readlink,
    .read = kafs_op_read,
    .write = kafs_op_write,
    .flush = kafs_op_flush,
    .fsync = kafs_op_fsync,
    .release = kafs_op_release,
    .opendir = kafs_op_opendir,
    .readdir = kafs_op_readdir,
    .fsyncdir = kafs_op_fsyncdir,
    .utimens = kafs_op_utimens,
    .truncate = kafs_op_truncate,
    .rename = kafs_op_rename,
    .unlink = kafs_op_unlink,
    .mkdir = kafs_op_mkdir,
    .rmdir = kafs_op_rmdir,
    .access = kafs_op_access,
    .chmod = kafs_op_chmod,
    .chown = kafs_op_chown,
    .symlink = kafs_op_symlink,
    .ioctl = kafs_op_ioctl,
};

static void usage(const char *prog)
{
  fprintf(stderr,
          "Usage: %s [--image <image>|--image=<image>] <mountpoint> [FUSE options...]\n"
          "       %s <image> <mountpoint> [FUSE options...] (mount helper compatible)\n"
          "       env KAFS_IMAGE can be used as fallback image path.\n"
          "       default runs multithreaded; pass -s or set env KAFS_MT=0 for single-thread.\n"
          "Examples:\n"
          "  %s --image test.img mnt -f\n",
          prog, prog, prog);
}

static void kafs_signal_handler(int sig)
{
  const char *name = strsignal(sig);
  kafs_log(KAFS_LOG_ERR, "kafs: caught signal %d (%s)\n", sig, name ? name : "?");
#ifdef __linux__
  void *bt[64];
  int n = backtrace(bt, (int)(sizeof(bt) / sizeof(bt[0])));
  char **syms = backtrace_symbols(bt, n);
  if (syms)
  {
    for (int i = 0; i < n; ++i)
      kafs_log(KAFS_LOG_ERR, "  bt[%02d]=%s\n", i, syms[i]);
  }
#endif
  signal(sig, SIG_DFL);
  raise(sig);
}

static void kafs_install_crash_handlers(void)
{
  struct sigaction sa;
  memset(&sa, 0, sizeof(sa));
  sa.sa_handler = kafs_signal_handler;
  sigemptyset(&sa.sa_mask);
  sigaction(SIGSEGV, &sa, NULL);
  sigaction(SIGABRT, &sa, NULL);
  sigaction(SIGBUS, &sa, NULL);
  sigaction(SIGILL, &sa, NULL);
  sigaction(SIGFPE, &sa, NULL);
}

int main(int argc, char **argv)
{
  kafs_install_crash_handlers();
  // 画像ファイル指定を受け取る: --image <path> または --image=<path>、環境変数 KAFS_IMAGE
  const char *image_path = getenv("KAFS_IMAGE");
  // argv から --image と --help を取り除き fuse_main へは渡さない
  char *argv_clean[argc];
  int argc_clean = 0;
  kafs_bool_t show_help = KAFS_FALSE;
  for (int i = 0; i < argc; ++i)
  {
    const char *a = argv[i];
    if (strcmp(a, "--help") == 0 || strcmp(a, "-h") == 0)
    {
      show_help = KAFS_TRUE;
      continue;
    }
    if (strcmp(a, "--image") == 0)
    {
      if (i + 1 < argc)
      {
        image_path = argv[++i];
        continue;
      }
      else
      {
        fprintf(stderr, "--image requires a path argument.\n");
        usage(argv[0]);
        return 2;
      }
    }
    if (strncmp(a, "--image=", 8) == 0)
    {
      image_path = a + 8;
      continue;
    }
    // pass other args through to FUSE
    argv_clean[argc_clean++] = argv[i];
  }
  // mount(8) helper compatibility: allow "kafs <image> <mountpoint> [FUSE options...]"
  // When invoked via mount -t fuse.kafs, the image path is typically passed as the first argument.
  if (image_path == NULL && argc_clean >= 3 && argv_clean[1][0] != '-')
  {
    image_path = argv_clean[1];
    for (int i = 1; i + 1 < argc_clean; ++i)
      argv_clean[i] = argv_clean[i + 1];
    argc_clean--;
  }
  if (show_help || image_path == NULL || argc_clean < 2)
  {
    usage(argv[0]);
    return 2;
  }

  static kafs_context_t ctx;
  ctx.c_fd = open(image_path, O_RDWR, 0666);
  if (ctx.c_fd < 0)
  {
    perror("open image");
    fprintf(stderr, "image not found. run mkfs.kafs first.\n");
    exit(2);
  }
  ctx.c_blo_search = 0;
  ctx.c_ino_search = 0;

  // まずスーパーブロックだけ読み出してレイアウトを決定
  kafs_ssuperblock_t sbdisk;
  ssize_t r = pread(ctx.c_fd, &sbdisk, sizeof(sbdisk), 0);
  if (r != (ssize_t)sizeof(sbdisk))
  {
    perror("pread superblock");
    exit(2);
  }
  if (kafs_sb_magic_get(&sbdisk) != KAFS_MAGIC)
  {
    fprintf(stderr, "invalid magic. run mkfs.kafs to format.\n");
    exit(2);
  }
  if (kafs_sb_format_version_get(&sbdisk) != KAFS_FORMAT_VERSION)
  {
    fprintf(stderr, "unsupported format version. expected %u.\n", (unsigned)KAFS_FORMAT_VERSION);
    exit(2);
  }
  // 読み出し値からレイアウト計算
  kafs_logblksize_t log_blksize = kafs_sb_log_blksize_get(&sbdisk);
  kafs_blksize_t blksize = 1u << log_blksize;
  kafs_blksize_t blksizemask = blksize - 1u;
  kafs_inocnt_t inocnt = kafs_inocnt_stoh(sbdisk.s_inocnt);
  kafs_blkcnt_t r_blkcnt = kafs_blkcnt_stoh(sbdisk.s_r_blkcnt);

  off_t mapsize = 0;
  mapsize += sizeof(kafs_ssuperblock_t);
  mapsize = (mapsize + blksizemask) & ~blksizemask;
  void *blkmask_off = (void *)mapsize;
  assert(sizeof(kafs_blkmask_t) <= 8);
  mapsize += (r_blkcnt + 7) >> 3;
  mapsize = (mapsize + 7) & ~7;
  mapsize = (mapsize + blksizemask) & ~blksizemask;
  void *inotbl_off = (void *)mapsize;
  mapsize += sizeof(kafs_sinode_t) * inocnt;
  mapsize = (mapsize + blksizemask) & ~blksizemask;
  // Full-image mapping size: use r_blkcnt * blksize (and ensure it also covers HRL/journal)
  off_t imgsize = (off_t)r_blkcnt << log_blksize;
  {
    uint64_t idx_off = kafs_sb_hrl_index_offset_get(&sbdisk);
    uint64_t idx_size = kafs_sb_hrl_index_size_get(&sbdisk);
    uint64_t ent_off = kafs_sb_hrl_entry_offset_get(&sbdisk);
    uint64_t ent_cnt = kafs_sb_hrl_entry_cnt_get(&sbdisk);
    uint64_t ent_size = ent_cnt * (uint64_t)sizeof(kafs_hrl_entry_t);
    uint64_t j_off = kafs_sb_journal_offset_get(&sbdisk);
    uint64_t j_size = kafs_sb_journal_size_get(&sbdisk);
    uint64_t end1 = (idx_off && idx_size) ? (idx_off + idx_size) : 0;
    uint64_t end2 = (ent_off && ent_size) ? (ent_off + ent_size) : 0;
    uint64_t end3 = (j_off && j_size) ? (j_off + j_size) : 0;
    uint64_t max_end = end1;
    if (end2 > max_end)
      max_end = end2;
    if (end3 > max_end)
      max_end = end3;
    if ((off_t)max_end > imgsize)
      imgsize = (off_t)max_end;
    imgsize = (imgsize + blksizemask) & ~blksizemask;
  }

  ctx.c_img_base = mmap(NULL, imgsize, PROT_READ | PROT_WRITE, MAP_SHARED, ctx.c_fd, 0);
  if (ctx.c_img_base == MAP_FAILED)
  {
    perror("mmap");
    exit(2);
  }
  ctx.c_img_size = (size_t)imgsize;

  // Keep existing pointers for metadata access
  ctx.c_superblock = (kafs_ssuperblock_t *)ctx.c_img_base;
  ctx.c_mapsize = (size_t)mapsize; // metadata region length (subset of img)
  ctx.c_blkmasktbl = (void *)ctx.c_superblock + (intptr_t)blkmask_off;
  ctx.c_inotbl = (void *)ctx.c_superblock + (intptr_t)inotbl_off;

  // HRL オープン
  (void)kafs_hrl_open(&ctx);

  // Journal 初期化（KAFS_JOURNAL=0/1/パス）
  (void)kafs_journal_init(&ctx, image_path);

  // 起動時リプレイ（in-image のみ対象）。致命的ではなくベストエフォート。
  (void)kafs_journal_replay(&ctx, NULL, NULL);

  // 画像ファイルに排他ロック（複数プロセスによる同時RW起動を防止）
  struct flock lk = {0};
  lk.l_type = F_WRLCK;
  lk.l_whence = SEEK_SET;
  lk.l_start = 0;
  lk.l_len = 0; // whole file
  if (fcntl(ctx.c_fd, F_SETLK, &lk) == -1)
  {
    perror("fcntl(F_SETLK)");
    fprintf(stderr, "image '%s' is busy (already mounted?).\n", image_path);
    exit(2);
  }

  // 既定はマルチスレッド。-s 指定時は単一スレッド。
  // (環境 KAFS_MT=0 で明示的に単一スレッドへ落とす)
  const char *mt = getenv("KAFS_MT");
  kafs_bool_t enable_mt = (mt && strcmp(mt, "0") == 0) ? KAFS_FALSE : KAFS_TRUE;
  int saw_single = 0;
  for (int i = 0; i < argc_clean; ++i)
  {
    if (strcmp(argv_clean[i], "-s") == 0)
    {
      saw_single = 1;
      break;
    }
  }
  // 余裕を持って追加オプション(-s, -o max_threads=)用のスロットを確保
  char *argv_fuse[argc_clean + 6];
  for (int i = 0; i < argc_clean; ++i)
    argv_fuse[i] = argv_clean[i];
  int argc_fuse = argc_clean;
  if (!enable_mt && !saw_single)
  {
    argv_fuse[argc_fuse++] = "-s";
  }
  if (enable_mt && saw_single)
  {
    // -s takes precedence for predictable CLI behavior.
    enable_mt = KAFS_FALSE;
  }
  if (kafs_debug_level() >= 3)
  {
    argv_fuse[argc_fuse++] = "-d"; // libfuse debug
  }
  // MT有効時は max_threads を明示（libfuseの既定/奇妙な値を避ける）
  char mt_opt_buf[64];
  if (enable_mt)
  {
    unsigned mt_cnt = 16; // デフォルト
    const char *mt_env = getenv("KAFS_MAX_THREADS");
    if (mt_env && *mt_env)
    {
      char *endp = NULL;
      unsigned long v = strtoul(mt_env, &endp, 10);
      if (endp && *endp == '\0')
        mt_cnt = (unsigned)v;
    }
    if (mt_cnt < 1)
      mt_cnt = 1;
    if (mt_cnt > 100000)
      mt_cnt = 100000;
    // 安全のため -o と値を分けて渡す（-omax_threads= 形式のパース差異を回避）
    snprintf(mt_opt_buf, sizeof(mt_opt_buf), "max_threads=%u", mt_cnt);
    argv_fuse[argc_fuse++] = "-o";
    argv_fuse[argc_fuse++] = mt_opt_buf;
    kafs_log(KAFS_LOG_INFO, "kafs: enabling multithread with -o %s\n", mt_opt_buf);
  }
  // 起動引数を一度だけ情報ログに出力（デバッグ用途）
  if (kafs_debug_level() >= 1)
  {
    kafs_log(KAFS_LOG_INFO, "kafs: fuse argv (%d):\n", argc_fuse);
    for (int i = 0; i < argc_fuse; ++i)
    {
      kafs_log(KAFS_LOG_INFO, "  argv[%d]=%s\n", i, argv_fuse[i]);
    }
  }
  argv_fuse[argc_fuse] = NULL; // ensure NULL-terminated argv for libfuse safety
  int rc = fuse_main(argc_fuse, argv_fuse, &kafs_operations, &ctx);
  kafs_journal_shutdown(&ctx);
  if (ctx.c_img_base && ctx.c_img_base != MAP_FAILED)
    munmap(ctx.c_img_base, ctx.c_img_size);
  return rc;
}
