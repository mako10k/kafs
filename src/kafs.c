#include "kafs.h"
#include "kafs_context.h"
#include "kafs_superblock.h"
#include "kafs_block.h"
#include "kafs_inode.h"
#include "kafs_dirent.h"
#include "kafs_hash.h"

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
#include <stdlib.h>
#include <stddef.h>

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
static int kafs_blk_read(struct kafs_context *ctx, kafs_blkcnt_t blo, void *buf)
{
  kafs_log(KAFS_LOG_DEBUG, "%s(blo = %" PRIuFAST32 ")\n", __func__, blo);
  assert(ctx != NULL);
  assert(buf != NULL);
  assert(blo < kafs_sb_r_blkcnt_get(ctx->c_superblock));
  assert(kafs_blk_get_usage(ctx, blo));
  kafs_logblksize_t log_blksize = kafs_sb_log_blksize_get(ctx->c_superblock);
  kafs_blksize_t blksize = kafs_sb_blksize_get(ctx->c_superblock);
  if (blo == KAFS_BLO_NONE)
    memset(buf, 0, blksize);
  else
  {
    ssize_t r = KAFS_IOCALL(pread, ctx->c_fd, buf, blksize, blo << log_blksize);
    assert(r == (ssize_t)blksize);
  }
  return KAFS_SUCCESS;
}

/// @brief ブロック単位でデータを書き込む
/// @param ctx コンテキスト
/// @param blo ブロック番号へのポインタ
/// @param buf 書き込むバッファ
/// @return 0: 成功, < 0: 失敗 (-errno)
static int kafs_blk_write(struct kafs_context *ctx, kafs_blkcnt_t blo, const void *buf)
{
  kafs_log(KAFS_LOG_DEBUG, "%s(blo = %" PRIuFAST32 ")\n", __func__, blo);
  assert(ctx != NULL);
  assert(buf != NULL);
  assert(blo != KAFS_INO_NONE);
  assert(blo < kafs_sb_r_blkcnt_get(ctx->c_superblock));
  assert(kafs_blk_get_usage(ctx, blo));
  kafs_logblksize_t log_blksize = kafs_sb_log_blksize_get(ctx->c_superblock);
  kafs_blksize_t blksize = kafs_sb_blksize_get(ctx->c_superblock);
  ssize_t w = KAFS_IOCALL(pwrite, ctx->c_fd, buf, blksize, blo << log_blksize);
  assert(w == (ssize_t)blksize);
  return KAFS_SUCCESS;
}

/// @brief ブロックデータを未使用に変更する
/// @param ctx コンテキスト
/// @param pblo ブロック番号へのポインタ
/// @return 0: 成功, < 0: 失敗 (-errno)
static int kafs_blk_release(struct kafs_context *ctx, kafs_blkcnt_t *pblo)
{
  kafs_log(KAFS_LOG_DEBUG, "%s(*pblo = %" PRIuFAST32 ")\n", __func__, *pblo);
  assert(ctx != NULL);
  assert(pblo != NULL);
  assert(*pblo != KAFS_INO_NONE);
  assert(*pblo < kafs_sb_r_blkcnt_get(ctx->c_superblock));
  assert(kafs_blk_get_usage(ctx, *pblo));
  kafs_blksize_t blksize = kafs_sb_blksize_get(ctx->c_superblock);
  // ブロックを0で埋める
  char zbuf[blksize];
  memset(zbuf, 0, blksize);
  // HRL 管理されていれば参照減算、そうでなければ直接解放
  (void)kafs_hrl_dec_ref_by_blo(ctx, *pblo);
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

    case KAFS_IBLKREF_FUNC_DEL:
      if (blo_data == KAFS_BLO_NONE)
      {
        *pblo = KAFS_BLO_NONE;
        return KAFS_SUCCESS;
      }
      KAFS_CALL(kafs_blk_release, ctx, &blo_data);
      inoent->i_blkreftbl[iblo] = kafs_blkcnt_htos(KAFS_BLO_NONE);
      *pblo = KAFS_BLO_NONE;
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

    case KAFS_IBLKREF_FUNC_DEL:
      if (blo_blkreftbl == KAFS_BLO_NONE)
      {
        *pblo = KAFS_BLO_NONE;
        return KAFS_SUCCESS;
      }
      KAFS_CALL(kafs_blk_read, ctx, blo_blkreftbl, blkreftbl);
      blo_data = kafs_blkcnt_stoh(blkreftbl[iblo]);
      if (blo_data != KAFS_BLO_NONE)
      {
        KAFS_CALL(kafs_blk_release, ctx, &blo_data);
        blkreftbl[iblo] = kafs_blkcnt_htos(blo_data);
      }
      if (!kafs_blk_is_zero(blkreftbl, blksize))
      {
        KAFS_CALL(kafs_blk_write, ctx, blo_blkreftbl, blkreftbl);
        *pblo = KAFS_BLO_NONE;
        return KAFS_SUCCESS;
      }
      KAFS_CALL(kafs_blk_release, ctx, &blo_blkreftbl);
      inoent->i_blkreftbl[12] = kafs_blkcnt_htos(blo_blkreftbl);
      *pblo = KAFS_BLO_NONE;
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

    case KAFS_IBLKREF_FUNC_DEL:
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
      blo_data = kafs_blkcnt_stoh(blkreftbl2[iblo2]);
      if (blo_data == KAFS_BLO_NONE)
      {
        *pblo = KAFS_BLO_NONE;
        return KAFS_SUCCESS;
      }
      KAFS_CALL(kafs_blk_release, ctx, &blo_data);
      blkreftbl2[iblo2] = kafs_blkcnt_htos(KAFS_BLO_NONE);
      if (!kafs_blk_is_zero(blkreftbl2, blksize))
      {
        KAFS_CALL(kafs_blk_write, ctx, blo_blkreftbl2, blkreftbl2);
        *pblo = KAFS_BLO_NONE;
        return KAFS_SUCCESS;
      }
      KAFS_CALL(kafs_blk_release, ctx, &blo_blkreftbl2);
      blkreftbl1[iblo1] = kafs_blkcnt_htos(KAFS_BLO_NONE);
      if (!kafs_blk_is_zero(blkreftbl1, blksize))
      {
        KAFS_CALL(kafs_blk_write, ctx, blo_blkreftbl1, blkreftbl1);
        *pblo = KAFS_BLO_NONE;
        return KAFS_SUCCESS;
      }
      KAFS_CALL(kafs_blk_release, ctx, &blo_blkreftbl1);
      inoent->i_blkreftbl[13] = kafs_blkcnt_htos(KAFS_BLO_NONE);
      *pblo = KAFS_BLO_NONE;
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

  case KAFS_IBLKREF_FUNC_DEL:
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
    blo_data = kafs_blkcnt_stoh(blkreftbl3[iblo3]);
    if (blo_data == KAFS_BLO_NONE)
    {
      *pblo = KAFS_BLO_NONE;
      return KAFS_SUCCESS;
    }
    KAFS_CALL(kafs_blk_release, ctx, &blo_data);
    blkreftbl3[iblo3] = kafs_blkcnt_htos(KAFS_BLO_NONE);
    if (!kafs_blk_is_zero(blkreftbl3, blksize))
    {
      KAFS_CALL(kafs_blk_write, ctx, blo_blkreftbl3, blkreftbl3);
      *pblo = KAFS_BLO_NONE;
      return KAFS_SUCCESS;
    }
    KAFS_CALL(kafs_blk_release, ctx, &blo_blkreftbl3);
    blkreftbl2[iblo2] = kafs_blkcnt_htos(KAFS_BLO_NONE);
    if (!kafs_blk_is_zero(blkreftbl2, blksize))
    {
      KAFS_CALL(kafs_blk_write, ctx, blo_blkreftbl2, blkreftbl2);
      *pblo = KAFS_BLO_NONE;
      return KAFS_SUCCESS;
    }
    KAFS_CALL(kafs_blk_release, ctx, &blo_blkreftbl2);
    blkreftbl1[iblo1] = kafs_blkcnt_htos(KAFS_BLO_NONE);
    if (!kafs_blk_is_zero(blkreftbl1, blksize))
    {
      KAFS_CALL(kafs_blk_write, ctx, blo_blkreftbl1, blkreftbl1);
      *pblo = KAFS_BLO_NONE;
      return KAFS_SUCCESS;
    }
    KAFS_CALL(kafs_blk_release, ctx, &blo_blkreftbl1);
    inoent->i_blkreftbl[14] = kafs_blkcnt_htos(KAFS_BLO_NONE);
    *pblo = KAFS_BLO_NONE;
    return KAFS_SUCCESS;
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
  kafs_log(KAFS_LOG_DEBUG, "%s(ino = %d, iblo = %" PRIuFAST32 ")\n", __func__,
           inoent - ctx->c_inotbl, iblo);
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
  kafs_log(KAFS_LOG_DEBUG, "%s(ino = %d, iblo = %" PRIuFAST32 ")\n", __func__,
           inoent - ctx->c_inotbl, iblo);
  assert(ctx != NULL);
  assert(buf != NULL);
  assert(inoent != NULL);
  assert(kafs_ino_get_usage(inoent));
  assert(kafs_ino_size_get(inoent) > KAFS_DIRECT_SIZE);
  kafs_blksize_t blksize = kafs_sb_blksize_get(ctx->c_superblock);
  // 注意: kafs_blk_is_zero は「非ゼロを含むと1、全ゼロで0」を返す
  if (kafs_blk_is_zero(buf, blksize))
  {
    // 非ゼロ: HRL 経路（失敗時は従来経路）
    kafs_blkcnt_t old_blo;
    KAFS_CALL(kafs_ino_ibrk_run, ctx, inoent, iblo, &old_blo, KAFS_IBLKREF_FUNC_GET);

    kafs_hrid_t hrid;
    int is_new = 0;
    kafs_blkcnt_t new_blo = KAFS_BLO_NONE;
    int rc = kafs_hrl_put(ctx, buf, &hrid, &is_new, &new_blo);
    if (rc == 0)
    {
      KAFS_CALL(kafs_hrl_inc_ref, ctx, hrid);
      if (old_blo != KAFS_BLO_NONE && old_blo != new_blo)
        (void)kafs_hrl_dec_ref_by_blo(ctx, old_blo);
      // 物理ブロック番号を直接設定
      inoent->i_blkreftbl[iblo] = kafs_blkcnt_htos(new_blo);
      return KAFS_SUCCESS;
    }
    // HRL 失敗時: 物理ブロックに直接書き込み
    kafs_blkcnt_t blo;
    KAFS_CALL(kafs_ino_ibrk_run, ctx, inoent, iblo, &blo, KAFS_IBLKREF_FUNC_PUT);
    assert(blo != KAFS_BLO_NONE);
    KAFS_CALL(kafs_blk_write, ctx, blo, buf);
    return KAFS_SUCCESS;
  }
  // 全ゼロ: スパース化（参照削除）
  kafs_blkcnt_t blo;
  KAFS_CALL(kafs_ino_ibrk_run, ctx, inoent, iblo, &blo, KAFS_IBLKREF_FUNC_GET);
  if (blo != KAFS_BLO_NONE)
  {
    (void)kafs_hrl_dec_ref_by_blo(ctx, blo);
    KAFS_CALL(kafs_ino_ibrk_run, ctx, inoent, iblo, &blo, KAFS_IBLKREF_FUNC_DEL);
    assert(blo == KAFS_BLO_NONE);
  }
  return KAFS_SUCCESS;
}

static int kafs_ino_iblk_release(struct kafs_context *ctx, kafs_sinode_t *inoent,
                                 kafs_iblkcnt_t iblo)
{
  kafs_log(KAFS_LOG_DEBUG, "%s(ino = %d, iblo = %" PRIuFAST32 ")\n", __func__,
           inoent - ctx->c_inotbl, iblo);
  assert(ctx != NULL);
  assert(inoent != NULL);
  assert(kafs_ino_get_usage(inoent));
  assert(kafs_ino_size_get(inoent) > KAFS_DIRECT_SIZE);
  kafs_blkcnt_t blo;
  KAFS_CALL(kafs_ino_ibrk_run, ctx, inoent, iblo, &blo, KAFS_IBLKREF_FUNC_GET);
  if (blo != KAFS_BLO_NONE)
  {
    (void)kafs_hrl_dec_ref_by_blo(ctx, blo);
    KAFS_CALL(kafs_ino_ibrk_run, ctx, inoent, iblo, &blo, KAFS_IBLKREF_FUNC_DEL);
    assert(blo == KAFS_BLO_NONE);
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
  kafs_log(KAFS_LOG_DEBUG, "%s(ino = %d, size = %" PRIuFAST64 ", offset = %" PRIuFAST64 ")\n",
           __func__, inoent - ctx->c_inotbl, size, offset);
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
  kafs_log(KAFS_LOG_DEBUG, "%s(ino = %d, filesize_new = %" PRIuFAST64 ")\n", __func__,
           inoent - ctx->c_inotbl, filesize_new);
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
  kafs_iblkcnt_t iblooff = filesize_new >> log_blksize;
  kafs_iblkcnt_t iblocnt = (filesize_orig + blksize - 1) >> log_blksize;
  kafs_blksize_t off = filesize_orig & (blksize - 1);
  if (filesize_orig <= KAFS_DIRECT_SIZE)
  {
    memset((void *)inoent->i_blkreftbl + filesize_new, 0, filesize_orig - filesize_new);
    kafs_ino_size_set(inoent, filesize_new);
    return KAFS_SUCCESS;
  }
  if (filesize_new <= KAFS_DIRECT_SIZE)
  {
    char buf[blksize];
    KAFS_CALL(kafs_ino_iblk_read, ctx, inoent, 0, buf);
    for (kafs_iblkcnt_t iblo = 0; iblo < iblocnt; iblo++)
      KAFS_CALL(kafs_ino_iblk_release, ctx, inoent, iblo);
    memcpy(inoent->i_blkreftbl, buf, filesize_orig - filesize_new);
    kafs_ino_size_set(inoent, filesize_new);
    return KAFS_SUCCESS;
  }

  if (off > 0)
  {
    char buf[blksize];
    KAFS_CALL(kafs_ino_iblk_read, ctx, inoent, iblooff, buf);
    memset(buf + off, 0, blksize - off);
    KAFS_CALL(kafs_ino_iblk_write, ctx, inoent, iblooff, buf);
    iblooff++;
  }
  while (iblooff < iblocnt)
    KAFS_CALL(kafs_ino_iblk_release, ctx, inoent, iblooff++);
  kafs_ino_size_set(inoent, filesize_new);
  return KAFS_SUCCESS;
}

static int kafs_trim(struct kafs_context *ctx, kafs_sinode_t *inoent, kafs_off_t off,
                     kafs_off_t size)
{
  kafs_log(KAFS_LOG_DEBUG, "%s(ino = %d, off = %" PRIuFAST64 ", size = %" PRIuFAST64 ")\n",
           __func__, inoent - ctx->c_inotbl, off, size);
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
  // TODO: 簡易実装なのでしっかり実装していくべき
  char buf[size_orig - (off + size)];
  ssize_t r = KAFS_CALL(kafs_pread, ctx, inoent, buf, size_orig - (off + size), off + size);
  ssize_t w = KAFS_CALL(kafs_pwrite, ctx, inoent, buf, r, off);
  KAFS_CALL(kafs_truncate, ctx, inoent, off + w);
  return KAFS_SUCCESS;
}

static int kafs_release(struct kafs_context *ctx, kafs_sinode_t *inoent)
{
  if (kafs_ino_linkcnt_decr(inoent) == 0)
  {
    KAFS_CALL(kafs_truncate, ctx, inoent, 0);
    memset(inoent, 0, sizeof(struct kafs_sinode));
    // TODO: SuperBlock の free_inode数を増やす
    kafs_sb_blkcnt_free_incr(ctx->c_superblock);
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
  kafs_log(KAFS_LOG_DEBUG, "%s(ino_dir = %d, offset = %" PRIuFAST64 ")\n", __func__,
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
    kafs_log(KAFS_LOG_DEBUG, "%s(short read dirent header)\n", __func__);
    return -EIO;
  }
  kafs_filenamelen_t filenamelen = kafs_dirent_filenamelen_get(dirent);
  ssize_t r2 = KAFS_CALL(kafs_pread, ctx, inoent_dir, dirent->d_filename, filenamelen, offset + r1);
  if (r2 < (ssize_t)filenamelen)
  {
    kafs_log(KAFS_LOG_DEBUG, "%s(short read dirent name)\n", __func__);
    return -EIO;
  }
  dirent->d_filename[r2] = '\0';
  kafs_log(KAFS_LOG_DEBUG,
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
  kafs_log(KAFS_LOG_DEBUG, "%s(ino = %d, filename = %s, filenamelen = %" PRIuFAST16 ")\n", __func__,
           (inoent - ctx->c_inotbl), filename, filenamelen);
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

static int kafs_dirent_add(struct kafs_context *ctx, kafs_sinode_t *inoent_dir, kafs_inocnt_t ino,
                           const char *filename)
{
  kafs_log(KAFS_LOG_DEBUG, "%s(ino_dir = %d, ino = %d, filename = %s)\n", __func__,
           inoent_dir - ctx->c_inotbl, ino, filename);
  assert(ctx != NULL);
  assert(inoent_dir != NULL);
  assert(ino != KAFS_INO_NONE);
  kafs_inocnt_t inocnt = kafs_sb_inocnt_get(ctx->c_superblock);
  assert(ino < inocnt);
  kafs_mode_t mode_dir = kafs_ino_mode_get(inoent_dir);
  if (!S_ISDIR(mode_dir))
    return -ENOTDIR;
  struct kafs_sdirent dirent;
  off_t offset = 0;
  kafs_filenamelen_t filenamelen = strlen(filename);
  while (1)
  {
    ssize_t r = KAFS_CALL(kafs_dirent_read, ctx, inoent_dir, &dirent, offset);
    if (r == 0)
      break;
    kafs_filenamelen_t d_filenamelen = kafs_dirent_filenamelen_get(&dirent);
    const char *d_filename = dirent.d_filename;
    if (r != (ssize_t)(offsetof(kafs_sdirent_t, d_filename) + d_filenamelen))
      return -EIO;
    if (d_filenamelen == filenamelen && memcmp(filename, d_filename, filenamelen) == 0)
      return -EEXIST;
    offset += r;
  }
  kafs_dirent_set(&dirent, ino, filename);
  ssize_t w = KAFS_CALL(kafs_pwrite, ctx, inoent_dir, &dirent,
                        offsetof(kafs_sdirent_t, d_filename) + filenamelen, offset);
  assert(w == (ssize_t)(offsetof(kafs_sdirent_t, d_filename) + filenamelen));
  struct kafs_sinode *inoent = &ctx->c_inotbl[ino];
  kafs_ino_linkcnt_incr(inoent);
  return KAFS_SUCCESS;
}

static int kafs_dirent_remove(struct kafs_context *ctx, kafs_sinode_t *inoent_dir,
                              const char *filename)
{
  kafs_log(KAFS_LOG_DEBUG, "%s(ino_dir = %" PRIuFAST32 ", filename = %s)\n", __func__,
           inoent_dir - ctx->c_inotbl, filename);
  assert(ctx != NULL);
  assert(inoent_dir != NULL);
  assert(filename != NULL);
  assert(kafs_ino_get_usage(inoent_dir));
  kafs_filenamelen_t filenamelen = strlen(filename);
  assert(filenamelen > 0);
  kafs_mode_t mode_dir = kafs_ino_mode_get(inoent_dir);
  if (!S_ISDIR(mode_dir))
    return -ENOTDIR;
  struct kafs_sdirent dirent;
  off_t offset = 0;
  while (1)
  {
    ssize_t r = KAFS_CALL(kafs_dirent_read, ctx, inoent_dir, &dirent, offset);
    if (r == 0)
      break;
    kafs_inocnt_t d_ino = kafs_dirent_ino_get(&dirent);
    kafs_filenamelen_t d_filenamelen = kafs_dirent_filenamelen_get(&dirent);
    const char *d_filename = dirent.d_filename;
    if (r != (ssize_t)(offsetof(kafs_sdirent_t, d_filename) + d_filenamelen))
      return -EIO;
    if (d_filenamelen == filenamelen && memcmp(d_filename, filename, filenamelen) == 0)
    {
      KAFS_CALL(kafs_trim, ctx, inoent_dir, offset, r);
      kafs_ino_linkcnt_decr(&ctx->c_inotbl[d_ino]);
      return KAFS_SUCCESS;
    }
    offset += r;
  }
  return -ENOENT;
}

static int kafs_access_check(int ok, kafs_sinode_t *inoent, kafs_bool_t is_dir, uid_t uid,
                             gid_t gid, size_t ngroups, gid_t groups[])
{
  mode_t mode = kafs_ino_mode_get(inoent);
  uid_t fuid = kafs_ino_uid_get(inoent);
  gid_t fgid = kafs_ino_gid_get(inoent);
  if (is_dir)
  {
    if (!S_ISDIR(mode))
      return -ENOTDIR;
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

static int kafs_access(struct fuse_context *fctx, kafs_context_t *ctx, const char *path,
                       struct fuse_file_info *fi, int ok, kafs_sinode_t **pinoent)
{
  assert(fctx != NULL);
  assert(ctx != NULL);
  assert(path != NULL || fi != NULL);
  assert(*path == '/');

  uid_t uid = fctx->uid;
  gid_t gid = fctx->gid;
  size_t ngroups = KAFS_IOCALL(fuse_getgroups, 0, NULL);
  gid_t groups[ngroups];
  KAFS_IOCALL(fuse_getgroups, ngroups, groups);

  kafs_inocnt_t ino = fi == NULL ? KAFS_INO_ROOTDIR : (kafs_inocnt_t)fi->fh;
  kafs_sinode_t *inoent = &ctx->c_inotbl[ino];
  const char *p = path == NULL ? "" : path + 1;
  while (*p != '\0')
  {
    KAFS_CALL(kafs_access_check, ok, inoent, KAFS_TRUE, uid, gid, ngroups, groups);
    const char *n = strchrnul(p, '/');
    KAFS_CALL(kafs_dirent_search, ctx, inoent, p, n - p, &inoent);
    if (*n == '\0')
      break;
    p = n + 1;
  }
  KAFS_CALL(kafs_access_check, ok, inoent, KAFS_FALSE, uid, gid, ngroups, groups);
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
  st->st_blocks = kafs_ino_blocks_get(inoent);
  st->st_atim = kafs_ino_atime_get(inoent);
  st->st_mtim = kafs_ino_mtime_get(inoent);
  st->st_ctim = kafs_ino_ctime_get(inoent);
  return 0;
}

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
  off_t filesize = kafs_ino_size_get(inoent_dir);
  off_t o = 0;
  kafs_sdirent_t dirent;
  if (filler(buf, ".", NULL, 0, 0))
    return -ENOENT;
  while (o < filesize)
  {
    ssize_t r = KAFS_CALL(kafs_dirent_read, ctx, inoent_dir, &dirent, o);
    kafs_inocnt_t d_ino = kafs_dirent_ino_get(&dirent);
    kafs_filenamelen_t d_filenamelen = kafs_dirent_filenamelen_get(&dirent);
    kafs_log(KAFS_LOG_DEBUG, "%s ino = %d, name = %s, namelen = %d\n", __func__, d_ino,
             dirent.d_filename, d_filenamelen);
    if (filler(buf, dirent.d_filename, NULL, 0, 0))
      return -ENOENT;
    o += r;
  }
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

  int ret = kafs_access(fctx, ctx, path, NULL, F_OK, NULL);
  if (ret == KAFS_SUCCESS)
    return -EEXIST;
  if (ret != -ENOENT)
    return ret;

  kafs_sinode_t *inoent_dir;
  KAFS_CALL(kafs_access, fctx, ctx, dirpath, NULL, W_OK, &inoent_dir);
  kafs_mode_t mode_dir = kafs_ino_mode_get(inoent_dir);
  if (!S_ISDIR(mode_dir))
    return -EIO;
  kafs_inocnt_t ino_new;
  KAFS_CALL(kafs_ino_find_free, ctx->c_inotbl, &ino_new, &ctx->c_ino_search,
            kafs_sb_inocnt_get(ctx->c_superblock));
  struct kafs_sinode *inoent_new = &ctx->c_inotbl[ino_new];
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
  ret = kafs_dirent_add(ctx, inoent_dir, ino_new, basepath);
  if (ret < 0)
  {
    KAFS_CALL(kafs_release, ctx, inoent_new);
    return ret;
  }
  if (pino_dir != NULL)
    *pino_dir = inoent_dir - ctx->c_inotbl;
  if (pino_new != NULL)
    *pino_new = ino_new;
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

static int kafs_op_mkdir(const char *path, mode_t mode)
{
  struct fuse_context *fctx = fuse_get_context();
  struct kafs_context *ctx = fctx->private_data;
  kafs_inocnt_t ino_dir;
  kafs_inocnt_t ino_new;
  KAFS_CALL(kafs_create, path, mode | S_IFDIR, 0, &ino_dir, &ino_new);
  kafs_sinode_t *inoent_new = &ctx->c_inotbl[ino_new];
  KAFS_CALL(kafs_dirent_add, ctx, inoent_new, ino_dir, "..");
  return 0;
}

static int kafs_op_rmdir(const char *path)
{
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

  kafs_sinode_t *inoent;
  KAFS_CALL(kafs_access, fctx, ctx, path, NULL, F_OK, &inoent);
  kafs_mode_t mode = kafs_ino_mode_get(inoent);
  if (!S_ISDIR(mode))
    return -ENOTDIR;
  kafs_sinode_t *inoent_dir;
  KAFS_CALL(kafs_access, fctx, ctx, dirpath, NULL, W_OK, &inoent_dir);

  struct kafs_sdirent dirent;
  off_t offset = 0;
  while (1)
  {
    ssize_t r = KAFS_CALL(kafs_dirent_read, ctx, inoent, &dirent, offset);
    if (r == 0)
      break;
    if (strcmp(dirent.d_filename, "..") != 0)
      return -ENOTEMPTY;
    offset += r;
  }
  KAFS_CALL(kafs_dirent_remove, ctx, inoent_dir, basepath);
  KAFS_CALL(kafs_dirent_remove, ctx, inoent, "..");
  return 0;
}

static int kafs_op_readlink(const char *path, char *buf, size_t buflen)
{
  struct fuse_context *fctx = fuse_get_context();
  struct kafs_context *ctx = fctx->private_data;
  kafs_sinode_t *inoent;
  KAFS_CALL(kafs_access, fctx, ctx, path, NULL, F_OK, &inoent);
  ssize_t r = KAFS_CALL(kafs_pread, ctx, inoent, buf, buflen - 1, 0);
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
  return kafs_pread(ctx, &ctx->c_inotbl[ino], buf, size, offset);
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
  return kafs_pwrite(ctx, &ctx->c_inotbl[ino], buf, size, offset);
}

static int kafs_op_utimens(const char *path, const struct timespec tv[2], struct fuse_file_info *fi)
{
  struct fuse_context *fctx = fuse_get_context();
  struct kafs_context *ctx = fctx->private_data;
  kafs_sinode_t *inoent;
  KAFS_CALL(kafs_access, fctx, ctx, path, fi, F_OK, &inoent);
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
  return 0;
}

static int kafs_op_unlink(const char *path)
{
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

  KAFS_CALL(kafs_access, fctx, ctx, path, NULL, F_OK, NULL);
  kafs_sinode_t *inoent_dir;
  KAFS_CALL(kafs_access, fctx, ctx, dirpath, NULL, W_OK, &inoent_dir);
  KAFS_CALL(kafs_dirent_remove, ctx, inoent_dir, basepath);
  return 0;
}

static int kafs_op_access(const char *path, int mode)
{
  struct fuse_context *fctx = fuse_get_context();
  struct kafs_context *ctx = fctx->private_data;
  KAFS_CALL(kafs_access, fctx, ctx, path, NULL, mode, NULL);
  return 0;
}

static int kafs_op_chmod(const char *path, mode_t mode, struct fuse_file_info *fi)
{
  struct fuse_context *fctx = fuse_get_context();
  struct kafs_context *ctx = fctx->private_data;
  kafs_sinode_t *inoent;
  KAFS_CALL(kafs_access, fctx, ctx, path, fi, F_OK, &inoent);
  kafs_mode_t m = kafs_ino_mode_get(inoent);
  kafs_ino_mode_set(inoent, (m & S_IFMT) | mode);
  return 0;
}

static int kafs_op_chown(const char *path, uid_t uid, gid_t gid, struct fuse_file_info *fi)
{
  struct fuse_context *fctx = fuse_get_context();
  struct kafs_context *ctx = fctx->private_data;
  kafs_sinode_t *inoent;
  KAFS_CALL(kafs_access, fctx, ctx, path, fi, F_OK, &inoent);
  kafs_ino_uid_set(inoent, uid);
  kafs_ino_gid_set(inoent, gid);
  return 0;
}

static int kafs_op_symlink(const char *target, const char *linkpath)
{
  struct fuse_context *fctx = fuse_get_context();
  struct kafs_context *ctx = fctx->private_data;
  kafs_inocnt_t ino;
  KAFS_CALL(kafs_create, linkpath, 0777 | S_IFLNK, 0, NULL, &ino);
  kafs_sinode_t *inoent = &ctx->c_inotbl[ino];
  ssize_t w = KAFS_CALL(kafs_pwrite, ctx, inoent, target, strlen(target), 0);
  assert(w == strlen(target));
  return 0;
}

static struct fuse_operations kafs_operations = {
    .getattr = kafs_op_getattr,
    .open = kafs_op_open,
    .create = kafs_op_create,
    .mknod = kafs_op_mknod,
    .readlink = kafs_op_readlink,
    .read = kafs_op_read,
    .write = kafs_op_write,
    .opendir = kafs_op_opendir,
    .readdir = kafs_op_readdir,
    .utimens = kafs_op_utimens,
    .unlink = kafs_op_unlink,
    .mkdir = kafs_op_mkdir,
    .rmdir = kafs_op_rmdir,
    .access = kafs_op_access,
    .chmod = kafs_op_chmod,
    .chown = kafs_op_chown,
    .symlink = kafs_op_symlink,
};

static void usage(const char *prog)
{
  fprintf(stderr,
          "Usage: %s [--image <image>|--image=<image>] <mountpoint> [FUSE options...]\n"
          "       env KAFS_IMAGE can be used as fallback image path.\n"
          "Examples:\n"
          "  %s --image test.img mnt -f\n",
          prog, prog);
}

int main(int argc, char **argv)
{
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
    argv_clean[argc_clean++] = argv[i];
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

  ctx.c_superblock = mmap(NULL, mapsize, PROT_READ | PROT_WRITE, MAP_SHARED, ctx.c_fd, 0);
  if (ctx.c_superblock == MAP_FAILED)
  {
    perror("mmap");
    exit(2);
  }
  ctx.c_blkmasktbl = (void *)ctx.c_superblock + (intptr_t)blkmask_off;
  ctx.c_inotbl = (void *)ctx.c_superblock + (intptr_t)inotbl_off;

  // HRL オープン
  (void)kafs_hrl_open(&ctx);

  return fuse_main(argc_clean, argv_clean, &kafs_operations, &ctx);
}
