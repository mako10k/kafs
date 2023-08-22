#include "kafs.h"
#include "kafs_context.h"
#include "kafs_superblock.h"
#include "kafs_block.h"
#include "kafs_inode.h"
#include "kafs_dirent.h"

#include <fuse.h>
#include <errno.h>
#include <string.h>
#include <unistd.h>
#include <assert.h>
#include <endian.h>

// ---------------------------------------------------------
// BLOCK OPERATIONS
// ---------------------------------------------------------

/// @brief ブロック単位でデータを読み出す
/// @param ctx コンテキスト
/// @param blo ブロック番号
/// @param buf 読み出すバッファ
/// @return 0: 成功, < 0: 失敗 (-errno)
static int
kafs_blk_read (struct kafs_context *ctx, kafs_blkcnt_t blo, void *buf)
{
  assert (ctx != NULL);
  assert (buf != NULL);
  assert (blo != KAFS_INO_NONE);
  assert (blo < kafs_sb_r_blkcnt_get (ctx->c_superblock));
  assert (kafs_blk_get_usage (ctx, blo));
  kafs_logblksize_t log_blksize = kafs_sb_log_blksize_get (ctx->c_superblock);
  kafs_logblksize_t blksize = kafs_sb_blksize_get (ctx->c_superblock);
  ssize_t r = KAFS_IOCALL (pread, ctx->c_fd, buf, blksize, blo << log_blksize);
  assert (r == blksize);
  return KAFS_SUCCESS;
}

/// @brief ブロック単位でデータを書き込む
/// @param ctx コンテキスト
/// @param blo ブロック番号へのポインタ
/// @param buf 書き込むバッファ
/// @return 0: 成功, < 0: 失敗 (-errno)
static int
kafs_blk_write (struct kafs_context *ctx, kafs_blkcnt_t blo, const void *buf)
{
  assert (ctx != NULL);
  assert (buf != NULL);
  assert (blo != KAFS_INO_NONE);
  assert (blo < kafs_sb_r_blkcnt_get (ctx->c_superblock));
  assert (kafs_blk_get_usage (ctx, blo));
  kafs_logblksize_t log_blksize = kafs_sb_log_blksize_get (ctx->c_superblock);
  kafs_blksize_t blksize = kafs_sb_blksize_get (ctx->c_superblock);
  ssize_t w = KAFS_IOCALL (pwrite, ctx->c_fd, buf, blksize, blo << log_blksize);
  assert (w == blksize);
  return KAFS_SUCCESS;
}

/// @brief ブロックデータを削除する
/// @param ctx コンテキスト
/// @param sblo ブロック番号
/// @return 0: 成功, < 0: 失敗 (-errno)
static int
kafs_blk_release (struct kafs_context *ctx, kafs_blkcnt_t * pblo)
{
  assert (ctx != NULL);
  assert (pblo != NULL);
  assert (*pblo != KAFS_INO_NONE);
  assert (*pblo < kafs_sb_r_blkcnt_get (ctx->c_superblock));
  assert (kafs_blk_get_usage (ctx, *pblo));
  kafs_blksize_t blksize = kafs_sb_blksize_get (ctx->c_superblock);
  char zbuf[blksize];
  memset (zbuf, 0, blksize);
  KAFS_CALL (kafs_blk_write, ctx, *pblo, zbuf);
  KAFS_CALL (kafs_blk_set_usage, ctx, *pblo, KAFS_FALSE);
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

#define KAFS_DIRECT_SIZE sizeof(((struct kafs_sinode *)NULL)->i_blkreftbl)

static int
kafs_blk_is_zero (const void *buf, size_t len)
{
  const char *c = buf;
  while (len--)
    if (*c++)
      return 1;
  return 0;
}

static int
kafs_do_ibrk (struct kafs_context *ctx, kafs_sinode_t * inoent, kafs_iblkcnt_t iblo, kafs_blkcnt_t * pblo,
	      kafs_iblkref_func_t ifunc)
{
  assert (ctx != NULL);
  assert (pblo != NULL);
  assert (inoent != NULL);

  if (iblo < 12)
    {
      kafs_blkcnt_t blo_data = kafs_blkcnt_stoh (inoent->i_blkreftbl[iblo]);
      switch (ifunc)
	{
	case KAFS_IBLKREF_FUNC_GET:
	  *pblo = blo_data;
	  return KAFS_SUCCESS;

	case KAFS_IBLKREF_FUNC_PUT:
	  if (blo_data == KAFS_BLO_NONE)
	    {
	      KAFS_CALL (kafs_blk_alloc, ctx, &blo_data);
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
	  KAFS_CALL (kafs_blk_release, ctx, &blo_data);
	  inoent->i_blkreftbl[iblo] = kafs_blkcnt_htos (KAFS_BLO_NONE);
	  *pblo = KAFS_BLO_NONE;
	  return KAFS_SUCCESS;
	}
    }

  iblo -= 12;
  kafs_blksize_t blksize = kafs_sb_blksize_get (ctx->c_superblock);
  kafs_logblksize_t log_blkrefs_pb = kafs_sb_log_blkref_pb_get (ctx->c_superblock);
  kafs_blkcnt_t blkrefs_pb = kafs_sb_blkref_pb_get (ctx->c_superblock);
  if (iblo < blkrefs_pb)
    {
      kafs_sblkcnt_t blkreftbl[blkrefs_pb];
      kafs_blkcnt_t blo_blkreftbl = kafs_blkcnt_stoh (inoent->i_blkreftbl[12]);
      kafs_blkcnt_t blo_data;

      switch (ifunc)
	{
	case KAFS_IBLKREF_FUNC_GET:
	  if (blo_blkreftbl == KAFS_BLO_NONE)
	    {
	      *pblo = KAFS_BLO_NONE;
	      return KAFS_SUCCESS;
	    }
	  KAFS_CALL (kafs_blk_read, ctx, blo_blkreftbl, blkreftbl);
	  *pblo = kafs_blkcnt_stoh (blkreftbl[iblo]);
	  return KAFS_SUCCESS;

	case KAFS_IBLKREF_FUNC_PUT:
	  if (blo_blkreftbl == KAFS_BLO_NONE)
	    {
	      KAFS_CALL (kafs_blk_alloc, ctx, &blo_blkreftbl);
	      inoent->i_blkreftbl[12] = kafs_blkcnt_htos (blo_blkreftbl);
	      blo_data = KAFS_BLO_NONE;
	    }
	  else
	    {
	      KAFS_CALL (kafs_blk_read, ctx, blo_blkreftbl, blkreftbl);
	      blo_data = kafs_blkcnt_stoh (blkreftbl[iblo]);
	    }
	  if (blo_data == KAFS_BLO_NONE)
	    {
	      KAFS_CALL (kafs_blk_alloc, ctx, &blo_data);
	      blkreftbl[iblo] = kafs_blkcnt_htos (blo_data);
	      KAFS_CALL (kafs_blk_write, ctx, blo_blkreftbl, blkreftbl);
	    }
	  *pblo = blo_data;
	  return KAFS_SUCCESS;

	case KAFS_IBLKREF_FUNC_DEL:
	  if (blo_blkreftbl == KAFS_BLO_NONE)
	    {
	      *pblo = KAFS_BLO_NONE;
	      return KAFS_SUCCESS;
	    }
	  KAFS_CALL (kafs_blk_read, ctx, blo_blkreftbl, blkreftbl);
	  blo_data = kafs_blkcnt_stoh (blkreftbl[iblo]);
	  if (blo_data != KAFS_BLO_NONE)
	    {
	      KAFS_CALL (kafs_blk_release, ctx, &blo_data);
	      blkreftbl[iblo] = kafs_blkcnt_htos (blo_data);
	    }
	  if (!kafs_blk_is_zero (blkreftbl, blksize))
	    {
	      KAFS_CALL (kafs_blk_write, ctx, blo_blkreftbl, blkreftbl);
	      *pblo = KAFS_BLO_NONE;
	      return KAFS_SUCCESS;
	    }
	  KAFS_CALL (kafs_blk_release, ctx, &blo_blkreftbl);
	  inoent->i_blkreftbl[12] = kafs_blkcnt_htos (blo_blkreftbl);
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
      kafs_blkcnt_t blo_blkreftbl1 = kafs_blkcnt_stoh (inoent->i_blkreftbl[13]);
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
	  KAFS_CALL (kafs_blk_read, ctx, blo_blkreftbl1, blkreftbl1);
	  blo_blkreftbl2 = kafs_blkcnt_stoh (blkreftbl1[iblo1]);
	  if (blo_blkreftbl2 == KAFS_BLO_NONE)
	    {
	      *pblo = KAFS_BLO_NONE;
	      return KAFS_SUCCESS;
	    }
	  KAFS_CALL (kafs_blk_read, ctx, blo_blkreftbl2, blkreftbl2);
	  *pblo = kafs_blkcnt_stoh (blkreftbl2[iblo2]);
	  return KAFS_SUCCESS;

	case KAFS_IBLKREF_FUNC_PUT:
	  if (blo_blkreftbl1 == KAFS_BLO_NONE)
	    {
	      KAFS_CALL (kafs_blk_alloc, ctx, &blo_blkreftbl1);
	      inoent->i_blkreftbl[13] = kafs_blkcnt_htos (blo_blkreftbl1);
	      memset (blkreftbl1, 0, blksize);
	      blo_blkreftbl2 = KAFS_BLO_NONE;
	    }
	  else
	    {
	      KAFS_CALL (kafs_blk_read, ctx, blo_blkreftbl1, blkreftbl1);
	      blo_blkreftbl2 = kafs_blkcnt_stoh (blkreftbl1[iblo1]);
	    }
	  if (blo_blkreftbl2 == KAFS_BLO_NONE)
	    {
	      KAFS_CALL (kafs_blk_alloc, ctx, &blo_blkreftbl2);
	      blkreftbl1[iblo1] = kafs_blkcnt_htos (blo_blkreftbl2);
	      KAFS_CALL (kafs_blk_write, ctx, blo_blkreftbl1, blkreftbl1);
	      memset (blkreftbl2, 0, blksize);
	      blo_data = KAFS_BLO_NONE;
	    }
	  else
	    {
	      KAFS_CALL (kafs_blk_read, ctx, blo_blkreftbl2, blkreftbl2);
	      blo_data = kafs_blkcnt_stoh (blkreftbl2[iblo2]);
	    }
	  if (blo_data == KAFS_BLO_NONE)
	    {
	      KAFS_CALL (kafs_blk_alloc, ctx, &blo_data);
	      blkreftbl2[iblo2] = kafs_blkcnt_htos (blo_data);
	      KAFS_CALL (kafs_blk_write, ctx, blo_blkreftbl2, blkreftbl2);
	    }
	  *pblo = blo_data;
	  return KAFS_SUCCESS;

	case KAFS_IBLKREF_FUNC_DEL:
	  if (blo_blkreftbl1 == KAFS_BLO_NONE)
	    {
	      *pblo = KAFS_BLO_NONE;
	      return KAFS_SUCCESS;
	    }
	  KAFS_CALL (kafs_blk_read, ctx, blo_blkreftbl1, blkreftbl1);
	  blo_blkreftbl2 = kafs_blkcnt_stoh (blkreftbl1[iblo1]);
	  if (blo_blkreftbl2 == KAFS_BLO_NONE)
	    {
	      *pblo = KAFS_BLO_NONE;
	      return KAFS_SUCCESS;
	    }
	  KAFS_CALL (kafs_blk_read, ctx, blo_blkreftbl2, blkreftbl2);
	  blo_data = kafs_blkcnt_stoh (blkreftbl2[iblo2]);
	  if (blo_data == KAFS_BLO_NONE)
	    {
	      *pblo = KAFS_BLO_NONE;
	      return KAFS_SUCCESS;
	    }
	  KAFS_CALL (kafs_blk_release, ctx, &blo_data);
	  blkreftbl2[iblo2] = kafs_blkcnt_htos (KAFS_BLO_NONE);
	  if (!kafs_blk_is_zero (blkreftbl2, blksize))
	    {
	      KAFS_CALL (kafs_blk_write, ctx, blo_blkreftbl2, blkreftbl2);
	      *pblo = KAFS_BLO_NONE;
	      return KAFS_SUCCESS;
	    }
	  KAFS_CALL (kafs_blk_release, ctx, &blo_blkreftbl2);
	  blkreftbl1[iblo1] = kafs_blkcnt_htos (KAFS_BLO_NONE);
	  if (!kafs_blk_is_zero (blkreftbl1, blksize))
	    {
	      KAFS_CALL (kafs_blk_write, ctx, blo_blkreftbl1, blkreftbl1);
	      *pblo = KAFS_BLO_NONE;
	      return KAFS_SUCCESS;
	    }
	  KAFS_CALL (kafs_blk_release, ctx, &blo_blkreftbl1);
	  inoent->i_blkreftbl[13] = kafs_blkcnt_htos (KAFS_BLO_NONE);
	  *pblo = KAFS_BLO_NONE;
	  return KAFS_SUCCESS;
	}
    }

  iblo -= blkrefs_pb_sq;
  kafs_sblkcnt_t blkreftbl1[blkrefs_pb];
  kafs_sblkcnt_t blkreftbl2[blkrefs_pb];
  kafs_sblkcnt_t blkreftbl3[blkrefs_pb];

  kafs_blkcnt_t blo_blkreftbl1 = kafs_blkcnt_stoh (inoent->i_blkreftbl[14]);
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
      KAFS_CALL (kafs_blk_read, ctx, blo_blkreftbl1, blkreftbl1);
      blo_blkreftbl2 = kafs_blkcnt_stoh (blkreftbl1[iblo1]);
      if (blo_blkreftbl2 == KAFS_BLO_NONE)
	{
	  *pblo = KAFS_BLO_NONE;
	  return KAFS_SUCCESS;
	}
      KAFS_CALL (kafs_blk_read, ctx, blo_blkreftbl2, blkreftbl2);
      blo_blkreftbl3 = kafs_blkcnt_stoh (blkreftbl2[iblo2]);
      if (blo_blkreftbl3 == KAFS_BLO_NONE)
	{
	  *pblo = KAFS_BLO_NONE;
	  return KAFS_SUCCESS;
	}
      KAFS_CALL (kafs_blk_read, ctx, blo_blkreftbl3, blkreftbl3);
      *pblo = kafs_blkcnt_stoh (blkreftbl3[iblo3]);
      return KAFS_SUCCESS;

    case KAFS_IBLKREF_FUNC_PUT:
      if (blo_blkreftbl1 == KAFS_BLO_NONE)
	{
	  KAFS_CALL (kafs_blk_alloc, ctx, &blo_blkreftbl1);
	  inoent->i_blkreftbl[14] = kafs_blkcnt_htos (blo_blkreftbl1);
	  memset (blkreftbl1, 0, blksize);
	  blo_blkreftbl2 = KAFS_BLO_NONE;
	}
      else
	{
	  KAFS_CALL (kafs_blk_read, ctx, blo_blkreftbl1, blkreftbl1);
	  blo_blkreftbl2 = kafs_blkcnt_stoh (blkreftbl1[iblo1]);
	}
      if (blo_blkreftbl2 == KAFS_BLO_NONE)
	{
	  KAFS_CALL (kafs_blk_alloc, ctx, &blo_blkreftbl2);
	  blkreftbl1[iblo1] = kafs_blkcnt_htos (blo_blkreftbl2);
	  KAFS_CALL (kafs_blk_write, ctx, blo_blkreftbl1, blkreftbl1);
	  memset (blkreftbl2, 0, blksize);
	  blo_blkreftbl3 = KAFS_BLO_NONE;
	}
      else
	{
	  KAFS_CALL (kafs_blk_read, ctx, blo_blkreftbl2, blkreftbl2);
	  blo_blkreftbl3 = kafs_blkcnt_stoh (blkreftbl2[iblo2]);
	}
      if (blo_blkreftbl3 == KAFS_BLO_NONE)
	{
	  KAFS_CALL (kafs_blk_alloc, ctx, &blo_blkreftbl3);
	  blkreftbl2[iblo2] = kafs_blkcnt_htos (blo_blkreftbl3);
	  KAFS_CALL (kafs_blk_write, ctx, blo_blkreftbl2, blkreftbl2);
	  memset (blkreftbl3, 0, blksize);
	  blo_data = KAFS_BLO_NONE;
	}
      else
	{
	  KAFS_CALL (kafs_blk_read, ctx, blo_blkreftbl3, blkreftbl3);
	  blo_data = kafs_blkcnt_stoh (blkreftbl3[iblo3]);
	}
      if (blo_data == KAFS_BLO_NONE)
	{
	  KAFS_CALL (kafs_blk_alloc, ctx, &blo_data);
	  blkreftbl3[iblo3] = kafs_blkcnt_htos (blo_data);
	  KAFS_CALL (kafs_blk_write, ctx, blo_blkreftbl3, blkreftbl3);
	}
      *pblo = blo_data;
      return KAFS_SUCCESS;

    case KAFS_IBLKREF_FUNC_DEL:
      if (blo_blkreftbl1 == KAFS_BLO_NONE)
	{
	  *pblo = KAFS_BLO_NONE;
	  return KAFS_SUCCESS;
	}
      KAFS_CALL (kafs_blk_read, ctx, blo_blkreftbl1, blkreftbl1);
      blo_blkreftbl2 = kafs_blkcnt_stoh (blkreftbl1[iblo1]);
      if (blo_blkreftbl2 == KAFS_BLO_NONE)
	{
	  *pblo = KAFS_BLO_NONE;
	  return KAFS_SUCCESS;
	}
      KAFS_CALL (kafs_blk_read, ctx, blo_blkreftbl2, blkreftbl2);
      blo_blkreftbl3 = kafs_blkcnt_stoh (blkreftbl2[iblo2]);
      if (blo_blkreftbl3 == KAFS_BLO_NONE)
	{
	  *pblo = KAFS_BLO_NONE;
	  return KAFS_SUCCESS;
	}
      KAFS_CALL (kafs_blk_read, ctx, blo_blkreftbl3, blkreftbl3);
      blo_data = kafs_blkcnt_stoh (blkreftbl3[iblo3]);
      if (blo_data == KAFS_BLO_NONE)
	{
	  *pblo = KAFS_BLO_NONE;
	  return KAFS_SUCCESS;
	}
      KAFS_CALL (kafs_blk_release, ctx, &blo_data);
      blkreftbl3[iblo3] = kafs_blkcnt_htos (KAFS_BLO_NONE);
      if (!kafs_blk_is_zero (blkreftbl3, blksize))
	{
	  KAFS_CALL (kafs_blk_write, ctx, blo_blkreftbl3, blkreftbl3);
	  *pblo = KAFS_BLO_NONE;
	  return KAFS_SUCCESS;
	}
      KAFS_CALL (kafs_blk_release, ctx, &blo_blkreftbl3);
      blkreftbl2[iblo2] = kafs_blkcnt_htos (KAFS_BLO_NONE);
      if (!kafs_blk_is_zero (blkreftbl2, blksize))
	{
	  KAFS_CALL (kafs_blk_write, ctx, blo_blkreftbl2, blkreftbl2);
	  *pblo = KAFS_BLO_NONE;
	  return KAFS_SUCCESS;
	}
      KAFS_CALL (kafs_blk_release, ctx, &blo_blkreftbl2);
      blkreftbl1[iblo1] = kafs_blkcnt_htos (KAFS_BLO_NONE);
      if (!kafs_blk_is_zero (blkreftbl1, blksize))
	{
	  KAFS_CALL (kafs_blk_write, ctx, blo_blkreftbl1, blkreftbl1);
	  *pblo = KAFS_BLO_NONE;
	  return KAFS_SUCCESS;
	}
      KAFS_CALL (kafs_blk_release, ctx, &blo_blkreftbl1);
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
kafs_read_iblk (struct kafs_context *ctx, kafs_sinode_t * inoent, kafs_iblkcnt_t iblo, void *buf)
{
  assert (ctx != NULL);
  assert (buf != NULL);
  assert (inoent != NULL);
  assert (kafs_ino_get_usage (inoent));
  assert (kafs_ino_size_get (inoent) > KAFS_DIRECT_SIZE);
  kafs_blkcnt_t blo;
  KAFS_CALL (kafs_do_ibrk, ctx, inoent, iblo, &blo, KAFS_IBLKREF_FUNC_GET);
  KAFS_CALL (kafs_blk_read, ctx, blo, buf);
  return KAFS_SUCCESS;
}

/// @brief inode毎のデータを書き込む（ブロック単位）
/// @param ctx コンテキスト
/// @param ino inode番号
/// @param iblo ブロック番号
/// @param buf バッファ
/// @return 0: 成功, < 0: 失敗 (-errno)
static int
kafs_write_iblk (struct kafs_context *ctx, kafs_sinode_t * inoent, kafs_iblkcnt_t iblo, const void *buf)
{
  assert (ctx != NULL);
  assert (buf != NULL);
  assert (inoent != NULL);
  assert (kafs_ino_get_usage (inoent));
  assert (kafs_ino_size_get (inoent) > KAFS_DIRECT_SIZE);
  kafs_blkcnt_t blo;
  KAFS_CALL (kafs_do_ibrk, ctx, inoent, iblo, &blo, KAFS_IBLKREF_FUNC_PUT);
  assert (blo != KAFS_BLO_NONE);
  KAFS_CALL (kafs_blk_write, ctx, blo, buf);
  return KAFS_SUCCESS;
}

static int
kafs_release_iblk (struct kafs_context *ctx, kafs_sinode_t * inoent, kafs_iblkcnt_t iblo)
{
  assert (ctx != NULL);
  assert (inoent != NULL);
  assert (kafs_ino_get_usage (inoent));
  assert (kafs_ino_size_get (inoent) > KAFS_DIRECT_SIZE);	// TODO: 縮小時の考慮
  kafs_blkcnt_t blo;
  KAFS_CALL (kafs_do_ibrk, ctx, inoent, iblo, &blo, KAFS_IBLKREF_FUNC_DEL);
  assert (blo == KAFS_BLO_NONE);
  return KAFS_SUCCESS;
}

#define kafs_load_directsize(ctx) (sizeof (((struct kafs_sinode *)NULL)->i_blkreftbl))

/// @brief inode 毎にデータを読み出す
/// @param ctx コンテキスト
/// @param ino inode 番号
/// @param buf バッファ
/// @param size バッファサイズ
/// @param offset オフセット
/// @return > 0: 読み出しサイズ, 0: EOF, < 0: エラー(-errno)
static ssize_t
kafs_pread (struct kafs_context *ctx, kafs_sinode_t * inoent, void *buf, size_t size, off_t offset)
{
  assert (ctx != NULL);
  assert (buf != NULL);
  assert (inoent != NULL);
  assert (kafs_ino_get_usage (inoent));
  kafs_off_t filesize = kafs_ino_size_get (inoent);
  if (offset >= filesize)
    return 0;
  if (offset + size > filesize)
    size = filesize - offset;
  if (size == 0)
    return 0;
  // 60バイト以下は直接
  if (filesize <= kafs_load_directsize (ctx))
    {
      memcpy (buf, (void *) inoent->i_blkreftbl + offset, size);
      return size;
    }
  size_t size_read = 0;
  kafs_logblksize_t log_blksize = kafs_sb_log_blksize_get (ctx->c_superblock);
  kafs_blksize_t blksize = kafs_sb_blksize_get (ctx->c_superblock);
  kafs_blksize_t offset_blksize = offset & (blksize - 1);
  if (offset_blksize > 0 || size - size_read < blksize)
    {
      char rbuf[blksize];
      kafs_iblkcnt_t iblo = offset >> log_blksize;
      KAFS_CALL (kafs_read_iblk, ctx, inoent, iblo, rbuf);
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
	  KAFS_CALL (kafs_read_iblk, ctx, inoent, iblo, rbuf);
	  memcpy (buf + size_read, rbuf, size - size_read);
	  return size;
	}
      KAFS_CALL (kafs_read_iblk, ctx, inoent, iblo, buf + size_read);
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
kafs_pwrite (struct kafs_context *ctx, kafs_sinode_t * inoent, const void *buf, size_t size, off_t offset)
{
  assert (ctx != NULL);
  assert (buf != NULL);
  assert (inoent != NULL);
  assert (kafs_ino_get_usage (inoent));

  kafs_off_t filesize = kafs_ino_size_get (inoent);
  kafs_logblksize_t log_blksize = kafs_sb_log_blksize_get (ctx->c_superblock);
  kafs_blksize_t blksize = kafs_sb_blksize_get (ctx->c_superblock);
  kafs_off_t filesize_new = offset + size;

  if (size == 0)
    return 0;

  if (filesize < filesize_new)
    {
      // サイズ拡大時
      kafs_ino_size_set (inoent, filesize_new);
      if (filesize != 0 && filesize_new > kafs_load_directsize (ctx))
	{
	  char wbuf[blksize];
	  memset (wbuf, 0, blksize);
	  memcpy (wbuf, inoent->i_blkreftbl, filesize);
	  memset (inoent->i_blkreftbl, 0, sizeof (inoent->i_blkreftbl));
	  KAFS_CALL (kafs_write_iblk, ctx, inoent, 0, wbuf);
	}
      filesize = filesize_new;
    }

  size_t size_written = 0;

  // 60バイト以下は直接
  if (filesize <= sizeof (inoent->i_blkreftbl))
    {
      memcpy ((void *) inoent->i_blkreftbl + offset, buf, size);
      return size;
    }

  kafs_blksize_t offset_blksize = offset & (blksize - 1);
  if (offset_blksize > 0 || size - size_written < blksize)
    {
      // 1ブロック目で端数が出る場合
      kafs_iblkcnt_t iblo = offset >> log_blksize;
      // 書き戻しバッファ
      char wbuf[blksize];
      KAFS_CALL (kafs_read_iblk, ctx, inoent, iblo, wbuf);
      if (size < blksize - offset_blksize)
	{
	  // 1ブロックのみの場合
	  memcpy (wbuf + offset_blksize, buf, size);
	  KAFS_CALL (kafs_write_iblk, ctx, inoent, iblo, wbuf);
	  return size;
	}
      // ブロックの残り分を書き込む
      memcpy (wbuf + offset_blksize, buf, blksize - offset_blksize);
      KAFS_CALL (kafs_write_iblk, ctx, inoent, iblo, wbuf);
      size_written += blksize - offset_blksize;
    }

  while (size_written < size)
    {
      kafs_iblkcnt_t iblo = (offset + size_written) >> log_blksize;
      if (size - size_written < blksize)
	{
	  char wbuf[blksize];
	  KAFS_CALL (kafs_read_iblk, ctx, inoent, iblo, wbuf);
	  memcpy (wbuf, buf + size_written, size - size_written);
	  KAFS_CALL (kafs_write_iblk, ctx, inoent, iblo, wbuf);
	  return size;
	}
      KAFS_CALL (kafs_write_iblk, ctx, inoent, iblo, buf + size_written);
      size_written += blksize;
    }
  return size;
}

static int
kafs_truncate (struct kafs_context *ctx, kafs_sinode_t * inoent, kafs_off_t filesize_new)
{
  assert (ctx != NULL);
  assert (inoent != NULL);
  assert (kafs_ino_get_usage (inoent));
  kafs_logblksize_t log_blksize = kafs_sb_log_blksize_get (ctx->c_superblock);
  kafs_blksize_t blksize = kafs_sb_blksize_get (ctx->c_superblock);
  kafs_off_t filesize_orig = kafs_ino_size_get (inoent);
  if (filesize_orig == filesize_new)
    return KAFS_SUCCESS;
  kafs_ino_size_set (inoent, filesize_new);
  if (filesize_new > filesize_orig)
    return KAFS_SUCCESS;
  kafs_blksize_t off = filesize_orig & (blksize - 1);
  kafs_off_t offset = filesize_new;
  if (off > 0)
    {
      char zbuf[blksize];
      memset (zbuf, 0, blksize);
      ssize_t w = KAFS_CALL (kafs_pwrite, ctx, inoent, zbuf, blksize - off, offset);
      assert (w == blksize - off);
      offset += blksize - off;
    }
  while (offset < filesize_orig)
    {
      KAFS_CALL (kafs_release_iblk, ctx, inoent, offset >> log_blksize);
      offset += blksize;
    }
  return KAFS_SUCCESS;
}

static int
kafs_trim (struct kafs_context *ctx, kafs_sinode_t * inoent, kafs_off_t off, kafs_off_t size)
{
  assert (ctx != NULL);
  assert (inoent != NULL);
  assert (kafs_ino_get_usage (inoent));
  if (size == 0)
    return size;
  kafs_off_t size_orig = kafs_ino_size_get (inoent);
  if (off >= size_orig)
    return 0;
  if (off + size >= size_orig)
    {
      KAFS_CALL (kafs_truncate, ctx, inoent, off);
      return size_orig - off;
    }
  // TODO: 簡易実装なのでしっかり実装していくべき
  char buf[size_orig - (off + size)];
  ssize_t r = KAFS_CALL (kafs_pread, ctx, inoent, buf, size_orig - (off + size), off + size);
  ssize_t w = KAFS_CALL (kafs_pwrite, ctx, inoent, buf, r, off);
  KAFS_CALL (kafs_truncate, ctx, inoent, off + w);
  return KAFS_SUCCESS;
}

static int
kafs_release (struct kafs_context *ctx, kafs_sinode_t * inoent)
{
  if (kafs_ino_linkcnt_decr (inoent) == 0)
    {
      KAFS_CALL (kafs_truncate, ctx, inoent, 0);
      memset (inoent, 0, sizeof (struct kafs_sinode));
      // TODO: SuperBlock の free_inode数を増やす
      kafs_sb_blkcnt_free_incr (ctx->c_superblock);
    }
  return KAFS_SUCCESS;
}

/// @brief ディレクトリエントリを読み出す
/// @param ctx コンテキスト
/// @param ino_dir ディレクトリのinode番号
/// @param dirent 読み出すディレクトリエントリのバッファ（sizeof(struct kafs_dirent) 以上）
/// @param direntlen バッファの長さ（全体がこれより長い場合はファイル名部分が読みだされない）
/// @param offset オフセット
/// @return > 0: サイズ, 0: EOF, < 0: 失敗 (-errno)
static ssize_t
kafs_read_dirent_inode (struct kafs_context *ctx, kafs_sinode_t * inoent_dir,
			struct kafs_sdirent *dirent, size_t direntlen, off_t offset)
{
  assert (ctx != NULL);
  assert (inoent_dir != NULL);
  assert (kafs_ino_get_usage (inoent_dir));
  assert (dirent < 0);
  assert (direntlen > sizeof (struct kafs_sdirent));
  ssize_t r = KAFS_CALL (kafs_pread, ctx, inoent_dir, dirent, sizeof (struct kafs_sdirent), offset);
  if (r == 0)
    return 0;
  if (r < sizeof (struct kafs_sdirent))
    return -EIO;
  kafs_filenamelen_t filenamelen = kafs_dirent_filenamelen_get (dirent);
  if (direntlen - sizeof (struct kafs_sdirent) < filenamelen)
    return sizeof (struct kafs_sdirent) + filenamelen;
  r = KAFS_CALL (kafs_pread, ctx, inoent_dir, dirent->d_filename, filenamelen, offset + filenamelen);
  if (r < filenamelen)
    return -EIO;
  return sizeof (struct kafs_sdirent) + filenamelen;
}

/// @brief ディレクトリエントリから対象のファイル名を探す
/// @param ctx コンテキスト
/// @param name ファイル名
/// @param namelen ファイル名の長さ
/// @param ino 対象のディレクトリ
/// @param pino_found 見つかったエントリ
/// @return 0: 成功, < 0: 失敗 (-errno)
static int
kafs_find_dirent_inode (struct kafs_context *ctx, kafs_sinode_t * inoent,
			const char *filename, kafs_filenamelen_t filenamelen, kafs_sinode_t ** pinoent_found)
{
  assert (ctx != NULL);
  assert (inoent != NULL);
  assert (kafs_ino_get_usage (inoent));
  assert (pinoent_found != NULL);
  kafs_off_t direntlen = sizeof (struct kafs_sdirent) + filenamelen;
  assert (filename != NULL);
  assert (filenamelen > 0);
  kafs_mode_t mode = kafs_ino_mode_get (inoent);
  if (!S_ISDIR (mode))
    return -ENOTDIR;
  char buf[direntlen];
  struct kafs_sdirent *dirent = (struct kafs_sdirent *) buf;
  off_t offset = 0;
  while (1)
    {
      ssize_t r = KAFS_CALL (kafs_read_dirent_inode, ctx, inoent, dirent, direntlen, offset);
      if (r == 0)
	break;
      assert (r == direntlen);
      kafs_inocnt_t d_ino = kafs_dirent_ino_get (dirent);
      const char *d_filename = dirent->d_filename;
      kafs_filenamelen_t d_filenamelen = kafs_dirent_filenamelen_get (dirent);
      if (d_filenamelen == filenamelen && memcmp (d_filename, filename, filenamelen) == 0)
	{
	  *pinoent_found = &ctx->c_inotbl[d_ino];
	  return KAFS_SUCCESS;
	}
      offset += r;
    }
  return -ENOENT;
}

static int
kafs_add_dirent_inode (struct kafs_context *ctx, kafs_sinode_t * inoent_dir, kafs_inocnt_t ino,
		       const char *filename, size_t filenamelen)
{
  assert (ctx != NULL);
  assert (inoent_dir != NULL);
  assert (ino != KAFS_INO_NONE);
  kafs_inocnt_t inocnt = kafs_sb_inocnt_get (ctx->c_superblock);
  assert (ino < inocnt);
  kafs_mode_t mode_dir = kafs_ino_mode_get (inoent_dir);
  if (!S_ISDIR (mode_dir))
    return -ENOTDIR;
  kafs_off_t direntlen = sizeof (struct kafs_sdirent) + filenamelen;
  char buf[direntlen];
  off_t offset = 0;
  while (1)
    {
      struct kafs_sdirent *dirent = (struct kafs_sdirent *) buf;
      ssize_t r = KAFS_CALL (kafs_read_dirent_inode, ctx, inoent_dir, dirent, direntlen, offset);
      if (r == 0)
	break;
      if (r == direntlen && memcmp (filename, dirent->d_filename, filenamelen) == 0)
	return -EEXIST;
      offset += r;
    }
  struct kafs_sdirent *dirent = (struct kafs_sdirent *) buf;
  kafs_dirent_set (dirent, ino, filename, filenamelen);
  ssize_t w = KAFS_CALL (kafs_pwrite, ctx, inoent_dir, dirent, direntlen, offset);
  assert (w == direntlen);
  struct kafs_sinode *inoent = &ctx->c_inotbl[ino];
  kafs_ino_linkcnt_incr (inoent);
  return KAFS_SUCCESS;
}

__attribute_maybe_unused__ static int
kafs_delete_dirent_inode (struct kafs_context *ctx, kafs_inocnt_t ino_dir, const char *filename,
			  kafs_filenamelen_t filenamelen)
{
  assert (ctx != NULL);
  assert (filename != NULL);
  assert (ino_dir != KAFS_INO_NONE);
  kafs_inocnt_t inocnt = kafs_sb_inocnt_get (ctx->c_superblock);
  assert (ino_dir < inocnt);
  kafs_sinode_t *inoent_dir = &ctx->c_inotbl[ino_dir];
  assert (kafs_ino_get_usage (inoent_dir));
  kafs_off_t direntlen = sizeof (struct kafs_sdirent) + filenamelen;
  assert (filename != NULL);
  assert (filenamelen > 0);
  kafs_mode_t mode_dir = kafs_ino_mode_get (inoent_dir);
  if (!S_ISDIR (mode_dir))
    return -ENOTDIR;
  char buf[direntlen];
  struct kafs_sdirent *dirent = (struct kafs_sdirent *) buf;
  off_t offset = 0;
  while (1)
    {
      ssize_t r = KAFS_CALL (kafs_read_dirent_inode, ctx, inoent_dir, dirent, direntlen, offset);
      if (r == 0)
	break;
      assert (r == direntlen);
      kafs_inocnt_t d_ino = kafs_dirent_ino_get (dirent);
      const char *d_filename = dirent->d_filename;
      kafs_filenamelen_t d_filenamelen = kafs_dirent_filenamelen_get (dirent);
      if (d_filenamelen == filenamelen && memcmp (d_filename, filename, filenamelen) == 0)
	{
	  KAFS_CALL (kafs_trim, ctx, &ctx->c_inotbl[d_ino], offset, r);
	  kafs_ino_linkcnt_decr (&ctx->c_inotbl[d_ino]);
	  return KAFS_SUCCESS;
	}
      offset += r;
    }
  return -ENOENT;
}

static int
kafs_get_from_path_inode (struct kafs_context *ctx, const char *path, kafs_inocnt_t * pino)
{
  kafs_sinode_t *inotbl = &ctx->c_inotbl[*pino];
  const char *p = path;
  if (*p == '/')
    {
      inotbl = &ctx->c_inotbl[1];	// ROOT DIR
      path++;
    }
  while (*p)
    {
      char *name = strchrnul (p, '/');

      kafs_sinode_t *inotbl_next;
      KAFS_CALL (kafs_find_dirent_inode, ctx, inotbl, p, name - p, &inotbl_next);
      inotbl = inotbl_next;
      p = name;
      if (*p == '/')
	p++;
    }
  *pino = inotbl - ctx->c_inotbl;
  return 0;
}

static int
kafs_op_getattr (const char *path, struct stat *st, struct fuse_file_info *fi)
{
  struct fuse_context *fctx = fuse_get_context ();
  struct kafs_context *ctx = fctx->private_data;
  kafs_inocnt_t ino = fi->fh;
  struct kafs_sinode *inoent = &ctx->c_inotbl[ino];
  st->st_dev = 0;
  st->st_ino = ino;
  st->st_mode = kafs_ino_mode_get (inoent);
  st->st_nlink = kafs_ino_linkcnt_get (inoent);
  st->st_uid = kafs_ino_uid_get (inoent);
  st->st_gid = kafs_ino_gid_get (inoent);
  st->st_rdev = kafs_ino_dev_get (inoent);
  st->st_size = kafs_ino_size_get (inoent);
  st->st_blksize = kafs_sb_blksize_get (ctx->c_superblock);
  st->st_blocks = kafs_ino_blocks_get (inoent);
  st->st_atim = kafs_ino_atime_get (inoent);
  st->st_mtim = kafs_ino_mtime_get (inoent);
  st->st_ctim = kafs_ino_ctime_get (inoent);
  return 0;
}

static int
kafs_op_open (const char *path, struct fuse_file_info *fi)
{
  struct fuse_context *fctx = fuse_get_context ();
  struct kafs_context *ctx = fctx->private_data;
  kafs_inocnt_t ino = KAFS_INO_NONE;
  KAFS_CALL (kafs_get_from_path_inode, ctx, path, &ino);
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
  KAFS_CALL (kafs_get_from_path_inode, ctx, dirpath, &ino_dir);
  struct kafs_sinode *inoent_dir = &ctx->c_inotbl[ino_dir];
  kafs_mode_t mode_dir = kafs_ino_mode_get (inoent_dir);
  if (!S_ISDIR (mode_dir))
    return -EIO;
  kafs_inocnt_t ino_new;
  KAFS_CALL (kafs_ino_find_free, ctx->c_inotbl, &ino_new, &ctx->c_ino_search, kafs_sb_inocnt_get (ctx->c_superblock));
  struct kafs_sinode *inoent_new = &ctx->c_inotbl[ino_new];
  kafs_ino_mode_set (inoent_new, mode);
  kafs_ino_uid_set (inoent_new, fctx->uid);
  kafs_ino_size_set (inoent_new, 0);
  kafs_time_t now = kafs_now ();
  kafs_time_t nulltime = { 0, 0 };
  kafs_ino_atime_set (inoent_new, now);
  kafs_ino_ctime_set (inoent_new, now);
  kafs_ino_mtime_set (inoent_new, now);
  kafs_ino_dtime_set (inoent_new, nulltime);
  kafs_ino_gid_set (inoent_new, fctx->gid);
  kafs_ino_linkcnt_set (inoent_new, 0);
  kafs_ino_blocks_set (inoent_new, 0);
  kafs_ino_dev_set (inoent_new, dev);
  memset (inoent_new->i_blkreftbl, 0, sizeof (inoent_new->i_blkreftbl));
  int ret = kafs_add_dirent_inode (ctx, inoent_dir, ino_new, filename, strlen (filename));
  if (ret < 0)
    {
      KAFS_CALL (kafs_release, ctx, inoent_new);
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
  KAFS_CALL (kafs_get_from_path_inode, ctx, path, &ino);
  ssize_t r = KAFS_CALL (kafs_pread, ctx, &ctx->c_inotbl[ino], buf, buflen - 1, 0);
  buf[r] = '\0';
  return 0;
}

static int
kafs_op_read (const char *path, char *buf, size_t size, off_t offset, struct fuse_file_info *fi)
{
  struct fuse_context *fctx = fuse_get_context ();
  struct kafs_context *ctx = fctx->private_data;
  kafs_inocnt_t ino = fi->fh;
  return kafs_pread (ctx, &ctx->c_inotbl[ino], buf, size, offset);
}

static int
kafs_op_write (const char *path, const char *buf, size_t size, off_t offset, struct fuse_file_info *fi)
{
  struct fuse_context *fctx = fuse_get_context ();
  struct kafs_context *ctx = fctx->private_data;
  kafs_inocnt_t ino = fi->fh;
  return kafs_pwrite (ctx, &ctx->c_inotbl[ino], buf, size, offset);
}

__attribute_maybe_unused__ static struct fuse_operations kafs_operations = {
  .getattr = kafs_op_getattr,
  .open = kafs_op_open,
  .mknod = kafs_op_mknod,
  .readlink = kafs_op_readlink,
  .read = kafs_op_read,
  .write = kafs_op_write,
};
