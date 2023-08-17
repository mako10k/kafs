#define FUSE_USE_VERSION 30

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include <fuse.h>
#include <errno.h>
#include <string.h>
#include <unistd.h>
#include <assert.h>

typedef unsigned int kafs_blkmask_t;
typedef uint32_t kafs_blkcnt_t;
typedef uint32_t kafs_inocnt_t;

struct kafs_superblock
{
  kafs_inocnt_t s_inodes_count;
  kafs_blkcnt_t s_blocks_count;
  kafs_blkcnt_t s_r_blocks_count;
  kafs_blkcnt_t s_free_blocks_count;
  kafs_inocnt_t s_free_inodes_count;
  kafs_blkcnt_t s_first_data_block;
  uint32_t s_log_block_size;	// 2 ^ (10 + s_log_block_size)
  uint32_t s_mtime;
  uint32_t s_wtime;
  uint16_t s_mnt_count;
};

struct kafs_inode
{
  uint16_t i_mode;
  uint16_t i_uid;
  uint32_t i_size;
  uint32_t i_atime;
  uint32_t i_ctime;
  uint32_t i_mtime;
  uint32_t i_dtime;
  uint16_t i_gid;
  uint16_t i_links_count;
  kafs_blkcnt_t i_blocks;
  uint32_t i_rdev;
  kafs_blkcnt_t i_block[15];
};

struct kafs_context
{
  struct kafs_superblock *sb;
  struct kafs_inode *inode;
  kafs_blkmask_t *blkmask;
  kafs_inocnt_t ino_search;
  kafs_blkcnt_t blo_search;
  int fd;
};

#define KAFS_BLKMASK_BITS (sizeof(kafs_blkmask_t) << 3)
#define KAFS_BLKMASK_LOG_BITS (sizeof(kafs_blkmask_t) + 3)
#define KAFS_BLKMASK_MASK_BITS (KAFS_BLKMASK_BITS - 1)

/// @brief inode 構造体の参照を取得
/// @param ctx コンテキスト
/// @param ino inode 番号
/// @return inode 構造体の参照
static struct kafs_inode *
kafs_iget (struct kafs_context *restrict ctx, kafs_inocnt_t ino)
{
  assert (ctx != NULL);
  assert (ino < ctx->sb->s_inodes_count);
  return ctx->inode[ino].i_mode ? ctx->inode + ino : NULL;
}

/// @brief inode 構造体の参照を取得
/// @param ctx コンテキスト
/// @param ino inode 番号
/// @return inode 構造体の参照
static const struct kafs_inode *restrict
kafs_iget_const (const struct kafs_context *restrict ctx, kafs_inocnt_t ino)
{
  assert (ctx != NULL);
  assert (ino < ctx->sb->s_inodes_count);
  return ctx->inode[ino].i_mode ? ctx->inode + ino : NULL;
}

/// @brief 空いている inode 番号を見つける
/// @param ctx コンテキスト
/// @param ino 見つかった inode 番号
/// @return 0: 成功、 < 0: 失敗 (-errno)
static int
kafs_ialloc (struct kafs_context *restrict ctx, kafs_inocnt_t * pino)
{
  assert (ctx != NULL);
  assert (pino != NULL);
  kafs_inocnt_t ino_count = ctx->sb->s_inodes_count;
  kafs_inocnt_t ino_search = ctx->ino_search;
  kafs_inocnt_t ino = ino_search + 1;
  struct kafs_inode *restrict inode = kafs_iget (ctx, 0);
  while (ino_search != ino)
    {
      if (ino >= ino_count)
	ino = 0;
      if (inode[ino].i_mode == 0)
	{
	  ctx->ino_search = ino;
	  *pino = ino;
	  return 0;
	}
      ino++;
    }
  return -ENOSPC;
}

/// @brief 指定されたブロックのフラグを取得する
/// @param ctx コンテキスト
/// @param blo ブロック番号
/// @return 0: 未使用, != 0: 使用中
static int
kafs_bmget (const struct kafs_context *restrict ctx, kafs_blkcnt_t blo)
{
  assert (ctx != NULL);
  assert (blo < ctx->sb->s_blocks_count);
  int blod = blo >> KAFS_BLKMASK_LOG_BITS;
  int blor = blo & KAFS_BLKMASK_MASK_BITS;
  return (ctx->blkmask[blod] & (1 << blor)) != 0;
}

/// @brief 指定されたブロックのフラグを操作する
/// @param ctx コンテキスト
/// @param blo ブロック番号
/// @param val 0: フラグをクリア, != 0: フラグをセット
/// @return 0: 成功, < 0: 失敗 (-errno)
static int
kafs_bmset (struct kafs_context *restrict ctx, kafs_blkcnt_t blo, int val)
{
  assert (ctx != NULL);
  assert (blo < ctx->sb->s_blocks_count);
  int blod = blo >> KAFS_BLKMASK_LOG_BITS;
  int blor = blo & KAFS_BLKMASK_MASK_BITS;
  if (val)
    ctx->blkmask[blod] |= 1 << blor;
  else
    ctx->blkmask[blod] &= ~(1 << blor);
  return 0;
}

/// @brief 未使用のブロック番号を取得し、使用中フラグをつける
/// @param ctx コンテキスト
/// @param blo ブロック番号
/// @return 0: 成功, < 0: 失敗 (-errno)
static uint32_t
kafs_balloc (struct kafs_context *restrict ctx, kafs_blkcnt_t * pblo)
{
  assert (ctx != NULL);
  assert (pblo != NULL);
  kafs_blkcnt_t blo_search = ctx->blo_search;
  kafs_blkcnt_t blo = blo_search + 1;
  kafs_blkmask_t *restrict blkmask = ctx->blkmask;
  kafs_blkcnt_t blo_max = ctx->sb->s_blocks_count;
  while (blo_search != blo)
    {
      if (blo >= blo_max)
	blo = 0;
      int blod = blo >> KAFS_BLKMASK_LOG_BITS;
      int blor = blo & KAFS_BLKMASK_MASK_BITS;	// ToDo: 2周目以降は常に0
      kafs_blkmask_t bm = ~blkmask[blod];
      if (bm != 0)
	{
	  // __builtin_ctz は、lsb側から続く0の数、つまり
	  // 最もlsb側に存在する 1 のビット位置
	  int blor_found = __builtin_ctz (bm);
	  kafs_blkcnt_t blo_found =
	    (blod << KAFS_BLKMASK_LOG_BITS) + blor_found;
	  if (blo_found < blo_max)
	    {
	      ctx->blo_search = blo_found;
	      *pblo = blo_found;
	      blkmask[blod] |= 1 << blor_found;
	      return 0;
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
kafs_brelease (struct kafs_context *restrict ctx, kafs_blkcnt_t * pblo)
{
  assert (ctx != NULL);
  assert (pblo != NULL);
  assert (*pblo != 0);
  assert (*pblo < ctx->sb->s_blocks_count);
  assert (kafs_bmget (ctx, *pblo) != 0);
  int ret = kafs_bmset (ctx, *pblo, 0);
  if (ret < 0)
    return ret;
  int log_blksize = 10 + ctx->sb->s_log_block_size;
  size_t blksize = 1 << log_blksize;
  char buf[blksize];
  ssize_t w = pwrite (ctx->fd, buf, blksize, blksize * *pblo);
  if (w < 0)
    return -errno;
  assert (w == blksize);
  *pblo = 0;
  return 0;
}

/// @brief ブロック単位でデータを読み出す
/// @param ctx コンテキスト
/// @param blo ブロック番号
/// @param buf 読み出すバッファ
/// @return 0: 成功, < 0: 失敗 (-errno)
static int
kafs_readb (struct kafs_context *restrict ctx, kafs_blkcnt_t blo, void *buf)
{
  assert (ctx != NULL);
  assert (buf != NULL);
  assert (blo < ctx->sb->s_blocks_count);
  int log_blksize = 10 + ctx->sb->s_log_block_size;
  size_t blksize = 1 << log_blksize;
  if (blo == 0)
    {
      memset (buf, 0, blksize);
      return 0;
    }
  ssize_t r = pread (ctx->fd, buf, blksize, blksize * blo);
  if (r < 0)
    return -errno;
  assert (r == blksize);
  return 0;
}

/// @brief ブロック単位でデータを読み出す
/// @param ctx コンテキスト
/// @param pblo ブロック番号へのポインタ、0の場合はブロックを確保し、その値が入る
/// @param buf 読み出すバッファ
/// @return 0: 成功, < 0: 失敗 (-errno)
static int
kafs_readba (struct kafs_context *restrict ctx, kafs_blkcnt_t * pblo,
	     void *buf)
{
  assert (ctx != NULL);
  assert (buf != NULL);
  assert (pblo != NULL);
  assert (*pblo < ctx->sb->s_blocks_count);
  int log_blksize = 10 + ctx->sb->s_log_block_size;
  size_t blksize = 1 << log_blksize;
  if (*pblo == 0)
    {
      int ret = kafs_balloc (ctx, pblo);
      if (ret < 0)
	return ret;
      memset (buf, 0, blksize);
      return 0;
    }
  ssize_t r = pread (ctx->fd, buf, blksize, blksize * *pblo);
  if (r < 0)
    return -errno;
  assert (r == blksize);
  return 0;
}

/// @brief ブロック単位でデータを書き込む
/// @param ctx コンテキスト
/// @param pblo ブロック番号へのポインタ、0の場合はブロックを確保し、その値が入る
/// @param buf 書き込むバッファ
/// @return 0: 成功, < 0: 失敗 (-errno)
static int
kafs_writeb (struct kafs_context *restrict ctx, kafs_blkcnt_t * pblo,
	     const void *buf)
{
  assert (ctx != NULL);
  assert (buf != NULL);
  assert (pblo != NULL);
  assert (*pblo < ctx->sb->s_blocks_count);
  int log_blksize = 10 + ctx->sb->s_log_block_size;
  size_t blksize = 1 << log_blksize;
  if (*pblo == 0)
    {
      int ret = kafs_balloc (ctx, pblo);
      if (ret < 0)
	return ret;
    }
  ssize_t w = pwrite (ctx->fd, buf, blksize, blksize * *pblo);
  if (w < 0)
    return -errno;
  assert (w == blksize);
  return 0;
}

/// @brief inode毎のデータを読み出す（ブロック単位）
/// @param ctx コンテキスト
/// @param inode inode情報
/// @param buf バッファ
/// @param iblo ブロック番号
/// @return 0: 成功, < 0: 失敗 (-errno)
static int
kafs_readib (struct kafs_context *restrict ctx,
	     struct kafs_inode *restrict inode, kafs_blkcnt_t iblo, void *buf)
{
  assert (ctx != NULL);
  assert (inode != NULL);
  assert (iblo != NULL);
  int log_blksize = 10 + ctx->sb->s_log_block_size;
  size_t blksize = 1 << log_blksize;
  kafs_blkcnt_t blo;

  // 0..11 は 直接
  if (iblo < 12)
    return kafs_readb (ctx, inode->i_block[iblo], buf);
  iblo -= 12;
  // 12 .. ブロックサイズ / 4 + 11 は、間接（１段階） 
  int log_blo_count_per_block = log_blksize - (sizeof (*inode->i_block) + 3);
  blksize_t blo_count_per_block = 1 << log_blo_count_per_block;
  char buf2[blksize];
  kafs_blkcnt_t *blkref = (kafs_blkcnt_t *) buf2;
  if (iblo < blo_count_per_block)
    {
      int ret = kafs_readb (ctx, inode->i_block[12], blkref);
      if (ret < 0)
	return ret;
      return kafs_readb (ctx, blkref[iblo], buf);
    }

  // ブロックサイズ/4 + 12  .. (ブロックサイズ/4)^2 + ブロックサイズ / 4 + 11 は、間接（２段階） 
  iblo -= blo_count_per_block;
  blkcnt_t blkid_per_block2 = 1 << (log_blo_count_per_block * 2);
  if (iblo < blkid_per_block2)
    {
      int ret = kafs_readb (ctx, inode->i_block[13], blkref);
      if (ret < 0)
	return ret;
      ret = kafs_readb (ctx, blkref[iblo >> log_blo_count_per_block], blkref);
      if (ret < 0)
	return ret;
      return kafs_readb (ctx, blkref[iblo & (blo_count_per_block - 1)],
			 blkref);
    }

  // (ブロックサイズ/4)^2 + ブロックサイズ / 4 + 12 .. Inf は、間接（３段階） 
  iblo -= blkid_per_block2;
  int ret = kafs_readb (ctx, inode->i_block[14], blkref);
  if (ret < 0)
    return ret;
  int blkid2 = iblo >> log_blo_count_per_block;
  ret = kafs_readb (ctx, blkref[blkid2 >> log_blo_count_per_block], blkref);
  if (ret < 0)
    return ret;
  ret = kafs_readb (ctx, blkref[blkid2 & (1 - blo_count_per_block)], blkref);
  if (ret < 0)
    return ret;
  return kafs_readb (ctx, blkref[iblo & (1 - blo_count_per_block)], blkref);
}

/// @brief inode毎のデータを書き込む（ブロック単位）
/// @param ctx コンテキスト
/// @param inode inode情報
/// @param buf バッファ
/// @param iblo ブロック番号
/// @return 0: 成功, < 0: 失敗 (-errno)
static int
kafs_writeib (struct kafs_context *restrict ctx,
	      struct kafs_inode *restrict inode, kafs_blkcnt_t iblo,
	      const void *buf)
{
  int log_blksize = 10 + ctx->sb->s_log_block_size;
  size_t blksize = 1 << log_blksize;
  kafs_blkcnt_t blo;

  // 0..11 は 直接
  if (iblo < 12)
    return kafs_writeb (ctx, inode->i_block + iblo, buf);

  // 12 .. ブロックサイズ / 4 + 11 は、間接（１段階） 
  iblo -= 12;
  int log_blo_count_per_block = log_blksize - (sizeof (*inode->i_block) + 3);
  blksize_t blo_count_per_block = 1 << log_blo_count_per_block;
  char buf2[blksize];
  kafs_blkcnt_t *blkref = (kafs_blkcnt_t *) buf2;
  if (iblo < blo_count_per_block)
    {
      int ret = kafs_readba (ctx, inode->i_block + 12, blkref);
      if (ret < 0)
	return ret;
      kafs_blkcnt_t blo2 = blkref[iblo];
      ret = kafs_writeb (ctx, blkref + iblo, buf);
      if (ret < 0)
	return ret;
      if (blo2 == 0)
	return kafs_writeb (ctx, inode->i_block + 12, blkref);
      return 0;
    }

  // ブロックサイズ/4 + 12  .. (ブロックサイズ/4)^2 + ブロックサイズ / 4 + 11 は、間接（２段階） 
  iblo -= blo_count_per_block;
  blkcnt_t blkid_per_block2 = 1 << (log_blo_count_per_block * 2);
  char buf3[blksize];
  kafs_blkcnt_t *blkref2 = (kafs_blkcnt_t *) buf3;
  if (iblo < blkid_per_block2)
    {
      int ret = kafs_readba (ctx, inode->i_block + 13, blkref);
      if (ret < 0)
	return ret;
      uint32_t blo2 = blkref[iblo >> log_blo_count_per_block];
      ret =
	kafs_readba (ctx, blkref + (iblo >> log_blo_count_per_block),
		     blkref2);
      if (ret < 0)
	return ret;
      if (blo2 == 0)
	{
	  ret = kafs_writeb (ctx, inode->i_block + 13, blkref);
	  if (ret < 0)
	    return ret;
	}
      blo2 = blkref2[iblo & (blo_count_per_block - 1)];
      ret =
	kafs_writeb (ctx, blkref2 + (iblo & (blo_count_per_block - 1)), buf);
      if (ret < 0)
	return ret;
      if (blo2 == 0)
	return kafs_writeb (ctx, blkref + (iblo >> log_blo_count_per_block),
			    blkref2);
      return 0;
    }

  // (ブロックサイズ/4)^2 + ブロックサイズ / 4 + 12 .. Inf は、間接（３段階） 
  iblo -= blkid_per_block2;

  int ret = kafs_readba (ctx, inode->i_block + 14, blkref);
  if (ret < 0)
    return ret;

  int iblo2 = iblo >> log_blo_count_per_block;
  kafs_blkcnt_t blo2 = blkref[iblo2 >> log_blo_count_per_block];
  ret =
    kafs_readba (ctx, blkref + (iblo2 >> log_blo_count_per_block), blkref2);
  if (ret < 0)
    return ret;
  if (blo2 == 0)
    {
      ret = kafs_writeb (ctx, inode->i_block + 14, blkref);
      if (ret < 0)
	return ret;
    }

  blo2 = blkref2[iblo2 & (blo_count_per_block - 1)];
  ret =
    kafs_readba (ctx, blkref2 + (iblo2 & (blo_count_per_block - 1)), blkref);
  if (ret < 0)
    return ret;
  if (blo2 == 0)
    return kafs_writeb (ctx, blkref + (iblo2 >> log_blo_count_per_block),
			blkref2);

  blo2 = blkref[iblo & (blo_count_per_block - 1)];
  ret =
    kafs_readba (ctx, blkref + (iblo & (blo_count_per_block - 1)), blkref2);
  if (ret < 0)
    return ret;
  if (blo2 == 0)
    return kafs_writeb (ctx, blkref2 + (iblo2 & (blo_count_per_block - 1)),
			blkref);
  return 0;
}

/// @brief inode 毎にデータを読み出す
/// @param ctx コンテキスト
/// @param ino inode 番号
/// @param buf バッファ
/// @param size バッファサイズ
/// @param offset オフセット
/// @return > 0: 読み出しサイズ, 0: EOF, < 0: エラー(-errno)
static int
kafs_preadi (struct kafs_context *restrict ctx, uint32_t ino, void *buf,
	     size_t size, off_t offset)
{
  struct kafs_inode *restrict inode = ctx->inode + ino;
  uint32_t i_size = inode->i_size;

  // 60バイト以下は直接
  if (i_size <= sizeof (inode->i_block))
    {
      if (offset >= sizeof (inode->i_block))
	return 0;
      if (offset + size > i_size)
	size = i_size - offset;
      memcpy (buf, (void *) inode->i_block + offset, size);
      return size;
    }

  size_t size_read = 0;
  if (offset >= i_size)
    return 0;
  if (offset + size > i_size)
    size = i_size - offset;
  if (size == 0)
    return 0;

  int log_blksize = 10 + ctx->sb->s_log_block_size;
  size_t blksize = 1 << log_blksize;
  size_t oblk_off = offset & (blksize - 1);
  if (oblk_off > 0 || size - size_read < blksize)
    {
      char buf2[blksize];
      int ret = kafs_readib (ctx, inode, offset >> log_blksize, buf2);
      if (ret < 0)
	return ret;
      if (size < blksize - oblk_off)
	{
	  memcpy (buf, buf2 + oblk_off, size);
	  return size;
	}
      memcpy (buf, buf2 + oblk_off, blksize - oblk_off);
      size_read += blksize - oblk_off;
    }
  while (size_read < size)
    {
      blkcnt_t oblkid = (offset + size_read) >> log_blksize;
      if (size - size_read <= blksize)
	{
	  char buf2[blksize];
	  int ret = kafs_readib (ctx, inode, oblkid, buf2);
	  if (ret < 0)
	    return ret;
	  memcpy (buf + size_read, buf2, size - size_read);
	  return size;
	}
      int ret = kafs_readib (ctx, inode, oblkid, buf + size_read);
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
static int
kafs_pwritei (struct kafs_context *restrict ctx, uint32_t ino,
	      const void *buf, size_t size, off_t offset)
{
  struct kafs_inode *restrict inode = ctx->inode + ino;
  uint32_t i_size = inode->i_size;
  uint32_t i_size_new = offset + size;
  int log_blksize = 10 + ctx->sb->s_log_block_size;
  size_t blksize = 1 << log_blksize;

  if (i_size < i_size_new)
    {
      inode->i_size = i_size_new;
      if (i_size <= sizeof (inode->i_block))
	{
	  char buf2[blksize];
	  memset (buf2, 0, blksize);
	  memcpy (buf2, inode->i_block, i_size);
	  memset (inode->i_block, 0, sizeof (inode->i_block));
	  int ret = kafs_writeib (ctx, inode, 0, buf2);
	  if (ret < 0)
	    return ret;
	}
      i_size = i_size_new;
    }

  // 60バイト以下は直接
  if (i_size <= sizeof (inode->i_block))
    {
      if (offset >= sizeof (inode->i_block))
	return 0;
      if (offset + size > i_size)
	size = i_size - offset;
      memcpy (inode->i_block + offset, buf, size);
      return size;
    }

  size_t size_written = 0;
  if (offset >= i_size)
    return 0;
  if (offset + size > i_size)
    size = i_size - offset;
  if (size == 0)
    return 0;

  size_t oblk_off = offset & (blksize - 1);
  if (oblk_off > 0 || size - size_written < blksize)
    {
      char buf2[blksize];
      int ret = kafs_readib (ctx, inode, offset >> log_blksize, buf2);
      if (ret < 0)
	return ret;
      if (size < blksize - oblk_off)
	{
	  memcpy (buf2 + oblk_off, buf, size);
	  ret = kafs_writeib (ctx, inode, offset >> log_blksize, buf2);
	  if (ret < 0)
	    return ret;
	  return size;
	}
      memcpy (buf2 + oblk_off, buf, blksize - oblk_off);
      ret = kafs_writeib (ctx, inode, offset >> log_blksize, buf2);
      if (ret < 0)
	return ret;
      size_written += blksize - oblk_off;
    }

  while (size_written < size)
    {
      blkcnt_t oblkid = (offset + size_written) >> log_blksize;
      if (size - size_written <= blksize)
	{
	  char buf2[blksize];
	  int ret = kafs_readib (ctx, inode, oblkid, buf2);
	  if (ret < 0)
	    return ret;
	  memcpy (buf2, buf + size_written, size - size_written);
	  ret = kafs_writeib (ctx, inode, oblkid, buf2);
	  if (ret < 0)
	    return ret;
	  return size;
	}
      int ret = kafs_writeib (ctx, inode, oblkid, buf + size_written);
      if (ret < 0)
	return ret;
      size_written += blksize;
    }
  return size;
}

struct kafs_dirent
{
  kafs_inocnt_t ino;
  uint16_t len;
  char name[0];
};

/// @brief ディレクトリエントリを読み出す
/// @param ctx コンテキスト
/// @param ino_dir ディレクトリのinode番号
/// @param dirent 読み出すディレクトリエントリのバッファ（sizeof(struct kafs_dirent) 以上）
/// @param direntlen バッファの長さ（全体がこれより長い場合はファイル名部分が読みだされない）
/// @param offset 
/// @return 
static int
kafs_dreadent (struct kafs_context *restrict ctx, kafs_inocnt_t ino_dir,
	       struct kafs_dirent *dirent, size_t direntlen, off_t offset)
{
  assert (direntlen > sizeof (struct kafs_dirent));
  int r =
    kafs_preadi (ctx, ino_dir, dirent, sizeof (struct kafs_dirent), offset);
  if (r < 0)
    return r;
  if (r < sizeof (struct kafs_dirent))
    return -EIO;
  if (direntlen - sizeof (struct kafs_dirent) < dirent->len)
    return sizeof (struct kafs_dirent) + dirent->len;
  r =
    kafs_preadi (ctx, ino_dir, dirent->name, dirent->len,
		 offset + dirent->len);
  if (r < 0)
    return r;
  if (r < dirent->len)
    return -EIO;
  return sizeof (struct kafs_dirent) + dirent->len;
}

/// @brief ディレクトリエントリから対象のファイル名を探す
/// @param ctx コンテキスト
/// @param name ファイル名
/// @param namelen ファイル名の長さ
/// @param ino 対象のディレクトリ
/// @param ino_found 見つかったエントリ
/// @return 0: 成功, < 0: 失敗 (-errno)
static int
kafs_dgetent (struct kafs_context *restrict ctx, kafs_inocnt_t ino_dir,
	      const char *name, size_t namelen, kafs_inocnt_t * ino_found)
{
  const struct kafs_inode *restrict inode = kafs_iget_const (ctx, ino_dir);
  if (inode == NULL)
    return -ENOENT;
  if (!S_ISDIR (inode->i_mode))
    return -ENOTDIR;
  char buf[sizeof (struct kafs_dirent) + namelen];
  struct kafs_dirent *dirent = (struct kafs_dirent *) buf;
  off_t offset = 0;
  while (1)
    {
      ssize_t r = kafs_dreadent (ctx, ino_dir, dirent,
				 sizeof (struct kafs_dirent) + namelen,
				 offset);
      if (r < 0)
	return r;
      if (r < sizeof (struct kafs_dirent) + namelen)
	return -EIO;
      if (dirent->len == namelen && memcmp (name, dirent->name, namelen) == 0)
	{
	  *ino_found = dirent->ino;
	  return 0;
	}
      offset += r;
    }
}

static int
kafs_itruncate (struct kafs_context *ctx, kafs_inocnt_t ino, off_t size)
{
  assert (ctx != NULL);
  assert (ino < ctx->sb->s_inodes_count);
  struct kafs_inode *inode = kafs_iget (ctx, ino);
  int log_blksize = 10 + ctx->sb->s_log_block_size;
  size_t blksize = 1 << log_blksize;
  off_t i_size = inode->i_size;
  if (i_size == size)
    return 0;
  inode->i_size = size;
  if (i_size < size)
    return 0;
  int off = i_size & (blksize - 1);
  off_t offset = size;
  if (off > 0)
    {
      char buf[blksize];
      memset (buf, 0, blksize);
      int w = kafs_pwritei (ctx, ino, buf, blksize - off, offset);
      if (w < 0)
	return w;
      offset += blksize - off;
    }
  while (offset < i_size)
    {
      int ret = kafs_releaseib (ctx, inode, offset >> log_blksize);
      if (ret < 0)
	return ret;
    }
  return 0;
}

static int
kafs_daddent (struct kafs_context *ctx, kafs_inocnt_t ino_dir,
	      const char *name, size_t namelen, kafs_inocnt_t ino)
{
  const struct kafs_inode *restrict inode_dir =
    kafs_iget_const (ctx, ino_dir);
  if (inode_dir == NULL)
    return -ENOENT;
  if (!S_ISDIR (inode_dir->i_mode))
    return -ENOTDIR;
  char buf[sizeof (struct kafs_dirent) + namelen];
  struct kafs_dirent *dirent = (struct kafs_dirent *) buf;
  off_t offset = 0;
  while (1)
    {
      ssize_t r =
	kafs_preadi (ctx, ino_dir, &dirent, sizeof (struct kafs_dirent),
		     offset);
      if (r < 0)
	return r;
      if (r < sizeof (struct kafs_dirent))
	{
	addent:
	  dirent->ino = ino;
	  dirent->len = namelen;
	  memcpy (dirent->name, name, namelen);
	  int ret = kafs_itruncate (ctx, ino_dir, offset);
	  ssize_t w = kafs_pwritei (ctx, ino_dir, dirent,
				    sizeof (struct kafs_dirent) + namelen,
				    offset);
	  if (w < 0)
	    return w;
	  if (w < sizeof (struct kafs_dirent) + namelen)
	    return -EIO;
	  struct kafs_inode *inode = kafs_iget (ctx, ino);
	  inode->i_links_count++;
	  return 0;
	}
      if (dirent->len == namelen)
	{
	  ssize_t r2 =
	    kafs_preadi (ctx, ino_dir, dirent->name, namelen, offset + r);
	  if (r2 < 0)
	    return r2;
	  if (r2 < namelen)
	    goto addent;
	  if (memcmp (name, dirent->name, namelen) == 0)
	    return -EEXIST;
	}
      offset += r + dirent->len;
    }
}

static int
kafs_ddelent (struct kafs_context *ctx, kafs_inocnt_t ino_dir,
	      const char *name, size_t namelen)
{
  const struct kafs_inode *restrict inode = kafs_iget_const (ctx, ino_dir);
  if (inode == NULL)
    return -ENOENT;
  if (!S_ISDIR (inode->i_mode))
    return -ENOTDIR;
  char buf[sizeof (struct kafs_dirent) + namelen];
  struct kafs_dirent *dirent = (struct kafs_dirent *) buf;
  off_t offset = 0;
  while (1)
    {
      ssize_t r =
	kafs_preadi (ctx, ino_dir, &dirent, sizeof (struct kafs_dirent),
		     offset);
      if (r < 0)
	return r;
      if (r < sizeof (struct kafs_dirent))
	return -ENOENT;
      if (dirent->len == namelen)
	{
	  ssize_t r2 =
	    kafs_preadi (ctx, ino_dir, dirent->name, namelen, offset + r);
	  if (r2 < 0)
	    return r2;
	  if (r2 < namelen)
	    return -ENOENT;
	  if (memcmp (name, dirent->name, namelen) == 0)
	    while (1)
	      {


	      }
	}
      offset += r + dirent->len;
    }
}

static int
kafs_iresolvepath (struct kafs_context *restrict ctx, const char *path,
		   kafs_inocnt_t * ino)
{
  kafs_inocnt_t i = *ino;
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
      int ret = kafs_dgetent (ctx, p, frag - p, i, &j);
      if (ret < 0)
	return ret;
      i = j;
      p = frag;
      if (*p == '/')
	p++;
    }
  *ino = i;
  return 0;
}

static int
kafs_op_getattr (const char *path, struct stat *st, struct fuse_file_info *fi)
{
  struct fuse_context *fctx = fuse_get_context ();
  struct kafs_context *ctx = fctx->private_data;
  kafs_inocnt_t ino = fi->fh;
  struct kafs_inode *inode = kafs_iget (ctx, ino);
  st->st_dev = 0;
  st->st_ino = ino + 1;
  st->st_mode = inode->i_mode;
  st->st_nlink = inode->i_links_count;
  st->st_uid = inode->i_uid;
  st->st_gid = inode->i_gid;
  st->st_rdev = inode->i_rdev;
  st->st_size = inode->i_size;
  st->st_blksize = 1 << (10 + ctx->sb->s_log_block_size);
  st->st_blocks = inode->i_links_count;
  return 0;
}

static int
kafs_op_open (const char *path, struct fuse_file_info *fi)
{
  struct fuse_context *fctx = fuse_get_context ();
  struct kafs_context *ctx = fctx->private_data;
  kafs_inocnt_t ino = 0;
  int ret = kafs_iresolvepath (ctx, path, &ino);
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
  int ret = kafs_iresolvepath (ctx, dirpath, &ino_dir);
  if (ret < 0)
    return -ret;
  struct kafs_inode *inode_dir = kafs_iget (ctx, ino_dir);
  if (!S_ISDIR (inode_dir->i_mode))
    return -EIO;
  kafs_inocnt_t ino;
  ret = kafs_ialloc (ctx, &ino);
  if (ret < 0)
    return ret;
  struct kafs_inode *inode = kafs_iget (ctx, ino);
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
  memset (inode->i_block, 0, sizeof (inode->i_block));
  ret = kafs_daddent (ctx, ino_dir, filename, ino);
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
  int ret = kafs_iresolvepath (ctx, path, &ino);
  if (ret < 0)
    return ret;
  ssize_t r = kafs_preadi (ctx, ino, buf, buflen - 1, 0);
  if (r < 0)
    return r;
  buf[r] = '\0';
  return 0;
}

static int
kafs_op_read (const char *path, char *buf, size_t size, off_t offset,
	      struct fuse_file_info *fi)
{
  struct fuse_context *fctx = fuse_get_context ();
  struct kafs_context *ctx = fctx->private_data;
  kafs_inocnt_t ino = fi->fh;
  return kafs_preadi (ctx, ino, buf, size, offset);
}

static int
kafs_op_write (const char *path, const char *buf, size_t size, off_t offset,
	       struct fuse_file_info *fi)
{
  struct fuse_context *fctx = fuse_get_context ();
  struct kafs_context *ctx = fctx->private_data;
  kafs_inocnt_t ino = fi->fh;
  return kafs_pwritei (ctx, ino, buf, size, offset);
}

static struct fuse_operations kafs_operations = {
  .getattr = kafs_op_getattr,
  .open = kafs_op_open,
  .mknod = kafs_op_mknod,
  .readlink = kafs_op_readlink,
  .read = kafs_op_read,
  .write = kafs_op_write,
};
