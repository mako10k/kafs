#define FUSE_USE_VERSION 30

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include <fuse.h>
#include <errno.h>
#include <string.h>
#include <unistd.h>

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
  union
  {
    kafs_blkcnt_t i_block[15];
    uint32_t i_rdev;
  };
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

static struct kafs_inode *
kafs_iget (struct kafs_context *restrict ctx, kafs_inocnt_t ino)
{
  return ctx->inode[ino].i_mode ? ctx->inode + ino : NULL;
}

static const struct kafs_inode *restrict
kafs_iget_const (const struct kafs_context *restrict ctx, kafs_inocnt_t ino)
{
  return ctx->inode[ino].i_mode ? ctx->inode + ino : NULL;
}

/// @brief 空いている inode 番号を見つける
/// @param ctx コンテキスト
/// @param ino 見つかった inode 番号
/// @return 0: 成功、 < 0: 失敗 (-errno)
static int
kafs_ialloc (struct kafs_context *restrict ctx, kafs_inocnt_t * ino)
{
  kafs_inocnt_t ino_count = ctx->sb->s_inodes_count;
  kafs_inocnt_t ino_search = ctx->ino_search;
  kafs_inocnt_t i = ino_search + 1;
  struct kafs_inode *restrict inode = kafs_iget (ctx, 0);
  while (ino_search != i)
    {
      if (i >= ino_count)
	i = 0;
      if (inode[i].i_mode == 0)
	{
	  ctx->ino_search = i;
	  *ino = i;
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
  int b = blo >> KAFS_BLKMASK_LOG_BITS;
  int i = blo & KAFS_BLKMASK_MASK_BITS;
  return (ctx->blkmask[b] & (1 << i)) != 0;
}

/// @brief 指定されたブロックのフラグを操作する
/// @param ctx コンテキスト
/// @param blo ブロック番号
/// @param val 0: フラグをクリア, != 0: フラグをセット
/// @return 0: 成功, < 0: 失敗 (-errno)
static int
kafs_bmset (struct kafs_context *restrict ctx, kafs_blkcnt_t blo, int val)
{
  int b = blo >> KAFS_BLKMASK_LOG_BITS;
  int i = blo & KAFS_BLKMASK_MASK_BITS;
  if (val)
    ctx->blkmask[b] |= 1 << i;
  else
    ctx->blkmask[b] &= ~(1 << i);
  return 0;
}

/// @brief 未使用のブロック番号を取得し、使用中フラグをつける
/// @param ctx コンテキスト
/// @param blo ブロック番号
/// @return 0: 成功, < 0: 失敗 (-errno)
static uint32_t
kafs_balloc (struct kafs_context *restrict ctx, kafs_blkcnt_t * blo)
{
  kafs_blkcnt_t blo_search = ctx->blo_search;
  kafs_blkcnt_t b = blo_search + 1;
  kafs_blkmask_t *restrict blkmask = ctx->blkmask;
  kafs_blkcnt_t blo_max = ctx->sb->s_blocks_count;
  while (blo_search != b)
    {
      if (b >= blo_max)
	b = 0;
      int bb = b >> KAFS_BLKMASK_LOG_BITS;
      int bi = b & KAFS_BLKMASK_MASK_BITS;
      kafs_blkmask_t bm = ~blkmask[bb];
      if (bm != 0)
	{
	  // __builtin_ctz は、lsb側から続く0の数、つまり
	  // 最もlsb側に存在する 1 のビット位置
	  int bi_found = __builtin_ctz (bm);
	  kafs_blkcnt_t b_found = (bb << KAFS_BLKMASK_LOG_BITS) + bi_found;
	  if (b_found < blo_max)
	    {
	      ctx->blo_search = b_found;
	      *blo = b_found;
	      blkmask[bb] |= 1 << bi_found;
	      return 0;
	    }
	}
      b++;
    }
  return -ENOSPC;
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
  int log_blksize = 10 + ctx->sb->s_log_block_size;
  size_t blksize = 1 << log_blksize;
  kafs_blkcnt_t blo;

  // 0..11 は 直接
  if (iblo < 12)
    {
      blo = inode->i_block[iblo];
      if (blo == 0)
	{
	  memset (buf, 0, blksize);
	  return 0;
	}
      ssize_t r = pread (ctx->fd, buf, blksize, blksize * blo);
      if (r < 0)
	return -errno;
      return 0;
    }
  iblo -= 12;
  // 12 .. ブロックサイズ / 4 + 11 は、間接（１段階） 
  int log_blo_count_per_block = log_blksize - (sizeof (*inode->i_block) + 3);
  blksize_t blo_count_per_block = 1 << log_blo_count_per_block;
  char buf2[blksize];
  kafs_blkcnt_t *blkref = (kafs_blkcnt_t *) buf2;
  if (iblo < blo_count_per_block)
    {
      blo = inode->i_block[12];
      if (blo == 0)
	{
	  memset (buf, 0, blksize);
	  return 0;
	}
      ssize_t r = pread (ctx->fd, buf2, blksize, blksize * blo);
      if (r < 0)
	return -errno;
      blo = blkref[iblo];
      if (blo == 0)
	{
	  memset (buf, 0, blksize);
	  return 0;
	}
      r = pread (ctx->fd, buf, blksize, blksize * blo);
      if (r < 0)
	return -errno;
      return 0;
    }

  // ブロックサイズ/4 + 12  .. (ブロックサイズ/4)^2 + ブロックサイズ / 4 + 11 は、間接（２段階） 
  iblo -= blo_count_per_block;
  blkcnt_t blkid_per_block2 = 1 << (log_blo_count_per_block * 2);
  if (iblo < blkid_per_block2)
    {
      blo = inode->i_block[13];
      if (blo == 0)
	{
	  memset (buf, 0, blksize);
	  return 0;
	}
      ssize_t r = pread (ctx->fd, buf2, blksize, blksize * blo);
      if (r < 0)
	return -errno;
      blo = blkref[iblo >> log_blo_count_per_block];
      if (blo == 0)
	{
	  memset (buf, 0, blksize);
	  return 0;
	}
      r = pread (ctx->fd, buf2, blksize, blksize * blo);
      if (r < 0)
	return -errno;
      blo = blkref[iblo & (blo_count_per_block - 1)];
      if (blo == 0)
	{
	  memset (buf, 0, blksize);
	  return 0;
	}
      r = pread (ctx->fd, buf, blksize, blksize * blo);
      if (r < 0)
	return -errno;
      return r;
    }

  // (ブロックサイズ/4)^2 + ブロックサイズ / 4 + 12 .. Inf は、間接（３段階） 
  iblo -= blkid_per_block2;
  blo = inode->i_block[14];
  if (blo == 0)
    {
      memset (buf, 0, blksize);
      return 0;
    }
  ssize_t r = pread (ctx->fd, buf2, blksize, blksize * blo);
  if (r < 0)
    return -errno;
  int blkid2 = iblo >> log_blo_count_per_block;
  blo = blkref[blkid2 >> log_blo_count_per_block];
  if (blo == 0)
    {
      memset (buf, 0, blksize);
      return 0;
    }
  r = pread (ctx->fd, buf2, blksize, blksize * blo);
  if (r < 0)
    return -errno;
  blo = blkref[blkid2 & (1 - blo_count_per_block)];
  if (blo == 0)
    {
      memset (buf, 0, blksize);
      return 0;
    }
  r = pread (ctx->fd, buf2, blksize, blksize * blo);
  if (r < 0)
    return -errno;
  blo = blkref[iblo & (1 - blo_count_per_block)];
  if (blo == 0)
    {
      memset (buf, 0, blksize);
      return 0;
    }
  r = pread (ctx->fd, buf, blksize, blksize * blo);
  if (r < 0)
    return -errno;
  return 0;
}

/// @brief inode毎のデータを書き込む（ブロック単位）
/// @param ctx コンテキスト
/// @param inode inode情報
/// @param buf バッファ
/// @param iblo ブロック番号
/// @return 0: 成功, < 0: 失敗 (-errno)
static int
kafs_writeib (struct kafs_context *restrict ctx,
	      struct kafs_inode *restrict inode,
	      kafs_blkcnt_t iblo, const void *buf)
{
  int log_blksize = 10 + ctx->sb->s_log_block_size;
  size_t blksize = 1 << log_blksize;
  kafs_blkcnt_t blo;

  // 0..11 は 直接
  if (iblo < 12)
    {
      blo = inode->i_block[iblo];
      if (blo == 0)
	{
	  int ret = kafs_balloc (ctx, &blo);
	  if (ret < 0)
	    return ret;
	  inode->i_block[iblo] = blo;
	}
      ssize_t r = pwrite (ctx->fd, buf, blksize, blksize * blo);
      if (r < 0)
	return -errno;
      return 0;
    }

  // 12 .. ブロックサイズ / 4 + 11 は、間接（１段階） 
  iblo -= 12;
  int log_blo_count_per_block = log_blksize - (sizeof (*inode->i_block) + 3);
  blksize_t blo_count_per_block = 1 << log_blo_count_per_block;
  char buf2[blksize];
  kafs_blkcnt_t *blkref = (kafs_blkcnt_t *) buf2;
  if (iblo < blo_count_per_block)
    {
      blo = inode->i_block[12];
      if (blo == 0)
	{
	  int ret = kafs_balloc (ctx, &blo);
	  if (ret < 0)
	    return ret;
	  inode->i_block[12] = blo;
	  // データを読む替わりにゼロセット
	  memset (buf2, 0, blksize);
	}
      else
	{
	  ssize_t r = pread (ctx->fd, buf2, blksize, blksize * blo);
	  if (r < 0)
	    return -errno;
	}
      uint32_t blo2 = blkref[iblo];
      if (blo2 == 0)
	{
	  int ret = kafs_balloc (ctx, &blo2);
	  if (ret < 0)
	    return ret;
	  blkref[iblo] = blo2;
	  ssize_t w = pwrite (ctx->fd, buf2, blksize, blksize * blo);
	  if (w < 0)
	    return -errno;
	}

      // データを書き込む
      ssize_t w = pwrite (ctx->fd, buf, blksize, blksize * blo2);
      if (w < 0)
	return -errno;
      return 0;
    }

  // ブロックサイズ/4 + 12  .. (ブロックサイズ/4)^2 + ブロックサイズ / 4 + 11 は、間接（２段階） 
  iblo -= blo_count_per_block;
  blkcnt_t blkid_per_block2 = 1 << (log_blo_count_per_block * 2);
  if (iblo < blkid_per_block2)
    {
      blo = inode->i_block[13];
      if (blo == 0)
	{
	  int ret = kafs_balloc (ctx, &blo);
	  if (ret < 0)
	    return ret;
	  inode->i_block[13] = blo;
	  // データを読む替わりにゼロセット
	  memset (buf2, 0, blksize);
	}
      else
	{
	  ssize_t r = pread (ctx->fd, buf2, blksize, blksize * blo);
	  if (r < 0)
	    return -errno;
	}
      uint32_t blo2 = blkref[iblo >> log_blo_count_per_block];
      if (blo2 == 0)
	{
	  int ret = kafs_balloc (ctx, &blo2);
	  if (ret < 0)
	    return ret;
	  blkref[iblo >> log_blo_count_per_block] = blo2;
	  ssize_t w = pwrite (ctx->fd, buf2, blksize, blksize * blo);
	  if (w < 0)
	    return -errno;
	  memset (buf2, 0, blksize);
	}
      else
	{
	  ssize_t r = pread (ctx->fd, buf2, blksize, blksize * blo2);
	  if (r < 0)
	    return -errno;
	}
      uint32_t blo3 = blkref[iblo & (blo_count_per_block - 1)];
      if (blo3 == 0)
	{
	  int ret = kafs_balloc (ctx, &blo3);
	  if (ret < 0)
	    return ret;
	  blkref[iblo & (blo_count_per_block - 1)] = blo3;
	  ssize_t w = pwrite (ctx->fd, buf2, blksize, blksize * blo2);
	  if (w < 0)
	    return -errno;
	  memset (buf2, 0, blksize);
	}
      ssize_t w = pwrite (ctx->fd, buf, blksize, blksize * blo3);
      if (w < 0)
	return -errno;
      return w;
    }

  // (ブロックサイズ/4)^2 + ブロックサイズ / 4 + 12 .. Inf は、間接（３段階） 
  iblo -= blkid_per_block2;
  blo = inode->i_block[14];
  if (blo == 0)
    {
      int ret = kafs_balloc (ctx, &blo);
      if (ret < 0)
	return ret;
      inode->i_block[14] = blo;
      // データを読む替わりにゼロセット
      memset (buf2, 0, blksize);
    }
  else
    {
      ssize_t r = pread (ctx->fd, buf2, blksize, blksize * blo);
      if (r < 0)
	return -errno;
    }
  int blkid2 = iblo >> log_blo_count_per_block;
  kafs_blkcnt_t blo2 = blkref[blkid2 >> log_blo_count_per_block];
  if (blo2 == 0)
    {
      int ret = kafs_balloc (ctx, &blo2);
      if (ret < 0)
	return ret;
      blkref[blkid2 >> log_blo_count_per_block] = blo2;
      ssize_t w = pwrite (ctx->fd, buf2, blksize, blksize * blo);
      if (w < 0)
	return -errno;
      memset (buf2, 0, blksize);
    }
  else
    {
      ssize_t r = pread (ctx->fd, buf2, blksize, blksize * blo2);
      if (r < 0)
	return -errno;
    }
  kafs_blkcnt_t blo3 = blkref[blkid2 & (1 - blo_count_per_block)];
  if (blo3 == 0)
    {
      int ret = kafs_balloc (ctx, &blo3);
      if (ret < 0)
	return ret;
      blkref[blkid2 & (1 - blo_count_per_block)] = blo3;
      ssize_t w = pwrite (ctx->fd, buf2, blksize, blksize * blo2);
      if (w < 0)
	return -errno;
      memset (buf2, 0, blksize);
    }
  else
    {
      ssize_t r = pread (ctx->fd, buf2, blksize, blksize * blo3);
      if (r < 0)
	return -errno;
    }
  kafs_blkcnt_t blo4 = blkref[iblo & (1 - blo_count_per_block)];
  if (blo4 == 0)
    {
      int ret = kafs_balloc (ctx, &blo4);
      if (ret < 0)
	return ret;
      blkref[iblo & (1 - blo_count_per_block)] = blo4;
      ssize_t w = pwrite (ctx->fd, buf2, blksize, blksize * blo3);
      if (w < 0)
	return -errno;
      memset (buf2, 0, blksize);
    }
  ssize_t w = pwrite (ctx->fd, buf, blksize, blksize * blo4);
  if (w < 0)
    return -errno;
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
	  memcpy (buf, buf2, size - size_read);
	  return size;
	}
      int ret = kafs_readib (ctx, inode, oblkid, buf);
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
	  memcpy (buf, buf2 + oblk_off, size);
	  return size;
	}
      memcpy (buf, buf2 + oblk_off, blksize - oblk_off);
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
	  memcpy (buf, buf2, size - size_written);
	  return size;
	}
      int ret = kafs_readib (ctx, inode, oblkid, buf);
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

/// @brief ディレクトリエントリから対象のファイル名を探す
/// @param ctx コンテキスト
/// @param name ファイル名
/// @param namelen ファイル名の長さ
/// @param ino 対象のディレクトリ
/// @param ino_found 見つかったエントリ
/// @return 0: 成功, < 0: 失敗 (-errno)
static int
kafs_dfindname (struct kafs_context *restrict ctx, const char *name,
		size_t namelen, kafs_inocnt_t ino, kafs_inocnt_t * ino_found)
{
  char buf[sizeof (struct kafs_dirent) + namelen];
  struct kafs_dirent *dirent = (struct kafs_dirent *) buf;
  off_t offset = 0;
  while (1)
    {
      const struct kafs_inode *restrict inode = kafs_iget_const (ctx, ino);
      if (inode == NULL)
	return -ENOENT;
      if (!S_ISDIR (inode->i_mode))
	return -ENOTDIR;
      ssize_t r =
	kafs_preadi (ctx, ino, &dirent, sizeof (struct kafs_dirent), offset);
      if (r < 0)
	return r;
      if (r < sizeof (struct kafs_dirent))
	return -ENOENT;
      offset += r;
      if (dirent->len == namelen)
	{
	  r = kafs_preadi (ctx, ino, dirent->name, namelen, offset);
	  if (r < 0)
	    return r;
	  if (r < namelen)
	    return -ENOENT;
	  if (memcmp (name, dirent->name, namelen) == 0)
	    {
	      *ino_found = dirent->ino;
	      return 0;
	    }
	}
      offset += dirent->len;
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
      int ret = kafs_dfindname (ctx, p, frag - p, i, &j);
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
  kafs_inocnt_t ino;
  int ret = kafs_iresolvepath (ctx, path, &ino);
  if (ret < 0)
    return ret;
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
kafs_op_readlink (const char *path, char *buf, size_t buflen)
{
  struct fuse_context *fctx = fuse_get_context ();
  struct kafs_context *ctx = fctx->private_data;
  kafs_inocnt_t ino;
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
  kafs_inocnt_t ino;
  int ret = kafs_iresolvepath (ctx, path, &ino);
  if (ret < 0)
    return ret;
  return kafs_preadi (ctx, ino, buf, size, offset);
}

static int
kafs_op_write (const char *path, const char *buf, size_t size, off_t offset,
	       struct fuse_file_info *fi)
{
  struct fuse_context *fctx = fuse_get_context ();
  struct kafs_context *ctx = fctx->private_data;
  kafs_inocnt_t ino;
  int ret = kafs_iresolvepath (ctx, path, &ino);
  if (ret < 0)
    return ret;
  return kafs_pwritei (ctx, ino, buf, size, offset);
}

static struct fuse_operations kafs_operations = {
  .getattr = kafs_op_getattr,
  .readlink = kafs_op_readlink,
  .read = kafs_op_read,
  .write = kafs_op_write,
};
