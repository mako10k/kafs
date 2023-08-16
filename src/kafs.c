#define _GNU_SOURCE
#define _FILE_OFFSET_BITS 64
#define FUSE_USE_VERSION 30
#include <fuse.h>
#include <errno.h>

struct kafs_superblock
{
  uint32_t s_inodes_count;
  uint32_t s_blocks_count;
  uint32_t s_r_blocks_count;
  uint32_t s_free_blocks_count;
  uint32_t s_free_inodes_count;
  uint32_t s_first_data_block;
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
  uint32_t i_blocks;
  uint32_t i_block[15];
};

struct kafs_context
{
  struct kafs_superblock *sb;
  struct kafs_inode *inode;
  unsigned int *blkmask;
  uint32_t ino_search;
  uint32_t bmidx_search;
  int fd;
};

/// @brief 空いている inode 番号を見つける
/// @param ctx コンテキスト
/// @param ino 見つかった inode 番号
/// @return 0: 成功、 < 0: 失敗 (-errno)
static int
kafs_find_free_ino (struct kafs_context *restrict ctx, uint32_t * ino)
{
  uint32_t ino_count = ctx->sb->s_inodes_count;
  uint32_t ino_search = ctx->ino_search;
  uint32_t i = ino_search + 1;
  struct kafs_inode *restrict inode = ctx->inode;
  while (ino_search != i)
    {
      if (ino >= ino_count)
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

/// @brief 指定されたブロックのフラグを操作する
/// @param ctx コンテキスト
/// @param blo ブロック番号
/// @param val 0: フラグをクリア, != 0: フラグをセット
/// @return 0: 成功, < 0: 失敗 (-errno)
static int
kafs_set_blo (struct kafs_context *restrict ctx, uint32_t blo, int val)
{
  int b = blo / (sizeof (*ctx->blkmask) * 8);
  int i = blo % (sizeof (*ctx->blkmask) * 8);
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
kafs_find_free_blo (struct kafs_context *restrict ctx, uint32_t * blo)
{
  uint32_t bmidx_search = ctx->bmidx_search;
  uint32_t bmidx = bmidx_search + 1;
  unsigned int *restrict blkmask = ctx->blkmask;
  uint32_t bm_count =
    (ctx->sb->s_blocks_count +
     (sizeof (*ctx->blkmask) * 8 - 1)) / (sizeof (*ctx->blkmask) * 8);
  while (bmidx_search != bmidx)
    {
      if (bmidx >= bm_count)
	bmidx = 0;
      unsigned int bm = ~blkmask[bmidx];
      if (bm != 0)
	{
	  // lsb側から続く0の数、つまり
	  // 最もlsb側に存在する 1 のビット位置
	  int ctz = __builtin_ctz (bm);
	  uint32_t b = bmidx * (sizeof (*blkmask) * 8) + ctz;
	  if (b < ctx->sb->s_blocks_count)
	    {
	      ctx->bmidx_search = bmidx;
	      *blo = b;
	      blkmask[bmidx] |= 1 << ctz;
	      return 0;
	    }
	}
      bmidx++;
    }
  return -ENOSPC;
}

/// @brief inode毎のブロック順でデータを読み出す
/// @param ctx コンテキスト
/// @param inode inode情報
/// @param buf バッファ
/// @param iblo ブロック番号
/// @return 0: 成功, < 0: 失敗 (-errno)
static int
kafs_read_data_by_inode_blo (struct kafs_context *restrict ctx,
			     struct kafs_inode *restrict inode,
			     uint32_t iblo, void *buf)
{
  int log_blksize = 10 + ctx->sb->s_log_block_size;
  size_t blksize = 1 << log_blksize;
  uint32_t blo;

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
  int log_blo_count_per_block = log_blksize >> (sizeof (*inode->i_block) * 8);
  blksize_t blo_count_per_block = 1 << log_blo_count_per_block;
  char buf2[blksize];
  typeof (inode->i_block) blkref = (void *) buf2;
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

/// @brief inode毎のブロック順でデータを書き込む
/// @param ctx コンテキスト
/// @param inode inode情報
/// @param buf バッファ
/// @param iblo ブロック番号
/// @return 0: 成功, < 0: 失敗 (-errno)
static int
kafs_write_data_by_inode_blo (struct kafs_context *restrict ctx,
			      struct kafs_inode *restrict inode,
			      uint32_t iblo, const void *buf)
{
  int log_blksize = 10 + ctx->sb->s_log_block_size;
  size_t blksize = 1 << log_blksize;
  uint32_t blo;

  // 0..11 は 直接
  if (iblo < 12)
    {
      blo = inode->i_block[iblo];
      if (blo == 0)
	{
	  int ret = kafs_find_free_blo (ctx, &blo);
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
  int log_blo_count_per_block = log_blksize - sizeof (*inode->i_block) * 8;
  blksize_t blo_count_per_block = 1 << log_blo_count_per_block;
  char buf2[blksize];
  typeof (inode->i_block) blkref = (void *) buf2;
  if (iblo < blo_count_per_block)
    {
      blo = inode->i_block[12];
      if (blo == 0)
	{
	  int ret = kafs_find_free_blo (ctx, &blo);
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
	  int ret = kafs_find_free_blo (ctx, &blo2);
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
	  int ret = kafs_find_free_blo (ctx, &blo);
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
	  int ret = kafs_find_free_blo (ctx, &blo2);
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
	  int ret = kafs_find_free_blo (ctx, &blo3);
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
      int ret = kafs_find_free_blo (ctx, &blo);
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
  uint32_t blo2 = blkref[blkid2 >> log_blo_count_per_block];
  if (blo2 == 0)
    {
      int ret = kafs_find_free_blo (ctx, &blo2);
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
  uint32_t blo3 = blkref[blkid2 & (1 - blo_count_per_block)];
  if (blo3 == 0)
    {
      int ret = kafs_find_free_blo (ctx, &blo3);
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
  uint32_t blo4 = blkref[iblo & (1 - blo_count_per_block)];
  if (blo4 == 0)
    {
      int ret = kafs_find_free_blo (ctx, &blo4);
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
kafs_read_ino (struct kafs_context *restrict ctx, uint32_t ino, void *buf,
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
      int ret = kafs_read_data_by_inode_blo (ctx, inode, buf2,
					     offset >> log_blksize);
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
	  int ret = kafs_read_data_by_inode_blo (ctx, inode, buf2, oblkid);
	  if (ret < 0)
	    return ret;
	  memcpy (buf, buf2, size - size_read);
	  return size;
	}
      int ret = kafs_read_data_by_inode_blo (ctx, inode, buf, oblkid);
      if (ret < 0)
	return ret;
      size_read += blksize;
    }
  return size;
}

static int
kafs_op_getattr (const char *path, struct stat *st, struct fuse_file_info *fi)
{
  struct kafs_stat ino_info;
  int ret;
  ret = kafs_stat (path, &ino_info, fi);
  if (ret < 0)
    return ret;
  st->st_dev = 0;
  st->st_ino = ino_info.kst_ino;
  st->st_mode = ino_info.kst_mode;
  st->st_nlink = ino_info.kst_nlink;
  st->st_uid = ino_info.kst_uid;
  st->st_gid = ino_info.kst_gid;
  st->st_rdev = ino_info.kst_rdev;
  st->st_size = ino_info.kst_size;
  st->st_blksize = 0;
  st->st_blocks = ino_info.kst_blocks;
  return 0;
}

static ssize_t
kafs_raw_pread (const char *path, char *buf, size_t buflen,
		struct fuse_file_info *fi, off_t offset)
{
  struct kafs_raw_stat raw_inode_info;
  int ret;
  ret = kafs_raw_stat (path, &raw_inode_info, fi);
}

static int
kafs_op_readlink (const char *path, char *buf, size_t buflen)
{
  ssize_t r;
  r = kafs_raw_pread (path, buf, buflen - 1, NULL, 0);
  if (r < 0)
    return r;
  if (r == buflen - 1)
    buf[buflen - 1] = '\0';
  return 0;
}

static int
kafs_access (const char *path, int mode)
{

}


static struct fuse_operations kafs_operations = {
  .getattr = kafs_op_getattr,
  .readlink = kafs_op_readlink,
};
