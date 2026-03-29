#include "kafs.h"
#include "kafs_context.h"
#include "kafs_superblock.h"
#include "kafs_block.h"
#include "kafs_inode.h"
#include "kafs_dirent.h"
#include "kafs_hash.h"
#include "kafs_journal.h"
#include "kafs_tailmeta.h"
#include "kafs_cli_opts.h"
#include "kafs_tool_util.h"

#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <sys/mman.h>
#include <sys/ioctl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#ifdef __linux__
#include <linux/fs.h>
#include <sys/syscall.h>
#include <linux/falloc.h>
#endif

static void usage(const char *prog)
{
  fprintf(stderr, "Usage:\n");
  fprintf(stderr, "  %s <image> [options]\n", prog);
  fprintf(stderr, "\n");
  fprintf(stderr, "Options:\n");
  fprintf(stderr, "  [Layout]\n");
  fprintf(stderr, "    -s, --size-bytes <N>              Total image size (default: 1GiB)\n");
  fprintf(stderr, "    -b, --blksize-log <L>             Block size log2 (default: 12 => 4096B)\n");
  fprintf(stderr, "    --format-version <V>              On-disk format version (default: 5)\n");
  fprintf(
      stderr,
      "    -i, --inodes <I>                  Inode count (default: 1 inode per 16KiB, min: 256)\n");
  fprintf(stderr,
          "    -J, --journal-size-bytes <J>      Journal size (default: 1MiB, min: 4KiB)\n");
  fprintf(stderr, "    --hrl-entry-ratio <R>             HRL entries/data-block ratio (default: "
                  "0.75, range: (0,1])\n");
  fprintf(stderr, "    --yes                             Skip overwrite confirmation prompt\n");
  fprintf(stderr, "\n");
  fprintf(stderr, "  [Space Reclaim]\n");
  fprintf(stderr,
          "    --trim-data-area                  Punch holes for free data area after format\n");
  fprintf(stderr, "\n");
  fprintf(stderr, "Notes:\n");
  fprintf(stderr, "    Size values accept K/M/G suffixes (binary units).\n");
  fprintf(stderr,
          "    If the image file already exists and has non-zero size, that size overrides -s.\n");
    fprintf(stderr,
      "    New images default to format version 5; use --format-version 4 for legacy v4 images.\n");
}

static int mkfs_confirm_overwrite_stdin(void)
{
  char buf[32];

  for (;;)
  {
    fprintf(stderr, "Overwrite existing filesystem? [Y/n]: ");
    fflush(stderr);

    if (!fgets(buf, sizeof(buf), stdin))
      return 0;

    buf[strcspn(buf, "\r\n")] = '\0';
    if (buf[0] == '\0' || strcasecmp(buf, "y") == 0 || strcasecmp(buf, "yes") == 0)
      return 1;
    if (strcasecmp(buf, "n") == 0 || strcasecmp(buf, "no") == 0)
      return 0;

    fprintf(stderr, "Please answer Y or n.\n");
  }
}

static int mkfs_trim_range(int fd, off_t off, off_t len)
{
  if (len <= 0)
    return 0;
#ifdef __linux__
#ifdef SYS_fallocate
  if (syscall(SYS_fallocate, fd, FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE, off, len) == 0)
    return 0;
  return -errno;
#else
  (void)fd;
  (void)off;
  (void)len;
  return -ENOSYS;
#endif
#else
  (void)fd;
  (void)off;
  (void)len;
  return -ENOTSUP;
#endif
}

struct mkfs_layout
{
  off_t mapsize;
  off_t blkmask_off;
  off_t inotbl_off;
  off_t allocator_off;
  size_t allocator_size;
  uint32_t hrl_bucket_cnt;
  size_t hrl_index_size;
  off_t hrl_index_off;
  uint32_t hrl_entry_cnt;
  off_t hrl_entry_off;
  off_t journal_off;
  size_t pendinglog_size;
  off_t pendinglog_off;
  size_t tailmeta_size;
  off_t tailmeta_off;
};

#define KAFS_MKFS_DEFAULT_BYTES_PER_INODE (16u * 1024u)
#define KAFS_MKFS_MIN_INODES 256u

static kafs_inocnt_t mkfs_default_inocnt_for_size(off_t total_bytes)
{
  uint64_t inode_count = 0;

  if (total_bytes <= 0)
    return KAFS_MKFS_MIN_INODES;

  inode_count = (uint64_t)total_bytes / (uint64_t)KAFS_MKFS_DEFAULT_BYTES_PER_INODE;
  if (inode_count < (uint64_t)KAFS_MKFS_MIN_INODES)
    inode_count = (uint64_t)KAFS_MKFS_MIN_INODES;
  if (inode_count > (uint64_t)UINT32_MAX)
    inode_count = (uint64_t)UINT32_MAX;

  return (kafs_inocnt_t)inode_count;
}

static int mkfs_format_version_is_supported(uint32_t format_version)
{
  return format_version == KAFS_FORMAT_VERSION || format_version == KAFS_FORMAT_VERSION_V5;
}

static size_t mkfs_tailmeta_region_size(uint32_t format_version, kafs_blksize_t blksize)
{
  return (format_version == KAFS_FORMAT_VERSION_V5) ? kafs_tailmeta_default_region_bytes(blksize)
                                                    : 0u;
}

static uint64_t mkfs_feature_flags_for_format(uint32_t format_version)
{
  uint64_t flags = KAFS_FEATURE_ALLOC_V2;

  if (format_version == KAFS_FORMAT_VERSION_V5)
    flags |= KAFS_FEATURE_TAIL_META_REGION;
  return flags;
}

static void compute_layout(uint32_t format_version, kafs_blkcnt_t blkcnt,
                           kafs_blksize_t blksizemask, kafs_blksize_t blksize, kafs_inocnt_t inocnt,
                           size_t journal_bytes, double hrl_entry_ratio, struct mkfs_layout *out)
{
  off_t mapsize = 0;
  mapsize += sizeof(kafs_ssuperblock_t);
  mapsize = (mapsize + blksizemask) & ~blksizemask;
  off_t blkmask_off = mapsize;
  mapsize += (blkcnt + 7) >> 3;                     // bitmap (bytes) = ceil(blkcnt/8)
  mapsize = (mapsize + 7) & ~7;                     // 64-bit align
  mapsize = (mapsize + blksizemask) & ~blksizemask; // block align
  off_t inotbl_off = mapsize;
  mapsize += (off_t)kafs_inode_table_bytes_for_format(format_version, inocnt);
  mapsize = (mapsize + blksizemask) & ~blksizemask;

  // allocator v2 reserved metadata area (L1/L2 summaries, future use)
  size_t l0_bytes = ((size_t)blkcnt + 7u) >> 3;
  size_t l1_bytes = (l0_bytes + 7u) >> 3;
  size_t l2_bytes = (l1_bytes + 7u) >> 3;
  size_t allocator_size = l1_bytes + l2_bytes;
  if (allocator_size < 4096u)
    allocator_size = 4096u;
  allocator_size = (allocator_size + (size_t)blksizemask) & ~(size_t)blksizemask;
  off_t allocator_off = mapsize;
  mapsize += (off_t)allocator_size;
  mapsize = (mapsize + blksizemask) & ~blksizemask;

  uint32_t bucket_cnt = 1024;
  while ((bucket_cnt << 1) <= (uint32_t)(blkcnt / 4))
    bucket_cnt <<= 1;
  size_t hrl_index_size = (size_t)bucket_cnt * sizeof(uint32_t);
  off_t hrl_index_off = mapsize;
  mapsize += (off_t)hrl_index_size;
  mapsize = (mapsize + 7) & ~7; // 64-bit align

  uint64_t entry_cnt_u64 = (uint64_t)((double)blkcnt * hrl_entry_ratio);
  if (entry_cnt_u64 == 0 && blkcnt > 0)
    entry_cnt_u64 = 1;
  if (entry_cnt_u64 > (uint64_t)blkcnt)
    entry_cnt_u64 = (uint64_t)blkcnt;
  uint32_t entry_cnt = (uint32_t)entry_cnt_u64;
  off_t hrl_entry_off = mapsize;
  mapsize += (off_t)entry_cnt * (off_t)sizeof(kafs_hrl_entry_t);
  mapsize = (mapsize + blksizemask) & ~blksizemask;

  off_t journal_off = mapsize;
  mapsize += (off_t)journal_bytes;
  mapsize = (mapsize + blksizemask) & ~blksizemask;

  size_t pendinglog_size = 1u << 20; // 1MiB default pending log region
  pendinglog_size = (pendinglog_size + (size_t)blksizemask) & ~(size_t)blksizemask;
  off_t pendinglog_off = mapsize;
  mapsize += (off_t)pendinglog_size;
  mapsize = (mapsize + blksizemask) & ~blksizemask;

  size_t tailmeta_size = mkfs_tailmeta_region_size(format_version, blksize);
  off_t tailmeta_off = (tailmeta_size > 0u) ? mapsize : 0;
  mapsize += (off_t)tailmeta_size;
  mapsize = (mapsize + blksizemask) & ~blksizemask;

  if (out)
  {
    out->mapsize = mapsize;
    out->blkmask_off = blkmask_off;
    out->inotbl_off = inotbl_off;
    out->allocator_off = allocator_off;
    out->allocator_size = allocator_size;
    out->hrl_bucket_cnt = bucket_cnt;
    out->hrl_index_size = hrl_index_size;
    out->hrl_index_off = hrl_index_off;
    out->hrl_entry_cnt = entry_cnt;
    out->hrl_entry_off = hrl_entry_off;
    out->journal_off = journal_off;
    out->pendinglog_size = pendinglog_size;
    out->pendinglog_off = pendinglog_off;
    out->tailmeta_size = tailmeta_size;
    out->tailmeta_off = tailmeta_off;
  }
}

static int compute_blkcnt_for_total(uint32_t format_version, off_t total_bytes,
                                    kafs_logblksize_t log_blksize, kafs_blksize_t blksizemask,
                                    kafs_blksize_t blksize, kafs_inocnt_t inocnt,
                                    size_t journal_bytes, double hrl_entry_ratio,
                                    kafs_blkcnt_t *out_blkcnt, struct mkfs_layout *out_layout)
{
  if (total_bytes <= 0 || !out_blkcnt)
    return -1;

  kafs_blkcnt_t blkcnt = (kafs_blkcnt_t)(total_bytes >> log_blksize);
  if (blkcnt == 0)
    return -1;

  struct mkfs_layout layout = {0};
  for (int i = 0; i < 16; ++i)
  {
    compute_layout(format_version, blkcnt, blksizemask, blksize, inocnt, journal_bytes,
                   hrl_entry_ratio, &layout);
    if (total_bytes <= layout.mapsize)
      return -1;
    kafs_blkcnt_t next = (kafs_blkcnt_t)((total_bytes - layout.mapsize) >> log_blksize);
    if (next == blkcnt)
      break;
    blkcnt = next;
    if (blkcnt == 0)
      return -1;
  }

  for (;;)
  {
    compute_layout(format_version, blkcnt, blksizemask, blksize, inocnt, journal_bytes,
                   hrl_entry_ratio, &layout);
    off_t imgsize = layout.mapsize + ((off_t)blkcnt << log_blksize);
    if (imgsize <= total_bytes)
      break;
    if (blkcnt == 0)
      return -1;
    blkcnt--;
  }

  *out_blkcnt = blkcnt;
  if (out_layout)
    *out_layout = layout;
  return 0;
}

int main(int argc, char **argv)
{
  const char *img = NULL;
  uint32_t format_version = KAFS_FORMAT_VERSION_V5;
  kafs_logblksize_t log_blksize = 12; // 4096B
  kafs_blksize_t blksize = 1u << log_blksize;
  kafs_blksize_t blksizemask = blksize - 1u;
  off_t total_bytes = 1024ll * 1024ll * 1024ll; // 1GiB
  kafs_inocnt_t inocnt = 0;                     // number of inodes
  size_t journal_bytes = 1u << 20;              // 1MiB default journal region
  double hrl_entry_ratio = 0.75;
  int size_arg_provided = 0;
  int inocnt_arg_provided = 0;
  int trim_data_area = 0;
  int assume_yes = 0;

  if (kafs_cli_exit_if_help(argc, argv, usage, argv[0]) == 0)
    return 0;

  for (int i = 1; i < argc; ++i)
  {
    if ((strcmp(argv[i], "--size-bytes") == 0 || strcmp(argv[i], "-s") == 0) && i + 1 < argc)
    {
      uint64_t tmp = 0;
      if (kafs_parse_size_bytes_u64(argv[++i], &tmp) != 0)
      {
        fprintf(stderr, "invalid size: %s\n", argv[i]);
        return 2;
      }
      total_bytes = (off_t)tmp;
      size_arg_provided = 1;
    }
    else if ((strcmp(argv[i], "--blksize-log") == 0 || strcmp(argv[i], "-b") == 0) && i + 1 < argc)
    {
      log_blksize = (kafs_logblksize_t)strtoul(argv[++i], NULL, 0);
      blksize = 1u << log_blksize;
      blksizemask = blksize - 1u;
    }
    else if (strcmp(argv[i], "--format-version") == 0 && i + 1 < argc)
    {
      format_version = (uint32_t)strtoul(argv[++i], NULL, 0);
      if (!mkfs_format_version_is_supported(format_version))
      {
        fprintf(stderr, "unsupported format version: %u\n", format_version);
        return 2;
      }
    }
    else if ((strcmp(argv[i], "--inodes") == 0 || strcmp(argv[i], "-i") == 0) && i + 1 < argc)
    {
      inocnt = (kafs_inocnt_t)strtoul(argv[++i], NULL, 0);
      inocnt_arg_provided = 1;
    }
    else if ((strcmp(argv[i], "--journal-size-bytes") == 0 || strcmp(argv[i], "-J") == 0) &&
             i + 1 < argc)
    {
      uint64_t tmp = 0;
      if (kafs_parse_size_bytes_u64(argv[++i], &tmp) != 0)
      {
        fprintf(stderr, "invalid journal size: %s\n", argv[i]);
        return 2;
      }
      journal_bytes = (size_t)tmp;
      if (journal_bytes < 4096)
        journal_bytes = 4096; // minimum
    }
    else if (strcmp(argv[i], "--hrl-entry-ratio") == 0 && i + 1 < argc)
    {
      if (kafs_parse_ratio_0_to_1(argv[++i], &hrl_entry_ratio) != 0)
      {
        fprintf(stderr, "invalid hrl-entry-ratio (expected 0<R<=1): %s\n", argv[i]);
        return 2;
      }
    }
    else if (strcmp(argv[i], "--trim-data-area") == 0)
    {
      trim_data_area = 1;
    }
    else if (strcmp(argv[i], "--yes") == 0)
    {
      assume_yes = 1;
    }
    else if (argv[i][0] != '-' && img == NULL)
    {
      img = argv[i];
    }
    else
    {
      usage(argv[0]);
      return 2;
    }
  }
  if (!img)
  {
    usage(argv[0]);
    return 2;
  }

  struct stat st;
  int have_stat = (stat(img, &st) == 0);
  if (!have_stat && errno != ENOENT)
  {
    perror("stat");
    return 1;
  }
  kafs_context_t ctx = {0};
  ctx.c_fd = open(img, O_RDWR | (have_stat ? 0 : O_CREAT), 0666);
  if (ctx.c_fd < 0)
  {
    perror("open");
    return 1;
  }
  ctx.c_blo_search = 0;
  ctx.c_ino_search = 0;

  if (fstat(ctx.c_fd, &st) != 0)
  {
    perror("fstat");
    close(ctx.c_fd);
    return 1;
  }

  if (S_ISREG(st.st_mode) && st.st_size > 0)
  {
    if (size_arg_provided)
      fprintf(stderr, "warning: size overridden by existing file size\n");
    total_bytes = st.st_size;
  }
  else if (S_ISBLK(st.st_mode))
  {
#ifdef __linux__
    uint64_t dev_bytes = 0;
    if (ioctl(ctx.c_fd, BLKGETSIZE64, &dev_bytes) != 0)
    {
      perror("ioctl(BLKGETSIZE64)");
      close(ctx.c_fd);
      return 1;
    }
    if (dev_bytes == 0)
    {
      fprintf(stderr, "invalid block device size: 0\n");
      close(ctx.c_fd);
      return 1;
    }
    if (size_arg_provided)
      fprintf(stderr, "warning: size overridden by block device size\n");
    total_bytes = (off_t)dev_bytes;
#else
    fprintf(stderr, "block devices are not supported on this platform\n");
    close(ctx.c_fd);
    return 1;
#endif
  }

  if (!inocnt_arg_provided)
    inocnt = mkfs_default_inocnt_for_size(total_bytes);

  struct mkfs_layout layout = {0};
  kafs_blkcnt_t blkcnt = 0;
  if (compute_blkcnt_for_total(format_version, total_bytes, log_blksize, blksizemask, blksize,
                               inocnt, journal_bytes, hrl_entry_ratio, &blkcnt, &layout) != 0)
  {
    fprintf(stderr, "invalid total size: %lld\n", (long long)total_bytes);
    close(ctx.c_fd);
    return 2;
  }

  if (total_bytes >= (off_t)sizeof(kafs_ssuperblock_t))
  {
    kafs_ssuperblock_t sbcheck;
    if (pread(ctx.c_fd, &sbcheck, sizeof(sbcheck), 0) == (ssize_t)sizeof(sbcheck))
    {
      if (kafs_sb_magic_get(&sbcheck) == KAFS_MAGIC &&
          mkfs_format_version_is_supported(kafs_sb_format_version_get(&sbcheck)))
      {
        fprintf(stderr, "warning: image appears formatted and will be overwritten: %s\n", img);
        if (!assume_yes && !mkfs_confirm_overwrite_stdin())
        {
          fprintf(stderr, "mkfs.kafs: aborted\n");
          close(ctx.c_fd);
          return 1;
        }
      }
    }
  }

  if (S_ISREG(st.st_mode) && ftruncate(ctx.c_fd, total_bytes) < 0)
  {
    perror("ftruncate");
    close(ctx.c_fd);
    return 1;
  }

  off_t mapsize = layout.mapsize;
  ctx.c_superblock = NULL;
  ctx.c_blkmasktbl = (void *)layout.blkmask_off;
  ctx.c_inotbl = (void *)layout.inotbl_off;

  ctx.c_superblock = mmap(NULL, mapsize, PROT_READ | PROT_WRITE, MAP_SHARED, ctx.c_fd, 0);
  if (ctx.c_superblock == MAP_FAILED)
  {
    perror("mmap");
    return 1;
  }
  // Reformat safety: clear the full metadata mapping so stale pendinglog/journal/HRL
  // state from previous formats cannot survive when the superblock shape remains valid.
  memset(ctx.c_superblock, 0, (size_t)mapsize);
  // 先頭アドレスにオフセットを加算（byte単位）
  ctx.c_blkmasktbl = (kafs_blkmask_t *)((char *)ctx.c_superblock + (intptr_t)ctx.c_blkmasktbl);
  ctx.c_inotbl = (kafs_sinode_t *)((char *)ctx.c_superblock + (intptr_t)ctx.c_inotbl);

  // 境界チェックとゼロ初期化
  size_t blkmask_bytes = ((size_t)blkcnt + 7) >> 3; // ビットマップの総バイト数
  size_t inotbl_bytes = (size_t)kafs_inode_table_bytes_for_format(format_version, inocnt);
  char *base = (char *)ctx.c_superblock;
  char *end = base + mapsize;
  char *bm_ptr = (char *)ctx.c_blkmasktbl;
  char *ino_ptr = (char *)ctx.c_inotbl;
  assert(bm_ptr >= base && bm_ptr + blkmask_bytes <= end);
  assert(ino_ptr >= base && ino_ptr + inotbl_bytes <= end);
  memset(bm_ptr, 0, blkmask_bytes);
  memset(ino_ptr, 0, inotbl_bytes);
  if (layout.allocator_size > 0)
  {
    char *alloc_ptr = (char *)ctx.c_superblock + layout.allocator_off;
    assert(alloc_ptr >= base && alloc_ptr + layout.allocator_size <= end);
    memset(alloc_ptr, 0, layout.allocator_size);
  }

  // スーパーブロック基本
  kafs_sb_log_blksize_set(ctx.c_superblock, log_blksize);
  kafs_sb_magic_set(ctx.c_superblock, KAFS_MAGIC);
  kafs_sb_format_version_set(ctx.c_superblock, format_version);
  kafs_sb_hash_fast_set(ctx.c_superblock, KAFS_HASH_FAST_XXH64);
  kafs_sb_hash_strong_set(ctx.c_superblock, KAFS_HASH_STRONG_BLAKE3_256);
  // HRL領域の割当
  kafs_sb_hrl_index_offset_set(ctx.c_superblock, (uint64_t)layout.hrl_index_off);
  kafs_sb_hrl_index_size_set(ctx.c_superblock, (uint64_t)layout.hrl_index_size);
  kafs_sb_hrl_entry_offset_set(ctx.c_superblock, (uint64_t)layout.hrl_entry_off);
  kafs_sb_hrl_entry_cnt_set(ctx.c_superblock, (uint32_t)layout.hrl_entry_cnt);
  // Journal region metadata
  kafs_sb_journal_offset_set(ctx.c_superblock, (uint64_t)layout.journal_off);
  kafs_sb_journal_size_set(ctx.c_superblock, (uint64_t)journal_bytes);
  kafs_sb_journal_flags_set(ctx.c_superblock, 0);
  kafs_sb_allocator_version_set(ctx.c_superblock, 2);
  kafs_sb_allocator_offset_set(ctx.c_superblock, (uint64_t)layout.allocator_off);
  kafs_sb_allocator_size_set(ctx.c_superblock, (uint64_t)layout.allocator_size);
  kafs_sb_pendinglog_offset_set(ctx.c_superblock, (uint64_t)layout.pendinglog_off);
  kafs_sb_pendinglog_size_set(ctx.c_superblock, (uint64_t)layout.pendinglog_size);
  kafs_sb_checkpoint_seq_set(ctx.c_superblock, 0);
  kafs_sb_commit_seq_set(ctx.c_superblock, 0);
  kafs_sb_tailmeta_offset_set(ctx.c_superblock, (uint64_t)layout.tailmeta_off);
  kafs_sb_tailmeta_size_set(ctx.c_superblock, (uint64_t)layout.tailmeta_size);
  kafs_sb_feature_flags_set(ctx.c_superblock, mkfs_feature_flags_for_format(format_version));
  kafs_sb_compat_flags_set(ctx.c_superblock, 0);

  // R/O items
  ctx.c_superblock->s_inocnt = kafs_inocnt_htos(inocnt);
  // root inode uses one entry
  kafs_sb_inocnt_free_set(ctx.c_superblock,
                          (inocnt > (kafs_inocnt_t)KAFS_INO_ROOTDIR) ? (inocnt - 1) : 0);
  // 一般/ルートどちらも同一のブロック数で初期化（シンプル化）
  ctx.c_superblock->s_blkcnt = kafs_blkcnt_htos(blkcnt);
  ctx.c_superblock->s_r_blkcnt = kafs_blkcnt_htos(blkcnt);
  kafs_blkcnt_t fdb = (kafs_blkcnt_t)(mapsize >> log_blksize);
  ctx.c_superblock->s_first_data_block = kafs_blkcnt_htos(fdb);
  kafs_sb_blkcnt_free_set(ctx.c_superblock, blkcnt - fdb);

  for (kafs_blkcnt_t blo = 0; blo < fdb; ++blo)
    kafs_blk_set_usage(&ctx, blo, KAFS_TRUE);

  // root inode
  // inotbl はゼロ初期化済み
  kafs_sinode_t *inoent_rootdir = (kafs_sinode_t *)kafs_inode_ptr_in_table(
      ctx.c_inotbl, kafs_sb_format_version_get(ctx.c_superblock), KAFS_INO_ROOTDIR);
  // 安全性チェック
  {
    char *p = (char *)inoent_rootdir;
    char *base2 = (char *)ctx.c_superblock;
    char *end2 = base2 + mapsize;
    if (!(p >= base2 && p + sizeof(*inoent_rootdir) <= end2))
    {
      fprintf(stderr, "inotbl/root inode out of range: ptr=%p base=%p end=%p mapsize=%lld\n",
              (void *)p, (void *)base2, (void *)end2, (long long)mapsize);
      abort();
    }
  }
  kafs_ino_mode_set(inoent_rootdir, S_IFDIR | 0755);
  // For unprivileged FUSE mounts, make the filesystem root owned by the image creator.
  kafs_ino_uid_set(inoent_rootdir, (kafs_uid_t)getuid());
  kafs_ino_size_set(inoent_rootdir, 0);
  kafs_time_t now = kafs_now();
  kafs_time_t nulltime = (kafs_time_t){0, 0};
  kafs_ino_atime_set(inoent_rootdir, now);
  kafs_ino_ctime_set(inoent_rootdir, now);
  kafs_ino_mtime_set(inoent_rootdir, now);
  kafs_ino_dtime_set(inoent_rootdir, nulltime);
  kafs_ino_gid_set(inoent_rootdir, (kafs_gid_t)getgid());
  // ログ呼び出しを避けて直接設定（デバッグ）
  inoent_rootdir->i_linkcnt = kafs_linkcnt_htos(1);
  kafs_ino_blocks_set(inoent_rootdir, 0);
  kafs_ino_dev_set(inoent_rootdir, 0);
  kafs_sdir_v4_hdr_t root_dir_hdr;
  kafs_dir_v4_hdr_init(&root_dir_hdr);
  memcpy(inoent_rootdir->i_blkreftbl, &root_dir_hdr, sizeof(root_dir_hdr));
  kafs_ino_size_set(inoent_rootdir, (kafs_off_t)sizeof(root_dir_hdr));
  // ルートディレクトリの実レコードは空開始（"." は readdir で注入、".." は保持しない）

  // HRL 初期化（ゼロクリア）
  ctx.c_hrl_index = (void *)((char *)ctx.c_superblock + layout.hrl_index_off);
  ctx.c_hrl_bucket_cnt = layout.hrl_bucket_cnt;
  (void)kafs_hrl_format(&ctx);

  // Journal header 初期化（fresh image が fsck --fast-check を通る前提）
  {
    size_t hsz = kj_header_size();
    if (journal_bytes > hsz)
    {
      kj_header_t jh;
      memset(&jh, 0, sizeof(jh));
      jh.magic = KJ_MAGIC;
      jh.version = KJ_VER;
      jh.flags = 0;
      jh.area_size = (uint64_t)journal_bytes - (uint64_t)hsz;
      jh.write_off = 0;
      jh.seq = 0;
      jh.reserved0 = 0;
      jh.header_crc = 0;
      jh.header_crc = kj_crc32(&jh, sizeof(jh));

      char *jhdr_ptr = (char *)ctx.c_superblock + layout.journal_off;
      char *base3 = (char *)ctx.c_superblock;
      char *end3 = base3 + mapsize;
      assert(jhdr_ptr >= base3 && jhdr_ptr + sizeof(jh) <= end3);
      memcpy(jhdr_ptr, &jh, sizeof(jh));
    }
  }

  if (layout.tailmeta_size >= sizeof(kafs_tailmeta_region_hdr_t))
  {
    kafs_tailmeta_region_hdr_t region_hdr;
    char *tailmeta_ptr = (char *)ctx.c_superblock + layout.tailmeta_off;
    char *base4 = (char *)ctx.c_superblock;
    char *end4 = base4 + mapsize;

    assert(tailmeta_ptr >= base4 && tailmeta_ptr + layout.tailmeta_size <= end4);
    kafs_tailmeta_region_hdr_init(&region_hdr);
    kafs_tailmeta_region_hdr_container_table_off_set(&region_hdr, (uint32_t)sizeof(region_hdr));
    kafs_tailmeta_region_hdr_container_table_bytes_set(
        &region_hdr,
        (uint32_t)(KAFS_TAILMETA_DEFAULT_CLASS_COUNT * sizeof(kafs_tailmeta_container_hdr_t)));
    kafs_tailmeta_region_hdr_container_count_set(&region_hdr, KAFS_TAILMETA_DEFAULT_CLASS_COUNT);
    kafs_tailmeta_region_hdr_class_count_set(&region_hdr, KAFS_TAILMETA_DEFAULT_CLASS_COUNT);
    memcpy(tailmeta_ptr, &region_hdr, sizeof(region_hdr));

    kafs_tailmeta_container_hdr_t *containers =
        (kafs_tailmeta_container_hdr_t *)(tailmeta_ptr + sizeof(region_hdr));
    memset(containers, 0,
           KAFS_TAILMETA_DEFAULT_CLASS_COUNT * sizeof(kafs_tailmeta_container_hdr_t));
    for (uint16_t index = 0; index < KAFS_TAILMETA_DEFAULT_CLASS_COUNT; ++index)
    {
      uint16_t class_bytes = kafs_tailmeta_default_class_bytes(index);
      uint16_t slot_count = kafs_tailmeta_default_slot_count_for_class(blksize, class_bytes);
      uint32_t slot_table_off =
          (uint32_t)((size_t)blksize * (size_t)(KAFS_TAILMETA_DEFAULT_REGION_META_BLOCKS + index));

      kafs_tailmeta_container_hdr_init(&containers[index]);
      kafs_tailmeta_container_hdr_class_bytes_set(&containers[index], class_bytes);
      kafs_tailmeta_container_hdr_slot_count_set(&containers[index], slot_count);
      kafs_tailmeta_container_hdr_free_bytes_set(&containers[index],
                                                 (uint16_t)(slot_count * class_bytes));
      kafs_tailmeta_container_hdr_slot_table_off_set(&containers[index], slot_table_off);
      kafs_tailmeta_container_hdr_slot_table_bytes_set(
          &containers[index], (uint32_t)(slot_count * sizeof(kafs_tailmeta_slot_desc_t)));
    }
  }

  if (trim_data_area)
  {
    off_t data_off = (off_t)fdb << log_blksize;
    off_t data_len = ((off_t)blkcnt - (off_t)fdb) << log_blksize;
    int trc = mkfs_trim_range(ctx.c_fd, data_off, data_len);
    if (trc != 0)
    {
      fprintf(stderr, "warning: --trim-data-area failed rc=%d\n", trc);
    }
  }

  munmap(ctx.c_superblock, mapsize);
  close(ctx.c_fd);

  fprintf(stderr, "Formatted %s: format=v%u size=%lld bytes, blksize=%u, blocks=%u, inodes=%u\n",
          img, (unsigned)format_version, (long long)total_bytes, (unsigned)blksize,
          (unsigned)blkcnt, (unsigned)inocnt);
  return 0;
}
