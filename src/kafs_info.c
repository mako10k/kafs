#include "kafs.h"
#include "kafs_cli_opts.h"
#include "kafs_superblock.h"
#include "kafs_inode.h"
#include "kafs_hash.h"

#include <fcntl.h>
#include <inttypes.h>
#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>

static uint64_t align_up_u64(uint64_t value, uint64_t align)
{
  if (align == 0)
    return value;
  return (value + align - 1u) & ~(align - 1u);
}

static int time_is_zero(kafs_time_t ts) { return ts.tv_sec == 0 && ts.tv_nsec == 0; }

static int time_before(kafs_time_t lhs, kafs_time_t rhs)
{
  if (lhs.tv_sec != rhs.tv_sec)
    return lhs.tv_sec < rhs.tv_sec;
  return lhs.tv_nsec < rhs.tv_nsec;
}

static void fmt_time(char out[64], const kafs_time_t *ts)
{
  if (!out)
    return;
  if (!ts || time_is_zero(*ts))
  {
    snprintf(out, 64, "none");
    return;
  }

  time_t sec = ts->tv_sec;
  struct tm tm;
  if (localtime_r(&sec, &tm) == NULL)
  {
    snprintf(out, 64, "%lld", (long long)ts->tv_sec);
    return;
  }
  strftime(out, 64, "%Y-%m-%d %H:%M:%S", &tm);
}

static void usage(const char *prog) { fprintf(stderr, "Usage: %s <image>\n", prog); }

int main(int argc, char **argv)
{
  if (kafs_cli_exit_if_help(argc, argv, usage, argv[0]) == 0)
    return 0;

  if (argc < 2)
  {
    usage(argv[0]);
    return 2;
  }
  const char *img = argv[1];
  int fd = open(img, O_RDONLY);
  if (fd < 0)
  {
    perror("open");
    return 1;
  }
  kafs_ssuperblock_t sb;
  ssize_t r = pread(fd, &sb, sizeof(sb), 0);
  if (r != (ssize_t)sizeof(sb))
  {
    perror("pread superblock");
    close(fd);
    return 1;
  }

  printf("magic=0x%08" PRIx32 " version=%" PRIu32 " log_blksize=%" PRIuFAST16 " (bytes=%u)\n",
         kafs_sb_magic_get(&sb), kafs_sb_format_version_get(&sb), kafs_sb_log_blksize_get(&sb),
         1u << kafs_sb_log_blksize_get(&sb));
  printf("inodes total=%" PRIuFAST32 " free=%" PRIuFAST32 "\n", kafs_sb_inocnt_get(&sb),
         (uint_fast32_t)kafs_sb_inocnt_free_get(&sb));
  printf("blocks user=%" PRIuFAST32 " root=%" PRIuFAST32 " free=%" PRIuFAST32
         " first_data=%" PRIuFAST32 "\n",
         kafs_sb_blkcnt_get(&sb), kafs_sb_r_blkcnt_get(&sb), kafs_sb_blkcnt_free_get(&sb),
         kafs_blkcnt_stoh(sb.s_first_data_block));
  printf("hash fast=%" PRIu32 " strong=%" PRIu32 "\n", kafs_sb_hash_fast_get(&sb),
         kafs_sb_hash_strong_get(&sb));
  printf("hrl index: off=%" PRIu64 " size=%" PRIu64 "; entries: off=%" PRIu64 " cnt=%" PRIu32 "\n",
         (uint64_t)kafs_sb_hrl_index_offset_get(&sb), (uint64_t)kafs_sb_hrl_index_size_get(&sb),
         (uint64_t)kafs_sb_hrl_entry_offset_get(&sb), (uint32_t)kafs_sb_hrl_entry_cnt_get(&sb));
  printf("tail metadata: enabled=%s off=%" PRIu64 " size=%" PRIu64 "\n",
         (kafs_sb_feature_flags_get(&sb) & KAFS_FEATURE_TAIL_META_REGION) ? "true" : "false",
         (uint64_t)kafs_sb_tailmeta_offset_get(&sb), (uint64_t)kafs_sb_tailmeta_size_get(&sb));

  kafs_logblksize_t log_blksize = kafs_sb_log_blksize_get(&sb);
  uint64_t blksize = 1u << log_blksize;
  uint64_t r_blkcnt = kafs_sb_r_blkcnt_get(&sb);
  uint64_t inocnt = kafs_sb_inocnt_get(&sb);
  uint64_t layout = sizeof(kafs_ssuperblock_t);
  layout = align_up_u64(layout, blksize);
  layout += (r_blkcnt + 7u) >> 3;
  layout = align_up_u64(layout, 8u);
  layout = align_up_u64(layout, blksize);
  uint64_t inotbl_off = layout;
  uint64_t inotbl_bytes =
      kafs_inode_table_bytes_for_format(kafs_sb_format_version_get(&sb), inocnt);

  uint64_t tombstone_count = 0;
  kafs_time_t oldest_tombstone = {0};
  int have_oldest_tombstone = 0;
  if (inocnt > 0)
  {
    kafs_sinode_t *inotbl = malloc((size_t)inotbl_bytes);
    if (!inotbl)
    {
      perror("malloc inode table");
      close(fd);
      return 1;
    }

    ssize_t ir = pread(fd, inotbl, (size_t)inotbl_bytes, (off_t)inotbl_off);
    if (ir != (ssize_t)inotbl_bytes)
    {
      perror("pread inode table");
      free(inotbl);
      close(fd);
      return 1;
    }

    for (kafs_inocnt_t ino = KAFS_INO_ROOTDIR; ino < inocnt; ++ino)
    {
      const kafs_sinode_t *inoent = &inotbl[ino];
      if (!kafs_ino_get_usage(inoent))
        continue;
      if (kafs_ino_linkcnt_get(inoent) != 0)
        continue;

      kafs_time_t dtime = kafs_ino_dtime_get(inoent);
      if (time_is_zero(dtime))
        continue;

      tombstone_count++;
      if (!have_oldest_tombstone || time_before(dtime, oldest_tombstone))
      {
        oldest_tombstone = dtime;
        have_oldest_tombstone = 1;
      }
    }
    free(inotbl);
  }

  char tombstone_oldest_buf[64];
  fmt_time(tombstone_oldest_buf, have_oldest_tombstone ? &oldest_tombstone : NULL);
  if (tombstone_count > 0)
  {
    printf("tombstones count=%" PRIu64 " oldest_dtime=%s (%lld.%09ld)\n", tombstone_count,
           tombstone_oldest_buf, (long long)oldest_tombstone.tv_sec, oldest_tombstone.tv_nsec);
  }
  else
  {
    printf("tombstones count=0 oldest_dtime=none\n");
  }

  uint64_t index_size = kafs_sb_hrl_index_size_get(&sb);
  uint64_t entry_off = kafs_sb_hrl_entry_offset_get(&sb);
  uint32_t entry_cnt = kafs_sb_hrl_entry_cnt_get(&sb);
  if (index_size && entry_off && entry_cnt)
  {
    uint32_t buckets = (uint32_t)(index_size / sizeof(uint32_t));
    printf("hrl buckets=%u\n", buckets);
    size_t ents_bytes = (size_t)entry_cnt * sizeof(kafs_hrl_entry_t);
    kafs_hrl_entry_t *ents = malloc(ents_bytes);
    if (!ents)
    {
      perror("malloc");
      close(fd);
      return 1;
    }
    ssize_t er = pread(fd, ents, ents_bytes, (off_t)entry_off);
    if (er != (ssize_t)ents_bytes)
    {
      perror("pread hrl entries");
      free(ents);
      close(fd);
      return 1;
    }
    uint32_t used = 0;
    for (uint32_t i = 0; i < entry_cnt; ++i)
      if (ents[i].refcnt)
        ++used;
    printf("hrl entries used=%u / %u\n", used, entry_cnt);
    free(ents);
  }

  close(fd);
  return 0;
}
