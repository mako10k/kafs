#include "kafs.h"
#include "kafs_cli_opts.h"
#include "kafs_offline_summary.h"
#include "kafs_superblock.h"
#include "kafs_inode.h"
#include "kafs_hash.h"

#include <fcntl.h>
#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

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

  uint64_t file_size = 0;
  int rc = kafs_offline_detect_file_size(fd, &file_size);
  if (rc != 0)
  {
    fprintf(stderr, "failed to detect image size: %s\n", strerror(-rc));
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

  uint64_t tombstone_count = 0;
  kafs_time_t oldest_tombstone = {0};
  int have_oldest_tombstone = 0;
  void *inotbl = NULL;
  uint64_t inocnt = 0;
  int rc_inode = kafs_offline_load_inode_table(fd, &sb, file_size, &inotbl, &inocnt);
  if (rc_inode == 0)
  {
    for (kafs_inocnt_t ino = KAFS_INO_ROOTDIR; ino < inocnt; ++ino)
    {
      const kafs_sinode_t *inoent = (const kafs_sinode_t *)kafs_inode_ptr_const_in_table(
          inotbl, kafs_sb_format_version_get(&sb), ino);
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

  struct inode_summary ino = {0};
  struct tailmeta_summary tm = {0};
  rc_inode = collect_inode_summary(fd, &sb, file_size, &ino);
  int rc_tailmeta = collect_tailmeta_summary(fd, &sb, file_size, &tm);
  if (rc_inode == 0)
  {
    printf("tail layouts: regular=%" PRIu64 " tail_only=%" PRIu64 " mixed_full_tail=%" PRIu64 "\n",
           ino.regular_files, ino.tail_only_regular, ino.mixed_full_tail_regular);
  }
  else
  {
    printf("tail layouts: unavailable (%s)\n", (rc_inode < 0) ? strerror(-rc_inode) : "error");
  }
  if (rc_tailmeta == 0 && tm.available)
  {
    printf("tail usage: live_slots=%" PRIu64 " live_bytes=%" PRIu64 " free_bytes=%" PRIu64
           " valid_containers=%" PRIu64 " invalid_containers=%" PRIu64 "\n",
           tm.live_slots, tm.live_bytes, tm.free_bytes, tm.valid_containers, tm.invalid_containers);
    for (uint16_t index = 0; index < tm.class_summary_count; ++index)
    {
      const struct tailmeta_class_summary *class_summary = &tm.classes[index];

      printf("tail class %uB: live_slots=%" PRIu64 " live_bytes=%" PRIu64 " free_bytes=%" PRIu64
             " valid=%" PRIu64 " invalid=%" PRIu64 "\n",
             (unsigned)class_summary->class_bytes, class_summary->live_slots,
             class_summary->live_bytes, class_summary->free_bytes, class_summary->valid_containers,
             class_summary->invalid_containers);
    }
  }
  else if (kafs_tailmeta_region_present(&sb))
  {
    printf("tail usage: unavailable (%s)\n", (rc_tailmeta < 0) ? strerror(-rc_tailmeta) : "error");
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

  free(inotbl);
  close(fd);
  return 0;
}
