#include "kafs_ioctl.h"

#include <errno.h>
#include <inttypes.h>
#include <stdio.h>
#include <string.h>
#include <fcntl.h>
#include <unistd.h>

static void usage(const char *prog)
{
  fprintf(stderr, "Usage: %s stats <mountpoint> [--json]\n", prog);
}

static int cmd_stats(const char *mnt, int json)
{
  int fd = open(mnt, O_RDONLY | O_DIRECTORY);
  if (fd < 0)
  {
    perror("open");
    return 1;
  }

  kafs_stats_t st;
  memset(&st, 0, sizeof(st));
  if (ioctl(fd, KAFS_IOCTL_GET_STATS, &st) != 0)
  {
    perror("ioctl(KAFS_IOCTL_GET_STATS)");
    close(fd);
    return 1;
  }
  close(fd);

  uint64_t logical_bytes = st.hrl_refcnt_sum * (uint64_t)st.blksize;
  uint64_t unique_bytes = st.hrl_entries_used * (uint64_t)st.blksize;
  uint64_t saved_bytes = (st.hrl_refcnt_sum > st.hrl_entries_used)
                           ? (st.hrl_refcnt_sum - st.hrl_entries_used) * (uint64_t)st.blksize
                           : 0;

  double dedup_ratio = 1.0;
  if (unique_bytes > 0)
    dedup_ratio = (double)logical_bytes / (double)unique_bytes;

  double hit_rate = 0.0;
  if (st.hrl_put_calls > 0)
    hit_rate = (double)st.hrl_put_hits / (double)st.hrl_put_calls;

  if (json)
  {
    printf("{\n");
    printf("  \"version\": %" PRIu32 ",\n", st.version);
    printf("  \"blksize\": %" PRIu32 ",\n", st.blksize);
    printf("  \"fs_blocks_total\": %" PRIu64 ",\n", st.fs_blocks_total);
    printf("  \"fs_blocks_free\": %" PRIu64 ",\n", st.fs_blocks_free);
    printf("  \"fs_inodes_total\": %" PRIu64 ",\n", st.fs_inodes_total);
    printf("  \"fs_inodes_free\": %" PRIu64 ",\n", st.fs_inodes_free);
    printf("  \"hrl_entries_total\": %" PRIu64 ",\n", st.hrl_entries_total);
    printf("  \"hrl_entries_used\": %" PRIu64 ",\n", st.hrl_entries_used);
    printf("  \"hrl_entries_duplicated\": %" PRIu64 ",\n", st.hrl_entries_duplicated);
    printf("  \"hrl_refcnt_sum\": %" PRIu64 ",\n", st.hrl_refcnt_sum);
    printf("  \"logical_bytes\": %" PRIu64 ",\n", logical_bytes);
    printf("  \"unique_bytes\": %" PRIu64 ",\n", unique_bytes);
    printf("  \"saved_bytes\": %" PRIu64 ",\n", saved_bytes);
    printf("  \"dedup_ratio\": %.6f,\n", dedup_ratio);
    printf("  \"hrl_put_calls\": %" PRIu64 ",\n", st.hrl_put_calls);
    printf("  \"hrl_put_hits\": %" PRIu64 ",\n", st.hrl_put_hits);
    printf("  \"hrl_put_misses\": %" PRIu64 ",\n", st.hrl_put_misses);
    printf("  \"hrl_put_fallback_legacy\": %" PRIu64 ",\n", st.hrl_put_fallback_legacy);
    printf("  \"hrl_put_hit_rate\": %.6f\n", hit_rate);
    printf("}\n");
    return 0;
  }

  printf("kafs stats v%" PRIu32 "\n", st.version);
  printf("  blksize: %" PRIu32 "\n", st.blksize);
  printf("  fs: blocks total=%" PRIu64 " free=%" PRIu64 ", inodes total=%" PRIu64 " free=%" PRIu64
         "\n",
         st.fs_blocks_total, st.fs_blocks_free, st.fs_inodes_total, st.fs_inodes_free);
  printf("  hrl: entries used=%" PRIu64 "/%" PRIu64 " duplicated=%" PRIu64 " refsum=%" PRIu64 "\n",
         st.hrl_entries_used, st.hrl_entries_total, st.hrl_entries_duplicated, st.hrl_refcnt_sum);
  printf("  dedup: logical=%" PRIu64 "B unique=%" PRIu64 "B saved=%" PRIu64 "B ratio=%.3f\n",
         logical_bytes, unique_bytes, saved_bytes, dedup_ratio);
  printf("  hrl_put: calls=%" PRIu64 " hits=%" PRIu64 " misses=%" PRIu64 " fallback_legacy=%" PRIu64
         " hit_rate=%.3f\n",
         st.hrl_put_calls, st.hrl_put_hits, st.hrl_put_misses, st.hrl_put_fallback_legacy, hit_rate);
  return 0;
}

int main(int argc, char **argv)
{
  if (argc < 3)
  {
    usage(argv[0]);
    return 2;
  }
  int json = 0;
  for (int i = 3; i < argc; ++i)
  {
    if (strcmp(argv[i], "--json") == 0)
      json = 1;
    else
    {
      usage(argv[0]);
      return 2;
    }
  }

  if (strcmp(argv[1], "stats") == 0)
    return cmd_stats(argv[2], json);

  usage(argv[0]);
  return 2;
}
