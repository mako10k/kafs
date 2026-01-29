#include "kafs_ioctl.h"

#include <errno.h>
#include <inttypes.h>
#include <stdio.h>
#include <string.h>
#include <fcntl.h>
#include <unistd.h>

typedef enum
{
  KAFS_UNIT_KIB = 0,
  KAFS_UNIT_BYTES,
  KAFS_UNIT_MIB,
  KAFS_UNIT_GIB,
} kafs_unit_t;

static double unit_divisor(kafs_unit_t u)
{
  switch (u)
  {
  case KAFS_UNIT_BYTES:
    return 1.0;
  case KAFS_UNIT_MIB:
    return 1024.0 * 1024.0;
  case KAFS_UNIT_GIB:
    return 1024.0 * 1024.0 * 1024.0;
  case KAFS_UNIT_KIB:
  default:
    return 1024.0;
  }
}

static const char *unit_suffix(kafs_unit_t u)
{
  switch (u)
  {
  case KAFS_UNIT_BYTES:
    return "B";
  case KAFS_UNIT_MIB:
    return "MiB";
  case KAFS_UNIT_GIB:
    return "GiB";
  case KAFS_UNIT_KIB:
  default:
    return "KiB";
  }
}

static void print_bytes(uint64_t bytes, kafs_unit_t unit)
{
  if (unit == KAFS_UNIT_BYTES)
  {
    printf("%" PRIu64 "B", bytes);
    return;
  }

  const double v = (double)bytes / unit_divisor(unit);
  if (v >= 100.0)
    printf("%.0f%s", v, unit_suffix(unit));
  else
    printf("%.1f%s", v, unit_suffix(unit));
}

static void usage(const char *prog)
{
  fprintf(stderr, "Usage: %s stats <mountpoint> [--json] [--bytes|--mib|--gib]\n", prog);
}

static int cmd_stats(const char *mnt, int json, kafs_unit_t unit)
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
  printf("  blksize: ");
  print_bytes(st.blksize, unit);
  printf("\n");

  printf("  fs: blocks total=%" PRIu64 " (", st.fs_blocks_total);
  print_bytes(st.fs_blocks_total * (uint64_t)st.blksize, unit);
  printf(") free=%" PRIu64 " (", st.fs_blocks_free);
  print_bytes(st.fs_blocks_free * (uint64_t)st.blksize, unit);
  printf(")\n");
  printf("      inodes total=%" PRIu64 " free=%" PRIu64 "\n", st.fs_inodes_total, st.fs_inodes_free);

  printf("  hrl: entries used=%" PRIu64 "/%" PRIu64 " duplicated=%" PRIu64 " refsum=%" PRIu64 "\n",
         st.hrl_entries_used, st.hrl_entries_total, st.hrl_entries_duplicated, st.hrl_refcnt_sum);

  printf("  dedup: logical=");
  print_bytes(logical_bytes, unit);
  printf(" unique=");
  print_bytes(unique_bytes, unit);
  printf(" saved=");
  print_bytes(saved_bytes, unit);
  printf(" ratio=%.3f\n", dedup_ratio);
  printf("  hrl_put: calls=%" PRIu64 " hits=%" PRIu64 " misses=%" PRIu64 " fallback_legacy=%" PRIu64
         " hit_rate=%.3f\n",
         st.hrl_put_calls, st.hrl_put_hits, st.hrl_put_misses, st.hrl_put_fallback_legacy,
         hit_rate);
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
  kafs_unit_t unit = KAFS_UNIT_KIB;
  for (int i = 3; i < argc; ++i)
  {
    if (strcmp(argv[i], "--json") == 0)
      json = 1;
    else if (strcmp(argv[i], "--bytes") == 0)
      unit = KAFS_UNIT_BYTES;
    else if (strcmp(argv[i], "--mib") == 0)
      unit = KAFS_UNIT_MIB;
    else if (strcmp(argv[i], "--gib") == 0)
      unit = KAFS_UNIT_GIB;
    else
    {
      usage(argv[0]);
      return 2;
    }
  }

  if (strcmp(argv[1], "stats") == 0)
    return cmd_stats(argv[2], json, unit);

  usage(argv[0]);
  return 2;
}
