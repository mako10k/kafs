#include "kafs.h"
#include "kafs_hash.h"
#include "kafs_inode.h"
#include "kafs_journal.h"
#include "kafs_superblock.h"

#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

struct inode_summary
{
  uint64_t total;
  uint64_t used;
  uint64_t free;
  uint64_t linkcnt_zero_used;
};

struct hrl_summary
{
  uint64_t entry_count;
  uint64_t live_entries;
  uint64_t total_refcnt;
};

struct journal_summary
{
  int available;
  int crc_ok;
  kj_header_t header;
};

static void usage(const char *prog)
{
  fprintf(stderr, "Usage: %s [--json] <image>\n", prog);
}

static int pread_all(int fd, void *buf, size_t size, off_t off)
{
  char *p = (char *)buf;
  size_t done = 0;
  while (done < size)
  {
    ssize_t r = pread(fd, p + done, size - done, off + (off_t)done);
    if (r == 0)
      return -EIO;
    if (r < 0)
    {
      if (errno == EINTR)
        continue;
      return -errno;
    }
    done += (size_t)r;
  }
  return 0;
}

static uint64_t align_up_u64(uint64_t v, uint64_t align)
{
  if (align == 0)
    return v;
  return (v + align - 1u) & ~(align - 1u);
}

static int check_bounds(uint64_t off, uint64_t len, uint64_t file_size)
{
  if (off > file_size)
    return -ERANGE;
  if (len > file_size - off)
    return -ERANGE;
  return 0;
}

static const char *rc_to_text(int rc)
{
  if (rc == 0)
    return "ok";
  if (rc < 0)
    return strerror(-rc);
  return "error";
}

static int collect_inode_summary(int fd, const kafs_ssuperblock_t *sb, uint64_t file_size,
                                 struct inode_summary *out)
{
  memset(out, 0, sizeof(*out));

  const uint64_t inocnt = (uint64_t)kafs_sb_inocnt_get(sb);
  const uint64_t r_blkcnt = (uint64_t)kafs_sb_r_blkcnt_get(sb);
  const uint64_t blksize = (uint64_t)kafs_sb_blksize_get(sb);

  uint64_t mapsize = sizeof(kafs_ssuperblock_t);
  mapsize = align_up_u64(mapsize, blksize);
  mapsize += (r_blkcnt + 7u) >> 3;
  mapsize = align_up_u64(mapsize, 8);
  mapsize = align_up_u64(mapsize, blksize);

  const uint64_t inotbl_off = mapsize;
  const uint64_t inotbl_bytes = (uint64_t)sizeof(kafs_sinode_t) * inocnt;

  if (check_bounds(inotbl_off, inotbl_bytes, file_size) != 0)
    return -ERANGE;

  kafs_sinode_t *inotbl = (kafs_sinode_t *)malloc((size_t)inotbl_bytes);
  if (!inotbl)
    return -ENOMEM;

  int rc = pread_all(fd, inotbl, (size_t)inotbl_bytes, (off_t)inotbl_off);
  if (rc != 0)
  {
    free(inotbl);
    return rc;
  }

  out->total = (inocnt > KAFS_INO_ROOTDIR) ? (inocnt - KAFS_INO_ROOTDIR) : 0;
  for (uint64_t ino = KAFS_INO_ROOTDIR; ino < inocnt; ++ino)
  {
    const kafs_sinode_t *e = &inotbl[ino];
    if (!kafs_ino_get_usage(e))
      continue;

    out->used++;
    if (kafs_ino_linkcnt_get(e) == 0)
      out->linkcnt_zero_used++;
  }

  out->free = (out->total >= out->used) ? (out->total - out->used) : 0;

  free(inotbl);
  return 0;
}

static int collect_hrl_summary(int fd, const kafs_ssuperblock_t *sb, uint64_t file_size,
                               struct hrl_summary *out)
{
  memset(out, 0, sizeof(*out));

  const uint64_t ent_off = kafs_sb_hrl_entry_offset_get(sb);
  const uint64_t ent_cnt = kafs_sb_hrl_entry_cnt_get(sb);
  const uint64_t ent_bytes = ent_cnt * (uint64_t)sizeof(kafs_hrl_entry_t);

  out->entry_count = ent_cnt;

  if (ent_off == 0 || ent_cnt == 0)
    return 0;

  if (check_bounds(ent_off, ent_bytes, file_size) != 0)
    return -ERANGE;

  kafs_hrl_entry_t *ents = (kafs_hrl_entry_t *)malloc((size_t)ent_bytes);
  if (!ents)
    return -ENOMEM;

  int rc = pread_all(fd, ents, (size_t)ent_bytes, (off_t)ent_off);
  if (rc != 0)
  {
    free(ents);
    return rc;
  }

  for (uint64_t i = 0; i < ent_cnt; ++i)
  {
    uint32_t refcnt = ents[i].refcnt;
    if (refcnt != 0)
    {
      out->live_entries++;
      out->total_refcnt += refcnt;
    }
  }

  free(ents);
  return 0;
}

static int collect_journal_summary(int fd, const kafs_ssuperblock_t *sb, uint64_t file_size,
                                   struct journal_summary *out)
{
  memset(out, 0, sizeof(*out));

  const uint64_t j_off = kafs_sb_journal_offset_get(sb);
  const uint64_t j_size = kafs_sb_journal_size_get(sb);

  if (j_off == 0 || j_size < sizeof(kj_header_t))
    return 0;

  if (check_bounds(j_off, sizeof(kj_header_t), file_size) != 0)
    return -ERANGE;

  int rc = pread_all(fd, &out->header, sizeof(out->header), (off_t)j_off);
  if (rc != 0)
    return rc;

  out->available = 1;

  kj_header_t tmp = out->header;
  uint32_t saved_crc = tmp.header_crc;
  tmp.header_crc = 0;
  out->crc_ok = (kj_crc32(&tmp, sizeof(tmp)) == saved_crc) ? 1 : 0;
  return 0;
}

static void print_text(const kafs_ssuperblock_t *sb, const struct inode_summary *ino,
                       const struct hrl_summary *hrl, const struct journal_summary *jr,
                       int rc_inode, int rc_hrl, int rc_journal)
{
  uint64_t blksize = kafs_sb_blksize_get(sb);
  uint64_t r_blkcnt = kafs_sb_r_blkcnt_get(sb);
  uint64_t first_data = kafs_sb_first_data_block_get(sb);
  uint64_t data_blocks = (r_blkcnt > first_data) ? (r_blkcnt - first_data) : 0;

  printf("superblock:\n");
  printf("  magic: 0x%08" PRIx32 "\n", kafs_sb_magic_get(sb));
  printf("  format_version: %" PRIu32 "\n", kafs_sb_format_version_get(sb));
  printf("  block_size: %" PRIu64 "\n", blksize);
  printf("  inode_count: %" PRIu64 "\n", (uint64_t)kafs_sb_inocnt_get(sb));
  printf("  inode_free: %" PRIu64 "\n", (uint64_t)kafs_sb_inocnt_free_get(sb));
  printf("  blkcnt_user: %" PRIu64 "\n", (uint64_t)kafs_sb_blkcnt_get(sb));
  printf("  blkcnt_root: %" PRIu64 "\n", r_blkcnt);
  printf("  blkcnt_free: %" PRIu64 "\n", (uint64_t)kafs_sb_blkcnt_free_get(sb));
  printf("  first_data_block: %" PRIu64 "\n", first_data);
  printf("  data_block_count: %" PRIu64 "\n", data_blocks);

  printf("inode_summary:\n");
  printf("  status: %s\n", rc_to_text(rc_inode));
  printf("  total: %" PRIu64 "\n", ino->total);
  printf("  used: %" PRIu64 "\n", ino->used);
  printf("  free: %" PRIu64 "\n", ino->free);
  printf("  linkcnt_zero_used: %" PRIu64 "\n", ino->linkcnt_zero_used);

  printf("hrl_summary:\n");
  printf("  status: %s\n", rc_to_text(rc_hrl));
  printf("  entries: %" PRIu64 "\n", hrl->entry_count);
  printf("  live_entries: %" PRIu64 "\n", hrl->live_entries);
  printf("  total_refcnt: %" PRIu64 "\n", hrl->total_refcnt);

  printf("journal_header:\n");
  printf("  status: %s\n", rc_to_text(rc_journal));
  printf("  available: %s\n", jr->available ? "true" : "false");
  if (jr->available)
  {
    printf("  magic: 0x%08" PRIx32 "\n", jr->header.magic);
    printf("  version: %" PRIu16 "\n", jr->header.version);
    printf("  flags: %" PRIu16 "\n", jr->header.flags);
    printf("  area_size: %" PRIu64 "\n", jr->header.area_size);
    printf("  write_off: %" PRIu64 "\n", jr->header.write_off);
    printf("  seq: %" PRIu64 "\n", jr->header.seq);
    printf("  header_crc: 0x%08" PRIx32 "\n", jr->header.header_crc);
    printf("  header_crc_ok: %s\n", jr->crc_ok ? "true" : "false");
  }
}

static void print_json(const kafs_ssuperblock_t *sb, const struct inode_summary *ino,
                       const struct hrl_summary *hrl, const struct journal_summary *jr,
                       int rc_inode, int rc_hrl, int rc_journal)
{
  uint64_t blksize = kafs_sb_blksize_get(sb);
  uint64_t r_blkcnt = kafs_sb_r_blkcnt_get(sb);
  uint64_t first_data = kafs_sb_first_data_block_get(sb);
  uint64_t data_blocks = (r_blkcnt > first_data) ? (r_blkcnt - first_data) : 0;

  printf("{\n");
  printf("  \"superblock\": {\n");
  printf("    \"magic\": %" PRIu32 ",\n", kafs_sb_magic_get(sb));
  printf("    \"format_version\": %" PRIu32 ",\n", kafs_sb_format_version_get(sb));
  printf("    \"block_size\": %" PRIu64 ",\n", blksize);
  printf("    \"inode_count\": %" PRIu64 ",\n", (uint64_t)kafs_sb_inocnt_get(sb));
  printf("    \"inode_free\": %" PRIu64 ",\n", (uint64_t)kafs_sb_inocnt_free_get(sb));
  printf("    \"blkcnt_user\": %" PRIu64 ",\n", (uint64_t)kafs_sb_blkcnt_get(sb));
  printf("    \"blkcnt_root\": %" PRIu64 ",\n", r_blkcnt);
  printf("    \"blkcnt_free\": %" PRIu64 ",\n", (uint64_t)kafs_sb_blkcnt_free_get(sb));
  printf("    \"first_data_block\": %" PRIu64 ",\n", first_data);
  printf("    \"data_block_count\": %" PRIu64 "\n", data_blocks);
  printf("  },\n");

  printf("  \"inode_summary\": {\n");
  printf("    \"status\": \"%s\",\n", rc_to_text(rc_inode));
  printf("    \"total\": %" PRIu64 ",\n", ino->total);
  printf("    \"used\": %" PRIu64 ",\n", ino->used);
  printf("    \"free\": %" PRIu64 ",\n", ino->free);
  printf("    \"linkcnt_zero_used\": %" PRIu64 "\n", ino->linkcnt_zero_used);
  printf("  },\n");

  printf("  \"hrl_summary\": {\n");
  printf("    \"status\": \"%s\",\n", rc_to_text(rc_hrl));
  printf("    \"entries\": %" PRIu64 ",\n", hrl->entry_count);
  printf("    \"live_entries\": %" PRIu64 ",\n", hrl->live_entries);
  printf("    \"total_refcnt\": %" PRIu64 "\n", hrl->total_refcnt);
  printf("  },\n");

  printf("  \"journal_header\": {\n");
  printf("    \"status\": \"%s\",\n", rc_to_text(rc_journal));
  printf("    \"available\": %s", jr->available ? "true" : "false");
  if (jr->available)
  {
    printf(",\n    \"magic\": %" PRIu32, jr->header.magic);
    printf(",\n    \"version\": %" PRIu16, jr->header.version);
    printf(",\n    \"flags\": %" PRIu16, jr->header.flags);
    printf(",\n    \"area_size\": %" PRIu64, jr->header.area_size);
    printf(",\n    \"write_off\": %" PRIu64, jr->header.write_off);
    printf(",\n    \"seq\": %" PRIu64, jr->header.seq);
    printf(",\n    \"header_crc\": %" PRIu32, jr->header.header_crc);
    printf(",\n    \"header_crc_ok\": %s", jr->crc_ok ? "true" : "false");
  }
  printf("\n  }\n");
  printf("}\n");
}

int main(int argc, char **argv)
{
  int json = 0;
  const char *image = NULL;

  for (int i = 1; i < argc; ++i)
  {
    if (strcmp(argv[i], "--json") == 0)
    {
      json = 1;
      continue;
    }

    if (argv[i][0] == '-')
    {
      usage(argv[0]);
      return 2;
    }

    image = argv[i];
  }

  if (!image)
  {
    usage(argv[0]);
    return 2;
  }

  int fd = open(image, O_RDONLY);
  if (fd < 0)
  {
    perror("open");
    return 1;
  }

  struct stat st;
  if (fstat(fd, &st) != 0)
  {
    perror("fstat");
    close(fd);
    return 1;
  }
  uint64_t file_size = (uint64_t)st.st_size;

  kafs_ssuperblock_t sb;
  int rc = pread_all(fd, &sb, sizeof(sb), 0);
  if (rc != 0)
  {
    fprintf(stderr, "failed to read superblock: %s\n", strerror(-rc));
    close(fd);
    return 1;
  }

  struct inode_summary ino;
  struct hrl_summary hrl;
  struct journal_summary jr;
  int rc_inode = collect_inode_summary(fd, &sb, file_size, &ino);
  int rc_hrl = collect_hrl_summary(fd, &sb, file_size, &hrl);
  int rc_journal = collect_journal_summary(fd, &sb, file_size, &jr);

  if (rc_inode != 0)
    fprintf(stderr, "warning: inode summary unavailable: %s\n", rc_to_text(rc_inode));
  if (rc_hrl != 0)
    fprintf(stderr, "warning: hrl summary unavailable: %s\n", rc_to_text(rc_hrl));
  if (rc_journal != 0)
    fprintf(stderr, "warning: journal header unavailable: %s\n", rc_to_text(rc_journal));

  if (json)
    print_json(&sb, &ino, &hrl, &jr, rc_inode, rc_hrl, rc_journal);
  else
    print_text(&sb, &ino, &hrl, &jr, rc_inode, rc_hrl, rc_journal);

  close(fd);
  return (rc_inode == 0 && rc_hrl == 0 && rc_journal == 0) ? 0 : 1;
}
