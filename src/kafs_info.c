#include "kafs.h"
#include "kafs_superblock.h"

#include <fcntl.h>
#include <inttypes.h>
#include <stdio.h>
#include <unistd.h>

static void usage(const char *prog) {
  fprintf(stderr, "Usage: %s <image>\n", prog);
}

int main(int argc, char **argv) {
  if (argc < 2) { usage(argv[0]); return 2; }
  const char *img = argv[1];
  int fd = open(img, O_RDONLY);
  if (fd < 0) { perror("open"); return 1; }
  kafs_ssuperblock_t sb;
  ssize_t r = pread(fd, &sb, sizeof(sb), 0);
  if (r != (ssize_t)sizeof(sb)) { perror("pread superblock"); close(fd); return 1; }

  printf("magic=0x%08" PRIx32 " version=%" PRIu32 " log_blksize=%" PRIuFAST16 " (bytes=%u)\n",
         kafs_sb_magic_get(&sb),
         kafs_sb_format_version_get(&sb),
         kafs_sb_log_blksize_get(&sb),
         1u << kafs_sb_log_blksize_get(&sb));
  printf("inodes total=%" PRIuFAST32 " free=%" PRIuFAST32 "\n",
         kafs_sb_inocnt_get(&sb),
         (uint_fast32_t)kafs_sb_inocnt_free_get(&sb));
  printf("blocks user=%" PRIuFAST32 " root=%" PRIuFAST32 " free=%" PRIuFAST32 " first_data=%" PRIuFAST32 "\n",
         kafs_sb_blkcnt_get(&sb),
         kafs_sb_r_blkcnt_get(&sb),
         kafs_sb_blkcnt_free_get(&sb),
         kafs_blkcnt_stoh(sb.s_first_data_block));
  printf("hash fast=%" PRIu32 " strong=%" PRIu32 "\n",
         kafs_sb_hash_fast_get(&sb),
         kafs_sb_hash_strong_get(&sb));
  printf("hrl index: off=%" PRIu64 " size=%" PRIu64 "; entries: off=%" PRIu64 " cnt=%" PRIu32 "\n",
         (uint64_t)kafs_sb_hrl_index_offset_get(&sb),
         (uint64_t)kafs_sb_hrl_index_size_get(&sb),
         (uint64_t)kafs_sb_hrl_entry_offset_get(&sb),
         (uint32_t)kafs_sb_hrl_entry_cnt_get(&sb));

  close(fd);
  return 0;
}
