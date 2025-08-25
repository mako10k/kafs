#include "kafs.h"
#include "kafs_superblock.h"
#include "kafs_hash.h"

#include <fcntl.h>
#include <inttypes.h>
#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>

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

       uint64_t index_size = kafs_sb_hrl_index_size_get(&sb);
       uint64_t entry_off = kafs_sb_hrl_entry_offset_get(&sb);
       uint32_t entry_cnt = kafs_sb_hrl_entry_cnt_get(&sb);
       if (index_size && entry_off && entry_cnt) {
              uint32_t buckets = (uint32_t)(index_size / sizeof(uint32_t));
              printf("hrl buckets=%u\n", buckets);
              size_t ents_bytes = (size_t)entry_cnt * sizeof(kafs_hrl_entry_t);
              kafs_hrl_entry_t *ents = malloc(ents_bytes);
              if (!ents) { perror("malloc"); close(fd); return 1; }
              ssize_t er = pread(fd, ents, ents_bytes, (off_t)entry_off);
              if (er != (ssize_t)ents_bytes) { perror("pread hrl entries"); free(ents); close(fd); return 1; }
              uint32_t used = 0;
              for (uint32_t i = 0; i < entry_cnt; ++i) if (ents[i].refcnt) ++used;
              printf("hrl entries used=%u / %u\n", used, entry_cnt);
              free(ents);
       }

  close(fd);
  return 0;
}
