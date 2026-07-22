#pragma once

#include <stdint.h>

typedef struct kafs_v7_import_options
{
  const char *source_path;
  const char *destination_path;
  uint64_t destination_size_bytes;
  uint32_t destination_inode_count;
  uint32_t destination_block_size;
  uint64_t journal_bytes;
  double hrl_entry_ratio;
  uint32_t group_count;
  int dry_run;
} kafs_v7_import_options_t;

typedef struct kafs_v7_import_report
{
  uint32_t source_inode_count;
  uint32_t imported_inode_count;
  uint32_t imported_directory_count;
  uint32_t imported_regular_count;
  uint32_t imported_symlink_count;
  uint32_t destination_block_size;
  uint32_t destination_group_count;
  uint64_t destination_size_bytes;
  uint64_t payload_bytes;
  uint64_t allocated_blocks;
  uint32_t source_crc32;
  char work_path[4096];
} kafs_v7_import_report_t;

int kafs_v7_import_image(const kafs_v7_import_options_t *options, kafs_v7_import_report_t *report);
