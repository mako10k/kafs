#pragma once

#include "kafs_v7_layout.h"

#include <stddef.h>
#include <stdint.h>

typedef struct kafs_v7_journal_replay
{
  kafs_v7_journal_report_t report;
  uint64_t recovered_free_blocks;
  uint64_t recovered_free_inodes;
  void *state;
} kafs_v7_journal_replay_t;

int kafs_v7_journal_analyze_fd(int fd, const kafs_v7_layout_report_t *layout,
                               kafs_v7_journal_replay_t *replay);
int kafs_v7_journal_overlay_pread(const kafs_v7_journal_replay_t *replay, int fd, void *buf,
                                  size_t bytes, uint64_t off);
void kafs_v7_journal_replay_clear(kafs_v7_journal_replay_t *replay);
