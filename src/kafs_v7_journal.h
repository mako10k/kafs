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

typedef struct kafs_v7_journal_segment
{
  kafs_v7_journal_header_t selected_header;
  uint64_t header_block_off;
  uint64_t data_off;
  uint64_t data_bytes;
  uint32_t group_id;
  uint32_t local_segment;
  uint32_t selected_slot;
  uint32_t slot_count;
} kafs_v7_journal_segment_t;

typedef struct kafs_v7_journal_apply_result
{
  uint32_t target_count;
  uint32_t written_target_count;
  uint32_t already_applied_mutation_count;
  uint32_t applied_mutation_count;
} kafs_v7_journal_apply_result_t;

typedef struct kafs_v7_journal_reclaim_result
{
  uint32_t segment_count;
  uint32_t reset_segment_count;
  uint32_t already_empty_segment_count;
} kafs_v7_journal_reclaim_result_t;

/* Select the same highest-generation valid header used by offline replay. */
int kafs_v7_journal_segment_read_fd(int fd, const kafs_v7_layout_report_t *layout,
                                    uint32_t group_id, uint32_t local_segment,
                                    kafs_v7_journal_segment_t *segment);

int kafs_v7_journal_analyze_fd(int fd, const kafs_v7_layout_report_t *layout,
                               kafs_v7_journal_replay_t *replay);
int kafs_v7_journal_delta_add(int64_t a, int64_t b, int64_t *out);
int kafs_v7_journal_overlay_pread(const kafs_v7_journal_replay_t *replay, int fd, void *buf,
                                  size_t bytes, uint64_t off);

/*
 * Materialize the analyzed committed after-images and flush them as one
 * metadata durability phase. The caller must exclude journal publication and
 * checkpoint publication for the complete analyze/apply/checkpoint sequence.
 */
int kafs_v7_journal_apply_fd(const kafs_v7_journal_replay_t *replay, int fd,
                             kafs_v7_journal_apply_result_t *result);

/*
 * Reset every selected non-empty segment covered by the durable checkpoint.
 * The caller must retain the checkpoint/write gate for the whole operation.
 */
int kafs_v7_journal_reclaim_fd(int fd, const kafs_v7_layout_report_t *layout,
                               kafs_v7_journal_reclaim_result_t *result);
void kafs_v7_journal_replay_clear(kafs_v7_journal_replay_t *replay);
