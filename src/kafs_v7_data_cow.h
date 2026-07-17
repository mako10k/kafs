#pragma once

#include "kafs_v7_journal.h"
#include "kafs_v7_journal_writer.h"

#include <stddef.h>
#include <stdint.h>

#define KAFS_V7_DATA_COW_NO_BLOCK UINT64_MAX
#define KAFS_V7_DATA_COW_ALLOCATOR_PATCH_COUNT 2u

typedef struct kafs_v7_data_cow_plan
{
  uint32_t group_id;
  uint32_t block_size;
  uint64_t logical_block;
  uint64_t physical_off;
  uint64_t retained_logical_block;
  uint64_t bitmap_word_logical;
  uint64_t allocator_logical;
  uint8_t bitmap_after[8];
  uint8_t *allocator_after;
  uint32_t allocator_bytes;
  int64_t free_blocks_delta;
} kafs_v7_data_cow_plan_t;

typedef struct kafs_v7_data_cow_plan_request
{
  const kafs_v7_layout_report_t *layout;
  const kafs_v7_journal_replay_t *replay;
  uint32_t group_id;
  uint64_t allocation_cursor;
  uint64_t retained_logical_block;
} kafs_v7_data_cow_plan_request_t;

typedef struct kafs_v7_data_retirement_plan_request
{
  const kafs_v7_layout_report_t *layout;
  const kafs_v7_journal_replay_t *replay;
  uint32_t group_id;
  uint64_t logical_block;
} kafs_v7_data_retirement_plan_request_t;

/*
 * Plan one group-local allocation against the journal-overlay view. The
 * caller must retain the v7 rank 1 -> 2 -> 3 transaction reservation until
 * the returned allocator patches are either published or discarded.
 */
int kafs_v7_data_cow_plan_fd(int fd, const kafs_v7_data_cow_plan_request_t *request,
                             kafs_v7_data_cow_plan_t *plan);
int kafs_v7_data_retirement_plan_fd(int fd, const kafs_v7_data_retirement_plan_request_t *request,
                                    kafs_v7_data_cow_plan_t *plan);
void kafs_v7_data_cow_plan_clear(kafs_v7_data_cow_plan_t *plan);

int kafs_v7_data_cow_plan_patches(
    const kafs_v7_data_cow_plan_t *plan,
    kafs_v7_journal_patch_t patches[KAFS_V7_DATA_COW_ALLOCATOR_PATCH_COUNT]);

/* Write and flush a full block, then read it back before metadata publication. */
int kafs_v7_data_cow_stage_fd(int fd, const kafs_v7_data_cow_plan_t *plan, const void *data,
                              size_t data_bytes, void *verified_copy);
int kafs_v7_data_cow_verify_fd(int fd, const kafs_v7_data_cow_plan_t *plan, const void *expected,
                               size_t expected_bytes);
