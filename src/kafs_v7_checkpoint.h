#pragma once

#include "kafs_v7_layout.h"
#include "kafs_v7_locks.h"

#include <stdint.h>

typedef struct kafs_v7_checkpoint_plan
{
  kafs_v7_checkpoint_t record;
  uint32_t target_replicas[2];
  uint32_t target_count;
  uint32_t existing_copy_count;
  uint8_t resumed;
} kafs_v7_checkpoint_plan_t;

typedef struct kafs_v7_checkpoint_publish_result
{
  uint64_t generation;
  uint64_t checkpoint_sequence;
  uint32_t target_replicas[2];
  uint32_t written_copy_count;
  uint32_t verified_copy_count;
  uint8_t resumed;
} kafs_v7_checkpoint_publish_result_t;

/*
 * Build the next publication plan from a freshly validated layout report.
 * A report with only one selected-generation copy resumes that generation;
 * otherwise publication advances to a new generation.
 */
int kafs_v7_checkpoint_plan(const kafs_v7_layout_report_t *layout, kafs_v7_checkpoint_plan_t *plan);

/*
 * Validate the image, flush earlier metadata/journal writes, publish and flush
 * each full checkpoint block separately, then require two byte-identical
 * read-back copies. Journal reclamation is intentionally a later operation.
 */
int kafs_v7_checkpoint_publish_fd(kafs_v7_lock_state_t *locks, int fd, const kafs_ssuperblock_t *sb,
                                  uint64_t file_size, kafs_v7_checkpoint_publish_result_t *result);
