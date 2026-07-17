#include "kafs_v7_checkpoint.h"

#include "kafs_tool_util.h"
#include "kafs_v7_io.h"

#include <endian.h>
#include <errno.h>
#include <limits.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

static void kafs_v7_checkpoint_build(kafs_v7_checkpoint_t *record, uint64_t generation,
                                     uint64_t descriptor_generation, uint64_t sequence,
                                     uint64_t free_blocks, uint64_t free_inodes)
{
  memset(record, 0, sizeof(*record));
  record->magic = htole32(KAFS_V7_CHECKPOINT_MAGIC);
  record->version = htole16(KAFS_V7_CHECKPOINT_VERSION);
  record->record_bytes = htole16(KAFS_V7_CHECKPOINT_BYTES);
  record->generation = htole64(generation);
  record->descriptor_generation = htole64(descriptor_generation);
  record->checkpoint_seq = htole64(sequence);
  record->free_blocks = htole64(free_blocks);
  record->free_inodes = htole64(free_inodes);
  record->crc32 = htole32(kafs_v7_crc32(record, sizeof(*record)));
}

static int kafs_v7_checkpoint_add_target(kafs_v7_checkpoint_plan_t *plan, uint32_t id)
{
  if (plan->target_count >= 2u)
    return -EOVERFLOW;
  for (uint32_t i = 0; i < plan->target_count; ++i)
  {
    if (plan->target_replicas[i] == id)
      return 0;
  }
  plan->target_replicas[plan->target_count++] = id;
  return 0;
}

int kafs_v7_checkpoint_plan(const kafs_v7_layout_report_t *layout, kafs_v7_checkpoint_plan_t *plan)
{
  if (!layout || !plan || !layout->descriptor || !layout->selected_found ||
      layout->replica_count < 2u || layout->replica_count > KAFS_V7_REPLICA_MAX_COUNT ||
      layout->selected_checkpoint >= layout->replica_count || layout->block_size == 0)
    return -EINVAL;
  const kafs_v7_layout_header_t *header = kafs_v7_report_header(layout);
  if (!header || le64toh(header->generation) != layout->selected_generation)
    return -EINVAL;

  memset(plan, 0, sizeof(*plan));
  plan->existing_copy_count = kafs_v7_layout_checkpoint_copy_count(layout);
  if (plan->existing_copy_count == 0u)
    return -EUCLEAN;

  uint64_t generation = layout->checkpoint_generation;
  uint64_t sequence = layout->checkpoint_sequence;
  uint64_t free_blocks = layout->checkpoint_free_blocks;
  uint64_t free_inodes = layout->checkpoint_free_inodes;
  uint32_t required_targets = 0;
  if (plan->existing_copy_count < 2u)
  {
    plan->resumed = 1u;
    required_targets = 2u - plan->existing_copy_count;
  }
  else
  {
    if (layout->journal.replay_mutation_count != 0u)
      return -EBUSY;
    if (generation == UINT64_MAX)
      return -EOVERFLOW;
    ++generation;
    if (layout->journal.last_sequence > sequence)
      sequence = layout->journal.last_sequence;
    free_blocks = layout->free_blocks;
    free_inodes = layout->free_inodes;
    required_targets = 2u;
  }

  kafs_v7_checkpoint_build(&plan->record, generation, layout->selected_generation, sequence,
                           free_blocks, free_inodes);

  uint32_t start = (uint32_t)(generation % layout->replica_count);
  for (uint32_t pass = 0; pass < 2u && plan->target_count < required_targets; ++pass)
  {
    for (uint32_t step = 0; step < layout->replica_count && plan->target_count < required_targets;
         ++step)
    {
      uint32_t id = (start + step) % layout->replica_count;
      int current = layout->checkpoints[id].status == KAFS_V7_REPLICA_STATUS_VALID &&
                    layout->checkpoints[id].generation == layout->checkpoint_generation;
      if (plan->resumed && current)
        continue;
      if (!plan->resumed && pass == 0u && id == layout->selected_checkpoint)
        continue;
      if (!plan->resumed && pass == 1u && id != layout->selected_checkpoint)
        continue;
      int rc = kafs_v7_checkpoint_add_target(plan, id);
      if (rc != 0)
        return rc;
    }
  }
  return plan->target_count == required_targets ? 0 : -EUCLEAN;
}

static int kafs_v7_checkpoint_block_matches(int fd, const kafs_v7_copy_report_t *copy,
                                            const void *expected, size_t bytes, int *matches)
{
  if (!copy || !expected || !matches || copy->offset > INT64_MAX ||
      bytes > (uint64_t)INT64_MAX - copy->offset)
    return -ERANGE;
  void *actual = malloc(bytes);
  if (!actual)
    return -ENOMEM;
  int rc = kafs_pread_all(fd, actual, bytes, (off_t)copy->offset);
  if (rc == 0)
    *matches = memcmp(actual, expected, bytes) == 0;
  free(actual);
  return rc;
}

static int kafs_v7_checkpoint_publish_unlocked_fd(int fd, const kafs_ssuperblock_t *sb,
                                                  uint64_t file_size,
                                                  kafs_v7_checkpoint_publish_result_t *result)
{
  if (fd < 0 || !sb || !result)
    return -EINVAL;
  int rc = kafs_v7_io_require_positional_writes(fd);
  if (rc != 0)
    return rc;

  memset(result, 0, sizeof(*result));
  kafs_v7_layout_report_t layout;
  memset(&layout, 0, sizeof(layout));
  rc = kafs_v7_validate_image_fd(fd, sb, file_size, &layout);
  kafs_v7_checkpoint_plan_t plan;
  if (rc == 0)
    rc = kafs_v7_checkpoint_plan(&layout, &plan);

  void *block = NULL;
  if (rc == 0)
  {
    block = calloc(1u, layout.block_size);
    if (!block)
      rc = -ENOMEM;
    else
      memcpy(block, &plan.record, sizeof(plan.record));
  }
  if (rc == 0 && fdatasync(fd) != 0)
    rc = -errno;

  for (uint32_t target = 0; rc == 0 && target < plan.target_count; ++target)
  {
    uint32_t id = plan.target_replicas[target];
    uint64_t off = layout.checkpoints[id].offset;
    if (off > INT64_MAX || layout.block_size > (uint64_t)INT64_MAX - off)
      rc = -ERANGE;
    else
      rc = kafs_pwrite_all(fd, block, layout.block_size, (off_t)off);
    if (rc == 0 && fdatasync(fd) != 0)
      rc = -errno;
  }

  uint32_t verified = 0;
  int readback_error = 0;
  for (uint32_t id = 0; rc == 0 && id < layout.replica_count; ++id)
  {
    int matches = 0;
    int read_rc = kafs_v7_checkpoint_block_matches(fd, &layout.checkpoints[id], block,
                                                   layout.block_size, &matches);
    if (read_rc == 0 && matches)
      ++verified;
    else if (read_rc != 0 && readback_error == 0)
      readback_error = read_rc;
  }
  if (rc == 0 && verified < 2u)
    rc = readback_error != 0 ? readback_error : -EIO;
  if (rc == 0)
  {
    result->generation = le64toh(plan.record.generation);
    result->checkpoint_sequence = le64toh(plan.record.checkpoint_seq);
    result->written_copy_count = plan.target_count;
    result->verified_copy_count = verified;
    result->resumed = plan.resumed;
    memcpy(result->target_replicas, plan.target_replicas, sizeof(result->target_replicas));
  }
  free(block);
  kafs_v7_layout_report_clear(&layout);
  return rc;
}

int kafs_v7_checkpoint_publish_fd(kafs_v7_lock_state_t *locks, int fd, const kafs_ssuperblock_t *sb,
                                  uint64_t file_size, kafs_v7_checkpoint_publish_result_t *result)
{
  if (!locks)
    return -EINVAL;
  int rc = kafs_v7_checkpoint_lock(locks);
  if (rc != 0)
    return rc;
  rc = kafs_v7_checkpoint_publish_unlocked_fd(fd, sb, file_size, result);
  int unlock_rc = kafs_v7_checkpoint_unlock(locks);
  return rc != 0 ? rc : unlock_rc;
}

static uint32_t kafs_v7_descriptor_current_copies(const kafs_v7_layout_report_t *layout)
{
  uint32_t count = 0u;
  for (uint32_t id = 0; id < layout->replica_count; ++id)
  {
    if (layout->descriptors[id].status == KAFS_V7_REPLICA_STATUS_VALID &&
        layout->descriptors[id].generation == layout->selected_generation)
      ++count;
  }
  return count;
}

static int kafs_v7_metadata_closeout_refresh(int fd, const kafs_ssuperblock_t *sb,
                                             uint64_t file_size, kafs_v7_layout_report_t *layout)
{
  kafs_v7_layout_report_clear(layout);
  memset(layout, 0, sizeof(*layout));
  return kafs_v7_validate_image_fd(fd, sb, file_size, layout);
}

static void
kafs_v7_metadata_closeout_record_checkpoint(kafs_v7_metadata_closeout_result_t *result,
                                            const kafs_v7_checkpoint_publish_result_t *publication)
{
  ++result->checkpoint_publication_count;
  if (publication->resumed)
    ++result->checkpoint_resume_count;
  result->last_checkpoint_publication = *publication;
}

static int kafs_v7_metadata_closeout_publish(kafs_v7_metadata_closeout_result_t *result, int fd,
                                             const kafs_ssuperblock_t *sb, uint64_t file_size)
{
  kafs_v7_checkpoint_publish_result_t publication;
  int rc = kafs_v7_checkpoint_publish_unlocked_fd(fd, sb, file_size, &publication);
  if (rc == 0)
    kafs_v7_metadata_closeout_record_checkpoint(result, &publication);
  return rc;
}

static int kafs_v7_metadata_closeout_apply(int fd, const kafs_v7_layout_report_t *layout,
                                           kafs_v7_metadata_closeout_result_t *result)
{
  kafs_v7_journal_replay_t replay;
  memset(&replay, 0, sizeof(replay));
  int rc = kafs_v7_journal_analyze_fd(fd, layout, &replay);
  if (rc == 0)
    rc = kafs_v7_journal_apply_fd(&replay, fd, &result->apply);
  kafs_v7_journal_replay_clear(&replay);
  return rc;
}

static int kafs_v7_metadata_closeout_redundancy(int fd, const kafs_ssuperblock_t *sb,
                                                uint64_t file_size, kafs_v7_layout_report_t *layout,
                                                kafs_v7_metadata_closeout_result_t *result)
{
  if (kafs_v7_descriptor_current_copies(layout) < 2u)
    return -EUCLEAN;
  int rc = 0;
  if (kafs_v7_layout_checkpoint_copy_count(layout) < 2u)
    rc = kafs_v7_metadata_closeout_publish(result, fd, sb, file_size);
  if (rc == 0)
    rc = kafs_v7_metadata_closeout_refresh(fd, sb, file_size, layout);
  if (rc != 0)
    return rc;
  return kafs_v7_descriptor_current_copies(layout) < 2u ||
                 kafs_v7_layout_checkpoint_copy_count(layout) < 2u
             ? -EUCLEAN
             : 0;
}

static int kafs_v7_metadata_closeout_checkpoint(int fd, const kafs_ssuperblock_t *sb,
                                                uint64_t file_size, kafs_v7_layout_report_t *layout,
                                                kafs_v7_metadata_closeout_result_t *result)
{
  int rc = 0;
  if (layout->journal.last_sequence > layout->checkpoint_sequence)
    rc = kafs_v7_metadata_closeout_apply(fd, layout, result);
  if (rc == 0)
    rc = kafs_v7_metadata_closeout_refresh(fd, sb, file_size, layout);
  if (rc == 0 && layout->journal.replay_mutation_count != 0u)
    rc = -EUCLEAN;
  if (rc == 0 && layout->journal.last_sequence > layout->checkpoint_sequence)
    rc = kafs_v7_metadata_closeout_publish(result, fd, sb, file_size);
  if (rc == 0)
    rc = kafs_v7_metadata_closeout_refresh(fd, sb, file_size, layout);
  if (rc != 0)
    return rc;
  return kafs_v7_layout_checkpoint_copy_count(layout) < 2u ||
                 layout->journal.last_sequence > layout->checkpoint_sequence
             ? -EUCLEAN
             : 0;
}

static int kafs_v7_metadata_closeout_reclaim(int fd, const kafs_ssuperblock_t *sb,
                                             uint64_t file_size, kafs_v7_layout_report_t *layout,
                                             kafs_v7_metadata_closeout_result_t *result)
{
  int rc = kafs_v7_journal_reclaim_fd(fd, layout, &result->reclaim);
  if (rc == 0)
    rc = kafs_v7_metadata_closeout_refresh(fd, sb, file_size, layout);
  if (rc == 0 && layout->journal.selected_nonempty_segment_count != 0u)
    rc = -EUCLEAN;
  if (rc == 0)
  {
    result->final_checkpoint_generation = layout->checkpoint_generation;
    result->final_checkpoint_sequence = layout->checkpoint_sequence;
  }
  return rc;
}

static int kafs_v7_metadata_closeout_unlocked_fd(int fd, const kafs_ssuperblock_t *sb,
                                                 uint64_t file_size,
                                                 kafs_v7_metadata_closeout_result_t *result)
{
  kafs_v7_layout_report_t layout;
  memset(&layout, 0, sizeof(layout));
  int rc = kafs_v7_validate_image_fd(fd, sb, file_size, &layout);
  if (rc == 0)
    rc = kafs_v7_metadata_closeout_redundancy(fd, sb, file_size, &layout, result);
  if (rc == 0)
    rc = kafs_v7_metadata_closeout_checkpoint(fd, sb, file_size, &layout, result);
  if (rc == 0)
    rc = kafs_v7_metadata_closeout_reclaim(fd, sb, file_size, &layout, result);
  kafs_v7_layout_report_clear(&layout);
  return rc;
}

int kafs_v7_metadata_closeout_fd(kafs_v7_lock_state_t *locks, int fd, const kafs_ssuperblock_t *sb,
                                 uint64_t file_size, kafs_v7_metadata_closeout_result_t *result)
{
  if (!locks || fd < 0 || !sb || !result)
    return -EINVAL;
  int rc = kafs_v7_io_require_positional_writes(fd);
  if (rc != 0)
    return rc;
  memset(result, 0, sizeof(*result));
  rc = kafs_v7_checkpoint_lock(locks);
  if (rc != 0)
    return rc;
  rc = kafs_v7_metadata_closeout_unlocked_fd(fd, sb, file_size, result);
  int unlock_rc = kafs_v7_checkpoint_unlock(locks);
  return rc != 0 ? rc : unlock_rc;
}
