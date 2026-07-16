#include "test_utils.h"

#include "kafs_offline_summary.h"
#include "kafs_tool_util.h"
#include "kafs_v7_checkpoint.h"
#include "kafs_v7_journal_writer.h"
#include "kafs_v7_mutation.h"
#include "kafs_v7_sequence.h"

#include <endian.h>
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

typedef struct checkpoint_fixture
{
  int fd;
  uint64_t file_size;
  kafs_ssuperblock_t superblock;
  kafs_v7_layout_report_t layout;
  kafs_v7_lock_state_t *locks;
} checkpoint_fixture_t;

static int run_command(char *const argv[])
{
  pid_t pid = fork();
  if (pid < 0)
    return -errno;
  if (pid == 0)
  {
    execvp(argv[0], argv);
    _exit(127);
  }
  int status = 0;
  if (waitpid(pid, &status, 0) != pid || !WIFEXITED(status))
    return -1;
  return WEXITSTATUS(status) == 0 ? 0 : -1;
}

static int format_image(const char *path)
{
  unlink(path);
  char *argv[] = {(char *)kafs_test_mkfs_bin(),
                  (char *)path,
                  (char *)"--format-version",
                  (char *)"7",
                  (char *)"--size-bytes",
                  (char *)"128M",
                  (char *)"--v7-group-count",
                  (char *)"4",
                  (char *)"--yes",
                  NULL};
  return run_command(argv);
}

static int fixture_open(checkpoint_fixture_t *fixture, const char *path, int flags)
{
  if (!fixture)
    return -EINVAL;
  memset(fixture, 0, sizeof(*fixture));
  fixture->fd = open(path, flags);
  if (fixture->fd < 0)
    return -errno;
  int rc = kafs_pread_all(fixture->fd, &fixture->superblock, sizeof(fixture->superblock), 0);
  if (rc == 0)
    rc = kafs_offline_detect_file_size(fixture->fd, &fixture->file_size);
  if (rc == 0)
    rc = kafs_v7_validate_image_fd(fixture->fd, &fixture->superblock, fixture->file_size,
                                   &fixture->layout);
  if (rc == 0)
    rc = kafs_v7_locks_init(fixture->layout.group_count, 0u, &fixture->locks);
  if (rc != 0)
  {
    kafs_v7_locks_destroy(fixture->locks);
    kafs_v7_layout_report_clear(&fixture->layout);
    close(fixture->fd);
    fixture->fd = -1;
  }
  return rc;
}

static void fixture_close(checkpoint_fixture_t *fixture)
{
  if (!fixture)
    return;
  if (fixture->fd >= 0)
    close(fixture->fd);
  kafs_v7_locks_destroy(fixture->locks);
  kafs_v7_layout_report_clear(&fixture->layout);
  memset(fixture, 0, sizeof(*fixture));
  fixture->fd = -1;
}

static int fixture_refresh(checkpoint_fixture_t *fixture)
{
  kafs_v7_layout_report_clear(&fixture->layout);
  memset(&fixture->layout, 0, sizeof(fixture->layout));
  return kafs_v7_validate_image_fd(fixture->fd, &fixture->superblock, fixture->file_size,
                                   &fixture->layout);
}

static int publish_inode_patch(checkpoint_fixture_t *fixture, uint64_t inode, uint16_t uid,
                               uint32_t terminal_tag)
{
  kafs_v7_mutation_route_t route;
  int rc = kafs_v7_mutation_route_target(&fixture->layout, KAFS_V7_JOURNAL_TARGET_INODE, inode,
                                         &route);
  kafs_v7_sequence_state_t *sequence = NULL;
  if (rc == 0)
    rc = kafs_v7_sequence_state_init(fixture->locks, &fixture->layout, &sequence);
  kafs_v7_sequence_reservation_t reservation;
  memset(&reservation, 0, sizeof(reservation));
  if (rc == 0)
    rc = kafs_v7_sequence_reserve(sequence, route.group_id, &reservation);
  uint16_t wire_uid = htole16(uid);
  kafs_v7_journal_patch_t patch = {
      .target_type = KAFS_V7_JOURNAL_TARGET_INODE,
      .logical_index = inode,
      .patch_off = offsetof(kafs_v7_inode_t, uid),
      .patch_bytes = sizeof(wire_uid),
      .patch = &wire_uid,
  };
  kafs_v7_journal_transaction_t *transaction = NULL;
  if (rc == 0)
    rc = kafs_v7_journal_transaction_encode_fd(fixture->fd, &fixture->layout, &reservation,
                                                &patch, 1u, terminal_tag, &transaction);
  kafs_v7_journal_publication_t publication;
  if (rc == 0)
    rc = kafs_v7_journal_transaction_publish_fd(fixture->fd, &fixture->layout, &reservation,
                                                 transaction, &publication);
  if (rc == 0)
    rc = kafs_v7_sequence_confirm_publication_fd(sequence, &reservation, fixture->fd,
                                                  &fixture->superblock, fixture->file_size);
  else if (reservation.active)
    (void)kafs_v7_sequence_cancel_reservation_fd(sequence, &reservation, fixture->fd,
                                                  &fixture->superblock, fixture->file_size);
  kafs_v7_journal_transaction_destroy(transaction);
  kafs_v7_sequence_state_destroy(sequence);
  return rc == 0 ? fixture_refresh(fixture) : rc;
}

static int read_inode_uid(const checkpoint_fixture_t *fixture, uint64_t inode, uint16_t *uid)
{
  kafs_v7_mutation_route_t route;
  int rc = kafs_v7_mutation_route_target(&fixture->layout, KAFS_V7_JOURNAL_TARGET_INODE, inode,
                                         &route);
  kafs_v7_inode_t record;
  if (rc == 0)
    rc = kafs_pread_all(fixture->fd, &record, sizeof(record), (off_t)route.physical_off);
  if (rc == 0)
    *uid = le16toh(record.uid);
  return rc;
}

static int find_inode_in_group(const checkpoint_fixture_t *fixture, uint32_t group_id,
                               uint64_t *inode)
{
  uint64_t inode_count = kafs_sb_inocnt_get(&fixture->superblock);
  for (uint64_t candidate = 2u; candidate < inode_count; ++candidate)
  {
    kafs_v7_mutation_route_t route;
    if (kafs_v7_mutation_route_target(&fixture->layout, KAFS_V7_JOURNAL_TARGET_INODE, candidate,
                                      &route) == 0 &&
        route.group_id == group_id)
    {
      *inode = candidate;
      return 0;
    }
  }
  return -ENOENT;
}

static int apply_metadata_only(checkpoint_fixture_t *fixture,
                               kafs_v7_journal_apply_result_t *result)
{
  int rc = kafs_v7_checkpoint_lock(fixture->locks);
  if (rc != 0)
    return rc;
  kafs_v7_journal_replay_t replay;
  memset(&replay, 0, sizeof(replay));
  rc = kafs_v7_journal_analyze_fd(fixture->fd, &fixture->layout, &replay);
  if (rc == 0)
    rc = kafs_v7_journal_apply_fd(&replay, fixture->fd, result);
  kafs_v7_journal_replay_clear(&replay);
  int unlock_rc = kafs_v7_checkpoint_unlock(fixture->locks);
  if (rc == 0)
    rc = unlock_rc;
  return rc == 0 ? fixture_refresh(fixture) : rc;
}

static uint32_t selected_generation_copies(const kafs_v7_layout_report_t *layout)
{
  uint32_t count = 0;
  for (uint32_t id = 0; id < layout->replica_count; ++id)
  {
    if (layout->checkpoints[id].status == KAFS_V7_REPLICA_STATUS_VALID &&
        layout->checkpoints[id].generation == layout->checkpoint_generation)
      ++count;
  }
  return count;
}

static int checkpoint_blocks_equal(const checkpoint_fixture_t *fixture, uint32_t left,
                                   uint32_t right)
{
  void *left_block = malloc(fixture->layout.block_size);
  void *right_block = malloc(fixture->layout.block_size);
  if (!left_block || !right_block)
  {
    free(left_block);
    free(right_block);
    return -ENOMEM;
  }
  int rc = kafs_pread_all(fixture->fd, left_block, fixture->layout.block_size,
                          (off_t)fixture->layout.checkpoints[left].offset);
  if (rc == 0)
    rc = kafs_pread_all(fixture->fd, right_block, fixture->layout.block_size,
                        (off_t)fixture->layout.checkpoints[right].offset);
  if (rc == 0 && memcmp(left_block, right_block, fixture->layout.block_size) != 0)
    rc = -EUCLEAN;
  free(left_block);
  free(right_block);
  return rc;
}

static int reset_one_journal_segment(checkpoint_fixture_t *fixture, uint32_t group_id,
                                     uint32_t local_segment)
{
  kafs_v7_journal_segment_t segment;
  int rc = kafs_v7_journal_segment_read_fd(fixture->fd, &fixture->layout, group_id, local_segment,
                                           &segment);
  uint64_t generation = rc == 0 ? le64toh(segment.selected_header.generation) : 0u;
  if (rc == 0 && (le64toh(segment.selected_header.write_bytes) == 0u ||
                  generation == UINT64_MAX))
    rc = -EINVAL;
  if (rc != 0)
    return rc;
  ++generation;
  uint32_t slot = (uint32_t)((generation - 1u) % segment.slot_count);
  uint64_t header_off = segment.header_block_off +
                        (uint64_t)slot * KAFS_V7_JOURNAL_HEADER_BYTES;
  if (header_off > INT64_MAX)
    return -ERANGE;
  kafs_v7_journal_header_t empty;
  memset(&empty, 0, sizeof(empty));
  empty.magic = htole32(KAFS_V7_JOURNAL_HEADER_MAGIC);
  empty.version = htole16(KAFS_V7_JOURNAL_HEADER_VERSION);
  empty.segment_id = segment.selected_header.segment_id;
  empty.slot_bytes = htole32(KAFS_V7_JOURNAL_HEADER_BYTES);
  empty.generation = htole64(generation);
  empty.data_bytes = segment.selected_header.data_bytes;
  empty.crc32 = htole32(kafs_v7_crc32(&empty, sizeof(empty)));
  rc = kafs_pwrite_all(fixture->fd, &empty, sizeof(empty), (off_t)header_off);
  if (rc == 0 && fdatasync(fixture->fd) != 0)
    rc = -errno;
  return rc == 0 ? fixture_refresh(fixture) : rc;
}

static int test_two_copy_publication(void)
{
  const char *path = "v7-checkpoint-two-copy.img";
  if (format_image(path) != 0)
    return -1;
  checkpoint_fixture_t fixture;
  if (fixture_open(&fixture, path, O_RDWR) != 0)
    return -1;
  kafs_v7_checkpoint_plan_t plan;
  int rc = kafs_v7_checkpoint_plan(&fixture.layout, &plan);
  if (rc == 0 && (le64toh(plan.record.generation) != 2u || plan.resumed ||
                  plan.target_count != 2u || plan.target_replicas[0] != 1u ||
                  plan.target_replicas[1] != 0u))
    rc = -1;
  kafs_v7_checkpoint_publish_result_t result;
  if (rc == 0)
    rc = kafs_v7_checkpoint_publish_fd(fixture.locks, fixture.fd, &fixture.superblock,
                                       fixture.file_size, &result);
  if (rc == 0 && (result.generation != 2u || result.resumed ||
                  result.written_copy_count != 2u || result.verified_copy_count != 2u))
    rc = -1;
  fixture_close(&fixture);
  if (rc != 0 || fixture_open(&fixture, path, O_RDONLY) != 0)
    return -1;
  rc = fixture.layout.checkpoint_generation == 2u &&
               selected_generation_copies(&fixture.layout) == 2u && !fixture.layout.degraded
           ? checkpoint_blocks_equal(&fixture, 0u, 1u)
           : -1;
  fixture_close(&fixture);
  return rc;
}

static int write_first_planned_copy(checkpoint_fixture_t *fixture,
                                    const kafs_v7_checkpoint_plan_t *plan)
{
  void *block = calloc(1u, fixture->layout.block_size);
  if (!block)
    return -ENOMEM;
  memcpy(block, &plan->record, sizeof(plan->record));
  uint32_t id = plan->target_replicas[0];
  int rc = kafs_pwrite_all(fixture->fd, block, fixture->layout.block_size,
                           (off_t)fixture->layout.checkpoints[id].offset);
  if (rc == 0 && fdatasync(fixture->fd) != 0)
    rc = -errno;
  free(block);
  return rc;
}

static int test_interrupted_publication_resume(void)
{
  const char *path = "v7-checkpoint-resume.img";
  if (format_image(path) != 0)
    return -1;
  checkpoint_fixture_t fixture;
  if (fixture_open(&fixture, path, O_RDWR) != 0)
    return -1;
  kafs_v7_checkpoint_plan_t plan;
  int rc = kafs_v7_checkpoint_plan(&fixture.layout, &plan);
  if (rc == 0)
    rc = write_first_planned_copy(&fixture, &plan);
  fixture_close(&fixture);
  if (rc != 0 || fixture_open(&fixture, path, O_RDWR) != 0)
    return -1;
  if (fixture.layout.checkpoint_generation != 2u || !fixture.layout.degraded ||
      selected_generation_copies(&fixture.layout) != 1u)
    rc = -1;
  kafs_v7_checkpoint_publish_result_t result;
  if (rc == 0)
    rc = kafs_v7_checkpoint_publish_fd(fixture.locks, fixture.fd, &fixture.superblock,
                                       fixture.file_size, &result);
  if (rc == 0 && (result.generation != 2u || !result.resumed ||
                  result.written_copy_count != 1u || result.verified_copy_count != 2u))
    rc = -1;
  fixture_close(&fixture);
  if (rc != 0 || fixture_open(&fixture, path, O_RDONLY) != 0)
    return -1;
  rc = fixture.layout.checkpoint_generation == 2u &&
               selected_generation_copies(&fixture.layout) == 2u && !fixture.layout.degraded
           ? checkpoint_blocks_equal(&fixture, 0u, 1u)
           : -1;
  fixture_close(&fixture);
  return rc;
}

static int test_plan_guards(void)
{
  const char *path = "v7-checkpoint-guards.img";
  if (format_image(path) != 0)
    return -1;
  checkpoint_fixture_t fixture;
  if (fixture_open(&fixture, path, O_RDONLY) != 0)
    return -1;
  kafs_v7_checkpoint_plan_t plan;
  fixture.layout.journal.replay_mutation_count = 1u;
  int rc = kafs_v7_checkpoint_plan(&fixture.layout, &plan) == -EBUSY ? 0 : -1;
  fixture.layout.journal.replay_mutation_count = 0u;
  fixture.layout.checkpoint_generation = UINT64_MAX;
  for (uint32_t id = 0; id < fixture.layout.replica_count; ++id)
    fixture.layout.checkpoints[id].generation = UINT64_MAX;
  if (rc == 0 && kafs_v7_checkpoint_plan(&fixture.layout, &plan) != -EOVERFLOW)
    rc = -1;
  kafs_v7_checkpoint_publish_result_t result;
  if (rc == 0 && kafs_v7_checkpoint_publish_fd(fixture.locks, fixture.fd, &fixture.superblock,
                                                fixture.file_size, &result) != -EBADF)
    rc = -1;
  kafs_v7_metadata_closeout_result_t closeout;
  if (rc == 0 && kafs_v7_metadata_closeout_fd(fixture.locks, fixture.fd, &fixture.superblock,
                                               fixture.file_size, &closeout) != -EBADF)
    rc = -1;
  fixture_close(&fixture);

  if (fixture_open(&fixture, path, O_RDWR | O_APPEND) != 0)
    return -1;
  if (rc == 0 && kafs_v7_checkpoint_publish_fd(fixture.locks, fixture.fd, &fixture.superblock,
                                                fixture.file_size, &result) != -EBADF)
    rc = -1;
  if (rc == 0 && kafs_v7_metadata_closeout_fd(fixture.locks, fixture.fd, &fixture.superblock,
                                               fixture.file_size, &closeout) != -EBADF)
    rc = -1;
  fixture_close(&fixture);

  if (fixture_open(&fixture, path, O_RDWR) != 0)
    return -1;
  int transaction_locked = 0;
  if (rc == 0)
  {
    rc = kafs_v7_transaction_lock(fixture.locks, 0u);
    transaction_locked = rc == 0;
  }
  if (rc == 0 && kafs_v7_checkpoint_publish_fd(fixture.locks, fixture.fd, &fixture.superblock,
                                                fixture.file_size, &result) != -EDEADLK)
    rc = -1;
  if (transaction_locked && kafs_v7_transaction_unlock(fixture.locks, 0u) != 0)
    rc = -1;
  fixture_close(&fixture);
  return rc;
}

static int test_metadata_closeout_commit(void)
{
  const char *path = "v7-closeout-commit.img";
  if (format_image(path) != 0)
    return -1;
  checkpoint_fixture_t fixture;
  if (fixture_open(&fixture, path, O_RDWR) != 0)
    return -1;
  uint16_t before = 0u;
  int rc = read_inode_uid(&fixture, 1u, &before);
  uint16_t after = (uint16_t)(before ^ 1u);
  if (rc == 0)
    rc = publish_inode_patch(&fixture, 1u, after, KAFS_V7_JOURNAL_COMMIT_TAG);
  uint16_t raw = 0u;
  if (rc == 0)
    rc = read_inode_uid(&fixture, 1u, &raw);
  if (rc == 0 && (raw != before || fixture.layout.journal.replay_mutation_count != 1u ||
                  fixture.layout.checkpoint_sequence != 0u))
    rc = -1;
  kafs_v7_metadata_closeout_result_t result;
  if (rc == 0)
    rc = kafs_v7_metadata_closeout_fd(fixture.locks, fixture.fd, &fixture.superblock,
                                      fixture.file_size, &result);
  if (rc == 0 &&
      (result.apply.written_target_count != 1u || result.apply.applied_mutation_count != 1u ||
       result.checkpoint_publication_count != 1u || result.checkpoint_resume_count != 0u ||
       result.final_checkpoint_sequence != 1u || result.reclaim.reset_segment_count != 1u))
    rc = -1;
  if (rc == 0)
    rc = fixture_refresh(&fixture);
  if (rc == 0)
    rc = read_inode_uid(&fixture, 1u, &raw);
  if (rc == 0 && (raw != after || fixture.layout.checkpoint_sequence != 1u ||
                  fixture.layout.journal.selected_nonempty_segment_count != 0u))
    rc = -1;
  uint64_t generation = fixture.layout.checkpoint_generation;
  if (rc == 0)
    rc = kafs_v7_metadata_closeout_fd(fixture.locks, fixture.fd, &fixture.superblock,
                                      fixture.file_size, &result);
  if (rc == 0 && (result.checkpoint_publication_count != 0u ||
                  result.reclaim.reset_segment_count != 0u ||
                  result.reclaim.already_empty_segment_count != fixture.layout.journal_segment_count ||
                  result.final_checkpoint_generation != generation))
    rc = -1;
  fixture_close(&fixture);
  return rc;
}

static int test_metadata_closeout_abort(void)
{
  const char *path = "v7-closeout-abort.img";
  if (format_image(path) != 0)
    return -1;
  checkpoint_fixture_t fixture;
  if (fixture_open(&fixture, path, O_RDWR) != 0)
    return -1;
  uint64_t inode = 0u;
  int rc = find_inode_in_group(&fixture, 3u, &inode);
  if (rc == 0)
    rc = publish_inode_patch(&fixture, inode, 1u, KAFS_V7_JOURNAL_ABORT_TAG);
  kafs_v7_metadata_closeout_result_t result;
  if (rc == 0)
    rc = kafs_v7_metadata_closeout_fd(fixture.locks, fixture.fd, &fixture.superblock,
                                      fixture.file_size, &result);
  uint16_t uid = UINT16_MAX;
  if (rc == 0)
    rc = fixture_refresh(&fixture);
  if (rc == 0)
    rc = read_inode_uid(&fixture, inode, &uid);
  if (rc == 0 && (uid != 0u || result.apply.written_target_count != 0u ||
                  result.apply.applied_mutation_count != 0u ||
                  result.final_checkpoint_sequence != 1u ||
                  fixture.layout.journal.selected_nonempty_segment_count != 0u))
    rc = -1;
  fixture_close(&fixture);
  return rc;
}

static int test_metadata_apply_resume(void)
{
  const char *path = "v7-closeout-apply-resume.img";
  if (format_image(path) != 0)
    return -1;
  checkpoint_fixture_t fixture;
  if (fixture_open(&fixture, path, O_RDWR) != 0)
    return -1;
  uint16_t before = 0u;
  int rc = read_inode_uid(&fixture, 1u, &before);
  if (rc == 0)
    rc = publish_inode_patch(&fixture, 1u, (uint16_t)(before ^ 1u),
                             KAFS_V7_JOURNAL_COMMIT_TAG);
  kafs_v7_journal_apply_result_t apply;
  if (rc == 0)
    rc = apply_metadata_only(&fixture, &apply);
  if (rc == 0 && (apply.written_target_count != 1u ||
                  fixture.layout.journal.already_applied_mutation_count != 1u ||
                  fixture.layout.journal.replay_mutation_count != 0u ||
                  fixture.layout.checkpoint_sequence != 0u))
    rc = -1;
  kafs_v7_metadata_closeout_result_t result;
  if (rc == 0)
    rc = kafs_v7_metadata_closeout_fd(fixture.locks, fixture.fd, &fixture.superblock,
                                      fixture.file_size, &result);
  if (rc == 0 && (result.apply.written_target_count != 0u ||
                  result.apply.already_applied_mutation_count != 1u ||
                  result.final_checkpoint_sequence != 1u || result.reclaim.reset_segment_count != 1u))
    rc = -1;
  fixture_close(&fixture);
  return rc;
}

static int test_checkpoint_copy_resume_before_reclaim(void)
{
  const char *path = "v7-closeout-checkpoint-resume.img";
  if (format_image(path) != 0)
    return -1;
  checkpoint_fixture_t fixture;
  if (fixture_open(&fixture, path, O_RDWR) != 0)
    return -1;
  uint16_t before = 0u;
  int rc = read_inode_uid(&fixture, 1u, &before);
  if (rc == 0)
    rc = publish_inode_patch(&fixture, 1u, (uint16_t)(before ^ 1u),
                             KAFS_V7_JOURNAL_COMMIT_TAG);
  kafs_v7_journal_apply_result_t apply;
  if (rc == 0)
    rc = apply_metadata_only(&fixture, &apply);
  kafs_v7_checkpoint_plan_t plan;
  if (rc == 0)
    rc = kafs_v7_checkpoint_plan(&fixture.layout, &plan);
  if (rc == 0)
    rc = write_first_planned_copy(&fixture, &plan);
  fixture_close(&fixture);
  if (rc != 0 || fixture_open(&fixture, path, O_RDWR) != 0)
    return -1;
  if (fixture.layout.checkpoint_generation != 2u || selected_generation_copies(&fixture.layout) != 1u)
    rc = -1;
  kafs_v7_metadata_closeout_result_t result;
  if (rc == 0)
    rc = kafs_v7_metadata_closeout_fd(fixture.locks, fixture.fd, &fixture.superblock,
                                      fixture.file_size, &result);
  if (rc == 0 && (result.checkpoint_publication_count != 1u ||
                  result.checkpoint_resume_count != 1u ||
                  result.last_checkpoint_publication.generation != 2u ||
                  result.final_checkpoint_sequence != 1u || result.reclaim.reset_segment_count != 1u))
    rc = -1;
  fixture_close(&fixture);
  return rc;
}

static int test_reclaim_guards_and_partial_resume(void)
{
  const char *path = "v7-closeout-reclaim-resume.img";
  if (format_image(path) != 0)
    return -1;
  checkpoint_fixture_t fixture;
  if (fixture_open(&fixture, path, O_RDWR) != 0)
    return -1;
  uint16_t before = 0u;
  int rc = read_inode_uid(&fixture, 1u, &before);
  if (rc == 0)
    rc = publish_inode_patch(&fixture, 1u, (uint16_t)(before ^ 1u),
                             KAFS_V7_JOURNAL_COMMIT_TAG);
  kafs_v7_journal_reclaim_result_t reclaim;
  if (rc == 0 && kafs_v7_journal_reclaim_fd(fixture.fd, &fixture.layout, &reclaim) != -EBUSY)
    rc = -1;
  if (rc == 0)
    rc = fixture_refresh(&fixture);
  uint64_t abort_inode = 0u;
  if (rc == 0)
    rc = find_inode_in_group(&fixture, 3u, &abort_inode);
  if (rc == 0)
    rc = publish_inode_patch(&fixture, abort_inode, 1u, KAFS_V7_JOURNAL_ABORT_TAG);
  kafs_v7_journal_apply_result_t apply;
  if (rc == 0)
    rc = apply_metadata_only(&fixture, &apply);
  kafs_v7_checkpoint_publish_result_t publication;
  if (rc == 0)
    rc = kafs_v7_checkpoint_publish_fd(fixture.locks, fixture.fd, &fixture.superblock,
                                       fixture.file_size, &publication);
  if (rc == 0)
    rc = fixture_refresh(&fixture);
  if (rc == 0 && (fixture.layout.checkpoint_sequence != 2u ||
                  fixture.layout.journal.selected_nonempty_segment_count != 2u))
    rc = -1;
  if (rc == 0)
    rc = reset_one_journal_segment(&fixture, 0u, 0u);
  if (rc == 0 && fixture.layout.journal.selected_nonempty_segment_count != 1u)
    rc = -1;
  kafs_v7_metadata_closeout_result_t result;
  if (rc == 0)
    rc = kafs_v7_metadata_closeout_fd(fixture.locks, fixture.fd, &fixture.superblock,
                                      fixture.file_size, &result);
  if (rc == 0 && (result.checkpoint_publication_count != 0u ||
                  result.reclaim.reset_segment_count != 1u ||
                  result.final_checkpoint_sequence != 2u))
    rc = -1;
  fixture_close(&fixture);
  return rc;
}

static int run_test(const char *name, int (*test)(void))
{
  int rc = test();
  if (rc != 0)
    fprintf(stderr, "%s failed: %d\n", name, rc);
  return rc;
}

int main(void)
{
  if (kafs_test_enter_tmpdir("v7-checkpoint") != 0)
    return 1;
  int rc = run_test("two-copy publication", test_two_copy_publication);
  if (rc == 0)
    rc = run_test("interrupted publication resume", test_interrupted_publication_resume);
  if (rc == 0)
    rc = run_test("plan guards", test_plan_guards);
  if (rc == 0)
    rc = run_test("metadata closeout commit", test_metadata_closeout_commit);
  if (rc == 0)
    rc = run_test("metadata closeout abort", test_metadata_closeout_abort);
  if (rc == 0)
    rc = run_test("metadata apply resume", test_metadata_apply_resume);
  if (rc == 0)
    rc = run_test("checkpoint copy resume", test_checkpoint_copy_resume_before_reclaim);
  if (rc == 0)
    rc = run_test("reclaim guards and partial resume", test_reclaim_guards_and_partial_resume);
  if (rc != 0)
    fprintf(stderr, "v7 checkpoint publication smoke test failed: %d\n", rc);
  return rc == 0 ? 0 : 1;
}
