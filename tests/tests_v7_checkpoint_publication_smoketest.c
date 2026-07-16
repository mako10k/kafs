#include "test_utils.h"

#include "kafs_offline_summary.h"
#include "kafs_tool_util.h"
#include "kafs_v7_checkpoint.h"

#include <endian.h>
#include <errno.h>
#include <fcntl.h>
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
  if (rc != 0)
  {
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
  kafs_v7_layout_report_clear(&fixture->layout);
  memset(fixture, 0, sizeof(*fixture));
  fixture->fd = -1;
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
    rc = kafs_v7_checkpoint_publish_fd(fixture.fd, &fixture.superblock, fixture.file_size,
                                       &result);
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
    rc = kafs_v7_checkpoint_publish_fd(fixture.fd, &fixture.superblock, fixture.file_size,
                                       &result);
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
  if (rc == 0 && kafs_v7_checkpoint_publish_fd(fixture.fd, &fixture.superblock,
                                                fixture.file_size, &result) != -EBADF)
    rc = -1;
  fixture_close(&fixture);

  if (fixture_open(&fixture, path, O_RDWR | O_APPEND) != 0)
    return -1;
  if (rc == 0 && kafs_v7_checkpoint_publish_fd(fixture.fd, &fixture.superblock,
                                                fixture.file_size, &result) != -EBADF)
    rc = -1;
  fixture_close(&fixture);
  return rc;
}

int main(void)
{
  if (kafs_test_enter_tmpdir("v7-checkpoint") != 0)
    return 1;
  int rc = test_two_copy_publication();
  if (rc == 0)
    rc = test_interrupted_publication_resume();
  if (rc == 0)
    rc = test_plan_guards();
  if (rc != 0)
    fprintf(stderr, "v7 checkpoint publication smoke test failed: %d\n", rc);
  return rc == 0 ? 0 : 1;
}
