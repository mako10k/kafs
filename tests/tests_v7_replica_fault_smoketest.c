#include "test_utils.h"

#include "kafs_offline_summary.h"
#include "kafs_superblock.h"
#include "kafs_tool_util.h"
#include "kafs_v7_checkpoint.h"
#include "kafs_v7_layout.h"

#include <endian.h>
#include <errno.h>
#include <fcntl.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

#define BASE_IMAGE_BYTES (UINT64_C(512) * 1024u * 1024u)
#define THREE_COPY_IMAGE_BYTES (UINT64_C(1024) * 1024u * 1024u)
#define TEST_GROUP_COUNT 8u

typedef struct recovery_fixture
{
  uint64_t file_size;
  uint64_t descriptor_offsets[KAFS_V7_REPLICA_MAX_COUNT];
  uint64_t checkpoint_offsets[KAFS_V7_REPLICA_MAX_COUNT];
  uint32_t block_size;
  uint32_t descriptor_bytes;
  uint32_t replica_count;
  uint32_t group_count;
} recovery_fixture_t;

typedef struct loss_case
{
  uint32_t descriptor_mask;
  uint32_t checkpoint_mask;
  uint32_t selected_descriptor;
  uint32_t selected_checkpoint;
} loss_case_t;

static int run_command(char *const argv[], int expected_exit)
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
  return WEXITSTATUS(status) == expected_exit ? 0 : -1;
}

static int format_base_image(const char *path)
{
  char *argv[] = {(char *)kafs_test_mkfs_bin(),
                  (char *)path,
                  (char *)"--format-version",
                  (char *)"7",
                  (char *)"--size-bytes",
                  (char *)"512M",
                  (char *)"--v7-group-count",
                  (char *)"8",
                  (char *)"--yes",
                  NULL};
  return run_command(argv, 0);
}

static int load_superblock_and_size(int fd, kafs_ssuperblock_t *sb, uint64_t *file_size)
{
  int rc = kafs_pread_all(fd, sb, sizeof(*sb), 0);
  if (rc == 0)
    rc = kafs_offline_detect_file_size(fd, file_size);
  return rc;
}

static int validate_image(const char *path, kafs_v7_layout_report_t *report)
{
  int fd = open(path, O_RDONLY);
  if (fd < 0)
    return -errno;
  kafs_ssuperblock_t sb;
  uint64_t file_size = 0;
  int rc = load_superblock_and_size(fd, &sb, &file_size);
  if (rc == 0)
    rc = kafs_v7_validate_image_fd(fd, &sb, file_size, report);
  close(fd);
  return rc;
}

static int load_fixture(const char *path, recovery_fixture_t *fixture)
{
  kafs_v7_layout_report_t report;
  int rc = validate_image(path, &report);
  if (rc != 0)
    return rc;
  memset(fixture, 0, sizeof(*fixture));
  fixture->block_size = report.block_size;
  fixture->descriptor_bytes = report.descriptor_bytes;
  fixture->replica_count = report.replica_count;
  fixture->group_count = report.group_count;
  for (uint32_t id = 0; id < report.replica_count; ++id)
  {
    fixture->descriptor_offsets[id] = report.descriptors[id].offset;
    fixture->checkpoint_offsets[id] = report.checkpoints[id].offset;
  }
  int fd = open(path, O_RDONLY);
  kafs_ssuperblock_t sb;
  rc = fd < 0 ? -errno : load_superblock_and_size(fd, &sb, &fixture->file_size);
  if (fd >= 0)
    close(fd);
  kafs_v7_layout_report_clear(&report);
  return rc;
}

static int expect_valid(const char *path, uint32_t descriptor_id, uint32_t checkpoint_id,
                        int degraded)
{
  kafs_v7_layout_report_t report;
  int rc = validate_image(path, &report);
  if (rc != 0)
    return -1;
  int matches = report.selected_replica == descriptor_id &&
                report.selected_checkpoint == checkpoint_id && report.group_count == TEST_GROUP_COUNT &&
                report.degraded == degraded;
  kafs_v7_layout_report_clear(&report);
  return matches ? 0 : -1;
}

static int expect_invalid(const char *path)
{
  kafs_v7_layout_report_t report;
  memset(&report, 0, sizeof(report));
  int rc = validate_image(path, &report);
  kafs_v7_layout_report_clear(&report);
  return rc == 0 ? -1 : 0;
}

static int expect_divergent(const char *path, int descriptor)
{
  kafs_v7_layout_report_t report;
  memset(&report, 0, sizeof(report));
  int rc = validate_image(path, &report);
  int matches = rc == -EUCLEAN &&
                (descriptor ? report.descriptor_divergent : report.checkpoint_divergent);
  kafs_v7_layout_report_clear(&report);
  return matches ? 0 : -1;
}

static int zero_range(int fd, uint64_t off, size_t bytes)
{
  void *zero = calloc(1u, bytes);
  if (!zero)
    return -ENOMEM;
  int rc = kafs_pwrite_all(fd, zero, bytes, (off_t)off);
  free(zero);
  return rc;
}

static void update_locator_crc(kafs_v7_root_locator_t *locator)
{
  locator->anchor_crc32 = 0;
  locator->anchor_crc32 = htole32(kafs_v7_crc32(locator, sizeof(*locator)));
}

static void update_descriptor_crc(void *descriptor)
{
  kafs_v7_layout_header_t *header = (kafs_v7_layout_header_t *)descriptor;
  header->descriptor_crc32 = 0;
  header->descriptor_crc32 =
      htole32(kafs_v7_crc32(descriptor, le32toh(header->descriptor_bytes)));
}

static void update_checkpoint_crc(kafs_v7_checkpoint_t *checkpoint)
{
  checkpoint->crc32 = 0;
  checkpoint->crc32 = htole32(kafs_v7_crc32(checkpoint, sizeof(*checkpoint)));
}

static void update_journal_header_crc(kafs_v7_journal_header_t *header)
{
  header->crc32 = 0;
  header->crc32 = htole32(kafs_v7_crc32(header, sizeof(*header)));
}

static void set_recovery_shard(kafs_v7_shard_desc_t *shard, uint16_t type,
                               uint16_t storage_class, uint64_t physical_off,
                               uint64_t physical_bytes, uint64_t replica_id,
                               uint32_t record_bytes)
{
  memset(shard, 0, sizeof(*shard));
  shard->type = htole16(type);
  shard->storage_class = htole16(storage_class);
  shard->group_id = htole32(UINT32_MAX);
  shard->physical_off = htole64(physical_off);
  shard->physical_bytes = htole64(physical_bytes);
  shard->logical_start = htole64(replica_id);
  shard->logical_count = htole64(1u);
  shard->record_bytes = htole32(record_bytes);
}

static int build_three_copy_fixture(const char *path)
{
  if (format_base_image(path) != 0)
    return -1;
  kafs_v7_layout_report_t report;
  if (validate_image(path, &report) != 0)
    return -1;
  if (report.replica_count != 2u || report.group_count != TEST_GROUP_COUNT)
  {
    kafs_v7_layout_report_clear(&report);
    return -1;
  }

  uint32_t block_size = report.block_size;
  uint32_t descriptor_bytes = report.descriptor_bytes;
  uint32_t recovery_start = report.group_count * KAFS_V7_GROUP_LOCAL_SHARDS;
  uint64_t old_tail_descriptor = report.descriptors[1].offset;
  uint64_t old_tail_checkpoint = report.checkpoints[1].offset;
  uint64_t primary_descriptor = report.descriptors[0].offset;
  uint64_t primary_checkpoint = report.checkpoints[0].offset;
  void *descriptor = malloc(descriptor_bytes);
  void *checkpoint_block = malloc(block_size);
  if (!descriptor || !checkpoint_block)
  {
    free(descriptor);
    free(checkpoint_block);
    kafs_v7_layout_report_clear(&report);
    return -ENOMEM;
  }
  memcpy(descriptor, report.descriptor, descriptor_bytes);

  int fd = open(path, O_RDWR);
  int rc = fd < 0 ? -errno
                  : kafs_pread_all(fd, checkpoint_block, block_size,
                                   (off_t)primary_checkpoint);
  if (rc == 0 && ftruncate(fd, (off_t)THREE_COPY_IMAGE_BYTES) != 0)
    rc = -errno;

  uint64_t tail_descriptor = THREE_COPY_IMAGE_BYTES - block_size - descriptor_bytes;
  uint64_t tail_checkpoint = tail_descriptor - block_size;
  uint64_t midpoint_descriptor = THREE_COPY_IMAGE_BYTES / 2u;
  uint64_t midpoint_checkpoint = midpoint_descriptor + descriptor_bytes;
  kafs_v7_layout_header_t *header = (kafs_v7_layout_header_t *)descriptor;
  kafs_v7_shard_desc_t *shards = (kafs_v7_shard_desc_t *)(
      (uint8_t *)descriptor + le32toh(header->shard_desc_off));
  kafs_v7_replica_desc_t *replicas = (kafs_v7_replica_desc_t *)(
      (uint8_t *)descriptor + le32toh(header->replica_desc_off));
  header->image_size_bytes = htole64(THREE_COPY_IMAGE_BYTES);
  header->shard_count = htole32(le32toh(header->shard_count) + 2u);
  header->replica_count = htole32(3u);
  shards[recovery_start + 2u].physical_off = htole64(tail_descriptor);
  shards[recovery_start + 3u].physical_off = htole64(tail_checkpoint);
  set_recovery_shard(&shards[recovery_start + 4u], KAFS_V7_SHARD_LAYOUT_DESCRIPTOR,
                     KAFS_V7_STORAGE_BYTE_SPAN, midpoint_descriptor, descriptor_bytes, 2u, 0u);
  set_recovery_shard(&shards[recovery_start + 5u], KAFS_V7_SHARD_SUPERBLOCK_CHECKPOINT,
                     KAFS_V7_STORAGE_FIXED_RECORD, midpoint_checkpoint, block_size, 2u,
                     KAFS_V7_CHECKPOINT_BYTES);
  replicas[1].physical_off = htole64(tail_descriptor);
  memset(&replicas[2], 0, sizeof(replicas[2]));
  replicas[2].replica_id = htole32(2u);
  replicas[2].role = htole16(KAFS_V7_REPLICA_MIDPOINT);
  replicas[2].physical_off = htole64(midpoint_descriptor);
  replicas[2].descriptor_bytes = htole32(descriptor_bytes);
  update_descriptor_crc(descriptor);

  kafs_ssuperblock_t sb;
  if (rc == 0)
    rc = kafs_pread_all(fd, &sb, sizeof(sb), 0);
  kafs_v7_root_locator_t locator;
  if (rc == 0)
  {
    memcpy(&locator, sb.s_reserved, sizeof(locator));
    locator.candidate_count = htole32(3u);
    update_locator_crc(&locator);
    memcpy(sb.s_reserved, &locator, sizeof(locator));
    sb.s_blkcnt = kafs_blkcnt_htos(THREE_COPY_IMAGE_BYTES / block_size);
  }
  if (rc == 0)
    rc = zero_range(fd, old_tail_checkpoint, block_size);
  if (rc == 0)
    rc = zero_range(fd, old_tail_descriptor, descriptor_bytes);
  if (rc == 0)
    rc = zero_range(fd, BASE_IMAGE_BYTES - block_size, block_size);
  if (rc == 0)
    rc = zero_range(fd, THREE_COPY_IMAGE_BYTES - block_size, block_size);
  if (rc == 0)
    rc = kafs_pwrite_all(fd, descriptor, descriptor_bytes, (off_t)primary_descriptor);
  if (rc == 0)
    rc = kafs_pwrite_all(fd, descriptor, descriptor_bytes, (off_t)tail_descriptor);
  if (rc == 0)
    rc = kafs_pwrite_all(fd, descriptor, descriptor_bytes, (off_t)midpoint_descriptor);
  if (rc == 0)
    rc = kafs_pwrite_all(fd, checkpoint_block, block_size, (off_t)tail_checkpoint);
  if (rc == 0)
    rc = kafs_pwrite_all(fd, checkpoint_block, block_size, (off_t)midpoint_checkpoint);
  if (rc == 0)
    rc = kafs_pwrite_all(fd, &locator, sizeof(locator),
                         (off_t)(THREE_COPY_IMAGE_BYTES - sizeof(locator)));
  if (rc == 0)
    rc = kafs_pwrite_all(fd, &sb, sizeof(sb), 0);
  if (rc == 0 && fsync(fd) != 0)
    rc = -errno;

  if (fd >= 0)
    close(fd);
  free(descriptor);
  free(checkpoint_block);
  kafs_v7_layout_report_clear(&report);
  return rc;
}

static int damage_recovery_copies(const char *path, const recovery_fixture_t *fixture,
                                  uint32_t descriptor_mask, uint32_t checkpoint_mask)
{
  int fd = open(path, O_RDWR);
  if (fd < 0)
    return -errno;
  int rc = 0;
  for (uint32_t id = 0; rc == 0 && id < fixture->replica_count; ++id)
  {
    if ((descriptor_mask & (1u << id)) != 0)
      rc = zero_range(fd, fixture->descriptor_offsets[id], fixture->descriptor_bytes);
    if (rc == 0 && (checkpoint_mask & (1u << id)) != 0)
      rc = zero_range(fd, fixture->checkpoint_offsets[id], fixture->block_size);
  }
  if (rc == 0 && fsync(fd) != 0)
    rc = -errno;
  close(fd);
  return rc;
}

static int update_descriptor_copies(const char *path, const recovery_fixture_t *fixture,
                                    uint32_t mask, uint64_t generation, int divergent)
{
  void *descriptor = malloc(fixture->descriptor_bytes);
  if (!descriptor)
    return -ENOMEM;
  int fd = open(path, O_RDWR);
  int rc = fd < 0 ? -errno : 0;
  for (uint32_t id = 0; rc == 0 && id < fixture->replica_count; ++id)
  {
    if ((mask & (1u << id)) == 0)
      continue;
    rc = kafs_pread_all(fd, descriptor, fixture->descriptor_bytes,
                        (off_t)fixture->descriptor_offsets[id]);
    if (rc != 0)
      break;
    kafs_v7_layout_header_t *header = (kafs_v7_layout_header_t *)descriptor;
    header->generation = htole64(generation);
    if (divergent)
    {
      kafs_v7_group_desc_t *groups = (kafs_v7_group_desc_t *)(
          (uint8_t *)descriptor + le32toh(header->group_desc_off));
      kafs_v7_shard_desc_t *shards = (kafs_v7_shard_desc_t *)(
          (uint8_t *)descriptor + le32toh(header->shard_desc_off));
      uint32_t last_group = le32toh(header->group_count) - 1u;
      uint32_t journal_data = le32toh(groups[last_group].first_shard_index) + 6u;
      uint64_t shrink = 2u * (uint64_t)fixture->block_size;
      shards[journal_data].physical_bytes =
          htole64(le64toh(shards[journal_data].physical_bytes) - shrink);
      groups[last_group].metadata_physical_bytes =
          htole64(le64toh(groups[last_group].metadata_physical_bytes) - shrink);
      groups[last_group].data_physical_off =
          htole64(le64toh(groups[last_group].data_physical_off) - shrink);
    }
    update_descriptor_crc(descriptor);
    rc = kafs_pwrite_all(fd, descriptor, fixture->descriptor_bytes,
                         (off_t)fixture->descriptor_offsets[id]);
  }
  if (fd >= 0)
    close(fd);
  free(descriptor);
  return rc;
}

static int update_variant_journal_header(const char *path, const recovery_fixture_t *fixture,
                                         uint32_t descriptor_id)
{
  void *descriptor = malloc(fixture->descriptor_bytes);
  if (!descriptor)
    return -ENOMEM;
  int fd = open(path, O_RDWR);
  int rc = fd < 0 ? -errno
                  : kafs_pread_all(fd, descriptor, fixture->descriptor_bytes,
                                   (off_t)fixture->descriptor_offsets[descriptor_id]);
  kafs_v7_journal_header_t journal_header;
  uint64_t header_off = 0;
  uint64_t data_bytes = 0;
  if (rc == 0)
  {
    kafs_v7_layout_header_t *header = (kafs_v7_layout_header_t *)descriptor;
    kafs_v7_group_desc_t *groups = (kafs_v7_group_desc_t *)(
        (uint8_t *)descriptor + le32toh(header->group_desc_off));
    kafs_v7_shard_desc_t *shards = (kafs_v7_shard_desc_t *)(
        (uint8_t *)descriptor + le32toh(header->shard_desc_off));
    uint32_t last_group = le32toh(header->group_count) - 1u;
    uint32_t first_shard = le32toh(groups[last_group].first_shard_index);
    header_off = le64toh(shards[first_shard + 5u].physical_off);
    data_bytes = le64toh(shards[first_shard + 6u].physical_bytes) /
                 le64toh(shards[first_shard + 6u].logical_count);
    rc = kafs_pread_all(fd, &journal_header, sizeof(journal_header), (off_t)header_off);
  }
  if (rc == 0)
  {
    journal_header.data_bytes = htole64(data_bytes);
    update_journal_header_crc(&journal_header);
    rc = kafs_pwrite_all(fd, &journal_header, sizeof(journal_header), (off_t)header_off);
  }
  if (fd >= 0)
    close(fd);
  free(descriptor);
  return rc;
}

static int update_checkpoint_copies(const char *path, const recovery_fixture_t *fixture,
                                    uint32_t mask, uint64_t generation,
                                    uint64_t descriptor_generation, int decrement_free_blocks)
{
  int fd = open(path, O_RDWR);
  int rc = fd < 0 ? -errno : 0;
  for (uint32_t id = 0; rc == 0 && id < fixture->replica_count; ++id)
  {
    if ((mask & (1u << id)) == 0)
      continue;
    kafs_v7_checkpoint_t checkpoint;
    rc = kafs_pread_all(fd, &checkpoint, sizeof(checkpoint),
                        (off_t)fixture->checkpoint_offsets[id]);
    if (rc != 0)
      break;
    checkpoint.generation = htole64(generation);
    checkpoint.descriptor_generation = htole64(descriptor_generation);
    if (decrement_free_blocks)
      checkpoint.free_blocks = htole64(le64toh(checkpoint.free_blocks) - 1u);
    update_checkpoint_crc(&checkpoint);
    rc = kafs_pwrite_all(fd, &checkpoint, sizeof(checkpoint),
                         (off_t)fixture->checkpoint_offsets[id]);
  }
  if (fd >= 0)
    close(fd);
  return rc;
}

static int check_placement(const char *path, uint32_t expected_replicas, uint64_t midpoint)
{
  kafs_v7_layout_report_t report;
  if (validate_image(path, &report) != 0)
    return -1;
  if (report.replica_count != expected_replicas || report.group_count != TEST_GROUP_COUNT)
  {
    kafs_v7_layout_report_clear(&report);
    return -1;
  }
  int fd = open(path, O_RDONLY);
  uint64_t file_size = 0;
  if (fd < 0 || kafs_offline_detect_file_size(fd, &file_size) != 0)
  {
    if (fd >= 0)
      close(fd);
    kafs_v7_layout_report_clear(&report);
    return -1;
  }
  close(fd);
  const kafs_v7_group_desc_t *groups = kafs_v7_report_groups(&report);
  uint64_t first_group = le64toh(groups[0].metadata_physical_off);
  uint64_t last_data_end = le64toh(groups[report.group_count - 1u].data_physical_off) +
                           le64toh(groups[report.group_count - 1u].data_physical_bytes);
  uint64_t descriptor_span = report.descriptors[1].offset + report.descriptor_bytes -
                             report.descriptors[0].offset;
  int valid = report.descriptors[0].offset + report.descriptor_bytes ==
                  report.checkpoints[0].offset &&
              report.checkpoints[0].offset + report.block_size <= first_group &&
              report.checkpoints[1].offset + report.block_size == report.descriptors[1].offset &&
              last_data_end <= report.checkpoints[1].offset &&
              descriptor_span >= file_size * 95u / 100u;
  if (expected_replicas == 3u)
    valid = valid && report.descriptors[2].offset == midpoint &&
            report.descriptors[2].offset + report.descriptor_bytes ==
                report.checkpoints[2].offset &&
            last_data_end <= report.descriptors[2].offset &&
            report.checkpoints[2].offset + report.block_size <= report.checkpoints[1].offset;
  kafs_v7_layout_report_clear(&report);
  return valid ? 0 : -1;
}

static int run_loss_case(const char *path, int three_copy, const loss_case_t *test)
{
  int rc = three_copy ? build_three_copy_fixture(path) : format_base_image(path);
  recovery_fixture_t fixture;
  if (rc == 0)
    rc = load_fixture(path, &fixture);
  if (rc == 0)
    rc = damage_recovery_copies(path, &fixture, test->descriptor_mask, test->checkpoint_mask);
  if (rc == 0)
    rc = expect_valid(path, test->selected_descriptor, test->selected_checkpoint, 1);
  return rc;
}

static int test_two_copy_fault_matrix(void)
{
  const loss_case_t losses[] = {
      {1u, 0u, 1u, 0u}, {0u, 1u, 0u, 1u}, {2u, 0u, 0u, 0u}, {0u, 2u, 0u, 0u},
      {1u, 1u, 1u, 1u}, {2u, 2u, 0u, 0u}, {1u, 2u, 1u, 0u}, {2u, 1u, 0u, 1u}};
  for (size_t i = 0; i < sizeof(losses) / sizeof(losses[0]); ++i)
  {
    char path[64];
    snprintf(path, sizeof(path), "v7-two-loss-%zu.img", i);
    if (run_loss_case(path, 0, &losses[i]) != 0)
      return -1;
  }

  const char *all_descriptors = "v7-two-loss-all-descriptors.img";
  recovery_fixture_t fixture;
  if (format_base_image(all_descriptors) != 0 || load_fixture(all_descriptors, &fixture) != 0 ||
      damage_recovery_copies(all_descriptors, &fixture, 3u, 0u) != 0 ||
      expect_invalid(all_descriptors) != 0)
    return -1;
  const char *all_checkpoints = "v7-two-loss-all-checkpoints.img";
  if (format_base_image(all_checkpoints) != 0 || load_fixture(all_checkpoints, &fixture) != 0 ||
      damage_recovery_copies(all_checkpoints, &fixture, 0u, 3u) != 0 ||
      expect_invalid(all_checkpoints) != 0)
    return -1;
  return 0;
}

static int test_two_copy_generation_matrix(void)
{
  recovery_fixture_t fixture;
  const char *checkpoint_new = "v7-two-checkpoint-new.img";
  if (format_base_image(checkpoint_new) != 0 || load_fixture(checkpoint_new, &fixture) != 0 ||
      update_checkpoint_copies(checkpoint_new, &fixture, 2u, 2u, 1u, 0) != 0 ||
      expect_valid(checkpoint_new, 0u, 1u, 1) != 0)
    return -1;

  const char *descriptor_incomplete = "v7-two-descriptor-incomplete.img";
  if (format_base_image(descriptor_incomplete) != 0 ||
      load_fixture(descriptor_incomplete, &fixture) != 0 ||
      update_descriptor_copies(descriptor_incomplete, &fixture, 2u, 2u, 0) != 0 ||
      expect_invalid(descriptor_incomplete) != 0)
    return -1;

  const char *pair_new = "v7-two-pair-new.img";
  if (format_base_image(pair_new) != 0 || load_fixture(pair_new, &fixture) != 0 ||
      update_descriptor_copies(pair_new, &fixture, 2u, 2u, 0) != 0 ||
      update_checkpoint_copies(pair_new, &fixture, 2u, 2u, 2u, 0) != 0 ||
      expect_valid(pair_new, 1u, 1u, 1) != 0)
    return -1;

  const char *descriptor_divergent = "v7-two-descriptor-divergent.img";
  if (format_base_image(descriptor_divergent) != 0 ||
      load_fixture(descriptor_divergent, &fixture) != 0 ||
      update_descriptor_copies(descriptor_divergent, &fixture, 2u, 1u, 1) != 0 ||
      expect_divergent(descriptor_divergent, 1) != 0)
    return -1;

  const char *descriptor_variant = "v7-two-descriptor-variant.img";
  if (format_base_image(descriptor_variant) != 0 || load_fixture(descriptor_variant, &fixture) != 0 ||
      update_descriptor_copies(descriptor_variant, &fixture, 2u, 1u, 1) != 0 ||
      update_variant_journal_header(descriptor_variant, &fixture, 1u) != 0 ||
      damage_recovery_copies(descriptor_variant, &fixture, 1u, 0u) != 0 ||
      expect_valid(descriptor_variant, 1u, 0u, 1) != 0)
    return -1;

  const char *checkpoint_divergent = "v7-two-checkpoint-divergent.img";
  if (format_base_image(checkpoint_divergent) != 0 ||
      load_fixture(checkpoint_divergent, &fixture) != 0 ||
      update_checkpoint_copies(checkpoint_divergent, &fixture, 2u, 1u, 1u, 1) != 0 ||
      expect_divergent(checkpoint_divergent, 0) != 0)
    return -1;
  return 0;
}

static int test_three_copy_fault_matrix(void)
{
  const loss_case_t losses[] = {
      {1u, 0u, 1u, 0u}, {0u, 1u, 0u, 1u}, {4u, 0u, 0u, 0u}, {0u, 4u, 0u, 0u},
      {3u, 0u, 2u, 0u}, {0u, 6u, 0u, 0u}, {1u, 1u, 1u, 1u}, {2u, 2u, 0u, 0u},
      {4u, 4u, 0u, 0u}, {3u, 3u, 2u, 2u}, {5u, 5u, 1u, 1u}, {6u, 6u, 0u, 0u},
      {3u, 6u, 2u, 0u}};
  for (size_t i = 0; i < sizeof(losses) / sizeof(losses[0]); ++i)
  {
    char path[64];
    snprintf(path, sizeof(path), "v7-three-loss-%zu.img", i);
    if (run_loss_case(path, 1, &losses[i]) != 0)
      return -1;
  }

  const char *all_descriptors = "v7-three-loss-all-descriptors.img";
  recovery_fixture_t fixture;
  if (build_three_copy_fixture(all_descriptors) != 0 ||
      load_fixture(all_descriptors, &fixture) != 0 ||
      damage_recovery_copies(all_descriptors, &fixture, 7u, 0u) != 0 ||
      expect_invalid(all_descriptors) != 0)
    return -1;
  const char *all_checkpoints = "v7-three-loss-all-checkpoints.img";
  if (build_three_copy_fixture(all_checkpoints) != 0 ||
      load_fixture(all_checkpoints, &fixture) != 0 ||
      damage_recovery_copies(all_checkpoints, &fixture, 0u, 7u) != 0 ||
      expect_invalid(all_checkpoints) != 0)
    return -1;
  return 0;
}

static int test_three_copy_generation_matrix(void)
{
  recovery_fixture_t fixture;
  const char *checkpoint_one = "v7-three-checkpoint-one-new.img";
  if (build_three_copy_fixture(checkpoint_one) != 0 || load_fixture(checkpoint_one, &fixture) != 0 ||
      update_checkpoint_copies(checkpoint_one, &fixture, 4u, 2u, 1u, 0) != 0 ||
      expect_valid(checkpoint_one, 0u, 2u, 1) != 0)
    return -1;

  const char *checkpoint_two = "v7-three-checkpoint-two-new.img";
  if (build_three_copy_fixture(checkpoint_two) != 0 || load_fixture(checkpoint_two, &fixture) != 0 ||
      update_checkpoint_copies(checkpoint_two, &fixture, 6u, 2u, 1u, 0) != 0 ||
      expect_valid(checkpoint_two, 0u, 1u, 1) != 0)
    return -1;

  const char *pair_one = "v7-three-pair-one-new.img";
  if (build_three_copy_fixture(pair_one) != 0 || load_fixture(pair_one, &fixture) != 0 ||
      update_descriptor_copies(pair_one, &fixture, 4u, 2u, 0) != 0 ||
      update_checkpoint_copies(pair_one, &fixture, 4u, 2u, 2u, 0) != 0 ||
      expect_valid(pair_one, 2u, 2u, 1) != 0)
    return -1;

  const char *pair_two = "v7-three-pair-two-new.img";
  if (build_three_copy_fixture(pair_two) != 0 || load_fixture(pair_two, &fixture) != 0 ||
      update_descriptor_copies(pair_two, &fixture, 6u, 2u, 0) != 0 ||
      update_checkpoint_copies(pair_two, &fixture, 6u, 2u, 2u, 0) != 0 ||
      expect_valid(pair_two, 1u, 1u, 1) != 0)
    return -1;

  const char *descriptor_incomplete = "v7-three-descriptor-incomplete.img";
  if (build_three_copy_fixture(descriptor_incomplete) != 0 ||
      load_fixture(descriptor_incomplete, &fixture) != 0 ||
      update_descriptor_copies(descriptor_incomplete, &fixture, 4u, 2u, 0) != 0 ||
      expect_invalid(descriptor_incomplete) != 0)
    return -1;

  const char *descriptor_divergent = "v7-three-descriptor-divergent.img";
  if (build_three_copy_fixture(descriptor_divergent) != 0 ||
      load_fixture(descriptor_divergent, &fixture) != 0 ||
      update_descriptor_copies(descriptor_divergent, &fixture, 4u, 1u, 1) != 0 ||
      expect_divergent(descriptor_divergent, 1) != 0)
    return -1;

  const char *checkpoint_divergent = "v7-three-checkpoint-divergent.img";
  if (build_three_copy_fixture(checkpoint_divergent) != 0 ||
      load_fixture(checkpoint_divergent, &fixture) != 0 ||
      update_checkpoint_copies(checkpoint_divergent, &fixture, 4u, 1u, 1u, 1) != 0 ||
      expect_divergent(checkpoint_divergent, 0) != 0)
    return -1;
  return 0;
}

static int test_three_copy_checkpoint_rotation(const char *path)
{
  int fd = open(path, O_RDWR);
  if (fd < 0)
    return -errno;
  kafs_ssuperblock_t sb;
  uint64_t file_size = 0;
  kafs_v7_layout_report_t report;
  memset(&report, 0, sizeof(report));
  int rc = load_superblock_and_size(fd, &sb, &file_size);
  if (rc == 0)
    rc = kafs_v7_validate_image_fd(fd, &sb, file_size, &report);
  kafs_v7_checkpoint_plan_t plan;
  if (rc == 0)
    rc = kafs_v7_checkpoint_plan(&report, &plan);
  if (rc == 0 && (plan.target_count != 2u || plan.target_replicas[0] != 2u ||
                  plan.target_replicas[1] != 1u))
    rc = -1;
  kafs_v7_checkpoint_publish_result_t result;
  if (rc == 0)
    rc = kafs_v7_checkpoint_publish_fd(fd, &sb, file_size, &result);
  kafs_v7_layout_report_clear(&report);
  close(fd);
  if (rc != 0 || validate_image(path, &report) != 0)
    return -1;
  int valid = result.generation == 2u && result.verified_copy_count == 2u &&
              report.checkpoint_generation == 2u && report.selected_checkpoint == 1u &&
              report.checkpoints[0].status == KAFS_V7_REPLICA_STATUS_STALE &&
              report.checkpoints[1].status == KAFS_V7_REPLICA_STATUS_VALID &&
              report.checkpoints[2].status == KAFS_V7_REPLICA_STATUS_VALID;
  kafs_v7_layout_report_clear(&report);
  return valid ? 0 : -1;
}

int main(void)
{
  if (kafs_test_enter_tmpdir("v7-replica-fault") != 0)
    return 1;

  const char *two_copy = "v7-two-placement.img";
  if (format_base_image(two_copy) != 0 || check_placement(two_copy, 2u, 0u) != 0)
  {
    fprintf(stderr, "v7 two-copy recovery placement failed\n");
    return 1;
  }
  const char *three_copy = "v7-three-placement.img";
  if (build_three_copy_fixture(three_copy) != 0 ||
      check_placement(three_copy, 3u, THREE_COPY_IMAGE_BYTES / 2u) != 0 ||
      test_three_copy_checkpoint_rotation(three_copy) != 0)
  {
    fprintf(stderr, "v7 three-copy recovery placement failed\n");
    return 1;
  }
  if (test_two_copy_fault_matrix() != 0)
  {
    fprintf(stderr, "v7 two-copy localized fault matrix failed\n");
    return 1;
  }
  if (test_two_copy_generation_matrix() != 0)
  {
    fprintf(stderr, "v7 two-copy generation matrix failed\n");
    return 1;
  }
  if (test_three_copy_fault_matrix() != 0)
  {
    fprintf(stderr, "v7 three-copy localized fault matrix failed\n");
    return 1;
  }
  if (test_three_copy_generation_matrix() != 0)
  {
    fprintf(stderr, "v7 three-copy generation matrix failed\n");
    return 1;
  }
  return 0;
}
