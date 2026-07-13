#include "test_utils.h"

#include "kafs_offline_summary.h"
#include "kafs_superblock.h"
#include "kafs_tool_util.h"
#include "kafs_v7_layout.h"

#include <endian.h>
#include <errno.h>
#include <fcntl.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

#define TEST_IMAGE_BYTES (UINT64_C(64) * 1024u * 1024u)
#define TEST_MIDPOINT_IMAGE_BYTES (UINT64_C(128) * 1024u * 1024u)

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

static int format_image(const char *path, const char *size, int expected_exit)
{
  char *argv[] = {(char *)kafs_test_mkfs_bin(), (char *)path, (char *)"--format-version",
                  (char *)"7", (char *)"--size-bytes", (char *)size, (char *)"--yes", NULL};
  return run_command(argv, expected_exit);
}

static int load_superblock_and_size(int fd, kafs_ssuperblock_t *sb, uint64_t *file_size)
{
  int rc = kafs_pread_all(fd, sb, sizeof(*sb), 0);
  if (rc == 0)
    rc = kafs_offline_detect_file_size(fd, file_size);
  return rc;
}

static int validate_path(const char *path, int expect_valid, kafs_v7_layout_report_t *out_report)
{
  int fd = open(path, O_RDONLY);
  if (fd < 0)
    return -errno;
  kafs_ssuperblock_t sb;
  uint64_t file_size = 0;
  int rc = load_superblock_and_size(fd, &sb, &file_size);
  kafs_v7_layout_report_t report;
  if (rc == 0)
    rc = kafs_v7_validate_image_fd(fd, &sb, file_size, &report);
  close(fd);
  if ((rc == 0) != expect_valid)
  {
    if (rc == 0)
      kafs_v7_layout_report_clear(&report);
    return -1;
  }
  if (rc == 0 && out_report)
    *out_report = report;
  else if (rc == 0)
    kafs_v7_layout_report_clear(&report);
  return 0;
}

static int copy_bytes(int fd, uint64_t source, uint64_t destination, size_t bytes)
{
  void *buf = malloc(bytes);
  if (!buf)
    return -ENOMEM;
  int rc = kafs_pread_all(fd, buf, bytes, (off_t)source);
  if (rc == 0)
    rc = kafs_pwrite_all(fd, buf, bytes, (off_t)destination);
  free(buf);
  return rc;
}

static int flip_byte(int fd, uint64_t off)
{
  uint8_t byte;
  int rc = kafs_pread_all(fd, &byte, sizeof(byte), (off_t)off);
  if (rc == 0)
  {
    byte ^= 0x5au;
    rc = kafs_pwrite_all(fd, &byte, sizeof(byte), (off_t)off);
  }
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
  uint32_t bytes = le32toh(header->descriptor_bytes);
  header->descriptor_crc32 = 0;
  header->descriptor_crc32 = htole32(kafs_v7_crc32(descriptor, bytes));
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

static int restore_primary_locator(int fd, uint64_t file_size)
{
  kafs_v7_root_locator_t locator;
  int rc = kafs_pread_all(fd, &locator, sizeof(locator),
                          (off_t)(file_size - sizeof(locator)));
  if (rc == 0)
    rc = kafs_pwrite_all(fd, &locator, sizeof(locator),
                         (off_t)offsetof(kafs_ssuperblock_t, s_reserved));
  return rc;
}

static int test_round_trip_and_fallbacks(void)
{
  const char *path = "v7-roundtrip.img";
  if (format_image(path, "64M", 0) != 0)
    return -1;

  kafs_v7_layout_report_t report;
  if (validate_path(path, 1, &report) != 0)
    return -1;
  if (report.replica_count != 2u || report.group_count != 1u || report.shard_count != 11u ||
      report.journal_segment_count != 2u || report.degraded)
  {
    kafs_v7_layout_report_clear(&report);
    return -1;
  }
  uint64_t primary_descriptor = report.descriptors[0].offset;
  uint64_t tail_descriptor = report.descriptors[1].offset;
  uint64_t primary_checkpoint = report.checkpoints[0].offset;
  uint64_t tail_checkpoint = report.checkpoints[1].offset;
  uint32_t descriptor_bytes = report.descriptor_bytes;
  uint32_t block_size = report.block_size;
  kafs_v7_layout_report_clear(&report);

  char *dump_argv[] = {(char *)kafs_test_kafsdump_bin(), (char *)"--json", (char *)path, NULL};
  char *fsck_argv[] = {(char *)kafs_test_fsck_bin(), (char *)path, NULL};
  if (run_command(dump_argv, 0) != 0 || run_command(fsck_argv, 0) != 0)
    return -1;
  char json_command[2048];
  int written = snprintf(
      json_command, sizeof(json_command),
      "%s --json %s | python3 -c 'import json,sys; d=json.load(sys.stdin); "
      "assert all(k in d for k in [\"root_locators\",\"layout_descriptor\","
      "\"descriptor_replicas\",\"checkpoints\",\"groups\",\"shards\","
      "\"journal_segments\"])'",
      kafs_test_kafsdump_bin(), path);
  if (written < 0 || (size_t)written >= sizeof(json_command) || system(json_command) != 0)
    return -1;

  int fd = open(path, O_RDWR);
  if (fd < 0)
    return -errno;
  kafs_ssuperblock_t sb;
  uint64_t file_size = 0;
  if (load_superblock_and_size(fd, &sb, &file_size) != 0)
  {
    close(fd);
    return -1;
  }

  sb.s_blkcnt_free.value = htole32(UINT32_C(0x12345678));
  sb.s_inocnt_free.value = htole32(UINT32_C(0x11223344));
  sb.s_first_data_block.value = htole32(UINT32_C(0x55667788));
  sb.s_hrl_index_offset.value = htole64(UINT64_C(0x1020304050607080));
  sb.s_journal_offset.value = htole64(UINT64_C(0x8877665544332211));
  if (kafs_pwrite_all(fd, &sb, sizeof(sb), 0) != 0)
  {
    close(fd);
    return -1;
  }
  close(fd);
  if (validate_path(path, 1, NULL) != 0)
    return -1;

  fd = open(path, O_RDWR);
  if (fd < 0)
    return -errno;
  if (flip_byte(fd, offsetof(kafs_ssuperblock_t, s_reserved) + 8u) != 0)
  {
    close(fd);
    return -1;
  }
  close(fd);
  if (validate_path(path, 1, &report) != 0 || !report.degraded || report.primary_locator_valid)
    return -1;
  kafs_v7_layout_report_clear(&report);

  fd = open(path, O_RDWR);
  if (fd < 0)
    return -errno;
  if (restore_primary_locator(fd, file_size) != 0 || flip_byte(fd, primary_descriptor + 16u) != 0)
  {
    close(fd);
    return -1;
  }
  close(fd);
  if (validate_path(path, 1, &report) != 0 || !report.degraded || report.selected_replica != 1u)
    return -1;
  kafs_v7_layout_report_clear(&report);

  fd = open(path, O_RDWR);
  if (fd < 0)
    return -errno;
  if (copy_bytes(fd, tail_descriptor, primary_descriptor, descriptor_bytes) != 0 ||
      flip_byte(fd, primary_checkpoint + 24u) != 0)
  {
    close(fd);
    return -1;
  }
  close(fd);
  if (validate_path(path, 1, &report) != 0 || !report.degraded ||
      report.selected_checkpoint != 1u)
    return -1;
  kafs_v7_layout_report_clear(&report);

  fd = open(path, O_RDWR);
  if (fd < 0)
    return -errno;
  int rc = copy_bytes(fd, tail_checkpoint, primary_checkpoint, block_size);
  close(fd);
  return rc == 0 && validate_path(path, 1, NULL) == 0 ? 0 : -1;
}

enum descriptor_fault
{
  DESCRIPTOR_UNKNOWN_TYPE,
  DESCRIPTOR_MISSING_INCOMPAT,
  DESCRIPTOR_RESERVED_NONZERO,
  DESCRIPTOR_MAPPING_POLICY,
  DESCRIPTOR_MAPPING_SEED,
  DESCRIPTOR_OVERLAP,
  DESCRIPTOR_UNKNOWN_CLASS,
  DESCRIPTOR_UNKNOWN_FLAG,
  DESCRIPTOR_RANGE_OVERFLOW,
  DESCRIPTOR_TABLE_COUNT,
  DESCRIPTOR_CROSS_GROUP,
};

static int apply_descriptor_fault(const char *path, enum descriptor_fault fault, int primary_only)
{
  kafs_v7_layout_report_t report;
  if (validate_path(path, 1, &report) != 0)
    return -1;
  kafs_v7_layout_header_t *header = (kafs_v7_layout_header_t *)report.descriptor;
  kafs_v7_shard_desc_t *shards = (kafs_v7_shard_desc_t *)(
      (uint8_t *)report.descriptor + le32toh(header->shard_desc_off));
  switch (fault)
  {
  case DESCRIPTOR_UNKNOWN_TYPE:
    shards[0].type = htole16(UINT16_C(99));
    break;
  case DESCRIPTOR_MISSING_INCOMPAT:
    header->incompat_flags = htole64(KAFS_V7_REQUIRED_INCOMPAT_FLAGS & ~UINT64_C(1));
    break;
  case DESCRIPTOR_RESERVED_NONZERO:
    header->reserved2 = htole32(1u);
    break;
  case DESCRIPTOR_MAPPING_POLICY:
    header->mapping_policy = htole16(1u);
    break;
  case DESCRIPTOR_MAPPING_SEED:
    header->mapping_seed = htole64(1u);
    break;
  case DESCRIPTOR_OVERLAP:
    shards[1].physical_off = shards[0].physical_off;
    break;
  case DESCRIPTOR_UNKNOWN_CLASS:
    shards[0].storage_class = htole16(UINT16_C(99));
    break;
  case DESCRIPTOR_UNKNOWN_FLAG:
    shards[0].flags = htole32(1u);
    break;
  case DESCRIPTOR_RANGE_OVERFLOW:
    shards[0].physical_bytes = htole64(UINT64_MAX);
    break;
  case DESCRIPTOR_TABLE_COUNT:
    header->group_count = htole32(UINT32_MAX);
    break;
  case DESCRIPTOR_CROSS_GROUP:
    shards[4].group_id = htole32(1u);
    break;
  }
  update_descriptor_crc(report.descriptor);

  int fd = open(path, O_RDWR);
  if (fd < 0)
  {
    kafs_v7_layout_report_clear(&report);
    return -errno;
  }
  uint32_t copies = primary_only ? 1u : report.replica_count;
  int rc = 0;
  for (uint32_t id = 0; rc == 0 && id < copies; ++id)
    rc = kafs_pwrite_all(fd, report.descriptor, report.descriptor_bytes,
                         (off_t)report.descriptors[id].offset);
  close(fd);
  kafs_v7_layout_report_clear(&report);
  return rc;
}

static int test_descriptor_fault_matrix(void)
{
  const enum descriptor_fault faults[] = {
      DESCRIPTOR_UNKNOWN_TYPE,     DESCRIPTOR_MISSING_INCOMPAT, DESCRIPTOR_RESERVED_NONZERO,
      DESCRIPTOR_MAPPING_POLICY,   DESCRIPTOR_MAPPING_SEED,     DESCRIPTOR_OVERLAP,
      DESCRIPTOR_UNKNOWN_CLASS,    DESCRIPTOR_UNKNOWN_FLAG,     DESCRIPTOR_RANGE_OVERFLOW,
      DESCRIPTOR_TABLE_COUNT,      DESCRIPTOR_CROSS_GROUP,
  };
  for (size_t i = 0; i < sizeof(faults) / sizeof(faults[0]); ++i)
  {
    char path[64];
    snprintf(path, sizeof(path), "v7-desc-fault-%zu.img", i);
    if (format_image(path, "64M", 0) != 0 || apply_descriptor_fault(path, faults[i], 0) != 0 ||
        validate_path(path, 0, NULL) != 0)
      return -1;
  }

  const char *divergent = "v7-desc-divergent.img";
  if (format_image(divergent, "64M", 0) != 0)
    return -1;
  kafs_v7_layout_report_t report;
  if (validate_path(divergent, 1, &report) != 0)
    return -1;
  kafs_v7_layout_header_t *header = (kafs_v7_layout_header_t *)report.descriptor;
  kafs_v7_group_desc_t *group = (kafs_v7_group_desc_t *)(
      (uint8_t *)report.descriptor + le32toh(header->group_desc_off));
  kafs_v7_shard_desc_t *shards = (kafs_v7_shard_desc_t *)(
      (uint8_t *)report.descriptor + le32toh(header->shard_desc_off));
  uint64_t block_size = report.block_size;
  shards[6].physical_bytes = htole64(le64toh(shards[6].physical_bytes) - 2u * block_size);
  group->metadata_physical_bytes =
      htole64(le64toh(group->metadata_physical_bytes) - 2u * block_size);
  group->data_physical_off = htole64(le64toh(group->data_physical_off) - 2u * block_size);
  update_descriptor_crc(report.descriptor);
  int fd = open(divergent, O_RDWR);
  int rc = fd < 0 ? -errno
                  : kafs_pwrite_all(fd, report.descriptor, report.descriptor_bytes,
                                    (off_t)report.descriptors[0].offset);
  if (fd >= 0)
    close(fd);
  kafs_v7_layout_report_clear(&report);
  return rc == 0 && validate_path(divergent, 0, NULL) == 0 ? 0 : -1;
}

static int test_checkpoint_and_payload_faults(void)
{
  const char *checkpoint_path = "v7-checkpoint-divergent.img";
  if (format_image(checkpoint_path, "64M", 0) != 0)
    return -1;
  kafs_v7_layout_report_t report;
  if (validate_path(checkpoint_path, 1, &report) != 0)
    return -1;
  int fd = open(checkpoint_path, O_RDWR);
  if (fd < 0)
    return -errno;
  kafs_v7_checkpoint_t checkpoint;
  int rc = kafs_pread_all(fd, &checkpoint, sizeof(checkpoint),
                          (off_t)report.checkpoints[0].offset);
  if (rc == 0)
  {
    checkpoint.free_blocks = htole64(report.free_blocks - 1u);
    update_checkpoint_crc(&checkpoint);
    rc = kafs_pwrite_all(fd, &checkpoint, sizeof(checkpoint),
                         (off_t)report.checkpoints[0].offset);
  }
  close(fd);
  kafs_v7_layout_report_clear(&report);
  if (rc != 0 || validate_path(checkpoint_path, 0, NULL) != 0)
    return -1;

  const char *count_path = "v7-checkpoint-count-mismatch.img";
  if (format_image(count_path, "64M", 0) != 0 || validate_path(count_path, 1, &report) != 0)
    return -1;
  fd = open(count_path, O_RDWR);
  rc = fd < 0 ? -errno : 0;
  for (uint32_t id = 0; rc == 0 && id < report.replica_count; ++id)
  {
    rc = kafs_pread_all(fd, &checkpoint, sizeof(checkpoint),
                        (off_t)report.checkpoints[id].offset);
    if (rc == 0)
    {
      checkpoint.free_blocks = htole64(report.free_blocks - 1u);
      update_checkpoint_crc(&checkpoint);
      rc = kafs_pwrite_all(fd, &checkpoint, sizeof(checkpoint),
                           (off_t)report.checkpoints[id].offset);
    }
  }
  if (fd >= 0)
    close(fd);
  kafs_v7_layout_report_clear(&report);
  if (rc != 0 || validate_path(count_path, 0, NULL) != 0)
    return -1;

  const char *stale_path = "v7-checkpoint-stale.img";
  if (format_image(stale_path, "64M", 0) != 0 || validate_path(stale_path, 1, &report) != 0)
    return -1;
  fd = open(stale_path, O_RDWR);
  rc = fd < 0 ? -errno
              : kafs_pread_all(fd, &checkpoint, sizeof(checkpoint),
                               (off_t)report.checkpoints[0].offset);
  if (rc == 0)
  {
    checkpoint.descriptor_generation =
        htole64(le64toh(checkpoint.descriptor_generation) + 1u);
    update_checkpoint_crc(&checkpoint);
    rc = kafs_pwrite_all(fd, &checkpoint, sizeof(checkpoint),
                         (off_t)report.checkpoints[0].offset);
  }
  if (fd >= 0)
    close(fd);
  kafs_v7_layout_report_clear(&report);
  if (rc != 0 || validate_path(stale_path, 1, &report) != 0 || !report.degraded ||
      report.selected_checkpoint != 1u ||
      report.checkpoints[0].status != KAFS_V7_REPLICA_STATUS_STALE)
    return -1;
  kafs_v7_layout_report_clear(&report);

  const char *bitmap_path = "v7-bitmap-padding.img";
  if (format_image(bitmap_path, "64M", 0) != 0 || validate_path(bitmap_path, 1, &report) != 0)
    return -1;
  const kafs_v7_shard_desc_t *shards = kafs_v7_report_shards(&report);
  uint64_t bitmap_last = le64toh(shards[0].physical_off) + le64toh(shards[0].physical_bytes) - 1u;
  fd = open(bitmap_path, O_RDWR);
  uint8_t zero = 0;
  rc = fd < 0 ? -errno : kafs_pwrite_all(fd, &zero, sizeof(zero), (off_t)bitmap_last);
  if (fd >= 0)
    close(fd);
  kafs_v7_layout_report_clear(&report);
  if (rc != 0 || validate_path(bitmap_path, 0, NULL) != 0)
    return -1;

  const char *inode_path = "v7-inode-canonical.img";
  if (format_image(inode_path, "64M", 0) != 0 || validate_path(inode_path, 1, &report) != 0)
    return -1;
  shards = kafs_v7_report_shards(&report);
  uint64_t inode2_byte = le64toh(shards[1].physical_off) + 2u * KAFS_V7_INODE_BYTES + 8u;
  uint8_t one = 1u;
  fd = open(inode_path, O_RDWR);
  rc = fd < 0 ? -errno : kafs_pwrite_all(fd, &one, sizeof(one), (off_t)inode2_byte);
  if (fd >= 0)
    close(fd);
  kafs_v7_layout_report_clear(&report);
  if (rc != 0 || validate_path(inode_path, 0, NULL) != 0)
    return -1;

  const char *journal_path = "v7-journal-gap.img";
  if (format_image(journal_path, "64M", 0) != 0 || validate_path(journal_path, 1, &report) != 0)
    return -1;
  shards = kafs_v7_report_shards(&report);
  uint64_t journal_header_off = le64toh(shards[5].physical_off);
  fd = open(journal_path, O_RDWR);
  kafs_v7_journal_header_t journal_header;
  rc = fd < 0 ? -errno : kafs_pread_all(fd, &journal_header, sizeof(journal_header),
                                        (off_t)journal_header_off);
  if (rc == 0)
  {
    journal_header.first_sequence = htole64(1u);
    update_journal_header_crc(&journal_header);
    rc = kafs_pwrite_all(fd, &journal_header, sizeof(journal_header),
                         (off_t)journal_header_off);
  }
  if (fd >= 0)
    close(fd);
  kafs_v7_layout_report_clear(&report);
  return rc == 0 && validate_path(journal_path, 0, NULL) == 0 ? 0 : -1;
}

static int test_root_and_identity_faults(void)
{
  const char *hash_path = "v7-hash-id.img";
  if (format_image(hash_path, "64M", 0) != 0)
    return -1;
  int fd = open(hash_path, O_RDWR);
  kafs_ssuperblock_t sb;
  kafs_v7_layout_report_t report;
  uint64_t file_size = 0;
  int rc = fd < 0 ? -errno : load_superblock_and_size(fd, &sb, &file_size);
  if (rc == 0)
  {
    kafs_sb_hash_fast_set(&sb, KAFS_HASH_FAST_XXH64);
    rc = kafs_pwrite_all(fd, &sb, sizeof(sb), 0);
  }
  if (fd >= 0)
    close(fd);
  if (rc != 0 || validate_path(hash_path, 0, NULL) != 0)
    return -1;

  const char *reserved_path = "v7-root-reserved.img";
  if (format_image(reserved_path, "64M", 0) != 0)
    return -1;
  fd = open(reserved_path, O_RDWR);
  uint8_t one = 1u;
  rc = fd < 0 ? -errno
              : kafs_pwrite_all(fd, &one, sizeof(one),
                                (off_t)(offsetof(kafs_ssuperblock_t, s_reserved) + 32u));
  if (fd >= 0)
    close(fd);
  if (rc != 0 || validate_path(reserved_path, 0, NULL) != 0)
    return -1;

  const char *prespec_path = "v7-prespec-v1.img";
  if (format_image(prespec_path, "64M", 0) != 0)
    return -1;
  fd = open(prespec_path, O_RDWR);
  kafs_v7_root_locator_t locator;
  rc = fd < 0 ? -errno : kafs_pread_all(fd, &locator, sizeof(locator),
                                        (off_t)offsetof(kafs_ssuperblock_t, s_reserved));
  if (rc == 0)
  {
    locator.version = htole16(1u);
    update_locator_crc(&locator);
    rc = kafs_pwrite_all(fd, &locator, sizeof(locator),
                         (off_t)offsetof(kafs_ssuperblock_t, s_reserved));
  }
  if (rc == 0)
    rc = kafs_pwrite_all(fd, &locator, sizeof(locator),
                         (off_t)(TEST_IMAGE_BYTES - sizeof(locator)));
  if (fd >= 0)
    close(fd);
  if (rc != 0 || validate_path(prespec_path, 0, NULL) != 0)
    return -1;

  const char *locator_path = "v7-locator-divergent.img";
  if (format_image(locator_path, "64M", 0) != 0)
    return -1;
  fd = open(locator_path, O_RDWR);
  rc = fd < 0 ? -errno : kafs_pread_all(fd, &locator, sizeof(locator),
                                        (off_t)offsetof(kafs_ssuperblock_t, s_reserved));
  if (rc == 0)
  {
    locator.candidate_count = htole32(3u);
    update_locator_crc(&locator);
    rc = kafs_pwrite_all(fd, &locator, sizeof(locator),
                         (off_t)offsetof(kafs_ssuperblock_t, s_reserved));
  }
  if (fd >= 0)
    close(fd);
  if (rc != 0 || validate_path(locator_path, 0, NULL) != 0)
    return -1;

  const char *offset_path = "v7-primary-locator-offset.img";
  if (format_image(offset_path, "64M", 0) != 0)
    return -1;
  fd = open(offset_path, O_RDWR);
  rc = fd < 0 ? -errno : kafs_pread_all(fd, &locator, sizeof(locator),
                                        (off_t)offsetof(kafs_ssuperblock_t, s_reserved));
  if (rc == 0)
  {
    locator.primary_desc_off =
        htole64(le64toh(locator.primary_desc_off) + le32toh(locator.block_size));
    update_locator_crc(&locator);
    rc = kafs_pwrite_all(fd, &locator, sizeof(locator),
                         (off_t)offsetof(kafs_ssuperblock_t, s_reserved));
  }
  if (fd >= 0)
    close(fd);
  if (rc != 0 || validate_path(offset_path, 1, &report) != 0 || !report.degraded ||
      report.primary_locator_valid)
    return -1;
  kafs_v7_layout_report_clear(&report);

  const char *all_descriptors = "v7-all-descriptors-invalid.img";
  if (format_image(all_descriptors, "64M", 0) != 0)
    return -1;
  if (validate_path(all_descriptors, 1, &report) != 0)
    return -1;
  fd = open(all_descriptors, O_RDWR);
  rc = 0;
  for (uint32_t id = 0; fd >= 0 && rc == 0 && id < report.replica_count; ++id)
    rc = flip_byte(fd, report.descriptors[id].offset + 16u);
  if (fd < 0)
    rc = -errno;
  if (fd >= 0)
    close(fd);
  kafs_v7_layout_report_clear(&report);
  return rc == 0 && validate_path(all_descriptors, 0, NULL) == 0 ? 0 : -1;
}

static int build_three_replica_fixture(const char *path)
{
  if (format_image(path, "64M", 0) != 0)
    return -1;
  kafs_v7_layout_report_t report;
  if (validate_path(path, 1, &report) != 0 || report.replica_count != 2u)
    return -1;
  uint32_t block_size = report.block_size;
  uint32_t descriptor_bytes = report.descriptor_bytes;
  uint64_t old_tail_descriptor = report.descriptors[1].offset;
  uint64_t old_tail_checkpoint = report.checkpoints[1].offset;

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
                                   (off_t)report.checkpoints[0].offset);
  if (rc == 0 && ftruncate(fd, (off_t)TEST_MIDPOINT_IMAGE_BYTES) != 0)
    rc = -errno;

  uint64_t tail_descriptor = TEST_MIDPOINT_IMAGE_BYTES - block_size - descriptor_bytes;
  uint64_t tail_checkpoint = tail_descriptor - block_size;
  uint64_t midpoint_descriptor = TEST_MIDPOINT_IMAGE_BYTES / 2u;
  uint64_t midpoint_checkpoint = midpoint_descriptor + descriptor_bytes;
  kafs_v7_layout_header_t *header = (kafs_v7_layout_header_t *)descriptor;
  kafs_v7_shard_desc_t *shards = (kafs_v7_shard_desc_t *)(
      (uint8_t *)descriptor + le32toh(header->shard_desc_off));
  kafs_v7_replica_desc_t *replicas = (kafs_v7_replica_desc_t *)(
      (uint8_t *)descriptor + le32toh(header->replica_desc_off));
  header->image_size_bytes = htole64(TEST_MIDPOINT_IMAGE_BYTES);
  header->shard_count = htole32(13u);
  header->replica_count = htole32(3u);
  shards[9].physical_off = htole64(tail_descriptor);
  shards[10].physical_off = htole64(tail_checkpoint);
  memset(&shards[11], 0, sizeof(shards[11]));
  shards[11].type = htole16(KAFS_V7_SHARD_LAYOUT_DESCRIPTOR);
  shards[11].storage_class = htole16(KAFS_V7_STORAGE_BYTE_SPAN);
  shards[11].group_id = htole32(UINT32_MAX);
  shards[11].physical_off = htole64(midpoint_descriptor);
  shards[11].physical_bytes = htole64(descriptor_bytes);
  shards[11].logical_start = htole64(2u);
  shards[11].logical_count = htole64(1u);
  memset(&shards[12], 0, sizeof(shards[12]));
  shards[12].type = htole16(KAFS_V7_SHARD_SUPERBLOCK_CHECKPOINT);
  shards[12].storage_class = htole16(KAFS_V7_STORAGE_FIXED_RECORD);
  shards[12].group_id = htole32(UINT32_MAX);
  shards[12].physical_off = htole64(midpoint_checkpoint);
  shards[12].physical_bytes = htole64(block_size);
  shards[12].logical_start = htole64(2u);
  shards[12].logical_count = htole64(1u);
  shards[12].record_bytes = htole32(KAFS_V7_CHECKPOINT_BYTES);
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
    sb.s_blkcnt = kafs_blkcnt_htos(TEST_MIDPOINT_IMAGE_BYTES / block_size);
  }
  size_t zero_bytes = descriptor_bytes > block_size ? descriptor_bytes : block_size;
  void *zero = calloc(1u, zero_bytes);
  if (!zero && rc == 0)
    rc = -ENOMEM;
  if (rc == 0)
    rc = kafs_pwrite_all(fd, zero, block_size, (off_t)old_tail_checkpoint);
  if (rc == 0)
    rc = kafs_pwrite_all(fd, zero, descriptor_bytes, (off_t)old_tail_descriptor);
  if (rc == 0)
    rc = kafs_pwrite_all(fd, zero, block_size,
                         (off_t)(TEST_IMAGE_BYTES - block_size));
  if (rc == 0)
    rc = kafs_pwrite_all(fd, zero, block_size,
                         (off_t)(TEST_MIDPOINT_IMAGE_BYTES - block_size));
  if (rc == 0)
    rc = kafs_pwrite_all(fd, descriptor, descriptor_bytes, (off_t)report.descriptors[0].offset);
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
                         (off_t)(TEST_MIDPOINT_IMAGE_BYTES - sizeof(locator)));
  if (rc == 0)
    rc = kafs_pwrite_all(fd, &sb, sizeof(sb), 0);

  free(zero);
  free(descriptor);
  free(checkpoint_block);
  if (fd >= 0)
    close(fd);
  kafs_v7_layout_report_clear(&report);
  return rc;
}

static int test_three_replica_and_small_image(void)
{
  const char *path = "v7-three-replica.img";
  if (build_three_replica_fixture(path) != 0)
    return -1;
  kafs_v7_layout_report_t report;
  if (validate_path(path, 1, &report) != 0)
    return -1;
  int valid = report.replica_count == 3u && report.shard_count == 13u && !report.degraded;
  const kafs_v7_group_desc_t *groups = kafs_v7_report_groups(&report);
  uint64_t slack_off = le64toh(groups[0].data_physical_off) +
                       le64toh(groups[0].data_physical_bytes);
  kafs_v7_layout_report_clear(&report);
  if (!valid)
    return -1;
  int fd = open(path, O_RDWR);
  uint8_t one = 1u;
  int rc = fd < 0 ? -errno : kafs_pwrite_all(fd, &one, sizeof(one), (off_t)slack_off);
  if (fd >= 0)
    close(fd);
  if (rc != 0 || validate_path(path, 0, NULL) != 0)
    return -1;
  return format_image("v7-too-small.img", "1M", 2);
}

int main(void)
{
  if (kafs_test_enter_tmpdir("v7-raw-layout") != 0)
    return 1;
  if (test_round_trip_and_fallbacks() != 0)
  {
    fprintf(stderr, "v7 raw-layout round trip/fallback test failed\n");
    return 1;
  }
  if (test_descriptor_fault_matrix() != 0)
  {
    fprintf(stderr, "v7 descriptor fault matrix failed\n");
    return 1;
  }
  if (test_checkpoint_and_payload_faults() != 0)
  {
    fprintf(stderr, "v7 checkpoint/payload fault matrix failed\n");
    return 1;
  }
  if (test_root_and_identity_faults() != 0)
  {
    fprintf(stderr, "v7 root/identity fault matrix failed\n");
    return 1;
  }
  if (test_three_replica_and_small_image() != 0)
  {
    fprintf(stderr, "v7 three-replica/small-image test failed\n");
    return 1;
  }
  return 0;
}
