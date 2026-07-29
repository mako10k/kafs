#include "test_utils.h"

#include "kafs_offline_summary.h"
#include "kafs_superblock.h"
#include "kafs_tool_util.h"
#include "kafs_v7_layout.h"

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

enum descriptor_fault
{
  DESCRIPTOR_DATA_GAP,
  DESCRIPTOR_PHYSICAL_OVERLAP,
  DESCRIPTOR_GROUP_OWNER,
  DESCRIPTOR_JOURNAL_GAP,
};

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

static int format_image(const char *path, const char *size, const char *groups, int expected_exit)
{
  unlink(path);
  char *argv[] = {(char *)kafs_test_mkfs_bin(),
                  (char *)path,
                  (char *)"--format-version",
                  (char *)"7",
                  (char *)"--size-bytes",
                  (char *)size,
                  (char *)"--v7-group-count",
                  (char *)groups,
                  (char *)"--yes",
                  NULL};
  if (!groups)
  {
    argv[6] = (char *)"--yes";
    argv[7] = NULL;
  }
  return run_command(argv, expected_exit);
}

static int validate_path(const char *path, int expect_valid, kafs_v7_layout_report_t *out)
{
  int fd = open(path, O_RDONLY);
  if (fd < 0)
    return -errno;
  kafs_ssuperblock_t sb;
  uint64_t file_size = 0;
  int rc = kafs_pread_all(fd, &sb, sizeof(sb), 0);
  if (rc == 0)
    rc = kafs_offline_detect_file_size(fd, &file_size);
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
  if (rc == 0 && out)
    *out = report;
  else if (rc == 0)
    kafs_v7_layout_report_clear(&report);
  return 0;
}

static void update_descriptor_crc(void *descriptor)
{
  kafs_v7_layout_header_t *header = (kafs_v7_layout_header_t *)descriptor;
  header->descriptor_crc32 = 0;
  header->descriptor_crc32 =
      htole32(kafs_v7_crc32(descriptor, le32toh(header->descriptor_bytes)));
}

static int test_auto_placement_and_diagnostics(void)
{
  const char *path = "v7-multi-auto.img";
  if (format_image(path, "512M", NULL, 0) != 0)
    return -1;
  kafs_v7_layout_report_t report;
  if (validate_path(path, 1, &report) != 0)
    return -1;
  const kafs_v7_group_desc_t *groups = kafs_v7_report_groups(&report);
  const kafs_v7_shard_desc_t *shards = kafs_v7_report_shards(&report);
  int rc = 0;
  if (report.group_count != 8u || report.shard_count != 60u ||
      report.journal_segment_count != 8u || report.placement_span_bytes == 0 ||
      report.placement_arena_bytes == 0 ||
      report.max_group_data_blocks - report.min_group_data_blocks > 64u ||
      report.placement_span_bytes * 100u < report.placement_arena_bytes * 70u)
    rc = -1;

  uint64_t logical_start = 0;
  uint64_t previous_end = 0;
  for (uint32_t group_id = 0; rc == 0 && group_id < report.group_count; ++group_id)
  {
    uint64_t metadata_off = le64toh(groups[group_id].metadata_physical_off);
    uint64_t data_off = le64toh(groups[group_id].data_physical_off);
    uint64_t data_bytes = le64toh(groups[group_id].data_physical_bytes);
    if (le32toh(groups[group_id].group_id) != group_id ||
        le32toh(groups[group_id].first_shard_index) !=
            group_id * KAFS_V7_GROUP_LOCAL_SHARDS ||
        le32toh(groups[group_id].shard_count) != KAFS_V7_GROUP_LOCAL_SHARDS ||
        le64toh(groups[group_id].data_logical_start) != logical_start ||
        (group_id + 1u < report.group_count &&
         le64toh(groups[group_id].data_logical_count) % 64u != 0) ||
        metadata_off < previous_end || data_off < metadata_off)
      rc = -1;
    for (uint32_t local = 0; rc == 0 && local < KAFS_V7_GROUP_LOCAL_SHARDS; ++local)
      if (le32toh(shards[group_id * KAFS_V7_GROUP_LOCAL_SHARDS + local].group_id) != group_id)
        rc = -1;
    logical_start += le64toh(groups[group_id].data_logical_count);
    previous_end = data_off + data_bytes;
  }
  if (rc == 0 && logical_start != report.free_blocks)
    rc = -1;
  kafs_v7_layout_report_clear(&report);
  if (rc != 0)
    return rc;

  char command[4096];
  int written = snprintf(
      command, sizeof(command),
      "%s --json %s | python3 -c 'import json,sys; d=json.load(sys.stdin); "
      "w=d[\"wear_distribution\"]; assert w[\"status\"]==\"ok\"; "
      "assert w[\"scope\"]==\"filesystem-placement\"; assert w[\"distributed\"]; "
      "assert w[\"group_count\"]==8; "
      "assert w[\"max_group_data_blocks\"]-w[\"min_group_data_blocks\"]<=64'",
      kafs_test_kafsdump_bin(), path);
  if (written < 0 || (size_t)written >= sizeof(command) || system(command) != 0)
    return -1;
  char *fsck_argv[] = {(char *)kafs_test_fsck_bin(), (char *)path, NULL};
  return run_command(fsck_argv, 0);
}

static int test_explicit_group_policy(void)
{
  const char *path = "v7-multi-explicit.img";
  if (format_image(path, "128M", "4", 0) != 0)
    return -1;
  kafs_v7_layout_report_t report;
  if (validate_path(path, 1, &report) != 0)
    return -1;
  int valid = report.group_count == 4u && report.journal_segment_count == 4u;
  kafs_v7_layout_report_clear(&report);
  if (!valid || format_image("v7-groups-invalid.img", "128M", "3", 2) != 0 ||
      format_image("v7-groups-too-many.img", "4M", "64", 2) != 0)
    return -1;

  char *non_v7[] = {(char *)kafs_test_mkfs_bin(),
                    (char *)"v7-groups-v5.img",
                    (char *)"--format-version",
                    (char *)"5",
                    (char *)"--v7-group-count",
                    (char *)"2",
                    NULL};
  return run_command(non_v7, 2);
}

static int inject_descriptor_fault(const char *path, enum descriptor_fault fault)
{
  kafs_v7_layout_report_t report;
  if (validate_path(path, 1, &report) != 0)
    return -1;
  int fd = open(path, O_RDWR);
  void *descriptor = malloc(report.descriptor_bytes);
  if (fd < 0 || !descriptor)
  {
    if (fd >= 0)
      close(fd);
    free(descriptor);
    kafs_v7_layout_report_clear(&report);
    return -ENOMEM;
  }
  int rc = kafs_pread_all(fd, descriptor, report.descriptor_bytes,
                          (off_t)report.descriptors[0].offset);
  kafs_v7_layout_header_t *header = (kafs_v7_layout_header_t *)descriptor;
  kafs_v7_group_desc_t *groups = (kafs_v7_group_desc_t *)((uint8_t *)descriptor +
                                                          le32toh(header->group_desc_off));
  kafs_v7_shard_desc_t *shards = (kafs_v7_shard_desc_t *)((uint8_t *)descriptor +
                                                          le32toh(header->shard_desc_off));
  if (rc == 0)
  {
    if (fault == DESCRIPTOR_DATA_GAP)
      groups[1].data_logical_start =
          htole64(le64toh(groups[1].data_logical_start) + 64u);
    else if (fault == DESCRIPTOR_PHYSICAL_OVERLAP)
      groups[1].metadata_physical_off = groups[0].data_physical_off;
    else if (fault == DESCRIPTOR_GROUP_OWNER)
      shards[KAFS_V7_GROUP_LOCAL_SHARDS].group_id = htole32(0u);
    else
      shards[KAFS_V7_GROUP_LOCAL_SHARDS + 5u].logical_start = htole64(0u);
    update_descriptor_crc(descriptor);
  }
  for (uint32_t id = 0; rc == 0 && id < report.replica_count; ++id)
    rc = kafs_pwrite_all(fd, descriptor, report.descriptor_bytes,
                         (off_t)report.descriptors[id].offset);
  close(fd);
  free(descriptor);
  kafs_v7_layout_report_clear(&report);
  return rc;
}

static int test_descriptor_fault_matrix(void)
{
  const char *path = "v7-multi-fault.img";
  const enum descriptor_fault faults[] = {
      DESCRIPTOR_DATA_GAP,
      DESCRIPTOR_PHYSICAL_OVERLAP,
      DESCRIPTOR_GROUP_OWNER,
      DESCRIPTOR_JOURNAL_GAP,
  };
  for (size_t i = 0; i < sizeof(faults) / sizeof(faults[0]); ++i)
  {
    if (format_image(path, "128M", "4", 0) != 0 ||
        inject_descriptor_fault(path, faults[i]) != 0 || validate_path(path, 0, NULL) != 0)
      return -1;
  }
  return 0;
}

static int test_cross_group_hrl_rejected(void)
{
  const char *path = "v7-cross-group-hrl.img";
  if (format_image(path, "128M", "4", 0) != 0)
    return -1;
  kafs_v7_layout_report_t report;
  if (validate_path(path, 1, &report) != 0)
    return -1;
  const kafs_v7_shard_desc_t *shards = kafs_v7_report_shards(&report);
  uint32_t foreign_entry = htole32(
      (uint32_t)le64toh(shards[KAFS_V7_GROUP_LOCAL_SHARDS + 4u].logical_start) + 1u);
  uint64_t bucket_off = le64toh(shards[3].physical_off);
  kafs_v7_layout_report_clear(&report);

  int fd = open(path, O_RDWR);
  int rc = fd < 0 ? -errno : kafs_pwrite_all(fd, &foreign_entry, sizeof(foreign_entry),
                                             (off_t)bucket_off);
  if (fd >= 0)
    close(fd);
  return rc == 0 ? validate_path(path, 0, NULL) : rc;
}

int main(void)
{
  if (kafs_test_enter_tmpdir("v7-multi-group") != 0)
    return 1;
  if (test_auto_placement_and_diagnostics() != 0)
  {
    fprintf(stderr, "v7 multi-group placement/diagnostics test failed\n");
    return 1;
  }
  if (test_explicit_group_policy() != 0)
  {
    fprintf(stderr, "v7 explicit group policy test failed\n");
    return 1;
  }
  if (test_descriptor_fault_matrix() != 0)
  {
    fprintf(stderr, "v7 multi-group descriptor fault matrix failed\n");
    return 1;
  }
  if (test_cross_group_hrl_rejected() != 0)
  {
    fprintf(stderr, "v7 cross-group HRL rejection test failed\n");
    return 1;
  }
  return 0;
}
