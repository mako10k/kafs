#include "test_utils.h"

#include "kafs_offline_summary.h"
#include "kafs_superblock.h"
#include "kafs_tool_util.h"
#include "kafs_v7_layout.h"
#include "kafs_v7_mutation.h"

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

typedef struct mutation_fixture
{
  int fd;
  uint64_t file_size;
  kafs_ssuperblock_t superblock;
  kafs_v7_layout_report_t layout;
} mutation_fixture_t;

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

static int fixture_open(mutation_fixture_t *fixture)
{
  const char *path = "v7-mutation-routing.img";
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
  if (!fixture || run_command(argv) != 0)
    return -1;

  memset(fixture, 0, sizeof(*fixture));
  fixture->fd = open(path, O_RDONLY);
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

static void fixture_close(mutation_fixture_t *fixture)
{
  if (!fixture)
    return;
  if (fixture->fd >= 0)
    close(fixture->fd);
  kafs_v7_layout_report_clear(&fixture->layout);
  memset(fixture, 0, sizeof(*fixture));
  fixture->fd = -1;
}

static const kafs_v7_shard_desc_t *group_shard(const kafs_v7_layout_report_t *layout,
                                                uint32_t group_id, uint16_t type,
                                                uint32_t *index_out)
{
  const kafs_v7_group_desc_t *groups = kafs_v7_report_groups(layout);
  const kafs_v7_shard_desc_t *shards = kafs_v7_report_shards(layout);
  if (!groups || !shards || group_id >= layout->group_count)
    return NULL;
  uint32_t first = le32toh(groups[group_id].first_shard_index);
  uint32_t count = le32toh(groups[group_id].shard_count);
  for (uint32_t local = 0; local < count; ++local)
  {
    if (le16toh(shards[first + local].type) == type)
    {
      if (index_out)
        *index_out = first + local;
      return &shards[first + local];
    }
  }
  return NULL;
}

static uint64_t ceil_div8(uint64_t value)
{
  return value / 8u + (value % 8u != 0u);
}

static int expected_route(const kafs_v7_layout_report_t *layout, uint32_t group_id,
                          uint16_t type, uint64_t logical_index,
                          kafs_v7_mutation_route_t *expected)
{
  uint32_t shard_index = 0;
  const kafs_v7_shard_desc_t *shard = group_shard(layout, group_id, type, &shard_index);
  if (!shard || !expected)
    return -1;
  uint64_t start = le64toh(shard->logical_start);
  uint64_t count = le64toh(shard->logical_count);
  uint64_t index = 0;
  uint32_t bytes = 0;
  if (type == KAFS_V7_JOURNAL_TARGET_ALLOCATOR_SUMMARY)
  {
    const kafs_v7_shard_desc_t *bitmap =
        group_shard(layout, group_id, KAFS_V7_SHARD_BLOCK_BITMAP, NULL);
    if (!bitmap || logical_index != le64toh(bitmap->logical_start))
      return -1;
    uint64_t l0 = ceil_div8(le64toh(bitmap->logical_count));
    uint64_t l1 = ceil_div8(l0);
    uint64_t l2 = ceil_div8(l1);
    if (l1 + l2 > UINT32_MAX)
      return -1;
    bytes = (uint32_t)(l1 + l2);
  }
  else
  {
    if (logical_index < start || logical_index - start >= count)
      return -1;
    index = logical_index - start;
    switch (type)
    {
    case KAFS_V7_JOURNAL_TARGET_BLOCK_BITMAP:
      if (index % 64u != 0u)
        return -1;
      index /= 64u;
      bytes = 8u;
      break;
    case KAFS_V7_JOURNAL_TARGET_INODE:
      bytes = KAFS_V7_INODE_BYTES;
      break;
    case KAFS_V7_JOURNAL_TARGET_HRL_INDEX:
      bytes = 4u;
      break;
    case KAFS_V7_JOURNAL_TARGET_HRL_ENTRY:
      bytes = KAFS_V7_HRL_ENTRY_BYTES;
      break;
    default:
      return -1;
    }
  }
  *expected = (kafs_v7_mutation_route_t){
      .target_type = type,
      .group_id = group_id,
      .logical_index = logical_index,
      .physical_off = le64toh(shard->physical_off) + index * bytes,
      .target_bytes = bytes,
      .shard_index = shard_index,
  };
  return 0;
}

static int check_route(const kafs_v7_layout_report_t *layout, uint32_t group_id, uint16_t type,
                       uint64_t logical_index)
{
  kafs_v7_mutation_route_t expected;
  kafs_v7_mutation_route_t actual;
  if (expected_route(layout, group_id, type, logical_index, &expected) != 0 ||
      kafs_v7_mutation_route_target(layout, type, logical_index, &actual) != 0 ||
      memcmp(&actual, &expected, sizeof(actual)) != 0)
    return -1;
  memset(&actual, 0, sizeof(actual));
  if (kafs_v7_mutation_route_target_in_group(layout, type, group_id, logical_index, &actual) != 0 ||
      memcmp(&actual, &expected, sizeof(actual)) != 0)
    return -1;
  return 0;
}

static int test_target_matrix(const kafs_v7_layout_report_t *layout)
{
  static const uint16_t types[] = {
      KAFS_V7_JOURNAL_TARGET_BLOCK_BITMAP,
      KAFS_V7_JOURNAL_TARGET_INODE,
      KAFS_V7_JOURNAL_TARGET_ALLOCATOR_SUMMARY,
      KAFS_V7_JOURNAL_TARGET_HRL_INDEX,
      KAFS_V7_JOURNAL_TARGET_HRL_ENTRY,
  };
  for (uint32_t group_id = 0; group_id < layout->group_count; ++group_id)
  {
    for (size_t i = 0; i < sizeof(types) / sizeof(types[0]); ++i)
    {
      const kafs_v7_shard_desc_t *shard = group_shard(layout, group_id, types[i], NULL);
      if (!shard)
        return -1;
      uint64_t logical_index = le64toh(shard->logical_start);
      if (types[i] == KAFS_V7_JOURNAL_TARGET_ALLOCATOR_SUMMARY)
      {
        const kafs_v7_shard_desc_t *bitmap =
            group_shard(layout, group_id, KAFS_V7_SHARD_BLOCK_BITMAP, NULL);
        if (!bitmap)
          return -1;
        logical_index = le64toh(bitmap->logical_start);
      }
      if (check_route(layout, group_id, types[i], logical_index) != 0)
        return -1;
      if (types[i] != KAFS_V7_JOURNAL_TARGET_ALLOCATOR_SUMMARY)
      {
        uint64_t count = le64toh(shard->logical_count);
        uint64_t last = logical_index + count - 1u;
        if (types[i] == KAFS_V7_JOURNAL_TARGET_BLOCK_BITMAP)
          last = logical_index + ((count - 1u) / 64u) * 64u;
        if (check_route(layout, group_id, types[i], last) != 0)
          return -1;
      }
    }
  }
  return 0;
}

static void group_requests(const kafs_v7_layout_report_t *layout, uint32_t group_id,
                           kafs_v7_mutation_request_t requests[5])
{
  const kafs_v7_shard_desc_t *bitmap =
      group_shard(layout, group_id, KAFS_V7_SHARD_BLOCK_BITMAP, NULL);
  const kafs_v7_shard_desc_t *inode =
      group_shard(layout, group_id, KAFS_V7_SHARD_INODE_TABLE, NULL);
  const kafs_v7_shard_desc_t *index =
      group_shard(layout, group_id, KAFS_V7_SHARD_HRL_INDEX, NULL);
  const kafs_v7_shard_desc_t *entry =
      group_shard(layout, group_id, KAFS_V7_SHARD_HRL_ENTRIES, NULL);
  requests[0] = (kafs_v7_mutation_request_t){KAFS_V7_JOURNAL_TARGET_BLOCK_BITMAP,
                                             le64toh(bitmap->logical_start)};
  requests[1] = (kafs_v7_mutation_request_t){KAFS_V7_JOURNAL_TARGET_ALLOCATOR_SUMMARY,
                                             le64toh(bitmap->logical_start)};
  requests[2] = (kafs_v7_mutation_request_t){KAFS_V7_JOURNAL_TARGET_INODE,
                                             le64toh(inode->logical_start)};
  requests[3] = (kafs_v7_mutation_request_t){KAFS_V7_JOURNAL_TARGET_HRL_INDEX,
                                             le64toh(index->logical_start)};
  requests[4] = (kafs_v7_mutation_request_t){KAFS_V7_JOURNAL_TARGET_HRL_ENTRY,
                                             le64toh(entry->logical_start)};
}

static int test_transaction_matrix(const kafs_v7_layout_report_t *layout)
{
  for (uint32_t group_id = 0; group_id < layout->group_count; ++group_id)
  {
    kafs_v7_mutation_request_t requests[5];
    kafs_v7_mutation_route_t routes[5];
    uint32_t selected_group = UINT32_MAX;
    group_requests(layout, group_id, requests);
    if (kafs_v7_mutation_route_transaction(layout, requests, 5u, routes, &selected_group) != 0 ||
        selected_group != group_id)
      return -1;
    for (size_t i = 0; i < 5u; ++i)
    {
      if (routes[i].group_id != group_id || routes[i].target_type != requests[i].target_type ||
          routes[i].logical_index != requests[i].logical_index)
        return -1;
    }
  }

  kafs_v7_mutation_request_t group0[5];
  kafs_v7_mutation_request_t group1[5];
  group_requests(layout, 0u, group0);
  group_requests(layout, 1u, group1);
  kafs_v7_mutation_request_t cross_group[] = {group0[0], group1[2]};
  kafs_v7_mutation_route_t routes[2];
  memset(routes, 0x5a, sizeof(routes));
  kafs_v7_mutation_route_t before[2];
  memcpy(before, routes, sizeof(before));
  uint32_t selected_group = UINT32_MAX;
  if (kafs_v7_mutation_route_transaction(layout, cross_group, 2u, routes, &selected_group) !=
          -EXDEV ||
      selected_group != UINT32_MAX || memcmp(routes, before, sizeof(routes)) != 0)
    return -1;

  kafs_v7_mutation_request_t duplicate[] = {group0[0], group0[0]};
  if (kafs_v7_mutation_route_transaction(layout, duplicate, 2u, routes, &selected_group) !=
      -EEXIST)
    return -1;
  return 0;
}

static int test_fail_closed_matrix(const kafs_v7_layout_report_t *layout)
{
  const kafs_v7_shard_desc_t *bitmap = group_shard(layout, 0u, KAFS_V7_SHARD_BLOCK_BITMAP, NULL);
  const kafs_v7_shard_desc_t *last_inode =
      group_shard(layout, layout->group_count - 1u, KAFS_V7_SHARD_INODE_TABLE, NULL);
  if (!bitmap || !last_inode)
    return -1;
  kafs_v7_mutation_route_t route;
  uint64_t bitmap_start = le64toh(bitmap->logical_start);
  uint64_t inode_end =
      le64toh(last_inode->logical_start) + le64toh(last_inode->logical_count);
  if (kafs_v7_mutation_route_target(layout, KAFS_V7_JOURNAL_TARGET_BLOCK_BITMAP,
                                    bitmap_start + 1u, &route) != -EINVAL ||
      kafs_v7_mutation_route_target(layout, KAFS_V7_JOURNAL_TARGET_INODE, inode_end, &route) !=
          -ENOENT ||
      kafs_v7_mutation_route_target(layout, UINT16_MAX, 0u, &route) != -EINVAL ||
      kafs_v7_mutation_route_target_in_group(layout, KAFS_V7_JOURNAL_TARGET_BLOCK_BITMAP, 1u,
                                             bitmap_start, &route) != -EXDEV ||
      kafs_v7_mutation_route_target_in_group(layout, KAFS_V7_JOURNAL_TARGET_BLOCK_BITMAP,
                                             layout->group_count, bitmap_start, &route) != -EINVAL)
    return -1;
  return 0;
}

int main(void)
{
  if (kafs_test_enter_tmpdir("v7-mutation-routing") != 0)
    return 1;
  mutation_fixture_t fixture;
  if (fixture_open(&fixture) != 0)
  {
    fprintf(stderr, "v7 mutation routing fixture setup failed\n");
    return 1;
  }
  int rc = test_target_matrix(&fixture.layout);
  if (rc == 0)
    rc = test_transaction_matrix(&fixture.layout);
  if (rc == 0)
    rc = test_fail_closed_matrix(&fixture.layout);
  fixture_close(&fixture);
  if (rc != 0)
  {
    fprintf(stderr, "v7 mutation routing matrix failed\n");
    return 1;
  }
  return 0;
}
