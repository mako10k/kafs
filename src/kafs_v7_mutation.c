#include "kafs_v7_mutation.h"

#include <endian.h>
#include <errno.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

static int kafs_v7_mutation_target_type_known(uint16_t type)
{
  return type == KAFS_V7_JOURNAL_TARGET_BLOCK_BITMAP || type == KAFS_V7_JOURNAL_TARGET_INODE ||
         type == KAFS_V7_JOURNAL_TARGET_ALLOCATOR_SUMMARY ||
         type == KAFS_V7_JOURNAL_TARGET_HRL_INDEX || type == KAFS_V7_JOURNAL_TARGET_HRL_ENTRY;
}

static uint64_t kafs_v7_mutation_ceil_div8(uint64_t value)
{
  return value / 8u + (value % 8u != 0u);
}

static int kafs_v7_mutation_group_shard(const kafs_v7_layout_report_t *layout, uint32_t group_index,
                                        uint16_t type, const kafs_v7_shard_desc_t **shard_out,
                                        uint32_t *shard_index_out)
{
  const kafs_v7_group_desc_t *groups = kafs_v7_report_groups(layout);
  const kafs_v7_shard_desc_t *shards = kafs_v7_report_shards(layout);
  if (!groups || !shards || group_index >= layout->group_count || !shard_out || !shard_index_out)
    return -EINVAL;

  const kafs_v7_group_desc_t *group = &groups[group_index];
  uint32_t first = le32toh(group->first_shard_index);
  uint32_t count = le32toh(group->shard_count);
  if (le32toh(group->group_id) != group_index || count == 0u || first > layout->shard_count ||
      count > layout->shard_count - first)
    return -EUCLEAN;

  const kafs_v7_shard_desc_t *found = NULL;
  uint32_t found_index = 0;
  for (uint32_t local = 0; local < count; ++local)
  {
    uint32_t index = first + local;
    const kafs_v7_shard_desc_t *candidate = &shards[index];
    if (le16toh(candidate->type) != type)
      continue;
    if (found || le32toh(candidate->group_id) != group_index)
      return -EUCLEAN;
    found = candidate;
    found_index = index;
  }
  if (!found)
    return -EUCLEAN;
  *shard_out = found;
  *shard_index_out = found_index;
  return 0;
}

static int kafs_v7_mutation_target_unit(const kafs_v7_layout_report_t *layout, uint32_t group_index,
                                        uint16_t type, uint64_t logical_index,
                                        kafs_v7_mutation_route_t *route)
{
  const kafs_v7_shard_desc_t *shard = NULL;
  uint32_t shard_index = 0;
  int rc = kafs_v7_mutation_group_shard(layout, group_index, type, &shard, &shard_index);
  if (rc != 0)
    return rc;

  uint64_t start = le64toh(shard->logical_start);
  uint64_t count = le64toh(shard->logical_count);
  uint64_t physical_start = le64toh(shard->physical_off);
  uint64_t physical_bytes = le64toh(shard->physical_bytes);
  if (count == 0u || physical_bytes == 0u || physical_start > UINT64_MAX - physical_bytes)
    return -EUCLEAN;

  uint16_t expected_storage = KAFS_V7_STORAGE_FIXED_RECORD;
  uint32_t expected_record_bytes = 0;
  switch (type)
  {
  case KAFS_V7_JOURNAL_TARGET_BLOCK_BITMAP:
    expected_storage = KAFS_V7_STORAGE_BIT_PACKED;
    expected_record_bytes = 8u;
    break;
  case KAFS_V7_JOURNAL_TARGET_INODE:
    expected_record_bytes = KAFS_V7_INODE_BYTES;
    break;
  case KAFS_V7_JOURNAL_TARGET_ALLOCATOR_SUMMARY:
    expected_storage = KAFS_V7_STORAGE_ALLOCATOR_SUMMARY;
    break;
  case KAFS_V7_JOURNAL_TARGET_HRL_INDEX:
    expected_record_bytes = 4u;
    break;
  case KAFS_V7_JOURNAL_TARGET_HRL_ENTRY:
    expected_record_bytes = KAFS_V7_HRL_ENTRY_BYTES;
    break;
  default:
    return -EINVAL;
  }
  if (le16toh(shard->storage_class) != expected_storage ||
      le32toh(shard->record_bytes) != expected_record_bytes)
    return -EUCLEAN;

  uint64_t unit_index = 0;
  uint32_t target_bytes = 0;
  if (type == KAFS_V7_JOURNAL_TARGET_ALLOCATOR_SUMMARY)
  {
    const kafs_v7_shard_desc_t *bitmap = NULL;
    uint32_t bitmap_index = 0;
    rc = kafs_v7_mutation_group_shard(layout, group_index, KAFS_V7_JOURNAL_TARGET_BLOCK_BITMAP,
                                      &bitmap, &bitmap_index);
    (void)bitmap_index;
    if (rc != 0)
      return rc;
    uint64_t bitmap_start = le64toh(bitmap->logical_start);
    uint64_t bitmap_count = le64toh(bitmap->logical_count);
    if (logical_index != bitmap_start)
      return -ENOENT;
    uint64_t l0_bytes = kafs_v7_mutation_ceil_div8(bitmap_count);
    uint64_t l1_bytes = kafs_v7_mutation_ceil_div8(l0_bytes);
    uint64_t l2_bytes = kafs_v7_mutation_ceil_div8(l1_bytes);
    if (bitmap_count == 0u || l1_bytes > UINT32_MAX - l2_bytes)
      return -EUCLEAN;
    target_bytes = (uint32_t)(l1_bytes + l2_bytes);
  }
  else
  {
    if (logical_index < start || logical_index - start >= count)
      return -ENOENT;
    unit_index = logical_index - start;
    switch (type)
    {
    case KAFS_V7_JOURNAL_TARGET_BLOCK_BITMAP:
      if (unit_index % 64u != 0u)
        return -EINVAL;
      unit_index /= 64u;
      target_bytes = 8u;
      break;
    case KAFS_V7_JOURNAL_TARGET_INODE:
      target_bytes = KAFS_V7_INODE_BYTES;
      break;
    case KAFS_V7_JOURNAL_TARGET_HRL_INDEX:
      target_bytes = 4u;
      break;
    case KAFS_V7_JOURNAL_TARGET_HRL_ENTRY:
      target_bytes = KAFS_V7_HRL_ENTRY_BYTES;
      break;
    default:
      return -EINVAL;
    }
  }

  if (target_bytes == 0u || unit_index > (UINT64_MAX - physical_start) / target_bytes)
    return -EOVERFLOW;
  uint64_t physical_off = physical_start + unit_index * target_bytes;
  if (physical_off < physical_start || physical_off > physical_start + physical_bytes ||
      target_bytes > physical_start + physical_bytes - physical_off)
    return -EUCLEAN;

  *route = (kafs_v7_mutation_route_t){
      .target_type = type,
      .group_id = group_index,
      .logical_index = logical_index,
      .physical_off = physical_off,
      .target_bytes = target_bytes,
      .shard_index = shard_index,
  };
  return 0;
}

int kafs_v7_mutation_route_target(const kafs_v7_layout_report_t *layout, uint16_t target_type,
                                  uint64_t logical_index, kafs_v7_mutation_route_t *route)
{
  if (!layout || !layout->descriptor || !route || layout->group_count == 0u ||
      !kafs_v7_mutation_target_type_known(target_type))
    return -EINVAL;

  kafs_v7_mutation_route_t selected;
  int found = 0;
  for (uint32_t group_index = 0; group_index < layout->group_count; ++group_index)
  {
    kafs_v7_mutation_route_t candidate;
    int rc =
        kafs_v7_mutation_target_unit(layout, group_index, target_type, logical_index, &candidate);
    if (rc == -ENOENT)
      continue;
    if (rc != 0)
      return rc;
    if (found)
      return -EUCLEAN;
    selected = candidate;
    found = 1;
  }
  if (!found)
    return -ENOENT;
  *route = selected;
  return 0;
}

int kafs_v7_mutation_route_target_in_group(const kafs_v7_layout_report_t *layout,
                                           uint16_t target_type, uint32_t group_id,
                                           uint64_t logical_index, kafs_v7_mutation_route_t *route)
{
  if (!layout || !route || group_id >= layout->group_count)
    return -EINVAL;
  kafs_v7_mutation_route_t selected;
  int rc = kafs_v7_mutation_route_target(layout, target_type, logical_index, &selected);
  if (rc != 0)
    return rc;
  if (selected.group_id != group_id)
    return -EXDEV;
  *route = selected;
  return 0;
}

int kafs_v7_mutation_route_transaction(const kafs_v7_layout_report_t *layout,
                                       const kafs_v7_mutation_request_t *requests,
                                       size_t request_count, kafs_v7_mutation_route_t *routes,
                                       uint32_t *group_id)
{
  if (!layout || !requests || request_count == 0u || !routes || !group_id)
    return -EINVAL;
  if (request_count > SIZE_MAX / sizeof(*routes))
    return -EOVERFLOW;

  kafs_v7_mutation_route_t *selected = calloc(request_count, sizeof(*selected));
  if (!selected)
    return -ENOMEM;
  int rc = 0;
  uint32_t selected_group = 0;
  for (size_t i = 0; i < request_count; ++i)
  {
    rc = kafs_v7_mutation_route_target(layout, requests[i].target_type, requests[i].logical_index,
                                       &selected[i]);
    if (rc != 0)
      break;
    if (i == 0u)
      selected_group = selected[i].group_id;
    else if (selected[i].group_id != selected_group)
    {
      rc = -EXDEV;
      break;
    }
    for (size_t previous = 0; previous < i; ++previous)
    {
      uint64_t previous_end = selected[previous].physical_off + selected[previous].target_bytes;
      uint64_t current_end = selected[i].physical_off + selected[i].target_bytes;
      if (selected[i].physical_off < previous_end && selected[previous].physical_off < current_end)
      {
        rc = selected[i].target_type == selected[previous].target_type &&
                     selected[i].logical_index == selected[previous].logical_index
                 ? -EEXIST
                 : -EUCLEAN;
        break;
      }
    }
    if (rc != 0)
      break;
  }
  if (rc == 0)
  {
    memcpy(routes, selected, request_count * sizeof(*routes));
    *group_id = selected_group;
  }
  free(selected);
  return rc;
}
