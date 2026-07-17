#include "kafs_v7_data_cow.h"

#include "kafs_tool_util.h"
#include "kafs_v7_io.h"
#include "kafs_v7_mutation.h"

#include <endian.h>
#include <errno.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

static uint64_t kafs_v7_data_ceil_div8(uint64_t value) { return value / 8u + (value % 8u != 0u); }

static int kafs_v7_data_summary_build(const uint8_t *bitmap, uint64_t block_count, uint8_t *summary,
                                      uint32_t summary_bytes)
{
  if (!bitmap || block_count == 0u || !summary)
    return -EINVAL;
  uint64_t l0_bytes = kafs_v7_data_ceil_div8(block_count);
  uint64_t l1_bytes = kafs_v7_data_ceil_div8(l0_bytes);
  uint64_t l2_bytes = kafs_v7_data_ceil_div8(l1_bytes);
  if (l1_bytes > UINT32_MAX || l2_bytes > UINT32_MAX - l1_bytes ||
      summary_bytes != l1_bytes + l2_bytes)
    return -EUCLEAN;

  memset(summary, 0, summary_bytes);
  for (uint64_t byte = 0; byte < l0_bytes; ++byte)
  {
    uint8_t valid_mask = 0xffu;
    if (byte + 1u == l0_bytes && (block_count & 7u) != 0u)
      valid_mask = (uint8_t)((1u << (block_count & 7u)) - 1u);
    if ((bitmap[byte] & valid_mask) != valid_mask)
      summary[byte / 8u] |= (uint8_t)(1u << (byte % 8u));
  }
  for (uint64_t byte = 0; byte < l1_bytes; ++byte)
  {
    if (summary[byte] != 0u)
      summary[l1_bytes + byte / 8u] |= (uint8_t)(1u << (byte % 8u));
  }
  return 0;
}

static int kafs_v7_data_find_free_range(const uint8_t *bitmap, const uint8_t *summary,
                                        uint64_t block_count, uint64_t first, uint64_t end,
                                        uint64_t *selected)
{
  uint64_t l0_bytes = kafs_v7_data_ceil_div8(block_count);
  uint64_t l1_bytes = kafs_v7_data_ceil_div8(l0_bytes);
  for (uint64_t block = first; block < end;)
  {
    uint64_t bitmap_byte = block / 8u;
    uint64_t l1_byte = bitmap_byte / 8u;
    if ((summary[l1_bytes + l1_byte / 8u] & (uint8_t)(1u << (l1_byte % 8u))) == 0u)
    {
      uint64_t next = (l1_byte + 1u) * 64u;
      block = next > block ? next : block + 1u;
      continue;
    }
    if ((summary[bitmap_byte / 8u] & (uint8_t)(1u << (bitmap_byte % 8u))) == 0u)
    {
      uint64_t next = (bitmap_byte + 1u) * 8u;
      block = next > block ? next : block + 1u;
      continue;
    }
    uint8_t start_bit = (uint8_t)(block % 8u);
    for (uint8_t bit = start_bit; bit < 8u && block - start_bit + bit < end; ++bit)
    {
      uint64_t candidate = block - start_bit + bit;
      if (candidate < block_count &&
          (bitmap[candidate / 8u] & (uint8_t)(1u << (candidate % 8u))) == 0u)
      {
        *selected = candidate;
        return 0;
      }
    }
    block = (bitmap_byte + 1u) * 8u;
  }
  return -ENOSPC;
}

static int kafs_v7_data_find_free(const uint8_t *bitmap, const uint8_t *summary,
                                  uint64_t block_count, uint64_t cursor, uint64_t *selected)
{
  int rc =
      kafs_v7_data_find_free_range(bitmap, summary, block_count, cursor, block_count, selected);
  if (rc == -ENOSPC && cursor != 0u)
    rc = kafs_v7_data_find_free_range(bitmap, summary, block_count, 0u, cursor, selected);
  return rc;
}

static int kafs_v7_data_group_geometry(const kafs_v7_layout_report_t *layout, uint32_t group_id,
                                       uint64_t *logical_start, uint64_t *logical_count,
                                       uint64_t *physical_start)
{
  const kafs_v7_group_desc_t *groups = kafs_v7_report_groups(layout);
  if (!layout || !groups || group_id >= layout->group_count || !logical_start || !logical_count ||
      !physical_start || le32toh(groups[group_id].group_id) != group_id)
    return -EINVAL;
  *logical_start = le64toh(groups[group_id].data_logical_start);
  *logical_count = le64toh(groups[group_id].data_logical_count);
  *physical_start = le64toh(groups[group_id].data_physical_off);
  return *logical_count == 0u ? -EUCLEAN : 0;
}

typedef struct kafs_v7_data_allocator_view
{
  uint64_t logical_start;
  uint64_t logical_count;
  uint64_t physical_start;
  kafs_v7_mutation_route_t bitmap_route;
  kafs_v7_mutation_route_t allocator_route;
  uint8_t *bitmap;
  uint8_t *summary;
} kafs_v7_data_allocator_view_t;

static void kafs_v7_data_allocator_view_clear(kafs_v7_data_allocator_view_t *view)
{
  if (!view)
    return;
  free(view->summary);
  free(view->bitmap);
  memset(view, 0, sizeof(*view));
}

static int kafs_v7_data_allocator_view_route(const kafs_v7_data_cow_plan_request_t *request,
                                             kafs_v7_data_allocator_view_t *view)
{
  int rc = kafs_v7_data_group_geometry(request->layout, request->group_id, &view->logical_start,
                                       &view->logical_count, &view->physical_start);
  if (rc == 0)
  {
    rc = kafs_v7_mutation_route_target_in_group(
        request->layout, KAFS_V7_JOURNAL_TARGET_BLOCK_BITMAP, request->group_id,
        view->logical_start, &view->bitmap_route);
  }
  if (rc == 0)
  {
    rc = kafs_v7_mutation_route_target_in_group(
        request->layout, KAFS_V7_JOURNAL_TARGET_ALLOCATOR_SUMMARY, request->group_id,
        view->logical_start, &view->allocator_route);
  }
  uint64_t bitmap_bytes = kafs_v7_data_ceil_div8(view->logical_count);
  if (rc == 0 && (bitmap_bytes == 0u || bitmap_bytes > SIZE_MAX))
    rc = -EOVERFLOW;
  return rc;
}

static int kafs_v7_data_allocator_view_read(int fd, const kafs_v7_data_cow_plan_request_t *request,
                                            kafs_v7_data_allocator_view_t *view)
{
  uint64_t bitmap_bytes = kafs_v7_data_ceil_div8(view->logical_count);
  int rc = 0;
  if (rc == 0)
  {
    view->bitmap = (uint8_t *)malloc((size_t)bitmap_bytes);
    view->summary = (uint8_t *)malloc(view->allocator_route.target_bytes);
    if (!view->bitmap || !view->summary)
      rc = -ENOMEM;
  }
  if (rc == 0)
  {
    rc = kafs_v7_journal_overlay_pread(request->replay, fd, view->bitmap, (size_t)bitmap_bytes,
                                       view->bitmap_route.physical_off);
  }
  if (rc == 0)
  {
    rc = kafs_v7_journal_overlay_pread(request->replay, fd, view->summary,
                                       view->allocator_route.target_bytes,
                                       view->allocator_route.physical_off);
  }
  return rc;
}

static int kafs_v7_data_allocator_view_validate(const kafs_v7_data_allocator_view_t *view)
{
  uint8_t *expected = (uint8_t *)malloc(view->allocator_route.target_bytes);
  if (!expected)
    return -ENOMEM;
  int rc = kafs_v7_data_summary_build(view->bitmap, view->logical_count, expected,
                                      view->allocator_route.target_bytes);
  if (rc == 0 && memcmp(view->summary, expected, view->allocator_route.target_bytes) != 0)
    rc = -EUCLEAN;
  free(expected);
  return rc;
}

static int kafs_v7_data_allocator_view_load(int fd, const kafs_v7_data_cow_plan_request_t *request,
                                            kafs_v7_data_allocator_view_t *view)
{
  int rc = kafs_v7_data_allocator_view_route(request, view);
  if (rc == 0)
    rc = kafs_v7_data_allocator_view_read(fd, request, view);
  if (rc == 0)
    rc = kafs_v7_data_allocator_view_validate(view);
  return rc;
}

static int kafs_v7_data_allocator_request_validate(const kafs_v7_data_allocator_view_t *view,
                                                   uint64_t cursor, uint64_t retained)
{
  if (cursor < view->logical_start || cursor - view->logical_start >= view->logical_count)
    return -EINVAL;
  if (retained == KAFS_V7_DATA_COW_NO_BLOCK)
    return 0;
  if (retained < view->logical_start || retained - view->logical_start >= view->logical_count)
    return -EXDEV;
  uint64_t local = retained - view->logical_start;
  return (view->bitmap[local / 8u] & (uint8_t)(1u << (local % 8u))) != 0u ? 0 : -ENOENT;
}

static int kafs_v7_data_allocator_mutation_build(int fd,
                                                 const kafs_v7_data_cow_plan_request_t *request,
                                                 const kafs_v7_data_allocator_view_t *view,
                                                 uint64_t selected, int allocate,
                                                 kafs_v7_data_cow_plan_t *plan)
{
  uint64_t word_local = selected & ~UINT64_C(63);
  kafs_v7_mutation_route_t bitmap_word;
  int rc = kafs_v7_mutation_route_target_in_group(
      request->layout, KAFS_V7_JOURNAL_TARGET_BLOCK_BITMAP, request->group_id,
      view->logical_start + word_local, &bitmap_word);
  if (rc == 0)
  {
    rc = kafs_v7_journal_overlay_pread(request->replay, fd, plan->bitmap_after,
                                       sizeof(plan->bitmap_after), bitmap_word.physical_off);
  }
  if (rc == 0)
  {
    uint64_t word_bit = selected - word_local;
    uint8_t mask = (uint8_t)(1u << (word_bit % 8u));
    if (allocate)
      plan->bitmap_after[word_bit / 8u] |= mask;
    else
      plan->bitmap_after[word_bit / 8u] &= (uint8_t)~mask;
    plan->allocator_after = (uint8_t *)malloc(view->allocator_route.target_bytes);
    if (!plan->allocator_after)
      rc = -ENOMEM;
  }
  if (rc == 0)
  {
    rc = kafs_v7_data_summary_build(view->bitmap, view->logical_count, plan->allocator_after,
                                    view->allocator_route.target_bytes);
  }
  if (rc == 0 && selected > (UINT64_MAX - view->physical_start) / request->layout->block_size)
    rc = -EOVERFLOW;
  if (rc == 0)
  {
    plan->group_id = request->group_id;
    plan->block_size = request->layout->block_size;
    plan->logical_block = view->logical_start + selected;
    plan->physical_off = view->physical_start + selected * request->layout->block_size;
    plan->retained_logical_block = request->retained_logical_block;
    plan->bitmap_word_logical = view->logical_start + word_local;
    plan->allocator_logical = view->logical_start;
    plan->allocator_bytes = view->allocator_route.target_bytes;
    plan->free_blocks_delta = allocate ? -1 : 1;
  }
  return rc;
}

static int kafs_v7_data_allocator_plan_build(int fd, const kafs_v7_data_cow_plan_request_t *request,
                                             kafs_v7_data_allocator_view_t *view,
                                             kafs_v7_data_cow_plan_t *plan)
{
  uint64_t selected = 0u;
  int rc = kafs_v7_data_find_free(view->bitmap, view->summary, view->logical_count,
                                  request->allocation_cursor - view->logical_start, &selected);
  if (rc != 0)
    return rc;
  view->bitmap[selected / 8u] |= (uint8_t)(1u << (selected % 8u));
  return kafs_v7_data_allocator_mutation_build(fd, request, view, selected, 1, plan);
}

int kafs_v7_data_cow_plan_fd(int fd, const kafs_v7_data_cow_plan_request_t *request,
                             kafs_v7_data_cow_plan_t *plan)
{
  if (fd < 0 || !request || !request->layout || !request->layout->descriptor || !request->replay ||
      !request->replay->state || !plan || request->layout->block_size == 0u)
    return -EINVAL;
  memset(plan, 0, sizeof(*plan));
  plan->retained_logical_block = KAFS_V7_DATA_COW_NO_BLOCK;
  kafs_v7_data_allocator_view_t view;
  memset(&view, 0, sizeof(view));
  int rc = kafs_v7_data_allocator_view_load(fd, request, &view);
  if (rc == 0)
  {
    rc = kafs_v7_data_allocator_request_validate(&view, request->allocation_cursor,
                                                 request->retained_logical_block);
  }
  if (rc == 0)
    rc = kafs_v7_data_allocator_plan_build(fd, request, &view, plan);
  kafs_v7_data_allocator_view_clear(&view);
  if (rc != 0)
    kafs_v7_data_cow_plan_clear(plan);
  return rc;
}

static int
kafs_v7_data_retirement_request_validate(int fd,
                                         const kafs_v7_data_retirement_plan_request_t *request,
                                         const kafs_v7_data_cow_plan_t *plan)
{
  if (fd < 0 || !request || !plan || !request->layout || !request->layout->descriptor ||
      request->layout->block_size == 0u || !request->replay || !request->replay->state)
    return -EINVAL;
  return 0;
}

static int kafs_v7_data_retirement_view_local(const kafs_v7_data_allocator_view_t *view,
                                              uint64_t logical_block, uint64_t *local)
{
  if (!view || !local)
    return -EINVAL;
  if (logical_block < view->logical_start ||
      logical_block - view->logical_start >= view->logical_count)
    return -EXDEV;
  *local = logical_block - view->logical_start;
  if ((view->bitmap[*local / 8u] & (uint8_t)(1u << (*local % 8u))) == 0u)
    return -EALREADY;
  return 0;
}

int kafs_v7_data_retirement_plan_fd(int fd, const kafs_v7_data_retirement_plan_request_t *request,
                                    kafs_v7_data_cow_plan_t *plan)
{
  int rc = kafs_v7_data_retirement_request_validate(fd, request, plan);
  if (rc != 0)
    return rc;
  memset(plan, 0, sizeof(*plan));
  plan->retained_logical_block = KAFS_V7_DATA_COW_NO_BLOCK;
  kafs_v7_data_cow_plan_request_t allocator_request = {
      .layout = request->layout,
      .replay = request->replay,
      .group_id = request->group_id,
      .retained_logical_block = KAFS_V7_DATA_COW_NO_BLOCK,
  };
  kafs_v7_data_allocator_view_t view;
  memset(&view, 0, sizeof(view));
  rc = kafs_v7_data_allocator_view_load(fd, &allocator_request, &view);
  uint64_t local = 0u;
  if (rc == 0)
    rc = kafs_v7_data_retirement_view_local(&view, request->logical_block, &local);
  if (rc == 0)
  {
    view.bitmap[local / 8u] &= (uint8_t) ~(1u << (local % 8u));
    rc = kafs_v7_data_allocator_mutation_build(fd, &allocator_request, &view, local, 0, plan);
  }
  kafs_v7_data_allocator_view_clear(&view);
  if (rc != 0)
    kafs_v7_data_cow_plan_clear(plan);
  return rc;
}

void kafs_v7_data_cow_plan_clear(kafs_v7_data_cow_plan_t *plan)
{
  if (!plan)
    return;
  free(plan->allocator_after);
  memset(plan, 0, sizeof(*plan));
}

int kafs_v7_data_cow_plan_patches(
    const kafs_v7_data_cow_plan_t *plan,
    kafs_v7_journal_patch_t patches[KAFS_V7_DATA_COW_ALLOCATOR_PATCH_COUNT])
{
  if (!plan || !patches || !plan->allocator_after || plan->allocator_bytes == 0u ||
      (plan->free_blocks_delta != -1 && plan->free_blocks_delta != 1))
    return -EINVAL;
  memset(patches, 0, KAFS_V7_DATA_COW_ALLOCATOR_PATCH_COUNT * sizeof(*patches));
  patches[0] = (kafs_v7_journal_patch_t){
      .target_type = KAFS_V7_JOURNAL_TARGET_BLOCK_BITMAP,
      .logical_index = plan->bitmap_word_logical,
      .patch_bytes = sizeof(plan->bitmap_after),
      .patch = plan->bitmap_after,
      .free_blocks_delta = plan->free_blocks_delta,
  };
  patches[1] = (kafs_v7_journal_patch_t){
      .target_type = KAFS_V7_JOURNAL_TARGET_ALLOCATOR_SUMMARY,
      .logical_index = plan->allocator_logical,
      .patch_bytes = plan->allocator_bytes,
      .patch = plan->allocator_after,
  };
  return 0;
}

int kafs_v7_data_cow_verify_fd(int fd, const kafs_v7_data_cow_plan_t *plan, const void *expected,
                               size_t expected_bytes)
{
  if (fd < 0 || !plan || !expected || plan->block_size == 0u ||
      expected_bytes != plan->block_size || plan->physical_off > INT64_MAX ||
      expected_bytes > (uint64_t)INT64_MAX - plan->physical_off)
    return -EINVAL;
  void *actual = malloc(expected_bytes);
  if (!actual)
    return -ENOMEM;
  int rc = kafs_pread_all(fd, actual, expected_bytes, (off_t)plan->physical_off);
  if (rc == 0 && memcmp(actual, expected, expected_bytes) != 0)
    rc = -EIO;
  free(actual);
  return rc;
}

int kafs_v7_data_cow_stage_fd(int fd, const kafs_v7_data_cow_plan_t *plan, const void *data,
                              size_t data_bytes, void *verified_copy)
{
  if (!plan || !data || !verified_copy || plan->block_size == 0u ||
      data_bytes != plan->block_size || plan->physical_off > INT64_MAX ||
      data_bytes > (uint64_t)INT64_MAX - plan->physical_off)
    return -EINVAL;
  int rc = kafs_v7_io_require_positional_writes(fd);
  if (rc == 0)
    rc = kafs_pwrite_all(fd, data, data_bytes, (off_t)plan->physical_off);
  if (rc == 0 && fdatasync(fd) != 0)
    rc = -errno;
  if (rc == 0)
    rc = kafs_pread_all(fd, verified_copy, data_bytes, (off_t)plan->physical_off);
  if (rc == 0 && memcmp(verified_copy, data, data_bytes) != 0)
    rc = -EIO;
  return rc;
}
