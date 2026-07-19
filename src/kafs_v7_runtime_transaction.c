#include "kafs_v7_runtime_transaction.h"

#include "kafs_v7_data_cow.h"
#include "kafs_v7_io.h"
#include "kafs_v7_mutation.h"
#include "kafs_v7_sequence.h"
#include "kafs_v7_test_fault.h"

#include <endian.h>
#include <errno.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

struct kafs_v7_runtime_transaction_service
{
  int fd;
  uint64_t file_size;
  kafs_ssuperblock_t superblock;
  kafs_v7_lock_state_t *locks;
  kafs_v7_sequence_state_t *sequence;
  uint64_t *allocation_cursors;
  uint32_t group_count;
};

struct kafs_v7_runtime_data_cow
{
  kafs_v7_runtime_transaction_service_t *service;
  kafs_v7_layout_report_t layout;
  kafs_v7_sequence_reservation_t reservation;
  kafs_v7_data_cow_plan_t plan;
  uint8_t *staged_data;
  uint32_t staged_crc32;
  uint8_t staged;
};

struct kafs_v7_runtime_data_cow_batch
{
  kafs_v7_runtime_transaction_service_t *service;
  kafs_v7_layout_report_t layout;
  kafs_v7_sequence_reservation_t reservation;
  kafs_v7_data_cow_plan_t *plans;
  uint8_t **staged_data;
  size_t plan_count;
};

static int kafs_v7_runtime_allocation_cursors_init(kafs_v7_runtime_transaction_service_t *service,
                                                   const kafs_v7_layout_report_t *layout)
{
  service->allocation_cursors = calloc(layout->group_count, sizeof(*service->allocation_cursors));
  if (!service->allocation_cursors)
    return -ENOMEM;
  const kafs_v7_group_desc_t *groups = kafs_v7_report_groups(layout);
  if (!groups)
    return -EUCLEAN;
  for (uint32_t group_id = 0; group_id < layout->group_count; ++group_id)
  {
    if (le32toh(groups[group_id].group_id) != group_id)
      return -EUCLEAN;
    service->allocation_cursors[group_id] = le64toh(groups[group_id].data_logical_start);
  }
  return 0;
}

static int kafs_v7_runtime_transaction_refresh(kafs_v7_runtime_transaction_service_t *service,
                                               kafs_v7_runtime_transaction_result_t *result)
{
  kafs_v7_layout_report_t layout;
  memset(&layout, 0, sizeof(layout));
  int rc =
      kafs_v7_validate_image_fd(service->fd, &service->superblock, service->file_size, &layout);
  if (rc == 0)
  {
    result->checkpoint_generation = layout.checkpoint_generation;
    result->checkpoint_sequence = layout.checkpoint_sequence;
    result->recovered_free_blocks = layout.free_blocks;
    result->recovered_free_inodes = layout.free_inodes;
  }
  kafs_v7_layout_report_clear(&layout);
  return rc;
}

int kafs_v7_runtime_transaction_service_init(int fd, const kafs_ssuperblock_t *superblock,
                                             uint64_t file_size,
                                             kafs_v7_runtime_transaction_service_t **service_out)
{
  if (!superblock || file_size == 0u || !service_out)
    return -EINVAL;
  *service_out = NULL;
  int rc = kafs_v7_io_require_positional_writes(fd);
  kafs_v7_layout_report_t layout;
  memset(&layout, 0, sizeof(layout));
  if (rc == 0)
    rc = kafs_v7_validate_image_fd(fd, superblock, file_size, &layout);

  kafs_v7_runtime_transaction_service_t *service = NULL;
  if (rc == 0)
  {
    service = calloc(1u, sizeof(*service));
    if (!service)
      rc = -ENOMEM;
  }
  if (rc == 0)
    rc = kafs_v7_locks_init(layout.group_count, 0u, &service->locks);
  if (rc == 0)
    rc = kafs_v7_sequence_state_init(service->locks, &layout, &service->sequence);
  if (rc == 0)
    rc = kafs_v7_runtime_allocation_cursors_init(service, &layout);
  if (rc == 0)
  {
    service->fd = fd;
    service->file_size = file_size;
    service->superblock = *superblock;
    service->group_count = layout.group_count;
    *service_out = service;
    service = NULL;
  }
  if (service)
  {
    free(service->allocation_cursors);
    kafs_v7_sequence_state_destroy(service->sequence);
    kafs_v7_locks_destroy(service->locks);
  }
  free(service);
  kafs_v7_layout_report_clear(&layout);
  return rc;
}

void kafs_v7_runtime_transaction_service_destroy(kafs_v7_runtime_transaction_service_t *service)
{
  if (!service)
    return;
  kafs_v7_sequence_state_destroy(service->sequence);
  kafs_v7_locks_destroy(service->locks);
  free(service->allocation_cursors);
  free(service);
}

static int kafs_v7_runtime_transaction_route(const kafs_v7_layout_report_t *layout,
                                             const kafs_v7_journal_patch_t *patches,
                                             size_t patch_count, uint32_t *group_id)
{
  if (!layout || !patches || patch_count == 0u || !group_id ||
      patch_count > SIZE_MAX / sizeof(kafs_v7_mutation_request_t) ||
      patch_count > SIZE_MAX / sizeof(kafs_v7_mutation_route_t))
    return -EINVAL;
  kafs_v7_mutation_request_t *requests = calloc(patch_count, sizeof(*requests));
  kafs_v7_mutation_route_t *routes = calloc(patch_count, sizeof(*routes));
  if (!requests || !routes)
  {
    free(routes);
    free(requests);
    return -ENOMEM;
  }
  for (size_t i = 0; i < patch_count; ++i)
  {
    requests[i].target_type = patches[i].target_type;
    requests[i].logical_index = patches[i].logical_index;
  }
  int rc = kafs_v7_mutation_route_transaction(layout, requests, patch_count, routes, group_id);
  free(routes);
  free(requests);
  return rc;
}

static int kafs_v7_runtime_transaction_publish_reserved(
    kafs_v7_runtime_transaction_service_t *service, const kafs_v7_layout_report_t *layout,
    kafs_v7_sequence_reservation_t *reservation, const kafs_v7_journal_patch_t *patches,
    size_t patch_count, kafs_v7_runtime_transaction_result_t *result)
{
  uint32_t group_id = 0u;
  int rc = kafs_v7_runtime_transaction_route(layout, patches, patch_count, &group_id);
  if (rc == 0 && group_id != reservation->group_id)
    rc = -EXDEV;
  kafs_v7_journal_transaction_t *transaction = NULL;
  if (rc == 0)
    rc = kafs_v7_journal_transaction_encode_fd(service->fd, layout, reservation, patches,
                                               patch_count, KAFS_V7_JOURNAL_COMMIT_TAG,
                                               &transaction);
  if (rc == 0)
    rc = kafs_v7_journal_transaction_publish_fd(service->fd, layout, reservation, transaction,
                                                &result->publication);
  if (rc == 0)
  {
    rc = kafs_v7_sequence_confirm_publication_fd(service->sequence, reservation, service->fd,
                                                 &service->superblock, service->file_size);
  }
  else if (reservation->active)
  {
    int cancel_rc = kafs_v7_sequence_cancel_reservation_fd(
        service->sequence, reservation, service->fd, &service->superblock, service->file_size);
    if (cancel_rc != 0)
      rc = cancel_rc;
  }
  kafs_v7_journal_transaction_destroy(transaction);
  return rc;
}

static int kafs_v7_runtime_transaction_publish(kafs_v7_runtime_transaction_service_t *service,
                                               const kafs_v7_journal_patch_t *patches,
                                               size_t patch_count,
                                               kafs_v7_runtime_transaction_result_t *result)
{
  kafs_v7_layout_report_t layout;
  memset(&layout, 0, sizeof(layout));
  int rc =
      kafs_v7_validate_image_fd(service->fd, &service->superblock, service->file_size, &layout);
  uint32_t group_id = 0u;
  if (rc == 0)
    rc = kafs_v7_runtime_transaction_route(&layout, patches, patch_count, &group_id);
  kafs_v7_sequence_reservation_t reservation;
  memset(&reservation, 0, sizeof(reservation));
  if (rc == 0)
    rc = kafs_v7_sequence_reserve(service->sequence, group_id, &reservation);
  if (rc == 0)
  {
    kafs_v7_layout_report_clear(&layout);
    memset(&layout, 0, sizeof(layout));
    rc = kafs_v7_validate_image_fd(service->fd, &service->superblock, service->file_size, &layout);
  }
  if (rc == 0)
  {
    rc = kafs_v7_runtime_transaction_publish_reserved(service, &layout, &reservation, patches,
                                                      patch_count, result);
  }
  else if (reservation.active)
  {
    int cancel_rc = kafs_v7_sequence_cancel_reservation_fd(
        service->sequence, &reservation, service->fd, &service->superblock, service->file_size);
    if (cancel_rc != 0)
      rc = cancel_rc;
  }
  kafs_v7_layout_report_clear(&layout);
  return rc;
}

static int kafs_v7_runtime_transaction_closeout(kafs_v7_runtime_transaction_service_t *service,
                                                kafs_v7_runtime_transaction_result_t *result)
{
  int rc = kafs_v7_metadata_closeout_fd(service->locks, service->fd, &service->superblock,
                                        service->file_size, &result->closeout);
  if (rc == 0)
    rc = kafs_v7_runtime_transaction_refresh(service, result);
  return rc;
}

int kafs_v7_runtime_transaction_commit(kafs_v7_runtime_transaction_service_t *service,
                                       const kafs_v7_journal_patch_t *patches, size_t patch_count,
                                       kafs_v7_runtime_transaction_result_t *result)
{
  if (!service || !patches || patch_count == 0u || !result)
    return -EINVAL;
  memset(result, 0, sizeof(*result));
  int rc = kafs_v7_runtime_transaction_publish(service, patches, patch_count, result);
  if (rc == 0)
    rc = kafs_v7_runtime_transaction_closeout(service, result);
  return rc;
}

static void kafs_v7_runtime_data_cow_advance_cursor(kafs_v7_runtime_data_cow_t *operation);

static void kafs_v7_runtime_data_cow_release(kafs_v7_runtime_data_cow_t *operation)
{
  if (!operation)
    return;
  free(operation->staged_data);
  kafs_v7_data_cow_plan_clear(&operation->plan);
  kafs_v7_layout_report_clear(&operation->layout);
  free(operation);
}

static int kafs_v7_runtime_data_cow_cancel(kafs_v7_runtime_data_cow_t *operation)
{
  int rc = 0;
  if (operation->reservation.active)
  {
    rc = kafs_v7_sequence_cancel_reservation_fd(
        operation->service->sequence, &operation->reservation, operation->service->fd,
        &operation->service->superblock, operation->service->file_size);
  }
  kafs_v7_runtime_data_cow_release(operation);
  return rc;
}

static void kafs_v7_runtime_data_cow_plan_export(const kafs_v7_runtime_data_cow_t *operation,
                                                 kafs_v7_runtime_data_cow_plan_t *plan)
{
  *plan = (kafs_v7_runtime_data_cow_plan_t){
      .group_id = operation->plan.group_id,
      .block_size = operation->plan.block_size,
      .sequence = operation->reservation.sequence,
      .logical_block = operation->plan.logical_block,
      .physical_off = operation->plan.physical_off,
      .retained_logical_block = operation->plan.retained_logical_block,
  };
}

int kafs_v7_runtime_data_cow_prepare(kafs_v7_runtime_transaction_service_t *service,
                                     const kafs_v7_runtime_data_cow_request_t *request,
                                     kafs_v7_runtime_data_cow_t **operation_out,
                                     kafs_v7_runtime_data_cow_plan_t *plan)
{
  if (!service || !request || request->reserved != 0u || !operation_out || !plan ||
      request->group_id >= service->group_count)
    return -EINVAL;
  *operation_out = NULL;
  memset(plan, 0, sizeof(*plan));
  kafs_v7_runtime_data_cow_t *operation = calloc(1u, sizeof(*operation));
  if (!operation)
    return -ENOMEM;
  operation->service = service;
  int rc = kafs_v7_sequence_reserve(service->sequence, request->group_id, &operation->reservation);
  if (rc == 0)
    rc = kafs_v7_validate_image_fd(service->fd, &service->superblock, service->file_size,
                                   &operation->layout);
  kafs_v7_journal_replay_t replay;
  memset(&replay, 0, sizeof(replay));
  if (rc == 0)
    rc = kafs_v7_journal_analyze_fd(service->fd, &operation->layout, &replay);
  if (rc == 0)
  {
    kafs_v7_data_cow_plan_request_t plan_request = {
        .layout = &operation->layout,
        .replay = &replay,
        .group_id = request->group_id,
        .allocation_cursor = service->allocation_cursors[request->group_id],
        .retained_logical_block = request->retained_logical_block,
    };
    rc = kafs_v7_data_cow_plan_fd(service->fd, &plan_request, &operation->plan);
  }
  kafs_v7_journal_replay_clear(&replay);
  if (rc != 0)
  {
    int cancel_rc = kafs_v7_runtime_data_cow_cancel(operation);
    return cancel_rc != 0 ? cancel_rc : rc;
  }
  kafs_v7_runtime_data_cow_plan_export(operation, plan);
  *operation_out = operation;
  return 0;
}

int kafs_v7_runtime_data_cow_stage(kafs_v7_runtime_data_cow_t *operation, const void *data,
                                   size_t data_bytes)
{
  if (!operation || !operation->reservation.active || !data || operation->staged)
    return -EINVAL;
  uint8_t *verified = malloc(operation->plan.block_size);
  if (!verified)
    return -ENOMEM;
  int rc = kafs_v7_data_cow_stage_fd(operation->service->fd, &operation->plan, data, data_bytes,
                                     verified);
  if (rc == 0)
  {
    operation->staged_data = verified;
    operation->staged_crc32 = kafs_v7_crc32(verified, data_bytes);
    operation->staged = 1u;
    kafs_v7_runtime_data_cow_advance_cursor(operation);
    verified = NULL;
  }
  free(verified);
  return rc;
}

static void kafs_v7_runtime_data_cow_advance_cursor(kafs_v7_runtime_data_cow_t *operation)
{
  const kafs_v7_group_desc_t *groups = kafs_v7_report_groups(&operation->layout);
  const kafs_v7_group_desc_t *group = &groups[operation->plan.group_id];
  uint64_t start = le64toh(group->data_logical_start);
  uint64_t count = le64toh(group->data_logical_count);
  uint64_t next = operation->plan.logical_block + 1u;
  if (next < start || next - start >= count)
    next = start;
  operation->service->allocation_cursors[operation->plan.group_id] = next;
}

static int kafs_v7_runtime_data_cow_patch_references(const kafs_v7_journal_patch_t *patch,
                                                     const uint32_t *expected,
                                                     const kafs_v7_inode_t *before,
                                                     const uint32_t *retained)
{
  if (patch->target_type != KAFS_V7_JOURNAL_TARGET_INODE || !patch->patch)
    return 0;
  const uint32_t first_ref = offsetof(kafs_v7_inode_t, inline_or_block_refs);
  for (uint32_t slot = 0; slot < 12u; ++slot)
  {
    uint32_t ref_off = first_ref + slot * sizeof(uint32_t);
    if (patch->patch_off > ref_off || patch->patch_bytes < sizeof(uint32_t) ||
        ref_off - patch->patch_off > patch->patch_bytes - sizeof(uint32_t))
      continue;
    if (memcmp((const uint8_t *)patch->patch + ref_off - patch->patch_off, expected,
               sizeof(*expected)) != 0)
      continue;
    if (!before || memcmp((const uint8_t *)before + ref_off, retained, sizeof(*retained)) == 0)
      return 1;
  }
  return 0;
}

static int kafs_v7_runtime_data_cow_load_inode(const kafs_v7_runtime_data_cow_t *operation,
                                               const kafs_v7_journal_replay_t *replay,
                                               uint64_t inode, kafs_v7_inode_t *before)
{
  kafs_v7_mutation_route_t route;
  int rc = kafs_v7_mutation_route_target_in_group(&operation->layout, KAFS_V7_JOURNAL_TARGET_INODE,
                                                  operation->plan.group_id, inode, &route);
  if (rc == 0)
  {
    rc = kafs_v7_journal_overlay_pread(replay, operation->service->fd, before, sizeof(*before),
                                       route.physical_off);
  }
  return rc;
}

static int
kafs_v7_runtime_data_cow_validate_direct_reference(const kafs_v7_runtime_data_cow_t *operation,
                                                   const kafs_v7_journal_patch_t *patches,
                                                   size_t patch_count)
{
  if (operation->plan.logical_block >= UINT32_MAX)
    return -ERANGE;
  uint32_t expected = htole32((uint32_t)operation->plan.logical_block + 1u);
  uint32_t retained = 0u;
  int needs_retained = operation->plan.retained_logical_block != KAFS_V7_DATA_COW_NO_BLOCK;
  if (needs_retained && operation->plan.retained_logical_block >= UINT32_MAX)
    return -ERANGE;
  if (needs_retained)
    retained = htole32((uint32_t)operation->plan.retained_logical_block + 1u);

  kafs_v7_journal_replay_t replay;
  memset(&replay, 0, sizeof(replay));
  int rc = needs_retained
               ? kafs_v7_journal_analyze_fd(operation->service->fd, &operation->layout, &replay)
               : 0;
  int found = 0;
  for (size_t patch_id = 0; rc == 0 && !found && patch_id < patch_count; ++patch_id)
  {
    const kafs_v7_journal_patch_t *patch = &patches[patch_id];
    if (!kafs_v7_runtime_data_cow_patch_references(patch, &expected, NULL, NULL))
      continue;
    if (!needs_retained)
    {
      found = 1;
      continue;
    }
    kafs_v7_inode_t before;
    memset(&before, 0, sizeof(before));
    rc = kafs_v7_runtime_data_cow_load_inode(operation, &replay, patch->logical_index, &before);
    if (rc == 0)
      found = kafs_v7_runtime_data_cow_patch_references(patch, &expected, &before, &retained);
  }
  kafs_v7_journal_replay_clear(&replay);
  return rc != 0 ? rc : found ? 0 : -EINVAL;
}

static int kafs_v7_runtime_data_cow_publish(kafs_v7_runtime_data_cow_t *operation,
                                            const kafs_v7_journal_patch_t *metadata_patches,
                                            size_t metadata_patch_count,
                                            kafs_v7_runtime_data_cow_result_t *result)
{
  int rc = kafs_v7_data_cow_verify_fd(operation->service->fd, &operation->plan,
                                      operation->staged_data, operation->plan.block_size);
  size_t patch_count = KAFS_V7_DATA_COW_ALLOCATOR_PATCH_COUNT + metadata_patch_count;
  kafs_v7_journal_patch_t *patches = NULL;
  if (rc == 0)
  {
    patches = calloc(patch_count, sizeof(*patches));
    if (!patches)
      rc = -ENOMEM;
  }
  if (rc == 0)
    rc = kafs_v7_data_cow_plan_patches(&operation->plan, patches);
  if (rc == 0)
  {
    memcpy(patches + KAFS_V7_DATA_COW_ALLOCATOR_PATCH_COUNT, metadata_patches,
           metadata_patch_count * sizeof(*metadata_patches));
    rc = kafs_v7_runtime_transaction_publish_reserved(operation->service, &operation->layout,
                                                      &operation->reservation, patches, patch_count,
                                                      &result->transaction);
  }
  free(patches);
  return rc;
}

static int kafs_v7_runtime_data_cow_finish(kafs_v7_runtime_data_cow_t *operation,
                                           kafs_v7_runtime_data_cow_result_t *result, int rc)
{
  if (rc == 0)
    kafs_v7_test_fault_maybe_crash(KAFS_V7_TEST_FAULT_JOURNAL_PUBLISH);
  if (rc == 0)
    rc = kafs_v7_runtime_transaction_closeout(operation->service, &result->transaction);
  if (operation->reservation.active)
  {
    int cancel_rc = kafs_v7_sequence_cancel_reservation_fd(
        operation->service->sequence, &operation->reservation, operation->service->fd,
        &operation->service->superblock, operation->service->file_size);
    if (cancel_rc != 0)
      rc = cancel_rc;
  }
  kafs_v7_runtime_data_cow_release(operation);
  return rc;
}

int kafs_v7_runtime_data_cow_commit(kafs_v7_runtime_data_cow_t **operation_ptr,
                                    const kafs_v7_journal_patch_t *metadata_patches,
                                    size_t metadata_patch_count,
                                    kafs_v7_runtime_data_cow_result_t *result)
{
  if (!operation_ptr || !*operation_ptr || !metadata_patches || metadata_patch_count == 0u ||
      metadata_patch_count > SIZE_MAX - KAFS_V7_DATA_COW_ALLOCATOR_PATCH_COUNT || !result)
    return -EINVAL;
  kafs_v7_runtime_data_cow_t *operation = *operation_ptr;
  if (!operation->reservation.active || !operation->staged || !operation->staged_data)
    return -EAGAIN;
  int rc = kafs_v7_runtime_data_cow_validate_direct_reference(operation, metadata_patches,
                                                              metadata_patch_count);
  if (rc != 0)
    return rc;
  *operation_ptr = NULL;
  memset(result, 0, sizeof(*result));
  kafs_v7_runtime_data_cow_plan_export(operation, &result->data);
  result->data_crc32 = operation->staged_crc32;
  rc = kafs_v7_runtime_data_cow_publish(operation, metadata_patches, metadata_patch_count, result);
  return kafs_v7_runtime_data_cow_finish(operation, result, rc);
}

int kafs_v7_runtime_data_cow_abort(kafs_v7_runtime_data_cow_t **operation_ptr)
{
  if (!operation_ptr || !*operation_ptr)
    return -EINVAL;
  kafs_v7_runtime_data_cow_t *operation = *operation_ptr;
  *operation_ptr = NULL;
  return kafs_v7_runtime_data_cow_cancel(operation);
}

static void kafs_v7_runtime_data_cow_batch_release(kafs_v7_runtime_data_cow_batch_t *operation)
{
  if (!operation)
    return;
  for (size_t i = 0; i < operation->plan_count; ++i)
  {
    if (operation->staged_data)
      free(operation->staged_data[i]);
    if (operation->plans)
      kafs_v7_data_cow_plan_clear(&operation->plans[i]);
  }
  free(operation->staged_data);
  free(operation->plans);
  kafs_v7_layout_report_clear(&operation->layout);
  free(operation);
}

static int kafs_v7_runtime_data_cow_batch_cancel(kafs_v7_runtime_data_cow_batch_t *operation)
{
  int rc = 0;
  if (operation->reservation.active)
    rc = kafs_v7_sequence_cancel_reservation_fd(
        operation->service->sequence, &operation->reservation, operation->service->fd,
        &operation->service->superblock, operation->service->file_size);
  kafs_v7_runtime_data_cow_batch_release(operation);
  return rc;
}

int kafs_v7_runtime_data_cow_batch_prepare(kafs_v7_runtime_transaction_service_t *service,
                                           const kafs_v7_runtime_data_cow_request_t *requests,
                                           size_t request_count,
                                           kafs_v7_runtime_data_cow_batch_t **operation_out,
                                           kafs_v7_runtime_data_cow_plan_t *plans_out)
{
  if (!service || !requests || request_count < 2u || !operation_out || !plans_out ||
      request_count > 64u || request_count > SIZE_MAX / sizeof(kafs_v7_data_cow_plan_request_t) ||
      request_count > SIZE_MAX / sizeof(kafs_v7_data_cow_plan_t) ||
      request_count > SIZE_MAX / sizeof(uint8_t *))
    return -EINVAL;
  *operation_out = NULL;
  kafs_v7_runtime_data_cow_batch_t *operation = calloc(1u, sizeof(*operation));
  kafs_v7_data_cow_plan_request_t *plan_requests = calloc(request_count, sizeof(*plan_requests));
  if (!operation || !plan_requests)
  {
    free(plan_requests);
    free(operation);
    return -ENOMEM;
  }
  operation->service = service;
  operation->plan_count = request_count;
  operation->plans = calloc(request_count, sizeof(*operation->plans));
  operation->staged_data = calloc(request_count, sizeof(*operation->staged_data));
  int rc = !operation->plans || !operation->staged_data ? -ENOMEM : 0;
  uint32_t group_id = requests[0].group_id;
  for (size_t i = 0; rc == 0 && i < request_count; ++i)
  {
    if (requests[i].reserved != 0u || requests[i].group_id != group_id ||
        group_id >= service->group_count)
      rc = -EXDEV;
  }
  if (rc == 0)
    rc = kafs_v7_sequence_reserve(service->sequence, group_id, &operation->reservation);
  if (rc == 0)
    rc = kafs_v7_validate_image_fd(service->fd, &service->superblock, service->file_size,
                                   &operation->layout);
  kafs_v7_journal_replay_t replay;
  memset(&replay, 0, sizeof(replay));
  if (rc == 0)
    rc = kafs_v7_journal_analyze_fd(service->fd, &operation->layout, &replay);
  uint64_t cursor = rc == 0 ? service->allocation_cursors[group_id] : 0u;
  for (size_t i = 0; rc == 0 && i < request_count; ++i)
  {
    plan_requests[i] = (kafs_v7_data_cow_plan_request_t){
        .layout = &operation->layout,
        .replay = &replay,
        .group_id = group_id,
        .allocation_cursor = cursor,
        .retained_logical_block = requests[i].retained_logical_block,
    };
  }
  if (rc == 0)
    rc =
        kafs_v7_data_cow_plan_batch_fd(service->fd, plan_requests, request_count, operation->plans);
  kafs_v7_journal_replay_clear(&replay);
  free(plan_requests);
  if (rc != 0)
  {
    int cancel_rc = kafs_v7_runtime_data_cow_batch_cancel(operation);
    return cancel_rc != 0 ? cancel_rc : rc;
  }
  for (size_t i = 0; i < request_count; ++i)
  {
    plans_out[i] = (kafs_v7_runtime_data_cow_plan_t){
        .group_id = group_id,
        .block_size = operation->plans[i].block_size,
        .sequence = operation->reservation.sequence,
        .logical_block = operation->plans[i].logical_block,
        .physical_off = operation->plans[i].physical_off,
        .retained_logical_block = operation->plans[i].retained_logical_block,
    };
  }
  const kafs_v7_group_desc_t *group = &kafs_v7_report_groups(&operation->layout)[group_id];
  uint64_t start = le64toh(group->data_logical_start);
  uint64_t count = le64toh(group->data_logical_count);
  uint64_t next = operation->plans[request_count - 1u].logical_block + 1u;
  service->allocation_cursors[group_id] = next >= start && next - start < count ? next : start;
  *operation_out = operation;
  return 0;
}

int kafs_v7_runtime_data_cow_batch_stage(kafs_v7_runtime_data_cow_batch_t *operation,
                                         size_t plan_index, const void *data, size_t data_bytes)
{
  if (!operation || plan_index >= operation->plan_count || !data ||
      operation->staged_data[plan_index])
    return -EINVAL;
  kafs_v7_data_cow_plan_t *plan = &operation->plans[plan_index];
  uint8_t *verified = malloc(plan->block_size);
  if (!verified)
    return -ENOMEM;
  int rc = kafs_v7_data_cow_stage_fd(operation->service->fd, plan, data, data_bytes, verified);
  if (rc == 0)
    operation->staged_data[plan_index] = verified;
  else
    free(verified);
  return rc;
}

int kafs_v7_runtime_data_cow_batch_commit(kafs_v7_runtime_data_cow_batch_t **operation_ptr,
                                          const kafs_v7_journal_patch_t *metadata_patches,
                                          size_t metadata_patch_count,
                                          kafs_v7_runtime_transaction_result_t *result)
{
  if (!operation_ptr || !*operation_ptr || !metadata_patches || metadata_patch_count == 0u ||
      !result)
    return -EINVAL;
  kafs_v7_runtime_data_cow_batch_t *operation = *operation_ptr;
  int rc = 0;
  size_t bitmap_count = 0u;
  for (size_t i = 0; rc == 0 && i < operation->plan_count; ++i)
  {
    if (!operation->staged_data[i])
      rc = -EAGAIN;
    if (rc == 0)
      rc = kafs_v7_data_cow_verify_fd(operation->service->fd, &operation->plans[i],
                                      operation->staged_data[i], operation->plans[i].block_size);
    kafs_v7_runtime_data_cow_t validator = {
        .service = operation->service, .layout = operation->layout, .plan = operation->plans[i]};
    if (rc == 0)
      rc = kafs_v7_runtime_data_cow_validate_direct_reference(&validator, metadata_patches,
                                                              metadata_patch_count);
    int last = 1;
    for (size_t j = i + 1u; j < operation->plan_count; ++j)
      if (operation->plans[j].bitmap_word_logical == operation->plans[i].bitmap_word_logical)
        last = 0;
    bitmap_count += last;
  }
  if (rc != 0)
    return rc;
  if (bitmap_count == SIZE_MAX || metadata_patch_count > SIZE_MAX - bitmap_count - 1u)
    return -EOVERFLOW;
  size_t patch_count = bitmap_count + 1u + metadata_patch_count;
  kafs_v7_journal_patch_t *patches = calloc(patch_count, sizeof(*patches));
  if (!patches)
    return -ENOMEM;
  size_t out = 0u;
  for (size_t i = 0; i < operation->plan_count; ++i)
  {
    int last = 1;
    for (size_t j = i + 1u; j < operation->plan_count; ++j)
      if (operation->plans[j].bitmap_word_logical == operation->plans[i].bitmap_word_logical)
        last = 0;
    if (!last)
      continue;
    int64_t free_blocks_delta = out == 0u ? -(int64_t)operation->plan_count : 0;
    patches[out++] = (kafs_v7_journal_patch_t){
        .target_type = KAFS_V7_JOURNAL_TARGET_BLOCK_BITMAP,
        .logical_index = operation->plans[i].bitmap_word_logical,
        .patch_bytes = sizeof(operation->plans[i].bitmap_after),
        .patch = operation->plans[i].bitmap_after,
        .free_blocks_delta = free_blocks_delta,
    };
  }
  kafs_v7_data_cow_plan_t *final = &operation->plans[operation->plan_count - 1u];
  patches[out++] = (kafs_v7_journal_patch_t){
      .target_type = KAFS_V7_JOURNAL_TARGET_ALLOCATOR_SUMMARY,
      .logical_index = final->allocator_logical,
      .patch_bytes = final->allocator_bytes,
      .patch = final->allocator_after,
  };
  memcpy(patches + out, metadata_patches, metadata_patch_count * sizeof(*metadata_patches));
  memset(result, 0, sizeof(*result));
  rc = kafs_v7_runtime_transaction_publish_reserved(operation->service, &operation->layout,
                                                    &operation->reservation, patches, patch_count,
                                                    result);
  free(patches);
  *operation_ptr = NULL;
  if (rc == 0)
    kafs_v7_test_fault_maybe_crash(KAFS_V7_TEST_FAULT_JOURNAL_PUBLISH);
  if (rc == 0)
    rc = kafs_v7_runtime_transaction_closeout(operation->service, result);
  if (operation->reservation.active)
  {
    int cancel_rc = kafs_v7_sequence_cancel_reservation_fd(
        operation->service->sequence, &operation->reservation, operation->service->fd,
        &operation->service->superblock, operation->service->file_size);
    if (cancel_rc != 0)
      rc = cancel_rc;
  }
  kafs_v7_runtime_data_cow_batch_release(operation);
  return rc;
}

int kafs_v7_runtime_data_cow_batch_abort(kafs_v7_runtime_data_cow_batch_t **operation_ptr)
{
  if (!operation_ptr || !*operation_ptr)
    return -EINVAL;
  kafs_v7_runtime_data_cow_batch_t *operation = *operation_ptr;
  *operation_ptr = NULL;
  return kafs_v7_runtime_data_cow_batch_cancel(operation);
}

static int kafs_v7_runtime_group_shard(const kafs_v7_layout_report_t *layout, uint32_t group_id,
                                       uint16_t type, const kafs_v7_shard_desc_t **shard_out)
{
  const kafs_v7_group_desc_t *groups = kafs_v7_report_groups(layout);
  const kafs_v7_shard_desc_t *shards = kafs_v7_report_shards(layout);
  if (!groups || !shards || group_id >= layout->group_count || !shard_out)
    return -EINVAL;
  uint32_t first = le32toh(groups[group_id].first_shard_index);
  uint32_t count = le32toh(groups[group_id].shard_count);
  if (le32toh(groups[group_id].group_id) != group_id || first > layout->shard_count ||
      count > layout->shard_count - first)
    return -EUCLEAN;
  const kafs_v7_shard_desc_t *found = NULL;
  for (uint32_t local = 0; local < count; ++local)
  {
    const kafs_v7_shard_desc_t *candidate = &shards[first + local];
    if (le16toh(candidate->type) != type)
      continue;
    if (found || le32toh(candidate->group_id) != group_id)
      return -EUCLEAN;
    found = candidate;
  }
  if (!found)
    return -EUCLEAN;
  *shard_out = found;
  return 0;
}

typedef struct kafs_v7_runtime_data_retirement_scan
{
  const kafs_v7_runtime_transaction_service_t *service;
  const kafs_v7_layout_report_t *layout;
  const kafs_v7_journal_replay_t *replay;
  uint32_t expected;
  int has_indirect;
} kafs_v7_runtime_data_retirement_scan_t;

static int
kafs_v7_runtime_data_retirement_read_shard(const kafs_v7_runtime_data_retirement_scan_t *scan,
                                           uint32_t group_id, uint16_t type, size_t record_bytes,
                                           void **records, uint64_t *record_count)
{
  if (!scan || record_bytes == 0u || !records || !record_count)
    return -EINVAL;
  *records = NULL;
  *record_count = 0u;
  const kafs_v7_shard_desc_t *shard = NULL;
  int rc = kafs_v7_runtime_group_shard(scan->layout, group_id, type, &shard);
  uint64_t count = rc == 0 ? le64toh(shard->logical_count) : 0u;
  if (rc == 0 && (count == 0u || count > SIZE_MAX / record_bytes))
    rc = -EUCLEAN;
  size_t bytes = rc == 0 ? (size_t)count * record_bytes : 0u;
  void *loaded = rc == 0 ? malloc(bytes) : NULL;
  if (rc == 0 && !loaded)
    rc = -ENOMEM;
  if (rc == 0)
  {
    rc = kafs_v7_journal_overlay_pread(scan->replay, scan->service->fd, loaded, bytes,
                                       le64toh(shard->physical_off));
  }
  if (rc != 0)
  {
    free(loaded);
    return rc;
  }
  *records = loaded;
  *record_count = count;
  return 0;
}

static int kafs_v7_runtime_data_retirement_scan_inodes(kafs_v7_runtime_data_retirement_scan_t *scan,
                                                       uint32_t group_id)
{
  kafs_v7_inode_t *inodes = NULL;
  uint64_t count = 0u;
  int rc = kafs_v7_runtime_data_retirement_read_shard(scan, group_id, KAFS_V7_SHARD_INODE_TABLE,
                                                      sizeof(*inodes), (void **)&inodes, &count);
  for (uint64_t inode = 0; rc == 0 && inode < count; ++inode)
  {
    if (le16toh(inodes[inode].mode) == 0u || le64toh(inodes[inode].size) <= 60u)
      continue;
    for (uint32_t slot = 0; slot < 15u; ++slot)
    {
      uint32_t reference = 0u;
      memcpy(&reference, inodes[inode].inline_or_block_refs + slot * sizeof(reference),
             sizeof(reference));
      reference = le32toh(reference);
      if (reference == scan->expected)
      {
        rc = -EBUSY;
        break;
      }
      if (slot >= 12u && reference != 0u)
        scan->has_indirect = 1;
    }
  }
  free(inodes);
  return rc;
}

static int
kafs_v7_runtime_data_retirement_scan_hrl(const kafs_v7_runtime_data_retirement_scan_t *scan,
                                         uint32_t group_id)
{
  kafs_v7_hrl_entry_t *entries = NULL;
  uint64_t count = 0u;
  int rc = kafs_v7_runtime_data_retirement_read_shard(scan, group_id, KAFS_V7_SHARD_HRL_ENTRIES,
                                                      sizeof(*entries), (void **)&entries, &count);
  for (uint64_t entry = 0; rc == 0 && entry < count; ++entry)
  {
    if (le32toh(entries[entry].ref_count) != 0u &&
        le32toh(entries[entry].logical_block_plus1) == scan->expected)
      rc = -EBUSY;
  }
  free(entries);
  return rc;
}

static int kafs_v7_runtime_data_retirement_validate_unreferenced(
    const kafs_v7_runtime_transaction_service_t *service, const kafs_v7_layout_report_t *layout,
    const kafs_v7_journal_replay_t *replay, uint64_t logical_block)
{
  if (logical_block >= UINT32_MAX)
    return -ERANGE;
  kafs_v7_runtime_data_retirement_scan_t scan = {
      .service = service,
      .layout = layout,
      .replay = replay,
      .expected = (uint32_t)logical_block + 1u,
  };
  int rc = 0;
  for (uint32_t group_id = 0; rc == 0 && group_id < layout->group_count; ++group_id)
    rc = kafs_v7_runtime_data_retirement_scan_inodes(&scan, group_id);
  for (uint32_t group_id = 0; rc == 0 && group_id < layout->group_count; ++group_id)
    rc = kafs_v7_runtime_data_retirement_scan_hrl(&scan, group_id);
  return rc != 0 ? rc : scan.has_indirect ? -EOPNOTSUPP : 0;
}

static int kafs_v7_runtime_data_retirement_publish(kafs_v7_runtime_transaction_service_t *service,
                                                   const kafs_v7_layout_report_t *layout,
                                                   kafs_v7_sequence_reservation_t *reservation,
                                                   const kafs_v7_data_cow_plan_t *plan,
                                                   kafs_v7_runtime_transaction_result_t *result)
{
  kafs_v7_journal_patch_t patches[KAFS_V7_DATA_COW_ALLOCATOR_PATCH_COUNT];
  int rc = kafs_v7_data_cow_plan_patches(plan, patches);
  if (rc == 0)
  {
    rc = kafs_v7_runtime_transaction_publish_reserved(
        service, layout, reservation, patches, KAFS_V7_DATA_COW_ALLOCATOR_PATCH_COUNT, result);
  }
  return rc;
}

static int kafs_v7_runtime_data_retirement_prepare(
    kafs_v7_runtime_transaction_service_t *service,
    const kafs_v7_runtime_data_retirement_request_t *request,
    kafs_v7_sequence_reservation_t *reservation, kafs_v7_layout_report_t *layout,
    kafs_v7_journal_replay_t *replay, kafs_v7_data_cow_plan_t *plan)
{
  int rc = kafs_v7_sequence_reserve(service->sequence, request->group_id, reservation);
  if (rc == 0)
    rc = kafs_v7_validate_image_fd(service->fd, &service->superblock, service->file_size, layout);
  if (rc == 0 && (layout->journal.selected_nonempty_segment_count != 0u ||
                  kafs_v7_layout_checkpoint_copy_count(layout) < 2u))
    rc = -EUCLEAN;
  if (rc == 0)
    rc = kafs_v7_journal_analyze_fd(service->fd, layout, replay);
  if (rc == 0)
  {
    kafs_v7_data_retirement_plan_request_t plan_request = {
        .layout = layout,
        .replay = replay,
        .group_id = request->group_id,
        .logical_block = request->logical_block,
    };
    rc = kafs_v7_data_retirement_plan_fd(service->fd, &plan_request, plan);
  }
  if (rc == 0)
  {
    rc = kafs_v7_runtime_data_retirement_validate_unreferenced(service, layout, replay,
                                                               request->logical_block);
  }
  return rc;
}

int kafs_v7_runtime_data_retire(kafs_v7_runtime_transaction_service_t *service,
                                const kafs_v7_runtime_data_retirement_request_t *request,
                                kafs_v7_runtime_data_retirement_result_t *result)
{
  if (!service || !request || request->reserved != 0u || !result ||
      request->group_id >= service->group_count)
    return -EINVAL;
  memset(result, 0, sizeof(*result));
  kafs_v7_runtime_transaction_result_t preflight;
  memset(&preflight, 0, sizeof(preflight));
  int rc = kafs_v7_runtime_transaction_closeout(service, &preflight);

  kafs_v7_sequence_reservation_t reservation;
  kafs_v7_layout_report_t layout;
  kafs_v7_journal_replay_t replay;
  kafs_v7_data_cow_plan_t plan;
  memset(&reservation, 0, sizeof(reservation));
  memset(&layout, 0, sizeof(layout));
  memset(&replay, 0, sizeof(replay));
  memset(&plan, 0, sizeof(plan));
  if (rc == 0)
    rc = kafs_v7_runtime_data_retirement_prepare(service, request, &reservation, &layout, &replay,
                                                 &plan);
  if (rc == 0)
  {
    result->group_id = request->group_id;
    result->logical_block = request->logical_block;
    rc = kafs_v7_runtime_data_retirement_publish(service, &layout, &reservation, &plan,
                                                 &result->transaction);
  }
  if (reservation.active)
  {
    int cancel_rc = kafs_v7_sequence_cancel_reservation_fd(
        service->sequence, &reservation, service->fd, &service->superblock, service->file_size);
    if (cancel_rc != 0)
      rc = cancel_rc;
  }
  kafs_v7_data_cow_plan_clear(&plan);
  kafs_v7_journal_replay_clear(&replay);
  kafs_v7_layout_report_clear(&layout);
  if (rc == 0)
    rc = kafs_v7_runtime_transaction_closeout(service, &result->transaction);
  return rc;
}

int kafs_v7_runtime_transaction_barrier(kafs_v7_runtime_transaction_service_t *service,
                                        kafs_v7_runtime_transaction_result_t *result)
{
  if (!service || !result)
    return -EINVAL;
  memset(result, 0, sizeof(*result));
  return kafs_v7_runtime_transaction_closeout(service, result);
}

int kafs_v7_runtime_transaction_barrier_context(kafs_context_t *ctx,
                                                kafs_v7_runtime_transaction_result_t *result)
{
  if (!ctx || !ctx->c_v7_runtime_transactions)
    return -EPROTO;
  int rc = kafs_v7_runtime_transaction_barrier(ctx->c_v7_runtime_transactions, result);
  if (rc == 0)
  {
    ctx->c_v7_checkpoint_generation = result->checkpoint_generation;
    ctx->c_v7_checkpoint_sequence = result->checkpoint_sequence;
    ctx->c_v7_recovered_free_blocks = result->recovered_free_blocks;
    ctx->c_v7_recovered_free_inodes = result->recovered_free_inodes;
  }
  return rc;
}
