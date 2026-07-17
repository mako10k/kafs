#include "kafs_v7_runtime_transaction.h"

#include "kafs_v7_data_cow.h"
#include "kafs_v7_io.h"
#include "kafs_v7_mutation.h"
#include "kafs_v7_sequence.h"

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
