#include "kafs_v7_runtime_transaction.h"

#include "kafs_v7_io.h"
#include "kafs_v7_mutation.h"
#include "kafs_v7_sequence.h"

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
};

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
  {
    service->fd = fd;
    service->file_size = file_size;
    service->superblock = *superblock;
    *service_out = service;
    service = NULL;
  }
  if (service)
  {
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
  kafs_v7_journal_transaction_t *transaction = NULL;
  if (rc == 0)
    rc = kafs_v7_journal_transaction_encode_fd(service->fd, &layout, &reservation, patches,
                                               patch_count, KAFS_V7_JOURNAL_COMMIT_TAG,
                                               &transaction);
  if (rc == 0)
    rc = kafs_v7_journal_transaction_publish_fd(service->fd, &layout, &reservation, transaction,
                                                &result->publication);
  if (rc == 0)
  {
    rc = kafs_v7_sequence_confirm_publication_fd(service->sequence, &reservation, service->fd,
                                                 &service->superblock, service->file_size);
  }
  else if (reservation.active)
  {
    int cancel_rc = kafs_v7_sequence_cancel_reservation_fd(
        service->sequence, &reservation, service->fd, &service->superblock, service->file_size);
    if (cancel_rc != 0)
      rc = cancel_rc;
  }
  kafs_v7_journal_transaction_destroy(transaction);
  kafs_v7_layout_report_clear(&layout);
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
  {
    rc = kafs_v7_metadata_closeout_fd(service->locks, service->fd, &service->superblock,
                                      service->file_size, &result->closeout);
  }
  if (rc == 0)
    rc = kafs_v7_runtime_transaction_refresh(service, result);
  return rc;
}

int kafs_v7_runtime_transaction_barrier(kafs_v7_runtime_transaction_service_t *service,
                                        kafs_v7_runtime_transaction_result_t *result)
{
  if (!service || !result)
    return -EINVAL;
  memset(result, 0, sizeof(*result));
  int rc = kafs_v7_metadata_closeout_fd(service->locks, service->fd, &service->superblock,
                                        service->file_size, &result->closeout);
  if (rc == 0)
    rc = kafs_v7_runtime_transaction_refresh(service, result);
  return rc;
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
