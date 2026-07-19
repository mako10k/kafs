#pragma once

#include "kafs_context.h"
#include "kafs_v7_checkpoint.h"
#include "kafs_v7_journal_writer.h"

#include <stddef.h>
#include <stdint.h>

#define KAFS_V7_RUNTIME_DATA_COW_NO_BLOCK UINT64_MAX

typedef struct kafs_v7_runtime_transaction_service kafs_v7_runtime_transaction_service_t;
typedef struct kafs_v7_runtime_data_cow kafs_v7_runtime_data_cow_t;
typedef struct kafs_v7_runtime_data_cow_batch kafs_v7_runtime_data_cow_batch_t;

typedef struct kafs_v7_runtime_transaction_result
{
  kafs_v7_journal_publication_t publication;
  kafs_v7_metadata_closeout_result_t closeout;
  uint64_t checkpoint_generation;
  uint64_t checkpoint_sequence;
  uint64_t recovered_free_blocks;
  uint64_t recovered_free_inodes;
} kafs_v7_runtime_transaction_result_t;

typedef struct kafs_v7_runtime_data_cow_request
{
  uint32_t group_id;
  uint32_t reserved;
  uint64_t retained_logical_block;
} kafs_v7_runtime_data_cow_request_t;

typedef struct kafs_v7_runtime_data_cow_plan
{
  uint32_t group_id;
  uint32_t block_size;
  uint64_t sequence;
  uint64_t logical_block;
  uint64_t physical_off;
  uint64_t retained_logical_block;
} kafs_v7_runtime_data_cow_plan_t;

typedef struct kafs_v7_runtime_data_cow_result
{
  kafs_v7_runtime_data_cow_plan_t data;
  uint32_t data_crc32;
  uint32_t reserved;
  kafs_v7_runtime_transaction_result_t transaction;
} kafs_v7_runtime_data_cow_result_t;

typedef struct kafs_v7_runtime_data_retirement_request
{
  uint32_t group_id;
  uint32_t reserved;
  uint64_t logical_block;
} kafs_v7_runtime_data_retirement_request_t;

typedef struct kafs_v7_runtime_data_retirement_result
{
  uint32_t group_id;
  uint32_t reserved;
  uint64_t logical_block;
  kafs_v7_runtime_transaction_result_t transaction;
} kafs_v7_runtime_data_retirement_result_t;

/*
 * Own the mount-lifetime rank 1-3 lock and global-sequence state. The caller
 * retains ownership of fd and must stop transaction users before destroy.
 */
int kafs_v7_runtime_transaction_service_init(int fd, const kafs_ssuperblock_t *superblock,
                                             uint64_t file_size,
                                             kafs_v7_runtime_transaction_service_t **service_out);
void kafs_v7_runtime_transaction_service_destroy(kafs_v7_runtime_transaction_service_t *service);

/*
 * Accept exactly one group's metadata patches and return only after journal
 * publication, metadata apply, two-copy checkpoint, and covered reclamation.
 */
int kafs_v7_runtime_transaction_commit(kafs_v7_runtime_transaction_service_t *service,
                                       const kafs_v7_journal_patch_t *patches, size_t patch_count,
                                       kafs_v7_runtime_transaction_result_t *result);

/*
 * Hold one group transaction reservation from allocation planning through
 * durable data staging and metadata publication. prepare never modifies
 * metadata. stage writes, flushes, and verifies data. commit requires a
 * caller-owned direct inode-reference patch naming the planned block and
 * always consumes *operation after precondition validation.
 * The old block, when supplied, remains allocated after the covering
 * checkpoint and is returned for a later retirement transaction.
 */
int kafs_v7_runtime_data_cow_prepare(kafs_v7_runtime_transaction_service_t *service,
                                     const kafs_v7_runtime_data_cow_request_t *request,
                                     kafs_v7_runtime_data_cow_t **operation,
                                     kafs_v7_runtime_data_cow_plan_t *plan);
int kafs_v7_runtime_data_cow_stage(kafs_v7_runtime_data_cow_t *operation, const void *data,
                                   size_t data_bytes);
int kafs_v7_runtime_data_cow_commit(kafs_v7_runtime_data_cow_t **operation,
                                    const kafs_v7_journal_patch_t *metadata_patches,
                                    size_t metadata_patch_count,
                                    kafs_v7_runtime_data_cow_result_t *result);
int kafs_v7_runtime_data_cow_abort(kafs_v7_runtime_data_cow_t **operation);

int kafs_v7_runtime_data_cow_batch_prepare(kafs_v7_runtime_transaction_service_t *service,
                                           const kafs_v7_runtime_data_cow_request_t *requests,
                                           size_t request_count,
                                           kafs_v7_runtime_data_cow_batch_t **operation,
                                           kafs_v7_runtime_data_cow_plan_t *plans);
int kafs_v7_runtime_data_cow_batch_stage(kafs_v7_runtime_data_cow_batch_t *operation,
                                         size_t plan_index, const void *data, size_t data_bytes);
int kafs_v7_runtime_data_cow_batch_commit(kafs_v7_runtime_data_cow_batch_t **operation,
                                          const kafs_v7_journal_patch_t *metadata_patches,
                                          size_t metadata_patch_count,
                                          kafs_v7_runtime_transaction_result_t *result);
int kafs_v7_runtime_data_cow_batch_abort(kafs_v7_runtime_data_cow_batch_t **operation);

/*
 * Retire one allocated, unreferenced direct-only block after first closing any
 * durable journal prefix. A live direct/HRL reference returns EBUSY; any
 * indirect root returns EOPNOTSUPP until indirect traversal is implemented.
 */
int kafs_v7_runtime_data_retire(kafs_v7_runtime_transaction_service_t *service,
                                const kafs_v7_runtime_data_retirement_request_t *request,
                                kafs_v7_runtime_data_retirement_result_t *result);

/* Finish any durable journal prefix without publishing a new transaction. */
int kafs_v7_runtime_transaction_barrier(kafs_v7_runtime_transaction_service_t *service,
                                        kafs_v7_runtime_transaction_result_t *result);
int kafs_v7_runtime_transaction_barrier_context(kafs_context_t *ctx,
                                                kafs_v7_runtime_transaction_result_t *result);
