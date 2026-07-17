#pragma once

#include "kafs_context.h"
#include "kafs_v7_checkpoint.h"
#include "kafs_v7_journal_writer.h"

#include <stddef.h>
#include <stdint.h>

typedef struct kafs_v7_runtime_transaction_service kafs_v7_runtime_transaction_service_t;

typedef struct kafs_v7_runtime_transaction_result
{
  kafs_v7_journal_publication_t publication;
  kafs_v7_metadata_closeout_result_t closeout;
  uint64_t checkpoint_generation;
  uint64_t checkpoint_sequence;
  uint64_t recovered_free_blocks;
  uint64_t recovered_free_inodes;
} kafs_v7_runtime_transaction_result_t;

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

/* Finish any durable journal prefix without publishing a new transaction. */
int kafs_v7_runtime_transaction_barrier(kafs_v7_runtime_transaction_service_t *service,
                                        kafs_v7_runtime_transaction_result_t *result);
int kafs_v7_runtime_transaction_barrier_context(kafs_context_t *ctx,
                                                kafs_v7_runtime_transaction_result_t *result);
