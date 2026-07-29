#pragma once

#include "kafs_v7_layout.h"
#include "kafs_v7_sequence.h"

#include <stddef.h>
#include <stdint.h>

typedef struct kafs_v7_journal_transaction kafs_v7_journal_transaction_t;

typedef struct kafs_v7_journal_patch
{
  uint16_t target_type;
  uint16_t reserved;
  uint64_t logical_index;
  uint32_t patch_off;
  uint32_t patch_bytes;
  const void *patch;
  int64_t free_blocks_delta;
  int64_t free_inodes_delta;
} kafs_v7_journal_patch_t;

typedef struct kafs_v7_journal_publication
{
  uint64_t sequence;
  uint64_t data_off;
  uint64_t header_off;
  uint64_t previous_write_bytes;
  uint64_t published_write_bytes;
  uint64_t header_generation;
  size_t transaction_bytes;
  uint32_t group_id;
  uint32_t local_segment;
  uint32_t segment_id;
  uint32_t header_slot;
} kafs_v7_journal_publication_t;

/*
 * Encode against the journal-overlay view while the reservation retains the
 * rank 1 -> 2 -> 3 transaction lock. Only COMMIT and ABORT terminal tags are
 * accepted. The transaction remains caller-owned until destroy.
 */
int kafs_v7_journal_transaction_encode_fd(int fd, const kafs_v7_layout_report_t *layout,
                                          const kafs_v7_sequence_reservation_t *reservation,
                                          const kafs_v7_journal_patch_t *patches,
                                          size_t patch_count, uint32_t terminal_tag,
                                          kafs_v7_journal_transaction_t **transaction_out);
void kafs_v7_journal_transaction_destroy(kafs_v7_journal_transaction_t *transaction);

/*
 * Revalidate the durable prefix, append transaction data, flush it, then
 * rotate and flush one header slot. Metadata targets are not modified.
 */
int kafs_v7_journal_transaction_publish_fd(int fd, const kafs_v7_layout_report_t *layout,
                                           const kafs_v7_sequence_reservation_t *reservation,
                                           const kafs_v7_journal_transaction_t *transaction,
                                           kafs_v7_journal_publication_t *publication);
