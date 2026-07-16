#pragma once

#include "kafs_v7_layout.h"
#include "kafs_v7_locks.h"

#include <stdint.h>

typedef struct kafs_v7_sequence_state kafs_v7_sequence_state_t;

typedef struct kafs_v7_sequence_reservation
{
  uint64_t sequence;
  uint64_t token;
  uint32_t group_id;
  uint8_t active;
} kafs_v7_sequence_reservation_t;

/* The lock state must outlive the sequence state. */
int kafs_v7_sequence_state_init(kafs_v7_lock_state_t *locks, const kafs_v7_layout_report_t *layout,
                                kafs_v7_sequence_state_t **state_out);

/* No reservation may be active when the sequence state is destroyed. */
void kafs_v7_sequence_state_destroy(kafs_v7_sequence_state_t *state);

/*
 * Reserve the next filesystem-global sequence while retaining the composite
 * transaction lock for exactly one group. The same thread must confirm or
 * cancel the reservation.
 */
int kafs_v7_sequence_reserve(kafs_v7_sequence_state_t *state, uint32_t group_id,
                             kafs_v7_sequence_reservation_t *reservation);

/*
 * Revalidate the image and consume the reservation only if a durable selected
 * journal header exposes its exact sequence.
 */
int kafs_v7_sequence_confirm_publication_fd(kafs_v7_sequence_state_t *state,
                                            kafs_v7_sequence_reservation_t *reservation, int fd,
                                            const kafs_ssuperblock_t *sb, uint64_t file_size);

/*
 * Cancel before header publication. The image is revalidated to prove that
 * the reserved sequence is still absent and may therefore be reused.
 */
int kafs_v7_sequence_cancel_reservation_fd(kafs_v7_sequence_state_t *state,
                                           kafs_v7_sequence_reservation_t *reservation, int fd,
                                           const kafs_ssuperblock_t *sb, uint64_t file_size);
