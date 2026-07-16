#pragma once

#include <stdint.h>

#define KAFS_V7_LOCK_RANK_WRITE_GATE 1u
#define KAFS_V7_LOCK_RANK_SEQUENCE 2u
#define KAFS_V7_LOCK_RANK_GROUP 3u
#define KAFS_V7_LOCK_DEFAULT_TIMEOUT_MS 5000u

typedef struct kafs_v7_lock_state kafs_v7_lock_state_t;

typedef struct kafs_v7_lock_stats
{
  uint64_t write_gate_acquisitions;
  uint64_t sequence_acquisitions;
  uint64_t group_acquisitions;
  uint64_t contended_acquisitions;
  uint64_t wait_ns;
} kafs_v7_lock_stats_t;

int kafs_v7_locks_init(uint32_t group_count, uint32_t timeout_ms, kafs_v7_lock_state_t **state_out);

/* All users must be stopped and joined before destroying the lock state. */
void kafs_v7_locks_destroy(kafs_v7_lock_state_t *state);

/*
 * Transactions acquire rank 1 -> 2 -> 3 and release in reverse order.
 * The composite API deliberately permits exactly one group per transaction.
 */
int kafs_v7_transaction_lock(kafs_v7_lock_state_t *state, uint32_t group_id);
int kafs_v7_transaction_unlock(kafs_v7_lock_state_t *state, uint32_t group_id);

/* Checkpoint publication holds rank 1, excluding all transactions. */
int kafs_v7_checkpoint_lock(kafs_v7_lock_state_t *state);
int kafs_v7_checkpoint_unlock(kafs_v7_lock_state_t *state);

int kafs_v7_locks_get_stats(const kafs_v7_lock_state_t *state, kafs_v7_lock_stats_t *stats);
