#include "kafs_v7_locks.h"

#include <errno.h>
#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>

typedef struct lock_worker
{
  kafs_v7_lock_state_t *state;
  uint32_t group_id;
  int rc;
} lock_worker_t;

static void *transaction_worker(void *opaque)
{
  lock_worker_t *worker = (lock_worker_t *)opaque;
  worker->rc = kafs_v7_transaction_lock(worker->state, worker->group_id);
  if (worker->rc == 0)
    worker->rc = kafs_v7_transaction_unlock(worker->state, worker->group_id);
  return NULL;
}

static void *abandoned_checkpoint_worker(void *opaque)
{
  lock_worker_t *worker = (lock_worker_t *)opaque;
  worker->rc = kafs_v7_checkpoint_lock(worker->state);
  return NULL;
}

static int test_order_and_stack_guards(void)
{
  kafs_v7_lock_state_t *state = NULL;
  int rc = kafs_v7_locks_init(4u, 500u, &state);
  if (rc != 0)
    return rc;
  int transaction_locked = 0;
  int checkpoint_locked = 0;
  if (kafs_v7_transaction_lock(state, 4u) != -EINVAL ||
      kafs_v7_transaction_unlock(state, 4u) != -EINVAL)
    rc = -1;
  if (rc == 0)
  {
    rc = kafs_v7_transaction_lock(state, 2u);
    transaction_locked = rc == 0;
  }
  if (rc == 0 && (kafs_v7_transaction_lock(state, 3u) != -EDEADLK ||
                  kafs_v7_checkpoint_lock(state) != -EDEADLK ||
                  kafs_v7_transaction_unlock(state, 1u) != -EPERM))
    rc = -1;
  if (transaction_locked)
  {
    int unlock_rc = kafs_v7_transaction_unlock(state, 2u);
    transaction_locked = unlock_rc != 0;
    if (unlock_rc != 0)
      rc = -1;
  }
  if (rc == 0)
  {
    rc = kafs_v7_checkpoint_lock(state);
    checkpoint_locked = rc == 0;
  }
  if (checkpoint_locked)
  {
    int unlock_rc = kafs_v7_checkpoint_unlock(state);
    checkpoint_locked = unlock_rc != 0;
    if (unlock_rc != 0)
      rc = -1;
  }
  if (rc == 0 && kafs_v7_checkpoint_unlock(state) != -EPERM)
    rc = -1;
  kafs_v7_lock_stats_t stats;
  if (rc == 0 &&
      (kafs_v7_locks_get_stats(state, &stats) != 0 || stats.write_gate_acquisitions != 2u ||
       stats.sequence_acquisitions != 1u || stats.group_acquisitions != 1u))
    rc = -1;
  if (transaction_locked || checkpoint_locked)
    return -1;
  kafs_v7_locks_destroy(state);
  return rc;
}

static int test_checkpoint_transaction_contention(void)
{
  kafs_v7_lock_state_t *state = NULL;
  int rc = kafs_v7_locks_init(2u, 1000u, &state);
  if (rc != 0 || kafs_v7_checkpoint_lock(state) != 0)
  {
    kafs_v7_locks_destroy(state);
    return -1;
  }
  lock_worker_t worker = {.state = state, .group_id = 1u, .rc = -1};
  pthread_t thread;
  if (pthread_create(&thread, NULL, transaction_worker, &worker) != 0)
  {
    (void)kafs_v7_checkpoint_unlock(state);
    kafs_v7_locks_destroy(state);
    return -1;
  }
  usleep(50000u);
  rc = kafs_v7_checkpoint_unlock(state);
  if (pthread_join(thread, NULL) != 0 || worker.rc != 0)
    rc = -1;
  kafs_v7_lock_stats_t stats;
  if (rc == 0 &&
      (kafs_v7_locks_get_stats(state, &stats) != 0 || stats.contended_acquisitions == 0u ||
       stats.wait_ns < UINT64_C(10000000)))
    rc = -1;
  kafs_v7_locks_destroy(state);
  return rc;
}

static int test_bounded_timeout(void)
{
  kafs_v7_lock_state_t *state = NULL;
  int rc = kafs_v7_locks_init(1u, 50u, &state);
  if (rc != 0 || kafs_v7_checkpoint_lock(state) != 0)
  {
    kafs_v7_locks_destroy(state);
    return -1;
  }
  lock_worker_t worker = {.state = state, .group_id = 0u, .rc = 0};
  pthread_t thread;
  if (pthread_create(&thread, NULL, transaction_worker, &worker) != 0)
  {
    (void)kafs_v7_checkpoint_unlock(state);
    kafs_v7_locks_destroy(state);
    return -1;
  }
  if (pthread_join(thread, NULL) != 0 || worker.rc != -ETIMEDOUT)
    rc = -1;
  if (kafs_v7_checkpoint_unlock(state) != 0)
    rc = -1;
  kafs_v7_locks_destroy(state);
  return rc;
}

static int test_owner_dead_recovery(void)
{
  kafs_v7_lock_state_t *state = NULL;
  int rc = kafs_v7_locks_init(1u, 500u, &state);
  if (rc != 0)
    return rc;
  lock_worker_t worker = {.state = state, .group_id = 0u, .rc = -1};
  pthread_t thread;
  if (pthread_create(&thread, NULL, abandoned_checkpoint_worker, &worker) != 0)
  {
    kafs_v7_locks_destroy(state);
    return -1;
  }
  if (pthread_join(thread, NULL) != 0 || worker.rc != 0)
    rc = -1;
  if (rc == 0)
  {
    int recovery_rc = kafs_v7_checkpoint_lock(state);
    if (recovery_rc == 0)
      (void)kafs_v7_checkpoint_unlock(state);
    if (recovery_rc != -EOWNERDEAD)
      rc = -1;
  }
  int checkpoint_locked = 0;
  if (rc == 0)
  {
    int lock_rc = kafs_v7_checkpoint_lock(state);
    checkpoint_locked = lock_rc == 0;
    if (lock_rc != 0)
      rc = -1;
  }
  if (checkpoint_locked && kafs_v7_checkpoint_unlock(state) != 0)
    return -1;
  kafs_v7_locks_destroy(state);
  return rc;
}

int main(void)
{
  int rc = test_order_and_stack_guards();
  if (rc == 0)
    rc = test_checkpoint_transaction_contention();
  if (rc == 0)
    rc = test_bounded_timeout();
  if (rc == 0)
    rc = test_owner_dead_recovery();
  if (rc != 0)
    fprintf(stderr, "v7 locks smoke test failed: %d\n", rc);
  return rc == 0 ? 0 : 1;
}
