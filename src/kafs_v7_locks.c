#include "kafs_v7_locks.h"

#include "kafs_lock_order.h"

#include <errno.h>
#include <inttypes.h>
#include <pthread.h>
#include <signal.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#ifdef __linux__
#include <sys/syscall.h>
#endif

#define KAFS_V7_LOCK_POLL_MS 200u

typedef struct kafs_v7_mutex
{
  pthread_mutex_t mutex;
  const char *name;
  uint32_t rank;
  uint32_t group_id;
  uint64_t owner_tid;
} kafs_v7_mutex_t;

struct kafs_v7_lock_state
{
  kafs_v7_mutex_t write_gate;
  kafs_v7_mutex_t sequence;
  kafs_v7_mutex_t *groups;
  uint32_t group_count;
  uint32_t timeout_ms;
  kafs_v7_lock_stats_t stats;
};

static _Thread_local uint32_t g_v7_cancel_depth;
static _Thread_local int g_v7_cancel_oldstate = PTHREAD_CANCEL_ENABLE;
#ifndef __linux__
static uint32_t g_v7_robust_warning_emitted;
#endif

static uint64_t kafs_v7_lock_tid(void)
{
#ifdef __linux__
  return (uint64_t)syscall(SYS_gettid);
#else
  return (uint64_t)getpid();
#endif
}

static int kafs_v7_lock_now_ns(uint64_t *now_ns)
{
  if (!now_ns)
    return -EINVAL;
  struct timespec now;
  if (clock_gettime(CLOCK_MONOTONIC, &now) != 0)
    return -errno;
  *now_ns = (uint64_t)now.tv_sec * UINT64_C(1000000000) + (uint64_t)now.tv_nsec;
  return 0;
}

static void kafs_v7_lock_timespec_add_ms(struct timespec *time, uint32_t milliseconds)
{
  time->tv_sec += (time_t)(milliseconds / 1000u);
  time->tv_nsec += (long)(milliseconds % 1000u) * 1000000L;
  if (time->tv_nsec >= 1000000000L)
  {
    ++time->tv_sec;
    time->tv_nsec -= 1000000000L;
  }
}

static int kafs_v7_lock_owner_alive(uint64_t tid)
{
#ifdef __linux__
  if (tid == 0u || tid == kafs_v7_lock_tid())
    return 1;
  if (kill((pid_t)tid, 0) == 0)
    return 1;
  return errno != ESRCH;
#else
  (void)tid;
  return 1;
#endif
}

static int kafs_v7_lock_stack_can_push(const kafs_v7_mutex_t *lock)
{
  if (!lock)
    return -EINVAL;
  return kafs_lock_order_can_acquire(lock->rank, 0);
}

static void kafs_v7_lock_cancel_enter(void)
{
  if (g_v7_cancel_depth == 0u)
  {
    int oldstate = PTHREAD_CANCEL_ENABLE;
    (void)pthread_setcancelstate(PTHREAD_CANCEL_DISABLE, &oldstate);
    g_v7_cancel_oldstate = oldstate;
  }
  ++g_v7_cancel_depth;
}

static void kafs_v7_lock_cancel_leave(void)
{
  if (g_v7_cancel_depth == 0u)
    return;
  --g_v7_cancel_depth;
  if (g_v7_cancel_depth == 0u)
    (void)pthread_setcancelstate(g_v7_cancel_oldstate, NULL);
}

static int kafs_v7_mutex_init(kafs_v7_mutex_t *lock, const char *name, uint32_t rank,
                              uint32_t group_id)
{
  if (!lock || !name)
    return -EINVAL;
  memset(lock, 0, sizeof(*lock));
  lock->name = name;
  lock->rank = rank;
  lock->group_id = group_id;
  pthread_mutexattr_t attr;
  int rc = pthread_mutexattr_init(&attr);
  if (rc != 0)
    return -rc;
  rc = pthread_mutexattr_settype(&attr, PTHREAD_MUTEX_ERRORCHECK);
#ifdef __linux__
  if (rc == 0)
    rc = pthread_mutexattr_setrobust(&attr, PTHREAD_MUTEX_ROBUST);
#else
  if (rc == 0 && __atomic_exchange_n(&g_v7_robust_warning_emitted, 1u, __ATOMIC_RELAXED) == 0u)
    fprintf(stderr, "v7 locks: robust mutexes unavailable; stale-owner checks remain active\n");
#endif
  if (rc == 0)
    rc = pthread_mutex_init(&lock->mutex, &attr);
  pthread_mutexattr_destroy(&attr);
  return rc == 0 ? 0 : -rc;
}

static int kafs_v7_mutex_recover_owner_dead(kafs_v7_mutex_t *lock)
{
  fprintf(stderr, "v7 lock owner died: name=%s group=%" PRIu32 "\n", lock->name, lock->group_id);
  int rc = pthread_mutex_consistent(&lock->mutex);
  if (rc == 0)
    rc = pthread_mutex_unlock(&lock->mutex);
  return rc == 0 ? -EOWNERDEAD : -rc;
}

static int kafs_v7_mutex_lock(kafs_v7_lock_state_t *state, kafs_v7_mutex_t *lock,
                              uint64_t *acquisitions)
{
  int rc = kafs_v7_lock_stack_can_push(lock);
  if (rc != 0)
    return rc;
  kafs_v7_lock_cancel_enter();
  uint64_t started = 0u;
  rc = kafs_v7_lock_now_ns(&started);
  if (rc != 0)
  {
    kafs_v7_lock_cancel_leave();
    return rc;
  }
  rc = pthread_mutex_trylock(&lock->mutex);
  int owner_dead_acquired = rc == EOWNERDEAD;
  if (rc == EBUSY)
  {
    __atomic_add_fetch(&state->stats.contended_acquisitions, 1u, __ATOMIC_RELAXED);
    for (;;)
    {
      uint64_t now_ns = 0u;
      rc = kafs_v7_lock_now_ns(&now_ns);
      if (rc != 0)
        break;
      uint64_t elapsed_ns = now_ns - started;
      if (elapsed_ns / UINT64_C(1000000) >= state->timeout_ms)
      {
        uint64_t owner = __atomic_load_n(&lock->owner_tid, __ATOMIC_ACQUIRE);
        fprintf(stderr,
                "v7 lock timeout: name=%s group=%" PRIu32 " owner=%" PRIu64 " waiter=%" PRIu64
                " waited_ms=%" PRIu64 "\n",
                lock->name, lock->group_id, owner, kafs_v7_lock_tid(),
                elapsed_ns / UINT64_C(1000000));
        rc = kafs_v7_lock_owner_alive(owner) ? ETIMEDOUT : EOWNERDEAD;
        break;
      }
      struct timespec until;
      if (clock_gettime(CLOCK_REALTIME, &until) != 0)
      {
        rc = errno;
        break;
      }
      uint64_t remaining_ms = state->timeout_ms - elapsed_ns / UINT64_C(1000000);
      uint32_t poll_ms =
          remaining_ms < KAFS_V7_LOCK_POLL_MS ? (uint32_t)remaining_ms : KAFS_V7_LOCK_POLL_MS;
      kafs_v7_lock_timespec_add_ms(&until, poll_ms == 0u ? 1u : poll_ms);
      rc = pthread_mutex_timedlock(&lock->mutex, &until);
      if (rc == 0 || rc == EOWNERDEAD || rc == ENOTRECOVERABLE)
      {
        owner_dead_acquired = rc == EOWNERDEAD;
        break;
      }
      if (rc != ETIMEDOUT)
        break;
      uint64_t owner = __atomic_load_n(&lock->owner_tid, __ATOMIC_ACQUIRE);
      if (owner != 0u && !kafs_v7_lock_owner_alive(owner))
      {
        rc = EOWNERDEAD;
        break;
      }
      rc = EBUSY;
    }
  }
  if (rc == EOWNERDEAD)
  {
    int owner_rc = owner_dead_acquired ? kafs_v7_mutex_recover_owner_dead(lock) : -EOWNERDEAD;
    kafs_v7_lock_cancel_leave();
    return owner_rc;
  }
  if (rc != 0)
  {
    kafs_v7_lock_cancel_leave();
    return -rc;
  }

  rc = kafs_lock_order_acquired(lock->rank, lock, 0);
  if (rc != 0)
  {
    (void)pthread_mutex_unlock(&lock->mutex);
    kafs_v7_lock_cancel_leave();
    return rc;
  }
  __atomic_store_n(&lock->owner_tid, kafs_v7_lock_tid(), __ATOMIC_RELEASE);
  __atomic_add_fetch(acquisitions, 1u, __ATOMIC_RELAXED);
  uint64_t finished = 0u;
  if (kafs_v7_lock_now_ns(&finished) == 0)
    __atomic_add_fetch(&state->stats.wait_ns, finished - started, __ATOMIC_RELAXED);
  return 0;
}

static void kafs_v7_mutex_destroy_or_abort(kafs_v7_mutex_t *lock)
{
  int rc = pthread_mutex_destroy(&lock->mutex);
  if (rc == 0)
    return;
  fprintf(stderr, "v7 lock destroy failed: name=%s group=%" PRIu32 " error=%d\n", lock->name,
          lock->group_id, rc);
  abort();
}

static int kafs_v7_mutex_unlock(kafs_v7_mutex_t *lock)
{
  if (!lock)
    return -EINVAL;
  int rc = kafs_lock_order_can_release(lock->rank, lock);
  if (rc != 0)
    return rc;
  __atomic_store_n(&lock->owner_tid, 0u, __ATOMIC_RELEASE);
  rc = pthread_mutex_unlock(&lock->mutex);
  if (rc != 0)
  {
    __atomic_store_n(&lock->owner_tid, kafs_v7_lock_tid(), __ATOMIC_RELEASE);
    return -rc;
  }
  rc = kafs_lock_order_released(lock->rank, lock);
  if (rc != 0)
    abort();
  kafs_v7_lock_cancel_leave();
  return 0;
}

int kafs_v7_locks_init(uint32_t group_count, uint32_t timeout_ms, kafs_v7_lock_state_t **state_out)
{
  if (!state_out || group_count == 0u || group_count > 64u)
    return -EINVAL;
  *state_out = NULL;
  kafs_v7_lock_state_t *state = (kafs_v7_lock_state_t *)calloc(1u, sizeof(*state));
  if (!state)
    return -ENOMEM;
  state->group_count = group_count;
  state->timeout_ms = timeout_ms == 0u ? KAFS_V7_LOCK_DEFAULT_TIMEOUT_MS : timeout_ms;
  int write_gate_initialized = 0;
  int rc = kafs_v7_mutex_init(&state->write_gate, "v7_write_gate", KAFS_V7_LOCK_RANK_WRITE_GATE,
                              UINT32_MAX);
  if (rc == 0)
  {
    write_gate_initialized = 1;
    rc =
        kafs_v7_mutex_init(&state->sequence, "v7_sequence", KAFS_V7_LOCK_RANK_SEQUENCE, UINT32_MAX);
  }
  if (rc != 0)
  {
    if (write_gate_initialized)
      pthread_mutex_destroy(&state->write_gate.mutex);
    free(state);
    return rc;
  }
  state->groups = (kafs_v7_mutex_t *)calloc(group_count, sizeof(*state->groups));
  if (!state->groups)
    rc = -ENOMEM;
  uint32_t initialized = 0;
  while (rc == 0 && initialized < group_count)
  {
    rc = kafs_v7_mutex_init(&state->groups[initialized], "v7_group", KAFS_V7_LOCK_RANK_GROUP,
                            initialized);
    if (rc == 0)
      ++initialized;
  }
  if (rc != 0)
  {
    for (uint32_t id = 0; id < initialized; ++id)
      pthread_mutex_destroy(&state->groups[id].mutex);
    free(state->groups);
    pthread_mutex_destroy(&state->sequence.mutex);
    pthread_mutex_destroy(&state->write_gate.mutex);
    free(state);
    return rc;
  }
  *state_out = state;
  return 0;
}

void kafs_v7_locks_destroy(kafs_v7_lock_state_t *state)
{
  if (!state)
    return;
  for (uint32_t id = 0; id < state->group_count; ++id)
    kafs_v7_mutex_destroy_or_abort(&state->groups[id]);
  free(state->groups);
  kafs_v7_mutex_destroy_or_abort(&state->sequence);
  kafs_v7_mutex_destroy_or_abort(&state->write_gate);
  free(state);
}

int kafs_v7_transaction_lock(kafs_v7_lock_state_t *state, uint32_t group_id)
{
  if (!state || group_id >= state->group_count)
    return -EINVAL;
  int rc = kafs_v7_mutex_lock(state, &state->write_gate, &state->stats.write_gate_acquisitions);
  if (rc == 0)
    rc = kafs_v7_mutex_lock(state, &state->sequence, &state->stats.sequence_acquisitions);
  if (rc == 0)
    rc = kafs_v7_mutex_lock(state, &state->groups[group_id], &state->stats.group_acquisitions);
  if (rc != 0)
  {
    if (kafs_lock_order_can_release(state->sequence.rank, &state->sequence) == 0)
      (void)kafs_v7_mutex_unlock(&state->sequence);
    if (kafs_lock_order_can_release(state->write_gate.rank, &state->write_gate) == 0)
      (void)kafs_v7_mutex_unlock(&state->write_gate);
  }
  return rc;
}

int kafs_v7_transaction_unlock(kafs_v7_lock_state_t *state, uint32_t group_id)
{
  if (!state || group_id >= state->group_count)
    return -EINVAL;
  int rc = kafs_v7_mutex_unlock(&state->groups[group_id]);
  if (rc == 0)
    rc = kafs_v7_mutex_unlock(&state->sequence);
  if (rc == 0)
    rc = kafs_v7_mutex_unlock(&state->write_gate);
  return rc;
}

int kafs_v7_checkpoint_lock(kafs_v7_lock_state_t *state)
{
  if (!state)
    return -EINVAL;
  return kafs_v7_mutex_lock(state, &state->write_gate, &state->stats.write_gate_acquisitions);
}

int kafs_v7_checkpoint_unlock(kafs_v7_lock_state_t *state)
{
  if (!state)
    return -EINVAL;
  return kafs_v7_mutex_unlock(&state->write_gate);
}

int kafs_v7_locks_get_stats(const kafs_v7_lock_state_t *state, kafs_v7_lock_stats_t *stats)
{
  if (!state || !stats)
    return -EINVAL;
  stats->write_gate_acquisitions =
      __atomic_load_n(&state->stats.write_gate_acquisitions, __ATOMIC_RELAXED);
  stats->sequence_acquisitions =
      __atomic_load_n(&state->stats.sequence_acquisitions, __ATOMIC_RELAXED);
  stats->group_acquisitions = __atomic_load_n(&state->stats.group_acquisitions, __ATOMIC_RELAXED);
  stats->contended_acquisitions =
      __atomic_load_n(&state->stats.contended_acquisitions, __ATOMIC_RELAXED);
  stats->wait_ns = __atomic_load_n(&state->stats.wait_ns, __ATOMIC_RELAXED);
  return 0;
}
