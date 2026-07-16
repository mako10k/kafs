#include "kafs_lock_order.h"

#include <errno.h>
#include <stddef.h>

#define KAFS_LOCK_ORDER_STACK_MAX 64u

typedef struct kafs_lock_order_entry
{
  uint32_t rank;
  const void *identity;
} kafs_lock_order_entry_t;

static _Thread_local kafs_lock_order_entry_t g_kafs_lock_order_stack[KAFS_LOCK_ORDER_STACK_MAX];
static _Thread_local uint32_t g_kafs_lock_order_depth;

int kafs_lock_order_can_acquire(uint32_t rank, int allow_equal_rank)
{
  if (rank == 0u)
    return -EINVAL;
  if (g_kafs_lock_order_depth >= KAFS_LOCK_ORDER_STACK_MAX)
    return -EOVERFLOW;
  if (g_kafs_lock_order_depth != 0u)
  {
    uint32_t current = g_kafs_lock_order_stack[g_kafs_lock_order_depth - 1u].rank;
    if (rank < current || (!allow_equal_rank && rank == current))
      return -EDEADLK;
  }
  return 0;
}

int kafs_lock_order_acquired(uint32_t rank, const void *identity, int allow_equal_rank)
{
  if (!identity)
    return -EINVAL;
  int rc = kafs_lock_order_can_acquire(rank, allow_equal_rank);
  if (rc != 0)
    return rc;
  g_kafs_lock_order_stack[g_kafs_lock_order_depth++] =
      (kafs_lock_order_entry_t){.rank = rank, .identity = identity};
  return 0;
}

int kafs_lock_order_can_release(uint32_t rank, const void *identity)
{
  if (rank == 0u || !identity)
    return -EINVAL;
  if (g_kafs_lock_order_depth == 0u)
    return -EPERM;
  const kafs_lock_order_entry_t *top = &g_kafs_lock_order_stack[g_kafs_lock_order_depth - 1u];
  if (top->rank != rank || top->identity != identity)
    return -EPERM;
  return 0;
}

int kafs_lock_order_released(uint32_t rank, const void *identity)
{
  int rc = kafs_lock_order_can_release(rank, identity);
  if (rc != 0)
    return rc;
  --g_kafs_lock_order_depth;
  g_kafs_lock_order_stack[g_kafs_lock_order_depth] = (kafs_lock_order_entry_t){0};
  return 0;
}

uint32_t kafs_lock_order_depth(void) { return g_kafs_lock_order_depth; }

int kafs_lock_order_rank_at(uint32_t index, uint32_t *rank_out)
{
  if (!rank_out)
    return -EINVAL;
  if (index >= g_kafs_lock_order_depth)
    return -ERANGE;
  *rank_out = g_kafs_lock_order_stack[index].rank;
  return 0;
}
