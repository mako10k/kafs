#pragma once

#include <stdint.h>

/*
 * Format-neutral per-thread lock-order tracker. Lock wrappers retain ownership
 * of their mutex and wait policy, but all lock families publish acquisitions
 * here so ranks are checked across translation-unit and format boundaries.
 */
int kafs_lock_order_can_acquire(uint32_t rank, int allow_equal_rank);
int kafs_lock_order_acquired(uint32_t rank, const void *identity, int allow_equal_rank);
int kafs_lock_order_can_release(uint32_t rank, const void *identity);
int kafs_lock_order_released(uint32_t rank, const void *identity);

uint32_t kafs_lock_order_depth(void);
int kafs_lock_order_rank_at(uint32_t index, uint32_t *rank_out);
