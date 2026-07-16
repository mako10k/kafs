# Locking Policy (Detailed)

This document defines lock design and implementation rules for `kafs`.

## 1. Top-Level Principles
- Keep lock scope minimal and explicit.
- Prefer deterministic lock ordering over opportunistic ordering.
- Treat silent lock hangs as correctness failures, not performance issues.
- If lock ownership state is corrupted or stale, fail fast with diagnostics.

## 2. Global Lock Order (Rank)
Acquire locks only in ascending rank order:
1. `v7_write_gate` (rank 1)
2. `v7_sequence` (rank 2)
3. `v7_group` (rank 3)
4. `hrl_global` (rank 10)
5. `inode_alloc` (rank 20)
6. `inode` (rank 30)
7. `hrl_bucket` (rank 40)
8. `bitmap` (rank 50)

The v7 composite transaction API acquires ranks 1, 2, and 3 in that order and
permits one group only.  Checkpoint publication holds `v7_write_gate` alone,
excluding transactions.  Until a multi-group protocol is accepted, callers
must not acquire a second `v7_group` lock.  If a v7 transaction also needs the
existing metadata locks, it acquires them only after `v7_group` and releases
them before the v7 composite unlock.

The v7-owned and existing metadata wrappers currently maintain separate
per-thread rank stacks.  No admitted runtime path crosses those wrapper
families yet.  Before a v7 writer may do so, the integration must add a
cross-family order check and a regression that rejects acquiring ranks 1-3
while any rank 10-50 lock is held.

Rules:
- Never acquire a lower rank while holding a higher rank.
- Unlock in strict reverse order.
- New lock classes must declare an explicit rank and be inserted into this table.

## 3. Runtime Enforcement Requirements
- Lock wrappers must enforce rank order at runtime.
- Lock wrappers must maintain per-thread lock stack consistency.
- Any rank mismatch, stack underflow/overflow, or order violation is a hard failure.

## 4. Cancellation and Robustness
- Disable thread cancellation while any internal mutex is held.
- Restore previous cancellation state when the thread releases the outermost lock.
- Use error-checking mutex attributes.
- Use robust mutex mode when available; when not available, warn once and continue with diagnostics enabled.

## 5. Wait Strategy and Stale Owner Detection
- Avoid unbounded blocking lock waits without observability.
- Use timed waits and periodic owner health checks.
- If owner TID is stale/non-existent while wait persists, treat as fatal and emit diagnostics.

## 6. Operational Rules for Code Changes
- Any change that introduces a new lock path must document lock order in code comments near the path.
- Cross-subsystem lock interactions (`inode` + `hrl_*` + `bitmap`) must be reviewed for order compliance.
- Long operations and I/O while holding high-contention locks should be minimized or moved out of critical sections when safe.
- Do not use `KAFS_CALL` after acquiring a lock; use `rc` capture plus a single unlock path instead.

## 7. Validation Checklist
- Build succeeds.
- Tests succeed.
- At least one contention path is exercised during validation.
- No new lock-order violation logs appear.
