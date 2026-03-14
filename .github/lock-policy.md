# Locking Policy (Detailed)

This document defines lock design and implementation rules for `kafs`.

## 1. Top-Level Principles
- Keep lock scope minimal and explicit.
- Prefer deterministic lock ordering over opportunistic ordering.
- Treat silent lock hangs as correctness failures, not performance issues.
- If lock ownership state is corrupted or stale, fail fast with diagnostics.

## 2. Global Lock Order (Rank)
Acquire locks only in ascending rank order:
1. `hrl_global` (rank 10)
2. `inode_alloc` (rank 20)
3. `inode` (rank 30)
4. `hrl_bucket` (rank 40)
5. `bitmap` (rank 50)

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
