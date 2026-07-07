# KAFS format v6 production diagnostic scaffolding inventory

Date: 2026-07-07
Status: updated through SDW-V6RT-T53 readonly smoke env gate retirement

## Boundary

`kafs-v6` owns successful format-v6 runtime admission:

- `kafs-v6 --inspection-mount`
- `kafs-v6 --controlled-write-mount`

Production `kafs` remains the v4/v5 runtime entrypoint. Its remaining format-v6
code is legacy compatibility and diagnostic scaffolding: it may explain why a
v6 image or legacy v6 token is rejected, but it must not become a new
successful v6 runtime path.

Production `kafs` still does not link `kafs_v6_runtime.c`.

## Retain Until Compatibility Retirement

These surfaces should stay until operator compatibility guidance no longer
depends on them:

- `src/kafs_legacy_v6_failclosed.h` classifies legacy
  `--v6-inspection-mount`, `--v6-write-mount`, `v6_inspection_mount`, and
  `v6_write_mount` requests and prints `kafs-v6` guidance.
- `kafs_main_record_legacy_v6_request()` records only that a legacy v6 token
  was requested.
- `kafs_legacy_v6_reject_if_requested()` runs in production `main()` after
  mount-option filtering and before runtime context open. This is the active
  fail-closed gate for legacy v6 tokens.
- A plain v6 image mount through production `kafs` still runs descriptor
  admission preflight and then exits through the offline-only gate. This keeps
  operator diagnostics for malformed v6 descriptors without admitting FUSE.

## Diagnostic-Only Gates

These surfaces are diagnostic scaffolding, not the target runtime contract:

- `KAFS_V6_ADMISSION_HANDOFF=1` maps the full v6 image into the production
  runtime context, validates descriptor-backed views and worker policy, unmaps
  the image, and still exits through the offline-only gate.

## Retired By T53

`KAFS_V6_READONLY_SMOKE=1` was removed from production `kafs`. It was an older
debug gate for a read-only smoke path; current operator-facing read-only
admission and current test coverage use `kafs-v6 --inspection-mount`.

## Retirement Candidates

The safest next reductions are:

1. Remove the legacy-token successful branches from
   `kafs_main_open_runtime_context()`. The production `main()` path rejects
   legacy `v6_inspection_mount` and `v6_write_mount` before opening the runtime
   context, so the `kafs_main_v6_inspection_mount()` and
   `kafs_main_v6_controlled_write_mount()` call sites now read as historical
   leftovers.
2. After the above, decide whether production `kafs` should keep descriptor
   preflight for plain v6 image mounts or reduce it to direct `kafs-v6`
   guidance. Removing that preflight would also remove production `kafs`'s
   dependency on `kafs_v6_admission.h`.

## Shared Helpers That Are Not Production Ownership

`kafs_v6_admission.h` is a shared descriptor-admission helper used by both the
production diagnostic scaffolding and `kafs_v6_runtime.c`. Its presence in
production `kafs` is not successful v6 runtime ownership. Do not move this
helper wholesale into production code; remove the production diagnostic use
first if the goal is to shrink production's v6 surface.

## Evidence

`lsp-cli` references confirmed the local call scopes:

- `kafs_main_v6_admission_preflight()` is referenced only by its definition and
  the plain-v6 offline-only branch in `kafs_main_open_runtime_context()`.
- `kafs_main_v6_admission_handoff_enabled()` and
  `kafs_main_v6_admission_handoff()` are each referenced only by their
  definition and the `KAFS_V6_ADMISSION_HANDOFF` branch.
- `kafs_main_v6_inspection_mount()` and
  `kafs_main_v6_controlled_write_mount()` are referenced only by their
  definitions and the legacy-token branches in `kafs_main_open_runtime_context()`.

`rg` found current test coverage for `KAFS_V6_ADMISSION_HANDOFF`. After T53,
`rg KAFS_V6_READONLY_SMOKE src tests` returns no matches. The read-only FUSE
smoke uses `kafs-v6 --inspection-mount`.
