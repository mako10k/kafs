# KAFS format v6 production diagnostic scaffolding inventory

Date: 2026-07-07
Status: updated through SDW-V6RT-T57 legacy v6 token guidance retirement

## Boundary

`kafs-v6` owns successful format-v6 runtime admission:

- `kafs-v6 --inspection-mount`
- `kafs-v6 --controlled-write-mount`

Production `kafs` remains the v4/v5 runtime entrypoint. Its remaining format-v6
code is direct runtime-admission ownership guidance: it explains that v6 runtime
admission is owned by `kafs-v6`, but it must not validate, specially parse, or
admit a successful v6 runtime path.

Production `kafs` still does not link `kafs_v6_runtime.c`.

## Retained Production Guidance

This surface remains:

- Plain v6 image mounts through production `kafs` reject directly with
  `kafs-v6` guidance and do not run descriptor admission preflight or legacy v6
  token compatibility handling.

## Retired By T53

`KAFS_V6_READONLY_SMOKE=1` was removed from production `kafs`. It was an older
debug gate for a read-only smoke path; current operator-facing read-only
admission and current test coverage use `kafs-v6 --inspection-mount`.

## Retired By T54

The legacy-token successful branches were removed from
`kafs_main_open_runtime_context()`. After T54 and before T57, production `kafs`
still recognized legacy `v6_inspection_mount` and `v6_write_mount` tokens only
so `kafs_legacy_v6_reject_if_requested()` could fail closed with `kafs-v6`
guidance. It no longer had a successful v6 inspection or controlled-write branch
behind that fail-closed gate.

## Retired By T55

`KAFS_V6_ADMISSION_HANDOFF=1` was removed from production `kafs`. It was an
env-only diagnostic gate that mapped the full v6 image into a production runtime
context and still exited through the offline-only gate. After T55 and before
T56, production `kafs` used the same plain-v6 admission preflight /
offline-only path regardless of that environment variable.

## Retired By T56

Plain-v6 descriptor admission preflight was removed from production `kafs`.
Production `kafs` no longer includes `kafs_v6_admission.h`, no longer defines
`kafs_main_v6_admission_preflight()`, and rejects v6 images before descriptor
validation with direct `kafs-v6` guidance.

## Retired By T57

Legacy v6 token compatibility guidance was removed from production `kafs`.
Production `kafs` no longer includes `kafs_legacy_v6_failclosed.h`, no longer
records `v6_inspection_mount` / `v6_write_mount` requests, and no longer exposes
those legacy tokens in `kafs --help`, `man/kafs.1`, or shell completions.

## Retirement Candidates

The safest next reductions are:

1. Reduce the remaining common-object adapter path around shared FUSE operation
   implementations without broadening the v6 write surface.

## Shared Helpers That Are Not Production Ownership

`kafs_v6_admission.h` is a v6 descriptor-admission helper used by
`kafs_v6_runtime.c` and tests. After T56, production `kafs` no longer includes
it.

## Evidence

`lsp-cli` references confirmed the local call scopes:

- Before T56, `kafs_main_v6_admission_preflight()` was referenced only by its
  definition and the plain-v6 offline-only branch in
  `kafs_main_open_runtime_context()`.

After T53, `rg KAFS_V6_READONLY_SMOKE src tests` returns no matches. The
read-only FUSE smoke uses `kafs-v6 --inspection-mount`.
After T54, `rg` finds no `kafs_main_v6_inspection_mount()` or
`kafs_main_v6_controlled_write_mount()` symbol in `src/` or tests.
After T55, `lsp-cli` reports no workspace symbol for
`kafs_main_v6_admission_handoff` or `kafs_main_v6_runtime_admit_context`.
After T56, `rg` finds no `kafs_main_v6_admission_preflight`,
`kafs_main_rc_text`, or `kafs_v6_admission.h` include in production `src/kafs.c`.
After T57, `rg` finds no `kafs_legacy_v6`, `kafs_main_record_legacy_v6_request`,
`kafs_main_handle_v6_inspection_token`, or `kafs_main_handle_v6_write_token` in
production `src/kafs.c`.
