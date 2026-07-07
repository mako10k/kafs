# KAFS format v6 runtime entrypoint plan

Date: 2026-07-07
Status: production legacy v6 diagnostic scaffolding inventoried

## Boundary

The dedicated format v6 runtime entrypoint is `kafs-v6`.

`kafs` remains the production runtime entrypoint for v4/v5 images. Legacy v6
inspection and controlled-write tokens in `kafs` now fail closed with
`kafs-v6` guidance. New v6 runtime admission and write-surface expansion must
move behind `kafs-v6`.

Until v6 production cutover, v6 has no backward compatibility promise. The
format and feature set may change drastically when the pure v6 target requires
it. The v4/v5 production runtime remains compatibility-preserving.

## CLI contract

Initial `kafs-v6` modes are explicit and mutually exclusive:

- `--inspection-mount`: read-only v6 inspection contract. Requires `-o ro`.
- `--controlled-write-mount`: experimental controlled write contract. Requires
  `-o rw,no_writeback_cache,no_trim_on_free,bg_dedup_scan=off,fsync_policy=full`.

The entrypoint rejects legacy `v6_inspection_mount` / `v6_write_mount` tokens.
Those tokens belong to the historical `kafs` bounded diagnostic surface. In
`kafs-v6`, selecting the binary and mode is the v6 admission signal.

The T20 skeleton validated the CLI boundary and image format, then failed
closed before mounting. T25 moves the read-only inspection admission path behind
`kafs-v6`: descriptor preflight still runs first, then `--inspection-mount`
admits a read-only FUSE mount. T26 moves controlled-write admission behind
`kafs-v6`: descriptor preflight still runs first, then
`--controlled-write-mount` admits only the existing conservative v6 write
surface.

## T25/T26 v6 admission

T25 linked `kafs-v6` with the shared FUSE runtime object set through the
original `KAFS_V6_ENTRYPOINT` common-object guard. T46 later renamed the
remaining `kafs.c` shared-runner export guard to
`KAFS_SHARED_FUSE_RUNNER_EXPORT`; production `kafs` still does not link
`kafs_v6_runtime.c` and no longer admits `v6_inspection_mount` or
`v6_write_mount` as a successful runtime path.

The read-only inspection path now uses:

- `kafs-v6 --inspection-mount ... -o ro` as the admission signal;
- descriptor and journal preflight from `kafs_v6_runtime.c`;
- shared FUSE/runtime mechanics from `kafs.c` compiled as a common object;
- forced read-only runtime state before `fuse_main`.

The controlled-write path now uses:

- `kafs-v6 --controlled-write-mount ... -o
  rw,no_writeback_cache,no_trim_on_free,bg_dedup_scan=off,fsync_policy=full`
  as the admission signal;
- descriptor and journal preflight from `kafs_v6_runtime.c`;
- shared FUSE/runtime mechanics from `kafs.c` compiled as a common object;
- the pre-existing bounded write surface: regular-file create/write/fsync and
  release, with broader metadata mutations still rejected.

## Runtime View Pureification

T28 moved the first v6-native runtime view boundary into code:

- successful `kafs-v6` admission maps the image and superblock, then installs
  descriptor-backed bitmap / inode / allocator / HRL views;
- v6 admission no longer installs v5-style contiguous `c_blkmasktbl` /
  `c_inotbl` pointers or `c_mapsize` as a runtime table view;
- successful v6 admission validates that descriptor-backed mappings are active
  and legacy contiguous table pointers are absent;
- v6 contexts do not start the journal meta-delta bitmap overlay, which still
  assumes a contiguous bitmap table.

T29 sealed the controlled-write worker-policy boundary:

- successful v6 admission validates that pending log drain, tombstone GC,
  background dedup worker, and hotplug delegation are disabled in the runtime
  context;
- controlled-write runtime setup revalidates the same policy after journal init,
  before the FUSE mount is admitted;
- the v6 FUSE init branch remains outside the generic v4/v5 worker start path.

T30 moved the v6 image-open boundary into the v6 runtime helper:

- `kafs_v6_runtime_open_context_image()` now owns inspection vs.
  controlled-write open flags, context fd setup, superblock read, magic check,
  format v6 check, and failure cleanup for `kafs-v6`;
- the `KAFS_V6_ENTRYPOINT` adapter path no longer carries a local
  read-superblock helper and proceeds from the v6 runtime helper result into
  descriptor-backed admission and diag/journal init;
- the extraction does not add a new executable surface and does not broaden the
  controlled-write operation allowlist.

T31 moved the v6 admission and service-init boundary into the v6 runtime helper
while preserving the production `kafs` link surface:

- `kafs_v6_admission.h` now owns the shared descriptor-backed preflight and
  runtime admission core used by `kafs-v6` and legacy production `kafs`
  diagnostic scaffolding;
- `kafs_context.h` now owns shared v6 context invariants for admission mmap,
  descriptor-backed runtime view validation, delayed/background mutation
  suppression, and worker-policy sealing;
- `kafs_v6_runtime_admit_mount_context()` now owns `kafs-v6` descriptor
  admission, full-image mmap mode selection, journal segment validation, mode
  state, and admission messages;
- `kafs_v6_runtime_init_mount_services()` now owns `kafs-v6` diag/journal
  service setup and post-init invariant revalidation;
- production `kafs` still does not link `kafs_v6_runtime.c`; its legacy v6
  diagnostic scaffolding uses the shared context helpers without becoming a
  successful v6 runtime entrypoint.

T32 moved the entrypoint request policy reporter into the v6 runtime helper:

- `kafs_v6_runtime_print_validation_error()` and
  `kafs_v6_runtime_report_entrypoint_request()` now own the `kafs-v6`
  admission rejection wording for runtime request validation;
- the standalone `kafs-v6` parser and the `KAFS_V6_ENTRYPOINT` adapter path
  both use the same request reporter;
- the adapter builds a `kafs_v6_runtime_request_t` from filtered mount options
  and no longer carries ad hoc inspection / controlled-write option policy
  checks;
- production `kafs` keeps its legacy v6 validation and fail-closed guidance
  local to `kafs.c`.

T33 narrows the remaining common-object adapter surface without changing FUSE
operation semantics:

- `kafs_v6_entrypoint_adapter.h` owns the `KAFS_V6_ENTRYPOINT` entrypoint
  adapter declarations, so `kafs_v6_runtime.h` is again limited to runtime request,
  admission, image-open, and service-init helpers;
- `kafs_main_run_fuse()` is the single local helper that installs the FUSE log
  hook, calls `fuse_main()` with the shared operation table, and runs the
  existing cleanup path for both production `kafs` and the `kafs-v6` adapter
  path;
- the shared FUSE operation table remains in `kafs.c`; the change only
  reduces direct adapter contact with the FUSE invocation mechanics.

T34 extracts the v6 controlled-write FUSE policy guard:

- `kafs_v6_fuse_policy.h` owns the controlled-write active check and
  allowlist-out rejection message used by shared FUSE operation code;
- the shared FUSE operations still live in `kafs.c`, but they now call the
  policy guard through a dedicated v6 FUSE policy helper;
- the initial controlled-write surface remains regular-file
  create/write/fsync/release only.

T35 continues that FUSE policy extraction at the operation entry gates:

- `kafs_v6_fuse_policy.h` now owns condition-gated rejection and regular-file
  write-surface checks used by the shared FUSE operation code;
- `kafs.c` still owns the shared operation implementations, but control-plane
  open/write, `open(O_TRUNC)`, hotplug delegated write, and non-regular write
  checks now call through the v6 FUSE policy helper boundary;
- the initial controlled-write surface remains unchanged.

T36 names the remaining controlled-write data-layout policy inside shared FUSE
write operations:

- `kafs_v6_fuse_policy.h` now owns helper names for zero-block preservation,
  tail-layout bypass, release reclaim bypass, and local write-path selection;
- `kafs.c` still owns the shared write, fsync, and release implementations,
  but no longer reads `kafs_v6_controlled_write_active()` directly;
- the initial controlled-write surface remains unchanged.

T37 moves rejected-operation vocabulary into the v6 FUSE policy helper:

- `kafs_v6_fuse_policy.h` owns `kafs_v6_controlled_write_op_t` and the mapping
  from policy operation ids to rejection wording;
- `kafs.c` now passes enum values to the v6 policy helper instead of
  free-form operation strings;
- the shared FUSE operations still live in `kafs.c`, and the initial
  controlled-write surface remains unchanged.

T38 moves v6 entrypoint request/open preparation into the entrypoint adapter:

- `kafs_v6_entrypoint_adapter.c` owns adapter-local option validation and the
  open/admit/init sequence for the `kafs-v6` runtime context;
- `kafs.c` no longer includes `kafs_v6_runtime.h` directly for the
  `KAFS_V6_ENTRYPOINT` adapter path;
- `kafs_v6_entrypoint_adapter.h` exposes an adapter-local mode enum and does not
  re-export the v6 runtime helper header;
- `kafs.c` still owns generic mount-option parsing, FUSE argv assembly, image
  locking, and the shared FUSE runner.

T39 clarifies the current source ownership and naming:

- the temporary mount bridge file/symbol names are renamed to
  `kafs_v6_entrypoint_adapter.*` and `kafs_v6_entrypoint_adapter_*`;
- `kafs_v6_entrypoint_adapter.*` is documented as a v6-only internal adapter
  from `kafs-v6` CLI/admission state to shared FUSE runtime entrypoints still
  implemented in `kafs.c`;
- it is explicitly not a v5/v6 compatibility layer, not a user-facing helper,
  and not linked into production `kafs`;
- `src/Makefile.am` and the shared artifact boundary plan now record the
  source ownership map so common-looking file names have a stated role.

T40 moves the v6 mount-main preparation body into the entrypoint adapter:

- `kafs_v6.c` converts the already-validated `kafs_v6_runtime_request_t` into
  adapter options and calls `kafs_v6_entrypoint_adapter_mount_main()`;
- `kafs_v6_entrypoint_adapter.c` owns the v6-only FUSE option filtering,
  context initialization, image lock, FUSE argv assembly, runtime option
  handoff, and the call into the shared FUSE runner;
- `kafs.c` keeps only the `KAFS_V6_ENTRYPOINT` shared FUSE runner wrapper that
  logs runtime options, enters `fuse_main()` with the shared operation table, and runs
  the existing cleanup path;
- shared FUSE operation implementations and the operation table remain in
  `kafs.c`; the controlled-write surface is unchanged.

T41 separates v6 mount option policy from mount-main orchestration:

- `kafs_v6_mount_options.[ch]` owns the v6-owned `-o` token vocabulary,
  runtime-admission request recording, FUSE passthrough filtering, and
  `multi_thread` / `max_threads` handoff state;
- `kafs_v6.c` keeps CLI argument shape, mode/image/mountpoint selection, and
  `-o` list splitting, but delegates token meaning to the option helper;
- the option helper rejects KAFS-owned production tuning tokens that are not
  part of the v6 admission vocabulary instead of silently stripping them;
- `kafs_v6_entrypoint_adapter.c` keeps mount-main orchestration, context setup,
  image locking, FUSE argv assembly, and shared-runner handoff, but no longer
  carries the v6 option vocabulary.

T42 separates the shared FUSE runner hook from the v6 adapter API:

- `kafs_shared_fuse_runner.h` owns the narrow internal handoff to the shared
  `fuse_main()` / operation-table runner still implemented in `kafs.c`;
- `kafs_v6_entrypoint_adapter.h` now exposes only adapter-local mode/options,
  validation/open helpers, and `kafs_v6_entrypoint_adapter_mount_main()`;
- `kafs_v6_entrypoint_adapter.c` calls `kafs_shared_fuse_run()` instead of
  publishing a runner hook in the adapter namespace.

T43 extracts the format-v6 FUSE init worker policy:

- `kafs_v6_fuse_init_policy.h` owns the FUSE-init decision that format-v6
  runtime mounts keep delayed/background mutation workers suppressed;
- `kafs_op_init()` still lives in `kafs.c`, but it now delegates the v6 worker
  policy check and diagnostic to that helper before starting v4/v5 workers;
- shared FUSE operation implementations and the operation table remain in
  `kafs.c`.

T44 separates legacy v6 fail-closed guidance from the production mount flow:

- `kafs_legacy_v6_failclosed.h` owns legacy v6 token classification and the
  `kafs-v6` guidance emitted by production `kafs`;
- `kafs.c` keeps the production CLI/mount flow and legacy request flags, but it
  delegates token vocabulary and fail-closed wording to that helper;
- successful v6 runtime admission remains owned by `kafs-v6`, and production
  `kafs` still does not link `kafs_v6_runtime.c`.

T45 names the remaining shared FUSE operation table boundary inside `kafs.c`:

- the local compile guard introduced for this boundary was
  `KAFS_COMPILE_SHARED_FUSE_OPERATIONS`, later renamed by T51 to
  `KAFS_COMPILE_SHARED_FUSE_RUNTIME`, so the table/runner code is described by
  the shared FUSE implementation it compiles rather than by the v6 adapter
  target that also consumes it;
- `kafs_operations` is now `kafs_shared_fuse_operation_table`, with
  `kafs_shared_fuse_operations()` as the local accessor used by `fuse_main()`;
- production `kafs` and `kafs-v6` still share the same operation
  implementations, and no successful production v6 runtime path is added.

T46 separates the shared FUSE runner export guard from v6 entrypoint naming:

- the `kafs-v6` target now defines `KAFS_SHARED_FUSE_RUNNER_EXPORT` when it
  links `kafs.c` for the shared runner and operation table;
- `kafs.c` compiles shared FUSE operations for production `kafs` or for that
  shared-runner export guard, and exports `kafs_shared_fuse_run()` only under
  the shared-runner export guard;
- successful v6 runtime admission remains owned by `kafs-v6` /
  `kafs_v6_runtime.c`, and production `kafs` still does not link v6 runtime
  helpers.

T47 renames the local shared FUSE runner helper inside `kafs.c`:

- `kafs_main_run_fuse()` is now `kafs_shared_fuse_run_with_cleanup()`, matching
  the fact that both production `kafs` main and the `kafs-v6` shared-runner
  export use the same local `fuse_main()` / cleanup path;
- the exported `kafs_shared_fuse_run()` handoff contract is unchanged;
- no FUSE operation implementation is duplicated, and no successful production
  v6 runtime path is added.

T48-T51 continue the same pureification direction without changing the write
surface:

- T48 makes `kafs-v6` reject unsupported production-only KAFS tuning options
  instead of silently stripping options that are not reflected into the v6
  runtime context;
- T49 renames the shared post-`fuse_main()` cleanup helper to
  `kafs_shared_fuse_cleanup_after_run()`;
- T50 renames the shared runtime option logger to
  `kafs_shared_fuse_log_runtime_options()`;
- T51 renames the shared table/runner compile guard to
  `KAFS_COMPILE_SHARED_FUSE_RUNTIME`, because the guard covers the shared FUSE
  runtime boundary rather than only the operation table.

T52 inventories the production `kafs` legacy v6 diagnostic scaffolding in
[sd-card-wear-v6-production-diagnostic-scaffolding-inventory.md](sd-card-wear-v6-production-diagnostic-scaffolding-inventory.md):

- legacy `v6_inspection_mount` / `v6_write_mount` token handling remains
  production-local only so `kafs` can fail closed with `kafs-v6` guidance;
- `KAFS_V6_ADMISSION_HANDOFF` and `KAFS_V6_READONLY_SMOKE` are documented as
  diagnostic-only gates, not operator entrypoints;
- `KAFS_V6_READONLY_SMOKE` and the legacy-token successful branches left behind
  `kafs_main_open_runtime_context()` are the next retirement candidates.

The remaining pureification pressure points are:

- retirement of legacy v6 diagnostic scaffolding in production `kafs`, starting
  with `KAFS_V6_READONLY_SMOKE` and the legacy-token successful branches that
  are already shielded by production `main()` fail-closed guidance;
- further reduction of the shared FUSE common-object adapter path,
  especially around shared operation implementations,
  without duplicating filesystem logic.

## Shared implementation boundary

Do not duplicate filesystem logic between `kafs` and `kafs-v6`. Final runtime
binaries are the product boundary; shared implementation may move into common
objects or libraries (`.o`, `.a`, and future `.so`) when it is needed by both
binaries:

- descriptor parsing and validation
- journal segment validation
- runtime context setup/cleanup helpers
- common FUSE operation tables where policy checks are explicit
- inode, block allocation, HRL, and filesystem operation helpers

`src/Makefile.am` builds `kafs-v6` as a separate binary. T21/T22 created the v6
runtime helper surface for the dedicated entrypoint. T25 links `kafs-v6` with a
common object set that includes `kafs.c`, guarded by `KAFS_V6_ENTRYPOINT`, while
keeping `kafs_v6_runtime.c` out of production `kafs`. T26 uses the same common
object boundary for controlled-write admission. T27 removes the `kafs-v6`
adapter dependency on the generic v4/v5 `kafs_main_open_runtime_context()` path
and gives the dedicated entrypoint its own v6 open/admit/init helper. T28 keeps
that adapter path but makes successful v6 runtime views descriptor-backed
rather than legacy contiguous table-backed. T29 keeps v6 runtime setup outside
generic v5 worker assumptions by sealing and revalidating the worker policy.
T30 moves v6 image open, superblock read, magic validation, and format
validation into `kafs_v6_runtime.c`, leaving the adapter to handle admission
handoff and the remaining shared FUSE runtime setup. T31 moves the shared
descriptor-backed admission core into `kafs_v6_admission.h`, and moves
`kafs-v6` mode state, diag
setup, and controlled-write journal service setup behind `kafs_v6_runtime.c`.
The reusable invariants remain in `kafs_context.h`, so production `kafs` does
not gain a `kafs_v6_runtime.c` link. T32 moves the entrypoint request rejection
reporter into `kafs_v6_runtime.c` and makes the adapter consume the same
`kafs_v6_runtime_request_t` validation contract as the standalone `kafs-v6`
parser. T33 moves the entrypoint adapter declarations out of the runtime helper
header and hides direct `fuse_main()` / shared operation-table invocation behind
`kafs_main_run_fuse()`. T34 moves the v6 controlled-write FUSE policy guard
into `kafs_v6_fuse_policy.h`, leaving the shared operation implementation in
`kafs.c` while making the policy boundary explicit. T35 moves the next
condition-specific v6 entry gates into that helper boundary while keeping the
shared FUSE operations in `kafs.c`. T36 moves the remaining direct
controlled-write active checks in shared write/fsync/release code behind
policy-named helpers. T37 moves rejected-operation names into the same helper
boundary, so `kafs.c` no longer owns the controlled-write rejection vocabulary.
T38 moves adapter-local v6 request validation and open/admit/init sequencing
into `kafs_v6_entrypoint_adapter.c`, leaving `kafs.c` with the generic parser,
FUSE argv assembly, image lock, and shared FUSE runner. The entrypoint adapter
header exposes an adapter-local mode enum instead of re-exporting
`kafs_v6_runtime.h`. T39 renames the former mount bridge file/symbols to
entrypoint adapter terminology and records source ownership. T40 moves v6
mount-main preparation, FUSE option filtering, context initialization, image
lock, and FUSE argv assembly into the adapter; `kafs.c` keeps the shared
`fuse_main()` / operation-table runner and cleanup wrapper. T41 moves v6
option interpretation and FUSE passthrough filtering from the adapter into
`kafs_v6_mount_options.[ch]`, leaving the adapter with orchestration and argv
assembly only. T42 moves the runner hook declaration and runtime option summary
out of the adapter header into `kafs_shared_fuse_runner.h`, so the adapter API
no longer appears to own a `kafs.c`-implemented shared runner. Later slices can
replace the common-object adapter path with a non-installed static archive or
narrower shared FUSE operation helpers. T43 moves the v6 FUSE-init
background-worker suppression check into `kafs_v6_fuse_init_policy.h`, keeping
the shared `kafs_op_init()` implementation in `kafs.c` while reducing the v6
policy embedded directly in it. T44 moves production `kafs` legacy v6
fail-closed token classification and guidance wording into
`kafs_legacy_v6_failclosed.h` without creating a successful production v6
runtime path. T45 renames the local shared FUSE operation table boundary in
`kafs.c`, replacing direct table-level `KAFS_V6_ENTRYPOINT` naming with a
shared-operation compile guard and `kafs_shared_fuse_operation_table`. T46
renames the remaining `kafs.c` shared-runner export guard to
`KAFS_SHARED_FUSE_RUNNER_EXPORT`, so the source-level export condition describes
the shared runner instead of v6 admission. T47 renames the local shared runner
implementation helper to `kafs_shared_fuse_run_with_cleanup()`, so the helper
used by both production main and the exported shared-runner handoff no longer
appears production-main-owned. T48 makes `kafs-v6` reject unsupported
production-only KAFS tuning options instead of silently stripping options that
the v6 entrypoint does not apply. T49 and T50 rename the shared cleanup and
runtime option logging helpers so those local helpers no longer read as
production-main-owned. T51 renames the shared table/runner compile guard to
`KAFS_COMPILE_SHARED_FUSE_RUNTIME`, matching the shared FUSE runtime boundary
it actually covers. T52 inventories production `kafs` legacy v6 diagnostic
scaffolding and records `KAFS_V6_READONLY_SMOKE` plus the fail-closed legacy
successful branches as the next retirement candidates.

The concrete shared artifact boundary is recorded in
[sd-card-wear-v6-shared-artifact-boundary-plan.md](sd-card-wear-v6-shared-artifact-boundary-plan.md).
The immediate next implementation boundary remains v6 runtime pureification,
not write-surface expansion.

## Smoke

The minimum smoke for this entrypoint is:

```sh
make -j2
./src/kafs-v6 --help
./scripts/test-cli-surface.sh
make -C tests check TESTS=v6_descriptor_smoketest
```

Broader regression remains required when the split starts moving shared runtime
or FUSE operation code.
