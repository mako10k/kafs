# KAFS format v6 shared artifact boundary plan

> **Historical v6 plan:** The v6-owned common artifacts described here have been removed. Its
> runtime commands are retired; retain this document only as decision history. See
> [the current retirement plan](sd-card-wear-v6-retirement-plan.md).

Date: 2026-07-07
Status: accepted; shared FUSE runtime source ownership reflected

## Purpose

This plan defines how v6 runtime work may share implementation with v4/v5
without adding user-facing helper binaries or reintroducing v6 policy into the
production `kafs` runtime. It is the preparation step before moving more v6
runtime setup behind `kafs-v6`.

## Product Boundary

Only final runtime binaries are product artifacts:

- `kafs`: production runtime for v4/v5 before v6 cutover.
- `kafs-v6`: dedicated format v6 runtime entrypoint.

Shared implementation artifacts are allowed:

- per-target common source/object lists in `Makefile.am`;
- non-installed static archives such as `noinst_LIBRARIES` when the build system
  is prepared for archive generation;
- future shared libraries when an ABI/install policy exists.

Do not create additional executable helper binaries for code sharing. A helper
that users can run is a product surface and needs its own CLI policy.

## Current Link Surface

| Target | Current shared sources | v6-specific sources | Boundary |
| --- | --- | --- | --- |
| `kafs` | `kafs_shared_fuse_runtime.c`, `kafs_hrl.c`, `kafs_locks.c`, `kafs_journal.c`, `kafs_rpc.c` | none in `Makefile.am` | v4/v5 production runtime; v6 images fail closed with direct `kafs-v6` guidance |
| `kafs-v6` | `kafs_shared_fuse_runtime.c`, `kafs_hrl.c`, `kafs_locks.c`, `kafs_journal.c`, `kafs_rpc.c` through `KAFS_SHARED_FUSE_RUNNER_EXPORT` | `kafs_v6.c`, `kafs_v6_runtime.c`, `kafs_v6_entrypoint_adapter.c`, `kafs_v6_mount_options.c` | v6 CLI/admission owner; read-only inspection and controlled-write admission are active |
| `kafsctl` / `kafs-back` | selected runtime support sources through `kafs_shared_fuse_runtime.c` and RPC/HRL/journal helpers | none in `Makefile.am` | must not gain `kafs_v6_runtime.c` implicitly |
| offline tools | mostly standalone source files plus header-only v6 layout helpers | header-only v6 descriptor logic | offline/staging tools, not runtime admission owners |

This boundary deliberately keeps `kafs_v6_runtime.c` out of the production
`kafs` link. T25 temporarily shared the former `kafs.c` implementation into
`kafs-v6` as a guarded common object path for FUSE operations; T46 named the
source-level export guard `KAFS_SHARED_FUSE_RUNNER_EXPORT`, and T59 moved that
shared runtime implementation into `kafs_shared_fuse_runtime.c`. Production
`src/kafs.c` is now only the `kafs` process entrypoint wrapper. The current
`kafs_v6_entrypoint_adapter.*` name means a v6-only adapter from the dedicated
`kafs-v6` entrypoint state into the shared FUSE runtime entrypoints; it is not
a v5/v6 compatibility layer.

## Source Ownership Map

| Source | Artifact class | Links into | Owns | Must not own |
| --- | --- | --- | --- | --- |
| `src/kafs.c` | production entrypoint wrapper | `kafs` only | `kafs` process crash diagnostic name and delegation to production main | shared FUSE operation implementation, v6 admission policy, v6 runtime open/admit/init ownership, v6 write-surface expansion |
| `src/kafs_shared_fuse_runtime.c` | shared runtime mechanics | `kafs`, `kafs-v6`, `kafsctl`, `kafs-back` with per-target guards | v4/v5 production mount flow behind `kafs_production_main()`, shared FUSE operations, shared FUSE runner/cleanup wrapper, runtime support helpers used by control tools | v6 admission policy, v6 mount-main preparation, v6 runtime open/admit/init ownership, legacy v6 guidance wording, installed ABI |
| `src/kafs_v6.c` | v6 product entrypoint | `kafs-v6` only | `kafs-v6` CLI shape, mode selection, descriptor preflight handoff, admission signal | shared FUSE operation implementation, production `kafs` behavior |
| `src/kafs_v6_runtime.c` | v6-only runtime policy/helper | `kafs-v6` only | v6 request reporting, image open, descriptor-backed runtime admission, service init | production `kafs` link surface, generic v4/v5 runtime context setup |
| `src/kafs_v6_mount_options.[ch]` | v6-only mount option policy helper | `kafs-v6` only | v6-owned `-o` token vocabulary, runtime request token recording, FUSE passthrough filtering, `multi_thread` / `max_threads` handoff state | mount-main orchestration, context open/admit/init, shared FUSE operation implementation, installed ABI |
| `src/kafs_v6_entrypoint_adapter.[ch]` | v6-only entrypoint adapter | `kafs-v6` only | translation from `kafs-v6` parser/main state to shared FUSE runtime entrypoints, context initialization, image lock, FUSE argv assembly, and runtime handoff | v5/v6 compatibility policy, user-facing helper CLI, v6 option vocabulary, shared FUSE runner hook ownership, installed ABI |
| `src/kafs_shared_fuse_runner.h` | internal shared FUSE runner boundary | targets compiling shared FUSE operations | request-based handoff to `kafs_shared_fuse_runtime.c` shared FUSE runner, runtime cache/TRIM option summary, optional shared cleanup path input | v6 admission policy, mount-main orchestration, FUSE operation implementation, installed ABI |
| `src/kafs_v6_fuse_init_policy.h` | v6 FUSE init policy helper | targets compiling shared FUSE operations | format-v6 FUSE init worker suppression check and diagnostic | FUSE operation table ownership, v4/v5 worker startup policy, installed ABI |
| `src/kafs_v6_fuse_policy.h` | v6 FUSE policy helper | targets compiling shared FUSE operations | controlled-write active checks, rejected-operation vocabulary, write-surface gates | FUSE operation table ownership, generic filesystem mutation logic |
| `src/kafs_v6_admission.h` | shared pure v6 metadata/admission helper | runtime diagnostics and v6 runtime helper users | descriptor-backed preflight/runtime admission core without product CLI ownership | successful production `kafs` v6 admission |
| `src/kafs_v6_layout.h` | shared pure metadata helper | offline and runtime users | descriptor layout parsing and validation primitives | runtime admission wording or mount policy |

Files with generic-looking names are therefore not automatically shared product
surfaces. A source is shared only when the link target and artifact class above
say it is shared.

## Artifact Classes

### v6-Only Runtime Policy

Owns CLI shape, admission wording, and v6 cutover policy. This code should link
only into `kafs-v6` unless a later product decision changes the binary boundary.

Current and near-term candidates:

- `src/kafs_v6.c`
- `src/kafs_v6_runtime.c`
- `src/kafs_v6_entrypoint_adapter.c`
- future `src/kafs_v6_mount_admission.c`
- future `src/kafs_v6_runtime_context.c`

### Shared Pure Metadata Helpers

May be common `.o`, `.a`, or future `.so` implementation because these helpers
do not own user-facing runtime policy.

Current candidates:

- descriptor discovery and replica validation from `kafs_v6_layout.h`;
- descriptor-backed bitmap/inode/allocator/HRL coverage checks;
- descriptor-backed journal header/data/segment lookup and validation;
- read-only fsck/dump descriptor report helpers after their CLI output remains
  owned by the calling tool.

The first extraction should prefer a non-installed static archive or common
source list, not another executable. If a static archive is used, add the needed
Autotools archive support in the same slice and keep the archive `noinst`.

### Shared Runtime Mechanics

May become common implementation only when the policy boundary is explicit and
tests show v4/v5 behavior is unchanged.

Candidates:

- image open/read-superblock helpers when their policy-independent portion is
  needed outside `kafs-v6`; the current v6-only open helper lives in
  `kafs_v6_runtime.c`;
- mmap and unmap helpers that do not assume v5 prefix metadata;
- descriptor-backed runtime context setup and cleanup;
- common FUSE operation helpers after write policy remains selected by the
  final binary.

These helpers must not silently start v5 workers, hotplug delegation, pending
log drain, tombstone GC, or background dedup for a v6 context.

### Production `kafs` Local Legacy Surface

Keep this code local until retired or moved:

- legacy `v6_inspection_mount` parsing in `kafs`, now fail-closed with
  `kafs-v6` guidance;
- legacy `v6_write_mount` parsing in `kafs`, now fail-closed with `kafs-v6`
  guidance;
- legacy v6 diagnostic messages emitted by `kafs`;
- production auto-migration and v2/v3/v4/v5 compatibility gates.

These paths are compatibility and smoke scaffolding, not the target v6 runtime
contract.

## T25-T39 Result And Next Boundary

`SDW-V6RT-T25 kafs-v6 inspection admission migration` moved the read-only v6
inspection acceptance path behind `kafs-v6`.

T25:

1. move read-only v6 inspection admission out of `kafs.c` and behind
   `kafs-v6`;
2. keep controlled-write migration out of scope;
3. preserve `kafs` v4/v5 behavior and keep legacy `kafs` v6 flags fail-closed
   or diagnostic-only;
4. add tests that measure v6 acceptance through `kafs-v6`, not through the
   production `kafs` binary.

`SDW-V6RT-T26 kafs-v6 controlled-write admission isolation` moved the existing
bounded controlled-write acceptance path behind `kafs-v6`.

T26:

1. route valid `--controlled-write-mount` requests through the dedicated v6
   entrypoint after descriptor preflight;
2. make legacy `kafs -o v6_write_mount` fail closed with `kafs-v6` guidance;
3. update tests and operator smoke helpers so v6 write acceptance is measured
   through `kafs-v6`, not the production `kafs` binary;
4. keep the write surface unchanged and limited to the pre-existing controlled
   regular-file operations.

The next slice should focus on v6-native runtime context pureification. If it
needs more shared build machinery, the acceptable first form remains a
non-installed static archive or common object/source list. Do not add a new
runtime executable.

`SDW-V6RT-T27` starts that pureification by removing the `kafs-v6` adapter call
into the generic v4/v5 runtime context opener. `kafs-v6` still shares common
runtime mechanics through `KAFS_V6_ENTRYPOINT`, but its open/read/admit/init
sequence is now a dedicated v6 entrypoint helper rather than a legacy
production `kafs` branch.

`SDW-V6RT-T28` continues that pureification by making successful v6 runtime
views descriptor-backed. The v6 admission mmap path now installs the image and
superblock only, requires descriptor-backed bitmap / inode / allocator / HRL
mapping, and validates that legacy contiguous `c_blkmasktbl` / `c_inotbl`
views are not present. The journal meta-delta bitmap overlay remains a v4/v5
contiguous-table optimization and is disabled for v6 contexts.

`SDW-V6RT-T29` seals the v6 worker-policy boundary. The successful v6 runtime
context now validates that pending worker, tombstone GC worker, background dedup
worker, and hotplug delegation stay disabled, including after controlled-write
journal init. Future shared runtime helpers must preserve that policy explicitly
instead of inheriting generic v5 worker setup.

`SDW-V6RT-T30` extracts v6 image open and superblock validation into
`kafs_v6_runtime_open_context_image()`. The helper remains v6-only for now:
it owns inspection vs. controlled-write open flags, `c_fd` setup, search cursor
initialization, superblock read, magic / format checks, and fd cleanup on
failure. This narrows the `KAFS_V6_ENTRYPOINT` adapter path without creating
another user-facing executable or expanding the v6 write surface.

`SDW-V6RT-T31` extracts the next successful `kafs-v6` runtime boundary without
changing the product link surface. `kafs_v6_admission.h` owns the shared
descriptor-backed preflight/runtime admission core for `kafs-v6` and legacy
production `kafs` diagnostic scaffolding. `kafs_v6_runtime.c` owns the
`kafs-v6` mode state/messages, diag setup, and controlled-write journal service
init. The reusable context invariants live in `kafs_context.h`, so production
`kafs` can keep diagnostic scaffolding without linking `kafs_v6_runtime.c` or
gaining a successful v6 runtime admission path.

`SDW-V6RT-T32` moves the `kafs-v6` runtime request rejection reporter into
`kafs_v6_runtime.c`. The standalone `kafs-v6` parser and the
`KAFS_V6_ENTRYPOINT` adapter path now consume the same
`kafs_v6_runtime_request_t` validation/reporting contract, and the adapter no
longer carries local inspection / controlled-write option policy checks.
Production `kafs` keeps its legacy v6 fail-closed validation local.

`SDW-V6RT-T33` narrows the common-object adapter API without moving or
expanding the FUSE operation table. `kafs_v6_entrypoint_adapter.h` now declares
only the `KAFS_V6_ENTRYPOINT` adapter entrypoints, leaving
`kafs_v6_runtime.h` for runtime helper contracts. `kafs_main_run_fuse()` hides
the direct `fuse_main()` / shared operation-table invocation from both production
`kafs` main and the `kafs-v6` adapter path, while preserving the same cleanup
path and write-surface policy.

`SDW-V6RT-T34` extracts the v6 controlled-write FUSE policy guard into
`kafs_v6_fuse_policy.h`. The shared FUSE operation implementations remain in
`kafs.c`, but the v6 allowlist-out active check and rejection wording now live
behind a dedicated policy helper. This keeps the write surface unchanged while
making the v6 policy boundary explicit for later operation-helper extraction.

`SDW-V6RT-T35` reduces the next FUSE operation entry gate layer. The shared
operation implementations still live in `kafs.c`, but condition-gated
controlled-write rejection and regular-file-only write checks now call through
`kafs_v6_fuse_policy.h`. This keeps the common-object adapter behavior unchanged
while moving v6 write-surface policy decisions behind the explicit helper
boundary.

`SDW-V6RT-T36` names the remaining direct controlled-write checks inside shared
write/fsync/release paths. Zero-block materialization, tail-layout bypass,
release reclaim bypass, and local write-path selection now call
`kafs_v6_fuse_policy.h` helpers. The shared FUSE operation implementations
remain in `kafs.c`, but `kafs.c` no longer reads the raw controlled-write active
flag directly.

`SDW-V6RT-T37` moves controlled-write rejected-operation vocabulary into
`kafs_v6_fuse_policy.h`. The shared operation implementations still live in
`kafs.c`, but they pass `kafs_v6_controlled_write_op_t` values instead of
free-form strings. This keeps rejection wording and operation ids on the v6
policy side while preserving the same write-surface boundary.

`SDW-V6RT-T38` moves the next adapter-local runtime preparation layer into
`kafs_v6_entrypoint_adapter.c`. The adapter helper now owns v6 entrypoint
option validation plus the open/admit/init sequence for the runtime context.
`kafs_v6_entrypoint_adapter.h` exposes an adapter-local mode enum instead of
re-exporting `kafs_v6_runtime.h`. `kafs.c` still owns generic option parsing,
FUSE argv assembly, image locking, and the shared FUSE runner / operation table.

`SDW-V6RT-T39` renames the temporary `kafs-v6` mount bridge files and symbols
to `kafs_v6_entrypoint_adapter.*`, then records the source ownership map above.
The term "bridge" may still appear in historical ticket text, but current
source names and boundary wording use "entrypoint adapter" for the v6-only
handoff from `kafs-v6` state into shared FUSE runtime entrypoints. This keeps
the implementation aligned with the product boundary: `kafs` remains v4/v5
production runtime, `kafs-v6` owns successful v6 admission, and shared FUSE
operation implementation remains a temporary common-object path.

`SDW-V6RT-T40` moves v6 mount-main preparation into
`kafs_v6_entrypoint_adapter.c`. The adapter now owns the v6-only FUSE option
filter, context initialization, runtime option handoff, image lock, and FUSE
argv assembly. `kafs.c` keeps only the shared FUSE runner wrapper for the
shared-runner export path, plus the shared operation table and cleanup path.

`SDW-V6RT-T41` moves v6 option interpretation out of the adapter and into
`kafs_v6_mount_options.[ch]`. The helper is now the single v6-only owner of
`-o` token classification for both CLI admission recording and FUSE
passthrough filtering; the adapter consumes the helper result instead of
carrying its own token vocabulary.

`SDW-V6RT-T42` moves the shared FUSE runner hook declaration and runtime option
summary out of `kafs_v6_entrypoint_adapter.h` and into
`kafs_shared_fuse_runner.h`. The adapter still orchestrates the `kafs-v6`
mount-main path, but it no longer owns the API name for the `kafs.c`
`fuse_main()` / shared operation-table handoff.

`SDW-V6RT-T43` moves the format-v6 FUSE-init worker suppression check and
diagnostic into `kafs_v6_fuse_init_policy.h`. `kafs_op_init()` remains in
`kafs.c`, but the v6-specific "do not start delayed/background workers" policy
is now named and kept beside the other v6 FUSE policy helpers.

`SDW-V6RT-T44` moves production `kafs` legacy v6 token classification and
fail-closed guidance into `kafs_legacy_v6_failclosed.h`. This preserves the
operator-facing rejection behavior while making clear that successful v6
runtime admission remains outside production `kafs`.

`SDW-V6RT-T45` renames the local shared FUSE operation table boundary inside
`kafs.c`. The table is now `kafs_shared_fuse_operation_table`, reached through
`kafs_shared_fuse_operations()`. `SDW-V6RT-T51` later renames the table/runner
compile guard to `KAFS_COMPILE_SHARED_FUSE_RUNTIME`, because the guard covers
the shared FUSE runtime boundary rather than only the operation table. This
keeps the shared operation implementations in place while avoiding table-level
wording that makes the common object path look like v6 admission ownership.

`SDW-V6RT-T46` renames the remaining `kafs.c` shared-runner export guard to
`KAFS_SHARED_FUSE_RUNNER_EXPORT`. `kafs-v6` still links `kafs.c` for shared
FUSE operations, but the compile-time name now describes the runner export
rather than v6 admission or a v5/v6 compatibility layer.

`SDW-V6RT-T47` renames the local shared FUSE runner helper in `kafs.c` from
`kafs_main_run_fuse()` to `kafs_shared_fuse_run_with_cleanup()`. Production
`kafs` main and the exported `kafs_shared_fuse_run()` handoff still share the
same `fuse_main()` / cleanup path; only the local ownership wording changes.

`SDW-V6RT-T52` inventories production `kafs` legacy v6 diagnostic scaffolding
in
[sd-card-wear-v6-production-diagnostic-scaffolding-inventory.md](sd-card-wear-v6-production-diagnostic-scaffolding-inventory.md).
The inventory keeps legacy token fail-closed guidance separate from successful
`kafs-v6` admission, records `KAFS_V6_ADMISSION_HANDOFF` and
`KAFS_V6_READONLY_SMOKE` as diagnostic-only gates, and identifies
`KAFS_V6_READONLY_SMOKE` plus the already fail-closed legacy-token successful
branches as the next retirement candidates.

`SDW-V6RT-T53` retires the production `KAFS_V6_READONLY_SMOKE` gate. Read-only
format-v6 FUSE inspection is now owned by `kafs-v6 --inspection-mount`; the
remaining production diagnostic gate is `KAFS_V6_ADMISSION_HANDOFF`.

`SDW-V6RT-T54` retires the production `kafs` legacy-token successful branches.
Legacy `v6_inspection_mount` / `v6_write_mount` tokens are still parsed only so
production `kafs` can fail closed with `kafs-v6` guidance; they no longer reach
a production successful v6 runtime path.

`SDW-V6RT-T55` retires the production `KAFS_V6_ADMISSION_HANDOFF` gate.
After T55 and before T56, production `kafs` kept only plain-v6 offline-only
descriptor preflight as its remaining v6 diagnostic surface.

`SDW-V6RT-T56` retires the production plain-v6 descriptor preflight.
Production `kafs` now rejects v6 images directly with `kafs-v6` guidance and no
longer includes `kafs_v6_admission.h`.

`SDW-V6RT-T57` retires production legacy v6 token compatibility guidance.
Production `kafs` no longer includes `kafs_legacy_v6_failclosed.h` and no
longer advertises legacy v6 tokens in help, man page, or shell completion.

`SDW-V6RT-T58` narrows the shared FUSE runner handoff to
`kafs_shared_fuse_run_request_t`. Production `kafs` main and
`kafs_v6_entrypoint_adapter.c` now assemble the same request shape before
entering the shared runner, while the operation table and operation
implementations remain in `src/kafs.c`.

`SDW-V6RT-T59` moves the shared FUSE runtime implementation from `src/kafs.c`
to `src/kafs_shared_fuse_runtime.c`. `src/kafs.c` is now a production
entrypoint wrapper, and `kafs-v6` links the shared runtime source directly
without depending on that wrapper.

The next slice should reduce the remaining broad shared runtime source by
separating production mount-main helpers or smaller FUSE operation support
helpers where that reduces coupling. Do not add another runtime executable and
do not broaden the controlled-write surface.

## Validation Standard

For boundary-only changes:

- `git diff --check`
- `./scripts/test-cli-surface.sh`
- `make -j2`

For any slice that moves runtime context setup or FUSE operation code:

- `make -C tests check TESTS=v6_descriptor_smoketest`
- `make check -j2`
- targeted checks showing `kafs` still rejects or preserves legacy v6 behavior
  while `kafs-v6` owns the new v6 admission path.
