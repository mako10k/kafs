# KAFS format v6 shared artifact boundary plan

Date: 2026-07-06
Status: accepted; v6 entrypoint adapter naming and source ownership reflected

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
| `kafs` | `kafs_hrl.c`, `kafs_locks.c`, `kafs_journal.c`, `kafs_rpc.c` | none in `Makefile.am` | v4/v5 production runtime; legacy v6 inspection/write tokens fail closed with `kafs-v6` guidance |
| `kafs-v6` | `kafs.c`, `kafs_hrl.c`, `kafs_locks.c`, `kafs_journal.c`, `kafs_rpc.c` through `KAFS_V6_ENTRYPOINT` | `kafs_v6.c`, `kafs_v6_runtime.c`, `kafs_v6_entrypoint_adapter.c` | v6 CLI/admission owner; read-only inspection and controlled-write admission are active |
| `kafsctl` / `kafs-back` | selected runtime support sources through `kafs.c` and RPC/HRL/journal helpers | none in `Makefile.am` | must not gain `kafs_v6_runtime.c` implicitly |
| offline tools | mostly standalone source files plus header-only v6 layout helpers | header-only v6 descriptor logic | offline/staging tools, not runtime admission owners |

This boundary deliberately keeps `kafs_v6_runtime.c` out of the production
`kafs` link. T25 temporarily shares `kafs.c` into `kafs-v6` as a guarded
common object path for FUSE operations, compiled with `KAFS_V6_ENTRYPOINT`.
That path is not part of the production `kafs` entrypoint. The current
`kafs_v6_entrypoint_adapter.*` name means a v6-only adapter from the dedicated
`kafs-v6` entrypoint state into the shared FUSE runtime entrypoints that still
live in `kafs.c`; it is not a v5/v6 compatibility layer.

## Source Ownership Map

| Source | Artifact class | Links into | Owns | Must not own |
| --- | --- | --- | --- | --- |
| `src/kafs.c` | production runtime plus temporary shared FUSE implementation | `kafs`, `kafs-v6`, `kafsctl`, `kafs-back` with per-target guards | v4/v5 runtime, legacy v6 fail-closed diagnostics, shared FUSE operations, shared FUSE runner/cleanup wrapper | v6 admission policy, v6 mount-main preparation, v6 runtime open/admit/init ownership, v6 write-surface expansion |
| `src/kafs_v6.c` | v6 product entrypoint | `kafs-v6` only | `kafs-v6` CLI shape, mode selection, descriptor preflight handoff, admission signal | shared FUSE operation implementation, production `kafs` behavior |
| `src/kafs_v6_runtime.c` | v6-only runtime policy/helper | `kafs-v6` only | v6 request reporting, image open, descriptor-backed runtime admission, service init | production `kafs` link surface, generic v4/v5 runtime context setup |
| `src/kafs_v6_mount_options.[ch]` | v6-only mount option policy helper | `kafs-v6` only | v6-owned `-o` token vocabulary, runtime request token recording, FUSE passthrough filtering, `multi_thread` / `max_threads` handoff state | mount-main orchestration, context open/admit/init, shared FUSE operation implementation, installed ABI |
| `src/kafs_v6_entrypoint_adapter.[ch]` | v6-only entrypoint adapter | `kafs-v6` only | translation from `kafs-v6` parser/main state to shared FUSE runtime entrypoints, context initialization, image lock, FUSE argv assembly, and runtime handoff | v5/v6 compatibility policy, user-facing helper CLI, v6 option vocabulary, shared FUSE runner hook ownership, installed ABI |
| `src/kafs_shared_fuse_runner.h` | internal shared FUSE runner boundary | targets compiling shared FUSE operations | narrow handoff to `kafs.c` shared FUSE runner, runtime cache/TRIM option summary | v6 admission policy, mount-main orchestration, FUSE operation implementation, installed ABI |
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
- `KAFS_V6_READONLY_SMOKE` and `KAFS_V6_ADMISSION_HANDOFF`;
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
the direct `fuse_main()` / `kafs_operations` invocation from both production
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
`KAFS_V6_ENTRYPOINT` path, plus the shared operation table and cleanup path.

`SDW-V6RT-T41` moves v6 option interpretation out of the adapter and into
`kafs_v6_mount_options.[ch]`. The helper is now the single v6-only owner of
`-o` token classification for both CLI admission recording and FUSE
passthrough filtering; the adapter consumes the helper result instead of
carrying its own token vocabulary.

`SDW-V6RT-T42` moves the shared FUSE runner hook declaration and runtime option
summary out of `kafs_v6_entrypoint_adapter.h` and into
`kafs_shared_fuse_runner.h`. The adapter still orchestrates the `kafs-v6`
mount-main path, but it no longer owns the API name for the `kafs.c`
`fuse_main()` / `kafs_operations` handoff.

The next slice should reduce the remaining common-object adapter path around
shared FUSE operation implementations, or retire legacy `kafs` v6 diagnostic
scaffolding after operator workflows no longer depend on it. Do not add another
runtime executable and do not broaden the controlled-write surface.

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
