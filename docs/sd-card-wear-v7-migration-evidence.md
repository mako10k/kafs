# KAFS v5-to-v7 migration lifecycle evidence contract

- Task: `SDW-V7RT-T59-A`
- Start HEAD: `7a5c750`
- Gate: `scripts/v5-v7-migration-evidence-gate.sh`
- Safety boundary: validate-only; no image write, mount, import, or cutover

## Purpose

`kafsresize --migrate-create` proves that a clean v5 source can be prechecked
and an empty format-v7 destination can be created. It does not copy the source
namespace, metadata, or payload. The current `kafs-v7` controlled-write surface
also cannot serve as a general importer: it admits bounded regular-file
creation/write/truncate but rejects directory creation, symlink creation,
ownership/time mutation, indirect-directory mutation, and cross-group
mutation.

T59-A therefore fixes the migration lifecycle and evidence semantics before a
v7-owned offline importer is implemented. The gate distinguishes complete
acceptance from an interrupted run that requires resume and an operator
rollback that preserves the failed destination. A synthetic or structurally
valid bundle does not prove that any data was imported.

## State Boundary

```text
PLANNED
  -> SOURCE_CAPTURED
  -> DESTINATION_CREATED
  -> COPYING
       -> VERIFYING -> ACCEPTED
       -> RESUME_REQUIRED -> COPYING
       -> ROLLED_BACK
```

`VERIFYING` may also move to `RESUME_REQUIRED` or `ROLLED_BACK`, and
`RESUME_REQUIRED` may move to `ROLLED_BACK`. Only
`RESUME_REQUIRED -> COPYING` increments the attempt number. Phase timestamps
and attempt numbers are monotonic.

The externally visible decisions are:

- `ACCEPT`: the ledger and destination are complete, source and destination
  semantic inventories match exactly, fsck/dump passed, and the destination is
  marked admission-ready;
- `RESUME_REQUIRED`: at least one source object remains incomplete, the work
  destination is partial and not admission-ready, and the first incomplete
  object is the exact resume boundary; or
- `ROLLBACK`: the failed destination is preserved, admission remains false,
  and the unchanged source is explicitly selected.

No decision authorizes production cutover, RC eligibility, real-media
qualification, physical-media qualification, or release-candidate status.

## Bundle And Digest Chain

One evidence directory contains exactly:

```text
plan.json
source.json
ledger.json
destination.json
decision.json
artifacts.sha256
```

`artifacts.sha256` covers every JSON file and no other entry is allowed. The
documents also form this byte-for-byte digest chain:

```text
plan.json
  <- source.json.plan_sha256
  <- ledger.json.plan_sha256 + source_sha256
  <- destination.json.plan_sha256 + source_sha256 + ledger_sha256
  <- decision.json.bindings.{plan,source,ledger,destination}_sha256
```

Changing an upstream record and merely refreshing the top-level inventory is
therefore insufficient. Every downstream binding must name the same retained
bytes.

## Plan Contract

`KAFS.V5V7MigrationPlan.v1` binds:

- a unique migration id and creation timestamp;
- exact clean format-v5 source image id, byte size, and SHA-256;
- exact format-v7 destination image id, byte size, inode count, group count,
  and block size;
- a frozen source-write policy;
- preserved directory, regular-file, and symlink types;
- preserved permissions, 16-bit uid/gid, atime/mtime nanoseconds, and regular
  hardlinks;
- rejection of special files and sparse files before destination write;
- never accepting a partial destination;
- preserving a failed destination and choosing the unchanged source on
  rollback; and
- idempotence as identical bindings producing the same semantic inventory.

T59-B must refine unsupported-source detection from real v5 evidence. Changing
these policies requires a new schema version rather than silently adding a
field to v1.

## Source And Destination Inventory

`KAFS.V5MigrationSourceInventory.v1` and
`KAFS.V7MigrationDestinationInventory.v1` use the same canonical semantic
model:

- `objects` are strictly sorted by stable object id;
- each object records type, four-octal-digit permissions, uid/gid, atime/mtime,
  logical size, namespace link count, and payload SHA-256;
- directories have logical size zero, a null payload digest, and exactly one
  namespace path;
- symlinks have a non-empty payload and exactly one namespace path;
- only regular files may have multiple paths for hardlink preservation;
- `entries` are strictly sorted normalized relative POSIX paths;
- `.` exists exactly once and references a directory; and
- every non-root entry has a present directory parent.

An accepted destination has the exact source objects and entries. A partial
destination may contain only source-identical completed objects and their valid
namespace paths. It cannot claim fsck, dump, or admission readiness.

## Copy Ledger

`KAFS.V5V7MigrationCopyLedger.v1` binds the plan and source bytes, destination
image id, attempt, ordered phase history, and the source image digest before and
after migration work. The before/after values must equal the planned source
digest.

Every source object occurs exactly once with `PENDING`, `IN_PROGRESS`,
`COMPLETE`, or `ROLLED_BACK` status, expected/copy byte counts, and the source
payload digest. `ACCEPT` requires all objects complete. `RESUME_REQUIRED`
points to the first non-complete object in canonical ledger order.

## Decision

`KAFS.V5V7MigrationDecision.v1` binds all four upstream documents, names the
operator and decision time, asserts source immutability, records destination
completeness, and carries the exact confirmation:

```text
MIGRATION T59A <migration-id> <ACCEPT|RESUME_REQUIRED|ROLLBACK>
```

Resume requires a non-empty failure/interruption reason and must not select
rollback. Rollback requires a reason and explicitly selects the unchanged
source. Acceptance has neither a resume object nor a failure reason.

## Validate-only Commands

```sh
./scripts/v5-v7-migration-evidence-gate.sh \
  --evidence-dir <bundle> \
  --require-decision ACCEPT \
  --validate-only --json
```

Replace `ACCEPT` with `RESUME_REQUIRED` or `ROLLBACK` when validating those
retained states. Requiring the decision at the command line prevents a valid
rollback bundle from being mistaken for accepted migration evidence.

Without `--json`, success includes the compatible human marker:

```text
KAFS_V5_V7_MIGRATION_EVIDENCE PASS migration_id=<id> decision=<decision>
```

The marker proves schema, lifecycle, semantic-inventory, and digest
consistency only. It does not prove a working importer or authorize cutover.
With `--json`, stdout is one
`KAFS.V5V7MigrationEvidenceValidation.v1` object for both PASS (exit 0) and
validation FAIL (exit 1); diagnostics remain on stderr. Invalid invocation or
an invalid evidence-directory argument exits 2 without a JSON result.

## Regression Boundary

The regression constructs source semantics containing nested directories,
regular files including an empty file, a symlink, and a two-path hardlink. It
validates an attempt-2 replayed `ACCEPT`, an interrupted `RESUME_REQUIRED`, and
a preserved `ROLLBACK` without creating or mounting KAFS images.

Negative cases cover required-decision mismatch, source mutation, identity
drift, incomplete accepted inventory, payload mismatch, illegal phase and
attempt transitions, partial acceptance, claim escalation, the wrong resume
object, rollback without a reason, hardlink-count mismatch, artifact tampering,
unexpected files, and symlinked evidence.

## T59-B Offline Import Surface

T59-B adds the data-construction surface that T59-A deliberately did not
provide:

```sh
./kafsresize --migrate-import-v7 \
  --src-image <frozen-clean-v5.img> \
  --dst-image <new-v7.img> \
  [--size-bytes N] [--inodes I] [--blksize-log L] \
  [--journal-size-bytes N] [--hrl-entry-ratio R] \
  [--v7-group-count N] [--dry-run] [--json]
```

The importer is an offline, v7-owned writer. It parses v5 inode, KDIR,
direct/single/double/triple block-reference, and tail-metadata state read-only;
it does not call a v5/v6 runtime entrypoint and does not broaden the bounded v7
FUSE mutation surface. It preserves stable inode numbers, regular-file
hardlinks, directory parents, symlink payload, permissions, 16-bit uid/gid,
atime/mtime/ctime/dtime, and dense payload bytes. Tombstoned directory records
are omitted from the canonical v7 KDIR output.

Source preflight rejects formats other than v5, a dirty checkpoint/commit
pair, special inode types, invalid or unreachable namespace graphs,
inconsistent regular-file/symlink hardlinks, invalid time or tail descriptors,
pending block references, sparse payloads, and source changes detected by
identity plus whole-image CRC32. Capacity planning includes destination data
and all indirect-index blocks before creation. `--dry-run` performs these
checks without creating either destination path.

Normal execution creates `<dst>.kafs-import-partial` with mode 0600, constructs
group-local v7 inode/data/bitmap/allocator/checkpoint state, runs the full v7
image validator, synchronizes the image, changes it to mode 0644, and only then
publishes `<dst>` without replacement. A failure before the publication link
keeps the final path absent and preserves the private partial image where one
was created. A directory-sync or rollback failure after linking fails the
command and identifies both paths for inspection. Successful publication
normally removes the partial name; if
post-publication name cleanup or its directory sync fails, the durable final
image remains successful and a warning identifies the retained/uncertain work
name. This is an admission-ready image boundary, not migration-lifecycle
acceptance or cutover; T59-C still owns evidence-bundle generation,
interruption recovery by full replay, rollback, and idempotence rehearsal.

`--json` replaces only the normal human result on stdout with
`KAFS.V5V7MigrationImportResult.v1`; diagnostics remain on stderr. PASS records
the exact geometry and counts in `result`, while execution FAIL returns exit 1
with `result: null`. CLI contract errors return exit 2 without JSON. Dry-run
PASS explicitly records no writes and no destination admission readiness.

The disposable-image regression covers nested directories, an inline regular
file, a v5 tail-only regular file, single- and double-indirect regular files, an empty
file, a symlink, a two-path hardlink, metadata equality from a frozen read-only
source view, a two-group destination, `fsck.kafs`, `kafsdump`, and v7
read-only inspection. Negative cases cover special files, sparse block
references, insufficient capacity, and injected partial construction; none
publishes the final destination.

During fixture development, a `13 * 4096 + 73` byte v5 mixed-tail file was
directly observed with one unresolved pending reference and an HRL mismatch
after the attempted source drain. The importer correctly rejected that image;
the normal fixture was changed to the clean exact `13 * 4096` indirect shape.
This is not evidence of an importer cause or a reason to accept pending state.
It remains owned as `SDW-V7RT-T59-B-F1` for v5 source-preparation investigation
and T59-C rejection coverage, off the current importer critical edge.

A second disposable source with 1040 identical 4096-byte blocks reproduced a
different v5 source inconsistency. Inode 6 retained single-indirect root block
720 while the source bitmap marked block 720 free. Default fsck and
`--full-check` both exited zero; the latter reported zero pending, invalid, and
mismatched references. The importer rejected the bitmap-aware traversal with
`EUCLEAN`. The raw mismatch and fsck detection gap are confirmed, while the
causal component and general trigger remain unknown despite correlation with
high duplication, background dedup, and pending-worker activity. A correctly
built block-unique run with background dedup disabled and full fsync still
reproduced the importer rejection, so those controls are not sufficient. This
is owned as `SDW-V7RT-T59-B-F2` by the v5 source-preparation/full-fsck backlog
and T59-C rejection coverage. The double-indirect regression now uses
block-unique data and a valid v5 image with no pending-log region, which makes
the existing runtime use its synchronous write path. This isolates importer
coverage from the unresolved pending-enabled source path; no bitmap check or
repair policy was weakened.

## T59-C Disposable Lifecycle Rehearsal

T59-C joins the T59-A contract and T59-B importer in one repository runner:

```sh
./scripts/v5-v7-migration-rehearsal.sh [--json]
```

The runner accepts report-location, timeout, retention, and JSON-output options
only. It does not accept a source image, destination image, device, or
mountpoint from the caller. Its C
workload creates a disposable clean v5 source, captures its read-only mounted
semantic inventory and whole-image SHA-256, and produces fresh v7 destination
images through `kafsresize --migrate-import-v7`.

One passed report retains the disposable source, normal destination, replayed
destination, attempt-1 partial image, and rollback-preserved failed image. It
also retains fsck/dump output, mounted source/normal/replayed inventories,
executable digests, raw workload output, a SHA-256 artifact manifest, and four
gate-validated lifecycle bundles:

- normal `ACCEPT`;
- interrupted `RESUME_REQUIRED` after exactly two copied objects, meaning a
  full replay is required rather than partial continuation;
- attempt-2 replayed `ACCEPT`; and
- preserved `ROLLBACK` with the unchanged source selected.

The supported attempt-2 strategy is explicit: preserve the attempt-1 partial
image for evidence, then replay the complete import from the same frozen source
and destination identity. This is restart/replay recovery, not in-place
continuation of the partial image. The final source, normal destination, and
attempt-2 destination semantic inventories must be byte-identical JSON. The
source device/inode/size/time identity and whole-image SHA-256 must remain
unchanged. A retry against the already published normal destination must fail
without changing that destination.

The workload also constructs pending-reference and referenced-block/bitmap
inconsistency shapes and proves that neither publishes a final destination.
These are rejection coverage for the owned T59-B-F1/F2 finding shapes; they do
not identify the general trigger or root cause of either finding.

The prerequisites for PASS are the built repository KAFS tools and workload,
`python3`, `sha256sum`, and a usable FUSE device. The default report root is
`report/v5-v7-migration-rehearsal/`; `--report-root` selects another timestamped
parent and `--report-dir` selects one exact new or empty directory.

After report allocation, every outcome retains `result.json` using
`KAFS.V5V7MigrationRehearsalResult.v1`. PASS additionally retains the v2
`rehearsal.json` manifest, bundles, images, and diagnostics. FAIL and SKIP keep
all artifacts produced before the stop. The disposable `work/` tree is removed
unless `--keep-workdir` was specified, and the result records the actual
retention choice. Exit 0 is PASS, 1 is an execution or validation FAIL, 2 is a
usage or prerequisite error, and 77 is an environment SKIP. A usage error
before report allocation has no result file. With `--json`, stdout is exactly
the retained `result.json`; otherwise the compatible human summary is printed.

The default PASS marker remains:

```text
KAFS_V5_V7_MIGRATION_REHEARSAL PASS
```

It proves only the disposable-file-image matrix. It does not qualify production
data, an in-place continuation algorithm, VHDX recovery, physical media, RC status, or
production cutover. No WSL termination or shutdown is part of the runner.

## Automation Result Schema Boundary

The three command-result schemas are separate because they describe different
operations. Their v1 top-level fields are closed contracts; adding, removing,
or changing a field or enum requires a new schema version.

- `KAFS.V5V7MigrationImportResult.v1`: `schema`, `operation`, `status`,
  `exit_status`, `mode`, `source_image`, `destination_image`, `result`, and
  `claims`. `status` is `PASS` or `FAIL`; `mode` is `dry-run` or `import`.
  A PASS `result` contains `source_crc32`, `source_inode_count`,
  `imported_inodes`, `imported_directories`, `imported_regular_files`,
  `imported_symlinks`, `payload_bytes`, `destination_size_bytes`,
  `destination_block_size`, `destination_group_count`, `allocated_blocks`,
  `writes_performed`, and `destination_admission_ready`. FAIL uses null.
  `claims` contains false `production_cutover_authorized` and
  `migration_lifecycle_accepted` values.
- `KAFS.V5V7MigrationEvidenceValidation.v1`: `schema`, `operation`, `status`,
  `exit_status`, `evidence_dir`, `required_decision`, `validated`, `errors`, and
  `claims`. `validated` is populated only on PASS; FAIL has a non-empty
  `errors` array. A populated `validated` object contains `migration_id`,
  `decision`, `source_objects`, `source_entries`, and `attempt`. `claims`
  contains false `data_imported` and `production_cutover_authorized` values.
- `KAFS.V5V7MigrationRehearsalResult.v1`: `schema`, `operation`, `status`,
  `exit_status`, `report_dir`, `report_retained`, `workdir_retained`,
  `artifacts_dir`, `bundles_dir`, `rehearsal_manifest`, `recovery_strategy`,
  `reason`, `prerequisites`, and `claims`. `status` is `PASS`, `FAIL`, or
  `SKIP`, and `recovery_strategy` is `full-replay-from-frozen-source`.
  `prerequisites.fuse` is `AVAILABLE`, `UNAVAILABLE`, or `UNKNOWN`. `claims`
  contains false production-cutover, real-media, physical-media, and
  release-candidate qualification values.

All three emit JSON only when requested; human output remains the default.
Machine consumers must check both process exit status and the matching JSON
`exit_status`, reject unknown schema versions, and must not infer cutover or
media qualification from any PASS result.

## Closeout Evidence

T59-A completed on 2026-07-22 at start HEAD `7a5c750`. The implementation
closed the contract edge only; no importer or actual migration was added.

- `bash -n scripts/v5-v7-migration-evidence-gate.sh tests/v5_v7_migration_evidence_gate_test.sh`: PASS;
- direct synthetic regression and focused Automake regression with
  `kafsresize`: PASS;
- `autoreconf -fi`, `./configure`, and `make -j2`: PASS;
- `KAFS_TEST_MOUNT_TIMEOUT_MS=15000 make check -j2`: all 46 tests passed;
- `./scripts/format.sh`, `./scripts/lint.sh`, `./scripts/clones.sh`, and
  `./scripts/static-checks.sh`: PASS;
- `make dist`: PASS, with this document, the gate, and its regression present
  in `kafs-0.4.0.tar.gz`; and
- `git diff --check`: PASS.

The regression used only synthetic JSON under `${TMPDIR:-/tmp}`. No KAFS
image was created or mounted by the new test, and no PowerShell, VHDX, WSL
terminate/shutdown, physical-device, production-source, or cutover action ran.

T59-B completed on 2026-07-22 at start HEAD `4bebbaa`. The implementation
closed the offline construction edge only; it did not add lifecycle acceptance
or perform a production migration.

- `make -j2` and the focused `v5_v7_import_smoketest`: PASS;
- the final import regression repeated ten times before the final gate: PASS;
- `KAFS_TEST_MOUNT_TIMEOUT_MS=15000 make check -j2`: all 47 tests passed;
- `./scripts/format.sh`, `./scripts/lint.sh`, `./scripts/clones.sh`, and
  `./scripts/static-checks.sh`: PASS; the strict source clone ratio is 0.94%
  and no importer clone is reported;
- `make dist`: PASS, with `kafs_v7_import.c`, `kafs_v7_import.h`, the import
  regression, this document, and `kafsresize.8` present in
  `kafs-0.4.0.tar.gz`; and
- `git diff --check`: PASS.

All new image and mount activity used disposable test paths under
`${TMPDIR:-/tmp}`. No PowerShell, VHDX, WSL terminate/shutdown, physical
device, production source, or cutover action ran. The confirmed
`SDW-V7RT-T59-B-F1` and `SDW-V7RT-T59-B-F2` source-preparation findings remain
owned and do not change the importer's fail-closed pending-reference or bitmap
policy.

T59-C completed on 2026-07-22 at start HEAD `d46cefd`. The focused workload and
Automake rehearsal regression passed with real disposable v5/v7 images and all
four T59-A lifecycle decisions.

- shell and Python syntax checks: PASS;
- focused `v5_v7_migration_rehearsal_test.sh`: PASS;
- `KAFS_TEST_MOUNT_TIMEOUT_MS=15000 make check -j2`: all 48 tests passed;
- `./scripts/format.sh`, `./scripts/lint.sh`, `./scripts/clones.sh`, and
  `./scripts/static-checks.sh`: PASS; the strict source clone ratio remains
  0.94%;
- `make dist`: PASS, with the runner, mounted-inventory helper, regression, and
  this document in `kafs-0.4.0.tar.gz`;
- `./scripts/pert-next-task.sh plans/current.pert`: PASS with no `RUNNABLE NOW`
  node and both external tasks in `BLOCKED NOW`; and
- `git diff --check`: PASS.

All T59-C image and mount activity was created by the test workload under
`${TMPDIR:-/tmp}` or the chosen report directory. No PowerShell, VHDX, WSL
terminate/shutdown, physical device, production source, in-place migration, or
cutover action ran.

`MIGRATION_AUTOMATION_CONTRACT` completed on 2026-07-22 at start HEAD
`3af5ce0`. The three automation surfaces now publish versioned JSON results,
while the established human-readable output remains the default where it was
already public. Rehearsal recovery is explicitly a full replay from the frozen
source, not a partial resume, and `result.json` truthfully records exit status,
prerequisites, report retention, work-directory retention, and the absence of
production acceptance claims.

- `make -j2` and focused importer, evidence-gate, rehearsal, and `kafsresize`
  regressions: PASS;
- `KAFS_TEST_MOUNT_TIMEOUT_MS=15000 make check -j2`: 45 tests passed and
  `stress_fs` self-reported SKIP after its mount attempt failed;
- `./scripts/format.sh`, `./scripts/lint.sh`, `./scripts/clones.sh`, and
  `./scripts/static-checks.sh`: PASS;
- `make dist`: PASS, including the migration scripts, tests, documentation,
  manual, completion, importer source, and cutover playbook;
- `./scripts/pert-next-task.sh plans/cli-v6-retirement.pert`: PASS, with
  `V6_FINAL_ENTRYPOINT_RETIREMENT` as the sole `RUNNABLE NOW` task; and
- `git diff --check`: PASS.

The validation used repository tests and disposable paths only. No PowerShell,
VHDX, WSL terminate/shutdown, physical device, production source,
production mount, in-place migration, or cutover action ran.
