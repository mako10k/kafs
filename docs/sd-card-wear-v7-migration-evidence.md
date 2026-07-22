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
  --validate-only
```

Replace `ACCEPT` with `RESUME_REQUIRED` or `ROLLBACK` when validating those
retained states. Requiring the decision at the command line prevents a valid
rollback bundle from being mistaken for accepted migration evidence.

Success includes:

```text
KAFS_V5_V7_MIGRATION_EVIDENCE PASS migration_id=<id> decision=<decision>
```

The marker proves schema, lifecycle, semantic-inventory, and digest
consistency only. It does not prove a working importer or authorize cutover.

## Regression Boundary

The regression constructs source semantics containing nested directories,
regular files including an empty file, a symlink, and a two-path hardlink. It
validates a resumed `ACCEPT`, an interrupted `RESUME_REQUIRED`, and a preserved
`ROLLBACK` without creating or mounting KAFS images.

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
  [--v7-group-count N] [--dry-run]
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
name. This is an admission-ready image boundary, not migration-lifecycle acceptance or cutover;
T59-C still owns evidence-bundle generation, interruption resume, rollback,
and idempotence rehearsal.

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
./scripts/v5-v7-migration-rehearsal.sh
```

The runner accepts report-location and timeout options only. It does not accept
a source image, destination image, device, or mountpoint from the caller. Its C
workload creates a disposable clean v5 source, captures its read-only mounted
semantic inventory and whole-image SHA-256, and produces fresh v7 destination
images through `kafsresize --migrate-import-v7`.

One passed report retains the disposable source, normal destination, resumed
destination, attempt-1 partial image, and rollback-preserved failed image. It
also retains fsck/dump output, mounted source/normal/resumed inventories,
executable digests, raw workload output, a SHA-256 artifact manifest, and four
gate-validated lifecycle bundles:

- normal `ACCEPT`;
- interrupted `RESUME_REQUIRED` after exactly two copied objects;
- attempt-2 `ACCEPT`; and
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

The report marker is:

```text
KAFS_V5_V7_MIGRATION_REHEARSAL PASS
```

It proves only the disposable-file-image matrix. It does not qualify production
data, an in-place resume algorithm, VHDX recovery, physical media, RC status, or
production cutover. No WSL termination or shutdown is part of the runner.

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
