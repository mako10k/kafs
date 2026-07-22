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
