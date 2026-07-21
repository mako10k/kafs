# KAFS format v7 controlled-write qualification plan

- Task: `SDW-V7RT-T48`
- Current software refresh: `SDW-V7RT-T49 regular-file inline-to-direct promotion`
- T48-A baseline: `3a86fde`
- Status: T48-A and the T49 file-image refresh complete; real-media qualification not started

## Goal And Critical Path

The accepted v7 goal is an SD-card-friendly filesystem that keeps recovery and
offline validation deterministic while distributing mutable metadata away from
one hot prefix. The current bounded direct controlled-write surface has software
recovery evidence but has not crossed the real-media RC boundary.

T48-A closes one dependency on that path: it turns the existing software
recovery matrix into a repeatable evidence-producing workflow before any real
device is selected or modified.

```text
bounded direct controlled write
  -> T48-A non-destructive file-image evidence pipeline
  -> approved card/controller/power-cut sample matrix
  -> real-media controlled interruption runs
  -> independent review
  -> bounded M7 RC decision
```

The schema and validator are not separate cleanup tasks. The slice exits only
when the runner has generated evidence from the FUSE workload and the gate has
validated that report.

## Task Start Record

- Branch: `feat/v7-runtime-admission-foundation`
- Start HEAD: `3a86fde`
- Current capability: explicit v7 format/inspection/controlled-write admission,
  bounded direct overwrite/growth/truncate/create/directory transitions, and
  process-fault recovery through journal publication, metadata apply,
  checkpoint copy, and journal reclaim.
- Current gap: no repeatable operator report joining the FUSE workload, raw
  logs, image digests, offline fsck/dump, environment identity, and an explicit
  non-RC claim boundary.
- Invariants: v7 owns its entrypoint and evidence workflow; v6 public paths are
  not a compatibility shortcut; skipped or inconclusive work is not PASS.
- Non-goals: runtime feature expansion, `/dev/*`, caller-supplied images, real
  SD-card formatting, physical power interruption, RC approval, and NAND/FTL
  independence claims.
- Start decision: `PASS`.

## Workload Boundary

`scripts/v7-controlled-write-qualification-dry-run.sh` invokes the existing
`tests/v7_inspection_mount_smoketest` as its workload engine. That test starts
with `mkfs.kafs --format-version 7`, then uses v7 test-owned fixture preparation
to expose block-backed files and directory boundary states before mounting the
image through `kafs-v7`.

The fixture preparation is explicit evidence scaffolding, not an operator image
creation claim. T49 now permits a file created through the public
controlled-write surface to cross the 60-byte inline boundary into one direct
block. Pre-seeded block-backed files remain necessary to exercise the wider
multi-block and direct-limit matrix.

The workload emits one stable marker for each completed case. A zero test exit
without every required marker and raw log is a failed qualification dry run.

## Required Case Matrix

| Class | Required evidence |
| --- | --- |
| Format and inspection | accepted format/seed image, inspection mount |
| Direct overwrite | partial and multi-block overwrite through FUSE |
| Direct geometry | contiguous growth, shrinking above the inline boundary or to zero, non-zero direct-to-inline rejection, `O_TRUNC` |
| Create | create, inline-file write, inline-to-one-direct-block promotion, and full fsync |
| Directory transitions | inline append/growth, direct append/growth representatives, direct-limit rejection |
| Recovery | journal publication, metadata apply, checkpoint copy, journal reclaim, and promotion-specific process interruption |
| Admission | degraded inspection succeeds; unpaired generation fails closed |
| Closeout | inspection remount, offline `fsck.kafs`, and `kafsdump --json` |

The current process-fault cases exercise the durable transaction boundaries.
They do not model loss of power to a card/controller and cannot replace the
later real-media sample matrix.

## Evidence Contract

The runner writes `qualification.json` with schema
`KAFS.V7ControlledWriteQualification.v1` and preserves raw artifacts below the
same report directory.

Required fields include:

- overall `PASS`, `SKIP`, or `FAIL` status;
- file-image sample identity and the explicit
  `process-fault-injection` interruption model;
- Git revision, dirty state, UTC timestamp, kernel, uname, libfuse version, and
  SHA-256 identities for the runner, gate, workload, and invoked v7 tools;
- one result for every required case;
- relative artifact paths with byte counts and SHA-256 digests;
- false RC, real-media, and controller-independent-wear claims;
- explicit limitations for physical power loss and NAND/FTL independence.

`scripts/v7-controlled-write-qualification-gate.sh --validate-only` performs no
mount or image mutation. It rejects missing cases, any non-PASS result, missing
sample/environment identity, unsafe device claims, absent limitations, unknown
evidence references, and artifact size/digest mismatch.

## Status Semantics

- `PASS`: the file-image FUSE workload, every required marker, raw-log set,
  final fsck/dump, and validate-only gate all passed.
- `SKIP`: the host cannot run the FUSE workload, for example because
  `/dev/fuse` is unavailable. The runner exits 77 and still records a report.
- `FAIL`: the workload, artifact collection, offline validation, required case
  coverage, or gate failed.
- `INCONCLUSIVE`: reserved for the later real-media matrix. It must never be
  accepted as PASS.

Even a T48-A PASS has `rc_eligible=false` and is not M7 closeout.

## Commands

Build the workload engine and run the non-destructive report:

```sh
make -j2
make -C tests v7_inspection_mount_smoketest
./scripts/v7-controlled-write-qualification-dry-run.sh
```

Revalidate a preserved report without mounting anything:

```sh
./scripts/v7-controlled-write-qualification-gate.sh \
  --report-dir report/v7-controlled-write-qualification/<timestamp> \
  --validate-only
```

## T48-A Exit Criteria

1. The runner accepts no image/device input and only creates temporary sparse
   files below its report work directory.
2. The actual FUSE workload emits every required case marker and preserves the
   mapped raw logs.
3. The report records environment/sample identity, case status, artifacts,
   digests, limitations, and false RC/media claims.
4. The gate accepts a complete synthetic and actual report and rejects skip,
   missing identity, false RC/device claims, and changed artifacts.
5. FUSE unavailability is reported as SKIP/77, not PASS.
6. Focused regression, full tests, formatting/lint, ownership, clone,
   complexity, cppcheck, and Git checks have explicit results.

## T48-A Closeout Evidence

The 2026-07-21 local dry run under
`report/v7-controlled-write-qualification/current-slice-final` completed with
all 23 required cases at PASS and 84 digest-checked artifacts. The report
records Linux
`6.18.33.2-microsoft-standard-WSL2`, libfuse `3.14.0`, a temporary sparse image,
and `process-fault-injection`; all RC, real-media, and controller-independent
wear claims remain false.

Validation results:

- focused Automake gate: 2/2 PASS;
- full `make check -j2`: 41/41 PASS, with one unrelated existing FUSE test
  skipped after its mount startup timed out;
- format, lint, v7 layout ownership, v7 runtime policy ownership, and v7
  filesystem placement proof: PASS;
- aggregate static checks: PASS; the strict source clone result remains 41
  clones and 409 duplicated lines (0.86%);
- `make dist`: PASS; the archive contains the runner, validate-only gate, and
  synthetic gate regression;
- cppcheck: 28 existing const-style diagnostics, with no semantic,
  portability, or unused-function finding attributable to this slice;
- Git whitespace and generated-artifact checks: PASS; T48-A was consolidated
  into `9a9ef10` with no WIP commit left in the final history;
- the validate-only gate accepted the actual report and the synthetic positive
  case, and rejected the synthetic skipped, missing-identity, false-RC-claim,
  device-path, and changed-artifact cases.

This closes T48-A only. It does not satisfy the controlled power-interruption,
real-media, or independent-review conditions of T48 and M7.

## T49 File-Image Refresh

The 2026-07-21 T49 working-tree dry run completed with all 26 required cases at
PASS and 90 digest-checked artifacts. The three added cases are
`regular_inline_promotion`, `direct_to_inline_truncate_rejection`, and
`regular_inline_promotion_recovery`; the latter covers journal publication,
metadata apply, and checkpoint-copy interruption for the representation change.
The synthetic validate-only gate also passed with all three cases required.

This refresh widens only the software/file-image evidence. All RC, real-media,
and controller-independent-wear claims remain false. The DRAFT real-media
matrix now includes the promotion workload and must receive a new digest-bound
approval after exact hardware identities are supplied.

## T53 Single-Indirect File-Image Refresh

The 2026-07-21 T53 dry run completed with all 29 required results at PASS and
97 digest-checked artifacts. The three added results are
`single_indirect_write_truncate`, `single_indirect_to_direct_truncate`, and
`single_indirect_recovery_matrix`; the recovery result requires the raw journal
publication, metadata apply, and checkpoint-copy recovery logs. The synthetic
validate-only gate also requires all three results.

This remains non-destructive file-image evidence. RC, real-media, and
controller-independent-wear claims remain false. The DRAFT real-media matrix
now includes `regular_file_single_indirect_lifecycle`, so every earlier draft
digest is obsolete and cannot authorize execution.

## T54 Double-Indirect File-Image Refresh

The 2026-07-21 T54 dry run completed with all 32 required results at PASS and
104 digest-checked artifacts. T54 adds three required results:
`double_indirect_write_truncate`, `double_indirect_to_single_truncate`, and
`double_indirect_recovery_matrix`. The normal path crosses the single boundary,
performs double-depth overwrite and shrink, verifies persisted leaf/root/data
blocks, remounts read-only, and then contracts through single and direct to
zero. The recovery result requires raw journal-publication, metadata-apply, and
checkpoint-copy logs for a double-leaf tail COW.

This remains non-destructive file-image evidence. RC, real-media, and
controller-independent-wear claims remain false. The DRAFT real-media matrix
adds `regular_file_double_indirect_lifecycle`; all previous draft digests are
obsolete and cannot authorize execution.

## T55 Triple-Indirect File-Image Refresh

The 2026-07-21 T55 dry run completed with all 37 required results at PASS and
116 digest-checked artifacts. T55 adds five required results:
`triple_indirect_write_truncate`, `triple_indirect_to_double_truncate`,
`triple_indirect_contraction_matrix`, `triple_indirect_middle_boundary`, and
`triple_indirect_recovery_matrix`. The normal evidence crosses double to triple
depth, verifies partial shrink and invalid unused references, contracts directly
to every lower representation, and crosses the first triple middle-table
boundary. The recovery result requires raw journal-publication, metadata-apply,
and checkpoint-copy recovery logs for a triple tail COW.

This remains non-destructive file-image evidence. RC, real-media, and
controller-independent-wear claims remain false. The DRAFT real-media matrix
adds `regular_file_triple_indirect_lifecycle`; all previous draft digests are
obsolete and cannot authorize execution.

## Follow-On Boundary

T48-B1 adds the fail-closed matrix and digest-bound approval contract in
`docs/sd-card-wear-v7-real-media-qualification-approval.md`. Current read-only
discovery found no eligible real-media device, so the repository matrix remains
`DRAFT`. The next hardware-path action is to supply the exact host, card unit,
reader/controller, isolated power-cut apparatus, trigger protocol, and cycle
count, then request approval for the resulting matrix SHA-256. Only after that
approval may a later T48 slice add a `/dev/*` runner and physical
controlled-interruption procedure. Independent review remains required before
any bounded RC decision.
