# KAFS format v7 capability rebaseline (2026-07-21)

- Branch: `feat/v7-runtime-admission-foundation`
- Baseline: `460f7b0`
- Status: baseline accepted; T49 software-only expansion complete while M7 awaits hardware identity

## Purpose

This document closes the post-RCA R2 recovery wave, replaces milestone status
that predates the direct-mutation recovery work, and selects the next product
slice from the capability that is present in the current checkout.

The original rebaseline did not widen the admitted mutation surface. The T49
addendum widens one direct transition only: an inline regular file may promote
to one direct block. It does not enable indirect-block mutation, cross-group
allocation, stable/GA claims, or real-device destructive testing.

## Evidence Baseline

The following results were observed on the baseline checkout:

| Evidence | Result |
| --- | --- |
| `make -j2` | PASS under the configured `-Wall -Werror` build |
| `make check -j2` | PASS: 36 tests passed; 5 FUSE-dependent tests were not run after mount startup timed out |
| `./scripts/static-checks.sh` | PASS |
| Active-source clone gate | PASS: 41 clones, 409 duplicated lines, 0.86% against the 1% limit |
| `./scripts/check-v7-layout-ownership.sh` | PASS |
| `./scripts/check-v7-runtime-policy-ownership.sh` | PASS |
| `./scripts/deadcode.sh` | PASS as a report generator; 28 const-style diagnostics remain, with no enabled-path correctness, data-integrity, durability, portability, or unused-function diagnostic |
| `git diff --check` | PASS for the rebaseline documentation changes |

The five not-run tests are an environment limitation, not passing FUSE
evidence. The v7 non-FUSE and image-path tests in that run passed. Real media,
controller, kernel/FUSE, and controlled-power-interruption evidence has not yet
been collected.

## Current Capability Matrix

| Surface | Current capability | Boundary |
| --- | --- | --- |
| Image creation | `mkfs.kafs` creates explicit format v7 images | v7 is not the default production format |
| Offline validation | `fsck.kafs`, `kafsdump`, layout/policy checks, and wear-placement proof understand the current v7 image contract | This is software/image evidence, not physical NAND/FTL proof |
| Offline migration | `kafsresize --migrate-create` can build a v7 destination image | No in-place metadata relocation or automatic cutover |
| Runtime admission | Dedicated `kafs-v7` entrypoint supports inspection and explicit controlled-write mounts | Successful admission does not route through v5/v6 public entrypoints |
| Inspection mount | Read-only FUSE view with selected recovery state and recovered `statfs` counters | Mutation fails closed |
| Controlled write | Existing direct-block regular-file overwrite/growth, direct-only shrink above the inline boundary or to zero, empty-file creation, inline-file writes, inline-to-one-direct-block regular-file promotion, and same-group inline/direct directory append/growth through the twelve direct references | Non-zero direct-to-inline conversion, holes, promotion beyond one block, indirect mutation, cross-group allocation, and unrelated metadata mutation are rejected |
| Request contract | One FUSE write callback is one journal transaction; negotiated `max_write` is capped at twelve v7 blocks | No application-system-call atomicity claim across kernel-split requests |
| Recovery | Admission closeout, interruption matrices, offline fsck, and readback cover the enabled direct mutation transitions | No real-card power-interruption qualification yet |
| Indirect foundation | Direct/single/double/triple address calculation, walk, and retirement guard exist in v7-owned code | Indirect write/create/truncate admission remains `EOPNOTSUPP` |
| Cross-group mutation | Not enabled | Design direction remains deferred by the accepted boundary |

## R2 Closeout

R2 is closed at this baseline because:

1. aggregate static checks preserve report collection and return nonzero when a
   constituent fails;
2. the active-source clone result passes the unchanged 1% policy after removing
   unreachable descriptor-wire code and preserving the frozen-v6 ownership
   exception;
3. selector-bound, fsck portability/read, repair-result, HRL release-failure,
   and unsigned-zero findings with semantic impact were corrected;
4. the remaining cppcheck output is explicitly owned as repository-wide
   const-style hygiene and does not identify an enabled-path correctness,
   data-integrity, or durability defect;
5. build/test, format/lint/clone/complexity/cppcheck, ownership, and Git evidence
   have explicit results.

This disposition does not turn complexity or test-clone reports into zero-debt
claims. They remain measured maintenance inputs. The missing local `reuse`
executable is also a tooling prerequisite for the separate license script, not
an R2 correctness waiver.

## Goal And Critical Path Decision

The accepted product priorities are deterministic recovery/fail-closed
behavior and SD-card write distribution. The current implementation has a
substantial bounded direct-write surface with software recovery evidence, but
it has not crossed the real-device RC qualification boundary.

```text
R1 cardinality-independent direct mutation
  -> R2 structural/static closure
  -> current capability rebaseline
  -> M7 controlled-write RC qualification
  -> separately approved expansion or release decision
```

M7 is selected before M8-C, M9, and M10:

- **M7 controlled-write RC qualification** is the shortest path from the
  enabled surface to evidence for the accepted fault-tolerance and wear goals.
- **M8-C indirect mutation** would enlarge the journal, recovery, allocator,
  and qualification surface before the currently enabled path is qualified.
- **M9 migration/cutover** already has destination-image creation, but a full
  operator cutover claim should depend on a qualified destination runtime.
- **M10 cross-group allocation** remains behind an explicit design-direction
  decision and is not inferred from the current same-group implementation.

## T49 Deferred-Media Addendum

Exact SD-card, reader/controller, and isolated power-cut identities were not yet
available, and the user explicitly deferred their preparation. This blocks the
physical part of M7 without changing its priority once hardware is ready. The
2026-07-21 refreshed Task Start Gate therefore selected
`SDW-V7RT-T49 regular-file inline-to-direct promotion` as the smallest coherent
software-only closure.

T49 reuses the existing same-group data COW transaction and the already proven
directory representation-transition pattern. Its admitted state is limited to
a regular inode with `blocks=0`, size at most 60 bytes, no hole, and a request
ending within one filesystem block. It zero-initializes and stages one new data
block, preserves the old inline bytes, applies the request, and atomically
publishes allocator state plus the inode's first direct reference, size, and
block count. There is no retained block to retire.

Observed T49 software evidence includes the low-level transition and immediate
direct-COW chaining, actual FUSE write/full-fsync/read-only-remount, offline
fsck, and journal-publication, metadata-apply, and checkpoint-copy recovery.
The refreshed non-destructive qualification report passed 26/26 required cases
with 90 digest-checked artifacts. Its RC, real-media, and
controller-independent-wear claims remain false.

This exception is smaller than M8-C because it adds no indirect reachability or
path-copy contract. M9 cutover still depends on a qualified destination runtime,
and M10 still requires an explicit cross-group design decision. When hardware is
available, M7 resumes against the expanded matrix, including the T49 promotion
workload.

## Next Task: SDW-V7RT-T48 Controlled-write RC Qualification Gate

### Scope

- define the exact host, kernel, libfuse, card, reader/controller, and power-cut
  identity recorded for each sample;
- add a repository-run qualification procedure that formats, mounts, executes
  each currently enabled mutation class, performs full `fsync`, introduces
  controlled interruption points, remounts, and runs `fsck.kafs` plus
  `kafsdump`;
- retain raw command logs, image/sample identifiers, digests, and observed
  recovery outcomes;
- require independent review of the evidence before any RC wording is accepted;
- keep physical-device formatting and power interruption behind an explicit
  device/sample matrix and explicit operator authorization.

### Exit Criteria

1. Every enabled mutation class has a normal-path and controlled-interruption
   case on the approved matrix.
2. Each interrupted sample either recovers to an allowed state or fails closed,
   with offline fsck and dump evidence.
3. The result distinguishes filesystem-placement evidence from unverified
   NAND/FTL behavior.
4. The report records skipped, inconclusive, and environment-limited cases
   separately from PASS.
5. An independent reviewer accepts or rejects the bounded RC claim from the raw
   evidence.

### Non-goals

- enabling indirect or cross-group mutation;
- claiming stable or GA readiness;
- claiming controller-independent wear behavior;
- formatting any real device without the operator's explicit authorization.
