# KAFS format v7 real-media qualification approval contract

- Task: `SDW-V7RT-T48-B1`
- Baseline: `9a9ef10`
- Matrix: `docs/sd-card-wear-v7-real-media-qualification-matrix.json`
- Status: T48-B1 contract complete; exact real-media identity and approval are pending
- Safety boundary: this slice performs no raw-device I/O or power control

## Goal And Current Position

The accepted goal is a bounded format-v7 RC decision supported by real-SD-card
controlled-write, full-fsync, controlled power-interruption, offline recovery,
and independent-review evidence. T48-A closed the non-destructive file-image
evidence path. The remaining critical path is:

```text
T48-A file-image evidence
  -> T48-B1 exact matrix and digest-bound approval contract
  -> identify the physical card, reader/controller, host, and power switch
  -> operator approval of the completed matrix digest
  -> real-media runner and controlled interruption
  -> independent raw-evidence review
  -> bounded M7 RC decision
```

Implementing a `/dev/*` runner before the identity and approval steps would
cross the destructive boundary without an agreed target. Expanding indirect or
cross-group mutation first would enlarge the surface that still lacks
qualification. T48-B1 is therefore the shortest safe dependency slice.

## Task Start Record

- Branch: `feat/v7-runtime-admission-foundation`
- Start HEAD: `9a9ef10`
- Worktree at start: clean and two commits ahead of the remote tracking branch.
- Source: the 2026-07-21 T48-A closeout, capability rebaseline, accepted v7
  raw-layout RC boundary, and current read-only block-device discovery.
- Direct observation: the current WSL2 host exposes four Microsoft Virtual
  Disks and no removable or SD-transport candidate. It cannot supply an exact
  card, reader/controller, or isolated power-cut identity.
- Still valid: T48-A is complete; real-media evidence and independent review
  remain mandatory; approval must precede physical formatting and power loss.
- Unknown: target host, physical card unit, reader/controller, stable device
  identity, isolated power-cut apparatus, trigger protocol, and cycle count.
- Replan decision: the original exact-matrix execution slice is `REPLAN` until
  those identities exist. The fail-closed matrix/approval contract is `PASS`.
- Non-goals: inventing missing hardware identity, raw-device execution,
  formatting, mounting, power control, RC approval, and broader mutation.

## Matrix States

The matrix schema is `KAFS.V7RealMediaQualificationMatrix.v1`.

- `DRAFT`: unresolved fields are named in `blocked_by`. Raw-device and physical
  power-cut execution without approval remain false. Draft validation does not
  authorize work.
- `READY_FOR_APPROVAL`: every host, sample, device, controller, power-cut, and
  cycle field is exact; `blocked_by` is empty. This state still does not
  authorize work.
- `APPROVED` is not a matrix state. It is a separate operator record bound to
  the byte-for-byte matrix SHA-256, authorized actions, and expiration time.

The gate is validate-only and never opens the named device:

```sh
./scripts/v7-real-media-qualification-approval-gate.sh \
  --matrix docs/sd-card-wear-v7-real-media-qualification-matrix.json \
  --validate-only
```

An execution preflight must additionally require an approval record:

```sh
./scripts/v7-real-media-qualification-approval-gate.sh \
  --matrix <ready-matrix.json> \
  --approval <approval.json> \
  --validate-only \
  --require-approved
```

Passing either command only validates records. No real-media execution path is
introduced by T48-B1.

## Exact Identity Requirements

Each approved sample must record:

- host id, kernel, libfuse, and native/passthrough environment;
- physical card manufacturer, model, capacity in bytes, per-unit id, and
  whether that id comes from CID, serial, or a durable inventory label;
- reader/controller manufacturer, model, stable id, and transport;
- stable `/dev/disk/by-id/*` or `/dev/disk/by-path/*` identity, the resolved
  kernel path, major:minor, and exact size;
- an isolated power-cut method, apparatus id, trigger protocol, and power
  domain that cannot remove power from host or system storage;
- an explicit positive cycle count for the full workload/interruption matrix.

A volatile `/dev/sdX` name is recorded only as the resolved kernel path. It is
never sufficient as the stable identity. Removable flags are supporting
evidence, not identity. Before any later execution, all stable-path, model,
unit-id, size, whole-device, mount, root, swap, device-mapper, and power-domain
checks must agree with the approved matrix or fail closed.

## Destructive Impact

Approval acknowledges all of the following:

1. Formatting targets the whole listed device and destroys the partition table
   and all existing data.
2. Only disposable test media may be listed. Backup or recovery is not assumed.
3. The power switch must affect the listed reader/card only. Host, root, swap,
   and unrelated storage power loss is forbidden.
4. A changed matrix, changed device resolution, expired approval, mounted
   partition, ambiguous identity, or missing evidence stops the run.
5. Physical interruption can damage the test card and may make it permanently
   unreadable.

## Workload And Result Contract

Every listed sample runs the current bounded surface normally and at every
controlled interruption boundary. `interruption_cross_product_required=true`
means the approved cycle count applies to each workload/boundary combination.

Normal workloads cover partial and multi-block overwrite, contiguous direct
growth, direct shrink above the inline boundary or to zero, non-zero
direct-to-inline rejection, `O_TRUNC`, inline create/write, regular-file
inline-to-one-direct-block promotion, inline directory append/growth, and
direct directory append/growth. Interruption boundaries are journal
publication, metadata apply, checkpoint-copy publication, and journal reclaim.
Each cycle uses the controlled-write safe option set and full fsync, then
remounts or fails closed and runs `fsck.kafs --check` plus `kafsdump --json`.

T49 added the regular-file promotion workload while this matrix remained
`DRAFT`. Any previously calculated draft digest is therefore obsolete; later
operator approval must bind the byte-for-byte digest of the completed matrix
that includes this workload.

Results are only `PASS`, `FAIL`, `SKIP`, or `INCONCLUSIVE`. Missing timing
evidence, uncertain power isolation, or an environment-limited cycle is not
PASS. A recovered sample is PASS only when the observed state is one of the
allowed durable outcomes and the offline evidence agrees.

## Evidence Retention

The current proposal retains an immutable copy through the M7 decision plus at
least 180 days. Each cycle preserves environment and device identity before and
after, exact commands, workload and mount logs, power-cut trigger evidence,
before/after dump and fsck output, and a SHA-256 inventory. Raw artifacts remain
the authority; summaries do not replace them.

## Independent Review Checklist

The reviewer must differ from the operator and must decide `ACCEPT`, `REJECT`,
or `INCONCLUSIVE` from raw artifacts. The review checks:

1. approval matrix id, digest, validity interval, and exact physical identities;
2. no target drift between identity-before and identity-after evidence;
3. complete workload/interruption/cycle coverage with no false PASS for skipped
   or inconclusive cases;
4. full-fsync completion before the declared cut trigger where applicable;
5. remount recovery or fail-closed behavior, followed by consistent fsck/dump;
6. artifact digest completeness and immutable-copy retention;
7. claims remain bounded to filesystem behavior and do not imply NAND/FTL,
   controller-independent wear, stable, or GA readiness.

Independent acceptance may support a bounded M7 RC recommendation. It does not
itself alter release state.

## Approval Record

After the matrix reaches `READY_FOR_APPROVAL`, compute its SHA-256 and create a
separate record in this shape:

```json
{
  "schema": "KAFS.V7RealMediaQualificationApproval.v1",
  "state": "APPROVED",
  "matrix_id": "<exact matrix id>",
  "matrix_sha256": "<64 lowercase hex characters>",
  "approved_by": "<operator identity>",
  "approved_at_utc": "<ISO-8601 timestamp>",
  "valid_until_utc": "<future ISO-8601 timestamp>",
  "authorized_actions": [
    "format_whole_device",
    "controlled_power_interruption"
  ],
  "destructive_impact_acknowledged": true,
  "disposable_media_confirmed": true,
  "confirmation": "AUTHORIZE T48 <matrix id> <matrix sha256>"
}
```

The repository draft intentionally has no approval record. It must remain
non-executable until the missing identities are supplied and the exact digest
is explicitly approved.

## T48-B1 Closeout Evidence

The repository draft validates as `DRAFT_VALID` with matrix SHA-256
`c1e2b467e94b937101f91c8223b649794e91f4ce8d0462e89a3725bb6ea8d36a`.
This digest identifies the current blocked draft and is not an execution
approval.

Validation results on 2026-07-21:

- direct draft validation: PASS;
- synthetic `READY_FOR_APPROVAL` plus digest-bound, unexpired approval: PASS;
- negative regression rejects approval-free execution, volatile-only device
  identity, system/host storage, matrix digest mismatch, expired approval, and
  approval of a draft matrix: PASS;
- Automake gate regressions: 2/2 PASS;
- `autoreconf -fi`, `./configure`, and lint: PASS;
- `make dist`: PASS; the archive contains the contract, draft matrix, gate, and
  regression.

No device was opened, mounted, formatted, or power-controlled. T48-B1 closes
the approval-contract dependency only. Exact physical identity and operator
approval remain the next blocking inputs before a real-media runner can start.
