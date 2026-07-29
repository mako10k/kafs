# KAFS format v7 real-media evidence and review contract

- Task: `SDW-V7RT-T58`
- Start HEAD: `173ef72`
- Evidence schema: `KAFS.V7RealMediaQualificationEvidence.v1`
- Review schema: `KAFS.V7RealMediaQualificationReview.v1`
- Gate: `scripts/v7-real-media-qualification-evidence-gate.sh`
- Safety boundary: validate-only; no device, filesystem, or power operation

## Purpose

This contract separates authorization, destructive evidence capture, and
independent review. T48-B1 already proves that an exact matrix and approval can
be bound by SHA-256. T58 defines what a later runner must preserve and what an
independent reviewer must decide before any bounded real-media evidence may be
accepted.

The gate validates records and files only. It does not introduce a `/dev/*`
runner, open or resolve a device, format or mount media, control power, or make
an RC claim. Passing evidence validation means that a bundle is structurally
complete and digest-consistent. Passing review validation means that a separate
review record is internally valid; only an `ACCEPT` decision with all results
at `PASS` and all review checks true is accepted as such.

## State Boundaries

```text
READY_FOR_APPROVAL matrix
  -> separately APPROVED record
  -> COMPLETE evidence bundle
  -> FINAL independent review
  -> ACCEPT | REJECT | INCONCLUSIVE
```

- Authorization is the existing `KAFS.V7RealMediaQualificationApproval.v1`
  record. The evidence gate reuses the approval gate at the evidence start time.
- Capture is one immutable directory containing `evidence.json` and every
  relative artifact it hashes.
- Review is a separate JSON file. It binds the byte-for-byte evidence digest
  and may not be authored by the evidence operator.
- Input claims `rc_eligible`, `real_media_qualified`, and
  `controller_independent_wear` remain false in both evidence and review. The
  gate result is evidence-contract validation, not release-state mutation.

## Evidence Bundle

`evidence.json` records:

- schema, real-media scope, `COMPLETE` state, and a unique run id;
- exact matrix id, matrix SHA-256, and approval SHA-256;
- operator id and start/completion timestamps;
- the target-host environment copied exactly from the approved matrix;
- immutable retention duration copied from the matrix;
- every artifact's relative path, semantic kind, byte count, and SHA-256;
- every approved sample's exact card, reader/controller, device, and power-cut
  identity before and after execution; and
- one result for every approved
  `sample × workload × (normal + interruption boundary) × cycle` tuple.

Each result uses only `PASS`, `FAIL`, `SKIP`, or `INCONCLUSIVE`, records whether
the observed state is allowed, describes that state, and references hashed raw
artifacts. Normal results must reference command, workload, mount, post-fsck,
and post-dump evidence. Interrupted results must additionally reference the
power-cut log.

The artifact inventory must contain every kind named by the approved matrix:
environment, identities before/after, commands, workload and mount logs,
power-cut evidence, fsck/dump before and after, and the SHA-256 inventory. Paths
must remain below the evidence directory. Missing files, path traversal,
duplicate paths, size mismatch, or digest mismatch fail closed.

An abbreviated shape is:

```json
{
  "schema": "KAFS.V7RealMediaQualificationEvidence.v1",
  "scope": "format-v7-bounded-controlled-write-real-media",
  "state": "COMPLETE",
  "run_id": "<unique run id>",
  "binding": {
    "matrix_id": "<approved matrix id>",
    "matrix_sha256": "<exact matrix digest>",
    "approval_sha256": "<exact approval digest>"
  },
  "operator": {"id": "<operator identity>"},
  "started_at_utc": "<ISO-8601 timestamp>",
  "completed_at_utc": "<ISO-8601 timestamp>",
  "environment": {},
  "claims": {
    "rc_eligible": false,
    "real_media_qualified": false,
    "controller_independent_wear": false
  },
  "retention": {
    "immutable_copy_created": true,
    "minimum_days": 180,
    "retain_through": "M7 decision plus 180 days"
  },
  "artifacts": [],
  "samples": []
}
```

The empty objects and arrays above are placeholders for exposition and will not
pass the gate.

## Independent Review

The separate review binds matrix, approval, and `evidence.json` SHA-256 values,
repeats the run and operator identities, names a different reviewer, and records
one final decision. Required boolean checks cover:

1. approval and digest binding;
2. device/controller/power identity continuity;
3. complete workload, boundary, and cycle coverage;
4. raw artifact digest verification;
5. allowed durable outcomes; and
6. the bounded claim boundary.

`ACCEPT` requires every captured result to be `PASS` and every check to be true.
`REJECT` and `INCONCLUSIVE` require at least one textual finding. This preserves
negative or uncertain raw evidence without relabeling it as a passing run.

An abbreviated review shape is:

```json
{
  "schema": "KAFS.V7RealMediaQualificationReview.v1",
  "state": "FINAL",
  "review_id": "<unique review id>",
  "run_id": "<evidence run id>",
  "binding": {
    "matrix_id": "<approved matrix id>",
    "matrix_sha256": "<exact matrix digest>",
    "approval_sha256": "<exact approval digest>",
    "evidence_sha256": "<exact evidence.json digest>"
  },
  "operator_id": "<evidence operator identity>",
  "reviewer": {"id": "<different reviewer identity>"},
  "decision": "ACCEPT",
  "reviewed_at_utc": "<ISO-8601 timestamp>",
  "raw_evidence_reviewed": true,
  "checks": {
    "approval_binding_verified": true,
    "identity_continuity_verified": true,
    "coverage_complete": true,
    "artifact_digests_verified": true,
    "allowed_outcomes_verified": true,
    "claim_boundary_verified": true
  },
  "findings": [],
  "claims": {
    "rc_eligible": false,
    "real_media_qualified": false,
    "controller_independent_wear": false
  },
  "confirmation": "REVIEW T58 <run-id> <evidence-sha256> ACCEPT"
}
```

The placeholders above do not pass validation. For a non-`ACCEPT` decision,
`findings` must contain at least one explanation.

The review confirmation is exact:

```text
REVIEW T58 <run-id> <evidence-sha256> <ACCEPT|REJECT|INCONCLUSIVE>
```

## Validate-only Commands

Validate a complete evidence bundle before review:

```sh
./scripts/v7-real-media-qualification-evidence-gate.sh \
  --evidence-dir <immutable-evidence-dir> \
  --matrix <ready-matrix.json> \
  --approval <approval.json> \
  --validate-only
```

Validate the same bundle and the independent decision:

```sh
./scripts/v7-real-media-qualification-evidence-gate.sh \
  --evidence-dir <immutable-evidence-dir> \
  --matrix <ready-matrix.json> \
  --approval <approval.json> \
  --review <review.json> \
  --validate-only \
  --require-review
```

The evidence start timestamp is passed to the approval gate as `--as-of`, so a
retained bundle remains verifiable after the approval expires while still
proving that authorization was active when the run began. The evidence and raw
timestamps remain review inputs; this validation is not a trusted time-stamp
service.

## Implementation Boundary

The synthetic regression builds an approved one-sample matrix and complete
cross-product bundle without opening a device. It proves the positive evidence
and `ACCEPT` paths, a valid `INCONCLUSIVE` review, and rejection of missing
cycles, identity drift, wrong matrix/approval/evidence digests, artifact
tampering, operator self-review, claim escalation, incomplete review checks,
and false acceptance of a non-PASS result.

A later real-media runner must emit this contract but is not part of T58. It
must still pass the separate hardware approval, audited VHDX qualification, and
its own fresh Task Start Gate before destructive execution.

## Closeout Evidence

T58 closed on 2026-07-22 without any device, mount, format, power, or WSL
operation. Direct and focused Automake regression passed for the approval,
evidence, and qualification gates. The full suite passed 43 tests; `stress_fs`
was the single environment-limited FUSE skip. Formatting, lint, clone/static
checks, build, and source distribution generation also passed. The source clone
baseline remained 48 clones, 490 duplicated lines, and 0.97% duplication.
