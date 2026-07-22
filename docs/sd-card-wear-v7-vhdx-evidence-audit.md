# KAFS v7 VHDX four-point evidence audit contract

- Task: `SDW-V7RT-T57`
- Start HEAD: `17d8820`
- Gate: `scripts/v7-vhdx-evidence-audit-gate.sh`
- Input schemas: `KAFS.V7VhdxArmContext.v1`,
  `KAFS.V7VhdxHostController.v1`, and `KAFS.V7VhdxRecoveryEvidence.v1`
- Safety boundary: validate-only; no VHDX, mount, image write, or WSL operation

## Purpose

T56 produces one retained state directory for each durability point, but a
per-point PASS is not an aggregate VHDX qualification decision. This contract
binds the four directories below one controller-generated run ID and rejects an
incomplete, mixed-identity, tampered, substitute, or claim-escalated bundle.

The gate reads regular files below the supplied run directory. It does not run
the PowerShell controller, invoke `wsl.exe`, inspect or open `ext4.vhdx`, mount
KAFS, modify the retained image, or produce a real-media or release claim.

## Run Boundary

The input is exactly:

```text
<YYYYMMDDTHHMMSSZ-8hex>/
  journal_publish/
  checkpoint_copy/
  metadata_apply/
  journal_reclaim/
```

The parent basename is the run ID emitted by the native Windows controller.
The exact four fault directories must be real directories. An unexpected,
missing, duplicate-by-name-substitution, or symlinked entry fails closed.

Each fault directory may retain empty mount directories on the same filesystem,
but every regular top-level artifact other than `artifacts.sha256` must occur
exactly once in the inventory. Non-empty or cross-filesystem directories and
symbolic links are rejected because the T56 producer does not hash them.

## Per-fault Validation

For each fault, the audit verifies:

- `arm-context.json` has the exact schema/fault, ext4 source, exact Git commit,
  clean tracked worktree, dedicated regular-file image, and false claims;
- `pause.marker`, `vhdx-recovery.meta`, and `vhdx-verify.ok` all name that exact
  fault;
- `host-controller.json` records `wsl.exe --terminate`, exit code zero for
  terminate/restart, positive VHDX lengths, ordered timezone-aware timestamps,
  and false claims;
- `verification-manifest.json` follows restart, binds the recovered-image
  SHA-256, records host interruption/full-fsck/kafsdump/payload-diagnostic PASS,
  and keeps all out-of-scope claims false;
- the recovery diagnostic is complete and resumes from the requested durability
  boundary with the expected current harness counts;
- `kafsdump.json` reports a selected valid format-v7 layout and a selected clean
  journal; and
- `artifacts.sha256` exactly covers and matches every retained top-level file.

Across all four points, distro, VHDX path, WSL kernel/filesystem source, and Git
HEAD must not drift. The marker-to-restart intervals must not overlap. VHDX
length may grow between points and therefore is validated per point rather than
required to remain byte-identical.

## Command And Result

Run only after preserving a completed controller run:

```sh
./scripts/v7-vhdx-evidence-audit-gate.sh \
  --run-dir <state-root>/<run-id> \
  --validate-only
```

Success includes this machine-readable marker:

```text
KAFS_V7_VHDX_EVIDENCE_AUDIT PASS run_id=<run-id>
```

This marker proves only that the retained aggregate contract passed at audit
time. It does not prove NAND/FTL behavior, physical power interruption,
real-media qualification, RC readiness, or production cutover readiness.

## Synthetic And Substitute Evidence

The regression builds a complete synthetic four-point directory without
mounting or opening an image as a filesystem. Positive validation covers the
exact producer schemas and digest layout. Negative validation covers missing
and unexpected faults, identity drift, claim escalation, artifact tampering,
unlisted and incomplete artifacts, non-empty retained mount directories,
marker/recovery/dump mismatches, overlapping fault intervals, and an invalid
run ID.

Process-kill substitute evidence is deliberately negative: changing the
controller identity away from `wsl.exe --terminate` or clearing
`host_terminate_restart_observed` fails the audit. The earlier substitute run
remains useful harness evidence, but cannot close the host-interruption
qualification edge.

## Closeout Evidence

T57 closed on 2026-07-22 without PowerShell, VHDX, mount, image-write, device,
or WSL lifecycle operations. The focused audit/inspection pair passed. Full
`KAFS_TEST_MOUNT_TIMEOUT_MS=15000 make check -j2` passed 44 tests;
`min_git_hooks` was the single FUSE-permission skip. Formatting, lint,
clone/static checks, build, and source distribution generation passed. The
strict source clone baseline remained 48 clones, 490 duplicated lines, and
0.97% duplication.
