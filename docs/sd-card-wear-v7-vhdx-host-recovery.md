# KAFS v7 Windows-host VHDX recovery prequalification

- Status: harness implemented; Windows-host terminate/restart execution deferred
  until the user identifies a safe maintenance window
- Evidence-audit status: T57 read-only aggregate gate registered and waiting for
  the primary implementation stream; no WSL stop is required for that work
- Scope: format-v7 controlled-write recovery on a dedicated regular-file image
- Controller: `scripts/v7-vhdx-host-recovery.ps1`
- WSL runner: `scripts/v7-vhdx-host-recovery.sh`

## Purpose and claim boundary

This workflow closes a host-level interruption evidence gap while disposable SD
card hardware is unavailable. It places a dedicated 128 MiB KAFS image on the
active Ubuntu ext4 filesystem, pauses `kafs-v7` at an existing deterministic
durability boundary, terminates the entire Ubuntu WSL distribution from native
Windows, restarts it, and verifies recovery.

It does **not** treat the active Ubuntu VHDX as disposable media. Neither script
opens, mounts, formats, or raw-writes the VHDX. The only KAFS-formatted object is
`vhdx-recovery.img`, a newly created regular file below the runner's state root.

The resulting evidence may establish Windows-host WSL terminate/restart recovery
for this VHDX-backed file image. It cannot establish any of these claims:

- real-media or SD-card qualification;
- isolated physical power-cut behavior;
- flash-controller-independent behavior or wear limits;
- release-candidate qualification.

The physical-media identity and destructive-approval gate remains mandatory.

## Observed local backing

The 2026-07-21 preflight observed Ubuntu on WSL2 with the repository filesystem
at `/dev/sdd`, `ext4`. Windows registry discovery identified the distro backing
as:

```text
C:\Users\katsumata-m\AppData\Local\wsl\{fdd6db75-5c65-4ca8-a29c-7148a9dd06cb}\ext4.vhdx
```

Its observed allocated file length was `120680611840` bytes. These are discovery
facts, not stable configuration. The PowerShell controller must rediscover and
record the current registry identity and length on every run. The user-facing
name `WSLHOME.vhdx` is therefore treated as intent; the registered local file is
currently named `ext4.vhdx`.

## Interruption protocol

The four required points are:

1. `journal_publish`;
2. `checkpoint_copy`;
3. `metadata_apply`;
4. `journal_reclaim`.

For each point the protocol is:

```text
Windows preflight
  -> WSL runner creates fresh state and dedicated KAFS image
  -> kafs-v7 reaches the requested durability boundary
  -> marker is created, fsynced, and the kafs-v7 process SIGSTOPs
  -> Windows verifies the marker value
  -> Windows runs: wsl.exe --terminate Ubuntu
  -> Windows restarts Ubuntu
  -> WSL runner reopens the existing image and performs recovery
  -> payload/recovery diagnostic, full fsck, and kafsdump evidence are sealed
```

The normal process-fault path is unchanged: without both pause environment
variables, the existing deterministic `_exit(86 + point)` behavior remains in
effect. A pause marker is created with `O_EXCL`, so stale state fails closed.

## Safe preflight

Run this from **native Windows PowerShell**, not from a shell inside the Ubuntu
distribution that will be terminated. The following command performs discovery
and Linux-side safety checks but never terminates WSL:

```powershell
& '\\wsl.localhost\Ubuntu\home\katsumata-m\kafs\scripts\v7-vhdx-host-recovery.ps1' `
  -Distro Ubuntu
```

Expected terminal marker:

```text
KAFS_V7_VHDX_HOST_PREFLIGHT PASS (no distro termination requested)
```

Preflight rejects a state root outside the current WSL home, on `/mnt/*`, on a
non-ext4 root, or on a filesystem different from the repository's active Ubuntu
filesystem. It also requires the already-built workload, `fsck.kafs`, and
`kafsdump` executables.

## Execute from native Windows

Do not start this section while the active Ubuntu distro is running other tasks.
The user must first identify a safe maintenance window and explicitly resume the
qualification; then refresh the Task Start Gate and host preflight.

After reviewing the discovered distro, exact VHDX path, length, state root, and
false claim fields, run:

```powershell
& '\\wsl.localhost\Ubuntu\home\katsumata-m\kafs\scripts\v7-vhdx-host-recovery.ps1' `
  -Distro Ubuntu -Execute -Confirm
```

`-Execute` is mandatory and PowerShell `ShouldProcess` adds a high-impact
confirmation gate. The default run covers all four interruption points. Use
`-Fault journal_publish` only for a bounded diagnostic rerun; it is not the full
qualification matrix.

Do not run the execute command from Codex or another process hosted inside the
target Ubuntu distribution: `wsl.exe --terminate Ubuntu` intentionally kills
that environment. The native Windows controller must remain outside it to
restart and verify the state.

## Durable state and resumption

The default state root is:

```text
$HOME/.local/state/kafs-v7-vhdx-recovery/<run-id>/<fault>/
```

Arm and verify are separate Linux modes. Terminating WSL kills the arm process,
but the state directory, KAFS image, fsynced arm context, and pause marker remain
on ext4. Verification resumes from those files after restart; it never recreates
the image.

If automated verification stops after `host-controller.json` has been copied,
preserve the directory and diagnose the failing stage. Do not delete or reuse it
for another arm attempt. A new attempt must use a new run ID because stale
markers, metadata, images, and completed verification markers fail closed.

## Evidence contract

Each successful fault directory contains at least:

- `arm-context.json`: WSL kernel, distro, filesystem, Git identity, and claim
  boundary at arm time;
- `pause.marker`: exact requested durability point;
- `host-controller.json`: registry-resolved VHDX identity and length, marker
  observation time, terminate/restart times and exit codes, and false claims;
- `vhdx-recovery.meta` and `vhdx-recovery.img`: resumable test state;
- crash and recovery logs plus `vhdx-verify.ok`;
- `fsck-full-check.stdout`, `fsck-full-check.stderr`, `kafsdump.json`, and
  `kafsdump.stderr`;
- `verification-manifest.json`: PASS facts and explicit claim boundary;
- `artifacts.sha256`: digest of every evidence artifact, including the recovered
  image and manifest.

Success requires both per-point markers and the final controller marker:

```text
KAFS_V7_VHDX_VERIFY <fault> PASS
KAFS_V7_VHDX_HOST_RECOVERY PASS run_id=<run-id>
```

Absent either marker, the result is incomplete. A preflight-only result is never
host-recovery evidence.

The per-fault files are the capture contract, not by themselves an aggregate
qualification decision. T57 must provide a validate-only gate that binds one
run ID to the exact four unique faults, common host/distro/VHDX identity,
successful controller exits, false claims, per-fault recovery/fsck/dump/payload
results, and every artifact digest. Missing or duplicate faults, identity drift,
claim escalation, incomplete state, or digest mismatch must fail closed. The
gate must not mount/write the image or terminate/restart WSL.

T58 closed on 2026-07-22, and the rebuilt PERT plan now selects T57 as the only
zero-slack `RUNNABLE NOW` task. This selects only the read-only aggregate gate;
the actual host capture remains blocked until the user supplies a safe window.

## Remaining physical-media gate

This prequalification is an additional predecessor, not a substitute for the
real-media join. After all four host runs pass, `plans/current.pert` may mark
the capture milestone reached; VHDX qualification closes only after the T57
audit also passes. `HARDWARE_APPROVAL` remains blocked until an exact disposable
card, reader/controller, isolated power apparatus, cycle count, matrix digest,
and time-bounded destructive approval are present. The separate T58 evidence
contract is ready, but does not remove those hardware or VHDX predecessors.
