# KAFS Offline Migration And Cutover Playbook

This playbook covers the currently supported offline image-migration surfaces:

- v4/v5 destination creation with `kafsresize --migrate-create`, followed by a
  mounted `rsync` cutover; and
- frozen clean v5 to new v7 import with `kafsresize --migrate-import-v7`.

Format v6 creation and runtime workflows are retired. `kafs-v6` is a temporary
fail-closed placeholder and must not be used as a migration, inspection, or
controlled-write command. See
[the v6 source retirement plan](sd-card-wear-v6-retirement-plan.md) and the
[residual inventory](sd-card-wear-v6-retirement-inventory-20260722.md).

Neither workflow by itself authorizes production cutover. Keep the source image
unchanged until the destination has passed the applicable validation and the
operator has separately accepted the cutover evidence.

## Choose The Workflow

Use `--migrate-create` when the destination remains v4 or v5 and data will be
copied through mounted filesystems. New images default to v5; select v4 only
when legacy v4 output is explicitly required.

Use `--migrate-import-v7` when the source is an unmounted, frozen, clean v5
regular-file image and the destination must be a new v7 image. The importer
copies the supported namespace and payload directly through the v7-owned
offline path. It is not an in-place conversion and does not perform cutover.

## Common Preconditions

- Preserve a known-good source image or immutable snapshot.
- Capture the source path, size, format, digest, and filesystem-check result.
- Reserve enough space for a second image and temporary failure artifacts.
- Use a new destination path unless the selected command explicitly documents
  overwrite behavior.
- Do not infer production qualification from a successful file-image test.

Capture a read-only baseline before migration work:

```sh
sha256sum /var/lib/kafs/source.img
./kafsdump --json /var/lib/kafs/source.img
./fsck.kafs --balanced-check /var/lib/kafs/source.img
```

## v4/v5 Destination Creation And Copy

### 1. Preflight destination geometry

Run a dry-run with the intended destination geometry:

```sh
./kafsresize --migrate-create \
	--dst-image /var/lib/kafs/destination.img \
	--size-bytes 128G \
	--inodes 524288 \
	--hrl-entry-ratio 0.75 \
	--dry-run
```

Add `--format-version 4` only when the destination must remain v4. Otherwise
the current mkfs default creates v5.

### 2. Create the destination

```sh
./kafsresize --migrate-create \
	--dst-image /var/lib/kafs/destination.img \
	--size-bytes 128G \
	--inodes 524288 \
	--hrl-entry-ratio 0.75 \
	--yes --force
```

`--force` permits replacement of an existing regular-file destination in this
mode. Confirm the exact path and retained source before using it.

### 3. Mount and seed

```sh
mkdir -p /mnt/kafs-src /mnt/kafs-dst
./kafs --image /var/lib/kafs/source.img /mnt/kafs-src -f
./kafs --image /var/lib/kafs/destination.img /mnt/kafs-dst -f
```

From another shell, confirm both mounts and run the seed copy:

```sh
./kafsctl fsstat /mnt/kafs-src --json --mib
./kafsctl fsstat /mnt/kafs-dst --json --mib
rsync -aH --delete /mnt/kafs-src/ /mnt/kafs-dst/
```

### 4. Freeze and final sync

Stop writes to the source workload, record the freeze boundary, and run the
low-transfer final sync:

```sh
rsync -aH --delete --inplace --no-whole-file /mnt/kafs-src/ /mnt/kafs-dst/
```

### 5. Validate and decide

```sh
./kafsctl fsstat /mnt/kafs-dst --json --mib
./fsck.kafs --balanced-check /var/lib/kafs/destination.img
```

Verify application data, link semantics, permissions, expected format and
geometry, and the destination digest/evidence set. Cut over only after the
operator has accepted those results. Keep the frozen source until post-cutover
checks pass.

## Frozen v5 To New v7 Import

### 1. Freeze and verify the source

The source must be an unmounted clean v5 regular-file image. Stop the source
workload, unmount it, then repeat the digest, dump, and offline filesystem check.
Do not continue if the source changes after this point.

### 2. Run the exact-capacity dry-run

```sh
./kafsresize --migrate-import-v7 \
	--src-image /var/lib/kafs/source-v5.img \
	--dst-image /var/lib/kafs/destination-v7.img \
	--size-bytes 128G \
	--inodes 524288 \
	--v7-group-count 2 \
	--dry-run
```

The final destination and `<destination>.kafs-import-partial` must not already
exist. Import mode intentionally rejects `--force`.

### 3. Import through the v7-owned offline path

```sh
./kafsresize --migrate-import-v7 \
	--src-image /var/lib/kafs/source-v5.img \
	--dst-image /var/lib/kafs/destination-v7.img \
	--size-bytes 128G \
	--inodes 524288 \
	--v7-group-count 2
```

The importer builds a private partial image and publishes the requested final
path only after full offline validation. On a pre-publication failure, preserve
the partial image and logs for diagnosis. A later attempt is a full replay from
the unchanged frozen source, not an in-place continuation of that partial
image.

### 4. Validate the published destination

```sh
sha256sum /var/lib/kafs/source-v5.img /var/lib/kafs/destination-v7.img
./kafsdump --json /var/lib/kafs/destination-v7.img
./fsck.kafs --balanced-check /var/lib/kafs/destination-v7.img
mkdir -p /mnt/kafs-v7-inspect
./kafs-v7 --image /var/lib/kafs/destination-v7.img \
	--inspection-mount /mnt/kafs-v7-inspect -f -o ro
```

Inspection is read-only. Compare a mounted semantic inventory with the source,
then unmount normally. Import success and inspection do not authorize the v7
controlled-write surface or production cutover.

### 5. Rehearse lifecycle evidence separately

For repository-owned disposable file images only, run:

```sh
./scripts/v5-v7-migration-rehearsal.sh
```

The rehearsal does not accept a caller-supplied image, device, mountpoint, or
production path. It exercises normal import, interrupted full replay, rollback,
and idempotence, and writes evidence under
`report/v5-v7-migration-rehearsal/`. It performs no WSL shutdown, physical-media
operation, or production cutover.

## Failure And Rollback Boundary

- Before publication or cutover, keep using the known-good source and preserve
  the failed destination or partial image for diagnosis.
- Do not retry against a changing source or treat a partial v7 image as a resume
  checkpoint.
- After a v4/v5 cutover failure, stop new writes before returning to the frozen
  source. If source and destination have diverged, handle recovery as an
  incident rather than attempting an automatic bidirectional merge.
- Do not repair-write a failed v7 destination merely to make migration evidence
  pass. Preserve logs, dump/fsck output, exact commands, path stats, and digests.
