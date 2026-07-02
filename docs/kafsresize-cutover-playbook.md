# KAFS Resize Cutover Playbook

This playbook describes the recommended offline migration-image workflow built
around `kafsresize --migrate-create`.

## When To Use This Workflow

Use this workflow when you need to move to a new image with different geometry,
such as a larger size, a higher inode count, or a new default destination
format. This is the preferred path when existing metadata layout cannot be grown
in place safely.

## Preconditions

- The source image is healthy enough to mount and read from
- You have capacity for a second destination image during cutover
- You can schedule a short write freeze for the final sync and switch
- You know whether the destination must stay on legacy v4, can use the v5 default,
  or is being staged as a v6 descriptor destination

## Step 1: Inspect The Source Image

Capture baseline information before provisioning the destination:

```sh
./kafsdump --json /var/lib/kafs/source.img
./fsck.kafs --balanced-check /var/lib/kafs/source.img
```

Recommended checks:

- verify the image passes a normal offline check before migration work
- record current size, inode pressure, and any tail metadata observations

For v5-to-v6 planning, run the create precheck before touching the destination:

```sh
./kafsresize --migrate-create \
	--src-image /var/lib/kafs/source.img \
	--dst-image /var/lib/kafs/destination.img \
	--size-bytes 128G \
	--inodes 524288 \
	--format-version 6 \
	--dry-run
```

This validates that the source is a clean v5 image, computes the v6 destination
geometry, and reports descriptor replica offsets without formatting the
destination.

## Step 2: Create The Destination Image

Provision a destination image with the desired geometry:

```sh
./kafsresize --migrate-create \
	--dst-image /var/lib/kafs/destination.img \
	--size-bytes 128G \
	--inodes 524288 \
	--hrl-entry-ratio 0.75 \
	--yes --force
```

If the destination must stay on legacy v4, add `--format-version 4` explicitly.
Otherwise the destination follows the current mkfs default and will be created
as v5.

For a v6 destination, keep the source image in the create command so the same
clean-v5 and capacity precheck runs before the destination is overwritten:

```sh
./kafsresize --migrate-create \
	--src-image /var/lib/kafs/source.img \
	--dst-image /var/lib/kafs/destination.img \
	--size-bytes 128G \
	--inodes 524288 \
	--hrl-entry-ratio 0.75 \
	--format-version 6 \
	--yes --force
```

Current v6 images remain non-production descriptor destinations. They can be
inspected with `kafsdump`, `fsck.kafs`, and the explicit v6 inspection mount
path. They can also be exercised through the experimental controlled write
smoke described below, but they are not production write mount targets yet.

## Step 3: Mount Supported Source And Destination

For v4/v5 destinations, mount both images and verify basic access:

```sh
mkdir -p /mnt/kafs-src /mnt/kafs-dst
./kafs --image /var/lib/kafs/source.img /mnt/kafs-src -f
./kafs --image /var/lib/kafs/destination.img /mnt/kafs-dst -f
```

In another shell, confirm both mounts answer stats requests:

```sh
./kafsctl fsstat /mnt/kafs-src --json --mib
./kafsctl fsstat /mnt/kafs-dst --json --mib
```

Do not mount a v6 descriptor destination as a production write target in this
phase. For optional inspection, use the explicit inspection mount:

```sh
./kafs --image /var/lib/kafs/destination.img /mnt/kafs-v6-inspect -f -o ro,v6_inspection_mount
```

This path is for inspection only. It keeps the image read-only, rejects
mutations with `EROFS`, and does not enable v6 write admission. For v6, skip the
copy steps below, validate the image offline in Step 6, and keep it staged
rather than cutting traffic over to it. Use the controlled write helper only for
explicit acceptance smoke, not for production data movement.

## Step 4: Seed Copy

For v4/v5 destinations, run the first bulk copy while the source stays online:

```sh
rsync -aH --delete /mnt/kafs-src/ /mnt/kafs-dst/
```

This step is expected to copy most of the data volume. Current v6 descriptor
destinations are not general writable data-copy targets. The experimental
controlled write path is limited to regular-file smoke evidence, so content
cutover still waits for a separately scoped production copy path.

## Step 5: Freeze Writes And Final Sync

For v4/v5 destinations, schedule a write stop on the source workload, then
perform a final low-transfer sync:

```sh
rsync -aH --delete --inplace --no-whole-file /mnt/kafs-src/ /mnt/kafs-dst/
```

The `--inplace --no-whole-file` combination matches the cutover guidance that
`kafsresize` prints when source and destination mount paths are provided.

## Step 6: Validate The Destination

Before switching production traffic for a mountable v4/v5 destination, validate
the destination image offline or via the mounted instance:

```sh
./kafsctl fsstat /mnt/kafs-dst --json --mib
./fsck.kafs --balanced-check /var/lib/kafs/destination.img
```

For current v6 descriptor destinations, baseline validation remains offline:

```sh
./kafsdump --json /var/lib/kafs/destination.img
./fsck.kafs --balanced-check /var/lib/kafs/destination.img
```

Recommended validation points:

- key application data is present and readable
- offline fsck passes on the destination image
- expected format version and geometry match the migration plan
- for v6 destinations, `kafsdump --json` reports a healthy descriptor scaffold,
  and `fsck.kafs --balanced-check` reports the v6 descriptor, shard coverage, HRL
  chain, and journal segment checks as `status=ok`

## Step 7: Cut Over

For v4/v5 destinations, unmount the old and new mounts in a controlled order,
update service paths, and remount the destination image at the production
location.

Keep the old source image untouched until the new mount has passed smoke checks.

For v6 descriptor destinations, do not perform production cutover based on the
experimental controlled write smoke alone. Production cutover still needs a
separately scoped acceptance gate for real workload copy, rollback, and
operations. The current safe endpoint is an offline-validated staged image,
optionally inspected through `-o ro,v6_inspection_mount` and exercised by the
controlled smoke helper.

## Controlled v6 Write Opt-In Boundary

This boundary is an experimental controlled write path. It is disabled by
default and must not be treated as a production default cutover path.

The mount must use the dedicated v6 runtime entrypoint and explicit controlled
write mode:

```sh
./kafs-v6 --image /var/lib/kafs/destination.img \
	--controlled-write-mount /mnt/kafs-v6 -f \
	-o rw,no_writeback_cache,no_trim_on_free,bg_dedup_scan=off,fsync_policy=full
```

Use the repeatable operator helper for smoke evidence:

```sh
scripts/v6-controlled-write-smoke.sh \
	--image /var/lib/kafs/destination.img \
	--yes \
	--report-root /var/tmp/kafs-v6-controlled-write-smoke
```

The helper uses `kafs-v6 --controlled-write-mount`, runs an explicit
regular-file create/write/fsync workload, unmounts, and stores
timestamped before/after artifacts under the report root.

Use the pre-production acceptance gate when you need a mechanical check that the
smoke artifacts are complete:

```sh
scripts/v6-controlled-write-acceptance-gate.sh \
	--image /var/lib/kafs/destination.img \
	--yes \
	--report-root /var/tmp/kafs-v6-controlled-write-acceptance
```

The gate verifies the controlled write mount log, workload success, before/after
`kafsdump --json`, before/after `fsck.kafs --balanced-check`, image stat/digest,
and the absence of copy/reflink workload evidence. Passing this gate is not
production cutover approval.

Admission fails closed when `kafs-v6 --controlled-write-mount` is not explicit,
when required mount options are missing, when `ro` or legacy `v6_*` mode tokens
are present, or when unsupported writeback cache, runtime TRIM,
delayed/background mutation, hotplug delegated write, or v6 repair-write paths
are requested. The required mount options are:
`rw,no_writeback_cache,no_trim_on_free,bg_dedup_scan=off,fsync_policy=full`.

Use explicit regular-file create/write/fsync smoke commands. Do not use `cp` or
copy/reflink operations as the acceptance signal. Some kernels satisfy
`copy_file_range(2)` through generic read/write fallback before the FUSE copy
hook is reached; that fallback is ordinary regular-file write behavior, while
`KAFS_IOCTL_COPY`, `FICLONE`, and the FUSE copy hook remain unsupported.

For manual reproduction only, capture an offline baseline before the first
write-capable smoke session:

```sh
./kafsdump --json /var/lib/kafs/destination.img > /var/tmp/kafs-v6-before.json
./fsck.kafs --balanced-check /var/lib/kafs/destination.img
```

After a manual smoke session, unmount and validate offline again:

```sh
sync
fusermount3 -u /mnt/kafs-v6
./kafsdump --json /var/lib/kafs/destination.img > /var/tmp/kafs-v6-after.json
./fsck.kafs --balanced-check /var/lib/kafs/destination.img
```

If the post-write check fails, do not repair-write the v6 image in place. Preserve
the destination image, logs, and dump/fsck output for diagnosis, then return to
the known-good source image when its write-freeze boundary is still valid.

Minimum artifacts to preserve on failure:

- failed destination image, or an immutable copy of it
- mount log from the controlled write session
- exact smoke workload commands or script
- `kafsdump --json` before/after output
- `fsck.kafs --balanced-check` stdout/stderr and exit status
- image size/stat and digest if they were captured before cutover

## Rollback Guidance

If validation fails before cutover, discard the destination image and repeat the
workflow after fixing the cause.

If post-cutover validation fails, stop new writes and return to the original
source image while preserving the failed destination image for inspection.

For v6 staging, rollback before runtime support is enabled means discarding the
staged destination and keeping the unchanged v5 source in service; no production
traffic should have been moved to the v6 image.

## Notes

- `--grow` remains limited to preallocated headroom inside a single image
- `--migrate-create` is the safer choice when geometry must change materially
- Existing v4 images can stay in service; only the new destination format choice
  needs an explicit decision
