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

Current v6 images remain write-disabled descriptor destinations. They can be
inspected with `kafsdump`, `fsck.kafs`, and the explicit v6 inspection mount
path, but they are not production write mount targets yet.

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
rather than cutting traffic over to it.

## Step 4: Seed Copy

For v4/v5 destinations, run the first bulk copy while the source stays online:

```sh
rsync -aH --delete /mnt/kafs-src/ /mnt/kafs-dst/
```

This step is expected to copy most of the data volume. Current v6 descriptor
destinations are not writable data-copy targets, so content cutover waits for
v6 write mount support or a separately scoped offline copy path.

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

For current v6 descriptor destinations, validation is offline only:

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

For v6 descriptor destinations, do not perform production cutover until v6 write
mount support is enabled and a write-capable smoke mount has passed. The current
safe endpoint is an offline-validated staged image, optionally inspected through
`-o ro,v6_inspection_mount`.

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
