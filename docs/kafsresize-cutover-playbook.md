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
- You know whether the destination must stay on legacy v4 or can use the v5 default

## Step 1: Inspect The Source Image

Capture baseline information before provisioning the destination:

```sh
./kafsdump --json /var/lib/kafs/source.img
./fsck.kafs --balanced-check /var/lib/kafs/source.img
```

Recommended checks:

- verify the image passes a normal offline check before migration work
- record current size, inode pressure, and any tail metadata observations

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

## Step 3: Mount Source And Destination

Mount both images and verify basic access:

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

## Step 4: Seed Copy

Run the first bulk copy while the source stays online:

```sh
rsync -aH --delete /mnt/kafs-src/ /mnt/kafs-dst/
```

This step is expected to copy most of the data volume.

## Step 5: Freeze Writes And Final Sync

Schedule a write stop on the source workload, then perform a final low-transfer
sync:

```sh
rsync -aH --delete --inplace --no-whole-file /mnt/kafs-src/ /mnt/kafs-dst/
```

The `--inplace --no-whole-file` combination matches the cutover guidance that
`kafsresize` prints when source and destination mount paths are provided.

## Step 6: Validate The Destination

Before switching production traffic, validate the destination image offline or
via the mounted instance:

```sh
./kafsctl fsstat /mnt/kafs-dst --json --mib
./fsck.kafs --balanced-check /var/lib/kafs/destination.img
```

Recommended validation points:

- key application data is present and readable
- offline fsck passes on the destination image
- expected format version and geometry match the migration plan

## Step 7: Cut Over

Unmount the old and new mounts in a controlled order, update service paths, and
remount the destination image at the production location.

Keep the old source image untouched until the new mount has passed smoke checks.

## Rollback Guidance

If validation fails before cutover, discard the destination image and repeat the
workflow after fixing the cause.

If post-cutover validation fails, stop new writes and return to the original
source image while preserving the failed destination image for inspection.

## Notes

- `--grow` remains limited to preallocated headroom inside a single image
- `--migrate-create` is the safer choice when geometry must change materially
- Existing v4 images can stay in service; only the new destination format choice
  needs an explicit decision