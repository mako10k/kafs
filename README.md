# KAFS

KAFS is a FUSE-based filesystem backed by a single image file with a journal and
optional deduplication features. This repository contains the filesystem
implementation, tools, and tests.

## Release Highlights (v0.2.3)

- Added integrated `fsck.kafs` modes: `full`, `balanced`, and `fast`
- Added low-level fsck options: journal replay and unreferenced-block punch-hole
- Fixed `fsck.kafs` journal link dependency and addressed static-analysis findings
- Reduced code duplication across RPC/CLI/block/back-end paths and tightened quality gates

## Features

- FUSE mount backed by an image file
- Embedded journal with fsck support
- mkfs and fsck tooling
- Hotplug control via a hidden control endpoint
- Test suites and reproducible scripts

## Build

```sh
autoreconf -fi
./configure
make
```

## Quick Start

Create an image, mount it, and inspect stats:

```sh
./mkfs.kafs /tmp/kafs.img -s 1G
mkdir -p /tmp/kafs-mnt
./kafs --image /tmp/kafs.img /tmp/kafs-mnt -f
```

In another shell:

```sh
./kafsctl stats /tmp/kafs-mnt
```

## Tools and Options

### mkfs.kafs

Create a filesystem image:

```sh
./mkfs.kafs image.kafs -s 2G -J 4M -b 12 -i 65536
```

Key options:
- `-s, --size-bytes`: total image size (accepts K/M/G suffixes)
- `-b, --blksize-log`: block size as log2 (default 12 = 4096 bytes)
- `-i, --inodes`: inode count
- `-J, --journal-size-bytes`: journal size (accepts K/M/G suffixes)

### kafs

Mount a filesystem image:

```sh
./kafs --image /tmp/kafs.img /tmp/kafs-mnt -f
```

FUSE options:
- `-o multi_thread[=N]`: enable multi-thread mode with optional thread count

Migration options:
- `--migrate-v2`: migrate a v2 image to v3 in one-shot mode at startup, then exit
- `--yes`: skip migration confirmation

### fsck.kafs

Typical integrated modes:

```sh
./fsck.kafs --full-check /tmp/kafs.img
./fsck.kafs --full-repair /tmp/kafs.img
./fsck.kafs --balanced-check /tmp/kafs.img      # default when no mode is given
./fsck.kafs --balanced-repair /tmp/kafs.img
./fsck.kafs --fast-check /tmp/kafs.img
./fsck.kafs --fast-repair /tmp/kafs.img
```

Low-level layer options:

```sh
./fsck.kafs --check-journal /tmp/kafs.img
./fsck.kafs --repair-journal-reset /tmp/kafs.img
./fsck.kafs --check-dirent-ino-orphans /tmp/kafs.img
./fsck.kafs --repair-dirent-ino-orphans /tmp/kafs.img
./fsck.kafs --check-hrl-blo-refcounts /tmp/kafs.img
./fsck.kafs --replay-journal /tmp/kafs.img
./fsck.kafs --punch-hole-unreferenced-data-blocks /tmp/kafs.img
```

Exit status:
- `0`: validation/repair completed successfully
- `3`: journal validation failed
- `4`: journal reset operation failed
- `5`: dirent->ino check found orphan inconsistencies
- `6`: dirent->ino repair completed with partial failures
- `7`: hrl->blo check found refcount mismatches
- `8`: journal replay failed
- `9`: punch-hole completed with partial failures

### kafsdump

Inspect an offline image without modifying it:

```sh
./kafsdump /tmp/kafs.img
./kafsdump --json /tmp/kafs.img
```

Output sections:
- `superblock`: magic/version/block geometry and free counts
- `inode_summary`: used/free inode counts and `linkcnt==0` in-use count
- `hrl_summary`: HRL entry/live/refcnt totals
- `journal_header`: in-image journal header fields and header CRC check result

### kafsimage

Export metadata-only image payload (e2image-like first step):

```sh
./kafsimage --metadata-only /tmp/kafs.img /tmp/kafs.meta
./kafsimage --metadata-only --verify /tmp/kafs.img /tmp/kafs.meta
./kafsimage --raw /tmp/kafs.img /tmp/kafs.raw
./kafsimage --raw --verify /tmp/kafs.img /tmp/kafs.raw
./kafsimage --sparse /tmp/kafs.img /tmp/kafs.sparse
./kafsimage --sparse --verify /tmp/kafs.img /tmp/kafs.sparse
```

`--metadata-only` copies the metadata prefix (`[0, first_data_block * block_size)`) to
the destination file without mutating the source image.
`--raw` copies the full source image byte-for-byte.
`--sparse` copies the selected range while skipping all-zero chunks as holes.

### kafsctl

Inspect stats, migration, and hotplug controls:

```sh
./kafsctl fsstat /tmp/kafs-mnt --json --mib    # alias: stats
./kafsctl hotplug status /tmp/kafs-mnt
./kafsctl hotplug compat /tmp/kafs-mnt --json
./kafsctl hotplug set-timeout /tmp/kafs-mnt 2000
./kafsctl hotplug set-dedup-priority /tmp/kafs-mnt idle 10
./kafsctl hotplug env list /tmp/kafs-mnt
./kafsctl hotplug env set /tmp/kafs-mnt KAFS_BACK_ENABLE_IMAGE=1
./kafsctl hotplug env unset /tmp/kafs-mnt KAFS_BACK_ENABLE_IMAGE
./kafsctl migrate /tmp/kafs.img
```

`migrate` is an irreversible operation that updates a v2 image to v3.
When `--yes` is not specified, it requires confirmation by entering `YES`.

`fsstat` supports output units via `--bytes`, `--mib`, and `--gib`.

## Migration (v2 -> v3)

Explicit offline execution is recommended.

```sh
./kafsctl migrate /path/to/image.kafs
./kafsctl migrate /path/to/image.kafs --yes
```

To run it as a startup one-shot:

```sh
./kafs --image /path/to/image.kafs --migrate-v2 /mnt/kafs -f
./kafs --image /path/to/image.kafs --migrate-v2 --yes /mnt/kafs -f
```

See `docs/migration-v2-to-v3.md` for details.

## Hotplug Control

Hotplug control is exposed via the hidden `/.kafs.sock` endpoint. To enable the
front/back connection, set a UDS path and start `kafs-back` separately:

```sh
export KAFS_HOTPLUG_UDS=/tmp/kafs-hotplug.sock
./kafs --image /tmp/kafs.img /tmp/kafs-mnt -f
./kafs-back
```

Use `kafsctl` to inspect or control hotplug:

```sh
./kafsctl hotplug status /tmp/kafs-mnt
./kafsctl hotplug compat /tmp/kafs-mnt
./kafsctl hotplug restart-back /tmp/kafs-mnt
./kafsctl hotplug set-timeout /tmp/kafs-mnt 2000
./kafsctl hotplug env list /tmp/kafs-mnt
```

Hotplug flow example:

```sh
./kafsctl hotplug status /tmp/kafs-mnt
./kafsctl hotplug env set /tmp/kafs-mnt KAFS_BACK_ENABLE_IMAGE=1
./kafsctl hotplug restart-back /tmp/kafs-mnt
./kafsctl hotplug status /tmp/kafs-mnt
```

## Environment Variables

### kafs

- `KAFS_IMAGE`: fallback image path when `--image` is omitted
- `KAFS_JOURNAL`: set to `0` to disable journal even if present
- `KAFS_MT`: set to `1` to enable multi-thread mode
- `KAFS_MAX_THREADS`: default thread count when multi-thread mode is enabled
- `KAFS_JOURNAL_GC_NS`: group commit window in nanoseconds

### hotplug

- `KAFS_HOTPLUG_UDS`: UDS path for front/back connection
- `KAFS_HOTPLUG_DATA_MODE`: `inline`, `plan_only`, or `shm`
- `KAFS_HOTPLUG_WAIT_TIMEOUT_MS`: wait timeout in milliseconds
- `KAFS_HOTPLUG_WAIT_QUEUE_LIMIT`: max wait queue length
- `KAFS_HOTPLUG_BACK_FD`: inherited socket FD for `kafs-back`

## Testing

```sh
make check
./scripts/run-all-tests.sh
```

## Documentation

- docs/INDEX.md: documentation index
- docs/hotplug-*.md and docs/hotplug-pipe-*.md: hotplug plans/designs
- docs/dedup-design.md: deduplication design notes
- man/: manual pages for kafs, mkfs.kafs, fsck.kafs, kafsctl
- CHANGELOG.md: release notes
