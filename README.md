# KAFS

KAFS is a FUSE-based filesystem backed by a single image file with a journal and
optional deduplication features. This repository contains the filesystem
implementation, tools, and tests.

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

### fsck.kafs

Validate or clear the in-image journal:

```sh
./fsck.kafs --check-only /tmp/kafs.img
./fsck.kafs --journal-clear /tmp/kafs.img
```

Exit status:
- `0`: journal OK (or cleared successfully)
- `3`: journal validation failed
- `4`: clear operation failed

### kafsctl

Inspect stats and hotplug status:

```sh
./kafsctl stats /tmp/kafs-mnt --json
./kafsctl hotplug status /tmp/kafs-mnt
```

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
