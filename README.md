# KAFS

KAFS is a FUSE-based filesystem backed by a single image file with a journal and
optional deduplication features. This repository contains the filesystem
implementation, tools, and tests.

## Release Highlights (v0.4.0)

- New images now default to on-disk format v5, including v5 migration destinations created by `kafsresize --migrate-create`
- Runtime mount continues to accept existing v4 images, so operators can keep legacy images in place while moving new provisioning to v5
- Unified the offline migration path so `kafsctl migrate` and `kafs --migrate` use the same shared converter for legacy v2/v3 images
- Expanded regression coverage and refreshed operator guidance for the current v5-default, v4-compatible workflow

## Features

- FUSE mount backed by an image file
- Embedded journal with fsck support
- mkfs and fsck tooling
- Hotplug control via a hidden control endpoint
- Test suites and reproducible scripts

## Current Defaults

- New images created by `mkfs.kafs` default to format v5
- `kafsresize --migrate-create` also defaults to a v5 destination image
- Existing v4 images remain supported for runtime mount
- Legacy v2/v3 images still require an explicit offline migration step before use

## Build

```sh
autoreconf -fi
./configure
make
```

Release-oriented build:

```sh
./configure --enable-lto
make -j"$(nproc)"
```

Debug-oriented build:

```sh
./configure --enable-debug-build
make -j"$(nproc)"
```

`--enable-debug-build` appends `-O0 -g3 -fno-omit-frame-pointer`, forces LTO off, and enables the extra diagnostic logging code paths. For runtime logs, set `KAFS_DEBUG=1..3` when running `kafs` or the mount-based tests. For slow FUSE startup while debugging, `KAFS_TEST_MOUNT_TIMEOUT_MS=15000 make check` extends the shared mount wait window used by the regression tests.

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

Documentation entrypoints:

- Product and operator docs index: [docs/INDEX.md](docs/INDEX.md)
- Tool overview and roadmap: [docs/tools-suite.md](docs/tools-suite.md)

## Tools and Options

### mkfs.kafs

Create a filesystem image:

```sh
./mkfs.kafs image.kafs -s 2G -J 4M -b 12 -i 65536
```

Key options:
- `-s, --size-bytes`: total image size (accepts K/M/G suffixes)
- `-b, --blksize-log`: block size as log2 (default 12 = 4096 bytes)
- `--format-version`: on-disk format version to emit (default 5; use `--format-version 4` for a legacy v4 image)
- `-i, --inodes`: inode count
- `-J, --journal-size-bytes`: journal size (accepts K/M/G suffixes)
- `--hrl-entry-ratio`: HRL entries/data-block ratio (default 0.75, range 0<R<=1)

### kafs

Mount a filesystem image:

```sh
./kafs --image /tmp/kafs.img /tmp/kafs-mnt -f
```

Mount helper compatible forms:

```sh
mount.kafs /tmp/kafs.img /tmp/kafs-mnt -o allow_other,multi_thread=8
mount -t fuse.kafs /tmp/kafs.img /tmp/kafs-mnt -o allow_other,multi_thread=8
```

FUSE options:
- `-o multi_thread[=N]`: enable multi-thread mode with optional thread count
- `-o bg_dedup_scan=on|off` (alias: `-o dedup_scan=on|off`): idle background dedup scan switch (default: `on`)
- `-o bg_dedup_interval_ms=N` (alias: `-o dedup_interval_ms=N`): idle background dedup scan interval in ms
- `--option <opt[,opt...]>` / `--option=<opt[,opt...]>`: long-option alias of `-o`

Example:

```sh
./kafs --image /tmp/kafs.img /tmp/kafs-mnt -f --option dedup_scan=off
./kafs --image /tmp/kafs.img /tmp/kafs-mnt -f -o dedup_scan=on,dedup_interval_ms=20
```

fstab example:

```fstab
/var/lib/kafs/data.img  /mnt/kafs  fuse.kafs  rw,nofail,allow_other,multi_thread=8  0  0
```

The filesystem type stays `fuse.kafs` because KAFS is a FUSE userspace filesystem, not a native kernel-registered filesystem.

Migration options:
- `--migrate`: run offline pre-start migration for a v2/v3 image to v4, then exit
- `--migrate-v2`: deprecated alias of `--migrate`
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
- `superblock`: magic/version/block geometry, free counts, and tail metadata region flags/offset/size
- `tail_metadata`: probe status and, when available, region header/container/slot summary
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

### kafsresize

Offline resize and migration-image creation:

```sh
./kafsresize --grow --size-bytes 2G /tmp/kafs.img

# Create migration destination image with custom HRL ratio:
./kafsresize --migrate-create --dst-image /tmp/kafs-new.img --inodes 524288 \
	--size-bytes 128G --hrl-entry-ratio 0.75 --yes --force
```

Current v0 constraint: growth is only supported within preallocated headroom
(`s_blkcnt < s_r_blkcnt`). Shrink is not supported.
`--grow` accepts both v4 and v5 images when the requested size stays within preallocated headroom.
`--migrate-create` now defaults to a v5 destination image; pass `--format-version 4` when you need a legacy v4 destination.

Operator guidance:
- Existing v4 images can remain in place; runtime mount continues to accept v4 images.
- Newly created images default to v5 so tail metadata scaffolding is provisioned from mkfs time.
- If you are staging an offline migration and want the destination to stay on v4, create it explicitly with `--format-version 4`.

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

`migrate` is an irreversible offline pre-start operation that updates a v2/v3 image to v4.
When `--yes` is not specified, it requires a Yes/No confirmation prompt.

`fsstat` supports output units via `--bytes`, `--mib`, and `--gib`.

Background dedup observability (live dashboard):

```sh
scripts/watch-bg-dedup.sh /tmp/kafs-mnt
scripts/watch-bg-dedup.sh /tmp/kafs-mnt 0.5
```

This monitor prints cumulative and delta values for:
- `bg_dedup_steps`
- `bg_dedup_scanned_blocks`
- `bg_dedup_replacements`
- `bg_dedup_direct_candidates` / `bg_dedup_direct_hits`
- `bg_dedup_index_evicts`
- `bg_dedup_cooldowns`

## Migration (v2/v3 -> v4)

Explicit offline execution is recommended.

```sh
./kafsctl migrate /path/to/image.kafs
./kafsctl migrate /path/to/image.kafs --yes
```

To run it as a startup one-shot:

```sh
./kafs --image /path/to/image.kafs --migrate /mnt/kafs -f
./kafs --image /path/to/image.kafs --migrate --yes /mnt/kafs -f
```

See `docs/migration-v2-to-v3.md` for details.

## Hotplug Control

Hotplug control is exposed via the hidden `/.kafs.sock` endpoint. To enable the
front/back connection, set a UDS path when mounting:

```sh
export KAFS_HOTPLUG_UDS=/tmp/kafs-hotplug.sock
./kafs --image /tmp/kafs.img /tmp/kafs-mnt -f
```

You can also enable hotplug explicitly at mount time (including `mount -o ...` style):

```sh
./kafs --image /tmp/kafs.img --hotplug /tmp/kafs-mnt -f
./kafs --image /tmp/kafs.img --hotplug=/run/kafs/hotplug.sock /tmp/kafs-mnt -f
./kafs --image /tmp/kafs.img /tmp/kafs-mnt -f -o hotplug
./kafs --image /tmp/kafs.img /tmp/kafs-mnt -f -o hotplug_uds=/run/kafs/hotplug.sock
./kafs --image /tmp/kafs.img /tmp/kafs-mnt -f -o hotplug_back_bin=/usr/local/bin/kafs-back
```

Use `kafsctl` to inspect or control hotplug:

```sh
./kafsctl hotplug status /tmp/kafs-mnt
./kafsctl hotplug compat /tmp/kafs-mnt
./kafsctl hotplug restart-back /tmp/kafs-mnt
./kafsctl hotplug set-timeout /tmp/kafs-mnt 2000
./kafsctl hotplug env list /tmp/kafs-mnt
```

`hotplug restart-back` now asks the mounted `kafs` front to restart the back process.
The executable defaults to `kafs-back`; override it with `--hotplug-back-bin`,
`-o hotplug_back_bin=...`, or `kafsctl hotplug env set <mountpoint> KAFS_HOTPLUG_BACK_BIN=...`.

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
- `KAFS_BG_DEDUP_SCAN`: set idle background dedup scan on/off (default `on`)
- `KAFS_BG_DEDUP_INTERVAL_MS`: default idle background dedup scan interval in ms
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
- docs/kafsimage-format.md: kafsimage mode semantics and output rules
- man/: manual pages for kafs, kafsctl, mkfs.kafs, fsck.kafs, kafs-info, kafsdump, kafsimage, and kafsresize
- CHANGELOG.md: release notes
