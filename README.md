# KAFS

KAFS is a FUSE-based filesystem backed by a single image file with a journal and
optional deduplication features. This repository contains the filesystem
implementation, tools, and tests.

## Release Highlights (v0.2.2)

- pending 競合対策を強化（inode epoch 楽観ガード + stale pending 抑止）
- pending 上書き時の二重 `dec_ref` を防止し、参照寿命不整合を修正
- `truncate` の競合窓を縮小し、並列 stress での安定性を改善
- stress 条件で 5-run 連続 PASS（`invalid block ref=0`）

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
- `--migrate-v2`: v2 image を起動時に one-shot で v3 へ移行して終了
- `--yes`: マイグレーション確認を省略

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
./kafsctl migrate /tmp/kafs.img
```

`migrate` は v2 イメージを v3 に更新する不可逆操作です。
`--yes` 未指定時は `YES` 入力による確認を要求します。

## Migration (v2 -> v3)

推奨はオフラインでの明示実行です。

```sh
./kafsctl migrate /path/to/image.kafs
./kafsctl migrate /path/to/image.kafs --yes
```

起動時 one-shot で実行する場合:

```sh
./kafs --image /path/to/image.kafs --migrate-v2 /mnt/kafs -f
./kafs --image /path/to/image.kafs --migrate-v2 --yes /mnt/kafs -f
```

詳細は `docs/migration-v2-to-v3.md` を参照してください。

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
