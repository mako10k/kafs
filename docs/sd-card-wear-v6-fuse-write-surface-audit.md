# KAFS format v6 FUSE write surface admission audit

最終更新: 2026-06-26

対象: `SDW-V6RT-T11 v6 FUSE write surface admission audit`

## 目的

Controlled write mount の成功 path を入れる前に、user-visible FUSE write operations の初期許可範囲を
function 単位で固定する。この文書自体は admission audit であり、T12 以降の実装結果は末尾に追記する。

## 観測した入口

`src/kafs.c` の `struct fuse_operations` は、user-visible write surface として少なくとも次を公開する。

- `kafs_op_create`
- `kafs_op_write`
- `kafs_op_truncate`
- `kafs_op_fallocate`
- `kafs_op_unlink`
- `kafs_op_rename`
- `kafs_op_link`
- `kafs_op_symlink`
- `kafs_op_ioctl` 経由の `FICLONE` / `KAFS_IOCTL_COPY`
- `kafs_op_copy_file_range`
- `kafs_op_fsync`
- `kafs_op_release`

T11 監査時点の入口 guard は `kafs_runtime_write_guard()` / `kafs_mutation_path_context()` で
`c_runtime_read_only` の場合だけ `-EROFS` を返していた。T10 時点では `rw,v6_write_mount` の推奨形も
`controlled write mount is not enabled yet` で拒否されるため、以下の判定は次の成功 path 実装向けの
allowlist である。

## 初期 allowlist 方針

初期 controlled write opt-in は、通常ファイルの foreground data write と durability closeout に限定する。
Directory topology change、link topology change、copy/reflink、hole punch、tail metadata packing / reclaim、
pending log、background worker、runtime TRIM、writeback cache は含めない。

成功 path を実装する場合、v6 runtime context には少なくとも次の追加状態が必要である。

- `format v6 controlled write mount` を示す runtime flag。
- v6 では `kafs_op_fsync()` が tail metadata normalization と pendinglog drain を行わない guard。
- v6 では `kafs_op_release()` が tail metadata normalization / tombstone reclaim を行わない guard。
- v6 では `kafs_pwrite()` / `kafs_truncate()` が tail-only / mixed-tail layout へ遷移しない guard。
- `kafs_op_open()` の `O_TRUNC` は `kafs_op_truncate()` と同じ policy に通す guard。
- hotplug delegated write path は初期 allowlist 外として拒否する guard。

## Operation matrix

| Operation | Entry / helper | 主な mutation | 依存 | 初期判定 |
| --- | --- | --- | --- | --- |
| create | `kafs_op_create()` -> `kafs_create()` | inode allocation、inode table、parent dirent、journal begin/commit | descriptor-backed inode / allocator / directory write、journal、inode_alloc + inode lock policy | 許可候補。通常ファイル作成だけ。`mknod` / `mkdir` は別扱いで初期対象外。 |
| write | `kafs_op_write()` -> `kafs_pwrite()` | inode size、inline data または block refs、HRL / bitmap、inode table | descriptor-backed inode / bitmap / allocator / HRL、T8 lock policy、T6 delayed disabled policy | 許可候補。通常ファイルのみ。tail-only / mixed-tail 生成、pendinglog、hotplug delegated write は拒否する。 |
| truncate | `kafs_op_truncate()` -> `kafs_truncate()` | inode size、block ref release、tail layout transition | descriptor-backed inode / bitmap / allocator / HRL、tail metadata disabled policy | 初期は拒否。`O_TRUNC` も同じ扱い。後続で block-backed regular file の block-aligned grow/shrink だけ再評価する。 |
| fallocate | `kafs_op_fallocate()` | grow、zero edge、hole punch、block release | descriptor-backed block release、tail metadata disabled policy | 拒否。hole punch / KEEP_SIZE の write ordering と release path を別チケットで証明する。 |
| unlink | `kafs_op_unlink()` | parent dirent tombstone、link count decrement、optional reclaim | descriptor-backed directory/inode、tombstone GC disabled policy | 拒否。tombstone / reclaim boundary が初期 allowlist 外。 |
| rename | `kafs_op_rename()` | source/destination dirent remove/add、destination replacement、dotdot update | multi-inode lock policy、directory mutation、tombstone policy | 拒否。multi-directory / replacement / directory rename を初期 allowlist 外にする。 |
| link | `kafs_op_link()` | source link count increment、destination parent dirent add | inode + directory mutation、journal | 拒否。topology mutation は create 以外を初期 allowlist 外にする。 |
| symlink | `kafs_op_symlink()` -> `kafs_create()` + `kafs_pwrite()` | symlink inode create、target payload write | create + write の複合、tail/inline payload policy | 拒否。regular file create/write に限定する。 |
| copy | `kafs_op_copy_file_range()` / `KAFS_IOCTL_COPY` | destination truncate/write、block sharing fallback | dual-inode lock、HRL ref sharing/release、truncate policy | 拒否。regular buffered copy と ioctl copy は初期 allowlist 外。 |
| reflink | `FICLONE` / copy flags -> `kafs_reflink_clone()` | destination truncate、HRL ref increment、block ref install | dual-inode lock、HRL ref sharing/release、truncate policy | 拒否。copy-on-write semantics がないため初期 allowlist 外。 |
| fsync | `kafs_op_fsync()` | journal flush、`fsync`/`fdatasync`; 現状は tail normalize / pending drain も行う | journal segment validation、T6 disabled policy | 許可候補。ただし v6 guard で tail normalize と pending drain を skip してから許可する。 |
| release | `kafs_op_release()` | open count decrement、flush; 現状は last close で tail normalize / reclaim も行う | open count、T6 disabled tail/reclaim policy | 許可候補。ただし v6 guard で tail normalize / tombstone reclaim を skip してから許可する。 |

## 初期 smoke workload

成功 path を実装する場合、最小 smoke は通常ファイルの作成、block-backed write、overwrite、fsync、release、
unmount、detect-only fsck に限定する。`cp`、`mv`、`ln`、`truncate`、`fallocate` は使わない。

事前 check:

```sh
./kafsdump --json /var/lib/kafs/destination.img > /var/tmp/kafs-v6-before.json
./fsck.kafs --balanced-check /var/lib/kafs/destination.img
```

Mount:

```sh
./kafs --image /var/lib/kafs/destination.img /mnt/kafs-v6 -f \
  -o rw,v6_write_mount,no_writeback_cache,no_trim_on_free,bg_dedup_scan=off
```

Smoke workload:

```sh
python3 - <<'PY'
import os

path = "/mnt/kafs-v6/t11-regular.bin"
with open(path, "xb", buffering=0) as f:
    f.write(b"KAFS-V6-T11\n" + (b"A" * (4096 - len(b"KAFS-V6-T11\n"))))
    f.write(b"B" * 4096)
    os.fsync(f.fileno())

with open(path, "r+b", buffering=0) as f:
    f.seek(4096)
    f.write(b"C" * 4096)
    os.fsync(f.fileno())
PY
sync
fusermount3 -u /mnt/kafs-v6
./kafsdump --json /var/lib/kafs/destination.img > /var/tmp/kafs-v6-after.json
./fsck.kafs --balanced-check /var/lib/kafs/destination.img
```

## Rollback / closeout

- `fsck.kafs --balanced-check` が失敗した場合、同じ v6 image で repair write や再 mount を行わない。
- 失敗 image、`kafsdump --json` の before/after、mount log を保存する。
- Production traffic を移す前の smoke 失敗なら、v6 destination を破棄して clean v5 source を継続利用する。
- Production cutover 後の失敗は、旧 source の write-freeze boundary がまだ有効な場合だけ旧 source に戻す。
  分岐した write を自動 merge しない。

## T12 実装結果

`SDW-V6RT-T12 v6 controlled write admission skeleton and operation guard` で、T10 の reserved parser gate を
explicit opt-in 成功 path に接続し、上記 allowlist 外 operation を runtime guard に接続した。通常 v6 mount は
引き続き暗黙 write admission にしない。copy_file_range syscall は kernel が通常 read/write fallback で満たす
環境があるため、明示 copy/reflink ioctl と FUSE hook を guard 対象とする。

## T13 hardening result

`SDW-V6RT-T13 v6 controlled write durability and fallback hardening` で、controlled write smoke は
zero-filled block、partial block overwrite、ENOSPC、`fsync` / `fdatasync`、unmount 後
`fsck.kafs --balanced-check` までを確認するようになった。

copy/reflink の operator wording は次の通り固定する。

- `KAFS_IOCTL_COPY`、`FICLONE`、FUSE copy hook は初期 controlled write surface では unsupported。
- kernel が `copy_file_range(2)` を FUSE copy hook に渡さず generic read/write fallback で処理する場合、
  それは通常 regular-file read/write と同じ扱いであり、copy/reflink support の証明にはしない。
- controlled write acceptance smoke は `cp` / `copy_file_range` ではなく、明示的な create/write/fsync と
  post-write `fsck.kafs --balanced-check` を使う。

## T14-T16 operator boundary result

`SDW-V6RT-T14` で `scripts/v6-controlled-write-smoke.sh` を追加し、controlled write
acceptance evidence は repeatable helper で取得できるようになった。`SDW-V6RT-T15` で
allowlist 外 operation の rejection matrix を追加し、`SDW-V6RT-T16` で `kafs --help` と
`man/kafs.1` の operator-facing wording を現在の experimental opt-in に同期した。

この結果、現在の boundary は次の通りである。

- experimental controlled write opt-in は実装済み。
- production v6 cutover は未解禁。
- 初期許可面は regular-file create/write/fsync/release に限定する。
- acceptance smoke は `scripts/v6-controlled-write-smoke.sh --image <image> --yes` を使う。
