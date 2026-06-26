# KAFS format v6 explicit write opt-in cutover boundary

最終更新: 2026-06-26

対象: `SDW-V6RT-T9 v6 explicit write opt-in cutover boundary`

## 目的

v6 write mount を user-visible opt-in として実装する前に、operator が見る名前、admission の
fail-closed 条件、unsupported option、write 前後の fsck、rollback、release note の境界を固定する。

この文書は cutover boundary であり、現時点では v6 write mount を有効化しない。

## User-visible opt-in

次の名前を予約する。

- CLI option: `--v6-write-mount`
- FUSE mount option: `-o v6_write_mount`
- Alias: `-o v6-write-mount`
- Operator-facing label: `format v6 controlled write mount`

実装時の admission は、通常の v6 mount を暗黙 write admission にしない。`-o rw` と
`v6_write_mount` の両方が明示されている場合だけ、controlled write admission の判定に進む。

## Fail-closed admission 条件

`--v6-write-mount` / `-o v6_write_mount` を実装する場合、次を全て満たさない限り exit 2 で拒否する。

- image は format v6 であり、selected descriptor、replica status、generation、CRC が valid。
- bitmap、inode、allocator summary、HRL、journal segment の coverage / bounds / overlap validation が PASS。
- journal は clean state として admit できる。dirty journal、torn latest segment、valid segment 0、
  descriptor divergence は拒否する。
- `fsck.kafs --balanced-check <image>` が直前の offline check として PASS している運用手順である。
- mount option は `rw,v6_write_mount` を明示している。
- `ro`、`v6_inspection_mount`、`writeback_cache`、`trim_on_free`、`bg_dedup_scan=on` は同時指定を拒否する。
- delayed/background mutation は T6 policy の disabled を維持する。
- pending log、tail metadata packing / normalization、tombstone GC、background dedup scan は起動しない。
- v6 repair/write fsck は T7 policy の detect-only を維持する。

推奨 mount option は、次の形に固定する。

```sh
./kafs --image /var/lib/kafs/destination.img /mnt/kafs-v6 -f \
  -o rw,v6_write_mount,no_writeback_cache,no_trim_on_free,bg_dedup_scan=off
```

このコマンドは T9 時点ではまだ使用不可である。実際に使えるようにするには、別チケットで mount admission
wiring と regression を追加する。

## Unsupported option boundary

Controlled write opt-in の初期段階では、次を unsupported とする。

| 項目 | 初期 policy |
| --- | --- |
| default v6 rw mount | 拒否。`rw,v6_write_mount` の明示が必要。 |
| v6 inspection + write opt-in | 拒否。inspection は read-only 専用。 |
| writeback cache | 拒否。backing image digest / write ordering の追加証明まで対象外。 |
| runtime TRIM | 拒否。初期 cutover は metadata write path の検証範囲を狭くする。 |
| pending log / pending worker | disabled。descriptor-backed pending-log view まで対象外。 |
| tail metadata packing / reclaim | disabled。v6 tail-metadata shard routing まで対象外。 |
| tombstone GC | disabled。遅延 inode/block mutation を初期 cutover に含めない。 |
| background dedup scan | disabled。非同期 HRL/bitmap/inode mutation を初期 cutover に含めない。 |
| v6 repair write | 拒否。`fsck.kafs --balanced-check` の detect-only を維持。 |

## Required operator checks

Write 前:

```sh
./kafsdump --json /var/lib/kafs/destination.img > /var/tmp/kafs-v6-before.json
./fsck.kafs --balanced-check /var/lib/kafs/destination.img
```

Write smoke 後:

```sh
sync
fusermount3 -u /mnt/kafs-v6
./kafsdump --json /var/lib/kafs/destination.img > /var/tmp/kafs-v6-after.json
./fsck.kafs --balanced-check /var/lib/kafs/destination.img
```

`fsck.kafs --balanced-check` が失敗した場合、同じ v6 image で再 mount して修復を試みない。v6 repair write は
未対応なので、失敗 image を保存して旧 source に戻す。

## Rollback boundary

- production traffic を移す前に失敗した場合、v6 destination を破棄し、clean v5 source を継続利用する。
- smoke write 中に失敗した場合、新規 write を止め、v6 mount を unmount し、失敗 image を調査用に保存して
  旧 source を再 mount する。
- production cutover 後に失敗した場合、旧 source が write freeze 後から変更されていない場合だけ旧 source へ戻す。
  変更が分岐している場合は自動 rollback や双方向 merge を行わず、データ復旧インシデントとして扱う。
- rollback 後も失敗 v6 image の `kafsdump --json`、`fsck.kafs --balanced-check` 出力、mount log を保存する。

## Release note boundary

Release note には次を明記する。

- v6 write mount は default では有効化されない。
- controlled write mount は `rw,v6_write_mount` の明示 opt-in が必要。
- 初期 opt-in は experimental / controlled であり、writeback cache、runtime TRIM、delayed/background
  mutation、v6 repair write は対象外。
- operator は write 前後に `fsck.kafs --balanced-check` を実行する。
- 失敗時は v6 image を repair write せず、旧 source へ rollback する。

## 次の実装条件

次に code wiring へ進む場合、最初の slice は option parser と fail-closed regression にする。

- `--v6-write-mount`、`-o v6_write_mount`、`-o v6-write-mount` を parser に追加する。
- `rw` なし、`ro` 同時指定、inspection 同時指定、writeback cache、TRIM、background scan enabled を拒否する。
- 通常の v6 mount は引き続き offline-only gate で拒否する。
- mount admission 成功 path を入れる場合は、T4-T9 の regression / docs gate を closeout に含める。
