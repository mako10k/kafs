# KAFS format v6 delayed/background mutation policy

最終更新: 2026-06-26

対象: `SDW-V6RT-T6 v6 delayed/background mutation policy`

## 目的

v6 write admission を検討する前に、foreground operation から遅れて metadata を書き得る path の扱いを
固定する。この文書は policy checkpoint であり、v6 write mount を有効化しない。

## 決定

現段階の v6 runtime admission は、delayed/background mutation をすべて無効化する。

| Path | 現段階の v6 policy | 理由 |
| --- | --- | --- |
| Pending log / pending worker | disabled | runtime は superblock offset を参照する実装で、descriptor-backed pending-log view がない。 |
| Tail metadata packing / normalization | disabled | v5 tail metadata layout 前提で、v6 descriptor shard として routing されていない。 |
| Tombstone GC / tail reclaim | disabled | reclaim path が tail metadata と inode/block mutation を遅延実行し得る。 |
| Background dedup scan | disabled | worker が HRL / bitmap / inode / allocator を非同期に更新し得る。 |

`kafs_main_v6_runtime_admit_context()` は selected descriptor admission 後にこの policy を適用し、
`kafs_op_init()` は v6 runtime context では pending worker、tombstone GC worker、background dedup worker を
起動しない。

## Fail-closed boundary

- v6 inspection mount は read-only なので、delayed/background worker は起動しない。
- `KAFS_V6_ADMISSION_HANDOFF=1` の write mapping 診断でも、runtime context に delayed/background mutation
  disabled policy を適用してから offline-only gate で終了する。
- v6 write mount を有効化する場合、次のどちらかを別チケットで選ぶ。
  - disabled policy を維持し、該当 option / delayed mutation request を明示拒否する。
  - descriptor-backed pending log / tail metadata / worker routing を実装し、stress regression を追加する。

## 検証

- `v6_descriptor_smoketest` は v6 admission handoff の診断出力に
  `pending_log=disabled tail_metadata=disabled tombstone_gc=disabled bg_dedup=disabled` が含まれることを確認する。
