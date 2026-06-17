# KAFS SDカード劣化対策 実装計画

最終更新: 2026-06-17

## 背景

現在の KAFS は、主要メタデータを image 先頭側にまとめて配置している。

- superblock
- block bitmap
- inode table
- allocator summary
- HRL index / entries
- in-image journal
- pending log
- tail metadata region

この prefix layout は検証しやすい一方で、メタデータ更新が狭い物理領域に集中しやすい。
SDカードなど wear leveling が弱い媒体では、同じ付近への小さな反復書き込みが局所劣化を早める可能性がある。

## 目的

クラッシュ復旧性と運用上の安全性を維持しつつ、メタデータ書き込みの集中を減らす。

段階的に進める。

1. 既存形式の中で最も熱い更新点を減らし、測れるようにする。
2. format v6 として分散メタデータ配置を設計する。
3. offline migration で安全に v6 destination image を作れるようにする。

## 非目的

- メタデータ書き込みを完全ランダム化しない。SDカードではランダム小書き込みが逆効果になる場合がある。
- この作業では IPC/control-plane の設計を変更しない。
- 既存 image の inode table を in-place で移動しない。
- fsck と migration path が固まるまで format v6 を安易に mountable にしない。

## 設計方針

完全なランダム分散ではなく、group 単位の分散を基本にする。

```text
[SB primary]
[group 0 metadata][group 0 data]
[group 1 metadata][group 1 data]
[group 2 metadata][group 2 data]
...
[SB/checkpoint backup][layout descriptor backup]
```

各 metadata group は近傍の data range を担当する。
inode table shard、bitmap shard、HRL shard、journal segment は deterministic に group へ割り当てる。

基本ルール:

- hot metadata を group 間に分散する。
- group 内では可能な限り batch/sequential 書き込みに寄せる。
- generation と CRC で、最新かつ完全な状態を復旧できるようにする。

## 実装フェーズ

### Phase 1: Journal Header Rotation

全体レイアウトを変えず、既存 in-image journal の hot spot を先に減らす。

journal data は ring だが、journal header の `write_off` や `seq` は同じ場所に更新が集中しやすい。
header slot を複数持たせ、mount/fsck 時に generation と CRC で最新の valid slot を選ぶ。

想定範囲:

- 可能なら v5 互換の feature flag 拡張。難しい場合は format-gated extension として扱う。
- mount、fsck、kafsdump、journal init/update path。
- partial header-slot write の破損テスト。

### Phase 2: Metadata Write Counters And Heatmap

大きな format 変更前に、metadata hot spot を可視化する。

metadata region ごとの書き込み回数を記録し、developer/operator が確認できるようにする。
SD-card mode の効果測定と format v6 の受け入れ判定に使う。

想定範囲:

- superblock/checkpoint、bitmap、inode table、allocator summary、HRL、journal header/data、pending log、tail metadata の counters。
- 可能な範囲で `kafsdump` offline summary に region 情報を追加。
- mounted filesystem では `kafsctl stats` に runtime counters を追加。
- before/after 比較に使える report 出力。

Metadata region ID は観測用 ABI として固定する。

| ID | name | 対象 |
| --- | --- | --- |
| 0 | `superblock_checkpoint` | superblock counters/checkpoint fields |
| 1 | `block_bitmap` | block allocation bitmap |
| 2 | `inode_table` | inode table entries and inline inode payload metadata |
| 3 | `allocator_summary` | allocator summary region |
| 4 | `hrl_index` | HRL bucket index |
| 5 | `hrl_entries` | HRL entry table |
| 6 | `journal_header` | journal header slot(s) |
| 7 | `journal_data` | journal ring records |
| 8 | `pending_log` | pending write log header/entries |
| 9 | `tail_metadata` | v5 tail metadata descriptors/payload |
| 10 | `unknown` | classified-failed or out-of-range metadata write |

SD-card profile は明示 opt-in とし、選択されない限り default behavior は変えない。
`conservative` profile は format v6 なしで避けられる metadata churn を下げるため、runtime TRIM
を off、idle background dedup scan を off、pending/bg worker を idle nice=19、fsync policy を
`journal_only` に揃える。KAFS runtime は read path で atime を更新しないため、profile diagnostics
では `atime_policy=no_runtime_updates` として報告する。

### Phase 3: Format v6 Metadata Layout Descriptor

分散配置の実装前に、format v6 の root descriptor を設計する。

format v6 では metadata groups、各 range、shard mapping を表す layout descriptor を導入する。
descriptor は複数箇所に複製し、局所劣化や torn write で image が発見不能にならないようにする。
root descriptor の v1 field layout と discovery / rejection rules は
[sd-card-wear-format-v6-descriptor.md](sd-card-wear-format-v6-descriptor.md) で固定する。

想定範囲:

- format v6 spec。
- magic、version、generation、CRC、bounds を持つ layout descriptor。
- descriptor replica policy。
- mount/fsck の discovery rule。
- 未対応 version の明示拒否。

### Phase 4: Distributed Metadata Shards

format v6 の group layout を実装する。

想定範囲:

- inode table shards。
- block bitmap shards。
- allocator summary shards。
- HRL index/entry shards。
- metadata group 間を回る journal segment rotation。
- fsck による shard bounds と cross-shard reference の検証。

このフェーズは format-breaking として扱い、v5 write 互換は維持しない。
既存 v4/v5 image の runtime mount は維持する。

### Phase 5: Offline Migration To Format v6

既存 image から分散配置の destination image を作る運用 path を追加する。

想定範囲:

- `kafsresize --migrate-create --format-version 6`
- deterministic な v5-to-v6 metadata copy/rebuild。
- cutover 前 validation。
- cutover documentation。
- source image を変更しない rollback guidance。

## 検証方針

- journal slot selection と torn-write recovery の unit/regression tests。
- 既知の hot path を叩く metadata heatmap tests。
- descriptor replicas と shard boundaries の `fsck.kafs` validation。
- v5-to-v6 migration tests。pre/post で `kafsdump` と `fsck.kafs` を実行する。
- write-heavy workload による metadata write distribution 比較。

## 未決定事項

- Phase 1 を v5-compatible feature flag にできるか、format-gated extension にするか。
- journal header slot の個数と配置。
- metadata group のサイズ方針と default group count。
- v6 descriptor replica を固定 anchor のみに置くか、metadata group 内にも置くか。
- heatmap data をどこまで永続化し、どこまで runtime-only にするか。
