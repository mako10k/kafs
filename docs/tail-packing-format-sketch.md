# KAFS Tail Packing Format Sketch

最終更新: 2026-03-21

この文書は Issue #51 のための設計スケッチであり、実装仕様ではない。
目的は、non-dedup tail arena を導入する場合の最小構成と制約を先に固定することにある。

## Summary

- 現行 KAFS は inode 直持ち (`KAFS_DIRECT_SIZE`) と block-backed layout の 2 段構成である
- HRL は block 全体を hash / dedup 単位として扱う
- そのため tail packing の初手は fragment-aware HRL ではなく、non-dedup tail arena が現実的である
- 本案では inline storage は維持し、その直後の層として tail arena を追加する

## Goals

- 61B..4KiB の小ファイルと、大きな regular file の末尾端数で tail waste を減らす
- 既存の full-block HRL パスを維持する
- directories と metadata blocks は初期導入の対象外にする
- crash recovery と fsck の責務境界を block path と分離して明確化する

## Non-Goals

- tail fragment を dedup 対象にしない
- fragment-aware HRL を導入しない
- directory data や journal/pendinglog/allocator metadata を tail packing しない
- 初回導入で large-file の中間 partial update を最適化しない

## Current Constraints

### Inode layout

- inode payload は `i_blkreftbl[15]` を持つ固定 shape である
- `KAFS_DIRECT_SIZE` までは inode 内に inline 格納される
- `KAFS_DIRECT_SIZE` を超えると block-backed path に昇格する

### HRL

- `kafs_hrl_put()` は filesystem block size 全体を hash する
- refcount と ownership は physical block 単位で管理される
- fragment 単位 dedup を混ぜると HRL entry 自体の意味が変わる

### Allocator

- 現行 allocator は bitmap + allocator-v2 summary ベースで block 単位管理である
- allocator metadata の予約領域は存在するが、fragment allocator は未定義である

## Proposed Storage Tiers

導入後の regular file データ配置は 3 層とする。

1. Inline
   - サイズが `KAFS_DIRECT_SIZE` 以下
   - 現行挙動を維持

2. Tail arena
   - 小さな regular file で inline に収まらないが full block を専有させたくない場合
   - または large regular file の final partial block
   - dedup 対象外

3. Full-block HRL path
   - 既存の block-backed path
   - full block data は従来どおり HRL 管理

## Format Direction

tail packing は既存 inode block refs に ad hoc な意味を上書きせず、明示的な tail descriptor を持つべきである。

最小案:

- filesystem format version を将来 bump する
- regular inode に tail descriptor を追加する
- tail arena metadata を allocator reserved area か専用 metadata region に配置する

### Tail Descriptor Requirements

各 inode で最低限必要な情報:

- storage kind
  - inline
  - tail-fragment
  - full-block
  - mixed (full-block + final tail)
- tail container identifier
- tail fragment offset
- tail fragment length
- generation or sequence for stale descriptor detection

### Tail Arena Metadata Requirements

tail arena 側で最低限必要な情報:

- container block identifier
- free/used fragment map
- owner inode reference or reverse map
- fragment generation/check field
- optional size class / bucket metadata

## Recommended First-Cut Policy

初回導入は簡潔にする。

- 対象 inode 種別: regular file のみ
- 対象サイズ:
  - `KAFS_DIRECT_SIZE < size < blksize` の単独小ファイル
  - `size >= blksize` の final partial block
- 書込み方針:
  - append や overwrite で fragment policy を外れたら full-block に昇格
  - fragment のまま複雑な repack はしない
- dedup:
  - tail fragments には適用しない

これにより効果の主戦場を small-file / final-tail に絞り、再配置ロジックを最小化できる。

## State Transitions

最初に定義すべき遷移は次の 4 つ。

1. inline -> tail
   - file grows beyond `KAFS_DIRECT_SIZE`
   - still smaller than one full block

2. tail -> full-block
   - append or overwrite growth no longer fits tail policy
   - simplest first-cut is allocate full block(s), copy data, free fragment

3. full-block -> tail
   - initial implementationでは不要
   - truncate 後の自動降格は後回しにする

4. full-block + tail final block updates
   - final partial block only may reside in tail arena
   - middle blocks remain full-block / HRL managed

## Journaling Expectations

tail path を入れるなら、少なくとも次の原子性を保つ必要がある。

- fragment allocation
- inode descriptor update
- reverse map / occupancy update
- old fragment release

初回導入では次の順序が妥当である。

1. new fragment reserve
2. payload write
3. inode descriptor switch
4. old storage release

クラッシュ時には fsck が以下を修復できる必要がある。

- owner のいない fragment
- inode から参照されない occupied fragment
- reverse map と inode descriptor の不一致

## Fsck Scope

fsck は block invariants と fragment invariants を分離して検証するべきである。

block path:

- 既存 HRL / bitmap / inode block refs を検証

tail path:

- fragment descriptor の妥当性
- container occupancy と owner reverse map の整合性
- inode size と fragment length の整合性
- mixed layout の final tail consistency

## Locking Constraints

新しい tail allocator lock を導入する場合も、既存の lock rank policy を壊してはいけない。

最低条件:

- inode lock と bitmap lock の既存順序を維持する
- tail allocator lock を導入する場合は rank を明示する
- stale owner detection は silent wait にしない
- fragment repack を初回導入で避け、複数 inode 間移動を減らす

## Suggested On-Disk Sketch

これは概念スケッチであり、field 確定案ではない。

### Regular inode extension

- data_layout_kind
- full_block_count
- tail_desc_offset or inline tail descriptor fields
- tail_desc_generation

### Tail descriptor

- container_blo
- fragment_off
- fragment_len
- flags
- generation

### Tail container header

- magic
- version
- class / usable bytes
- free_bytes
- fragment bitmap or slot table
- optional owner count

## Candidate Field Sketch

ここでは implementation discussion に必要な粒度まで field 候補を下げる。
まだ確定仕様ではないが、少なくとも fsck と migration を考えられる程度には具体化する。

### Storage Kind Encoding

regular inode の data layout kind は次のような固定値を想定する。

- `0`: inline
- `1`: full-block
- `2`: tail-only
- `3`: mixed-full-plus-tail

初回導入では regular file のみを対象にし、directory / symlink / special inode では常に既存 path を維持する。

### Regular Inode Candidate Fields

inode 拡張 field の候補:

- `i_data_layout_kind` : `u8`
- `i_tail_flags` : `u8`
- `i_tail_inline_len` : `u16`
- `i_tail_container_blo` : `u32`
- `i_tail_fragment_off` : `u16`
- `i_tail_fragment_len` : `u16`
- `i_tail_generation` : `u32`

意味:

- `i_data_layout_kind`
   - inode の primary data layout
- `i_tail_flags`
   - readonly future bits
   - 例: packed-small-file / final-tail / needs-fsck-review
- `i_tail_inline_len`
   - tail descriptor 自体を inode 内に持つ場合の補助長さか、将来予約
- `i_tail_container_blo`
   - tail container の block 番号
- `i_tail_fragment_off`
   - container block 内 offset
- `i_tail_fragment_len`
   - fragment payload 長
- `i_tail_generation`
   - stale owner / stale reverse map 検出用

### Why This Shape

この構成だと次が成立する。

- tail-only small file は inode から 1 hop で fragment を引ける
- mixed layout は「通常 block refs + final tail descriptor」で表現できる
- fragment-aware HRL を導入せずに owner を inode 側で確定できる

### Tail Descriptor Externalization Alternative

inode field を増やしたくない場合の代替:

- inode には `tail_desc_slot` だけを置く
- 実 descriptor は専用 tail-desc table に格納する

Pros:

- inode サイズ増加を抑えられる
- future expansion がしやすい

Cons:

- read path が 1 hop 増える
- fsck で inode <-> descriptor table <-> container の 3 点整合が必要
- migration の実装量が増える

初回導入では inode 内に固定長 descriptor を置く方が単純である。

### Descriptor Placement Comparison

inode 内固定長 descriptor 案と外部 table 案の比較を以下にまとめる。

| 観点 | inode 内固定長 descriptor | 外部 tail descriptor table |
|------|---------------------------|-----------------------------|
| read path | inode から 1 hop で解決できる | inode -> table -> container の 2 hop になる |
| write path | inode 更新だけで descriptor を切替しやすい | table slot 管理が追加で必要 |
| inode size pressure | inode layout を拡張する必要がある | inode layout 追加は最小化できる |
| migration cost | inode 変換が必要 | inode + table 新設の両方が必要 |
| fsck complexity | inode <-> container の 2 点整合 | inode <-> table <-> container の 3 点整合 |
| cache locality | metadata locality が良い | descriptor table 読みが別 I/O になりやすい |
| future extensibility | field 追加余地が小さい | table schema 拡張で吸収しやすい |
| small-file fast path | 有利 | 不利 |
| large-scale feature growth | 不利 | 有利 |
| 初回導入難度 | 低い | 高い |

### Recommended Choice For First Implementation

初回導入では inode 内固定長 descriptor を優先する。

理由:

- read path が最短で済む
- small-file 最適化の狙いと整合する
- fsck の整合点を 2 系統に抑えられる
- migration / journal の変更面積が比較的小さい

### Conditions That Would Force Externalization

次のどれかが成立するなら、外部 table 案を再検討する価値がある。

- inode 拡張で既存 on-disk alignment を大きく壊す
- tail descriptor に可変長 metadata が必要になる
- multiple tail fragments per inode を扱いたくなる
- tiered packing policy を 1 inode あたり複数保持したくなる
- future compaction / online repack で descriptor 更新回数が急増する

### Selection Criteria

実装前に最終決定する基準を明示する。

1. small-file read latency を最優先するなら inode 内固定長
2. 将来の fragment policy 拡張を最優先するなら外部 table
3. fsck / migration / journal の初期実装量を抑えたいなら inode 内固定長
4. 同時に複数 fragment を持つ inode まで視野に入れるなら外部 table

現時点の Issue #51 では 1 と 3 が優先なので、第一候補は inode 内固定長 descriptor でよい。

### Tail Container Header Candidate Fields

container block 先頭 header の候補:

- `tc_magic` : `u32`
- `tc_version` : `u16`
- `tc_flags` : `u16`
- `tc_class_bytes` : `u16`
- `tc_slot_count` : `u16`
- `tc_live_count` : `u16`
- `tc_free_bytes` : `u16`
- `tc_generation` : `u32`
- `tc_owner_checksum` : `u32`

意味:

- `tc_class_bytes`
   - この container が管理する fragment class
   - 例: 128 / 256 / 512 / 1024 / 2048 / 3072
- `tc_slot_count`
   - 固定サイズ slot の総数
- `tc_live_count`
   - live fragment 数
- `tc_free_bytes`
   - 空き容量の概算
- `tc_generation`
   - container 全体の更新世代
- `tc_owner_checksum`
   - reverse map の簡易破損検出用

### Tail Slot Table Candidate Fields

fixed-size slot table 方式の候補:

- `ts_owner_ino` : `u32`
- `ts_generation` : `u32`
- `ts_len` : `u16`
- `ts_flags` : `u16`

slot payload は header/slot table の後ろに class-size 単位で並べる。

初回導入で variable-length packing を避ける理由:

- append/overwrite 時の再配置ロジックが単純
- fsck の occupancy 判定が簡単
- reverse map を 1 slot = 1 owner で扱える

### Initial Size Classes

small-file 主戦場に合わせ、初回候補は次の class を想定する。

- `128`
- `256`
- `512`
- `1024`
- `2048`
- `3072`

`4096` は既存 full-block path と衝突するため class には含めない。

### Read/Write Rules Bound To Fields

read path:

- `inline`: inode payload を読む
- `full-block`: 現行 path
- `tail-only`: descriptor から container slot を引く
- `mixed-full-plus-tail`: full blocks + final fragment を連結する

write path first-cut:

- `inline` からの成長で `size < blksize` なら `tail-only` を優先
- `tail-only` で class 超過なら `full-block` に昇格
- `full-block` で final partial block を持つ場合のみ `mixed-full-plus-tail` を許可

### Fsck Checks Implied By These Fields

最低限必要な検査:

- inode が `tail-only` なのに `i_tail_fragment_len == 0` でないか
- `i_tail_container_blo` が有効 block 範囲内か
- `i_tail_fragment_off + i_tail_fragment_len` が class 範囲内か
- slot table の `ts_owner_ino` と inode descriptor が相互一致するか
- `mixed-full-plus-tail` の final tail length が inode size と矛盾しないか

### Migration Preference

初回 migration では、既存 inode を積極的に tail 化しない方が安全である。

推奨:

- migrate 時は全 inode を従来 layout のまま移す
- format bump 後の新規作成/更新 inode から tail arena を使う
- optional な offline repack は後段 task として分離する

## Migration Strategy

tail packing 導入時は新しい filesystem format version が必要である。

移行方針:

- existing v4 images は mount 時に拒否するか、明示 migrate を要求する
- initial migration は read-only transform ではなく offline migration を前提にする
- first migration step では既存 regular file をそのまま full-block に残してもよい
- tail packing は migrate 後の新規/更新 inode から適用してもよい

## Phased Implementation Plan

1. format sketch を固定
2. superblock / inode / fsck metadata 追加
3. mkfs support を追加
4. read path を追加
5. small regular file write path を追加
6. truncate/unlink cleanup を追加
7. final partial block support を追加
8. observability / counters を追加

## Open Questions

- tail descriptor を inode 内へ収めるか、別 metadata table に逃がすか
- allocator reserved area を流用するか、tail 専用 region を新設するか
- final partial block を tail arena に置く条件を fixed policy にするか tunable にするか
- tail -> full-block promotion を eager にするか threshold 制にするか
- fsck repair で fragment orphan を即 free するか quarantine するか

## Recommendation

Issue #51 の次段では、まず Option A の format sketch と fsck invariants を固定するべきである。

現段階で code change に入るのは早い。理由は次のとおり。

- inode metadata の表現が未確定
- journal repair boundary が未確定
- allocator と lock rank の追加設計が未確定
- 効果の主戦場は明確でも、導入コストが block path 全体へ波及するため

先に format sketch を固め、そのあと read-only parsing と fsck support を置くのが最も安全である。