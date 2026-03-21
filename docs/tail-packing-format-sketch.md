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

### Mixed Full-Block Plus Tail Update Sequence

`mixed-full-plus-tail` の更新は、初回導入では次の直列手順に制限するのが安全である。

1. inode の現レイアウトを読む
2. full-block 部分の更新があるなら既存 block path を先に処理する
3. final partial block 用の新 fragment を reserve する
4. new fragment へ final tail payload を書く
5. slot table / reverse map を更新する
6. inode の tail descriptor を新 fragment へ切り替える
7. 旧 fragment を release する
8. container header 集計値を再計算する

この順序なら、途中 crash が起きても fsck は「新 descriptor が未反映」か「旧 fragment がまだ残っている」のどちらかとして扱いやすい。

### Mixed Update Failure Boundaries

各段階で fsck がどう見るかも先に決めておく。

| crash point | expected surviving state | fsck handling |
|-------------|--------------------------|---------------|
| reserve 前 | old descriptor only | 何もしない |
| reserve 後 / payload 前 | unowned reserved slot | orphan / quarantine 候補 |
| payload 後 / descriptor switch 前 | old descriptor + prepared new slot | 新 slot を orphan 候補として detach |
| descriptor switch 後 / old release 前 | old slot と new slot が両方 live | generation を見て old slot を stale 扱い |
| old release 後 / header recount 前 | new descriptor は正しいが header 集計が古い | header を再計算 |

### Why Not In-Place Tail Mutation

初回導入で同一 slot 上書きを避ける理由:

- partial overwrite と append の境界が複雑になる
- crash 時に old payload を保全できない
- generation による stale owner 判定が弱くなる
- header/live-count/free-bytes の更新順序が読みづらくなる

したがって first-cut は copy-on-write 的に tail fragment を差し替える前提に寄せるのがよい。

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

### Fsck Validation Matrix

初回導入で fsck が最低限見るべき項目を表にする。

| check | source of truth | detect | first-cut repair |
|-------|-----------------|--------|------------------|
| `i_data_layout_kind` が定義済み値か | inode | unknown layout kind | inode を invalid 扱いにして quarantine 候補化 |
| `tail-only` で `i_tail_fragment_len > 0` か | inode | zero-length tail descriptor | inode を full-block に戻さず orphan 扱いで保全 |
| `i_tail_container_blo` が有効範囲内か | inode + superblock geometry | out-of-range container ref | descriptor を無効化して orphan fragment scan へ回す |
| `i_tail_fragment_off + i_tail_fragment_len` が class 上限以下か | inode + container header | fragment overflow | descriptor を破損扱いにし slot ownership を切る |
| slot owner と inode 番号が一致するか | inode + slot table | owner mismatch | generation が新しい側を残し、古い側を detach |
| slot generation と inode generation が一致するか | inode + slot table | stale reverse map | stale 側を解放候補にする |
| container `tc_live_count` が slot 実数と一致するか | container header + slot table | live count mismatch | slot table 再走査で header を再計算 |
| container `tc_free_bytes` が class/slot 状態と一致するか | container header + slot table | free space mismatch | header 値を再計算 |
| `mixed-full-plus-tail` の file size が full blocks + tail len と一致するか | inode size + block refs + tail descriptor | mixed final length mismatch | tail descriptor を切る前に file size を conservative に維持 |
| unowned occupied slot が残っていないか | slot table + inode scan | orphan fragment | orphan を quarantine か free 候補に送る |

### Fsck Repair Posture

初回導入では aggressive repair より conservative repair を優先する。

- inode data loss が見込まれる場合は即 free せず quarantine 寄りに扱う
- container header の集計値不一致は再計算で直す
- owner mismatch は generation で勝者を決め、それでも不明なら detach のみ行う
- orphan fragment の即時再利用は避け、1 回の fsck 実行中は再割当てしない

これにより first-cut では「壊れた metadata を再利用して二次破壊する」リスクを抑えられる。

## Locking Constraints

新しい tail allocator lock を導入する場合も、既存の lock rank policy を壊してはいけない。

最低条件:

- inode lock と bitmap lock の既存順序を維持する
- tail allocator lock を導入する場合は rank を明示する
- stale owner detection は silent wait にしない
- fragment repack を初回導入で避け、複数 inode 間移動を減らす

### Candidate Lock Rank Placement

現行 policy は次の rank 順である。

1. `hrl_global` (10)
2. `inode_alloc` (20)
3. `inode` (30)
4. `hrl_bucket` (40)
5. `bitmap` (50)

tail allocator lock を追加するなら、first-cut の候補は `45` とする。

理由:

- tail allocator は inode descriptor 決定後に取る想定なので `inode` より後段が自然
- tail container slot 更新は最終的に bitmap と相互作用しうるため `bitmap` より先に置く方が安全
- `hrl_bucket` と同列にしないことで、HRL path と tail path の混線時に order を明示しやすい

したがって初回導入の推奨順序は次の通り。

1. `hrl_global` (10)
2. `inode_alloc` (20)
3. `inode` (30)
4. `hrl_bucket` (40)
5. `tail_allocator` (45)
6. `bitmap` (50)

### Lock Acquisition Guidance For Tail Paths

tail path では次を守る。

- inode 状態の判定は `inode` lock 下で行う
- tail slot の reserve / release / reverse-map 更新は `tail_allocator` 下で行う
- block allocator へ降りる必要があるなら `bitmap` は最後にだけ取得する
- `bitmap` 保持中に `inode` や `tail_allocator` を再取得しない

### Paths To Explicitly Avoid

次の経路は inversion 予備軍なので first-cut では禁止扱いにする。

- `bitmap -> tail_allocator`
- `bitmap -> inode`
- `tail_allocator -> inode`
- `tail_allocator -> hrl_global`

### Validation Expectation

tail packing を code 化する段階では、少なくとも次の確認が必要である。

- lock wrapper に `tail_allocator` rank を追加する
- contention path で rank violation が出ないことを確認する
- mixed-full-plus-tail update path を並列実行して stale owner 検出が壊れないことを確認する

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

### First-Cut Fixed Descriptor Draft

初回導入で inode 内固定長 descriptor を採るなら、descriptor 自体は 16 bytes から始めるのが妥当である。

| field | size | note |
|-------|------|------|
| `i_data_layout_kind` | 1 byte | inline / full-block / tail-only / mixed |
| `i_tail_flags` | 1 byte | final-tail / packed-small-file など |
| `i_tail_fragment_len` | 2 bytes | payload bytes |
| `i_tail_container_blo` | 4 bytes | tail container block |
| `i_tail_fragment_off` | 2 bytes | container 内 offset |
| `i_tail_generation` | 4 bytes | stale owner 検出 |
| `i_tail_reserved` | 2 bytes | checksum / class id / future bits 用 |

この 16 bytes は次の理由で初回導入に向く。

- 4KiB block 前提では `fragment_off` が `u16` で足りる
- `fragment_len` も初回 size class を十分表現できる
- `container_blo` と `generation` が fsck / crash recovery に必要な最小限になる
- reserved 2 bytes を残すことで minor format tweak を吸収しやすい

### Inode Layout Recommendation

初回実装の推奨は「inode struct を format bump で明示拡張する」である。

避けたい案:

- 既存 `i_blkreftbl` の末尾 bytes を descriptor 用に流用する
- layout kind に応じて `i_blkreftbl` の意味を暗黙に reinterpret する

避ける理由:

- 現行では `KAFS_DIRECT_SIZE` が `i_blkreftbl` サイズに直接依存している
- inline payload と descriptor を同じ領域で多義的に扱うと fsck / migration が複雑化する
- mixed layout で final tail の有無を読むたびに条件分岐が増える

従って first-cut の docs 方針としては次を置く。

1. tail packing 対応は format bump 前提
2. inode に固定長 descriptor 領域を明示追加する
3. `KAFS_DIRECT_SIZE` は新 inode layout に合わせて再定義する
4. 既存 image からの migration では old inode -> new inode を一意に変換する

### Byte-Budget Tradeoff

inode に 16 bytes を追加すると、設計上は次のどちらかになる。

- inode サイズを増やして inline payload を維持する
- inode 総サイズを固定し、inline payload を 16 bytes 減らす

初回導入では前者を第一候補とする。

理由:

- tail packing の主眼は 61B..4KiB の space efficiency 改善であり、inline 上限を削る利得は小さい
- inline 上限を下げると既存 small-file fast path に副作用が出る
- migration 時に「old inline fits / new inline no longer fits」の境界ケースを増やしたくない

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