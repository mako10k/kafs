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