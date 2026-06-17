# KAFS SDカード劣化対策 バックログ

最終更新: 2026-06-17

計画: [sd-card-wear-plan.md](sd-card-wear-plan.md)

## 方針

- 実装順は固定で **Phase 1 -> Phase 2 -> Phase 3 -> Phase 4 -> Phase 5**。
- チケットが format v6 を明示しない限り、既存 v4/v5 image の mount 互換を維持する。
- metadata relocation は in-place ではなく offline migration を優先する。
- format v6 の layout decision は実装前の design checkpoint として扱う。
- 各実装 PR では、関連する最小テストと metadata durability / wear-distribution 前提を明記する。

---

## Phase 1: Journal Header Rotation

### SDW-P1-T1 Journal header slot format

- 目的: generation と CRC を持つ複数 journal header slot を定義する。
- 変更:
  - `src/kafs_journal.h`
  - feature flag または size field が必要な場合は `src/kafs_superblock.h`
  - format documentation
- 完了条件:
  - 旧 single-header image を正しく検出できる。
  - 新 multi-slot image を初期化できる。
  - CRC 不正 slot を無視できる。

### SDW-P1-T2 Journal mount/fsck slot selection

- 目的: clean write / torn write 後に最新の valid journal header slot を選択する。
- 変更:
  - journal init path
  - `fsck.kafs` journal validation
  - `kafsdump` journal reporting
- 完了条件:
  - mount が最高 generation の valid slot を選ぶ。
  - fsck は stale/corrupt slot を報告しつつ、valid slot が残る場合は image 全体を拒否しない。
  - kafsdump が slot count、active slot、generation、CRC status を表示する。

### SDW-P1-T3 Rotating header update path

- 目的: journal state update のたびに同一 header location を書き換える挙動を避ける。
- 変更:
  - journal commit/flush header write path
  - wraparound / partial write tests
- 完了条件:
  - repeated commit で header slot が進む。
  - simulated partial slot write 後も recovery できる。
  - `make check -j2` PASS。

### SDW-P1-T4 Phase 1 validation

- 目的: journal header hot spot が減ったことを確認する。
- 完了条件:
  - build/test PASS。
  - journal slot rotation test PASS。
  - before/after counter または trace で header write が複数 slot に分散している。
- 完了メモ:
  - `journal_boundary` が single-header baseline と rotated-header spread を比較する。
  - 2026-06-17 時点の観測値は `single_valid=1`, `rotated_valid=8`, `rotated_active=4`。

---

## Phase 2: Metadata Write Counters And Heatmap

### SDW-P2-T1 Metadata region taxonomy

- 目的: measurement 用の安定した metadata region ID を定義する。
- 対象 region:
  - superblock/checkpoint
  - block bitmap
  - inode table
  - allocator summary
  - HRL index
  - HRL entries
  - journal header
  - journal data
  - pending log
  - tail metadata
- 完了条件:
  - region ID が文書化されている。
  - unknown/out-of-range write を別枠で count できる。
- 完了メモ:
  - `src/kafs_meta_region.h` に 0..10 の stable region ID を定義。
  - `docs/sd-card-wear-plan.md` に region ID/name/対象を文書化。
  - `unknown` region を runtime counter 配列の末尾に確保。

### SDW-P2-T2 Runtime metadata write counters

- 目的: mounted operation 中の metadata write を region ごとに count する。
- 変更:
  - `src/kafs_context.h`
  - metadata write helpers または instrumentation wrappers
  - `kafsctl stats`
- 完了条件:
  - counters が `kafsctl stats` で見える。
  - write-heavy smoke test で期待 region の counters が増える。
  - data write を metadata write と誤分類しない。
- 完了メモ:
  - `kafsctl stats --json` / text に `metadata_write_regions` を追加。
  - journal header/data、bitmap/superblock、inode table、allocator summary、HRL index/entries、pending log、tail metadata の runtime counter を追加。
  - `fs_semantics` smoke で metadata counter 増加と `unknown` counter 0 を確認する。

### SDW-P2-T3 Offline metadata heatmap summary

- 目的: operator と benchmark が metadata layout / write distribution を確認できるようにする。
- 変更:
  - `kafsdump`
  - 必要なら `scripts/` 配下に report helper
- 完了条件:
  - `kafsdump` text / JSON に metadata region offsets/sizes が出る。
  - JSON output が parseable のまま。
  - runtime counters がある場合、report が 2 run または 2 image を比較できる。
- 完了メモ:
  - `kafsdump` text / JSON に `metadata_regions` span summary を追加。
  - rotating journal header は slot0 と tail slots を separate spans として出力する。

### SDW-P2-T4 SD-card profile options

- 目的: format v6 なしでも避けられる metadata churn を下げる operator profile を用意する。
- 候補:
  - journal header rotation が利用可能なら有効化。
  - KAFS が制御できる timestamp は noatime/relatime 寄りにする。
  - free 時の TRIM を batch 化する。
  - background dedup 頻度を下げる。
- 完了条件:
  - options が明示的に文書化されている。
  - profile を選ばない限り default behavior は変えない。
  - diagnostics に profile settings が出る。
- 完了メモ:
  - `--sd-card-profile[=conservative]` と `-o sd_card_profile=<none|conservative>` を追加。
  - conservative profile は runtime TRIM off、idle background dedup scan off、pending/bg worker idle nice=19、journal-only fsync policy を適用する。
  - `kafsctl stats` text / JSON と起動ログに profile と実効設定を出力する。

### SDW-P2-T5 Phase 2 validation

- 目的: format v6 前の measurement baseline を確立する。
- 完了条件:
  - build/test PASS。
  - `kafsctl stats` と `kafsdump --json` に期待 fields が出る。
  - write-heavy workload で metadata heatmap report を生成できる。
- 完了メモ:
  - `scripts/metadata-heatmap-report.sh` を追加し、write-heavy workload から runtime counter と offline layout span を結合した heatmap report を生成可能にした。
  - 2026-06-17 の conservative profile baseline は `report/perf/metadata-heatmap-20260617-164454` に生成。
  - 検証結果は [sd-card-wear-phase2-validation-20260617.md](sd-card-wear-phase2-validation-20260617.md) に記録。

---

## Phase 3: Format v6 Metadata Layout Descriptor

### SDW-P3-T1 Format v6 descriptor spec

- 目的: distributed metadata の root descriptor を定義する。
- 変更:
  - 新規または更新 format spec document
  - magic、version、generation、CRC、group count、group descriptors、shard mapping fields
- 完了条件:
  - descriptor bounds が曖昧でない。
  - unsupported version を明示拒否する。
  - fsck discovery behavior が仕様化されている。
- 完了メモ:
  - [sd-card-wear-format-v6-descriptor.md](sd-card-wear-format-v6-descriptor.md) を追加。
  - superblock anchor、layout descriptor header、group descriptor、shard descriptor の v1 field layout を固定。
  - descriptor / table / group / shard bounds、unsupported version / incompat flags の拒否条件、`fsck.kafs` discovery order を仕様化。

### SDW-P3-T2 Descriptor replica policy

- 目的: 局所劣化または torn write 後も v6 layout を発見できるようにする。
- 完了条件:
  - primary / backup descriptor locations が決まっている。
  - generation/CRC による latest-valid selection が決まっている。
  - kafsdump/fsck の stale replica reporting が決まっている。
- 完了メモ:
  - descriptor replica は candidate 0 primary、candidate 1 tail backup、candidate 2 optional midpoint backup として deterministic に配置する。
  - reader は全 candidate を独立に CRC / bounds / generation 検証し、highest-generation valid descriptor を選択する。
  - `kafsdump` / `fsck.kafs` は `selected` / `valid` / `stale` / `corrupt` / `unsupported` / `missing` / `divergent` を replica status として報告する。

### SDW-P3-T3 mkfs v6 skeleton

- 目的: distributed shards の全面有効化前に、descriptor を持つ v6 image を作れるようにする。
- 変更:
  - `mkfs.kafs --format-version 6`
  - mount-time explicit handling
  - `kafsdump` descriptor reporting
- 完了条件:
  - mkfs が valid descriptor replicas を書く。
  - Phase 4 support が入るまで、未対応 mount path は明確に失敗する。
  - fsck が descriptor bounds を検証する。
- 完了メモ:
  - `mkfs.kafs --format-version 6` が superblock anchor と primary / tail / midpoint descriptor replicas を書く。
  - runtime mount は v6 image を offline-only scaffold として exit 2 で明示拒否する。
  - `kafsdump` text / JSON と `fsck.kafs` が v6 descriptor discovery、bounds、replica status を報告する。
  - `v6_descriptor_smoketest` が mkfs / direct parser / kafsdump JSON / fsck / mount rejection を検証する。

### SDW-P3-T4 Phase 3 validation

- 目的: shard 実装前に descriptor semantics を固定する。
- 完了条件:
  - descriptor parser tests PASS。
  - corrupt/stale descriptor replica tests PASS。
  - Phase 4 に残る layout dependencies が docs に列挙されている。
- 完了メモ:
  - `v6_descriptor_validation` が anchor CRC、descriptor CRC、table bounds、unsupported version、incompat flags を検証する。
  - 同 test が primary corrupt 時の backup selection、stale generation reporting、same-generation divergence rejection を検証する。
  - [sd-card-wear-format-v6-descriptor.md](sd-card-wear-format-v6-descriptor.md) に Phase 4 の shard coverage / mapping dependency を列挙した。

---

## Phase 4: Distributed Metadata Shards

### SDW-P4-T1 Block bitmap shards

- 目的: data block を近傍 bitmap shard に対応させる。
- 完了条件:
  - allocation/free path が正しい shard を更新する。
  - fsck が shard coverage と overlaps/gaps を検出する。
  - v5 bitmap behavior は変更しない。

### SDW-P4-T2 Inode table shards

- 目的: inode range を分散 inode-table shard に対応させる。
- 完了条件:
  - inode lookup/allocation が descriptor mapping 経由で解決する。
  - root inode location が deterministic。
  - fsck が inode shard bounds と counts を検証する。

### SDW-P4-T3 Allocator summary shards

- 目的: allocation summaries を bitmap/data group に局所化する。
- 完了条件:
  - L1/L2 summary update が担当 shard に反映される。
  - corrupt summary の fallback/rebuild path がある。
  - allocation scan metrics が維持される。

### SDW-P4-T4 HRL shard mapping

- 目的: dedup metadata を hash bucket range または group policy で分散する。
- 完了条件:
  - lookup/put/inc/dec が正しい shard に route される。
  - fsck が壊れた HRL shard chain を検出する。
  - 既存 v5 HRL behavior は変更しない。

### SDW-P4-T5 Journal segment distribution

- 目的: journal segment を単一 prefix region ではなく metadata group 間で rotate する。
- 完了条件:
  - replay が generation order で segments を scan する。
  - torn segment/header write から復旧できる。
  - kafsdump が segment health を表示する。

### SDW-P4-T6 Phase 4 validation

- 目的: v6 distributed metadata が mountable / checkable であることを確認する。
- 完了条件:
  - build/test PASS。
  - v6 mkfs/mount/basic filesystem semantics PASS。
  - fsck が shard boundary corruption を検出する。
  - metadata heatmap で writes が group 間に分散している。

---

## Phase 5: Offline Migration To Format v6

### SDW-P5-T1 v5-to-v6 migration design

- 目的: safe offline rebuild semantics を定義する。
- 完了条件:
  - source image は変更しない。
  - destination geometry と group policy が文書化されている。
  - failure/rollback behavior が文書化されている。

### SDW-P5-T2 `kafsresize --migrate-create --format-version 6`

- 目的: 既存 image から distributed v6 destination image を作成する。
- 完了条件:
  - v5 source を v6 destination に migrate できる。
  - pre/post `kafsdump --json` summaries が期待する logical metadata と一致する。
  - unsupported / dirty states を明確な error で拒否する。

### SDW-P5-T3 Migration fsck and cutover workflow

- 目的: operator cutover を安全にする。
- 変更:
  - `fsck.kafs` v6 validation coverage
  - `docs/kafsresize-cutover-playbook.md` update
- 完了条件:
  - migration workflow に precheck、create、fsck、smoke mount、rollback steps が含まれる。
  - cutover docs が v6 compatibility requirements を明記する。

### SDW-P5-T4 Phase 5 validation

- 目的: v6 migration を operator workflow として受け入れる。
- 完了条件:
  - build/test PASS。
  - v5-to-v6 migration regression PASS。
  - migrated image が fsck と basic mount semantics に通る。
  - documentation と release notes に compatibility impact が記載されている。

---

## 最初に着手するチケット

1. SDW-P1-T1 Journal header slot format
2. SDW-P1-T2 Journal mount/fsck slot selection
3. SDW-P1-T3 Rotating header update path

Phase 2 で metadata heatmap baseline を出せるまで、Phase 3 の format v6 実装には着手しない。

---

## 直近実装メモ（2026-06-17）

### Phase 1 初期実装

- 実施内容:
  - journal header rotation 用の superblock journal flag を追加。
  - `kj_header_t.reserved0` を rotated header slot の generation として使用。
  - slot0 は legacy anchor として維持し、追加 header slots は journal 領域末尾に配置。
  - journal data offset は従来どおり `journal_offset + kj_header_size()` のまま維持。
  - `mkfs.kafs --journal-header-rotation` で opt-in 可能にした。
  - mount/replay/fsck/kafsdump が highest-generation valid slot を選択するようにした。
  - `journal_boundary` に rotation と latest-slot CRC corruption recovery の regression を追加。
  - `journal_boundary` に single-header baseline と rotated-header spread の比較を追加。
- 対応チケット:
  - SDW-P1-T1
  - SDW-P1-T2
  - SDW-P1-T3
  - SDW-P1-T4
- 残り:
  - Phase 2 として metadata write counters / heatmap に進む。
