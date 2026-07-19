# KAFS SDカード劣化対策 バックログ

最終更新: 2026-07-16

計画: [sd-card-wear-plan.md](sd-card-wear-plan.md)

## 方針

- 実装順は固定で **Phase 1 -> Phase 2 -> Phase 3 -> Phase 4 -> Phase 5**。
- チケットが format v6 を明示しない限り、既存 v4/v5 image の mount 互換を維持する。
- metadata relocation は in-place ではなく offline migration を優先する。
- format v6 は実験的実装として凍結し、descriptor-backed runtime entrypoint split と
  controlled-write opt-in boundary の検証結果として扱う。
- 破壊的変更を伴う descriptor-backed format work は、format v7 / `kafs-v7` を入口にする。
- v7 の accepted RAW LAYOUT は、
  [sd-card-wear-format-v7-inception-deck.md](sd-card-wear-format-v7-inception-deck.md)
  の accepted decision model / scope と metadata region coverage matrix、および
  [sd-card-wear-format-v7-raw-layout.md](sd-card-wear-format-v7-raw-layout.md) の
  `K7LD` descriptor version 2 contract を正とする。
- v7 は wear distribution と fault tolerance / deterministic fsck recovery を同率最優先とし、
  recovery が曖昧になる placement は採用しない。
- 各実装 PR では、関連する最小テストと metadata durability / wear-distribution 前提を明記する。
- 現行の v7 方針は
  [sd-card-wear-format-v7-pivot.md](sd-card-wear-format-v7-pivot.md) を正とする。

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
- 進捗メモ:
  - v6 selected descriptor から block bitmap shard coverage を検証する共通 helper を追加。
  - 現行 mkfs v6 scaffold の bitmap shard は root block namespace `[0, s_r_blkcnt)` を 1 shard
    で覆う。`fsck.kafs` / `kafsdump` は gap、logical overlap、physical overlap を報告する。
  - data block lookup を descriptor 経由で bitmap byte/bit に解決する helper を追加。
  - `49cd632 feat: route bitmap updates through descriptor mapper` で runtime の
    `kafs_blk_get_usage` / set/free / claim / legacy allocation scan を bitmap word mapper 経由に
    再構成した。v5 は従来の contiguous bitmap または meta-delta overlay を使い続け、v6 は
    `c_v6_bitmap_mapping_enabled` と selected descriptor が設定された場合だけ descriptor-backed
    word を解決する dormant path を持つ。
  - `v6_descriptor_validation` に `bitmap_runtime_descriptor_mapping` を追加し、v6 image を
    `MAP_PRIVATE` で mmap して mount admission なしに descriptor-backed set/clear が対象 bitmap byte
    に反映されることを検証した。
  - `kafs_bitmap_descriptor_mapping_admit_fd` / `kafs_bitmap_descriptor_mapping_clear` を追加し、
    selected descriptor を `kafs_context` owned lifetime として保持し、`fsck.kafs` と同じ bitmap
    coverage / overlap validation を通った場合だけ `c_v6_bitmap_mapping_enabled` を立てる dormant
    admission hook を追加した。close/unmap/cleanup path は owned descriptor を破棄する。
  - `v6_descriptor_validation` に 2 shard の positive runtime mapping と、gap coverage で admission が
    mapping を有効化しない negative test を追加した。
- 引き継ぎメモ:
  - v6 runtime mount はまだ有効化しない。runtime mount gate は v6 を許可せず、CLI も v6 descriptor
    scaffold を offline-only として拒否する。
  - 現在の runtime mapper は `kafs_blkmask_t` word 単位で読むため、descriptor-backed write path は
    bitmap word の物理 offset が `sizeof(kafs_blkmask_t)` に揃うことを要求する。将来の split shard
    で logical start を任意 bit 境界にするなら byte-granular scan/update を追加する。そうしない場合は
    v6 bitmap shard logical ranges を word 境界に揃える validation rule を追加する。
  - allocator v3 summary は SDW-P4-T3 で descriptor-backed dormant path に対応した。v6 runtime mount は
    まだ有効化しない。
  - P4-T2 に進む clone は inode table shard mapping の設計に入ってよい。

### SDW-P4-T2 Inode table shards

- 目的: inode range を分散 inode-table shard に対応させる。
- 完了条件:
  - inode lookup/allocation が descriptor mapping 経由で解決する。
  - root inode location が deterministic。
  - fsck が inode shard bounds と counts を検証する。
- 進捗メモ:
  - `kafs_v6_inode_lookup` / `kafs_v6_inode_validate_coverage` を追加し、inode table shard の
    `sd_record_bytes`、logical coverage `[0, s_inocnt)`、logical/physical overlap、root inode lookup
    を検証できるようにした。
  - `fsck.kafs` の v6 offline path は bitmap shard に続いて inode table shard を検証し、不正な inode
    coverage を v6 descriptor failure として fail closed する。
  - `v6_descriptor_validation` に valid inode coverage / root lookup と inode coverage gap の fsck
    rejection を追加した。
  - `kafs_v6_descriptor_mapping_admit_fd` を追加し、selected descriptor を `kafs_context` owned lifetime
    として保持し、bitmap と inode coverage validation の両方を通った場合だけ bitmap/inode runtime
    mapping を有効化する dormant admission hook を追加した。
  - v6 inode runtime shard map を `kafs_context` に保持し、`kafs_ctx_inode()` /
    `kafs_ctx_inode_const()` / `kafs_ctx_ino_no()` が descriptor-backed inode table を参照できるようにした。
  - inode allocation の free scan は `kafs_ctx_ino_find_free()` 経由に切り替え、v4/v5 は従来の contiguous
    table scan、v6 dormant mapping 時は descriptor-backed shard scan を使う。
  - `v6_descriptor_validation` に root inode runtime lookup、2 shard inode allocation scan、inode gap
    coverage で admission が mapping を有効化しない negative test を追加した。
- 引き継ぎメモ:
  - v6 runtime mount は引き続き disabled。mount admission gate と CLI は v6 descriptor scaffold を
    offline-only として拒否する。
  - allocator summary shard は SDW-P4-T3 で descriptor-backed dormant path に進めた。

### SDW-P4-T3 Allocator summary shards

- 目的: allocation summaries を bitmap/data group に局所化する。
- 完了条件:
  - L1/L2 summary update が担当 shard に反映される。
  - corrupt summary の fallback/rebuild path がある。
  - allocation scan metrics が維持される。
- 進捗メモ:
  - `kafs_v6_allocator_summary_shape` / `kafs_v6_allocator_summary_lookup` /
    `kafs_v6_allocator_summary_validate_coverage` を追加し、allocator summary shard の
    `[0, s_r_blkcnt)` coverage、logical/physical overlap、summary record lookup を検証できるようにした。
  - `fsck.kafs` の v6 offline path は bitmap/inode に続いて allocator summary shard を検証し、不正な
    allocator coverage を v6 descriptor failure として fail closed する。
  - `kafs_v6_descriptor_mapping_admit_fd` は bitmap/inode/allocator coverage のすべてを通った場合だけ
    dormant runtime mapping を有効化し、`kafs_context` に allocator summary shard map を保持する。
  - allocator v3 summary path は contiguous v4 summary と v6 descriptor-backed summary view を分け、
    L1/L2 sync、dirty rebuild、allocation find を担当 shard に route する。
  - corrupt/stale summary で候補が見つからない場合、allocation path は 1 回 dirty rebuild して再検索する。
    真に空きがない場合は従来通り `ENOSPC` になる。
  - `blk_alloc` の scan/claim/set_usage metrics は既存 path のまま維持し、descriptor-backed summary でも
    `blk_alloc_calls` と allocator summary write counter が更新される。
  - `v6_descriptor_validation` に allocator summary valid lookup、2 shard summary allocation、corrupt summary
    rebuild、gap coverage fsck rejection、admission rejection を追加した。
- 引き継ぎメモ:
  - v6 runtime mount は引き続き disabled。mount admission gate と CLI は v6 descriptor scaffold を
    offline-only として拒否する。
  - descriptor-backed summary shard は block-aligned physical range を要求する。分割 shard を mkfs/migration
    で本格生成する場合、各 shard の L1/L2 summary が block-aligned physical range に収まるよう padding を
    明示的に確保する。

### SDW-P4-T4 HRL shard mapping

- 目的: dedup metadata を hash bucket range または group policy で分散する。
- 完了条件:
  - lookup/put/inc/dec が正しい shard に route される。
  - fsck が壊れた HRL shard chain を検出する。
  - 既存 v5 HRL behavior は変更しない。
- 進捗メモ:
  - `kafs_v6_hrl_index_lookup` / `kafs_v6_hrl_entry_lookup` と HRL index/entry coverage validation
    を追加し、HRL bucket range と entry id range の exact coverage、logical/physical overlap、
    record size を検証できるようにした。
  - `fsck.kafs` の v6 offline path は allocator summary に続いて HRL index/entry coverage と
    bucket chain bounds を検証し、entry id 範囲外、loop、read error、index shard group と entry
    shard group の不一致を v6 descriptor failure として fail closed する。
  - `kafs_v6_descriptor_mapping_admit_fd` は bitmap/inode/allocator/HRL coverage のすべてを通った
    場合だけ dormant runtime mapping を有効化し、`kafs_context` に HRL index/entry runtime shard map
    を保持する。
  - `kafs_hrl.c` の index/entry access は contiguous v5 path と descriptor-backed v6 path を分岐し、
    `lookup/put/inc/dec` と free-list scan が bucket/entry shard map 経由で動くようにした。
  - `v6_descriptor_validation` に HRL valid lookup/chain scan、2 shard runtime put/inc/dec routing、
    HRL index/entry gap rejection、chain out-of-range fsck rejection、admission rejection を追加した。
- 引き継ぎメモ:
  - v6 runtime mount は引き続き disabled。mount admission gate と CLI は v6 descriptor scaffold を
    offline-only として拒否する。
  - descriptor-backed HRL entries は fixed-record shard なので、分割 shard の physical range は
    `sizeof(kafs_hrl_entry_t)` と block alignment の両方を満たす record count で分ける必要がある。

### SDW-P4-T5 Journal segment distribution

- 目的: journal segment を単一 prefix region ではなく metadata group 間で rotate する。
- 完了条件:
  - replay が generation order で segments を scan する。
  - torn segment/header write から復旧できる。
  - kafsdump が segment health を表示する。
- 進捗メモ:
  - `journal_header` fixed-record shard lookup と `journal_data` byte-span shard lookup を追加し、
    segment id から descriptor-backed header offset / data span を解決できるようにした。
  - `fsck.kafs` は v6 selected descriptor について journal header coverage、journal data coverage、
    header/data pair group matching、header CRC / area bounds、data record CRC を検証する。
  - `kafsdump` text / JSON に `v6_journal_segments` を追加し、segment count、selected segment、
    selected generation、selected seq/write offset、health flags、first bad segment を表示する。
  - torn newer segment/header があっても older valid segment が残る場合は highest valid generation を
    選択して offline health OK とし、valid segment が残らない場合は fail closed する。
  - `v6_descriptor_validation` に journal coverage valid lookup、journal header gap rejection、
    torn latest segment recovery の regression を追加した。
- 引き継ぎメモ:
  - v6 runtime mount は引き続き disabled。今回の実装は offline scaffold validation と dormant
    admission coverage validation までで、live journal write/replay の descriptor-backed routing は
    v6 write mount を有効化する直前に接続する。
  - v6 scaffold の journal header `area_size` は descriptor-backed journal data segment size に合わせる。
    v4/v5 の legacy journal geometry は変更しない。

### SDW-P4-T6 Phase 4 validation

- 目的: v6 distributed metadata が mountable / checkable であることを確認する。
- 進捗メモ:
  - `kafs` の v6 runtime mount path は、offline-only 拒否の前に selected descriptor の admission
    preflight を実行し、bitmap/inode/allocator/HRL coverage と journal segment health の成功/失敗を
    診断表示する。v6 runtime mount はまだ有効化しない。
  - `kafs_journal_init` / `kafs_journal_replay` は、v6 selected descriptor を所有する context では
    descriptor-backed journal segment を選択し、header/data writes と replay reset を legacy prefix
    offset ではなく `journal_header` / `journal_data` lookup 経由にする。v6 runtime mount はまだ有効化
    しない。
  - `KAFS_V6_ADMISSION_HANDOFF=1` を指定した CLI mount は full image を実 runtime context に mmap し、
    selected descriptor と shard maps を `kafs_context` に保持した状態で journal segment health まで
    検証してから解放し、同じ offline-only gate で拒否する。FUSE mount / v6 write admission はまだ
    有効化しない。
  - `KAFS_V6_READONLY_SMOKE=1` を指定した CLI mount は admitted descriptor を保持した実 runtime
    context で read-only FUSE mount だけを許可する。image は read-only mmap、journal replay と
    background mutation worker は起動しない。root と nested directory の `getattr` / `readdir` /
    lookup、inline small-file `read`、symlink `readlink`、`EROFS` write rejection を smoke test で
    確認する。通常の v6 mount と v6 write admission はまだ有効化しない。
  - `v6_descriptor_validation` は inode shard の record-size mismatch / physical truncation と
    journal-data shard の record-size mismatch を、selected descriptor として読めるが
    fsck / descriptor admission / CLI admission preflight で fail-closed になる regression として
    確認する。
  - `v6_descriptor_validation` は 2 metadata group / 2 journal segment の descriptor fixture を
    作り、highest generation segment が group 1 から選択され、`fsck.kafs` / `kafsdump --json` が
    `selected_group=1` として報告することを確認する。v6 write mount はまだ有効化しない。
  - `kafsdump` text / JSON は selected v6 descriptor の group / shard summaries を表示し、
    2-group fixture で group 1 の journal header/data shards を機械的に読めることを
    `v6_descriptor_validation` で確認する。
  - `v6_descriptor_validation` は inode shard の descriptor-level physical boundary corruption と
    selected descriptor の logical boundary corruption を確認する。前者は descriptor discovery で
    corrupt replica として拒否し、後者は fsck / descriptor admission / CLI admission preflight で
    fail-closed になる。
  - `metadata-heatmap-report.sh --v6-kafsdump-json` は `kafsdump --json` から v6 group/shard heatmap
    を生成し、2-group fixture で descriptor-backed write-candidate metadata spans が group 0/1 に
    分散していることを確認する。v6 write mount はまだ有効化しない。
- 完了条件:
  - build/test PASS。
  - v6 mkfs/mount/basic filesystem semantics PASS。
  - fsck が shard boundary corruption を検出する。
  - metadata heatmap で writes が group 間に分散している。

---

## Phase 5: Offline Migration To Format v6

### SDW-P5-T1 v5-to-v6 migration design

- 目的: safe offline rebuild semantics を定義する。
- 進捗:
  - `kafsresize --migrate-create --src-image <v5> --dst-image <dst> --format-version 6 --dry-run`
    は source を変更せず、clean v5 source、宛先 inode/data capacity、v6 descriptor replica
    placement を事前診断する。
- 完了条件:
  - source image は変更しない。
  - destination geometry と group policy が文書化されている。
  - failure/rollback behavior が文書化されている。

### SDW-P5-T2 `kafsresize --migrate-create --format-version 6`

- 目的: 既存 image から distributed v6 destination image を作成する。
- 進捗:
  - `--format-version 6` の通常 `--migrate-create` でも `--src-image` を必須にし、clean v5 source、
    宛先 inode/data capacity、v6 descriptor replica placement の precheck を destination overwrite 前に
    実行する。
  - v5 source / v6 destination の `kafsdump --json` pre/post summaries を regression に追加し、source
    summary が不変で、destination が v6 descriptor replica / group / shard scaffold を持つことを確認する。
- 完了条件:
  - v5 source を v6 destination に migrate できる。
  - pre/post `kafsdump --json` summaries が期待する logical metadata と一致する。
  - unsupported / dirty states を明確な error で拒否する。

### SDW-P5-T3 Migration fsck and cutover workflow

- 目的: operator cutover を安全にする。
- 進捗:
  - v6 `migrate-create` destination に対して `fsck.kafs --balanced-check` を実行し、descriptor replica、
    shard coverage、HRL chain、journal segment health の summary を regression で確認する。
  - cutover playbook は v4/v5 の smoke mount / rsync cutover と、現行 v6 descriptor destination の
    offline-only staging を分離し、v6 runtime mount compatibility gate を明記する。
- 変更:
  - `fsck.kafs` v6 validation coverage
  - `docs/kafsresize-cutover-playbook.md` update
- 完了条件:
  - migration workflow に precheck、create、fsck、smoke mount、rollback steps が含まれる。
  - cutover docs が v6 compatibility requirements を明記する。

### SDW-P5-T4 Phase 5 validation

- 目的: v6 migration を operator workflow として受け入れる。
- 進捗:
  - `docs/sd-card-wear-phase5-validation-20260626.md` に Phase 5 の実行条件、validation coverage、PASS
    判定、現行 v6 offline-only compatibility boundary を記録した。
  - `CHANGELOG.md` の Unreleased に v6 `migrate-create` precheck / regression と runtime mount
    compatibility impact を記載した。
  - v6 `migrate-create` destination の runtime mount attempt が admission preflight 後に offline-only gate
    で exit 2 になることを regression に追加し、現行境界の basic mount admission semantics を固定した。
- 完了条件:
  - build/test PASS。
  - v5-to-v6 migration regression PASS。
  - migrated image が fsck と現行互換境界の basic mount admission semantics に通る。
  - documentation と release notes に compatibility impact が記載されている。
- 完了メモ:
  - v6 `migrate-create` workflow は offline migration staging として受け入れ済み。
  - validation 結果は [sd-card-wear-phase5-validation-20260626.md](sd-card-wear-phase5-validation-20260626.md) に記録した。
  - v6 destination は `kafsdump --json` / `fsck.kafs --balanced-check` の offline validation に通る。
  - runtime mount attempt は admission preflight で descriptor-backed metadata checks を通した後、
    offline-only gate で exit 2 として fail closed する。
  - v6 production runtime mount / cutover は Phase 5 の範囲外とし、次段チケットで扱う。

---

## Post-Phase 5: Format v6 Runtime Mount Enablement

### SDW-V6RT-T1 v6 runtime mount admission design checkpoint

- 目的: offline-only v6 scaffold から runtime mount 有効化へ進む前に、許可する mount mode と安全境界を固定する。
- 進捗:
  - [sd-card-wear-v6-runtime-mount-checkpoint.md](sd-card-wear-v6-runtime-mount-checkpoint.md) に
    read-only admission 先行、write admission 併走、offline-only 継続の判断基準を整理した。
  - A + dedicated opt-in を採用し、operator-facing な名称は `v6 inspection mount` とする。
    read-only は安全性質として強制するが、機能名にはしない。
  - write admission は journal / mutation workers / repair / lock policy の条件を同時に満たす場合だけ選ぶ。
- 変更:
  - v6 read-only mount と write mount を同時に進めるか、read-only admission を先に昇格するかを決める。
  - descriptor-backed journal replay/write、background mutation workers、repair/write fsck の解禁順を決める。
  - v5/v6 compatibility、rollback、operator cutover、release note の境界を文書化する。
- 完了条件:
  - v6 runtime mount の最初の有効化対象が read-only / write のどちらか明記されている。
  - admission preflight、journal health、descriptor lifetime、lock/rank policy の必須条件が列挙されている。
  - Phase 5 で作成した v6 migration destination を本番 cutover 対象にできる条件と、まだ対象外にする条件が分離されている。
- 完了メモ:
  - 初回 runtime 対象は `v6 inspection mount` とし、write admission / production cutover は対象外にした。
  - 専用 opt-in は `-o ro,v6_inspection_mount`。`-o ro` だけでは v6 を許可しない。

### SDW-V6RT-T2 v6 inspection mount admission

- 目的: v6 destination image を production write cutover ではなく、検査用 runtime path として mount できるようにする。
- 進捗:
  - `-o ro,v6_inspection_mount` を v6 inspection mount の専用 opt-in として追加した。
  - `-o ro` だけでは v6 mount を許可せず、offline-only gate と inspection option guidance を返す。
  - v6 inspection mount は image を read-only open / `PROT_READ` mapping / read lock / FUSE `ro` で扱い、
    writeback cache と TRIM を無効化する。
  - `v6_descriptor_smoketest` に explicit option regression、writeback_cache rejection、mutation `EROFS`
    rejection、mount/unmount 前後の backing image content unchanged check を追加した。
- 完了条件:
  - `-o ro,v6_inspection_mount` で v6 fixture の `statfs` / traversal / read / readlink が通る。
  - mutation attempt が `EROFS` で拒否され、backing image content が変化しない。
  - v6 write mount / production cutover は引き続き対象外として文書化されている。
- 完了メモ:
  - `v6_descriptor_smoketest`、`kafsresize`、`make check -j2`、format/lint/static gates で PASS。
  - `make check -j2` の 15 秒 timeout run では `stress_fs` が一度 SKIP したが、同条件の単体再実行で PASS。

### SDW-V6RT-T3 v6 write-mount dependency audit

- 目的: v6 write mount を有効化する前に、journal、metadata mutation path、pending/tail/worker、
  fsck/repair、lock policy の依存関係を固定する。
- 進捗:
  - [sd-card-wear-v6-write-mount-dependency-audit.md](sd-card-wear-v6-write-mount-dependency-audit.md) に
    write admission 前の必須条件とブロッカーを記録した。
  - v6 write mount は単一 gate ではなく、descriptor-backed journal proof、live metadata mutation
    routing proof、pending/tail/worker policy、post-write fsck/repair policy、lock/stress validation に
    分けて進める方針にした。
- 完了条件:
  - write mount の未対応 path が journal / mutation / worker / repair / lock / operator 境界に分解されている。
  - 次に実装するチケットが明記されている。
- 完了メモ:
  - 次は `SDW-V6RT-T4 descriptor-backed journal write/replay proof` に進む。
  - T4 では write mount をまだ有効化せず、selected v6 journal segment への write/replay routing を証明する。

### SDW-V6RT-T4 descriptor-backed journal write/replay proof

- 目的: v6 write admission 前に、journal write/replay が selected descriptor-backed segment だけを使うことを証明する。
- 進捗:
  - `journal_boundary` の v6 descriptor-backed routing regression を強化し、
    `kafs_journal_begin()` / `kafs_journal_commit()` / `kafs_journal_force_flush()` が selected
    `journal_header` / `journal_data` shard を更新することを確認する。
  - replay は descriptor-backed segment を scan し、replay reset 後に selected journal data 先頭が zeroed
    になることを確認する。
  - v6 legacy journal data prefix 相当の byte range を snapshot し、write/force_flush 後と replay reset 後の
    両方で変化しないことを確認する。
- 変更:
  - focused journal regression
  - 必要なら v6 descriptor-backed runtime journal harness
- 完了条件:
  - `kafs_journal_begin()` / `kafs_journal_commit()` / `kafs_journal_force_flush()` が selected
    `journal_header` / `journal_data` shard を更新する。
  - `kafs_journal_replay()` が descriptor-backed segment を scan し、replay reset も selected segment に限定される。
  - legacy journal prefix region が v6 test fixture で更新されない。
- 完了メモ:
  - write mount はまだ有効化しない。
  - 次は `SDW-V6RT-T5 v6 live metadata mutation routing proof` に進む。

### SDW-V6RT-T5 v6 live metadata mutation routing proof

- 目的: write mount の主要 mutation operations が descriptor-backed metadata shards を正しく更新することを確認する。
- 進捗:
  - `v6_descriptor_validation` に `live_metadata_mutation_routing_matrix` を追加し、同一 v6 descriptor
    fixture 上で bitmap / inode / allocator summary / HRL index / HRL entries をすべて 2 shard 化して
    dormant runtime admission 後の live mutation routing を検証する。
  - allocator allocation、bitmap set/free、inode allocation/init、HRL put/inc/dec を実行し、対象 shard の
    byte/record が変化し、対応する metadata write counter が増えることを確認する。
  - 各 region の非対象 shard は targeted snapshot で変化しないことを確認する。allocator は dirty rebuild
    ではなく通常 summary sync path を通るよう、対象 L0 group だけを「最後の 1 block が空き」の状態にして
    第2 shard の L1/L2 更新を証明する。
  - v6 write mount と FUSE operation matrix はまだ有効化しない。今回の proof は write admission 前に必要な
    lower-level metadata mutation routing の閉じ込みとして扱う。
- 完了条件:
  - write mount 解禁前の lower-level matrix として、allocator allocation、bitmap set/free、inode mutation、
    HRL put/inc/dec が descriptor-backed shard 経由で動くことを確認する。
  - create/write/truncate/fallocate/unlink/rename/link/symlink/copy/reflink/fsync/release の FUSE operation
    matrix は write admission gate で再確認する。
  - bitmap / inode / allocator summary / HRL の expected shard write counters が増え、unexpected shard が変化しない。
  - bitmap word alignment 制約または byte-granular update 方針が admission validation に反映されている。
- 完了メモ:
  - write mount はまだ有効化しない。
  - 次は `SDW-V6RT-T6 v6 delayed/background mutation policy` に進む。

### SDW-V6RT-T6 v6 delayed/background mutation policy

- 目的: pending log、tail metadata、tombstone GC、background dedup worker を v6 write mount でどう扱うか固定する。
- 進捗:
  - [sd-card-wear-v6-delayed-background-policy.md](sd-card-wear-v6-delayed-background-policy.md) に
    v6 runtime admission 時の delayed/background mutation policy を記録した。
  - 現段階では pending log / pending worker、tail metadata packing / normalization、tombstone GC /
    tail reclaim、background dedup scan はすべて disabled とする。
  - `kafs_main_v6_runtime_admit_context()` は selected descriptor admission 後に disabled policy を適用し、
    `kafs_op_init()` は v6 runtime context では pending worker、tombstone GC worker、background dedup worker
    を起動しない。
  - `v6_descriptor_smoketest` は `KAFS_V6_ADMISSION_HANDOFF=1` の診断出力で
    `pending_log=disabled tail_metadata=disabled tombstone_gc=disabled bg_dedup=disabled` を確認する。
- 完了条件:
  - v6 write mount で無効化する機能と、descriptor-backed 実装して有効化する機能が分離されている。
  - pending log / tail metadata を無効化する場合、該当 option や delayed mutation が fail closed する。
  - worker を有効化する場合、descriptor-backed routing と stress regression がある。
- 完了メモ:
  - write mount はまだ有効化しない。
  - 次は `SDW-V6RT-T7 v6 post-write fsck and repair policy` に進む。

### SDW-V6RT-T7 v6 post-write fsck and repair policy

- 目的: v6 write mount 後に fsck が detect-only で安全に判定できる境界と repair 解禁順を決める。
- 進捗:
  - [sd-card-wear-v6-post-write-fsck-repair-policy.md](sd-card-wear-v6-post-write-fsck-repair-policy.md)
    に v6 post-write fsck / repair policy を記録した。
  - 現段階の format v6 `fsck.kafs` は detect-only validation のみを supported path とする。
  - `fsck.kafs --balanced-check <image>` を production write cutover 前後の required check とする。
  - v6 repair/write option は fail closed し、journal replay/reset、descriptor replica repair、
    metadata shard repair は別チケットで順に解禁する。
  - `v6_descriptor_validation` は repair/write option 拒否、same-generation descriptor divergence 拒否、
    valid journal segment が 0 の torn journal 拒否を確認する。
- 完了条件:
  - dirty journal、torn journal、descriptor divergence、metadata shard corruption の detect-only 判定がある。
  - repair write を未対応のままにする状態と、repair write が必要な状態が明確に分かれている。
  - production write cutover 前の required fsck command が文書化されている。
- 完了メモ:
  - write mount と v6 repair write はまだ有効化しない。
  - 次は `SDW-V6RT-T8 v6 write mount lock/stress gate` に進む。

### SDW-V6RT-T8 v6 write mount lock/stress gate

- 目的: write admission 対象 path が `.github/lock-policy.md` に従うことを確認する。
- 実施内容:
  - `docs/sd-card-wear-v6-lock-stress-gate.md` に lock rank order、`KAFS_CALL` after lock、対象範囲外の
    user-visible FUSE write surface を整理した。
  - `v6_descriptor_validation` に `v6_write_lock_stress_gate` を追加した。
  - 追加 regression は v6 descriptor-backed HRL/bitmap mapping を使い、全 lock class の contention
    counter と並行 HRL mutation を確認する。
- 完了条件:
  - lock rank order と `KAFS_CALL` after lock 禁止の監査が完了している。
  - contention / concurrent write regression が少なくとも 1 つある。
  - T4-T7 の結果を踏まえて、write mount を explicit opt-in で有効化できるか判断できる。
- 完了メモ:
  - T4-T7 で固定した explicit write opt-in 候補の低レイヤ path は T8 gate を通過した。
  - 通常の v6 FUSE write mount はまだ有効化しない。
  - 次は `SDW-V6RT-T9 v6 explicit write opt-in cutover boundary` に進む。

### SDW-V6RT-T9 v6 explicit write opt-in cutover boundary

- 目的: v6 write を user-visible opt-in に進める前に、operator cutover と rollback の境界を固定する。
- 実施内容:
  - `docs/sd-card-wear-v6-explicit-write-cutover-boundary.md` に opt-in 名、fail-closed admission 条件、
    unsupported option、required fsck、rollback を固定した。
  - `docs/kafsresize-cutover-playbook.md` に、当時 future 扱いだった controlled v6 write opt-in
    boundary を追加した。
  - `docs/release-note-v6-explicit-write-opt-in-boundary.md` に experimental / controlled opt-in の release
    note draft を追加した。
- 完了条件:
  - user-visible opt-in 名と fail-closed admission 条件が決まっている。
  - unsupported option、delayed/background mutation、v6 repair write の拒否境界が operator 向けに明記されている。
  - write 前後の required fsck command と失敗時 rollback が文書化されている。
  - release note に experimental / controlled opt-in の範囲が記載されている。
- 完了メモ:
  - user-visible opt-in 名は `--v6-write-mount` / `-o v6_write_mount` / `-o v6-write-mount` とする。
  - 初期 controlled opt-in は `rw,v6_write_mount,no_writeback_cache,no_trim_on_free,bg_dedup_scan=off` を
    推奨形とし、通常 v6 mount は引き続き暗黙 write admission にしない。
  - 次は `SDW-V6RT-T10 v6 write opt-in parser and fail-closed gate` に進む。

### SDW-V6RT-T10 v6 write opt-in parser and fail-closed gate

- 目的: T9 で予約した v6 write opt-in を parser に追加し、未対応条件を fail closed する。
- 実施内容:
  - `--v6-write-mount`、`-o v6_write_mount`、`-o v6-write-mount` を parser に追加した。
  - `rw` 未指定、`ro` 同時指定、`v6_inspection_mount` 同時指定、`writeback_cache`、`trim_on_free`、
    `bg_dedup_scan=on` を拒否する fail-closed gate を追加した。
  - 推奨形 `rw,v6_write_mount,no_writeback_cache,no_trim_on_free,bg_dedup_scan=off` まで満たしても、
    T10 時点では `controlled write mount is not enabled yet` として拒否する。
  - `v6_descriptor_smoketest` に parser / fail-closed regression を追加した。
- 完了条件:
  - `--v6-write-mount`、`-o v6_write_mount`、`-o v6-write-mount` が parser で認識される。
  - `rw` 未指定、`ro` 同時指定、`v6_inspection_mount` 同時指定、`writeback_cache`、`trim_on_free`、
    `bg_dedup_scan=on` が v6 write opt-in で拒否される。
  - 通常 v6 mount と v6 inspection mount の既存挙動が変わらない regression がある。
  - 実際の write admission 成功 path を入れる場合は、T4-T9 の gate と `make check -j2` を closeout に含める。
- 完了メモ:
  - v6 write opt-in は parser で認識されるが、write admission 成功 path はまだ有効化しない。
  - 次は `SDW-V6RT-T11 v6 FUSE write surface admission audit` に進む。

### SDW-V6RT-T11 v6 FUSE write surface admission audit

- 目的: controlled write mount の成功 path を入れる前に、user-visible FUSE write operations の対象範囲を
  function 単位で固定する。
- 実施内容:
  - `docs/sd-card-wear-v6-fuse-write-surface-audit.md` を追加し、FUSE write surface を
    `create`、`write`、`truncate`、`fallocate`、`unlink`、`rename`、`link`、`symlink`、`copy`、
    `reflink`、`fsync`、`release` に分解した。
  - 初期 controlled write opt-in の許可候補を regular file `create` / `write` / `fsync` / `release`
    に限定し、それ以外を runtime guard で拒否する方針に固定した。
  - `fsync` / `release` / `write` / `open(O_TRUNC)` に必要な v6 専用 guard と、最小 smoke workload、
    rollback / fsck closeout command を固定した。
- 完了条件:
  - create/write/truncate/fallocate/unlink/rename/link/symlink/copy/reflink/fsync/release の各 path が
    descriptor-backed metadata routing、disabled delayed/background policy、lock policy のどれに依存するか
    表で整理されている。
  - 初期 controlled write opt-in で許可する operation と拒否する operation が決まっている。
  - 成功 path を実装する場合の最小 smoke workload と rollback/fsck closeout command が決まっている。
- 完了メモ:
  - 次は `SDW-V6RT-T12 v6 controlled write admission skeleton and operation guard` に進む。

### SDW-V6RT-T12 v6 controlled write admission skeleton and operation guard

- 目的: T10 の reserved parser gate を explicit opt-in 成功 path に接続しつつ、T11 allowlist 外の
  FUSE operation を runtime で拒否する。
- 完了条件:
  - `rw,v6_write_mount,no_writeback_cache,no_trim_on_free,bg_dedup_scan=off` だけが v6 controlled write
    admission の成功 path に進み、通常 v6 mount は引き続き拒否される。
  - v6 runtime context は `O_RDWR`、`PROT_READ | PROT_WRITE`、write lock、descriptor-backed bitmap / inode /
    allocator / HRL / journal segment validation、delayed/background disabled policy を満たしている。
  - T11 allowlist 外の truncate/fallocate/unlink/rename/link/symlink/copy/reflink と hotplug delegated write が
    fail closed する regression がある。
  - 許可候補の create/write/fsync/release は v6 専用 guard により tail metadata normalization / reclaim、
    pendinglog drain、`open(O_TRUNC)` を初期範囲外として扱う。
  - `make check -j2` と T11 smoke workload 相当の regression / 手順が closeout に含まれる。
- 実装メモ (2026-06-26):
  - `rw,v6_write_mount,no_writeback_cache,no_trim_on_free,bg_dedup_scan=off` を controlled write admission の
    成功 path に接続した。通常 v6 mount と `-o ro` のみの v6 mount は引き続き offline-only gate で拒否する。
  - controlled write runtime は `O_RDWR` / `PROT_READ | PROT_WRITE` / write lock を使い、descriptor-backed
    bitmap / inode / allocator / HRL / journal segment validation 後に runtime context を保持する。
  - v6 controlled write flag を追加し、truncate/fallocate/unlink/rename/link/symlink/mknod/mkdir/rmdir/chmod/
    chown/utimens、copy/reflink ioctl、control-plane write、`open(O_TRUNC)`、hotplug delegated write を
    `-EOPNOTSUPP` で拒否する。
  - create/write/fsync/release は初期許可面として残し、fsync の tail normalize / pendinglog drain と release の
    tail normalize / tombstone reclaim は v6 controlled write では skip する。
  - copy_file_range syscall は kernel が FUSE high-level op に渡さず通常 read/write fallback で満たす環境がある。
    明示 copy/reflink ioctl と FUSE copy_file_range hook は guard 済みだが、fallback 経由は通常 write と同一視する。

### SDW-V6RT-T13 v6 controlled write durability and fallback hardening

- 目的: T12 で入れた controlled write path を、write failure / fallback / post-write validation の観点で固める。
- 完了条件:
  - zero-filled block write、partial block write、ENOSPC、fsync/fdatasync failure の regression がある。
  - copy_file_range が通常 read/write fallback になる kernel での operator wording と test expectation を文書化する。
  - post-write `fsck.kafs --balanced-check` 失敗時の rollback log/artifact 保存手順を release note / cutover playbook に反映する。
  - `make check -j2` と relevant static gates が PASS している。
- 実装メモ (2026-06-26):
  - `v6_descriptor_smoketest` の controlled write smoke を拡張し、zero-filled block materialization、
    partial block overwrite、`fsync_policy=full` での `fsync` / `fdatasync`、post-write
    `fsck.kafs --balanced-check` を確認する。
  - 8MiB の独立 v6 image で内容の異なる data block を書き続け、HRL dedup に吸収されない ENOSPC regression
    と、ENOSPC 後の unmount / balanced fsck を確認する。
  - `KAFS_TEST_FORCE_FSYNC_ERROR=all` の test-only fault injection を追加し、backing `fsync` / `fdatasync`
    failure が FUSE sync path から `EIO` として返る regression を追加した。
  - copy/reflink ioctl と FUSE copy hook は引き続き拒否する。kernel が `copy_file_range` を通常 read/write
    fallback で満たす場合は、regular write と同じ扱いとして test expectation / operator wording に反映する。
  - release note / cutover playbook に、post-write fsck 失敗時に保存する image、mount log、before/after dump、
    fsck stdout/stderr、実行 workload の証跡を追加した。

### SDW-V6RT-T14 v6 controlled write operator smoke script

- 目的: controlled write acceptance smoke を手順書依存から再現可能な operator script にする。
- 変更:
  - `scripts/` 配下の smoke / artifact capture helper
  - 必要に応じて docs の実行例を script 前提に更新
- 完了条件:
  - before/after `kafsdump --json`、before/after `fsck.kafs --balanced-check`、mount log、image stat/digest、
    regular-file create/write/fsync workload を timestamped report directory に保存できる。
  - mount、workload、unmount、post-write fsck のいずれかが失敗した場合、非 0 exit で artifact を残す。
  - copy/reflink や `cp` を acceptance evidence とせず、明示的な regular-file create/write/fsync を使う。
  - closeout で relevant smoke/test と script/doc 向け static gates が PASS している。
- 実装メモ (2026-07-01):
  - `scripts/v6-controlled-write-smoke.sh` を追加し、既定で `report/v6-controlled-write-smoke/<timestamp>`
    に artifact を保存する。
  - script は `--yes` を必須にする。既定 mount option は
    `rw,v6_write_mount,no_writeback_cache,no_trim_on_free,bg_dedup_scan=off,fsync_policy=full`
    とする。
  - `ro`、`v6_inspection_mount`、`writeback_cache`、`trim_on_free`、`bg_dedup_scan=on`、hotplug、
    `fsync_policy=full` 以外の sync policy は helper 側でも拒否する。
  - workload は script 内に保存される Python helper で regular-file create/write/fsync/fdatasync/readback を
    実行し、`cp` / `copy_file_range` / reflink を acceptance evidence にしない。

### SDW-V6RT-T15 v6 controlled write rejection matrix completion

- 目的: 初期 controlled write の allowlist 外 operation が引き続き fail closed することを regression で固定する。
- 変更:
  - `v6_descriptor_smoketest` の controlled write smoke に rejection matrix を追加
- 完了条件:
  - `mkdir`、`rmdir`、non-regular create、`chmod`、`chown`、`utimens`、`fsyncdir` が
    `-EOPNOTSUPP` で拒否される。
  - 拒否された directory / FIFO path が作成されない。
  - 既存の regular-file create/write/fsync/release 成功 path と post-write `fsck.kafs --balanced-check`
    が維持される。

### SDW-V6RT-T16 v6 controlled write help/man synchronization

- 目的: 実装済みの experimental controlled write opt-in と、operator-facing help / man page の説明を一致させる。
- 変更:
  - `kafs --help` の `--v6-write-mount` / `-o v6_write_mount` 説明から reserved/future 表現を外す。
  - `man/kafs.1` に、accepted option shape、fail-closed 条件、初期 write surface を明記する。
- 完了条件:
  - `--help` と `man/kafs.1` が `rw,v6_write_mount,no_writeback_cache,no_trim_on_free,bg_dedup_scan=off`
    を experimental controlled write の accepted shape として説明する。
  - `ro`、`v6_inspection_mount`、`writeback_cache`、`trim_on_free`、`bg_dedup_scan=on` が
    fail-closed 条件として user-facing docs に出る。
  - 初期許可面が regular-file create/write/fsync/release に限定され、その他 metadata mutation は
    `EOPNOTSUPP` として説明される。
- 実装メモ (2026-07-01):
  - `kafs --help` と `man/kafs.1` を、reserved/future gate ではなく experimental controlled write
    opt-in として説明する形に更新した。
  - `rw,v6_write_mount,no_writeback_cache,no_trim_on_free,bg_dedup_scan=off`、fail-closed 条件、
    regular-file create/write/fsync/release の初期許可面を user-facing text に反映した。

### SDW-V6RT-T17 v6 controlled write operator-doc consistency sweep

- 目的: T14-T16 後の docs を、experimental controlled write は実装済みだが production cutover は
  未解禁という境界にそろえる。
- 変更:
  - `docs/kafsresize-cutover-playbook.md` の v6 destination guidance を、controlled smoke helper は使えるが
    general data-copy / production cutover ではないという表現に更新する。
  - runtime handoff / cutover boundary / write-surface audit に、T14-T16 後の current boundary を追記する。
- 完了条件:
  - 現在状態として「v6 controlled write は未実装」と読める表現を残さない。
  - 現在状態として「production v6 cutover が解禁済み」と読める表現を残さない。
  - acceptance evidence は `scripts/v6-controlled-write-smoke.sh --image <image> --yes` を正規 helper として案内する。
  - docs-only closeout として `git diff --check` と境界文言の `rg` 確認が PASS している。
- 実装メモ (2026-07-01):
  - `kafsresize-cutover-playbook.md` は、v6 destination を non-production descriptor destination とし、
    controlled write helper は explicit acceptance smoke 専用で production data movement ではないと明記した。
  - `sd-card-wear-v6-runtime-handoff-20260626.md`、`sd-card-wear-v6-explicit-write-cutover-boundary.md`、
    `sd-card-wear-v6-fuse-write-surface-audit.md` に、T14-T16 後の current boundary を追記した。
  - T9-T11 の historical note は履歴として残し、現在状態と誤読される `現段階` / future 表現を避けた。

### SDW-V6RT-T18 v6 controlled write pre-production acceptance gate

- 目的: experimental controlled write smoke の artifact が production 前の evidence として揃っていることを
  機械的に確認する。ただし production cutover approval にはしない。
- 変更:
  - `scripts/v6-controlled-write-acceptance-gate.sh` を追加し、既存 smoke helper の実行と既存 report の
    validate-only check をサポートする。
  - `docs/kafsresize-cutover-playbook.md` に acceptance gate の使い方と非 cutover 境界を追記する。
- 完了条件:
  - gate は controlled write mount log、regular-file workload success、before/after `kafsdump --json`、
    before/after `fsck.kafs --balanced-check`、image stat/digest を検証する。
  - gate は workload が copy/reflink evidence に依存していないことを確認する。
  - gate の PASS は production cutover approval ではないと output / docs に明記される。
  - `bash -n`、`shellcheck`、fresh v6 image に対する gate 実行、validate-only rerun が PASS する。

### SDW-V6RT-T19 v6 runtime binary split decision

- 目的: v6 runtime write work を production `kafs` binary の中で広げ続ける方針を止め、専用 v6 runtime
  entrypoint へ分離する方針変更を記録する。
- 決定:
  - 既存 `kafs` は v4/v5 production runtime の入口として維持する。
  - v6 の今後の write runtime admission / user-facing write surface 拡張は、専用 v6 runtime binary または
    front-end の背後で進める。
  - 共通実装は library または Automake common object として共有し、filesystem logic を重複させない。
  - 現在の `kafs` 内 v6 inspection / controlled-write path は、bounded diagnostic / smoke surface として扱い、
    ここへ `mkdir` などの新しい user-facing v6 write operation を追加しない。
- 完了条件:
  - decision record が docs に追加され、handoff / index から参照できる。
  - 次の implementation boundary が「v6 runtime binary split plan」であり、broader `kafs` controlled-write
    expansion ではないと明記される。
- 実装メモ (2026-07-01):
  - `docs/sd-card-wear-v6-runtime-binary-split-decision.md` を追加した。
  - `docs/sd-card-wear-v6-runtime-handoff-20260626.md` の current next boundary を、production acceptance
    gate ではなく v6 runtime binary split に更新した。

### SDW-V6RT-T20 v6 runtime entrypoint skeleton

- 目的: 専用 v6 runtime binary の名前・CLI 契約・初期 fail-closed 境界を固定し、今後の v6 runtime
  admission を production `kafs` binary の拡張ではなく専用入口へ移す準備をする。
- 変更:
  - `kafs-v6` を dedicated format v6 runtime entrypoint skeleton として追加する。
  - `src/Makefile.am` に `kafs-v6` を追加し、build surface を固定する。
  - `docs/sd-card-wear-v6-runtime-entrypoint-plan.md` に CLI 契約、移動対象、shared implementation
    boundary、smoke を記録する。
- 完了条件:
  - `kafs-v6 --help` が専用 v6 entrypoint の CLI 契約を表示する。
  - `kafs-v6` は `--inspection-mount` / `--controlled-write-mount` の明示 mode を要求し、legacy
    `v6_inspection_mount` / `v6_write_mount` token を拒否する。
  - T20 skeleton は v6 image format と mount option shape を検証したうえで、実 mount 前に fail closed
    する。
  - `kafs` の v4/v5 production entrypoint behavior を広げない。
  - `make -j2` と CLI help surface smoke が PASS している。
- 実装メモ (2026-07-02):
  - `kafs-v6` を追加し、inspection / controlled-write の明示 mode と mount option shape を検証する
    fail-closed skeleton とした。
  - `kafs-v6` は format v6 image を確認した後も実 mount へ進まず、次 slice で admission code を移す
    入口だけを固定する。
  - `docs/sd-card-wear-v6-runtime-entrypoint-plan.md` に CLI 契約、移動対象、shared implementation
    boundary、T20 smoke を記録した。

### SDW-V6RT-T21 v6 runtime admission extraction phase 1

- 目的: v6 runtime admission の option / image-format validation を専用 helper へ切り出し、`kafs-v6`
  と production `kafs` entrypoint が同じ request model を使えるようにする。
- 変更:
  - `kafs_v6_runtime_request_t` と validation reason を `src/kafs_v6_runtime.h` に追加する。
  - `kafs-v6` の CLI shape / image-format validation を `src/kafs_v6_runtime.c` 経由にする。
  - production `kafs` の既存 v6 inspection / controlled-write option validation は user-facing error
    wording を維持したまま、同じ helper を使う。
- 完了条件:
  - `kafs-v6` の T20 fail-closed behavior が維持される。
  - `v6_descriptor_smoketest` の既存 v6 admission / rejection matrix が PASS する。
  - `kafs` の v4/v5 production runtime behavior を広げない。
  - `make -j2` と CLI surface smoke が PASS している。

### SDW-V6RT-T22 v6 descriptor/journal preflight extraction

- 目的: v6 descriptor / journal segment preflight を `kafs-v6` と production `kafs` の共有 helper
  へ移し、専用 entrypoint 側で format check より深い admission evidence を取れるようにする。
- 変更:
  - `kafs_v6_runtime_admission_preflight_fd()` を追加し、descriptor mapping admission と journal segment
    validation を実行する。
  - `kafs_v6_runtime_admission_preflight_image()` を追加し、`kafs-v6` が image format check 後に同じ
    preflight を実行する。
  - production `kafs` の既存 offline-only preflight 表示は維持し、v4/v5 runtime behavior は変更しない。
- 完了条件:
  - valid v6 image に対する `kafs-v6` が descriptor / journal preflight 成功後に mount 前 fail closed
    する。
  - corrupt v6 descriptor に対する `kafs-v6` が preflight failure で exit 2 になる。
  - `v6_descriptor_smoketest` の既存 v6 admission / rejection matrix が PASS する。
  - `make -j2` と CLI surface smoke が PASS している。

### SDW-V6RT-T23 v6 cutover preparation policy reset

- 目的: v6 production cutover 前の開発方針を再固定し、v6 は後方互換を約束せず純粋な v6 target
  design を優先すること、最終 runtime binary は `kafs` / `kafs-v6` の分離であることを明記する。
- 変更:
  - `docs/sd-card-wear-v6-cutover-preparation.md` を追加し、v6 cutover 前の互換性ポリシー、最終
    binary 境界、許可する `.o` / `.a` / `.so` 共有境界を記録する。
  - v5 互換都合で v6 実装が歪む箇所を棚卸しし、v6-native direction を明文化する。
  - `kafs_v6_runtime.c` を production `kafs` / `kafsctl` / `kafs-back` の link surface から外し、
    dedicated `kafs-v6` entrypoint 側の helper として扱う。
- 完了条件:
  - `kafs` / `kafs-v6` は最終生成物として分離し、共有は user-facing binary ではなく `.o` / `.a` /
    `.so` などの implementation artifact で行うと docs に明記されている。
  - v6 cutover 前は v6 format / feature の drastic change を許可する一方、v4/v5 compatibility は
    維持する方針が記録されている。
  - v5 互換制約に引っ張られている v6 実装領域と v6-native direction が一覧化されている。
  - `make -j2`、`v6_descriptor_smoketest`、CLI surface smoke が PASS している。
- 実装メモ (2026-07-02):
  - `docs/sd-card-wear-v6-cutover-preparation.md` を追加し、v6 cutover 前の互換性ポリシー、v6-native
    direction、最終 binary / shared artifact 境界を記録した。
  - `kafs_v6_runtime.c` は `kafs-v6` だけへ link し、production `kafs` / `kafsctl` / `kafs-back` から
    外した。legacy `kafs` の v6 診断 surface は既存挙動を維持するためローカル検査として残した。
  - `make -j2`、`make -C tests check TESTS=v6_descriptor_smoketest`、`./scripts/test-cli-surface.sh`、
    `make check -j2` が PASS した。

### SDW-V6RT-T24 v6 shared artifact boundary plan

- 目的: `kafs-v6` へ runtime setup を移す前に、最終 binary 境界と `.o` / `.a` / `.so`
  implementation artifact 境界を具体化し、user-facing helper binary を増やさずに共有する範囲を固定する。
- 変更:
  - `docs/sd-card-wear-v6-shared-artifact-boundary-plan.md` を追加し、product boundary、current link
    surface、artifact classes、次の implementation boundary を記録する。
  - `kafs_v6_runtime.c` は引き続き `kafs-v6` 専用 link とし、production `kafs` には戻さない方針を明記する。
  - shared pure metadata helper、shared runtime mechanics、v6-only runtime policy、legacy `kafs`
    diagnostic surface を分類する。
- 完了条件:
  - 共有は user-facing binary ではなく common object list、non-installed static archive、将来の shared
    library として扱う方針が文書化されている。
  - T25 の境界が `kafs-v6` read-only inspection admission migration であり、controlled-write expansion
    ではないと明記されている。
  - `git diff --check`、`./scripts/test-cli-surface.sh`、`make -j2` が PASS している。
- 実装メモ (2026-07-02):
  - `docs/sd-card-wear-v6-shared-artifact-boundary-plan.md` を追加し、product boundary、current link
    surface、artifact classes、T25 境界を固定した。
  - 現時点では Autotools archive support を追加せず、common source/object list または non-installed
    archive を次 slice で選択する方針にした。
  - `./scripts/format.sh`、`git diff --check`、`./scripts/test-cli-surface.sh`、`make -j2` が PASS した。

### SDW-V6RT-T25 kafs-v6 inspection admission migration

- 目的: read-only v6 inspection admission を production `kafs` の successful runtime path から外し、
  `kafs-v6 --inspection-mount` を v6 acceptance の入口にする。
- 変更:
  - `kafs-v6` を `KAFS_V6_ENTRYPOINT` 付きで `kafs.c` / HRL / journal / RPC の common object set
    に link し、read-only inspection mount だけを shared FUSE runtime bridge へ渡す。
  - `kafs-v6` は legacy `v6_inspection_mount` / `v6_write_mount` token を拒否し、valid
    `--inspection-mount -o ro` では descriptor / journal preflight 後に read-only FUSE mount へ進む。
  - production `kafs` の legacy `v6_inspection_mount` は successful path から外し、`kafs-v6`
    への案内で fail-closed にする。controlled-write legacy path は今回の scope 外として残す。
  - `v6_descriptor_smoketest` の read-only mount smoke を `kafs-v6` 起動に切り替え、legacy `kafs`
    inspection token の fail-closed guidance を regression にする。
- 完了条件:
  - `kafs-v6 --inspection-mount ... -o ro` が v6 read-only inspection acceptance を所有する。
  - `kafs -o v6_inspection_mount` は successful mount path に入らず `kafs-v6` guidance を出す。
  - controlled-write admission は拡張せず、既存 legacy path の isolation は次 slice に残す。
  - `./scripts/format.sh`、`git diff --check`、`./scripts/test-cli-surface.sh`、`make -j2`、
    `make -C tests check TESTS=v6_descriptor_smoketest`、`make check -j2` が PASS している。
- 実装メモ (2026-07-02):
  - `src/Makefile.am` で `kafs-v6` に common object set を追加し、`KAFS_V6_ENTRYPOINT` で bridge
    compile 対象を dedicated entrypoint に限定した。
  - `src/kafs_v6.c` は FUSE passthrough 引数を保持し、inspection mode だけ shared bridge に進める。
    controlled-write mode は引き続き fail-closed。
  - `src/kafs.c` の legacy inspection token は validation 前に `kafs-v6` guidance で拒否する。
  - `make check -j2` は all 29 tests passed で完了した。

### SDW-V6RT-T26 kafs-v6 controlled-write admission isolation

- 目的: existing controlled-write admission を production `kafs` の successful runtime path から外し、
  `kafs-v6 --controlled-write-mount` を v6 write acceptance の入口にする。これは isolation slice であり、
  write surface expansion ではない。
- 変更:
  - `kafs-v6 --controlled-write-mount` は descriptor / journal preflight 後に shared FUSE runtime bridge
    へ進み、既存の conservative controlled-write policy shape だけを許可する。
  - production `kafs` の legacy `v6_write_mount` は successful path から外し、`kafs-v6`
    への案内で fail-closed にする。
  - `v6_descriptor_smoketest` の controlled-write mount / ENOSPC / fsync-failure smoke を `kafs-v6`
    起動に切り替え、legacy `kafs` write token の fail-closed guidance を regression にする。
  - operator smoke helper は `kafs-v6 --controlled-write-mount` を使い、legacy `v6_write_mount`
    mount option を拒否する。
- 完了条件:
  - `kafs-v6 --controlled-write-mount ... -o
    rw,no_writeback_cache,no_trim_on_free,bg_dedup_scan=off,fsync_policy=full` が v6
    controlled-write acceptance を所有する。
  - `kafs -o v6_write_mount` は successful mount path に入らず `kafs-v6` guidance を出す。
  - controlled-write write surface は既存の regular-file create/write/fsync/release 範囲から広げない。
  - `./scripts/format.sh`、`git diff --check`、`./scripts/test-cli-surface.sh`、`make -j2`、
    `make -C tests check TESTS=v6_descriptor_smoketest`、`make check -j2` が PASS している。
- 実装メモ (2026-07-02):
  - `src/kafs.c` の `KAFS_V6_ENTRYPOINT` bridge を inspection / controlled-write 共通にし、
    `kafs-v6 --controlled-write-mount` から既存 controlled write runtime setup へ進むようにした。
  - production `kafs` の legacy `v6_write_mount` は validation 前に `kafs-v6` guidance で拒否する。
  - `scripts/v6-controlled-write-smoke.sh` / acceptance gate は `kafs-v6 --controlled-write-mount`
    を正規 mount command とし、legacy `v6_write_mount` mount option を拒否する。
  - `make check -j2` は all 26 tests passed、3 tests not run で完了した。

### SDW-V6RT-T27 kafs-v6 runtime context opener pureification

- 目的: `kafs-v6` の successful runtime path が production `kafs` の v4/v5 汎用
  `kafs_main_open_runtime_context()` 分岐に依存しないようにし、v6 専用 open/read/admit/init sequence
  へ切り分ける。これは pureification slice であり、write surface expansion ではない。
- 変更:
  - `KAFS_V6_ENTRYPOINT` bridge に v6 専用 runtime context opener を追加し、image open、superblock
    read、format v6 check、descriptor-backed admission、diag/journal init、runtime lock を dedicated
    entrypoint sequence として実行する。
  - `kafs-v6` bridge から `kafs_main_open_runtime_context()` 呼び出しを外し、legacy `kafs` の v6
    offline/preflight/handoff branch と successful `kafs-v6` mount path を分離する。
  - `kafs_main_init_context()` は fd 初期値を `-1` にし、dedicated opener の failure path が stdin を
    誤って close しないようにする。
- 完了条件:
  - `kafs-v6 --inspection-mount` / `--controlled-write-mount` の acceptance behavior が T25/T26 と同じ。
  - production `kafs` の v4/v5 runtime と legacy v6 fail-closed guidance は維持されている。
  - `./scripts/format.sh`、`git diff --check`、`./scripts/test-cli-surface.sh`、`make -j2`、
    `make -C tests check TESTS=v6_descriptor_smoketest`、`make check -j2` が PASS している。
- 実装メモ (2026-07-02):
  - `kafs-v6` bridge から `kafs_main_open_runtime_context()` 呼び出しを外し、
    `kafs_v6_entrypoint_open_runtime_context()` で v6 専用の open/read/admit/init sequence を実行するようにした。
  - inspection は read-only open + descriptor-backed admission + diag init、controlled-write は read-write open
    + descriptor-backed admission + diag/journal init を dedicated entrypoint helper で分離した。
  - `make check -j2` は all 29 tests passed で完了した。

### SDW-V6RT-T28 v6 descriptor-backed runtime view pureification phase 1

- 目的: `kafs-v6` の successful runtime path が v5-style contiguous metadata table view を持たず、
  descriptor-backed bitmap / inode / allocator / HRL view を runtime view の所有者として使うことを固定する。
  これは pureification slice であり、write surface expansion ではない。
- 変更:
  - v6 admission 用 mmap 初期化では image 全体と superblock だけを設定し、`c_blkmasktbl` /
    `c_inotbl` / `c_mapsize` を legacy contiguous view として初期化しない。
  - successful v6 admission 後に descriptor-backed bitmap / inode / allocator / HRL mapping が揃い、
    legacy contiguous inode/bitmap table pointer が未設定であることを runtime invariant として検査する。
  - v6 context では journal meta-delta bitmap overlay を起動せず、contiguous `c_blkmasktbl` への依存を
    controlled-write path へ戻さない。
  - `v6_descriptor_smoketest` は `KAFS_V6_ADMISSION_HANDOFF` と `kafs-v6` mount log で、
    descriptor-backed runtime view が active で legacy contiguous table が未設定であることを確認する。
- 完了条件:
  - `kafs-v6 --inspection-mount` / `--controlled-write-mount` の T25/T26 acceptance behavior が維持される。
  - production `kafs` の v4/v5 runtime と legacy v6 fail-closed guidance は維持されている。
  - controlled-write write surface は既存の regular-file create/write/fsync/release 範囲から広げない。
  - `./scripts/format.sh fix`、`git diff --check`、`./scripts/test-cli-surface.sh`、`make -j2`、
    `make -C tests check TESTS=v6_descriptor_smoketest`、`make check -j2` が PASS している。
- 実装メモ (2026-07-03):
  - `kafs_main_map_v6_runtime_admission_memory()` は v6 admission で image / superblock だけを mmap
    view とし、legacy contiguous `c_blkmasktbl` / `c_inotbl` / `c_mapsize` を設定しないようにした。
  - `kafs_main_v6_validate_runtime_views()` を追加し、successful v6 admission が descriptor-backed
    runtime view だけを持つことを fail-closed invariant にした。
  - v6 context では journal meta-delta bitmap overlay を無効化し、contiguous bitmap table 前提の
    optimization を v6 controlled-write path から外した。
  - `v6_descriptor_smoketest` で handoff output と `kafs-v6` mount log の descriptor-backed runtime
    view guidance を regression にした。
  - `make check -j2` は all 25 tests passed、4 tests not run で完了した。

### SDW-V6RT-T29 v6 controlled-write runtime worker-policy pureification

- 目的: `kafs-v6 --controlled-write-mount` の successful runtime path が generic v5 worker
  assumptions を継承せず、pending log drain、tombstone GC、background dedup worker、hotplug
  delegated write が sealed disabled policy のまま残ることを runtime invariant として固定する。
  これは pureification slice であり、write surface expansion ではない。
- 変更:
  - v6 admission 後に `pending_worker=disabled`、`tombstone_gc_worker=disabled`、
    `bg_dedup_worker=disabled`、`hotplug=disabled` を `kafs_main_v6_validate_worker_policy()` で
    fail-closed 検査する。
  - controlled-write path は journal init 後にも同じ worker-policy invariant を再検査し、
    generic runtime init が pending/background worker state を再導入していないことを確認する。
  - FUSE init の v6 branch は generic worker start path に入らず、policy invariant が崩れている場合は
    error log を出して delayed/background workers を抑止したままにする。
  - `v6_descriptor_smoketest` は handoff output と `kafs-v6` mount log で
    `v6 worker policy sealed` guidance を確認する。
- 完了条件:
  - `kafs-v6 --inspection-mount` / `--controlled-write-mount` の T25/T26 acceptance behavior が維持される。
  - controlled-write write surface は既存の regular-file create/write/fsync/release 範囲から広げない。
  - `./scripts/format.sh fix`、`git diff --check`、`./scripts/test-cli-surface.sh`、`make -j2`、
    `make -C tests check TESTS=v6_descriptor_smoketest`、`make check -j2` が PASS している。
- 実装メモ (2026-07-03):
  - `kafs_main_v6_validate_worker_policy()` を追加し、v6 admission と controlled-write journal init 後に
    pending worker / tombstone GC worker / background dedup worker / hotplug delegation が disabled
    のまま残ることを検査するようにした。
  - `kafs_op_init()` は v6 context で generic worker start path に入らず、worker-policy invariant の
    異常を error log として記録する。
  - `v6_descriptor_smoketest` で handoff output と `kafs-v6` mount log の
    `v6 worker policy sealed` guidance を regression にした。
  - `make check -j2` は all 27 tests passed、2 tests not run で完了した。SKIP になった
    `min_git_hooks` / `fs_semantics` は
    `KAFS_TEST_MOUNT_TIMEOUT_MS=15000 make -C tests check TESTS='min_git_hooks fs_semantics'` で
    all 2 tests passed を確認した。

### SDW-V6RT-T30 v6 runtime context open helper extraction

- 目的: `kafs-v6` の successful runtime path に残っていた image open / superblock read /
  magic / format version validation を `kafs_v6_runtime.c` 側の v6 runtime helper へ移し、
  `KAFS_V6_ENTRYPOINT` bridge をさらに薄くする。これは pureification slice であり、write surface
  expansion ではない。
- 変更:
  - `kafs_v6_runtime_open_context_image()` を追加し、inspection / controlled-write mode に応じた
    open flag、`c_fd` 設定、search cursor 初期化、superblock read、invalid magic / non-v6 format
    failure cleanup を v6 runtime helper に集約する。
  - `kafs-v6` bridge は local read-superblock helper を持たず、v6 runtime helper の結果だけを
    descriptor-backed admission と diag/journal init へ渡す。
  - production `kafs` の v4/v5 runtime と legacy v6 fail-closed guidance は変更しない。
- 完了条件:
  - `kafs-v6 --inspection-mount` / `--controlled-write-mount` の T25/T26 acceptance behavior が維持される。
  - controlled-write write surface は既存の regular-file create/write/fsync/release 範囲から広げない。
  - `./scripts/format.sh fix`、`git diff --check`、`./scripts/test-cli-surface.sh`、`make -j2`、
    `make -C tests check TESTS=v6_descriptor_smoketest`、`make check -j2` が PASS している。
- 実装メモ (2026-07-03):
  - `src/kafs_v6_runtime.c` に `kafs_v6_runtime_open_context_image()` を追加し、open/read-superblock
    failure path で fd を閉じて `ctx->c_fd = -1` に戻すようにした。
  - read-only format check / admission preflight の open/read-superblock 処理も
    `kafs_v6_runtime_open_readonly_superblock()` に寄せ、T30 の helper 追加で重複が増えないようにした。
  - `src/kafs.c` の `KAFS_V6_ENTRYPOINT` bridge から local `kafs_v6_entrypoint_read_superblock()` を
    削除し、v6 専用 open helper を呼ぶ形にした。
  - `KAFS_TEST_MOUNT_TIMEOUT_MS=15000 make check -j2` は all 29 tests passed で完了した。
  - `./scripts/static-checks.sh` は format / lint / clones / complexity を完了した。strict source
    clone report は 36 clones / duplicated lines 365 (1.00%) で threshold 内、test clone report は
    informational として生成された。

### SDW-V6RT-T31 v6 runtime admission/service helper extraction

- 目的: `kafs-v6` の successful runtime path に残っていた descriptor-backed admission /
  mmap / mode state / diag / journal service setup を `kafs_v6_runtime.c` 側へ移し、
  `KAFS_V6_ENTRYPOINT` bridge をさらに薄くする。これは pureification slice であり、
  write surface expansion ではない。
- 変更:
  - descriptor-backed preflight / runtime admission core を `kafs_v6_admission.h` に集約し、
    `kafs-v6` と production `kafs` の diagnostic-only path が同じ検証実装を共有するようにした。
  - v6 admission mmap、descriptor-backed runtime view validation、delayed/background mutation
    suppression、worker-policy seal を `kafs_context.h` の reusable invariant helper にした。
  - `kafs_v6_runtime_admit_mount_context()` を追加し、`kafs-v6` の descriptor-backed admission、
    full-image mmap mode、journal segment validation、inspection / controlled-write mode state、
    admission log を v6 runtime helper に集約した。
  - `kafs_v6_runtime_init_mount_services()` を追加し、`kafs-v6` の diag setup と
    controlled-write journal service init、post-init invariant validation を v6 runtime helper に集約した。
  - production `kafs` は引き続き `kafs_v6_runtime.c` を link しない。legacy v6 diagnostic
    scaffolding は shared context helper を使うが、successful v6 runtime admission にはならない。
- 完了条件:
  - `kafs-v6 --inspection-mount` / `--controlled-write-mount` の T25/T26 acceptance behavior が維持される。
  - production `kafs` の v4/v5 runtime と legacy v6 fail-closed guidance は維持されている。
  - controlled-write write surface は既存の regular-file create/write/fsync/release 範囲から広げない。
  - `./scripts/format.sh fix`、`git diff --check`、`./scripts/test-cli-surface.sh`、`make -j2`、
    `make -C tests check TESTS=v6_descriptor_smoketest`、`make check -j2` が PASS している。
- 実装メモ (2026-07-03):
  - `src/kafs_v6_runtime.c` に `kafs_v6_runtime_admit_mount_context()` と
    `kafs_v6_runtime_init_mount_services()` を追加し、`kafs-v6` bridge は open/admit/init helper の
    結果から image lock と FUSE 起動へ進むだけになった。
  - `src/kafs_v6_admission.h` に descriptor-backed preflight / runtime admission core を追加し、
    legacy production `kafs` diagnostic path と `kafs-v6` helper の重複を避けた。
  - `src/kafs_context.h` の v6 invariant helper は production `kafs` の diagnostic-only path と
    `kafs-v6` helper の両方から使う。これにより production `kafs` へ `kafs_v6_runtime.c` link を
    追加せず、message / invariant を重複させない。
  - `src/Makefile.am` の `noinst_HEADERS` に `kafs_v6_admission.h` を追加し、
    `autoreconf -fi` / `./configure` で生成物を更新した。
  - `./scripts/static-checks.sh` は format / lint / clones / complexity を完了した。strict source
    clone report は 35 clones / duplicated lines 345 (0.94%) で threshold 内、test clone report は
    informational として生成された。
  - `KAFS_TEST_MOUNT_TIMEOUT_MS=15000 make check -j2` は all 28 tests passed、1 test not run で
    完了した。SKIP は `stress_fs` で、ログ上は FUSE mount failure によるもの。

### SDW-V6RT-T32 v6 entrypoint request policy reporter extraction

- 目的: `KAFS_V6_ENTRYPOINT` bridge に残っていた `kafs-v6` 固有の inspection /
  controlled-write mount option policy check と rejection wording を `kafs_v6_runtime.c`
  側の request helper へ移し、standalone `kafs-v6` parser と bridge の admission policy を
  同じ `kafs_v6_runtime_request_t` contract に揃える。これは pureification slice であり、
  write surface expansion ではない。
- 変更:
  - `kafs_v6_runtime_print_validation_error()` と
    `kafs_v6_runtime_report_entrypoint_request()` を追加し、`kafs-v6` runtime request の
    validation と rejection wording を v6 runtime helper に集約する。
  - `src/kafs_v6.c` の static rejection reporter を削除し、standalone `kafs-v6` parser は
    `kafs_v6_runtime_report_entrypoint_request()` を呼ぶ。
  - `src/kafs.c` の `KAFS_V6_ENTRYPOINT` bridge は filtered mount options から
    `kafs_v6_runtime_request_t` を組み立て、v6 runtime helper で検証する。
  - bridge から local inspection / controlled-write option policy check を削除した。production
    `kafs` の legacy v6 validation / fail-closed guidance は引き続き `kafs.c` に残す。
- 完了条件:
  - `kafs-v6 --inspection-mount` / `--controlled-write-mount` の T25/T26 acceptance behavior が維持される。
  - invalid `kafs-v6` request の rejection wording は既存 smoke 期待を維持する。
  - controlled-write write surface は既存の regular-file create/write/fsync/release 範囲から広げない。
  - `./scripts/format.sh fix`、`git diff --check`、`./scripts/test-cli-surface.sh`、`make -j2`、
    `make -C tests check TESTS=v6_descriptor_smoketest`、`make check -j2` が PASS している。
- 実装結果:
  - `kafs-v6` standalone parser と `KAFS_V6_ENTRYPOINT` bridge は、同じ
    `kafs_v6_runtime_report_entrypoint_request()` 経由で runtime request を検証する。
  - bridge の local inspection / controlled-write option policy check は削除した。
  - production `kafs` の legacy v6 fail-closed validation は `kafs.c` に残り、write surface は広げていない。
- 検証:
  - `./scripts/format.sh fix`、`make -j2`、`make -C tests check TESTS=v6_descriptor_smoketest`、
    `git diff --check`、`./scripts/test-cli-surface.sh`、`./scripts/clones.sh`、
    `./scripts/static-checks.sh`、`KAFS_TEST_MOUNT_TIMEOUT_MS=15000 make check -j2` が成功した。
  - clone strict source gate は 36 clones / 353 duplicated lines / 0.96% で閾値内だった。
  - `make check` は all 28 tests passed、1 test not run。SKIP は `stress_fs` で、
    ログ上は FUSE mount failure によるもの。

### SDW-V6RT-T33 v6 FUSE bridge API narrowing

- 目的: `KAFS_V6_ENTRYPOINT` common-object bridge の外部APIと FUSE 実行境界を少し狭め、
  `kafs_v6_runtime.h` を v6 runtime helper contract に戻す。これは bridge pureification slice であり、
  write surface expansion ではない。
- 変更:
  - `kafs_v6_inspection_mount_main()` と `kafs_v6_controlled_write_mount_main()` の宣言を
    `kafs_v6_runtime.h` から新規 `kafs_v6_entrypoint_adapter.h` へ移す。
  - `src/kafs.c` の production `kafs` と `KAFS_V6_ENTRYPOINT` bridge が直接 `fuse_main()` と
    `kafs_operations` を触る重複を `kafs_main_run_fuse()` に集約する。
  - `src/Makefile.am` の `noinst_HEADERS` に bridge header を追加し、`autoreconf -fi` /
    `./configure` でローカル生成状態を更新する。
- 完了条件:
  - `kafs-v6` は bridge header 経由で mount bridge API だけを参照し、`kafs_v6_runtime.h` は
    runtime request / admission helper surface に集中している。
  - production `kafs` と `kafs-v6` の FUSE operation table は同じままだが、FUSE 実行呼び出しは
    shared helper 経由になっている。
  - controlled-write write surface は既存の regular-file create/write/fsync/release 範囲から広げない。
  - `autoreconf -fi`、`./configure`、`make -j2`、`git diff --check`、
    `./scripts/test-cli-surface.sh`、`make -C tests check TESTS=v6_descriptor_smoketest`、
    `make check -j2` が PASS している。
- 実装結果:
  - `src/kafs_v6_entrypoint_adapter.h` を追加し、`kafs-v6` parser は bridge entrypoint API をこのヘッダから参照する。
  - `kafs_v6_runtime.h` から bridge entrypoint 宣言を削除し、runtime helper contract に集中させた。
  - `src/kafs.c` の production main と `KAFS_V6_ENTRYPOINT` bridge は `kafs_main_run_fuse()` 経由で
    `fuse_main()` と cleanup path を共有する。
- 検証:
  - `./scripts/format.sh fix`、`autoreconf -fi`、`./configure`、`make -j2`、`git diff --check`、
    `./scripts/test-cli-surface.sh`、`make -C tests check TESTS=v6_descriptor_smoketest`、
    `./scripts/static-checks.sh`、`KAFS_TEST_MOUNT_TIMEOUT_MS=15000 make check -j2` が成功した。
  - clone strict source gate は 36 clones / 353 duplicated lines / 0.96% で閾値内だった。
  - `make check` は all 29 tests passed で完了した。

### SDW-V6RT-T34 v6 controlled-write FUSE policy guard extraction

- 目的: `src/kafs.c` の shared FUSE operation 実装に埋まっている v6 controlled-write
  allowlist-out rejection policy を専用ヘルパ境界へ移し、operation 実装と v6 policy の境界を
  明示する。これは bridge pureification slice であり、write surface expansion ではない。
- 変更:
  - `src/kafs_v6_fuse_policy.h` を追加し、`kafs_v6_controlled_write_active()` と
    `kafs_v6_controlled_write_reject()` を移す。
  - `src/kafs.c` は v6 controlled-write policy guard を header 経由で参照し、FUSE operation
    実装側には既存の guard 呼び出しだけを残す。
  - `src/Makefile.am` の `noinst_HEADERS` に policy header を追加し、`autoreconf -fi` /
    `./configure` でローカル生成状態を更新する。
- 完了条件:
  - controlled-write write surface は既存の regular-file create/write/fsync/release 範囲から広げない。
  - `kafs-v6` の rejection wording と v6 descriptor smoke behavior は維持される。
  - `autoreconf -fi`、`./configure`、`make -j2`、`git diff --check`、
    `./scripts/test-cli-surface.sh`、`make -C tests check TESTS=v6_descriptor_smoketest`、
    `make check -j2` が PASS している。
- 実装結果:
  - `src/kafs_v6_fuse_policy.h` を追加し、v6 controlled-write active check と rejection helper を移した。
  - `src/kafs.c` の FUSE operation 実装は既存の `kafs_v6_controlled_write_active()` /
    `kafs_v6_controlled_write_reject()` 呼び出しを維持し、policy 定義だけを helper header へ分離した。
  - controlled-write write surface は広げていない。
- 検証:
  - `./scripts/format.sh fix`、`autoreconf -fi`、`./configure`、`make -j2`、`git diff --check`、
    `./scripts/test-cli-surface.sh`、`make -C tests check TESTS=v6_descriptor_smoketest`、
    `./scripts/static-checks.sh`、`KAFS_TEST_MOUNT_TIMEOUT_MS=15000 make check -j2` が成功した。
  - clone strict source gate は 36 clones / 353 duplicated lines / 0.96% で閾値内だった。
  - `make check` は all 29 tests passed で完了した。

### SDW-V6RT-T35 v6 controlled-write FUSE entry gate helper extraction

- 目的: T34 で作った `kafs_v6_fuse_policy.h` 境界を、shared FUSE operation の入口 gate
  まで広げる。これは operation-helper extraction slice であり、write surface expansion ではない。
- 変更:
  - `kafs_v6_controlled_write_reject_if()` を追加し、condition-gated rejection を policy
    helper 側へ寄せる。
  - `kafs_v6_controlled_write_require_regular_write()` を追加し、controlled-write の
    regular-file-only write check を helper 側へ寄せる。
  - `src/kafs.c` の control-plane open/write、`open(O_TRUNC)`、hotplug delegated write、
    non-regular write の gate を helper 呼び出しに置き換える。
- 完了条件:
  - controlled-write write surface は既存の regular-file create/write/fsync/release 範囲から広げない。
  - shared FUSE operation 実装と operation table は `src/kafs.c` に残る。
  - `make -j2`、`git diff --check`、`./scripts/test-cli-surface.sh`、
    `make -C tests check TESTS=v6_descriptor_smoketest`、`./scripts/static-checks.sh`、
    `KAFS_TEST_MOUNT_TIMEOUT_MS=15000 make check -j2` が PASS している。
- 実装結果:
  - `src/kafs_v6_fuse_policy.h` に condition-gated rejection helper と regular-file-only
    write helper を追加した。
  - `src/kafs.c` の FUSE operation 入口は v6 policy condition だけを渡し、active 判定と
    v6 allowlist-out rejection は helper 側へ寄せた。
  - controlled-write write surface は広げていない。
- 検証:
  - `./scripts/format.sh fix`、`make -j2`、`git diff --check`、
    `./scripts/test-cli-surface.sh`、`make -C tests check TESTS=v6_descriptor_smoketest`、
    `./scripts/static-checks.sh`、`KAFS_TEST_MOUNT_TIMEOUT_MS=15000 make check -j2` が成功した。
  - clone strict source gate は 36 clones / 353 duplicated lines / 0.96% で閾値内だった。
  - `make check` は all 29 tests passed で完了した。

### SDW-V6RT-T36 v6 controlled-write data-layout policy helper extraction

- 目的: shared write/fsync/release path に残る v6 controlled-write active 判定を
  data-layout policy 名の helper へ移し、`src/kafs.c` が raw active flag を直接読まない
  境界にする。これは operation-helper extraction slice であり、write surface expansion ではない。
- 変更:
  - `kafs_v6_controlled_write_preserve_zero_block()` を追加し、controlled-write で
    zero-filled block を sparse release せず materialize する policy を明示する。
  - `kafs_v6_controlled_write_skip_tail_layout()` を追加し、write/fsync path の v5 tail
    metadata optimization / normalization bypass を明示する。
  - `kafs_v6_controlled_write_skip_release_reclaim()` を追加し、release path の reclaim bypass
    を明示する。
  - `kafs_v6_controlled_write_use_local_write_path()` を追加し、controlled-write の local write
    fallback selection を明示する。
- 完了条件:
  - `src/kafs.c` は shared FUSE operation 実装と operation table を保持するが、
    `kafs_v6_controlled_write_active()` を直接呼ばない。
  - controlled-write write surface は既存の regular-file create/write/fsync/release 範囲から広げない。
  - `make -j2`、`git diff --check`、`./scripts/test-cli-surface.sh`、
    `make -C tests check TESTS=v6_descriptor_smoketest`、`./scripts/static-checks.sh`、
    `KAFS_TEST_MOUNT_TIMEOUT_MS=15000 make check -j2` が PASS している。
- 実装結果:
  - `src/kafs_v6_fuse_policy.h` に data-layout policy helper を追加した。
  - `src/kafs.c` の pwrite zero-block handling、tail layout preparation/sync、write path
    selection、fsync preparation、release reclaim bypass を helper 呼び出しへ置換した。
  - controlled-write write surface は広げていない。
- 検証:
  - `./scripts/format.sh fix`、`make -j2`、`git diff --check`、
    `./scripts/test-cli-surface.sh`、`make -C tests check TESTS=v6_descriptor_smoketest`、
    `./scripts/static-checks.sh`、`KAFS_TEST_MOUNT_TIMEOUT_MS=15000 make check -j2` が成功した。
  - clone strict source gate は 36 clones / 353 duplicated lines / 0.96% で閾値内だった。
  - `make check` は all 29 tests passed で完了した。

### SDW-V6RT-T37 v6 controlled-write rejected-operation vocabulary extraction

- 目的: `src/kafs.c` の shared FUSE operation 実装に散っている controlled-write
  rejection operation 文字列を `kafs_v6_fuse_policy.h` 側の語彙に移す。これは
  operation-helper extraction slice であり、write surface expansion ではない。
- 変更:
  - `kafs_v6_controlled_write_op_t` を追加し、reflink / copy / truncate / unlink など
    rejected operation id を v6 policy helper 側に集約する。
  - `kafs_v6_controlled_write_op_name()`、`kafs_v6_controlled_write_reject_op()`、
    `kafs_v6_controlled_write_reject_if_op()` を追加する。
  - `src/kafs.c` の controlled-write rejection は free-form string ではなく enum value を
    policy helper に渡す。
  - 旧 `kafs_v6_controlled_write_reject_if()` は header surface から外す。
- 完了条件:
  - `src/kafs.c` は shared FUSE operation 実装と operation table を保持するが、
    controlled-write rejection wording を直接所有しない。
  - controlled-write write surface は既存の regular-file create/write/fsync/release 範囲から広げない。
  - `make -j2`、`git diff --check`、`./scripts/test-cli-surface.sh`、
    `make -C tests check TESTS=v6_descriptor_smoketest`、`./scripts/static-checks.sh`、
    `KAFS_TEST_MOUNT_TIMEOUT_MS=15000 make check -j2` が PASS している。
- 実装結果:
  - `src/kafs_v6_fuse_policy.h` に rejected-operation enum と名前変換 helper を追加した。
  - `src/kafs.c` の direct rejection は enum-based helper 呼び出しへ置換した。
  - controlled-write write surface は広げていない。
- 検証:
  - `./scripts/format.sh fix`、`make -j2`、`git diff --check`、
    `./scripts/test-cli-surface.sh`、`make -C tests check TESTS=v6_descriptor_smoketest`、
    `./scripts/static-checks.sh`、`KAFS_TEST_MOUNT_TIMEOUT_MS=15000 make check -j2` が成功した。
  - clone strict source gate は 36 clones / 353 duplicated lines / 0.96% で閾値内だった。
  - `make check` は all 29 tests passed で完了した。

### SDW-V6RT-T38 v6 entrypoint adapter request/open helper extraction

- 目的: `KAFS_V6_ENTRYPOINT` adapter path に残っていた v6 entrypoint 固有の
  runtime request 検証と open/admit/init sequence を `kafs_v6_entrypoint_adapter`
  側へ移す。これは common-object adapter pureification slice であり、write surface
  expansion ではない。
- 変更:
  - `src/kafs_v6_entrypoint_adapter.c` を追加し、`kafs_v6_entrypoint_adapter_validate_options()` と
    `kafs_v6_entrypoint_adapter_open_context()` を実装する。
  - `src/kafs_v6_entrypoint_adapter.h` に adapter-local mode enum と option summary 構造体を追加する。
  - `src/kafs.c` の `KAFS_V6_ENTRYPOINT` ブロックは private parser result を adapter option
    summary へ写し、FUSE argv assembly / shared FUSE runner / image lock を保持する。
  - `src/Makefile.am` の `kafs-v6` source set に `kafs_v6_entrypoint_adapter.c` を追加する。
- 完了条件:
  - `kafs.c` は `kafs_v6_runtime.h` を直接 include せず、entrypoint adapter header は runtime helper
    header を再公開しない。v6 entrypoint request validation と open/admit/init sequence は
    entrypoint adapter 経由になる。
  - shared FUSE operation implementation と operation table は `kafs.c` に残す。
  - controlled-write write surface は既存の regular-file create/write/fsync/release 範囲から広げない。
  - `./scripts/format.sh fix`、`autoreconf -fi`、`./configure`、`make -j2`、`git diff --check`、
    `./scripts/test-cli-surface.sh`、`make -C tests check TESTS=v6_descriptor_smoketest`、
    `./scripts/static-checks.sh`、`KAFS_TEST_MOUNT_TIMEOUT_MS=15000 make check -j2` が PASS
    している。
- 実装結果:
  - `kafs_v6_entrypoint_adapter.c` が v6 entrypoint runtime request 検証、adapter-local mode から
    runtime mode への変換、v6 context open/admit/init sequence を所有するようにした。
  - `kafs.c` の `KAFS_V6_ENTRYPOINT` adapter path は generic mount option parser、FUSE argv
    assembly、runtime image lock、shared FUSE runner を保持するだけになった。
  - controlled-write write surface は広げていない。
- 検証:
  - `./scripts/format.sh fix`、`autoreconf -fi`、`./configure`、`make -j2`、
    `git diff --check`、`./scripts/test-cli-surface.sh`、
    `make -C tests check TESTS=v6_descriptor_smoketest`、`./scripts/static-checks.sh`、
    `KAFS_TEST_MOUNT_TIMEOUT_MS=15000 make check -j2` が成功した。
  - clone strict source gate は 36 clones / 353 duplicated lines / 0.95% で閾値内だった。
  - `make check` は all 29 tests passed で完了した。

### SDW-V6RT-T39 v6 entrypoint adapter naming and source ownership clarification

- 目的: `v6 bridge` という共通名に見える表現を現行責務に合わせて明確化し、v5/v6
  互換 layer ではなく `kafs-v6` 専用 entrypoint adapter であることを source 名と
  docs から読めるようにする。これは boundary clarification slice であり、write
  surface expansion ではない。
- 変更:
  - `src/kafs_v6_mount_bridge.c` / `.h` を `src/kafs_v6_entrypoint_adapter.c` / `.h`
    に改名する。
  - `kafs_v6_mount_bridge_*` / `KAFS_V6_MOUNT_BRIDGE_*` を
    `kafs_v6_entrypoint_adapter_*` / `KAFS_V6_ENTRYPOINT_ADAPTER_*` に改名する。
  - `src/Makefile.am` に runtime source ownership コメントを追加し、production
    `kafs`、dedicated `kafs-v6`、v6-only entrypoint adapter の link boundary を明記する。
  - `docs/sd-card-wear-v6-shared-artifact-boundary-plan.md` に source ownership map を追加し、
    common-looking file names の位置づけを artifact class / link target / owns / must not own
    で明示する。
  - runtime entrypoint plan / handoff / tickets の現行説明を `bridge` ではなく
    `entrypoint adapter` に更新する。過去チケットの旧称は履歴として残す。
- 完了条件:
  - `kafs_v6_entrypoint_adapter.*` は v6-only internal adapter として説明され、v5/v6
    compatibility layer や user-facing helper ではないことが明記されている。
  - production `kafs` は引き続き `kafs_v6_runtime.c` を link しない。
  - shared FUSE operation implementation と operation table は `src/kafs.c` に残す。
  - controlled-write write surface は既存の regular-file create/write/fsync/release 範囲から広げない。
  - `./scripts/format.sh fix`、`autoreconf -fi`、`./configure`、`make -j2`、`git diff --check`、
    `./scripts/test-cli-surface.sh`、`make -C tests check TESTS=v6_descriptor_smoketest`、
    `./scripts/static-checks.sh`、`KAFS_TEST_MOUNT_TIMEOUT_MS=15000 make check -j2` が PASS
    している。
- 実装結果:
  - `kafs-v6` の common-object handoff 名を entrypoint adapter に統一した。
  - `src/Makefile.am` と v6 shared artifact boundary plan に source ownership を追記した。
  - write surface と production `kafs` の v6 fail-closed boundary は変更していない。
- 検証:
  - `./scripts/format.sh fix`、`autoreconf -fi`、`./configure`、`make -j2`、
    `git diff --check`、`./scripts/test-cli-surface.sh`、
    `make -C tests check TESTS=v6_descriptor_smoketest`、`./scripts/static-checks.sh`、
    `KAFS_TEST_MOUNT_TIMEOUT_MS=15000 make check -j2` が成功した。

### SDW-V6RT-T40 v6 mount-main ownership split

- 目的: `kafs_v6_entrypoint_adapter.h` が公開する v6 mount entrypoint API の実装本体を
  `src/kafs.c` から `src/kafs_v6_entrypoint_adapter.c` へ移し、`src/kafs.c` を shared FUSE
  runner / operation table / cleanup path に寄せる。これは common-object adapter
  pureification slice であり、write surface expansion ではない。
- 変更:
  - `src/kafs_v6.c` は validation 済み `kafs_v6_runtime_request_t` から
    `kafs_v6_entrypoint_adapter_options_t` を組み立て、
    `kafs_v6_entrypoint_adapter_mount_main()` を呼ぶ。
  - `src/kafs_v6_entrypoint_adapter.c` は v6-only FUSE option filtering、context
    initialization、image lock、FUSE argv assembly、runtime option handoff、
    open/admit/init sequence を所有する。
  - `src/kafs.c` の `KAFS_V6_ENTRYPOINT` ブロックは
    `kafs_v6_entrypoint_adapter_run_shared_fuse()` だけを公開し、
    `fuse_main()` / `kafs_operations` / cleanup path への接続だけを保持する。
  - `kafs_v6_inspection_mount_main()` /
    `kafs_v6_controlled_write_mount_main()` の旧 API を adapter-local mount main へ統合する。
- 完了条件:
  - `kafs.c` の `KAFS_V6_ENTRYPOINT` ブロックは v6 mount option parser、v6 mount-main
    preparation、open/admit/init sequence を所有しない。
  - `kafs_v6_entrypoint_adapter.c` は `kafs-v6` request state から shared FUSE runner
    へ渡すまでの v6-only handoff を所有する。
  - shared FUSE operation implementation と operation table は `src/kafs.c` に残す。
  - controlled-write write surface は既存の regular-file create/write/fsync/release 範囲から広げない。
  - `./scripts/format.sh fix`、`make -j2`、`git diff --check`、
    `./scripts/test-cli-surface.sh`、`make -C tests check TESTS=v6_descriptor_smoketest`、
    `./scripts/static-checks.sh`、`KAFS_TEST_MOUNT_TIMEOUT_MS=15000 make check -j2` が PASS
    している。
- 実装結果:
  - `src/kafs.c` の v6 mount-main body を削除し、shared FUSE runner wrapper だけを残した。
  - `src/kafs_v6_entrypoint_adapter.c` に v6-only FUSE option filter / FUSE argv builder /
    context initializer / image lock / mount-main body を移した。
  - `src/kafs_v6.c` は runtime request を adapter options へ変換し、single adapter mount API
    経由で mount path へ進む。
  - write surface と production `kafs` の v6 fail-closed boundary は変更していない。
- 検証:
  - `./scripts/format.sh fix`、`make -j2`、`git diff --check`、
    `./scripts/test-cli-surface.sh`、`make -C tests check TESTS=v6_descriptor_smoketest`、
    `./scripts/static-checks.sh`、`KAFS_TEST_MOUNT_TIMEOUT_MS=15000 make check -j2` が成功した。

### SDW-V6RT-T41 v6 mount option policy helper split

- 目的: T40 で `src/kafs_v6_entrypoint_adapter.c` に寄せた v6 mount-main から `-o` token の意味付けを
  分離し、`kafs-v6` CLI と adapter が同じ v6-only option policy helper を参照する形にする。
  これは option interpretation ownership split であり、write surface expansion ではない。
- 変更:
  - `src/kafs_v6_mount_options.[ch]` を追加し、v6-owned `-o` token vocabulary、
    runtime request への記録、FUSE passthrough filter、`multi_thread` / `max_threads`
    handoff state を所有させる。
  - `src/kafs_v6.c` は `-o` list の構文分割と CLI mode / image / mountpoint 選択を続けて所有し、
    token ごとの admission state update は `kafs_v6_mount_options_record_runtime_token()` へ委譲する。
  - `src/kafs_v6_entrypoint_adapter.c` は mount-main orchestration、context initialization、image lock、
    FUSE argv assembly、shared runner handoff に集中し、kafs-owned token vocabulary を持たない。
  - `src/Makefile.am` の source ownership comment と `kafs-v6` link set に v6 mount option helper を追加する。
- 完了条件:
  - `kafs_v6.c` と `kafs_v6_entrypoint_adapter.c` が同じ `-o` token 群を別々に再実装しない。
  - FUSE に渡す token と adapter が消費する token の境界が helper API 名で分かる。
  - controlled-write write surface は既存の regular-file create/write/fsync/release 範囲から広げない。
  - `./scripts/format.sh fix`、`autoreconf -fi`、`./configure`、`make -j2`、
    `git diff --check`、`./scripts/test-cli-surface.sh`、
    `make -C tests check TESTS=v6_descriptor_smoketest`、`./scripts/static-checks.sh`、
    `KAFS_TEST_MOUNT_TIMEOUT_MS=15000 make check -j2` が PASS している。
- 実装結果:
  - `src/kafs_v6_mount_options.[ch]` に v6 mount option policy を切り出した。
  - `src/kafs_v6.c` から admission token の if-chain を削除し、runtime request 記録を helper に寄せた。
  - `src/kafs_v6_entrypoint_adapter.c` から FUSE option filtering / internal token 判定の if-chain を削除し、
    helper の filter result と thread handoff state だけを使うようにした。
  - write surface と production `kafs` の v6 fail-closed boundary は変更していない。
- 検証:
  - `./scripts/format.sh fix`、`autoreconf -fi`、`./configure`、`make -j2`、
    `git diff --check`、`./scripts/test-cli-surface.sh`、
    `make -C tests check TESTS=v6_descriptor_smoketest`、`./scripts/static-checks.sh`、
    `KAFS_TEST_MOUNT_TIMEOUT_MS=15000 make check -j2` が成功した。

### SDW-V6RT-T42 shared FUSE runner API split

- 目的: T40/T41 後も `kafs_v6_entrypoint_adapter.h` に残っていた shared FUSE runner hook を
  adapter API から分離し、`kafs.c` が実装する共通 FUSE 実行境界を専用ヘッダで表す。
  これは common-object adapter pureification slice であり、write surface expansion ではない。
- 変更:
  - `src/kafs_shared_fuse_runner.h` を追加し、shared FUSE runner handoff と runtime option
    summary を所有させる。
  - `src/kafs_v6_entrypoint_adapter.h` から shared runner option struct と
    `kafs_v6_entrypoint_adapter_run_shared_fuse()` 宣言を削除し、adapter header を
    v6 entrypoint adapter API に集中させる。
  - `src/kafs_v6_entrypoint_adapter.c` は `kafs_shared_fuse_run()` を呼び、shared runner
    hook の名前空間を adapter から切り離す。
  - `src/kafs.c` の `KAFS_V6_ENTRYPOINT` ブロックは `kafs_shared_fuse_run()` を実装し、
    `fuse_main()` / `kafs_operations` / cleanup path への接続を保持する。
- 完了条件:
  - `kafs_v6_entrypoint_adapter.h` は `kafs-v6` mount-main adapter API だけを公開し、
    `kafs.c` 実装の shared runner hook を所有しない。
  - shared FUSE operation implementation と operation table は `src/kafs.c` に残す。
  - controlled-write write surface は既存の regular-file create/write/fsync/release 範囲から広げない。
  - `./scripts/format.sh fix`、`autoreconf -fi`、`./configure`、`make -j2`、
    `git diff --check`、`./scripts/test-cli-surface.sh`、
    `make -C tests check TESTS=v6_descriptor_smoketest`、`./scripts/static-checks.sh`、
    `KAFS_TEST_MOUNT_TIMEOUT_MS=15000 make check -j2` が PASS している。
- 実装結果:
  - `src/kafs_shared_fuse_runner.h` に shared runner handoff と runtime option summary を
    切り出した。
  - `src/kafs_v6_entrypoint_adapter.h` は adapter-local mode/options と mount-main API だけを
    公開するようにした。
  - `src/kafs.c` は `kafs_shared_fuse_run()` だけを v6 adapter 向けに公開し、shared FUSE
    operation implementation と operation table は引き続き `src/kafs.c` に残した。
  - write surface と production `kafs` の v6 fail-closed boundary は変更していない。
- 検証:
  - `./scripts/format.sh fix`、`autoreconf -fi`、`./configure`、`make -j2`、
    `git diff --check`、`./scripts/test-cli-surface.sh`、
    `make -C tests check TESTS=v6_descriptor_smoketest`、`./scripts/static-checks.sh`、
    `KAFS_TEST_MOUNT_TIMEOUT_MS=15000 make check -j2` が成功した。

### SDW-V6RT-T43 v6 FUSE init worker-policy helper split

- 目的: shared FUSE operation の `kafs_op_init()` に残っている format-v6 worker-policy 判定を
  v6 FUSE init policy helper へ分離する。これは operation-helper extraction slice であり、
  write surface expansion ではない。
- 変更:
  - `src/kafs_v6_fuse_init_policy.h` を追加し、format-v6 runtime mount では delayed /
    background mutation workers を起動しない判定と診断を所有させる。
  - `src/kafs.c` の `kafs_op_init()` は v6 判定と worker policy validation を
    `kafs_v6_fuse_init_suppresses_background_workers()` へ委譲する。
  - `src/Makefile.am` の `noinst_HEADERS` と source ownership docs に v6 FUSE init policy
    helper を追加する。
- 完了条件:
  - `kafs_op_init()` は shared FUSE operation implementation として `src/kafs.c` に残るが、
    format-v6 固有の background worker suppression policy を直接所有しない。
  - v4/v5 worker startup policy と shared FUSE operation table は `src/kafs.c` に残す。
  - controlled-write write surface は既存の regular-file create/write/fsync/release 範囲から広げない。
  - `./scripts/format.sh fix`、`autoreconf -fi`、`./configure`、`make -j2`、
    `git diff --check`、`./scripts/test-cli-surface.sh`、
    `make -C tests check TESTS=v6_descriptor_smoketest`、`./scripts/static-checks.sh`、
    `KAFS_TEST_MOUNT_TIMEOUT_MS=15000 make check -j2` が PASS している。
- 実装結果:
  - `src/kafs_v6_fuse_init_policy.h` に v6 FUSE init worker-policy helper を追加した。
  - `src/kafs.c` の `kafs_op_init()` から format-v6 superblock 判定と
    `kafs_ctx_v6_validate_worker_policy()` の直接呼び出しを削除した。
  - shared FUSE operation implementation と operation table は引き続き `src/kafs.c` に残した。
  - write surface と production `kafs` の v6 fail-closed boundary は変更していない。
- 検証:
  - `./scripts/format.sh fix`、`autoreconf -fi`、`./configure`、`make -j2`、
    `git diff --check`、`./scripts/test-cli-surface.sh`、
    `make -C tests check TESTS=v6_descriptor_smoketest`、`./scripts/static-checks.sh`、
    `KAFS_TEST_MOUNT_TIMEOUT_MS=15000 make check -j2` が成功した。

### SDW-V6RT-T44 legacy v6 fail-closed helper split

- 目的: production `kafs` に残す legacy v6 token の fail-closed guidance を専用 helper
  に分離し、successful v6 runtime admission は引き続き `kafs-v6` が所有することを明確にする。
  これは legacy diagnostic ownership split であり、write surface expansion ではない。
- 変更:
  - `src/kafs_legacy_v6_failclosed.h` を追加し、legacy `--v6-inspection-mount` /
    `--v6-write-mount` / `-o v6_inspection_mount` / `-o v6_write_mount` の token 分類と
    `kafs-v6` guidance を所有させる。
  - `src/kafs.c` は legacy v6 request flag を保持し、token 判定と fail-closed message は
    helper 呼び出しへ委譲する。
  - `src/Makefile.am` の source ownership comment と `noinst_HEADERS` に helper を追加する。
- 完了条件:
  - production `kafs` は legacy v6 token を成功 mount path に通さず、同じ `kafs-v6`
    guidance で fail closed する。
  - production `kafs` は引き続き `kafs_v6_runtime.c` を link しない。
  - controlled-write write surface は既存の regular-file create/write/fsync/release 範囲から広げない。
  - `./scripts/format.sh fix`、`autoreconf -fi`、`./configure`、`make -j2`、
    `git diff --check`、`./scripts/test-cli-surface.sh`、
    `make -C tests check TESTS=v6_descriptor_smoketest`、`./scripts/static-checks.sh`、
    `KAFS_TEST_MOUNT_TIMEOUT_MS=15000 make check -j2` が PASS している。
- 実装結果:
  - `src/kafs_legacy_v6_failclosed.h` に legacy v6 token 分類と guidance 出力を切り出した。
  - `src/kafs.c` の CLI flag / `-o` token handling は helper の分類結果を request flag へ写すだけにした。
  - `src/kafs.c` の main path は helper 経由で legacy v6 request を拒否する。
  - write surface と production `kafs` の v6 fail-closed boundary は変更していない。
- 検証:
  - `./scripts/format.sh fix`、`autoreconf -fi`、`./configure`、`make -j2`、
    `git diff --check`、`./scripts/test-cli-surface.sh`、
    `make -C tests check TESTS=v6_descriptor_smoketest`、`./scripts/static-checks.sh`、
    `KAFS_TEST_MOUNT_TIMEOUT_MS=15000 make check -j2` が成功した。

### SDW-V6RT-T45 shared FUSE operation table boundary naming

- 目的: `kafs.c` に残る shared FUSE operation table / runner 経路を、v6 adapter ではなく
  shared FUSE 実装境界として読める名前に寄せる。これは common-object adapter surface の
  命名整理であり、filesystem operation duplication や write surface expansion ではない。
- 変更:
  - `kafs.c` の direct `!KAFS_NO_MAIN || KAFS_V6_ENTRYPOINT` 条件を
    `KAFS_COMPILE_SHARED_FUSE_OPERATIONS` に集約する。
  - `kafs_operations` を `kafs_shared_fuse_operation_table` に改名し、
    `kafs_shared_fuse_operations()` 経由で `fuse_main()` へ渡す。
  - production `kafs` と `kafs-v6` は同じ shared FUSE operation table を使い続ける。
- 完了条件:
  - `KAFS_V6_ENTRYPOINT` は `kafs.c` 内で shared FUSE operation table を直接名付けない。
  - `kafs-v6` の successful v6 runtime admission は引き続き `kafs_v6_runtime.c` /
    `kafs_v6_entrypoint_adapter.c` が所有する。
  - production `kafs` の v6 fail-closed boundary と controlled-write write surface は変更しない。
  - `./scripts/format.sh fix`、`autoreconf -fi`、`./configure`、`make -j2`、
    `git diff --check`、`./scripts/test-cli-surface.sh`、
    `make -C tests check TESTS=v6_descriptor_smoketest`、`./scripts/static-checks.sh`、
    `KAFS_TEST_MOUNT_TIMEOUT_MS=15000 make check -j2` が PASS している。
- 実装結果:
  - `KAFS_COMPILE_SHARED_FUSE_OPERATIONS` を `kafs.c` 内の shared table/runner compile guard
    として追加した。
  - shared operation table を `kafs_shared_fuse_operation_table` として明示し、
    local accessor から `fuse_main()` へ渡すようにした。
  - runtime admission、legacy fail-closed guidance、write-surface policy は変更していない。
- 検証:
  - `./scripts/format.sh fix`、`autoreconf -fi`、`./configure`、`make -j2`、
    `git diff --check`、`./scripts/test-cli-surface.sh`、
    `make -C tests check TESTS=v6_descriptor_smoketest`、`./scripts/static-checks.sh`、
    `KAFS_TEST_MOUNT_TIMEOUT_MS=15000 make check -j2` が成功した。

### SDW-V6RT-T46 shared FUSE runner export guard split

- 目的: `kafs.c` に残る shared runner export の compile guard を v6 entrypoint 名から分離し、
  `kafs_shared_fuse_run()` の所有境界を shared FUSE runner export として明示する。これは
  common-object adapter surface の命名整理であり、v6 admission ownership や write surface は
  変更しない。
- 変更:
  - `src/Makefile.am` の `kafs-v6` target は `KAFS_V6_ENTRYPOINT` ではなく
    `KAFS_SHARED_FUSE_RUNNER_EXPORT` を定義する。
  - `src/kafs.c` の shared operation compile guard は
    `!KAFS_NO_MAIN || KAFS_SHARED_FUSE_RUNNER_EXPORT` を条件にする。
  - `src/kafs.c` の `kafs_shared_fuse_run()` export guard を
    `KAFS_SHARED_FUSE_RUNNER_EXPORT` にする。
  - `src/kafs_v6_entrypoint_adapter.h` の説明を新しい export guard 名へ更新する。
- 完了条件:
  - production `kafs` は `kafs_v6_runtime.c` を link しない。
  - `kafs-v6` は dedicated entrypoint のまま、shared FUSE runner export だけを `kafs.c` から受け取る。
  - controlled-write write surface は既存の regular-file create/write/fsync/release 範囲から広げない。
  - `./scripts/format.sh fix`、`autoreconf -fi`、`./configure`、`make -j2`、
    `git diff --check`、`./scripts/test-cli-surface.sh`、
    `make -C tests check TESTS=v6_descriptor_smoketest`、`./scripts/static-checks.sh`、
    `KAFS_TEST_MOUNT_TIMEOUT_MS=15000 make check -j2` が PASS している。
- 実装結果:
  - `KAFS_SHARED_FUSE_RUNNER_EXPORT` を `kafs-v6` target 専用の shared runner export guard にした。
  - `kafs.c` の実コードから `KAFS_V6_ENTRYPOINT` guard を外し、shared FUSE runner export 名に置き換えた。
  - runtime admission、legacy fail-closed guidance、write-surface policy は変更していない。
- 検証:
  - `./scripts/format.sh fix`、`autoreconf -fi`、`./configure`、`make -j2`、
    `git diff --check`、`./scripts/test-cli-surface.sh`、
    `make -C tests check TESTS=v6_descriptor_smoketest`、`./scripts/static-checks.sh`、
    `KAFS_TEST_MOUNT_TIMEOUT_MS=15000 make check -j2` が成功した。

### SDW-V6RT-T47 shared FUSE local runner naming split

- 目的: production `kafs` main と `kafs-v6` shared runner export が共用する local
  `fuse_main()` / cleanup helper を production main 名から分離し、shared FUSE runner
  実装として読める名前に寄せる。これは命名境界の整理であり、FUSE operation duplication や
  write surface expansion ではない。
- 変更:
  - `src/kafs.c` の local `kafs_main_run_fuse()` を
    `kafs_shared_fuse_run_with_cleanup()` に改名する。
  - production `kafs` main と `kafs_shared_fuse_run()` export は同じ local helper を呼び続ける。
- 完了条件:
  - production `kafs` と `kafs-v6` は同じ shared FUSE operation table / cleanup path を使う。
  - `kafs-v6` の successful v6 runtime admission は引き続き `kafs_v6_runtime.c` /
    `kafs_v6_entrypoint_adapter.c` が所有する。
  - controlled-write write surface は既存の regular-file create/write/fsync/release 範囲から広げない。
  - `./scripts/format.sh fix`、`autoreconf -fi`、`./configure`、`make -j2`、
    `git diff --check`、`./scripts/test-cli-surface.sh`、
    `make -C tests check TESTS=v6_descriptor_smoketest`、`./scripts/static-checks.sh`、
    `KAFS_TEST_MOUNT_TIMEOUT_MS=15000 make check -j2` が PASS している。
- 実装結果:
  - `kafs.c` の local runner helper 名を shared FUSE runner / cleanup ownership に合わせた。
  - `kafs_shared_fuse_run()` の exported handoff contract と production `kafs` main の挙動は変更していない。
  - runtime admission、legacy fail-closed guidance、write-surface policy は変更していない。
- 検証:
  - `./scripts/format.sh fix`、`autoreconf -fi`、`./configure`、`make -j2`、
    `git diff --check`、`./scripts/test-cli-surface.sh`、
    `make -C tests check TESTS=v6_descriptor_smoketest`、`./scripts/static-checks.sh`、
    `KAFS_TEST_MOUNT_TIMEOUT_MS=15000 make check -j2` が成功した。

### SDW-V6RT-T48 kafs-v6 unsupported KAFS option rejection

- 目的: T41 で v6 専用 `-o` token helper へ分離した後、`kafs-v6` が解釈しない
  production `kafs` 固有 tuning token を黙って strip しないようにする。これは option
  interpretation ownership correction であり、write surface expansion ではない。
- 変更:
  - `src/kafs_v6_mount_options.c` は v6 admission vocabulary と production-only KAFS
    tuning vocabulary を分ける。
  - `sd_card_profile=*`、pending worker / TTL / capacity tuning、bg-dedup interval /
    threshold / worker tuning など、v6 が context に反映しない KAFS 固有 token は明示的に
    reject する。
  - v6 admission token、`multi_thread` / `max_threads` handoff、FUSE passthrough option の
    既存経路は維持する。
  - `tests/tests_v6_descriptor_smoketest.c` に unsupported KAFS option の rejection 回帰を追加する。
- 完了条件:
  - `kafs-v6 --inspection-mount -o ro,sd_card_profile=conservative` は mount 続行せず、
    unsupported KAFS mount option として exit 2 になる。
  - `kafs-v6` の controlled-write write surface は既存の regular-file
    create/write/fsync/release 範囲から広げない。
  - `make -j2`、`git diff --check`、`./scripts/test-cli-surface.sh`、
    `make -C tests check TESTS=v6_descriptor_smoketest` が PASS している。
- 実装結果:
  - `kafs_v6_mount_options` の internal token 判定を v6 admission token と
    unsupported production-only KAFS token に分けた。
  - production-only KAFS tuning token は FUSE passthrough から黙って消さず、
    `unsupported KAFS mount option` として exit 2 にする。
  - `no_dedup_scan` / `no-dedup-scan` は bg-dedup off alias として v6 admission token
    vocabulary に揃えた。
- 検証:
  - `./scripts/format.sh fix`、`make -j2`、`git diff --check`、
    `./scripts/test-cli-surface.sh`、`make -C tests check TESTS=v6_descriptor_smoketest`、
    `./scripts/static-checks.sh`、`KAFS_TEST_MOUNT_TIMEOUT_MS=15000 make check -j2` が成功した。
  - clone strict source gate は 35 clones / 345 duplicated lines / 0.91% で閾値内だった。
  - `make check` は all 29 tests passed で完了した。

### SDW-V6RT-T49 shared FUSE cleanup helper boundary naming

- 目的: production `kafs` main と `kafs-v6` shared runner export が共用する cleanup
  helper を production main 名から分離し、shared FUSE runner の post-`fuse_main()`
  cleanup boundary として読める名前に寄せる。これは命名境界の整理であり、FUSE
  operation duplication や write surface expansion ではない。
- 変更:
  - `src/kafs.c` の local `kafs_main_cleanup()` を
    `kafs_shared_fuse_cleanup_after_run()` に改名する。
  - `kafs_shared_fuse_run_with_cleanup()` は同じ cleanup sequence を呼び続ける。
  - production `kafs` main と `kafs-v6` shared runner export は引き続き同じ
    shared FUSE operation table / cleanup path を使う。
- 完了条件:
  - `lsp-cli` の rename dry-run / references で、cleanup helper の rename 対象が
    定義と shared runner からの呼び出しに閉じていることを確認する。
  - `src/kafs.c` の cleanup helper 名が production main 所有ではなく shared FUSE runner
    cleanup 所有として読める。
  - controlled-write write surface は既存の regular-file create/write/fsync/release
    範囲から広げない。
  - `./scripts/format.sh fix`、`make -j2`、`git diff --check`、
    `./scripts/test-cli-surface.sh`、`make -C tests check TESTS=v6_descriptor_smoketest`、
    `./scripts/static-checks.sh`、`KAFS_TEST_MOUNT_TIMEOUT_MS=15000 make check -j2` が PASS
    している。
- 実装結果:
  - `lsp-cli` の rename dry-run で `kafs_main_cleanup()` の参照が定義と呼び出しの
    2 箇所だけであることを確認し、`kafs_shared_fuse_cleanup_after_run()` に semantic
    rename した。
  - cleanup sequence、shared FUSE operation table、production `kafs` main、
    `kafs-v6` shared runner export の呼び出し構造は維持した。
  - runtime admission、legacy fail-closed guidance、write-surface policy は変更していない。
- 検証:
  - `./scripts/format.sh fix`、`make -j2`、`git diff --check`、
    `./scripts/test-cli-surface.sh`、`make -C tests check TESTS=v6_descriptor_smoketest`、
    `./scripts/static-checks.sh`、`KAFS_TEST_MOUNT_TIMEOUT_MS=15000 make check -j2` が成功した。
  - `lsp-cli --root . --server clangd --format pretty references src/kafs.c 13420 11` は
    definition と `kafs_shared_fuse_run_with_cleanup()` からの call site の 2 箇所を返した。
  - clone strict source gate は 35 clones / 345 duplicated lines / 0.91% で閾値内だった。
  - `make check` は all 29 tests passed で完了した。

### SDW-V6RT-T50 shared FUSE runtime option logger boundary naming

- 目的: production `kafs` main と `kafs-v6` shared runner export が共用する runtime
  option logging helper を production main 名から分離し、shared FUSE runner の
  pre-`fuse_main()` runtime option boundary として読める名前に寄せる。これは命名境界の
  整理であり、runtime option semantics や write surface expansion ではない。
- 変更:
  - `src/kafs.c` の local `kafs_main_log_runtime_options()` を
    `kafs_shared_fuse_log_runtime_options()` に改名する。
  - production `kafs` main と `kafs-v6` shared runner export は引き続き同じ runtime
    option logging helper を呼ぶ。
  - logging content、writeback cache / trim state handoff、FUSE argv dump behavior は変更しない。
- 完了条件:
  - `lsp-cli` の rename dry-run / references で active compile branch の rename 対象を
    確認し、preprocessor inactive branch は `rg` で残参照を確認する。
  - `src/kafs.c` の runtime option logging helper 名が production main 所有ではなく
    shared FUSE runner runtime option 所有として読める。
  - controlled-write write surface は既存の regular-file create/write/fsync/release
    範囲から広げない。
  - `./scripts/format.sh fix`、`make -j2`、`git diff --check`、
    `./scripts/test-cli-surface.sh`、`make -C tests check TESTS=v6_descriptor_smoketest`、
    `./scripts/static-checks.sh`、`KAFS_TEST_MOUNT_TIMEOUT_MS=15000 make check -j2` が PASS
    している。
- 実装結果:
  - `lsp-cli` の rename dry-run は active production compile branch の定義と
    production main call site を返した。
  - `KAFS_SHARED_FUSE_RUNNER_EXPORT` 側は compile database 上の inactive branch だったため、
    `rg` で残参照を確認して同じ `kafs_shared_fuse_log_runtime_options()` 呼び出しへ揃えた。
  - runtime option logging content、shared FUSE operation table、cleanup path、
    write-surface policy は変更していない。
- 検証:
  - `./scripts/format.sh fix`、`make -j2`、`git diff --check`、
    `./scripts/test-cli-surface.sh`、`make -C tests check TESTS=v6_descriptor_smoketest`、
    `./scripts/static-checks.sh`、`KAFS_TEST_MOUNT_TIMEOUT_MS=15000 make check -j2` が成功した。
  - `lsp-cli --root . --server clangd --format pretty references src/kafs.c 12981 12` は
    active compile branch の definition と production main call site の 2 箇所を返した。
  - `rg` は `kafs_shared_fuse_log_runtime_options()` の definition、`kafs-v6`
    shared runner export call site、production main call site の 3 箇所を返し、旧名は残らなかった。
  - clone strict source gate は 35 clones / 345 duplicated lines / 0.91% で閾値内だった。
  - `make check` は all 29 tests passed で完了した。

### SDW-V6RT-T51 shared FUSE runtime compile guard naming

- 目的: T45 で導入した local compile guard `KAFS_COMPILE_SHARED_FUSE_OPERATIONS` が
  operation table だけでなく `kafs_shared_fuse_run_with_cleanup()` も guard しているため、
  shared FUSE runtime boundary として読める `KAFS_COMPILE_SHARED_FUSE_RUNTIME` に改名する。
  これは compile guard の命名整理であり、runtime admission ownership や write surface は
  変更しない。
- 変更:
  - `src/kafs.c` の local shared table/runner compile guard を
    `KAFS_COMPILE_SHARED_FUSE_RUNTIME` に改名する。
  - production `kafs` main と `kafs-v6` shared runner export は引き続き同じ shared
    FUSE operation table / cleanup path を使う。
  - historical ticket text では旧名が T45 の導入名だったことを残し、現行仕様は T51 の
    新名として記録する。
- 完了条件:
  - `lsp-cli` references/hover と `rg` で compile guard の定義と `#ifdef` 範囲を確認する。
  - `src/kafs.c` の guard 名が operation-table-only ではなく shared FUSE runtime 所有として読める。
  - controlled-write write surface は既存の regular-file create/write/fsync/release
    範囲から広げない。
  - `./scripts/format.sh fix`、`make -j2`、`git diff --check`、
    `./scripts/test-cli-surface.sh`、`make -C tests check TESTS=v6_descriptor_smoketest`、
    `./scripts/static-checks.sh`、`KAFS_TEST_MOUNT_TIMEOUT_MS=15000 make check -j2` が PASS
    している。
- 実装結果:
  - `lsp-cli` references/hover で macro definition と shared operation table /
    local shared runner helper の `#ifdef` sites を確認した。
  - `lsp-cli rename` は macro kind unsupported だったため、`rg` で全参照を確認して
    現行コードと current-boundary docs を `KAFS_COMPILE_SHARED_FUSE_RUNTIME` に揃えた。
  - runtime admission、legacy fail-closed guidance、write-surface policy は変更していない。
- 検証:
  - `./scripts/format.sh fix`、`make -j2`、`git diff --check`、
    `./scripts/test-cli-surface.sh`、`make -C tests check TESTS=v6_descriptor_smoketest`、
    `./scripts/static-checks.sh`、`KAFS_TEST_MOUNT_TIMEOUT_MS=15000 make check -j2` が成功した。
  - `lsp-cli --root . --server clangd --format pretty references src/kafs.c 51 8` は
    macro definition と shared table / local shared runner guard の 3 箇所を返した。
  - clone strict source gate は 35 clones / 345 duplicated lines / 0.91% で閾値内だった。
  - `make check` は all 29 tests passed で完了した。

### SDW-V6RT-T52 production kafs legacy v6 diagnostic scaffolding inventory

- 目的: production `kafs` に残る format-v6 diagnostic/admission scaffolding を棚卸しし、
  `kafs-v6` が所有する successful v6 runtime admission と、production `kafs` が保持する
  legacy fail-closed / diagnostic guidance を混同しないようにする。これは inventory slice であり、
  runtime admission ownership や write surface は変更しない。
- 変更:
  - `docs/sd-card-wear-v6-production-diagnostic-scaffolding-inventory.md` を追加し、
    retain / diagnostic-only / retirement-candidate の 3 分類で production `kafs` の v6 surface を記録する。
  - `KAFS_V6_ADMISSION_HANDOFF` は現行 test coverage がある diagnostic gate として残す。
  - `KAFS_V6_READONLY_SMOKE` は現行 operator/test path が `kafs-v6 --inspection-mount` へ移っているため、
    次の retirement candidate として記録する。
  - legacy `v6_inspection_mount` / `v6_write_mount` の successful branch は production `main()` の
    fail-closed gate で遮断されている historical leftover として記録し、今回の slice では削除しない。
- 完了条件:
  - `lsp-cli` references と `rg` で production `kafs` 側の v6 diagnostic helper call scope を確認する。
  - `kafs-v6` の successful v6 admission ownership と production `kafs` の legacy fail-closed guidance が
    docs 上で分かれている。
  - controlled-write write surface は既存の regular-file create/write/fsync/release 範囲から広げない。
  - `git diff --check`、`./scripts/test-cli-surface.sh`、
    `make -C tests check TESTS=v6_descriptor_smoketest` が PASS している。
- 実装結果:
  - production `kafs` の retain surface は `kafs_legacy_v6_failclosed.h` と plain-v6 offline-only
    preflight/guidance に限定して記録した。
  - diagnostic-only surface は `KAFS_V6_ADMISSION_HANDOFF` と `KAFS_V6_READONLY_SMOKE` に分け、
    latter を次の retirement candidate とした。
  - legacy-token successful branches は、production `main()` の reject gate より後ろに残る
    unreachable/historical branch として次の reduction candidate にした。
- 検証:
  - `lsp-cli` references は production v6 diagnostic helpers が定義と単一 call site に閉じていることを返した。
  - `rg` は `KAFS_V6_ADMISSION_HANDOFF` の test coverage と、`KAFS_V6_READONLY_SMOKE` env を直接設定する
    test がないことを示した。
  - `git diff --check`、`./scripts/test-cli-surface.sh`、
    `make -C tests check TESTS=v6_descriptor_smoketest` が成功した。

### SDW-V6RT-T53 retire production kafs readonly smoke env gate

- 目的: T52 で retirement candidate とした `KAFS_V6_READONLY_SMOKE` を production
  `kafs` から削除し、read-only format-v6 FUSE coverage を `kafs-v6 --inspection-mount` に一本化する。
  これは diagnostic scaffolding retirement であり、runtime admission ownership や write surface は
  変更しない。
- 変更:
  - `src/kafs.c` から `KAFS_V6_READONLY_SMOKE` env gate、readonly smoke helper、
    `kafs_main_open_runtime_context()` の readonly-smoke branch を削除する。
  - `src/kafs_context.h` から readonly-smoke 専用 runtime flag を削除する。
  - inventory / handoff / runtime plan docs を、`KAFS_V6_READONLY_SMOKE` retired として更新する。
- 完了条件:
  - `rg KAFS_V6_READONLY_SMOKE src tests` が該当なしになる。
  - `kafs-v6 --inspection-mount` が引き続き read-only v6 FUSE inspection coverage の所有 entrypoint である。
  - production `kafs` の legacy v6 token fail-closed guidance と plain-v6 offline-only preflight は維持される。
  - controlled-write write surface は既存の regular-file create/write/fsync/release 範囲から広げない。
  - `./scripts/format.sh fix`、`make -j2`、`git diff --check`、
    `./scripts/test-cli-surface.sh`、`make -C tests check TESTS=v6_descriptor_smoketest`、
    `./scripts/static-checks.sh`、`KAFS_TEST_MOUNT_TIMEOUT_MS=15000 make check -j2` が PASS している。
- 実装結果:
  - `KAFS_V6_READONLY_SMOKE` env gate と専用 runtime flag を削除した。
  - production `kafs` の v6 image path は `KAFS_V6_ADMISSION_HANDOFF` diagnostic または
    plain-v6 offline-only preflight に閉じるようになった。
  - read-only FUSE inspection は `kafs-v6 --inspection-mount` のまま維持し、write-surface policy は変更していない。
- 検証:
  - `rg KAFS_V6_READONLY_SMOKE src tests` は該当なしだった。
  - `./scripts/format.sh fix`、`make -j2`、`git diff --check`、
    `./scripts/test-cli-surface.sh`、`make -C tests check TESTS=v6_descriptor_smoketest`、
    `./scripts/static-checks.sh`、`KAFS_TEST_MOUNT_TIMEOUT_MS=15000 make check -j2` が成功した。
  - clone strict source gate は 35 clones / 345 duplicated lines / 0.91% で閾値内だった。
  - `make check` は all 29 tests passed で完了した。

### SDW-V6RT-T54 retire production kafs legacy-token successful branches

- 目的: production `main()` の fail-closed gate より後ろに残っていた legacy
  `v6_inspection_mount` / `v6_write_mount` successful branch を削除し、successful
  format-v6 runtime admission の所有を `kafs-v6` に一本化する。legacy token の認識と
  `kafs-v6` guidance は互換案内として維持する。
- 変更:
  - `src/kafs.c` から `kafs_main_v6_inspection_mount()`、
    `kafs_main_v6_controlled_write_mount()`、および
    `kafs_main_open_runtime_context()` の legacy-token successful branch を削除する。
  - production `kafs` の v6 token validation は fail-closed guidance に一本化し、
    `kafs_main_open_runtime_context()` は plain-v6 preflight / offline-only と
    `KAFS_V6_ADMISSION_HANDOFF` diagnostic だけを扱う。
  - inventory / handoff / runtime plan docs を、legacy-token successful branch retired として更新する。
- 完了条件:
  - `rg "kafs_main_v6_(inspection|controlled_write)_mount|kafs_main_v6_validate_controlled_write_runtime" src tests`
    が該当なしになる。
  - `kafs -o v6_inspection_mount` と `kafs -o v6_write_mount` は引き続き `kafs-v6`
    guidance で fail closed する。
  - `kafs-v6 --inspection-mount` と `kafs-v6 --controlled-write-mount` の focused smoke は維持される。
  - production `kafs` の plain-v6 offline-only preflight と `KAFS_V6_ADMISSION_HANDOFF`
    diagnostic の扱いは変更しない。
  - `./scripts/format.sh fix`、`make -j2`、`git diff --check`、
    `./scripts/test-cli-surface.sh`、`make -C tests check TESTS=v6_descriptor_smoketest`、
    `./scripts/static-checks.sh`、`KAFS_TEST_MOUNT_TIMEOUT_MS=15000 make check -j2` が PASS している。
- 実装結果:
  - production `kafs` から legacy-token successful branch と専用 runtime helper を削除した。
  - legacy token parser / fail-closed guidance は維持し、production `kafs` が successful
    v6 runtime path を持たない状態にした。
  - 次の production v6 diagnostic reduction candidate は `KAFS_V6_ADMISSION_HANDOFF` と
    plain-v6 offline-only preflight の扱いに移った。
- 検証:
  - `lsp-cli --server clangd --server-cmd /usr/bin/clangd-18 --root . --format pretty ws-symbols`
    は `kafs_main_v6_inspection_mount` / `kafs_main_v6_controlled_write_mount` を no result とした。
  - `rg` は removed helper / validator が `src/` と tests に残っていないことを示した。
  - `./scripts/format.sh fix`、`make -j2`、`git diff --check`、
    `./scripts/test-cli-surface.sh`、`make -C tests check TESTS=v6_descriptor_smoketest`、
    `./scripts/static-checks.sh`、`KAFS_TEST_MOUNT_TIMEOUT_MS=15000 make check -j2` が成功した。

### SDW-V6RT-T55 retire production kafs admission handoff env gate

- 目的: production `kafs` に残る env-only diagnostic gate
  `KAFS_V6_ADMISSION_HANDOFF` を削除し、production `kafs` の v6 image path を
  plain-v6 admission preflight / offline-only guidance に統一する。successful
  format-v6 runtime admission は引き続き `kafs-v6` が所有する。
- 変更:
  - `src/kafs.c` から `KAFS_V6_ADMISSION_HANDOFF` env 判定、
    `kafs_main_v6_admission_handoff()`、production-local runtime-admit wrapper を削除する。
  - `kafs_main_open_runtime_context()` の v6 branch は常に
    `kafs_main_v6_admission_preflight()` を実行し、offline-only gate へ進む。
  - `v6_descriptor_smoketest` は retired env を指定しても production `kafs` の
    preflight/offline-only 出力が変わらず、handoff 出力が出ないことを確認する。
  - inventory / handoff / runtime plan docs を、`KAFS_V6_ADMISSION_HANDOFF` retired として更新する。
- 完了条件:
  - `rg KAFS_V6_ADMISSION_HANDOFF src tests` が、retired-env no-op regression 以外に該当しない。
  - `lsp-cli` workspace symbol で `kafs_main_v6_admission_handoff` と
    `kafs_main_v6_runtime_admit_context` が no result になる。
  - plain-v6 production `kafs` mount は引き続き admission preflight / offline-only guidance を出す。
  - `kafs-v6 --inspection-mount` と `kafs-v6 --controlled-write-mount` は successful
    v6 runtime admission entrypoint のまま維持される。
  - `./scripts/format.sh fix`、`make -j2`、`git diff --check`、
    `./scripts/test-cli-surface.sh`、`make -C tests check TESTS=v6_descriptor_smoketest`、
    `./scripts/static-checks.sh`、`KAFS_TEST_MOUNT_TIMEOUT_MS=15000 make check -j2` が PASS している。
- 実装結果:
  - production `kafs` の env-only handoff branch を削除した。
  - production `kafs` の remaining v6 diagnostic surface は plain-v6 offline-only
    descriptor preflight に限定された。
  - `kafs_v6_admission.h` は production preflight と `kafs_v6_runtime.c` の
    dedicated runtime admission helperで引き続き共有される。
- 検証:
  - `lsp-cli --server clangd --server-cmd /usr/bin/clangd-18 --root . --format pretty ws-symbols`
    は `kafs_main_v6_admission_handoff` / `kafs_main_v6_runtime_admit_context` を no result とした。
  - `rg` は production source に `KAFS_V6_ADMISSION_HANDOFF` と removed helper が残っていないことを示した。
  - `./scripts/format.sh fix`、`make -j2`、`git diff --check`、
    `./scripts/test-cli-surface.sh`、`make -C tests check TESTS=v6_descriptor_smoketest`、
    `./scripts/static-checks.sh`、`KAFS_TEST_MOUNT_TIMEOUT_MS=15000 make check -j2` が成功した。

### SDW-V6RT-T56 retire production kafs plain-v6 descriptor preflight

- 目的: production `kafs` に残る plain-v6 descriptor admission preflight を削除し、
  production `kafs` の v6 image path を direct `kafs-v6` guidance に一本化する。
  descriptor validation と successful runtime admission は `fsck.kafs` / offline helpers /
  `kafs-v6` が所有する。
- 変更:
  - `src/kafs.c` から `kafs_v6_admission.h` include、
    `kafs_main_v6_admission_preflight()`、preflight-local `kafs_main_rc_text()` を削除する。
  - production `kafs` の v6 image path は descriptor preflight を実行せず、
    `kafs-v6 --inspection-mount` / `kafs-v6 --controlled-write-mount` guidance で
    fail closed する。
  - `v6_descriptor_smoketest`、`v6_descriptor_validation`、`kafsresize` は production
    `kafs` が preflight を出さないことを regression として固定する。
  - inventory / handoff / runtime plan docs を、plain-v6 preflight retired として更新する。
- 完了条件:
  - `rg kafs_main_v6_admission_preflight src/kafs.c tests` が no match になる。
  - `rg 'kafs_v6_admission.h' src/kafs.c` が no match になる。
  - production `kafs` の v6 image mount は direct `kafs-v6` guidance を出し、
    `admission preflight` を出さない。
  - `kafs-v6 --inspection-mount` と `kafs-v6 --controlled-write-mount` は successful
    v6 runtime admission entrypoint のまま維持される。
  - `./scripts/format.sh fix`、`make -j2`、`git diff --check`、
    `./scripts/test-cli-surface.sh`、`make -C tests check TESTS=v6_descriptor_smoketest`、
    `make -C tests check TESTS=v6_descriptor_validation`、`make -C tests check TESTS=kafsresize`、
    `./scripts/static-checks.sh`、`KAFS_TEST_MOUNT_TIMEOUT_MS=15000 make check -j2` が PASS している。
- 実装結果:
  - production `kafs` の plain-v6 descriptor preflight branch を削除した。
  - production `kafs` は v6 image mount で descriptor validation を実行せず、direct
    `kafs-v6` guidance で fail closed する。
  - production `src/kafs.c` は `kafs_v6_admission.h` を include しなくなった。
- 検証:
  - `rg` は production `src/kafs.c` と更新対象 tests から
    `kafs_main_v6_admission_preflight` / `kafs_main_rc_text` / production
    `kafs_v6_admission.h` include が消えたことを示した。
  - `./scripts/format.sh fix`、`make -j2`、`git diff --check`、
    `./scripts/test-cli-surface.sh`、`make -C tests check TESTS=v6_descriptor_smoketest`、
    `make -C tests check TESTS=v6_descriptor_validation`、`make -C tests check TESTS=kafsresize`、
    `./scripts/static-checks.sh`、`KAFS_TEST_MOUNT_TIMEOUT_MS=15000 make check -j2` が成功した。

### SDW-V6RT-T57 retire production kafs legacy v6 token guidance

- 目的: production `kafs` に残る legacy v6 token compatibility guidance を削除し、
  production `kafs` の v6 image path を direct `kafs-v6` guidance だけにする。legacy
  token rejection は dedicated entrypoint である `kafs-v6` の policy として残す。
- 変更:
  - `src/kafs.c` から `kafs_legacy_v6_failclosed.h` include、legacy v6 request
    flags、`kafs_main_record_legacy_v6_request()`、`kafs_main_handle_v6_inspection_token()`、
    `kafs_main_handle_v6_write_token()`、`kafs_legacy_v6_reject_if_requested()` call を削除する。
  - `src/kafs_legacy_v6_failclosed.h` を削除し、`src/Makefile.am` の header list から外す。
  - `kafs --help`、`man/kafs.1`、`completions/kafs` から production legacy v6 tokens を削除する。
  - `v6_descriptor_smoketest` は production `kafs -o v6_inspection_mount` /
    `-o v6_write_mount` が legacy-specific message ではなく direct `kafs-v6` guidance で
    fail closed することを確認する。
- 完了条件:
  - `rg kafs_legacy_v6 src/kafs.c src/Makefile.am src/kafs_legacy_v6_failclosed.h` が
    deleted file 以外に該当しない。
  - `rg 'v6_inspection_mount|v6_write_mount' src/kafs.c man/kafs.1 completions/kafs` が
    no match になる。
  - `kafs-v6` 側の legacy token rejection は維持される。
  - `./scripts/format.sh fix`、`make -j2`、`git diff --check`、
    `./scripts/test-cli-surface.sh`、`make -C tests check TESTS=v6_descriptor_smoketest`、
    `./scripts/static-checks.sh`、`KAFS_TEST_MOUNT_TIMEOUT_MS=15000 make check -j2` が PASS している。
- 実装結果:
  - `src/kafs.c` から legacy v6 token 専用の request flags、token handlers、fail-closed
    include/call を削除した。
  - `src/kafs_legacy_v6_failclosed.h` を削除し、`src/Makefile.am` の header list から外した。
  - `kafs --help`、`man/kafs.1`、`completions/kafs` から production legacy v6 tokens を削除した。
  - `v6_descriptor_smoketest` は production `kafs -o v6_inspection_mount` /
    `-o v6_write_mount` が plain v6 image と同じ direct `kafs-v6` guidance で
    fail closed することを確認する。
- 検証:
  - `rg` は `src/kafs.c`、`src/Makefile.am`、`man/kafs.1`、`completions/kafs` から
    `kafs_legacy_v6` と production legacy v6 tokens が消えたことを示した。
  - `./scripts/format.sh fix`、`make -j2`、`git diff --check`、
    `./scripts/test-cli-surface.sh`、`make -C tests check TESTS=v6_descriptor_smoketest`、
    `./scripts/static-checks.sh`、`KAFS_TEST_MOUNT_TIMEOUT_MS=15000 make check -j2` が成功した。

### SDW-V6RT-T58 shared FUSE runner request boundary

- 目的: `src/kafs.c` に残る shared FUSE runner / operation table handoff を
  request-based API に狭め、production `kafs` main と `kafs-v6` entrypoint adapter が同じ
  shared runner request 経路を通るようにする。filesystem operation 実装の移動や v6 write
  surface 拡張は行わない。
- 変更:
  - `src/kafs_shared_fuse_runner.h` に `kafs_shared_fuse_run_request_t` を追加し、
    `ctx`、FUSE argv、optional hotplug socket path、runtime option summary を 1 つの
    request として渡す。
  - exported handoff を `kafs_shared_fuse_run()` から
    `kafs_shared_fuse_run_request()` に置き換える。
  - production `kafs` main と `kafs_v6_entrypoint_adapter.c` は同じ request 型を組み立て、
    `src/kafs.c` 内の local shared runner に渡す。
- 完了条件:
  - `rg 'kafs_shared_fuse_run\\(' src` が no match になる。
  - `rg kafs_shared_fuse_run_request src` が header、`kafs.c` export/local path、
    `kafs_v6_entrypoint_adapter.c` call site、production main の local request call を示す。
  - `lsp-cli --server-cmd clangd-18` で shared runner request 周辺の symbols/references を
    確認できる。
  - `./scripts/format.sh fix`、`make -j2`、`git diff --check`、
    `make -C tests check TESTS=v6_descriptor_smoketest`、
    `./scripts/static-checks.sh`、`KAFS_TEST_MOUNT_TIMEOUT_MS=15000 make check -j2` が PASS している。
- 実装結果:
  - `src/kafs_shared_fuse_runner.h` に `kafs_shared_fuse_run_request_t` を追加し、
    shared runner handoff を request-based API にした。
  - `src/kafs.c` の exported handoff を `kafs_shared_fuse_run_request()` に置き換え、
    local shared runner は同じ request object から runtime option logging、
    `fuse_main()`、cleanup を実行する。
  - production `kafs` main と `kafs_v6_entrypoint_adapter.c` は同じ request 型を組み立てて
    shared runner に入る。
- 検証:
  - `lsp-cli --root . --server-cmd clangd-18` の `symbols` / `references` で
    `src/kafs.c` の shared runner 周辺を確認した。
  - `rg` は `src` から raw `kafs_shared_fuse_run(` call が消え、
    `kafs_shared_fuse_run_request` の header / export / adapter / production main 経路が
    残ることを示した。
  - `./scripts/format.sh fix`、`autoreconf -fi && ./configure && make -j2`、
    `git diff --check`、`make -C tests check TESTS=v6_descriptor_smoketest`、
    `./scripts/static-checks.sh`、`KAFS_TEST_MOUNT_TIMEOUT_MS=15000 make check -j2` が成功した。

### SDW-V6RT-T59 shared FUSE runtime source ownership split

- 目的: `kafs-v6` が production `src/kafs.c` entrypoint wrapper を経由せず、shared FUSE
  runtime implementation へ直接 link するように source ownership を分ける。filesystem
  operation 実装の複製や v6 write surface 拡張は行わない。
- 変更:
  - 旧 `src/kafs.c` の shared FUSE runtime implementation を
    `src/kafs_shared_fuse_runtime.c` に移す。
  - `src/kafs.c` を production `kafs` process entrypoint wrapper にし、
    `kafs_production_main()` へ委譲する。
  - `src/Makefile.am` で `kafs-v6`、`kafsctl`、`kafs-back` の common source を
    `kafs.c` から `kafs_shared_fuse_runtime.c` に差し替える。
  - shared runtime を direct include する internal regression test と source-level
    ownership comments を新しい source 境界に合わせる。
- 完了条件:
  - `src/Makefile.am` の `kafs_v6_SOURCES` が `kafs.c` を含まない。
  - `rg '#include "kafs\\.c"' src tests` が no match になる。
  - `lsp-cli --server-cmd clangd-18` で `kafs_production_main` と
    `kafs_shared_fuse_run_request` 周辺の symbols/references を確認できる。
  - `./scripts/format.sh fix`、`autoreconf -fi && ./configure && make -j2`、
    `git diff --check`、`make -C tests check TESTS=v6_descriptor_smoketest`、
    `./scripts/static-checks.sh`、`KAFS_TEST_MOUNT_TIMEOUT_MS=15000 make check -j2` が PASS している。
- 実装結果:
  - `src/kafs_shared_fuse_runtime.c` を追加し、shared FUSE operation
    implementation、operation table、shared runner、production mount-main body を
    旧 `src/kafs.c` から移した。
  - `src/kafs.c` は `kafs_crash_diag_install("kafs")` と
    `kafs_production_main()` 呼び出しだけを持つ production wrapper になった。
  - `kafs-v6`、`kafsctl`、`kafs-back` は `src/Makefile.am` で
    `kafs_shared_fuse_runtime.c` を link し、`kafs-v6` は `src/kafs.c` を link しなくなった。
  - `tests/tests_bg_dedup_skip_dirs.c` は shared runtime source を direct include
    する internal test として更新した。
- 検証:
  - `bear -- make -j2` で `compile_commands.json` を更新し、`kafs-v6` が
    `kafs_v6-kafs_shared_fuse_runtime.o` を使うことを確認した。
  - `lsp-cli --root . --server-cmd clangd-18` の `symbols` / `references` /
    `definition` / `hover` で `kafs_production_main` と
    `kafs_shared_fuse_run_request` 周辺を確認した。
  - `rg` は `src/Makefile.am` の `kafs_v6_SOURCES` から `kafs.c` 直接依存が消え、
    `src` / `tests` から `#include "kafs.c"` が消えたことを示した。
  - `./scripts/format.sh fix`、`autoreconf -fi && ./configure && make -j2`、
    `make clean && bear -- make -j2`、`git diff --check`、
    `make -C tests check TESTS=v6_descriptor_smoketest`、
    `./scripts/test-cli-surface.sh`、`./scripts/static-checks.sh`、
    `KAFS_TEST_MOUNT_TIMEOUT_MS=15000 make check -j2` が成功した。

### SDW-V7RT-T1 format v7 pivot entrypoint and offline tooling

- 目的: format v6 を実験的実装として凍結し、破壊的変更を伴う descriptor-backed work の
  新しい入口を format v7 / `kafs-v7` に切り替える。
- 変更:
  - `KAFS_FORMAT_VERSION_V7` と descriptor-backed format 判定 helper を追加する。
  - `src/Makefile.am` に `kafs-v7` target を追加し、v7 専用 entrypoint/runtime/adapter source set
    を build できるようにする。
  - `mkfs.kafs --format-version 7`、`fsck.kafs`、`kafsdump` が descriptor-backed v7 image を
    offline に扱えるようにする。
  - production `kafs` は v6/v7 descriptor-backed image を fail closed し、format に応じて
    `kafs-v6` / `kafs-v7` guidance を出す。
  - man page、completion、tool suite docs、v7 pivot ADR を更新する。
- 完了条件:
  - `kafs-v7 --help` が format v7 entrypoint として表示される。
  - `mkfs.kafs --format-version 7` で作成した image を `fsck.kafs` / `kafsdump` が
    descriptor-backed image として報告する。
  - production `kafs` は v7 image を mount 成功 path に通さず、`kafs-v7` guidance で拒否する。
  - `kafs-v7` は v6 image を拒否する。
  - `lsp-cli --server-cmd clangd-18`、`./scripts/format.sh fix`、`autoreconf -fi`、
    `./configure`、`make -j2`、`git diff --check`、targeted tests、`make check -j2` が
    PASS している。
- 実装結果:
  - v7 は `kafs_v7.c`、`kafs_v7_runtime.*`、`kafs_v7_mount_options.*`、
    `kafs_v7_entrypoint_adapter.*` を持つ独立 source set として実装した。
  - 低レベル descriptor scaffold parser/builder は、次の中立名抽出まで歴史名の
    `kafs_v6_layout.h` を参照する。
  - この時点で生成する `K7LD` version 1 image は pre-specification scaffold であり、
    2026-07-13 accepted raw-layout の descriptor version 2 image ではない。
  - v6 は frozen experimental entrypoint として残し、今後の破壊的 layout/policy work は
    v7 側へ進める方針を文書化した。
  - v6 の `v6_layout_descriptor` JSON key は report consumer 互換のため残し、v7 は
    `layout_descriptor` key を使うようにした。
- 検証:
  - `lsp-cli --root . --server clangd --server-cmd clangd-18 --format pretty diagnostics src/kafs_v7_runtime.c`
    と `tests/tests_v7_entrypoint_smoketest.c` は diagnostics 0 件だった。
  - `./scripts/format.sh fix`、`autoreconf -fi`、`./configure`、`make -j2`、`git diff --check` が成功した。
  - `make -C tests check TESTS=v7_entrypoint_smoketest`、
    `make -C tests check TESTS=v6_descriptor_validation`、
    `make -C tests check TESTS=v6_descriptor_smoketest`、
    `make -C tests check TESTS=kafsresize` が成功した。
  - `make check -j2` は 29 tests passed / 1 skipped で成功した。

### SDW-V7RT-T2 v7 raw-layout decision closeout

- 目的: wear distribution と fault tolerance / deterministic fsck recovery を同率最優先にし、
  v7 raw-layout の implementation-blocking decision を code 変更前に accepted contract として閉じる。
- 決定:
  - accepted layout は pre-spec `K7LD` version 1 scaffold と wire meaning を共有せず、
    v7-owned 96-byte group/shard record と replicated `K7CP` を持つ descriptor version 2 とする。
  - group は metadata physical span と data logical/physical span を明示し、shard は
    `storage_class` と physical/logical coverage を wire field として持つ。
  - mutable free counts は rotated `K7CP` replica が authoritative であり、offset 0 superblock は
    immutable identity/discovery state とする。
  - journal header/data は同一 group に置き、segment sequence/replay order は filesystem-global
    とする。multi-group transaction protocol がない path は mutation 前に fail closed する。
  - HRL bucket/entry group consistency を必須とし、cross-group 対応は incompatible flag、
    mapping、recovery、fsck proof を伴う将来の独立変更にする。
  - valid image は primary/tail descriptor/checkpoint replica を必須とし、決定的に配置可能なら
    midpoint replica も必須とする。one-replica test-image exception は設けない。
  - offset 0 の locator 破損だけで offline discovery を失わないよう、final block 全体を予約し、
    その final 32 bytes に block size を持つ byte-identical tail `K7SA` version 2 locator を置く。
  - CRC は reflected polynomial `0xEDB88320` / init・final XOR `0xffffffff` に固定し、CRC 一致を
    replica 同値判定には使わない。同世代 locator/descriptor/checkpoint は covered span の
    byte-identical 比較で divergence を判定する。
  - inode、bitmap、allocator、HRL、journal は size/offset/endianness/padding を v7-owned wire shape
    として自己完結に固定する。journal は rotating `K7JH` slot と structured transaction を使い、
    全 valid segment の transaction を global sequence で merge する。
- 完了条件:
  - inception deck と raw-layout specification が `Status: accepted` になり、open question が残らない。
  - record size/offset、little-endian、alignment、overflow、type/class、generation/CRC、replica
    selection/copy-update、same-generation divergence の扱いが一意に記録されている。
  - pending log / tail metadata、cross-group HRL、multi-group atomic mutation は明示的に fail closed
    または deferred であり、暗黙の v5/v6 fallback がない。
  - accepted version 2 と現行 version 1 scaffold の境界、および次の offline implementation slice
    が明記されている。
- 実装結果:
  - [sd-card-wear-format-v7-inception-deck.md](sd-card-wear-format-v7-inception-deck.md) と
    [sd-card-wear-format-v7-raw-layout.md](sd-card-wear-format-v7-raw-layout.md) を accepted にした。
  - [sd-card-wear-format-v7-pivot.md](sd-card-wear-format-v7-pivot.md) の current boundary と
    follow-up order を accepted version 2 contract に合わせた。
  - single-group は wear-leveling completion ではなく、multi-group placement と障害耐性検証の
    foundation であることを明記した。
- 検証:
  - `git diff --check` が成功した。
  - `rg -n '^(Accepted: 2026-07-13|Status: accepted)$|K7LD.*version 2|K7JH|SDW-V7RT-T3' docs/sd-card-wear-format-v7-*.md docs/sd-card-wear-tickets.md`
    で accepted status、version 2 wire contract、journal header、T3 handoff を確認した。
  - `! rg -n '^## (Open Questions|Questions To Resolve)$' docs/sd-card-wear-format-v7-*.md`
    で未解決 question heading がないことを確認した。
  - docs-only decision closeout のため build/test は実施していない。

### SDW-V7RT-T3 v7-owned single-group raw-layout offline round trip

- 目的: accepted descriptor version 2 の strict subset として `group_count == 1` の
  `mkfs.kafs -> kafsdump -> fsck.kafs` offline round trip と replica failover を証明する。
  これは wear leveling 完了ではなく、後続 multi-group placement の v7-owned foundation とする。
- 変更:
  - v7-owned `K7LD` header/group/shard/replica、`K7SA` version 2 locator、`K7CP` checkpoint の
    encode/decode、CRC、coverage validator を実装する。`sizeof` / `offsetof` assertion を持ち、
    successful v7 path は v6 wire typedef や v6 public layout entrypoint を通らない。
  - `src/kafs_v7_layout.c` / `src/kafs_v7_layout.h` を accepted wire の所有元とし、
    `scripts/check-v7-layout-ownership.sh` で v6 wire/layout facade への逆依存を拒否する。
  - mkfs は group 0 の metadata/data physical/logical span、required shard、最低2組の
    descriptor/checkpoint replica と最低2つの空 journal segment を構築し、決定式で配置可能な
    image では midpoint も構築する。raw-layout の canonical seven-shard order と最大データ数の
    fixed-point rule を使い、midpoint のために data block を減らさない。
  - fsck/kafsdump は `K7SA -> K7LD -> K7CP/shards` の discovery chain、各 replica selection、
    全shard coverage、journal pair/global order、HRL group invariant、recovered-state free counts を
    v7-owned names で検証・報告する。
  - `kafsdump --json` は `root_locators`、`layout_descriptor`、`descriptor_replicas`、
    `checkpoints`、`groups`、`shards`、`journal_segments` key を持つ。各 replica の `status` は
    `valid|invalid|stale|divergent`、選択・縮退は `selected` / `degraded` boolean で表す。
  - `man/mkfs.kafs.8`、`man/fsck.kafs.8`、`man/kafsdump.8` を accepted v2 と pre-spec v1 の
    境界に合わせ、JSON parse regression を dedicated test に含める。
  - dedicated `v7_raw_layout_smoketest` を追加する。T3 完了後も
    `kafs-v7 --inspection-mount` と controlled-write admission は後続 ticket まで明示的に拒否する。
- 完了条件:
  - valid image は primary identity と `K7SA -> K7LD -> K7CP/shards` だけから offline round trip が
    成功する。legacy prefix offset/free-count field を non-authoritative 値へ変えても authoritative
    state と admission/validation 結果は変わらないが、raw diagnostic 値の表示は変わってよい。
  - locator、descriptor、checkpoint の各々について、primary 1本の破損から backup を選択でき、
    same-generation non-byte-identical divergence を個別に拒否する。
  - 全replica破損、同世代divergence、gap/overlap/out-of-bounds、unknown type/class/flag、
    required incompat bit 欠落、reserved 非ゼロ、table/range 算術 overflow、unknown mapping policy、
    non-zero mapping seed、unsupported block-size/hash id、primary `K7SA` offset/remaining
    reserved-byte 違反、LE64 bitmap alignment/padding 違反、canonical free-inode/count 違反、
    bitmap/checkpoint 不整合、journal free-count delta/sequence gap/divergence、HRL cross-group
    corruption を決定的に拒否する。
  - canonical single-group mkfs の2-replica geometry、midpoint slack を持つ valid 3-replica parser
    fixture、too-small rejection を sparse image で検証し、pre-spec version 1 scaffold を accepted
    layout として読まない。
  - test workdir は `${TMPDIR:-/tmp}` 配下に作成し、成功・失敗の双方で repo tree を汚さない。
  - v6 fixture/test と production `kafs` の v7 fail-closed behavior は変わらない。
- 対象外:
  - multi-group placement/wear proof、runtime mount、controlled write、multi-group atomicity、
    cross-group HRL、pending/tail、v5-to-v7 migration、in-place relocation。
- 実装結果:
  - `src/kafs_v7_layout.c` / `src/kafs_v7_layout.h` に accepted version 2 の wire record、
    checked geometry planner、CRC、single-group builder、independent replica selector/validator を実装した。
    successful path は v6 public layout/wire entrypoint を使わない。mkfs publication は旧 root 無効化、
    metadata、non-primary copy、primary copy、tail locator、primary root の順に flush boundary を持つ。
  - `mkfs.kafs` は primary/tail の descriptor/checkpoint、canonical seven-shard group、2つの空 journal
    segment を構築する。`kafsdump` / `fsck.kafs` は authoritative discovery chain と recovered count を
    v7-owned path だけで検証・報告する。
  - primary locator/descriptor/checkpoint の個別 failover、lower generation と descriptor-generation
    mismatch の `stale` 扱い、same-generation divergence、3-replica parser fixture、unowned slack zero、
    malformed descriptor/payload/root identity を dedicated regression に固定した。
  - `kafs-v7 --inspection-mount`、production `kafs`、v6 entrypoint から accepted v7 layout への admission は
    offline-only error で fail closed のままとした。runtime mount、write、cross-group HRL は有効化していない。
- 完了時の検証:
  - `./scripts/format.sh fix`
  - `autoreconf -fi && ./configure && make -j2`
  - `lsp-cli --root . --server clangd --server-cmd clangd-18 --format pretty symbols src/kafs_v7_layout.c`
  - `lsp-cli --root . --server clangd --server-cmd clangd-18 --format pretty ws-symbols kafs_v7_layout`
  - `lsp-cli --root . --server clangd --server-cmd clangd-18 --format pretty diagnostics src/kafs_v7_layout.c`
  - `lsp-cli --root . --server clangd --server-cmd clangd-18 --format pretty diagnostics tests/tests_v7_raw_layout_smoketest.c`
  - `./scripts/check-v7-layout-ownership.sh`
  - `! rg -n 'kafs_sv6_|kafs_v6_|kafs_descriptor_layout|#include "kafs_v6_layout.h"' src/kafs_v7_layout.c src/kafs_v7_layout.h tests/tests_v7_raw_layout_smoketest.c`
  - 上記 build/LSP/ownership check 後、次の同一 shell block で test 前後の status を比較する。未commitの
    意図した T3変更自体ではなく、gateが新しいtracked/untracked artifactを増やしていないことを判定する。

    ```sh
    set -euo pipefail
    status_before=$(mktemp "${TMPDIR:-/tmp}/kafs-t3-status-before.XXXXXX")
    status_after=$(mktemp "${TMPDIR:-/tmp}/kafs-t3-status-after.XXXXXX")
    trap 'rm -f "$status_before" "$status_after"' EXIT
    git status --short --untracked-files=all >"$status_before"
    make -C tests check TESTS=v7_raw_layout_smoketest
    make -C tests check TESTS='v7_entrypoint_smoketest v6_descriptor_validation v6_descriptor_smoketest kafsresize'
    ./scripts/lint.sh
    ./scripts/clones.sh
    ./scripts/static-checks.sh
    KAFS_TEST_MOUNT_TIMEOUT_MS=15000 make check -j2
    git diff --check
    git status --short --untracked-files=all >"$status_after"
    diff -u "$status_before" "$status_after"
    ```
- 検証結果（2026-07-13）:
  - `autoreconf -fi && ./configure && make -j2`、`./scripts/format.sh`、`./scripts/lint.sh`、
    `git diff --check` は PASS。
  - 指定した4つの LSP query は成功し、source/test diagnostics はともに 0 件。
  - `./scripts/check-v7-layout-ownership.sh` と v6 facade 禁止 pattern check は PASS。
  - dedicated `v7_raw_layout_smoketest` と `v7_entrypoint_smoketest` は 2/2 PASS。
    `v7_entrypoint_smoketest v6_descriptor_validation v6_descriptor_smoketest kafsresize` は 4/4 PASS。
    64 MiB sparse image の 1024-byte / 65536-byte block-size round trip も PASS。
  - `KAFS_TEST_MOUNT_TIMEOUT_MS=15000 make check -j2` は 30/31 PASS。今回の v7 test は PASS したが、
    既存 v5/FUSE hotplug 経路の `e2e_hotplug` は全体実行と単独再実行の双方で
    `hotplug connect timeout` となった。
  - `./scripts/clones.sh` は 87 clones / 2.9% で 1.0% threshold を超え FAIL。基準 commit
    `b737ce0` も 87 clones / 3.1% で FAIL しており、新規 `src/kafs_v7_layout.c` の clone は検出されない。
    `./scripts/static-checks.sh` の唯一の non-passing step も同じ clone gate で、format/lint は PASS。
  - test/gate 前後の `git status --short --untracked-files=all` は一致し、新しい tracked/untracked artifact は
    増えていない。

### SDW-V7RT-T4 v7 multi-group placement and filesystem wear-distribution proof

- 目的: accepted version 2 record の group-local mapping を実際の multi-group geometry に広げ、
  mutable metadata を image prefix 一箇所へ集中させない filesystem-level placement と、group 境界破損を
  deterministic に拒否する offline proof を追加する。SD controller の FTL/erase-block 配置を観測したとは
  主張しない。
- 変更:
  - canonical group count は power-of-two の 1..64 とし、1024 HRL bucket を等分する。
    自動選択は image 64 MiB あたり1 group を上限に supported power-of-two へ丸め、geometry 不成立時は
    半減する。`mkfs.kafs --v7-group-count N` は exact override であり、別 count への fallback を行わない。
  - logical data は完全な64-block bitmap wordを quotient/remainder で分け、末尾 partial word は最終 group
    だけに置く。inode/HRL entry は exact coverage、HRL bucket は等分し、multi-group journal は1 group
    1 segment とする。
  - physical order は group-id 順の `[seven metadata shards][data]` interleave とし、midpoint replica は
    最大 data geometry 後の zero slack に自然に収まる場合だけ追加する。
  - validator は全 group の logical/physical coverage、canonical shard owner/order/size、inode/free count、
    bitmap/allocator、HRL、journal segment id を集計する。cross-group HRL head/chain は引き続き fail closed。
  - `kafsdump` に `wear_distribution` を追加し、group count、metadata placement span/arena、group data
    min/max を text/JSON で報告する。`scripts/check-v7-wear-distribution.sh` は512 MiB sparse imageで
    8 group、data skew 64 blocks 以下、metadata span 70%以上を固定する。
  - `v7_multi_group_smoketest` は自動8 group、明示4 group、invalid/too-small/non-v7 CLI rejection、
    logical gap、physical overlap、group owner偽装、journal logical gap、cross-group HRL headを検証する。
- 完了条件:
  - `mkfs.kafs --format-version 7` の512 MiB imageが8 groupで offline round tripし、全 group の free
    block/inode/journal countが selected checkpoint と一致する。
  - internal data logical boundaryは64-block alignment、group data skewは64 blocks以下、physical metadata
    startはgroup順に分散する。
  - group descriptor/shardのgap、overlap、owner、logical coverage、およびcross-group HRL破損を
    descriptor replicaがbyte-identicalでも確実に拒否する。
  - v6 public wire/layout entrypointへの依存を追加せず、runtime inspection/write admission、cross-group HRL、
    multi-group atomic mutationを有効化しない。
- 実装結果:
  - `src/kafs_v7_layout.*` のplanner/builder/validatorを最大64 groupへ拡張し、single-group accepted imageと
    既存 fault/recovery behaviorを維持した。
  - `mkfs.kafs` に自動 policy と exact `--v7-group-count`、全 group data spanのtrimを追加した。
  - `kafsdump` と dedicated script/test からfilesystem placement分散を観測可能にした。
- 検証結果（2026-07-16）:
  - `autoreconf -fi && ./configure` と clean `bear -- make -j2` は PASS。更新した
    `compile_commands.json` に対する `clangd` diagnostics は layout、mkfs、kafsdump、multi-group test の
    4ファイルすべて0件。
  - `./scripts/check-v7-layout-ownership.sh` は PASS。
    `./scripts/check-v7-wear-distribution.sh` は
    `groups=8 span=469684224/536838144 data_blocks=16128..16192` で PASS。
  - dedicated `v7_multi_group_smoketest v7_raw_layout_smoketest` は2/2 PASS。
    `KAFS_TEST_MOUNT_TIMEOUT_MS=15000 make check -j2` は32/32 PASSし、v5/v6/FUSEを含む全回帰も通過した。
  - 512 MiB sparse imageの1024-byte / 65536-byte block-size双方で、自動8 groupの
    `mkfs.kafs -> fsck.kafs -> kafsdump` round tripがPASSした。
  - `./scripts/format.sh`、`./scripts/lint.sh`、`git diff --check`、complexity reportはPASS。
    strict clone gateは既存87件・2.9%が1.0%閾値を超えるためnon-passingだが、今回追加した
    `src/kafs_v7_layout.c` 内のcloneは0件で、変更前相当の87件へ戻した。

### SDW-V7RT-T5 v7 recovery replica placement and fault-matrix proof

- 目的: multi-group配置後のdescriptor/checkpoint recovery copyについて、filesystem address上の
  primary/midpoint/tail分散、独立損傷、世代選択、同世代divergenceをoffline fixtureで固定する。
  SD controller内部のFTL、ECC、erase-block相関故障を再現したとは主張しない。
- 変更:
  - `v7_replica_fault_smoketest` は512 MiB/8-groupのcanonical 2-copy imageと、それを1 GiBへ
    sparse拡張してzero slackへmidpoint pairを置くaccepted 3-copy fixtureを使う。
  - primary/tail spanはimage address rangeの95%以上、midpointはimage中央、各checkpointは対応する
    descriptorの隣接block、全recovery rangeはgroup metadata/data外であることを確認する。
  - descriptor spanとcheckpoint blockを独立にzero化し、2-copyの片系喪失、非対称損傷、全copy喪失、
    3-copyの任意1/2 recovery neighborhood喪失を検証する。
  - checkpointだけが新世代、descriptorだけが新世代、descriptor/checkpoint pairが協調して新世代、
    3-copy中2-copyが新世代の各publication途中状態を検証する。
  - independently shape-validな別geometry descriptorと、CRC-validな別checkpointを使い、
    selected generationのnon-byte-identical copyがmajorityで解決されず`EUCLEAN`になることを固定する。
- 完了条件:
  - 2-copyは一方のdescriptor/checkpoint喪失からdegradedで復旧し、全descriptorまたは全checkpoint喪失を
    fail closedにする。
  - 3-copyは任意1/2 neighborhood喪失後もoffline読取可能で、残存descriptor/checkpointのreplica idが
    異なる場合もそれぞれを独立選択する。全descriptorまたは全checkpoint喪失はfail closedにする。
  - highest generationを選択し、descriptor generationに一致するcheckpointがないpublication途中は
    fail closed、協調pairがあればdegradedでoffline読取可能とする。
  - runtime inspection/write、repair、actual-media fault claim、cross-group HRL、multi-group mutationは
    有効化しない。
- 実装結果:
  - 現行selectorがaccepted contractどおり動作することを専用回帰testで固定した。production selectorの
    変更は不要だった。
  - testは各caseで`${TMPDIR:-/tmp}`配下の独立sparse imageを使い、repo treeを汚さない。
- 検証結果（2026-07-16）:
  - `autoreconf -fi && ./configure` と clean `bear -- make -j2` は PASS。更新した
    `compile_commands.json` に対する `tests/tests_v7_replica_fault_smoketest.c` の clangd diagnostics は0件。
  - dedicated `make -C tests check TESTS=v7_replica_fault_smoketest` と
    `./scripts/check-v7-layout-ownership.sh` は PASS。
  - `v7_replica_fault_smoketest v7_multi_group_smoketest v7_raw_layout_smoketest
    v7_entrypoint_smoketest` は4/4 PASS。
  - `KAFS_TEST_MOUNT_TIMEOUT_MS=15000 make check -j2` は33/33 PASSし、v5/v6/FUSEを含む全回帰も通過した。
  - `./scripts/format.sh`、`./scripts/lint.sh`、`git diff --check`、complexity reportはPASS。
    strict clone gateは既存87件・2.9%が1.0%閾値を超えるためnon-passingだが、production `src/`を
    変更しておらず、既存baselineからcloneを増やしていない。

### SDW-V7RT-T6 v7-owned read-only runtime views and inspection mount

- 目的: FTL/ECC相関故障をRC media qualification制約として切り離したうえで、accepted v7 imageを
  legacy/v6 wire assumptionで誤読せず、意味のあるread-only FUSE inspectionへadmitする。
- 確認済みblocker:
  - `src/kafs_block.h`のdescriptor runtime-view loaderはaccepted v7を`-EPROTONOSUPPORT`で拒否する。
    単にgateを外すとv6 descriptor parser/shapeへ流れるため、v7-owned successful pathにならない。
  - shared block readはlogical blockを`logical_block << log_blksize`でimage offsetへ直結するが、v7は
    group descriptorによるlogical-to-physical mappingがauthoritativeである。
  - v7 inode/indirect block referenceはzeroが未割当、`N+1`がlogical block `N`だが、shared pathは旧raw
    block numberとして解釈する。
  - shared `statfs`/counter pathはoffset 0のlegacy mutable fieldsを読むが、v7はselected `K7CP`の
    recovered countsと`s_r_blkcnt`がauthoritativeである。
  - accepted v7 specはinode payloadまで定義済みだが、filesystem directory payloadを明示していない。
    空rootだけのmountではnested/block-backed readを証明できない。
  - 現行v7 journal validatorは`checkpoint_seq == 0`かつempty segmentだけを受理する。
- 変更:
  - 実装前に既存`KDIR` version 1 payload shapeをv7が意図的に採用するdirectory wire contractとして
    accepted raw-layoutへ明記する。暗黙のv4/v5/v6 compatibility shortcutにはしない。
  - `kafs_v7_validate_image_fd()`が選んだdescriptor/checkpointを保持し、v7-owned inode shard map、
    group data map、plus-one block-reference decoder、recovered-state viewをruntime contextへ構築する。
  - 初回admissionは`checkpoint_seq == 0`/empty journalに限定し、それ以外は理由付きでfail closedにする。
  - backing imageは`O_RDONLY`、mappingはread-only、FUSEは`ro`、runtime mutation guardは`EROFS`とする。
  - accepted multi-group fixtureへnested directory、inline file、block-backed file/symlinkを配置し、
    actual mount smokeでlookup/readdir/getattr/read/readlink/statfs/unmountを検証する。
  - `kafs-v7 --help`と`man/kafs-v7.8`を、実際に有効なinspection surfaceとoffline-only制限へ揃える。
- 完了条件:
  - successful v7 pathがv6 public wire/layout entrypointへ依存せず、ownership checkを通過する。
  - pristine multi-group imageのinline/block-backed dataを正しいgroup physical rangeから読める。
  - selected `K7CP` recovered countsが`statfs`へ反映され、legacy zero countsを表示しない。
  - single surviving recovery copyはdegraded inspection可能、same-generation divergence/malformed mapは
    FUSE開始前にfail closedとなる。
  - create/write/truncate/unlink/rename/link/mkdir/rmdir/chmod/chown/utimens/xattr/fallocate/copy-range等の
    mutation surfaceが`EROFS`で、image hashがmount前後で不変である。
  - dedicated mount regression、全v7 regression、full `make check -j2`、ownership/static gateがPASSする。
- 対象外:
  - non-empty journal replay、repair、controlled write、checkpoint publication、multi-group atomic mutation、
    cross-group HRL、migration、FTL/ECC physical-failure-domain proof。
- 実装結果:
  - accepted raw-layoutへv7-owned `KDIR` version 1 contractを追加し、mkfsのroot inodeはcanonical empty
    directory payloadを持つようにした。
  - validatorが選択したdescriptor/checkpoint、inode shard map、group data map、recovered countersを
    `kafs_v7_runtime_view`が保持し、successful admissionはv6 public wire/layout entrypointを通らない。
  - `N+1` data referenceはlogical block 0をholeと区別するため物理offset解決まで保持し、group mapから
    authoritative physical rangeへ解決する。`statfs`は`s_r_blkcnt`とselected `K7CP` counterを使う。
  - inspection imageを`O_RDONLY`/read-only mapping/FUSE `ro`で扱い、shared mutation guardと低level
    write defenseは`EROFS`を返す。controlled-write admissionは引き続きfail closedである。
  - 4-group fixtureのactual FUSE mountでnested traversal、inline file、group 3のblock-backed file、symlink、
    recovered `statfs`、mutation `EROFS`、unmount後image digest不変を確認した。primary descriptor/checkpoint
    lossはdegraded mountでき、unpaired higher descriptor generationはFUSE開始前に拒否する。
- 検証結果（2026-07-16）:
  - `make -C tests -j2 v7_inspection_mount_smoketest`と専用test直接実行はPASSし、pristine/degradedの
    2回とも実FUSE mountを通過した。
  - v7回帰5本と`./scripts/check-v7-layout-ownership.sh`、`git diff --check`はPASSした。
  - clean `bear -- make -j2`で`compile_commands.json`を更新し、v7 runtime view、shared FUSE runtime、
    inspection mount testのclangd diagnosticsは0件だった。
  - refactor前のfull `KAFS_TEST_MOUNT_TIMEOUT_MS=15000 make check -j2`は34/34 PASS。最終codeでは
    33 PASS / `stress_fs` 1 SKIPでexit 0となり、SKIP理由は一時的なmount失敗だった。直後の
    `make -C tests check TESTS=stress_fs`はPASSし、全34 testのPASSを確認した。
  - `./scripts/format.sh`、`./scripts/lint.sh`、complexity、ownership、`git diff --check`はPASS。
    `./scripts/static-checks.sh`はcloneだけnon-passingで、strict gateは既存baselineと同じ87件・2.9%が
    1.0%閾値を超えた。実装中に増えた3 cloneはneutral helper抽出で解消し、baselineへ戻した。

### SDW-V7RT-T7 group-local structured journal offline replay proof

- 目的: selected `K7JH` prefix の非空 journal を v7-owned wire parser で検証し、元imageを書き換えずに
  committed transaction の最終metadata/counterへ収束できることを crash fixture で証明する。
- 変更:
  - `src/kafs_v7_journal.*` が `K7JB/K7JM/K7JC/K7JA` record CRC、padding、control一致、mutation stream
    CRC、group-local target identity/deltaを検証する。
  - 各segment内のsequence単調増加とheaderのfirst/lastを検証し、filesystem-globalにbyte-identical duplicateを
    dedupeする。checkpoint後のgap、同一sequenceのdivergence、commit/abort不一致はfail closedにする。
  - committed mutationをtarget単位のbefore/after CRC chainへまとめ、current targetがinitialまたは任意の
    after stateであることを確認して残りのpatchをmemory overlayへ適用する。aborted mutationはtransitionへ
    加えず、そのbefore stateがcommitted chainと一致することだけを要求する。
  - overlay後のbitmap/allocator、inode、HRLを既存v7 semantic validatorで再検証し、checkpoint counterへ
    committed deltaを一度だけ加えたrecovered counterと照合する。元imageへのrepair/writeは行わない。
  - `fsck.kafs`と`kafsdump`はnonempty segment、transaction、duplicate、commit/abort、already-applied/replay
    mutation、checkpoint/recovered counterを報告する。
  - runtime inspection admissionはselected nonempty segmentを引き続き`ENOTSUP`で拒否する。
- 完了条件:
  - 未適用、bitmapのみ適用、bitmap+allocator summary適用、全target適用の同一transaction fixtureが、同じ
    recovered metadata/counterへoffline収束する。
  - torn selected prefix、record CRC破損、sequence gap、divergent duplicate、targetの第三状態を確実に拒否する。
  - byte-identical duplicateとabortはsequenceを正しく消費し、abortのdelta/patchをrecovered stateへ適用しない。
  - v6 public wire/layout entrypointへ依存せず、nonempty runtime mount、repair、controlled writeを有効化しない。
- 実装結果:
  - v7-owned packed wire recordsとcompile-time size/offset assertion、offline parser、global replay planner、
    memory overlayを追加した。
  - dedicated `v7_journal_replay_smoketest`が上記crash/rejection matrixとruntime fail-closed、fsck/dump reportを
    固定する。
- 検証結果（2026-07-16）:
  - `autoreconf -fi && ./configure && make -j2`: PASS。
  - `KAFS_TEST_MOUNT_TIMEOUT_MS=15000 make check -j2`: 35 PASS。
  - v7 entrypoint/raw-layout/multi-group/replica-fault/inspection-mount/journal-replayの6 smoke test: PASS。
  - `v7_journal_replay_smoketest`のValgrind definite/indirect leak gate: PASS。
  - `./scripts/format.sh`、`./scripts/lint.sh`、`./scripts/check-v7-layout-ownership.sh`、
    `git diff --check`: PASS。
  - `./scripts/static-checks.sh`はcloneだけnon-passing。strict clone gateは既存baselineと同じ87件・2.81%が
    1.0%閾値を超えたが、新規journal実装由来のcloneは検出されていない。

### SDW-V7RT-T8 v7-owned mutation routing proof

- 目的: journal encoderやruntime write admissionより先に、全v7 metadata targetをselected descriptorから
  canonicalなgroup-local物理範囲へ解決し、cross-group operationをjournal begin前に拒否する。
- 変更:
  - `src/kafs_v7_mutation.*`がblock bitmap word、inode、allocator summary、HRL index、HRL entryの
    global logical identityを、一意な`group_id`、shard、`physical_off`、`target_bytes`へ解決する。
  - bitmap wordの64-block canonical alignment、allocator summaryのgroup bitmap start identity、各fixed
    record幅、shard storage class、物理境界を検証する。
  - transaction plannerは全requestが同じgroupに属する場合だけrouteをoutputへ確定し、cross-groupは
    `EXDEV`、duplicate/overlapはfail closedにする。失敗時はroute/group出力を変更しない。
  - journal parser/replayのtarget検証も同じv7-owned resolverへ統合し、readerと将来writerの解決規則を
    分岐させない。
  - image mutation、journal record生成、checkpoint更新、runtime controlled write admissionは有効化しない。
- 完了条件:
  - 4-group imageの全groupについて5 target typeの先頭/末尾identityが正しいgroup/shard/物理範囲へ
    解決される。
  - 同一groupの5 target transactionは成功し、cross-group、duplicate、非canonical bitmap、範囲外identity、
    unknown typeを確実に拒否する。
  - 既存journal replay matrixが同じresolverを使用した状態で回帰しない。
- 実装結果:
  - v7-owned target/transaction routing APIとdedicated `v7_mutation_routing_smoketest`を追加した。
  - runtime mount/write境界は変更していない。
- 検証結果（2026-07-16）:
  - `autoreconf -fi && ./configure && make -j2`: PASS。
  - `v7_mutation_routing_smoketest`と`v7_journal_replay_smoketest`: PASS。
  - `KAFS_TEST_MOUNT_TIMEOUT_MS=15000 make check -j2`: 34 PASS、FUSE権限依存の2 test SKIP。
  - `v7_mutation_routing_smoketest`のValgrind definite/indirect leak gate: PASS。
  - `./scripts/format.sh`、`./scripts/lint.sh`、`./scripts/check-v7-layout-ownership.sh`、
    `git diff --check`: PASS。
  - `./scripts/static-checks.sh`はcloneだけnon-passing。strict clone gateは既存baselineと同じ87件・2.79%が
    1.0%閾値を超えたが、新規mutation router由来のcloneは検出されていない。

### SDW-V7RT-T9 replicated K7CP publication

- 目的: runtime write admissionを広げず、accepted raw-layoutの2-copy checkpoint publication順序と
  power-loss再開規則をv7-owned APIで固定する。
- 変更:
  - `src/kafs_v7_checkpoint.*`がfresh validationから出版計画を作り、先行metadata/journalをflush後、
    full checkpoint blockを1 copyずつwrite/flushする。
  - canonical 2-copyでは両copyを更新する。3-copyではselected old generationを残したまま、循環順の
    他2 copyへbyte-identicalな新世代を出版する。
  - 各write後のflushと全replica再読込を行い、同一blockが2 copy以上確認できた時だけ成功する。
  - power lossで新世代が1 copyだけ残った場合はgenerationを進めず、同じK7CP recordの2-copy目を
    補完する。未適用journal mutation、generation overflow、read-only FDはfail closedにする。
  - journal reclaim、concurrent writer locking、runtime controlled write admissionは有効化しない。
- 完了条件:
  - canonical 2-copyの通常出版後に新世代が2 copy一致し、image validatorが非degradedで再読込できる。
  - 1-copy出版直後を模擬したimageがdegradedで選択され、publisherが同世代を2-copyへ復旧する。
  - 3-copy出版ではold selected copyを保持したまま、他2 copyが新世代として選択される。
  - guard matrixと既存descriptor/checkpoint fault matrixが回帰しない。
- 実装結果:
  - v7-owned plan/publish APIとdedicated `v7_checkpoint_publication_smoketest`を追加した。
  - 既存`v7_replica_fault_smoketest`へ3-copy rotation proofを追加した。
  - runtime mount/write境界は変更していない。
- 検証結果（2026-07-16）:
  - `autoreconf -fi && ./configure && make -j2`: PASS。
  - `v7_checkpoint_publication_smoketest`と`v7_replica_fault_smoketest`: PASS。
  - `v7_checkpoint_publication_smoketest`のValgrind definite/indirect leak gate: PASS。
  - `KAFS_TEST_MOUNT_TIMEOUT_MS=15000 make check -j2`: 37 PASS。
  - `./scripts/format.sh`、`./scripts/lint.sh`、`./scripts/check-v7-layout-ownership.sh`、
    `git diff --check`: PASS。
  - `./scripts/static-checks.sh`はcloneだけnon-passing。strict clone gateは87件・2.78%で、直前baselineの
    87件・2.79%から件数増加はなく、新規checkpoint module由来のcloneは検出されていない。

### SDW-V7RT-T10 ranked v7 write and checkpoint locking

- 目的: runtime write admissionを広げず、v7 transactionとcheckpoint publicationを直列化する
  v7-owned lock順序、bounded wait、stale-owner failureを固定する。
- 変更:
  - `src/kafs_v7_locks.*`が`v7_write_gate` rank 1、`v7_sequence` rank 2、`v7_group` rank 3を所有する。
  - transaction composite APIはrank 1 -> 2 -> 3を取得し、逆順に解放する。groupはexactly oneに限定し、
    nested/cross-group取得とunlock mismatchをfail closedにする。
  - checkpoint publisherは`v7_write_gate`だけを取得し、進行中transactionと相互排他にする。
  - lock待ちは設定可能なtimeoutと定期owner確認を持ち、contention/wait統計とtimeout診断を出す。
    cancellationは保持中無効化し、Linux robust mutexのowner-deadはmutexを回復しても当該operationを
    `EOWNERDEAD`で失敗させる。
  - 正しさ優先のRC境界としてwrite gateは全transactionを直列化する。multi-group transaction、runtime
    controlled write、journal encoderは有効化しない。
  - v7 rank stackと既存metadata rank 10-50 stackは現時点で別管理であり、両familyを横断するadmitted pathは
    ない。writer接続前にcross-family order checkと逆順拒否regressionを追加する。
- 完了条件:
  - composite lockの正順/逆順解放、invalid group、nested acquisition、wrong unlockを検証する。
  - checkpoint holderとtransaction waiterのcontentionを実行し、待ち統計が増える。
  - timeoutがboundedで`ETIMEDOUT`となり、owner-dead後は当該取得を`EOWNERDEAD`で失敗させた後に再取得
    できる。
  - checkpoint通常出版、1-copy resume、3-copy rotation、既存replica fault matrixが回帰しない。
- 実装結果:
  - v7-owned opaque lock state/composite APIとdedicated `v7_locks_smoketest`を追加した。
  - checkpoint publisherはlock stateを必須とし、transaction中の再入を`EDEADLK`で拒否する。
  - runtime mount/write境界は変更していない。
- 検証結果（2026-07-16）:
  - `autoreconf -fi && ./configure && make -j2`: PASS。
  - `v7_locks_smoketest`、`v7_checkpoint_publication_smoketest`、
    `v7_replica_fault_smoketest`: PASS。
  - `KAFS_TEST_MOUNT_TIMEOUT_MS=15000 make check -j2`: 38 PASS。
  - `v7_locks_smoketest`と`v7_checkpoint_publication_smoketest`のValgrind definite/indirect leak gate:
    PASS（0 error、0 leak）。
  - `./scripts/format.sh`、`./scripts/lint.sh`、`./scripts/check-v7-layout-ownership.sh`、
    `git diff --check`: PASS。
  - `./scripts/static-checks.sh`はcloneだけnon-passing。strict clone gateは87件・2.75%で件数増加はなく、
    新規lock module由来のcloneは検出されていない。

### SDW-V7RT-T11 filesystem-global sequence reservation and publication confirmation

- 目的: journal encoderより先に、group-local journalへfilesystem-global sequenceをgapなく割り当て、
  durable publicationを確認するstate machineをv7-owned APIで固定する。
- 変更:
  - `src/kafs_v7_sequence.*`はvalidated viewの`max(checkpoint_seq, journal.last_sequence)`から開始し、
    composite transaction lockを保持したままexact next sequenceだけを予約する。
  - 予約は同一threadで完了または取消する。完了はfresh image validationを行い、予約sequenceが予約groupの
    selected journal prefixのlast sequenceとして見える場合だけ番号を消費する。
  - header公開前の取消はfresh validationでvisible sequenceが変化していないことを証明し、同じ番号の再利用を
    許す。確認不能、sequence/group不一致、validator failureはstateをpoisonし、後続予約を`EUCLEAN`で拒否する。
  - process restart/reopenはfresh validated viewから次番号を再構築する。sequence/token overflowはfail closedに
    する。
  - `kafs_v7_journal_report_t`はselected global last sequenceのgroup idを保持し、checkpointだけの異常前進や
    別groupへの誤出版をjournal公開成功と誤認しない。
  - test fixtureのjournal publicationもdata write/flush -> header write/flush順へ強化した。
  - journal record encoder、metadata apply、runtime controlled write admissionは有効化しない。
- 完了条件:
  - 未公開予約を取消すと同じsequenceを再予約できる。
  - group 0のsequence 1を公開確認するまでgroup 1の予約はtimeoutし、確認後はgroup 1へsequence 2を予約・
    公開できる。
  - state再初期化後はdurable viewからsequence 3を予約できる。
  - invalid group、sequence overflow、予約groupと公開groupの不一致をfail closedにし、不一致後のstateを
    poisonする。
- 実装結果:
  - v7-owned sequence state/reservation APIを追加し、既存rank 1 -> 2 -> 3 composite lockと統合した。
  - `v7_journal_replay_smoketest`へcancel/reuse、cross-group serialization、restart、overflow、poison matrixを
    追加した。
  - runtime mount/write境界は変更していない。
- 検証結果（2026-07-16）:
  - `autoreconf -fi && ./configure`と`kafs-v7`を含むbuild: PASS。
  - `v7_journal_replay_smoketest`、`v7_raw_layout_smoketest`、
    `v7_checkpoint_publication_smoketest`: PASS。
  - `KAFS_TEST_MOUNT_TIMEOUT_MS=15000 make check -j2`: 38 PASS。
  - `v7_journal_replay_smoketest`のValgrind: PASS（0 error、0 leak）。
  - `./scripts/format.sh`、`./scripts/lint.sh`、`./scripts/check-v7-layout-ownership.sh`、
    `git diff --check`: PASS。
  - `./scripts/static-checks.sh`はcloneだけnon-passing。strict clone gateは既存baselineと同じ87件・2.74%で、
    新規sequence module由来のcloneは検出されていない。

### SDW-V7RT-T12 journal encoder and data-before-header publication

- 目的: active filesystem-global sequence reservationをgroup-local journalのcanonical wire recordへ変換し、
  crash時に未flush dataやtorn headerをselected prefixとして誤認しない順序で公開する。
- 変更:
  - `src/kafs_v7_journal_writer.*`は既存journal overlay後の論理targetをbefore stateとして読み、partial patch後の
    before/after CRC、mutation stream CRC、control deltaを含む`K7JB/K7JM/K7JC/K7JA`をencodeする。
  - encoderは既存mutation routerとreader共通のdelta validatorを使い、single-group、非重複target、bitmap/inode
    のexact free counter deltaをheader公開前に検証する。
  - opaque transactionはactive sequence/token/groupとencode threadへbindし、取消済み、別token、別threadの
    publicationを拒否する。
  - reader/writer共通のsegment snapshotは各header blockからCRC-validなhighest generationだけを選ぶ。
  - publisherはfresh replayでexact next sequenceを再検証し、容量のあるsegmentのうちselected header generationが
    最小のものへappendする。transaction data write -> `fdatasync` -> rotated `K7JH` write -> `fdatasync`の順を固定する。
  - metadata target apply、checkpoint連携、journal reclamation、cross-family runtime lock integration、controlled-write
    admissionは有効化しない。
- 完了条件:
  - COMMIT/ABORTをwriterだけで生成・公開し、fresh readerがglobal sequence、group、mutation/counter、overlay stateを
    byte-exactに復元できる。
  - dataだけをflushしてheaderを公開しないcrash stateはempty journalとして扱われる。
  - single-groupの2 segmentは連続publicationで異なるsegmentを選び、1-segment-per-group geometryではheader slotを
    generation順に回す。
  - wrong token、in-rangeだが不正なexact delta、cross-group target、stale sequenceをimage/output不変で拒否する。
- 実装結果:
  - v7-owned encoder/publisher API、共有segment selector、incremental CRC/delta helperを追加した。
  - `v7_journal_replay_smoketest`へdata-only crash、writer round-trip、COMMIT/ABORT、overlay chaining、segment/header
    rotation、multi-group sequence、token/delta/routing rejectionを追加した。
  - runtime mount/write境界は変更していない。
- 検証結果（2026-07-16）:
  - `autoreconf -fi && ./configure`、`kafs-v7` build: PASS。
  - `make -C tests check TESTS=v7_journal_replay_smoketest`: 1 PASS。
  - `KAFS_TEST_MOUNT_TIMEOUT_MS=15000 make check -j2`: 37 PASS、FUSE mount権限依存の
    `stress_fs` 1 test SKIP。
  - `v7_journal_replay_smoketest`のValgrind: PASS（0 error、0 leak）。
  - clangd diagnostics（writer、journal/layout/mutation、test）: 0件。
  - `./scripts/format.sh`、`./scripts/lint.sh`、`./scripts/check-v7-layout-ownership.sh`、
    `git diff --check`: PASS。
  - `./scripts/static-checks.sh`はcloneだけnon-passing。strict clone gateは既存baselineと同じ87件・2.69%で、
    writer由来のcloneは0件。writerのcomplexity warningは責務分割後0件。

### SDW-V7RT-T13 multi-group mutation fault matrix

- 目的: group-local journalがfilesystem-global sequenceで連結される条件を4 groupで固定し、1 groupの
  payload/header損失やforeign-group mutationを部分成功として扱わずfail closedにする。
- 変更:
  - group順`0,3,1,2`、sequence順`1,2,3,4`の正常interleaveを追加し、4 groupのrecovered counterと
    last-sequence ownerを検証した。
  - 異なるgroupに同一sequenceを置くcollisionと、groupをまたぐsequence gapを拒否するmatrixを追加した。
  - sequence 1/2/3の中央groupについて、mutation payload corruptionとlatest header CRC corruptionを注入した。
    header corruptionでは旧empty headerへのfallback後にglobal sequence gapとしてfail closedになることを固定した。
  - mutationのgroup idをforeign groupへ変更し、mutation-stream/control/record CRCを再計算した入力も拒否する。
  - cross-group atomic transactionは有効化せず、transactionは引き続きexactly one groupに限定する。
- 完了条件:
  - 正常interleaveだけがreplay可能で、collision、gap、中央group data/header loss、checksum-consistent foreign
    mutationは全image validationを失敗させる。
  - runtime mount/write境界とcross-group HRL policyは変更しない。
- 検証結果（2026-07-16）:
  - `v7_journal_replay_smoketest`: PASS。
  - 同testのValgrind: PASS（0 error、0 leak、2,273 alloc/free）。
  - clangd diagnostics: 0件。
  - `./scripts/format.sh`、`./scripts/lint.sh`、`./scripts/check-v7-layout-ownership.sh`、
    `git diff --check`: PASS。
  - `KAFS_TEST_MOUNT_TIMEOUT_MS=15000 make check -j2`: 38 PASS（`stress_fs`を含む）。
  - `./scripts/static-checks.sh`はcloneだけnon-passing。strict clone gateは既存baselineと同じ
    87件・2.69%で、production `src/`は変更していない。

### SDW-V7RT-T14 metadata apply/checkpoint/reclamation closeout

- 目的: flushed journal transactionをmetadataへ冪等適用し、2-copy checkpointがそのsequenceを覆った後だけ
  group-local journalを再利用可能にするdurability closeoutをv7-owned APIとして固定する。
- 変更:
  - replay解析時に各targetの現在chain stageを保持し、apply直前のraw target CRCが同じstageか再確認する。
    committed after-imageをtarget単位で書き、metadata全体を`fdatasync`した後にread-backする。
  - checkpoint/write gateを1回だけ保持するcoordinatorを追加した。selected-generation checkpointが1コピーなら
    metadata変更前に同世代2コピーへ復旧し、metadata apply後にjournal最終sequenceを覆う新checkpointを2コピー
    publishする。
  - 全segmentを先にpreflightし、checkpointより新しいsequenceが1件でもあれば無変更でreclaimを拒否する。
    covered segmentはdataを消去せず、generationを進めたempty `K7JH`を1segmentずつflush/read-backする。
  - commit、abort-only、metadata apply直後、checkpoint 1コピー直後、checkpoint前reclaim拒否、1segmentだけ
    reset済みの再開をfocused smokeへ追加した。closeoutの再実行はcheckpoint generationを不要に進めない。
  - runtime mount/write admissionとcross-group atomic transactionは有効化していない。
- 完了条件:
  - `journal data/header -> metadata -> checkpoint replica 2 copies -> empty journal header`のdurability順を維持する。
  - crash後のraw targetはmutation chain上のstageだけを受理し、第三状態はfail closedにする。
  - checkpoint 2コピーが`last_sequence`を覆う前はjournal dataをreclaimしない。
- 検証結果（2026-07-16）:
  - `v7_checkpoint_publication_smoketest`: PASS。
  - 同testのValgrind: PASS（0 error、0 leak、3,597 alloc/free）。
  - clangd diagnostics（journal、checkpoint、layout、test）: 0件。
  - `./scripts/format.sh`、`./scripts/lint.sh`、`./scripts/check-v7-layout-ownership.sh`、
    `git diff --check`: PASS。
  - `KAFS_TEST_MOUNT_TIMEOUT_MS=15000 make check -j2`: 37 PASS、1 SKIP
    （`stress_fs`: FUSE mount permission不足）。
  - `./scripts/static-checks.sh`はcloneだけnon-passing。strict clone gateは既存baselineと同じ
    87件・2.67%で、closeout由来のcloneとcomplexity warningは0件。

### SDW-V7RT-T15 cross-family lock order integration

- 目的: v7 rank 1-3と既存metadata rank 10-50を同一thread-local stackで検証し、将来のruntime
  mutation pathが`v7_write_gate -> v7_sequence -> v7_group -> metadata locks`の順序を外れた場合に
  mutex取得前にfail closedできるようにする。
- 変更:
  - format-neutral `kafs_lock_order` trackerへrankとmutex identityを記録する。v7 wrapperと既存metadata
    wrapperは別々のrank stackを持たず、同じLIFO stackでcross-family acquisition/releaseを検証する。
  - v7 rankは同rankのnested acquisitionを拒否し、既存metadata側は複数inodeなどの同rank acquisitionを
    維持する。stack overflow、underflow、identity/rank mismatchも拒否する。
  - metadata rankを保持したthreadからのv7 acquisitionはmutexを触る前に`EDEADLK`を返す。v7 transaction
    保持中のmetadata rank 10-50取得は許可し、逆順解放を共通trackerで検証する。
  - `v7_locks_smoketest`で`hrl_global`、`inode_alloc`、`inode`、`hrl_bucket`、`bitmap`の全classについて
    逆順拒否と正順取得を実行する。runtime controlled-write admissionは有効化しない。
- 完了条件:
  - metadata rank 10-50のいずれかを保持中はv7 checkpoint/transaction開始を`EDEADLK`で拒否する。
  - v7 composite transaction中は既存metadata lockを正順に取得・解放でき、最終stack depthが0になる。
  - production、v6/v7、offline tool、全test targetが同じneutral tracker implementationをlinkする。
- 検証結果（2026-07-16）:
  - `v7_locks_smoketest`: PASS（全metadata rank classのcross-family orderを含む）。
  - `make -C src kafs-v7 -j2`: PASS。
  - shared trackerの全体適用で検出した既存の同rank inode解放順違反は、directory block置換時の
    HRL参照をoutermost inode unlockまでdeferし、2-inode copyを取得順の厳密な逆順で解放するよう修正した。
    strict identity LIFO検証は緩和していない。
  - `KAFS_TEST_MOUNT_TIMEOUT_MS=15000 make check -j2`: 38 PASS。
  - `v7_locks_smoketest`のValgrind: PASS（0 error、0 leak）。
  - clangd diagnostics（neutral tracker、既存/v7 wrapper、test）: 0件。
  - `./scripts/format.sh`、`./scripts/lint.sh`、`./scripts/check-v7-layout-ownership.sh`、
    `git diff --check`: PASS。
  - `./scripts/static-checks.sh`はcloneだけnon-passing。strict clone gateは既存baselineと同じ87件・2.66%で、
    neutral tracker/v7 wrapper由来のcloneとcomplexity warningは0件。

### SDW-V7RT-T16 v7-owned runtime mutation/admission policy

- 目的: v7 controlled-write context setupからv6-owned state/helper依存を除去し、write admissionを開く前に
  v7固有のfail-closed policy境界を固定する。
- 変更:
  - `kafs_context`へ`c_v7_controlled_write_enabled`を追加し、`kafs_v7_runtime_admit_mount_context()`は
    v6 flagを変更せず、v7-owned helperだけでpolicy stateを初期化・設定する。
  - `kafs_v7_fuse_policy.h`はpolicy無効時を`EROFS`、未知operationを`EOPNOTSUPP`とし、将来接続する
    surfaceを`create` / regular-file `write` / `fsync` / `release`の4操作だけに限定する。
  - `check-v7-runtime-policy-ownership.sh`でv7 runtime/policyへのv6 controlled-write flag/helper/include再混入を
    拒否する。v6の既存state/helper/entrypoint behaviorは変更しない。
  - v7 controlled-write entrypointは引き続きFUSE開始前に拒否する。4操作のpolicy許可は将来の接続境界であり、
    このticketではruntime image mutationを有効化しない。
- 完了条件:
  - v7 policy有効化がv6 flagを変更せず、無効・未知・許可対象のdecision matrixをfocused testで検証する。
  - v7 runtime/policy ownership gate、v7/v6 admission regression、full regressionがPASSする。
  - write admissionと未列挙mutation surfaceはfail closedのままとする。
- 検証結果（2026-07-17）:
  - `autoreconf -fi && ./configure && make -j2`: PASS（`-Wall -Werror` build）。
  - `v7_entrypoint_smoketest v6_descriptor_smoketest`: 2/2 PASS。
  - `v7_entrypoint_smoketest`のValgrind: PASS（0 error、0 leak、39 allocs/39 frees）。
  - `KAFS_TEST_MOUNT_TIMEOUT_MS=15000 make check -j2`: 38/38 PASS。
  - clangd diagnostics（v7 policy、v7 runtime、focused test）: 0件。
  - `./scripts/format.sh`、`./scripts/lint.sh`、`./scripts/check-v7-layout-ownership.sh`、
    `./scripts/check-v7-runtime-policy-ownership.sh`、`git diff --check`: PASS。
  - `./scripts/static-checks.sh`はcloneだけnon-passing。strict clone gateは既存baselineと同じ87件・2.66%で、
    v7 policy由来のcloneとcomplexity warningは0件。

### SDW-V7RT-T17 mount-lifetime transaction coordinator and fail-closed FUSE boundary

- 目的: T10-T16で個別に固定したlock、sequence、journal publication、metadata closeoutをmount-lifetimeの
  v7-owned serviceへ統合し、FUSE接続前にlegacy mutationへのfallthroughを閉じる。
- 変更:
  - `kafs_v7_runtime_transaction`はmountごとにrank 1-3 lock stateとfilesystem-global sequence stateを保持する。
    transactionをexactly one groupへrouteし、journal publish/confirm後にmetadata apply、2-copy checkpoint、
    covered journal reclamationまで完了してから成功を返す。read-onlyまたは`O_APPEND` FDは初期化時に拒否する。
  - controlled-write service初期化からlegacy v4/v5 runtime journalを除去し、v7 coordinatorの生成・破棄を
    `kafs-v7` adapter lifetimeへ接続した。production `kafs`とfrozen `kafs-v6`にはv7 source/macroをlinkしない。
  - v7 controlled contextのshared FUSE legacy mutation guardは`EOPNOTSUPP`を返す。`O_TRUNC`、control-plane open、
    `fsyncdir`もfail closedとし、regular-file `fsync` / `release`だけをv7 closeout barrierへ接続した。
    `create` / regular-file `write`はv7 data/metadata plannerが完成するまでlegacy実装へ進めない。
  - focused regressionは同一serviceでsequence 1/2を連続commitし、各commitのmetadata反映、checkpoint世代前進、
    journal空化とbarrier冪等性を検証する。cross-group patchは`EXDEV`かつimage無変更に固定した。
  - checkpointとcoordinatorに重複していたpositional-write FD条件を`kafs_v7_io.h`へ抽出し、新規clone増分を除去した。
- 完了条件:
  - single-group transactionのdurability順が
    `journal data/header -> metadata -> 2-copy checkpoint -> journal reclaim`を外れない。
  - 同一mountのsequence stateを継続利用でき、cross-group、read-only、append FDはpublication前にfail closedとなる。
  - v7 controlled contextからlegacy mutation実装へ到達せず、v4/v5/v6 runtime behaviorを変更しない。
  - controlled-write entrypointは引き続きFUSE開始前に拒否し、runtime data writeを有効化しない。
- 検証結果（2026-07-17）:
  - `autoreconf -fi && ./configure && make -j2`: PASS（`-Wall -Werror` build）。
  - `v7_checkpoint_publication_smoketest v7_entrypoint_smoketest v6_descriptor_smoketest`: 3/3 PASS。
  - `KAFS_TEST_MOUNT_TIMEOUT_MS=15000 make check -j2`: 38/38 PASS。
  - `v7_checkpoint_publication_smoketest`のValgrind: PASS（0 error、0 leak、5,151 allocs/frees）。
  - clangd diagnostics（coordinator、policy、runtime/FUSE接続、focused tests）: 0件。
  - `./scripts/format.sh`、`./scripts/lint.sh`、両v7 ownership check、`git diff --check`: PASS。
  - `./scripts/static-checks.sh`はcloneだけnon-passing。strict clone gateは既存baselineの87件・1,246行
    （2.64%）で、新規v7 coordinator/IO helper由来のcloneとcomplexity warningは0件。

### SDW-V7RT-T18 v7-owned data-block COW and allocator planner

- 目的: v7 group-local data allocationとfull-block COWをT17 coordinatorのrank 1-3 reservation内で計画し、
  new dataがmetadata pointer/allocator publicationより先にdurableとなる境界を固定する。
- 変更:
  - `kafs_v7_data_cow`はjournal overlay上のauthoritative bitmapとcomplete unpadded L1/L2 allocator summaryを
    読み、両者の一致を検証してからgroup-local free blockをmount-lifetime cursor起点で巡回選択する。
    bitmap wordとallocator summaryのafter-imageを1つのsingle-group transactionへ追加する。
  - runtime coordinatorはallocation planningからfull-block write、`fdatasync`、read-back、metadata journal
    publicationまで同じrank 1 -> 2 -> 3 reservationを保持する。publication直前にもstaged dataを再読し、
    callerのdirect inode slot patchが選択blockを指すことを確認する。
  - overwriteでは同じinode direct slotのbefore-imageが指定旧blockを指すことを確認する。旧blockはnew pointerを
    覆う2-copy checkpoint後もallocatedのまま残し、このticketでは解放しない。abortまたはdata検証失敗時は
    metadata/bitmapを変更せず、free data spanに残ったstaged bytesは参照不能のままとする。
  - cursorはdata stage成功時に進めるため、abortやpublication前failureでも同一physical blockを繰り返し叩かない。
    indirect reference、multi-block write、directory mutation、FUSE `create` / `write` admissionは有効化しない。
  - 既存metadata-only transactionもreservation取得後にlayoutをfresh validateし直し、pre-lock viewをpublicationに
    使用しないようにした。
- 完了条件:
  - data write/read-back/flush前にはbitmap、inode、checkpointが変化せず、commit後だけ選択blockとdirect inode
    referenceが同じcovering checkpointで可視になる。
  - abort、missing/wrong direct reference、cross-group retained block、publication前のstaged-data corruptionを
    fail closedにし、journal/allocator counterを進めない。
  - 成功overwrite後はnew/old両blockがallocatedで、旧blockの解放は次ticketの明示的retirementだけが行う。
- 検証結果（2026-07-17）:
  - `autoreconf -fi && ./configure && make -j2`: PASS（`-Wall -Werror` build）。
  - `v7_checkpoint_publication_smoketest v7_entrypoint_smoketest v6_descriptor_smoketest`: 3/3 PASS。
  - `KAFS_TEST_MOUNT_TIMEOUT_MS=15000 make check -j2`: 38/38 PASS。
  - `v7_checkpoint_publication_smoketest`のValgrind: PASS（0 error、0 leak、7,439 allocs/frees）。
  - clangd-18 diagnostics（data COW、runtime transaction、公開header、focused test）: 0件。
  - `./scripts/format.sh`、`./scripts/lint.sh`、両v7 ownership check、`git diff --check`: PASS。
  - `./scripts/static-checks.sh`はcloneだけnon-passing。strict clone gateは既存baselineの87件・1,246行
    （2.60%）で、新規data COW/transaction module由来のcloneは0件。新規2 moduleのlizard threshold warningも0件。

### SDW-V7RT-T19 retained data-block retirement and retry closeout

- 目的: T18 overwrite後に保持した旧data blockを、covering checkpoint後の独立transactionで安全に解放し、
  allocation/COW/retirement lifecycleを閉じる。
- 変更:
  - `kafs_v7_data_cow`のallocator after-image生成をallocation/retirementで共有し、retirementでは対象groupの
    allocated bitだけをclearしてcomplete L1/L2 summaryを再構築する。bitmap patchは
    `free_blocks_delta=+1`となり、既にfreeなら`EALREADY`、foreign-group blockなら`EXDEV`を返す。
  - runtime retirementは最初に既存durable journal prefixをcloseoutし、rank 1 -> 2 -> 3 reservation下で
    fresh layout/replayを検証する。journalが空で2-copy checkpointが揃う場合だけretirementを計画する。
  - 全groupのinode tableとHRL entriesをjournal overlay経由で走査する。対象へのlive direct/HRL referenceは
    `EBUSY`、どこかに非0 indirect rootがある場合はindirect traversal実装まで`EOPNOTSUPP`でfail closedとする。
  - 参照なしを確認したbitmap/summary patchをsingle-group transactionとしてpublishし、metadata apply、
    2-copy checkpoint、journal reclamationまで完了してから成功を返す。公開済みretirementを伴う再起動後の
    retryはpreflight closeoutで収束し、その後`EALREADY`を返す。
  - regressionはlive direct guard、indirect guard、cross-group guard、正常解放と空き数回復、同一process retry、
    journal公開後かつcloseout前の再起動retryを固定する。FUSE controlled-write admissionは変更しない。
- 完了条件:
  - allocatedかつ全direct/HRL参照から外れたgroup-local blockだけが解放され、bitmap、summary、free counterが
    同じcovering checkpointで一致する。
  - live reference、indirect root、foreign-group、already-freeの各状態でallocator metadataを変更しない。
  - retirement transaction公開後の中断を再起動時にcloseoutでき、二重解放やcounter二重加算が起きない。
- 検証結果（2026-07-17）:
  - `make -j2`: PASS（`-Wall -Werror` build）。
  - `v7_checkpoint_publication_smoketest`: PASS。
  - `KAFS_TEST_MOUNT_TIMEOUT_MS=15000 make check -j2`: 36 PASS / 2 SKIP。`min_git_hooks`と`stress_fs`は
    この環境のFUSE mount権限不足でSKIPした。
  - `v7_checkpoint_publication_smoketest`のValgrind: PASS（0 error、0 leak、12,471 allocs/frees）。
  - clangd-18 diagnostics（allocator/retirement、runtime transaction、両header、focused test）: 0件。
  - `./scripts/format.sh`、`./scripts/lint.sh`、両v7 ownership check、`git diff --check`: PASS。
  - `./scripts/static-checks.sh`はcloneだけnon-passing。strict clone gateは既存baselineの87件・1,246行
    （2.58%）で、今回変更したmodule由来のcloneは0件。新規retirement関数のlizard threshold warningも0件。

### SDW-V7RT-T20 bounded aligned direct overwrite adapter

- 目的: 既存regular fileのaligned full-block direct overwriteだけをv7-owned FUSE write surfaceからT17-T19
  coordinatorへ接続し、partial writeや未実装のmetadata mutationを混入させない。
- 変更:
  - `kafs_v7_fuse_write` adapterを追加し、controlled-write contextで既存regular file、既存サイズ内、block境界、
    direct slot、既存block参照の条件だけを受理する。
  - T18のfull-block COW、inode direct-reference transaction、T19の旧block retirementを順序どおり接続した。
  - shared FUSE `write`はv7 buildだけadapterへ分岐し、v4/v5/v6のwrite pathとhotplug pathは変更しない。
  - focused smoke testで境界外write拒否、data read-back、direct reference更新、旧block解放、fsck整合性を固定した。
- 完了条件:
  - partial-block、growth、indirect/multi-block、createを`EOPNOTSUPP`で拒否する。
  - data durability、metadata pointer、checkpoint、retirementの順序をT17-T19 API経由で維持する。
  - `make check -j2` と focused v7 write test がPASSする。
- 検証結果（2026-07-19）:
  - `autoreconf -fi && ./configure && make -j2`: PASS（`-Wall -Werror`）。
  - `v7_fuse_write_smoketest`、既存v7/v6 focused 4 tests: PASS。
  - `KAFS_TEST_MOUNT_TIMEOUT_MS=15000 make check -j2`: all 39 tests passed。

### SDW-V7RT-T21 controlled-write FUSE admission matrix

- 目的: T20 adapterを実際の`kafs-v7 --controlled-write-mount`から到達可能にし、FUSE境界と永続化を
  end-to-endで検証する。
- 変更:
  - controlled-writeではimage FDをread-writeで開く一方、descriptor-backed mmapはread-onlyのまま保持する。
  - shared FUSE initはv7 buildでv7-owned worker policyを検証し、通常ファイルwriteはlegacy mutation guardより
    前にT20 adapterへrouteする。control-planeと未列挙mutationは引き続き`EOPNOTSUPP`で拒否する。
  - inspection fixtureを複製して実マウントし、partial write拒否、aligned full-block overwrite、full fsync、
    unmount、fsck、offline descriptor/inode/data read-backを検証する。
  - write後のnonzero checkpoint sequenceは、journalが空でdescriptor/checkpoint validationが通るclean
    closeout状態に限ってinspection再マウントを許可する。non-empty journalは引き続きfail closedとする。
- 完了条件:
  - explicit safe mount optionsなしではcontrolled-write admissionを拒否する。
  - 実FUSE経由でT20の限定writeだけが成功し、unmount後のfsckとraw dataが一致する。
  - v4/v5/v6 runtime behaviorとv7 inspection-only behaviorを変更しない。

### SDW-V7RT-T22 controlled-write admission recovery

- 目的: full-block overwriteのdurable journal publication後にprocess/power interruptionが発生したimageを、
  次回controlled-write admissionで安全にcloseoutして再利用可能にする。
- 変更:
  - controlled-write admissionはruntime mmap構築前にraw v7 layoutを検証し、non-empty journalがある場合だけ
    一時v7 lock state下でmetadata apply、checkpoint redundancy/publication、covered journal reclaimを再開する。
  - closeout後にimageを再検証し、journalが空にならなければFUSE開始前にfail closedとする。inspection preflightは
    read-onlyのままでnon-empty journalを変更せず拒否する。
  - test-only crash hookでdata COW journal publication直後にFUSE serverを終了し、同じimageのcontrolled-write
    再起動、fsck、raw inode/data read-backまでを実mount smokeで検証する。
- 完了条件:
  - committed journal prefixは次回controlled-write admissionでexactly-onceに収束する。
  - incomplete/invalid journal、descriptor/checkpoint corruptionは修復対象にせずfail closedとする。
  - recoveryを伴わないmountとv4/v5/v6 runtime behaviorを変更しない。

### SDW-V7RT-T23 controlled-write closeout interruption matrix

- 目的: T22の実FUSE recoveryをmetadata apply後とcheckpoint replica 1コピー後へ拡張し、closeoutの
  durability境界をend-to-endで固定する。
- 変更:
  - test-only crash hookをmetadata applyのflush完了後と、最初のcheckpoint blockのwrite/flush完了後に追加する。
  - pristine fixtureをケースごとに複製し、full-block write中のserver停止、controlled-write再起動、fsck、
    raw inode/data read-backをjournal publication、metadata apply、checkpoint 1コピーの3境界で共通検証する。
  - metadata apply済みmutationは再適用せず、checkpoint片側状態は同generationの不足replicaだけを補完してから
    covered journalをreclaimする。
- 完了条件:
  - 3境界のいずれでもcommitted data/inode updateがexactly-onceに収束する。
  - checkpoint generation/sequenceを不要に進めず、最終的に2-copy checkpointと空journalを得る。
  - 通常mountではtest hookが無効で、既存durability順を変更しない。

### SDW-V7RT-T24 multi-segment journal reclaim interruption

- 目的: 複数groupのcovered journal segmentをreclaimしている途中のprocess interruptionを実FUSE admission
  recoveryで再開し、durability matrixを閉じる。
- 変更:
  - inspection mount smokeへv7 sequence/journal writerをlinkし、production APIで異なる2 groupのinode patchを
    committed journal transactionとしてpublishするfixtureを追加する。
  - test-only hookで最初のnon-empty segmentをempty headerへflush/read-backした直後にserverを終了する。
  - raw validatorでnon-empty segmentが2から1へ減ったことを確認し、次のcontrolled-write admissionで残り1件だけを
    reclaimする。最終状態はjournal 0件とfsck PASSを要求する。
- 完了条件:
  - checkpointが全transactionをcoverする前はreclaimせず、cover後は各segmentを独立に再実行可能とする。
  - 既にemptyのsegment generationを不要に進めず、残存segmentだけをresetする。
  - journal publication、metadata apply、checkpoint 1コピー、partial reclaimの全境界が実FUSE再起動で収束する。

### SDW-V7RT-T25 shared recovery fault-point API

- 目的: T22-T24で各moduleへ個別追加したtest-only crash hookを共通contractへ統合し、stage選択と診断を
  一貫させる。
- 変更:
  - `kafs_v7_test_fault.h`へ`journal_publish`、`checkpoint_copy`、`metadata_apply`、`journal_reclaim`のenum、
    stable name、exit status 86-89、単一`KAFS_V7_TEST_CRASH_POINT` dispatcherを集約する。
  - transaction/checkpoint/journal moduleは個別の環境変数解析と`_exit`を持たず、durability boundaryで共通
    dispatcherだけを呼ぶ。
  - mount testはwrite中断childとmount前admission中断childのexit statusを明示的に照合し、別stageでの偶発停止を
    recovery成功として扱わない。
- 完了条件:
  - fault point名とexit statusの対応が1 headerだけで定義される。
  - 未指定/未知stageではproduction pathに副作用がなく、全4stageの実FUSE matrixがPASSする。
  - test helperはmount前early exit statusをcallerへ返せる。

### SDW-V7RT-T26 machine-readable recovery diagnostics

- 目的: controlled-write admission recoveryの再開位置と実行量を安定したkey/value診断へ集約し、
  各durability boundaryが期待した経路だけを通ることを実FUSE testで固定する。
- 変更:
  - recovery完了時に`status`、`format`、`trigger`、`resume_from`、初期/最終checkpoint状態、metadata apply、
    checkpoint publication/resume、journal reclaimの各counterを1行で出力する。
  - checkpoint replica数、checkpoint sequence、journal replay結果から`checkpoint_copy`、`journal_publish`、
    `metadata_apply`、`journal_reclaim`の再開位置を分類する。
  - 全4 fault caseで独立したrecovery logを採取し、stage、apply済みmutation数、checkpoint resume数、
    reclaim数、最終journal空状態を照合する。
- 完了条件:
  - recovery診断は機械的に解析できるstable key/value形式で、全4stageを区別できる。
  - counterはidempotent replayを含む実際の処理量を表し、各fault caseの期待値と一致する。
  - recoveryを伴わないmountの出力とdurability順を変更しない。

### SDW-V7RT-T27 recovery diagnostic contract API

- 目的: T26のmachine-readable recovery summaryをproductionとtestが共有する型付きcontractへ移し、
  診断形式の意図しないdriftを検出する。
- 変更:
  - `kafs_v7_recovery_diagnostic`へresume stage enum、summary struct、writer、strict parserを集約する。
  - parserは全必須key、固定status/format/trigger、resume stage、unsigned数値範囲を検証し、keyの重複、欠落、
    未知key/value、不正tokenを拒否する。
  - production recoveryは型付きsummaryをwriterへ渡し、実FUSE smokeはlogをparserで読み戻してcounterを照合する。
    独立contract testはround-tripとmalformed input matrixを検証する。
- 完了条件:
  - productionとtestに診断key/valueの手書きparserまたはformatterを残さない。
  - duplicate/missing/unknown/range overflowをfail closedで拒否する。
  - T26の全4stage recovery logが同じAPIでparseされ、期待counterと一致する。

### SDW-V7RT-T28 offline recovery log inspection

- 目的: 保存したv7 controlled-write recovery logをoperatorがimageやmountを変更せず検証し、text/JSONで
  再利用できるread-only診断経路を提供する。
- 変更:
  - `kafsdump [--json] --recovery-log <log>`を追加し、通常のoffline image inspectionとは排他的なinput modeにする。
  - recovery diagnostic APIへstream readerを追加し、通常のmount出力からmachine-readable recordを探索して
    strict parserへ渡す。record欠落、malformed、長すぎるrecord、I/O errorはfail closedとする。
  - textはresume stageとinitial/apply/checkpoint/reclaim/final summaryを表示し、JSONはstable top-level
    `recovery` objectへ全contract fieldを出力する。
  - 実FUSEの全4 fault caseで保存されたlogを`kafsdump`のtext/JSON両方から再検証する。
- 完了条件:
  - logはread-onlyで開かれ、inspectionによる内容変更がない。
  - valid recovery recordはtext/JSONで同じstage/counterを返し、不正recordはexit status 1になる。
  - image inspectionの既存CLI、text/JSON schema、終了statusを変更しない。

### SDW-V7RT-T29 v5-to-v7 migration target creation

- 目的: clean v5 sourceからaccepted v7 destinationを作るoffline cutover入口を`kafsresize`へ追加し、
  destination geometryとsource収容量を作成前に検証する。
- 変更:
  - v7 layout ownerへwrite-free `kafs_v7_mkfs_plan` APIを追加し、mkfsと同じgroup/replica/data capacity
    計算をdry-runとpreflightで共有する。
  - `kafsresize --migrate-create --src-image <v5> --format-version 7`を許可し、clean source、inode count、
    source used data bytes、v7 descriptor/group/journal geometryをdestination overwrite前に検証する。
  - dry-runはv7 descriptor bytes、replica count、group count、journal segment count、group placement policyを
    出力し、destinationへ書き込まない。
  - 実作成後にsource `kafsdump --json`不変、destination accepted v7 superblock、`kafsdump --json`、
    `fsck.kafs --check`を回帰検証する。source未指定はdestinationを変更せずfail closedとする。
- 完了条件:
  - v7 migration planと`mkfs.kafs --format-version 7`のgeometryが同じv7-owned plannerから得られる。
  - source/destination block sizeが異なる場合もdata capacityをbytesで比較する。
  - 作成されたdestinationはaccepted offline/inspection surfaceで検証可能で、source imageを変更しない。

### SDW-V7RT-T30 bounded partial-block overwrite

- 目的: v7 controlled-writeを既存allocated direct block内のpartial overwriteへ拡張し、既存byteを保持した
  read-modify-COWをfull-block transactionと同じdurability contractで実行する。
- 変更:
  - write validationは1 direct block内、現在のfile size内、nonzero existing referenceに限定してpartial rangeを許可する。
    block境界跨ぎ、hole、indirect block、file growthは引き続き`EOPNOTSUPP`/`EFBIG`で拒否する。
  - partial writeはretained blockをpositional readし、request rangeだけをmergeしたfull-block bufferをdata COWへstageする。
    journal publication、inode reference replacement、checkpoint、retained block retirementの順序は変更しない。
  - unit smokeでprefix/suffix保持、new logical block、old block retirement、full-block互換、境界拒否を検証する。
  - 実FUSE smokeでpartial write/full fsync/offline readback/inspection remountを検証し、journal publication、
    metadata apply、checkpoint copy、journal reclaimの全中断段階をpartial payloadで再実行する。
- 完了条件:
  - request外byteがretained blockと一致し、request内byteだけが置換される。
  - partial transactionはpower interruption後もexactly-onceに収束し、fsckとrecovery diagnosticsがPASSする。
  - full-block overwriteを維持し、file growth、multi-block、indirect writeを暗黙に許可しない。

### SDW-V7RT-T31 atomic multi-block direct overwrite

- 目的: v7 controlled-writeを既存allocated direct block間のmulti-block overwriteへ拡張し、全data COWと
  inode reference replacementを単一journal transactionでpublishする。
- 変更:
  - batch allocator plannerは同一groupのoverlay bitmapへ最大12 direct blockの割当を累積し、重複する
    bitmap wordを最終状態へ集約してallocator summaryとfree-block deltaを一度だけpublishする。
  - batch runtime transactionは1 sequence reservation内で全replacement blockをstage、flush、readback検証し、
    全new direct referencesを含むinode patchとallocator patchesをatomicにcommitする。
  - FUSE writeは先頭・末尾のpartial rangeをretained blockからmergeし、中間blockを含むrequest全体をbatchへ渡す。
    inode切替のcheckpoint完了後に旧blockを個別retirement transactionで解放する。
  - smoke testは3-block fileを作成し、3 blockに跨るpartial/full/partial payload、全reference切替、payload保持、
    retirement後のfsckを検証する。
- 完了条件:
  - crash前には旧references、journal publication後には全new referencesとして回復し、混在状態を公開しない。
  - 同一bitmap wordへ複数割当した場合も全bitとsummary/free-block counterが一致する。
  - file growth、hole、12 direct blocks外、indirect writeは引き続き拒否する。

### SDW-V7RT-T32 multi-block interruption recovery

- 目的: atomic multi-block direct overwriteを実FUSE経路の全closeout中断点で検証し、回復後に全referencesと
  payloadが同じtransaction generationへ収束することを確認する。
- 変更:
  - inspection fixtureのregular fileを同一groupの3 allocated direct blocksへ拡張し、statfs、read-only mount、
    offline reference解決を3-block layoutに対応させる。
  - controlled-write smokeはpage-aligned 3-block requestを発行し、full fsync、offline fsck、全3 referencesの
    positional readback、inspection remountを検証する。
  - journal publication、metadata apply、checkpoint copy、journal reclaimの各fault caseを3-block payloadで実行し、
    recovery diagnostic、fsck、全block readbackを検証する。
  - `no_writeback_cache`では非整列の大きなapplication writeがkernel FUSE層で複数requestへ分割され得るため、
    atomicity contractが1 FUSE write request単位であることを明記する。
- 完了条件:
  - 各中断点からのadmission recovery後にjournalが空になり、3 referencesすべてがnew blocksを指す。
  - allocator bitmap/summary/free-block counterと3-block payloadがoffline fsck/readbackで一致する。
  - request分割を跨ぐapplication syscall全体のatomicityを暗黙に保証しない。

### SDW-V7RT-T33 FUSE atomic request negotiation

- 目的: controlled-writeのatomicity上限をkernel FUSE negotiationへ反映し、実際に合意した値を安定した
  startup diagnosticとしてoperatorとtestへ公開する。
- 変更:
  - FUSE initはkernel提示`max_write`を保存し、controlled-writeでは`12 * v7 block_size`との小さい方へ
    `conn->max_write`を制限する。inspection mountはkernel値を変更せずatomic write上限を0とする。
  - runtime contextへkernel max、negotiated max、atomic write maxを保持する。
  - `kafs-v7-fuse-contract`診断はmode、3つのsize、block size、direct block数を1行で出力する。
  - 実FUSE smokeはinspection/controlled-write両方のlogをparseし、negotiated値とatomic上限が計算規則に
    一致することを検証する。
- 完了条件:
  - 1 FUSE write requestがv7 direct range上限を超えず、advertised atomic上限と実装上限が一致する。
  - inspection modeは書き込みatomicityをadvertiseしない。
  - kernelがさらに小さい上限を提示した場合はその値を尊重する。

### SDW-V7RT-T34 bounded direct growth

- 目的: 既存regular inodeを12 direct blocks内で連続的に拡張し、追加block allocationとinode size更新を
  overwriteと同じatomic COW transactionへ載せる。
- 変更:
  - write開始位置を現在EOF以下に限定したままrequest endのgrowthを許可し、既存block間のholeとEOFより先から
    始まるwriteは拒否する。
  - 未割当の次direct slotsはzero-filled full blockとしてstageし、new references、inode size、blocks、allocator
    bitmap/summary/free counterを同じsingle/batch transactionでpublishする。
  - low-level smokeは既存末尾から未割当2 blocksへ跨るgrowthを検証し、実FUSE smokeと全4 interruption caseは
    EOFへの1-block append、full fsync、recovery diagnostics、size/blocks、offline payload、fsckを検証する。
- 完了条件:
  - request外の新規block領域はzeroで、追加block数とinode blocks/free-block counterが一致する。
  - crash recovery後にold sizeまたはnew sizeの混在しないtransaction stateへ収束する。
  - hole、12 direct blocks外、indirect growthは引き続き拒否する。

### SDW-V7RT-T35 bounded direct truncate

- 目的: 既存regular inodeを12 direct blocks内で縮小し、partial-tail zeroingと参照解除をv7 transactionへ
  接続する。
- 変更:
  - `truncate(2)` / `ftruncate(2)`の縮小をcontrolled-write FUSE入口からv7専用adapterへrouteする。
  - 非block境界の末尾はCOWして切捨て領域をzero-fillし、new tail reference、inode size/blocks、allocator metadataを
    同じtransactionでpublishする。block境界の縮小はinode reference更新を直接transaction commitする。
  - checkpoint後に参照が外れたtail/whole blocksを既存data retirement transactionで解放する。
  - low-level smokeはpartial/aligned両経路を検証し、実FUSE smokeは通常縮小と全4 closeout中断点からのrecovery、
    offline payload、inode size/blocks、fsckを検証する。
- 完了条件:
  - partial tailのlogical EOF以降がzeroで、削除slot、inode blocks、allocator counterが一致する。
  - crash recovery後にold inodeまたは縮小済みinodeの混在しないtransaction stateへ収束する。
  - truncateによる拡張、hole/indirect fileは引き続き拒否する。

### SDW-V7RT-T36 bounded open truncate

- 目的: `open(2)`の`O_TRUNC`をbounded direct truncateへ接続し、handle公開前にsize-0 transactionを完了する。
- 変更:
  - v7 controlled-writeでは`O_TRUNC`をlegacy mutation guardから外し、write access確認後にdirect truncate adapterを
    inode lock下で実行する。失敗時はfile handle/open countを公開しない。
  - write accessを伴わない`O_TRUNC`は`EACCES`、direct-only regular file以外は既存adapterの境界で拒否する。
  - 実FUSE smokeは`open(O_WRONLY|O_TRUNC)`後のsize/blocks/references、offline fsckを検証する。LinuxがOPEN内の
    flagまたはSETATTR+OPENのどちらへ分解しても、同じsize-0 transaction contractへ収束する。
- 完了条件:
  - successful openの時点でinodeはsize 0かつblocks 0で、旧direct blocksはretirement済みである。
  - journal/checkpoint recoveryはT35のsize-0を含むtruncate transaction contractを再利用する。

---

## 次に着手する候補

1. M8-Bの最初のsliceとして、空regular inode作成と親directory record追加をsingle-group transactionへ載せ、
   create/openのhandle公開、free-inode counter、全closeout中断点からのrecoveryを検証する。

FTL/ECC相関fault injectionは通常のimplementation blockerにはせず、RC media qualificationとrelease noteの
既知制約として扱う。これはsoftware recovery gateの免除ではなく、RCでは通常の実SD card上の
format/mount/unmount/remount/fsckと独立reviewを必須とし、controlled writeを含むRCではさらに
write/full-fsync/controlled power-interruption cycleを必須とする。stable/GAではphysical
failure-domainの残存riskを再評価する。

cross-group HRL と multi-group atomic mutation は、まず group-local placement と recovery replica の
wear/fault proof を固めた後に段階的に扱う。

履歴上の Phase 1/2 backlog は下記の直近実装メモに残す。現行の descriptor-backed format work は
format v7 を入口にする。

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
