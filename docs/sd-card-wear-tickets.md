# KAFS SDカード劣化対策 バックログ

最終更新: 2026-07-03

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
