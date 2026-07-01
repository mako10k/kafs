# KAFS SDカード劣化対策 バックログ

最終更新: 2026-07-01

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
  - `docs/kafsresize-cutover-playbook.md` に future controlled v6 write opt-in boundary を追加した。
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
    現段階では `controlled write mount is not enabled yet` として拒否する。
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
