# KAFS Format v3 仕様（1→2→3 前提）

最終更新: 2026-02-28

## 1. 目的

本仕様は、以下の順序で性能ボトルネックを解消するための**互換性破壊あり**フォーマットを定義する。

1. ブロック割当方式の刷新（Allocator v2）
2. メタ更新のトランザクション集約（Metadata batching）
3. 非同期 dedup/refcount 確定（Async commit path）

前提: 既存 v2 互換は維持しない。`mkfs` で新規作成を基本とする。

---

## 2. 非互換ポリシー

- `s_format_version = 3` を新設。
- v2 イメージは**マウント拒否**（`-EPROTONOSUPPORT` 相当）。
- 移行は offline ツール（別実装）でのみ許可。
- `fsck.kafs` は v3 専用検査モードを追加。

---

## 3. オンディスク全体レイアウト（v3）

ブロック単位レイアウト（概念）:

1. Superblock v3（固定長 256B）
2. Allocator Directory（階層ビットマップ/フリーリスト管理）
3. Inode table
4. Dentry/metadata area
5. HRL index + HRL entries
6. Journal v3（ring + checkpoint）
7. Async Pending Log（未確定操作）
8. Data blocks

### 3.1 Superblock v3 追加フィールド

既存に加え、以下を必須化:

- `s_allocator_version` (u32)
- `s_allocator_offset`, `s_allocator_size` (u64)
- `s_pendinglog_offset`, `s_pendinglog_size` (u64)
- `s_checkpoint_seq` (u64)
- `s_commit_seq` (u64)
- `s_feature_flags` (u64)
  - `FEATURE_ALLOC_V2`
  - `FEATURE_META_BATCH`
  - `FEATURE_ASYNC_DEDUP`
- `s_compat_flags` (u64)（将来拡張）

---

## 4. ① Allocator v2（割当刷新）

## 4.1 目的

`blk_alloc` の探索時間を線形走査から脱却し、平均計算量を低減する。

## 4.2 方式

- **L0**: ブロック使用ビットマップ（既存相当）
- **L1**: L0 の「空きあり」要約ビットマップ
- **L2**: L1 の要約（大容量向け）
- 任意: 小〜中サイズイメージでは `L0+L1` のみ

探索アルゴリズム:

1. L2/L1 から空き候補チャンクを O(1) 近傍で取得
2. L0 で最終ビット確定
3. claim は単一ロック区間で確定

## 4.3 追加メタ

- `alloc_hint_cursor`（CPUごと/スレッドごと）
- `free_run_hint`（連続領域ヒント）

---

## 5. ② Metadata batching（集約更新）

## 5.1 目的

`set_usage` 由来の更新（bit/freecnt/wtime）を 1 回ごとに確定せず、トランザクション境界で集約反映する。

## 5.2 モデル

- 操作中はメモリ上 `meta_delta` に差分を蓄積:
  - `delta_free_blocks`
  - `dirty_bitmap_words[]`
  - `wtime_dirty`
- commit 時に順序固定で反映:
  1. bitmap words
  2. free counters
  3. superblock timestamps
  4. journal commit marker

## 5.3 失敗時

- commit 前クラッシュ: delta は破棄（journal 未commit）
- commit 後クラッシュ: replay で再適用可能

---

## 6. ③ Async dedup/refcount（非同期化）

## 6.1 目的

書込みの同期クリティカルパスから dedup 確定処理を外し、レイテンシを平準化する。

## 6.2 書込み2段階

### Stage-A（同期パス）

- 一時ブロックを割当・書込み
- inode には `PENDING_REF` を設定
- Pending Log に `pending_id` を記録
- ここまでを同期完了条件とする

### Stage-B（非同期ワーカー）

- hash 計算
- HRL lookup/insert
- refcount 調整
- inode の `PENDING_REF -> FINAL_HRID` 置換
- 古い一時ブロック解放
- pending 完了マーカー記録

## 6.3 可視性ルール

- read は `PENDING_REF` を解決可能であること（pending table参照）
- fsync(fd): 対象 inode の pending が空であることを保証（必要なら待機）
- syncfs: 全 pending flush + metadata commit を保証

---

## 7. Journal v3 / Pending Log

## 7.1 Journal v3 レコード種別

- `TX_BEGIN`
- `TX_META_DELTA`
- `TX_PENDING_ENQUEUE`
- `TX_PENDING_RESOLVE`
- `TX_COMMIT`
- `TX_ABORT`
- `CHECKPOINT`

## 7.2 Pending Log

固定長リング（または可変長TLV）:

- `pending_id` (u64)
- `ino`, `iblk`
- `temp_blo`
- `state` (`QUEUED|HASHED|RESOLVED|FAILED`)
- `target_hrid`（解決後）
- `seq`

再起動時:

- `QUEUED/HASHED` は再実行
- `RESOLVED` で inode 未反映なら最終反映

---

## 8. 一貫性モデル

- commit 直後に保証されるもの:
  - inode 参照整合（pending か final のいずれか）
  - allocator 整合
- 遅延してよいもの:
  - dedup 収束（最終 hrid 置換）

不変条件:

1. 参照されない data block は最終的に解放される
2. `free_blocks` は replay 後に bitmap と一致
3. inode の block ref は `FINAL_HRID` または `PENDING_REF` のみ

---

## 9. fsck v3 要件

- allocator 階層整合（L0/L1/L2）
- pending と inode の対応整合
- HRL refcount と inode 実参照数一致
- temp block リーク検出と回収
- checkpoint 以降journal replay 検証

---

## 10. 性能目標（受け入れ基準）

基準: 現行ベンチ（w3_npm_offline 中央値）

- `blk_alloc_claim_ms`: さらに 30%以上削減
- `blk_alloc_set_usage_ms`: 50%以上削減
- 全体 `elapsed_s`: 15%以上短縮
- P99 write latency: 25%以上短縮

---

## 11. 実装順（1→2→3）

### Phase 1: Allocator v2

- on-disk allocator 領域導入
- 既存 API を新 allocator backend に差替
- fsck/mkfs 対応

### Phase 2: Metadata batching

- `meta_delta` 導入
- commit/replay 経路整備
- durability テスト

### Phase 3: Async dedup

- pending log / worker / read path 解決
- fsync/syncfs 契約実装
- 障害注入テスト

---

## 12. マイグレーション方針

- 初期リリースは**新規作成のみ**
- 後続で offline migrator 提供（v2 -> v3）
  - フルスキャン
  - 新 allocator 再構築
  - pending なし状態で出力

---

## 13. リスクと対策

- 非同期化で複雑化: pending state machine を最小化、状態遷移表を固定
- リプレイ時間増大: checkpoint + bounded replay を必須化
- 実装バグ: 破壊的変更のため feature flag ではなく独立フォーマットとして隔離

---

## 14. 決定済み/未決定

決定済み:

- v3 は互換破壊
- 1→2→3 の順で実装
- 3 前提で on-disk pending を持つ

未決定（実装前に確定）:

- pending log を固定長にするか TLV 可変長にするか
- async worker の並列度（1固定か複数か）
- checkpoint 間隔（時間/件数/容量ベース）
