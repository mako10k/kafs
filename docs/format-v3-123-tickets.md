# KAFS Format v3 実装チケット（1→2→3）

最終更新: 2026-02-28

対象仕様: [format-v3-123-spec.md](format-v3-123-spec.md)

## 方針

- 実装順は固定で **1 → 2 → 3**。
- 各フェーズで「ビルド通過 + 最低限の整合性テスト + 性能計測」をゲートにする。
- 互換性破壊は許容（v2 read-only 互換も持たない）。

---

## Phase 1: Allocator v2（割当刷新）

### P1-T1 Superblock v3 拡張
- 目的: allocator/pending/journal v3 用メタ定義を追加
- 変更: `kafs_superblock.h`, `mkfs_kafs.c`, mount時version検査
- 完了条件:
  - `s_format_version=3` 作成/読込
  - v2イメージを明示的に拒否

### P1-T2 Allocator領域レイアウト導入
- 目的: L0/L1/L2 要約ビットマップ領域の確保
- 変更: mkfsレイアウト計算、オフセット保存
- 完了条件:
  - mkfsがallocator領域を正しく初期化
  - fsckで領域境界検証に通る

### P1-T3 Allocator API 抽象化
- 目的: 既存`kafs_blk_alloc`呼び出しを backend 切替可能にする
- 変更: `kafs_block.h` に backend層（legacy/v3）
- 完了条件:
  - コンパイル時/実行時でv3 backendが選択される

### P1-T4 L1/L2探索実装
- 目的: 線形探索削減
- 変更: `alloc_find_free()` を階層探索化
- 完了条件:
  - `blk_alloc_ns_scan` が既存比で中央値 30%以上削減

### P1-T5 Claim更新の整合確保
- 目的: 競合時でも L0/L1/L2 が破綻しない
- 変更: claim/rollback時の階層更新
- 完了条件:
  - 故障注入（競合/中断）後に fsck 一致

### P1-T6 Phase1 検証
- 目的: 受け入れ判定
- 完了条件:
  - build/test PASS
  - `w3`中央値で全体 `elapsed` 改善（目安 5%以上）

---

## Phase 2: Metadata batching（集約更新）

### P2-T1 tx_meta_delta 導入
- 目的: set_usage由来更新をメモリ集約
- 変更: contextに`meta_delta`構造追加
- 完了条件:
  - commit前は on-disk へ即時反映しない

### P2-T2 bitmap word バッチ反映
- 目的: bit更新の書き込み回数削減
- 変更: dirty word list を commit で一括書込み
- 完了条件:
  - `blk_set_usage_bit_ms` の中央値 20%以上削減

### P2-T3 freecnt/wtime バッチ反映
- 目的: superblock更新回数削減
- 変更: `delta_free_blocks` と `wtime_dirty` の commit時反映
- 完了条件:
  - `blk_set_usage_freecnt_ms` の中央値 20%以上削減

### P2-T4 Journal v3 レコード拡張
- 目的: replay可能なメタ差分を記録
- 変更: `TX_META_DELTA`, `TX_COMMIT` 強化
- 完了条件:
  - crash後 replay で bitmap/freecnt が一致

### P2-T5 fsync/syncfs セマンティクス固定
- 目的: durability境界の明確化
- 変更: fsyncでtx flush保証
- 完了条件:
  - durabilityテスト PASS（kill -9 再起動）

### P2-T6 Phase2 検証
- 完了条件:
  - build/test PASS
  - `blk_alloc_set_usage_ms` 50%以上削減（Phase1基準）

---

## Phase 3: Async dedup/refcount（非同期化）

### P3-T1 Pending Log on-disk 実装
- 目的: 未確定書込み状態を永続化
- 変更: pending ring 定義・読み書きAPI
- 完了条件:
  - restart後に pending を再読込可能

### P3-T2 PENDING_REF 表現導入
- 目的: inodeが pending/final を表現可能にする
- 変更: inode block ref encoding 拡張
- 完了条件:
  - read path が pending 解決可能

### P3-T3 Async Worker 実装
- 目的: hash/HRL/refcount をバックグラウンド処理
- 変更: worker loop + retry/backoff
- 完了条件:
  - Stage-A 書込み完了後に Stage-B が収束

### P3-T4 fsync対象inodeの pending drain
- 目的: POSIX期待との整合
- 変更: inode単位 wait/flush
- 完了条件:
  - fsync直後に対象 inode pending=0

### P3-T5 crash replay state machine
- 目的: QUEUED/HASHED/RESOLVED 復元
- 変更: replay時の遷移固定
- 完了条件:
  - ランダム中断テストでリーク/重複なし

### P3-T6 Phase3 検証
- 完了条件:
  - build/test PASS
  - `w3` 全体 `elapsed` 15%以上短縮（v2基準）
  - P99 write latency 25%以上短縮

---

## 依存関係

- P2 は P1 完了後のみ着手
- P3 は P2 の journal/tx 境界確定後のみ着手
- P3-T2 と P3-T5 は同一PRで扱わない（デバッグ困難化回避）

---

## PR 分割ルール

- 1チケット = 1PR を原則
- PRごとに必須添付:
  - 変更ファイル一覧
  - 互換破壊の有無
  - 再現手順
  - 計測結果（最低5-run中央値）

---

## ロールバック方針

- P1/P2/P3 のフェーズ境界ごとにタグを打つ
- 問題発生時はフェーズ単位で戻す（チケット単位ロールバックは非保証）

---

## 最初に着手すべきチケット

1. P1-T1 Superblock v3 拡張
2. P1-T2 Allocator領域レイアウト導入
3. P1-T3 Allocator API 抽象化

この3件が完了すると、以降の性能改善チケットを安全に積める。
