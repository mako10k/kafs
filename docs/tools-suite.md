# KAFS ツール一式: 現状と理想像

この文書は、KAFS を「ファイルシステムとして使える一式ツール」に整えるために、現状を整理し、理想的な構成と段階的な整備計画を示します。

## 現状（2025-08-28 時点）

- 実行バイナリ
  - `kafs`（FUSE3 ベースのファイルシステム本体）
    - フォアグラウンド実行が前提。FUSE マルチスレッドは `-o multi_thread[=N]` または `KAFS_MT=1` で切り替え。
    - ジャーナル: 画像内リングバッファ（v2）を使用。ヘッダ/レコードとも CRC32 付き。
    - グループコミット: 既定 10ms 窓。`KAFS_JOURNAL_GC_NS` で調整（0=即時 fsync）。
    - サイドカー（外部ファイル）廃止。画像内ジャーナルが無い場合は無効化。
    - リプレイ: マウント時にスキャン・クリーン。既定では「再適用」は実施せず（コールバックIFあり）。
- ツール/テスト
  - `stress_fs` テスト（Automake tests）。マウント/並行操作のストレス検証で PASS。
  - `mkfs.kafs` / `fsck.kafs` は `src/Makefile.am` の `bin_PROGRAMS` に登録済みで、既定ビルド/インストール対象。
  - 実装はそれぞれ `mkfs_kafs.c` / `fsck_kafs.c`。
- ドキュメント
  - `docs/journal-plan.md`（M1〜M4 の計画）。

課題（ギャップ）
- mkfs/検査・修復ツール/ジャーナル閲覧ツールなど「ユーティリティ」が未整備。
- インストールターゲット（bin_PROGRAMS）や man ページが未設定。
- デフォルト・リプレイ方針（再適用の有無・条件）が保守的（スキャン/クリーンのみ）。

## 理想像（ユーティリティ群と役割）

- フォーマット
  - `mkfs.kafs`
    - 画像作成。ブロックサイズ、ブロック数/総容量、inode 数、ジャーナルサイズ等を指定。
    - 例: `mkfs.kafs -s 10G -b 12 -i 200000 -J 8M image.kafs`
- マウント/アンマウント
  - `mount.kafs`（ラッパー、または `kafs` 本体にオプション集約）
    - FUSE オプション（`-o allow_other,ro,...`）や KAFS 固有フラグを整理して提供。
  - `umount.kafs`（オプション、標準 `fusermount3 -u` の薄いラッパー）
- 検査・修復
  - `fsck.kafs`
    - スーパーブロック・ビットマップ・inode・HRL の一貫性チェック。
    - ジャーナル CRC 検証と「既定の安全なリプレイ（再適用）」とクリーンアップ。
    - モード: `--check-only` / `--auto-repair` / `--journal-only`。
- ジャーナル操作
  - `kafs-journalctl`
    - ジャーナルの列挙・フィルタ（op/time/seq）・簡易集計。
  - `kafs-journal-clear`
    - オフライン安全条件下でのリング初期化（CRC 付きヘッダ更新）。
- 監査・可視化
  - `kafs-inspect`
    - メタデータ（SB/bitmap/inode/HRL 概要）を人間可読で表示。
  - `kafs-dump`
    - ローレベルダンプ（十六進/構造体ビュー）。
- ベンチ/診断
  - `kafs-bench`
    - 代表的な操作のスループット/レイテンシ測定。グループコミット窓などの影響を可視化。

命名方針
- Linux ディストロ慣習に合わせ、`mkfs.kafs` / `fsck.kafs` の形を推奨。
- その他の補助は `kafs-<subcmd>` プレフィクスで統一。

## 推奨 CLI とオプション（要約）

- `mkfs.kafs`
  - `-s, --size-bytes N` 総サイズ（既定 1GiB）
  - `-b, --blksize-log L` ブロックサイズの log2（既定 12=4096B）
  - `-i, --inodes I` inode 数（既定 65536）
  - `-J, --journal-size-bytes J` ジャーナル領域サイズ（既定 1MiB、最小 4KiB）
- `kafs`（または `mount.kafs`）
  - `-f` フォアグラウンド、`-o allow_other,ro,...` FUSE オプション。
  - 環境変数: `KAFS_MT`、`KAFS_MAX_THREADS`、`KAFS_JOURNAL_GC_NS`。
- `fsck.kafs`
  - `--check-only` / `--auto-repair` / `--journal-only` / `-v` 詳細出力。
- `kafs-journalctl`
  - `--list`、`--op <name>`、`--seq <a>[:<b>]`、`--since <ts>`、`--until <ts>`、`--stats`。
- `kafs-journal-clear`
  - `--force`（確認抑制）、`--dry-run`。

## ビルド/インストール構成（提案）

- Automake
  - `bin_PROGRAMS = kafs mkfs.kafs fsck.kafs kafs-journalctl kafs-journal-clear kafs-inspect`（段階導入）
  - まず `mkfs.kafs` を既存 `mkfs_kafs.c` から切り出して追加。
  - man ページ（`man8`）と補助ドキュメント（`/usr/share/doc/kafs/`）。
- 依存
  - FUSE3、pthread（任意、なければフォールバック）。
  - fsck/inspect はライブラリ化した共通パーサ/CRC/レイアウトヘルパを再利用。

## 運用ポリシー（推奨）

- ジャーナル
  - 画像内リング（v2）を標準。COMMIT 時のみ耐久化、グループコミット窓でバッチ。
  - マウント時: 既定はスキャン/クリーン。`fsck.kafs --journal-only` で再適用実施。
- 安全モード
  - `ro` マウント時はジャーナル書き込み禁止、リプレイは read-only で可能な範囲のみ。
- 可観測性
  - `kafs-journalctl --stats` による直近のトランザクション・サイズ・遅延集計。

## ロードマップ（段階整備）

- フェーズ A（最小実用セット）
  1) `mkfs.kafs` をビルドターゲットに追加・インストール対応
  2) `fsck.kafs` 雛形（ジャーナル CRC 検証 + クリーン + オプション）
  3) `kafs-journalctl --list/--stats` の最小機能（読み取りのみ）
- フェーズ B（検査/再適用の完成）
  4) `fsck.kafs --auto-repair`（安全な再適用と簡易整合性修復）
  5) 画像オフラインでの `kafs-journal-clear` 実装
- フェーズ C（可視化と保守）
  6) `kafs-inspect` 基本ビュー（SB/bitmap/inode/HRL 概要）
  7) man ページ・サンプル・チュートリアル整備
- フェーズ D（最適化/拡張）
  8) グループコミット自動チューニング（負荷に応じた窓調整）
  9) ベンチ/診断の追加（`kafs-bench`）

## メモ（現状の重要仕様）

- ジャーナル
  - ヘッダ: `write_off/area_size/seq` + CRC32。
  - レコード: `BEGIN/COMMIT/ABORT/NOTE/WRAP`、各レコードに CRC32。
  - リプレイ: CRC 不一致・部分レコード検出時は安全側に倒してスキャン停止し、クリアを実施。
- 環境変数
  - `KAFS_JOURNAL=0` で完全無効化（画像内があっても使わない）。
  - `KAFS_JOURNAL_GC_NS` でグループコミット窓（ns）を指定（既定 10ms）。

---
このドキュメントは、当面の「設計と整備計画」の基準として更新していきます。