# KAFS ツール一式: 現状と理想像

この文書は、KAFS を「ファイルシステムとして使える一式ツール」に整えるために、現状を整理し、理想的な構成と段階的な整備計画を示します。

`kafsctl` の path-based file-op 再整理要件は、別紙 [kafsctl-path-ops-requirements.md](kafsctl-path-ops-requirements.md) を参照。

## 現状（2026-03-22 時点）

- 実行バイナリ
  - `kafs`（FUSE3 ベースのファイルシステム本体）
    - フォアグラウンド実行が前提。FUSE マルチスレッドは `-o multi_thread[=N]` または `KAFS_MT=1` で切り替え。
    - ジャーナル: 画像内リングバッファ（v2）を使用。ヘッダ/レコードとも CRC32 付き。
    - グループコミット: 既定 10ms 窓。`KAFS_JOURNAL_GC_NS` で調整（0=即時 fsync）。
    - サイドカー（外部ファイル）廃止。画像内ジャーナルが無い場合は無効化。
    - リプレイ: マウント時にスキャン・クリーン。既定では「再適用」は実施せず（コールバックIFあり）。
- ツール/テスト
  - `kafs-info`（オフライン image の基本サマリ表示）
    - superblock version / block geometry / inode-block counters / hash 設定を出力。
    - tombstone 概要、HRL 領域、v5 tail metadata region の enabled/off/size を表示。
  - `kafsdump`（オフライン image の read-only 可視化）
    - `--json` 対応。
    - superblock / tail metadata / inode 集計 / HRL 集計 / journal header(CRC) を出力。
  - `kafsimage`（オフライン image のエクスポート）
    - `--metadata-only` / `--raw` / `--sparse` と `--verify` を提供。
    - metadata-only ではメタデータ先頭領域 `[0, first_data_block * block_size)` を書き出す。
  - `kafsresize`（オフライン image の resize / migration-image 作成）
    - `--grow --size-bytes` と `--migrate-create` を提供。
    - 事前確保ヘッドルーム（`s_blkcnt < s_r_blkcnt`）内の増設のみ対応。
    - format version 5 scaffold image の grow と offline migrate-create を受け付ける。
  - `mkfs.kafs` / `fsck.kafs`
    - `mkfs.kafs` は `--format-version` に対応し、新規 image は既定で v5 を作成する。legacy v4 image が必要な場合は `--format-version 4` を明示する。
    - `fsck.kafs` は統合モードに加えて tail metadata region の境界と owner 整合も検査する。
  - `stress_fs` テスト（Automake tests）。マウント/並行操作のストレス検証で PASS。
  - offline tool 回帰は `tests/tests_kafsresize.c` に集約され、empty v5 tailmeta scaffold に対する mkfs/fsck/kafsresize/kafsdump/kafsimage/kafs-info の read-only coverage を持つ。
- ドキュメント
  - man page は `kafs-info(8)` / `kafsdump(8)` / `kafsimage(8)` / `kafsresize(8)` / `mkfs.kafs(8)` / `fsck.kafs(8)` を提供。
  - `docs/journal-plan.md`（M1〜M4 の計画）。

課題（ギャップ）
- `kafs-journalctl` / `kafs-journal-clear` / `mount.kafs` など、理想像にある補助ユーティリティはまだ未実装。
- populated v5 image の runtime mount/remount と offline default-promotion gate まで通っており、新規 image の既定を v5 に寄せる前提が揃っている。
- デフォルト・リプレイ方針（再適用の有無・条件）が保守的（スキャン/クリーンのみ）。
- 最小 operator diagnostics の staged plan は [operator-diagnostics-plan.md](operator-diagnostics-plan.md) を参照。

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
    - モード: `--check-journal` / `--repair-journal-reset` / `--check-dirent-ino-orphans` /
      `--repair-dirent-ino-orphans` / `--check-hrl-blo-refcounts`。
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
  - 統合モード:
    - `--full-check` / `--full-repair`
    - `--balanced-check` / `--balanced-repair`（既定は `--balanced-check`）
    - `--fast-check` / `--fast-repair`
  - `--check-journal` / `--repair-journal-reset` /
    `--check-dirent-ino-orphans` / `--repair-dirent-ino-orphans` /
    `--check-hrl-blo-refcounts` / `--replay-journal` /
    `--punch-hole-unreferenced-data-blocks` / `-v` 詳細出力。
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
  - マウント時: 既定はスキャン/クリーン。`fsck.kafs --check-journal` でジャーナル検証。
- 安全モード
  - `ro` マウント時はジャーナル書き込み禁止、リプレイは read-only で可能な範囲のみ。
- 可観測性
  - `kafs-journalctl --stats` による直近のトランザクション・サイズ・遅延集計。

## ロードマップ（段階整備）

- 実装着手前メモ
  - operator diagnostics の次スライスは [operator-diagnostics-plan.md](operator-diagnostics-plan.md) の staged design に従い、control path の選択を先に確定する。
- フェーズ A（最小実用セット）
  1) `mkfs.kafs` をビルドターゲットに追加・インストール対応
  2) `fsck.kafs` 雛形（ジャーナル CRC 検証 + クリーン + オプション）
  3) `kafs-journalctl --list/--stats` の最小機能（読み取りのみ）
- フェーズ B（検査/再適用の完成）
  4) `fsck.kafs --repair-journal-reset` / `--repair-dirent-ino-orphans` の拡張
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