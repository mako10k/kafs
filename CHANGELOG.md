# Changelog

## v0.3.0 - 2026-03-14
- `kafsdump`, `kafsimage`, `kafsresize`, 拡張 `fsck.kafs` を含むオフライン運用ツール群を強化。
- `fsck.kafs` に統合モード `full` / `balanced` / `fast`、ジャーナル再生、inode block-count 修復、未参照/空きデータブロック punch-hole を追加。
- idle background dedup のスキャン制御、テレメトリ、HRL rescue 指標、`fsstat`/`stats` 可視化を追加。
- logical delete tombstone と background GC を導入し、`kafsctl stats` / `kafs-info` / `fsck.kafs` で tombstone を扱えるようにした。
- `mkfs.kafs` の上書き確認、クラッシュ診断、bash completion、clone/static/lock policy の品質ゲートを強化。

## v0.2.2 - 2026-03-01
- pending 非同期処理の競合対策を強化（inode epoch 楽観ガード導入、stale pending の適用抑止）。
- pending 上書き時の二重 `dec_ref` を防止し、参照寿命の不整合を修正。
- `truncate` の中間 unlock/relock 競合窓を縮小（解放候補を遅延収集し一括 `dec_ref`）。
- stress 条件（`profile=stress`, `codec=plain`, `par=2`, `mode=normal`）で 5-run 連続 PASS を確認。

## v0.2.1 - 2026-02-28
- README に v0.2.0 の移行導線（`kafsctl migrate` / `kafs --migrate-v2`）を追記。
- man ページ（`kafs(1)`, `kafsctl(1)`）を v0.2.0 仕様に同期。
- 本リリースはドキュメント追補のみ（動作変更なし）。

## v0.2.0 - 2026-02-28
- HRL/pending log 非同期化の実装を完了し、回帰テストを通過。
- 旧フォーマット(v2)検出時は通常起動を停止し、マイグレーションガイダンスを表示。
- 新コマンド `kafsctl migrate <image> [--yes]` を追加（不可逆実行の対話確認付き）。
- 起動時特殊オプション `--migrate-v2 [--yes]` による one-shot 自動マイグレーションを追加。

## v0.1.0 - 2026-02-08
- Hotplug control now uses RPC via the hidden `/.kafs.sock` endpoint (kafsctl status/compat/restart/env).
