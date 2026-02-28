# Changelog

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
