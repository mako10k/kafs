# Release Announcement (v0.2.1)

## Short summary

- `v0.2.1` をリリースしました（ドキュメント追補のみ）。
- 実行時の挙動変更はありません。
- `v0.2.0` で導入した移行導線の説明を README / man に同期しました。

## What changed since v0.2.0

対象差分（`v0.2.0..HEAD`）:

- `README.md`
- `man/kafs.1`
- `man/kafsctl.1`
- `CHANGELOG.md`

コミット:

- `1c502c1` docs: reflect v0.2.0 migration flow in README
- `9973c6e` docs(man): document migration options and commands for v0.2.0
- `7171842` chore(release): add v0.2.1 changelog entry

## Links

- v0.2.0: https://github.com/mako10k/kafs/releases/tag/v0.2.0
- v0.2.1: https://github.com/mako10k/kafs/releases/tag/v0.2.1

---

## Slack template (JP)

KAFS `v0.2.1` をリリースしました。

- これは **ドキュメント追補のみ** のパッチリリースです（動作変更なし）
- `v0.2.0` で追加した migration 導線（`kafsctl migrate` / `kafs --migrate-v2`）の説明を README と man に反映しました

詳細: https://github.com/mako10k/kafs/releases/tag/v0.2.1

## GitHub Discussion template (JP)

### KAFS v0.2.1 をリリースしました

`v0.2.1` はドキュメント追補のパッチリリースです。`v0.2.0` で導入した v2→v3 マイグレーション導線の説明を README / man に同期し、運用時の参照性を改善しました。

#### 変更点

- README: migration 手順と注意事項を追記
- man: `kafs(1)`, `kafsctl(1)` を v0.2.0 仕様へ同期
- CHANGELOG: v0.2.1 エントリを追加

#### 互換性

- 実行時の挙動変更はありません
- migration 動作は v0.2.0 から変更なし

Release: https://github.com/mako10k/kafs/releases/tag/v0.2.1