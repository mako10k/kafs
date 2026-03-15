# Release Announcement (v0.3.1)

## Short summary

- `v0.3.1` をリリースします。
- `v0.3.0` 後の hotplug supervised restart 整理と `master` 上の安定化修正をまとめたメンテナンスリリースです。
- 互換性を崩さずに、restart-back、docs、man page、release gate の一貫性を高めました。

## What changed since v0.3.0

対象差分（`v0.3.0..HEAD`）:

- `src/kafs.c`, `src/kafs_front.c`, `src/kafs_back.c`, `src/kafsctl.c`
- `tests/tests_e2e_hotplug.c`
- `docs/hotplug-*.md`, `man/*.1`, `man/*.8`, `CHANGELOG.md`, `README.md`

主な変更:

- hotplug supervised restart を `restart-back` / `socketpair` / FD handoff 前提で整理
- UDS bootstrap 互換を維持しつつ、docs・man page・`kafsctl` 文言を現行モデルへ同期
- `kafs-front` crash diagnostics、`kafs-back` handshake/serve loop 整理、restart 時の予約 env 保護を追加
- `fsstat --verbose` の改善と symlink lock-order abort 修正を `master` に取り込み
- release gate では `deadcode` を継続、通常 PR/update gate では任意化

## Links

- v0.3.0: https://github.com/mako10k/kafs/releases/tag/v0.3.0
- v0.3.1: https://github.com/mako10k/kafs/releases/tag/v0.3.1

---

## Slack template (JP)

KAFS `v0.3.1` をリリースします。

- hotplug の `restart-back` 経路を supervised restart 前提で整理
- UDS bootstrap 互換を残したまま docs / man page / e2e test を同期
- `kafs-front` crash diagnostics と restart hardening を追加
- `fsstat --verbose` 改善と symlink lock-order 修正を取り込み

詳細: https://github.com/mako10k/kafs/releases/tag/v0.3.1

## GitHub Discussion template (JP)

### KAFS v0.3.1 をリリースします

`v0.3.1` は、`v0.3.0` 後に入った hotplug supervised restart の整理と `master` 上の安定化修正をまとめたメンテナンスリリースです。大きな新機能追加ではなく、現行アーキテクチャに合う形で restart 経路と公開説明を揃えることを目的にしています。

#### 変更点

- hotplug: `restart-back`、`socketpair`/FD handoff、session/epoch handshake を整理
- compatibility: UDS bootstrap / relisten は維持しつつ、docs/man page/`kafsctl` の説明を同期
- hardening: `kafs-front` crash diagnostics、restart 時の予約 transport env 保護を追加
- master fixes: `fsstat --verbose` 改善、symlink lock-order abort 修正を取り込み

#### 互換性

- 🟩 bootstrap 側の UDS 互換は維持されます
- 🟩 `restart-back` は supervised restart 前提で動作します
- 🟨 完全 FD-only bootstrap は引き続き out-of-scope です

Release: https://github.com/mako10k/kafs/releases/tag/v0.3.1