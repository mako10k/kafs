# Release Announcement (v0.3.0)

## Short summary

- `v0.3.0` をリリースしました。
- オフライン運用ツール群、background dedup、logical delete tombstone の一連の機能をまとめたマイルストーンリリースです。
- 運用性だけでなく、観測性・修復性・品質ゲートも強化しました。

## What changed since v0.2.2

対象差分（`v0.2.2..HEAD`）:

- `src/kafs.c`, `src/kafsctl.c`, `src/fsck_kafs.c`
- `src/kafsdump.c`, `src/kafsimage.c`, `src/kafsresize.c`, `src/kafs_info.c`
- `tests/tests_kafsresize.c`, `tests/tests_open_unlink_visibility.c`, `tests/tests_journal_boundary.c`
- `README.md`, `man/*.1`, `man/*.8`, `CHANGELOG.md`

主な変更:

- `fsck.kafs` に統合モードと低レベル修復オプションを追加
- `kafsdump`, `kafsimage`, `kafsresize` によるオフライン保守フローを拡充
- idle background dedup の制御・可視化・HRL rescue 指標を追加
- logical delete tombstone と background GC を追加し、統計・info・fsck に反映
- `mkfs.kafs` の上書き確認、クラッシュ診断、completion、CI/clone/static gate を強化

## Links

- v0.2.2: https://github.com/mako10k/kafs/releases/tag/v0.2.2
- v0.3.0: https://github.com/mako10k/kafs/releases/tag/v0.3.0

---

## Slack template (JP)

KAFS `v0.3.0` をリリースしました。

- `fsck.kafs`, `kafsdump`, `kafsimage`, `kafsresize` を中心にオフライン運用ツール群を強化
- idle background dedup の制御と observability を追加
- logical delete tombstone と background GC を導入し、`kafsctl stats` / `kafs-info` / `fsck.kafs` に反映
- `mkfs.kafs` の上書き確認、クラッシュ診断、品質ゲートも強化

詳細: https://github.com/mako10k/kafs/releases/tag/v0.3.0

## GitHub Discussion template (JP)

### KAFS v0.3.0 をリリースしました

`v0.3.0` は、オフライン修復・イメージ操作・統計可視化・logical delete 運用までをまとめたマイルストーンリリースです。単一の機能追加ではなく、保守運用フロー全体を前進させる更新になっています。

#### 変更点

- `fsck.kafs`: 統合モード、journal replay、inode block-count repair、punch-hole 系オプションを追加
- `kafsdump` / `kafsimage` / `kafsresize`: オフライン点検・出力・移行フローを拡充
- background dedup: idle scan 制御、テレメトリ、HRL rescue 指標を追加
- tombstone GC: logical delete tombstone、background GC、stats/info/fsck 連携を追加
- operation hardening: `mkfs.kafs` overwrite confirmation、crash diagnostics、completion、品質ゲートを強化

#### 互換性

- 既存の single-image ベース運用を前提としたまま、保守ツールと観測性を拡張しています
- 🟨 新機能が広範囲にまたがるため、既存運用へ適用する際は `fsck.kafs` とオフラインツール群の手順差分を先に確認するのが安全です

Release: https://github.com/mako10k/kafs/releases/tag/v0.3.0