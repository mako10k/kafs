# Migration Guide: KAFS format v2 -> v3

## Summary

- v2 イメージは通常起動時にマウントせず、ガイダンスを表示して終了します。
- マイグレーションは**不可逆**です。
- 推奨はオフライン実行: `kafsctl migrate <image> [--yes]`

## Preconditions

- 対象イメージをアンマウントしていること
- 必要に応じてバックアップを取得していること

## Recommended procedure (offline)

1. バックアップ（推奨）
2. マイグレーション実行
   - 対話確認あり:
     - `kafsctl migrate <image>`
   - 非対話（CI/自動化向け）:
     - `kafsctl migrate <image> --yes`
3. 通常どおりマウント
   - `kafs --image <image> <mountpoint> [FUSE options...]`

## One-shot startup migration

`kafs` 起動時に特殊オプションで自動移行できます。

- 対話確認あり:
  - `kafs --image <image> --migrate-v2 <mountpoint> [FUSE options...]`
- 非対話:
  - `kafs --image <image> --migrate-v2 --yes <mountpoint> [FUSE options...]`

実行後は `migration completed: v2 -> v3. please restart mount.` を出して終了するため、再度通常マウントしてください。

## Notes

- すでに v3 の場合はマイグレーションは不要です。
- 非対応バージョンはエラー終了します。
