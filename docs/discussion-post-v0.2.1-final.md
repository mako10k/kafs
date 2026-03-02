# GitHub Discussion Post (Final) - v0.2.1

## タイトル案

KAFS v0.2.1 リリース（ドキュメント追補のみ）

## 本文（そのまま投稿可）

KAFS `v0.2.1` をリリースしました。

今回の `v0.2.1` は **ドキュメント追補のみ** のパッチリリースです。実行時の挙動変更はありません。

`v0.2.0` で導入した v2→v3 マイグレーション導線について、README と man ページの説明を同期し、運用時に参照しやすい形に整えました。

### 変更点

- `README.md`
  - `kafsctl migrate <image> [--yes]`
  - `kafs --migrate-v2 [--yes]` の one-shot 移行オプション
  - 不可逆実行時の注意事項
- `man/kafs.1`
  - `--migrate-v2` / `--yes` を追記
- `man/kafsctl.1`
  - `migrate <image> [--yes]` を追記
- `CHANGELOG.md`
  - `v0.2.1` エントリを追加

### 互換性

- 実行時の動作変更: なし
- マイグレーション動作: `v0.2.0` から変更なし

### リンク

- `v0.2.0`: https://github.com/mako10k/kafs/releases/tag/v0.2.0
- `v0.2.1`: https://github.com/mako10k/kafs/releases/tag/v0.2.1

必要に応じて、今後はこの投稿スレッドで移行運用時の Q&A（バックアップ方針、`--yes` 運用、ロールアウト手順）を追記していきます。