# テストフォルダ/アーティファクト運用ルール

目的: テスト用の一時成果物がリポジトリに散在・混入・コミットされる事故を防ぎ、再現性と掃除の容易さを上げる。

## 1. テストソースの置き場所

- テスト実行スクリプト（E2E/統合/再現手順）: `scripts/`
  - 例: `scripts/run-all-tests.sh` や `scripts/test-*.sh`
- 生成物（ログ/レポート）の出力先: 既存の規約に従い `./report/` を基本とする
- Cのテストソース（`tests_*.c` など）: `tests/`
- Cのテスト用バイナリ/補助バイナリ:
  - 原則は「ビルド出力」であり、Git管理しない
  - インツリー（リポジトリ内）ビルドでは `tests/` 配下に生成され得るため、`.gitignore` で無視する

レガシー:
- 過去の再現/解析のために、リポジトリ直下に `test-*.sh` や `*-strace*.sh` が存在する場合がある。
- これらは段階的に整理する前提とし、**新規のテストスクリプトは `scripts/` に追加する**。

## 2. スモーク/テストの一時フォルダ（workdir/mountpoint）

原則: **リポジトリ直下に一時フォルダを作らない**。

- 既定: `${TMPDIR:-/tmp}` 配下に `mktemp -d` で作る
  - 例: `workdir=$(mktemp -d "${TMPDIR:-/tmp}/kafs-test.XXXXXX")`
- 必ず `trap` で後片付けする（成功/失敗/中断いずれでも）
  - 例: `trap 'rm -rf "$workdir"' EXIT`

例外（どうしても repo 配下が必要な場合のみ）:
- `./mnt-<purpose>-<timestamp>-<pid>/` のように **一意なディレクトリ名** を使う
- 例外ケースでも `trap` cleanup を必須とし、テスト開始前後にクリーンアップを行う

## 3. クリーンアップ方針

- リポジトリ直下の `mnt-*` と `mnt_setup_temp`、および直下の `*.img` は「一時成果物」として削除可能な扱い
- クリーンアップは専用スクリプトで行う: `scripts/cleanup-mnt-artifacts.sh`
  - 既定は dry-run（何が消えるかの一覧表示のみ）
  - 実削除は `--yes` を明示したときのみ

推奨運用:
- 破壊的な統合テストの前: `scripts/cleanup-mnt-artifacts.sh --dry-run` で汚れを検知
- 統合テストの後: 可能なら `scripts/cleanup-mnt-artifacts.sh --yes` で確実に掃除

## 4. Git運用（事故防止）

- `mnt-*` / `mnt_setup_temp` / `*.img` は Git に入れない（`.gitignore` で無視）
- もし誤って追跡された場合は、削除ではなく **追跡解除**（`git rm -r --cached`）を検討する
  - 注意: `git rm` は破壊的操作になり得るので、実行前に必ず対象と影響を確認する
