# kafsctl パスベース file-op 再整理要件

最終更新: 2026-03-17

## 1. 目的

`kafsctl` の file-op 系サブコマンドを、`<mountpoint> <path>` 前提の専用 CLI から、`cp` / `mv` / `rm` / `mkdir` / `rsync` に近い「パス中心」の CLI に整理し直す。

対象コマンド:

- `stat`
- `cat`
- `write`
- `cp`
- `mv`
- `rm`
- `mkdir`
- `rmdir`
- `ln`
- `symlink`
- `readlink`
- `chmod`
- `touch`
- `rsync` 追加

対象外:

- `migrate`
- `fsstat` / `stats`
- `hotplug *`

これらは mount 単位の制御コマンドであり、今回の「path から mount を自動推定する file-op 再設計」とは分離する。

## 2. 現状の確認

### 2.1 現行 CLI

現行実装は file-op 系で一律に `<mountpoint>` を必須にしている。

例:

- `kafsctl stat <mountpoint> <path>`
- `kafsctl cp <mountpoint> <src> <dst> [--reflink]`
- `kafsctl mv <mountpoint> <src> <dst>`

### 2.2 実装上の制約

現行の `src/kafsctl.c` から確認できる事実:

- 単一路径コマンドは、指定 mountpoint を `open(..., O_DIRECTORY)` して `*at` 系 syscall で処理している。
- 路径は `to_mount_rel_path()` / `to_kafs_path()` で mountpoint 基準に正規化している。
- `cp` は `KAFS_IOCTL_COPY` を使う KAFS 専用経路しか持たない。
- `KAFS_IOCTL_COPY` 実装は現状 `KAFS_IOCTL_COPY_F_REFLINK` 必須で、通常コピー要求を受け付けない。
- `mv` は単一 dirfd に対する `renameat()` だけで、同一 mount 内 rename しか扱えない。
- `rsync` 相当のサブコマンドは存在しない。

したがって、mountpoint 引数の削除だけでは足りず、`cp` / `mv` / `rsync` は「経路選択と fallback」の仕様を先に定義する必要がある。

## 3. 新 CLI の基本方針

### 3.1 path-first へ変更

file-op 系は原則として `<mountpoint>` を受け取らず、通常のパス引数から KAFS mount を自動解決する。

新しい基本形:

- `kafsctl stat <path>`
- `kafsctl cat <path>`
- `kafsctl write <path>`
- `kafsctl cp [options] <src> <dst>`
- `kafsctl mv [options] <src> <dst>`
- `kafsctl rm [options] <path>...`
- `kafsctl mkdir [options] <path>...`
- `kafsctl rsync [rsync options...] <src>... <dst>`

### 3.2 mount 自動解決

`<path>` から KAFS mount を自動解決する。

必須要件:

- 既存パスでは、そのパス自身または最近傍の既存祖先から KAFS mount を判定できること。
- 未作成パスでは、親ディレクトリ側から KAFS mount を判定できること。
- `stat` / `readlink` / `chmod` のように「最終要素を辿りたくない」操作では、親ディレクトリ基準で解決すること。
- `symlink` では linkpath 側だけを mount 解決対象にし、target 文字列はそのまま保存すること。

推奨判定手順:

1. パスを lexical に正規化する。
2. 最終要素または既存親を `openat` 可能な位置まで遡る。
3. 候補 fd に対して `KAFS_IOCTL_GET_STATS` を試行し、KAFS かどうか確認する。
4. 必要なら `/proc/self/mountinfo` を参照して mount root を確定する。

注意点:

- `/.kafs.sock` の有無を mount 判定の必須条件にしてはならない。hotplug 無効 mount でも file-op は成立すべきだからである。

## 4. コマンド分類

### 4.1 KAFS 単独コマンド

以下は「対象 path が KAFS 上にあること」を前提とし、非 KAFS path なら明示エラーにする。

- `stat`
- `cat`
- `write`
- `rm`
- `mkdir`
- `rmdir`
- `ln`
- `symlink`
- `readlink`
- `chmod`
- `touch`

理由:

- これらは `kafsctl` 独自価値が「KAFS を対象に mountpoint なしで扱えること」にあり、非 KAFS まで透過実行させると責務が曖昧になる。

### 4.2 ハイブリッド dispatch コマンド

以下は path の組み合わせに応じて、native 実装と外部コマンド fallback を切り替える。

- `cp`
- `mv`
- `rsync`

## 5. 互換オプション方針

### 5.1 共通方針

- POSIX / GNU 互換は「低コストで意味が明確なもの」に限定する。
- 実装しないが誤用されやすいオプションは、黙殺せず「未対応」と明示して終了コード 2 で失敗させる。
- `--help` と `--` は全対象コマンドで受け付ける。
- 短いオプション束ね書きは、採用するオプションに限って許可する。
- 外部コマンド fallback 時も、`kafsctl` 側で構文検証してから `execvp()` する。
- shell 展開前提の動作はしない。`system()` は使わない。

### 5.2 最低限サポートするオプション

#### stat

- `-L`
- `--help`

未対応で明示エラー:

- `-c`
- `--format`
- `--printf`
- `-f`

#### cat

- `--help`

未対応で明示エラー:

- `-n`
- `-b`
- `-s`
- `-A`

#### write

- `--help`
- `-a` / `--append`

`write` は KAFS 独自コマンドとして残し、GNU `tee` 互換までは狙わない。

#### cp

- `-f`
- `-i`
- `-n`
- `-v`
- `-T`
- `-t <dir>`
- `--reflink=auto|always|never`
- `--help`

初期フェーズでは未対応で明示エラー:

- `-a`
- `-p`
- `-R` / `-r`
- `-x`
- `--parents`
- `--preserve=*`

#### mv

- `-f`
- `-i`
- `-n`
- `-v`
- `-T`
- `-t <dir>`
- `--help`

#### rm

- `-f`
- `-r` / `-R`
- `-d`
- `-v`
- `--help`

#### mkdir

- `-p`
- `-m <mode>`
- `-v`
- `--help`

#### rmdir

- `-p`
- `-v`
- `--help`

#### ln

- `-f`
- `-n`
- `-s`
- `-T`
- `-t <dir>`
- `-v`
- `--help`

`symlink` は `ln -s` の明示別名として残してよい。

#### readlink

- `-n`
- `-f`
- `-e`
- `-m`
- `--help`

ただし初期フェーズでは `-f/-e/-m` は「KAFS 内 lexical/canonical 解決」に限定し、GNU `readlink(1)` と完全一致までは求めない。

#### chmod

- `-R`
- `-v`
- `--help`

モード指定:

- フェーズ 1: 8 進数のみ必須
- フェーズ 2: POSIX symbolic mode を追加検討

symbolic mode をまだ受けない場合は、`u+w` 等を「未対応モード」として明示エラーにする。

#### touch

- `-a`
- `-m`
- `-c` / `--no-create`
- `-d <datetime>`
- `-r <file>`
- `--help`

#### rsync

- 基本方針は「`kafsctl` 独自 rsync エンジン」ではなく、外部 `rsync` への安全なラッパー。
- `kafsctl rsync` は、path 解決、KAFS 判定、将来最適化フック、エラー整形を担当する。
- オプション本体は `rsync` へ透過転送する。

## 6. dispatch 要件

### 6.1 単一路径コマンド

`stat` / `cat` / `write` / `rm` / `mkdir` / `rmdir` / `ln` / `symlink` / `readlink` / `chmod` / `touch` は、対象 path が KAFS と判定できた場合のみ native 実装に入る。

KAFS でない場合:

- 標準エラーに「対象が KAFS mount 配下ではない」と明示する。
- 終了コードは 2。
- 自動で通常コマンドへ fallback はしない。

### 6.2 `cp`

`cp` は以下の分類で dispatch する。

#### A. src/dst とも同一 KAFS mount

- native 実装を使う。
- `--reflink=always|auto` のときは reflink/clone を最優先する。
- `--reflink=never` または reflink 不成立時は、KAFS 内データコピー経路を使う。

必須変更:

- 現状 `KAFS_IOCTL_COPY` が reflink 専用なので、通常コピー要求も受け付けるよう拡張する。

#### B. src または dst の片側だけが KAFS

- 片側 KAFS で native に有意な最適化がまだ無い間は、外部 `cp` に fallback する。
- ただし将来、KAFS 側 import/export を userspace バウンス無しで処理できる専用経路を追加する余地を残す。

#### C. 両側 KAFS だが別 mount

- 初期フェーズでは外部 `cp` に fallback する。
- mount を跨ぐ server-side clone は対象外。

### 6.3 `mv`

`mv` は以下の分類で dispatch する。

#### A. src/dst とも同一 KAFS mount

- native `renameat()` を使う。

#### B. mount を跨ぐ、または片側非 KAFS

- 外部 `mv` に fallback する。
- fallback 不可なら `EXDEV` 相当の診断で失敗する。

理由:

- cross-FS `mv` は実質 `copy + unlink` であり、初期フェーズで `kafsctl` が独自再実装するより外部 `mv` の意味論を使う方が安全。

### 6.4 `ln`

- hardlink は mount を跨げないため、src/dst が同一 KAFS mount でない場合は明示的に `EXDEV` 相当で失敗する。
- `ln -s` / `symlink` は linkpath 側の mount だけが KAFS であればよい。

### 6.5 `rsync`

`kafsctl rsync` は初期フェーズでは外部 `rsync` ラッパーとする。

必須要件:

- KAFS path を mountpoint なしで扱える。
- `rsync` 実行前に src/dst の KAFS 判定結果を整理し、診断に反映する。
- `rsync` バイナリがない場合は明示エラーにする。

非目標:

- `rsync` の再実装
- KAFS 専用差分転送アルゴリズムの導入

## 7. 実装順序

### フェーズ 1: parser と path resolver の共通化

- mount 自動解決ヘルパ追加
- 既存 `<mountpoint> <path>` 系 parser を path-first parser に置換
- 単一路径コマンドを新 parser へ移行

### フェーズ 2: `cp` / `mv` dispatcher

- src/dst 分類ロジック追加
- 外部 `cp` / `mv` fallback 実装
- `KAFS_IOCTL_COPY` を通常コピー対応に拡張

### フェーズ 3: option 互換の底上げ

- `rm -r`
- `mkdir -p -m`
- `touch -d/-r`
- `chmod -R`
- `readlink -f/-e/-m`

### フェーズ 4: `rsync` 追加

- `kafsctl rsync` ラッパー追加
- テストと man page 整備

## 8. テスト要件

最低限必要なケース:

- 既存ファイル path から KAFS mount を自動解決できる。
- 未作成 path の親から KAFS mount を自動解決できる。
- `..` を含む危険な path は従来どおり拒否できる。
- 同一 KAFS mount の `cp --reflink=always` が native clone 経路を通る。
- 同一 KAFS mount の通常 `cp` が reflink 非依存で成功する。
- KAFS -> 非 KAFS の `cp` が外部 `cp` fallback で成功する。
- 非 KAFS -> KAFS の `cp` が外部 `cp` fallback で成功する。
- 同一 KAFS mount の `mv` が native rename 経路を通る。
- cross-FS `mv` が外部 `mv` fallback を使う。
- `ln` の cross-FS hardlink が明示的に失敗する。
- `kafsctl rsync` が外部 `rsync` に透過実行できる。
- `rsync` 未導入環境で明示エラーになる。

## 9. ドキュメント反映対象

本要件に沿って更新対象になる文書:

- `man/kafsctl.1`
- `README.md` の CLI 例
- 必要なら `docs/tools-suite.md`
- 追加するテスト仕様書

## 10. ここで確定した設計判断

- file-op 系は `<mountpoint>` をやめて path-first にする。
- mount 判定は `/.kafs.sock` 依存にしない。
- 単一路径コマンドは非 KAFS へ自動 fallback しない。
- `cp` / `mv` / `rsync` は hybrid dispatch にする。
- `cp` は same-mount KAFS native 経路と外部 `cp` fallback の両方を持つ。
- `mv` の cross-FS は外部 `mv` fallback を基本にする。
- `rsync` は独自実装ではなく外部 `rsync` ラッパーとして追加する。
- 「簡単な POSIX / GNU 互換」に限定し、重い互換性は段階導入または明示エラーにする。
