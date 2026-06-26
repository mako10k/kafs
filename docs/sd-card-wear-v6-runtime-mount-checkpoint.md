# KAFS format v6 runtime mount admission checkpoint

最終更新: 2026-06-26

対象: `SDW-V6RT-T1 v6 runtime mount admission design checkpoint`

## 目的

Phase 5 で作成できるようになった v6 destination image を、offline-only staging から runtime
mount 対象へ進める前に、最初に許可する mount mode と安全境界を決める。

この文書は判断材料であり、runtime mount を有効化しない。判断後に別チケットで実装する。

## 現在の境界

直接観測済みの境界:

- 通常の v6 runtime mount は admission preflight で descriptor-backed metadata checks と journal
  segment health を確認した後、offline-only gate で exit 2 として fail closed する。
- `KAFS_V6_ADMISSION_HANDOFF=1` は selected descriptor と shard maps を実 runtime context に保持できる
  ことを診断するが、FUSE mount と write admission は有効化しない。
- `KAFS_V6_READONLY_SMOKE=1` は read-only FUSE smoke に限って admitted descriptor を保持する。
  image は read-only mapping、journal replay と background mutation workers は未起動、mutation
  operations は `EROFS` で拒否する。
- Phase 5 migration validation では、v6 destination が `kafsdump --json` と
  `fsck.kafs --balanced-check` の offline validation に通り、通常 mount が offline-only gate で
  拒否されることを確認した。

未解禁の境界:

- 通常の v6 mount path で read-only mount をサポート対象にすること。
- v6 write mount、descriptor-backed live journal replay/write、repair/write fsck。
- v6 destination を production write cutover 対象にすること。

## 判断候補

| 候補 | 内容 | 適する場合 | 主なリスク |
| --- | --- | --- | --- |
| A. read-only admission 先行 | v6 は明示 read-only 指定時だけ mount 可能にする。write admission は引き続き拒否する。 | staged v6 image を FUSE 経由で検査したい。実装リスクを小さく分けたい。 | read-only と言い切るため、open/mmap/FUSE/cache/cleanup が一切 write しない証拠が必要。 |
| B. write admission 併走 | read-only と write mount を同じ段階で有効化する。 | 近い時期に production write cutover が必要で、検証コストをまとめて払える。 | journal、allocator、HRL、pending、tail、background workers、repair 境界を同時に閉じる必要がある。 |
| C. offline-only 継続 | v6 は `kafsdump` / `fsck.kafs` / migration staging のみ許可する。 | runtime mount の業務要求がまだ弱い。read-only smoke だけでは受け入れ根拠が足りない。 | Phase 5 destination は production cutover に進めないまま残る。 |

実装側の推奨は A。現時点で read-only smoke の足場があり、write path より失敗時の影響範囲を狭くできる。
B は production write cutover の期限が明確で、下記の write 条件を同時に満たす体制がある場合だけ選ぶ。

## A を選ぶ場合の必須条件

Admission:

- v6 read-only mount は明示指定された場合だけ許可する。通常の v6 mount は、判断後の実装でも当面
  fail closed を維持する。
- image open、mmap、FUSE option、image lock が read-only として揃っている。
  最低条件は read-only file descriptor または同等の write 不可能性、`PROT_READ` mapping、
  read lock、FUSE `ro` option。
- writeback cache は無効化するか、read-only mount で backing image に write が出ないことを regression
  で証明する。
- descriptor discovery は selected descriptor を 1 つに決め、same-generation divergence、
  unknown incompat flag、unknown runtime-relevant `ro_compat` flag を runtime mount では拒否する。
- bitmap、inode、allocator summary、HRL、journal segment の coverage と bounds checks が全て PASS する。
- journal は clean と判断できる状態だけ admit する。read-only replay が必要な dirty state は、
  replay semantics を別途固定するまで拒否する。

Runtime behavior:

- journal replay/write、pending worker、tombstone GC、background dedup、tail normalization など、
  metadata mutation を起こし得る path は起動しない。
- `statfs`、`getattr`、`readdir`、`lookup`、`open(O_RDONLY)`、`read`、`readlink` をサポート対象にする。
- `open(O_WRONLY/O_RDWR/O_TRUNC)`、`write`、`create`、`mkdir`、`unlink`、`rename`、`truncate`、
  `chmod`、`chown`、`utimens`、`symlink`、`link`、xattr、copy/reflink、control path mutation は
  `EROFS` または明確な read-only error で拒否する。
- `fsync` / `flush` / `release` / unmount は backing image を更新しない。

Verification:

- env-gated smoke ではなく、通常の明示 read-only admission の regression を追加する。
- migrated v6 destination と dedicated fixture の両方で read-only mount smoke を通す。
- mutation attempt が `EROFS` で拒否され、mount / unmount 前後の image digest または write counter が
  変わらないことを確認する。
- `make -C tests check TESTS=v6_descriptor_smoketest`、`make -C tests check TESTS=kafsresize`、
  `make check -j2` を PASS させる。

Operator boundary:

- v6 read-only mount は inspection / smoke / cutover rehearsal 用に限定する。
- production write cutover は引き続き対象外とし、cutover playbook と release note に明記する。

## B を選ぶ場合の追加必須条件

Admission:

- A の条件を全て満たす。
- default v6 mount を write 可能にする前に、explicit opt-in 期間を置くかどうかを決める。
- dirty journal、unclean shutdown、torn write、stale descriptor replica の扱いを admission で固定する。

Journal and metadata writes:

- `kafs_journal_init`、replay、force flush、header write、data ring write が selected v6 journal segment
  だけを使う。
- bitmap、inode、allocator summary、HRL の全 mutation path が descriptor-backed mapping を使う。
- pending log、tail metadata、dedup/tombstone cleanup、truncate、reflink/copy、rename/unlink などの
  background or delayed mutation path が v6 対応済みか、v6 write mount では明示的に無効化される。
- descriptor lifetime は mount context の lifetime と一致し、unmap/cleanup 後に shard pointer が残らない。

Repair and recovery:

- write mount 後の `fsck.kafs --balanced-check` が v6 image を検証できる。
- repair write を有効化しない場合でも、repair が必要な状態を detect-only で fail closed できる。
- descriptor replica の repair/update を行う場合は、backup first / primary last の write ordering と
  generation/CRC の回復ルールを regression で固定する。

Locking:

- `.github/lock-policy.md` の rank 順を維持する。
  `hrl_global` -> `inode_alloc` -> `inode` -> `hrl_bucket` -> `bitmap` の順で取得し、逆順で解放する。
- 新しい lock class を入れる場合は rank を文書化して policy に追加する。
- lock を取った後に `KAFS_CALL` を使わない。`rc` を保持し、単一 unlock path に寄せる。
- contention path を少なくとも 1 つ regression で通す。

Verification:

- journal torn-write、descriptor divergence、metadata shard gap/overlap、dirty allocator summary、
  HRL chain corruption、background worker mutation を含む negative tests を追加する。
- write-heavy workload の metadata heatmap で write candidate spans が group 間に分散していることを確認する。
- `make check -j2`、format/lint/static gates、必要なら TSan non-FUSE smoke を PASS させる。

Operator boundary:

- production write cutover を許可する条件を cutover playbook に追加する。
- rollback は old v5 source に戻る条件、v6 destination を保存して調査する条件、v6 destination を破棄する
  条件に分ける。

## 判断してほしい点

1. 最初の実装対象を A / B / C のどれにするか。
2. A の場合、read-only mount を `-o ro` だけで許可するか、専用 option も要求するか。
3. B の場合、write admission を最初から default にするか、明示 opt-in の実験段階を挟むか。

判断が A なら、次の実装チケットは `SDW-V6RT-T2 v6 explicit read-only mount admission` とする。
判断が B なら、次の実装チケットは read/write admission ではなく、先に
`SDW-V6RT-T2 v6 write-mount dependency audit` として mutation path の未対応箇所を潰す。
