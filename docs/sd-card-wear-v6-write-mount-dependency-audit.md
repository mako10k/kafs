# KAFS format v6 write-mount dependency audit

最終更新: 2026-06-26

対象: `SDW-V6RT-T3 v6 write-mount dependency audit`

## 目的

v6 inspection mount の次に write mount を検討する前に、write admission で同時に有効になる
metadata mutation path、journal、background worker、repair/fsck、locking の依存関係を固定する。

この文書は監査結果であり、v6 write mount を有効化しない。v6 production write cutover も引き続き
対象外とする。

## 直接観測した境界

- `src/kafs.c` の runtime write guard は `c_runtime_read_only` の場合だけ `EROFS` を返す。
  つまり v6 を non-read-only context で admission すると、FUSE mutation operations は通常の write path
  に入る。
- `kafs_main_v6_runtime_admit_context()` は selected descriptor を runtime context に保持し、
  bitmap、inode、allocator summary、HRL、journal segment health を validation できる。
  ただし現行の supported path は `-o ro,v6_inspection_mount` の read-only admission だけである。
- `KAFS_V6_ADMISSION_HANDOFF=1` は `PROT_READ | PROT_WRITE` mapping と descriptor retention を診断するが、
  FUSE mount せず context を unmap する。
- `kafs_journal_init()` / `kafs_journal_replay()` は context が v6 descriptor を保持している場合、
  selected descriptor-backed journal segment の header/data offsets を選ぶ dormant path を持つ。
- `fsck.kafs` は v6 image で repair/write option が指定された場合、
  `format v6 repair/write modes are not supported yet.` として拒否する。
- pending log は runtime 初期化時に superblock の `pendinglog_offset` / `pendinglog_size` から
  `c_img_base + offset` を使う。descriptor-backed pending-log lookup はまだ runtime path にない。
- tail metadata は v5 inode tail descriptor と superblock の `tailmeta_offset` / `tailmeta_size` を前提にした
  path で、v6 descriptor shard として runtime routing されていない。
- `kafs_op_init()` は read-only context では早期 return する。write context では pending worker、
  tombstone GC、background dedup worker が条件に応じて起動する。

## 依存関係

| 領域 | 現状 | write admission 前の必須条件 |
| --- | --- | --- |
| Admission / open mode | inspection mount は `O_RDONLY`、`PROT_READ`、read lock、FUSE `ro`。handoff は write mapping を診断だけ行う。 | explicit write opt-in を要求し、`O_RDWR`、`PROT_READ | PROT_WRITE`、write lock、descriptor admission を全て fail closed にする。通常 v6 mount を暗黙 write admission にしない。 |
| Journal write / replay | descriptor-backed segment selection はあるが、supported runtime write mount では未使用。 | `begin` / `commit` / `abort` / `force_flush` / replay reset が selected v6 segment だけに write することを torn-write fixture と regression で証明する。 |
| Bitmap / inode / allocator / HRL | dormant descriptor-backed mapping は実装済み。 | create/write/truncate/fallocate/unlink/rename/link/symlink/copy/reflink/fsync/release を通した live mutation matrix で、対象 shard だけが更新されることを確認する。bitmap word alignment 制約も admission で固定する。 |
| Pending log | v6 descriptor には pending-log shard があるが、runtime は superblock offset 参照。T6 で現段階の v6 admission policy は disabled と決定した。 | v6 write mount では disabled policy を維持して該当 option / delayed mutation request を拒否するか、descriptor-backed pending-log view を実装して replay/drain/worker を検証する。 |
| Tail metadata | v5 tail metadata path は存在するが v6 routing はない。T6 で tail packing / normalization / tail reclaim は disabled と決定した。 | v6 write mount では disabled policy を維持するか、v6 tail-metadata shard を生成・検証・routing する。 |
| Background workers | read-only context では起動しない。write context では pending worker、tombstone GC、background dedup が起動し得る。T6 で v6 runtime context では worker 起動を抑止する policy を追加した。 | v6 write admission では disabled policy を維持するか、descriptor-backed metadata routing と lock/stress tests を先に通す。 |
| fsck / repair | v6 detect-only validation は可能。write/repair modes は拒否。 | write mount 後の unclean state を detect-only で fail closed できることを保証し、repair write を別段階にするか、repair ordering を実装する。 |
| Locking | `.github/lock-policy.md` は rank order と `KAFS_CALL` after lock 禁止を要求する。 | write admission 対象の mutation paths を rank order で監査し、少なくとも contention regression を追加する。 |
| Operator cutover | v6 destination は offline staging と inspection mount まで。 | production write cutover 条件、rollback、post-write fsck、known unsupported options を cutover playbook と release note に明記する。 |

## ブロッカー

v6 write mount は 1 つの gate を外す作業ではなく、少なくとも次のブロッカーを順番に閉じる必要がある。

1. Descriptor-backed journal write/replay proof
2. Live metadata mutation routing proof
3. Pending log / tail metadata / background worker policy
4. v6 post-write fsck and repair policy
5. Lock-order and stress validation
6. Operator cutover and rollback documentation

## 推奨する次タスク

次は `SDW-V6RT-T4 descriptor-backed journal write/replay proof` に進む。理由は、write mount の前に
dirty journal の replay と commit durability を閉じないと、metadata shard の mutation tests が失敗時に
診断不能になるためである。

T4 では write mount をまだ有効化しない。runtime context または focused harness で v6 descriptor-backed
journal segment を選び、`kafs_journal_begin()` / `kafs_journal_commit()` / `kafs_journal_replay()` /
`kafs_journal_force_flush()` が legacy prefix region ではなく selected journal header/data shard だけを
更新することを証明する。
