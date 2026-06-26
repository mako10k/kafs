# KAFS format v6 lock/stress gate

最終更新: 2026-06-26

対象: `SDW-V6RT-T8 v6 write mount lock/stress gate`

## 目的

T4-T7 で固定した v6 explicit write opt-in 候補 path について、write admission 前の
lock-order blocker と contention regression blocker を閉じる。

この文書は lock/stress gate の監査結果であり、通常の v6 FUSE write mount を有効化しない。

## 監査範囲

対象:

- `src/kafs_locks.c` の rank enforcement と lock wrapper。
- `src/kafs_hrl.c` の HRL put/inc/dec/ref release path。
- `src/kafs_block.h` の bitmap-backed block allocation/free path。
- `tests/tests_v6_descriptor_validation.c` の v6 descriptor admission、HRL mapping、lock stress regression。

対象外:

- `src/kafs.c` の通常 FUSE create/write/truncate/unlink/rename/link/symlink/release/fsync surface 全体。
- production write cutover。
- v6 repair write。

## 直接観測した lock 境界

- `.github/lock-policy.md` は `hrl_global` rank 10、`inode_alloc` rank 20、`inode` rank 30、
  `hrl_bucket` rank 40、`bitmap` rank 50 の昇順取得を要求する。
- `src/kafs_locks.c` は wrapper 内で per-thread rank stack を管理し、低 rank を高 rank の後に取ると
  hard failure にする。
- `src/kafs_locks.c` は `pthread_mutex_trylock()` miss を contention として数え、timed wait と stale owner
  diagnostics を持つ。
- T8 対象ファイルに `KAFS_CALL` after lock の直接使用はない。
  確認コマンド: `rg -n "KAFS_CALL" tests/tests_v6_descriptor_validation.c src/kafs_block.h src/kafs_hrl.c src/kafs_locks.c`
  は match なし。

## 対象 path の lock order

| path | observed lock order | 判定 |
| --- | --- | --- |
| `hrl_reserve_free_slot()` | `hrl_global` only | lower-rank reacquire なし。 |
| `kafs_hrl_put()` existing hit | `hrl_bucket` only | bucket lock 内で完結。 |
| `kafs_hrl_put()` new entry | `hrl_bucket` -> unlock -> `hrl_global` -> unlock -> `hrl_bucket` -> `bitmap` | free-slot reserve は bucket 外で行い、bucket 保持中の block allocation は rank 40 から rank 50。 |
| `kafs_hrl_inc_ref()` | `hrl_bucket` only | bucket lock 内で完結。 |
| `kafs_hrl_dec_ref()` ref remains | `hrl_bucket` only | bucket lock 内で完結。 |
| `kafs_hrl_dec_ref()` final release | `hrl_bucket` -> `bitmap` -> unlock -> `hrl_global` | block free は rank 40 から rank 50。free-list publish は bucket unlock 後に rank 10 を単独取得。 |
| `kafs_blk_alloc()` / free | `bitmap` only | 呼び出し元が `hrl_bucket` を保持する場合も rank 40 から rank 50。 |

この範囲では `.github/lock-policy.md` の rank order に反する path は観測していない。

## 追加 regression

`v6_descriptor_validation` に `v6_write_lock_stress_gate` を追加した。

- 1G の v6 image を作り、bitmap、allocator summary、HRL index、HRL entries を 2 shard に分割する。
- `MAP_PRIVATE` の write mapping で descriptor admission を行う。FUSE mount は行わない。
- `kafs_hrl_open()` で lock state を初期化する。
- `hrl_global`、`inode_alloc`、`inode`、`hrl_bucket`、`bitmap` の各 lock に holder/waiter pair を作り、
  contention counter が増えることを確認する。
- 8 thread x 24 round で `kafs_hrl_put()`、`kafs_hrl_inc_ref()`、`kafs_hrl_dec_ref()`、final
  `kafs_hrl_dec_ref()` を並行実行する。
- HRL bucket lock acquire、bitmap lock acquire、HRL entry write counter が増えることを確認する。
- `tests/Makefile.am` はこの test binary に `-pthread` を付与する。

## 判定

T8 の lock/stress gate は、T4-T7 で固定した explicit write opt-in 候補の低レイヤ path について通過とする。

ただし、通常の v6 FUSE write mount はまだ有効化しない。`src/kafs.c` の user-visible write surface 全体は
今回の監査範囲外であり、operator cutover、rollback、unsupported option、fail-closed admission boundary を
別 gate で固定する必要がある。

## 次の判断

次は `SDW-V6RT-T9 v6 explicit write opt-in cutover boundary` で、以下を固定する。

- user-visible opt-in 名と fail-closed 条件。
- unsupported option と delayed/background mutation の拒否境界。
- write 前後の required fsck command。
- rollback と release note の表現。
