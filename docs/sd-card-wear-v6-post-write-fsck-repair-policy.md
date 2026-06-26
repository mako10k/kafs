# KAFS format v6 post-write fsck and repair policy

最終更新: 2026-06-26

対象: `SDW-V6RT-T7 v6 post-write fsck and repair policy`

## 目的

v6 write mount を有効化する前に、write 後の image を `fsck.kafs` で安全に判定する境界と、
repair write の解禁順を固定する。この文書は policy checkpoint であり、v6 write mount や v6 repair
write を有効化しない。

## 決定

現段階の format v6 `fsck.kafs` は detect-only validation のみを supported path とする。

| 状態 | 現段階の判定 | repair policy |
| --- | --- | --- |
| Clean descriptor / shard / journal segment | `fsck.kafs --balanced-check <image>` が成功する。 | repair 不要。 |
| Stale or corrupt non-selected descriptor replica | selected descriptor と shard validation が成功すれば警告として報告し、check は成功し得る。 | replica rewrite は未対応。 |
| Same-generation descriptor divergence | `v6 descriptor discovery failed` として非 0 終了する。 | 自動 repair 不可。offline rebuild または別チケットの replica repair が必要。 |
| Metadata shard gap / overlap / physical bounds corruption | `v6 metadata shard validation failed` として非 0 終了する。 | 自動 repair 不可。descriptor-backed repair design が必要。 |
| Torn journal with older valid segment | older valid segment を selected として detect-only check は成功し得る。 | replay/reset write は未対応。 |
| Torn or dirty journal with no valid segment | `v6 metadata shard validation failed` として非 0 終了する。 | 自動 repair 不可。offline rebuild または descriptor-backed journal repair が必要。 |
| v6 repair/write option | `format v6 repair/write modes are not supported yet` として拒否する。 | fail closed。 |

## Required command

production write cutover 前後の required check は次の detect-only command とする。

```sh
fsck.kafs --balanced-check <image>
```

この command が成功しない v6 image は、runtime write cutover の対象にしない。

## Repair 解禁順

repair write を有効化する場合は、write mount とは別チケットで次の順に実装する。

1. descriptor-backed journal replay/reset。selected segment 以外を書かないことを regression で確認する。
2. descriptor replica repair。backup first / primary last の ordering を確認する。
3. metadata shard repair。bitmap、inode、allocator summary、HRL の各 shard routing と write ordering を確認する。
4. operator rollback 手順。repair 前 backup、repair 後 `fsck.kafs --balanced-check`、失敗時の rollback を明記する。

## 検証

- `v6_descriptor_validation` は v6 repair/write option が fail closed することを確認する。
- `v6_descriptor_validation` は same-generation descriptor divergence を `fsck.kafs` が拒否することを確認する。
- `v6_descriptor_validation` は valid journal segment が 0 の torn journal を `fsck.kafs` が拒否することを確認する。
