# SDカード劣化対策 Phase 5 validation（2026-06-26）

対象: `SDW-P5-T4 Phase 5 validation`

## 実行条件

- 対象 workflow: `kafsresize --migrate-create --format-version 6`
- source format: clean v5 image
- destination format: v6 descriptor scaffold image
- compatibility boundary: current v6 image は offline-only staging 対象であり、runtime mount / production
  cutover 対象ではない

実行コマンド:

```sh
make -C tests check TESTS=kafsresize
make check -j2
./scripts/format.sh
./scripts/lint.sh
git diff --check
./scripts/clones.sh
./scripts/static-checks.sh
```

## 検証内容

`tests/tests_kafsresize.c` の regression は次を確認する。

- v6 `--migrate-create --dry-run` は source を変更せず、clean v5 source、destination capacity、descriptor
  replica placement を事前診断する。
- 通常の v6 `--migrate-create` は `--src-image` を必須にし、destination overwrite 前に同じ precheck を実行する。
- source の pre/post `kafsdump --json` summary は一致する。
- destination の `kafsdump --json` summary は v6 descriptor replica、metadata group、各 shard scaffold、
  journal segment summary を報告する。
- destination の `fsck.kafs --balanced-check` は v6 descriptor、bitmap/inode/allocator/HRL/journal shard
  coverage、HRL chain、journal segment health を `status=ok` として報告する。
- destination の runtime mount attempt は v6 admission preflight を実行し、descriptor-backed metadata
  checks が OK であることを診断してから、現行の offline-only gate で exit 2 として拒否する。
- missing source と dirty source は v6 destination を formatting する前に拒否される。

## 互換性境界

直接観測した現在の受け入れ境界:

- v4/v5 image の runtime mount regression は `make check -j2` に含まれ、継続して PASS 対象。
- v6 destination image は `kafsdump --json` と `fsck.kafs --balanced-check` の offline validation 対象。
- v6 runtime mount は admission preflight で descriptor-backed metadata checks を実行したうえで、
  offline-only gate により fail closed する。cutover playbook では v6 を mount / rsync / production
  cutover の対象にせず、offline-validated staged image として保持する。

このため Phase 5 は「offline migration staging workflow」として受け入れる。v6 image の basic runtime mount
semantics と production cutover acceptance は、v6 runtime mount support を有効化する別チケットで扱う。

## 判定

- build/test: PASS
- v5-to-v6 migration regression: PASS
- migrated v6 destination の offline fsck: PASS
- migrated v6 destination の runtime admission / offline-only rejection: PASS
- documentation: PASS
- release notes compatibility impact: PASS
- v6 production runtime mount / cutover: out of scope for current offline-only compatibility boundary
