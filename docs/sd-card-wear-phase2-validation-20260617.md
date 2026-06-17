# SDカード劣化対策 Phase 2 validation（2026-06-17）

対象: `SDW-P2-T5 Phase 2 validation`

## 実行条件

- workload: `scripts/metadata-heatmap-report.sh`
- profile: `conservative`
- image size: 256 MiB
- workload scale: files=240, rounds=2
- 計測出力: `report/perf/metadata-heatmap-20260617-164454`

実行コマンド:

```sh
scripts/metadata-heatmap-report.sh --profile conservative
```

## 検証内容

`metadata-heatmap-report.sh` は次を実行し、field 欠落または期待値不一致を非 0 終了にする。

- `kafsctl fsstat --json` の runtime metadata write counters を取得する。
- `kafsdump --json` の offline `metadata_regions` span summary を取得する。
- SD-card profile の実効設定を確認する。
- runtime counter と offline layout span を region ID/name で結合し、heatmap report を生成する。
- `unknown` metadata write が 0 であることを確認する。
- write-heavy workload が 3 region 以上の metadata counter を増やすことを確認する。

確認した `kafsctl fsstat --json` fields:

- `metadata_write_total`
- `metadata_write_bytes_total`
- `metadata_write_regions`
- `sd_card_profile_str`
- `trim_on_free`
- `atime_policy_str`
- `fsync_policy_str`
- `bg_dedup_enabled`

確認した `kafsdump --json` field:

- `metadata_regions`

## 観測結果

直接観測した report summary:

- `fsstat_version`: 18
- `metadata_write_total`: 58548
- `metadata_write_bytes_total`: 3867191
- `active_regions`: `superblock_checkpoint`, `block_bitmap`, `inode_table`, `hrl_index`,
  `hrl_entries`, `journal_header`, `journal_data`, `pending_log`, `tail_metadata`
- runtime config: `sd_card_profile=conservative`, `trim_on_free=0`,
  `atime_policy=no_runtime_updates`, `fsync_policy=journal_only`, `bg_dedup_enabled=0`
- `unknown`: writes=0, bytes=0

| id | region | writes | bytes | layout bytes | spans |
| --- | --- | ---: | ---: | ---: | ---: |
| 0 | `superblock_checkpoint` | 1184 | 9836 | 256 | 1 |
| 1 | `block_bitmap` | 921 | 7368 | 8027 | 1 |
| 2 | `inode_table` | 2041 | 261248 | 2097152 | 1 |
| 3 | `allocator_summary` | 0 | 0 | 4096 | 1 |
| 4 | `hrl_index` | 382 | 1528 | 32768 | 1 |
| 5 | `hrl_entries` | 49461 | 1187064 | 1155768 | 1 |
| 6 | `journal_header` | 2143 | 94292 | 64 | 1 |
| 7 | `journal_data` | 1574 | 1145423 | 1048512 | 1 |
| 8 | `pending_log` | 757 | 1136272 | 1048576 | 1 |
| 9 | `tail_metadata` | 85 | 24160 | 28672 | 1 |
| 10 | `unknown` | 0 | 0 | 0 | 0 |

## 判定

- `make -j2`: PASS
- `./scripts/static-checks.sh`: PASS
- `KAFS_TEST_MOUNT_TIMEOUT_MS=15000 make check -j2`: PASS（27 tests passed）
- `kafsctl fsstat --json` に期待 fields が出る: PASS
- `kafsdump --json` に期待 fields が出る: PASS
- write-heavy workload で metadata heatmap report を生成できる: PASS
- Phase 3 の format v6 layout 検討前に使う baseline を生成できる状態: PASS
