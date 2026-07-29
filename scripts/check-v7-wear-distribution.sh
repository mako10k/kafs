#!/usr/bin/env bash
set -euo pipefail

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
mkfs_bin=${KAFS_TEST_MKFS:-"$repo_root/src/mkfs.kafs"}
dump_bin=${KAFS_TEST_KAFSDUMP:-"$repo_root/src/kafsdump"}
workdir=$(mktemp -d "${TMPDIR:-/tmp}/kafs-v7-wear.XXXXXX")
trap 'rm -rf "$workdir"' EXIT

image=$workdir/v7-wear.img
"$mkfs_bin" "$image" --format-version 7 --size-bytes 512M --yes >/dev/null
"$dump_bin" --json "$image" >"$workdir/dump.json"

python3 - "$workdir/dump.json" <<'PY'
import json
import sys

with open(sys.argv[1], encoding="utf-8") as stream:
    dump = json.load(stream)

wear = dump["wear_distribution"]
groups = dump["groups"]
assert wear["status"] == "ok"
assert wear["scope"] == "filesystem-placement"
assert wear["distributed"] is True
assert wear["group_count"] == 8 == len(groups)
assert wear["max_group_data_blocks"] - wear["min_group_data_blocks"] <= 64
assert wear["placement_span_bytes"] * 100 >= wear["placement_arena_bytes"] * 70
assert all(groups[index]["metadata_physical_off"] < groups[index + 1]["metadata_physical_off"]
           for index in range(len(groups) - 1))

print(
    "v7 filesystem placement proof: PASS "
    f"groups={wear['group_count']} "
    f"span={wear['placement_span_bytes']}/{wear['placement_arena_bytes']} "
    f"data_blocks={wear['min_group_data_blocks']}..{wear['max_group_data_blocks']}"
)
PY
