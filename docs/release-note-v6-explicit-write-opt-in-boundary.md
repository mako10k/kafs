# Draft release note: format v6 controlled write opt-in boundary

Status: experimental boundary. This note documents the release wording for the
explicit controlled opt-in and does not announce format v6 write mount as a
default production path.

## Summary

Format v6 write mount remains disabled by default. The experimental controlled
write mount requires explicit `rw,v6_write_mount` opt-in and fails closed unless
descriptor validation, journal health, metadata shard coverage, lock policy, and
operator fsck requirements are all satisfied.

## Compatibility

- Existing v4/v5 runtime mounts are unchanged.
- v6 inspection remains read-only through `-o ro,v6_inspection_mount`.
- Normal v6 runtime mount attempts remain fail-closed unless the explicit
  controlled write opt-in is present.

## Controlled opt-in scope

The user-visible entrypoints are:

- `--v6-write-mount`
- `-o v6_write_mount`
- `-o v6-write-mount`

The controlled opt-in is experimental and must be documented as operationally
bounded. It is not a production default.

## Initial unsupported scope

The initial controlled write surface is limited to regular-file
create/write/fsync/release.

The initial controlled opt-in excludes:

- truncate/fallocate/unlink/rename/link/symlink/copy/reflink
- writeback cache
- runtime TRIM
- pending log / pending worker
- tail metadata packing or reclaim
- tombstone GC
- background dedup scan
- v6 repair write

`fsck.kafs` remains detect-only for v6 write outcomes. Operators must run
`fsck.kafs --balanced-check <image>` before and after a write smoke session.

The repeatable helper is:

```sh
scripts/v6-controlled-write-smoke.sh --image <image> --yes
```

It records timestamped before/after dumps, fsck stdout/stderr, mount log,
image stat/digest, and the exact regular-file workload under `report/` by
default.

## Durability and copy fallback notes

The controlled write smoke covers zero-filled block materialization, partial
block overwrite, ENOSPC handling, explicit `fsync` / `fdatasync`, unmount, and
post-write `fsck.kafs --balanced-check`.

Explicit copy/reflink interfaces remain unsupported. `KAFS_IOCTL_COPY`,
`FICLONE`, and the FUSE copy hook fail closed. On some kernels,
`copy_file_range(2)` may be satisfied by a generic read/write fallback before
the FUSE copy hook is reached; that fallback is treated as ordinary regular-file
read/write, not as proof that copy/reflink is supported.

## Rollback wording

If validation fails, do not repair-write the v6 image in place. Stop new writes,
unmount the v6 image, preserve the failed image and logs for diagnosis, and
return to the known-good source image when its write-freeze boundary is still
valid.

Preserve at least the failed destination image, mount log, exact write workload,
`kafsdump --json` before/after output, `fsck.kafs --balanced-check` stdout and
stderr, and any image digest/stat metadata captured during cutover.
