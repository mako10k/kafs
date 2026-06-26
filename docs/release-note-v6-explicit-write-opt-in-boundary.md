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

## Rollback wording

If validation fails, do not repair-write the v6 image in place. Stop new writes,
unmount the v6 image, preserve the failed image and logs for diagnosis, and
return to the known-good source image when its write-freeze boundary is still
valid.
