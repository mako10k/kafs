# Draft release note: format v6 controlled write opt-in boundary

Status: planning boundary. This note documents the release wording for a future
controlled opt-in and does not announce an enabled v6 write mount.

## Summary

Format v6 write mount remains disabled by default. A future controlled write
mount must require an explicit `rw,v6_write_mount` opt-in and must fail closed
unless descriptor validation, journal health, metadata shard coverage, lock
policy, and operator fsck requirements are all satisfied.

## Compatibility

- Existing v4/v5 runtime mounts are unchanged.
- v6 inspection remains read-only through `-o ro,v6_inspection_mount`.
- Normal v6 runtime mount attempts remain fail-closed until the write admission
  wiring is implemented and validated.

## Controlled opt-in scope

The reserved user-visible entrypoints are:

- `--v6-write-mount`
- `-o v6_write_mount`
- `-o v6-write-mount`

The controlled opt-in is experimental and must be documented as operationally
bounded. It is not a production default.

## Initial unsupported scope

The initial controlled opt-in excludes:

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
