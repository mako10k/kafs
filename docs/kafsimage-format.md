# kafsimage Output Format (v0)

This document specifies the current output behavior of `kafsimage`.

## Command Modes

Exactly one mode must be specified:

- `--metadata-only`
- `--raw`
- `--sparse`

Optional:

- `--verify`

Syntax:

```sh
kafsimage --metadata-only [--verify] <src-image> <dst-file>
kafsimage --raw [--verify] <src-image> <dst-file>
kafsimage --sparse [--verify] <src-image> <dst-file>
```

## Common Rules

- Source image is opened read-only.
- Destination file is created/truncated and written as output.
- Exit codes:
  - `0`: success
  - `1`: I/O, bounds, or verification error
  - `2`: usage error

## Mode Semantics

### `--metadata-only`

Copies only the metadata prefix:

- Range: `[0, first_data_block * block_size)`
- `first_data_block` and `block_size` are read from superblock.
- Fails if the computed range exceeds source file size.

Result:

- Destination logical size equals metadata prefix length.

### `--raw`

Copies full source image byte-for-byte.

Result:

- Destination content and size match source image exactly.

### `--sparse`

Copies full source image range, but writes all-zero chunks as holes.

- Logical destination size is preserved using `ftruncate`.
- Non-zero chunks are written normally.

Result:

- Byte-equivalent to source when read back.
- Physical disk usage may be smaller than `--raw`.

## Verification (`--verify`)

When enabled:

1. Destination size is checked against expected copied range size.
2. Source and destination bytes are compared over copied range.

- `--metadata-only`: compare metadata prefix length.
- `--raw`/`--sparse`: compare full source length.

Any mismatch returns exit code `1`.

## Stability Notes

- This v0 format is intentionally simple and file-based.
- No custom container header is added yet.
- Future versions may introduce structured headers for richer metadata exports.
