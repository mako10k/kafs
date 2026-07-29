#!/usr/bin/env python3
"""Capture the T59 semantic inventory from one read-only mounted tree."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import stat
import sys
from typing import NoReturn


def fail(message: str) -> NoReturn:
    raise SystemExit(f"mounted inventory: {message}")


def payload_digest(path: Path, mode: int) -> str | None:
    digest = hashlib.sha256()
    if stat.S_ISDIR(mode):
        return None
    if stat.S_ISLNK(mode):
        target = os.readlink(os.fsencode(path))
        if not isinstance(target, bytes):
            target = os.fsencode(target)
        if not target:
            fail(f"empty symlink payload: {path}")
        digest.update(target)
        return digest.hexdigest()
    if not stat.S_ISREG(mode):
        fail(f"unsupported object type: {path}")
    with path.open("rb", buffering=0) as stream:
        while chunk := stream.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def normalized_relative(root: Path, path: Path) -> str:
    if path == root:
        return "."
    value = path.relative_to(root).as_posix()
    pure = PurePosixPath(value)
    if pure.is_absolute() or str(pure) != value or any(part in ("", ".", "..") for part in pure.parts):
        fail(f"non-canonical relative path: {value}")
    return value


def collect(root: Path) -> dict:
    pending = [root]
    records: list[tuple[str, Path, os.stat_result]] = []
    while pending:
        path = pending.pop()
        try:
            info = path.lstat()
        except OSError as exc:
            fail(f"cannot stat {path}: {exc}")
        relative = normalized_relative(root, path)
        records.append((relative, path, info))
        if stat.S_ISDIR(info.st_mode):
            try:
                children = sorted(path.iterdir(), key=lambda item: os.fsencode(item.name), reverse=True)
            except OSError as exc:
                fail(f"cannot enumerate {path}: {exc}")
            pending.extend(children)

    records.sort(key=lambda item: item[0])
    by_object: dict[str, list[tuple[str, Path, os.stat_result]]] = {}
    for relative, path, info in records:
        object_id = f"ino-{info.st_ino:020d}"
        by_object.setdefault(object_id, []).append((relative, path, info))

    objects = []
    entries = []
    for relative, _path, info in records:
        entries.append({"path": relative, "object_id": f"ino-{info.st_ino:020d}"})

    for object_id in sorted(by_object):
        aliases = by_object[object_id]
        relative, path, info = aliases[0]
        mode = info.st_mode
        if stat.S_ISDIR(mode):
            kind = "directory"
            logical_size = 0
        elif stat.S_ISREG(mode):
            kind = "regular"
            logical_size = info.st_size
        elif stat.S_ISLNK(mode):
            kind = "symlink"
            logical_size = info.st_size
        else:
            fail(f"unsupported object type: {relative}")
        if kind != "regular" and len(aliases) != 1:
            fail(f"only regular objects may have multiple paths: {object_id}")
        objects.append(
            {
                "object_id": object_id,
                "type": kind,
                "permissions": f"{stat.S_IMODE(mode):04o}",
                "uid": info.st_uid,
                "gid": info.st_gid,
                "atime_ns": info.st_atime_ns,
                "mtime_ns": info.st_mtime_ns,
                "size_bytes": logical_size,
                "link_count": len(aliases),
                "payload_sha256": payload_digest(path, mode),
            }
        )

    return {
        "schema": "KAFS.MountedSemanticInventory.v1",
        "objects": objects,
        "entries": entries,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    root = Path(args.root)
    output = Path(args.output)
    if root.is_symlink() or not root.is_dir():
        fail(f"root must be a non-symlink directory: {root}")
    if output.exists() or output.is_symlink():
        fail(f"output must not already exist: {output}")
    inventory = collect(root)
    with output.open("x", encoding="utf-8") as stream:
        stream.write(json.dumps(inventory, indent=2, sort_keys=True) + "\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
