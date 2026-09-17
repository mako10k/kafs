#!/usr/bin/env python3
"""Exercise the bounded v7 public write surface on an already-mounted image.

This is an internal workload engine, not a device opener or qualification gate.
The caller owns device identity, formatting, mount, offline checks, and cleanup.
Python 3.5 syntax is intentional for the Moxa test host.
"""

from __future__ import print_function

import argparse
import errno
import hashlib
import json
import os
import sys
import time
import traceback


CASES = (
    "create_inline_write",
    "direct_contiguous_growth",
    "direct_multi_block_overwrite",
    "direct_partial_overwrite",
    "direct_shrink_truncate",
    "directory_direct_append_and_growth",
    "directory_inline_append_and_growth",
    "open_truncate",
    "regular_file_inline_to_direct_promotion",
    "regular_file_single_indirect_lifecycle",
    "regular_file_double_indirect_lifecycle",
    "regular_file_triple_indirect_lifecycle",
)


def checked(condition, message):
    if not condition:
        raise AssertionError(message)


def v7_mount_present(mountpoint):
    with open("/proc/self/mountinfo", "r") as stream:
        for line in stream:
            parts = line.rstrip("\n").split(" - ", 1)
            if len(parts) != 2:
                continue
            left = parts[0].split()
            right = parts[1].split()
            if (len(left) > 4 and len(right) > 1 and
                    left[4] == mountpoint and right[0] == "fuse.kafs-v7" and
                    right[1] == "kafs-v7"):
                return True
    return False


class Workload(object):
    def __init__(self, mountpoint, block_size):
        self.mountpoint = mountpoint
        self.block_size = block_size
        self.cases = []
        self.bytes_written = 0
        self.started_at = time.time()

    def mark(self, name, detail):
        checked(name in CASES, "unknown case: " + name)
        checked(name not in [entry["name"] for entry in self.cases],
                "duplicate case: " + name)
        entry = {"name": name, "status": "PASS", "detail": detail,
                 "elapsed_seconds": round(time.time() - self.started_at, 3)}
        self.cases.append(entry)
        print("CASE PASS {}: {}".format(name, detail), flush=True)

    def write_all(self, fd, data):
        offset = 0
        while offset < len(data):
            count = os.write(fd, data[offset:])
            checked(count > 0, "short write without progress")
            offset += count
            self.bytes_written += count

    def write_at(self, fd, offset, data):
        os.lseek(fd, offset, os.SEEK_SET)
        self.write_all(fd, data)

    def read_at(self, fd, offset, size):
        os.lseek(fd, offset, os.SEEK_SET)
        output = bytearray()
        while len(output) < size:
            data = os.read(fd, size - len(output))
            checked(data, "unexpected EOF at {}".format(offset + len(output)))
            output.extend(data)
        return bytes(output)

    def grow_by_writes(self, fd, target_size):
        current = os.fstat(fd).st_size
        checked(target_size >= current, "growth target precedes current size")
        os.lseek(fd, current, os.SEEK_SET)
        pattern = bytes(bytearray((index * 13 + 7) % 256 for index in range(65536)))
        next_notice = ((current // (4 * 1024 * 1024)) + 1) * (4 * 1024 * 1024)
        while current < target_size:
            size = min(len(pattern), target_size - current)
            self.write_all(fd, pattern[:size])
            current += size
            if current >= next_notice:
                print("GROWTH {} bytes".format(current), flush=True)
                next_notice += 4 * 1024 * 1024
        checked(os.fstat(fd).st_size == target_size, "growth size mismatch")

    def directory_cases(self):
        initial = os.stat(self.mountpoint).st_size
        checked(initial <= 60, "root directory is not initially inline")
        first = os.path.join(self.mountpoint, "d000")
        fd = os.open(first, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        os.close(fd)
        inline_size = os.stat(self.mountpoint).st_size
        checked(inline_size > initial and inline_size <= 60,
                "first directory append did not remain inline")
        self.mark("directory_inline_append_and_growth",
                  "root size {} -> {} bytes".format(initial, inline_size))

        created = 1
        while os.stat(self.mountpoint).st_size <= 2 * self.block_size:
            checked(created < 300, "directory growth did not reach two blocks")
            name = "d{:03d}_".format(created) + "x" * 235
            fd = os.open(os.path.join(self.mountpoint, name),
                         os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
            os.close(fd)
            created += 1
        checked(len([name for name in os.listdir(self.mountpoint)
                     if name.startswith("d")]) == created,
                "directory entries missing after growth")
        self.mark("directory_direct_append_and_growth",
                  "{} entries, root size {} bytes".format(
                      created, os.stat(self.mountpoint).st_size))

    def file_cases(self):
        block = self.block_size
        direct_blocks = 12
        pointers = block // 4
        single_blocks = direct_blocks + pointers
        double_blocks = single_blocks + pointers * pointers
        triple_target = (double_blocks + 2) * block
        checked(triple_target < 80 * 1024 * 1024,
                "triple workload exceeds bounded 80 MiB logical size")

        path = os.path.join(self.mountpoint, "normal.bin")
        fd = os.open(path, os.O_RDWR | os.O_CREAT | os.O_EXCL, 0o600)
        try:
            inline = b"v7-inline-create"
            self.write_all(fd, inline)
            os.fsync(fd)
            checked(self.read_at(fd, 0, len(inline)) == inline,
                    "inline readback mismatch")
            self.mark("create_inline_write", "{} bytes, fsync and readback".format(len(inline)))

            self.grow_by_writes(fd, block)
            os.fsync(fd)
            checked(os.fstat(fd).st_size == block, "promotion size mismatch")
            self.mark("regular_file_inline_to_direct_promotion",
                      "inline to one {}-byte block".format(block))

            patch = bytes(bytearray((index * 17 + 3) % 256 for index in range(97)))
            self.write_at(fd, 31, patch)
            os.fsync(fd)
            checked(self.read_at(fd, 31, len(patch)) == patch,
                    "partial overwrite mismatch")
            self.mark("direct_partial_overwrite", "97 bytes at offset 31")

            multi = bytes(bytearray((index * 19 + 5) % 256
                                    for index in range(block + 53)))
            self.write_at(fd, block - 29, multi)
            os.fsync(fd)
            checked(self.read_at(fd, block - 29, len(multi)) == multi,
                    "multi-block overwrite mismatch")
            self.mark("direct_multi_block_overwrite",
                      "{} bytes across block boundary".format(len(multi)))

            try:
                os.ftruncate(fd, 60)
            except OSError as error:
                checked(error.errno in (errno.EOPNOTSUPP, errno.ENOTSUP),
                        "unexpected direct-to-inline errno {}".format(error.errno))
            else:
                raise AssertionError("nonzero direct-to-inline truncate succeeded")

            self.grow_by_writes(fd, direct_blocks * block)
            os.fsync(fd)
            self.mark("direct_contiguous_growth",
                      "{} blocks".format(direct_blocks))

            self.grow_by_writes(fd, (direct_blocks + 2) * block)
            os.fsync(fd)
            checked(os.fstat(fd).st_size == (direct_blocks + 2) * block,
                    "single-indirect growth mismatch")

            self.grow_by_writes(fd, (single_blocks + 2) * block)
            os.fsync(fd)
            checked(os.fstat(fd).st_size == (single_blocks + 2) * block,
                    "double-indirect growth mismatch")

            self.grow_by_writes(fd, triple_target)
            os.fsync(fd)
            checked(os.fstat(fd).st_size == triple_target,
                    "triple-indirect growth mismatch")
            tail = self.read_at(fd, triple_target - block, block)
            checked(len(tail) == block, "triple tail readback mismatch")
            os.ftruncate(fd, (single_blocks + 1) * block + 53)
            os.fsync(fd)
            self.mark("regular_file_triple_indirect_lifecycle",
                      "grow to {} bytes and shrink to double-indirect".format(triple_target))

            os.ftruncate(fd, (direct_blocks + 1) * block + 53)
            os.fsync(fd)
            self.mark("regular_file_double_indirect_lifecycle",
                      "shrink through double-to-single boundary")

            os.ftruncate(fd, block + 53)
            os.fsync(fd)
            self.mark("regular_file_single_indirect_lifecycle",
                      "shrink through single-to-direct boundary")

            os.ftruncate(fd, 0)
            os.fsync(fd)
            checked(os.fstat(fd).st_size == 0, "zero truncate mismatch")
            self.mark("direct_shrink_truncate",
                      "direct-to-zero truncate; nonzero direct-to-inline rejected")
        finally:
            os.close(fd)

        self.open_truncate_case()
        return self.create_proof()

    def create_proof(self):
        block = self.block_size
        proof = os.path.join(self.mountpoint, "proof.bin")
        payload = bytes(bytearray((index * 23 + 11) % 256 for index in range(block)))
        fd = os.open(proof, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        try:
            self.write_all(fd, payload)
            os.fsync(fd)
        finally:
            os.close(fd)
        return {"path": "proof.bin", "sha256": hashlib.sha256(payload).hexdigest(),
                "size": len(payload)}

    def open_truncate_case(self, resume_existing=False):
        block = self.block_size
        path = os.path.join(self.mountpoint, "open-truncate.bin")
        if resume_existing:
            checked(os.stat(path).st_size == 0,
                    "incomplete open-truncate file is not empty")
            fd = os.open(path, os.O_WRONLY)
        else:
            fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        try:
            self.write_all(fd, b"T" * block)
            self.write_all(fd, b"T" * 53)
            os.fsync(fd)
        finally:
            os.close(fd)
        fd = os.open(path, os.O_WRONLY | os.O_TRUNC)
        os.fsync(fd)
        os.close(fd)
        checked(os.stat(path).st_size == 0, "O_TRUNC did not clear file")
        self.mark("open_truncate", "O_TRUNC cleared a block-backed file")

    def run(self):
        checked(v7_mount_present(self.mountpoint),
                "mountpoint is not a dedicated kafs-v7 FUSE mount")
        self.directory_cases()
        proof = self.file_cases()
        checked(set(entry["name"] for entry in self.cases) == set(CASES),
                "normal workload case coverage incomplete")
        return proof


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mountpoint", required=True)
    parser.add_argument("--block-size", type=int, required=True)
    parser.add_argument("--report", required=True)
    parser.add_argument("--resume-open-truncate-from")
    args = parser.parse_args()
    mountpoint = os.path.abspath(args.mountpoint)
    checked(args.block_size == 1024, "this bounded workload requires 1024-byte blocks")
    workload = Workload(mountpoint, args.block_size)
    report = {"schema": "KAFS.V7RealMediaNormalWorkload.v1",
              "mountpoint": mountpoint, "block_size": args.block_size,
              "expected_cases": list(CASES), "cases": [], "status": "FAIL"}
    try:
        if args.resume_open_truncate_from:
            with open(args.resume_open_truncate_from, "rb") as stream:
                prior_bytes = stream.read()
            prior = json.loads(prior_bytes.decode("utf-8"))
            names = [entry["name"] for entry in prior["cases"]]
            checked(prior["schema"] == "KAFS.V7RealMediaNormalWorkload.v1" and
                    prior["status"] == "FAIL" and
                    prior["block_size"] == args.block_size and
                    prior["expected_cases"] == list(CASES) and
                    len(names) == len(set(names)) and
                    set(names) == set(CASES) - {"open_truncate"} and
                    all(entry["status"] == "PASS" for entry in prior["cases"]) and
                    "[Errno 95]" in prior["error"],
                    "prior report is not the expected single-case failure")
            checked(v7_mount_present(mountpoint),
                    "mountpoint is not a dedicated kafs-v7 FUSE mount")
            workload.open_truncate_case(resume_existing=True)
            report["proof"] = workload.create_proof()
            report["schema"] = "KAFS.V7RealMediaNormalCompletion.v1"
            report["prior_report_path"] = os.path.abspath(args.resume_open_truncate_from)
            report["prior_report_sha256"] = hashlib.sha256(prior_bytes).hexdigest()
            report["prior_cases"] = names
            report["combined_cases"] = sorted(set(names) | {"open_truncate"})
            report["status"] = "PASS_WITH_COMPLETION"
        else:
            report["proof"] = workload.run()
            report["status"] = "PASS"
    except Exception as error:
        report["error"] = "{}: {}".format(type(error).__name__, error)
        traceback.print_exc()
    report["cases"] = workload.cases
    report["bytes_written_by_workload"] = workload.bytes_written
    report["elapsed_seconds"] = round(time.time() - workload.started_at, 3)
    with open(args.report, "w") as stream:
        json.dump(report, stream, sort_keys=True, indent=2)
        stream.write("\n")
    return 0 if report["status"] in ("PASS", "PASS_WITH_COMPLETION") else 1


if __name__ == "__main__":
    sys.exit(main())
