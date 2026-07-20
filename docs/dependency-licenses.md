# Dependency License Inventory

KAFS source code is licensed under Apache-2.0. The following dependencies and
externally maintained files retain their own licenses. They are not relicensed
by this repository.

| Component | Use | Distribution status | License |
| --- | --- | --- | --- |
| libfuse3 | Runtime filesystem interface | Dynamically linked; not vendored | LGPL-2.1-or-later |
| libc and pthread | Runtime platform interfaces | Supplied by the target system | Platform implementation license |
| `test-driver` | Automake test harness | Source copy distributed in this repository | GPL-2.0-or-later WITH Autoconf-exception-3.0 |
| Autoconf, Automake, compiler, pkg-config | Build tooling | Not included in KAFS artifacts | Tool-specific licenses |
| clang-format, cppcheck, lizard, jscpd | Static analysis | CI/development only | Tool-specific licenses |
| REUSE 6.2.0 | License compliance check | CI/development only | Tool-specific licenses |

The authoritative per-file declarations and full license texts are maintained
in `REUSE.toml` and `LICENSES/`. Run `./scripts/license-check.sh` after adding or
moving files.
