# Concurrency Hardening Plan

Checklist (phased):

Phase 0 (done)
- [x] Advisory file lock on image to prevent multi-process RW mount
- [x] Default single-threaded FUSE (-s), enable MT via env KAFS_MT=1

Phase 1 (HRL safety)
- [ ] Add per-bucket mutex array and a global HRL mutex in context
- [ ] Initialize/destroy locks in hrl_open/close
- [ ] Guard HRL ops (find/put/inc/dec/remove/dec_by_blo) with locks

Phase 2 (Allocator/bitmap safety)
- [ ] Add bitmap mutex in context; guard allocation/free/mark
- [ ] Define lock ordering to avoid deadlocks (global -> bitmap -> HRL bucket)

Phase 3 (Inode/dir entries safety)
- [ ] Add inode/dirent mutexes or coarse-grained FS mutex
- [ ] Protect inode table updates and directory modifications

Phase 4 (Validation)
- [ ] Add parallel stress tests (N threads, same/different data)
- [ ] Add ThreadSanitizer CI job

Notes:
- Start with HRL to eliminate primary races in dedup index and refcounts.
- Keep lock scopes minimal; avoid holding I/O under locks where feasible.