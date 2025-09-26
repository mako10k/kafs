AUTOMAKE_OPTIONS = serial-tests
bin_PROGRAMS = kafs mkfs.kafs fsck.kafs kafs-info

# add per-target flags; silence unused-function as headers provide many static helpers
kafs_CFLAGS = $(KAFS_CFLAGS) -Wno-unused-function -Wno-unused-parameter
kafs_LDADD = $(KAFS_LIBS)
kafs_SOURCES = kafs.c kafs_hrl.c kafs_locks.c kafs_journal.c

# new mkfs.kafs target
mkfs_kafs_SOURCES = mkfs_kafs.c kafs_hrl.c kafs_locks.c
mkfs_kafs_CFLAGS = $(KAFS_CFLAGS) -Wno-unused-function -Wno-unused-parameter
mkfs_kafs_LDADD = $(KAFS_LIBS)

# fsck.kafs target (journal check/clear minimal)
fsck_kafs_SOURCES = fsck_kafs.c
fsck_kafs_CFLAGS = $(KAFS_CFLAGS) -Wno-unused-function -Wno-unused-parameter
fsck_kafs_LDADD = $(KAFS_LIBS)

# simple info tool
kafs_info_SOURCES = kafs_info.c
kafs_info_CFLAGS = $(KAFS_CFLAGS) -Wno-unused-function -Wno-unused-parameter
kafs_info_LDADD = $(KAFS_LIBS)

noinst_HEADERS = kafs_block.h kafs_context.h kafs_dirent.h kafs_inode.h kafs_superblock.h kafs.h test_utils.h kafs_journal.h

CFLAGS = -Wall -Werror -g -O0 -D_GNU_SOURCE -D_FILE_OFFSET_BITS=64

# --- Tests ---
# NOTE: stress_fs mounts in multithread mode and exercises heavier workloads.
# To minimize any residual side effects on subsequent tests, run it last.
check_PROGRAMS = hrl_smoketest hrl_dec_ref_by_blo hrl_unconfigured min_git_hooks fs_semantics clone_template_copy git_template_copy_mt rename_overwrite_dirfsync open_unlink_visibility prune_indirect_single prune_indirect_double prune_indirect_triple truncate_prune stress_fs
TESTS = hrl_smoketest hrl_dec_ref_by_blo hrl_unconfigured min_git_hooks fs_semantics clone_template_copy git_template_copy_mt rename_overwrite_dirfsync open_unlink_visibility prune_indirect_single prune_indirect_double prune_indirect_triple truncate_prune stress_fs
hrl_smoketest_SOURCES = tests_hrl_smoketest.c kafs_hrl.c kafs_locks.c test_utils.c
hrl_smoketest_CFLAGS = $(KAFS_CFLAGS) -Wno-unused-function -Wno-unused-parameter
hrl_smoketest_LDADD = $(KAFS_LIBS)

hrl_dec_ref_by_blo_SOURCES = tests_hrl_dec_ref_by_blo.c kafs_hrl.c kafs_locks.c test_utils.c
hrl_dec_ref_by_blo_CFLAGS = $(KAFS_CFLAGS) -Wno-unused-function -Wno-unused-parameter
hrl_dec_ref_by_blo_LDADD = $(KAFS_LIBS)

hrl_unconfigured_SOURCES = tests_hrl_unconfigured.c kafs_hrl.c kafs_locks.c test_utils.c
hrl_unconfigured_CFLAGS = $(KAFS_CFLAGS) -Wno-unused-function -Wno-unused-parameter
hrl_unconfigured_LDADD = $(KAFS_LIBS)

# Stress test that mounts kafs in foreground and runs multi-threaded operations
stress_fs_SOURCES = tests_stress_fs.c test_utils.c kafs_hrl.c kafs_locks.c
stress_fs_CFLAGS = $(KAFS_CFLAGS) -Wno-unused-function -Wno-unused-parameter -pthread
stress_fs_LDADD = $(KAFS_LIBS)
stress_fs_LDFLAGS = -pthread

# Minimal reproduction for .git/hooks and rmdir behaviors
min_git_hooks_SOURCES = tests_min_git_hooks.c test_utils.c kafs_hrl.c kafs_locks.c
min_git_hooks_CFLAGS = $(KAFS_CFLAGS) -Wno-unused-function -Wno-unused-parameter
min_git_hooks_LDADD = $(KAFS_LIBS)

# Filesystem semantics tests (errors, perms, truncation, etc.)
fs_semantics_SOURCES = tests_fs_semantics.c test_utils.c kafs_hrl.c kafs_locks.c
fs_semantics_CFLAGS = $(KAFS_CFLAGS) -Wno-unused-function -Wno-unused-parameter
fs_semantics_LDADD = $(KAFS_LIBS)

# Template copy simulation (like git hooks template copy)
clone_template_copy_SOURCES = tests_clone_template_copy.c test_utils.c kafs_hrl.c kafs_locks.c
clone_template_copy_CFLAGS = $(KAFS_CFLAGS) -Wno-unused-function -Wno-unused-parameter
clone_template_copy_LDADD = $(KAFS_LIBS)

# Concurrent template copy test (multi-file, multi-worker)
git_template_copy_mt_SOURCES = tests_git_template_copy_mt.c test_utils.c kafs_hrl.c kafs_locks.c
git_template_copy_mt_CFLAGS = $(KAFS_CFLAGS) -Wno-unused-function -Wno-unused-parameter -pthread
git_template_copy_mt_LDADD = $(KAFS_LIBS)
git_template_copy_mt_LDFLAGS = -pthread

rename_overwrite_dirfsync_SOURCES = tests_rename_overwrite_dirfsync.c test_utils.c kafs_hrl.c kafs_locks.c
rename_overwrite_dirfsync_CFLAGS = $(KAFS_CFLAGS) -Wno-unused-function -Wno-unused-parameter
rename_overwrite_dirfsync_LDADD = $(KAFS_LIBS)

open_unlink_visibility_SOURCES = tests_open_unlink_visibility.c test_utils.c kafs_hrl.c kafs_locks.c
open_unlink_visibility_CFLAGS = $(KAFS_CFLAGS) -Wno-unused-function -Wno-unused-parameter
open_unlink_visibility_LDADD = $(KAFS_LIBS)

prune_indirect_single_SOURCES = tests_prune_indirect_single.c test_utils.c kafs_hrl.c kafs_locks.c
prune_indirect_single_CFLAGS = $(KAFS_CFLAGS) -Wno-unused-function -Wno-unused-parameter
prune_indirect_single_LDADD = $(KAFS_LIBS)

prune_indirect_double_SOURCES = tests_prune_indirect_double.c test_utils.c kafs_hrl.c kafs_locks.c
prune_indirect_double_CFLAGS = $(KAFS_CFLAGS) -Wno-unused-function -Wno-unused-parameter
prune_indirect_double_LDADD = $(KAFS_LIBS)

prune_indirect_triple_SOURCES = tests_prune_indirect_triple.c test_utils.c kafs_hrl.c kafs_locks.c
prune_indirect_triple_CFLAGS = $(KAFS_CFLAGS) -Wno-unused-function -Wno-unused-parameter
prune_indirect_triple_LDADD = $(KAFS_LIBS)

truncate_prune_SOURCES = tests_truncate_prune.c test_utils.c kafs_hrl.c kafs_locks.c
truncate_prune_CFLAGS = $(KAFS_CFLAGS) -Wno-unused-function -Wno-unused-parameter
truncate_prune_LDADD = $(KAFS_LIBS)

# All tests are expected to pass
XFAIL_TESTS =