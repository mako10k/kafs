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
check_PROGRAMS = hrl_smoketest hrl_dec_ref_by_blo hrl_unconfigured stress_fs min_git_hooks fs_semantics clone_template_copy git_template_copy_mt rename_overwrite_dirfsync open_unlink_visibility
TESTS = hrl_smoketest hrl_dec_ref_by_blo hrl_unconfigured stress_fs min_git_hooks fs_semantics clone_template_copy git_template_copy_mt rename_overwrite_dirfsync open_unlink_visibility
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

XFAIL_TESTS = git_template_copy_mt