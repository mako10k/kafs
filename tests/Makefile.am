AUTOMAKE_OPTIONS = serial-tests subdir-objects

# Keep these tests self-contained and avoid polluting the repo tree: each test
# enters a fresh workdir under ${TMPDIR:-/tmp} at startup.

AM_CPPFLAGS = -I$(top_srcdir)/src -I$(srcdir)

# Match src/ build flags for consistent warnings/debug.
CFLAGS = -Wall -Werror -g -O0 -D_GNU_SOURCE -D_FILE_OFFSET_BITS=64 \
	-Wno-unused-function -Wno-unused-parameter

noinst_HEADERS = test_utils.h

# Provide deterministic paths to the built tools for tests that exec them.
AM_TESTS_ENVIRONMENT = \
	KAFS_TEST_KAFS='$(top_builddir)/src/kafs'; \
	KAFS_TEST_KAFSCTL='$(top_builddir)/src/kafsctl'; \
	KAFS_TEST_MKFS='$(top_builddir)/src/mkfs.kafs'; \
	KAFS_TEST_FSCK='$(top_builddir)/src/fsck.kafs'; \
	KAFS_TEST_KAFS_BACK='$(top_builddir)/src/kafs-back'; \
	export KAFS_TEST_KAFS KAFS_TEST_KAFSCTL KAFS_TEST_MKFS KAFS_TEST_FSCK KAFS_TEST_KAFS_BACK;

# --- Tests ---
# NOTE: stress_fs mounts in multithread mode and exercises heavier workloads.
# To minimize any residual side effects on subsequent tests, run it last.
check_PROGRAMS = hrl_smoketest hrl_dec_ref_by_blo hrl_unconfigured min_git_hooks fs_semantics \
	clone_template_copy git_template_copy_mt rename_overwrite_dirfsync open_unlink_visibility \
	prune_indirect_single prune_indirect_double prune_indirect_triple truncate_prune reflink_clone \
	stress_fs hotplug_rpc

TESTS = $(check_PROGRAMS)

hrl_smoketest_SOURCES = tests_hrl_smoketest.c test_utils.c \
	$(top_srcdir)/src/kafs_hrl.c $(top_srcdir)/src/kafs_locks.c
hrl_smoketest_CFLAGS = $(KAFS_CFLAGS) -Wno-unused-function -Wno-unused-parameter
hrl_smoketest_LDADD = $(KAFS_LIBS)

hrl_dec_ref_by_blo_SOURCES = tests_hrl_dec_ref_by_blo.c test_utils.c \
	$(top_srcdir)/src/kafs_hrl.c $(top_srcdir)/src/kafs_locks.c
hrl_dec_ref_by_blo_CFLAGS = $(KAFS_CFLAGS) -Wno-unused-function -Wno-unused-parameter
hrl_dec_ref_by_blo_LDADD = $(KAFS_LIBS)

hrl_unconfigured_SOURCES = tests_hrl_unconfigured.c test_utils.c \
	$(top_srcdir)/src/kafs_hrl.c $(top_srcdir)/src/kafs_locks.c
hrl_unconfigured_CFLAGS = $(KAFS_CFLAGS) -Wno-unused-function -Wno-unused-parameter
hrl_unconfigured_LDADD = $(KAFS_LIBS)

min_git_hooks_SOURCES = tests_min_git_hooks.c test_utils.c \
	$(top_srcdir)/src/kafs_hrl.c $(top_srcdir)/src/kafs_locks.c
min_git_hooks_CFLAGS = $(KAFS_CFLAGS) -Wno-unused-function -Wno-unused-parameter
min_git_hooks_LDADD = $(KAFS_LIBS)

fs_semantics_SOURCES = tests_fs_semantics.c test_utils.c \
	$(top_srcdir)/src/kafs_hrl.c $(top_srcdir)/src/kafs_locks.c
fs_semantics_CFLAGS = $(KAFS_CFLAGS) -Wno-unused-function -Wno-unused-parameter
fs_semantics_LDADD = $(KAFS_LIBS)

clone_template_copy_SOURCES = tests_clone_template_copy.c test_utils.c \
	$(top_srcdir)/src/kafs_hrl.c $(top_srcdir)/src/kafs_locks.c
clone_template_copy_CFLAGS = $(KAFS_CFLAGS) -Wno-unused-function -Wno-unused-parameter
clone_template_copy_LDADD = $(KAFS_LIBS)

git_template_copy_mt_SOURCES = tests_git_template_copy_mt.c test_utils.c \
	$(top_srcdir)/src/kafs_hrl.c $(top_srcdir)/src/kafs_locks.c
git_template_copy_mt_CFLAGS = $(KAFS_CFLAGS) -Wno-unused-function -Wno-unused-parameter -pthread
git_template_copy_mt_LDADD = $(KAFS_LIBS)
git_template_copy_mt_LDFLAGS = -pthread

rename_overwrite_dirfsync_SOURCES = tests_rename_overwrite_dirfsync.c test_utils.c \
	$(top_srcdir)/src/kafs_hrl.c $(top_srcdir)/src/kafs_locks.c
rename_overwrite_dirfsync_CFLAGS = $(KAFS_CFLAGS) -Wno-unused-function -Wno-unused-parameter
rename_overwrite_dirfsync_LDADD = $(KAFS_LIBS)

open_unlink_visibility_SOURCES = tests_open_unlink_visibility.c test_utils.c \
	$(top_srcdir)/src/kafs_hrl.c $(top_srcdir)/src/kafs_locks.c
open_unlink_visibility_CFLAGS = $(KAFS_CFLAGS) -Wno-unused-function -Wno-unused-parameter
open_unlink_visibility_LDADD = $(KAFS_LIBS)

prune_indirect_single_SOURCES = tests_prune_indirect_single.c test_utils.c \
	$(top_srcdir)/src/kafs_hrl.c $(top_srcdir)/src/kafs_locks.c
prune_indirect_single_CFLAGS = $(KAFS_CFLAGS) -Wno-unused-function -Wno-unused-parameter
prune_indirect_single_LDADD = $(KAFS_LIBS)

prune_indirect_double_SOURCES = tests_prune_indirect_double.c test_utils.c \
	$(top_srcdir)/src/kafs_hrl.c $(top_srcdir)/src/kafs_locks.c
prune_indirect_double_CFLAGS = $(KAFS_CFLAGS) -Wno-unused-function -Wno-unused-parameter
prune_indirect_double_LDADD = $(KAFS_LIBS)

prune_indirect_triple_SOURCES = tests_prune_indirect_triple.c test_utils.c \
	$(top_srcdir)/src/kafs_hrl.c $(top_srcdir)/src/kafs_locks.c
prune_indirect_triple_CFLAGS = $(KAFS_CFLAGS) -Wno-unused-function -Wno-unused-parameter
prune_indirect_triple_LDADD = $(KAFS_LIBS)

truncate_prune_SOURCES = tests_truncate_prune.c test_utils.c \
	$(top_srcdir)/src/kafs_hrl.c $(top_srcdir)/src/kafs_locks.c
truncate_prune_CFLAGS = $(KAFS_CFLAGS) -Wno-unused-function -Wno-unused-parameter
truncate_prune_LDADD = $(KAFS_LIBS)

reflink_clone_SOURCES = tests_reflink_clone.c test_utils.c \
	$(top_srcdir)/src/kafs_hrl.c $(top_srcdir)/src/kafs_locks.c
reflink_clone_CFLAGS = $(KAFS_CFLAGS) -Wno-unused-function -Wno-unused-parameter
reflink_clone_LDADD = $(KAFS_LIBS)

stress_fs_SOURCES = tests_stress_fs.c test_utils.c \
	$(top_srcdir)/src/kafs_hrl.c $(top_srcdir)/src/kafs_locks.c
stress_fs_CFLAGS = $(KAFS_CFLAGS) -Wno-unused-function -Wno-unused-parameter -pthread
stress_fs_LDADD = $(KAFS_LIBS)
stress_fs_LDFLAGS = -pthread

hotplug_rpc_SOURCES = tests_hotplug_rpc.c $(top_srcdir)/src/kafs_rpc.c
hotplug_rpc_CFLAGS = $(KAFS_CFLAGS) -Wno-unused-function -Wno-unused-parameter
hotplug_rpc_LDADD = $(KAFS_LIBS)

# All tests are expected to pass
XFAIL_TESTS =
