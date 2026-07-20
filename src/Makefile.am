AM_CPPFLAGS = -include $(srcdir)/kafs_config.h

bin_PROGRAMS = kafs kafs-v6 kafs-v7 mkfs.kafs fsck.kafs kafs-info kafsdump kafsimage kafsresize kafsctl kafs-front kafs-back
sbin_SCRIPTS = mount.kafs
EXTRA_DIST = mount.kafs.in
CLEANFILES = $(sbin_SCRIPTS)

# add per-target flags; silence unused-function as headers provide many static helpers
kafs_CFLAGS = $(KAFS_CFLAGS) -Wno-unused-function -Wno-unused-parameter
kafs_LDADD = $(KAFS_LIBS)
kafs_SOURCES = kafs.c kafs_shared_fuse_runtime.c kafs_hrl.c kafs_lock_order.c kafs_locks.c kafs_journal.c kafs_rpc.c

# Runtime source ownership:
# - kafs is the production v4/v5 runtime and does not own descriptor-backed
#   runtime admission.
# - kafs_shared_fuse_runtime.c owns the shared FUSE operation implementation and
#   request runner used by production kafs, format-specific runtime entrypoints,
#   and internal control tools.
# - kafs.c is the production kafs process entrypoint wrapper only; kafs-v6/v7 must
#   not link it.
# - kafs-v6 is a temporary fail-closed retirement placeholder. Remove the
#   entrypoint only after the remaining v6 offline/layout sources are retired.
# - kafs-v7 is the next descriptor-backed runtime entrypoint. It has separate
#   v7 entrypoint/runtime/adapter sources. v7 layout wire ownership lives in
#   kafs_v7_layout.c/.h and is not a v6 descriptor compatibility facade.
# - v7 must not directly reuse v5/v6-owned entrypoints, layout APIs, or
#   admission paths as compatibility shortcuts. Copy behavior into v7-owned
#   resources or extract a neutral helper before sharing it.
# - the v7 format-specific entrypoint links the shared FUSE runtime source with
#   KAFS_SHARED_FUSE_RUNNER_EXPORT while admission/open/init policy stays in
#   format-entrypoint sources.
# - kafs_v7_mount_options.* owns v7-specific -o token interpretation.
#   kafs_mount_options_common.* owns only neutral argv filtering, thread
#   options, and the shared rejected legacy-token vocabulary.
# - kafs_v7_entrypoint_adapter.* adapts v7 CLI state to the shared FUSE runtime
#   entrypoint. It is not a v5/v6/v7 compatibility layer.
# - kafs_shared_fuse_runner.h is the request-based internal handoff to the
#   shared FUSE runner / operation table in kafs_shared_fuse_runtime.c.
kafs_v6_SOURCES = kafs_v6.c
kafs_v6_CFLAGS = $(KAFS_CFLAGS)

kafs_v7_SOURCES = kafs_v7.c kafs_v7_runtime.c kafs_v7_runtime_view.c kafs_v7_layout.c \
	kafs_v7_recovery_diagnostic.c \
	kafs_v7_checkpoint.c kafs_v7_journal.c kafs_v7_locks.c kafs_v7_mutation.c \
	kafs_v7_journal_writer.c kafs_v7_sequence.c kafs_v7_data_cow.c \
	kafs_v7_runtime_transaction.c kafs_v7_fuse_write.c \
	kafs_mount_options_common.c kafs_v7_mount_options.c kafs_v7_entrypoint_adapter.c \
	kafs_shared_fuse_runtime.c \
	kafs_hrl.c kafs_lock_order.c kafs_locks.c kafs_journal.c kafs_rpc.c
kafs_v7_CFLAGS = $(KAFS_CFLAGS) -Wno-unused-function -Wno-unused-parameter -DKAFS_NO_MAIN \
	-DKAFS_SHARED_FUSE_RUNNER_EXPORT -DKAFS_V7_RUNTIME_ENTRYPOINT
kafs_v7_LDADD = $(KAFS_LIBS)

# new mkfs.kafs target
mkfs_kafs_SOURCES = mkfs_kafs.c kafs_v7_layout.c kafs_v7_journal.c kafs_v7_mutation.c \
	kafs_hrl.c kafs_lock_order.c kafs_locks.c
mkfs_kafs_CFLAGS = $(KAFS_CFLAGS) -Wno-unused-function -Wno-unused-parameter
mkfs_kafs_LDADD = $(KAFS_LIBS)

# fsck.kafs target (journal check/clear + orphan reclaim)
fsck_kafs_SOURCES = fsck_kafs.c kafs_v7_layout.c kafs_v7_journal.c kafs_v7_mutation.c \
	kafs_hrl.c kafs_lock_order.c kafs_locks.c kafs_journal.c
fsck_kafs_CFLAGS = $(KAFS_CFLAGS) -Wno-unused-function -Wno-unused-parameter
fsck_kafs_LDADD = $(KAFS_LIBS)

# simple info tool
kafs_info_SOURCES = kafs_info.c
kafs_info_CFLAGS = $(KAFS_CFLAGS) -Wno-unused-function -Wno-unused-parameter
kafs_info_LDADD = $(KAFS_LIBS)

# offline image dump tool
kafsdump_SOURCES = kafsdump.c kafs_v7_layout.c kafs_v7_journal.c kafs_v7_mutation.c \
	kafs_v7_recovery_diagnostic.c
kafsdump_CFLAGS = $(KAFS_CFLAGS) -Wno-unused-function -Wno-unused-parameter
kafsdump_LDADD = $(KAFS_LIBS)

# offline image export tool
kafsimage_SOURCES = kafsimage.c
kafsimage_CFLAGS = $(KAFS_CFLAGS) -Wno-unused-function -Wno-unused-parameter
kafsimage_LDADD = $(KAFS_LIBS)

# offline image grow tool (grow-only)
kafsresize_SOURCES = kafsresize.c kafs_v7_layout.c kafs_v7_journal.c kafs_v7_mutation.c
kafsresize_CFLAGS = $(KAFS_CFLAGS) -Wno-unused-function -Wno-unused-parameter
kafsresize_LDADD = $(KAFS_LIBS)

# runtime control/stats tool
kafsctl_SOURCES = kafsctl.c kafs_shared_fuse_runtime.c kafs_hrl.c kafs_lock_order.c kafs_locks.c kafs_journal.c kafs_rpc.c
kafsctl_CFLAGS = $(KAFS_CFLAGS) -Wno-unused-function -Wno-unused-parameter -DKAFS_NO_MAIN
kafsctl_LDADD = $(KAFS_LIBS)

kafs_front_SOURCES = kafs_front.c kafs_rpc.c
kafs_front_CFLAGS = $(KAFS_CFLAGS) -Wno-unused-function -Wno-unused-parameter
kafs_front_LDADD = $(KAFS_LIBS)

kafs_back_SOURCES = kafs_back.c kafs_rpc.c kafs_shared_fuse_runtime.c kafs_hrl.c kafs_lock_order.c kafs_locks.c kafs_journal.c
kafs_back_CFLAGS = $(KAFS_CFLAGS) -Wno-unused-function -Wno-unused-parameter -DKAFS_NO_MAIN
kafs_back_LDADD = $(KAFS_LIBS)

noinst_HEADERS = kafs_block.h kafs_config.h kafs_context.h kafs_dirent.h kafs_inode.h \
	kafs_descriptor_layout.h kafs_meta_region.h kafs_profile.h kafs_superblock.h kafs.h \
	kafs_ioctl.h kafs_journal.h kafs_lock_order.h kafs_rpc.h kafs_core.h kafs_shared_fuse_runner.h \
	kafs_v6_fuse_init_policy.h kafs_v6_fuse_policy.h \
	kafs_mount_options_common.h kafs_v6_layout.h \
	kafs_v7_admission.h kafs_v7_entrypoint_adapter.h \
	kafs_v7_checkpoint.h kafs_v7_data_cow.h kafs_v7_fuse_policy.h kafs_v7_fuse_write.h \
	kafs_v7_io.h kafs_v7_journal.h kafs_v7_test_fault.h \
	kafs_v7_layout.h kafs_v7_locks.h \
	kafs_v7_journal_writer.h kafs_v7_mount_options.h \
	kafs_v7_mutation.h kafs_v7_runtime.h kafs_v7_runtime_transaction.h kafs_v7_sequence.h \
	kafs_v7_runtime_view.h kafs_v7_recovery_diagnostic.h

CFLAGS = @CFLAGS@ -Wall -Werror -Wno-unused-function -Wno-unused-parameter
