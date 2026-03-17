AM_CPPFLAGS = -include $(srcdir)/kafs_config.h

bin_PROGRAMS = kafs mkfs.kafs fsck.kafs kafs-info kafsdump kafsimage kafsresize kafsctl kafs-front kafs-back

# add per-target flags; silence unused-function as headers provide many static helpers
kafs_CFLAGS = $(KAFS_CFLAGS) -Wno-unused-function -Wno-unused-parameter
kafs_LDADD = $(KAFS_LIBS)
kafs_SOURCES = kafs.c kafs_hrl.c kafs_locks.c kafs_journal.c kafs_rpc.c

# new mkfs.kafs target
mkfs_kafs_SOURCES = mkfs_kafs.c kafs_hrl.c kafs_locks.c
mkfs_kafs_CFLAGS = $(KAFS_CFLAGS) -Wno-unused-function -Wno-unused-parameter
mkfs_kafs_LDADD = $(KAFS_LIBS)

# fsck.kafs target (journal check/clear + orphan reclaim)
fsck_kafs_SOURCES = fsck_kafs.c kafs_hrl.c kafs_locks.c kafs_journal.c
fsck_kafs_CFLAGS = $(KAFS_CFLAGS) -Wno-unused-function -Wno-unused-parameter
fsck_kafs_LDADD = $(KAFS_LIBS)

# simple info tool
kafs_info_SOURCES = kafs_info.c
kafs_info_CFLAGS = $(KAFS_CFLAGS) -Wno-unused-function -Wno-unused-parameter
kafs_info_LDADD = $(KAFS_LIBS)

# offline image dump tool
kafsdump_SOURCES = kafsdump.c
kafsdump_CFLAGS = $(KAFS_CFLAGS) -Wno-unused-function -Wno-unused-parameter
kafsdump_LDADD = $(KAFS_LIBS)

# offline image export tool
kafsimage_SOURCES = kafsimage.c
kafsimage_CFLAGS = $(KAFS_CFLAGS) -Wno-unused-function -Wno-unused-parameter
kafsimage_LDADD = $(KAFS_LIBS)

# offline image grow tool (grow-only)
kafsresize_SOURCES = kafsresize.c
kafsresize_CFLAGS = $(KAFS_CFLAGS) -Wno-unused-function -Wno-unused-parameter
kafsresize_LDADD = $(KAFS_LIBS)

# runtime control/stats tool
kafsctl_SOURCES = kafsctl.c kafs.c kafs_hrl.c kafs_locks.c kafs_journal.c kafs_rpc.c
kafsctl_CFLAGS = $(KAFS_CFLAGS) -Wno-unused-function -Wno-unused-parameter -DKAFS_NO_MAIN
kafsctl_LDADD = $(KAFS_LIBS)

kafs_front_SOURCES = kafs_front.c kafs_rpc.c
kafs_front_CFLAGS = $(KAFS_CFLAGS) -Wno-unused-function -Wno-unused-parameter
kafs_front_LDADD = $(KAFS_LIBS)

kafs_back_SOURCES = kafs_back.c kafs_rpc.c kafs.c kafs_hrl.c kafs_locks.c kafs_journal.c
kafs_back_CFLAGS = $(KAFS_CFLAGS) -Wno-unused-function -Wno-unused-parameter -DKAFS_NO_MAIN
kafs_back_LDADD = $(KAFS_LIBS)

noinst_HEADERS = kafs_block.h kafs_config.h kafs_context.h kafs_dirent.h kafs_inode.h kafs_superblock.h kafs.h kafs_ioctl.h kafs_journal.h kafs_rpc.h kafs_core.h

CFLAGS = -Wall -Werror -g -O2 -Wno-unused-function -Wno-unused-parameter
