bin_PROGRAMS = kafs mkfs.kafs kafs-info

# add per-target flags; silence unused-function as headers provide many static helpers
kafs_CFLAGS = $(KAFS_CFLAGS) -Wno-unused-function -Wno-unused-parameter
kafs_LDADD = $(KAFS_LIBS)
kafs_SOURCES = kafs.c kafs_hrl.c

# new mkfs.kafs target
mkfs_kafs_SOURCES = mkfs_kafs.c kafs_hrl.c
mkfs_kafs_CFLAGS = $(KAFS_CFLAGS) -Wno-unused-function -Wno-unused-parameter
mkfs_kafs_LDADD = $(KAFS_LIBS)

# simple info tool
kafs_info_SOURCES = kafs_info.c
kafs_info_CFLAGS = $(KAFS_CFLAGS) -Wno-unused-function -Wno-unused-parameter
kafs_info_LDADD = $(KAFS_LIBS)

noinst_HEADERS = kafs_block.h kafs_context.h kafs_dirent.h kafs_inode.h kafs_superblock.h kafs.h

CFLAGS = -Wall -Werror -g -O0 -D_GNU_SOURCE -D_FILE_OFFSET_BITS=64