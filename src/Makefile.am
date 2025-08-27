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

noinst_HEADERS = kafs_block.h kafs_context.h kafs_dirent.h kafs_inode.h kafs_superblock.h kafs.h test_utils.h

CFLAGS = -Wall -Werror -g -O0 -D_GNU_SOURCE -D_FILE_OFFSET_BITS=64

# --- Tests ---
check_PROGRAMS = hrl_smoketest hrl_dec_ref_by_blo hrl_unconfigured
TESTS = hrl_smoketest hrl_dec_ref_by_blo hrl_unconfigured
hrl_smoketest_SOURCES = tests_hrl_smoketest.c kafs_hrl.c test_utils.c
hrl_smoketest_CFLAGS = $(KAFS_CFLAGS) -Wno-unused-function -Wno-unused-parameter
hrl_smoketest_LDADD = $(KAFS_LIBS)

hrl_dec_ref_by_blo_SOURCES = tests_hrl_dec_ref_by_blo.c kafs_hrl.c test_utils.c
hrl_dec_ref_by_blo_CFLAGS = $(KAFS_CFLAGS) -Wno-unused-function -Wno-unused-parameter
hrl_dec_ref_by_blo_LDADD = $(KAFS_LIBS)

hrl_unconfigured_SOURCES = tests_hrl_unconfigured.c kafs_hrl.c test_utils.c
hrl_unconfigured_CFLAGS = $(KAFS_CFLAGS) -Wno-unused-function -Wno-unused-parameter
hrl_unconfigured_LDADD = $(KAFS_LIBS)