#include "kafs_superblock.h"
#include "kafs_inode.h"
#include "kafs_hash.h"
#include "kafs_locks.h"
#include "kafs_block.h"
/* jscpd:ignore-start */
#include <errno.h>
#include <fcntl.h>
#include <inttypes.h> // PRIu64, PRIu32
#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/syscall.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#ifdef __linux__
#include <linux/falloc.h>
#endif
/* jscpd:ignore-end */

#include "kafs_journal.h"

// In-image journal format definitions come from kafs_journal.h

#define FSCK_EXIT_USAGE 2
#define FSCK_EXIT_JOURNAL_CHECK_FAILED 3
#define FSCK_EXIT_JOURNAL_RESET_FAILED 4
#define FSCK_EXIT_DIRENT_INO_INCONSISTENT 5
#define FSCK_EXIT_DIRENT_INO_REPAIR_PARTIAL 6
#define FSCK_EXIT_HRL_BLO_INCONSISTENT 7
#define FSCK_EXIT_JOURNAL_REPLAY_FAILED 8
#define FSCK_EXIT_PUNCH_HOLE_PARTIAL 9

#define KAFS_PENDING_REF_FLAG 0x80000000u

struct orphan_stats
{
  uint64_t found;
  uint64_t dec_attempted;
  uint64_t dec_failed;
};

struct hrl_refcheck_stats
{
  uint64_t inode_refs;
  uint64_t pending_refs;
  uint64_t invalid_refs;
  uint64_t live_entries;
  uint64_t mismatch_entries;
  uint64_t hrl_invalid_entries;
};

struct punch_stats
{
  uint64_t candidates;
  uint64_t already_free;
  uint64_t punched;
  uint64_t punch_failed;
  uint64_t mark_failed;
};

typedef enum fsck_mode
{
  FSCK_MODE_UNSET = 0,
  FSCK_MODE_FAST_CHECK,
  FSCK_MODE_FAST_REPAIR,
  FSCK_MODE_BALANCED_CHECK,
  FSCK_MODE_BALANCED_REPAIR,
  FSCK_MODE_FULL_CHECK,
  FSCK_MODE_FULL_REPAIR,
} fsck_mode_t;

static int fsck_decode_data_ref(kafs_blkcnt_t raw, kafs_blkcnt_t *out_blo, int *out_is_pending)
{
  if (raw == KAFS_BLO_NONE)
  {
    *out_blo = KAFS_BLO_NONE;
    *out_is_pending = 0;
    return 0;
  }

  if ((((uint32_t)raw) & KAFS_PENDING_REF_FLAG) != 0u)
  {
    *out_blo = KAFS_BLO_NONE;
    *out_is_pending = 1;
    return 0;
  }

  *out_blo = raw;
  *out_is_pending = 0;
  return 0;
}

static int fsck_punch_hole(int fd, off_t off, off_t len)
{
#ifdef __linux__
#ifdef SYS_fallocate
  if (syscall(SYS_fallocate, fd, FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE, off, len) == 0)
    return 0;
  return -errno;
#else
  (void)fd;
  (void)off;
  (void)len;
  return -ENOSYS;
#endif
#else
  (void)fd;
  (void)off;
  (void)len;
  return -ENOTSUP;
#endif
}

static int fsck_hrl_dec_ref_counted(kafs_context_t *ctx, kafs_blkcnt_t raw,
                                    struct orphan_stats *stats, int raw_is_data_ref)
{
  kafs_blkcnt_t blo = raw;
  if (raw_is_data_ref)
  {
    int is_pending = 0;
    (void)fsck_decode_data_ref(raw, &blo, &is_pending);
    if (is_pending || blo == KAFS_BLO_NONE)
    {
      if (stats)
        stats->dec_failed++;
      return -EINVAL;
    }
  }

  if (blo == KAFS_BLO_NONE)
    return 0;

  if (stats)
    stats->dec_attempted++;
  int rc = kafs_hrl_dec_ref_by_blo(ctx, blo);
  if (rc != 0 && stats)
    stats->dec_failed++;
  return rc;
}

static int pread_all(int fd, void *buf, size_t sz, off_t off)
{
  ssize_t r = pread(fd, buf, sz, off);
  return (r == (ssize_t)sz) ? 0 : -1;
}
static int pwrite_all(int fd, const void *buf, size_t sz, off_t off)
{
  ssize_t w = pwrite(fd, buf, sz, off);
  return (w == (ssize_t)sz) ? 0 : -1;
}

static void *img_ptr(void *base, size_t img_size, off_t off, size_t len)
{
  if (off < 0 || (size_t)off + len > img_size)
    return NULL;
  return (void *)((uint8_t *)base + off);
}

struct rel_ctx
{
  kafs_context_t *ctx;
  void *base;
  size_t img_size;
  kafs_logblksize_t l2;
  uint32_t refs_pb;
  kafs_blksize_t blksize;
  struct orphan_stats *stats;
};

static int rel_tbl_impl(struct rel_ctx *rctx, kafs_blkcnt_t blo, int depth)
{
  if (blo == KAFS_BLO_NONE)
    return 0;

  void *p = img_ptr(rctx->base, rctx->img_size, (off_t)blo << rctx->l2, rctx->blksize);
  if (!p)
    return -EIO;

  kafs_sblkcnt_t *tbl = (kafs_sblkcnt_t *)p;
  for (uint32_t i = 0; i < rctx->refs_pb; ++i)
  {
    kafs_blkcnt_t child = kafs_blkcnt_stoh(tbl[i]);
    if (child == KAFS_BLO_NONE)
      continue;

    if (depth > 1)
    {
      (void)rel_tbl_impl(rctx, child, depth - 1);
      (void)fsck_hrl_dec_ref_counted(rctx->ctx, child, rctx->stats, 0);
    }
    else
    {
      (void)fsck_hrl_dec_ref_counted(rctx->ctx, child, rctx->stats, 1);
    }
  }

  return 0;
}

static int orphan_reclaim(kafs_context_t *ctx, int do_fix, struct orphan_stats *stats)
{
  kafs_ssuperblock_t *sb = ctx->c_superblock;
  kafs_inocnt_t inocnt = kafs_sb_inocnt_get(sb);
  kafs_logblksize_t log_blksize = kafs_sb_log_blksize_get(sb);
  kafs_blksize_t blksize = kafs_sb_blksize_get(sb);
  uint32_t refs_pb = (uint32_t)(blksize / sizeof(kafs_sblkcnt_t));

  int found = 0;
  for (kafs_inocnt_t ino = KAFS_INO_ROOTDIR; ino < inocnt; ++ino)
  {
    kafs_sinode_t *e = &ctx->c_inotbl[ino];
    if (!kafs_ino_get_usage(e))
      continue;
    if (kafs_ino_linkcnt_get(e) != 0)
      continue;

    found++;
    if (stats)
      stats->found++;
    if (!do_fix)
      continue;

    // Free all referenced blocks (direct + indirect tables) best-effort.
    // NOTE: For "direct" small files, data is in inode and there are no blocks to free.
    if (kafs_ino_size_get(e) > (kafs_off_t)sizeof(e->i_blkreftbl))
    {
      // Direct data blocks
      for (uint32_t i = 0; i < 12; ++i)
      {
        kafs_blkcnt_t b = kafs_blkcnt_stoh(e->i_blkreftbl[i]);
        if (b != KAFS_BLO_NONE)
          (void)fsck_hrl_dec_ref_counted(ctx, b, stats, 1);
      }

      // Recursive indirect release
      struct rel_ctx rctx = {ctx, ctx->c_img_base, ctx->c_img_size,
                 log_blksize, refs_pb, blksize, stats};

      kafs_blkcnt_t si = kafs_blkcnt_stoh(e->i_blkreftbl[12]);
      kafs_blkcnt_t di = kafs_blkcnt_stoh(e->i_blkreftbl[13]);
      kafs_blkcnt_t ti = kafs_blkcnt_stoh(e->i_blkreftbl[14]);
      if (si != KAFS_BLO_NONE)
      {
        (void)rel_tbl_impl(&rctx, si, 1);
        (void)fsck_hrl_dec_ref_counted(ctx, si, stats, 0);
      }
      if (di != KAFS_BLO_NONE)
      {
        (void)rel_tbl_impl(&rctx, di, 2);
        (void)fsck_hrl_dec_ref_counted(ctx, di, stats, 0);
      }
      if (ti != KAFS_BLO_NONE)
      {
        (void)rel_tbl_impl(&rctx, ti, 3);
        (void)fsck_hrl_dec_ref_counted(ctx, ti, stats, 0);
      }
    }

    memset(e, 0, sizeof(*e));
    (void)kafs_sb_inocnt_free_incr(sb);
    kafs_sb_wtime_set(sb, kafs_now());
  }

  if (found > 0)
    fprintf(stderr, "Orphan inodes: %d\n", found);
  return found;
}

struct hrl_scan_ctx
{
  kafs_context_t *ctx;
  kafs_blkcnt_t r_blkcnt;
  kafs_logblksize_t l2;
  kafs_blksize_t blksize;
  uint32_t refs_pb;
  uint32_t *expected;
  struct hrl_refcheck_stats *stats;
};

static int hrl_count_raw_ref(struct hrl_scan_ctx *sctx, kafs_blkcnt_t raw)
{
  kafs_blkcnt_t blo = KAFS_BLO_NONE;
  int is_pending = 0;
  (void)fsck_decode_data_ref(raw, &blo, &is_pending);
  if (is_pending)
  {
    sctx->stats->pending_refs++;
    return 0;
  }
  if (blo == KAFS_BLO_NONE)
    return 0;
  if (blo >= sctx->r_blkcnt)
  {
    sctx->stats->invalid_refs++;
    return 0;
  }
  sctx->expected[blo]++;
  sctx->stats->inode_refs++;
  return 0;
}

static int hrl_count_tbl_refs(struct hrl_scan_ctx *sctx, kafs_blkcnt_t tbl_blo, int depth)
{
  if (tbl_blo == KAFS_BLO_NONE)
    return 0;
  if (tbl_blo >= sctx->r_blkcnt)
  {
    sctx->stats->invalid_refs++;
    return 0;
  }

  void *p = img_ptr(sctx->ctx->c_img_base, sctx->ctx->c_img_size, (off_t)tbl_blo << sctx->l2,
                    sctx->blksize);
  if (!p)
    return -EIO;

  kafs_sblkcnt_t *tbl = (kafs_sblkcnt_t *)p;
  for (uint32_t i = 0; i < sctx->refs_pb; ++i)
  {
    kafs_blkcnt_t child = kafs_blkcnt_stoh(tbl[i]);
    if (child == KAFS_BLO_NONE)
      continue;
    if (depth == 1)
      (void)hrl_count_raw_ref(sctx, child);
    else
      (void)hrl_count_tbl_refs(sctx, child, depth - 1);
  }
  return 0;
}

static int check_hrl_blo_refcounts(kafs_context_t *ctx, struct hrl_refcheck_stats *stats)
{
  kafs_ssuperblock_t *sb = ctx->c_superblock;
  kafs_blkcnt_t r_blkcnt = kafs_sb_blkcnt_get(sb);
  kafs_inocnt_t inocnt = kafs_sb_inocnt_get(sb);
  kafs_blksize_t blksize = kafs_sb_blksize_get(sb);
  kafs_logblksize_t l2 = kafs_sb_log_blksize_get(sb);
  uint32_t refs_pb = (uint32_t)(blksize / sizeof(kafs_sblkcnt_t));

  if ((size_t)r_blkcnt > SIZE_MAX / sizeof(uint32_t))
    return -ENOMEM;
  uint32_t *expected = (uint32_t *)calloc((size_t)r_blkcnt, sizeof(uint32_t));
  uint32_t *actual = (uint32_t *)calloc((size_t)r_blkcnt, sizeof(uint32_t));
  if (!expected || !actual)
  {
    free(expected);
    free(actual);
    return -ENOMEM;
  }

  struct hrl_scan_ctx sctx = {ctx, r_blkcnt, l2, blksize, refs_pb, expected, stats};

  for (kafs_inocnt_t ino = KAFS_INO_ROOTDIR; ino < inocnt; ++ino)
  {
    kafs_sinode_t *e = &ctx->c_inotbl[ino];
    if (!kafs_ino_get_usage(e))
      continue;
    if (kafs_ino_size_get(e) <= (kafs_off_t)sizeof(e->i_blkreftbl))
      continue; // inline data only

    for (uint32_t i = 0; i < 12; ++i)
      (void)hrl_count_raw_ref(&sctx, kafs_blkcnt_stoh(e->i_blkreftbl[i]));

    (void)hrl_count_tbl_refs(&sctx, kafs_blkcnt_stoh(e->i_blkreftbl[12]), 1);
    (void)hrl_count_tbl_refs(&sctx, kafs_blkcnt_stoh(e->i_blkreftbl[13]), 2);
    (void)hrl_count_tbl_refs(&sctx, kafs_blkcnt_stoh(e->i_blkreftbl[14]), 3);
  }

  uint64_t ent_off = kafs_sb_hrl_entry_offset_get(sb);
  uint64_t ent_cnt = kafs_sb_hrl_entry_cnt_get(sb);
  uint64_t ent_size = ent_cnt * (uint64_t)sizeof(kafs_hrl_entry_t);
  kafs_hrl_entry_t *ents = (kafs_hrl_entry_t *)img_ptr(ctx->c_img_base, ctx->c_img_size,
                                                        (off_t)ent_off, (size_t)ent_size);
  if (!ents)
  {
    free(expected);
    free(actual);
    return -EIO;
  }

  uint64_t shown_invalid = 0;
  for (uint64_t i = 0; i < ent_cnt; ++i)
  {
    const kafs_hrl_entry_t *ent = &ents[i];
    if (ent->refcnt == 0)
      continue;
    stats->live_entries++;

    if (ent->blo >= r_blkcnt)
    {
      stats->hrl_invalid_entries++;
      if (shown_invalid < 20)
      {
        fprintf(stderr, "HRL mismatch: entry=%" PRIu64 " invalid blo=%u refcnt=%u\n", i,
                ent->blo, ent->refcnt);
        shown_invalid++;
      }
      continue;
    }

    actual[ent->blo] += ent->refcnt;
  }

  uint64_t shown = 0;
  for (kafs_blkcnt_t blo = 0; blo < r_blkcnt; ++blo)
  {
    uint32_t exp = expected[blo];
    uint32_t act = actual[blo];
    if (exp != act)
    {
      stats->mismatch_entries++;
      if (shown < 20)
      {
        fprintf(stderr, "HRL mismatch: blo=%u expected=%u actual=%u\n", (unsigned)blo, exp,
                act);
        shown++;
      }
    }
  }

  free(expected);
  free(actual);
  return 0;
}

static int punch_unreferenced_data_blocks(kafs_context_t *ctx, int fd, struct punch_stats *stats)
{
  kafs_ssuperblock_t *sb = ctx->c_superblock;
  kafs_blkcnt_t r_blkcnt = kafs_sb_blkcnt_get(sb);
  kafs_blkcnt_t first_data_block = kafs_sb_first_data_block_get(sb);
  kafs_inocnt_t inocnt = kafs_sb_inocnt_get(sb);
  kafs_blksize_t blksize = kafs_sb_blksize_get(sb);
  kafs_logblksize_t l2 = kafs_sb_log_blksize_get(sb);
  uint32_t refs_pb = (uint32_t)(blksize / sizeof(kafs_sblkcnt_t));

  if ((size_t)r_blkcnt > SIZE_MAX / sizeof(uint32_t))
    return -ENOMEM;

  uint32_t *expected = (uint32_t *)calloc((size_t)r_blkcnt, sizeof(uint32_t));
  uint32_t *actual = (uint32_t *)calloc((size_t)r_blkcnt, sizeof(uint32_t));
  if (!expected || !actual)
  {
    free(expected);
    free(actual);
    return -ENOMEM;
  }

  struct hrl_refcheck_stats hst;
  memset(&hst, 0, sizeof(hst));
  struct hrl_scan_ctx sctx = {ctx, r_blkcnt, l2, blksize, refs_pb, expected, &hst};

  for (kafs_inocnt_t ino = KAFS_INO_ROOTDIR; ino < inocnt; ++ino)
  {
    kafs_sinode_t *e = &ctx->c_inotbl[ino];
    if (!kafs_ino_get_usage(e))
      continue;
    if (kafs_ino_size_get(e) <= (kafs_off_t)sizeof(e->i_blkreftbl))
      continue;

    for (uint32_t i = 0; i < 12; ++i)
      (void)hrl_count_raw_ref(&sctx, kafs_blkcnt_stoh(e->i_blkreftbl[i]));

    (void)hrl_count_tbl_refs(&sctx, kafs_blkcnt_stoh(e->i_blkreftbl[12]), 1);
    (void)hrl_count_tbl_refs(&sctx, kafs_blkcnt_stoh(e->i_blkreftbl[13]), 2);
    (void)hrl_count_tbl_refs(&sctx, kafs_blkcnt_stoh(e->i_blkreftbl[14]), 3);
  }

  uint64_t ent_off = kafs_sb_hrl_entry_offset_get(sb);
  uint64_t ent_cnt = kafs_sb_hrl_entry_cnt_get(sb);
  uint64_t ent_size = ent_cnt * (uint64_t)sizeof(kafs_hrl_entry_t);
  kafs_hrl_entry_t *ents = (kafs_hrl_entry_t *)img_ptr(ctx->c_img_base, ctx->c_img_size,
                                                        (off_t)ent_off, (size_t)ent_size);
  if (!ents)
  {
    free(expected);
    free(actual);
    return -EIO;
  }

  for (uint64_t i = 0; i < ent_cnt; ++i)
  {
    const kafs_hrl_entry_t *ent = &ents[i];
    if (ent->refcnt == 0)
      continue;
    if (ent->blo >= r_blkcnt)
      continue;
    actual[ent->blo] += ent->refcnt;
  }

  for (kafs_blkcnt_t blo = first_data_block; blo < r_blkcnt; ++blo)
  {
    if (expected[blo] != 0 || actual[blo] != 0)
      continue;

    stats->candidates++;
    if (!kafs_blk_get_usage(ctx, blo))
    {
      stats->already_free++;
      continue;
    }

    int prc = fsck_punch_hole(fd, ((off_t)blo << l2), (off_t)blksize);
    if (prc != 0)
    {
      stats->punch_failed++;
      continue;
    }

    if (kafs_blk_set_usage(ctx, blo, KAFS_FALSE) != 0)
    {
      stats->mark_failed++;
      continue;
    }
    stats->punched++;
  }

  free(expected);
  free(actual);
  return 0;
}

static void usage(const char *prog)
{
  fprintf(stderr,
          "Usage: %s [--full-check|--full-repair|--balanced-check|--balanced-repair|"
          "--fast-check|--fast-repair] <image>\n"
          "       %s [--check-journal] [--repair-journal-reset] "
          "[--check-dirent-ino-orphans] [--repair-dirent-ino-orphans] "
          "[--check-hrl-blo-refcounts] [--replay-journal] "
          "[--punch-hole-unreferenced-data-blocks] <image>\n",
          prog,
          prog);
}

static int check_region_bounds(const char *name, uint64_t off, uint64_t size, uint64_t file_size)
{
  if (size == 0)
    return 0;
  if (off >= file_size || size > file_size - off)
  {
    fprintf(stderr, "%s out of range: off=%" PRIu64 " size=%" PRIu64 " file=%" PRIu64 "\n",
            name, off, size, file_size);
    return -1;
  }
  return 0;
}

int main(int argc, char **argv)
{
  int do_journal_reset = 0;         // repair: journal layer
  int do_check_dirent_ino_orphans = 0;
  int do_repair_dirent_ino_orphans = 0;
  int do_check_hrl_blo_refcounts = 0;
  int do_replay_journal = 0;
  int do_punch_hole_unreferenced_data_blocks = 0;
  int do_check_journal = 1;

  int has_preset_mode = 0;
  int has_low_level_mode = 0;
  fsck_mode_t mode = FSCK_MODE_UNSET;

  int exit_code = 0;
  const char *img = NULL;
  for (int i = 1; i < argc; ++i)
  {
    if (strcmp(argv[i], "--full-check") == 0)
    {
      has_preset_mode = 1;
      mode = FSCK_MODE_FULL_CHECK;
    }
    else if (strcmp(argv[i], "--full-repair") == 0)
    {
      has_preset_mode = 1;
      mode = FSCK_MODE_FULL_REPAIR;
    }
    else if (strcmp(argv[i], "--balanced-check") == 0)
    {
      has_preset_mode = 1;
      mode = FSCK_MODE_BALANCED_CHECK;
    }
    else if (strcmp(argv[i], "--balanced-repair") == 0)
    {
      has_preset_mode = 1;
      mode = FSCK_MODE_BALANCED_REPAIR;
    }
    else if (strcmp(argv[i], "--fast-check") == 0)
    {
      has_preset_mode = 1;
      mode = FSCK_MODE_FAST_CHECK;
    }
    else if (strcmp(argv[i], "--fast-repair") == 0)
    {
      has_preset_mode = 1;
      mode = FSCK_MODE_FAST_REPAIR;
    }
    else if (strcmp(argv[i], "--check-journal") == 0)
    {
      // no-op: default behavior is journal validation
      has_low_level_mode = 1;
    }
    else if (strcmp(argv[i], "--repair-journal-reset") == 0)
    {
      do_journal_reset = 1;
      has_low_level_mode = 1;
    }
    else if (strcmp(argv[i], "--repair-dirent-ino-orphans") == 0)
    {
      do_repair_dirent_ino_orphans = 1;
      has_low_level_mode = 1;
    }
    else if (strcmp(argv[i], "--check-dirent-ino-orphans") == 0)
    {
      do_check_dirent_ino_orphans = 1;
      has_low_level_mode = 1;
    }
    else if (strcmp(argv[i], "--check-hrl-blo-refcounts") == 0)
    {
      do_check_hrl_blo_refcounts = 1;
      has_low_level_mode = 1;
    }
    else if (strcmp(argv[i], "--replay-journal") == 0)
    {
      do_replay_journal = 1;
      has_low_level_mode = 1;
    }
    else if (strcmp(argv[i], "--punch-hole-unreferenced-data-blocks") == 0)
    {
      do_punch_hole_unreferenced_data_blocks = 1;
      has_low_level_mode = 1;
    }
    else if (argv[i][0] != '-' && !img)
    {
      img = argv[i];
    }
    else
    {
      usage(argv[0]);
      return FSCK_EXIT_USAGE;
    }
  }
  if (!img)
  {
    usage(argv[0]);
    return FSCK_EXIT_USAGE;
  }

  if (has_preset_mode && has_low_level_mode)
  {
    fprintf(stderr, "fsck.kafs: preset mode and low-level flags cannot be mixed\n");
    usage(argv[0]);
    return FSCK_EXIT_USAGE;
  }

  if (has_preset_mode)
  {
    do_check_journal = 1;
    do_journal_reset = 0;
    do_check_dirent_ino_orphans = 0;
    do_repair_dirent_ino_orphans = 0;
    do_check_hrl_blo_refcounts = 0;

    switch (mode)
    {
    case FSCK_MODE_FAST_CHECK:
      break;
    case FSCK_MODE_FAST_REPAIR:
      do_journal_reset = 1;
      break;
    case FSCK_MODE_BALANCED_CHECK:
      do_check_dirent_ino_orphans = 1;
      break;
    case FSCK_MODE_BALANCED_REPAIR:
      do_journal_reset = 1;
      do_repair_dirent_ino_orphans = 1;
      break;
    case FSCK_MODE_FULL_CHECK:
      do_check_dirent_ino_orphans = 1;
      do_check_hrl_blo_refcounts = 1;
      break;
    case FSCK_MODE_FULL_REPAIR:
      do_journal_reset = 1;
      do_repair_dirent_ino_orphans = 1;
      do_check_hrl_blo_refcounts = 1;
      break;
    case FSCK_MODE_UNSET:
    default:
      break;
    }
  }
  else if (!has_low_level_mode)
  {
    // default: balanced check
    do_check_journal = 1;
    do_check_dirent_ino_orphans = 1;
  }

  int want_write = do_journal_reset || do_repair_dirent_ino_orphans;
  if (do_replay_journal || do_punch_hole_unreferenced_data_blocks)
    want_write = 1;
  int fd = open(img, want_write ? O_RDWR : O_RDONLY);
  if (fd < 0)
  {
    perror("open");
    return 1;
  }

  // read superblock
  kafs_ssuperblock_t sb;
  if (pread_all(fd, &sb, sizeof(sb), 0) != 0)
  {
    perror("pread superblock");
    close(fd);
    return 1;
  }

  struct stat st;
  if (fstat(fd, &st) != 0)
  {
    perror("fstat");
    close(fd);
    return 1;
  }
  uint64_t file_size = (uint64_t)st.st_size;

  if (check_region_bounds("allocator", kafs_sb_allocator_offset_get(&sb),
                          kafs_sb_allocator_size_get(&sb), file_size) != 0)
  {
    close(fd);
    return 3;
  }
  if (check_region_bounds("pendinglog", kafs_sb_pendinglog_offset_get(&sb),
                          kafs_sb_pendinglog_size_get(&sb), file_size) != 0)
  {
    close(fd);
    return FSCK_EXIT_JOURNAL_CHECK_FAILED;
  }

  if (do_check_dirent_ino_orphans || do_repair_dirent_ino_orphans || do_check_hrl_blo_refcounts ||
      do_replay_journal || do_punch_hole_unreferenced_data_blocks)
  {
    kafs_context_t ctx;
    memset(&ctx, 0, sizeof(ctx));
    ctx.c_fd = fd;

    // layout and full-image mmap (matches kafs.c)
    kafs_logblksize_t log_blksize = kafs_sb_log_blksize_get(&sb);
    kafs_blksize_t blksize = 1u << log_blksize;
    kafs_blksize_t blksizemask = blksize - 1u;
    kafs_inocnt_t inocnt = kafs_inocnt_stoh(sb.s_inocnt);
    kafs_blkcnt_t r_blkcnt = kafs_blkcnt_stoh(sb.s_r_blkcnt);

    off_t mapsize = 0;
    mapsize += sizeof(kafs_ssuperblock_t);
    mapsize = (mapsize + blksizemask) & ~blksizemask;
    void *blkmask_off = (void *)mapsize;
    mapsize += (r_blkcnt + 7) >> 3;
    mapsize = (mapsize + 7) & ~7;
    mapsize = (mapsize + blksizemask) & ~blksizemask;
    void *inotbl_off = (void *)mapsize;
    mapsize += (off_t)sizeof(kafs_sinode_t) * inocnt;
    mapsize = (mapsize + blksizemask) & ~blksizemask;

    off_t imgsize = (off_t)r_blkcnt << log_blksize;
    {
      uint64_t idx_off = kafs_sb_hrl_index_offset_get(&sb);
      uint64_t idx_size = kafs_sb_hrl_index_size_get(&sb);
      uint64_t ent_off = kafs_sb_hrl_entry_offset_get(&sb);
      uint64_t ent_cnt = kafs_sb_hrl_entry_cnt_get(&sb);
      uint64_t ent_size = ent_cnt * (uint64_t)sizeof(kafs_hrl_entry_t);
      uint64_t j_off = kafs_sb_journal_offset_get(&sb);
      uint64_t j_size = kafs_sb_journal_size_get(&sb);
      uint64_t end1 = (idx_off && idx_size) ? (idx_off + idx_size) : 0;
      uint64_t end2 = (ent_off && ent_size) ? (ent_off + ent_size) : 0;
      uint64_t end3 = (j_off && j_size) ? (j_off + j_size) : 0;
      uint64_t max_end = end1;
      if (end2 > max_end)
        max_end = end2;
      if (end3 > max_end)
        max_end = end3;
      if ((off_t)max_end > imgsize)
        imgsize = (off_t)max_end;
      imgsize = (imgsize + blksizemask) & ~blksizemask;
    }

    int prot = want_write ? (PROT_READ | PROT_WRITE) : PROT_READ;
    ctx.c_img_base = mmap(NULL, (size_t)imgsize, prot, MAP_SHARED, fd, 0);
    if (ctx.c_img_base == MAP_FAILED)
    {
      perror("mmap");
      close(fd);
      return 1;
    }
    ctx.c_img_size = (size_t)imgsize;
    ctx.c_superblock = (kafs_ssuperblock_t *)ctx.c_img_base;
    ctx.c_mapsize = (size_t)mapsize;
    ctx.c_blkmasktbl = (void *)ctx.c_superblock + (intptr_t)blkmask_off;
    ctx.c_inotbl = (void *)ctx.c_superblock + (intptr_t)inotbl_off;
    ctx.c_blo_search = 0;
    ctx.c_ino_search = 0;

    if (do_replay_journal)
    {
      int rrc = kafs_journal_replay(&ctx, NULL, NULL);
      if (rrc != 0)
      {
        munmap(ctx.c_img_base, ctx.c_img_size);
        close(fd);
        return FSCK_EXIT_JOURNAL_REPLAY_FAILED;
      }
      fprintf(stderr, "Journal replay: applied pending metadata and cleared ring.\n");
    }

    if (do_check_dirent_ino_orphans || do_repair_dirent_ino_orphans)
    {
      struct orphan_stats ost;
      memset(&ost, 0, sizeof(ost));

      if (do_repair_dirent_ino_orphans)
        (void)kafs_hrl_open(&ctx);

      int found = orphan_reclaim(&ctx, do_repair_dirent_ino_orphans, &ost);
      if (found < 0)
      {
        if (do_repair_dirent_ino_orphans)
          (void)kafs_hrl_close(&ctx);
        munmap(ctx.c_img_base, ctx.c_img_size);
        close(fd);
        return 1;
      }

      if (do_repair_dirent_ino_orphans)
      {
        (void)kafs_hrl_close(&ctx);
        fprintf(stderr,
                "Dirent->ino orphan repair summary: found=%" PRIu64 " dec_attempted=%" PRIu64
                " dec_failed=%" PRIu64 "\n",
                ost.found, ost.dec_attempted, ost.dec_failed);
        if (ost.dec_failed > 0)
          exit_code = FSCK_EXIT_DIRENT_INO_REPAIR_PARTIAL;
      }
      else if (ost.found > 0)
      {
        exit_code = FSCK_EXIT_DIRENT_INO_INCONSISTENT;
      }
    }

    if (do_check_hrl_blo_refcounts)
    {
      struct hrl_refcheck_stats hst;
      memset(&hst, 0, sizeof(hst));
      int hrc = check_hrl_blo_refcounts(&ctx, &hst);
      if (hrc != 0)
      {
        munmap(ctx.c_img_base, ctx.c_img_size);
        close(fd);
        return 1;
      }

      fprintf(stderr,
              "HRL->BLO check summary: inode_refs=%" PRIu64 " pending_refs=%" PRIu64
              " invalid_refs=%" PRIu64 " live_entries=%" PRIu64 " invalid_entries=%" PRIu64
              " mismatches=%" PRIu64 "\n",
              hst.inode_refs, hst.pending_refs, hst.invalid_refs, hst.live_entries,
              hst.hrl_invalid_entries, hst.mismatch_entries);

      if (hst.mismatch_entries > 0 || hst.pending_refs > 0 || hst.invalid_refs > 0 ||
          hst.hrl_invalid_entries > 0)
      {
        if (exit_code == 0)
          exit_code = FSCK_EXIT_HRL_BLO_INCONSISTENT;
      }
    }

    if (do_punch_hole_unreferenced_data_blocks)
    {
      struct punch_stats pst;
      memset(&pst, 0, sizeof(pst));
      int prc = punch_unreferenced_data_blocks(&ctx, fd, &pst);
      if (prc != 0)
      {
        munmap(ctx.c_img_base, ctx.c_img_size);
        close(fd);
        return 1;
      }

      fprintf(stderr,
              "Punch-hole summary: candidates=%" PRIu64 " already_free=%" PRIu64
              " punched=%" PRIu64 " punch_failed=%" PRIu64 " mark_failed=%" PRIu64 "\n",
              pst.candidates, pst.already_free, pst.punched, pst.punch_failed, pst.mark_failed);

      if ((pst.punch_failed > 0 || pst.mark_failed > 0) && exit_code == 0)
        exit_code = FSCK_EXIT_PUNCH_HOLE_PARTIAL;
    }

    if (want_write)
    {
      (void)msync(ctx.c_img_base, ctx.c_img_size, MS_SYNC);
      (void)fsync(fd);
    }
    munmap(ctx.c_img_base, ctx.c_img_size);
  }

  if (!do_check_journal)
  {
    close(fd);
    return exit_code;
  }

  uint64_t joff = kafs_sb_journal_offset_get(&sb);
  uint64_t jsize = kafs_sb_journal_size_get(&sb);
  if (check_region_bounds("journal", joff, jsize, file_size) != 0)
  {
    close(fd);
    return FSCK_EXIT_JOURNAL_CHECK_FAILED;
  }
  if (joff == 0 || jsize < 4096)
  {
    fprintf(stderr, "No in-image journal: off=%" PRIu64 " size=%" PRIu64 "\n", joff, jsize);
    close(fd);
    return exit_code;
  }

  size_t hsz = kj_header_size();
  uint64_t data_off = joff + hsz;
  uint64_t area_size = (jsize > hsz) ? (jsize - hsz) : 0;
  if (area_size == 0)
  {
    fprintf(stderr, "Invalid journal area size 0\n");
    close(fd);
    return 1;
  }

  kj_header_t hdr;
  if (pread_all(fd, &hdr, sizeof(hdr), (off_t)joff) != 0)
  {
    perror("pread journal header");
    close(fd);
    return 1;
  }

  int ok = 1;
  if (hdr.magic != KJ_MAGIC)
  {
    fprintf(stderr, "Journal: bad magic\n");
    ok = 0;
  }
  if (hdr.version != KJ_VER)
  {
    fprintf(stderr, "Journal: bad version (%u)\n", hdr.version);
    ok = 0;
  }
  if (hdr.area_size != area_size)
  {
    fprintf(stderr, "Journal: area_size mismatch (sb=%" PRIu64 ", hdr=%" PRIu64 ")\n", area_size,
            (uint64_t)hdr.area_size);
    ok = 0;
  }
  {
    kj_header_t tmp = hdr;
    tmp.header_crc = 0;
    uint32_t c = kj_crc32(&tmp, sizeof(tmp));
    if (c != hdr.header_crc)
    {
      fprintf(stderr, "Journal: header CRC mismatch\n");
      ok = 0;
    }
  }

  if (!ok && !do_journal_reset)
  {
    close(fd);
    return FSCK_EXIT_JOURNAL_CHECK_FAILED;
  }

  // scan records up to write_off
  if (ok)
  {
    uint64_t pos = 0;
    while (pos + sizeof(kj_rec_hdr_t) <= hdr.write_off)
    {
      kj_rec_hdr_t rh;
      if (pread_all(fd, &rh, sizeof(rh), (off_t)(data_off + pos)) != 0)
      {
        perror("pread rec hdr");
        ok = 0;
        break;
      }
      pos += sizeof(rh);
      if (rh.tag == KJ_TAG_WRAP)
      {
        pos = 0;
        continue;
      }
      if (pos + rh.size > hdr.write_off)
      {
        fprintf(stderr, "Journal: partial tail\n");
        ok = 0;
        break;
      }
      char *pl = NULL;
      if (rh.size)
      {
        pl = (char *)malloc((size_t)rh.size);
        if (!pl)
        {
          perror("malloc");
          ok = 0;
          break;
        }
        if (pread_all(fd, pl, rh.size, (off_t)(data_off + pos)) != 0)
        {
          perror("pread rec payload");
          free(pl);
          ok = 0;
          break;
        }
      }
      // CRC check
      kj_rec_hdr_t rh2 = rh;
      rh2.crc32 = 0;
      uint32_t c = kj_crc32_update(0, (const uint8_t *)&rh2, sizeof(rh2));
      if (rh.size && pl)
        c = kj_crc32_update(c, (const uint8_t *)pl, rh.size);
      if (pl)
        free(pl);
      if (c != rh.crc32)
      {
        fprintf(stderr, "Journal: record CRC mismatch at off=%" PRIu64 "\n",
                (uint64_t)(pos - sizeof(rh)));
        ok = 0;
        break;
      }
      pos += rh.size;
    }
  }

  if (!ok && do_journal_reset)
  {
    // Reset ring: zero data area and write fresh header with write_off=0 (seq preserved if hdr
    // valid) zero data area in chunks
    const size_t chunk = 4096;
    char z[chunk];
    memset(z, 0, sizeof(z));
    uint64_t rem = area_size;
    uint64_t off = 0;
    while (rem)
    {
      size_t n = rem > chunk ? chunk : (size_t)rem;
      if (pwrite_all(fd, z, n, (off_t)(data_off + off)) != 0)
      {
        perror("pwrite zero");
        close(fd);
        return FSCK_EXIT_JOURNAL_RESET_FAILED;
      }
      off += n;
      rem -= n;
    }
    kj_header_t nh = {
        .magic = KJ_MAGIC,
        .version = KJ_VER,
        .flags = 0,
        .area_size = area_size,
        .write_off = 0,
        .seq = (ok ? hdr.seq : 0),
        .reserved0 = 0,
        .header_crc = 0,
    };
    nh.header_crc = kj_crc32(&nh, sizeof(nh));
    if (pwrite_all(fd, &nh, sizeof(nh), (off_t)joff) != 0)
    {
      perror("pwrite header");
      close(fd);
      return FSCK_EXIT_JOURNAL_RESET_FAILED;
    }
    if (fsync(fd) != 0)
    {
      perror("fsync");
      close(fd);
      return FSCK_EXIT_JOURNAL_RESET_FAILED;
    }
    fprintf(stderr, "Journal cleared.\n");
    close(fd);
    return exit_code;
  }

  if (!ok)
  {
    fprintf(stderr, "Journal check: FAIL\n");
    close(fd);
    return FSCK_EXIT_JOURNAL_CHECK_FAILED;
  }
  fprintf(stderr, "Journal check: OK\n");
  close(fd);
  return exit_code;
}
