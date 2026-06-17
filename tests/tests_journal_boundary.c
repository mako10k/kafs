#include "kafs_context.h"
#include "kafs_journal.h"
#include "kafs_superblock.h"
#include "test_utils.h"

#include <errno.h>
#include <inttypes.h>
#include <stdarg.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/types.h>
#include <unistd.h>

static void tlogf(const char *fmt, ...)
{
  va_list ap;
  va_start(ap, fmt);
  vfprintf(stderr, fmt, ap);
  fputc('\n', stderr);
  va_end(ap);
}

static int read_header(int fd, uint64_t off, kj_header_t *hdr)
{
  ssize_t r = pread(fd, hdr, sizeof(*hdr), (off_t)off);
  return (r == (ssize_t)sizeof(*hdr)) ? 0 : -EIO;
}

static int write_header(int fd, uint64_t off, const kj_header_t *in)
{
  kj_header_t hdr = *in;
  hdr.header_crc = kj_header_crc_calc(&hdr);
  ssize_t w = pwrite(fd, &hdr, sizeof(hdr), (off_t)off);
  if (w != (ssize_t)sizeof(hdr))
    return -EIO;
  fsync(fd);
  return 0;
}

struct test_journal_header_read_ctx
{
  int fd;
  uint64_t joff;
  uint64_t area_size;
};

static int read_header_slot_cb(void *user, uint32_t slot, kj_header_t *hdr)
{
  const struct test_journal_header_read_ctx *ctx =
      (const struct test_journal_header_read_ctx *)user;
  uint64_t off = kj_header_slot_offset(ctx->joff, ctx->area_size, slot);

  return read_header(ctx->fd, off, hdr);
}

static int find_best_header(int fd, uint64_t joff, uint64_t area_size, uint32_t slot_count,
                            kj_header_t *out, uint32_t *out_slot, uint32_t *out_valid_count)
{
  struct test_journal_header_read_ctx read_ctx = {
      .fd = fd,
      .joff = joff,
      .area_size = area_size,
  };

  return (kj_header_select_best(slot_count, area_size, read_header_slot_cb, &read_ctx, out, out_slot,
                                out_valid_count) == 0)
             ? 0
             : -ENOENT;
}

static int corrupt_header_crc(int fd, uint64_t off)
{
  kj_header_t hdr;
  if (read_header(fd, off, &hdr) != 0)
    return -EIO;
  hdr.header_crc ^= 0x5a5a5a5au;
  ssize_t w = pwrite(fd, &hdr, sizeof(hdr), (off_t)off);
  if (w != (ssize_t)sizeof(hdr))
    return -EIO;
  fsync(fd);
  return 0;
}

static int read_rec_hdr(int fd, uint64_t data_off, uint64_t ring_off, kj_rec_hdr_t *rh)
{
  ssize_t r = pread(fd, rh, sizeof(*rh), (off_t)(data_off + ring_off));
  return (r == (ssize_t)sizeof(*rh)) ? 0 : -EIO;
}

static int replay_count_cb(struct kafs_context *ctx, const char *op, const char *args, void *user)
{
  (void)ctx;
  (void)op;
  (void)args;
  int *count = (int *)user;
  (*count)++;
  return 0;
}

int main(void)
{
  if (kafs_test_enter_tmpdir("journal_boundary") != 0)
    return 77;

  const char *img = "journal-boundary.img";
  kafs_context_t ctx;
  off_t mapsize = 0;

  if (kafs_test_mkimg_with_hrl(img, 64u * 1024u * 1024u, 12, 4096, &ctx, &mapsize) != 0)
  {
    tlogf("mkimg failed");
    return 77;
  }

  const uint64_t joff = kafs_sb_journal_offset_get(ctx.c_superblock);
  const uint64_t jsize = kafs_sb_journal_size_get(ctx.c_superblock);
  const uint64_t data_off = joff + kj_header_size();
  if (joff == 0 || jsize < 4096)
  {
    tlogf("journal region unavailable");
    munmap(ctx.c_superblock, mapsize);
    close(ctx.c_fd);
    return 77;
  }

  // --- Case 1: write_off == area_size must still be replayable (boundary regression) ---
  if (kafs_journal_init(&ctx, img) != 0)
  {
    tlogf("journal init failed (case1)");
    munmap(ctx.c_superblock, mapsize);
    close(ctx.c_fd);
    return 1;
  }
  uint64_t seq = kafs_journal_begin(&ctx, "BOUNDARY", "path=/x");
  if (seq == 0)
  {
    tlogf("journal begin returned 0");
    kafs_journal_shutdown(&ctx);
    munmap(ctx.c_superblock, mapsize);
    close(ctx.c_fd);
    return 1;
  }
  kafs_journal_commit(&ctx, seq);
  kafs_journal_force_flush(&ctx);
  kafs_journal_shutdown(&ctx);

  kj_header_t hdr;
  if (read_header(ctx.c_fd, joff, &hdr) != 0)
  {
    tlogf("read header failed (case1)");
    munmap(ctx.c_superblock, mapsize);
    close(ctx.c_fd);
    return 1;
  }
  hdr.write_off = hdr.area_size;
  if (write_header(ctx.c_fd, joff, &hdr) != 0)
  {
    tlogf("write header failed (case1)");
    munmap(ctx.c_superblock, mapsize);
    close(ctx.c_fd);
    return 1;
  }

  int replay_count = 0;
  if (kafs_journal_replay(&ctx, replay_count_cb, &replay_count) != 0)
  {
    tlogf("replay failed (case1)");
    munmap(ctx.c_superblock, mapsize);
    close(ctx.c_fd);
    return 1;
  }
  if (replay_count != 1)
  {
    tlogf("expected replay_count=1 at write_off==area_size, got %d", replay_count);
    munmap(ctx.c_superblock, mapsize);
    close(ctx.c_fd);
    return 1;
  }

  // --- Measure one NOTE record length for precise boundary positioning ---
  if (kafs_journal_init(&ctx, img) != 0)
  {
    tlogf("journal init failed (measure)");
    munmap(ctx.c_superblock, mapsize);
    close(ctx.c_fd);
    return 1;
  }

  kj_header_t h_before;
  kj_header_t h_after;
  if (read_header(ctx.c_fd, joff, &h_before) != 0)
  {
    tlogf("read header failed before NOTE");
    kafs_journal_shutdown(&ctx);
    munmap(ctx.c_superblock, mapsize);
    close(ctx.c_fd);
    return 1;
  }
  kafs_journal_note(&ctx, "N", "x=%d", 1);
  kafs_journal_force_flush(&ctx);
  if (read_header(ctx.c_fd, joff, &h_after) != 0)
  {
    tlogf("read header failed after NOTE");
    kafs_journal_shutdown(&ctx);
    munmap(ctx.c_superblock, mapsize);
    close(ctx.c_fd);
    return 1;
  }
  if (h_after.write_off <= h_before.write_off)
  {
    tlogf("unexpected write_off progression: before=%" PRIu64 " after=%" PRIu64, h_before.write_off,
          h_after.write_off);
    kafs_journal_shutdown(&ctx);
    munmap(ctx.c_superblock, mapsize);
    close(ctx.c_fd);
    return 1;
  }
  uint64_t note_len = h_after.write_off - h_before.write_off;
  if (note_len <= sizeof(kj_rec_hdr_t) || note_len >= h_after.area_size)
  {
    tlogf("invalid note_len=%" PRIu64 " area=%" PRIu64, note_len, h_after.area_size);
    kafs_journal_shutdown(&ctx);
    munmap(ctx.c_superblock, mapsize);
    close(ctx.c_fd);
    return 1;
  }
  kafs_journal_shutdown(&ctx);

  // --- Case 2: remaining == note_len (exact-fit, no WRAP marker expected) ---
  if (read_header(ctx.c_fd, joff, &hdr) != 0)
  {
    tlogf("read header failed (case2)");
    munmap(ctx.c_superblock, mapsize);
    close(ctx.c_fd);
    return 1;
  }
  if (note_len + sizeof(kj_rec_hdr_t) >= hdr.area_size)
  {
    tlogf("journal area too small for boundary test: note_len=%" PRIu64 " area=%" PRIu64, note_len,
          hdr.area_size);
    munmap(ctx.c_superblock, mapsize);
    close(ctx.c_fd);
    return 77;
  }

  const uint64_t exact_off = hdr.area_size - note_len;
  hdr.write_off = exact_off;
  if (write_header(ctx.c_fd, joff, &hdr) != 0)
  {
    tlogf("write header failed (case2)");
    munmap(ctx.c_superblock, mapsize);
    close(ctx.c_fd);
    return 1;
  }

  if (kafs_journal_init(&ctx, img) != 0)
  {
    tlogf("journal init failed (case2)");
    munmap(ctx.c_superblock, mapsize);
    close(ctx.c_fd);
    return 1;
  }
  kafs_journal_note(&ctx, "N", "x=%d", 2);
  kafs_journal_force_flush(&ctx);
  if (read_header(ctx.c_fd, joff, &h_after) != 0)
  {
    tlogf("read header failed after exact-fit NOTE");
    kafs_journal_shutdown(&ctx);
    munmap(ctx.c_superblock, mapsize);
    close(ctx.c_fd);
    return 1;
  }
  if (h_after.write_off != hdr.area_size)
  {
    tlogf("exact-fit should end at area_size: got=%" PRIu64 " want=%" PRIu64, h_after.write_off,
          hdr.area_size);
    kafs_journal_shutdown(&ctx);
    munmap(ctx.c_superblock, mapsize);
    close(ctx.c_fd);
    return 1;
  }
  kj_rec_hdr_t rh_exact;
  if (read_rec_hdr(ctx.c_fd, data_off, exact_off, &rh_exact) != 0)
  {
    tlogf("read record failed at exact_off");
    kafs_journal_shutdown(&ctx);
    munmap(ctx.c_superblock, mapsize);
    close(ctx.c_fd);
    return 1;
  }
  if (rh_exact.tag == KJ_TAG_WRAP)
  {
    tlogf("unexpected WRAP marker on exact-fit write");
    kafs_journal_shutdown(&ctx);
    munmap(ctx.c_superblock, mapsize);
    close(ctx.c_fd);
    return 1;
  }
  kafs_journal_shutdown(&ctx);

  // --- Case 3: remaining == note_len-1 (must emit WRAP marker and wrap to 0) ---
  if (read_header(ctx.c_fd, joff, &hdr) != 0)
  {
    tlogf("read header failed (case3)");
    munmap(ctx.c_superblock, mapsize);
    close(ctx.c_fd);
    return 1;
  }
  const uint64_t wrap_off = hdr.area_size - note_len + 1;
  hdr.write_off = wrap_off;
  if (write_header(ctx.c_fd, joff, &hdr) != 0)
  {
    tlogf("write header failed (case3)");
    munmap(ctx.c_superblock, mapsize);
    close(ctx.c_fd);
    return 1;
  }

  if (kafs_journal_init(&ctx, img) != 0)
  {
    tlogf("journal init failed (case3)");
    munmap(ctx.c_superblock, mapsize);
    close(ctx.c_fd);
    return 1;
  }
  kafs_journal_note(&ctx, "N", "x=%d", 3);
  kafs_journal_force_flush(&ctx);
  if (read_header(ctx.c_fd, joff, &h_after) != 0)
  {
    tlogf("read header failed after wrap NOTE");
    kafs_journal_shutdown(&ctx);
    munmap(ctx.c_superblock, mapsize);
    close(ctx.c_fd);
    return 1;
  }
  if (h_after.write_off != note_len)
  {
    tlogf("wrap write should end at note_len: got=%" PRIu64 " want=%" PRIu64, h_after.write_off,
          note_len);
    kafs_journal_shutdown(&ctx);
    munmap(ctx.c_superblock, mapsize);
    close(ctx.c_fd);
    return 1;
  }

  kj_rec_hdr_t rh_wrap;
  if (read_rec_hdr(ctx.c_fd, data_off, wrap_off, &rh_wrap) != 0)
  {
    tlogf("read record failed at wrap_off");
    kafs_journal_shutdown(&ctx);
    munmap(ctx.c_superblock, mapsize);
    close(ctx.c_fd);
    return 1;
  }
  if (rh_wrap.tag != KJ_TAG_WRAP)
  {
    tlogf("expected WRAP marker at boundary, got tag=0x%08x", rh_wrap.tag);
    kafs_journal_shutdown(&ctx);
    munmap(ctx.c_superblock, mapsize);
    close(ctx.c_fd);
    return 1;
  }

  kafs_journal_shutdown(&ctx);

  // --- Baseline: single-header mode can only keep one valid header location ---
  const uint32_t single_slots = kj_header_slot_count(0, jsize);
  const uint64_t single_area = kj_journal_area_size(jsize, 0);
  if (single_slots != 1u || single_area == 0)
  {
    tlogf("single journal header configuration unavailable");
    munmap(ctx.c_superblock, mapsize);
    close(ctx.c_fd);
    return 77;
  }

  kj_header_t single_best;
  uint32_t single_best_slot = 0;
  uint32_t single_valid_count = 0;
  if (find_best_header(ctx.c_fd, joff, single_area, single_slots, &single_best, &single_best_slot,
                       &single_valid_count) != 0)
  {
    tlogf("no valid single journal header found");
    munmap(ctx.c_superblock, mapsize);
    close(ctx.c_fd);
    return 1;
  }
  if (single_valid_count != 1u || single_best_slot != 0)
  {
    tlogf("unexpected single header spread: valid=%" PRIu32 " slot=%" PRIu32,
          single_valid_count, single_best_slot);
    munmap(ctx.c_superblock, mapsize);
    close(ctx.c_fd);
    return 1;
  }

  // --- Case 4: rotated header slots survive latest-slot corruption ---
  kafs_sb_journal_flags_set(ctx.c_superblock, KAFS_JOURNAL_FLAG_ROTATING_HEADERS);
  if (msync(ctx.c_superblock, sizeof(*ctx.c_superblock), MS_SYNC) != 0)
  {
    tlogf("msync superblock failed for rotated header case");
    munmap(ctx.c_superblock, mapsize);
    close(ctx.c_fd);
    return 1;
  }

  const uint32_t rot_slots =
      kj_header_slot_count(kafs_sb_journal_flags_get(ctx.c_superblock), jsize);
  const uint64_t rot_area =
      kj_journal_area_size(jsize, kafs_sb_journal_flags_get(ctx.c_superblock));
  if (rot_slots < 2 || rot_area == 0)
  {
    tlogf("rotated journal header configuration unavailable");
    munmap(ctx.c_superblock, mapsize);
    close(ctx.c_fd);
    return 77;
  }

  if (kafs_journal_init(&ctx, img) != 0)
  {
    tlogf("journal init failed (rotated)");
    munmap(ctx.c_superblock, mapsize);
    close(ctx.c_fd);
    return 1;
  }
  for (uint32_t i = 0; i < rot_slots + 2u; ++i)
  {
    kafs_journal_note(&ctx, "R", "i=%u", i);
    kafs_journal_force_flush(&ctx);
  }
  kafs_journal_shutdown(&ctx);

  kj_header_t best;
  uint32_t best_slot = 0;
  uint32_t valid_count = 0;
  if (find_best_header(ctx.c_fd, joff, rot_area, rot_slots, &best, &best_slot, &valid_count) != 0)
  {
    tlogf("no valid rotated journal header found");
    munmap(ctx.c_superblock, mapsize);
    close(ctx.c_fd);
    return 1;
  }
  if (valid_count < 2)
  {
    tlogf("expected multiple valid rotated header slots, got %" PRIu32, valid_count);
    munmap(ctx.c_superblock, mapsize);
    close(ctx.c_fd);
    return 1;
  }
  if (valid_count <= single_valid_count || best_slot == single_best_slot)
  {
    tlogf("rotated headers did not spread: single_valid=%" PRIu32
          " rotated_valid=%" PRIu32 " single_slot=%" PRIu32 " rotated_slot=%" PRIu32,
          single_valid_count, valid_count, single_best_slot, best_slot);
    munmap(ctx.c_superblock, mapsize);
    close(ctx.c_fd);
    return 1;
  }
  tlogf("journal header spread: single_valid=%" PRIu32 " rotated_valid=%" PRIu32
        " rotated_active=%" PRIu32 " generation=%" PRIu64,
        single_valid_count, valid_count, best_slot, best.reserved0);

  const uint64_t corrupt_generation = best.reserved0;
  const uint64_t corrupt_off = kj_header_slot_offset(joff, rot_area, best_slot);
  if (corrupt_header_crc(ctx.c_fd, corrupt_off) != 0)
  {
    tlogf("failed to corrupt latest rotated header slot");
    munmap(ctx.c_superblock, mapsize);
    close(ctx.c_fd);
    return 1;
  }

  if (kafs_journal_init(&ctx, img) != 0)
  {
    tlogf("journal init failed after rotated latest-slot corruption");
    munmap(ctx.c_superblock, mapsize);
    close(ctx.c_fd);
    return 1;
  }
  kafs_journal_note(&ctx, "R", "after_corrupt=1");
  kafs_journal_force_flush(&ctx);
  kafs_journal_shutdown(&ctx);

  if (find_best_header(ctx.c_fd, joff, rot_area, rot_slots, &best, &best_slot, &valid_count) != 0)
  {
    tlogf("no valid rotated journal header found after corruption recovery");
    munmap(ctx.c_superblock, mapsize);
    close(ctx.c_fd);
    return 1;
  }
  if (best.reserved0 <= corrupt_generation)
  {
    tlogf("rotated header generation did not advance after recovery: got=%" PRIu64 " old=%" PRIu64,
          best.reserved0, corrupt_generation);
    munmap(ctx.c_superblock, mapsize);
    close(ctx.c_fd);
    return 1;
  }

  munmap(ctx.c_superblock, mapsize);
  close(ctx.c_fd);
  tlogf("journal_boundary OK");
  return 0;
}
