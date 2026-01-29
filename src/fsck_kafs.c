#include "kafs_superblock.h"
#include "kafs_inode.h"
#include "kafs_hash.h"
#include "kafs_locks.h"
#include "kafs_block.h"

#include <errno.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

// In-image journal format (must match kafs_journal.c)
#define KJ_MAGIC 0x4b414a4c /* 'KAJL' */
#define KJ_VER 2
typedef struct kj_header
{
  uint32_t magic;
  uint16_t version;
  uint16_t flags;
  uint64_t area_size;
  uint64_t write_off;
  uint64_t seq;
  uint64_t reserved0;
  uint32_t header_crc; // CRC over this struct with this field zeroed
} __attribute__((packed)) kj_header_t;

#define KJ_TAG_BEG 0x42454732u  /* 'BEG2' */
#define KJ_TAG_CMT 0x434d5432u  /* 'CMT2' */
#define KJ_TAG_ABR 0x41425232u  /* 'ABR2' */
#define KJ_TAG_NOTE 0x4e4f5432u /* 'NOT2' */
#define KJ_TAG_WRAP 0x57524150u /* 'WRAP' */

typedef struct kj_rec_hdr
{
  uint32_t tag;
  uint32_t size;
  uint64_t seq;
  uint32_t crc32;
} __attribute__((packed)) kj_rec_hdr_t;

static size_t kj_header_size(void)
{
  size_t s = sizeof(kj_header_t);
  if (s % 64)
    s += 64 - (s % 64);
  return s;
}

static uint32_t crc32_update(uint32_t crc, const uint8_t *buf, size_t len)
{
  crc = ~crc;
  for (size_t i = 0; i < len; ++i)
  {
    crc ^= buf[i];
    for (int k = 0; k < 8; ++k)
      crc = (crc >> 1) ^ (0xEDB88320u & (-(int)(crc & 1)));
  }
  return ~crc;
}
static uint32_t crc32_all(const void *buf, size_t len)
{
  return crc32_update(0, (const uint8_t *)buf, len);
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

static int orphan_reclaim(kafs_context_t *ctx, int do_fix)
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
          (void)kafs_hrl_dec_ref_by_blo(ctx, b);
      }

      // Recursive indirect release
      struct rel_ctx
      {
        kafs_context_t *ctx;
        void *base;
        size_t img_size;
        kafs_logblksize_t l2;
        uint32_t refs_pb;
      } rctx = {ctx, ctx->c_img_base, ctx->c_img_size, log_blksize, refs_pb};

      // nested helper via macro-style local function (GNU C)
      int (*rel_tbl)(kafs_blkcnt_t, int) = NULL;
      int rel_tbl_impl(kafs_blkcnt_t blo, int depth)
      {
        if (blo == KAFS_BLO_NONE)
          return 0;
        void *p = img_ptr(rctx.base, rctx.img_size, (off_t)blo << rctx.l2, blksize);
        if (!p)
          return -EIO;
        kafs_sblkcnt_t *tbl = (kafs_sblkcnt_t *)p;
        for (uint32_t i = 0; i < rctx.refs_pb; ++i)
        {
          kafs_blkcnt_t child = kafs_blkcnt_stoh(tbl[i]);
          if (child == KAFS_BLO_NONE)
            continue;
          if (depth > 1)
          {
            (void)rel_tbl(child, depth - 1);
            (void)kafs_hrl_dec_ref_by_blo(rctx.ctx, child);
          }
          else
          {
            (void)kafs_hrl_dec_ref_by_blo(rctx.ctx, child);
          }
        }
        return 0;
      }
      rel_tbl = rel_tbl_impl;

      kafs_blkcnt_t si = kafs_blkcnt_stoh(e->i_blkreftbl[12]);
      kafs_blkcnt_t di = kafs_blkcnt_stoh(e->i_blkreftbl[13]);
      kafs_blkcnt_t ti = kafs_blkcnt_stoh(e->i_blkreftbl[14]);
      if (si != KAFS_BLO_NONE)
      {
        (void)rel_tbl(si, 1);
        (void)kafs_hrl_dec_ref_by_blo(ctx, si);
      }
      if (di != KAFS_BLO_NONE)
      {
        (void)rel_tbl(di, 2);
        (void)kafs_hrl_dec_ref_by_blo(ctx, di);
      }
      if (ti != KAFS_BLO_NONE)
      {
        (void)rel_tbl(ti, 3);
        (void)kafs_hrl_dec_ref_by_blo(ctx, ti);
      }
    }

    memset(e, 0, sizeof(*e));
    (void)kafs_sb_inocnt_free_incr(sb);
    kafs_sb_wtime_set(sb, kafs_now());
  }

  if (found > 0)
    fprintf(stderr, "Orphan inodes: %d\n", found);
  return 0;
}

static void usage(const char *prog)
{
  fprintf(stderr,
          "Usage: %s [--check-only|--journal-only] [--journal-clear] [--orphan-reclaim] <image>\n",
          prog);
}

int main(int argc, char **argv)
{
  int do_journal_clear = 0;   // optional clear
  int do_orphan_reclaim = 0;   // optional fix
  const char *img = NULL;
  for (int i = 1; i < argc; ++i)
  {
    if (strcmp(argv[i], "--check-only") == 0 || strcmp(argv[i], "--journal-only") == 0)
    {
      // no-op: default is check-only
    }
    else if (strcmp(argv[i], "--journal-clear") == 0)
    {
      do_journal_clear = 1;
    }
    else if (strcmp(argv[i], "--orphan-reclaim") == 0)
    {
      do_orphan_reclaim = 1;
    }
    else if (argv[i][0] != '-' && !img)
    {
      img = argv[i];
    }
    else
    {
      usage(argv[0]);
      return 2;
    }
  }
  if (!img)
  {
    usage(argv[0]);
    return 2;
  }

  int want_write = do_journal_clear || do_orphan_reclaim;
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

  // Optional orphan reclaim (mount-time recovery equivalent)
  if (do_orphan_reclaim)
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

    (void)kafs_hrl_open(&ctx);
    (void)orphan_reclaim(&ctx, 1);
    (void)kafs_hrl_close(&ctx);
    if (want_write)
    {
      (void)msync(ctx.c_img_base, ctx.c_img_size, MS_SYNC);
      (void)fsync(fd);
    }
    munmap(ctx.c_img_base, ctx.c_img_size);
  }

  uint64_t joff = kafs_sb_journal_offset_get(&sb);
  uint64_t jsize = kafs_sb_journal_size_get(&sb);
  if (joff == 0 || jsize < 4096)
  {
    fprintf(stderr, "No in-image journal: off=%" PRIu64 " size=%" PRIu64 "\n", joff, jsize);
    close(fd);
    return 0;
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
    uint32_t c = crc32_all(&tmp, sizeof(tmp));
    if (c != hdr.header_crc)
    {
      fprintf(stderr, "Journal: header CRC mismatch\n");
      ok = 0;
    }
  }

  if (!ok && !do_journal_clear)
  {
    close(fd);
    return 3;
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
      size_t total = sizeof(rh) + rh.size;
      char *buf = (char *)malloc(total);
      if (!buf)
      {
        if (pl)
          free(pl);
        perror("malloc");
        ok = 0;
        break;
      }
      kj_rec_hdr_t rh2 = rh;
      rh2.crc32 = 0;
      memcpy(buf, &rh2, sizeof(rh2));
      if (rh.size && pl)
        memcpy(buf + sizeof(rh2), pl, rh.size);
      uint32_t c = crc32_all(buf, total);
      free(buf);
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

  if (!ok && do_journal_clear)
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
        return 4;
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
    nh.header_crc = crc32_all(&nh, sizeof(nh));
    if (pwrite_all(fd, &nh, sizeof(nh), (off_t)joff) != 0)
    {
      perror("pwrite header");
      close(fd);
      return 4;
    }
    if (fsync(fd) != 0)
    {
      perror("fsync");
      close(fd);
      return 4;
    }
    fprintf(stderr, "Journal cleared.\n");
    close(fd);
    return 0;
  }

  if (!ok)
  {
    fprintf(stderr, "Journal check: FAIL\n");
    close(fd);
    return 3;
  }
  fprintf(stderr, "Journal check: OK\n");
  close(fd);
  return 0;
}
