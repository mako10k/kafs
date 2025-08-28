#include "kafs_superblock.h"

#include <fcntl.h>
#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

// In-image journal format (must match kafs_journal.c)
#define KJ_MAGIC 0x4b414a4c /* 'KAJL' */
#define KJ_VER   2
typedef struct kj_header {
  uint32_t magic;
  uint16_t version;
  uint16_t flags;
  uint64_t area_size;
  uint64_t write_off;
  uint64_t seq;
  uint64_t reserved0;
  uint32_t header_crc; // CRC over this struct with this field zeroed
} __attribute__((packed)) kj_header_t;

#define KJ_TAG_BEG  0x42454732u /* 'BEG2' */
#define KJ_TAG_CMT  0x434d5432u /* 'CMT2' */
#define KJ_TAG_ABR  0x41425232u /* 'ABR2' */
#define KJ_TAG_NOTE 0x4e4f5432u /* 'NOT2' */
#define KJ_TAG_WRAP 0x57524150u /* 'WRAP' */

typedef struct kj_rec_hdr {
  uint32_t tag;
  uint32_t size;
  uint64_t seq;
  uint32_t crc32;
} __attribute__((packed)) kj_rec_hdr_t;

static size_t kj_header_size(void) {
  size_t s = sizeof(kj_header_t);
  if (s % 64) s += 64 - (s % 64);
  return s;
}

static uint32_t crc32_update(uint32_t crc, const uint8_t *buf, size_t len) {
  crc = ~crc;
  for (size_t i = 0; i < len; ++i) {
    crc ^= buf[i];
    for (int k = 0; k < 8; ++k) crc = (crc >> 1) ^ (0xEDB88320u & (-(int)(crc & 1)));
  }
  return ~crc;
}
static uint32_t crc32_all(const void *buf, size_t len) { return crc32_update(0, (const uint8_t *)buf, len); }

static int pread_all(int fd, void *buf, size_t sz, off_t off) {
  ssize_t r = pread(fd, buf, sz, off);
  return (r == (ssize_t)sz) ? 0 : -1;
}
static int pwrite_all(int fd, const void *buf, size_t sz, off_t off) {
  ssize_t w = pwrite(fd, buf, sz, off);
  return (w == (ssize_t)sz) ? 0 : -1;
}

static void usage(const char *prog) {
  fprintf(stderr, "Usage: %s [--check-only|--journal-only] [--journal-clear] <image>\n", prog);
}

int main(int argc, char **argv) {
  int do_journal_clear = 0; // optional clear
  const char *img = NULL;
  for (int i = 1; i < argc; ++i) {
    if (strcmp(argv[i], "--check-only") == 0 || strcmp(argv[i], "--journal-only") == 0) {
      // no-op: default is check-only
    } else if (strcmp(argv[i], "--journal-clear") == 0) {
      do_journal_clear = 1;
    } else if (argv[i][0] != '-' && !img) {
      img = argv[i];
    } else {
      usage(argv[0]);
      return 2;
    }
  }
  if (!img) { usage(argv[0]); return 2; }

  int fd = open(img, do_journal_clear ? O_RDWR : O_RDONLY);
  if (fd < 0) { perror("open"); return 1; }

  // read superblock
  kafs_ssuperblock_t sb;
  if (pread_all(fd, &sb, sizeof(sb), 0) != 0) { perror("pread superblock"); close(fd); return 1; }

  uint64_t joff = kafs_sb_journal_offset_get(&sb);
  uint64_t jsize = kafs_sb_journal_size_get(&sb);
  if (joff == 0 || jsize < 4096) {
    fprintf(stderr, "No in-image journal: off=%" PRIu64 " size=%" PRIu64 "\n", joff, jsize);
    close(fd);
    return 0;
  }

  size_t hsz = kj_header_size();
  uint64_t data_off = joff + hsz;
  uint64_t area_size = (jsize > hsz) ? (jsize - hsz) : 0;
  if (area_size == 0) { fprintf(stderr, "Invalid journal area size 0\n"); close(fd); return 1; }

  kj_header_t hdr;
  if (pread_all(fd, &hdr, sizeof(hdr), (off_t)joff) != 0) { perror("pread journal header"); close(fd); return 1; }

  int ok = 1;
  if (hdr.magic != KJ_MAGIC) { fprintf(stderr, "Journal: bad magic\n"); ok = 0; }
  if (hdr.version != KJ_VER) { fprintf(stderr, "Journal: bad version (%u)\n", hdr.version); ok = 0; }
  if (hdr.area_size != area_size) { fprintf(stderr, "Journal: area_size mismatch (sb=%" PRIu64 ", hdr=%" PRIu64 ")\n", area_size, (uint64_t)hdr.area_size); ok = 0; }
  {
    kj_header_t tmp = hdr; tmp.header_crc = 0; uint32_t c = crc32_all(&tmp, sizeof(tmp));
    if (c != hdr.header_crc) { fprintf(stderr, "Journal: header CRC mismatch\n"); ok = 0; }
  }

  if (!ok && !do_journal_clear) { close(fd); return 3; }

  // scan records up to write_off
  if (ok) {
    uint64_t pos = 0;
    while (pos + sizeof(kj_rec_hdr_t) <= hdr.write_off) {
      kj_rec_hdr_t rh;
      if (pread_all(fd, &rh, sizeof(rh), (off_t)(data_off + pos)) != 0) { perror("pread rec hdr"); ok = 0; break; }
      pos += sizeof(rh);
      if (rh.tag == KJ_TAG_WRAP) { pos = 0; continue; }
      if (pos + rh.size > hdr.write_off) { fprintf(stderr, "Journal: partial tail\n"); ok = 0; break; }
      char *pl = NULL;
      if (rh.size) {
        pl = (char *)malloc((size_t)rh.size);
        if (!pl) { perror("malloc"); ok = 0; break; }
        if (pread_all(fd, pl, rh.size, (off_t)(data_off + pos)) != 0) { perror("pread rec payload"); free(pl); ok = 0; break; }
      }
      // CRC check
      size_t total = sizeof(rh) + rh.size;
      char *buf = (char *)malloc(total);
      if (!buf) { if (pl) free(pl); perror("malloc"); ok = 0; break; }
      kj_rec_hdr_t rh2 = rh; rh2.crc32 = 0;
      memcpy(buf, &rh2, sizeof(rh2));
      if (rh.size && pl) memcpy(buf + sizeof(rh2), pl, rh.size);
      uint32_t c = crc32_all(buf, total);
      free(buf);
      if (pl) free(pl);
      if (c != rh.crc32) { fprintf(stderr, "Journal: record CRC mismatch at off=%" PRIu64 "\n", (uint64_t)(pos - sizeof(rh))); ok = 0; break; }
      pos += rh.size;
    }
  }

  if (!ok && do_journal_clear) {
    // Reset ring: zero data area and write fresh header with write_off=0 (seq preserved if hdr valid)
    // zero data area in chunks
    const size_t chunk = 4096; char z[chunk]; memset(z, 0, sizeof(z));
    uint64_t rem = area_size; uint64_t off = 0;
    while (rem) {
      size_t n = rem > chunk ? chunk : (size_t)rem;
      if (pwrite_all(fd, z, n, (off_t)(data_off + off)) != 0) { perror("pwrite zero"); close(fd); return 4; }
      off += n; rem -= n;
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
    if (pwrite_all(fd, &nh, sizeof(nh), (off_t)joff) != 0) { perror("pwrite header"); close(fd); return 4; }
    if (fsync(fd) != 0) { perror("fsync"); close(fd); return 4; }
    fprintf(stderr, "Journal cleared.\n");
    close(fd);
    return 0;
  }

  if (!ok) { fprintf(stderr, "Journal check: FAIL\n"); close(fd); return 3; }
  fprintf(stderr, "Journal check: OK\n");
  close(fd);
  return 0;
}
