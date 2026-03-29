#include "kafs_journal.h"
#include "kafs_context.h"
#include "kafs_locks.h"
#include "kafs_superblock.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#include <sys/stat.h>
#include <time.h>
#include <inttypes.h>

#ifdef __has_include
#if __has_include(<pthread.h>)
#include <pthread.h>
#define KAFS_JOURNAL_HAS_PTHREAD 1
#else
#define KAFS_JOURNAL_HAS_PTHREAD 0
#endif
#else
#define KAFS_JOURNAL_HAS_PTHREAD 1
#include <pthread.h>
#endif

// Extend context with journal pointer via forward declaration; we avoid header coupling.
typedef struct kafs_context_ext
{
  // mirror initial layout of kafs_context to keep ABI consistent within TU
  void *c_superblock;
  void *c_inotbl;
  void *c_blkmasktbl;
  unsigned int c_ino_search;
  unsigned int c_blo_search;
  int c_fd;
  size_t c_mapsize;
  void *c_img_base;
  size_t c_img_size;
  void *c_hrl_index;
  unsigned int c_hrl_bucket_cnt;
  void *c_lock_hrl_buckets;
  void *c_lock_hrl_global;
  void *c_lock_bitmap;
  void *c_lock_inode;
  // not in header: attach journal pointer at end through separate symbol
} kafs_context_ext_t;

// We'll store journal instance in a static map keyed by ctx pointer (simple single-instance)
typedef struct journal_state
{
  struct kafs_context *ctx;
  kafs_journal_t j;
} journal_state_t;

static journal_state_t g_state = {0};

static int kj_write_record(kafs_journal_t *j, uint32_t tag, uint64_t seq, const char *payload);

typedef struct kj_meta_snapshot
{
  int valid;
  kafs_blkcnt_t free_abs;
  kafs_time_t wtime;
  uint32_t wtime_dirty;
  size_t word_count;
  uint32_t *word_idx;
  kafs_blkmask_t *word_val;
} kj_meta_snapshot_t;

static void kj_meta_snapshot_clear(kj_meta_snapshot_t *s)
{
  if (!s)
    return;
  free(s->word_idx);
  free(s->word_val);
  memset(s, 0, sizeof(*s));
}

static int kj_collect_meta_snapshot(struct kafs_context *ctx, kj_meta_snapshot_t *out)
{
  if (!ctx || !out)
    return 0;
  if (!ctx->c_meta_delta_enabled)
    return 0;

  memset(out, 0, sizeof(*out));

  kafs_bitmap_lock(ctx);

  kafs_blkcnt_t blkcnt = kafs_sb_blkcnt_get(ctx->c_superblock);
  kafs_blkcnt_t free_now = kafs_sb_blkcnt_free_get(ctx->c_superblock);
  int64_t merged = (int64_t)free_now + ctx->c_meta_delta_free_blocks;
  if (merged < 0)
    merged = 0;
  if ((uint64_t)merged > (uint64_t)blkcnt)
    merged = (int64_t)blkcnt;
  out->free_abs = (kafs_blkcnt_t)merged;

  out->wtime_dirty = ctx->c_meta_delta_wtime_dirty ? 1u : 0u;
  out->wtime = ctx->c_meta_delta_last_wtime;

  if (ctx->c_meta_bitmap_words_enabled && ctx->c_meta_bitmap_dirty && ctx->c_meta_bitmap_words &&
      ctx->c_meta_bitmap_wordcnt > 0 && ctx->c_meta_bitmap_dirty_count > 0)
  {
    size_t dirty_count = ctx->c_meta_bitmap_dirty_count;
    out->word_idx = (uint32_t *)calloc(dirty_count, sizeof(uint32_t));
    out->word_val = (kafs_blkmask_t *)calloc(dirty_count, sizeof(kafs_blkmask_t));
    if (out->word_idx && out->word_val)
    {
      size_t n = 0;
      for (size_t i = 0; i < ctx->c_meta_bitmap_wordcnt && n < dirty_count; ++i)
      {
        if (!ctx->c_meta_bitmap_dirty[i])
          continue;
        out->word_idx[n] = (uint32_t)i;
        out->word_val[n] = ctx->c_meta_bitmap_words[i];
        ++n;
      }
      out->word_count = n;
    }
    else
    {
      free(out->word_idx);
      free(out->word_val);
      out->word_idx = NULL;
      out->word_val = NULL;
      out->word_count = 0;
    }
  }

  kafs_bitmap_unlock(ctx);

  out->valid = 1;
  return 1;
}

static int kj_write_meta_delta_record(kafs_journal_t *j, uint64_t seq, const kj_meta_snapshot_t *s)
{
  if (!j || !s || !s->valid)
    return 0;

  size_t cap = 160u + (s->word_count * 44u);
  char *payload = (char *)calloc(cap, 1u);
  if (!payload)
    return -ENOMEM;

  int n = snprintf(payload, cap,
                   "free_abs=%" PRIuFAST32 " wtime_dirty=%u wtime_sec=%lld "
                   "wtime_nsec=%ld wc=%zu words=",
                   s->free_abs, s->wtime_dirty, (long long)s->wtime.tv_sec, s->wtime.tv_nsec,
                   s->word_count);
  if (n < 0)
  {
    free(payload);
    return -EINVAL;
  }
  size_t off = (size_t)n;

  for (size_t i = 0; i < s->word_count && off < cap; ++i)
  {
    int m = snprintf(payload + off, cap - off, "%s%u:%" PRIxMAX, (i == 0) ? "" : ",",
                     s->word_idx[i], (uintmax_t)s->word_val[i]);
    if (m < 0)
      break;
    off += (size_t)m;
  }

  int rc = kj_write_record(j, KJ_TAG_MDT, seq, payload);
  free(payload);
  return rc;
}

static void kj_replay_apply_meta_delta(struct kafs_context *ctx, const char *payload)
{
  if (!ctx || !payload || !*payload)
    return;

  uint64_t free_abs = 0;
  uint32_t wtime_dirty = 0;
  long long wtime_sec = 0;
  long wtime_nsec = 0;

  const char *p = strstr(payload, "free_abs=");
  if (p)
    (void)sscanf(p, "free_abs=%" SCNu64, &free_abs);
  p = strstr(payload, "wtime_dirty=");
  if (p)
    (void)sscanf(p, "wtime_dirty=%u", &wtime_dirty);
  p = strstr(payload, "wtime_sec=");
  if (p)
    (void)sscanf(p, "wtime_sec=%lld", &wtime_sec);
  p = strstr(payload, "wtime_nsec=");
  if (p)
    (void)sscanf(p, "wtime_nsec=%ld", &wtime_nsec);

  kafs_blkcnt_t blkcnt = kafs_sb_blkcnt_get(ctx->c_superblock);
  if (free_abs > (uint64_t)blkcnt)
    free_abs = (uint64_t)blkcnt;
  kafs_sb_blkcnt_free_set(ctx->c_superblock, (kafs_blkcnt_t)free_abs);

  if (wtime_dirty)
  {
    kafs_time_t wt = {.tv_sec = (time_t)wtime_sec, .tv_nsec = wtime_nsec};
    kafs_sb_wtime_set(ctx->c_superblock, wt);
  }

  const char *w = strstr(payload, "words=");
  if (!w)
    return;
  w += 6;

  size_t bits = sizeof(kafs_blkmask_t) * 8u;
  size_t wordcnt = ((size_t)kafs_sb_blkcnt_get(ctx->c_superblock) + bits - 1u) / bits;

  char *copy = strdup(w);
  if (!copy)
    return;

  char *save = NULL;
  for (char *tok = strtok_r(copy, ",", &save); tok; tok = strtok_r(NULL, ",", &save))
  {
    unsigned idx = 0;
    uintmax_t val = 0;
    if (sscanf(tok, "%u:%" SCNxMAX, &idx, &val) != 2)
      continue;
    if ((size_t)idx >= wordcnt)
      continue;
    ctx->c_blkmasktbl[idx] = (kafs_blkmask_t)val;
  }
  free(copy);
}

static void kj_apply_meta_delta(struct kafs_context *ctx)
{
  if (!ctx || !ctx->c_meta_delta_enabled)
    return;

  kafs_bitmap_lock(ctx);

  int64_t free_delta = ctx->c_meta_delta_free_blocks;
  uint32_t wtime_dirty = ctx->c_meta_delta_wtime_dirty;
  kafs_time_t wtime = ctx->c_meta_delta_last_wtime;

  if (ctx->c_meta_bitmap_words_enabled && ctx->c_meta_bitmap_dirty && ctx->c_meta_bitmap_words &&
      ctx->c_meta_bitmap_wordcnt > 0 && ctx->c_meta_bitmap_dirty_count > 0)
  {
    for (size_t i = 0; i < ctx->c_meta_bitmap_wordcnt; ++i)
    {
      if (!ctx->c_meta_bitmap_dirty[i])
        continue;
      ctx->c_blkmasktbl[i] = ctx->c_meta_bitmap_words[i];
      ctx->c_meta_bitmap_dirty[i] = 0;
    }
    ctx->c_meta_bitmap_dirty_count = 0;
  }

  if (free_delta != 0)
  {
    kafs_blkcnt_t free_now = kafs_sb_blkcnt_free_get(ctx->c_superblock);
    int64_t merged = (int64_t)free_now + free_delta;
    if (merged < 0)
      merged = 0;
    kafs_blkcnt_t blkcnt = kafs_sb_blkcnt_get(ctx->c_superblock);
    if ((uint64_t)merged > (uint64_t)blkcnt)
      merged = (int64_t)blkcnt;
    kafs_sb_blkcnt_free_set(ctx->c_superblock, (kafs_blkcnt_t)merged);
    ctx->c_meta_delta_free_blocks = 0;
  }

  if (wtime_dirty)
  {
    if (wtime.tv_sec == 0 && wtime.tv_nsec == 0)
      clock_gettime(CLOCK_REALTIME, &wtime);
    kafs_sb_wtime_set(ctx->c_superblock, wtime);
    ctx->c_meta_delta_wtime_dirty = 0;
    ctx->c_meta_delta_last_wtime = (kafs_time_t){0};
  }

  kafs_bitmap_unlock(ctx);
}

static void timespec_now(struct timespec *ts) { clock_gettime(CLOCK_REALTIME, ts); }

static uint64_t nsec_now_mono(void)
{
  struct timespec ts;
  clock_gettime(CLOCK_MONOTONIC, &ts);
  return (uint64_t)ts.tv_sec * 1000000000ull + (uint64_t)ts.tv_nsec;
}

static void sleep_ns(uint64_t ns)
{
  if (ns == 0)
    return;
  struct timespec req;
  req.tv_sec = (time_t)(ns / 1000000000ull);
  req.tv_nsec = (long)(ns % 1000000000ull);
  while (nanosleep(&req, &req) == -1 && errno == EINTR)
  {
    // retry with remaining time in req
  }
}

static int jlock(kafs_journal_t *j)
{
#if KAFS_JOURNAL_HAS_PTHREAD
  if (!j->mtx)
    return 0;
  return pthread_mutex_lock((pthread_mutex_t *)j->mtx);
#else
  (void)j;
  return 0;
#endif
}
static int junlock(kafs_journal_t *j)
{
#if KAFS_JOURNAL_HAS_PTHREAD
  if (!j->mtx)
    return 0;
  return pthread_mutex_unlock((pthread_mutex_t *)j->mtx);
#else
  (void)j;
  return 0;
#endif
}

static void jwritef(kafs_journal_t *j, const char *line)
{
  if (j->fd < 0)
    return;
  size_t len = strlen(line);
  (void)write(j->fd, line, len);
  (void)write(j->fd, "\n", 1);
}

static void jprintf(kafs_journal_t *j, const char *fmt, ...)
{
  if (j->fd < 0)
    return;
  char buf[1024];
  va_list ap;
  va_start(ap, fmt);
  vsnprintf(buf, sizeof(buf), fmt, ap);
  va_end(ap);
  jwritef(j, buf);
}

typedef enum
{
  KJ_SIDECAR_BEGIN,
  KJ_SIDECAR_ABORT,
  KJ_SIDECAR_NOTE,
} kj_sidecar_kind_t;

static void kj_write_sidecar_tsline(kafs_journal_t *j, kj_sidecar_kind_t kind, uint64_t seq,
                                    const char *op, const char *opt_fmt, va_list opt_ap)
{
  if (j->fd < 0)
    return;
  struct timespec ts;
  timespec_now(&ts);
  char prefix[160];
  switch (kind)
  {
  case KJ_SIDECAR_BEGIN:
    snprintf(prefix, sizeof(prefix), "BEGIN %llu %s %ld.%09ld ", (unsigned long long)seq,
             op ? op : "", (long)ts.tv_sec, ts.tv_nsec);
    break;
  case KJ_SIDECAR_ABORT:
    snprintf(prefix, sizeof(prefix), "ABORT %llu %ld.%09ld ", (unsigned long long)seq,
             (long)ts.tv_sec, ts.tv_nsec);
    break;
  case KJ_SIDECAR_NOTE:
    snprintf(prefix, sizeof(prefix), "NOTE %s %ld.%09ld ", op ? op : "", (long)ts.tv_sec,
             ts.tv_nsec);
    break;
  }
  dprintf(j->fd, "%s", prefix);
  if (opt_fmt && *opt_fmt)
    vdprintf(j->fd, opt_fmt, opt_ap);
  dprintf(j->fd, "\n");
  fsync(j->fd);
}

// ----------------------
// In-image ring journal
// ----------------------
// Journal format definitions live in kafs_journal.h

static int kj_pread(int fd, void *buf, size_t sz, off_t off)
{
  ssize_t r = pread(fd, buf, sz, off);
  return (r == (ssize_t)sz) ? 0 : -EIO;
}
static int kj_pwrite_fsync(int fd, const void *buf, size_t sz, off_t off)
{
  ssize_t w = pwrite(fd, buf, sz, off);
  if (w != (ssize_t)sz)
    return -EIO;
  fsync(fd);
  return 0;
}

static int kj_pwrite_nosync(int fd, const void *buf, size_t sz, off_t off)
{
  ssize_t w = pwrite(fd, buf, sz, off);
  return (w == (ssize_t)sz) ? 0 : -EIO;
}

static int kj_header_load(kafs_journal_t *j, kj_header_t *hdr)
{
  if (!j->use_inimage)
    return -EINVAL;
  return kj_pread(j->fd, hdr, sizeof(*hdr), (off_t)j->base_off);
}
static int kj_header_store(kafs_journal_t *j, const kj_header_t *hdr)
{
  if (!j->use_inimage)
    return -EINVAL;
  return kj_pwrite_fsync(j->fd, hdr, sizeof(*hdr), (off_t)j->base_off);
}

static void kj_reset_area(kafs_journal_t *j)
{
  // Not strictly necessary to zero, but keeps tests deterministic
  const size_t chunk = 4096;
  char z[chunk];
  memset(z, 0, sizeof(z));
  uint64_t remaining = j->area_size;
  uint64_t off = 0;
  while (remaining)
  {
    size_t n = remaining > chunk ? chunk : (size_t)remaining;
    (void)kj_pwrite_fsync(j->fd, z, n, (off_t)(j->data_off + off));
    off += n;
    remaining -= n;
  }
}

static int kj_init_or_load(kafs_journal_t *j)
{
  kj_header_t hdr;
  if (kj_header_load(j, &hdr) == 0 && hdr.magic == KJ_MAGIC && hdr.area_size == j->area_size)
  {
    if (hdr.version == KJ_VER)
    {
      kj_header_t tmp = hdr;
      tmp.header_crc = 0;
      uint32_t c = kj_crc32(&tmp, sizeof(tmp));
      if (c == hdr.header_crc)
      {
        j->write_off = hdr.write_off <= j->area_size ? hdr.write_off : 0;
        j->seq = hdr.seq;
        return 0;
      }
    }
  }
  // initialize fresh header
  j->write_off = 0;
  j->seq = 0;
  kj_reset_area(j);
  kj_header_t nh = {
      .magic = KJ_MAGIC,
      .version = KJ_VER,
      .flags = 0,
      .area_size = j->area_size,
      .write_off = 0,
      .seq = 0,
      .reserved0 = 0,
      .header_crc = 0,
  };
  nh.header_crc = kj_crc32(&nh, sizeof(nh));
  return kj_header_store(j, &nh);
}

static void kj_persist_header(kafs_journal_t *j, int do_fsync)
{
  kj_header_t hdr;
  if (kj_header_load(j, &hdr) == 0)
  {
    hdr.write_off = j->write_off;
    hdr.seq = j->seq;
    hdr.header_crc = 0;
    hdr.header_crc = kj_crc32(&hdr, sizeof(hdr));
    if (do_fsync)
      (void)kj_header_store(j, &hdr);
    else
      (void)kj_pwrite_nosync(j->fd, &hdr, sizeof(hdr), (off_t)j->base_off);
  }
}

static int kj_ring_write(kafs_journal_t *j, const void *data, size_t len, int do_fsync)
{
  if (!j->use_inimage)
    return -EINVAL;
  uint64_t remaining = j->area_size - j->write_off;
  if ((uint64_t)len > remaining)
  {
    // write WRAP marker if space allows
    kj_rec_hdr_t wrap = {.tag = KJ_TAG_WRAP, .size = 0, .seq = 0};
    if (remaining >= sizeof(wrap))
    {
      if (kj_pwrite_nosync(j->fd, &wrap, sizeof(wrap), (off_t)(j->data_off + j->write_off)) != 0)
        return -EIO;
    }
    j->write_off = 0;
  }
  if ((do_fsync ? kj_pwrite_fsync : kj_pwrite_nosync)(j->fd, data, len,
                                                      (off_t)(j->data_off + j->write_off)) != 0)
    return -EIO;
  j->write_off += len;
  // persist header with updated write offset and seq
  kj_persist_header(j, do_fsync);
  return 0;
}

static int kj_write_record(kafs_journal_t *j, uint32_t tag, uint64_t seq, const char *payload)
{
  kj_rec_hdr_t rh = {.tag = tag, .size = 0, .seq = seq, .crc32 = 0};
  size_t psize = 0;
  if (payload && *payload)
    psize = strlen(payload);
  rh.size = (uint32_t)psize;
  // write header then payload atomically w.r.t. ring wrap logic
  // Simple approach: allocate contiguous buffer and write once via kj_ring_write.
  size_t total = sizeof(rh) + psize;
  char *buf = (char *)malloc(total);
  if (!buf)
    return -ENOMEM;
  memcpy(buf, &rh, sizeof(rh));
  if (psize)
    memcpy(buf + sizeof(rh), payload, psize);
  // compute CRC over header(with crc32=0) + payload, then write back to header.crc32
  uint32_t c = kj_crc32(buf, total);
  ((kj_rec_hdr_t *)buf)->crc32 = c;
  int rc = kj_ring_write(j, buf, total, 0);
  free(buf);
  return rc;
}

static void j_build_payload(char *dst, size_t cap, const char *op, const char *fmt, va_list ap)
{
  size_t n = 0;
  if (op && *op)
    n = (size_t)snprintf(dst, cap, "op=%s", op);
  if (fmt && *fmt && n < cap)
  {
    if (n > 0 && dst[n - 1] != ' ')
    {
      dst[n++] = ' ';
    }
    vsnprintf(dst + n, cap - n, fmt, ap);
  }
}

static void kj_write_inimage_payload(kafs_journal_t *j, uint32_t tag, uint64_t seq, const char *op,
                                     const char *fmt, va_list ap)
{
  char payload[512];
  j_build_payload(payload, sizeof(payload), op, fmt, ap);
  (void)kj_write_record(j, tag, seq, payload);
}

static void kj_state_disable(struct kafs_context *ctx)
{
  g_state.ctx = ctx;
  g_state.j.enabled = 0;
  g_state.j.fd = -1;
  g_state.j.seq = 0;
  g_state.j.mtx = NULL;
  g_state.j.use_inimage = 0;
  g_state.j.base_off = 0;
  g_state.j.data_off = 0;
  g_state.j.base_ptr = NULL;
  g_state.j.area_size = 0;
  g_state.j.gc_delay_ns = 0;
  g_state.j.gc_last_ns = 0;
  g_state.j.gc_pending = 0;
}

int kafs_journal_init(struct kafs_context *ctx, const char *image_path)
{
  const char *env = getenv("KAFS_JOURNAL");
  if (env && strcmp(env, "0") == 0)
  {
    kj_state_disable(ctx);
    return 0;
  }
  // Prefer in-image journal if present in superblock and env != explicit path
  uint64_t joff = kafs_sb_journal_offset_get(ctx->c_superblock);
  uint64_t jsize = kafs_sb_journal_size_get(ctx->c_superblock);
  // Enable by default when in-image is available (env unset or "1").
  int use_inimg = (joff != 0 && jsize >= 4096);
  // prepare mutex
#if KAFS_JOURNAL_HAS_PTHREAD
  pthread_mutex_t *mtx_ptr = malloc(sizeof(pthread_mutex_t));
  if (mtx_ptr)
    pthread_mutex_init(mtx_ptr, NULL);
#else
  void *mtx_ptr = NULL;
#endif
  if (use_inimg)
  {
    g_state.ctx = ctx;
    g_state.j.enabled = 1;
    g_state.j.fd = ctx->c_fd; // same file descriptor
    g_state.j.seq = 0;
    g_state.j.mtx = mtx_ptr;
    g_state.j.use_inimage = 1;
    g_state.j.base_off = joff;
    size_t hsz = kj_header_size();
    g_state.j.data_off = joff + hsz;
    g_state.j.area_size = jsize > hsz ? (jsize - hsz) : 0;
    g_state.j.base_ptr = (char *)ctx->c_superblock + joff;
    // group commit window (ns) from env KAFS_JOURNAL_GC_NS; default 10000000ns (10ms)
    const char *gc = getenv("KAFS_JOURNAL_GC_NS");
    g_state.j.gc_delay_ns = gc ? strtoull(gc, NULL, 0) : 10000000ull;
    g_state.j.gc_last_ns = 0;
    g_state.j.gc_pending = 0;
    // initialize or load header
    (void)kj_init_or_load(&g_state.j);
    return 0;
  }
  // 外部サイドカーは廃止。ジャーナル無効で起動。
  kj_state_disable(ctx);
  return 0;
}

void kafs_journal_shutdown(struct kafs_context *ctx)
{
  kj_apply_meta_delta(ctx);
  if (g_state.ctx != ctx)
    return;
  kafs_journal_t *j = &g_state.j;
  if (j->fd >= 0)
  {
    if (j->use_inimage && j->enabled)
    {
      // flush pending group commit batch, if any
      jlock(j);
      if (j->gc_pending)
      {
        kj_persist_header(j, 1);
        j->gc_pending = 0;
      }
      junlock(j);
    }
    if (!j->use_inimage)
    {
      struct timespec ts;
      timespec_now(&ts);
      dprintf(j->fd, "# kafs journal end %ld.%09ld\n", (long)ts.tv_sec, ts.tv_nsec);
      fsync(j->fd);
      close(j->fd);
    }
    // in-image: header/state is persisted by writes; underlying fd is owned by ctx
  }
#if KAFS_JOURNAL_HAS_PTHREAD
  if (j->mtx)
  {
    pthread_mutex_destroy((pthread_mutex_t *)j->mtx);
    free(j->mtx);
  }
#endif
  memset(&g_state, 0, sizeof(g_state));
}

int kafs_journal_is_enabled(struct kafs_context *ctx)
{
  return (g_state.ctx == ctx && g_state.j.enabled) ? 1 : 0;
}

void kafs_journal_force_flush(struct kafs_context *ctx)
{
  if (!ctx)
    return;

  kj_apply_meta_delta(ctx);

  if (g_state.ctx != ctx || !g_state.j.enabled)
    return;

  kafs_journal_t *j = &g_state.j;
  jlock(j);
  if (j->use_inimage)
  {
    kj_persist_header(j, 1);
    j->gc_pending = 0;
  }
  else if (j->fd >= 0)
  {
    fsync(j->fd);
  }
  junlock(j);
}

uint64_t kafs_journal_begin(struct kafs_context *ctx, const char *op, const char *fmt, ...)
{
  if (g_state.ctx != ctx || !g_state.j.enabled)
    return 0;
  kafs_journal_t *j = &g_state.j;
  jlock(j);
  uint64_t id = ++j->seq;
  if (j->use_inimage)
  {
    va_list ap;
    va_start(ap, fmt);
    kj_write_inimage_payload(j, KJ_TAG_BEG, id, op, fmt, ap);
    va_end(ap);
  }
  else
  {
    va_list ap;
    va_start(ap, fmt);
    kj_write_sidecar_tsline(j, KJ_SIDECAR_BEGIN, id, op, fmt, ap);
    va_end(ap);
  }
  junlock(j);
  return id;
}

void kafs_journal_commit(struct kafs_context *ctx, uint64_t seq)
{
  if (g_state.ctx != ctx || !g_state.j.enabled || seq == 0)
    return;

  kj_meta_snapshot_t meta = {0};
  (void)kj_collect_meta_snapshot(ctx, &meta);

  kj_apply_meta_delta(ctx);

  kafs_journal_t *j = &g_state.j;
  jlock(j);
  if (j->use_inimage)
  {
    if (meta.valid)
      (void)kj_write_meta_delta_record(j, seq, &meta);

    // COMMITは書き込み自体は非同期。グループコミット窓の最後に1回だけfsync+ヘッダ更新。
    kj_rec_hdr_t rh = {.tag = KJ_TAG_CMT, .size = 0, .seq = seq, .crc32 = 0};
    rh.crc32 = 0;
    rh.crc32 = kj_crc32(&rh, sizeof(rh));
    (void)kj_ring_write(j, &rh, sizeof(rh), 0);

    uint64_t delay = j->gc_delay_ns;
    if (delay == 0)
    {
      // すぐに耐久化
      kj_persist_header(j, 1);
      junlock(j);
      kj_meta_snapshot_clear(&meta);
      return;
    }

    // リーダースレッド選出: 最初のCOMMITのみ待機してバッチフラッシュを担当
    if (!j->gc_pending)
    {
      j->gc_pending = 1;
      j->gc_last_ns = nsec_now_mono();
      // unlockして窓が閉じるまで待機
      uint64_t start = j->gc_last_ns;
      junlock(j);
      uint64_t now = nsec_now_mono();
      uint64_t elapsed = now - start;
      if (elapsed < delay)
        sleep_ns(delay - elapsed);
      // 再ロックしてフラッシュ（まだ自分が担当なら）
      jlock(j);
      if (j->gc_pending && (nsec_now_mono() - j->gc_last_ns) >= delay)
      {
        kj_persist_header(j, 1);
        j->gc_pending = 0;
      }
      junlock(j);
      kj_meta_snapshot_clear(&meta);
      return;
    }
    // 非リーダー: すでにバッチ中。フラッシュはリーダーに任せる。
    junlock(j);
    kj_meta_snapshot_clear(&meta);
    return;
  }
  else
  {
    struct timespec ts;
    timespec_now(&ts);
    dprintf(j->fd, "COMMIT %llu %ld.%09ld\n", (unsigned long long)seq, (long)ts.tv_sec, ts.tv_nsec);
    fsync(j->fd);
  }
  junlock(j);
  kj_meta_snapshot_clear(&meta);
}

void kafs_journal_abort(struct kafs_context *ctx, uint64_t seq, const char *reason_fmt, ...)
{
  if (g_state.ctx != ctx || !g_state.j.enabled || seq == 0)
    return;
  kafs_journal_t *j = &g_state.j;
  jlock(j);
  if (j->use_inimage)
  {
    char payload[256] = {0};
    if (reason_fmt && *reason_fmt)
    {
      va_list ap;
      va_start(ap, reason_fmt);
      vsnprintf(payload, sizeof(payload), reason_fmt, ap);
      va_end(ap);
    }
    (void)kj_write_record(j, KJ_TAG_ABR, seq, payload[0] ? payload : NULL);
  }
  else
  {
    va_list ap;
    va_start(ap, reason_fmt);
    kj_write_sidecar_tsline(j, KJ_SIDECAR_ABORT, seq, NULL, reason_fmt, ap);
    va_end(ap);
  }
  junlock(j);
}

void kafs_journal_note(struct kafs_context *ctx, const char *op, const char *fmt, ...)
{
  if (g_state.ctx != ctx || !g_state.j.enabled)
    return;
  kafs_journal_t *j = &g_state.j;
  jlock(j);
  if (j->use_inimage)
  {
    va_list ap;
    va_start(ap, fmt);
    kj_write_inimage_payload(j, KJ_TAG_NOTE, 0, op, fmt, ap);
    va_end(ap);
  }
  else
  {
    va_list ap;
    va_start(ap, fmt);
    kj_write_sidecar_tsline(j, KJ_SIDECAR_NOTE, 0, op, fmt, ap);
    va_end(ap);
  }
  junlock(j);
}

// ----------------------
// Replay
// ----------------------
int kafs_journal_replay(struct kafs_context *ctx, kafs_journal_replay_cb cb, void *user)
{
  (void)user;
  // Only in-image journal is replayable
  uint64_t joff = kafs_sb_journal_offset_get(ctx->c_superblock);
  uint64_t jsize = kafs_sb_journal_size_get(ctx->c_superblock);
  if (joff == 0 || jsize < 4096)
    return 0;
  kafs_journal_t j = {0};
  j.enabled = 1;
  j.fd = ctx->c_fd;
  j.use_inimage = 1;
  j.base_off = joff;
  size_t hsz = kj_header_size();
  j.data_off = joff + hsz;
  j.area_size = jsize > hsz ? (jsize - hsz) : 0;
  if (j.area_size == 0)
    return 0;
  if (kj_init_or_load(&j) != 0)
    return -EIO;

  // scan records from start to write_off
  uint64_t pos = 0;
  // track open transactions: limited small map for simplicity
  enum
  {
    MAX_OPEN = 256
  };
  struct
  {
    uint64_t seq;
    char *payload;
    char *meta_payload;
  } open[MAX_OPEN];
  size_t open_cnt = 0;
  while (pos + sizeof(kj_rec_hdr_t) <= j.write_off)
  {
    kj_rec_hdr_t rh;
    if (kj_pread(j.fd, &rh, sizeof(rh), (off_t)(j.data_off + pos)) != 0)
      break;
    pos += sizeof(rh);
    if (rh.tag == KJ_TAG_WRAP)
    {
      pos = 0;
      continue;
    }
    if (pos + rh.size > j.write_off)
      break; // partial tail
    char *pl = NULL;
    if (rh.size)
    {
      pl = (char *)malloc(rh.size + 1);
      if (!pl)
        break;
      if (kj_pread(j.fd, pl, rh.size, (off_t)(j.data_off + pos)) != 0)
      {
        free(pl);
        break;
      }
      pl[rh.size] = '\0';
    }
    pos += rh.size;
    // CRC検証（WRAPは除外）
    if (rh.tag != KJ_TAG_WRAP)
    {
      size_t total = sizeof(rh) + rh.size;
      char *buf = (char *)malloc(total);
      if (!buf)
      {
        if (pl)
          free(pl);
        break;
      }
      kj_rec_hdr_t rh2 = rh;
      rh2.crc32 = 0;
      memcpy(buf, &rh2, sizeof(rh2));
      if (rh.size && pl)
        memcpy(buf + sizeof(rh2), pl, rh.size);
      uint32_t c = kj_crc32(buf, total);
      free(buf);
      if (c != rh.crc32)
      {
        if (pl)
          free(pl);
        break;
      }
    }
    switch (rh.tag)
    {
    case KJ_TAG_BEG:
    {
      if (open_cnt < MAX_OPEN)
      {
        open[open_cnt].seq = rh.seq;
        open[open_cnt].payload = pl;
        open[open_cnt].meta_payload = NULL;
        open_cnt++;
        pl = NULL;
      }
      break;
    }
    case KJ_TAG_MDT:
    {
      for (size_t i = 0; i < open_cnt; ++i)
      {
        if (open[i].seq != rh.seq)
          continue;
        free(open[i].meta_payload);
        open[i].meta_payload = pl;
        pl = NULL;
        break;
      }
      break;
    }
    case KJ_TAG_CMT:
    {
      // find matching begin
      for (size_t i = 0; i < open_cnt; ++i)
      {
        if (open[i].seq == rh.seq)
        {
          if (open[i].meta_payload)
            kj_replay_apply_meta_delta(ctx, open[i].meta_payload);
          if (cb && open[i].payload)
          {
            // payload is "op=... <args>"
            const char *payload = open[i].payload;
            const char *opkv = strstr(payload, "op=");
            const char *sp = opkv ? strchr(opkv, '=') : NULL;
            const char *op = sp ? (sp + 1) : "";
            const char *args = op ? strchr(op, ' ') : NULL;
            size_t oplen = args ? (size_t)(args - op) : strlen(op);
            char opbuf[64];
            if (oplen >= sizeof(opbuf))
              oplen = sizeof(opbuf) - 1;
            memcpy(opbuf, op, oplen);
            opbuf[oplen] = '\0';
            const char *argstr = args ? (args + 1) : "";
            (void)cb(ctx, opbuf, argstr, user);
          }
          free(open[i].payload);
          free(open[i].meta_payload);
          // compact remove
          open[i] = open[open_cnt - 1];
          open_cnt--;
          break;
        }
      }
      break;
    }
    case KJ_TAG_ABR:
    {
      // drop open txn if present
      for (size_t i = 0; i < open_cnt; ++i)
        if (open[i].seq == rh.seq)
        {
          free(open[i].payload);
          free(open[i].meta_payload);
          open[i] = open[open_cnt - 1];
          open_cnt--;
          break;
        }
      break;
    }
    case KJ_TAG_NOTE:
    default:
      break;
    }
    if (pl)
      free(pl);
  }

  // cleanup: reset ring for fresh start
  j.write_off = 0;
  j.seq = j.seq; // keep seq as loaded
  kj_reset_area(&j);
  kj_header_t hdr;
  if (kj_header_load(&j, &hdr) == 0)
  {
    hdr.write_off = 0;
    hdr.header_crc = 0;
    hdr.header_crc = kj_crc32(&hdr, sizeof(hdr));
    kj_header_store(&j, &hdr);
  }
  if (g_state.ctx == ctx && g_state.j.enabled && g_state.j.use_inimage &&
      g_state.j.base_off == j.base_off)
    g_state.j.write_off = 0;
  for (size_t i = 0; i < open_cnt; ++i)
  {
    free(open[i].payload);
    free(open[i].meta_payload);
  }
  return 0;
}
