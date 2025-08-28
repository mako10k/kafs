#include "kafs_journal.h"
#include "kafs_context.h"
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
#  if __has_include(<pthread.h>)
#    include <pthread.h>
#    define KAFS_JOURNAL_HAS_PTHREAD 1
#  else
#    define KAFS_JOURNAL_HAS_PTHREAD 0
#  endif
#else
#  define KAFS_JOURNAL_HAS_PTHREAD 1
#  include <pthread.h>
#endif

// Extend context with journal pointer via forward declaration; we avoid header coupling.
typedef struct kafs_context_ext {
  // mirror initial layout of kafs_context to keep ABI consistent within TU
  void *c_superblock;
  void *c_inotbl;
  void *c_blkmasktbl;
  unsigned int c_ino_search;
  unsigned int c_blo_search;
  int c_fd;
  void *c_hrl_index;
  unsigned int c_hrl_bucket_cnt;
  void *c_lock_hrl_buckets; 
  void *c_lock_hrl_global;  
  void *c_lock_bitmap;      
  void *c_lock_inode;       
  // not in header: attach journal pointer at end through separate symbol
} kafs_context_ext_t;

// We'll store journal instance in a static map keyed by ctx pointer (simple single-instance)
typedef struct journal_state {
  struct kafs_context *ctx;
  kafs_journal_t j;
} journal_state_t;

static journal_state_t g_state = {0};

static void timespec_now(struct timespec *ts) {
  clock_gettime(CLOCK_REALTIME, ts);
}

static int jlock(kafs_journal_t *j) {
#if KAFS_JOURNAL_HAS_PTHREAD
  if (!j->mtx) return 0;
  return pthread_mutex_lock((pthread_mutex_t *)j->mtx);
#else
  (void)j; return 0;
#endif
}
static int junlock(kafs_journal_t *j) {
#if KAFS_JOURNAL_HAS_PTHREAD
  if (!j->mtx) return 0;
  return pthread_mutex_unlock((pthread_mutex_t *)j->mtx);
#else
  (void)j; return 0;
#endif
}

static void jwritef(kafs_journal_t *j, const char *line) {
  if (j->fd < 0) return;
  size_t len = strlen(line);
  (void)write(j->fd, line, len);
  (void)write(j->fd, "\n", 1);
}

static void jprintf(kafs_journal_t *j, const char *fmt, ...) {
  if (j->fd < 0) return;
  char buf[1024];
  va_list ap; va_start(ap, fmt);
  vsnprintf(buf, sizeof(buf), fmt, ap);
  va_end(ap);
  jwritef(j, buf);
}

// ----------------------
// In-image ring journal
// ----------------------
// Journal header persisted within image
#define KJ_MAGIC 0x4b414a4c /* 'KAJL' */
#define KJ_VER   1
typedef struct kj_header {
  uint32_t magic;      // KJ_MAGIC
  uint16_t version;    // KJ_VER
  uint16_t flags;      // future
  uint64_t area_size;  // ring capacity in bytes
  uint64_t write_off;  // current write offset within ring [0..area_size)
  uint64_t seq;        // last used sequence id
  uint64_t reserved0;
} __attribute__((packed)) kj_header_t;

// Record tags
#define KJ_TAG_BEG 0x42454731u /* 'BEG1' */
#define KJ_TAG_CMT 0x434d5431u /* 'CMT1' */
#define KJ_TAG_ABR 0x41425231u /* 'ABR1' */
#define KJ_TAG_NOTE 0x4e4f5431u /* 'NOT1' */
#define KJ_TAG_WRAP 0x57524150u /* 'WRAP' */

typedef struct kj_rec_hdr {
  uint32_t tag;     // KJ_TAG_*
  uint32_t size;    // payload size in bytes (may be 0)
  uint64_t seq;     // sequence id (0 for NOTE)
} __attribute__((packed)) kj_rec_hdr_t;

static size_t kj_header_size(void) {
  // align header to 64 bytes for future growth
  size_t s = sizeof(kj_header_t);
  if (s % 64) s += 64 - (s % 64);
  return s;
}

static int kj_pread(int fd, void *buf, size_t sz, off_t off) {
  ssize_t r = pread(fd, buf, sz, off);
  return (r == (ssize_t)sz) ? 0 : -EIO;
}
static int kj_pwrite_fsync(int fd, const void *buf, size_t sz, off_t off) {
  ssize_t w = pwrite(fd, buf, sz, off);
  if (w != (ssize_t)sz) return -EIO;
  fsync(fd);
  return 0;
}

static int kj_header_load(kafs_journal_t *j, kj_header_t *hdr) {
  if (!j->use_inimage) return -EINVAL;
  return kj_pread(j->fd, hdr, sizeof(*hdr), (off_t)j->base_off);
}
static int kj_header_store(kafs_journal_t *j, const kj_header_t *hdr) {
  if (!j->use_inimage) return -EINVAL;
  return kj_pwrite_fsync(j->fd, hdr, sizeof(*hdr), (off_t)j->base_off);
}

static void kj_reset_area(kafs_journal_t *j) {
  // Not strictly necessary to zero, but keeps tests deterministic
  const size_t chunk = 4096;
  char z[chunk]; memset(z, 0, sizeof(z));
  uint64_t remaining = j->area_size;
  uint64_t off = 0;
  while (remaining) {
    size_t n = remaining > chunk ? chunk : (size_t)remaining;
    (void)kj_pwrite_fsync(j->fd, z, n, (off_t)(j->data_off + off));
    off += n; remaining -= n;
  }
}

static int kj_init_or_load(kafs_journal_t *j) {
  kj_header_t hdr;
  if (kj_header_load(j, &hdr) == 0 && hdr.magic == KJ_MAGIC && hdr.version == KJ_VER &&
      hdr.area_size == j->area_size) {
    j->write_off = hdr.write_off < j->area_size ? hdr.write_off : 0;
    j->seq = hdr.seq;
    return 0;
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
  };
  return kj_header_store(j, &nh);
}

static int kj_ring_write(kafs_journal_t *j, const void *data, size_t len) {
  if (!j->use_inimage) return -EINVAL;
  uint64_t remaining = j->area_size - j->write_off;
  if ((uint64_t)len > remaining) {
    // write WRAP marker if space allows
    kj_rec_hdr_t wrap = { .tag = KJ_TAG_WRAP, .size = 0, .seq = 0 };
    if (remaining >= sizeof(wrap)) {
      if (kj_pwrite_fsync(j->fd, &wrap, sizeof(wrap), (off_t)(j->data_off + j->write_off)) != 0)
        return -EIO;
    }
    j->write_off = 0;
  }
  if (kj_pwrite_fsync(j->fd, data, len, (off_t)(j->data_off + j->write_off)) != 0)
    return -EIO;
  j->write_off += len;
  // persist header with updated write offset and seq
  kj_header_t hdr;
  if (kj_header_load(j, &hdr) == 0) {
    hdr.write_off = j->write_off;
    hdr.seq = j->seq;
    (void)kj_header_store(j, &hdr);
  }
  return 0;
}

static int kj_write_record(kafs_journal_t *j, uint32_t tag, uint64_t seq, const char *payload) {
  kj_rec_hdr_t rh = { .tag = tag, .size = 0, .seq = seq };
  size_t psize = 0;
  if (payload && *payload) psize = strlen(payload);
  rh.size = (uint32_t)psize;
  // write header then payload atomically w.r.t. ring wrap logic
  // Simple approach: allocate contiguous buffer and write once via kj_ring_write.
  size_t total = sizeof(rh) + psize;
  char *buf = (char *)malloc(total);
  if (!buf) return -ENOMEM;
  memcpy(buf, &rh, sizeof(rh));
  if (psize) memcpy(buf + sizeof(rh), payload, psize);
  int rc = kj_ring_write(j, buf, total);
  free(buf);
  return rc;
}

static void j_build_payload(char *dst, size_t cap, const char *op, const char *fmt, va_list ap) {
  size_t n = 0;
  if (op && *op) n = (size_t)snprintf(dst, cap, "op=%s", op);
  if (fmt && *fmt && n < cap) {
    if (n > 0 && dst[n-1] != ' ') { dst[n++] = ' '; }
    vsnprintf(dst + n, cap - n, fmt, ap);
  }
}

int kafs_journal_init(struct kafs_context *ctx, const char *image_path) {
  const char *env = getenv("KAFS_JOURNAL");
  if (env && strcmp(env, "0") == 0) {
    g_state.ctx = ctx;
    g_state.j.enabled = 0;
    g_state.j.fd = -1;
    g_state.j.seq = 0;
    g_state.j.mtx = NULL;
    return 0;
  }
  // Prefer in-image journal if present in superblock and env != explicit path
  uint64_t joff = kafs_sb_journal_offset_get(ctx->c_superblock);
  uint64_t jsize = kafs_sb_journal_size_get(ctx->c_superblock);
  // Enable by default when in-image is available (env unset or "1").
  int use_inimg = (joff != 0 && jsize >= 4096 && (!env || strcmp(env, "1") == 0));
  // prepare mutex
#if KAFS_JOURNAL_HAS_PTHREAD
  pthread_mutex_t *mtx_ptr = malloc(sizeof(pthread_mutex_t));
  if (mtx_ptr) pthread_mutex_init(mtx_ptr, NULL);
#else
  void *mtx_ptr = NULL;
#endif
  if (use_inimg) {
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
    // initialize or load header
    (void)kj_init_or_load(&g_state.j);
    return 0;
  }
  // else: external sidecar
  char pathbuf[1024];
  const char *jpath = env;
  if (strcmp(env, "1") == 0) {
    // default sidecar: <image>.journal
    snprintf(pathbuf, sizeof(pathbuf), "%s.journal", image_path ? image_path : "kafs");
    jpath = pathbuf;
  }
  int fd = open(jpath, O_CREAT | O_APPEND | O_WRONLY, 0644);
  if (fd < 0) {
    perror("open journal");
    // don't fail filesystem; run without journal
    g_state.ctx = ctx;
    g_state.j.enabled = 0;
    g_state.j.fd = -1;
    g_state.j.seq = 0;
    g_state.j.mtx = NULL;
    g_state.j.use_inimage = 0;
    g_state.j.base_off = 0;
    g_state.j.base_ptr = NULL;
    g_state.j.area_size = 0;
    return 0;
  }
#if KAFS_JOURNAL_HAS_PTHREAD
  pthread_mutex_t *m = malloc(sizeof(pthread_mutex_t));
  if (m) pthread_mutex_init(m, NULL);
#else
  void *m = NULL;
#endif
  g_state.ctx = ctx;
  g_state.j.enabled = 1;
  g_state.j.fd = fd;
  g_state.j.seq = 0;
  g_state.j.mtx = mtx_ptr;
  g_state.j.use_inimage = 0;
  g_state.j.base_off = 0;
  g_state.j.data_off = 0;
  g_state.j.base_ptr = NULL;
  g_state.j.area_size = 0;
  struct timespec ts; timespec_now(&ts);
  dprintf(fd, "# kafs journal start %ld.%09ld\n", (long)ts.tv_sec, ts.tv_nsec);
  fsync(fd);
  return 0;
}

void kafs_journal_shutdown(struct kafs_context *ctx) {
  (void)ctx;
  if (g_state.ctx != ctx) return;
  kafs_journal_t *j = &g_state.j;
  if (j->fd >= 0) {
    if (!j->use_inimage) {
      struct timespec ts; timespec_now(&ts);
      dprintf(j->fd, "# kafs journal end %ld.%09ld\n", (long)ts.tv_sec, ts.tv_nsec);
      fsync(j->fd);
      close(j->fd);
    }
    // in-image: header/state is persisted by writes; underlying fd is owned by ctx
  }
#if KAFS_JOURNAL_HAS_PTHREAD
  if (j->mtx) { pthread_mutex_destroy((pthread_mutex_t *)j->mtx); free(j->mtx); }
#endif
  memset(&g_state, 0, sizeof(g_state));
}

uint64_t kafs_journal_begin(struct kafs_context *ctx, const char *op, const char *fmt, ...) {
  if (g_state.ctx != ctx || !g_state.j.enabled) return 0;
  kafs_journal_t *j = &g_state.j;
  jlock(j);
  uint64_t id = ++j->seq;
  if (j->use_inimage) {
    char payload[512];
    va_list ap; va_start(ap, fmt);
    j_build_payload(payload, sizeof(payload), op, fmt, ap);
    va_end(ap);
    (void)kj_write_record(j, KJ_TAG_BEG, id, payload);
  } else {
    struct timespec ts; timespec_now(&ts);
    dprintf(j->fd, "BEGIN %llu %s %ld.%09ld ", (unsigned long long)id, op, (long)ts.tv_sec, ts.tv_nsec);
    if (fmt && *fmt) {
      va_list ap; va_start(ap, fmt);
      vdprintf(j->fd, fmt, ap);
      va_end(ap);
    }
    dprintf(j->fd, "\n");
    fsync(j->fd);
  }
  junlock(j);
  return id;
}

void kafs_journal_commit(struct kafs_context *ctx, uint64_t seq) {
  if (g_state.ctx != ctx || !g_state.j.enabled || seq == 0) return;
  kafs_journal_t *j = &g_state.j;
  jlock(j);
  if (j->use_inimage) {
    (void)kj_write_record(j, KJ_TAG_CMT, seq, NULL);
  } else {
    struct timespec ts; timespec_now(&ts);
    dprintf(j->fd, "COMMIT %llu %ld.%09ld\n", (unsigned long long)seq, (long)ts.tv_sec, ts.tv_nsec);
    fsync(j->fd);
  }
  junlock(j);
}

void kafs_journal_abort(struct kafs_context *ctx, uint64_t seq, const char *reason_fmt, ...) {
  if (g_state.ctx != ctx || !g_state.j.enabled || seq == 0) return;
  kafs_journal_t *j = &g_state.j;
  jlock(j);
  if (j->use_inimage) {
    char payload[256] = {0};
    if (reason_fmt && *reason_fmt) {
      va_list ap; va_start(ap, reason_fmt);
      vsnprintf(payload, sizeof(payload), reason_fmt, ap);
      va_end(ap);
    }
    (void)kj_write_record(j, KJ_TAG_ABR, seq, payload[0] ? payload : NULL);
  } else {
    struct timespec ts; timespec_now(&ts);
    dprintf(j->fd, "ABORT %llu %ld.%09ld ", (unsigned long long)seq, (long)ts.tv_sec, ts.tv_nsec);
    if (reason_fmt && *reason_fmt) {
      va_list ap; va_start(ap, reason_fmt);
      vdprintf(j->fd, reason_fmt, ap);
      va_end(ap);
    }
    dprintf(j->fd, "\n");
    fsync(j->fd);
  }
  junlock(j);
}

void kafs_journal_note(struct kafs_context *ctx, const char *op, const char *fmt, ...) {
  if (g_state.ctx != ctx || !g_state.j.enabled) return;
  kafs_journal_t *j = &g_state.j;
  jlock(j);
  if (j->use_inimage) {
    char payload[512];
    va_list ap; va_start(ap, fmt);
    j_build_payload(payload, sizeof(payload), op, fmt, ap);
    va_end(ap);
    (void)kj_write_record(j, KJ_TAG_NOTE, 0, payload);
  } else {
    struct timespec ts; timespec_now(&ts);
    dprintf(j->fd, "NOTE %s %ld.%09ld ", op, (long)ts.tv_sec, ts.tv_nsec);
    if (fmt && *fmt) {
      va_list ap; va_start(ap, fmt);
      vdprintf(j->fd, fmt, ap);
      va_end(ap);
    }
    dprintf(j->fd, "\n");
    fsync(j->fd);
  }
  junlock(j);
}

// ----------------------
// Replay
// ----------------------
int kafs_journal_replay(struct kafs_context *ctx, kafs_journal_replay_cb cb, void *user) {
  (void)user;
  // Only in-image journal is replayable
  uint64_t joff = kafs_sb_journal_offset_get(ctx->c_superblock);
  uint64_t jsize = kafs_sb_journal_size_get(ctx->c_superblock);
  if (joff == 0 || jsize < 4096) return 0;
  kafs_journal_t j = {0};
  j.enabled = 1;
  j.fd = ctx->c_fd;
  j.use_inimage = 1;
  j.base_off = joff;
  size_t hsz = kj_header_size();
  j.data_off = joff + hsz;
  j.area_size = jsize > hsz ? (jsize - hsz) : 0;
  if (j.area_size == 0) return 0;
  if (kj_init_or_load(&j) != 0) return -EIO;

  // scan records from start to write_off
  uint64_t pos = 0;
  // track open transactions: limited small map for simplicity
  enum { MAX_OPEN = 256 };
  struct { uint64_t seq; char *payload; } open[MAX_OPEN];
  size_t open_cnt = 0;
  while (pos + sizeof(kj_rec_hdr_t) <= j.write_off) {
    kj_rec_hdr_t rh;
    if (kj_pread(j.fd, &rh, sizeof(rh), (off_t)(j.data_off + pos)) != 0) break;
    pos += sizeof(rh);
    if (rh.tag == KJ_TAG_WRAP) { pos = 0; continue; }
    if (pos + rh.size > j.write_off) break; // partial tail
    char *pl = NULL;
    if (rh.size) {
      pl = (char *)malloc(rh.size + 1);
      if (!pl) break;
      if (kj_pread(j.fd, pl, rh.size, (off_t)(j.data_off + pos)) != 0) { free(pl); break; }
      pl[rh.size] = '\0';
    }
    pos += rh.size;
    switch (rh.tag) {
      case KJ_TAG_BEG: {
        if (open_cnt < MAX_OPEN) { open[open_cnt].seq = rh.seq; open[open_cnt].payload = pl; open_cnt++; pl = NULL; }
        break;
      }
      case KJ_TAG_CMT: {
        // find matching begin
        for (size_t i = 0; i < open_cnt; ++i) {
          if (open[i].seq == rh.seq) {
            if (cb && open[i].payload) {
              // payload is "op=... <args>"
              const char *payload = open[i].payload;
              const char *opkv = strstr(payload, "op=");
              const char *sp = opkv ? strchr(opkv, '=') : NULL;
              const char *op = sp ? (sp + 1) : "";
              const char *args = op ? strchr(op, ' ') : NULL;
              size_t oplen = args ? (size_t)(args - op) : strlen(op);
              char opbuf[64];
              if (oplen >= sizeof(opbuf)) oplen = sizeof(opbuf) - 1;
              memcpy(opbuf, op, oplen); opbuf[oplen] = '\0';
              const char *argstr = args ? (args + 1) : "";
              (void)cb(ctx, opbuf, argstr, user);
            }
            free(open[i].payload);
            // compact remove
            open[i] = open[open_cnt - 1];
            open_cnt--;
            break;
          }
        }
        break;
      }
      case KJ_TAG_ABR: {
        // drop open txn if present
        for (size_t i = 0; i < open_cnt; ++i) if (open[i].seq == rh.seq) { free(open[i].payload); open[i] = open[open_cnt-1]; open_cnt--; break; }
        break;
      }
      case KJ_TAG_NOTE: default:
        break;
    }
    if (pl) free(pl);
  }

  // cleanup: reset ring for fresh start
  j.write_off = 0; j.seq = j.seq; // keep seq as loaded
  kj_reset_area(&j);
  kj_header_t hdr; if (kj_header_load(&j, &hdr) == 0) { hdr.write_off = 0; kj_header_store(&j, &hdr); }
  for (size_t i = 0; i < open_cnt; ++i) free(open[i].payload);
  return 0;
}
