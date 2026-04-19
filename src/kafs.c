#include "kafs.h"
#include "kafs_context.h"
#include "kafs_superblock.h"
#include "kafs_block.h"
#include "kafs_inode.h"
#include "kafs_dirent.h"
#include "kafs_hash.h"
#include "kafs_journal.h"
#include "kafs_cli_opts.h"
#include "kafs_ioctl.h"
#include "kafs_mmap_io.h"
#include "kafs_rpc.h"
#include "kafs_core.h"
#include "kafs_crash_diag.h"
#include "kafs_tailmeta.h"

#include <fuse.h>
#include <fuse_log.h>
#include <errno.h>
#include <string.h>
#include <unistd.h>
#include <assert.h>
#include <endian.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/mman.h>
#include <sys/statvfs.h>
#include <sys/resource.h>

#include <sys/time.h>
#include <sys/un.h>
#include <stdlib.h>
#include <stddef.h>
#include <signal.h>
#include <limits.h>
#include <poll.h>
#include <sched.h>
#include <time.h>
#ifdef __linux__
#include <execinfo.h>
#include <linux/fs.h>
#include <sys/syscall.h>
#endif

#ifndef SEEK_DATA
#define SEEK_DATA 3
#endif

#ifndef SEEK_HOLE
#define SEEK_HOLE 4
#endif

#ifdef DEBUG
static void *memset_orig(void *d, int x, size_t l) { return memset(d, x, l); }

#define memset(d, x, l)                                                                            \
  ({                                                                                               \
    void *_d = (d);                                                                                \
    int _x = (x);                                                                                  \
    size_t _l = (l);                                                                               \
    kafs_log(KAFS_LOG_DEBUG, "%s:%d: memset(%p, %d, %zu)\n", __FILE__, __LINE__, _d, _x, _l);      \
    memset_orig(_d, _x, _l);                                                                       \
  })

static void *memcpy_orig(void *d, const void *s, size_t l)
{
  assert(d + l <= s || s + l <= d);
  return memcpy(d, s, l);
}

#define memcpy(d, s, l)                                                                            \
  ({                                                                                               \
    void *_d = (d);                                                                                \
    const void *_s = (s);                                                                          \
    size_t _l = (l);                                                                               \
    kafs_log(KAFS_LOG_DEBUG, "%s:%d: memcpy(%p, %p, %zu)\n", __FILE__, __LINE__, _d, _s, _l);      \
    memcpy_orig(_d, _s, _l);                                                                       \
  })
#endif

static inline uint64_t kafs_now_ns(void)
{
  struct timespec ts;
  clock_gettime(CLOCK_MONOTONIC, &ts);
  return (uint64_t)ts.tv_sec * 1000000000ull + (uint64_t)ts.tv_nsec;
}

static inline uint64_t kafs_now_realtime_ns(void)
{
  struct timespec ts;
  clock_gettime(CLOCK_REALTIME, &ts);
  return (uint64_t)ts.tv_sec * 1000000000ull + (uint64_t)ts.tv_nsec;
}

static inline void kafs_stat_record_pwrite_iblk_write_latency(kafs_context_t *ctx, uint64_t ns)
{
  if (!ctx)
    return;
  const uint32_t cap = (uint32_t)(sizeof(ctx->c_stat_pwrite_iblk_write_samples) /
                                  sizeof(ctx->c_stat_pwrite_iblk_write_samples[0]));
  if (cap == 0)
    return;
  uint64_t seq =
      __atomic_fetch_add(&ctx->c_stat_pwrite_iblk_write_sample_seq, 1u, __ATOMIC_RELAXED);
  uint32_t idx = (uint32_t)(seq % (uint64_t)cap);
  ctx->c_stat_pwrite_iblk_write_samples[idx] = ns;
  uint32_t prev = __atomic_load_n(&ctx->c_stat_pwrite_iblk_write_sample_count, __ATOMIC_RELAXED);
  while (prev < cap)
  {
    if (__atomic_compare_exchange_n(&ctx->c_stat_pwrite_iblk_write_sample_count, &prev, prev + 1u,
                                    1, __ATOMIC_RELAXED, __ATOMIC_RELAXED))
      break;
  }
  ctx->c_stat_pwrite_iblk_write_sample_cap = cap;
}

#define KAFS_DIAG_CREATE_PATH_MAX 160u

#if KAFS_ENABLE_EXTRA_DIAG
static void kafs_diag_appendf(struct kafs_context *ctx, const char *fmt, ...)
{
  if (!ctx || ctx->c_diag_log_fd < 0 || !fmt)
    return;

  char buf[1024];
  va_list ap;
  va_start(ap, fmt);
  int n = vsnprintf(buf, sizeof(buf), fmt, ap);
  va_end(ap);
  if (n <= 0)
    return;

  size_t len = (size_t)n;
  if (len >= sizeof(buf))
    len = sizeof(buf) - 1u;
  size_t off = 0;
  while (off < len)
  {
    ssize_t w = write(ctx->c_diag_log_fd, buf + off, len - off);
    if (w <= 0)
      break;
    off += (size_t)w;
  }
}

static void kafs_diag_log_open(struct kafs_context *ctx, const char *image_path)
{
  if (!ctx || ctx->c_diag_log_fd >= 0 || !kafs_extra_diag_enabled())
    return;

  const char *log_path = getenv("KAFS_DIAG_LOG");
  char auto_path[PATH_MAX];
  if (!log_path || log_path[0] == '\0')
  {
    snprintf(auto_path, sizeof(auto_path), "/tmp/kafs-diag-%ld.log", (long)getpid());
    log_path = auto_path;
  }

  int fd = open(log_path, O_CREAT | O_WRONLY | O_APPEND | O_CLOEXEC, 0644);
  if (fd < 0)
  {
    kafs_log(KAFS_LOG_WARNING, "%s: open failed path=%s errno=%d\n", __func__, log_path, errno);
    return;
  }

  ctx->c_diag_log_fd = fd;
  kafs_diag_appendf(ctx, "diag-log-open pid=%ld image=%s\n", (long)getpid(),
                    image_path ? image_path : "(null)");
  kafs_log(KAFS_LOG_WARNING, "%s: path=%s\n", __func__, log_path);
}

static void kafs_diag_log_close(struct kafs_context *ctx)
{
  if (!ctx || ctx->c_diag_log_fd < 0)
    return;
  close(ctx->c_diag_log_fd);
  ctx->c_diag_log_fd = -1;
}

static __thread const char *g_diag_write_path = NULL;
static __thread uint32_t g_diag_write_ino = KAFS_INO_NONE;

typedef struct kafs_diag_write_scope
{
  const char *prev_path;
  uint32_t prev_ino;
} kafs_diag_write_scope_t;

static kafs_diag_write_scope_t kafs_diag_write_scope_enter(const char *path, uint32_t ino)
{
  kafs_diag_write_scope_t scope = {g_diag_write_path, g_diag_write_ino};
  g_diag_write_path = path;
  g_diag_write_ino = ino;
  return scope;
}

static void kafs_diag_write_scope_leave(kafs_diag_write_scope_t scope)
{
  g_diag_write_path = scope.prev_path;
  g_diag_write_ino = scope.prev_ino;
}

const char *kafs_diag_current_write_path(void) { return g_diag_write_path; }

uint32_t kafs_diag_current_write_ino(void) { return g_diag_write_ino; }

static void kafs_diag_format_sample(const void *buf, size_t len, char *hex_out, size_t hex_cap,
                                    char *ascii_out, size_t ascii_cap)
{
  if (!hex_out || hex_cap == 0 || !ascii_out || ascii_cap == 0)
    return;

  hex_out[0] = '\0';
  ascii_out[0] = '\0';
  if (!buf || len == 0)
    return;

  const unsigned char *bytes = (const unsigned char *)buf;
  size_t sample_len = len;
  if (sample_len > 16u)
    sample_len = 16u;

  size_t hex_off = 0;
  size_t ascii_off = 0;
  for (size_t i = 0; i < sample_len; ++i)
  {
    if (hex_off + 4 >= hex_cap || ascii_off + 2 >= ascii_cap)
      break;
    int hex_written = snprintf(hex_out + hex_off, hex_cap - hex_off, "%s%02x", (i == 0) ? "" : " ",
                               (unsigned)bytes[i]);
    if (hex_written < 0)
      break;
    hex_off += (size_t)hex_written;
    ascii_out[ascii_off++] = (bytes[i] >= 32 && bytes[i] <= 126) ? (char)bytes[i] : '.';
  }
  ascii_out[ascii_off] = '\0';
}

static void kafs_diag_log_dir_iblk_write(const char *phase, struct kafs_context *ctx,
                                         kafs_sinode_t *inoent, kafs_iblkcnt_t iblo,
                                         kafs_blkcnt_t old_ref, kafs_blkcnt_t new_ref,
                                         const void *buf, size_t len)
{
  if (!phase || !ctx || !inoent || iblo != 0 || !kafs_extra_diag_enabled())
    return;

  kafs_mode_t mode = kafs_ino_mode_get(inoent);
  if (!S_ISDIR(mode))
    return;

  char hex_sample[3 * 16 + 1];
  char ascii_sample[16 + 1];
  kafs_diag_format_sample(buf, len, hex_sample, sizeof(hex_sample), ascii_sample,
                          sizeof(ascii_sample));

  kafs_log(KAFS_LOG_WARNING,
           "%s: dir-iblk-write ino=%" PRIuFAST32 " mode=%o size=%" PRIuFAST64 " iblo=%" PRIuFAST32
           " old_ref=%" PRIuFAST32 " new_ref=%" PRIuFAST32 " sample_hex=%s sample_ascii='%s'\n",
           phase, (uint_fast32_t)kafs_ctx_ino_no(ctx, inoent), (unsigned)mode,
           (uint_fast64_t)kafs_ino_size_get(inoent), (uint_fast32_t)iblo, (uint_fast32_t)old_ref,
           (uint_fast32_t)new_ref, hex_sample[0] ? hex_sample : "-",
           ascii_sample[0] ? ascii_sample : "-");
  kafs_diag_appendf(ctx,
                    "%s: dir-iblk-write ino=%" PRIuFAST32 " mode=%o size=%" PRIuFAST64
                    " iblo=%" PRIuFAST32 " old_ref=%" PRIuFAST32 " new_ref=%" PRIuFAST32
                    " src_ino=%" PRIuFAST32 " src_path=%s sample_hex=%s sample_ascii='%s'\n",
                    phase, (uint_fast32_t)kafs_ctx_ino_no(ctx, inoent), (unsigned)mode,
                    (uint_fast64_t)kafs_ino_size_get(inoent), (uint_fast32_t)iblo,
                    (uint_fast32_t)old_ref, (uint_fast32_t)new_ref, (uint_fast32_t)g_diag_write_ino,
                    g_diag_write_path ? g_diag_write_path : "(null)",
                    hex_sample[0] ? hex_sample : "-", ascii_sample[0] ? ascii_sample : "-");
}

static void kafs_diag_log_dir_ref_set(const char *phase, struct kafs_context *ctx,
                                      kafs_sinode_t *inoent, kafs_iblkcnt_t iblo,
                                      kafs_blkcnt_t old_ref, kafs_blkcnt_t new_ref)
{
  kafs_diag_log_dir_iblk_write(phase, ctx, inoent, iblo, old_ref, new_ref, NULL, 0);
}

static void kafs_diag_clear_create_event(struct kafs_context *ctx, kafs_inocnt_t ino)
{
  if (!ctx || !ctx->c_superblock || !ctx->c_diag_create_seq || !ctx->c_diag_create_mode ||
      !ctx->c_diag_create_first_write_seen || !ctx->c_diag_create_paths)
    return;
  if (ino >= kafs_sb_inocnt_get(ctx->c_superblock))
    return;

  ctx->c_diag_create_seq[ino] = 0;
  ctx->c_diag_create_mode[ino] = 0;
  ctx->c_diag_create_first_write_seen[ino] = 0;
  char *slot = ctx->c_diag_create_paths + ((size_t)ino * KAFS_DIAG_CREATE_PATH_MAX);
  slot[0] = '\0';
}

static void kafs_diag_note_create_event(struct kafs_context *ctx, kafs_inocnt_t ino,
                                        const char *path, kafs_mode_t mode)
{
  if (!ctx || !ctx->c_superblock || !ctx->c_diag_create_seq || !ctx->c_diag_create_mode ||
      !ctx->c_diag_create_first_write_seen || !ctx->c_diag_create_paths)
    return;
  if (ino >= kafs_sb_inocnt_get(ctx->c_superblock))
    return;

  uint64_t seq = __atomic_add_fetch(&ctx->c_diag_create_seq_next, 1u, __ATOMIC_RELAXED);
  if (seq == 0)
    seq = __atomic_add_fetch(&ctx->c_diag_create_seq_next, 1u, __ATOMIC_RELAXED);

  ctx->c_diag_create_seq[ino] = seq;
  ctx->c_diag_create_mode[ino] = (uint16_t)mode;
  ctx->c_diag_create_first_write_seen[ino] = 0;

  char *slot = ctx->c_diag_create_paths + ((size_t)ino * KAFS_DIAG_CREATE_PATH_MAX);
  if (path && path[0] != '\0')
    snprintf(slot, KAFS_DIAG_CREATE_PATH_MAX, "%s", path);
  else
    slot[0] = '\0';

  kafs_log(KAFS_LOG_WARNING, "%s: seq=%" PRIuFAST64 " ino=%" PRIuFAST32 " mode=%o path=%s\n",
           __func__, (uint_fast64_t)seq, (uint_fast32_t)ino, (unsigned)mode,
           slot[0] ? slot : "(null)");
  kafs_diag_appendf(ctx, "%s: seq=%" PRIuFAST64 " ino=%" PRIuFAST32 " mode=%o path=%s\n", __func__,
                    (uint_fast64_t)seq, (uint_fast32_t)ino, (unsigned)mode,
                    slot[0] ? slot : "(null)");
}

static int kafs_diag_should_log_first_pwrite(struct kafs_context *ctx, kafs_sinode_t *inoent,
                                             const void *buf, kafs_off_t size,
                                             kafs_inocnt_t *ino_out)
{
  if (!ctx || !inoent || !buf || size <= 0 || !ctx->c_superblock || !ctx->c_diag_create_seq ||
      !ctx->c_diag_create_mode || !ctx->c_diag_create_first_write_seen ||
      !ctx->c_diag_create_paths || !kafs_extra_diag_enabled())
    return 0;

  kafs_inocnt_t ino = kafs_ctx_ino_no(ctx, inoent);
  if (ino >= kafs_sb_inocnt_get(ctx->c_superblock))
    return 0;
  if (ctx->c_diag_create_first_write_seen[ino])
    return 0;

  *ino_out = ino;
  return 1;
}

static void kafs_diag_emit_first_pwrite_after_create(struct kafs_context *ctx,
                                                     kafs_sinode_t *inoent, kafs_inocnt_t ino,
                                                     uint64_t seq, kafs_off_t size,
                                                     kafs_off_t offset, const char *hex_sample,
                                                     const char *ascii_sample)
{
  char *slot = ctx->c_diag_create_paths + ((size_t)ino * KAFS_DIAG_CREATE_PATH_MAX);
  kafs_log(KAFS_LOG_WARNING,
           "%s: seq=%" PRIuFAST64 " ino=%" PRIuFAST32 " expected_mode=%o current_mode=%o "
           "size=%" PRIuFAST64 " off=%" PRIuFAST64 " path=%s sample_hex=%s sample_ascii='%s'\n",
           __func__, (uint_fast64_t)seq, (uint_fast32_t)ino, (unsigned)ctx->c_diag_create_mode[ino],
           (unsigned)kafs_ino_mode_get(inoent), (uint_fast64_t)size, (uint_fast64_t)offset,
           slot[0] ? slot : "(null)", hex_sample[0] ? hex_sample : "-",
           ascii_sample[0] ? ascii_sample : "-");
  kafs_diag_appendf(ctx,
                    "%s: seq=%" PRIuFAST64 " ino=%" PRIuFAST32
                    " expected_mode=%o current_mode=%o size=%" PRIuFAST64 " off=%" PRIuFAST64
                    " path=%s sample_hex=%s sample_ascii='%s'\n",
                    __func__, (uint_fast64_t)seq, (uint_fast32_t)ino,
                    (unsigned)ctx->c_diag_create_mode[ino], (unsigned)kafs_ino_mode_get(inoent),
                    (uint_fast64_t)size, (uint_fast64_t)offset, slot[0] ? slot : "(null)",
                    hex_sample[0] ? hex_sample : "-", ascii_sample[0] ? ascii_sample : "-");
}

static void kafs_diag_log_first_pwrite_after_create(struct kafs_context *ctx, kafs_sinode_t *inoent,
                                                    const void *buf, kafs_off_t size,
                                                    kafs_off_t offset)
{
  kafs_inocnt_t ino = 0;
  if (!kafs_diag_should_log_first_pwrite(ctx, inoent, buf, size, &ino))
    return;

  uint64_t seq = ctx->c_diag_create_seq[ino];
  if (seq == 0)
    return;
  ctx->c_diag_create_first_write_seen[ino] = 1u;

  char hex_sample[3 * 16 + 1];
  char ascii_sample[16 + 1];
  kafs_diag_format_sample(buf, (size_t)size, hex_sample, sizeof(hex_sample), ascii_sample,
                          sizeof(ascii_sample));
  kafs_diag_emit_first_pwrite_after_create(ctx, inoent, ino, seq, size, offset, hex_sample,
                                           ascii_sample);
}

#else
typedef struct kafs_diag_write_scope
{
  int unused;
} kafs_diag_write_scope_t;

static void kafs_diag_log_open(struct kafs_context *ctx, const char *image_path)
{
  (void)ctx;
  (void)image_path;
}

static void kafs_diag_log_close(struct kafs_context *ctx) { (void)ctx; }

static kafs_diag_write_scope_t kafs_diag_write_scope_enter(const char *path, uint32_t ino)
{
  (void)path;
  (void)ino;
  return (kafs_diag_write_scope_t){0};
}

static void kafs_diag_write_scope_leave(kafs_diag_write_scope_t scope) { (void)scope; }

const char *kafs_diag_current_write_path(void) { return NULL; }

uint32_t kafs_diag_current_write_ino(void) { return KAFS_INO_NONE; }

static void kafs_diag_log_dir_ref_set(const char *phase, struct kafs_context *ctx,
                                      kafs_sinode_t *inoent, kafs_iblkcnt_t iblo,
                                      kafs_blkcnt_t old_ref, kafs_blkcnt_t new_ref)
{
  (void)phase;
  (void)ctx;
  (void)inoent;
  (void)iblo;
  (void)old_ref;
  (void)new_ref;
}

static void kafs_diag_log_dir_iblk_write(const char *phase, struct kafs_context *ctx,
                                         kafs_sinode_t *inoent, kafs_iblkcnt_t iblo,
                                         kafs_blkcnt_t old_ref, kafs_blkcnt_t new_ref,
                                         const void *buf, size_t len)
{
  (void)phase;
  (void)ctx;
  (void)inoent;
  (void)iblo;
  (void)old_ref;
  (void)new_ref;
  (void)buf;
  (void)len;
}

static void kafs_diag_log_first_pwrite_after_create(struct kafs_context *ctx, kafs_sinode_t *inoent,
                                                    const void *buf, kafs_off_t size,
                                                    kafs_off_t offset)
{
  (void)ctx;
  (void)inoent;
  (void)buf;
  (void)size;
  (void)offset;
}

static void kafs_diag_clear_create_event(struct kafs_context *ctx, kafs_inocnt_t ino)
{
  (void)ctx;
  (void)ino;
}

static void kafs_diag_note_create_event(struct kafs_context *ctx, kafs_inocnt_t ino,
                                        const char *path, kafs_mode_t mode)
{
  (void)ctx;
  (void)ino;
  (void)path;
  (void)mode;
}
#endif

#define KAFS_PENDINGLOG_MAGIC 0x4b504c47u /* 'KPLG' */
#define KAFS_PENDINGLOG_VERSION 1u
#define KAFS_PENDING_REF_FLAG 0x80000000u
#define KAFS_PENDING_REF_MASK 0x7fffffffu
#define KAFS_PENDINGLOG_CAPACITY_FLOOR 2u
#define KAFS_TOMBSTONE_GC_INTERVAL_MS_DEFAULT 1000u
#define KAFS_TOMBSTONE_GC_PRESSURE_INTERVAL_MS_DEFAULT 100u
#define KAFS_TOMBSTONE_GC_SCAN_BUDGET_DEFAULT 64u
#define KAFS_TOMBSTONE_GC_SCAN_BUDGET_PRESSURE 512u
#define KAFS_TOMBSTONE_GC_PRESSURE_FREE_INODES_MIN 4u
#define KAFS_TOMBSTONE_GC_PRESSURE_FREE_INODES_PCT 5u
#define KAFS_BG_DEDUP_INTERVAL_MS_DEFAULT 2000u
#define KAFS_BG_DEDUP_INTERVAL_MS_MIN 1u
#define KAFS_BG_DEDUP_INTERVAL_MS_MAX 60000u
#define KAFS_BG_DEDUP_QUIET_INTERVAL_MS_DEFAULT 5000u
#define KAFS_BG_DEDUP_PRESSURE_INTERVAL_MS_DEFAULT 100u
#define KAFS_BG_DEDUP_START_USED_PCT_DEFAULT 85u
#define KAFS_BG_DEDUP_PRESSURE_USED_PCT_DEFAULT 95u
#define KAFS_BG_DEDUP_PRESSURE_BURST_STEPS 16u
#define KAFS_BG_DEDUP_MONITOR_INTERVAL_MS 500u
#define KAFS_BG_DEDUP_RESAMPLE_QUIET_CYCLES 12u
#define KAFS_BG_DEDUP_COOLDOWN_MS 2000u
#define KAFS_BG_DEDUP_BLOCK_BUDGET_DEFAULT 8u
#define KAFS_BG_DEDUP_BLOCK_BUDGET_PRESSURE 32u
#define KAFS_BG_DEDUP_SCAN_WINDOW_DEFAULT 64u
#define KAFS_BG_DEDUP_SCAN_WINDOW_PRESSURE 256u
#define KAFS_BG_DEDUP_SWEEP_IDX_CAP_DEFAULT 256u
#define KAFS_BG_DEDUP_SWEEP_IDX_CAP_PRESSURE 1024u
#define KAFS_BG_DEDUP_SWEEP_BUCKETS 2048u
#define KAFS_HRL_RESCUE_RECENT_CAP 64u

#define KAFS_BG_DEDUP_MODE_COLD 1u
#define KAFS_BG_DEDUP_MODE_ADAPTIVE 2u
#define KAFS_BG_DEDUP_MODE_PRESSURE 3u

static uint32_t g_suppress_fuse_max_threads_warn = 1u;

static void kafs_fuse_log_func(enum fuse_log_level level, const char *fmt, va_list ap)
{
  if (!fmt)
    return;
  if (g_suppress_fuse_max_threads_warn && strstr(fmt, "Ignoring invalid max threads value") != NULL)
    return;
  (void)level;
  vfprintf(stderr, fmt, ap);
}

enum
{
  KAFS_PENDING_QUEUED = 1,
  KAFS_PENDING_HASHED = 2,
  KAFS_PENDING_RESOLVED = 3,
  KAFS_PENDING_FAILED = 4,
};

typedef struct kafs_pendinglog_hdr
{
  uint32_t magic;
  uint16_t version;
  uint16_t flags;
  uint32_t entry_size;
  uint32_t capacity;
  uint32_t head;
  uint32_t tail;
  uint64_t next_pending_id;
  uint64_t last_seq;
  uint64_t reserved[4];
} __attribute__((packed)) kafs_pendinglog_hdr_t;

typedef struct kafs_pendinglog_entry
{
  uint64_t pending_id;
  uint32_t ino;
  uint32_t iblk;
  uint32_t ino_epoch;
  uint32_t temp_blo;
  uint32_t state;
  uint32_t target_hrid;
  uint32_t reserved0;
  uint64_t seq;
} __attribute__((packed)) kafs_pendinglog_entry_t;

typedef enum
{
  KAFS_IBLKREF_FUNC_GET_RAW,
  KAFS_IBLKREF_FUNC_GET,
  KAFS_IBLKREF_FUNC_PUT,
  KAFS_IBLKREF_FUNC_SET
} kafs_iblkref_func_t;

static int kafs_ino_ibrk_run(struct kafs_context *ctx, kafs_sinode_t *inoent, kafs_iblkcnt_t iblo,
                             kafs_blkcnt_t *pblo, kafs_iblkref_func_t ifunc);
static int kafs_blk_read(struct kafs_context *ctx, kafs_blkcnt_t blo, void *buf);
static int kafs_pending_worker_start(struct kafs_context *ctx);
static void kafs_pending_worker_stop(struct kafs_context *ctx);
static int kafs_tombstone_gc_worker_start(struct kafs_context *ctx);
static void kafs_tombstone_gc_worker_stop(struct kafs_context *ctx);
static int kafs_bg_dedup_worker_start(struct kafs_context *ctx);
static void kafs_bg_dedup_worker_stop(struct kafs_context *ctx);
static int kafs_inode_is_tombstone(const kafs_sinode_t *inoent);
static int kafs_try_reclaim_unlinked_inode_locked(struct kafs_context *ctx, kafs_inocnt_t ino,
                                                  int *reclaimed);

static int kafs_pendinglog_region(struct kafs_context *ctx, uint64_t *off, uint64_t *size)
{
  if (!ctx || !ctx->c_superblock)
    return -EINVAL;
  uint64_t p_off = kafs_sb_pendinglog_offset_get(ctx->c_superblock);
  uint64_t p_size = kafs_sb_pendinglog_size_get(ctx->c_superblock);
  if (p_off == 0 || p_size < sizeof(kafs_pendinglog_hdr_t) + sizeof(kafs_pendinglog_entry_t))
    return -ENOENT;
  if (p_off > (uint64_t)ctx->c_img_size || p_size > (uint64_t)ctx->c_img_size - p_off)
    return -EINVAL;
  if (off)
    *off = p_off;
  if (size)
    *size = p_size;
  return 0;
}

static kafs_pendinglog_hdr_t *kafs_pendinglog_hdr_ptr(struct kafs_context *ctx)
{
  if (!ctx || !ctx->c_pendinglog_enabled || !ctx->c_pendinglog_base)
    return NULL;
  return (kafs_pendinglog_hdr_t *)ctx->c_pendinglog_base;
}

static kafs_pendinglog_entry_t *kafs_pendinglog_entry_ptr(struct kafs_context *ctx, uint32_t idx)
{
  kafs_pendinglog_hdr_t *hdr = kafs_pendinglog_hdr_ptr(ctx);
  if (!hdr || idx >= hdr->capacity)
    return NULL;
  char *base = (char *)ctx->c_pendinglog_base + sizeof(*hdr);
  return (kafs_pendinglog_entry_t *)(base + ((size_t)idx * (size_t)hdr->entry_size));
}

static uint32_t kafs_pendinglog_effective_capacity(struct kafs_context *ctx,
                                                   const kafs_pendinglog_hdr_t *hdr)
{
  if (!ctx || !hdr || hdr->capacity == 0)
    return 0;

  uint32_t hard_cap = hdr->capacity;
  uint32_t min_cap = ctx->c_pendinglog_capacity_min;
  uint32_t max_cap = ctx->c_pendinglog_capacity_max;
  uint32_t cur_cap = ctx->c_pendinglog_capacity;

  if (min_cap == 0)
    min_cap = KAFS_PENDINGLOG_CAPACITY_FLOOR;
  if (min_cap > hard_cap)
    min_cap = hard_cap;

  if (max_cap == 0 || max_cap > hard_cap)
    max_cap = hard_cap;
  if (max_cap < min_cap)
    max_cap = min_cap;

  if (cur_cap == 0)
    cur_cap = max_cap;
  if (cur_cap < min_cap)
    cur_cap = min_cap;
  if (cur_cap > max_cap)
    cur_cap = max_cap;

  ctx->c_pendinglog_capacity_min = min_cap;
  ctx->c_pendinglog_capacity_max = max_cap;
  ctx->c_pendinglog_capacity = cur_cap;
  return cur_cap;
}

static uint32_t kafs_pendinglog_count(struct kafs_context *ctx)
{
  kafs_pendinglog_hdr_t *hdr = kafs_pendinglog_hdr_ptr(ctx);
  if (!hdr || hdr->capacity == 0)
    return 0;
  if (hdr->tail >= hdr->head)
    return hdr->tail - hdr->head;
  return (hdr->capacity - hdr->head) + hdr->tail;
}

static void kafs_pendinglog_adapt_capacity_locked(struct kafs_context *ctx)
{
  kafs_pendinglog_hdr_t *hdr = kafs_pendinglog_hdr_ptr(ctx);
  if (!ctx || !hdr || hdr->capacity == 0)
    return;

  uint32_t cur_cap = kafs_pendinglog_effective_capacity(ctx, hdr);
  uint32_t min_cap = ctx->c_pendinglog_capacity_min;
  uint32_t max_cap = ctx->c_pendinglog_capacity_max;
  uint32_t qcnt = kafs_pendinglog_count(ctx);

  // Grow when queue usage approaches effective limit.
  if (cur_cap < max_cap && qcnt + (qcnt / 8u) >= cur_cap - 1u)
  {
    uint32_t next_cap = cur_cap + (cur_cap / 4u) + 1u;
    if (next_cap > max_cap)
      next_cap = max_cap;
    if (next_cap > cur_cap)
    {
      ctx->c_pendinglog_capacity = next_cap;
      kafs_log(KAFS_LOG_INFO, "kafs: pendinglog capacity grow %u -> %u (qcnt=%u)\n", cur_cap,
               next_cap, qcnt);
      return;
    }
  }

  // Shrink when queue is consistently low.
  if (cur_cap > min_cap && qcnt <= (cur_cap / 4u))
  {
    uint32_t target = (qcnt * 2u) + 8u;
    if (target < min_cap)
      target = min_cap;
    if (target < cur_cap)
    {
      ctx->c_pendinglog_capacity = target;
      kafs_log(KAFS_LOG_INFO, "kafs: pendinglog capacity shrink %u -> %u (qcnt=%u)\n", cur_cap,
               target, qcnt);
    }
  }
}

static int kafs_pendinglog_init_or_load(struct kafs_context *ctx)
{
  if (!ctx)
    return -EINVAL;

  ctx->c_pendinglog_enabled = 0;
  ctx->c_pendinglog_base = NULL;
  ctx->c_pendinglog_size = 0;
  ctx->c_pendinglog_capacity = 0;

  uint64_t off = 0, size = 0;
  int rc = kafs_pendinglog_region(ctx, &off, &size);
  if (rc < 0)
    return 0;

  ctx->c_pendinglog_base = (char *)ctx->c_img_base + off;
  ctx->c_pendinglog_size = (size_t)size;

  kafs_pendinglog_hdr_t *hdr = (kafs_pendinglog_hdr_t *)ctx->c_pendinglog_base;
  uint32_t cap =
      (uint32_t)((ctx->c_pendinglog_size - sizeof(*hdr)) / sizeof(kafs_pendinglog_entry_t));
  if (cap == 0)
    return -EINVAL;

  int valid = (hdr->magic == KAFS_PENDINGLOG_MAGIC && hdr->version == KAFS_PENDINGLOG_VERSION &&
               hdr->entry_size == sizeof(kafs_pendinglog_entry_t) && hdr->capacity == cap &&
               hdr->head < cap && hdr->tail < cap);
  if (!valid)
  {
    memset(ctx->c_pendinglog_base, 0, ctx->c_pendinglog_size);
    hdr->magic = KAFS_PENDINGLOG_MAGIC;
    hdr->version = KAFS_PENDINGLOG_VERSION;
    hdr->flags = 0;
    hdr->entry_size = sizeof(kafs_pendinglog_entry_t);
    hdr->capacity = cap;
    hdr->head = 0;
    hdr->tail = 0;
    hdr->next_pending_id = 1;
    hdr->last_seq = 0;
  }

  ctx->c_pendinglog_enabled = 1;
  (void)kafs_pendinglog_effective_capacity(ctx, hdr);
  return 0;
}

static int kafs_pendinglog_enqueue(struct kafs_context *ctx, const kafs_pendinglog_entry_t *ent,
                                   uint64_t *pending_id)
{
  kafs_pendinglog_hdr_t *hdr = kafs_pendinglog_hdr_ptr(ctx);
  if (!hdr || !ent)
    return -ENOSYS;

  (void)kafs_pendinglog_effective_capacity(ctx, hdr);
  kafs_pendinglog_adapt_capacity_locked(ctx);
  uint32_t eff_cap = ctx->c_pendinglog_capacity;
  if (eff_cap < KAFS_PENDINGLOG_CAPACITY_FLOOR)
    eff_cap = KAFS_PENDINGLOG_CAPACITY_FLOOR;
  if (kafs_pendinglog_count(ctx) >= eff_cap - 1u)
    return -ENOSPC;

  uint32_t next = hdr->tail + 1;
  if (next >= hdr->capacity)
    next = 0;

  kafs_pendinglog_entry_t *slot = kafs_pendinglog_entry_ptr(ctx, hdr->tail);
  if (!slot)
    return -EIO;

  *slot = *ent;
  slot->pending_id = hdr->next_pending_id++;
  hdr->tail = next;
  if (pending_id)
    *pending_id = slot->pending_id;
  return 0;
}

static int kafs_pendinglog_read(struct kafs_context *ctx, uint32_t idx,
                                kafs_pendinglog_entry_t *out)
{
  if (!out)
    return -EINVAL;
  kafs_pendinglog_entry_t *slot = kafs_pendinglog_entry_ptr(ctx, idx);
  if (!slot)
    return -ENOENT;
  *out = *slot;
  return 0;
}

static uint32_t kafs_pendinglog_next_idx(const kafs_pendinglog_hdr_t *hdr, uint32_t idx)
{
  idx += 1u;
  if (idx >= hdr->capacity)
    idx = 0;
  return idx;
}

static int kafs_pendinglog_find_by_id(struct kafs_context *ctx, uint64_t pending_id,
                                      kafs_pendinglog_entry_t *out)
{
  if (!ctx || !out || pending_id == 0)
    return -EINVAL;

  int need_unlock = 0;
  if (ctx->c_pending_worker_lock_init)
  {
    pthread_mutex_lock(&ctx->c_pending_worker_lock);
    need_unlock = 1;
  }

  kafs_pendinglog_hdr_t *hdr = kafs_pendinglog_hdr_ptr(ctx);
  if (!hdr || hdr->capacity == 0)
  {
    if (need_unlock)
      pthread_mutex_unlock(&ctx->c_pending_worker_lock);
    return -ENOENT;
  }

  uint32_t idx = hdr->head;
  while (idx != hdr->tail)
  {
    kafs_pendinglog_entry_t *slot = kafs_pendinglog_entry_ptr(ctx, idx);
    if (!slot)
    {
      if (need_unlock)
        pthread_mutex_unlock(&ctx->c_pending_worker_lock);
      return -EIO;
    }
    if (slot->pending_id == pending_id)
    {
      *out = *slot;
      if (need_unlock)
        pthread_mutex_unlock(&ctx->c_pending_worker_lock);
      return 0;
    }
    idx += 1u;
    if (idx >= hdr->capacity)
      idx = 0;
  }
  if (need_unlock)
    pthread_mutex_unlock(&ctx->c_pending_worker_lock);
  return -ENOENT;
}

static uint32_t kafs_inode_epoch_get(struct kafs_context *ctx, uint32_t ino)
{
  if (!ctx || !ctx->c_ino_epoch)
    return 0;
  if (ino >= kafs_sb_inocnt_get(ctx->c_superblock))
    return 0;
  return __atomic_load_n(&ctx->c_ino_epoch[ino], __ATOMIC_RELAXED);
}

static uint32_t kafs_inode_epoch_bump(struct kafs_context *ctx, uint32_t ino)
{
  if (!ctx || !ctx->c_ino_epoch)
    return 0;
  if (ino >= kafs_sb_inocnt_get(ctx->c_superblock))
    return 0;
  uint32_t v = __atomic_add_fetch(&ctx->c_ino_epoch[ino], 1u, __ATOMIC_RELAXED);
  if (v == 0)
  {
    __atomic_store_n(&ctx->c_ino_epoch[ino], 1u, __ATOMIC_RELAXED);
    v = 1u;
  }
  return v;
}

static int kafs_ref_is_pending(kafs_blkcnt_t ref)
{
  uint32_t raw = (uint32_t)ref;
  return (raw & KAFS_PENDING_REF_FLAG) != 0u;
}

static uint64_t kafs_ref_pending_id(kafs_blkcnt_t ref)
{
  uint32_t raw = (uint32_t)ref;
  return (uint64_t)(raw & KAFS_PENDING_REF_MASK);
}

__attribute_maybe_unused__ static int kafs_ref_pending_encode(uint64_t pending_id,
                                                              kafs_blkcnt_t *out_ref)
{
  if (!out_ref)
    return -EINVAL;
  if (pending_id == 0 || pending_id > (uint64_t)KAFS_PENDING_REF_MASK)
    return -ERANGE;
  *out_ref = (kafs_blkcnt_t)(KAFS_PENDING_REF_FLAG | (uint32_t)pending_id);
  return 0;
}

static int kafs_ref_resolve_data_blo(struct kafs_context *ctx, kafs_blkcnt_t ref,
                                     kafs_blkcnt_t *out_blo)
{
  if (!out_blo)
    return -EINVAL;
  if (ref == KAFS_BLO_NONE)
  {
    *out_blo = KAFS_BLO_NONE;
    return 0;
  }
  if (!kafs_ref_is_pending(ref))
  {
    *out_blo = ref;
    return 0;
  }

  uint64_t pending_id = kafs_ref_pending_id(ref);
  if (pending_id == 0)
    return -EIO;

  kafs_pendinglog_entry_t ent;
  int rc = kafs_pendinglog_find_by_id(ctx, pending_id, &ent);
  if (rc < 0)
    return rc;

  if (ent.temp_blo == KAFS_BLO_NONE)
    return -EIO;
  *out_blo = (kafs_blkcnt_t)ent.temp_blo;
  return 0;
}

static void kafs_pending_worker_notify(struct kafs_context *ctx)
{
  if (!ctx || !ctx->c_pending_worker_lock_init)
    return;
  pthread_mutex_lock(&ctx->c_pending_worker_lock);
  pthread_cond_signal(&ctx->c_pending_worker_cond);
  pthread_mutex_unlock(&ctx->c_pending_worker_lock);
}

static void kafs_pending_worker_notify_all(struct kafs_context *ctx)
{
  if (!ctx || !ctx->c_pending_worker_lock_init)
    return;
  pthread_mutex_lock(&ctx->c_pending_worker_lock);
  pthread_cond_broadcast(&ctx->c_pending_worker_cond);
  pthread_mutex_unlock(&ctx->c_pending_worker_lock);
}

static void kafs_pending_worker_watermarks(struct kafs_context *ctx, uint32_t *high_wm,
                                           uint32_t *low_wm)
{
  kafs_pendinglog_adapt_capacity_locked(ctx);
  uint32_t cap = ctx->c_pendinglog_capacity;
  if (cap < KAFS_PENDINGLOG_CAPACITY_FLOOR)
    cap = KAFS_PENDINGLOG_CAPACITY_FLOOR;
  *high_wm = cap - (cap / 8u);
  *low_wm = cap / 2u;
  if (*high_wm <= *low_wm)
    *high_wm = *low_wm + 1u;
}

static uint64_t kafs_pending_worker_oldest_age_ms(struct kafs_context *ctx,
                                                  kafs_pendinglog_hdr_t *hdr, uint32_t qcnt)
{
  if (qcnt == 0)
    return 0;

  kafs_pendinglog_entry_t *head = kafs_pendinglog_entry_ptr(ctx, hdr->head);
  uint64_t now_rt_ns = kafs_now_realtime_ns();
  if (!head || head->seq == 0)
    return 0;
  if (head->seq > now_rt_ns + 300000000000ull || now_rt_ns < head->seq)
    return 0;
  return (now_rt_ns - head->seq) / 1000000ull;
}

static void kafs_pending_worker_update_ttl_state(struct kafs_context *ctx, uint64_t oldest_age_ms,
                                                 uint32_t *over_soft, uint32_t *over_hard)
{
  *over_soft = 0;
  *over_hard = 0;
  if (ctx->c_pending_ttl_soft_ms > 0 && oldest_age_ms >= (uint64_t)ctx->c_pending_ttl_soft_ms)
    *over_soft = 1;
  if (ctx->c_pending_ttl_hard_ms > 0 && oldest_age_ms >= (uint64_t)ctx->c_pending_ttl_hard_ms)
    *over_hard = 1;

  ctx->c_pending_oldest_age_ms = oldest_age_ms;
  ctx->c_pending_ttl_over_soft = *over_soft;
  ctx->c_pending_ttl_over_hard = *over_hard;
}

static void kafs_pending_worker_apply_auto_boost(struct kafs_context *ctx, uint32_t qcnt,
                                                 uint32_t high_wm, uint32_t low_wm,
                                                 uint32_t over_soft)
{
  if (!ctx->c_pending_worker_auto_boosted)
  {
    if ((qcnt >= high_wm || over_soft) &&
        (ctx->c_pending_worker_prio_base_mode != KAFS_PENDING_WORKER_PRIO_NORMAL ||
         ctx->c_pending_worker_nice_base != 0))
    {
      ctx->c_pending_worker_prio_mode = KAFS_PENDING_WORKER_PRIO_NORMAL;
      ctx->c_pending_worker_nice = 0;
      ctx->c_pending_worker_prio_dirty = 1;
      ctx->c_pending_worker_auto_boosted = 1;
    }
    return;
  }

  if (qcnt <= low_wm && !over_soft)
  {
    ctx->c_pending_worker_prio_mode = ctx->c_pending_worker_prio_base_mode;
    ctx->c_pending_worker_nice = ctx->c_pending_worker_nice_base;
    ctx->c_pending_worker_prio_dirty = 1;
    ctx->c_pending_worker_auto_boosted = 0;
  }
}

static void kafs_pending_worker_adjust_priority_locked(struct kafs_context *ctx)
{
  if (!ctx || !ctx->c_pending_worker_running)
    return;

  kafs_pendinglog_hdr_t *hdr = kafs_pendinglog_hdr_ptr(ctx);
  if (!hdr || hdr->capacity == 0)
    return;

  uint32_t high_wm = 0;
  uint32_t low_wm = 0;
  kafs_pending_worker_watermarks(ctx, &high_wm, &low_wm);

  uint32_t qcnt = kafs_pendinglog_count(ctx);
  uint32_t over_soft = 0;
  uint32_t over_hard = 0;
  uint64_t oldest_age_ms = kafs_pending_worker_oldest_age_ms(ctx, hdr, qcnt);
  kafs_pending_worker_update_ttl_state(ctx, oldest_age_ms, &over_soft, &over_hard);
  kafs_pending_worker_apply_auto_boost(ctx, qcnt, high_wm, low_wm, over_soft);
}

static void kafs_pending_worker_begin_boost(struct kafs_context *ctx, uint32_t *saved_mode,
                                            int *saved_nice, int *changed)
{
  if (!changed)
    return;
  *changed = 0;
  if (saved_mode)
    *saved_mode = KAFS_PENDING_WORKER_PRIO_NORMAL;
  if (saved_nice)
    *saved_nice = 0;
  if (!ctx || !ctx->c_pending_worker_lock_init || !ctx->c_pending_worker_running)
    return;

  pthread_mutex_lock(&ctx->c_pending_worker_lock);
  if (saved_mode)
    *saved_mode = ctx->c_pending_worker_prio_mode;
  if (saved_nice)
    *saved_nice = ctx->c_pending_worker_nice;

  if (ctx->c_pending_worker_prio_mode != KAFS_PENDING_WORKER_PRIO_NORMAL ||
      ctx->c_pending_worker_nice != 0)
  {
    ctx->c_pending_worker_prio_mode = KAFS_PENDING_WORKER_PRIO_NORMAL;
    ctx->c_pending_worker_nice = 0;
    ctx->c_pending_worker_prio_dirty = 1;
    pthread_cond_signal(&ctx->c_pending_worker_cond);
    *changed = 1;
  }
  pthread_mutex_unlock(&ctx->c_pending_worker_lock);
}

static void kafs_pending_worker_end_boost(struct kafs_context *ctx, uint32_t saved_mode,
                                          int saved_nice, int changed)
{
  if (!changed)
    return;
  if (!ctx || !ctx->c_pending_worker_lock_init || !ctx->c_pending_worker_running)
    return;

  pthread_mutex_lock(&ctx->c_pending_worker_lock);
  ctx->c_pending_worker_prio_mode = saved_mode;
  ctx->c_pending_worker_nice = saved_nice;
  ctx->c_pending_worker_prio_dirty = 1;
  pthread_cond_signal(&ctx->c_pending_worker_cond);
  pthread_mutex_unlock(&ctx->c_pending_worker_lock);
}

static int kafs_pendinglog_inode_state_locked(struct kafs_context *ctx, uint32_t ino,
                                              int *has_pending, int *has_failed)
{
  if (!has_pending || !has_failed)
    return -EINVAL;
  *has_pending = 0;
  *has_failed = 0;

  kafs_pendinglog_hdr_t *hdr = kafs_pendinglog_hdr_ptr(ctx);
  if (!hdr || hdr->capacity == 0)
    return 0;

  uint32_t idx = hdr->head;
  while (idx != hdr->tail)
  {
    kafs_pendinglog_entry_t *slot = kafs_pendinglog_entry_ptr(ctx, idx);
    if (!slot)
      return -EIO;
    if (slot->ino == ino)
    {
      if (slot->state == KAFS_PENDING_FAILED)
        *has_failed = 1;
      else if (slot->state != KAFS_PENDING_RESOLVED)
        *has_pending = 1;
    }
    idx = kafs_pendinglog_next_idx(hdr, idx);
  }
  return 0;
}

static int kafs_pendinglog_drain_inode(struct kafs_context *ctx, uint32_t ino)
{
  if (!ctx || !ctx->c_pendinglog_enabled || !ctx->c_pending_worker_running ||
      !ctx->c_pending_worker_lock_init)
    return 0;

  struct timespec deadline;
  clock_gettime(CLOCK_REALTIME, &deadline);
  deadline.tv_sec += 30;

  pthread_mutex_lock(&ctx->c_pending_worker_lock);
  for (;;)
  {
    int has_pending = 0;
    int has_failed = 0;
    int rc = kafs_pendinglog_inode_state_locked(ctx, ino, &has_pending, &has_failed);
    if (rc < 0)
    {
      pthread_mutex_unlock(&ctx->c_pending_worker_lock);
      return rc;
    }
    if (has_failed)
    {
      pthread_mutex_unlock(&ctx->c_pending_worker_lock);
      return -EIO;
    }
    if (!has_pending)
    {
      pthread_mutex_unlock(&ctx->c_pending_worker_lock);
      return 0;
    }

    pthread_cond_signal(&ctx->c_pending_worker_cond);
    int tw =
        pthread_cond_timedwait(&ctx->c_pending_worker_cond, &ctx->c_pending_worker_lock, &deadline);
    if (tw == ETIMEDOUT)
    {
      pthread_mutex_unlock(&ctx->c_pending_worker_lock);
      return -ETIMEDOUT;
    }
  }
}

static int kafs_pendinglog_inode_has_pending_id(struct kafs_context *ctx, uint32_t ino,
                                                uint32_t iblk, uint64_t pending_id, int *is_pending)
{
  if (!ctx || !is_pending)
    return -EINVAL;
  *is_pending = 0;
  if (pending_id == 0)
    return 0;
  if (ino >= kafs_sb_inocnt_get(ctx->c_superblock))
    return 0;

  kafs_inode_lock(ctx, ino);
  kafs_sinode_t *inoent = kafs_ctx_inode(ctx, ino);
  if (kafs_ino_get_usage(inoent))
  {
    kafs_off_t cur_size = kafs_ino_size_get(inoent);
    if (cur_size > KAFS_INODE_DIRECT_BYTES)
    {
      kafs_blksize_t bs = kafs_sb_blksize_get(ctx->c_superblock);
      kafs_iblkcnt_t iblocnt = (kafs_iblkcnt_t)((cur_size + bs - 1) / bs);
      if ((kafs_iblkcnt_t)iblk < iblocnt)
      {
        kafs_blkcnt_t cur_raw = KAFS_BLO_NONE;
        if (kafs_ino_ibrk_run(ctx, inoent, (kafs_iblkcnt_t)iblk, &cur_raw,
                              KAFS_IBLKREF_FUNC_GET_RAW) == 0)
        {
          if (kafs_ref_is_pending(cur_raw) && kafs_ref_pending_id(cur_raw) == pending_id)
            *is_pending = 1;
        }
      }
    }
  }
  kafs_inode_unlock(ctx, ino);
  return 0;
}

static void kafs_pendinglog_requeue_entry(kafs_pendinglog_entry_t *slot, uint64_t now_rt_ns,
                                          uint32_t *replay_requeued)
{
  slot->state = KAFS_PENDING_QUEUED;
  if (slot->seq == 0)
    slot->seq = now_rt_ns;
  (*replay_requeued)++;
}

static int kafs_pendinglog_replay_scan_entries(struct kafs_context *ctx, kafs_pendinglog_hdr_t *hdr,
                                               uint64_t now_rt_ns, uint32_t *replay_requeued)
{
  uint32_t idx = hdr->head;
  while (idx != hdr->tail)
  {
    kafs_pendinglog_entry_t *slot = kafs_pendinglog_entry_ptr(ctx, idx);
    if (!slot)
      return -EIO;

    if (slot->state == KAFS_PENDING_HASHED)
      kafs_pendinglog_requeue_entry(slot, now_rt_ns, replay_requeued);
    else if (slot->state == KAFS_PENDING_RESOLVED)
    {
      int still_pending = 0;
      (void)kafs_pendinglog_inode_has_pending_id(ctx, slot->ino, slot->iblk, slot->pending_id,
                                                 &still_pending);
      if (still_pending)
        kafs_pendinglog_requeue_entry(slot, now_rt_ns, replay_requeued);
    }

    idx = kafs_pendinglog_next_idx(hdr, idx);
  }
  return 0;
}

static int kafs_pendinglog_replay_trim_head(struct kafs_context *ctx, kafs_pendinglog_hdr_t *hdr,
                                            uint64_t now_rt_ns, uint32_t *replay_requeued,
                                            uint32_t *replay_dropped)
{
  while (hdr->head != hdr->tail)
  {
    kafs_pendinglog_entry_t *slot = kafs_pendinglog_entry_ptr(ctx, hdr->head);
    if (!slot)
      return -EIO;

    if (slot->state == KAFS_PENDING_FAILED)
    {
      hdr->head = kafs_pendinglog_next_idx(hdr, hdr->head);
      (*replay_dropped)++;
      continue;
    }

    if (slot->state == KAFS_PENDING_RESOLVED)
    {
      int still_pending = 0;
      (void)kafs_pendinglog_inode_has_pending_id(ctx, slot->ino, slot->iblk, slot->pending_id,
                                                 &still_pending);
      if (!still_pending)
      {
        hdr->head = kafs_pendinglog_next_idx(hdr, hdr->head);
        (*replay_dropped)++;
        continue;
      }

      kafs_pendinglog_requeue_entry(slot, now_rt_ns, replay_requeued);
    }

    break;
  }
  return 0;
}

static int kafs_pendinglog_replay_mount(struct kafs_context *ctx)
{
  if (!ctx || !ctx->c_pendinglog_enabled)
    return 0;

  uint64_t now_rt_ns = kafs_now_realtime_ns();

  kafs_pendinglog_hdr_t *hdr = kafs_pendinglog_hdr_ptr(ctx);
  if (!hdr || hdr->capacity == 0)
    return 0;

  uint32_t replay_requeued = 0;
  uint32_t replay_dropped = 0;

  int rc = kafs_pendinglog_replay_scan_entries(ctx, hdr, now_rt_ns, &replay_requeued);
  if (rc != 0)
    return rc;
  rc = kafs_pendinglog_replay_trim_head(ctx, hdr, now_rt_ns, &replay_requeued, &replay_dropped);
  if (rc != 0)
    return rc;

  if (replay_requeued || replay_dropped)
  {
    kafs_journal_note(ctx, "PENDINGLOG", "replay: requeued=%u dropped=%u remain=%u",
                      replay_requeued, replay_dropped, kafs_pendinglog_count(ctx));
  }
  return 0;
}

static int kafs_fsync_policy_parse(const char *s, uint32_t *policy)
{
  if (!s || !policy)
    return -EINVAL;
  if (strcmp(s, "full") == 0)
  {
    *policy = KAFS_FSYNC_POLICY_FULL;
    return 0;
  }
  if (strcmp(s, "journal_only") == 0 || strcmp(s, "journal-only") == 0 || strcmp(s, "journal") == 0)
  {
    *policy = KAFS_FSYNC_POLICY_JOURNAL_ONLY;
    return 0;
  }
  if (strcmp(s, "adaptive") == 0)
  {
    *policy = KAFS_FSYNC_POLICY_ADAPTIVE;
    return 0;
  }
  return -EINVAL;
}

static int kafs_parse_u32_range(const char *s, uint32_t minv, uint32_t maxv, uint32_t *out)
{
  if (!s || !out)
    return -EINVAL;
  char *endp = NULL;
  unsigned long v = strtoul(s, &endp, 10);
  if (!endp || *endp != '\0')
    return -EINVAL;
  if (v < (unsigned long)minv || v > (unsigned long)maxv)
    return -ERANGE;
  *out = (uint32_t)v;
  return 0;
}

static int kafs_parse_onoff(const char *s, uint32_t *out)
{
  if (!s || !out)
    return -EINVAL;
  if (strcmp(s, "1") == 0 || strcmp(s, "on") == 0 || strcmp(s, "true") == 0 ||
      strcmp(s, "yes") == 0 || strcmp(s, "enable") == 0 || strcmp(s, "enabled") == 0)
  {
    *out = 1u;
    return 0;
  }
  if (strcmp(s, "0") == 0 || strcmp(s, "off") == 0 || strcmp(s, "false") == 0 ||
      strcmp(s, "no") == 0 || strcmp(s, "disable") == 0 || strcmp(s, "disabled") == 0)
  {
    *out = 0u;
    return 0;
  }
  return -EINVAL;
}

static int kafs_pending_worker_prio_mode_parse(const char *s, uint32_t *mode)
{
  if (!s || !mode)
    return -EINVAL;
  if (strcmp(s, "normal") == 0)
  {
    *mode = KAFS_PENDING_WORKER_PRIO_NORMAL;
    return 0;
  }
  if (strcmp(s, "idle") == 0)
  {
    *mode = KAFS_PENDING_WORKER_PRIO_IDLE;
    return 0;
  }
  return -EINVAL;
}

static int kafs_apply_worker_priority_self(uint32_t prio_mode, int nice_value)
{
  int err = 0;
  if (prio_mode == KAFS_PENDING_WORKER_PRIO_IDLE)
  {
#ifdef SCHED_IDLE
    struct sched_param sp;
    memset(&sp, 0, sizeof(sp));
    int prc = pthread_setschedparam(pthread_self(), SCHED_IDLE, &sp);
    if (prc != 0)
      err = -prc;
#else
    err = -ENOTSUP;
#endif
  }
  else
  {
    struct sched_param sp;
    memset(&sp, 0, sizeof(sp));
    int prc = pthread_setschedparam(pthread_self(), SCHED_OTHER, &sp);
    if (prc != 0)
      err = -prc;
  }

  if (nice_value < 0 || nice_value > 19)
  {
    if (err == 0)
      err = -ERANGE;
    return err;
  }

#ifdef __linux__
  long tid = syscall(SYS_gettid);
  if (tid <= 0)
    tid = 0;
  if (setpriority(PRIO_PROCESS, (id_t)tid, nice_value) != 0 && err == 0)
    err = -errno;
#else
  if (setpriority(PRIO_PROCESS, 0, nice_value) != 0 && err == 0)
    err = -errno;
#endif
  return err;
}

static int kafs_pending_worker_apply_priority_self(kafs_context_t *ctx)
{
  if (!ctx)
    return -EINVAL;

  int err =
      kafs_apply_worker_priority_self(ctx->c_pending_worker_prio_mode, ctx->c_pending_worker_nice);

  ctx->c_pending_worker_prio_apply_error = err;
  ctx->c_pending_worker_prio_dirty = 0;
  return err;
}

static int kafs_bg_dedup_worker_apply_priority_self(kafs_context_t *ctx)
{
  if (!ctx)
    return -EINVAL;

  int err = kafs_apply_worker_priority_self(ctx->c_bg_dedup_worker_prio_mode,
                                            ctx->c_bg_dedup_worker_nice);
  ctx->c_bg_dedup_worker_prio_apply_error = err;
  ctx->c_bg_dedup_worker_prio_dirty = 0;
  return err;
}

// Keep a tiny rolling cache to cheaply spot DIRECT duplicates that are not yet in HRL.
static uint64_t kafs_bg_hash64(const void *buf, size_t len)
{
  const unsigned char *p = (const unsigned char *)buf;
  uint64_t h = 1469598103934665603ull;
  const uint64_t prime = 1099511628211ull;
  for (size_t i = 0; i < len; ++i)
  {
    h ^= p[i];
    h *= prime;
  }
  return h;
}

static uint32_t kafs_bg_prng_next(struct kafs_context *ctx)
{
  if (!ctx)
    return 0;
  uint32_t x = ctx->c_bg_dedup_prng;
  if (x == 0)
    x = (uint32_t)(kafs_now_ns() & 0xffffffffu) ^ 0x9e3779b9u;
  x ^= x << 13;
  x ^= x >> 17;
  x ^= x << 5;
  ctx->c_bg_dedup_prng = x;
  return x;
}

static void kafs_bg_index_clear(struct kafs_context *ctx)
{
  if (!ctx)
    return;
  ctx->c_bg_dedup_idx_count = 0;
  ctx->c_bg_dedup_idx_next_insert = 0;
}

static void kafs_bg_index_note(struct kafs_context *ctx, uint64_t fast, kafs_blkcnt_t blo)
{
  if (!ctx || blo == KAFS_BLO_NONE)
    return;

  uint32_t cap = (uint32_t)(sizeof(ctx->c_bg_dedup_idx_fast) / sizeof(ctx->c_bg_dedup_idx_fast[0]));
  if (cap == 0)
    return;

  uint32_t slot = 0;
  if (ctx->c_bg_dedup_idx_count < cap)
  {
    slot = ctx->c_bg_dedup_idx_next_insert % cap;
    ctx->c_bg_dedup_idx_next_insert = slot + 1u;
    ctx->c_bg_dedup_idx_count++;
  }
  else
  {
    // Intentionally unstable eviction: random replacement gives different coverage each sweep.
    slot = kafs_bg_prng_next(ctx) % cap;
    __atomic_add_fetch(&ctx->c_stat_bg_dedup_index_evicts, 1u, __ATOMIC_RELAXED);
  }

  ctx->c_bg_dedup_idx_fast[slot] = fast;
  ctx->c_bg_dedup_idx_blo[slot] = (uint32_t)blo;
}

static kafs_blkcnt_t kafs_bg_index_find_dup_blo(struct kafs_context *ctx, uint64_t fast,
                                                kafs_blkcnt_t self_blo, const void *buf)
{
  if (!ctx || !buf)
    return KAFS_BLO_NONE;
  uint32_t n = ctx->c_bg_dedup_idx_count;
  if (n == 0)
    return KAFS_BLO_NONE;

  kafs_blksize_t bs = kafs_sb_blksize_get(ctx->c_superblock);
  char tmp[bs];
  __atomic_add_fetch(&ctx->c_stat_bg_dedup_direct_candidates, 1u, __ATOMIC_RELAXED);
  for (uint32_t i = 0; i < n; ++i)
  {
    if (ctx->c_bg_dedup_idx_fast[i] != fast)
      continue;
    kafs_blkcnt_t cand = (kafs_blkcnt_t)ctx->c_bg_dedup_idx_blo[i];
    if (cand == KAFS_BLO_NONE || cand == self_blo)
      continue;
    if (kafs_blk_read(ctx, cand, tmp) != 0)
      continue;
    if (memcmp(tmp, buf, bs) == 0)
      return cand;
  }
  return KAFS_BLO_NONE;
}

static void kafs_bg_recent_note(struct kafs_context *ctx, uint64_t fast, kafs_blkcnt_t blo)
{
  if (!ctx || blo == KAFS_BLO_NONE)
    return;
  uint32_t pos = ctx->c_bg_dedup_recent_pos % (uint32_t)(sizeof(ctx->c_bg_dedup_recent_fast) /
                                                         sizeof(ctx->c_bg_dedup_recent_fast[0]));
  ctx->c_bg_dedup_recent_fast[pos] = fast;
  ctx->c_bg_dedup_recent_blo[pos] = (uint32_t)blo;
  ctx->c_bg_dedup_recent_pos = (pos + 1u) % (uint32_t)(sizeof(ctx->c_bg_dedup_recent_fast) /
                                                       sizeof(ctx->c_bg_dedup_recent_fast[0]));
}

static kafs_blkcnt_t kafs_bg_recent_find_dup_blo(struct kafs_context *ctx, uint64_t fast,
                                                 kafs_blkcnt_t self_blo, const void *buf)
{
  if (!ctx || !buf)
    return KAFS_BLO_NONE;
  kafs_blksize_t bs = kafs_sb_blksize_get(ctx->c_superblock);
  char tmp[bs];
  uint32_t n =
      (uint32_t)(sizeof(ctx->c_bg_dedup_recent_fast) / sizeof(ctx->c_bg_dedup_recent_fast[0]));
  for (uint32_t i = 0; i < n; ++i)
  {
    if (ctx->c_bg_dedup_recent_fast[i] != fast)
      continue;
    kafs_blkcnt_t cand = (kafs_blkcnt_t)ctx->c_bg_dedup_recent_blo[i];
    if (cand == KAFS_BLO_NONE || cand == self_blo)
      continue;
    if (kafs_blk_read(ctx, cand, tmp) != 0)
      continue;
    if (memcmp(tmp, buf, bs) == 0)
      return cand;
  }
  return KAFS_BLO_NONE;
}

static void kafs_hrl_rescue_recent_note(struct kafs_context *ctx, uint64_t fast, kafs_blkcnt_t blo)
{
  if (!ctx || blo == KAFS_BLO_NONE)
    return;
  uint32_t pos = __atomic_fetch_add(&ctx->c_hrl_rescue_recent_pos, 1u, __ATOMIC_RELAXED) %
                 KAFS_HRL_RESCUE_RECENT_CAP;
  __atomic_store_n(&ctx->c_hrl_rescue_recent_fast[pos], fast, __ATOMIC_RELAXED);
  __atomic_store_n(&ctx->c_hrl_rescue_recent_blo[pos], (uint32_t)blo, __ATOMIC_RELAXED);
}

static kafs_blkcnt_t kafs_hrl_rescue_recent_find_dup_blo(struct kafs_context *ctx, uint64_t fast,
                                                         const void *buf)
{
  if (!ctx || !buf)
    return KAFS_BLO_NONE;
  kafs_blksize_t bs = kafs_sb_blksize_get(ctx->c_superblock);
  char tmp[bs];
  for (uint32_t i = 0; i < KAFS_HRL_RESCUE_RECENT_CAP; ++i)
  {
    if (__atomic_load_n(&ctx->c_hrl_rescue_recent_fast[i], __ATOMIC_RELAXED) != fast)
      continue;
    kafs_blkcnt_t cand =
        (kafs_blkcnt_t)__atomic_load_n(&ctx->c_hrl_rescue_recent_blo[i], __ATOMIC_RELAXED);
    if (cand == KAFS_BLO_NONE)
      continue;
    if (kafs_blk_read(ctx, cand, tmp) != 0)
      continue;
    if (memcmp(tmp, buf, bs) == 0)
      return cand;
  }
  return KAFS_BLO_NONE;
}

static int kafs_hrl_try_enospc_rescue(struct kafs_context *ctx, const void *buf,
                                      kafs_blkcnt_t *out_blo, int *out_is_new)
{
  if (!ctx || !buf || !out_blo || !out_is_new)
    return -EINVAL;

  kafs_blksize_t bs = kafs_sb_blksize_get(ctx->c_superblock);
  uint64_t fast = kafs_bg_hash64(buf, bs);

  __atomic_add_fetch(&ctx->c_stat_hrl_rescue_attempts, 1u, __ATOMIC_RELAXED);
  kafs_blkcnt_t nucleus = kafs_hrl_rescue_recent_find_dup_blo(ctx, fast, buf);
  if (nucleus == KAFS_BLO_NONE)
    return -ENOENT;

  kafs_blkcnt_t evicted_blo = KAFS_BLO_NONE;
  if (kafs_hrl_evict_ref1_to_direct(ctx, &evicted_blo) != 0)
    return -ENOSPC;
  __atomic_add_fetch(&ctx->c_stat_hrl_rescue_evicts, 1u, __ATOMIC_RELAXED);

  kafs_hrid_t hrid = 0;
  int is_new = 0;
  kafs_blkcnt_t new_blo = KAFS_BLO_NONE;
  int rc = kafs_hrl_put(ctx, buf, &hrid, &is_new, &new_blo);
  if (rc != 0)
    return rc;

  __atomic_add_fetch(&ctx->c_stat_hrl_rescue_hits, 1u, __ATOMIC_RELAXED);
  *out_blo = new_blo;
  *out_is_new = is_new;
  return 0;
}

static kafs_blkcnt_t kafs_bg_sweep_find_dup_blo(struct kafs_context *ctx, uint64_t fast,
                                                kafs_blkcnt_t self_blo, const void *buf,
                                                const uint32_t *bucket_heads,
                                                const uint64_t *entry_fast,
                                                const uint32_t *entry_blo,
                                                const uint32_t *entry_next)
{
  if (!ctx || !buf || !bucket_heads || !entry_fast || !entry_blo || !entry_next)
    return KAFS_BLO_NONE;

  kafs_blksize_t bs = kafs_sb_blksize_get(ctx->c_superblock);
  char tmp[bs];
  uint32_t bucket = (uint32_t)(fast & (KAFS_BG_DEDUP_SWEEP_BUCKETS - 1u));
  uint32_t idx = bucket_heads[bucket];
  while (idx != UINT32_MAX)
  {
    if (entry_fast[idx] == fast)
    {
      kafs_blkcnt_t cand = (kafs_blkcnt_t)entry_blo[idx];
      if (cand != KAFS_BLO_NONE && cand != self_blo)
      {
        if (kafs_blk_read(ctx, cand, tmp) == 0 && memcmp(tmp, buf, bs) == 0)
          return cand;
      }
    }
    idx = entry_next[idx];
  }
  return KAFS_BLO_NONE;
}

static void kafs_bg_sweep_note(uint64_t fast, kafs_blkcnt_t blo, uint32_t cap,
                               uint32_t *entry_count, uint32_t *bucket_heads, uint64_t *entry_fast,
                               uint32_t *entry_blo, uint32_t *entry_next)
{
  if (!entry_count || !bucket_heads || !entry_fast || !entry_blo || !entry_next)
    return;
  if (blo == KAFS_BLO_NONE)
    return;
  if (*entry_count >= cap)
    return;

  uint32_t idx = *entry_count;
  (*entry_count)++;

  uint32_t bucket = (uint32_t)(fast & (KAFS_BG_DEDUP_SWEEP_BUCKETS - 1u));
  entry_fast[idx] = fast;
  entry_blo[idx] = (uint32_t)blo;
  entry_next[idx] = bucket_heads[bucket];
  bucket_heads[bucket] = idx;
}

static int kafs_bg_advance_cursor(struct kafs_context *ctx, kafs_inocnt_t inocnt,
                                  uint32_t next_iblk)
{
  if (!ctx || inocnt == 0)
    return 0;

  if (!ctx->c_bg_dedup_anchor_valid)
  {
    ctx->c_bg_dedup_anchor_ino = ctx->c_bg_dedup_ino_cursor;
    ctx->c_bg_dedup_anchor_iblk = ctx->c_bg_dedup_iblk_cursor;
    ctx->c_bg_dedup_anchor_advance_count = 0;
    ctx->c_bg_dedup_anchor_valid = 1u;
  }

  ctx->c_bg_dedup_iblk_cursor = next_iblk;
  if (next_iblk == 0)
    ctx->c_bg_dedup_ino_cursor = (ctx->c_bg_dedup_ino_cursor + 1u) % inocnt;

  ctx->c_bg_dedup_anchor_advance_count++;
  if (ctx->c_bg_dedup_anchor_advance_count > 0 &&
      ctx->c_bg_dedup_ino_cursor == ctx->c_bg_dedup_anchor_ino &&
      ctx->c_bg_dedup_iblk_cursor == ctx->c_bg_dedup_anchor_iblk)
  {
    ctx->c_bg_dedup_anchor_valid = 0;
    ctx->c_bg_dedup_anchor_advance_count = 0;
    return 1;
  }
  return 0;
}

static void kafs_bg_enter_cooldown(struct kafs_context *ctx, int pressure_mode)
{
  if (!ctx)
    return;
  uint32_t cooldown_ms = pressure_mode ? 100u : KAFS_BG_DEDUP_COOLDOWN_MS;
  ctx->c_bg_dedup_cooldown_until_ns = kafs_now_ns() + (uint64_t)cooldown_ms * 1000000ull;
  kafs_bg_index_clear(ctx);
  __atomic_add_fetch(&ctx->c_stat_bg_dedup_cooldowns, 1u, __ATOMIC_RELAXED);
}

static int kafs_bg_dedup_try_install(struct kafs_context *ctx, uint32_t ino, kafs_iblkcnt_t iblk,
                                     kafs_blkcnt_t expect_raw, kafs_blkcnt_t new_blo)
{
  if (!ctx || new_blo == KAFS_BLO_NONE)
    return 0;

  kafs_inode_lock(ctx, ino);
  kafs_sinode_t *inoent = kafs_ctx_inode(ctx, ino);
  int installed = 0;
  if (kafs_ino_get_usage(inoent))
  {
    kafs_off_t size = kafs_ino_size_get(inoent);
    if (size > KAFS_INODE_DIRECT_BYTES)
    {
      kafs_blksize_t bs = kafs_sb_blksize_get(ctx->c_superblock);
      kafs_iblkcnt_t iblocnt = (kafs_iblkcnt_t)((size + bs - 1) / bs);
      if (iblk < iblocnt)
      {
        kafs_blkcnt_t cur_raw = KAFS_BLO_NONE;
        if (kafs_ino_ibrk_run(ctx, inoent, iblk, &cur_raw, KAFS_IBLKREF_FUNC_GET_RAW) == 0 &&
            cur_raw == expect_raw)
        {
          kafs_blkcnt_t set_blo = new_blo;
          if (kafs_ino_ibrk_run(ctx, inoent, iblk, &set_blo, KAFS_IBLKREF_FUNC_SET) == 0)
            installed = 1;
        }
      }
    }
  }
  kafs_inode_unlock(ctx, ino);
  return installed;
}

typedef struct kafs_bg_dedup_sweep_state
{
  uint32_t bucket_heads[KAFS_BG_DEDUP_SWEEP_BUCKETS];
  uint64_t entry_fast[KAFS_BG_DEDUP_SWEEP_IDX_CAP_PRESSURE];
  uint32_t entry_blo[KAFS_BG_DEDUP_SWEEP_IDX_CAP_PRESSURE];
  uint32_t entry_next[KAFS_BG_DEDUP_SWEEP_IDX_CAP_PRESSURE];
  uint32_t entry_count;
} kafs_bg_dedup_sweep_state_t;

static void kafs_bg_dedup_advance_or_cooldown(struct kafs_context *ctx, kafs_inocnt_t inocnt,
                                              uint32_t next_iblk, int pressure_mode)
{
  if (kafs_bg_advance_cursor(ctx, inocnt, next_iblk))
    kafs_bg_enter_cooldown(ctx, pressure_mode);
}

static void kafs_bg_dedup_sweep_state_init(kafs_bg_dedup_sweep_state_t *state)
{
  state->entry_count = 0;
  for (uint32_t i = 0; i < KAFS_BG_DEDUP_SWEEP_BUCKETS; ++i)
    state->bucket_heads[i] = UINT32_MAX;
}

static int kafs_bg_dedup_prepare_inode_scan(struct kafs_context *ctx, kafs_sinode_t *inoent,
                                            kafs_inocnt_t scanned, kafs_inocnt_t inocnt,
                                            int pressure_mode, kafs_blksize_t bs,
                                            uint32_t scan_window, kafs_iblkcnt_t *iblocnt_out,
                                            uint32_t *direct_cnt_out)
{
  if (!kafs_ino_get_usage(inoent) || S_ISDIR(kafs_ino_mode_get(inoent)))
  {
    kafs_inode_unlock(ctx, kafs_ctx_ino_no(ctx, inoent));
    if (scanned == 0)
      kafs_bg_dedup_advance_or_cooldown(ctx, inocnt, 0, pressure_mode);
    return 0;
  }

  kafs_off_t size = kafs_ino_size_get(inoent);
  if (size <= KAFS_INODE_DIRECT_BYTES)
  {
    kafs_inode_unlock(ctx, kafs_ctx_ino_no(ctx, inoent));
    if (scanned == 0)
      kafs_bg_dedup_advance_or_cooldown(ctx, inocnt, 0, pressure_mode);
    return 0;
  }

  *iblocnt_out = (kafs_iblkcnt_t)((size + bs - 1) / bs);
  *direct_cnt_out = (*iblocnt_out < scan_window) ? (uint32_t)*iblocnt_out : scan_window;
  if (*direct_cnt_out == 0)
  {
    kafs_inode_unlock(ctx, kafs_ctx_ino_no(ctx, inoent));
    if (scanned == 0)
      kafs_bg_dedup_advance_or_cooldown(ctx, inocnt, 0, pressure_mode);
    return 0;
  }

  return 1;
}

static int kafs_bg_dedup_handle_scanned_block(struct kafs_context *ctx, uint32_t ino,
                                              kafs_iblkcnt_t iblk, kafs_blkcnt_t snapshot_raw,
                                              kafs_blkcnt_t old_blo, const void *buf,
                                              kafs_blksize_t bs, uint32_t sweep_cap,
                                              uint32_t block_budget,
                                              uint32_t *scanned_blocks_this_step,
                                              kafs_bg_dedup_sweep_state_t *sweep_state)
{
  kafs_blkcnt_t match_blo = KAFS_BLO_NONE;
  int mrc = kafs_hrl_match_inc_by_block_excluding_blo(ctx, buf, old_blo, &match_blo);
  if (mrc == 0 && match_blo != KAFS_BLO_NONE)
  {
    if (kafs_bg_dedup_try_install(ctx, ino, iblk, snapshot_raw, match_blo))
    {
      __atomic_add_fetch(&ctx->c_stat_bg_dedup_replacements, 1u, __ATOMIC_RELAXED);
      (void)kafs_inode_release_hrl_ref(ctx, old_blo);
      __atomic_add_fetch(&ctx->c_stat_pending_old_block_freed, 1u, __ATOMIC_RELAXED);
      return 2;
    }
    (void)kafs_inode_release_hrl_ref(ctx, match_blo);
  }

  uint64_t fast = kafs_bg_hash64(buf, bs);
  kafs_blkcnt_t dup_direct = kafs_bg_sweep_find_dup_blo(
      ctx, fast, old_blo, buf, sweep_state->bucket_heads, sweep_state->entry_fast,
      sweep_state->entry_blo, sweep_state->entry_next);
  if (dup_direct == KAFS_BLO_NONE)
    dup_direct = kafs_bg_recent_find_dup_blo(ctx, fast, old_blo, buf);
  if (dup_direct == KAFS_BLO_NONE)
    dup_direct = kafs_bg_index_find_dup_blo(ctx, fast, old_blo, buf);
  kafs_bg_sweep_note(fast, old_blo, sweep_cap, &sweep_state->entry_count, sweep_state->bucket_heads,
                     sweep_state->entry_fast, sweep_state->entry_blo, sweep_state->entry_next);
  kafs_bg_recent_note(ctx, fast, old_blo);
  kafs_bg_index_note(ctx, fast, old_blo);
  if (dup_direct == KAFS_BLO_NONE)
  {
    if (*scanned_blocks_this_step >= block_budget)
      return 2;
    return 1;
  }

  __atomic_add_fetch(&ctx->c_stat_bg_dedup_direct_hits, 1u, __ATOMIC_RELAXED);

  kafs_hrid_t hrid = 0;
  int is_new = 0;
  kafs_blkcnt_t final_blo = KAFS_BLO_NONE;
  int prc = kafs_hrl_put(ctx, buf, &hrid, &is_new, &final_blo);
  if (prc == -ENOSPC)
  {
    kafs_blkcnt_t evicted_blo = KAFS_BLO_NONE;
    if (kafs_hrl_evict_ref1_to_direct(ctx, &evicted_blo) == 0)
    {
      __atomic_add_fetch(&ctx->c_stat_bg_dedup_evicts, 1u, __ATOMIC_RELAXED);
      __atomic_add_fetch(&ctx->c_stat_bg_dedup_retries, 1u, __ATOMIC_RELAXED);
      prc = kafs_hrl_put(ctx, buf, &hrid, &is_new, &final_blo);
    }
  }

  if (prc == 0 && final_blo != KAFS_BLO_NONE && final_blo != old_blo)
  {
    if (kafs_bg_dedup_try_install(ctx, ino, iblk, snapshot_raw, final_blo))
    {
      __atomic_add_fetch(&ctx->c_stat_bg_dedup_replacements, 1u, __ATOMIC_RELAXED);
      (void)kafs_inode_release_hrl_ref(ctx, old_blo);
      __atomic_add_fetch(&ctx->c_stat_pending_old_block_freed, 1u, __ATOMIC_RELAXED);
      return 2;
    }
    (void)kafs_inode_release_hrl_ref(ctx, final_blo);
  }

  if (*scanned_blocks_this_step >= block_budget)
    return 2;
  return 1;
}

static int kafs_bg_dedup_scan_inode(struct kafs_context *ctx, uint32_t ino, kafs_inocnt_t scanned,
                                    kafs_inocnt_t inocnt, int pressure_mode, kafs_blksize_t bs,
                                    uint32_t scan_window, uint32_t sweep_cap, uint32_t block_budget,
                                    uint32_t *scanned_blocks_this_step,
                                    kafs_bg_dedup_sweep_state_t *sweep_state)
{
  int inode_locked = 0;
  kafs_inode_lock(ctx, ino);
  inode_locked = 1;
  kafs_sinode_t *inoent = kafs_ctx_inode(ctx, ino);
  kafs_iblkcnt_t iblocnt = 0;
  uint32_t direct_cnt = 0;
  if (!kafs_bg_dedup_prepare_inode_scan(ctx, inoent, scanned, inocnt, pressure_mode, bs,
                                        scan_window, &iblocnt, &direct_cnt))
    return 0;

  char buf[bs];
  uint32_t start = (scanned == 0) ? (ctx->c_bg_dedup_iblk_cursor % direct_cnt) : 0u;
  for (uint32_t delta = 0; delta < direct_cnt; ++delta)
  {
    uint32_t iblk = (start + delta) % direct_cnt;
    uint32_t next_iblk = (iblk + 1u < direct_cnt) ? (iblk + 1u) : 0u;

    kafs_blkcnt_t raw = KAFS_BLO_NONE;
    if (kafs_ino_ibrk_run(ctx, inoent, (kafs_iblkcnt_t)iblk, &raw, KAFS_IBLKREF_FUNC_GET_RAW) != 0)
      continue;
    if (kafs_ref_is_pending(raw))
      continue;

    kafs_blkcnt_t old_blo = KAFS_BLO_NONE;
    if (kafs_ref_resolve_data_blo(ctx, raw, &old_blo) != 0 || old_blo == KAFS_BLO_NONE)
      continue;
    if (kafs_blk_read(ctx, old_blo, buf) != 0)
      continue;

    __atomic_add_fetch(&ctx->c_stat_bg_dedup_scanned_blocks, 1u, __ATOMIC_RELAXED);
    (*scanned_blocks_this_step)++;

    kafs_bg_dedup_advance_or_cooldown(ctx, inocnt, next_iblk, pressure_mode);

    kafs_blkcnt_t snapshot_raw = raw;
    kafs_inode_unlock(ctx, ino);
    inode_locked = 0;

    int action = kafs_bg_dedup_handle_scanned_block(ctx, ino, (kafs_iblkcnt_t)iblk, snapshot_raw,
                                                    old_blo, buf, bs, sweep_cap, block_budget,
                                                    scanned_blocks_this_step, sweep_state);
    if (action == 2)
      return 2;
    if (action == 1)
      break;

    kafs_inode_lock(ctx, ino);
    inode_locked = 1;
    inoent = kafs_ctx_inode(ctx, ino);
  }

  if (inode_locked)
    kafs_inode_unlock(ctx, ino);
  if (scanned == 0)
    kafs_bg_dedup_advance_or_cooldown(ctx, inocnt, 0, pressure_mode);
  return 0;
}

static void kafs_bg_dedup_step(struct kafs_context *ctx, int pressure_mode)
{
  if (!ctx || !ctx->c_superblock)
    return;
  if (ctx->c_hrl_bucket_cnt == 0)
    return;

  kafs_inocnt_t inocnt = kafs_sb_inocnt_get(ctx->c_superblock);
  if (inocnt == 0)
    return;

  uint64_t now_ns = kafs_now_ns();
  if (!pressure_mode && ctx->c_bg_dedup_cooldown_until_ns > now_ns)
    return;

  __atomic_add_fetch(&ctx->c_stat_bg_dedup_steps, 1u, __ATOMIC_RELAXED);

  kafs_blksize_t bs = kafs_sb_blksize_get(ctx->c_superblock);
  uint32_t block_budget =
      pressure_mode ? KAFS_BG_DEDUP_BLOCK_BUDGET_PRESSURE : KAFS_BG_DEDUP_BLOCK_BUDGET_DEFAULT;
  uint32_t scan_window =
      pressure_mode ? KAFS_BG_DEDUP_SCAN_WINDOW_PRESSURE : KAFS_BG_DEDUP_SCAN_WINDOW_DEFAULT;
  uint32_t sweep_cap =
      pressure_mode ? KAFS_BG_DEDUP_SWEEP_IDX_CAP_PRESSURE : KAFS_BG_DEDUP_SWEEP_IDX_CAP_DEFAULT;
  uint32_t scanned_blocks_this_step = 0;
  kafs_bg_dedup_sweep_state_t sweep_state;
  kafs_bg_dedup_sweep_state_init(&sweep_state);

  for (kafs_inocnt_t scanned = 0; scanned < inocnt; ++scanned)
  {
    uint32_t ino = (ctx->c_bg_dedup_ino_cursor + scanned) % inocnt;
    if (kafs_bg_dedup_scan_inode(ctx, ino, scanned, inocnt, pressure_mode, bs, scan_window,
                                 sweep_cap, block_budget, &scanned_blocks_this_step,
                                 &sweep_state) == 2)
      return;
  }
}

static void kafs_pending_worker_record_exit(kafs_context_t *ctx);
static int kafs_pending_worker_wait_next(kafs_context_t *ctx, uint32_t *idx,
                                         kafs_pendinglog_entry_t *ent);
static void kafs_pending_worker_skip_terminal_entry(kafs_context_t *ctx, uint32_t idx);
static int kafs_pending_worker_try_install_block(kafs_context_t *ctx,
                                                 const kafs_pendinglog_entry_t *ent,
                                                 kafs_blkcnt_t final_blo);
static void kafs_pending_worker_finalize_success(kafs_context_t *ctx, uint32_t idx,
                                                 const kafs_pendinglog_entry_t *ent,
                                                 kafs_hrid_t hrid, kafs_blkcnt_t final_blo,
                                                 int installed);
static uint32_t kafs_pending_worker_note_retry(kafs_context_t *ctx, uint32_t idx);
static void kafs_pending_worker_backoff(const kafs_context_t *ctx, uint32_t retry,
                                        uint64_t entry_seq);

static void *kafs_pending_worker_main(void *arg)
{
  kafs_context_t *ctx = (kafs_context_t *)arg;
  if (!ctx)
    return NULL;

  __atomic_store_n(&ctx->c_stat_pending_worker_lwp_tid, (int32_t)syscall(SYS_gettid),
                   __ATOMIC_RELAXED);
  __atomic_add_fetch(&ctx->c_stat_pending_worker_main_entries, 1u, __ATOMIC_RELAXED);

  (void)kafs_pending_worker_apply_priority_self(ctx);

  for (;;)
  {
    uint32_t idx = 0;
    kafs_pendinglog_entry_t ent;
    if (kafs_pending_worker_wait_next(ctx, &idx, &ent) != 0)
      return NULL;

    if (ctx->c_pending_worker_prio_dirty)
      (void)kafs_pending_worker_apply_priority_self(ctx);

    if (ent.state == KAFS_PENDING_RESOLVED || ent.state == KAFS_PENDING_FAILED)
    {
      kafs_pending_worker_skip_terminal_entry(ctx, idx);
      continue;
    }

    kafs_blksize_t blksize = kafs_sb_blksize_get(ctx->c_superblock);
    char buf[blksize];
    int rc = kafs_blk_read(ctx, (kafs_blkcnt_t)ent.temp_blo, buf);
    if (rc == 0)
    {
      kafs_hrid_t hrid = 0;
      int is_new = 0;
      kafs_blkcnt_t final_blo = KAFS_BLO_NONE;
      int installed = 0;
      ctx->c_stat_hrl_put_calls++;
      uint64_t t0 = kafs_now_ns();
      rc = kafs_hrl_put(ctx, buf, &hrid, &is_new, &final_blo);
      uint64_t t1 = kafs_now_ns();
      __atomic_add_fetch(&ctx->c_stat_iblk_write_ns_hrl_put, t1 - t0, __ATOMIC_RELAXED);
      if (rc == 0)
      {
        if (is_new)
          ctx->c_stat_hrl_put_misses++;
        else
          ctx->c_stat_hrl_put_hits++;

        installed = kafs_pending_worker_try_install_block(ctx, &ent, final_blo);
        kafs_pending_worker_finalize_success(ctx, idx, &ent, hrid, final_blo, installed);
        continue;
      }
    }

    uint32_t retry = kafs_pending_worker_note_retry(ctx, idx);
    kafs_pending_worker_backoff(ctx, retry, ent.seq);
  }
}

static void kafs_pending_worker_record_exit(kafs_context_t *ctx)
{
  __atomic_store_n(&ctx->c_stat_pending_worker_lwp_tid, 0, __ATOMIC_RELAXED);
  __atomic_add_fetch(&ctx->c_stat_pending_worker_main_exits, 1u, __ATOMIC_RELAXED);
}

static int kafs_pending_worker_wait_next(kafs_context_t *ctx, uint32_t *idx,
                                         kafs_pendinglog_entry_t *ent)
{
  pthread_mutex_lock(&ctx->c_pending_worker_lock);
  for (;;)
  {
    kafs_pendinglog_hdr_t *hdr = kafs_pendinglog_hdr_ptr(ctx);
    if (ctx->c_pending_worker_stop)
    {
      pthread_mutex_unlock(&ctx->c_pending_worker_lock);
      kafs_pending_worker_record_exit(ctx);
      return -1;
    }
    if (hdr && hdr->capacity > 0 && hdr->head != hdr->tail)
    {
      *idx = hdr->head;
      kafs_pendinglog_entry_t *slot = kafs_pendinglog_entry_ptr(ctx, *idx);
      if (slot)
      {
        *ent = *slot;
        pthread_mutex_unlock(&ctx->c_pending_worker_lock);
        return 0;
      }
    }
    pthread_cond_wait(&ctx->c_pending_worker_cond, &ctx->c_pending_worker_lock);
  }
}

static void kafs_pending_worker_skip_terminal_entry(kafs_context_t *ctx, uint32_t idx)
{
  pthread_mutex_lock(&ctx->c_pending_worker_lock);
  kafs_pendinglog_hdr_t *hdr = kafs_pendinglog_hdr_ptr(ctx);
  if (hdr && hdr->capacity > 0 && hdr->head == idx)
    hdr->head = kafs_pendinglog_next_idx(hdr, hdr->head);
  kafs_pending_worker_adjust_priority_locked(ctx);
  pthread_mutex_unlock(&ctx->c_pending_worker_lock);
}

static int kafs_pending_worker_try_install_block(kafs_context_t *ctx,
                                                 const kafs_pendinglog_entry_t *ent,
                                                 kafs_blkcnt_t final_blo)
{
  if (ent->ino >= kafs_sb_inocnt_get(ctx->c_superblock))
    return 0;

  int installed = 0;
  kafs_inode_lock(ctx, ent->ino);
  kafs_sinode_t *inoent = kafs_ctx_inode(ctx, ent->ino);
  if (kafs_ino_get_usage(inoent))
  {
    uint32_t cur_epoch = kafs_inode_epoch_get(ctx, ent->ino);
    if (ent->ino_epoch != 0 && ent->ino_epoch == cur_epoch)
    {
      kafs_off_t cur_size = kafs_ino_size_get(inoent);
      if (cur_size > KAFS_INODE_DIRECT_BYTES)
      {
        kafs_blksize_t bs = kafs_sb_blksize_get(ctx->c_superblock);
        kafs_iblkcnt_t iblocnt = (kafs_iblkcnt_t)((cur_size + bs - 1) / bs);
        if (ent->iblk < iblocnt)
        {
          kafs_blkcnt_t cur_raw = KAFS_BLO_NONE;
          if (kafs_ino_ibrk_run(ctx, inoent, ent->iblk, &cur_raw, KAFS_IBLKREF_FUNC_GET_RAW) == 0 &&
              kafs_ref_is_pending(cur_raw) && kafs_ref_pending_id(cur_raw) == ent->pending_id &&
              kafs_ino_ibrk_run(ctx, inoent, ent->iblk, &final_blo, KAFS_IBLKREF_FUNC_SET) == 0)
          {
            installed = 1;
          }
        }
      }
    }
  }
  kafs_inode_unlock(ctx, ent->ino);
  return installed;
}

static void kafs_pending_worker_finalize_success(kafs_context_t *ctx, uint32_t idx,
                                                 const kafs_pendinglog_entry_t *ent,
                                                 kafs_hrid_t hrid, kafs_blkcnt_t final_blo,
                                                 int installed)
{
  kafs_blkcnt_t old_blo = (kafs_blkcnt_t)ent->target_hrid;
  if (installed && old_blo != KAFS_BLO_NONE && old_blo != (kafs_blkcnt_t)ent->temp_blo &&
      old_blo != final_blo)
  {
    uint64_t t_dec0 = kafs_now_ns();
    (void)kafs_inode_release_hrl_ref(ctx, old_blo);
    __atomic_add_fetch(&ctx->c_stat_pending_old_block_freed, 1u, __ATOMIC_RELAXED);
    uint64_t t_dec1 = kafs_now_ns();
    __atomic_add_fetch(&ctx->c_stat_iblk_write_ns_dec_ref, t_dec1 - t_dec0, __ATOMIC_RELAXED);
  }

  if (!installed && final_blo != KAFS_BLO_NONE && final_blo != (kafs_blkcnt_t)ent->temp_blo)
    (void)kafs_inode_release_hrl_ref(ctx, final_blo);

  if ((kafs_blkcnt_t)ent->temp_blo != KAFS_BLO_NONE && (kafs_blkcnt_t)ent->temp_blo != final_blo)
    (void)kafs_inode_release_hrl_ref(ctx, (kafs_blkcnt_t)ent->temp_blo);

  pthread_mutex_lock(&ctx->c_pending_worker_lock);
  kafs_pendinglog_hdr_t *hdr = kafs_pendinglog_hdr_ptr(ctx);
  kafs_pendinglog_entry_t *slot = hdr ? kafs_pendinglog_entry_ptr(ctx, idx) : NULL;
  if (hdr && slot)
  {
    slot->state = KAFS_PENDING_RESOLVED;
    __atomic_add_fetch(&ctx->c_stat_pending_resolved, 1u, __ATOMIC_RELAXED);
    slot->target_hrid = (uint32_t)hrid;
    slot->reserved0 = 0;
    if (hdr->head == idx)
      hdr->head = kafs_pendinglog_next_idx(hdr, hdr->head);
  }
  kafs_pending_worker_adjust_priority_locked(ctx);
  pthread_cond_broadcast(&ctx->c_pending_worker_cond);
  pthread_mutex_unlock(&ctx->c_pending_worker_lock);
}

static uint32_t kafs_pending_worker_note_retry(kafs_context_t *ctx, uint32_t idx)
{
  uint32_t retry = 0;

  pthread_mutex_lock(&ctx->c_pending_worker_lock);
  kafs_pendinglog_hdr_t *hdr = kafs_pendinglog_hdr_ptr(ctx);
  kafs_pendinglog_entry_t *slot = hdr ? kafs_pendinglog_entry_ptr(ctx, idx) : NULL;
  if (slot)
  {
    slot->state = KAFS_PENDING_HASHED;
    slot->reserved0 += 1u;
    retry = slot->reserved0;
    if (retry >= 32u)
    {
      slot->state = KAFS_PENDING_FAILED;
      if (hdr && hdr->head == idx)
        hdr->head = kafs_pendinglog_next_idx(hdr, hdr->head);
    }
  }
  kafs_pending_worker_adjust_priority_locked(ctx);
  pthread_cond_broadcast(&ctx->c_pending_worker_cond);
  pthread_mutex_unlock(&ctx->c_pending_worker_lock);
  return retry;
}

static void kafs_pending_worker_backoff(const kafs_context_t *ctx, uint32_t retry,
                                        uint64_t entry_seq)
{
  if (retry == 0)
    return;

  int hard_ttl_exceeded = 0;
  if (ctx->c_pending_ttl_hard_ms > 0 && entry_seq > 0)
  {
    uint64_t now_rt_ns = kafs_now_realtime_ns();
    if (now_rt_ns >= entry_seq)
    {
      uint64_t age_ms = (now_rt_ns - entry_seq) / 1000000ull;
      if (age_ms >= (uint64_t)ctx->c_pending_ttl_hard_ms)
        hard_ttl_exceeded = 1;
    }
  }

  uint32_t backoff_ms = 1u;
  if (!hard_ttl_exceeded)
  {
    if (retry > 6u)
      retry = 6u;
    backoff_ms = 1u << retry;
  }

  struct timespec ts = {
      .tv_sec = (time_t)(backoff_ms / 1000u),
      .tv_nsec = (long)((backoff_ms % 1000u) * 1000000u),
  };
  nanosleep(&ts, NULL);
}

static int kafs_pending_worker_start(struct kafs_context *ctx)
{
  if (!ctx || !ctx->c_pendinglog_enabled)
    return 0;
  if (ctx->c_pending_worker_running)
    return 0;

  __atomic_add_fetch(&ctx->c_stat_pending_worker_start_calls, 1u, __ATOMIC_RELAXED);

  if (!ctx->c_pending_worker_lock_init)
  {
    if (pthread_mutex_init(&ctx->c_pending_worker_lock, NULL) != 0)
    {
      __atomic_add_fetch(&ctx->c_stat_pending_worker_start_failures, 1u, __ATOMIC_RELAXED);
      __atomic_store_n(&ctx->c_stat_pending_worker_start_last_error, -EIO, __ATOMIC_RELAXED);
      return -EIO;
    }
    if (pthread_cond_init(&ctx->c_pending_worker_cond, NULL) != 0)
    {
      pthread_mutex_destroy(&ctx->c_pending_worker_lock);
      __atomic_add_fetch(&ctx->c_stat_pending_worker_start_failures, 1u, __ATOMIC_RELAXED);
      __atomic_store_n(&ctx->c_stat_pending_worker_start_last_error, -EIO, __ATOMIC_RELAXED);
      return -EIO;
    }
    ctx->c_pending_worker_lock_init = 1;
  }

  ctx->c_pending_worker_stop = 0;
  int prc = pthread_create(&ctx->c_pending_worker_tid, NULL, kafs_pending_worker_main, ctx);
  if (prc != 0)
  {
    __atomic_add_fetch(&ctx->c_stat_pending_worker_start_failures, 1u, __ATOMIC_RELAXED);
    __atomic_store_n(&ctx->c_stat_pending_worker_start_last_error, -prc, __ATOMIC_RELAXED);
    return -prc;
  }

  ctx->c_pending_worker_running = 1;
  __atomic_store_n(&ctx->c_stat_pending_worker_start_last_error, 0, __ATOMIC_RELAXED);
  if (kafs_pendinglog_count(ctx) > 0)
    kafs_pending_worker_notify(ctx);
  return 0;
}

static void kafs_pending_worker_stop(struct kafs_context *ctx)
{
  if (!ctx)
    return;

  if (ctx->c_pending_worker_running)
  {
    pthread_mutex_lock(&ctx->c_pending_worker_lock);
    ctx->c_pending_worker_stop = 1;
    pthread_cond_broadcast(&ctx->c_pending_worker_cond);
    pthread_mutex_unlock(&ctx->c_pending_worker_lock);
    pthread_join(ctx->c_pending_worker_tid, NULL);
    ctx->c_pending_worker_running = 0;
  }

  if (ctx->c_pending_worker_lock_init)
  {
    pthread_cond_destroy(&ctx->c_pending_worker_cond);
    pthread_mutex_destroy(&ctx->c_pending_worker_lock);
    ctx->c_pending_worker_lock_init = 0;
  }
}

static uint32_t kafs_fs_used_pct(kafs_context_t *ctx)
{
  if (!ctx || !ctx->c_superblock)
    return 0;

  uint64_t total = (uint64_t)kafs_sb_blkcnt_get(ctx->c_superblock);
  if (total == 0)
    return 0;

  kafs_bitmap_lock(ctx);
  uint64_t free_blocks = (uint64_t)kafs_sb_blkcnt_free_get(ctx->c_superblock);
  kafs_bitmap_unlock(ctx);
  if (free_blocks > total)
    free_blocks = total;

  uint64_t used = total - free_blocks;
  if (used >= total)
    return 100u;
  return (uint32_t)((used * 100u) / total);
}

static int kafs_tombstone_pressure(kafs_context_t *ctx)
{
  if (!ctx || !ctx->c_superblock)
    return 0;

  uint32_t used_pct = kafs_fs_used_pct(ctx);
  uint32_t pressure_used_pct = ctx->c_bg_dedup_pressure_used_pct;
  if (pressure_used_pct > 0 && used_pct >= pressure_used_pct)
    return 1;

  kafs_inocnt_t inode_total = kafs_sb_inocnt_get(ctx->c_superblock);
  kafs_inocnt_t inode_free = 0;
  kafs_inode_alloc_lock(ctx);
  inode_free = kafs_sb_inocnt_free_get(ctx->c_superblock);
  kafs_inode_alloc_unlock(ctx);

  uint64_t free_floor = (uint64_t)inode_total * KAFS_TOMBSTONE_GC_PRESSURE_FREE_INODES_PCT / 100u;
  if (free_floor < KAFS_TOMBSTONE_GC_PRESSURE_FREE_INODES_MIN)
    free_floor = KAFS_TOMBSTONE_GC_PRESSURE_FREE_INODES_MIN;
  if (free_floor > inode_total)
    free_floor = inode_total;
  return (uint64_t)inode_free <= free_floor;
}

static uint32_t kafs_bg_dedup_used_pct(kafs_context_t *ctx) { return kafs_fs_used_pct(ctx); }

static uint32_t kafs_tombstone_gc_step(struct kafs_context *ctx, int pressure_mode)
{
  if (!ctx || !ctx->c_superblock)
    return 0;

  const kafs_inocnt_t first_ino = KAFS_INO_ROOTDIR + 1u;
  kafs_inocnt_t inode_total = kafs_sb_inocnt_get(ctx->c_superblock);
  if (inode_total <= first_ino)
    return 0;

  uint32_t budget = pressure_mode ? KAFS_TOMBSTONE_GC_SCAN_BUDGET_PRESSURE
                                  : KAFS_TOMBSTONE_GC_SCAN_BUDGET_DEFAULT;
  kafs_inocnt_t cursor = ctx->c_tombstone_gc_cursor;
  if (cursor < first_ino || cursor >= inode_total)
    cursor = first_ino;

  uint32_t reclaimed = 0;
  for (uint32_t scanned = 0; scanned < budget; ++scanned)
  {
    kafs_inocnt_t ino = cursor;
    cursor++;
    if (cursor >= inode_total)
      cursor = first_ino;

    int reclaimed_now = 0;
    kafs_inode_lock(ctx, (uint32_t)ino);
    if (kafs_inode_is_tombstone(kafs_ctx_inode(ctx, ino)))
      (void)kafs_try_reclaim_unlinked_inode_locked(ctx, ino, &reclaimed_now);
    kafs_inode_unlock(ctx, (uint32_t)ino);

    if (reclaimed_now)
    {
      kafs_inode_alloc_lock(ctx);
      (void)kafs_sb_inocnt_free_incr(ctx->c_superblock);
      kafs_sb_wtime_set(ctx->c_superblock, kafs_now());
      kafs_inode_alloc_unlock(ctx);
      reclaimed++;
    }
  }

  ctx->c_tombstone_gc_cursor = cursor;
  return reclaimed;
}

static void *kafs_tombstone_gc_worker_main(void *arg)
{
  kafs_context_t *ctx = (kafs_context_t *)arg;
  if (!ctx)
    return NULL;

  for (;;)
  {
    pthread_mutex_lock(&ctx->c_tombstone_gc_worker_lock);
    if (ctx->c_tombstone_gc_worker_stop)
    {
      pthread_mutex_unlock(&ctx->c_tombstone_gc_worker_lock);
      return NULL;
    }
    pthread_mutex_unlock(&ctx->c_tombstone_gc_worker_lock);

    int pressure_mode = kafs_tombstone_pressure(ctx);
    uint32_t reclaimed = kafs_tombstone_gc_step(ctx, pressure_mode);
    uint32_t wait_ms = pressure_mode ? KAFS_TOMBSTONE_GC_PRESSURE_INTERVAL_MS_DEFAULT
                                     : KAFS_TOMBSTONE_GC_INTERVAL_MS_DEFAULT;
    if (reclaimed > 0 && !pressure_mode)
      wait_ms = 1u;

    pthread_mutex_lock(&ctx->c_tombstone_gc_worker_lock);
    if (ctx->c_tombstone_gc_worker_stop)
    {
      pthread_mutex_unlock(&ctx->c_tombstone_gc_worker_lock);
      return NULL;
    }

    struct timespec wake;
    clock_gettime(CLOCK_REALTIME, &wake);
    wake.tv_nsec += (long)(wait_ms % 1000u) * 1000000l;
    wake.tv_sec += (time_t)(wait_ms / 1000u);
    if (wake.tv_nsec >= 1000000000l)
    {
      wake.tv_sec += 1;
      wake.tv_nsec -= 1000000000l;
    }
    (void)pthread_cond_timedwait(&ctx->c_tombstone_gc_worker_cond, &ctx->c_tombstone_gc_worker_lock,
                                 &wake);
    pthread_mutex_unlock(&ctx->c_tombstone_gc_worker_lock);
  }
}

static int kafs_tombstone_gc_worker_start(struct kafs_context *ctx)
{
  if (!ctx)
    return -EINVAL;
  if (ctx->c_tombstone_gc_worker_running)
    return 0;

  if (!ctx->c_tombstone_gc_worker_lock_init)
  {
    if (pthread_mutex_init(&ctx->c_tombstone_gc_worker_lock, NULL) != 0)
      return -EIO;
    if (pthread_cond_init(&ctx->c_tombstone_gc_worker_cond, NULL) != 0)
    {
      pthread_mutex_destroy(&ctx->c_tombstone_gc_worker_lock);
      return -EIO;
    }
    ctx->c_tombstone_gc_worker_lock_init = 1;
  }

  ctx->c_tombstone_gc_worker_stop = 0;
  int prc =
      pthread_create(&ctx->c_tombstone_gc_worker_tid, NULL, kafs_tombstone_gc_worker_main, ctx);
  if (prc != 0)
    return -prc;

  ctx->c_tombstone_gc_worker_running = 1;
  return 0;
}

static void kafs_tombstone_gc_worker_stop(struct kafs_context *ctx)
{
  if (!ctx)
    return;

  if (ctx->c_tombstone_gc_worker_running)
  {
    pthread_mutex_lock(&ctx->c_tombstone_gc_worker_lock);
    ctx->c_tombstone_gc_worker_stop = 1;
    pthread_cond_broadcast(&ctx->c_tombstone_gc_worker_cond);
    pthread_mutex_unlock(&ctx->c_tombstone_gc_worker_lock);
    pthread_join(ctx->c_tombstone_gc_worker_tid, NULL);
    ctx->c_tombstone_gc_worker_running = 0;
  }

  if (ctx->c_tombstone_gc_worker_lock_init)
  {
    pthread_cond_destroy(&ctx->c_tombstone_gc_worker_cond);
    pthread_mutex_destroy(&ctx->c_tombstone_gc_worker_lock);
    ctx->c_tombstone_gc_worker_lock_init = 0;
  }
}

static inline uint32_t kafs_u32_min(uint32_t a, uint32_t b) { return (a < b) ? a : b; }

static void kafs_bg_dedup_worker_adjust_priority_locked(kafs_context_t *ctx, int pressure_mode)
{
  if (!ctx || !ctx->c_bg_dedup_worker_running)
    return;

  uint32_t target_mode = ctx->c_bg_dedup_worker_prio_base_mode;
  int target_nice = ctx->c_bg_dedup_worker_nice_base;
  if (pressure_mode)
  {
    target_mode = KAFS_PENDING_WORKER_PRIO_NORMAL;
    target_nice = 0;
  }

  if (ctx->c_bg_dedup_worker_prio_mode != target_mode || ctx->c_bg_dedup_worker_nice != target_nice)
  {
    ctx->c_bg_dedup_worker_prio_mode = target_mode;
    ctx->c_bg_dedup_worker_nice = target_nice;
    ctx->c_bg_dedup_worker_prio_dirty = 1;
  }
  ctx->c_bg_dedup_worker_auto_boosted = pressure_mode ? 1u : 0u;
}

typedef struct kafs_bg_dedup_worker_plan
{
  uint32_t sleep_ms;
  int run_scan;
  int pressure_mode;
  int apply_prio;
  uint32_t mode;
} kafs_bg_dedup_worker_plan_t;

static void kafs_bg_dedup_worker_plan_locked(kafs_context_t *ctx, uint64_t now_ns,
                                             kafs_bg_dedup_worker_plan_t *plan)
{
  plan->sleep_ms = ctx->c_bg_dedup_quiet_interval_ms;
  plan->run_scan = 0;
  plan->pressure_mode = 0;
  plan->mode = KAFS_BG_DEDUP_MODE_COLD;

  if (ctx->c_bg_dedup_enabled && ctx->c_bg_dedup_interval_ms > 0)
  {
    uint32_t used_pct = kafs_bg_dedup_used_pct(ctx);
    int over_start =
        (ctx->c_bg_dedup_start_used_pct == 0 || used_pct >= ctx->c_bg_dedup_start_used_pct);
    plan->pressure_mode =
        (ctx->c_bg_dedup_pressure_used_pct > 0 && used_pct >= ctx->c_bg_dedup_pressure_used_pct);

    if (plan->pressure_mode)
    {
      plan->mode = KAFS_BG_DEDUP_MODE_PRESSURE;
      plan->run_scan = 1;
      plan->sleep_ms = ctx->c_bg_dedup_pressure_interval_ms;
      ctx->c_bg_dedup_idle_skip_streak = 0;
    }
    else if (!ctx->c_bg_dedup_telemetry_valid)
    {
      if (ctx->c_bg_dedup_cold_start_due_ns == 0)
        ctx->c_bg_dedup_cold_start_due_ns =
            now_ns + (uint64_t)ctx->c_bg_dedup_quiet_interval_ms * 1000000ull;

      if (over_start || now_ns >= ctx->c_bg_dedup_cold_start_due_ns)
      {
        plan->run_scan = 1;
        plan->sleep_ms = ctx->c_bg_dedup_interval_ms;
        ctx->c_bg_dedup_idle_skip_streak = 0;
        ctx->c_bg_dedup_cold_start_due_ns =
            now_ns + (uint64_t)ctx->c_bg_dedup_quiet_interval_ms * 1000000ull;
      }
    }
    else
    {
      int should_run = 0;
      plan->mode = KAFS_BG_DEDUP_MODE_ADAPTIVE;
      if (ctx->c_bg_dedup_last_direct_candidates > 0 || ctx->c_bg_dedup_last_replacements > 0)
        should_run = 1;
      else
      {
        ctx->c_bg_dedup_idle_skip_streak++;
        if (ctx->c_bg_dedup_idle_skip_streak >= KAFS_BG_DEDUP_RESAMPLE_QUIET_CYCLES)
          should_run = 1;
      }

      if (should_run)
      {
        plan->run_scan = 1;
        plan->sleep_ms = ctx->c_bg_dedup_interval_ms;
        ctx->c_bg_dedup_idle_skip_streak = 0;
      }
    }

    kafs_bg_dedup_worker_adjust_priority_locked(ctx, plan->pressure_mode);
  }

  ctx->c_bg_dedup_mode = plan->mode;
  plan->apply_prio = (ctx->c_bg_dedup_worker_prio_dirty != 0);
}

static void kafs_bg_dedup_worker_run_scan(kafs_context_t *ctx, int pressure_mode)
{
  uint64_t before_steps = __atomic_load_n(&ctx->c_stat_bg_dedup_steps, __ATOMIC_RELAXED);
  uint64_t before_scanned = __atomic_load_n(&ctx->c_stat_bg_dedup_scanned_blocks, __ATOMIC_RELAXED);
  uint64_t before_candidates =
      __atomic_load_n(&ctx->c_stat_bg_dedup_direct_candidates, __ATOMIC_RELAXED);
  uint64_t before_repl = __atomic_load_n(&ctx->c_stat_bg_dedup_replacements, __ATOMIC_RELAXED);
  uint32_t steps = pressure_mode ? KAFS_BG_DEDUP_PRESSURE_BURST_STEPS : 1u;

  for (uint32_t i = 0; i < steps; ++i)
    kafs_bg_dedup_step(ctx, pressure_mode);

  uint64_t after_steps = __atomic_load_n(&ctx->c_stat_bg_dedup_steps, __ATOMIC_RELAXED);
  uint64_t after_scanned = __atomic_load_n(&ctx->c_stat_bg_dedup_scanned_blocks, __ATOMIC_RELAXED);
  uint64_t after_candidates =
      __atomic_load_n(&ctx->c_stat_bg_dedup_direct_candidates, __ATOMIC_RELAXED);
  uint64_t after_repl = __atomic_load_n(&ctx->c_stat_bg_dedup_replacements, __ATOMIC_RELAXED);

  if (after_steps > before_steps)
  {
    pthread_mutex_lock(&ctx->c_bg_dedup_worker_lock);
    ctx->c_bg_dedup_telemetry_valid = 1u;
    ctx->c_bg_dedup_last_scanned_blocks =
        (after_scanned >= before_scanned) ? (after_scanned - before_scanned) : 0u;
    ctx->c_bg_dedup_last_direct_candidates =
        (after_candidates >= before_candidates) ? (after_candidates - before_candidates) : 0u;
    ctx->c_bg_dedup_last_replacements =
        (after_repl >= before_repl) ? (after_repl - before_repl) : 0u;
    pthread_mutex_unlock(&ctx->c_bg_dedup_worker_lock);
  }
}

static int kafs_bg_dedup_worker_wait(kafs_context_t *ctx, int pressure_mode, uint32_t sleep_ms)
{
  uint32_t wait_ms =
      pressure_mode ? sleep_ms : kafs_u32_min(sleep_ms, KAFS_BG_DEDUP_MONITOR_INTERVAL_MS);
  if (wait_ms == 0)
    wait_ms = 1;

  pthread_mutex_lock(&ctx->c_bg_dedup_worker_lock);
  if (ctx->c_bg_dedup_worker_stop)
  {
    pthread_mutex_unlock(&ctx->c_bg_dedup_worker_lock);
    return 1;
  }

  struct timespec wake;
  clock_gettime(CLOCK_REALTIME, &wake);
  wake.tv_nsec += (long)(wait_ms % 1000u) * 1000000l;
  wake.tv_sec += (time_t)(wait_ms / 1000u);
  if (wake.tv_nsec >= 1000000000l)
  {
    wake.tv_sec += 1;
    wake.tv_nsec -= 1000000000l;
  }

  (void)pthread_cond_timedwait(&ctx->c_bg_dedup_worker_cond, &ctx->c_bg_dedup_worker_lock, &wake);
  pthread_mutex_unlock(&ctx->c_bg_dedup_worker_lock);
  return 0;
}

static void *kafs_bg_dedup_worker_main(void *arg)
{
  kafs_context_t *ctx = (kafs_context_t *)arg;
  if (!ctx)
    return NULL;

  (void)kafs_bg_dedup_worker_apply_priority_self(ctx);

  for (;;)
  {
    kafs_bg_dedup_worker_plan_t plan;
    uint64_t now_ns = kafs_now_ns();

    pthread_mutex_lock(&ctx->c_bg_dedup_worker_lock);
    if (ctx->c_bg_dedup_worker_stop)
    {
      pthread_mutex_unlock(&ctx->c_bg_dedup_worker_lock);
      return NULL;
    }

    kafs_bg_dedup_worker_plan_locked(ctx, now_ns, &plan);
    pthread_mutex_unlock(&ctx->c_bg_dedup_worker_lock);

    if (plan.apply_prio)
      (void)kafs_bg_dedup_worker_apply_priority_self(ctx);

    if (plan.run_scan)
      kafs_bg_dedup_worker_run_scan(ctx, plan.pressure_mode);

    if (kafs_bg_dedup_worker_wait(ctx, plan.pressure_mode, plan.sleep_ms) != 0)
      return NULL;
  }
}

static int kafs_bg_dedup_worker_start(struct kafs_context *ctx)
{
  if (!ctx || !ctx->c_bg_dedup_enabled)
    return 0;
  if (ctx->c_bg_dedup_worker_running)
    return 0;

  if (!ctx->c_bg_dedup_worker_lock_init)
  {
    if (pthread_mutex_init(&ctx->c_bg_dedup_worker_lock, NULL) != 0)
      return -EIO;
    if (pthread_cond_init(&ctx->c_bg_dedup_worker_cond, NULL) != 0)
    {
      pthread_mutex_destroy(&ctx->c_bg_dedup_worker_lock);
      return -EIO;
    }
    ctx->c_bg_dedup_worker_lock_init = 1;
  }

  ctx->c_bg_dedup_worker_stop = 0;
  int prc = pthread_create(&ctx->c_bg_dedup_worker_tid, NULL, kafs_bg_dedup_worker_main, ctx);
  if (prc != 0)
    return -prc;

  ctx->c_bg_dedup_worker_running = 1;
  return 0;
}

static void kafs_bg_dedup_worker_stop(struct kafs_context *ctx)
{
  if (!ctx)
    return;

  if (ctx->c_bg_dedup_worker_running)
  {
    pthread_mutex_lock(&ctx->c_bg_dedup_worker_lock);
    ctx->c_bg_dedup_worker_stop = 1;
    pthread_cond_broadcast(&ctx->c_bg_dedup_worker_cond);
    pthread_mutex_unlock(&ctx->c_bg_dedup_worker_lock);
    pthread_join(ctx->c_bg_dedup_worker_tid, NULL);
    ctx->c_bg_dedup_worker_running = 0;
  }

  if (ctx->c_bg_dedup_worker_lock_init)
  {
    pthread_cond_destroy(&ctx->c_bg_dedup_worker_cond);
    pthread_mutex_destroy(&ctx->c_bg_dedup_worker_lock);
    ctx->c_bg_dedup_worker_lock_init = 0;
  }
}

// ---------------------------------------------------------
// BLOCK OPERATIONS
// ---------------------------------------------------------

/// @brief ブロック単位でデータを読み出す
/// @param ctx コンテキスト
/// @param blo ブロック番号
/// @param buf 読み出すバッファ
/// @return 0: 成功, < 0: 失敗 (-errno)
// cppcheck-suppress constParameterCallback
static int kafs_blk_read(struct kafs_context *ctx, kafs_blkcnt_t blo, void *buf)
{
  static uint32_t s_invalid_blkref_bt_emitted = 0;
  kafs_dlog(3, "%s(blo = %" PRIuFAST32 ")\n", __func__, blo);
  assert(ctx != NULL);
  assert(buf != NULL);
  kafs_blkcnt_t max_blo = kafs_sb_r_blkcnt_get(ctx->c_superblock);
  if (blo != KAFS_BLO_NONE && blo >= max_blo)
  {
    kafs_log(KAFS_LOG_ERR, "%s: invalid block ref blo=%" PRIuFAST32 " (max=%" PRIuFAST32 ")\n",
             __func__, blo, max_blo);
#ifdef __linux__
    uint32_t c = __atomic_fetch_add(&s_invalid_blkref_bt_emitted, 1u, __ATOMIC_RELAXED);
    if (c < 3u)
    {
      void *bt[20];
      int n = backtrace(bt, (int)(sizeof(bt) / sizeof(bt[0])));
      char **syms = backtrace_symbols(bt, n);
      if (syms)
      {
        for (int i = 0; i < n; ++i)
          kafs_log(KAFS_LOG_ERR, "  invalid-blo bt[%d]=%s\n", i, syms[i]);
        free(syms);
      }
    }
#endif
    return -EIO;
  }
  kafs_logblksize_t log_blksize = kafs_sb_log_blksize_get(ctx->c_superblock);
  kafs_blksize_t blksize = kafs_sb_blksize_get(ctx->c_superblock);
  if (blo == KAFS_BLO_NONE)
  {
    memset(buf, 0, blksize);
    return KAFS_SUCCESS;
  }
  off_t off = (off_t)blo << log_blksize;
  if ((size_t)off + (size_t)blksize > ctx->c_img_size)
    return -EIO;
  memcpy(buf, kafs_img_ptr(ctx, off, (size_t)blksize), (size_t)blksize);
  return KAFS_SUCCESS;
}

static void kafs_diag_log_live_dir_block0_write(struct kafs_context *ctx, kafs_blkcnt_t blo,
                                                const void *buf, size_t len)
{
#if KAFS_ENABLE_EXTRA_DIAG
  if (!ctx || !buf || blo == KAFS_BLO_NONE || !kafs_extra_diag_enabled())
    return;

  kafs_inocnt_t inocnt = kafs_sb_inocnt_get(ctx->c_superblock);
  for (kafs_inocnt_t ino = KAFS_INO_ROOTDIR; ino < inocnt; ++ino)
  {
    kafs_sinode_t *inoent = kafs_ctx_inode(ctx, ino);
    if (!kafs_ino_get_usage(inoent))
      continue;
    kafs_mode_t mode = kafs_ino_mode_get(inoent);
    if (!S_ISDIR(mode))
      continue;
    if (kafs_ino_size_get(inoent) <= KAFS_INODE_DIRECT_BYTES)
      continue;
    kafs_blkcnt_t cur_ref = kafs_blkcnt_stoh(inoent->i_blkreftbl[0]);
    if (cur_ref != blo)
      continue;

    char hex_sample[3 * 16 + 1];
    char ascii_sample[16 + 1];
    kafs_diag_format_sample(buf, len, hex_sample, sizeof(hex_sample), ascii_sample,
                            sizeof(ascii_sample));
    kafs_log(KAFS_LOG_WARNING,
             "%s: blk=%" PRIuFAST32 " matches live dir block0 ino=%" PRIuFAST32
             " mode=%o size=%" PRIuFAST64 " sample_hex=%s sample_ascii='%s'\n",
             __func__, (uint_fast32_t)blo, (uint_fast32_t)ino, (unsigned)mode,
             (uint_fast64_t)kafs_ino_size_get(inoent), hex_sample[0] ? hex_sample : "-",
             ascii_sample[0] ? ascii_sample : "-");
    kafs_diag_appendf(ctx,
                      "%s: blk=%" PRIuFAST32 " matches live dir block0 ino=%" PRIuFAST32
                      " mode=%o size=%" PRIuFAST64 " src_ino=%" PRIuFAST32
                      " src_path=%s sample_hex=%s sample_ascii='%s'\n",
                      __func__, (uint_fast32_t)blo, (uint_fast32_t)ino, (unsigned)mode,
                      (uint_fast64_t)kafs_ino_size_get(inoent), (uint_fast32_t)g_diag_write_ino,
                      g_diag_write_path ? g_diag_write_path : "(null)",
                      hex_sample[0] ? hex_sample : "-", ascii_sample[0] ? ascii_sample : "-");
  }
#else
  (void)ctx;
  (void)blo;
  (void)buf;
  (void)len;
#endif
}

/// @brief ブロック単位でデータを書き込む
/// @param ctx コンテキスト
/// @param blo ブロック番号へのポインタ
/// @param buf 書き込むバッファ
/// @return 0: 成功, < 0: 失敗 (-errno)
// cppcheck-suppress constParameterCallback
static int kafs_blk_write(struct kafs_context *ctx, kafs_blkcnt_t blo, const void *buf)
{
  kafs_dlog(3, "%s(blo = %" PRIuFAST32 ")\n", __func__, blo);
  assert(ctx != NULL);
  assert(buf != NULL);
  assert(blo != KAFS_INO_NONE);
  kafs_blkcnt_t max_blo = kafs_sb_r_blkcnt_get(ctx->c_superblock);
  if (blo != KAFS_BLO_NONE && blo >= max_blo)
  {
    kafs_log(KAFS_LOG_ERR, "%s: invalid block ref blo=%" PRIuFAST32 " (max=%" PRIuFAST32 ")\n",
             __func__, blo, max_blo);
    return -EIO;
  }
  kafs_logblksize_t log_blksize = kafs_sb_log_blksize_get(ctx->c_superblock);
  kafs_blksize_t blksize = kafs_sb_blksize_get(ctx->c_superblock);
  if (blo == KAFS_BLO_NONE)
    return KAFS_SUCCESS;
  off_t off = (off_t)blo << log_blksize;
  if ((size_t)off + (size_t)blksize > ctx->c_img_size)
    return -EIO;
  kafs_diag_log_live_dir_block0_write(ctx, blo, buf, (size_t)blksize);
  memcpy(kafs_img_ptr(ctx, off, (size_t)blksize), buf, (size_t)blksize);
  return KAFS_SUCCESS;
}

/// @brief ブロックデータを未使用に変更する
/// @param ctx コンテキスト
/// @param pblo ブロック番号へのポインタ
/// @return 0: 成功, < 0: 失敗 (-errno)
// kafs_blk_release は DEL 経路廃止に伴い不要になったため削除しました

// ---------------------------------------------------------
// INODE BLOCK OPERATIONS
// ---------------------------------------------------------

static int kafs_blk_is_zero(const void *buf, size_t len)
{
  const char *c = buf;
  while (len--)
    if (*c++)
      return 0;
  return 1;
}

static void kafs_ino_blocks_adjust(kafs_sinode_t *inoent, int delta)
{
  kafs_blkcnt_t cur = kafs_ino_blocks_get(inoent);
  if (delta > 0)
  {
    if (cur < UINT32_MAX)
      kafs_ino_blocks_set(inoent, cur + 1);
    return;
  }
  if (delta < 0)
  {
    if (cur > 0)
      kafs_ino_blocks_set(inoent, cur - 1);
  }
}

static int kafs_ino_ibrk_run_direct(struct kafs_context *ctx, kafs_sinode_t *inoent,
                                    kafs_iblkcnt_t iblo, kafs_iblkcnt_t iblo_orig,
                                    kafs_blkcnt_t *pblo, kafs_iblkref_func_t ifunc)
{
  kafs_blkcnt_t blo_data_raw = kafs_blkcnt_stoh(inoent->i_blkreftbl[iblo]);
  kafs_blkcnt_t blo_data = KAFS_BLO_NONE;

  switch (ifunc)
  {
  case KAFS_IBLKREF_FUNC_GET_RAW:
    *pblo = blo_data_raw;
    return KAFS_SUCCESS;

  case KAFS_IBLKREF_FUNC_GET:
    KAFS_CALL(kafs_ref_resolve_data_blo, ctx, blo_data_raw, &blo_data);
    *pblo = blo_data;
    return KAFS_SUCCESS;

  case KAFS_IBLKREF_FUNC_PUT:
    KAFS_CALL(kafs_ref_resolve_data_blo, ctx, blo_data_raw, &blo_data);
    if (blo_data == KAFS_BLO_NONE)
    {
      KAFS_CALL(kafs_blk_alloc, ctx, &blo_data);
      inoent->i_blkreftbl[iblo] = kafs_blkcnt_htos(blo_data);
    }
    *pblo = blo_data;
    return KAFS_SUCCESS;

  case KAFS_IBLKREF_FUNC_SET:
    kafs_diag_log_dir_ref_set("ibrk_set_direct", ctx, inoent, iblo_orig, blo_data_raw, *pblo);
    if (blo_data_raw == KAFS_BLO_NONE && *pblo != KAFS_BLO_NONE)
      kafs_ino_blocks_adjust(inoent, +1);
    else if (blo_data_raw != KAFS_BLO_NONE && *pblo == KAFS_BLO_NONE)
      kafs_ino_blocks_adjust(inoent, -1);
    inoent->i_blkreftbl[iblo] = kafs_blkcnt_htos(*pblo);
    return KAFS_SUCCESS;
  }

  return KAFS_SUCCESS;
}

static int kafs_ino_ibrk_run_single(struct kafs_context *ctx, kafs_sinode_t *inoent,
                                    kafs_iblkcnt_t iblo, kafs_iblkcnt_t iblo_orig,
                                    kafs_blkcnt_t *pblo, kafs_iblkref_func_t ifunc,
                                    kafs_blksize_t blksize, kafs_blkcnt_t blkrefs_pb)
{
  kafs_sblkcnt_t blkreftbl[blkrefs_pb];
  kafs_blkcnt_t blo_blkreftbl = kafs_blkcnt_stoh(inoent->i_blkreftbl[12]);
  kafs_blkcnt_t blo_data;

  kafs_dlog(3,
            "ibrk_run: single-indirect idx=%" PRIuFAST32 ", blkrefs_pb=%" PRIuFAST32
            ", tbl_blo=%" PRIuFAST32 "\n",
            iblo, blkrefs_pb, blo_blkreftbl);

  switch (ifunc)
  {
  case KAFS_IBLKREF_FUNC_GET_RAW:
    if (blo_blkreftbl == KAFS_BLO_NONE)
    {
      *pblo = KAFS_BLO_NONE;
      return KAFS_SUCCESS;
    }
    KAFS_CALL(kafs_blk_read, ctx, blo_blkreftbl, blkreftbl);
    *pblo = kafs_blkcnt_stoh(blkreftbl[iblo]);
    return KAFS_SUCCESS;

  case KAFS_IBLKREF_FUNC_GET:
    if (blo_blkreftbl == KAFS_BLO_NONE)
    {
      *pblo = KAFS_BLO_NONE;
      return KAFS_SUCCESS;
    }
    KAFS_CALL(kafs_blk_read, ctx, blo_blkreftbl, blkreftbl);
    KAFS_CALL(kafs_ref_resolve_data_blo, ctx, kafs_blkcnt_stoh(blkreftbl[iblo]), pblo);
    return KAFS_SUCCESS;

  case KAFS_IBLKREF_FUNC_PUT:
    if (blo_blkreftbl == KAFS_BLO_NONE)
    {
      KAFS_CALL(kafs_blk_alloc, ctx, &blo_blkreftbl);
      inoent->i_blkreftbl[12] = kafs_blkcnt_htos(blo_blkreftbl);
      memset(blkreftbl, 0, blksize);
      blo_data = KAFS_BLO_NONE;
    }
    else
    {
      KAFS_CALL(kafs_blk_read, ctx, blo_blkreftbl, blkreftbl);
      KAFS_CALL(kafs_ref_resolve_data_blo, ctx, kafs_blkcnt_stoh(blkreftbl[iblo]), &blo_data);
    }
    if (blo_data == KAFS_BLO_NONE)
    {
      KAFS_CALL(kafs_blk_alloc, ctx, &blo_data);
      blkreftbl[iblo] = kafs_blkcnt_htos(blo_data);
      KAFS_CALL(kafs_blk_write, ctx, blo_blkreftbl, blkreftbl);
    }
    *pblo = blo_data;
    return KAFS_SUCCESS;

  case KAFS_IBLKREF_FUNC_SET:
    if (blo_blkreftbl == KAFS_BLO_NONE)
    {
      KAFS_CALL(kafs_blk_alloc, ctx, &blo_blkreftbl);
      inoent->i_blkreftbl[12] = kafs_blkcnt_htos(blo_blkreftbl);
      kafs_ino_blocks_adjust(inoent, +1);
      memset(blkreftbl, 0, blksize);
    }
    else
    {
      KAFS_CALL(kafs_blk_read, ctx, blo_blkreftbl, blkreftbl);
    }
    blo_data = kafs_blkcnt_stoh(blkreftbl[iblo]);
    kafs_diag_log_dir_ref_set("ibrk_set_single", ctx, inoent, iblo_orig, blo_data, *pblo);
    if (blo_data == KAFS_BLO_NONE && *pblo != KAFS_BLO_NONE)
      kafs_ino_blocks_adjust(inoent, +1);
    else if (blo_data != KAFS_BLO_NONE && *pblo == KAFS_BLO_NONE)
      kafs_ino_blocks_adjust(inoent, -1);
    blkreftbl[iblo] = kafs_blkcnt_htos(*pblo);
    KAFS_CALL(kafs_blk_write, ctx, blo_blkreftbl, blkreftbl);
    return KAFS_SUCCESS;
  }

  return KAFS_SUCCESS;
}

static int kafs_ino_ibrk_run_double(struct kafs_context *ctx, kafs_sinode_t *inoent,
                                    kafs_iblkcnt_t iblo, kafs_iblkcnt_t iblo_orig,
                                    kafs_blkcnt_t *pblo, kafs_iblkref_func_t ifunc,
                                    kafs_blksize_t blksize, kafs_logblksize_t log_blkrefs_pb,
                                    kafs_blkcnt_t blkrefs_pb)
{
  kafs_sblkcnt_t blkreftbl1[blkrefs_pb];
  kafs_sblkcnt_t blkreftbl2[blkrefs_pb];
  kafs_blkcnt_t blo_blkreftbl1 = kafs_blkcnt_stoh(inoent->i_blkreftbl[13]);
  kafs_blkcnt_t blo_blkreftbl2;
  kafs_blkcnt_t blo_data;
  kafs_blkcnt_t iblo1 = iblo >> log_blkrefs_pb;
  kafs_blkcnt_t iblo2 = iblo & (blkrefs_pb - 1);

  switch (ifunc)
  {
  case KAFS_IBLKREF_FUNC_GET_RAW:
    if (blo_blkreftbl1 == KAFS_BLO_NONE)
    {
      *pblo = KAFS_BLO_NONE;
      return KAFS_SUCCESS;
    }
    KAFS_CALL(kafs_blk_read, ctx, blo_blkreftbl1, blkreftbl1);
    blo_blkreftbl2 = kafs_blkcnt_stoh(blkreftbl1[iblo1]);
    if (blo_blkreftbl2 == KAFS_BLO_NONE)
    {
      *pblo = KAFS_BLO_NONE;
      return KAFS_SUCCESS;
    }
    KAFS_CALL(kafs_blk_read, ctx, blo_blkreftbl2, blkreftbl2);
    *pblo = kafs_blkcnt_stoh(blkreftbl2[iblo2]);
    return KAFS_SUCCESS;

  case KAFS_IBLKREF_FUNC_GET:
    if (blo_blkreftbl1 == KAFS_BLO_NONE)
    {
      *pblo = KAFS_BLO_NONE;
      return KAFS_SUCCESS;
    }
    KAFS_CALL(kafs_blk_read, ctx, blo_blkreftbl1, blkreftbl1);
    blo_blkreftbl2 = kafs_blkcnt_stoh(blkreftbl1[iblo1]);
    if (blo_blkreftbl2 == KAFS_BLO_NONE)
    {
      *pblo = KAFS_BLO_NONE;
      return KAFS_SUCCESS;
    }
    KAFS_CALL(kafs_blk_read, ctx, blo_blkreftbl2, blkreftbl2);
    KAFS_CALL(kafs_ref_resolve_data_blo, ctx, kafs_blkcnt_stoh(blkreftbl2[iblo2]), pblo);
    return KAFS_SUCCESS;

  case KAFS_IBLKREF_FUNC_PUT:
    if (blo_blkreftbl1 == KAFS_BLO_NONE)
    {
      KAFS_CALL(kafs_blk_alloc, ctx, &blo_blkreftbl1);
      inoent->i_blkreftbl[13] = kafs_blkcnt_htos(blo_blkreftbl1);
      memset(blkreftbl1, 0, blksize);
      blo_blkreftbl2 = KAFS_BLO_NONE;
    }
    else
    {
      KAFS_CALL(kafs_blk_read, ctx, blo_blkreftbl1, blkreftbl1);
      blo_blkreftbl2 = kafs_blkcnt_stoh(blkreftbl1[iblo1]);
    }
    if (blo_blkreftbl2 == KAFS_BLO_NONE)
    {
      KAFS_CALL(kafs_blk_alloc, ctx, &blo_blkreftbl2);
      blkreftbl1[iblo1] = kafs_blkcnt_htos(blo_blkreftbl2);
      KAFS_CALL(kafs_blk_write, ctx, blo_blkreftbl1, blkreftbl1);
      memset(blkreftbl2, 0, blksize);
      blo_data = KAFS_BLO_NONE;
    }
    else
    {
      KAFS_CALL(kafs_blk_read, ctx, blo_blkreftbl2, blkreftbl2);
      KAFS_CALL(kafs_ref_resolve_data_blo, ctx, kafs_blkcnt_stoh(blkreftbl2[iblo2]), &blo_data);
    }
    if (blo_data == KAFS_BLO_NONE)
    {
      KAFS_CALL(kafs_blk_alloc, ctx, &blo_data);
      blkreftbl2[iblo2] = kafs_blkcnt_htos(blo_data);
      KAFS_CALL(kafs_blk_write, ctx, blo_blkreftbl2, blkreftbl2);
    }
    *pblo = blo_data;
    return KAFS_SUCCESS;

  case KAFS_IBLKREF_FUNC_SET:
    if (blo_blkreftbl1 == KAFS_BLO_NONE)
    {
      KAFS_CALL(kafs_blk_alloc, ctx, &blo_blkreftbl1);
      inoent->i_blkreftbl[13] = kafs_blkcnt_htos(blo_blkreftbl1);
      kafs_ino_blocks_adjust(inoent, +1);
      memset(blkreftbl1, 0, blksize);
      blo_blkreftbl2 = KAFS_BLO_NONE;
    }
    else
    {
      KAFS_CALL(kafs_blk_read, ctx, blo_blkreftbl1, blkreftbl1);
      blo_blkreftbl2 = kafs_blkcnt_stoh(blkreftbl1[iblo1]);
    }
    if (blo_blkreftbl2 == KAFS_BLO_NONE)
    {
      KAFS_CALL(kafs_blk_alloc, ctx, &blo_blkreftbl2);
      blkreftbl1[iblo1] = kafs_blkcnt_htos(blo_blkreftbl2);
      KAFS_CALL(kafs_blk_write, ctx, blo_blkreftbl1, blkreftbl1);
      kafs_ino_blocks_adjust(inoent, +1);
      memset(blkreftbl2, 0, blksize);
    }
    else
    {
      KAFS_CALL(kafs_blk_read, ctx, blo_blkreftbl2, blkreftbl2);
    }
    blo_data = kafs_blkcnt_stoh(blkreftbl2[iblo2]);
    kafs_diag_log_dir_ref_set("ibrk_set_double", ctx, inoent, iblo_orig, blo_data, *pblo);
    if (blo_data == KAFS_BLO_NONE && *pblo != KAFS_BLO_NONE)
      kafs_ino_blocks_adjust(inoent, +1);
    else if (blo_data != KAFS_BLO_NONE && *pblo == KAFS_BLO_NONE)
      kafs_ino_blocks_adjust(inoent, -1);
    blkreftbl2[iblo2] = kafs_blkcnt_htos(*pblo);
    KAFS_CALL(kafs_blk_write, ctx, blo_blkreftbl2, blkreftbl2);
    return KAFS_SUCCESS;
  }

  return KAFS_SUCCESS;
}

static int kafs_ino_ibrk_run_triple(struct kafs_context *ctx, kafs_sinode_t *inoent,
                                    kafs_iblkcnt_t iblo, kafs_iblkcnt_t iblo_orig,
                                    kafs_blkcnt_t *pblo, kafs_iblkref_func_t ifunc,
                                    kafs_blksize_t blksize, kafs_logblksize_t log_blkrefs_pb,
                                    kafs_logblksize_t log_blkrefs_pb_sq, kafs_blkcnt_t blkrefs_pb)
{
  kafs_sblkcnt_t blkreftbl1[blkrefs_pb];
  kafs_sblkcnt_t blkreftbl2[blkrefs_pb];
  kafs_sblkcnt_t blkreftbl3[blkrefs_pb];
  kafs_blkcnt_t blo_blkreftbl1 = kafs_blkcnt_stoh(inoent->i_blkreftbl[14]);
  kafs_blkcnt_t blo_blkreftbl2;
  kafs_blkcnt_t blo_blkreftbl3;
  kafs_blkcnt_t blo_data;
  kafs_blkcnt_t iblo1 = iblo >> log_blkrefs_pb_sq;
  kafs_blkcnt_t iblo2 = (iblo >> log_blkrefs_pb) & (blkrefs_pb - 1);
  kafs_blkcnt_t iblo3 = iblo & (blkrefs_pb - 1);

  switch (ifunc)
  {
  case KAFS_IBLKREF_FUNC_GET_RAW:
    if (blo_blkreftbl1 == KAFS_BLO_NONE)
    {
      *pblo = KAFS_BLO_NONE;
      return KAFS_SUCCESS;
    }
    KAFS_CALL(kafs_blk_read, ctx, blo_blkreftbl1, blkreftbl1);
    blo_blkreftbl2 = kafs_blkcnt_stoh(blkreftbl1[iblo1]);
    if (blo_blkreftbl2 == KAFS_BLO_NONE)
    {
      *pblo = KAFS_BLO_NONE;
      return KAFS_SUCCESS;
    }
    KAFS_CALL(kafs_blk_read, ctx, blo_blkreftbl2, blkreftbl2);
    blo_blkreftbl3 = kafs_blkcnt_stoh(blkreftbl2[iblo2]);
    if (blo_blkreftbl3 == KAFS_BLO_NONE)
    {
      *pblo = KAFS_BLO_NONE;
      return KAFS_SUCCESS;
    }
    KAFS_CALL(kafs_blk_read, ctx, blo_blkreftbl3, blkreftbl3);
    *pblo = kafs_blkcnt_stoh(blkreftbl3[iblo3]);
    return KAFS_SUCCESS;

  case KAFS_IBLKREF_FUNC_GET:
    if (blo_blkreftbl1 == KAFS_BLO_NONE)
    {
      *pblo = KAFS_BLO_NONE;
      return KAFS_SUCCESS;
    }
    KAFS_CALL(kafs_blk_read, ctx, blo_blkreftbl1, blkreftbl1);
    blo_blkreftbl2 = kafs_blkcnt_stoh(blkreftbl1[iblo1]);
    if (blo_blkreftbl2 == KAFS_BLO_NONE)
    {
      *pblo = KAFS_BLO_NONE;
      return KAFS_SUCCESS;
    }
    KAFS_CALL(kafs_blk_read, ctx, blo_blkreftbl2, blkreftbl2);
    blo_blkreftbl3 = kafs_blkcnt_stoh(blkreftbl2[iblo2]);
    if (blo_blkreftbl3 == KAFS_BLO_NONE)
    {
      *pblo = KAFS_BLO_NONE;
      return KAFS_SUCCESS;
    }
    KAFS_CALL(kafs_blk_read, ctx, blo_blkreftbl3, blkreftbl3);
    KAFS_CALL(kafs_ref_resolve_data_blo, ctx, kafs_blkcnt_stoh(blkreftbl3[iblo3]), pblo);
    return KAFS_SUCCESS;

  case KAFS_IBLKREF_FUNC_PUT:
    if (blo_blkreftbl1 == KAFS_BLO_NONE)
    {
      KAFS_CALL(kafs_blk_alloc, ctx, &blo_blkreftbl1);
      inoent->i_blkreftbl[14] = kafs_blkcnt_htos(blo_blkreftbl1);
      memset(blkreftbl1, 0, blksize);
      blo_blkreftbl2 = KAFS_BLO_NONE;
    }
    else
    {
      KAFS_CALL(kafs_blk_read, ctx, blo_blkreftbl1, blkreftbl1);
      blo_blkreftbl2 = kafs_blkcnt_stoh(blkreftbl1[iblo1]);
    }
    if (blo_blkreftbl2 == KAFS_BLO_NONE)
    {
      KAFS_CALL(kafs_blk_alloc, ctx, &blo_blkreftbl2);
      blkreftbl1[iblo1] = kafs_blkcnt_htos(blo_blkreftbl2);
      KAFS_CALL(kafs_blk_write, ctx, blo_blkreftbl1, blkreftbl1);
      memset(blkreftbl2, 0, blksize);
      blo_blkreftbl3 = KAFS_BLO_NONE;
    }
    else
    {
      KAFS_CALL(kafs_blk_read, ctx, blo_blkreftbl2, blkreftbl2);
      blo_blkreftbl3 = kafs_blkcnt_stoh(blkreftbl2[iblo2]);
    }
    if (blo_blkreftbl3 == KAFS_BLO_NONE)
    {
      KAFS_CALL(kafs_blk_alloc, ctx, &blo_blkreftbl3);
      blkreftbl2[iblo2] = kafs_blkcnt_htos(blo_blkreftbl3);
      KAFS_CALL(kafs_blk_write, ctx, blo_blkreftbl2, blkreftbl2);
      memset(blkreftbl3, 0, blksize);
      blo_data = KAFS_BLO_NONE;
    }
    else
    {
      KAFS_CALL(kafs_blk_read, ctx, blo_blkreftbl3, blkreftbl3);
      KAFS_CALL(kafs_ref_resolve_data_blo, ctx, kafs_blkcnt_stoh(blkreftbl3[iblo3]), &blo_data);
    }
    if (blo_data == KAFS_BLO_NONE)
    {
      KAFS_CALL(kafs_blk_alloc, ctx, &blo_data);
      blkreftbl3[iblo3] = kafs_blkcnt_htos(blo_data);
      KAFS_CALL(kafs_blk_write, ctx, blo_blkreftbl3, blkreftbl3);
    }
    *pblo = blo_data;
    return KAFS_SUCCESS;

  case KAFS_IBLKREF_FUNC_SET:
    if (blo_blkreftbl1 == KAFS_BLO_NONE)
    {
      KAFS_CALL(kafs_blk_alloc, ctx, &blo_blkreftbl1);
      inoent->i_blkreftbl[14] = kafs_blkcnt_htos(blo_blkreftbl1);
      kafs_ino_blocks_adjust(inoent, +1);
      memset(blkreftbl1, 0, blksize);
      blo_blkreftbl2 = KAFS_BLO_NONE;
    }
    else
    {
      KAFS_CALL(kafs_blk_read, ctx, blo_blkreftbl1, blkreftbl1);
      blo_blkreftbl2 = kafs_blkcnt_stoh(blkreftbl1[iblo1]);
    }
    if (blo_blkreftbl2 == KAFS_BLO_NONE)
    {
      KAFS_CALL(kafs_blk_alloc, ctx, &blo_blkreftbl2);
      blkreftbl1[iblo1] = kafs_blkcnt_htos(blo_blkreftbl2);
      KAFS_CALL(kafs_blk_write, ctx, blo_blkreftbl1, blkreftbl1);
      kafs_ino_blocks_adjust(inoent, +1);
      memset(blkreftbl2, 0, blksize);
      blo_blkreftbl3 = KAFS_BLO_NONE;
    }
    else
    {
      KAFS_CALL(kafs_blk_read, ctx, blo_blkreftbl2, blkreftbl2);
      blo_blkreftbl3 = kafs_blkcnt_stoh(blkreftbl2[iblo2]);
    }
    if (blo_blkreftbl3 == KAFS_BLO_NONE)
    {
      KAFS_CALL(kafs_blk_alloc, ctx, &blo_blkreftbl3);
      blkreftbl2[iblo2] = kafs_blkcnt_htos(blo_blkreftbl3);
      KAFS_CALL(kafs_blk_write, ctx, blo_blkreftbl2, blkreftbl2);
      kafs_ino_blocks_adjust(inoent, +1);
      memset(blkreftbl3, 0, blksize);
    }
    else
    {
      KAFS_CALL(kafs_blk_read, ctx, blo_blkreftbl3, blkreftbl3);
    }
    blo_data = kafs_blkcnt_stoh(blkreftbl3[iblo3]);
    kafs_diag_log_dir_ref_set("ibrk_set_triple", ctx, inoent, iblo_orig, blo_data, *pblo);
    if (blo_data == KAFS_BLO_NONE && *pblo != KAFS_BLO_NONE)
      kafs_ino_blocks_adjust(inoent, +1);
    else if (blo_data != KAFS_BLO_NONE && *pblo == KAFS_BLO_NONE)
      kafs_ino_blocks_adjust(inoent, -1);
    blkreftbl3[iblo3] = kafs_blkcnt_htos(*pblo);
    KAFS_CALL(kafs_blk_write, ctx, blo_blkreftbl3, blkreftbl3);
    return KAFS_SUCCESS;
  }

  return KAFS_SUCCESS;
}

static int kafs_ino_ibrk_run(struct kafs_context *ctx, kafs_sinode_t *inoent, kafs_iblkcnt_t iblo,
                             kafs_blkcnt_t *pblo, kafs_iblkref_func_t ifunc)
{
  kafs_iblkcnt_t iblo_orig = iblo;
  assert(ctx != NULL);
  assert(pblo != NULL);
  assert(inoent != NULL);
  kafs_dlog(3, "ibrk_run: iblo=%" PRIuFAST32 " ifunc=%d (size=%" PRIuFAST64 ")\n", iblo, (int)ifunc,
            kafs_ino_size_get(inoent));

  if (iblo < 12)
    return kafs_ino_ibrk_run_direct(ctx, inoent, iblo, iblo_orig, pblo, ifunc);

  iblo -= 12;
  kafs_blksize_t blksize = kafs_sb_blksize_get(ctx->c_superblock);
  kafs_logblksize_t log_blkrefs_pb = kafs_sb_log_blkref_pb_get(ctx->c_superblock);
  kafs_blkcnt_t blkrefs_pb = kafs_sb_blkref_pb_get(ctx->c_superblock);
  if (iblo < blkrefs_pb)
    return kafs_ino_ibrk_run_single(ctx, inoent, iblo, iblo_orig, pblo, ifunc, blksize, blkrefs_pb);

  iblo -= blkrefs_pb;
  kafs_logblksize_t log_blkrefs_pb_sq = log_blkrefs_pb << 1;
  kafs_blksize_t blkrefs_pb_sq = 1 << log_blkrefs_pb_sq;
  if (iblo < blkrefs_pb_sq)
    return kafs_ino_ibrk_run_double(ctx, inoent, iblo, iblo_orig, pblo, ifunc, blksize,
                                    log_blkrefs_pb, blkrefs_pb);

  iblo -= blkrefs_pb_sq;
  return kafs_ino_ibrk_run_triple(ctx, inoent, iblo, iblo_orig, pblo, ifunc, blksize,
                                  log_blkrefs_pb, log_blkrefs_pb_sq, blkrefs_pb);
}

// SET(NONE) 実施後に、空になった間接テーブルを親から切り離す。
// - 呼び出しは inode ロック内で行うこと
// - 解放すべきブロック番号（最大3段）を返し、物理解放は呼び出し側がロック外で行う
static int kafs_ino_prune_empty_indirects(struct kafs_context *ctx, kafs_sinode_t *inoent,
                                          kafs_iblkcnt_t iblo, kafs_blkcnt_t *free_blo1,
                                          kafs_blkcnt_t *free_blo2, kafs_blkcnt_t *free_blo3)
{
  assert(ctx != NULL);
  assert(inoent != NULL);
  assert(kafs_ino_get_usage(inoent));
  assert(free_blo1 && free_blo2 && free_blo3);
  *free_blo1 = *free_blo2 = *free_blo3 = KAFS_BLO_NONE;

  if (iblo < 12)
    return KAFS_SUCCESS; // 直接参照は親テーブルなし

  kafs_blksize_t blksize = kafs_sb_blksize_get(ctx->c_superblock);
  kafs_logblksize_t log_blkrefs_pb = kafs_sb_log_blkref_pb_get(ctx->c_superblock);
  kafs_blkcnt_t blkrefs_pb = kafs_sb_blkref_pb_get(ctx->c_superblock);

  kafs_iblkcnt_t rem = iblo - 12;
  if (rem < blkrefs_pb)
  {
    // 単間接
    kafs_blkcnt_t blo_tbl = kafs_blkcnt_stoh(inoent->i_blkreftbl[12]);
    if (blo_tbl == KAFS_BLO_NONE)
      return KAFS_SUCCESS;
    kafs_sblkcnt_t tbl[blkrefs_pb];
    KAFS_CALL(kafs_blk_read, ctx, blo_tbl, tbl);
    if (kafs_blk_is_zero(tbl, blksize))
    {
      *free_blo1 = blo_tbl;
      inoent->i_blkreftbl[12] = kafs_blkcnt_htos(KAFS_BLO_NONE);
      kafs_ino_blocks_adjust(inoent, -1);
    }
    return KAFS_SUCCESS;
  }

  rem -= blkrefs_pb;
  kafs_logblksize_t log_blkrefs_pb_sq = log_blkrefs_pb << 1;
  kafs_blkcnt_t blkrefs_pb_sq = 1u << log_blkrefs_pb_sq;
  if (rem < blkrefs_pb_sq)
  {
    // 二重間接
    kafs_iblkcnt_t ib1 = rem >> log_blkrefs_pb;
    kafs_blkcnt_t blo_tbl1 = kafs_blkcnt_stoh(inoent->i_blkreftbl[13]);
    if (blo_tbl1 == KAFS_BLO_NONE)
      return KAFS_SUCCESS;
    kafs_sblkcnt_t tbl1[blkrefs_pb];
    KAFS_CALL(kafs_blk_read, ctx, blo_tbl1, tbl1);
    kafs_blkcnt_t blo_tbl2 = kafs_blkcnt_stoh(tbl1[ib1]);
    if (blo_tbl2 == KAFS_BLO_NONE)
      return KAFS_SUCCESS;
    kafs_sblkcnt_t tbl2[blkrefs_pb];
    KAFS_CALL(kafs_blk_read, ctx, blo_tbl2, tbl2);
    if (kafs_blk_is_zero(tbl2, blksize))
    {
      *free_blo1 = blo_tbl2;
      tbl1[ib1] = kafs_blkcnt_htos(KAFS_BLO_NONE);
      KAFS_CALL(kafs_blk_write, ctx, blo_tbl1, tbl1);
      // 親も空なら切り離し
      if (kafs_blk_is_zero(tbl1, blksize))
      {
        *free_blo2 = blo_tbl1;
        inoent->i_blkreftbl[13] = kafs_blkcnt_htos(KAFS_BLO_NONE);
        kafs_ino_blocks_adjust(inoent, -1);
      }
      kafs_ino_blocks_adjust(inoent, -1);
    }
    return KAFS_SUCCESS;
  }

  // 三重間接
  rem -= blkrefs_pb_sq;
  kafs_iblkcnt_t ib1 = rem >> log_blkrefs_pb_sq;
  kafs_iblkcnt_t ib2 = (rem >> log_blkrefs_pb) & (blkrefs_pb - 1);
  kafs_blkcnt_t blo_tbl1 = kafs_blkcnt_stoh(inoent->i_blkreftbl[14]);
  if (blo_tbl1 == KAFS_BLO_NONE)
    return KAFS_SUCCESS;
  kafs_sblkcnt_t tbl1[blkrefs_pb];
  KAFS_CALL(kafs_blk_read, ctx, blo_tbl1, tbl1);
  kafs_blkcnt_t blo_tbl2 = kafs_blkcnt_stoh(tbl1[ib1]);
  if (blo_tbl2 == KAFS_BLO_NONE)
    return KAFS_SUCCESS;
  kafs_sblkcnt_t tbl2[blkrefs_pb];
  KAFS_CALL(kafs_blk_read, ctx, blo_tbl2, tbl2);
  kafs_blkcnt_t blo_tbl3 = kafs_blkcnt_stoh(tbl2[ib2]);
  if (blo_tbl3 == KAFS_BLO_NONE)
    return KAFS_SUCCESS;
  kafs_sblkcnt_t tbl3[blkrefs_pb];
  KAFS_CALL(kafs_blk_read, ctx, blo_tbl3, tbl3);
  if (kafs_blk_is_zero(tbl3, blksize))
  {
    *free_blo1 = blo_tbl3;
    tbl2[ib2] = kafs_blkcnt_htos(KAFS_BLO_NONE);
    KAFS_CALL(kafs_blk_write, ctx, blo_tbl2, tbl2);
    if (kafs_blk_is_zero(tbl2, blksize))
    {
      *free_blo2 = blo_tbl2;
      tbl1[ib1] = kafs_blkcnt_htos(KAFS_BLO_NONE);
      KAFS_CALL(kafs_blk_write, ctx, blo_tbl1, tbl1);
      if (kafs_blk_is_zero(tbl1, blksize))
      {
        *free_blo3 = blo_tbl1;
        inoent->i_blkreftbl[14] = kafs_blkcnt_htos(KAFS_BLO_NONE);
        kafs_ino_blocks_adjust(inoent, -1);
      }
      kafs_ino_blocks_adjust(inoent, -1);
    }
    kafs_ino_blocks_adjust(inoent, -1);
  }
  return KAFS_SUCCESS;
}

/// @brief inode毎のデータを読み出す（ブロック単位）
/// @param ctx コンテキスト
/// @param inoent inode テーブルエントリ
/// @param buf バッファ
/// @param iblo ブロック番号
/// @return 0: 成功, < 0: 失敗 (-errno)
static int kafs_ino_iblk_read(struct kafs_context *ctx, kafs_sinode_t *inoent, kafs_iblkcnt_t iblo,
                              void *buf)
{
  kafs_dlog(3, "%s(ino = %d, iblo = %" PRIuFAST32 ")\n", __func__, kafs_ctx_ino_no(ctx, inoent),
            iblo);
  assert(ctx != NULL);
  assert(buf != NULL);
  assert(inoent != NULL);
  assert(kafs_ino_get_usage(inoent));
  kafs_blkcnt_t blo;
  int rc = kafs_ino_ibrk_run(ctx, inoent, iblo, &blo, KAFS_IBLKREF_FUNC_GET);
  if (rc < 0)
    return rc;
  rc = kafs_blk_read(ctx, blo, buf);
  if (rc < 0)
    return rc;
  return KAFS_SUCCESS;
}

// Read one data block and treat an absent block mapping as a sparse hole.
static int kafs_ino_iblk_read_or_zero(struct kafs_context *ctx, kafs_sinode_t *inoent,
                                      kafs_iblkcnt_t iblo, void *buf)
{
  int rc = kafs_ino_iblk_read(ctx, inoent, iblo, buf);
  if (rc == -ENOENT)
  {
    memset(buf, 0, kafs_sb_blksize_get(ctx->c_superblock));
    return 0;
  }
  return rc;
}

static int kafs_ino_iblk_write_legacy(struct kafs_context *ctx, kafs_sinode_t *inoent,
                                      kafs_iblkcnt_t iblo, const void *buf, int record_rescue_hint)
{
  kafs_blkcnt_t new_blo = KAFS_BLO_NONE;
  KAFS_CALL(kafs_blk_alloc, ctx, &new_blo);

  uint64_t t_lw0 = kafs_now_ns();
  KAFS_CALL(kafs_blk_write, ctx, new_blo, buf);
  uint64_t t_lw1 = kafs_now_ns();
  __atomic_add_fetch(&ctx->c_stat_iblk_write_ns_legacy_blk_write, t_lw1 - t_lw0, __ATOMIC_RELAXED);

  kafs_blkcnt_t old_raw = KAFS_BLO_NONE;
  KAFS_CALL(kafs_ino_ibrk_run, ctx, inoent, iblo, &old_raw, KAFS_IBLKREF_FUNC_GET_RAW);
  kafs_diag_log_dir_iblk_write("iblk_write_legacy", ctx, inoent, iblo, old_raw, new_blo, buf,
                               kafs_sb_blksize_get(ctx->c_superblock));
  KAFS_CALL(kafs_ino_ibrk_run, ctx, inoent, iblo, &new_blo, KAFS_IBLKREF_FUNC_SET);

  if (record_rescue_hint)
  {
    kafs_hrl_rescue_recent_note(ctx, kafs_bg_hash64(buf, kafs_sb_blksize_get(ctx->c_superblock)),
                                new_blo);
  }

  if (old_raw != KAFS_BLO_NONE)
  {
    uint32_t ino_idx = (uint32_t)kafs_ctx_ino_no(ctx, inoent);
    kafs_inode_unlock(ctx, ino_idx);
    kafs_blkcnt_t old_blo = KAFS_BLO_NONE;
    if (kafs_ref_resolve_data_blo(ctx, old_raw, &old_blo) == 0 && old_blo != KAFS_BLO_NONE &&
        old_blo != new_blo)
    {
      uint64_t t_dec0 = kafs_now_ns();
      (void)kafs_inode_release_hrl_ref(ctx, old_blo);
      uint64_t t_dec1 = kafs_now_ns();
      __atomic_add_fetch(&ctx->c_stat_iblk_write_ns_dec_ref, t_dec1 - t_dec0, __ATOMIC_RELAXED);
    }
    kafs_inode_lock(ctx, ino_idx);
  }

  return KAFS_SUCCESS;
}

static int kafs_ino_iblk_write_pending(struct kafs_context *ctx, kafs_sinode_t *inoent,
                                       kafs_iblkcnt_t iblo, const void *buf, uint32_t *warned_state)
{
  kafs_blkcnt_t temp_blo = KAFS_BLO_NONE;
  int rc = kafs_blk_alloc(ctx, &temp_blo);
  if (rc < 0)
    return rc;

  uint64_t t_lw0 = kafs_now_ns();
  rc = kafs_blk_write(ctx, temp_blo, buf);
  uint64_t t_lw1 = kafs_now_ns();
  __atomic_add_fetch(&ctx->c_stat_iblk_write_ns_legacy_blk_write, t_lw1 - t_lw0, __ATOMIC_RELAXED);
  if (rc < 0)
    return rc;

  uint32_t ino_idx = (uint32_t)kafs_ctx_ino_no(ctx, inoent);
  kafs_blkcnt_t old_raw = KAFS_BLO_NONE;
  kafs_blkcnt_t old_blo = KAFS_BLO_NONE;
  uint32_t ino_epoch = kafs_inode_epoch_get(ctx, ino_idx);
  rc = kafs_ino_ibrk_run(ctx, inoent, iblo, &old_raw, KAFS_IBLKREF_FUNC_GET_RAW);
  if (rc < 0)
    return rc;
  if (!kafs_ref_is_pending(old_raw))
  {
    rc = kafs_ref_resolve_data_blo(ctx, old_raw, &old_blo);
    if (rc < 0)
      return rc;
  }

  kafs_pendinglog_entry_t ent = {0};
  ent.ino = ino_idx;
  ent.iblk = (uint32_t)iblo;
  ent.ino_epoch = ino_epoch;
  ent.temp_blo = (uint32_t)temp_blo;
  ent.state = KAFS_PENDING_QUEUED;
  ent.target_hrid = (uint32_t)old_blo;
  ent.seq = kafs_now_realtime_ns();

  uint64_t pending_id = 0;
  if (ctx->c_pending_worker_lock_init)
    pthread_mutex_lock(&ctx->c_pending_worker_lock);
  int qrc = kafs_pendinglog_enqueue(ctx, &ent, &pending_id);
  if (ctx->c_pending_worker_lock_init)
  {
    kafs_pending_worker_adjust_priority_locked(ctx);
    pthread_mutex_unlock(&ctx->c_pending_worker_lock);
  }
  if (qrc < 0)
  {
    (void)kafs_inode_release_hrl_ref(ctx, temp_blo);
    if (qrc != -ENOSPC)
      return qrc;

    uint32_t c = __atomic_fetch_add(warned_state, 1u, __ATOMIC_RELAXED);
    if (c < 8u)
    {
      kafs_log(KAFS_LOG_WARNING,
               "%s: pendinglog full (ino=%" PRIu32 ", iblk=%" PRIu32 "), fallback to sync write\n",
               __func__, ino_idx, (uint32_t)iblo);
    }
    return 1;
  }

  kafs_blkcnt_t pref = KAFS_BLO_NONE;
  rc = kafs_ref_pending_encode(pending_id, &pref);
  if (rc < 0)
    return rc;
  kafs_diag_log_dir_iblk_write("iblk_write_pending", ctx, inoent, iblo, old_raw, pref, buf,
                               kafs_sb_blksize_get(ctx->c_superblock));
  rc = kafs_ino_ibrk_run(ctx, inoent, iblo, &pref, KAFS_IBLKREF_FUNC_SET);
  if (rc < 0)
    return rc;

  kafs_pending_worker_notify(ctx);
  return 0;
}

static int kafs_ino_iblk_write_hrl_retry(struct kafs_context *ctx, kafs_sinode_t *inoent,
                                         kafs_iblkcnt_t iblo, const void *buf)
{
  uint32_t ino_idx = (uint32_t)kafs_ctx_ino_no(ctx, inoent);
  for (unsigned retry = 0; retry < 8; ++retry)
  {
    kafs_blkcnt_t expected_old_blo = KAFS_BLO_NONE;
    int rc = kafs_ino_ibrk_run(ctx, inoent, iblo, &expected_old_blo, KAFS_IBLKREF_FUNC_GET);
    if (rc < 0)
      return rc;

    kafs_inode_unlock(ctx, ino_idx);

    kafs_hrid_t hrid = 0;
    int is_new = 0;
    kafs_blkcnt_t candidate_blo = KAFS_BLO_NONE;
    int candidate_kind = 0;

    ctx->c_stat_hrl_put_calls++;
    uint64_t t_hrl0 = kafs_now_ns();
    rc = kafs_hrl_put(ctx, buf, &hrid, &is_new, &candidate_blo);
    uint64_t t_hrl1 = kafs_now_ns();
    __atomic_add_fetch(&ctx->c_stat_iblk_write_ns_hrl_put, t_hrl1 - t_hrl0, __ATOMIC_RELAXED);
    if (rc == 0)
      candidate_kind = 1;
    else if (rc == -ENOSPC)
    {
      int rescue_is_new = 0;
      kafs_blkcnt_t rescue_blo = KAFS_BLO_NONE;
      int rrc = kafs_hrl_try_enospc_rescue(ctx, buf, &rescue_blo, &rescue_is_new);
      if (rrc == 0)
      {
        rc = 0;
        is_new = rescue_is_new;
        candidate_blo = rescue_blo;
        candidate_kind = 2;
      }
    }

    kafs_inode_lock(ctx, ino_idx);

    kafs_blkcnt_t current_old_blo = KAFS_BLO_NONE;
    rc = kafs_ino_ibrk_run(ctx, inoent, iblo, &current_old_blo, KAFS_IBLKREF_FUNC_GET);
    if (rc < 0)
      return rc;
    if (current_old_blo != expected_old_blo)
    {
      if (rc == 0 && candidate_blo != KAFS_BLO_NONE)
      {
        kafs_inode_unlock(ctx, ino_idx);
        (void)kafs_inode_release_hrl_ref(ctx, candidate_blo);
        kafs_inode_lock(ctx, ino_idx);
      }
      continue;
    }

    if (rc == 0)
    {
      if (is_new)
        ctx->c_stat_hrl_put_misses++;
      else
        ctx->c_stat_hrl_put_hits++;
      kafs_diag_log_dir_iblk_write(candidate_kind == 2 ? "iblk_write_rescue" : "iblk_write_hrl",
                                   ctx, inoent, iblo, current_old_blo, candidate_blo, buf,
                                   kafs_sb_blksize_get(ctx->c_superblock));
      rc = kafs_ino_ibrk_run(ctx, inoent, iblo, &candidate_blo, KAFS_IBLKREF_FUNC_SET);
      if (rc < 0)
        return rc;
      if (current_old_blo != KAFS_BLO_NONE && current_old_blo != candidate_blo)
      {
        kafs_inode_unlock(ctx, ino_idx);
        uint64_t t_dec0 = kafs_now_ns();
        (void)kafs_inode_release_hrl_ref(ctx, current_old_blo);
        uint64_t t_dec1 = kafs_now_ns();
        __atomic_add_fetch(&ctx->c_stat_iblk_write_ns_dec_ref, t_dec1 - t_dec0, __ATOMIC_RELAXED);
        kafs_inode_lock(ctx, ino_idx);
      }
      return 0;
    }

    break;
  }

  ctx->c_stat_hrl_put_fallback_legacy++;
  return 1;
}

/// @brief inode毎のデータを書き込む（ブロック単位）
/// @param ctx コンテキスト
/// @param inoent inode テーブルエントリ
/// @param iblo ブロック番号
/// @param buf バッファ
/// @return 0: 成功, < 0: 失敗 (-errno)
static int kafs_ino_iblk_write(struct kafs_context *ctx, kafs_sinode_t *inoent, kafs_iblkcnt_t iblo,
                               const void *buf)
{
  static uint32_t s_pendinglog_full_warned = 0;
  kafs_dlog(3, "%s(ino = %d, iblo = %" PRIuFAST32 ")\n", __func__, kafs_ctx_ino_no(ctx, inoent),
            iblo);
  assert(ctx != NULL);
  assert(buf != NULL);
  assert(inoent != NULL);
  assert(kafs_ino_get_usage(inoent));
  // Directory metadata is frequently rewritten and can cross the inline/block-backed boundary.
  // Keep that path synchronous so shrink-to-inline and unlink do not race with pendinglog writes.
  if (ctx->c_pendinglog_enabled && ctx->c_pending_worker_running &&
      !S_ISDIR(kafs_ino_mode_get(inoent)))
  {
    int prc = kafs_ino_iblk_write_pending(ctx, inoent, iblo, buf, &s_pendinglog_full_warned);
    if (prc < 0)
      return prc;
    if (prc == 0)
      return KAFS_SUCCESS;
    return kafs_ino_iblk_write_legacy(ctx, inoent, iblo, buf, 1);
  }

  if (S_ISDIR(kafs_ino_mode_get(inoent)))
    return kafs_ino_iblk_write_legacy(ctx, inoent, iblo, buf, 0);

  // ゼロ/非ゼロを区別せず、常に通常のデータ書き込み経路を使う。
  // Lock order policy requires hrl_global before inode. To avoid taking a lower-rank
  // HRL lock while holding the inode lock, acquire the HRL ref outside the inode lock,
  // then revalidate the target block mapping before committing the new reference.
  int hrc = kafs_ino_iblk_write_hrl_retry(ctx, inoent, iblo, buf);
  if (hrc < 0)
    return hrc;
  if (hrc == 0)
    return KAFS_SUCCESS;
  return kafs_ino_iblk_write_legacy(ctx, inoent, iblo, buf, 1);
}

__attribute_maybe_unused__ static int
kafs_ino_iblk_release(struct kafs_context *ctx, kafs_sinode_t *inoent, kafs_iblkcnt_t iblo)
{
  kafs_dlog(3, "%s(ino = %d, iblo = %" PRIuFAST32 ")\n", __func__, kafs_ctx_ino_no(ctx, inoent),
            iblo);
  assert(ctx != NULL);
  assert(inoent != NULL);
  assert(kafs_ino_get_usage(inoent));
  assert(kafs_ino_size_get(inoent) > KAFS_INODE_DIRECT_BYTES);
  kafs_blkcnt_t old;
  int rc = kafs_ino_ibrk_run(ctx, inoent, iblo, &old, KAFS_IBLKREF_FUNC_GET);
  if (rc < 0)
    return rc;
  if (old != KAFS_BLO_NONE)
  {
    kafs_blkcnt_t none = KAFS_BLO_NONE;
    rc = kafs_ino_ibrk_run(ctx, inoent, iblo, &none, KAFS_IBLKREF_FUNC_SET);
    if (rc < 0)
      return rc;
    // 空になった中間テーブルを切り離し（inode ロック内）
    kafs_blkcnt_t f1, f2, f3;
    rc = kafs_ino_prune_empty_indirects(ctx, inoent, iblo, &f1, &f2, &f3);
    if (rc < 0)
      return rc;
    // dec_ref は inode ロック外で実施
    uint32_t ino_idx = (uint32_t)kafs_ctx_ino_no(ctx, inoent);
    kafs_inode_unlock(ctx, ino_idx);
    (void)kafs_inode_release_hrl_ref(ctx, old);
    if (f1 != KAFS_BLO_NONE)
      (void)kafs_inode_release_hrl_ref(ctx, f1);
    if (f2 != KAFS_BLO_NONE)
      (void)kafs_inode_release_hrl_ref(ctx, f2);
    if (f3 != KAFS_BLO_NONE)
      (void)kafs_inode_release_hrl_ref(ctx, f3);
    kafs_inode_lock(ctx, ino_idx);
  }
  return KAFS_SUCCESS;
}

struct kafs_tailmeta_region_view
{
  char *base;
  uint64_t size;
  kafs_blksize_t blksize;
  kafs_blkcnt_t base_blo;
  kafs_tailmeta_region_hdr_t *hdr;
  kafs_tailmeta_container_hdr_t *containers;
};

static int kafs_tailmeta_inode_is_regular_v5(const struct kafs_context *ctx,
                                             const kafs_sinode_t *inoent)
{
  return ctx != NULL && inoent != NULL && kafs_ctx_inode_format(ctx) == KAFS_FORMAT_VERSION_V5 &&
         S_ISREG(kafs_ino_mode_get(inoent));
}

static void kafs_tailmeta_inode_taildesc_set_inline(struct kafs_context *ctx, kafs_sinode_t *inoent)
{
  kafs_sinode_taildesc_v5_t *taildesc = kafs_ctx_inode_taildesc_v5(ctx, inoent);

  if (!taildesc)
    return;
  kafs_ino_taildesc_v5_init(taildesc);
}

static void kafs_tailmeta_inode_taildesc_set_full_block(struct kafs_context *ctx,
                                                        kafs_sinode_t *inoent)
{
  kafs_sinode_taildesc_v5_t *taildesc = kafs_ctx_inode_taildesc_v5(ctx, inoent);

  if (!taildesc)
    return;
  kafs_ino_taildesc_v5_init(taildesc);
  kafs_ino_taildesc_v5_layout_kind_set(taildesc, KAFS_TAIL_LAYOUT_FULL_BLOCK);
}

static int kafs_tailmeta_region_view_get(struct kafs_context *ctx,
                                         struct kafs_tailmeta_region_view *view)
{
  uint64_t region_off;
  uint64_t region_size;
  uint32_t table_off;

  if (!ctx || !view || !ctx->c_superblock || !ctx->c_img_base)
    return -EINVAL;
  if (!kafs_tailmeta_region_present(ctx->c_superblock))
    return -ENOTSUP;

  region_off = kafs_sb_tailmeta_offset_get(ctx->c_superblock);
  region_size = kafs_sb_tailmeta_size_get(ctx->c_superblock);
  if (region_off == 0u || region_size < sizeof(kafs_tailmeta_region_hdr_t))
    return -ENOTSUP;
  if ((region_off & ((uint64_t)kafs_sb_blksize_get(ctx->c_superblock) - 1u)) != 0u)
    return -EPROTO;
  if (region_off > (uint64_t)ctx->c_img_size ||
      region_size > (uint64_t)ctx->c_img_size - region_off)
    return -ERANGE;

  memset(view, 0, sizeof(*view));
  view->base = (char *)ctx->c_img_base + region_off;
  view->size = region_size;
  view->blksize = kafs_sb_blksize_get(ctx->c_superblock);
  view->base_blo = (kafs_blkcnt_t)(region_off >> kafs_sb_log_blksize_get(ctx->c_superblock));
  view->hdr = (kafs_tailmeta_region_hdr_t *)view->base;
  if (kafs_tailmeta_region_hdr_validate(view->hdr, view->size) != 0)
    return -EPROTO;

  table_off = kafs_tailmeta_region_hdr_container_table_off_get(view->hdr);
  if (table_off >= view->size)
    return -ERANGE;
  view->containers = (kafs_tailmeta_container_hdr_t *)(view->base + table_off);
  return 0;
}

static kafs_blkcnt_t kafs_tailmeta_container_block_blo(const struct kafs_tailmeta_region_view *view,
                                                       uint32_t index)
{
  return view->base_blo + KAFS_TAILMETA_DEFAULT_REGION_META_BLOCKS + index;
}

static int kafs_tailmeta_find_container_index_by_blo(const struct kafs_tailmeta_region_view *view,
                                                     kafs_blkcnt_t container_blo,
                                                     uint32_t *out_index)
{
  uint32_t container_count;

  if (!view || !out_index)
    return -EINVAL;
  container_count = kafs_tailmeta_region_hdr_container_count_get(view->hdr);
  for (uint32_t index = 0; index < container_count; ++index)
  {
    if (kafs_tailmeta_container_block_blo(view, index) == container_blo)
    {
      *out_index = index;
      return 0;
    }
  }
  return -ENOENT;
}

static kafs_tailmeta_slot_desc_t *
kafs_tailmeta_slot_desc_ptr(const struct kafs_tailmeta_region_view *view, uint32_t container_index,
                            uint16_t slot_index)
{
  const kafs_tailmeta_container_hdr_t *container = &view->containers[container_index];
  uint32_t slot_table_off = kafs_tailmeta_container_hdr_slot_table_off_get(container);
  uint32_t slot_table_bytes = kafs_tailmeta_container_hdr_slot_table_bytes_get(container);
  uint32_t slot_off =
      slot_table_off + (uint32_t)slot_index * (uint32_t)sizeof(kafs_tailmeta_slot_desc_t);

  if (slot_table_off >= view->size || slot_table_bytes > view->size - slot_table_off)
    return NULL;
  if (slot_off >= view->size || sizeof(kafs_tailmeta_slot_desc_t) > view->size - slot_off)
    return NULL;
  return (kafs_tailmeta_slot_desc_t *)(view->base + slot_off);
}

static char *kafs_tailmeta_slot_payload_ptr(const struct kafs_tailmeta_region_view *view,
                                            uint32_t container_index, uint16_t slot_index)
{
  const kafs_tailmeta_container_hdr_t *container = &view->containers[container_index];
  uint16_t class_bytes = kafs_tailmeta_container_hdr_class_bytes_get(container);
  uint32_t slot_table_off = kafs_tailmeta_container_hdr_slot_table_off_get(container);
  uint32_t slot_table_bytes = kafs_tailmeta_container_hdr_slot_table_bytes_get(container);
  uint32_t payload_off = slot_table_off + slot_table_bytes + (uint32_t)slot_index * class_bytes;

  if (payload_off >= view->size || class_bytes > view->size - payload_off)
    return NULL;
  return view->base + payload_off;
}

static int kafs_tailmeta_alloc_slot(struct kafs_context *ctx,
                                    struct kafs_tailmeta_region_view *view, kafs_inocnt_t ino,
                                    uint16_t len, uint32_t generation,
                                    uint32_t *out_container_index, uint16_t *out_slot_index)
{
  uint32_t container_count;

  if (!ctx || !view || !out_container_index || !out_slot_index || len == 0u)
    return -EINVAL;

  container_count = kafs_tailmeta_region_hdr_container_count_get(view->hdr);
  for (uint32_t container_index = 0; container_index < container_count; ++container_index)
  {
    kafs_tailmeta_container_hdr_t *container = &view->containers[container_index];
    uint16_t class_bytes = kafs_tailmeta_container_hdr_class_bytes_get(container);
    uint16_t slot_count = kafs_tailmeta_container_hdr_slot_count_get(container);

    if (class_bytes < len)
      continue;

    for (uint16_t slot_index = 0; slot_index < slot_count; ++slot_index)
    {
      kafs_tailmeta_slot_desc_t *slot =
          kafs_tailmeta_slot_desc_ptr(view, container_index, slot_index);
      if (!slot)
        return -ERANGE;
      if (kafs_tailmeta_slot_owner_ino_get(slot) != KAFS_INO_NONE)
        continue;

      kafs_tailmeta_slot_owner_ino_set(slot, ino);
      slot->ts_generation = kafs_u32_htos(generation);
      kafs_tailmeta_slot_len_set(slot, len);
      kafs_tailmeta_slot_flags_set(slot, 0u);
      kafs_tailmeta_container_hdr_live_count_set(
          container, (uint16_t)(kafs_tailmeta_container_hdr_live_count_get(container) + 1u));
      kafs_tailmeta_container_hdr_free_bytes_set(
          container,
          (uint16_t)(kafs_tailmeta_container_hdr_free_bytes_get(container) - class_bytes));
      *out_container_index = container_index;
      *out_slot_index = slot_index;
      return 0;
    }
  }

  return -ENOSPC;
}

static int kafs_tailmeta_release_slot(struct kafs_tailmeta_region_view *view,
                                      uint32_t container_index, uint16_t slot_index)
{
  kafs_tailmeta_container_hdr_t *container;
  kafs_tailmeta_slot_desc_t *slot;
  uint16_t class_bytes;

  if (!view)
    return -EINVAL;
  container = &view->containers[container_index];
  class_bytes = kafs_tailmeta_container_hdr_class_bytes_get(container);
  slot = kafs_tailmeta_slot_desc_ptr(view, container_index, slot_index);
  if (!slot)
    return -ERANGE;
  if (kafs_tailmeta_slot_owner_ino_get(slot) == KAFS_INO_NONE)
    return 0;

  kafs_tailmeta_slot_owner_ino_set(slot, KAFS_INO_NONE);
  slot->ts_generation = kafs_u32_htos(0u);
  kafs_tailmeta_slot_len_set(slot, 0u);
  kafs_tailmeta_slot_flags_set(slot, 0u);
  if (kafs_tailmeta_container_hdr_live_count_get(container) > 0u)
    kafs_tailmeta_container_hdr_live_count_set(
        container, (uint16_t)(kafs_tailmeta_container_hdr_live_count_get(container) - 1u));
  kafs_tailmeta_container_hdr_free_bytes_set(
      container, (uint16_t)(kafs_tailmeta_container_hdr_free_bytes_get(container) + class_bytes));
  return 0;
}

static int kafs_tailmeta_resolve_slot_shape(const kafs_sinode_taildesc_v5_t *taildesc,
                                            const struct kafs_tailmeta_region_view *view,
                                            uint32_t container_index, uint16_t *out_class_bytes,
                                            uint16_t *out_len, uint16_t *out_slot_index)
{
  uint16_t class_bytes;
  uint16_t len;

  if (!taildesc || !view || !out_class_bytes || !out_len || !out_slot_index)
    return -EINVAL;

  class_bytes = kafs_tailmeta_container_hdr_class_bytes_get(&view->containers[container_index]);
  len = kafs_ino_taildesc_v5_fragment_len_get(taildesc);
  if (class_bytes == 0u || len == 0u)
    return -EPROTO;
  if (kafs_ino_taildesc_v5_fragment_off_get(taildesc) % class_bytes != 0u)
    return -EPROTO;

  *out_class_bytes = class_bytes;
  *out_len = len;
  *out_slot_index = (uint16_t)(kafs_ino_taildesc_v5_fragment_off_get(taildesc) / class_bytes);
  return 0;
}

static int kafs_tailmeta_lookup_tail_slot(struct kafs_context *ctx, kafs_sinode_t *inoent,
                                          const kafs_sinode_taildesc_v5_t *taildesc,
                                          struct kafs_tailmeta_region_view *view,
                                          uint32_t *out_container_index, uint16_t *out_slot_index,
                                          uint16_t *out_class_bytes, uint16_t *out_len,
                                          char **out_payload)
{
  kafs_tailmeta_slot_desc_t *slot;
  kafs_tailmeta_inode_desc_t desc;
  kafs_inocnt_t ino;
  kafs_off_t inode_size;
  kafs_blksize_t blksize;
  uint32_t container_index;
  char *payload;
  int rc;

  if (!ctx || !inoent || !taildesc || !view || !out_container_index || !out_slot_index ||
      !out_class_bytes || !out_len || !out_payload)
    return -EINVAL;
  if (!kafs_tail_layout_uses_tail_storage(kafs_ino_taildesc_v5_layout_kind_get(taildesc)))
    return -EINVAL;

  rc = kafs_tailmeta_region_view_get(ctx, view);
  if (rc != 0)
    return rc;
  rc = kafs_tailmeta_find_container_index_by_blo(
      view, kafs_ino_taildesc_v5_container_blo_get(taildesc), &container_index);
  if (rc != 0)
    return rc;

  rc = kafs_tailmeta_resolve_slot_shape(taildesc, view, container_index, out_class_bytes, out_len,
                                        out_slot_index);
  if (rc != 0)
    return rc;

  slot = kafs_tailmeta_slot_desc_ptr(view, container_index, *out_slot_index);
  if (!slot)
    return -ERANGE;

  ino = (kafs_inocnt_t)kafs_ctx_ino_no(ctx, inoent);
  inode_size = kafs_ino_size_get(inoent);
  blksize = kafs_sb_blksize_get(ctx->c_superblock);
  kafs_tailmeta_inode_desc_from_inode_taildesc(&desc, taildesc);
  rc = kafs_tailmeta_inode_desc_matches_slot_for_inode(&desc, slot, *out_class_bytes, ino,
                                                       inode_size, blksize);
  if (rc != 0)
    return rc;

  payload = kafs_tailmeta_slot_payload_ptr(view, container_index, *out_slot_index);
  if (!payload)
    return -ERANGE;

  *out_container_index = container_index;
  *out_payload = payload;
  return 0;
}

static int kafs_tailmeta_release_desc_slot(struct kafs_context *ctx, kafs_sinode_t *inoent,
                                           const kafs_sinode_taildesc_v5_t *taildesc)
{
  struct kafs_tailmeta_region_view view;
  uint32_t container_index;
  uint16_t class_bytes;
  uint16_t slot_index;
  uint16_t len;
  char *payload;
  int rc;

  if (!ctx || !inoent || !taildesc)
    return -EINVAL;
  if (!kafs_tail_layout_uses_tail_storage(kafs_ino_taildesc_v5_layout_kind_get(taildesc)))
    return 0;

  rc = kafs_tailmeta_lookup_tail_slot(ctx, inoent, taildesc, &view, &container_index, &slot_index,
                                      &class_bytes, &len, &payload);
  if (rc != 0)
    return rc;
  (void)class_bytes;
  (void)len;
  (void)payload;
  return kafs_tailmeta_release_slot(&view, container_index, slot_index);
}

static int kafs_tailmeta_fragment_load(struct kafs_context *ctx, kafs_sinode_t *inoent, void *buf,
                                       kafs_off_t size, kafs_off_t offset)
{
  struct kafs_tailmeta_region_view view;
  const kafs_sinode_taildesc_v5_t *taildesc;
  uint32_t container_index;
  uint16_t slot_index;
  uint16_t class_bytes;
  uint16_t len;
  char *payload;
  int rc;

  if (!ctx || !inoent || !buf)
    return -EINVAL;
  taildesc = kafs_ctx_inode_taildesc_v5_const(ctx, inoent);
  if (!taildesc ||
      !kafs_tail_layout_uses_tail_storage(kafs_ino_taildesc_v5_layout_kind_get(taildesc)))
    return -EINVAL;

  rc = kafs_tailmeta_lookup_tail_slot(ctx, inoent, taildesc, &view, &container_index, &slot_index,
                                      &class_bytes, &len, &payload);
  if (rc != 0)
    return rc;
  (void)class_bytes;
  (void)container_index;
  (void)slot_index;
  if ((uint64_t)offset > (uint64_t)len || (uint64_t)size > (uint64_t)len)
    return -ERANGE;
  if ((uint64_t)offset + (uint64_t)size > (uint64_t)len)
    return -ERANGE;
  memcpy(buf, payload + offset, (size_t)size);
  return 0;
}

static int kafs_tailmeta_tail_only_load(struct kafs_context *ctx, kafs_sinode_t *inoent, void *buf,
                                        kafs_off_t size, kafs_off_t offset)
{
  const kafs_sinode_taildesc_v5_t *taildesc;

  if (!ctx || !inoent || !buf)
    return -EINVAL;
  taildesc = kafs_ctx_inode_taildesc_v5_const(ctx, inoent);
  if (!taildesc || kafs_ino_taildesc_v5_layout_kind_get(taildesc) != KAFS_TAIL_LAYOUT_TAIL_ONLY)
    return -EINVAL;
  return kafs_tailmeta_fragment_load(ctx, inoent, buf, size, offset);
}

static int kafs_tailmeta_store_mixed_final_tail(struct kafs_context *ctx, kafs_sinode_t *inoent)
{
  struct kafs_tailmeta_region_view view;
  kafs_sinode_taildesc_v5_t *taildesc;
  kafs_sinode_taildesc_v5_t old_taildesc;
  kafs_blksize_t blksize;
  kafs_off_t filesize;
  kafs_off_t fragment_len_off;
  uint32_t generation;
  uint32_t container_index;
  uint16_t slot_index;
  uint16_t class_bytes;
  char *payload;
  int rc;

  if (!ctx || !inoent)
    return -EINVAL;
  if (!kafs_tailmeta_inode_is_regular_v5(ctx, inoent))
    return -ENOTSUP;

  blksize = kafs_sb_blksize_get(ctx->c_superblock);
  filesize = kafs_ino_size_get(inoent);
  if (filesize <= (kafs_off_t)blksize)
    return -ERANGE;
  fragment_len_off = filesize & (kafs_off_t)(blksize - 1u);
  if (fragment_len_off == 0)
    return -ERANGE;

  taildesc = kafs_ctx_inode_taildesc_v5(ctx, inoent);
  if (!taildesc)
    return -EINVAL;
  old_taildesc = *taildesc;

  rc = kafs_tailmeta_region_view_get(ctx, &view);
  if (rc != 0)
    return rc;

  generation = kafs_inode_epoch_bump(ctx, (uint32_t)kafs_ctx_ino_no(ctx, inoent));
  rc = kafs_tailmeta_alloc_slot(ctx, &view, (uint32_t)kafs_ctx_ino_no(ctx, inoent),
                                (uint16_t)fragment_len_off, generation, &container_index,
                                &slot_index);
  if (rc != 0)
    return rc;

  class_bytes = kafs_tailmeta_container_hdr_class_bytes_get(&view.containers[container_index]);
  payload = kafs_tailmeta_slot_payload_ptr(&view, container_index, slot_index);
  if (!payload)
  {
    (void)kafs_tailmeta_release_slot(&view, container_index, slot_index);
    return -ERANGE;
  }

  {
    kafs_iblkcnt_t tail_iblo =
        (kafs_iblkcnt_t)(filesize >> kafs_sb_log_blksize_get(ctx->c_superblock));
    char block[blksize];

    rc = kafs_ino_iblk_read_or_zero(ctx, inoent, tail_iblo, block);
    if (rc != 0)
    {
      (void)kafs_tailmeta_release_slot(&view, container_index, slot_index);
      return rc;
    }
    memset(payload, 0, class_bytes);
    memcpy(payload, block, (size_t)fragment_len_off);

    rc = kafs_ino_iblk_release(ctx, inoent, tail_iblo);
    if (rc != 0)
    {
      (void)kafs_tailmeta_release_slot(&view, container_index, slot_index);
      return rc;
    }
  }

  kafs_ino_taildesc_v5_init(taildesc);
  kafs_ino_taildesc_v5_layout_kind_set(taildesc, KAFS_TAIL_LAYOUT_MIXED_FULL_TAIL);
  kafs_ino_taildesc_v5_flags_set(taildesc, KAFS_TAILDESC_FLAG_FINAL_TAIL);
  kafs_ino_taildesc_v5_fragment_len_set(taildesc, (uint16_t)fragment_len_off);
  kafs_ino_taildesc_v5_container_blo_set(taildesc,
                                         kafs_tailmeta_container_block_blo(&view, container_index));
  kafs_ino_taildesc_v5_fragment_off_set(taildesc, (uint16_t)(slot_index * class_bytes));
  kafs_ino_taildesc_v5_generation_set(taildesc, generation);

  if (kafs_tail_layout_uses_tail_storage(kafs_ino_taildesc_v5_layout_kind_get(&old_taildesc)))
  {
    rc = kafs_tailmeta_release_desc_slot(ctx, inoent, &old_taildesc);
    if (rc != 0)
      return rc;
  }
  return 0;
}

static int kafs_tailmeta_materialize_mixed_to_full_block(struct kafs_context *ctx,
                                                         kafs_sinode_t *inoent)
{
  struct kafs_tailmeta_region_view view;
  const kafs_sinode_taildesc_v5_t *taildesc;
  uint32_t container_index;
  uint16_t slot_index;
  uint16_t class_bytes;
  uint16_t len;
  char *payload;
  kafs_blksize_t blksize;
  kafs_off_t filesize;
  int rc;

  if (!ctx || !inoent)
    return -EINVAL;

  taildesc = kafs_ctx_inode_taildesc_v5_const(ctx, inoent);
  if (!taildesc ||
      kafs_ino_taildesc_v5_layout_kind_get(taildesc) != KAFS_TAIL_LAYOUT_MIXED_FULL_TAIL)
    return -EINVAL;

  rc = kafs_tailmeta_lookup_tail_slot(ctx, inoent, taildesc, &view, &container_index, &slot_index,
                                      &class_bytes, &len, &payload);
  if (rc != 0)
    return rc;
  (void)class_bytes;

  blksize = kafs_sb_blksize_get(ctx->c_superblock);
  filesize = kafs_ino_size_get(inoent);
  {
    kafs_iblkcnt_t tail_iblo =
        (kafs_iblkcnt_t)(filesize >> kafs_sb_log_blksize_get(ctx->c_superblock));
    char block[blksize];

    memset(block, 0, sizeof(block));
    memcpy(block, payload, len);
    rc = kafs_ino_iblk_write(ctx, inoent, tail_iblo, block);
    if (rc != 0)
      return rc;
  }

  kafs_tailmeta_inode_taildesc_set_full_block(ctx, inoent);
  return kafs_tailmeta_release_slot(&view, container_index, slot_index);
}

static int kafs_tailmeta_normalize_block_layout(struct kafs_context *ctx, kafs_sinode_t *inoent)
{
  kafs_blksize_t blksize;
  kafs_off_t filesize;
  const kafs_sinode_taildesc_v5_t *taildesc;

  if (!kafs_tailmeta_inode_is_regular_v5(ctx, inoent))
    return 0;

  blksize = kafs_sb_blksize_get(ctx->c_superblock);
  filesize = kafs_ino_size_get(inoent);
  taildesc = kafs_ctx_inode_taildesc_v5_const(ctx, inoent);
  if (!taildesc)
    return 0;
  if (filesize <= (kafs_off_t)blksize || (filesize & (kafs_off_t)(blksize - 1u)) == 0)
    return 0;
  if (kafs_ino_taildesc_v5_layout_kind_get(taildesc) != KAFS_TAIL_LAYOUT_FULL_BLOCK)
    return 0;
  return kafs_tailmeta_store_mixed_final_tail(ctx, inoent);
}

static int kafs_tailmeta_store_tail_only(struct kafs_context *ctx, kafs_sinode_t *inoent,
                                         const void *data, kafs_off_t size)
{
  struct kafs_tailmeta_region_view view;
  kafs_sinode_taildesc_v5_t old_taildesc;
  kafs_sinode_taildesc_v5_t *taildesc;
  uint32_t generation;
  uint32_t container_index;
  uint32_t old_container_index;
  uint16_t slot_index;
  uint16_t old_class_bytes;
  uint16_t old_slot_index;
  uint16_t class_bytes;
  char *payload;
  uint32_t ino;
  int had_old_tail;
  int rc;

  if (!ctx || !inoent || !data)
    return -EINVAL;
  if (size <= (kafs_off_t)KAFS_INODE_DIRECT_BYTES ||
      size >= (kafs_off_t)kafs_sb_blksize_get(ctx->c_superblock) || size > (kafs_off_t)UINT16_MAX)
    return -ERANGE;
  if (!kafs_tailmeta_inode_is_regular_v5(ctx, inoent))
    return -ENOTSUP;

  rc = kafs_tailmeta_region_view_get(ctx, &view);
  if (rc != 0)
    return rc;

  ino = (uint32_t)kafs_ctx_ino_no(ctx, inoent);
  generation = kafs_inode_epoch_bump(ctx, ino);
  taildesc = kafs_ctx_inode_taildesc_v5(ctx, inoent);
  old_taildesc = *taildesc;
  had_old_tail = kafs_ino_taildesc_v5_layout_kind_get(&old_taildesc) == KAFS_TAIL_LAYOUT_TAIL_ONLY;

  rc = kafs_tailmeta_alloc_slot(ctx, &view, ino, (uint16_t)size, generation, &container_index,
                                &slot_index);
  if (rc != 0)
    return rc;

  class_bytes = kafs_tailmeta_container_hdr_class_bytes_get(&view.containers[container_index]);
  payload = kafs_tailmeta_slot_payload_ptr(&view, container_index, slot_index);
  if (!payload)
  {
    (void)kafs_tailmeta_release_slot(&view, container_index, slot_index);
    return -ERANGE;
  }
  memset(payload, 0, class_bytes);
  memcpy(payload, data, (size_t)size);

  memset(inoent->i_blkreftbl, 0, sizeof(inoent->i_blkreftbl));
  kafs_ino_blocks_set(inoent, 0);
  kafs_ino_size_set(inoent, size);
  kafs_ino_taildesc_v5_init(taildesc);
  kafs_ino_taildesc_v5_layout_kind_set(taildesc, KAFS_TAIL_LAYOUT_TAIL_ONLY);
  kafs_ino_taildesc_v5_flags_set(taildesc, KAFS_TAILDESC_FLAG_PACKED_SMALL_FILE);
  kafs_ino_taildesc_v5_fragment_len_set(taildesc, (uint16_t)size);
  kafs_ino_taildesc_v5_container_blo_set(taildesc,
                                         kafs_tailmeta_container_block_blo(&view, container_index));
  kafs_ino_taildesc_v5_fragment_off_set(taildesc, (uint16_t)(slot_index * class_bytes));
  kafs_ino_taildesc_v5_generation_set(taildesc, generation);

  if (had_old_tail)
  {
    rc = kafs_tailmeta_find_container_index_by_blo(
        &view, kafs_ino_taildesc_v5_container_blo_get(&old_taildesc), &old_container_index);
    if (rc == 0)
    {
      old_class_bytes =
          kafs_tailmeta_container_hdr_class_bytes_get(&view.containers[old_container_index]);
      if (old_class_bytes != 0u &&
          kafs_ino_taildesc_v5_fragment_off_get(&old_taildesc) % old_class_bytes == 0u)
      {
        old_slot_index =
            (uint16_t)(kafs_ino_taildesc_v5_fragment_off_get(&old_taildesc) / old_class_bytes);
        (void)kafs_tailmeta_release_slot(&view, old_container_index, old_slot_index);
      }
    }
  }

  return 0;
}

static int kafs_tailmeta_promote_tail_only_to_full_block(struct kafs_context *ctx,
                                                         kafs_sinode_t *inoent)
{
  struct kafs_tailmeta_region_view view;
  kafs_sinode_taildesc_v5_t old_taildesc;
  kafs_sinode_taildesc_v5_t *taildesc;
  uint32_t container_index;
  uint16_t class_bytes;
  uint16_t slot_index;
  kafs_off_t filesize;
  kafs_blksize_t blksize;
  char *payload;
  int rc;

  if (!ctx || !inoent)
    return -EINVAL;
  taildesc = kafs_ctx_inode_taildesc_v5(ctx, inoent);
  if (!taildesc || kafs_ino_taildesc_v5_layout_kind_get(taildesc) != KAFS_TAIL_LAYOUT_TAIL_ONLY)
    return -EINVAL;

  rc = kafs_tailmeta_region_view_get(ctx, &view);
  if (rc != 0)
    return rc;

  old_taildesc = *taildesc;
  rc = kafs_tailmeta_find_container_index_by_blo(
      &view, kafs_ino_taildesc_v5_container_blo_get(&old_taildesc), &container_index);
  if (rc != 0)
    return rc;
  class_bytes = kafs_tailmeta_container_hdr_class_bytes_get(&view.containers[container_index]);
  if (class_bytes == 0u || kafs_ino_taildesc_v5_fragment_off_get(&old_taildesc) % class_bytes != 0u)
    return -EPROTO;
  slot_index = (uint16_t)(kafs_ino_taildesc_v5_fragment_off_get(&old_taildesc) / class_bytes);
  payload = kafs_tailmeta_slot_payload_ptr(&view, container_index, slot_index);
  if (!payload)
    return -ERANGE;

  blksize = kafs_sb_blksize_get(ctx->c_superblock);
  filesize = kafs_ino_size_get(inoent);
  char block[blksize];
  memset(block, 0, sizeof(block));
  memcpy(block, payload, (size_t)filesize);
  rc = kafs_ino_iblk_write(ctx, inoent, 0, block);
  if (rc != 0)
    return rc;

  kafs_tailmeta_inode_taildesc_set_full_block(ctx, inoent);
  return kafs_tailmeta_release_slot(&view, container_index, slot_index);
}

static int kafs_tailmeta_load_small_file_bytes(struct kafs_context *ctx, kafs_sinode_t *inoent,
                                               char *buf, kafs_off_t target_size)
{
  kafs_off_t filesize;
  const kafs_sinode_taildesc_v5_t *taildesc;
  int rc;

  if (!ctx || !inoent || !buf)
    return -EINVAL;
  memset(buf, 0, (size_t)target_size);
  filesize = kafs_ino_size_get(inoent);
  if (filesize == 0)
    return 0;
  if (filesize <= (kafs_off_t)KAFS_INODE_DIRECT_BYTES)
  {
    memcpy(buf, inoent->i_blkreftbl, (size_t)((filesize < target_size) ? filesize : target_size));
    return 0;
  }
  taildesc = kafs_ctx_inode_taildesc_v5_const(ctx, inoent);
  if (!taildesc || kafs_ino_taildesc_v5_layout_kind_get(taildesc) != KAFS_TAIL_LAYOUT_TAIL_ONLY)
    return -ENOTSUP;

  rc = kafs_tailmeta_tail_only_load(ctx, inoent, buf,
                                    (filesize < target_size) ? filesize : target_size, 0);
  return rc;
}

static int kafs_truncate(struct kafs_context *ctx, kafs_sinode_t *inoent, kafs_off_t filesize_new);

static int kafs_tailmeta_try_reclaim_tombstone_payload_locked(struct kafs_context *ctx,
                                                              kafs_inocnt_t ino)
{
  kafs_sinode_t *inoent;
  const kafs_sinode_taildesc_v5_t *taildesc;

  if (!ctx)
    return -EINVAL;
  if (!ctx->c_open_cnt)
    return 0;

  inoent = kafs_ctx_inode(ctx, ino);
  if (!inoent || !kafs_ino_get_usage(inoent) || kafs_ino_linkcnt_get(inoent) != 0)
    return 0;
  if (__atomic_load_n(&ctx->c_open_cnt[ino], __ATOMIC_RELAXED) != 0)
    return 0;
  if (!kafs_tailmeta_inode_is_regular_v5(ctx, inoent))
    return 0;

  taildesc = kafs_ctx_inode_taildesc_v5_const(ctx, inoent);
  if (!taildesc ||
      !kafs_tail_layout_uses_tail_storage(kafs_ino_taildesc_v5_layout_kind_get(taildesc)))
    return 0;

  return kafs_truncate(ctx, inoent, 0);
}

static ssize_t kafs_pread_full_block_layout(struct kafs_context *ctx, kafs_sinode_t *inoent,
                                            void *buf, kafs_off_t size, kafs_off_t offset)
{
  size_t size_read = 0;
  kafs_logblksize_t log_blksize = kafs_sb_log_blksize_get(ctx->c_superblock);
  kafs_blksize_t blksize = kafs_sb_blksize_get(ctx->c_superblock);
  kafs_blksize_t offset_blksize = offset & (blksize - 1);

  if (offset_blksize > 0 || size - size_read < blksize)
  {
    char rbuf[blksize];
    kafs_iblkcnt_t iblo = offset >> log_blksize;
    int rc = kafs_ino_iblk_read_or_zero(ctx, inoent, iblo, rbuf);
    if (rc < 0)
      return rc;
    if (size < blksize - offset_blksize)
    {
      memcpy(buf, rbuf + offset_blksize, size);
      return size;
    }
    memcpy(buf, rbuf + offset_blksize, blksize - offset_blksize);
    size_read += blksize - offset_blksize;
  }
  while (size_read < (size_t)size)
  {
    kafs_iblkcnt_t iblo = (offset + size_read) >> log_blksize;
    if (size - (kafs_off_t)size_read <= (kafs_off_t)blksize)
    {
      char rbuf[blksize];
      int rc = kafs_ino_iblk_read_or_zero(ctx, inoent, iblo, rbuf);
      if (rc < 0)
        return rc;
      memcpy((char *)buf + size_read, rbuf, (size_t)(size - (kafs_off_t)size_read));
      return size;
    }
    int rc = kafs_ino_iblk_read_or_zero(ctx, inoent, iblo, (char *)buf + size_read);
    if (rc < 0)
      return rc;
    size_read += blksize;
  }
  return size;
}

static ssize_t kafs_pread(struct kafs_context *ctx, kafs_sinode_t *inoent, void *buf,
                          kafs_off_t size, kafs_off_t offset)
{
#define KAFS_PREAD_TRY(_expr)                                                                      \
  do                                                                                               \
  {                                                                                                \
    int _rc = (_expr);                                                                             \
    if (_rc < 0)                                                                                   \
      return _rc;                                                                                  \
  } while (0)
  assert(inoent != NULL);
  assert(kafs_ino_get_usage(inoent));
  kafs_off_t filesize = kafs_ino_size_get(inoent);
  if (offset >= filesize)
    return 0;
  if (offset + size > filesize)
    size = filesize - offset;
  if (size == 0)
    return 0;
  const kafs_sinode_taildesc_v5_t *taildesc = kafs_ctx_inode_taildesc_v5_const(ctx, inoent);
  if (taildesc && kafs_ino_taildesc_v5_layout_kind_get(taildesc) == KAFS_TAIL_LAYOUT_TAIL_ONLY)
  {
    KAFS_PREAD_TRY(kafs_tailmeta_tail_only_load(ctx, inoent, buf, size, offset));
    return size;
  }
  if (taildesc &&
      kafs_ino_taildesc_v5_layout_kind_get(taildesc) == KAFS_TAIL_LAYOUT_MIXED_FULL_TAIL)
  {
    kafs_off_t fragment_len = (kafs_off_t)kafs_ino_taildesc_v5_fragment_len_get(taildesc);
    kafs_off_t full_bytes = filesize - fragment_len;
    kafs_off_t copied = 0;

    if (offset < full_bytes)
    {
      kafs_off_t prefix = size;
      if (offset + prefix > full_bytes)
        prefix = full_bytes - offset;
      KAFS_PREAD_TRY(kafs_pread_full_block_layout(ctx, inoent, buf, prefix, offset));
      copied = prefix;
    }
    if (copied < size)
    {
      kafs_off_t tail_offset = offset + copied - full_bytes;
      KAFS_PREAD_TRY(kafs_tailmeta_fragment_load(ctx, inoent, (char *)buf + copied, size - copied,
                                                 tail_offset));
    }
    return size;
  }
  // 60バイト以下は直接
  if (filesize <= KAFS_INODE_DIRECT_BYTES)
  {
    memcpy(buf, (void *)inoent->i_blkreftbl + offset, size);
    return size;
  }
  return kafs_pread_full_block_layout(ctx, inoent, buf, size, offset);
#undef KAFS_PREAD_TRY
}

static int kafs_pwrite_commit_block(struct kafs_context *ctx, kafs_sinode_t *inoent,
                                    kafs_iblkcnt_t iblo, const void *buf)
{
  kafs_blksize_t blksize = kafs_sb_blksize_get(ctx->c_superblock);

  if (kafs_ino_size_get(inoent) > KAFS_INODE_DIRECT_BYTES && kafs_blk_is_zero(buf, blksize))
    return kafs_ino_iblk_release(ctx, inoent, iblo);

  return kafs_ino_iblk_write(ctx, inoent, iblo, buf);
}

static void kafs_pwrite_record_write_latency(struct kafs_context *ctx, uint64_t t_w0, uint64_t t_w1)
{
  uint64_t d = t_w1 - t_w0;
  __atomic_add_fetch(&ctx->c_stat_pwrite_ns_iblk_write, d, __ATOMIC_RELAXED);
  kafs_stat_record_pwrite_iblk_write_latency(ctx, d);
}

static int kafs_pwrite_commit_block_timed(struct kafs_context *ctx, kafs_sinode_t *inoent,
                                          kafs_iblkcnt_t iblo, const void *buf)
{
  uint64_t t_w0 = kafs_now_ns();
  int rc = kafs_pwrite_commit_block(ctx, inoent, iblo, buf);
  uint64_t t_w1 = kafs_now_ns();
  kafs_pwrite_record_write_latency(ctx, t_w0, t_w1);
  return rc;
}

static int kafs_pwrite_read_block_timed(struct kafs_context *ctx, kafs_sinode_t *inoent,
                                        kafs_iblkcnt_t iblo, void *buf)
{
  uint64_t t_r0 = kafs_now_ns();
  int rc = kafs_ino_iblk_read_or_zero(ctx, inoent, iblo, buf);
  uint64_t t_r1 = kafs_now_ns();
  __atomic_add_fetch(&ctx->c_stat_pwrite_ns_iblk_read, t_r1 - t_r0, __ATOMIC_RELAXED);
  return rc;
}

static int kafs_pwrite_try_tail_only_store(struct kafs_context *ctx, kafs_sinode_t *inoent,
                                           const void *buf, kafs_off_t size, kafs_off_t offset,
                                           kafs_off_t filesize_new)
{
  kafs_blksize_t blksize = kafs_sb_blksize_get(ctx->c_superblock);
  char smallbuf[blksize];
  int rc = kafs_tailmeta_load_small_file_bytes(ctx, inoent, smallbuf, filesize_new);
  if (rc < 0)
    return rc;
  memcpy(smallbuf + offset, buf, (size_t)size);
  return kafs_tailmeta_store_tail_only(ctx, inoent, smallbuf, filesize_new);
}

static int kafs_pwrite_prepare_tail_layout(struct kafs_context *ctx, kafs_sinode_t *inoent,
                                           const void *buf, kafs_off_t size, kafs_off_t offset,
                                           kafs_off_t filesize, kafs_off_t filesize_new,
                                           int *completed_out)
{
  kafs_blksize_t blksize = kafs_sb_blksize_get(ctx->c_superblock);
  const kafs_sinode_taildesc_v5_t *taildesc = kafs_ctx_inode_taildesc_v5_const(ctx, inoent);

  *completed_out = 0;
  if (taildesc &&
      kafs_ino_taildesc_v5_layout_kind_get(taildesc) == KAFS_TAIL_LAYOUT_MIXED_FULL_TAIL)
  {
    int rc = kafs_tailmeta_materialize_mixed_to_full_block(ctx, inoent);
    if (rc < 0)
      return rc;
    taildesc = kafs_ctx_inode_taildesc_v5_const(ctx, inoent);
  }

  if (taildesc && kafs_ino_taildesc_v5_layout_kind_get(taildesc) == KAFS_TAIL_LAYOUT_TAIL_ONLY)
  {
    if (filesize_new < (kafs_off_t)blksize)
    {
      int rc = kafs_pwrite_try_tail_only_store(ctx, inoent, buf, size, offset, filesize_new);
      if (rc < 0)
        return rc;
      *completed_out = 1;
      return 0;
    }

    int rc = kafs_tailmeta_promote_tail_only_to_full_block(ctx, inoent);
    if (rc < 0)
      return rc;
  }

  if (kafs_tailmeta_inode_is_regular_v5(ctx, inoent) &&
      filesize_new > (kafs_off_t)KAFS_INODE_DIRECT_BYTES && filesize_new < (kafs_off_t)blksize &&
      filesize <= (kafs_off_t)KAFS_INODE_DIRECT_BYTES)
  {
    int rc = kafs_pwrite_try_tail_only_store(ctx, inoent, buf, size, offset, filesize_new);
    if (rc < 0)
      return rc;
    *completed_out = 1;
  }
  return 0;
}

static int kafs_pwrite_extend_inode_size(struct kafs_context *ctx, kafs_sinode_t *inoent,
                                         kafs_off_t *filesize, kafs_off_t filesize_new)
{
  if (*filesize >= filesize_new)
    return 0;

  kafs_blksize_t blksize = kafs_sb_blksize_get(ctx->c_superblock);
  kafs_ino_size_set(inoent, filesize_new);
  if (*filesize != 0 && *filesize <= KAFS_INODE_DIRECT_BYTES &&
      filesize_new > KAFS_INODE_DIRECT_BYTES)
  {
    char wbuf[blksize];
    memset(wbuf, 0, blksize);
    memcpy(wbuf, inoent->i_blkreftbl, *filesize);
    memset(inoent->i_blkreftbl, 0, sizeof(inoent->i_blkreftbl));
    int rc = kafs_ino_iblk_write(ctx, inoent, 0, wbuf);
    if (rc < 0)
      return rc;
  }

  *filesize = filesize_new;
  return 0;
}

static void kafs_pwrite_sync_regular_taildesc(struct kafs_context *ctx, kafs_sinode_t *inoent,
                                              kafs_off_t filesize)
{
  if (!kafs_tailmeta_inode_is_regular_v5(ctx, inoent))
    return;
  if (filesize <= (kafs_off_t)KAFS_INODE_DIRECT_BYTES)
    kafs_tailmeta_inode_taildesc_set_inline(ctx, inoent);
  else
    kafs_tailmeta_inode_taildesc_set_full_block(ctx, inoent);
}

static int kafs_pwrite_write_head_fragment(struct kafs_context *ctx, kafs_sinode_t *inoent,
                                           const char *buf, kafs_off_t size, kafs_off_t offset,
                                           size_t *size_written_out, int *completed_out)
{
  kafs_logblksize_t log_blksize = kafs_sb_log_blksize_get(ctx->c_superblock);
  kafs_blksize_t blksize = kafs_sb_blksize_get(ctx->c_superblock);
  kafs_blksize_t offset_blksize = offset & (blksize - 1);

  *completed_out = 0;
  if (offset_blksize == 0 && size - *size_written_out >= blksize)
    return 0;

  kafs_iblkcnt_t iblo = offset >> log_blksize;
  char wbuf[blksize];
  int rc = kafs_pwrite_read_block_timed(ctx, inoent, iblo, wbuf);
  if (rc < 0)
    return rc;
  if (size < blksize - offset_blksize)
  {
    memcpy(wbuf + offset_blksize, buf, (size_t)size);
    rc = kafs_pwrite_commit_block_timed(ctx, inoent, iblo, wbuf);
    if (rc < 0)
      return rc;
    *completed_out = 1;
    return 0;
  }

  memcpy(wbuf + offset_blksize, buf, blksize - offset_blksize);
  rc = kafs_pwrite_commit_block_timed(ctx, inoent, iblo, wbuf);
  if (rc < 0)
    return rc;
  *size_written_out += blksize - offset_blksize;
  return 0;
}

/// @brief inode 毎にデータを読み出す
/// @param ctx コンテキスト
/// @param inoent inode テーブルエントリ
/// @param buf バッファ
/// @param size バッファサイズ
/// @param offset オフセット
/// @return > 0: 読み出しサイズ, 0: EOF, < 0: エラー(-errno)
static ssize_t kafs_pwrite(struct kafs_context *ctx, kafs_sinode_t *inoent, const void *buf,
                           kafs_off_t size, kafs_off_t offset)
{
#define KAFS_PWRITE_TRY(_expr)                                                                     \
  do                                                                                               \
  {                                                                                                \
    int _rc = (_expr);                                                                             \
    if (_rc < 0)                                                                                   \
      return _rc;                                                                                  \
  } while (0)
  uint32_t ino = kafs_ctx_ino_no(ctx, inoent);
  kafs_dlog(3, "%s(ino = %d, size = %" PRIuFAST64 ", offset = %" PRIuFAST64 ")\n", __func__, ino,
            size, offset);
  assert(ctx != NULL);
  assert(buf != NULL);
  assert(inoent != NULL);
  assert(kafs_ino_get_usage(inoent));

  kafs_off_t filesize = kafs_ino_size_get(inoent);
  kafs_logblksize_t log_blksize = kafs_sb_log_blksize_get(ctx->c_superblock);
  kafs_blksize_t blksize = kafs_sb_blksize_get(ctx->c_superblock);
  kafs_off_t filesize_new = offset + size;
  const char *srcbuf = (const char *)buf;

  if (size == 0)
    return 0;

  __atomic_add_fetch(&ctx->c_stat_pwrite_calls, 1u, __ATOMIC_RELAXED);
  __atomic_add_fetch(&ctx->c_stat_pwrite_bytes, (uint64_t)size, __ATOMIC_RELAXED);
  kafs_diag_log_first_pwrite_after_create(ctx, inoent, buf, size, offset);

  int completed = 0;
  KAFS_PWRITE_TRY(kafs_pwrite_prepare_tail_layout(ctx, inoent, buf, size, offset, filesize,
                                                  filesize_new, &completed));
  if (completed)
    return size;

  KAFS_PWRITE_TRY(kafs_pwrite_extend_inode_size(ctx, inoent, &filesize, filesize_new));
  kafs_pwrite_sync_regular_taildesc(ctx, inoent, filesize);

  size_t size_written = 0;
  // 60バイト以下は直接
  if (kafs_inode_size_is_inline((kafs_off_t)filesize))
  {
    memcpy((void *)inoent->i_blkreftbl + offset, buf, size);
    return size;
  }

  KAFS_PWRITE_TRY(kafs_pwrite_write_head_fragment(ctx, inoent, srcbuf, size, offset, &size_written,
                                                  &completed));
  if (completed)
    goto out_success;

  while (size_written < size)
  {
    kafs_iblkcnt_t iblo = (offset + size_written) >> log_blksize;
    if (size - size_written < blksize)
    {
      char wbuf[blksize];
      KAFS_PWRITE_TRY(kafs_pwrite_read_block_timed(ctx, inoent, iblo, wbuf));
      memcpy(wbuf, srcbuf + size_written, size - size_written);
      KAFS_PWRITE_TRY(kafs_pwrite_commit_block_timed(ctx, inoent, iblo, wbuf));
      goto out_success;
    }
    KAFS_PWRITE_TRY(kafs_pwrite_commit_block_timed(ctx, inoent, iblo, srcbuf + size_written));
    size_written += blksize;
  }
out_success:
#undef KAFS_PWRITE_TRY
  return size;
}

static int kafs_truncate_push_free_ref(kafs_blkcnt_t **deferred_free, size_t *deferred_free_cnt,
                                       size_t *deferred_free_cap, kafs_blkcnt_t blo)
{
  if (blo == KAFS_BLO_NONE)
    return 0;
  if (*deferred_free_cnt == *deferred_free_cap)
  {
    size_t new_cap = *deferred_free_cap ? (*deferred_free_cap << 1) : 256u;
    kafs_blkcnt_t *nw = realloc(*deferred_free, new_cap * sizeof(*nw));
    if (!nw)
      return -ENOMEM;
    *deferred_free = nw;
    *deferred_free_cap = new_cap;
  }
  (*deferred_free)[(*deferred_free_cnt)++] = blo;
  return 0;
}

static int kafs_truncate_collect_free_range(struct kafs_context *ctx, kafs_sinode_t *inoent,
                                            kafs_iblkcnt_t start, kafs_iblkcnt_t end,
                                            kafs_blkcnt_t **deferred_free,
                                            size_t *deferred_free_cnt, size_t *deferred_free_cap)
{
  for (kafs_iblkcnt_t iblo = start; iblo < end; iblo++)
  {
    kafs_blkcnt_t old;
    int rc = kafs_ino_ibrk_run(ctx, inoent, iblo, &old, KAFS_IBLKREF_FUNC_GET);
    if (rc != 0)
      return rc;
    if (old == KAFS_BLO_NONE)
      continue;

    kafs_blkcnt_t none = KAFS_BLO_NONE;
    rc = kafs_ino_ibrk_run(ctx, inoent, iblo, &none, KAFS_IBLKREF_FUNC_SET);
    if (rc != 0)
      return rc;

    kafs_blkcnt_t f1, f2, f3;
    rc = kafs_ino_prune_empty_indirects(ctx, inoent, iblo, &f1, &f2, &f3);
    if (rc != 0)
      return rc;

    rc = kafs_truncate_push_free_ref(deferred_free, deferred_free_cnt, deferred_free_cap, old);
    if (rc != 0)
      return rc;
    rc = kafs_truncate_push_free_ref(deferred_free, deferred_free_cnt, deferred_free_cap, f1);
    if (rc != 0)
      return rc;
    rc = kafs_truncate_push_free_ref(deferred_free, deferred_free_cnt, deferred_free_cap, f2);
    if (rc != 0)
      return rc;
    rc = kafs_truncate_push_free_ref(deferred_free, deferred_free_cnt, deferred_free_cap, f3);
    if (rc != 0)
      return rc;
  }
  return 0;
}

static void kafs_truncate_release_deferred_refs(struct kafs_context *ctx, uint32_t ino_idx,
                                                kafs_blkcnt_t *deferred_free,
                                                size_t deferred_free_cnt)
{
  if (deferred_free_cnt == 0)
    return;
  kafs_inode_unlock(ctx, ino_idx);
  for (size_t i = 0; i < deferred_free_cnt; ++i)
    (void)kafs_inode_release_hrl_ref(ctx, deferred_free[i]);
  kafs_inode_lock(ctx, ino_idx);
}

static int kafs_truncate_handle_tail_only(struct kafs_context *ctx, kafs_sinode_t *inoent,
                                          const kafs_sinode_taildesc_v5_t *taildesc,
                                          kafs_off_t filesize_new, kafs_blksize_t blksize)
{
  if (filesize_new == 0)
  {
    struct kafs_tailmeta_region_view view;
    uint32_t container_index;
    uint16_t class_bytes;
    uint16_t slot_index;
    int rc = kafs_tailmeta_region_view_get(ctx, &view);
    if (rc != 0)
      return rc;
    rc = kafs_tailmeta_find_container_index_by_blo(
        &view, kafs_ino_taildesc_v5_container_blo_get(taildesc), &container_index);
    if (rc != 0)
      return rc;
    class_bytes = kafs_tailmeta_container_hdr_class_bytes_get(&view.containers[container_index]);
    if (class_bytes == 0u || kafs_ino_taildesc_v5_fragment_off_get(taildesc) % class_bytes != 0u)
      return -EPROTO;
    slot_index = (uint16_t)(kafs_ino_taildesc_v5_fragment_off_get(taildesc) / class_bytes);
    KAFS_CALL(kafs_tailmeta_release_slot, &view, container_index, slot_index);
    memset(inoent->i_blkreftbl, 0, sizeof(inoent->i_blkreftbl));
    kafs_ino_blocks_set(inoent, 0);
    kafs_ino_size_set(inoent, 0);
    kafs_tailmeta_inode_taildesc_set_inline(ctx, inoent);
    return KAFS_SUCCESS;
  }

  if (filesize_new <= (kafs_off_t)KAFS_INODE_DIRECT_BYTES)
  {
    struct kafs_tailmeta_region_view view;
    uint32_t container_index;
    uint16_t class_bytes;
    uint16_t slot_index;
    char inline_buf[KAFS_INODE_DIRECT_BYTES];
    int rc = kafs_tailmeta_tail_only_load(ctx, inoent, inline_buf, filesize_new, 0);
    if (rc != 0)
      return rc;
    rc = kafs_tailmeta_region_view_get(ctx, &view);
    if (rc != 0)
      return rc;
    rc = kafs_tailmeta_find_container_index_by_blo(
        &view, kafs_ino_taildesc_v5_container_blo_get(taildesc), &container_index);
    if (rc != 0)
      return rc;
    class_bytes = kafs_tailmeta_container_hdr_class_bytes_get(&view.containers[container_index]);
    if (class_bytes == 0u || kafs_ino_taildesc_v5_fragment_off_get(taildesc) % class_bytes != 0u)
      return -EPROTO;
    slot_index = (uint16_t)(kafs_ino_taildesc_v5_fragment_off_get(taildesc) / class_bytes);
    KAFS_CALL(kafs_tailmeta_release_slot, &view, container_index, slot_index);
    memset(inoent->i_blkreftbl, 0, sizeof(inoent->i_blkreftbl));
    memcpy(inoent->i_blkreftbl, inline_buf, (size_t)filesize_new);
    if (filesize_new < (kafs_off_t)KAFS_INODE_DIRECT_BYTES)
      memset((char *)inoent->i_blkreftbl + filesize_new, 0,
             KAFS_INODE_DIRECT_BYTES - (size_t)filesize_new);
    kafs_ino_blocks_set(inoent, 0);
    kafs_ino_size_set(inoent, filesize_new);
    kafs_tailmeta_inode_taildesc_set_inline(ctx, inoent);
    return KAFS_SUCCESS;
  }

  if (filesize_new < (kafs_off_t)blksize)
  {
    char smallbuf[blksize];
    KAFS_CALL(kafs_tailmeta_load_small_file_bytes, ctx, inoent, smallbuf, filesize_new);
    KAFS_CALL(kafs_tailmeta_store_tail_only, ctx, inoent, smallbuf, filesize_new);
    return KAFS_SUCCESS;
  }

  KAFS_CALL(kafs_tailmeta_promote_tail_only_to_full_block, ctx, inoent);
  kafs_ino_size_set(inoent, filesize_new);
  kafs_tailmeta_inode_taildesc_set_full_block(ctx, inoent);
  return KAFS_SUCCESS;
}

static int kafs_truncate_handle_growth(struct kafs_context *ctx, kafs_sinode_t *inoent,
                                       kafs_off_t filesize_orig, kafs_off_t filesize_new,
                                       kafs_blksize_t blksize)
{
  if (kafs_tailmeta_inode_is_regular_v5(ctx, inoent) &&
      filesize_orig <= (kafs_off_t)KAFS_INODE_DIRECT_BYTES &&
      filesize_new > (kafs_off_t)KAFS_INODE_DIRECT_BYTES && filesize_new < (kafs_off_t)blksize)
  {
    char smallbuf[blksize];
    memset(smallbuf, 0, sizeof(smallbuf));
    memcpy(smallbuf, inoent->i_blkreftbl, (size_t)filesize_orig);
    KAFS_CALL(kafs_tailmeta_store_tail_only, ctx, inoent, smallbuf, filesize_new);
    return KAFS_SUCCESS;
  }

  if (filesize_orig <= KAFS_INODE_DIRECT_BYTES && filesize_new > KAFS_INODE_DIRECT_BYTES)
  {
    char buf[blksize];
    memcpy(buf, inoent->i_blkreftbl, filesize_orig);
    memset(buf + filesize_orig, 0, blksize - filesize_orig);
    KAFS_CALL(kafs_ino_iblk_write, ctx, inoent, 0, buf);
  }

  kafs_ino_size_set(inoent, filesize_new);
  if (kafs_tailmeta_inode_is_regular_v5(ctx, inoent))
    kafs_tailmeta_inode_taildesc_set_full_block(ctx, inoent);
  return KAFS_SUCCESS;
}

static int kafs_truncate_handle_direct_shrink(struct kafs_context *ctx, kafs_sinode_t *inoent,
                                              kafs_off_t filesize_orig, kafs_off_t filesize_new)
{
  memset((void *)inoent->i_blkreftbl + filesize_new, 0, filesize_orig - filesize_new);
  kafs_ino_size_set(inoent, filesize_new);
  if (kafs_tailmeta_inode_is_regular_v5(ctx, inoent))
    kafs_tailmeta_inode_taildesc_set_inline(ctx, inoent);
  return KAFS_SUCCESS;
}

static int kafs_truncate_collect_batches(struct kafs_context *ctx, kafs_sinode_t *inoent,
                                         kafs_iblkcnt_t start, kafs_iblkcnt_t end,
                                         kafs_blkcnt_t **deferred_free, size_t *deferred_free_cnt,
                                         size_t *deferred_free_cap)
{
  const kafs_iblkcnt_t trunc_batch = 64;
  kafs_iblkcnt_t current = start;
  while (current < end)
  {
    kafs_iblkcnt_t batch_end = current + trunc_batch;
    if (batch_end > end)
      batch_end = end;

    int rc = kafs_truncate_collect_free_range(ctx, inoent, current, batch_end, deferred_free,
                                              deferred_free_cnt, deferred_free_cap);
    if (rc != 0)
      return rc;
    current = batch_end;
  }
  return 0;
}

static int kafs_truncate_handle_indirect_to_direct(
    struct kafs_context *ctx, kafs_sinode_t *inoent, uint32_t ino_idx, kafs_off_t filesize_new,
    kafs_iblkcnt_t iblocnt, kafs_blkcnt_t **deferred_free, size_t *deferred_free_cnt,
    size_t *deferred_free_cap, kafs_blksize_t blksize)
{
  char buf[blksize];
  KAFS_CALL(kafs_ino_iblk_read, ctx, inoent, 0, buf);

  kafs_ino_size_set(inoent, filesize_new);
  int rc = kafs_truncate_collect_batches(ctx, inoent, 0, iblocnt, deferred_free, deferred_free_cnt,
                                         deferred_free_cap);
  if (rc != 0)
    return rc;

  memcpy(inoent->i_blkreftbl, buf, (size_t)filesize_new);
  if (filesize_new < KAFS_INODE_DIRECT_BYTES)
    memset((void *)inoent->i_blkreftbl + filesize_new, 0, KAFS_INODE_DIRECT_BYTES - filesize_new);
  if (kafs_tailmeta_inode_is_regular_v5(ctx, inoent))
    kafs_tailmeta_inode_taildesc_set_inline(ctx, inoent);
  kafs_truncate_release_deferred_refs(ctx, ino_idx, *deferred_free, *deferred_free_cnt);
  return KAFS_SUCCESS;
}

static int kafs_truncate_handle_indirect_shrink(
    struct kafs_context *ctx, kafs_sinode_t *inoent, uint32_t ino_idx, kafs_off_t filesize_new,
    kafs_iblkcnt_t iblooff, kafs_iblkcnt_t iblocnt, kafs_blksize_t off, kafs_blksize_t blksize,
    kafs_blkcnt_t **deferred_free, size_t *deferred_free_cnt, size_t *deferred_free_cap)
{
  kafs_ino_size_set(inoent, filesize_new);

  if (off > 0)
  {
    char buf[blksize];
    kafs_blkcnt_t cur_blo = KAFS_BLO_NONE;
    (void)kafs_ino_ibrk_run(ctx, inoent, iblooff, &cur_blo, KAFS_IBLKREF_FUNC_GET);
    kafs_dlog(2,
              "%s: ino=%u partial-tail iblo=%" PRIuFAST32 " off=%" PRIuFAST16
              " cur_blo=%" PRIuFAST32 "\n",
              __func__, ino_idx, iblooff, off, cur_blo);
    KAFS_CALL(kafs_ino_iblk_read, ctx, inoent, iblooff, buf);
    memset(buf + off, 0, blksize - off);
    KAFS_CALL(kafs_ino_iblk_write, ctx, inoent, iblooff, buf);
    (void)kafs_ino_ibrk_run(ctx, inoent, iblooff, &cur_blo, KAFS_IBLKREF_FUNC_GET);
    kafs_dlog(2, "%s: ino=%u partial-tail wrote iblo=%" PRIuFAST32 " new_blo=%" PRIuFAST32 "\n",
              __func__, ino_idx, iblooff, cur_blo);
    iblooff++;
  }

  if (kafs_tailmeta_inode_is_regular_v5(ctx, inoent))
    kafs_tailmeta_inode_taildesc_set_full_block(ctx, inoent);

  int rc = kafs_truncate_collect_batches(ctx, inoent, iblooff, iblocnt, deferred_free,
                                         deferred_free_cnt, deferred_free_cap);
  if (rc != 0)
    return rc;

  kafs_truncate_release_deferred_refs(ctx, ino_idx, *deferred_free, *deferred_free_cnt);
  return kafs_tailmeta_normalize_block_layout(ctx, inoent);
}

static int kafs_truncate(struct kafs_context *ctx, kafs_sinode_t *inoent, kafs_off_t filesize_new)
{
  uint32_t ino_idx = kafs_ctx_ino_no(ctx, inoent);
  kafs_dlog(2, "%s(ino = %d, filesize_new = %" PRIuFAST64 ")\n", __func__, ino_idx, filesize_new);
  assert(ctx != NULL);
  assert(inoent != NULL);
  assert(kafs_ino_get_usage(inoent));
  kafs_logblksize_t log_blksize = kafs_sb_log_blksize_get(ctx->c_superblock);
  kafs_blksize_t blksize = kafs_sb_blksize_get(ctx->c_superblock);
  kafs_off_t filesize_orig = kafs_ino_size_get(inoent);
  if (filesize_orig == filesize_new)
    return KAFS_SUCCESS;
  kafs_blkcnt_t *deferred_free = NULL;
  size_t deferred_free_cnt = 0;
  size_t deferred_free_cap = 0;
  (void)kafs_inode_epoch_bump(ctx, ino_idx);
  const kafs_sinode_taildesc_v5_t *taildesc = kafs_ctx_inode_taildesc_v5_const(ctx, inoent);
  if (taildesc &&
      kafs_ino_taildesc_v5_layout_kind_get(taildesc) == KAFS_TAIL_LAYOUT_MIXED_FULL_TAIL)
  {
    int rc = kafs_tailmeta_materialize_mixed_to_full_block(ctx, inoent);
    if (rc != 0)
      return rc;
    taildesc = kafs_ctx_inode_taildesc_v5_const(ctx, inoent);
  }
  if (taildesc && kafs_ino_taildesc_v5_layout_kind_get(taildesc) == KAFS_TAIL_LAYOUT_TAIL_ONLY)
  {
    int rc = kafs_truncate_handle_tail_only(ctx, inoent, taildesc, filesize_new, blksize);
    free(deferred_free);
    return rc;
  }
  if (filesize_new > filesize_orig)
  {
    int rc = kafs_truncate_handle_growth(ctx, inoent, filesize_orig, filesize_new, blksize);
    free(deferred_free);
    return rc;
  }
  assert(filesize_new < filesize_orig);
  kafs_iblkcnt_t iblooff = filesize_new >> log_blksize;
  kafs_iblkcnt_t iblocnt = (filesize_orig + blksize - 1) >> log_blksize;
  kafs_blksize_t off = (kafs_blksize_t)(filesize_new & (blksize - 1));

  if (filesize_orig <= KAFS_INODE_DIRECT_BYTES)
    return kafs_truncate_handle_direct_shrink(ctx, inoent, filesize_orig, filesize_new);

  if (filesize_new <= KAFS_INODE_DIRECT_BYTES)
  {
    int rc = kafs_truncate_handle_indirect_to_direct(ctx, inoent, ino_idx, filesize_new, iblocnt,
                                                     &deferred_free, &deferred_free_cnt,
                                                     &deferred_free_cap, blksize);
    free(deferred_free);
    return rc;
  }

  int rc = kafs_truncate_handle_indirect_shrink(ctx, inoent, ino_idx, filesize_new, iblooff,
                                                iblocnt, off, blksize, &deferred_free,
                                                &deferred_free_cnt, &deferred_free_cap);
  free(deferred_free);
  return rc;
}

__attribute_maybe_unused__ static int kafs_trim(struct kafs_context *ctx, kafs_sinode_t *inoent,
                                                kafs_off_t off, kafs_off_t size)
{
  kafs_dlog(2, "%s(ino = %d, off = %" PRIuFAST64 ", size = %" PRIuFAST64 ")\n", __func__,
            kafs_ctx_ino_no(ctx, inoent), off, size);
  assert(ctx != NULL);
  assert(inoent != NULL);
  assert(kafs_ino_get_usage(inoent));
  if (size == 0)
    return size;
  kafs_off_t size_orig = kafs_ino_size_get(inoent);
  if (off >= size_orig)
    return 0;
  if (off + size >= size_orig)
  {
    KAFS_CALL(kafs_truncate, ctx, inoent, off);
    return size_orig - off;
  }
  // Slow but correct implementation: shift tail data left in bounded chunks.
  kafs_off_t src = off + size;
  kafs_off_t dst = off;
  kafs_off_t tail = size_orig - src;
  kafs_blksize_t blksize = kafs_sb_blksize_get(ctx->c_superblock);
  const size_t CHUNK_MAX = (size_t)blksize * 4u;
  char *buf = (char *)malloc(CHUNK_MAX);
  if (buf == NULL)
    return -ENOMEM;
  while (tail > 0)
  {
    size_t chunk = (tail > (kafs_off_t)CHUNK_MAX) ? CHUNK_MAX : (size_t)tail;
    ssize_t r = KAFS_CALL(kafs_pread, ctx, inoent, buf, (kafs_off_t)chunk, src);
    if (r < 0)
    {
      free(buf);
      return (int)r;
    }
    if (r == 0)
      break;
    ssize_t w = KAFS_CALL(kafs_pwrite, ctx, inoent, buf, (kafs_off_t)r, dst);
    if (w != r)
    {
      free(buf);
      return (w < 0) ? (int)w : -EIO;
    }
    src += r;
    dst += r;
    tail -= r;
  }
  free(buf);
  KAFS_CALL(kafs_truncate, ctx, inoent, dst);
  return KAFS_SUCCESS;
}

__attribute_maybe_unused__ static int kafs_release(struct kafs_context *ctx, kafs_sinode_t *inoent)
{
  // Requires: caller holds inode lock for inoent.
  if (kafs_ino_linkcnt_decr(inoent) == 0)
  {
    KAFS_CALL(kafs_truncate, ctx, inoent, 0);
    kafs_diag_clear_create_event(ctx, kafs_ctx_ino_no(ctx, inoent));
    kafs_ctx_inode_zero(ctx, inoent);
    // Best-effort accounting (avoid taking inode_alloc_lock here to prevent lock inversion).
    kafs_sb_inocnt_free_incr(ctx->c_superblock);
    kafs_sb_wtime_set(ctx->c_superblock, kafs_now());
  }
  return KAFS_SUCCESS;
}

static int kafs_time_is_zero(kafs_time_t ts) { return ts.tv_sec == 0 && ts.tv_nsec == 0; }

static int kafs_inode_is_tombstone(const kafs_sinode_t *inoent)
{
  if (!inoent || !kafs_ino_get_usage(inoent))
    return 0;
  if (kafs_ino_linkcnt_get(inoent) != 0)
    return 0;
  return !kafs_time_is_zero(kafs_ino_dtime_get(inoent));
}

// Requires: caller holds inode lock for ino.
static int kafs_try_reclaim_unlinked_inode_locked(struct kafs_context *ctx, kafs_inocnt_t ino,
                                                  int *reclaimed)
{
  assert(ctx != NULL);
  assert(reclaimed != NULL);
  *reclaimed = 0;

  kafs_sinode_t *inoent = kafs_ctx_inode(ctx, ino);
  if (!kafs_ino_get_usage(inoent))
    return KAFS_SUCCESS;
  if (kafs_ino_linkcnt_get(inoent) != 0)
    return KAFS_SUCCESS;
  if (ctx->c_open_cnt)
  {
    uint32_t open_cnt = __atomic_load_n(&ctx->c_open_cnt[ino], __ATOMIC_RELAXED);
    if (open_cnt != 0)
      return KAFS_SUCCESS;
  }

  (void)kafs_inode_epoch_bump(ctx, (uint32_t)ino);
  int trc = kafs_truncate(ctx, inoent, 0);
  if (trc < 0)
  {
    kafs_log(KAFS_LOG_WARNING, "%s: truncate failed ino=%" PRIuFAST32 " rc=%d\n", __func__,
             (uint32_t)ino, trc);
  }
  kafs_diag_clear_create_event(ctx, ino);
  kafs_ctx_inode_zero(ctx, inoent);
  *reclaimed = 1;
  return KAFS_SUCCESS;
}

static int kafs_dir_snapshot(struct kafs_context *ctx, kafs_sinode_t *inoent_dir, char **out,
                             size_t *out_len);

static int kafs_dir_writeback(struct kafs_context *ctx, kafs_sinode_t *inoent_dir, const char *buf,
                              size_t len);

typedef struct
{
  kafs_sdir_v4_hdr_t hdr;
  size_t data_off;
  size_t logical_len;
} kafs_dir_snapshot_meta_t;

typedef struct
{
  size_t record_off;
  size_t record_len;
  kafs_inocnt_t ino;
  kafs_filenamelen_t name_len;
  const char *name;
  uint16_t flags;
  uint32_t name_hash;
} kafs_dirent_view_t;

static int kafs_dirent_view_next(const char *buf, size_t len, size_t off, kafs_dirent_view_t *out);
static int kafs_dirent_view_next_meta(const char *buf, const kafs_dir_snapshot_meta_t *meta,
                                      size_t off, kafs_dirent_view_t *out);

static uint32_t kafs_dirent_name_hash(const char *name, kafs_filenamelen_t namelen)
{
  uint32_t hash = 2166136261u;
  for (kafs_filenamelen_t i = 0; i < namelen; ++i)
  {
    hash ^= (uint8_t)name[i];
    hash *= 16777619u;
  }
  return hash;
}

static int kafs_dir_snapshot_meta_load(const char *buf, size_t len, kafs_dir_snapshot_meta_t *out)
{
  assert(out != NULL);
  memset(out, 0, sizeof(*out));
  out->data_off = sizeof(kafs_sdir_v4_hdr_t);
  if (len == 0)
  {
    kafs_dir_v4_hdr_init(&out->hdr);
    out->logical_len = 0;
    return 0;
  }
  if (len < sizeof(kafs_sdir_v4_hdr_t))
    return -EIO;

  memcpy(&out->hdr, buf, sizeof(out->hdr));
  if (kafs_u32_stoh(out->hdr.dh_magic) != KAFS_DIRENT_V4_MAGIC)
    return -EIO;
  if (kafs_dir_v4_hdr_format_get(&out->hdr) != KAFS_DIRENT_V4_FORMAT_VERSION)
    return -EPROTONOSUPPORT;
  if (kafs_dir_v4_hdr_flags_get(&out->hdr) != 0u)
    return -EIO;

  size_t record_bytes = (size_t)kafs_dir_v4_hdr_record_bytes_get(&out->hdr);
  if (record_bytes > len - sizeof(kafs_sdir_v4_hdr_t))
    return -EIO;

  out->logical_len = sizeof(kafs_sdir_v4_hdr_t) + record_bytes;
  return 0;
}

static int kafs_dir_v4_read_header(struct kafs_context *ctx, kafs_sinode_t *inoent_dir,
                                   kafs_sdir_v4_hdr_t *hdr)
{
  assert(ctx != NULL);
  assert(inoent_dir != NULL);
  assert(hdr != NULL);

  kafs_off_t filesize = kafs_ino_size_get(inoent_dir);
  kafs_dir_v4_hdr_init(hdr);
  if (filesize == 0)
    return 0;
  if (filesize < (kafs_off_t)sizeof(*hdr))
    return -EIO;

  ssize_t r = kafs_pread(ctx, inoent_dir, hdr, (kafs_off_t)sizeof(*hdr), 0);
  if (r < 0)
    return (int)r;
  if (r != (ssize_t)sizeof(*hdr))
    return -EIO;
  if (kafs_u32_stoh(hdr->dh_magic) != KAFS_DIRENT_V4_MAGIC)
    return -EIO;
  if (kafs_dir_v4_hdr_format_get(hdr) != KAFS_DIRENT_V4_FORMAT_VERSION)
    return -EPROTONOSUPPORT;
  if (kafs_dir_v4_hdr_flags_get(hdr) != 0u)
    return -EIO;
  if ((kafs_off_t)sizeof(*hdr) + (kafs_off_t)kafs_dir_v4_hdr_record_bytes_get(hdr) > filesize)
    return -EIO;
  return 0;
}

static int kafs_dir_v4_write_header(struct kafs_context *ctx, kafs_sinode_t *inoent_dir,
                                    const kafs_sdir_v4_hdr_t *hdr)
{
  ssize_t w = kafs_pwrite(ctx, inoent_dir, hdr, (kafs_off_t)sizeof(*hdr), 0);
  if (w < 0)
    return (int)w;
  return (w == (ssize_t)sizeof(*hdr)) ? 0 : -EIO;
}

static int kafs_dir_v4_write_record_head(struct kafs_context *ctx, kafs_sinode_t *inoent_dir,
                                         size_t rec_off, uint16_t rec_len, uint16_t flags,
                                         kafs_inocnt_t ino, kafs_filenamelen_t namelen,
                                         uint32_t name_hash)
{
  kafs_sdirent_v4_t rec;
  memset(&rec, 0, sizeof(rec));
  kafs_dirent_v4_rec_len_set(&rec, rec_len);
  kafs_dirent_v4_flags_set(&rec, flags);
  kafs_dirent_v4_ino_set(&rec, ino);
  kafs_dirent_v4_filenamelen_set(&rec, namelen);
  kafs_dirent_v4_name_hash_set(&rec, name_hash);
  ssize_t w = kafs_pwrite(ctx, inoent_dir, &rec, (kafs_off_t)sizeof(rec), (kafs_off_t)rec_off);
  if (w < 0)
    return (int)w;
  return (w == (ssize_t)sizeof(rec)) ? 0 : -EIO;
}

/// @brief ディレクトリエントリから対象のファイル名を探す
/// @param ctx コンテキスト
/// @param name ファイル名
/// @param namelen ファイル名の長さ
/// @param ino 対象のディレクトリ
/// @param pino_found 見つかったエントリ
/// @return 0: 成功, < 0: 失敗 (-errno)
static int kafs_dirent_search_snapshot(struct kafs_context *ctx, const char *snap, size_t snap_len,
                                       const char *filename, kafs_filenamelen_t filenamelen,
                                       uint32_t dir_ino, kafs_sinode_t **pinoent_found)
{
  assert(ctx != NULL);
  assert(filename != NULL);
  assert(filenamelen > 0);
  assert(pinoent_found != NULL);

  if (snap_len == 0)
    return -ENOENT;

  kafs_dir_snapshot_meta_t meta;
  __atomic_add_fetch(&ctx->c_stat_dir_snapshot_meta_load_calls, 1u, __ATOMIC_RELAXED);
  int rc_meta = kafs_dir_snapshot_meta_load(snap, snap_len, &meta);
  if (rc_meta < 0)
    return rc_meta;

  uint32_t target_hash = kafs_dirent_name_hash(filename, filenamelen);

  size_t off = 0;
  while (1)
  {
    kafs_dirent_view_t view;
    __atomic_add_fetch(&ctx->c_stat_dirent_view_next_calls, 1u, __ATOMIC_RELAXED);
    int step = kafs_dirent_view_next_meta(snap, &meta, off, &view);
    if (step == 0)
      break;
    if (step < 0)
    {
      kafs_dlog(1, "%s: parse failed dir_ino=%u off=%zu snap_len=%zu target=%.*s\n", __func__,
                (unsigned)dir_ino, off, snap_len, (int)filenamelen, filename);
      return -EIO;
    }

    if ((view.flags & KAFS_DIRENT_FLAG_TOMBSTONE) == 0 && view.name_hash == target_hash &&
        view.name_len == filenamelen && memcmp(view.name, filename, filenamelen) == 0)
    {
      *pinoent_found = kafs_ctx_inode(ctx, view.ino);
      return KAFS_SUCCESS;
    }
    off = view.record_off + view.record_len;
  }

  return -ENOENT;
}

static int kafs_dirent_search(struct kafs_context *ctx, kafs_sinode_t *inoent, const char *filename,
                              kafs_filenamelen_t filenamelen, kafs_sinode_t **pinoent_found)
{
  kafs_dlog(3, "%s(ino = %d, filename = %.*s, filenamelen = %" PRIuFAST16 ")\n", __func__,
            kafs_ctx_ino_no(ctx, inoent), (int)filenamelen, filename, filenamelen);
  assert(ctx != NULL);
  assert(inoent != NULL);
  assert(filename != NULL);
  assert(filenamelen > 0);
  assert(pinoent_found != NULL);
  assert(kafs_ino_get_usage(inoent));
  kafs_mode_t mode = kafs_ino_mode_get(inoent);
  if (!S_ISDIR(mode))
    return -ENOTDIR;
  char *snap = NULL;
  size_t snap_len = 0;
  int rc = kafs_dir_snapshot(ctx, inoent, &snap, &snap_len);
  if (rc < 0)
    return rc;

  rc = kafs_dirent_search_snapshot(ctx, snap, snap_len, filename, filenamelen,
                                   (uint32_t)kafs_ctx_ino_no(ctx, inoent), pinoent_found);
  free(snap);
  return rc;
}

static int kafs_dir_snapshot(struct kafs_context *ctx, kafs_sinode_t *inoent_dir, char **out,
                             size_t *out_len)
{
  *out = NULL;
  *out_len = 0;
  size_t len = (size_t)kafs_ino_size_get(inoent_dir);
  __atomic_add_fetch(&ctx->c_stat_dir_snapshot_calls, 1u, __ATOMIC_RELAXED);
  __atomic_add_fetch(&ctx->c_stat_dir_snapshot_bytes, (uint64_t)len, __ATOMIC_RELAXED);
  if (len == 0)
    return 0;
  char *buf = (char *)malloc(len);
  if (!buf)
    return -ENOMEM;
  ssize_t r = kafs_pread(ctx, inoent_dir, buf, (kafs_off_t)len, 0);
  if (r < 0 || (size_t)r != len)
  {
    kafs_dlog(1, "%s: pread failed ino=%d req=%zu got=%zd\n", __func__,
              (int)kafs_ctx_ino_no(ctx, inoent_dir), len, r);
    free(buf);
    return -EIO;
  }
  *out = buf;
  *out_len = len;
  return 0;
}

static int kafs_dir_writeback(struct kafs_context *ctx, kafs_sinode_t *inoent_dir, const char *buf,
                              size_t len)
{
  size_t old = (size_t)kafs_ino_size_get(inoent_dir);
  if (len)
  {
    ssize_t w = kafs_pwrite(ctx, inoent_dir, buf, (kafs_off_t)len, 0);
    if (w < 0 || (size_t)w != len)
      return -EIO;
  }
  if (len < old)
    return kafs_truncate(ctx, inoent_dir, (kafs_off_t)len);
  if (len == 0 && old)
    return kafs_truncate(ctx, inoent_dir, 0);
  return 0;
}

static int kafs_dirent_view_next(const char *buf, size_t len, size_t off, kafs_dirent_view_t *out)
{
  kafs_dir_snapshot_meta_t meta;
  int rc = kafs_dir_snapshot_meta_load(buf, len, &meta);
  if (rc < 0)
    return rc;
  return kafs_dirent_view_next_meta(buf, &meta, off, out);
}

static int kafs_dirent_view_next_meta(const char *buf, const kafs_dir_snapshot_meta_t *meta,
                                      size_t off, kafs_dirent_view_t *out)
{
  if (meta == NULL)
    return -EINVAL;
  if (meta->logical_len == 0)
    return 0;

  size_t cur = off;
  if (cur < meta->data_off)
    cur = meta->data_off;
  if (cur >= meta->logical_len)
    return 0;
  if (meta->logical_len - cur < sizeof(kafs_sdirent_v4_t))
    return -EIO;

  kafs_sdirent_v4_t rec;
  memcpy(&rec, buf + cur, sizeof(rec));
  uint16_t rec_len = kafs_dirent_v4_rec_len_get(&rec);
  uint16_t flags = kafs_dirent_v4_flags_get(&rec);
  kafs_inocnt_t ino = kafs_dirent_v4_ino_get(&rec);
  kafs_filenamelen_t namelen = kafs_dirent_v4_filenamelen_get(&rec);
  uint32_t name_hash = kafs_dirent_v4_name_hash_get(&rec);
  if (rec_len < sizeof(kafs_sdirent_v4_t) || cur + rec_len > meta->logical_len)
    return -EIO;
  if ((flags & ~KAFS_DIRENT_FLAG_TOMBSTONE) != 0u)
    return -EIO;
  if (namelen == 0 || namelen >= FILENAME_MAX)
  {
    kafs_dlog(1,
              "%s: invalid dirent header off=%zu len=%zu ino=%" PRIuFAST16 " namelen=%" PRIuFAST16
              " flags=%u\n",
              __func__, cur, meta->logical_len, (uint_fast16_t)ino, (uint_fast16_t)namelen,
              (unsigned)flags);
    return -EIO;
  }
  if ((size_t)namelen > rec_len - sizeof(kafs_sdirent_v4_t))
    return -EIO;
  if ((flags & KAFS_DIRENT_FLAG_TOMBSTONE) == 0 && ino == KAFS_INO_NONE)
    return -EIO;

  out->record_off = cur;
  out->record_len = rec_len;
  out->ino = ino;
  out->name_len = namelen;
  out->name = buf + cur + sizeof(kafs_sdirent_v4_t);
  out->flags = flags;
  out->name_hash = name_hash;
  return 1;
}

static int kafs_dir_is_empty_locked(struct kafs_context *ctx, kafs_sinode_t *inoent_dir)
{
  kafs_sdir_v4_hdr_t hdr;
  int rc = kafs_dir_v4_read_header(ctx, inoent_dir, &hdr);
  if (rc < 0)
    return rc;

  uint32_t live_count = kafs_dir_v4_hdr_live_count_get(&hdr);
  uint32_t floor = (kafs_ctx_ino_no(ctx, inoent_dir) == KAFS_INO_ROOTDIR) ? 0u : 1u;
  return (live_count <= floor) ? 1 : 0;
}

// NOTE: caller holds dir inode lock. For rename(2) we must not change linkcount of the moved inode.
static int kafs_dirent_find_existing_v4(struct kafs_context *ctx, const char *old,
                                        const kafs_dir_snapshot_meta_t *meta, const char *filename,
                                        kafs_filenamelen_t filenamelen, uint32_t target_hash,
                                        kafs_dirent_view_t *tombstone_out, int *have_tombstone_out)
{
  size_t off = 0;
  while (1)
  {
    kafs_dirent_view_t view;
    __atomic_add_fetch(&ctx->c_stat_dirent_view_next_calls, 1u, __ATOMIC_RELAXED);
    int step = kafs_dirent_view_next_meta(old, meta, off, &view);
    if (step == 0)
      return 0;
    if (step < 0)
      return -EIO;
    if (view.name_hash == target_hash && view.name_len == filenamelen &&
        memcmp(view.name, filename, filenamelen) == 0)
    {
      if ((view.flags & KAFS_DIRENT_FLAG_TOMBSTONE) != 0)
      {
        *tombstone_out = view;
        *have_tombstone_out = 1;
      }
      else
      {
        return -EEXIST;
      }
    }
    off = view.record_off + view.record_len;
  }
}

static int kafs_dirent_reuse_tombstone(struct kafs_context *ctx, kafs_sinode_t *inoent_dir,
                                       kafs_dirent_view_t tombstone, kafs_inocnt_t ino,
                                       kafs_filenamelen_t filenamelen, uint32_t target_hash,
                                       kafs_sdir_v4_hdr_t *hdr)
{
  uint32_t live_count = kafs_dir_v4_hdr_live_count_get(hdr);
  uint32_t tombstone_count = kafs_dir_v4_hdr_tombstone_count_get(hdr);
  int rc = kafs_dir_v4_write_record_head(ctx, inoent_dir, tombstone.record_off,
                                         (uint16_t)tombstone.record_len, 0u, ino, filenamelen,
                                         target_hash);
  if (rc == 0)
  {
    kafs_dir_v4_hdr_live_count_set(hdr, live_count + 1u);
    kafs_dir_v4_hdr_tombstone_count_set(hdr, tombstone_count - 1u);
    rc = kafs_dir_v4_write_header(ctx, inoent_dir, hdr);
  }
  return rc;
}

static int kafs_dirent_append_new_v4(struct kafs_context *ctx, kafs_sinode_t *inoent_dir,
                                     size_t old_len, const kafs_dir_snapshot_meta_t *meta,
                                     kafs_sdir_v4_hdr_t *hdr, kafs_inocnt_t ino,
                                     const char *filename, kafs_filenamelen_t filenamelen,
                                     uint32_t target_hash)
{
  size_t rec_len = sizeof(kafs_sdirent_v4_t) + (size_t)filenamelen;
  size_t append_off = (old_len == 0) ? sizeof(kafs_sdir_v4_hdr_t) : meta->logical_len;
  if (old_len != 0 && append_off != old_len)
    return -EIO;

  char recbuf[sizeof(kafs_sdirent_v4_t) + FILENAME_MAX];
  kafs_sdirent_v4_t *rec = (kafs_sdirent_v4_t *)recbuf;
  memset(recbuf, 0, rec_len);
  kafs_dirent_v4_rec_len_set(rec, (uint16_t)rec_len);
  kafs_dirent_v4_flags_set(rec, 0u);
  kafs_dirent_v4_ino_set(rec, ino);
  kafs_dirent_v4_filenamelen_set(rec, filenamelen);
  kafs_dirent_v4_name_hash_set(rec, target_hash);
  memcpy(recbuf + sizeof(kafs_sdirent_v4_t), filename, filenamelen);

  if (old_len == 0)
  {
    int rc = kafs_dir_v4_write_header(ctx, inoent_dir, hdr);
    if (rc < 0)
      return rc;
  }

  ssize_t w = kafs_pwrite(ctx, inoent_dir, recbuf, (kafs_off_t)rec_len, (kafs_off_t)append_off);
  if (w < 0 || (size_t)w != rec_len)
    return (w < 0) ? (int)w : -EIO;

  uint32_t live_count = kafs_dir_v4_hdr_live_count_get(hdr);
  uint32_t tombstone_count = kafs_dir_v4_hdr_tombstone_count_get(hdr);
  uint32_t record_bytes = kafs_dir_v4_hdr_record_bytes_get(hdr);
  kafs_dir_v4_hdr_live_count_set(hdr, live_count + 1u);
  kafs_dir_v4_hdr_tombstone_count_set(hdr, tombstone_count);
  kafs_dir_v4_hdr_record_bytes_set(hdr, record_bytes + (uint32_t)rec_len);
  return kafs_dir_v4_write_header(ctx, inoent_dir, hdr);
}

static int kafs_dirent_add_nolink(struct kafs_context *ctx, kafs_sinode_t *inoent_dir,
                                  kafs_inocnt_t ino, const char *filename)
{
  assert(ctx != NULL);
  assert(inoent_dir != NULL);
  assert(filename != NULL);
  assert(ino != KAFS_INO_NONE);
  if (!S_ISDIR(kafs_ino_mode_get(inoent_dir)))
    return -ENOTDIR;

  kafs_filenamelen_t filenamelen = (kafs_filenamelen_t)strlen(filename);
  if (filenamelen == 0 || filenamelen >= FILENAME_MAX)
    return -EINVAL;

  char *old = NULL;
  size_t old_len = 0;
  int rc = kafs_dir_snapshot(ctx, inoent_dir, &old, &old_len);
  if (rc < 0)
    return rc;

  kafs_dir_snapshot_meta_t meta;
  __atomic_add_fetch(&ctx->c_stat_dir_snapshot_meta_load_calls, 1u, __ATOMIC_RELAXED);
  rc = kafs_dir_snapshot_meta_load(old, old_len, &meta);
  if (rc < 0)
  {
    free(old);
    return rc;
  }

  uint32_t target_hash = kafs_dirent_name_hash(filename, filenamelen);
  kafs_dirent_view_t tombstone = {0};
  int have_tombstone = 0;
  rc = kafs_dirent_find_existing_v4(ctx, old, &meta, filename, filenamelen, target_hash, &tombstone,
                                    &have_tombstone);
  if (rc < 0)
  {
    free(old);
    return rc;
  }

  kafs_sdir_v4_hdr_t hdr = meta.hdr;
  if (old_len == 0)
    kafs_dir_v4_hdr_init(&hdr);

  if (have_tombstone)
  {
    rc = kafs_dirent_reuse_tombstone(ctx, inoent_dir, tombstone, ino, filenamelen, target_hash,
                                     &hdr);
    free(old);
    return rc;
  }

  rc = kafs_dirent_append_new_v4(ctx, inoent_dir, old_len, &meta, &hdr, ino, filename, filenamelen,
                                 target_hash);
  free(old);
  return rc;
}

static int kafs_dirent_add(struct kafs_context *ctx, kafs_sinode_t *inoent_dir, kafs_inocnt_t ino,
                           const char *filename)
{
  int rc = kafs_dirent_add_nolink(ctx, inoent_dir, ino, filename);
  if (rc == 0)
    kafs_ino_linkcnt_incr(kafs_ctx_inode(ctx, ino));
  return rc;
}

// NOTE: caller holds dir inode lock.
static int kafs_dirent_remove_nolink(struct kafs_context *ctx, kafs_sinode_t *inoent_dir,
                                     const char *filename, kafs_inocnt_t *out_ino)
{
  assert(ctx != NULL);
  assert(inoent_dir != NULL);
  assert(filename != NULL);
  if (out_ino)
    *out_ino = KAFS_INO_NONE;
  if (!S_ISDIR(kafs_ino_mode_get(inoent_dir)))
    return -ENOTDIR;

  kafs_filenamelen_t filenamelen = (kafs_filenamelen_t)strlen(filename);
  if (filenamelen == 0 || filenamelen >= FILENAME_MAX)
    return -EINVAL;

  char *old = NULL;
  size_t old_len = 0;
  int rc = kafs_dir_snapshot(ctx, inoent_dir, &old, &old_len);
  if (rc < 0)
    return rc;

  kafs_dir_snapshot_meta_t meta;
  __atomic_add_fetch(&ctx->c_stat_dir_snapshot_meta_load_calls, 1u, __ATOMIC_RELAXED);
  rc = kafs_dir_snapshot_meta_load(old, old_len, &meta);
  if (rc < 0)
  {
    free(old);
    return rc;
  }

  uint32_t target_hash = kafs_dirent_name_hash(filename, filenamelen);

  size_t off = 0;
  while (1)
  {
    kafs_dirent_view_t view;
    __atomic_add_fetch(&ctx->c_stat_dirent_view_next_calls, 1u, __ATOMIC_RELAXED);
    int step = kafs_dirent_view_next_meta(old, &meta, off, &view);
    if (step == 0)
      break;
    if (step < 0)
    {
      free(old);
      return -EIO;
    }
    if ((view.flags & KAFS_DIRENT_FLAG_TOMBSTONE) == 0 && view.name_hash == target_hash &&
        view.name_len == filenamelen && memcmp(view.name, filename, filenamelen) == 0)
    {
      kafs_sdir_v4_hdr_t hdr = meta.hdr;
      uint32_t live_count = kafs_dir_v4_hdr_live_count_get(&hdr);
      uint32_t tombstone_count = kafs_dir_v4_hdr_tombstone_count_get(&hdr);
      rc = kafs_dir_v4_write_record_head(ctx, inoent_dir, view.record_off,
                                         (uint16_t)view.record_len, KAFS_DIRENT_FLAG_TOMBSTONE,
                                         view.ino, view.name_len, view.name_hash);
      if (rc == 0)
      {
        kafs_dir_v4_hdr_live_count_set(&hdr, live_count - 1u);
        kafs_dir_v4_hdr_tombstone_count_set(&hdr, tombstone_count + 1u);
        rc = kafs_dir_v4_write_header(ctx, inoent_dir, &hdr);
      }
      if (rc == 0 && out_ino)
        *out_ino = view.ino;
      free(old);
      return rc;
    }
    off = view.record_off + view.record_len;
  }

  free(old);
  return -ENOENT;
}

static int kafs_dirent_remove(struct kafs_context *ctx, kafs_sinode_t *inoent_dir,
                              const char *filename)
{
  kafs_inocnt_t d_ino;
  int rc = kafs_dirent_remove_nolink(ctx, inoent_dir, filename, &d_ino);
  if (rc == 0 && d_ino != KAFS_INO_NONE)
    kafs_ino_linkcnt_decr(kafs_ctx_inode(ctx, d_ino));
  return rc;
}

static kafs_bool_t kafs_access_group_match(gid_t file_gid, gid_t gid, size_t ngroups,
                                           const gid_t groups[])
{
  if (gid == file_gid)
    return KAFS_TRUE;
  for (size_t i = 0; i < ngroups; ++i)
    if (file_gid == groups[i])
      return KAFS_TRUE;
  return KAFS_FALSE;
}

static kafs_bool_t kafs_access_allowed(mode_t mode, mode_t other_bit, mode_t user_bit,
                                       mode_t group_bit, uid_t uid, uid_t file_uid, gid_t gid,
                                       gid_t file_gid, size_t ngroups, const gid_t groups[])
{
  if (mode & other_bit)
    return KAFS_TRUE;
  if ((mode & user_bit) && uid == file_uid)
    return KAFS_TRUE;
  if ((mode & group_bit) && kafs_access_group_match(file_gid, gid, ngroups, groups))
    return KAFS_TRUE;
  return KAFS_FALSE;
}

// cppcheck-suppress constParameterCallback
static int kafs_access_check(int ok, kafs_sinode_t *inoent, kafs_bool_t is_dir, uid_t uid,
                             gid_t gid, size_t ngroups,
                             /* cppcheck-suppress constParameterCallback */ gid_t groups[])
{
  mode_t mode = kafs_ino_mode_get(inoent);
  uid_t fuid = kafs_ino_uid_get(inoent);
  gid_t fgid = kafs_ino_gid_get(inoent);
  if (is_dir)
  {
    if (!S_ISDIR(mode))
    {
      kafs_dlog(1, "%s: ENOTDIR (mode=%o uid=%u gid=%u)\n", __func__, (unsigned)mode,
                (unsigned)fuid, (unsigned)fgid);
      return -ENOTDIR;
    }
    if (ok == F_OK)
      ok = X_OK;
  }
  if (ok == F_OK)
    return KAFS_SUCCESS;

  // Superuser semantics (uid 0): bypass R/W checks. For X_OK, require at least one
  // execute bit on non-directories; directories remain traversable.
  if (uid == 0)
  {
    if (!(ok & X_OK))
      return KAFS_SUCCESS;
    if (is_dir || (mode & (S_IXUSR | S_IXGRP | S_IXOTH)))
      return KAFS_SUCCESS;
    return -EACCES;
  }

  if (ok & R_OK)
    if (!kafs_access_allowed(mode, S_IROTH, S_IRUSR, S_IRGRP, uid, fuid, gid, fgid, ngroups,
                             groups))
      return -EACCES;
  if (ok & W_OK)
    if (!kafs_access_allowed(mode, S_IWOTH, S_IWUSR, S_IWGRP, uid, fuid, gid, fgid, ngroups,
                             groups))
      return -EACCES;
  if (ok & X_OK)
    if (!kafs_access_allowed(mode, S_IXOTH, S_IXUSR, S_IXGRP, uid, fuid, gid, fgid, ngroups,
                             groups))
      return -EACCES;
  return KAFS_SUCCESS;
}

static int kafs_resolve_fh_inode(struct kafs_context *ctx, const struct fuse_file_info *fi,
                                 kafs_sinode_t **pinoent)
{
  assert(ctx != NULL);
  assert(fi != NULL);
  assert(pinoent != NULL);

  kafs_inocnt_t ino = (kafs_inocnt_t)fi->fh;
  if (ino >= kafs_sb_inocnt_get(ctx->c_superblock))
    return -EBADF;

  kafs_sinode_t *inoent = kafs_ctx_inode(ctx, ino);
  if (!kafs_ino_get_usage(inoent))
    return -EBADF;

  *pinoent = inoent;
  return KAFS_SUCCESS;
}

// cppcheck-suppress constParameterCallback
static int kafs_access(struct fuse_context *fctx, kafs_context_t *ctx, const char *path,
                       struct fuse_file_info *fi, int ok, kafs_sinode_t **pinoent)
{
  assert(fctx != NULL);
  assert(ctx != NULL);
  assert(path == NULL || *path == '/' || *path == '\0');

  kafs_dlog(2, "%s(path=%s, ok=%d, fi=%p)\n", __func__, path ? path : "(null)", ok, (void *)fi);
  __atomic_add_fetch(&ctx->c_stat_access_calls, 1u, __ATOMIC_RELAXED);

  uid_t uid = fctx->uid;
  gid_t gid = fctx->gid;
  ssize_t ng0 = fuse_getgroups(0, NULL);
  size_t ngroups = (ng0 > 0) ? (size_t)ng0 : 0;
  gid_t groups[(ngroups > 0) ? ngroups : 1];
  if (ngroups > 0)
    (void)fuse_getgroups(ngroups, groups);

  kafs_sinode_t *inoent = NULL;
  const char *p = NULL;
  int fh_rc = -EBADF;
  if (fi != NULL)
    fh_rc = kafs_resolve_fh_inode(ctx, fi, &inoent);

  if (path == NULL || path[0] == '\0')
  {
    assert(fi != NULL);
    if (fh_rc < 0)
      return fh_rc;
    p = "";
  }
  else if (fh_rc == 0)
  {
    __atomic_add_fetch(&ctx->c_stat_access_fh_fastpath_hits, 1u, __ATOMIC_RELAXED);
    p = "";
  }
  else
  {
    __atomic_add_fetch(&ctx->c_stat_access_path_walk_calls, 1u, __ATOMIC_RELAXED);
    inoent = kafs_ctx_inode(ctx, KAFS_INO_ROOTDIR);
    p = path + 1;
  }

  int ok_final = ok;
  while (*p != '\0')
  {
    const char *n = strchrnul(p, '/');
    __atomic_add_fetch(&ctx->c_stat_access_path_components, 1u, __ATOMIC_RELAXED);
    kafs_mode_t cur_mode = kafs_ino_mode_get(inoent);
    kafs_dlog(2, "%s: component='%.*s' checking dir ino=%u mode=%o\n", __func__, (int)(n - p), p,
              (unsigned)kafs_ctx_ino_no(ctx, inoent), (unsigned)cur_mode);
    int ok_dirs = X_OK;
    int rc_ac = kafs_access_check(ok_dirs, inoent, KAFS_TRUE, uid, gid, ngroups, groups);
    if (rc_ac < 0)
      return rc_ac;

    uint32_t ino_dir = (uint32_t)kafs_ctx_ino_no(ctx, inoent);
    char *snap = NULL;
    size_t snap_len = 0;
    kafs_inode_lock(ctx, ino_dir);
    int rc = kafs_dir_snapshot(ctx, inoent, &snap, &snap_len);
    kafs_inode_unlock(ctx, ino_dir);
    if (rc < 0)
      return rc;

    rc = kafs_dirent_search_snapshot(ctx, snap, snap_len, p, n - p, ino_dir, &inoent);
    free(snap);
    if (rc < 0)
    {
      kafs_dlog(2, "%s: dirent_search('%.*s') rc=%d\n", __func__, (int)(n - p), p, rc);
      return rc;
    }
    if (*n == '\0')
      break;
    p = n + 1;
  }

  kafs_mode_t final_mode = kafs_ino_mode_get(inoent);
  kafs_dlog(2, "%s: final node ino=%u mode=%o ok=%d\n", __func__,
            (unsigned)kafs_ctx_ino_no(ctx, inoent), (unsigned)final_mode, ok_final);
  KAFS_CALL(kafs_access_check, ok_final, inoent, KAFS_FALSE, uid, gid, ngroups, groups);
  if (pinoent != NULL)
    *pinoent = inoent;
  return KAFS_SUCCESS;
}

static int kafs_hotplug_should_fallback(int rc)
{
  return rc == -ENOSYS || rc == -EOPNOTSUPP || rc == -EIO;
}

#define KAFS_HOTPLUG_WAIT_TIMEOUT_MS_DEFAULT 2000u
#define KAFS_HOTPLUG_WAIT_QUEUE_LIMIT_DEFAULT 64u
#define KAFS_HOTPLUG_UDS_DEFAULT "/tmp/kafs-hotplug.sock"

static int kafs_hotplug_enabled(const kafs_context_t *ctx)
{
  return ctx && ctx->c_hotplug_active && ctx->c_hotplug_fd >= 0;
}

static void kafs_timespec_add_ms(struct timespec *ts, uint32_t ms)
{
  ts->tv_sec += (time_t)(ms / 1000u);
  ts->tv_nsec += (long)(ms % 1000u) * 1000000L;
  if (ts->tv_nsec >= 1000000000L)
  {
    ts->tv_sec += 1;
    ts->tv_nsec -= 1000000000L;
  }
}

static void kafs_hotplug_wait_notify(kafs_context_t *ctx)
{
  if (!ctx || !ctx->c_hotplug_wait_lock_init)
    return;
  pthread_mutex_lock(&ctx->c_hotplug_wait_lock);
  pthread_cond_broadcast(&ctx->c_hotplug_wait_cond);
  pthread_mutex_unlock(&ctx->c_hotplug_wait_lock);
}

static int kafs_hotplug_is_disconnect_error(int rc)
{
  return rc == -EPIPE || rc == -ECONNRESET || rc == -ENOTCONN || rc == -ECONNABORTED;
}

static int kafs_hotplug_wait_for_back(kafs_context_t *ctx, const char *uds_path, int timeout_ms);
static void kafs_hotplug_env_lock(kafs_context_t *ctx);
static void kafs_hotplug_env_unlock(kafs_context_t *ctx);

static void kafs_hotplug_set_fd_timeout_ms(int fd, uint32_t timeout_ms)
{
  if (fd < 0)
    return;
  if (timeout_ms == 0)
    timeout_ms = KAFS_HOTPLUG_WAIT_TIMEOUT_MS_DEFAULT;
  struct timeval tv;
  tv.tv_sec = (time_t)(timeout_ms / 1000u);
  tv.tv_usec = (suseconds_t)((timeout_ms % 1000u) * 1000u);
  (void)setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
  (void)setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));
}

static int kafs_hotplug_complete_handshake(kafs_context_t *ctx, int cli)
{
  kafs_rpc_hdr_t hdr;
  kafs_rpc_hello_t hello;
  uint32_t payload_len = 0;
  int rc = kafs_rpc_recv_msg(cli, &hdr, &hello, sizeof(hello), &payload_len);
  if (rc != 0 || hdr.op != KAFS_RPC_OP_HELLO)
  {
    ctx->c_hotplug_state = KAFS_HOTPLUG_STATE_ERROR;
    ctx->c_hotplug_last_error = rc != 0 ? rc : -EBADMSG;
    ctx->c_hotplug_compat_result = KAFS_HOTPLUG_COMPAT_REJECT;
    ctx->c_hotplug_compat_reason = rc != 0 ? rc : -EBADMSG;
    return rc != 0 ? rc : -EBADMSG;
  }
  if (payload_len != sizeof(hello))
  {
    ctx->c_hotplug_state = KAFS_HOTPLUG_STATE_ERROR;
    ctx->c_hotplug_last_error = -EBADMSG;
    ctx->c_hotplug_compat_result = KAFS_HOTPLUG_COMPAT_REJECT;
    ctx->c_hotplug_compat_reason = -EBADMSG;
    return -EBADMSG;
  }
  ctx->c_hotplug_back_major = hello.major;
  ctx->c_hotplug_back_minor = hello.minor;
  ctx->c_hotplug_back_features = hello.feature_flags;
  if (hello.major != KAFS_RPC_HELLO_MAJOR || hello.minor != KAFS_RPC_HELLO_MINOR ||
      (hello.feature_flags & ~KAFS_RPC_HELLO_FEATURES) != 0)
  {
    ctx->c_hotplug_state = KAFS_HOTPLUG_STATE_ERROR;
    ctx->c_hotplug_last_error = -EPROTONOSUPPORT;
    ctx->c_hotplug_compat_result = KAFS_HOTPLUG_COMPAT_REJECT;
    ctx->c_hotplug_compat_reason = -EPROTONOSUPPORT;
    return -EPROTONOSUPPORT;
  }
  ctx->c_hotplug_compat_result = KAFS_HOTPLUG_COMPAT_OK;
  ctx->c_hotplug_compat_reason = 0;

  uint64_t session_id = ctx->c_hotplug_session_id;
  uint32_t next_epoch = ctx->c_hotplug_epoch;
  if (session_id == 0)
  {
    session_id = kafs_rpc_next_req_id();
    next_epoch = 0u;
  }
  else
  {
    next_epoch = ctx->c_hotplug_epoch + 1u;
  }

  kafs_rpc_session_restore_t restore;
  restore.open_handle_count = 0u;
  uint64_t req_id = kafs_rpc_next_req_id();
  rc = kafs_rpc_send_msg(cli, KAFS_RPC_OP_SESSION_RESTORE, KAFS_RPC_FLAG_ENDIAN_HOST, req_id,
                         session_id, next_epoch, &restore, sizeof(restore));
  if (rc != 0)
  {
    ctx->c_hotplug_state = KAFS_HOTPLUG_STATE_ERROR;
    ctx->c_hotplug_last_error = rc;
    return rc;
  }

  kafs_rpc_hdr_t ready_hdr;
  uint32_t ready_len = 0;
  rc = kafs_rpc_recv_msg(cli, &ready_hdr, NULL, 0, &ready_len);
  if (rc != 0 || ready_hdr.op != KAFS_RPC_OP_READY)
  {
    ctx->c_hotplug_state = KAFS_HOTPLUG_STATE_ERROR;
    ctx->c_hotplug_last_error = rc != 0 ? rc : -EBADMSG;
    return rc != 0 ? rc : -EBADMSG;
  }
  if (ready_len != 0)
  {
    ctx->c_hotplug_state = KAFS_HOTPLUG_STATE_ERROR;
    ctx->c_hotplug_last_error = -EBADMSG;
    return -EBADMSG;
  }

  ctx->c_hotplug_fd = cli;
  ctx->c_hotplug_active = 1;
  ctx->c_hotplug_state = KAFS_HOTPLUG_STATE_CONNECTED;
  ctx->c_hotplug_last_error = 0;
  ctx->c_hotplug_session_id = session_id;
  ctx->c_hotplug_epoch = next_epoch;
  if (!ctx->c_hotplug_lock_init)
  {
    if (pthread_mutex_init(&ctx->c_hotplug_lock, NULL) == 0)
      ctx->c_hotplug_lock_init = 1;
  }
  kafs_hotplug_wait_notify(ctx);
  return 0;
}

static int kafs_hotplug_spawn_back_for_restart(kafs_context_t *ctx, int *out_front_fd)
{
  if (!ctx || !out_front_fd)
    return -EINVAL;

  kafs_hotplug_env_entry_t envs[KAFS_HOTPLUG_ENV_MAX];
  uint32_t env_count = 0;
  char back_bin_buf[PATH_MAX];
  snprintf(back_bin_buf, sizeof(back_bin_buf), "%s", "kafs-back");

  kafs_hotplug_env_lock(ctx);
  env_count = ctx->c_hotplug_env_count;
  if (env_count > KAFS_HOTPLUG_ENV_MAX)
    env_count = KAFS_HOTPLUG_ENV_MAX;
  for (uint32_t i = 0; i < env_count; ++i)
  {
    envs[i] = ctx->c_hotplug_env[i];
    if (strcmp(envs[i].key, "KAFS_HOTPLUG_BACK_BIN") == 0 && envs[i].value[0] != '\0')
      snprintf(back_bin_buf, sizeof(back_bin_buf), "%s", envs[i].value);
  }
  kafs_hotplug_env_unlock(ctx);

  int fds[2];
  if (socketpair(AF_UNIX, SOCK_STREAM, 0, fds) != 0)
    return -errno;

  pid_t pid = fork();
  if (pid < 0)
  {
    int rc = -errno;
    close(fds[0]);
    close(fds[1]);
    return rc;
  }
  if (pid == 0)
  {
    close(fds[0]);
    char fd_buf[32];
    snprintf(fd_buf, sizeof(fd_buf), "%d", fds[1]);
    (void)setenv("KAFS_HOTPLUG_BACK_FD", fd_buf, 1);
    for (uint32_t i = 0; i < env_count; ++i)
    {
      if (strcmp(envs[i].key, "KAFS_HOTPLUG_BACK_FD") == 0 ||
          strcmp(envs[i].key, "KAFS_HOTPLUG_UDS") == 0)
        continue;
      (void)setenv(envs[i].key, envs[i].value, 1);
    }
    // Explicit restart uses front-managed transport variables only.
    (void)unsetenv("KAFS_HOTPLUG_UDS");

    char *args[] = {back_bin_buf, NULL};
    if (strchr(back_bin_buf, '/') != NULL)
      execv(back_bin_buf, args);
    else
      execvp(back_bin_buf, args);
    _exit(127);
  }

  close(fds[1]);
  *out_front_fd = fds[0];
  return 0;
}

static void *kafs_hotplug_relisten_thread(void *arg)
{
  kafs_context_t *ctx = (kafs_context_t *)arg;
  if (!ctx)
    return NULL;
  while (ctx->c_hotplug_uds_path[0] != '\0')
  {
    int rc = kafs_hotplug_wait_for_back(ctx, ctx->c_hotplug_uds_path,
                                        (int)ctx->c_hotplug_wait_timeout_ms);
    if (rc == 0)
      break;
    // Backoff to avoid tight loop on repeated failures/timeouts.
    struct timespec ts;
    ts.tv_sec = 0;
    ts.tv_nsec = 200 * 1000 * 1000;
    (void)nanosleep(&ts, NULL);
  }
  if (ctx->c_hotplug_wait_lock_init)
  {
    pthread_mutex_lock(&ctx->c_hotplug_wait_lock);
    ctx->c_hotplug_connecting = 0;
    pthread_cond_broadcast(&ctx->c_hotplug_wait_cond);
    pthread_mutex_unlock(&ctx->c_hotplug_wait_lock);
  }
  return NULL;
}

static void kafs_hotplug_schedule_relisten(kafs_context_t *ctx)
{
  if (!ctx || ctx->c_hotplug_uds_path[0] == '\0')
    return;
  if (!ctx->c_hotplug_wait_lock_init)
  {
    if (pthread_mutex_init(&ctx->c_hotplug_wait_lock, NULL) == 0 &&
        pthread_cond_init(&ctx->c_hotplug_wait_cond, NULL) == 0)
    {
      ctx->c_hotplug_wait_lock_init = 1;
    }
  }
  if (!ctx->c_hotplug_wait_lock_init)
    return;

  pthread_mutex_lock(&ctx->c_hotplug_wait_lock);
  if (ctx->c_hotplug_connecting)
  {
    pthread_mutex_unlock(&ctx->c_hotplug_wait_lock);
    return;
  }
  ctx->c_hotplug_connecting = 1;
  pthread_mutex_unlock(&ctx->c_hotplug_wait_lock);

  pthread_t tid;
  if (pthread_create(&tid, NULL, kafs_hotplug_relisten_thread, ctx) == 0)
  {
    pthread_detach(tid);
    return;
  }

  pthread_mutex_lock(&ctx->c_hotplug_wait_lock);
  ctx->c_hotplug_connecting = 0;
  pthread_cond_broadcast(&ctx->c_hotplug_wait_cond);
  pthread_mutex_unlock(&ctx->c_hotplug_wait_lock);
}

static void kafs_hotplug_mark_disconnected_internal(kafs_context_t *ctx, int rc,
                                                    int schedule_relisten)
{
  if (!ctx)
    return;
  if (ctx->c_hotplug_fd >= 0)
    close(ctx->c_hotplug_fd);
  ctx->c_hotplug_fd = -1;
  ctx->c_hotplug_active = 0;
  ctx->c_hotplug_state = KAFS_HOTPLUG_STATE_WAITING;
  ctx->c_hotplug_last_error = rc;
  kafs_hotplug_wait_notify(ctx);
  if (schedule_relisten)
    kafs_hotplug_schedule_relisten(ctx);
}

static void kafs_hotplug_mark_disconnected(kafs_context_t *ctx, int rc)
{
  kafs_hotplug_mark_disconnected_internal(ctx, rc, 1);
}

static int kafs_hotplug_wait_for_back(kafs_context_t *ctx, const char *uds_path, int timeout_ms)
{
  ctx->c_hotplug_state = KAFS_HOTPLUG_STATE_WAITING;
  ctx->c_hotplug_front_major = KAFS_RPC_HELLO_MAJOR;
  ctx->c_hotplug_front_minor = KAFS_RPC_HELLO_MINOR;
  ctx->c_hotplug_front_features = KAFS_RPC_HELLO_FEATURES;
  ctx->c_hotplug_back_major = 0;
  ctx->c_hotplug_back_minor = 0;
  ctx->c_hotplug_back_features = 0;
  ctx->c_hotplug_compat_result = KAFS_HOTPLUG_COMPAT_UNKNOWN;
  ctx->c_hotplug_compat_reason = 0;
  int srv = socket(AF_UNIX, SOCK_STREAM, 0);
  if (srv < 0)
  {
    ctx->c_hotplug_state = KAFS_HOTPLUG_STATE_ERROR;
    ctx->c_hotplug_last_error = -errno;
    return -errno;
  }

  struct sockaddr_un addr;
  memset(&addr, 0, sizeof(addr));
  addr.sun_family = AF_UNIX;
  if (snprintf(addr.sun_path, sizeof(addr.sun_path), "%s", uds_path) >= (int)sizeof(addr.sun_path))
  {
    close(srv);
    ctx->c_hotplug_state = KAFS_HOTPLUG_STATE_ERROR;
    ctx->c_hotplug_last_error = -ENAMETOOLONG;
    return -ENAMETOOLONG;
  }
  unlink(uds_path);

  if (bind(srv, (struct sockaddr *)&addr, sizeof(addr)) < 0)
  {
    int rc = -errno;
    close(srv);
    ctx->c_hotplug_state = KAFS_HOTPLUG_STATE_ERROR;
    ctx->c_hotplug_last_error = rc;
    return rc;
  }
  if (listen(srv, 1) < 0)
  {
    int rc = -errno;
    close(srv);
    ctx->c_hotplug_state = KAFS_HOTPLUG_STATE_ERROR;
    ctx->c_hotplug_last_error = rc;
    return rc;
  }

  if (timeout_ms >= 0)
  {
    struct pollfd pfd;
    pfd.fd = srv;
    pfd.events = POLLIN;
    int prc;
    do
    {
      prc = poll(&pfd, 1, timeout_ms);
    } while (prc < 0 && errno == EINTR);
    if (prc == 0)
    {
      close(srv);
      ctx->c_hotplug_last_error = -ETIMEDOUT;
      return -EIO;
    }
    if (prc < 0)
    {
      int rc = -errno;
      close(srv);
      ctx->c_hotplug_state = KAFS_HOTPLUG_STATE_ERROR;
      ctx->c_hotplug_last_error = rc;
      return rc;
    }
  }

  int cli = accept(srv, NULL, NULL);
  if (cli < 0)
  {
    int rc = -errno;
    close(srv);
    ctx->c_hotplug_state = KAFS_HOTPLUG_STATE_ERROR;
    ctx->c_hotplug_last_error = rc;
    return rc;
  }
  close(srv);

  int rc = kafs_hotplug_complete_handshake(ctx, cli);
  if (rc != 0)
  {
    close(cli);
    return rc;
  }
  return 0;
}

static int kafs_hotplug_wait_ready(kafs_context_t *ctx)
{
  if (!ctx || ctx->c_hotplug_state == KAFS_HOTPLUG_STATE_DISABLED)
    return -ENOSYS;
  if (kafs_hotplug_enabled(ctx))
    return 0;
  if (ctx->c_hotplug_wait_timeout_ms == 0 || ctx->c_hotplug_uds_path[0] == '\0')
    return -EIO;

  if (!ctx->c_hotplug_wait_lock_init)
  {
    if (pthread_mutex_init(&ctx->c_hotplug_wait_lock, NULL) == 0 &&
        pthread_cond_init(&ctx->c_hotplug_wait_cond, NULL) == 0)
    {
      ctx->c_hotplug_wait_lock_init = 1;
    }
  }
  if (!ctx->c_hotplug_wait_lock_init)
    return -EIO;

  pthread_mutex_lock(&ctx->c_hotplug_wait_lock);
  if (ctx->c_hotplug_wait_queue_len >= ctx->c_hotplug_wait_queue_limit)
  {
    ctx->c_hotplug_last_error = -EOVERFLOW;
    pthread_mutex_unlock(&ctx->c_hotplug_wait_lock);
    return -EIO;
  }
  ctx->c_hotplug_wait_queue_len++;
  int do_connect = 0;
  if (!ctx->c_hotplug_connecting)
  {
    ctx->c_hotplug_connecting = 1;
    do_connect = 1;
  }
  struct timespec deadline;
  clock_gettime(CLOCK_REALTIME, &deadline);
  kafs_timespec_add_ms(&deadline, ctx->c_hotplug_wait_timeout_ms);
  pthread_mutex_unlock(&ctx->c_hotplug_wait_lock);

  if (do_connect)
  {
    (void)kafs_hotplug_wait_for_back(ctx, ctx->c_hotplug_uds_path,
                                     (int)ctx->c_hotplug_wait_timeout_ms);
    pthread_mutex_lock(&ctx->c_hotplug_wait_lock);
    ctx->c_hotplug_connecting = 0;
    pthread_cond_broadcast(&ctx->c_hotplug_wait_cond);
    pthread_mutex_unlock(&ctx->c_hotplug_wait_lock);
  }

  int rc = 0;
  pthread_mutex_lock(&ctx->c_hotplug_wait_lock);
  while (!kafs_hotplug_enabled(ctx))
  {
    int tw =
        pthread_cond_timedwait(&ctx->c_hotplug_wait_cond, &ctx->c_hotplug_wait_lock, &deadline);
    if (tw == ETIMEDOUT)
    {
      ctx->c_hotplug_last_error = -ETIMEDOUT;
      rc = -EIO;
      break;
    }
  }
  if (ctx->c_hotplug_wait_queue_len > 0)
    ctx->c_hotplug_wait_queue_len--;
  pthread_mutex_unlock(&ctx->c_hotplug_wait_lock);

  if (rc == 0 && !kafs_hotplug_enabled(ctx))
    rc = -EIO;
  return rc;
}

static int kafs_hotplug_restart_back(kafs_context_t *ctx)
{
  if (!ctx)
    return -EINVAL;
  if (ctx->c_hotplug_state == KAFS_HOTPLUG_STATE_DISABLED)
    return -ENOSYS;

  // For explicit restart, stop current channel without scheduling background relisten;
  // we proactively spawn and re-handshake with kafs-back below.
  kafs_hotplug_mark_disconnected_internal(ctx, -ECONNRESET, 0);

  int front_fd = -1;
  int rc = kafs_hotplug_spawn_back_for_restart(ctx, &front_fd);
  if (rc != 0)
  {
    ctx->c_hotplug_last_error = rc;
    kafs_hotplug_schedule_relisten(ctx);
    return rc;
  }

  kafs_hotplug_set_fd_timeout_ms(front_fd, ctx->c_hotplug_wait_timeout_ms);
  rc = kafs_hotplug_complete_handshake(ctx, front_fd);
  if (rc != 0)
  {
    close(front_fd);
    kafs_hotplug_schedule_relisten(ctx);
    return rc;
  }
  return 0;
}

static int kafs_hotplug_call_getattr(struct fuse_context *fctx, kafs_context_t *ctx,
                                     kafs_sinode_t *inoent, struct stat *st)
{
  int wait_rc = kafs_hotplug_wait_ready(ctx);
  if (wait_rc != 0)
    return wait_rc;
  kafs_rpc_getattr_req_t req;
  req.ino = kafs_ctx_ino_no(ctx, inoent);
  req.uid = (uint32_t)fctx->uid;
  req.gid = (uint32_t)fctx->gid;
  req.pid = (uint32_t)fctx->pid;
  uint64_t req_id = kafs_rpc_next_req_id();

  if (ctx->c_hotplug_lock_init)
    pthread_mutex_lock(&ctx->c_hotplug_lock);
  int rc =
      kafs_rpc_send_msg(ctx->c_hotplug_fd, KAFS_RPC_OP_GETATTR, KAFS_RPC_FLAG_ENDIAN_HOST, req_id,
                        ctx->c_hotplug_session_id, ctx->c_hotplug_epoch, &req, sizeof(req));
  if (rc == 0)
  {
    kafs_rpc_resp_hdr_t resp_hdr;
    kafs_rpc_getattr_resp_t resp;
    uint32_t resp_len = 0;
    rc = kafs_rpc_recv_resp(ctx->c_hotplug_fd, &resp_hdr, &resp, sizeof(resp), &resp_len);
    if (rc == 0 && resp_hdr.req_id != req_id)
      rc = -EBADMSG;
    if (rc == 0 && resp_hdr.result != 0)
      rc = resp_hdr.result;
    if (rc == 0 && resp_len != sizeof(resp))
      rc = -EBADMSG;
    if (rc == 0)
      *st = resp.st;
  }
  if (ctx->c_hotplug_lock_init)
    pthread_mutex_unlock(&ctx->c_hotplug_lock);
  if (kafs_hotplug_is_disconnect_error(rc))
  {
    kafs_hotplug_mark_disconnected(ctx, rc);
    rc = -EIO;
  }
  return rc;
}

static void kafs_ctx_close_fd(kafs_context_t *ctx)
{
  if (ctx->c_fd >= 0)
    close(ctx->c_fd);
  ctx->c_fd = -1;
}

static void kafs_ctx_reset_mapping(kafs_context_t *ctx)
{
  ctx->c_img_base = NULL;
  ctx->c_img_size = 0;
  ctx->c_superblock = NULL;
  ctx->c_inotbl = NULL;
  ctx->c_blkmasktbl = NULL;
  ctx->c_mapsize = 0;
}

static void kafs_ctx_unmap_image(kafs_context_t *ctx)
{
  if (ctx->c_img_base && ctx->c_img_base != MAP_FAILED)
    munmap(ctx->c_img_base, ctx->c_img_size);
  kafs_ctx_reset_mapping(ctx);
}

static int kafs_ctx_read_superblock_fd(kafs_context_t *ctx, kafs_ssuperblock_t *sbdisk)
{
  ssize_t r = pread(ctx->c_fd, sbdisk, sizeof(*sbdisk), 0);
  if (r == (ssize_t)sizeof(*sbdisk))
    return 0;

  int err = -errno;
  kafs_ctx_close_fd(ctx);
  return err ? err : -EIO;
}

static void kafs_ctx_compute_map_layout(const kafs_ssuperblock_t *sbdisk, off_t *mapsize_out,
                                        off_t *imgsize_out, intptr_t *blkmask_off_out,
                                        intptr_t *inotbl_off_out)
{
  kafs_logblksize_t log_blksize = kafs_sb_log_blksize_get(sbdisk);
  kafs_blksize_t blksize = 1u << log_blksize;
  kafs_blksize_t blksizemask = blksize - 1u;
  kafs_inocnt_t inocnt = kafs_inocnt_stoh(sbdisk->s_inocnt);
  kafs_blkcnt_t r_blkcnt = kafs_blkcnt_stoh(sbdisk->s_r_blkcnt);

  off_t mapsize = sizeof(kafs_ssuperblock_t);
  mapsize = (mapsize + blksizemask) & ~blksizemask;
  intptr_t blkmask_off = (intptr_t)mapsize;
  mapsize += (r_blkcnt + 7) >> 3;
  mapsize = (mapsize + 7) & ~7;
  mapsize = (mapsize + blksizemask) & ~blksizemask;
  intptr_t inotbl_off = (intptr_t)mapsize;
  mapsize += (off_t)kafs_inode_table_bytes_for_format(kafs_sb_format_version_get(sbdisk), inocnt);
  mapsize = (mapsize + blksizemask) & ~blksizemask;

  off_t imgsize = (off_t)r_blkcnt << log_blksize;
  uint64_t idx_off = kafs_sb_hrl_index_offset_get(sbdisk);
  uint64_t idx_size = kafs_sb_hrl_index_size_get(sbdisk);
  uint64_t ent_off = kafs_sb_hrl_entry_offset_get(sbdisk);
  uint64_t ent_cnt = kafs_sb_hrl_entry_cnt_get(sbdisk);
  uint64_t ent_size = ent_cnt * (uint64_t)sizeof(kafs_hrl_entry_t);
  uint64_t j_off = kafs_sb_journal_offset_get(sbdisk);
  uint64_t j_size = kafs_sb_journal_size_get(sbdisk);
  uint64_t p_off = kafs_sb_pendinglog_offset_get(sbdisk);
  uint64_t p_size = kafs_sb_pendinglog_size_get(sbdisk);
  uint64_t end1 = (idx_off && idx_size) ? (idx_off + idx_size) : 0;
  uint64_t end2 = (ent_off && ent_size) ? (ent_off + ent_size) : 0;
  uint64_t end3 = (j_off && j_size) ? (j_off + j_size) : 0;
  uint64_t end4 = (p_off && p_size) ? (p_off + p_size) : 0;
  uint64_t max_end = end1;
  if (end2 > max_end)
    max_end = end2;
  if (end3 > max_end)
    max_end = end3;
  if (end4 > max_end)
    max_end = end4;
  if ((off_t)max_end > imgsize)
    imgsize = (off_t)max_end;
  imgsize = (imgsize + blksizemask) & ~blksizemask;

  *mapsize_out = mapsize;
  *imgsize_out = imgsize;
  *blkmask_off_out = blkmask_off;
  *inotbl_off_out = inotbl_off;
}

static int kafs_ctx_map_image(kafs_context_t *ctx, const kafs_ssuperblock_t *sbdisk)
{
  off_t mapsize = 0;
  off_t imgsize = 0;
  intptr_t blkmask_off = 0;
  intptr_t inotbl_off = 0;
  kafs_ctx_compute_map_layout(sbdisk, &mapsize, &imgsize, &blkmask_off, &inotbl_off);

  ctx->c_img_base = mmap(NULL, imgsize, PROT_READ | PROT_WRITE, MAP_SHARED, ctx->c_fd, 0);
  if (ctx->c_img_base == MAP_FAILED)
  {
    int err = -errno;
    kafs_ctx_reset_mapping(ctx);
    kafs_ctx_close_fd(ctx);
    return err;
  }

  ctx->c_img_size = (size_t)imgsize;
  ctx->c_superblock = (kafs_ssuperblock_t *)ctx->c_img_base;
  ctx->c_mapsize = (size_t)mapsize;
  ctx->c_blkmasktbl = (void *)ctx->c_superblock + blkmask_off;
  ctx->c_inotbl = (void *)ctx->c_superblock + inotbl_off;
  return 0;
}

static int kafs_ctx_runtime_mount_supported(const kafs_context_t *ctx)
{
  uint32_t fmt_ver;

  if (!ctx || !ctx->c_superblock)
    return 0;

  fmt_ver = kafs_ctx_inode_format(ctx);
  return (fmt_ver == KAFS_FORMAT_VERSION || fmt_ver == KAFS_FORMAT_VERSION_V5);
}

static int kafs_ctx_validate_runtime_mount_state(kafs_context_t *ctx)
{
  struct kafs_tailmeta_region_view view;
  uint32_t fmt_ver;

  if (!ctx || !ctx->c_superblock)
    return -EINVAL;

  fmt_ver = kafs_ctx_inode_format(ctx);
  if (fmt_ver != KAFS_FORMAT_VERSION_V5)
    return 0;
  if (!kafs_tailmeta_region_present(ctx->c_superblock))
    return -EPROTO;

  return kafs_tailmeta_region_view_get(ctx, &view);
}

int kafs_core_open_image(const char *image_path, kafs_context_t *ctx)
{
  if (!image_path || !ctx)
    return -EINVAL;

  memset(ctx, 0, sizeof(*ctx));
  ctx->c_fd = open(image_path, O_RDWR, 0666);
  if (ctx->c_fd < 0)
    return -errno;

  kafs_ssuperblock_t sbdisk;
  int rc = kafs_ctx_read_superblock_fd(ctx, &sbdisk);
  if (rc != 0)
    return rc;
  if (kafs_sb_magic_get(&sbdisk) != KAFS_MAGIC)
  {
    kafs_ctx_close_fd(ctx);
    return -EINVAL;
  }
  uint32_t fmt_ver = kafs_sb_format_version_get(&sbdisk);
  if (fmt_ver != KAFS_FORMAT_VERSION && fmt_ver != KAFS_FORMAT_VERSION_V5)
  {
    kafs_ctx_close_fd(ctx);
    return -EPROTONOSUPPORT;
  }

  kafs_blkcnt_t r_blkcnt = kafs_blkcnt_stoh(sbdisk.s_r_blkcnt);
  rc = kafs_ctx_map_image(ctx, &sbdisk);
  if (rc != 0)
    return rc;
  if (!kafs_ctx_runtime_mount_supported(ctx))
  {
    kafs_ctx_unmap_image(ctx);
    kafs_ctx_close_fd(ctx);
    return -EPROTONOSUPPORT;
  }
  if (kafs_ctx_validate_runtime_mount_state(ctx) != 0)
  {
    kafs_ctx_unmap_image(ctx);
    kafs_ctx_close_fd(ctx);
    return -EPROTO;
  }
  ctx->c_alloc_v3_summary_dirty = 1;

  (void)kafs_hrl_open(ctx);
  (void)kafs_journal_init(ctx, image_path);
  ctx->c_meta_delta_enabled = (uint32_t)kafs_journal_is_enabled(ctx);
  if (ctx->c_meta_delta_enabled)
  {
    size_t bits = sizeof(kafs_blkmask_t) * 8u;
    size_t words = ((size_t)r_blkcnt + bits - 1u) / bits;
    ctx->c_meta_bitmap_words = calloc(words, sizeof(kafs_blkmask_t));
    ctx->c_meta_bitmap_dirty = calloc(words, sizeof(uint8_t));
    if (!ctx->c_meta_bitmap_words || !ctx->c_meta_bitmap_dirty)
    {
      free(ctx->c_meta_bitmap_words);
      free(ctx->c_meta_bitmap_dirty);
      ctx->c_meta_bitmap_words = NULL;
      ctx->c_meta_bitmap_dirty = NULL;
      ctx->c_meta_bitmap_wordcnt = 0;
      ctx->c_meta_bitmap_dirty_count = 0;
      ctx->c_meta_bitmap_words_enabled = 0;
      ctx->c_meta_delta_enabled = 0;
    }
    else
    {
      memcpy(ctx->c_meta_bitmap_words, ctx->c_blkmasktbl, words * sizeof(kafs_blkmask_t));
      ctx->c_meta_bitmap_wordcnt = words;
      ctx->c_meta_bitmap_dirty_count = 0;
      ctx->c_meta_bitmap_words_enabled = 1u;
    }
  }
  (void)kafs_journal_replay(ctx, NULL, NULL);
  (void)kafs_pendinglog_init_or_load(ctx);
  if (ctx->c_pendinglog_enabled)
  {
    (void)kafs_pendinglog_replay_mount(ctx);
    (void)kafs_pending_worker_start(ctx);
    kafs_journal_note(ctx, "PENDINGLOG", "loaded entries=%u cap=%u", kafs_pendinglog_count(ctx),
                      ctx->c_pendinglog_capacity);
  }
  return 0;
}

void kafs_core_close_image(kafs_context_t *ctx)
{
  if (!ctx)
    return;
  kafs_pending_worker_stop(ctx);
  (void)kafs_journal_shutdown(ctx);
  (void)kafs_hrl_close(ctx);
  free(ctx->c_meta_bitmap_words);
  free(ctx->c_meta_bitmap_dirty);
  ctx->c_meta_bitmap_words = NULL;
  ctx->c_meta_bitmap_dirty = NULL;
  ctx->c_meta_bitmap_wordcnt = 0;
  ctx->c_meta_bitmap_dirty_count = 0;
  ctx->c_meta_bitmap_words_enabled = 0;
  if (ctx->c_img_base && ctx->c_img_base != MAP_FAILED)
    munmap(ctx->c_img_base, ctx->c_img_size);
  if (ctx->c_fd >= 0)
    close(ctx->c_fd);
  ctx->c_img_base = NULL;
  ctx->c_fd = -1;
}

int kafs_core_getattr(kafs_context_t *ctx, kafs_inocnt_t ino, struct stat *st)
{
  if (!ctx || !st)
    return -EINVAL;
  if (ino >= kafs_sb_inocnt_get(ctx->c_superblock))
    return -ENOENT;
  kafs_sinode_t *inoent = kafs_ctx_inode(ctx, ino);
  st->st_dev = 0;
  st->st_ino = ino;
  st->st_mode = kafs_ino_mode_get(inoent);
  st->st_nlink = kafs_ino_linkcnt_get(inoent);
  st->st_uid = kafs_ino_uid_get(inoent);
  st->st_gid = kafs_ino_gid_get(inoent);
  st->st_rdev = kafs_ino_dev_get(inoent);
  st->st_size = kafs_ino_size_get(inoent);
  st->st_blksize = kafs_sb_blksize_get(ctx->c_superblock);
  st->st_blocks =
      (blkcnt_t)((uint64_t)kafs_ino_blocks_get(inoent) * ((uint64_t)st->st_blksize / 512ull));
  st->st_atim = kafs_ino_atime_get(inoent);
  st->st_mtim = kafs_ino_mtime_get(inoent);
  st->st_ctim = kafs_ino_ctime_get(inoent);
  return 0;
}

ssize_t kafs_core_read(kafs_context_t *ctx, kafs_inocnt_t ino, void *buf, size_t size, off_t offset)
{
  if (!ctx || !buf)
    return -EINVAL;
  if (ino >= kafs_sb_inocnt_get(ctx->c_superblock))
    return -ENOENT;
  kafs_inode_lock(ctx, (uint32_t)ino);
  ssize_t rr = kafs_pread(ctx, kafs_ctx_inode(ctx, ino), buf, size, offset);
  kafs_inode_unlock(ctx, (uint32_t)ino);
  return rr;
}

ssize_t kafs_core_write(kafs_context_t *ctx, kafs_inocnt_t ino, const void *buf, size_t size,
                        off_t offset)
{
  if (!ctx || !buf)
    return -EINVAL;
  if (ino >= kafs_sb_inocnt_get(ctx->c_superblock))
    return -ENOENT;
  kafs_inode_lock(ctx, (uint32_t)ino);
  ssize_t ww = kafs_pwrite(ctx, kafs_ctx_inode(ctx, ino), buf, size, offset);
  kafs_inode_unlock(ctx, (uint32_t)ino);
  return ww;
}

static int kafs_hotplug_read_validate_request(kafs_context_t *ctx, size_t size)
{
  int wait_rc = kafs_hotplug_wait_ready(ctx);
  if (wait_rc != 0)
    return wait_rc;
  if (ctx->c_hotplug_data_mode == KAFS_RPC_DATA_SHM)
    return -EOPNOTSUPP;
  if (ctx->c_hotplug_data_mode == KAFS_RPC_DATA_INLINE &&
      size > (KAFS_RPC_MAX_PAYLOAD - sizeof(kafs_rpc_read_resp_t)))
    return -EOPNOTSUPP;
  return 0;
}

static void kafs_hotplug_read_prepare_request(struct fuse_context *fctx, kafs_context_t *ctx,
                                              kafs_inocnt_t ino, size_t size, off_t offset,
                                              kafs_rpc_read_req_t *req)
{
  req->ino = (uint32_t)ino;
  req->uid = (uint32_t)fctx->uid;
  req->gid = (uint32_t)fctx->gid;
  req->pid = (uint32_t)fctx->pid;
  req->off = (uint64_t)offset;
  req->size = (uint32_t)size;
  req->data_mode = ctx->c_hotplug_data_mode;
}

static int kafs_hotplug_read_handle_inline(kafs_context_t *ctx, char *buf, size_t size,
                                           uint8_t *resp_buf, uint32_t resp_len)
{
  kafs_rpc_read_resp_t *resp = (kafs_rpc_read_resp_t *)resp_buf;
  if (ctx->c_hotplug_data_mode != KAFS_RPC_DATA_INLINE)
  {
    if (resp_len != sizeof(*resp))
      return -EBADMSG;
    return 1;
  }

  uint32_t data_len = resp_len - (uint32_t)sizeof(*resp);
  if (resp->size > data_len || resp->size > size)
    return -EBADMSG;
  memcpy(buf, resp_buf + sizeof(*resp), resp->size);
  return (int)resp->size;
}

static int kafs_hotplug_read_finish(kafs_context_t *ctx, int rc, int need_local, kafs_inocnt_t ino,
                                    char *buf, size_t size, off_t offset)
{
  if (kafs_hotplug_is_disconnect_error(rc))
  {
    kafs_hotplug_mark_disconnected(ctx, rc);
    rc = -EIO;
  }
  if (rc == 0 && need_local)
  {
    ssize_t rlen = kafs_core_read(ctx, ino, buf, size, offset);
    rc = rlen < 0 ? (int)rlen : (int)rlen;
  }
  return rc;
}

int kafs_core_truncate(kafs_context_t *ctx, kafs_inocnt_t ino, off_t size)
{
  if (!ctx)
    return -EINVAL;
  if (ino >= kafs_sb_inocnt_get(ctx->c_superblock))
    return -ENOENT;
  kafs_inode_lock(ctx, (uint32_t)ino);
  int rc = kafs_truncate(ctx, kafs_ctx_inode(ctx, ino), (kafs_off_t)size);
  kafs_inode_unlock(ctx, (uint32_t)ino);
  return rc;
}

static ssize_t kafs_hotplug_call_read(struct fuse_context *fctx, kafs_context_t *ctx,
                                      kafs_inocnt_t ino, char *buf, size_t size, off_t offset)
{
  int rc = kafs_hotplug_read_validate_request(ctx, size);
  if (rc != 0)
    return rc;

  kafs_rpc_read_req_t req;
  kafs_hotplug_read_prepare_request(fctx, ctx, ino, size, offset, &req);
  uint64_t req_id = kafs_rpc_next_req_id();

  uint8_t resp_buf[KAFS_RPC_MAX_PAYLOAD];
  if (ctx->c_hotplug_lock_init)
    pthread_mutex_lock(&ctx->c_hotplug_lock);
  rc = kafs_rpc_send_msg(ctx->c_hotplug_fd, KAFS_RPC_OP_READ, KAFS_RPC_FLAG_ENDIAN_HOST, req_id,
                         ctx->c_hotplug_session_id, ctx->c_hotplug_epoch, &req, sizeof(req));
  int need_local = 0;
  if (rc == 0)
  {
    kafs_rpc_resp_hdr_t resp_hdr;
    uint32_t resp_len = 0;
    rc = kafs_rpc_recv_resp(ctx->c_hotplug_fd, &resp_hdr, resp_buf, sizeof(resp_buf), &resp_len);
    if (rc == 0 && resp_hdr.req_id != req_id)
      rc = -EBADMSG;
    if (rc == 0 && resp_hdr.result != 0)
      rc = resp_hdr.result;
    if (rc == 0 && resp_len < sizeof(kafs_rpc_read_resp_t))
      rc = -EBADMSG;
    if (rc == 0)
    {
      rc = kafs_hotplug_read_handle_inline(ctx, buf, size, resp_buf, resp_len);
      if (rc == 1)
      {
        rc = 0;
        need_local = 1;
      }
    }
  }
  if (ctx->c_hotplug_lock_init)
    pthread_mutex_unlock(&ctx->c_hotplug_lock);
  return kafs_hotplug_read_finish(ctx, rc, need_local, ino, buf, size, offset);
}

static int kafs_hotplug_write_validate_request(kafs_context_t *ctx, size_t size)
{
  int wait_rc = kafs_hotplug_wait_ready(ctx);
  if (wait_rc != 0)
    return wait_rc;
  if (ctx->c_hotplug_data_mode == KAFS_RPC_DATA_SHM)
    return -EOPNOTSUPP;
  if (ctx->c_hotplug_data_mode == KAFS_RPC_DATA_INLINE &&
      size > (KAFS_RPC_MAX_PAYLOAD - sizeof(kafs_rpc_write_req_t)))
    return -EOPNOTSUPP;
  return 0;
}

static uint32_t kafs_hotplug_write_prepare_payload(struct fuse_context *fctx, kafs_context_t *ctx,
                                                   kafs_inocnt_t ino, const char *buf, size_t size,
                                                   off_t offset, uint8_t *payload)
{
  kafs_rpc_write_req_t *req = (kafs_rpc_write_req_t *)payload;
  req->ino = (uint32_t)ino;
  req->uid = (uint32_t)fctx->uid;
  req->gid = (uint32_t)fctx->gid;
  req->pid = (uint32_t)fctx->pid;
  req->off = (uint64_t)offset;
  req->size = (uint32_t)size;
  req->data_mode = ctx->c_hotplug_data_mode;
  uint32_t payload_len = (uint32_t)sizeof(*req);
  if (ctx->c_hotplug_data_mode == KAFS_RPC_DATA_INLINE)
  {
    memcpy(payload + sizeof(*req), buf, size);
    payload_len = (uint32_t)(sizeof(*req) + size);
  }
  return payload_len;
}

static int kafs_hotplug_write_finish(kafs_context_t *ctx, int rc, int need_local, kafs_inocnt_t ino,
                                     const char *buf, size_t size, off_t offset)
{
  if (kafs_hotplug_is_disconnect_error(rc))
  {
    kafs_hotplug_mark_disconnected(ctx, rc);
    rc = -EIO;
  }
  if (rc == 0 && need_local)
  {
    ssize_t wlen = kafs_core_write(ctx, ino, buf, size, offset);
    rc = wlen < 0 ? (int)wlen : (int)wlen;
  }
  return rc;
}

static ssize_t kafs_hotplug_call_write(struct fuse_context *fctx, kafs_context_t *ctx,
                                       kafs_inocnt_t ino, const char *buf, size_t size,
                                       off_t offset)
{
  int rc = kafs_hotplug_write_validate_request(ctx, size);
  if (rc != 0)
    return rc;

  uint8_t payload[KAFS_RPC_MAX_PAYLOAD];
  uint32_t payload_len =
      kafs_hotplug_write_prepare_payload(fctx, ctx, ino, buf, size, offset, payload);
  uint64_t req_id = kafs_rpc_next_req_id();

  if (ctx->c_hotplug_lock_init)
    pthread_mutex_lock(&ctx->c_hotplug_lock);
  rc = kafs_rpc_send_msg(ctx->c_hotplug_fd, KAFS_RPC_OP_WRITE, KAFS_RPC_FLAG_ENDIAN_HOST, req_id,
                         ctx->c_hotplug_session_id, ctx->c_hotplug_epoch, payload, payload_len);
  int need_local = 0;
  if (rc == 0)
  {
    kafs_rpc_resp_hdr_t resp_hdr;
    kafs_rpc_write_resp_t resp;
    uint32_t resp_len = 0;
    rc = kafs_rpc_recv_resp(ctx->c_hotplug_fd, &resp_hdr, &resp, sizeof(resp), &resp_len);
    if (rc == 0 && resp_hdr.req_id != req_id)
      rc = -EBADMSG;
    if (rc == 0 && resp_hdr.result != 0)
      rc = resp_hdr.result;
    if (rc == 0 && resp_len != sizeof(resp))
      rc = -EBADMSG;
    if (rc == 0)
    {
      if (ctx->c_hotplug_data_mode == KAFS_RPC_DATA_INLINE)
        rc = (int)resp.size;
      else
      {
        need_local = 1;
      }
    }
  }
  if (ctx->c_hotplug_lock_init)
    pthread_mutex_unlock(&ctx->c_hotplug_lock);
  return kafs_hotplug_write_finish(ctx, rc, need_local, ino, buf, size, offset);
}

static int kafs_hotplug_call_truncate(struct fuse_context *fctx, kafs_context_t *ctx,
                                      kafs_sinode_t *inoent, off_t size)
{
  (void)fctx;
  int wait_rc = kafs_hotplug_wait_ready(ctx);
  if (wait_rc != 0)
    return wait_rc;

  kafs_rpc_truncate_req_t req;
  req.ino = (uint32_t)kafs_ctx_ino_no(ctx, inoent);
  req.reserved = 0;
  req.size = (uint64_t)size;
  uint64_t req_id = kafs_rpc_next_req_id();

  if (ctx->c_hotplug_lock_init)
    pthread_mutex_lock(&ctx->c_hotplug_lock);
  int rc =
      kafs_rpc_send_msg(ctx->c_hotplug_fd, KAFS_RPC_OP_TRUNCATE, KAFS_RPC_FLAG_ENDIAN_HOST, req_id,
                        ctx->c_hotplug_session_id, ctx->c_hotplug_epoch, &req, sizeof(req));
  if (rc == 0)
  {
    kafs_rpc_resp_hdr_t resp_hdr;
    kafs_rpc_truncate_resp_t resp;
    uint32_t resp_len = 0;
    rc = kafs_rpc_recv_resp(ctx->c_hotplug_fd, &resp_hdr, &resp, sizeof(resp), &resp_len);
    if (rc == 0 && resp_hdr.req_id != req_id)
      rc = -EBADMSG;
    if (rc == 0 && resp_hdr.result != 0)
      rc = resp_hdr.result;
    if (rc == 0 && resp_len != sizeof(resp))
      rc = -EBADMSG;
  }
  if (ctx->c_hotplug_lock_init)
    pthread_mutex_unlock(&ctx->c_hotplug_lock);
  if (kafs_hotplug_is_disconnect_error(rc))
  {
    kafs_hotplug_mark_disconnected(ctx, rc);
    rc = -EIO;
  }
  return rc;
}

static int kafs_is_ctl_path(const char *path);

static int kafs_op_getattr(const char *path, struct stat *st, struct fuse_file_info *fi)
{
  struct fuse_context *fctx = fuse_get_context();
  struct kafs_context *ctx = fctx->private_data;
  if (kafs_is_ctl_path(path))
  {
    memset(st, 0, sizeof(*st));
    st->st_mode = S_IFREG | 0600;
    st->st_nlink = 1;
    st->st_uid = fctx->uid;
    st->st_gid = fctx->gid;
    st->st_size = (off_t)(sizeof(kafs_rpc_resp_hdr_t) + KAFS_RPC_MAX_PAYLOAD);
    st->st_blksize = 4096;
    st->st_blocks = 0;
    st->st_atim = kafs_now();
    st->st_mtim = st->st_atim;
    st->st_ctim = st->st_atim;
    return 0;
  }
  struct kafs_sinode *inoent;
  KAFS_CALL(kafs_access, fctx, ctx, path, fi, F_OK, &inoent);
  int rc_hp = kafs_hotplug_call_getattr(fctx, ctx, inoent, st);
  if (rc_hp == 0)
    return 0;
  if (!kafs_hotplug_should_fallback(rc_hp))
    return rc_hp;
  st->st_dev = 0;
  st->st_ino = kafs_ctx_ino_no(ctx, inoent);
  st->st_mode = kafs_ino_mode_get(inoent);
  st->st_nlink = kafs_ino_linkcnt_get(inoent);
  st->st_uid = kafs_ino_uid_get(inoent);
  st->st_gid = kafs_ino_gid_get(inoent);
  st->st_rdev = kafs_ino_dev_get(inoent);
  st->st_size = kafs_ino_size_get(inoent);
  st->st_blksize = kafs_sb_blksize_get(ctx->c_superblock);
  st->st_blocks =
      (blkcnt_t)((uint64_t)kafs_ino_blocks_get(inoent) * ((uint64_t)st->st_blksize / 512ull));
  st->st_atim = kafs_ino_atime_get(inoent);
  st->st_mtim = kafs_ino_mtime_get(inoent);
  st->st_ctim = kafs_ino_ctime_get(inoent);
  return 0;
}

static int kafs_op_statfs(const char *path, struct statvfs *st)
{
  (void)path;
  struct fuse_context *fctx = fuse_get_context();
  struct kafs_context *ctx = fctx->private_data;
  memset(st, 0, sizeof(*st));

  const unsigned blksize = (unsigned)kafs_sb_blksize_get(ctx->c_superblock);
  const kafs_blkcnt_t blocks = kafs_sb_blkcnt_get(ctx->c_superblock);
  kafs_bitmap_lock(ctx);
  const kafs_blkcnt_t bfree = kafs_sb_blkcnt_free_get(ctx->c_superblock);
  kafs_bitmap_unlock(ctx);
  const kafs_inocnt_t files = kafs_sb_inocnt_get(ctx->c_superblock);
  const kafs_inocnt_t ffree = (kafs_inocnt_t)kafs_sb_inocnt_free_get(ctx->c_superblock);

  st->f_bsize = blksize;
  st->f_frsize = blksize;
  st->f_blocks = (fsblkcnt_t)blocks;
  st->f_bfree = (fsblkcnt_t)bfree;
  st->f_bavail = (fsblkcnt_t)bfree;
  st->f_files = (fsfilcnt_t)files;
  st->f_ffree = (fsfilcnt_t)ffree;
  st->f_favail = (fsfilcnt_t)ffree;
  st->f_namemax = 255;

  (void)fctx;
  return 0;
}

#define KAFS_STATS_VERSION 16u

static int kafs_u64_cmp(const void *a, const void *b)
{
  uint64_t av = *(const uint64_t *)a;
  uint64_t bv = *(const uint64_t *)b;
  if (av < bv)
    return -1;
  if (av > bv)
    return 1;
  return 0;
}

static uint64_t kafs_percentile_u64(uint64_t *arr, size_t n, double q)
{
  if (!arr || n == 0)
    return 0;
  if (q <= 0.0)
    return arr[0];
  if (q >= 1.0)
    return arr[n - 1];
  double pos = q * (double)(n - 1u);
  size_t idx = (size_t)(pos + 0.5);
  if (idx >= n)
    idx = n - 1;
  return arr[idx];
}

static int kafs_time_before(kafs_time_t lhs, kafs_time_t rhs)
{
  if (lhs.tv_sec != rhs.tv_sec)
    return lhs.tv_sec < rhs.tv_sec;
  return lhs.tv_nsec < rhs.tv_nsec;
}

// Forward decl (used by ioctl implementation)
static int kafs_create(const char *path, kafs_mode_t mode, kafs_dev_t dev, kafs_inocnt_t *pino_dir,
                       kafs_inocnt_t *pino_new);

static inline kafs_hrl_entry_t *kafs_hrl_entries_tbl(kafs_context_t *ctx)
{
  uintptr_t base = (uintptr_t)ctx->c_superblock;
  uint64_t off = kafs_sb_hrl_entry_offset_get(ctx->c_superblock);
  if (!off)
    return NULL;
  return (kafs_hrl_entry_t *)(base + (uintptr_t)off);
}

static void kafs_stats_snapshot(kafs_context_t *ctx, kafs_stats_t *out, uint32_t request_flags)
{
  memset(out, 0, sizeof(*out));
  out->struct_size = (uint32_t)sizeof(*out);
  out->version = KAFS_STATS_VERSION;
  out->request_flags = request_flags;

  out->blksize = (uint32_t)kafs_sb_blksize_get(ctx->c_superblock);
  out->fs_blocks_total = (uint64_t)kafs_sb_blkcnt_get(ctx->c_superblock);
  kafs_bitmap_lock(ctx);
  out->fs_blocks_free = (uint64_t)kafs_sb_blkcnt_free_get(ctx->c_superblock);
  kafs_bitmap_unlock(ctx);
  out->fs_inodes_total = (uint64_t)kafs_sb_inocnt_get(ctx->c_superblock);
  out->fs_inodes_free = (uint64_t)(kafs_inocnt_t)kafs_sb_inocnt_free_get(ctx->c_superblock);

  out->hrl_entries_total = (uint64_t)kafs_sb_hrl_entry_cnt_get(ctx->c_superblock);

  if ((request_flags & KAFS_STATS_F_VERBOSE_SCAN) != 0)
  {
    out->result_flags |= KAFS_STATS_R_VERBOSE_SCAN;

    kafs_time_t oldest_tombstone = {0};
    int have_oldest_tombstone = 0;
    for (kafs_inocnt_t ino = KAFS_INO_ROOTDIR; ino < (kafs_inocnt_t)out->fs_inodes_total; ++ino)
    {
      kafs_time_t dtime = {0};
      int is_tombstone = 0;

      kafs_inode_lock(ctx, ino);
      const kafs_sinode_t *inoent = kafs_ctx_inode_const(ctx, ino);
      if (kafs_inode_is_tombstone(inoent))
      {
        dtime = kafs_ino_dtime_get(inoent);
        is_tombstone = 1;
      }
      kafs_inode_unlock(ctx, ino);

      if (!is_tombstone)
        continue;

      out->tombstone_inodes++;
      if (!have_oldest_tombstone || kafs_time_before(dtime, oldest_tombstone))
      {
        oldest_tombstone = dtime;
        have_oldest_tombstone = 1;
      }
    }
    if (have_oldest_tombstone)
    {
      out->tombstone_oldest_dtime_sec = (uint64_t)oldest_tombstone.tv_sec;
      out->tombstone_oldest_dtime_nsec = (uint64_t)oldest_tombstone.tv_nsec;
    }

    uint32_t entry_cnt = kafs_sb_hrl_entry_cnt_get(ctx->c_superblock);
    kafs_hrl_entry_t *ents = kafs_hrl_entries_tbl(ctx);
    if (ents)
    {
      uint64_t used = 0, dup = 0, refsum = 0;
      for (uint32_t i = 0; i < entry_cnt; ++i)
      {
        uint32_t r = ents[i].refcnt;
        if (r)
        {
          used++;
          refsum += r;
          if (r > 1)
            dup++;
        }
      }
      out->hrl_entries_used = used;
      out->hrl_entries_duplicated = dup;
      out->hrl_refcnt_sum = refsum;
    }
  }

  out->hrl_put_calls = ctx->c_stat_hrl_put_calls;
  out->hrl_put_hits = ctx->c_stat_hrl_put_hits;
  out->hrl_put_misses = ctx->c_stat_hrl_put_misses;
  out->hrl_put_fallback_legacy = ctx->c_stat_hrl_put_fallback_legacy;
  out->hrl_put_ns_hash = ctx->c_stat_hrl_put_ns_hash;
  out->hrl_put_ns_find = ctx->c_stat_hrl_put_ns_find;
  out->hrl_put_ns_cmp_content = ctx->c_stat_hrl_put_ns_cmp_content;
  out->hrl_put_ns_slot_alloc = ctx->c_stat_hrl_put_ns_slot_alloc;
  out->hrl_put_ns_blk_alloc = ctx->c_stat_hrl_put_ns_blk_alloc;
  out->hrl_put_ns_blk_write = ctx->c_stat_hrl_put_ns_blk_write;
  out->hrl_put_chain_steps = ctx->c_stat_hrl_put_chain_steps;
  out->hrl_put_cmp_calls = ctx->c_stat_hrl_put_cmp_calls;
  out->hrl_rescue_attempts = ctx->c_stat_hrl_rescue_attempts;
  out->hrl_rescue_hits = ctx->c_stat_hrl_rescue_hits;
  out->hrl_rescue_evicts = ctx->c_stat_hrl_rescue_evicts;

  out->lock_hrl_bucket_acquire = ctx->c_stat_lock_hrl_bucket_acquire;
  out->lock_hrl_bucket_contended = ctx->c_stat_lock_hrl_bucket_contended;
  out->lock_hrl_bucket_wait_ns = ctx->c_stat_lock_hrl_bucket_wait_ns;
  out->lock_hrl_global_acquire = ctx->c_stat_lock_hrl_global_acquire;
  out->lock_hrl_global_contended = ctx->c_stat_lock_hrl_global_contended;
  out->lock_hrl_global_wait_ns = ctx->c_stat_lock_hrl_global_wait_ns;
  out->lock_bitmap_acquire = ctx->c_stat_lock_bitmap_acquire;
  out->lock_bitmap_contended = ctx->c_stat_lock_bitmap_contended;
  out->lock_bitmap_wait_ns = ctx->c_stat_lock_bitmap_wait_ns;
  out->lock_inode_acquire = ctx->c_stat_lock_inode_acquire;
  out->lock_inode_contended = ctx->c_stat_lock_inode_contended;
  out->lock_inode_wait_ns = ctx->c_stat_lock_inode_wait_ns;
  out->lock_inode_alloc_acquire = ctx->c_stat_lock_inode_alloc_acquire;
  out->lock_inode_alloc_contended = ctx->c_stat_lock_inode_alloc_contended;
  out->lock_inode_alloc_wait_ns = ctx->c_stat_lock_inode_alloc_wait_ns;

  out->access_calls = ctx->c_stat_access_calls;
  out->access_path_walk_calls = ctx->c_stat_access_path_walk_calls;
  out->access_fh_fastpath_hits = ctx->c_stat_access_fh_fastpath_hits;
  out->access_path_components = ctx->c_stat_access_path_components;
  out->dir_snapshot_calls = ctx->c_stat_dir_snapshot_calls;
  out->dir_snapshot_bytes = ctx->c_stat_dir_snapshot_bytes;
  out->dir_snapshot_meta_load_calls = ctx->c_stat_dir_snapshot_meta_load_calls;
  out->dirent_view_next_calls = ctx->c_stat_dirent_view_next_calls;

  out->pwrite_calls = ctx->c_stat_pwrite_calls;
  out->pwrite_bytes = ctx->c_stat_pwrite_bytes;
  out->pwrite_ns_iblk_read = ctx->c_stat_pwrite_ns_iblk_read;
  out->pwrite_ns_iblk_write = ctx->c_stat_pwrite_ns_iblk_write;
  out->pwrite_iblk_write_sample_count =
      __atomic_load_n(&ctx->c_stat_pwrite_iblk_write_sample_count, __ATOMIC_RELAXED);
  out->pwrite_iblk_write_sample_cap = (uint64_t)(sizeof(ctx->c_stat_pwrite_iblk_write_samples) /
                                                 sizeof(ctx->c_stat_pwrite_iblk_write_samples[0]));
  if (out->pwrite_iblk_write_sample_count > out->pwrite_iblk_write_sample_cap)
    out->pwrite_iblk_write_sample_count = out->pwrite_iblk_write_sample_cap;
  if (out->pwrite_iblk_write_sample_count > 0)
  {
    size_t n = (size_t)out->pwrite_iblk_write_sample_count;
    uint64_t tmp[n];
    for (size_t i = 0; i < n; ++i)
      tmp[i] = ctx->c_stat_pwrite_iblk_write_samples[i];
    qsort(tmp, n, sizeof(tmp[0]), kafs_u64_cmp);
    out->pwrite_iblk_write_p50_ns = kafs_percentile_u64(tmp, n, 0.50);
    out->pwrite_iblk_write_p95_ns = kafs_percentile_u64(tmp, n, 0.95);
    out->pwrite_iblk_write_p99_ns = kafs_percentile_u64(tmp, n, 0.99);
  }
  out->iblk_write_ns_hrl_put = ctx->c_stat_iblk_write_ns_hrl_put;
  out->iblk_write_ns_legacy_blk_write = ctx->c_stat_iblk_write_ns_legacy_blk_write;
  out->iblk_write_ns_dec_ref = ctx->c_stat_iblk_write_ns_dec_ref;

  out->blk_alloc_calls = ctx->c_stat_blk_alloc_calls;
  out->blk_alloc_claim_retries = ctx->c_stat_blk_alloc_claim_retries;
  out->blk_alloc_ns_scan = ctx->c_stat_blk_alloc_ns_scan;
  out->blk_alloc_ns_claim = ctx->c_stat_blk_alloc_ns_claim;
  out->blk_alloc_ns_set_usage = ctx->c_stat_blk_alloc_ns_set_usage;

  out->blk_set_usage_calls = ctx->c_stat_blk_set_usage_calls;
  out->blk_set_usage_alloc_calls = ctx->c_stat_blk_set_usage_alloc_calls;
  out->blk_set_usage_free_calls = ctx->c_stat_blk_set_usage_free_calls;
  out->blk_set_usage_ns_bit_update = ctx->c_stat_blk_set_usage_ns_bit_update;
  out->blk_set_usage_ns_freecnt_update = ctx->c_stat_blk_set_usage_ns_freecnt_update;
  out->blk_set_usage_ns_wtime_update = ctx->c_stat_blk_set_usage_ns_wtime_update;

  out->copy_share_attempt_blocks = ctx->c_stat_copy_share_attempt_blocks;
  out->copy_share_done_blocks = ctx->c_stat_copy_share_done_blocks;
  out->copy_share_fallback_blocks = ctx->c_stat_copy_share_fallback_blocks;
  out->copy_share_skip_unaligned = ctx->c_stat_copy_share_skip_unaligned;
  out->copy_share_skip_dst_inline = ctx->c_stat_copy_share_skip_dst_inline;
  out->bg_dedup_replacements = ctx->c_stat_bg_dedup_replacements;
  out->bg_dedup_evicts = ctx->c_stat_bg_dedup_evicts;
  out->bg_dedup_retries = ctx->c_stat_bg_dedup_retries;
  out->bg_dedup_steps = ctx->c_stat_bg_dedup_steps;
  out->bg_dedup_scanned_blocks = ctx->c_stat_bg_dedup_scanned_blocks;
  out->bg_dedup_direct_candidates = ctx->c_stat_bg_dedup_direct_candidates;
  out->bg_dedup_direct_hits = ctx->c_stat_bg_dedup_direct_hits;
  out->bg_dedup_index_evicts = ctx->c_stat_bg_dedup_index_evicts;
  out->bg_dedup_cooldowns = ctx->c_stat_bg_dedup_cooldowns;
  out->bg_dedup_mode = ctx->c_bg_dedup_mode;
  out->bg_dedup_telemetry_valid = ctx->c_bg_dedup_telemetry_valid;
  out->bg_dedup_last_scanned_blocks = ctx->c_bg_dedup_last_scanned_blocks;
  out->bg_dedup_last_direct_candidates = ctx->c_bg_dedup_last_direct_candidates;
  out->bg_dedup_last_replacements = ctx->c_bg_dedup_last_replacements;
  out->bg_dedup_idle_skip_streak = ctx->c_bg_dedup_idle_skip_streak;
  uint64_t now_ns = kafs_now_ns();
  if (ctx->c_bg_dedup_cold_start_due_ns > now_ns)
    out->bg_dedup_cold_start_due_ms = (ctx->c_bg_dedup_cold_start_due_ns - now_ns) / 1000000ull;
  else
    out->bg_dedup_cold_start_due_ms = 0;

  out->pending_worker_start_calls =
      __atomic_load_n(&ctx->c_stat_pending_worker_start_calls, __ATOMIC_RELAXED);
  out->pending_worker_start_failures =
      __atomic_load_n(&ctx->c_stat_pending_worker_start_failures, __ATOMIC_RELAXED);
  out->pending_worker_start_last_error =
      __atomic_load_n(&ctx->c_stat_pending_worker_start_last_error, __ATOMIC_RELAXED);
  out->pending_worker_lwp_tid =
      __atomic_load_n(&ctx->c_stat_pending_worker_lwp_tid, __ATOMIC_RELAXED);
  out->pending_worker_running = ctx->c_pending_worker_running;
  out->pending_worker_stop_flag = ctx->c_pending_worker_stop;
  out->pending_worker_main_entries =
      __atomic_load_n(&ctx->c_stat_pending_worker_main_entries, __ATOMIC_RELAXED);
  out->pending_worker_main_exits =
      __atomic_load_n(&ctx->c_stat_pending_worker_main_exits, __ATOMIC_RELAXED);
  out->pending_resolved = __atomic_load_n(&ctx->c_stat_pending_resolved, __ATOMIC_RELAXED);
  out->pending_old_block_freed =
      __atomic_load_n(&ctx->c_stat_pending_old_block_freed, __ATOMIC_RELAXED);

  pthread_mutex_lock(&ctx->c_pending_worker_lock);
  kafs_pendinglog_hdr_t *hdr = kafs_pendinglog_hdr_ptr(ctx);
  if (hdr)
  {
    out->pending_queue_depth = kafs_pendinglog_count(ctx);
    out->pending_queue_capacity = hdr->capacity;
    out->pending_queue_head = hdr->head;
    out->pending_queue_tail = hdr->tail;
  }
  pthread_mutex_unlock(&ctx->c_pending_worker_lock);
}

#ifdef __linux__
static int kafs_procfd_to_kafs_path(kafs_context_t *ctx, pid_t pid, int fd, char out[PATH_MAX])
{
  if (!ctx || !ctx->c_mountpoint)
    return -EINVAL;
  char proc[64];
  snprintf(proc, sizeof(proc), "/proc/%d/fd/%d", (int)pid, fd);
  ssize_t n = readlink(proc, out, PATH_MAX - 1);
  if (n < 0)
    return -errno;
  out[n] = '\0';

  // trim " (deleted)" suffix when present
  char *del = strstr(out, " (deleted)");
  if (del)
    *del = '\0';

  const char *mnt = ctx->c_mountpoint;
  size_t ml = strlen(mnt);
  if (ml > 1 && mnt[ml - 1] == '/')
    ml--;
  if (strncmp(out, mnt, ml) != 0 || (out[ml] != '/' && out[ml] != '\0'))
    return -EXDEV;
  const char *suf = out + ml;
  if (*suf == '\0')
    suf = "/";
  memmove(out, suf, strlen(suf) + 1);
  return 0;
}
#endif

static int kafs_reflink_snapshot_source(kafs_context_t *ctx, kafs_sinode_t *src,
                                        kafs_off_t *size_out, char *inline_buf, int *is_inline_out,
                                        kafs_blkcnt_t **blos_out, kafs_iblkcnt_t *iblocnt_out)
{
  *size_out = 0;
  *is_inline_out = 0;
  *blos_out = NULL;
  *iblocnt_out = 0;

  uint32_t ino_src = kafs_ctx_ino_no(ctx, src);
  kafs_inode_lock(ctx, ino_src);
  kafs_off_t size = kafs_ino_size_get(src);
  if (size <= (kafs_off_t)KAFS_INODE_DIRECT_BYTES)
  {
    memcpy(inline_buf, (void *)src->i_blkreftbl, (size_t)size);
    kafs_inode_unlock(ctx, ino_src);
    *size_out = size;
    *is_inline_out = 1;
    return 0;
  }

  kafs_blksize_t bs = kafs_sb_blksize_get(ctx->c_superblock);
  kafs_iblkcnt_t iblocnt = (kafs_iblkcnt_t)((size + (kafs_off_t)bs - 1) / (kafs_off_t)bs);
  kafs_blkcnt_t *blos =
      (kafs_blkcnt_t *)calloc((size_t)iblocnt ? (size_t)iblocnt : 1u, sizeof(*blos));
  if (!blos)
  {
    kafs_inode_unlock(ctx, ino_src);
    return -ENOMEM;
  }

  for (kafs_iblkcnt_t i = 0; i < iblocnt; ++i)
  {
    kafs_blkcnt_t b = KAFS_BLO_NONE;
    int rc = kafs_ino_ibrk_run(ctx, src, i, &b, KAFS_IBLKREF_FUNC_GET);
    if (rc < 0)
    {
      free(blos);
      kafs_inode_unlock(ctx, ino_src);
      return rc;
    }
    blos[i] = b;
  }

  kafs_inode_unlock(ctx, ino_src);
  *size_out = size;
  *blos_out = blos;
  *iblocnt_out = iblocnt;
  return 0;
}

static int kafs_reflink_prepare_destination(kafs_context_t *ctx, kafs_sinode_t *dst)
{
  uint32_t ino_dst = kafs_ctx_ino_no(ctx, dst);
  kafs_inode_lock(ctx, ino_dst);
  int trc = kafs_truncate(ctx, dst, 0);
  if (trc < 0)
  {
    kafs_inode_unlock(ctx, ino_dst);
    return trc;
  }
  memset(dst->i_blkreftbl, 0, sizeof(dst->i_blkreftbl));
  return 0;
}

static int kafs_reflink_apply_inline_locked(kafs_context_t *ctx, kafs_sinode_t *dst,
                                            kafs_off_t size, const char *inline_buf)
{
  uint32_t ino_dst = kafs_ctx_ino_no(ctx, dst);
  kafs_ino_size_set(dst, size);
  memcpy((void *)dst->i_blkreftbl, inline_buf, (size_t)size);
  kafs_time_t now = kafs_now();
  kafs_ino_mtime_set(dst, now);
  kafs_ino_ctime_set(dst, now);
  kafs_inode_unlock(ctx, ino_dst);
  return 0;
}

static int kafs_reflink_apply_blocks_locked(kafs_context_t *ctx, kafs_sinode_t *dst,
                                            kafs_off_t size, kafs_blkcnt_t *blos,
                                            kafs_iblkcnt_t iblocnt)
{
  uint32_t ino_dst = kafs_ctx_ino_no(ctx, dst);
  kafs_ino_size_set(dst, size);
  for (kafs_iblkcnt_t i = 0; i < iblocnt; ++i)
  {
    kafs_blkcnt_t b = blos[i];
    if (b == KAFS_BLO_NONE)
      continue;

    int irc = kafs_hrl_inc_ref_by_blo(ctx, b);
    if (irc != 0)
    {
      (void)kafs_truncate(ctx, dst, 0);
      kafs_inode_unlock(ctx, ino_dst);
      return (irc == -ENOENT || irc == -ENOSYS) ? -EOPNOTSUPP : irc;
    }

    int s = kafs_ino_ibrk_run(ctx, dst, i, &b, KAFS_IBLKREF_FUNC_SET);
    if (s < 0)
    {
      (void)kafs_truncate(ctx, dst, 0);
      kafs_inode_unlock(ctx, ino_dst);
      (void)kafs_inode_release_hrl_ref(ctx, b);
      return s;
    }
  }

  kafs_time_t now = kafs_now();
  kafs_ino_mtime_set(dst, now);
  kafs_ino_ctime_set(dst, now);
  kafs_inode_unlock(ctx, ino_dst);
  return 0;
}

static int kafs_reflink_clone(kafs_context_t *ctx, kafs_sinode_t *src, kafs_sinode_t *dst)
{
  if (!ctx || !src || !dst)
    return -EINVAL;
  if (src == dst)
    return 0;
  if (ctx->c_hrl_bucket_cnt == 0)
    return -EOPNOTSUPP;

  kafs_mode_t sm = kafs_ino_mode_get(src);
  kafs_mode_t dm = kafs_ino_mode_get(dst);
  if (!S_ISREG(sm) || !S_ISREG(dm))
    return -EINVAL;

  kafs_off_t size = 0;
  char inline_buf[KAFS_INODE_DIRECT_BYTES];
  int is_inline = 0;
  kafs_blkcnt_t *blos = NULL;
  kafs_iblkcnt_t iblocnt = 0;
  int rc = kafs_reflink_snapshot_source(ctx, src, &size, inline_buf, &is_inline, &blos, &iblocnt);
  if (rc < 0)
    return rc;
  rc = kafs_reflink_prepare_destination(ctx, dst);
  if (rc < 0)
  {
    free(blos);
    return rc;
  }

  if (is_inline)
    return kafs_reflink_apply_inline_locked(ctx, dst, size, inline_buf);

  rc = kafs_reflink_apply_blocks_locked(ctx, dst, size, blos, iblocnt);
  free(blos);
  return rc;
}

static int kafs_copy_share_one_iblk_locked(kafs_context_t *ctx, kafs_sinode_t *src,
                                           kafs_sinode_t *dst, kafs_iblkcnt_t src_iblo,
                                           kafs_iblkcnt_t dst_iblo, uint32_t ino_dst);

static int kafs_copy_regular_try_share_locked(kafs_context_t *ctx, kafs_sinode_t *ino_in,
                                              kafs_sinode_t *ino_out, uint32_t ino_dst,
                                              off_t offset_in, off_t offset_out,
                                              kafs_off_t dst_size, int *dst_is_block_backed,
                                              int *dst_empty_converted,
                                              kafs_logblksize_t log_blksize, kafs_blksize_t blksize,
                                              kafs_off_t max, kafs_off_t *done)
{
  kafs_off_t src_off = (kafs_off_t)offset_in + *done;
  kafs_off_t dst_off = (kafs_off_t)offset_out + *done;
  kafs_off_t remain = max - *done;

  if (ctx->c_hrl_bucket_cnt == 0 || remain < (kafs_off_t)blksize ||
      (src_off & ((kafs_off_t)blksize - 1)) != 0 || (dst_off & ((kafs_off_t)blksize - 1)) != 0)
  {
    __atomic_add_fetch(&ctx->c_stat_copy_share_skip_unaligned, 1u, __ATOMIC_RELAXED);
    return 0;
  }

  if (!*dst_is_block_backed)
  {
    if (dst_size == 0 && (kafs_off_t)offset_out == 0)
    {
      memset(ino_out->i_blkreftbl, 0, sizeof(ino_out->i_blkreftbl));
      *dst_is_block_backed = 1;
      *dst_empty_converted = 1;
    }
    else
    {
      __atomic_add_fetch(&ctx->c_stat_copy_share_skip_dst_inline, 1u, __ATOMIC_RELAXED);
      return 0;
    }
  }

  size_t blocks = (size_t)(remain >> log_blksize);
  __atomic_add_fetch(&ctx->c_stat_copy_share_attempt_blocks, (uint64_t)blocks, __ATOMIC_RELAXED);
  size_t copied_blocks = 0;
  for (size_t i = 0; i < blocks; ++i)
  {
    kafs_iblkcnt_t src_iblo = (kafs_iblkcnt_t)(src_off >> log_blksize) + (kafs_iblkcnt_t)i;
    kafs_iblkcnt_t dst_iblo = (kafs_iblkcnt_t)(dst_off >> log_blksize) + (kafs_iblkcnt_t)i;
    int frc = kafs_copy_share_one_iblk_locked(ctx, ino_in, ino_out, src_iblo, dst_iblo, ino_dst);
    if (frc == -EAGAIN)
      break;
    if (frc < 0)
      return frc;
    copied_blocks++;
    *done += (kafs_off_t)blksize;
  }

  if (copied_blocks > 0)
  {
    kafs_off_t end = (kafs_off_t)offset_out + *done;
    if (kafs_ino_size_get(ino_out) < end)
      kafs_ino_size_set(ino_out, end);
    return 1;
  }

  if (*dst_empty_converted)
    memset(ino_out->i_blkreftbl, 0, sizeof(ino_out->i_blkreftbl));
  return 0;
}

static ssize_t kafs_copy_regular_buffered_step(kafs_context_t *ctx, kafs_sinode_t *ino_in,
                                               kafs_sinode_t *ino_out, char *buf, size_t bufsz,
                                               off_t offset_in, off_t offset_out, kafs_off_t max,
                                               kafs_off_t done)
{
  size_t want = (size_t)((max - done) < (kafs_off_t)bufsz ? (max - done) : (kafs_off_t)bufsz);
  ssize_t r = kafs_pread(ctx, ino_in, buf, (kafs_off_t)want, (kafs_off_t)offset_in + done);
  if (r <= 0)
    return r;
  return kafs_pwrite(ctx, ino_out, buf, (kafs_off_t)r, (kafs_off_t)offset_out + done);
}

static ssize_t kafs_copy_regular_range(kafs_context_t *ctx, kafs_sinode_t *ino_in,
                                       kafs_sinode_t *ino_out, off_t offset_in, off_t offset_out,
                                       size_t size)
{
  uint32_t ino_src = kafs_ctx_ino_no(ctx, ino_in);
  uint32_t ino_dst = kafs_ctx_ino_no(ctx, ino_out);
  if (ino_src == ino_dst)
    return 0;

  if (ino_src < ino_dst)
  {
    kafs_inode_lock(ctx, ino_src);
    kafs_inode_lock(ctx, ino_dst);
  }
  else
  {
    kafs_inode_lock(ctx, ino_dst);
    kafs_inode_lock(ctx, ino_src);
  }

  kafs_off_t src_size = kafs_ino_size_get(ino_in);
  if ((kafs_off_t)offset_in >= src_size)
  {
    kafs_inode_unlock(ctx, ino_src);
    kafs_inode_unlock(ctx, ino_dst);
    return 0;
  }

  kafs_off_t max = src_size - (kafs_off_t)offset_in;
  if ((kafs_off_t)size < max)
    max = (kafs_off_t)size;

  kafs_logblksize_t log_blksize = kafs_sb_log_blksize_get(ctx->c_superblock);
  kafs_blksize_t blksize = kafs_sb_blksize_get(ctx->c_superblock);
  kafs_off_t dst_size = kafs_ino_size_get(ino_out);
  int dst_is_block_backed = (dst_size > (kafs_off_t)KAFS_INODE_DIRECT_BYTES);
  int dst_empty_converted = 0;

  const size_t bufsz = 128u * 1024u;
  char *buf = (char *)malloc(bufsz);
  if (!buf)
  {
    kafs_inode_unlock(ctx, ino_src);
    kafs_inode_unlock(ctx, ino_dst);
    return -ENOMEM;
  }

  kafs_off_t done = 0;
  while (done < max)
  {
    int src = kafs_copy_regular_try_share_locked(
        ctx, ino_in, ino_out, ino_dst, offset_in, offset_out, dst_size, &dst_is_block_backed,
        &dst_empty_converted, log_blksize, blksize, max, &done);
    if (src < 0)
    {
      free(buf);
      kafs_inode_unlock(ctx, ino_src);
      kafs_inode_unlock(ctx, ino_dst);
      return src;
    }
    if (src > 0)
      continue;

    ssize_t w = kafs_copy_regular_buffered_step(ctx, ino_in, ino_out, buf, bufsz, offset_in,
                                                offset_out, max, done);
    if (w < 0)
    {
      free(buf);
      kafs_inode_unlock(ctx, ino_src);
      kafs_inode_unlock(ctx, ino_dst);
      return w;
    }
    if (w == 0)
      break;
    done += w;
  }

  free(buf);
  kafs_inode_unlock(ctx, ino_src);
  kafs_inode_unlock(ctx, ino_dst);
  return (ssize_t)done;
}

// Copy one full block by sharing HRL reference instead of read+hash+write.
// Caller must hold src/dst inode locks.
static int kafs_copy_share_one_iblk_locked(kafs_context_t *ctx, kafs_sinode_t *src,
                                           kafs_sinode_t *dst, kafs_iblkcnt_t src_iblo,
                                           kafs_iblkcnt_t dst_iblo, uint32_t ino_dst)
{
  kafs_blkcnt_t release_list[4];
  size_t release_cnt = 0;
  kafs_blkcnt_t src_blo = KAFS_BLO_NONE;
  int rc = kafs_ino_ibrk_run(ctx, src, src_iblo, &src_blo, KAFS_IBLKREF_FUNC_GET);
  if (rc < 0)
    return rc;

  kafs_blkcnt_t old_blo = KAFS_BLO_NONE;
  rc = kafs_ino_ibrk_run(ctx, dst, dst_iblo, &old_blo, KAFS_IBLKREF_FUNC_GET);
  if (rc < 0)
    return rc;

  if (old_blo == src_blo)
  {
    __atomic_add_fetch(&ctx->c_stat_copy_share_done_blocks, 1u, __ATOMIC_RELAXED);
    return 0;
  }

  if (src_blo != KAFS_BLO_NONE)
  {
    int irc = kafs_hrl_inc_ref_by_blo(ctx, src_blo);
    if (irc == -ENOENT || irc == -ENOSYS)
    {
      __atomic_add_fetch(&ctx->c_stat_copy_share_fallback_blocks, 1u, __ATOMIC_RELAXED);
      return -EAGAIN;
    }
    if (irc < 0)
      return irc;
  }

  kafs_blkcnt_t set_blo = src_blo;
  rc = kafs_ino_ibrk_run(ctx, dst, dst_iblo, &set_blo, KAFS_IBLKREF_FUNC_SET);
  if (rc < 0)
  {
    if (src_blo != KAFS_BLO_NONE)
      (void)kafs_inode_release_hrl_ref(ctx, src_blo);
    return rc;
  }

  if (src_blo == KAFS_BLO_NONE)
  {
    kafs_blkcnt_t f1 = KAFS_BLO_NONE;
    kafs_blkcnt_t f2 = KAFS_BLO_NONE;
    kafs_blkcnt_t f3 = KAFS_BLO_NONE;
    rc = kafs_ino_prune_empty_indirects(ctx, dst, dst_iblo, &f1, &f2, &f3);
    if (rc < 0)
      return rc;
    if (f1 != KAFS_BLO_NONE)
      release_list[release_cnt++] = f1;
    if (f2 != KAFS_BLO_NONE)
      release_list[release_cnt++] = f2;
    if (f3 != KAFS_BLO_NONE)
      release_list[release_cnt++] = f3;
  }

  if (old_blo != KAFS_BLO_NONE)
    release_list[release_cnt++] = old_blo;

  if (release_cnt != 0)
  {
    // Keep existing lock ordering: dec_ref runs outside inode lock.
    kafs_inode_unlock(ctx, ino_dst);
    for (size_t i = 0; i < release_cnt; ++i)
      (void)kafs_inode_release_hrl_ref(ctx, release_list[i]);
    kafs_inode_lock(ctx, ino_dst);
  }
  __atomic_add_fetch(&ctx->c_stat_copy_share_done_blocks, 1u, __ATOMIC_RELAXED);
  return 0;
}

#define KAFS_CTL_PATH "/.kafs.sock"
#define KAFS_CTL_MAX_REQ (sizeof(kafs_rpc_hdr_t) + KAFS_RPC_MAX_PAYLOAD)
#define KAFS_CTL_MAX_RESP (sizeof(kafs_rpc_resp_hdr_t) + KAFS_RPC_MAX_PAYLOAD)

typedef struct
{
  size_t resp_len;
  unsigned char resp[KAFS_CTL_MAX_RESP];
} kafs_ctl_session_t;

static int kafs_is_ctl_path(const char *path) { return path && strcmp(path, KAFS_CTL_PATH) == 0; }

static int kafs_hotplug_env_key_len(const char *key)
{
  if (!key)
    return -1;
  size_t len = strnlen(key, KAFS_HOTPLUG_ENV_KEY_MAX);
  if (len == 0 || len >= KAFS_HOTPLUG_ENV_KEY_MAX)
    return -1;
  return (int)len;
}

static int kafs_hotplug_env_value_len(const char *value)
{
  if (!value)
    return -1;
  size_t len = strnlen(value, KAFS_HOTPLUG_ENV_VALUE_MAX);
  if (len >= KAFS_HOTPLUG_ENV_VALUE_MAX)
    return -1;
  return (int)len;
}

static int kafs_hotplug_env_find(const kafs_context_t *ctx, const char *key)
{
  if (!ctx || !key)
    return -1;
  for (uint32_t i = 0; i < ctx->c_hotplug_env_count; ++i)
  {
    if (strcmp(ctx->c_hotplug_env[i].key, key) == 0)
      return (int)i;
  }
  return -1;
}

static void kafs_hotplug_env_lock(kafs_context_t *ctx)
{
  if (ctx && ctx->c_hotplug_lock_init)
    pthread_mutex_lock(&ctx->c_hotplug_lock);
}

static void kafs_hotplug_env_unlock(kafs_context_t *ctx)
{
  if (ctx && ctx->c_hotplug_lock_init)
    pthread_mutex_unlock(&ctx->c_hotplug_lock);
}

static int kafs_hotplug_env_set(kafs_context_t *ctx, const char *key, const char *value)
{
  if (!ctx)
    return -EINVAL;
  if (kafs_hotplug_env_key_len(key) < 0)
    return -EINVAL;
  if (kafs_hotplug_env_value_len(value) < 0)
    return -EINVAL;

  kafs_hotplug_env_lock(ctx);
  int idx = kafs_hotplug_env_find(ctx, key);
  if (idx < 0)
  {
    if (ctx->c_hotplug_env_count >= KAFS_HOTPLUG_ENV_MAX)
    {
      kafs_hotplug_env_unlock(ctx);
      return -ENOSPC;
    }
    idx = (int)ctx->c_hotplug_env_count++;
  }

  snprintf(ctx->c_hotplug_env[idx].key, sizeof(ctx->c_hotplug_env[idx].key), "%s", key);
  snprintf(ctx->c_hotplug_env[idx].value, sizeof(ctx->c_hotplug_env[idx].value), "%s", value);
  kafs_hotplug_env_unlock(ctx);
  return 0;
}

static int kafs_hotplug_env_unset(kafs_context_t *ctx, const char *key)
{
  if (!ctx)
    return -EINVAL;
  if (kafs_hotplug_env_key_len(key) < 0)
    return -EINVAL;

  kafs_hotplug_env_lock(ctx);
  int idx = kafs_hotplug_env_find(ctx, key);
  if (idx < 0)
  {
    kafs_hotplug_env_unlock(ctx);
    return -ENOENT;
  }
  for (uint32_t i = (uint32_t)idx + 1; i < ctx->c_hotplug_env_count; ++i)
    ctx->c_hotplug_env[i - 1] = ctx->c_hotplug_env[i];
  ctx->c_hotplug_env_count--;
  kafs_hotplug_env_unlock(ctx);
  return 0;
}

static void kafs_ctl_fill_status(const kafs_context_t *ctx, kafs_rpc_hotplug_status_t *out)
{
  memset(out, 0, sizeof(*out));
  out->version = KAFS_HOTPLUG_STATUS_VERSION;
  out->state = (uint32_t)ctx->c_hotplug_state;
  out->data_mode = ctx->c_hotplug_data_mode;
  out->session_id = ctx->c_hotplug_session_id;
  out->epoch = ctx->c_hotplug_epoch;
  out->last_error = ctx->c_hotplug_last_error;
  out->wait_queue_len = ctx->c_hotplug_wait_queue_len;
  out->wait_timeout_ms = ctx->c_hotplug_wait_timeout_ms;
  out->wait_queue_limit = ctx->c_hotplug_wait_queue_limit;
  out->front_major = ctx->c_hotplug_front_major;
  out->front_minor = ctx->c_hotplug_front_minor;
  out->front_features = ctx->c_hotplug_front_features;
  out->back_major = ctx->c_hotplug_back_major;
  out->back_minor = ctx->c_hotplug_back_minor;
  out->back_features = ctx->c_hotplug_back_features;
  out->compat_result = ctx->c_hotplug_compat_result;
  out->compat_reason = ctx->c_hotplug_compat_reason;
  out->pending_worker_prio_mode = ctx->c_pending_worker_prio_mode;
  out->pending_worker_nice = ctx->c_pending_worker_nice;
  out->pending_worker_prio_apply_error = ctx->c_pending_worker_prio_apply_error;
  out->fsync_policy = ctx->c_fsync_policy;
  out->pending_ttl_soft_ms = ctx->c_pending_ttl_soft_ms;
  out->pending_ttl_hard_ms = ctx->c_pending_ttl_hard_ms;
  out->pending_oldest_age_ms = ctx->c_pending_oldest_age_ms;
  out->pending_ttl_over_soft = ctx->c_pending_ttl_over_soft;
  out->pending_ttl_over_hard = ctx->c_pending_ttl_over_hard;
}

static void kafs_ctl_write_status_response(const kafs_context_t *ctx, unsigned char *resp_payload,
                                           uint32_t *resp_len)
{
  kafs_rpc_hotplug_status_t st;
  kafs_ctl_fill_status(ctx, &st);
  memcpy(resp_payload, &st, sizeof(st));
  *resp_len = (uint32_t)sizeof(st);
}

static int kafs_ctl_handle_set_timeout(kafs_context_t *ctx, uint32_t payload_len,
                                       const unsigned char *payload)
{
  if (payload_len != sizeof(kafs_rpc_set_timeout_t))
    return -EINVAL;

  const kafs_rpc_set_timeout_t *req = (const kafs_rpc_set_timeout_t *)payload;
  if (req->timeout_ms == 0)
    return -EINVAL;

  ctx->c_hotplug_wait_timeout_ms = req->timeout_ms;
  return 0;
}

static void kafs_ctl_write_env_list_response(kafs_context_t *ctx, unsigned char *resp_payload,
                                             uint32_t *resp_len)
{
  kafs_rpc_env_list_t env;
  memset(&env, 0, sizeof(env));
  kafs_hotplug_env_lock(ctx);
  env.count = ctx->c_hotplug_env_count;
  for (uint32_t i = 0; i < env.count; ++i)
    env.entries[i] = ctx->c_hotplug_env[i];
  kafs_hotplug_env_unlock(ctx);
  memcpy(resp_payload, &env, sizeof(env));
  *resp_len = (uint32_t)sizeof(env);
}

static int kafs_ctl_handle_env_update(kafs_context_t *ctx, uint32_t payload_len,
                                      const unsigned char *payload, int unset)
{
  if (payload_len != sizeof(kafs_rpc_env_update_t))
    return -EINVAL;

  const kafs_rpc_env_update_t *req = (const kafs_rpc_env_update_t *)payload;
  if (unset)
    return kafs_hotplug_env_unset(ctx, req->key);
  return kafs_hotplug_env_set(ctx, req->key, req->value);
}

static void kafs_ctl_apply_pending_worker_priority(kafs_context_t *ctx, uint32_t mode,
                                                   int32_t nice_value, uint32_t flags)
{
  ctx->c_pending_worker_prio_mode = mode;
  if ((flags & KAFS_RPC_SET_DEDUP_PRIO_F_HAS_NICE) != 0)
    ctx->c_pending_worker_nice = nice_value;
  ctx->c_pending_worker_prio_base_mode = mode;
  if ((flags & KAFS_RPC_SET_DEDUP_PRIO_F_HAS_NICE) != 0)
    ctx->c_pending_worker_nice_base = nice_value;

  if (ctx->c_pending_worker_auto_boosted)
  {
    ctx->c_pending_worker_prio_mode = KAFS_PENDING_WORKER_PRIO_NORMAL;
    ctx->c_pending_worker_nice = 0;
  }
  else
  {
    ctx->c_pending_worker_prio_mode = ctx->c_pending_worker_prio_base_mode;
    ctx->c_pending_worker_nice = ctx->c_pending_worker_nice_base;
  }
  ctx->c_pending_worker_prio_dirty = 1;
  kafs_pending_worker_notify_all(ctx);
}

static void kafs_ctl_apply_bg_dedup_worker_priority(kafs_context_t *ctx, uint32_t mode,
                                                    int32_t nice_value, uint32_t flags)
{
  ctx->c_bg_dedup_worker_prio_base_mode = mode;
  if ((flags & KAFS_RPC_SET_DEDUP_PRIO_F_HAS_NICE) != 0)
    ctx->c_bg_dedup_worker_nice_base = nice_value;

  if (ctx->c_bg_dedup_worker_auto_boosted)
  {
    ctx->c_bg_dedup_worker_prio_mode = KAFS_PENDING_WORKER_PRIO_NORMAL;
    ctx->c_bg_dedup_worker_nice = 0;
  }
  else
  {
    ctx->c_bg_dedup_worker_prio_mode = ctx->c_bg_dedup_worker_prio_base_mode;
    ctx->c_bg_dedup_worker_nice = ctx->c_bg_dedup_worker_nice_base;
  }
  ctx->c_bg_dedup_worker_prio_dirty = 1;
  if (ctx->c_bg_dedup_worker_lock_init)
  {
    pthread_mutex_lock(&ctx->c_bg_dedup_worker_lock);
    pthread_cond_broadcast(&ctx->c_bg_dedup_worker_cond);
    pthread_mutex_unlock(&ctx->c_bg_dedup_worker_lock);
  }
}

static int kafs_ctl_handle_set_dedup_prio(kafs_context_t *ctx, uint32_t payload_len,
                                          const unsigned char *payload)
{
  if (payload_len != sizeof(kafs_rpc_set_dedup_prio_t))
    return -EINVAL;

  const kafs_rpc_set_dedup_prio_t *req = (const kafs_rpc_set_dedup_prio_t *)payload;
  if (req->mode != KAFS_PENDING_WORKER_PRIO_NORMAL && req->mode != KAFS_PENDING_WORKER_PRIO_IDLE)
    return -EINVAL;
  if ((req->flags & KAFS_RPC_SET_DEDUP_PRIO_F_HAS_NICE) != 0 &&
      (req->nice_value < 0 || req->nice_value > 19))
    return -ERANGE;

  kafs_ctl_apply_pending_worker_priority(ctx, req->mode, req->nice_value, req->flags);
  kafs_ctl_apply_bg_dedup_worker_priority(ctx, req->mode, req->nice_value, req->flags);
  return 0;
}

static int kafs_ctl_handle_set_runtime(kafs_context_t *ctx, uint32_t payload_len,
                                       const unsigned char *payload)
{
  if (payload_len != sizeof(kafs_rpc_set_runtime_t))
    return -EINVAL;

  const kafs_rpc_set_runtime_t *req = (const kafs_rpc_set_runtime_t *)payload;
  uint32_t next_policy = ctx->c_fsync_policy;
  uint32_t next_soft = ctx->c_pending_ttl_soft_ms;
  uint32_t next_hard = ctx->c_pending_ttl_hard_ms;

  if ((req->flags & KAFS_RPC_SET_RUNTIME_F_HAS_FSYNC_POLICY) != 0)
  {
    if (req->fsync_policy != KAFS_FSYNC_POLICY_FULL &&
        req->fsync_policy != KAFS_FSYNC_POLICY_JOURNAL_ONLY &&
        req->fsync_policy != KAFS_FSYNC_POLICY_ADAPTIVE)
      return -EINVAL;
    next_policy = req->fsync_policy;
  }

  if ((req->flags & KAFS_RPC_SET_RUNTIME_F_HAS_PENDING_TTL_SOFT_MS) != 0)
    next_soft = req->pending_ttl_soft_ms;

  if ((req->flags & KAFS_RPC_SET_RUNTIME_F_HAS_PENDING_TTL_HARD_MS) != 0)
    next_hard = req->pending_ttl_hard_ms;

  if (next_soft > 0 && next_hard > 0 && next_hard < next_soft)
    return -ERANGE;

  ctx->c_fsync_policy = next_policy;
  ctx->c_pending_ttl_soft_ms = next_soft;
  ctx->c_pending_ttl_hard_ms = next_hard;

  if (ctx->c_pending_worker_lock_init)
  {
    pthread_mutex_lock(&ctx->c_pending_worker_lock);
    kafs_pending_worker_adjust_priority_locked(ctx);
    if (ctx->c_pending_worker_prio_dirty)
      pthread_cond_broadcast(&ctx->c_pending_worker_cond);
    pthread_mutex_unlock(&ctx->c_pending_worker_lock);
  }
  return 0;
}

static int kafs_ctl_handle_request(kafs_context_t *ctx, kafs_ctl_session_t *sess,
                                   const unsigned char *buf, size_t size)
{
  if (!ctx || !sess || !buf)
    return -EINVAL;
  if (size < sizeof(kafs_rpc_hdr_t) || size > KAFS_CTL_MAX_REQ)
    return -EINVAL;

  kafs_rpc_hdr_t hdr;
  memcpy(&hdr, buf, sizeof(hdr));
  if (hdr.magic != KAFS_RPC_MAGIC || hdr.version != KAFS_RPC_VERSION)
    return -EPROTONOSUPPORT;
  if (hdr.payload_len > KAFS_RPC_MAX_PAYLOAD)
    return -EMSGSIZE;
  if (sizeof(hdr) + hdr.payload_len != size)
    return -EBADMSG;

  const unsigned char *payload = buf + sizeof(hdr);
  unsigned char resp_payload[KAFS_RPC_MAX_PAYLOAD];
  uint32_t resp_len = 0;
  int32_t result = 0;

  switch (hdr.op)
  {
  case KAFS_RPC_OP_CTL_STATUS:
  case KAFS_RPC_OP_CTL_COMPAT:
    kafs_ctl_write_status_response(ctx, resp_payload, &resp_len);
    break;
  case KAFS_RPC_OP_CTL_RESTART:
    result = kafs_hotplug_restart_back(ctx);
    break;
  case KAFS_RPC_OP_CTL_SET_TIMEOUT:
    result = kafs_ctl_handle_set_timeout(ctx, hdr.payload_len, payload);
    break;
  case KAFS_RPC_OP_CTL_ENV_LIST:
    kafs_ctl_write_env_list_response(ctx, resp_payload, &resp_len);
    break;
  case KAFS_RPC_OP_CTL_ENV_SET:
    result = kafs_ctl_handle_env_update(ctx, hdr.payload_len, payload, 0);
    break;
  case KAFS_RPC_OP_CTL_ENV_UNSET:
    result = kafs_ctl_handle_env_update(ctx, hdr.payload_len, payload, 1);
    break;
  case KAFS_RPC_OP_CTL_SET_DEDUP_PRIO:
    result = kafs_ctl_handle_set_dedup_prio(ctx, hdr.payload_len, payload);
    break;
  case KAFS_RPC_OP_CTL_SET_RUNTIME:
    result = kafs_ctl_handle_set_runtime(ctx, hdr.payload_len, payload);
    break;
  default:
    result = -ENOSYS;
    break;
  }

  kafs_rpc_resp_hdr_t rhdr;
  rhdr.req_id = hdr.req_id;
  rhdr.result = result;
  rhdr.payload_len = resp_len;
  if (sizeof(rhdr) + resp_len > sizeof(sess->resp))
    return -EMSGSIZE;
  memcpy(sess->resp, &rhdr, sizeof(rhdr));
  if (resp_len != 0)
    memcpy(sess->resp + sizeof(rhdr), resp_payload, resp_len);
  sess->resp_len = sizeof(rhdr) + resp_len;
  return (int)size;
}

#ifdef __linux__
static int kafs_ioctl_handle_ficlone(struct fuse_context *fctx, kafs_context_t *ctx,
                                     const char *path, int cmd, void *arg,
                                     struct fuse_file_info *fi, void *data)
{
  int srcfd = -1;
  if (data)
  {
    if (_IOC_SIZE((unsigned int)cmd) < sizeof(int))
      return -EINVAL;
    srcfd = *(int *)data;
  }
  else
  {
    srcfd = (int)(uintptr_t)arg;
  }

  char sp[PATH_MAX];
  int prc = kafs_procfd_to_kafs_path(ctx, fctx->pid, srcfd, sp);
  if (prc != 0)
    return prc;

  kafs_sinode_t *ino_src;
  kafs_sinode_t *ino_dst;
  KAFS_CALL(kafs_access, fctx, ctx, sp, NULL, R_OK, &ino_src);
  KAFS_CALL(kafs_access, fctx, ctx, path, fi, W_OK, &ino_dst);
  return kafs_reflink_clone(ctx, ino_src, ino_dst);
}
#endif

static int kafs_ioctl_lookup_copy_dst(struct fuse_context *fctx, kafs_context_t *ctx,
                                      const char *dst, kafs_sinode_t **ino_dst)
{
  int drc = kafs_access(fctx, ctx, dst, NULL, F_OK, ino_dst);
  if (drc == -ENOENT)
  {
    kafs_inocnt_t ino_new;
    KAFS_CALL(kafs_create, dst, 0644 | S_IFREG, 0, NULL, &ino_new);
    *ino_dst = kafs_ctx_inode(ctx, ino_new);
    return 0;
  }
  if (drc < 0)
    return drc;

  KAFS_CALL(kafs_access, fctx, ctx, dst, NULL, W_OK, ino_dst);
  return 0;
}

static int kafs_ioctl_handle_copy(struct fuse_context *fctx, kafs_context_t *ctx, void *arg,
                                  void *data)
{
  void *buf = data ? data : arg;
  if (!buf)
    return -EINVAL;
  if (_IOC_SIZE((unsigned int)KAFS_IOCTL_COPY) < sizeof(kafs_ioctl_copy_t))
    return -EINVAL;

  kafs_ioctl_copy_t req;
  memcpy(&req, buf, sizeof(req));
  if (req.struct_size < sizeof(req))
    return -EINVAL;
  if (req.src[0] != '/' || req.dst[0] != '/' || req.src[1] == '\0' || req.dst[1] == '\0')
    return -EINVAL;

  kafs_sinode_t *ino_src;
  kafs_sinode_t *ino_dst;
  KAFS_CALL(kafs_access, fctx, ctx, req.src, NULL, R_OK, &ino_src);
  int rc = kafs_ioctl_lookup_copy_dst(fctx, ctx, req.dst, &ino_dst);
  if (rc < 0)
    return rc;

  if ((req.flags & KAFS_IOCTL_COPY_F_REFLINK) != 0)
    return kafs_reflink_clone(ctx, ino_src, ino_dst);

  uint32_t ino_dst_no = (uint32_t)kafs_ctx_ino_no(ctx, ino_dst);
  kafs_inode_lock(ctx, ino_dst_no);
  int trc = kafs_truncate(ctx, ino_dst, 0);
  kafs_inode_unlock(ctx, ino_dst_no);
  if (trc < 0)
    return trc;

  kafs_off_t src_size = kafs_ino_size_get(ino_src);
  ssize_t copied = kafs_copy_regular_range(ctx, ino_src, ino_dst, 0, 0, (size_t)src_size);
  return copied < 0 ? (int)copied : 0;
}

static int kafs_ioctl_handle_get_stats(kafs_context_t *ctx, int cmd, void *arg, void *data)
{
  void *buf = data ? data : arg;
  if (!buf)
    return -EINVAL;
  if (_IOC_SIZE((unsigned int)cmd) < sizeof(kafs_stats_t))
    return -EINVAL;

  kafs_stats_t req;
  memset(&req, 0, sizeof(req));
  memcpy(&req, buf, sizeof(req));

  kafs_stats_t out;
  kafs_stats_snapshot(ctx, &out, req.request_flags);
  memcpy(buf, &out, sizeof(out));
  return 0;
}

static int kafs_op_ioctl(const char *path, int cmd, void *arg, struct fuse_file_info *fi,
                         unsigned int flags, void *data)
{
  (void)flags;

  struct fuse_context *fctx = fuse_get_context();
  kafs_context_t *ctx = (kafs_context_t *)fctx->private_data;

#ifdef __linux__
  if ((unsigned int)cmd == (unsigned int)FICLONE)
    return kafs_ioctl_handle_ficlone(fctx, ctx, path, cmd, arg, fi, data);
  if ((unsigned int)cmd == (unsigned int)FICLONERANGE)
    return -EOPNOTSUPP;
#endif

  if ((unsigned int)cmd == (unsigned int)KAFS_IOCTL_COPY)
    return kafs_ioctl_handle_copy(fctx, ctx, arg, data);

  if ((unsigned int)cmd == (unsigned int)KAFS_IOCTL_GET_STATS)
    return kafs_ioctl_handle_get_stats(ctx, cmd, arg, data);
  return -ENOTTY;
}

static ssize_t kafs_op_copy_file_range(const char *path_in, struct fuse_file_info *fi_in,
                                       off_t offset_in, const char *path_out,
                                       struct fuse_file_info *fi_out, off_t offset_out, size_t size,
                                       int flags)
{
  struct fuse_context *fctx = fuse_get_context();
  kafs_context_t *ctx = (kafs_context_t *)fctx->private_data;

  kafs_sinode_t *ino_in = NULL;
  kafs_sinode_t *ino_out = NULL;
  if (fi_in)
    ino_in = kafs_ctx_inode(ctx, fi_in->fh);
  else
    KAFS_CALL(kafs_access, fctx, ctx, path_in, NULL, R_OK, &ino_in);
  if (fi_out)
    ino_out = kafs_ctx_inode(ctx, fi_out->fh);
  else
    KAFS_CALL(kafs_access, fctx, ctx, path_out, NULL, W_OK, &ino_out);

  // Kernel may route ioctl(FICLONE) through copy_file_range with internal flags.
  if (flags != 0)
  {
    kafs_off_t src_size = kafs_ino_size_get(ino_in);
    if (offset_in != 0 || offset_out != 0 || size < (size_t)src_size)
      return -EOPNOTSUPP;
    int rc = kafs_reflink_clone(ctx, ino_in, ino_out);
    return rc < 0 ? rc : (ssize_t)src_size;
  }

  // Regular copy_file_range(2)
  if (size == 0)
    return 0;
  return kafs_copy_regular_range(ctx, ino_in, ino_out, offset_in, offset_out, size);
}

#undef KAFS_STATS_VERSION

static int kafs_op_open(const char *path, struct fuse_file_info *fi)
{
  struct fuse_context *fctx = fuse_get_context();
  struct kafs_context *ctx = fctx->private_data;
  if (kafs_is_ctl_path(path))
  {
    int accmode = fi->flags & O_ACCMODE;
    if (accmode != O_RDWR)
      return -EACCES;
    kafs_ctl_session_t *sess = (kafs_ctl_session_t *)calloc(1, sizeof(*sess));
    if (!sess)
      return -ENOMEM;
    fi->fh = (uint64_t)(uintptr_t)sess;
    fi->direct_io = 1;
    return 0;
  }
  int ok = 0;
  int accmode = fi->flags & O_ACCMODE;
  if (accmode == O_RDONLY || accmode == O_RDWR)
    ok |= R_OK;
  if (accmode == O_WRONLY || accmode == O_RDWR)
    ok |= W_OK;
  kafs_sinode_t *inoent;
  KAFS_CALL(kafs_access, fctx, ctx, path, NULL, ok, &inoent);
  fi->fh = kafs_ctx_ino_no(ctx, inoent);
  if (ctx->c_open_cnt)
    __atomic_add_fetch(&ctx->c_open_cnt[fi->fh], 1u, __ATOMIC_RELAXED);
  // Handle O_TRUNC on open for existing files to match POSIX semantics
  if ((fi->flags & O_TRUNC) && (accmode == O_WRONLY || accmode == O_RDWR))
  {
    kafs_inode_lock(ctx, (uint32_t)fi->fh);
    (void)kafs_truncate(ctx, kafs_ctx_inode(ctx, fi->fh), 0);
    kafs_inode_unlock(ctx, (uint32_t)fi->fh);
  }
  return 0;
}

static int kafs_op_opendir(const char *path, struct fuse_file_info *fi)
{
  struct fuse_context *fctx = fuse_get_context();
  struct kafs_context *ctx = fctx->private_data;
  kafs_sinode_t *inoent;
  KAFS_CALL(kafs_access, fctx, ctx, path, NULL, R_OK, &inoent);
  fi->fh = kafs_ctx_ino_no(ctx, inoent);
  return 0;
}

static int kafs_op_readdir(const char *path, void *buf, fuse_fill_dir_t filler, off_t offset,
                           struct fuse_file_info *fi, enum fuse_readdir_flags flags)
{
  (void)offset;
  (void)flags;
  struct fuse_context *fctx = fuse_get_context();
  struct kafs_context *ctx = fctx->private_data;
  kafs_sinode_t *inoent_dir;
  KAFS_CALL(kafs_access, fctx, ctx, path, fi, R_OK, &inoent_dir);
  uint32_t ino_dir = (uint32_t)kafs_ctx_ino_no(ctx, inoent_dir);

  kafs_inode_lock(ctx, ino_dir);
  char *snap = NULL;
  size_t snap_len = 0;
  int rc = kafs_dir_snapshot(ctx, inoent_dir, &snap, &snap_len);
  kafs_inode_unlock(ctx, ino_dir);
  if (rc < 0)
    return rc;
  if (filler(buf, ".", NULL, 0, 0))
  {
    free(snap);
    return -ENOENT;
  }
  kafs_dir_snapshot_meta_t meta;
  __atomic_add_fetch(&ctx->c_stat_dir_snapshot_meta_load_calls, 1u, __ATOMIC_RELAXED);
  rc = kafs_dir_snapshot_meta_load(snap, snap_len, &meta);
  if (rc < 0)
  {
    free(snap);
    return rc;
  }
  size_t o = 0;
  while (1)
  {
    kafs_dirent_view_t view;
    __atomic_add_fetch(&ctx->c_stat_dirent_view_next_calls, 1u, __ATOMIC_RELAXED);
    int step = kafs_dirent_view_next_meta(snap, &meta, o, &view);
    if (step == 0)
      break;
    if (step < 0)
    {
      free(snap);
      return -EIO;
    }
    if ((view.flags & KAFS_DIRENT_FLAG_TOMBSTONE) != 0)
    {
      o = view.record_off + view.record_len;
      continue;
    }
    char name[FILENAME_MAX];
    memcpy(name, view.name, view.name_len);
    name[view.name_len] = '\0';
    if (filler(buf, name, NULL, 0, 0))
    {
      free(snap);
      return -ENOENT;
    }
    o = view.record_off + view.record_len;
  }
  free(snap);
  return 0;
}

static void kafs_create_split_path(char *path_copy, const char **dirpath_out, char **basepath_out)
{
  const char *dirpath = path_copy;
  char *basepath = strrchr(path_copy, '/');
  if (dirpath == basepath)
    dirpath = "/";
  *basepath = '\0';
  ++basepath;
  *dirpath_out = dirpath;
  *basepath_out = basepath;
}

static int kafs_create_ensure_absent(struct fuse_context *fctx, struct kafs_context *ctx,
                                     const char *path, uint64_t jseq)
{
  int ret = kafs_access(fctx, ctx, path, NULL, F_OK, NULL);
  kafs_dlog(2, "%s: access(path) rc=%d\n", __func__, ret);
  if (ret == KAFS_SUCCESS)
  {
    kafs_journal_abort(ctx, jseq, "EEXIST");
    return -EEXIST;
  }
  if (ret != -ENOENT)
  {
    kafs_journal_abort(ctx, jseq, "access=%d", ret);
    return ret;
  }
  return 0;
}

static int kafs_create_resolve_parent(struct fuse_context *fctx, struct kafs_context *ctx,
                                      const char *dirpath, uint64_t jseq,
                                      kafs_sinode_t **inoent_dir_out)
{
  kafs_sinode_t *inoent_dir = NULL;
  int ret = kafs_access(fctx, ctx, dirpath, NULL, W_OK, &inoent_dir);
  kafs_dlog(2, "%s: access(dirpath='%s') rc=%d ino_dir=%u\n", __func__, dirpath, ret,
            (unsigned)(inoent_dir ? kafs_ctx_ino_no(ctx, inoent_dir) : 0));
  if (ret < 0)
  {
    kafs_journal_abort(ctx, jseq, "parent access=%d", ret);
    return ret;
  }
  if (!S_ISDIR(kafs_ino_mode_get(inoent_dir)))
  {
    kafs_journal_abort(ctx, jseq, "ENOTDIR");
    return -ENOTDIR;
  }
  *inoent_dir_out = inoent_dir;
  return 0;
}

static int kafs_create_allocate_inode(struct fuse_context *fctx, struct kafs_context *ctx,
                                      kafs_mode_t mode, kafs_dev_t dev, uint64_t jseq,
                                      kafs_inocnt_t *ino_new_out,
                                      struct kafs_sinode **inoent_new_out)
{
  kafs_inocnt_t ino_new;
  kafs_inode_alloc_lock(ctx);
  int ret = kafs_ino_find_free(ctx->c_inotbl, kafs_ctx_inode_format(ctx), &ino_new,
                               &ctx->c_ino_search, kafs_sb_inocnt_get(ctx->c_superblock));
  if (ret < 0)
  {
    kafs_inode_alloc_unlock(ctx);
    kafs_journal_abort(ctx, jseq, "ino_find_free=%d", ret);
    return ret;
  }

  kafs_dlog(2, "%s: alloc ino=%u\n", __func__, (unsigned)ino_new);
  struct kafs_sinode *inoent_new = kafs_ctx_inode(ctx, ino_new);

  kafs_ctx_inode_zero(ctx, inoent_new);
  kafs_ino_mode_set(inoent_new, mode);
  kafs_ino_uid_set(inoent_new, fctx->uid);
  kafs_ino_size_set(inoent_new, 0);
  kafs_time_t now = kafs_now();
  kafs_time_t nulltime = {0, 0};
  kafs_ino_atime_set(inoent_new, now);
  kafs_ino_ctime_set(inoent_new, now);
  kafs_ino_mtime_set(inoent_new, now);
  kafs_ino_dtime_set(inoent_new, nulltime);
  kafs_ino_gid_set(inoent_new, fctx->gid);
  kafs_ino_linkcnt_set(inoent_new, 0);
  kafs_ino_blocks_set(inoent_new, 0);
  kafs_ino_dev_set(inoent_new, dev);
  memset(inoent_new->i_blkreftbl, 0, sizeof(inoent_new->i_blkreftbl));
  if (S_ISDIR(mode))
  {
    kafs_sdir_v4_hdr_t dir_hdr;
    kafs_dir_v4_hdr_init(&dir_hdr);
    memcpy(inoent_new->i_blkreftbl, &dir_hdr, sizeof(dir_hdr));
    kafs_ino_size_set(inoent_new, (kafs_off_t)sizeof(dir_hdr));
  }

  kafs_inode_alloc_unlock(ctx);
  *ino_new_out = ino_new;
  *inoent_new_out = inoent_new;
  return 0;
}

static void kafs_create_lock_inodes(struct kafs_context *ctx, uint32_t ino_dir_u32,
                                    uint32_t ino_new_u32)
{
  if (ino_dir_u32 < ino_new_u32)
  {
    kafs_inode_lock(ctx, ino_dir_u32);
    kafs_inode_lock(ctx, ino_new_u32);
    return;
  }

  kafs_inode_lock(ctx, ino_new_u32);
  if (ino_dir_u32 != ino_new_u32)
    kafs_inode_lock(ctx, ino_dir_u32);
}

static void kafs_create_unlock_inodes(struct kafs_context *ctx, uint32_t ino_dir_u32,
                                      uint32_t ino_new_u32)
{
  if (ino_dir_u32 < ino_new_u32)
  {
    kafs_inode_unlock(ctx, ino_new_u32);
    kafs_inode_unlock(ctx, ino_dir_u32);
    return;
  }

  if (ino_dir_u32 != ino_new_u32)
    kafs_inode_unlock(ctx, ino_dir_u32);
  kafs_inode_unlock(ctx, ino_new_u32);
}

static int kafs_create(const char *path, kafs_mode_t mode, kafs_dev_t dev, kafs_inocnt_t *pino_dir,
                       kafs_inocnt_t *pino_new)
{
  assert(path != NULL);
  assert(path[0] == '/');
  assert(path[1] != '\0');
  struct fuse_context *fctx = fuse_get_context();
  struct kafs_context *ctx = fctx->private_data;
  char path_copy[strlen(path) + 1];
  strcpy(path_copy, path);
  const char *dirpath = NULL;
  char *basepath = NULL;
  kafs_create_split_path(path_copy, &dirpath, &basepath);

  uint64_t jseq = kafs_journal_begin(ctx, "CREATE", "path=%s mode=%o", path, (unsigned)mode);
  kafs_dlog(2, "%s: dirpath='%s' base='%s'\n", __func__, dirpath, basepath);
  int ret = kafs_create_ensure_absent(fctx, ctx, path, jseq);
  if (ret < 0)
    return ret;

  kafs_sinode_t *inoent_dir = NULL;
  ret = kafs_create_resolve_parent(fctx, ctx, dirpath, jseq, &inoent_dir);
  if (ret < 0)
    return ret;

  kafs_inocnt_t ino_new;
  struct kafs_sinode *inoent_new = NULL;
  ret = kafs_create_allocate_inode(fctx, ctx, mode, dev, jseq, &ino_new, &inoent_new);
  if (ret < 0)
    return ret;

  uint32_t ino_dir_u32 = (uint32_t)kafs_ctx_ino_no(ctx, inoent_dir);
  uint32_t ino_new_u32 = (uint32_t)ino_new;
  kafs_create_lock_inodes(ctx, ino_dir_u32, ino_new_u32);

  kafs_dlog(2, "%s: dirent_add start dir=%u name='%s'\n", __func__, (unsigned)ino_dir_u32,
            basepath);
  ret = kafs_dirent_add(ctx, inoent_dir, ino_new, basepath);
  kafs_dlog(2, "%s: dirent_add done rc=%d\n", __func__, ret);
  if (ret < 0)
  {
    kafs_ctx_inode_zero(ctx, inoent_new);
    kafs_create_unlock_inodes(ctx, ino_dir_u32, ino_new_u32);
    kafs_journal_abort(ctx, jseq, "dirent_add=%d", ret);
    return ret;
  }

  if (pino_dir != NULL)
    *pino_dir = kafs_ctx_ino_no(ctx, inoent_dir);
  if (pino_new != NULL)
    *pino_new = ino_new;

  kafs_create_unlock_inodes(ctx, ino_dir_u32, ino_new_u32);

  kafs_inode_alloc_lock(ctx);
  (void)kafs_sb_inocnt_free_decr(ctx->c_superblock);
  kafs_sb_wtime_set(ctx->c_superblock, kafs_now());
  kafs_inode_alloc_unlock(ctx);

  kafs_dlog(2, "%s: success ino=%u added to dir ino=%u\n", __func__, (unsigned)ino_new,
            (unsigned)(pino_dir ? *pino_dir : 0));
  kafs_diag_note_create_event(ctx, ino_new, path, mode);
  kafs_journal_commit(ctx, jseq);
  return KAFS_SUCCESS;
}

static int kafs_op_create(const char *path, mode_t mode, struct fuse_file_info *fi)
{
  struct fuse_context *fctx = fuse_get_context();
  struct kafs_context *ctx = fctx ? fctx->private_data : NULL;
  if (kafs_is_ctl_path(path))
    return -EACCES;
  kafs_inocnt_t ino_new;
  KAFS_CALL(kafs_create, path, mode | S_IFREG, 0, NULL, &ino_new);
  if (ctx)
  {
    kafs_mode_t created_mode = kafs_ino_mode_get(kafs_ctx_inode(ctx, ino_new));
    if (!S_ISREG(created_mode))
    {
      kafs_log(KAFS_LOG_ERR,
               "%s: create type mismatch path=%s ino=%" PRIuFAST32 " mode=%o expected=regular\n",
               __func__, path ? path : "(null)", (uint_fast32_t)ino_new, (unsigned)created_mode);
      return -EIO;
    }
  }
  fi->fh = ino_new;
  if (ctx && ctx->c_open_cnt)
    __atomic_add_fetch(&ctx->c_open_cnt[ino_new], 1u, __ATOMIC_RELAXED);
  return 0;
}

static int kafs_op_mknod(const char *path, mode_t mode, dev_t dev)
{
  if (kafs_is_ctl_path(path))
    return -EACCES;
  KAFS_CALL(kafs_create, path, mode, dev, NULL, NULL);
  return 0;
}

static int kafs_op_truncate(const char *path, off_t size, struct fuse_file_info *fi)
{
  struct fuse_context *fctx = fuse_get_context();
  struct kafs_context *ctx = fctx->private_data;
  if (kafs_is_ctl_path(path))
    return -EACCES;
  kafs_sinode_t *inoent;
  KAFS_CALL(kafs_access, fctx, ctx, path, fi, F_OK, &inoent);
  uint32_t ino = (uint32_t)kafs_ctx_ino_no(ctx, inoent);
  kafs_dlog(2, "%s: enter path=%s ino=%" PRIuFAST32 " size=%" PRIuFAST64 "\n", __func__,
            path ? path : "(null)", ino, (uint64_t)size);
  int rc_hp = kafs_hotplug_call_truncate(fctx, ctx, inoent, size);
  if (rc_hp == 0)
  {
    kafs_dlog(2, "%s: exit rc=0 path=%s ino=%" PRIuFAST32 " size=%" PRIuFAST64 " hotplug=1\n",
              __func__, path ? path : "(null)", ino, (uint64_t)size);
    return 0;
  }
  if (!kafs_hotplug_should_fallback(rc_hp))
  {
    kafs_dlog(2, "%s: exit rc=%d path=%s ino=%" PRIuFAST32 " size=%" PRIuFAST64 " hotplug=err\n",
              __func__, rc_hp, path ? path : "(null)", ino, (uint64_t)size);
    return rc_hp;
  }
  uint32_t inode_no = kafs_ctx_ino_no(ctx, inoent);
  kafs_inode_lock(ctx, inode_no);
  int rc = kafs_truncate(ctx, inoent, (kafs_off_t)size);
  kafs_inode_unlock(ctx, inode_no);
  kafs_dlog(2, "%s: exit rc=%d path=%s ino=%" PRIuFAST32 " size=%" PRIuFAST64 " hotplug=fallback\n",
            __func__, (rc == 0 ? 0 : rc), path ? path : "(null)", ino, (uint64_t)size);
  return rc == 0 ? 0 : rc;
}

static int kafs_fallocate_expand_locked(struct kafs_context *ctx, kafs_sinode_t *inoent,
                                        uint32_t ino, kafs_off_t end_req)
{
  kafs_inode_lock(ctx, ino);
  kafs_off_t filesize = kafs_ino_size_get(inoent);
  int rc = 0;
  if (end_req > filesize)
    rc = kafs_truncate(ctx, inoent, end_req);
  kafs_inode_unlock(ctx, ino);
  return rc;
}

static int kafs_fallocate_zero_left_edge(struct kafs_context *ctx, kafs_sinode_t *inoent,
                                         kafs_off_t offset, kafs_off_t end, kafs_blksize_t blksize,
                                         kafs_logblksize_t log_blksize)
{
  if ((offset & ((kafs_off_t)blksize - 1)) == 0)
    return 0;

  kafs_iblkcnt_t iblo = (kafs_iblkcnt_t)(offset >> log_blksize);
  char wbuf[blksize];
  int rc = kafs_ino_iblk_read_or_zero(ctx, inoent, iblo, wbuf);
  if (rc < 0)
    return rc;

  kafs_blksize_t start = (kafs_blksize_t)(offset & ((kafs_off_t)blksize - 1));
  kafs_blksize_t stop = blksize;
  if ((kafs_off_t)(iblo + 1) << log_blksize > end)
    stop = (kafs_blksize_t)(end & ((kafs_off_t)blksize - 1));
  if (stop <= start)
    return 0;

  memset(wbuf + start, 0, stop - start);
  return kafs_ino_iblk_write(ctx, inoent, iblo, wbuf);
}

static int kafs_fallocate_release_full_blocks(struct kafs_context *ctx, kafs_sinode_t *inoent,
                                              kafs_off_t filesize, kafs_iblkcnt_t first_full,
                                              kafs_iblkcnt_t last_full_excl)
{
  if (filesize <= KAFS_INODE_DIRECT_BYTES)
    return 0;

  for (kafs_iblkcnt_t iblo = first_full; iblo < last_full_excl; ++iblo)
  {
    int rc = kafs_ino_iblk_release(ctx, inoent, iblo);
    if (rc < 0)
      return rc;
  }
  return 0;
}

static int kafs_fallocate_zero_right_edge(struct kafs_context *ctx, kafs_sinode_t *inoent,
                                          kafs_off_t offset, kafs_off_t end, kafs_off_t filesize,
                                          kafs_blksize_t blksize, kafs_logblksize_t log_blksize)
{
  if ((end & ((kafs_off_t)blksize - 1)) == 0)
    return 0;

  kafs_iblkcnt_t iblo = (kafs_iblkcnt_t)(end >> log_blksize);
  if ((offset >> log_blksize) == (kafs_off_t)iblo && (offset & ((kafs_off_t)blksize - 1)) != 0)
    return 0;

  kafs_blksize_t stop = (kafs_blksize_t)(end & ((kafs_off_t)blksize - 1));
  if (end == filesize)
  {
    kafs_blksize_t valid = (kafs_blksize_t)(filesize & ((kafs_off_t)blksize - 1));
    if (valid == 0)
      valid = blksize;
    if (stop == valid && filesize > KAFS_INODE_DIRECT_BYTES)
      return kafs_ino_iblk_release(ctx, inoent, iblo);
  }

  char wbuf[blksize];
  int rc = kafs_ino_iblk_read_or_zero(ctx, inoent, iblo, wbuf);
  if (rc < 0)
    return rc;
  memset(wbuf, 0, stop);
  return kafs_ino_iblk_write(ctx, inoent, iblo, wbuf);
}

static int kafs_op_fallocate(const char *path, int mode, off_t offset, off_t length,
                             struct fuse_file_info *fi)
{
  struct fuse_context *fctx = fuse_get_context();
  struct kafs_context *ctx = fctx->private_data;
  if (kafs_is_ctl_path(path))
    return -EACCES;
  if (offset < 0 || length < 0)
    return -EINVAL;
  if (length == 0)
    return 0;

  kafs_off_t end_req = (kafs_off_t)offset + (kafs_off_t)length;
  if (end_req < (kafs_off_t)offset)
    return -EOVERFLOW;

#ifdef FALLOC_FL_PUNCH_HOLE
  const int supported = FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE;
  if ((mode & ~supported) != 0)
    return -EOPNOTSUPP;
#else
  (void)mode;
  return -EOPNOTSUPP;
#endif

  kafs_sinode_t *inoent;
  KAFS_CALL(kafs_access, fctx, ctx, path, fi, F_OK, &inoent);
  uint32_t ino = kafs_ctx_ino_no(ctx, inoent);

#ifdef FALLOC_FL_PUNCH_HOLE
  if ((mode & FALLOC_FL_PUNCH_HOLE) == 0)
  {
    // Expansion path treats the region past EOF as sparse until data is written.
    if ((mode & FALLOC_FL_KEEP_SIZE) != 0)
      return 0;

    return kafs_fallocate_expand_locked(ctx, inoent, ino, end_req);
  }

  if ((mode & FALLOC_FL_KEEP_SIZE) == 0)
    return -EOPNOTSUPP;
#endif

  kafs_inode_lock(ctx, ino);
  int rc = 0;
  kafs_off_t filesize = kafs_ino_size_get(inoent);
  if ((kafs_off_t)offset >= filesize)
  {
    kafs_inode_unlock(ctx, ino);
    return 0;
  }

  kafs_off_t end = end_req;
  if (end > filesize)
    end = filesize;

  kafs_blksize_t blksize = kafs_sb_blksize_get(ctx->c_superblock);
  kafs_logblksize_t log_blksize = kafs_sb_log_blksize_get(ctx->c_superblock);
  rc = kafs_fallocate_zero_left_edge(ctx, inoent, (kafs_off_t)offset, end, blksize, log_blksize);
  if (rc < 0)
    goto fallocate_unlock;

  kafs_iblkcnt_t first_full = (kafs_iblkcnt_t)(((kafs_off_t)offset + blksize - 1) >> log_blksize);
  kafs_iblkcnt_t last_full_excl = (kafs_iblkcnt_t)(end >> log_blksize);
  rc = kafs_fallocate_release_full_blocks(ctx, inoent, filesize, first_full, last_full_excl);
  if (rc < 0)
    goto fallocate_unlock;

  rc = kafs_fallocate_zero_right_edge(ctx, inoent, (kafs_off_t)offset, end, filesize, blksize,
                                      log_blksize);

fallocate_unlock:
  kafs_inode_unlock(ctx, ino);
  return rc;
}

static off_t kafs_lseek_find_data_locked(kafs_context_t *ctx, kafs_sinode_t *inoent, uint32_t ino,
                                         off_t off, kafs_logblksize_t log_blksize,
                                         kafs_iblkcnt_t start_iblo, kafs_iblkcnt_t end_iblo)
{
  for (kafs_iblkcnt_t iblo = start_iblo; iblo < end_iblo; ++iblo)
  {
    kafs_blkcnt_t blo = KAFS_BLO_NONE;
    int rc = kafs_ino_ibrk_run(ctx, inoent, iblo, &blo, KAFS_IBLKREF_FUNC_GET);
    if (rc < 0)
    {
      kafs_inode_unlock(ctx, ino);
      return rc;
    }
    if (blo == KAFS_BLO_NONE)
      continue;

    kafs_off_t pos = ((kafs_off_t)iblo << log_blksize);
    if (pos < (kafs_off_t)off)
      pos = (kafs_off_t)off;
    kafs_inode_unlock(ctx, ino);
    return (off_t)pos;
  }

  kafs_inode_unlock(ctx, ino);
  return -ENXIO;
}

static off_t kafs_lseek_find_hole_locked(kafs_context_t *ctx, kafs_sinode_t *inoent, uint32_t ino,
                                         off_t off, kafs_off_t size, kafs_logblksize_t log_blksize,
                                         kafs_iblkcnt_t start_iblo, kafs_iblkcnt_t end_iblo)
{
  for (kafs_iblkcnt_t iblo = start_iblo; iblo < end_iblo; ++iblo)
  {
    kafs_blkcnt_t blo = KAFS_BLO_NONE;
    int rc = kafs_ino_ibrk_run(ctx, inoent, iblo, &blo, KAFS_IBLKREF_FUNC_GET);
    if (rc < 0)
    {
      kafs_inode_unlock(ctx, ino);
      return rc;
    }
    if (blo != KAFS_BLO_NONE)
      continue;

    kafs_off_t pos = ((kafs_off_t)iblo << log_blksize);
    if (pos < (kafs_off_t)off)
      pos = (kafs_off_t)off;
    if (pos > size)
      pos = size;
    kafs_inode_unlock(ctx, ino);
    return (off_t)pos;
  }

  kafs_inode_unlock(ctx, ino);
  return (off_t)size;
}

static off_t kafs_op_lseek(const char *path, off_t off, int whence, struct fuse_file_info *fi)
{
  struct fuse_context *fctx = fuse_get_context();
  struct kafs_context *ctx = fctx->private_data;
  if (kafs_is_ctl_path(path))
    return -EACCES;
  if (off < 0)
    return -EINVAL;
  if (whence != SEEK_DATA && whence != SEEK_HOLE)
    return -EINVAL;

  kafs_sinode_t *inoent;
  KAFS_CALL(kafs_access, fctx, ctx, path, fi, F_OK, &inoent);
  uint32_t ino = kafs_ctx_ino_no(ctx, inoent);

  kafs_inode_lock(ctx, ino);
  kafs_off_t size = kafs_ino_size_get(inoent);
  if ((kafs_off_t)off > size)
  {
    kafs_inode_unlock(ctx, ino);
    return -ENXIO;
  }
  if ((kafs_off_t)off == size)
  {
    kafs_inode_unlock(ctx, ino);
    return (whence == SEEK_HOLE) ? off : -ENXIO;
  }

  if (size <= KAFS_INODE_DIRECT_BYTES)
  {
    kafs_inode_unlock(ctx, ino);
    return (whence == SEEK_DATA) ? off : (off_t)size;
  }

  kafs_blksize_t blksize = kafs_sb_blksize_get(ctx->c_superblock);
  kafs_logblksize_t log_blksize = kafs_sb_log_blksize_get(ctx->c_superblock);
  kafs_iblkcnt_t start_iblo = (kafs_iblkcnt_t)((kafs_off_t)off >> log_blksize);
  kafs_iblkcnt_t end_iblo = (kafs_iblkcnt_t)((size + blksize - 1) >> log_blksize);

  if (whence == SEEK_DATA)
    return kafs_lseek_find_data_locked(ctx, inoent, ino, off, log_blksize, start_iblo, end_iblo);
  return kafs_lseek_find_hole_locked(ctx, inoent, ino, off, size, log_blksize, start_iblo,
                                     end_iblo);
}

static int kafs_op_mkdir(const char *path, mode_t mode)
{
  struct fuse_context *fctx = fuse_get_context();
  struct kafs_context *ctx = fctx->private_data;
  if (kafs_is_ctl_path(path))
    return -EACCES;
  uint64_t jseq = kafs_journal_begin(ctx, "MKDIR", "path=%s mode=%o", path, (unsigned)mode);
  kafs_inocnt_t ino_dir;
  kafs_inocnt_t ino_new;
  KAFS_CALL(kafs_create, path, mode | S_IFDIR, 0, &ino_dir, &ino_new);
  kafs_sinode_t *inoent_new = kafs_ctx_inode(ctx, ino_new);
  if (!S_ISDIR(kafs_ino_mode_get(inoent_new)))
  {
    kafs_log(KAFS_LOG_ERR,
             "%s: create type mismatch path=%s ino=%" PRIuFAST32 " mode=%o expected=dir\n",
             __func__, path ? path : "(null)", (uint_fast32_t)ino_new,
             (unsigned)kafs_ino_mode_get(inoent_new));
    kafs_journal_abort(ctx, jseq, "mkdir type mismatch ino=%u mode=%o", (unsigned)ino_new,
                       (unsigned)kafs_ino_mode_get(inoent_new));
    return -EIO;
  }
  kafs_dlog(2, "%s: created ino=%u mode=%o\n", __func__, (unsigned)ino_new,
            (unsigned)kafs_ino_mode_get(inoent_new));
  // Lock parent + new dir in stable order (dirent_add("..") increments parent linkcnt)
  uint32_t ino_parent = (uint32_t)ino_dir;
  uint32_t ino_new_u32 = (uint32_t)ino_new;
  if (ino_parent < ino_new_u32)
  {
    kafs_inode_lock(ctx, ino_parent);
    kafs_inode_lock(ctx, ino_new_u32);
  }
  else
  {
    kafs_inode_lock(ctx, ino_new_u32);
    kafs_inode_lock(ctx, ino_parent);
  }

  int rc = kafs_dirent_add(ctx, inoent_new, ino_dir, "..");
  if (rc < 0)
  {
    if (ino_parent < ino_new_u32)
    {
      kafs_inode_unlock(ctx, ino_new_u32);
      kafs_inode_unlock(ctx, ino_parent);
    }
    else
    {
      kafs_inode_unlock(ctx, ino_parent);
      kafs_inode_unlock(ctx, ino_new_u32);
    }
    kafs_journal_abort(ctx, jseq, "dirent_add=%d", rc);
    return rc;
  }

  // ".." counts as a link for the new directory too.
  kafs_ino_linkcnt_incr(inoent_new);
  if (ino_parent < ino_new_u32)
  {
    kafs_inode_unlock(ctx, ino_new_u32);
    kafs_inode_unlock(ctx, ino_parent);
  }
  else
  {
    kafs_inode_unlock(ctx, ino_parent);
    kafs_inode_unlock(ctx, ino_new_u32);
  }
  kafs_journal_commit(ctx, jseq);
  return 0;
}

static int kafs_op_rmdir(const char *path)
{
  struct fuse_context *fctx = fuse_get_context();
  struct kafs_context *ctx = fctx->private_data;
  uint64_t jseq = kafs_journal_begin(ctx, "RMDIR", "path=%s", path);
  char path_copy[strlen(path) + 1];
  strcpy(path_copy, path);
  const char *dirpath = path_copy;
  char *basepath = strrchr(path_copy, '/');
  if (dirpath == basepath)
    dirpath = "/";
  *basepath = '\0';
  basepath++;

  kafs_sinode_t *inoent;
  KAFS_CALL(kafs_access, fctx, ctx, path, NULL, F_OK, &inoent);
  kafs_mode_t mode = kafs_ino_mode_get(inoent);
  if (!S_ISDIR(mode))
  {
    kafs_journal_abort(ctx, jseq, "ENOTDIR");
    return -ENOTDIR;
  }
  kafs_sinode_t *inoent_dir;
  KAFS_CALL(kafs_access, fctx, ctx, dirpath, NULL, W_OK, &inoent_dir);

  // lock parent then target dir in stable order by inode number to avoid deadlock
  uint32_t ino_parent = kafs_ctx_ino_no(ctx, inoent_dir);
  uint32_t ino_target = kafs_ctx_ino_no(ctx, inoent);
  if (ino_parent < ino_target)
  {
    kafs_inode_lock(ctx, ino_parent);
    kafs_inode_lock(ctx, ino_target);
  }
  else
  {
    kafs_inode_lock(ctx, ino_target);
    kafs_inode_lock(ctx, ino_parent);
  }

  // Verify directory emptiness under lock (TOCTOU-safe).
  int empty_rc = kafs_dir_is_empty_locked(ctx, inoent);
  if (empty_rc < 0)
  {
    kafs_inode_unlock(ctx, ino_target);
    kafs_inode_unlock(ctx, ino_parent);
    kafs_journal_abort(ctx, jseq, "dir_empty=%d", empty_rc);
    return empty_rc;
  }
  if (empty_rc == 0)
  {
    kafs_inode_unlock(ctx, ino_target);
    kafs_inode_unlock(ctx, ino_parent);
    kafs_journal_abort(ctx, jseq, "ENOTEMPTY");
    return -ENOTEMPTY;
  }

  int rc = kafs_dirent_remove(ctx, inoent_dir, basepath);
  if (rc < 0)
  {
    kafs_inode_unlock(ctx, ino_target);
    kafs_inode_unlock(ctx, ino_parent);
    kafs_journal_abort(ctx, jseq, "dirent_remove(parent)=%d", rc);
    return rc;
  }
  rc = kafs_dirent_remove(ctx, inoent, "..");
  if (rc < 0)
  {
    kafs_inode_unlock(ctx, ino_target);
    kafs_inode_unlock(ctx, ino_parent);
    kafs_journal_abort(ctx, jseq, "dirent_remove(dotdot)=%d", rc);
    return rc;
  }

  // Unlock in reverse order of acquisition
  if (ino_parent < ino_target)
  {
    kafs_inode_unlock(ctx, ino_target);
    kafs_inode_unlock(ctx, ino_parent);
  }
  else
  {
    kafs_inode_unlock(ctx, ino_parent);
    kafs_inode_unlock(ctx, ino_target);
  }
  kafs_journal_commit(ctx, jseq);
  return 0;
}

static int kafs_op_readlink(const char *path, char *buf, size_t buflen)
{
  struct fuse_context *fctx = fuse_get_context();
  struct kafs_context *ctx = fctx->private_data;
  kafs_sinode_t *inoent;
  KAFS_CALL(kafs_access, fctx, ctx, path, NULL, F_OK, &inoent);
  uint32_t ino = kafs_ctx_ino_no(ctx, inoent);
  kafs_inode_lock(ctx, ino);
  ssize_t r = kafs_pread(ctx, inoent, buf, buflen - 1, 0);
  if (r < 0)
  {
    kafs_inode_unlock(ctx, ino);
    return (int)r;
  }
  kafs_inode_unlock(ctx, ino);
  buf[r] = '\0';
  return 0;
}

static int kafs_op_read(const char *path, char *buf, size_t size, off_t offset,
                        struct fuse_file_info *fi)
{
  struct fuse_context *fctx = fuse_get_context();
  struct kafs_context *ctx = fctx->private_data;
  if (kafs_is_ctl_path(path))
  {
    kafs_ctl_session_t *sess = (kafs_ctl_session_t *)(uintptr_t)fi->fh;
    if (!sess)
      return -EIO;
    if (offset < 0 || (size_t)offset >= sess->resp_len)
      return 0;
    size_t remain = sess->resp_len - (size_t)offset;
    size_t n = size < remain ? size : remain;
    memcpy(buf, sess->resp + offset, n);
    return (int)n;
  }
  kafs_inocnt_t ino = fi->fh;
  ssize_t rc_hp = kafs_hotplug_call_read(fctx, ctx, ino, buf, size, offset);
  if (rc_hp >= 0)
    return (int)rc_hp;
  if (!kafs_hotplug_should_fallback((int)rc_hp))
    return (int)rc_hp;
  kafs_inode_lock(ctx, (uint32_t)ino);
  ssize_t rr = kafs_pread(ctx, kafs_ctx_inode(ctx, ino), buf, size, offset);
  kafs_inode_unlock(ctx, (uint32_t)ino);
  return rr;
}

static int kafs_op_write_ctl(struct kafs_context *ctx, struct fuse_file_info *fi, const char *buf,
                             size_t size, off_t offset)
{
  kafs_ctl_session_t *sess = (kafs_ctl_session_t *)(uintptr_t)fi->fh;
  if (!sess)
    return -EIO;
  if (offset != 0)
    return -EINVAL;
  if (size > KAFS_CTL_MAX_REQ)
    return -EMSGSIZE;

  int rc = kafs_ctl_handle_request(ctx, sess, (const unsigned char *)buf, size);
  return (rc < 0) ? rc : (int)size;
}

static int kafs_op_write_fallback(struct kafs_context *ctx, const char *path, const char *buf,
                                  size_t size, off_t offset, kafs_inocnt_t ino)
{
  kafs_inode_lock(ctx, (uint32_t)ino);
  kafs_sinode_t *inoent = kafs_ctx_inode(ctx, ino);
  kafs_mode_t mode = kafs_ino_mode_get(inoent);
  if (S_ISDIR(mode))
  {
    kafs_inode_unlock(ctx, (uint32_t)ino);
    kafs_log(KAFS_LOG_ERR,
             "%s: rejecting write to directory path=%s ino=%" PRIuFAST32
             " size=%zu off=%" PRIuFAST64 " mode=%o\n",
             __func__, path ? path : "(null)", (uint32_t)ino, size, (uint64_t)offset,
             (unsigned)mode);
    return -EISDIR;
  }

  kafs_diag_write_scope_t write_scope =
      kafs_diag_write_scope_enter(path ? path : "(null)", (uint32_t)ino);
  int rc_write = kafs_pwrite(ctx, inoent, buf, size, offset);
  kafs_diag_write_scope_leave(write_scope);
  kafs_inode_unlock(ctx, (uint32_t)ino);

  if (rc_write < 0)
  {
    kafs_log(KAFS_LOG_WARNING,
             "%s: pwrite failed path=%s ino=%" PRIuFAST32 " size=%zu off=%" PRIuFAST64 " rc=%d\n",
             __func__, path ? path : "(null)", (uint32_t)ino, size, (uint64_t)offset, rc_write);
  }
  kafs_dlog(2,
            "%s: exit rc=%d path=%s ino=%" PRIuFAST32 " size=%zu off=%" PRIuFAST64
            " hotplug=fallback\n",
            __func__, rc_write, path ? path : "(null)", (uint32_t)ino, size, (uint64_t)offset);
  return rc_write;
}

static int kafs_op_write(const char *path, const char *buf, size_t size, off_t offset,
                         struct fuse_file_info *fi)
{
  kafs_dlog(3, "%s(path=%s, size=%zu, off=%" PRIuFAST64 ")\n", __func__, path ? path : "(null)",
            size, (uint64_t)offset);
  struct fuse_context *fctx = fuse_get_context();
  struct kafs_context *ctx = fctx->private_data;
  if (kafs_is_ctl_path(path))
    return kafs_op_write_ctl(ctx, fi, buf, size, offset);
  kafs_inocnt_t ino = fi->fh;
  if ((fi->flags & O_ACCMODE) == O_RDONLY)
  {
    // Kernel writeback may issue pid=0 WRITE requests even when fi flags look read-only.
    // Keep userspace O_RDONLY protection, but allow kernel-originated writeback writes.
    if (!fctx || fctx->pid != 0)
      return -EACCES;
  }
  ssize_t rc_hp = kafs_hotplug_call_write(fctx, ctx, ino, buf, size, offset);
  if (rc_hp >= 0)
  {
    kafs_dlog(
        2, "%s: exit rc=%zd path=%s ino=%" PRIuFAST32 " size=%zu off=%" PRIuFAST64 " hotplug=1\n",
        __func__, rc_hp, path ? path : "(null)", ino, size, (uint64_t)offset);
    return (int)rc_hp;
  }
  if (!kafs_hotplug_should_fallback((int)rc_hp))
  {
    kafs_log(KAFS_LOG_WARNING,
             "%s: hotplug write failed path=%s ino=%" PRIuFAST32 " size=%zu off=%" PRIuFAST64
             " rc=%zd\n",
             __func__, path ? path : "(null)", ino, size, (uint64_t)offset, rc_hp);
    kafs_dlog(
        2, "%s: exit rc=%zd path=%s ino=%" PRIuFAST32 " size=%zu off=%" PRIuFAST64 " hotplug=err\n",
        __func__, rc_hp, path ? path : "(null)", ino, size, (uint64_t)offset);
    return (int)rc_hp;
  }
  return kafs_op_write_fallback(ctx, path, buf, size, offset, ino);
}

static int kafs_op_utimens(const char *path, const struct timespec tv[2], struct fuse_file_info *fi)
{
  struct fuse_context *fctx = fuse_get_context();
  struct kafs_context *ctx = fctx->private_data;
  if (kafs_is_ctl_path(path))
    return -EACCES;
  uint64_t t0_ns = kafs_now_ns();
  kafs_sinode_t *inoent = NULL;
  uint32_t ino = KAFS_INO_NONE;

  // Fast path: when file handle is available, avoid path traversal and dir snapshot work.
  if (fi)
  {
    kafs_inocnt_t fh_ino = (kafs_inocnt_t)fi->fh;
    if (fh_ino < kafs_sb_inocnt_get(ctx->c_superblock) &&
        kafs_ino_get_usage(kafs_ctx_inode(ctx, fh_ino)))
    {
      ino = (uint32_t)fh_ino;
      inoent = kafs_ctx_inode(ctx, ino);
      KAFS_CALL(kafs_access_check, F_OK, inoent, KAFS_FALSE, fctx->uid, fctx->gid, 0, NULL);
    }
  }

  if (!inoent)
  {
    KAFS_CALL(kafs_access, fctx, ctx, path, fi, F_OK, &inoent);
    ino = (uint32_t)kafs_ctx_ino_no(ctx, inoent);
  }

  kafs_inode_lock(ctx, ino);

  kafs_time_t now = {0, 0};
  if (tv[0].tv_nsec == UTIME_NOW || tv[1].tv_nsec == UTIME_NOW)
    now = kafs_now();
  switch (tv[0].tv_nsec)
  {
  case UTIME_NOW:
    kafs_ino_atime_set(inoent, now);
    break;
  case UTIME_OMIT:
    break;
  default:
    kafs_ino_atime_set(inoent, tv[0]);
    break;
  }
  switch (tv[1].tv_nsec)
  {
  case UTIME_NOW:
    kafs_ino_mtime_set(inoent, now);
    break;
  case UTIME_OMIT:
    break;
  default:
    kafs_ino_mtime_set(inoent, tv[1]);
    break;
  }

  kafs_inode_unlock(ctx, ino);

  uint64_t elapsed_ns = kafs_now_ns() - t0_ns;
  if (elapsed_ns >= 1000000000ull)
  {
    kafs_log(KAFS_LOG_WARNING,
             "%s: slow op path=%s ino=%" PRIuFAST32 " elapsed_ms=%" PRIuFAST64 "\n", __func__,
             path ? path : "(null)", ino, elapsed_ns / 1000000ull);
  }
  return 0;
}

static kafs_linkcnt_t kafs_inode_drop_link_locked(struct kafs_context *ctx, kafs_inocnt_t ino,
                                                  int reclaim_now, int *reclaimed_now)
{
  kafs_sinode_t *inoent = kafs_ctx_inode(ctx, ino);
  kafs_linkcnt_t nl = kafs_ino_linkcnt_get(inoent);

  if (nl > 0)
  {
    nl = kafs_ino_linkcnt_decr(inoent);
  }
  else
  {
    kafs_log(KAFS_LOG_WARNING,
             "%s: ino=%" PRIuFAST32 " already has linkcnt=0; treating as tombstone\n", __func__,
             (uint32_t)ino);
  }

  if (nl == 0)
  {
    if (!kafs_inode_is_tombstone(inoent))
      kafs_ino_dtime_set(inoent, kafs_now());
    if (!reclaim_now)
    {
      int trc = kafs_tailmeta_try_reclaim_tombstone_payload_locked(ctx, ino);
      if (trc < 0)
      {
        kafs_log(KAFS_LOG_WARNING, "%s: early tail reclaim failed ino=%" PRIuFAST32 " rc=%d\n",
                 __func__, (uint32_t)ino, trc);
      }
    }
    if (reclaim_now)
      (void)kafs_try_reclaim_unlinked_inode_locked(ctx, ino, reclaimed_now);
  }

  return nl;
}

static int kafs_op_unlink(const char *path)
{
  assert(path != NULL);
  assert(path[0] == '/');
  assert(path[1] != '\0');
  if (kafs_is_ctl_path(path))
    return -EACCES;
  struct fuse_context *fctx = fuse_get_context();
  struct kafs_context *ctx = fctx->private_data;
  uint64_t jseq = kafs_journal_begin(ctx, "UNLINK", "path=%s", path);
  char path_copy[strlen(path) + 1];
  strcpy(path_copy, path);
  const char *dirpath = path_copy;
  char *basepath = strrchr(path_copy, '/');
  if (dirpath == basepath)
    dirpath = "/";
  *basepath = '\0';
  basepath++;

  if (strcmp(basepath, ".") == 0 || strcmp(basepath, "..") == 0)
  {
    kafs_journal_abort(ctx, jseq, "EINVAL");
    return -EINVAL;
  }

  kafs_sinode_t *inoent_dir;
  // Requests issued from kernel/internal context (pid==0) may not carry caller uid/gid
  // suitable for W_OK checks. For those internal requests, parent existence is sufficient.
  int need_mode = (fctx && fctx->pid == 0) ? F_OK : W_OK;
  int arc = kafs_access(fctx, ctx, dirpath, NULL, need_mode, &inoent_dir);
  if (arc < 0)
  {
    kafs_journal_abort(ctx, jseq, "parent access=%d", arc);
    return arc;
  }
  uint32_t ino_dir = kafs_ctx_ino_no(ctx, inoent_dir);

  kafs_inocnt_t target_ino = KAFS_INO_NONE;
  kafs_sinode_t *inoent_target = NULL;

  kafs_inode_lock(ctx, ino_dir);
  int s = kafs_dirent_search(ctx, inoent_dir, basepath, (kafs_filenamelen_t)strlen(basepath),
                             &inoent_target);
  if (s < 0)
  {
    kafs_inode_unlock(ctx, ino_dir);
    kafs_journal_abort(ctx, jseq, "ENOENT");
    return s;
  }
  target_ino = kafs_ctx_ino_no(ctx, inoent_target);
  // unlink(2) should not remove directories
  if (S_ISDIR(kafs_ino_mode_get(inoent_target)))
  {
    kafs_inode_unlock(ctx, ino_dir);
    kafs_journal_abort(ctx, jseq, "EISDIR");
    return -EISDIR;
  }

  kafs_inocnt_t removed_ino = KAFS_INO_NONE;
  int rrc = kafs_dirent_remove_nolink(ctx, inoent_dir, basepath, &removed_ino);
  if (rrc < 0)
  {
    kafs_inode_unlock(ctx, ino_dir);
    kafs_journal_abort(ctx, jseq, "dirent_remove=%d", rrc);
    return rrc;
  }
  kafs_inode_unlock(ctx, ino_dir);

  if (removed_ino != target_ino)
    return -ESTALE;

  // Decrement link count under target inode lock (keep dir lock hold time short)
  int reclaim_now = kafs_tombstone_pressure(ctx);
  int reclaimed_now = 0;
  kafs_inode_lock(ctx, (uint32_t)removed_ino);
  (void)kafs_inode_drop_link_locked(ctx, removed_ino, reclaim_now, &reclaimed_now);
  kafs_inode_unlock(ctx, (uint32_t)removed_ino);

  if (reclaimed_now)
  {
    kafs_inode_alloc_lock(ctx);
    (void)kafs_sb_inocnt_free_incr(ctx->c_superblock);
    kafs_sb_wtime_set(ctx->c_superblock, kafs_now());
    kafs_inode_alloc_unlock(ctx);
  }

  kafs_journal_commit(ctx, jseq);
  return 0;
}

static int kafs_op_access(const char *path, int mode)
{
  if (kafs_is_ctl_path(path))
    return 0;
  struct fuse_context *fctx = fuse_get_context();
  struct kafs_context *ctx = fctx->private_data;
  KAFS_CALL(kafs_access, fctx, ctx, path, NULL, mode, NULL);
  return 0;
}

static int kafs_split_parent_basename(const char *path, char *path_copy, const char **dir,
                                      char **base)
{
  strcpy(path_copy, path);
  *dir = path_copy;
  *base = strrchr(path_copy, '/');
  if (*dir == *base)
    *dir = "/";
  **base = '\0';
  (*base)++;
  if ((*base)[0] == '\0')
    return -EINVAL;
  if (strcmp(*base, ".") == 0 || strcmp(*base, "..") == 0)
    return -EINVAL;
  return 0;
}

static size_t kafs_rename_prepare_lock_list(uint32_t lock_list[4], uint32_t ino_from_dir,
                                            uint32_t ino_to_dir, uint32_t ino_src_u32,
                                            uint32_t ino_dst_u32)
{
  size_t lock_n = 0;
  lock_list[lock_n++] = ino_from_dir;
  if (ino_to_dir != ino_from_dir)
    lock_list[lock_n++] = ino_to_dir;
  if (ino_src_u32 != ino_from_dir && ino_src_u32 != ino_to_dir)
    lock_list[lock_n++] = ino_src_u32;
  if (ino_dst_u32 != UINT32_MAX && ino_dst_u32 != ino_from_dir && ino_dst_u32 != ino_to_dir &&
      ino_dst_u32 != ino_src_u32)
    lock_list[lock_n++] = ino_dst_u32;

  for (size_t i = 0; i < lock_n; ++i)
    for (size_t j = i + 1; j < lock_n; ++j)
      if (lock_list[j] < lock_list[i])
      {
        uint32_t tmp = lock_list[i];
        lock_list[i] = lock_list[j];
        lock_list[j] = tmp;
      }

  return lock_n;
}

static void kafs_rename_lock_list_acquire(struct kafs_context *ctx, const uint32_t *lock_list,
                                          size_t lock_n)
{
  for (size_t i = 0; i < lock_n; ++i)
    kafs_inode_lock(ctx, lock_list[i]);
}

static void kafs_rename_lock_list_release(struct kafs_context *ctx, const uint32_t *lock_list,
                                          size_t lock_n)
{
  for (size_t i = lock_n; i > 0; --i)
    kafs_inode_unlock(ctx, lock_list[i - 1]);
}

static int kafs_rename_prepare_existing_destination_locked(struct kafs_context *ctx, uint64_t jseq,
                                                           const uint32_t *lock_list, size_t lock_n,
                                                           int src_is_dir, int exists_to,
                                                           kafs_sinode_t *inoent_to_exist)
{
  if (!src_is_dir || exists_to != 0 || !inoent_to_exist)
    return 0;

  int empty_rc = kafs_dir_is_empty_locked(ctx, inoent_to_exist);
  if (empty_rc < 0)
  {
    kafs_journal_abort(ctx, jseq, "dst_dir_empty=%d", empty_rc);
    kafs_rename_lock_list_release(ctx, lock_list, lock_n);
    return empty_rc;
  }
  if (empty_rc == 0)
  {
    kafs_journal_abort(ctx, jseq, "DST_DIR_NOT_EMPTY");
    kafs_rename_lock_list_release(ctx, lock_list, lock_n);
    return -ENOTEMPTY;
  }

  int rr = kafs_dirent_remove(ctx, inoent_to_exist, "..");
  if (rr < 0)
  {
    kafs_journal_abort(ctx, jseq, "dst_remove_dotdot=%d", rr);
    kafs_rename_lock_list_release(ctx, lock_list, lock_n);
    return rr;
  }

  return 0;
}

static int kafs_rename_move_entries_locked(struct kafs_context *ctx, uint64_t jseq,
                                           const uint32_t *lock_list, size_t lock_n,
                                           kafs_sinode_t *inoent_dir_from,
                                           kafs_sinode_t *inoent_dir_to, kafs_inocnt_t ino_src,
                                           const char *from_base, const char *to_base,
                                           kafs_inocnt_t *removed_dst_ino)
{
  int rc_locked;
  kafs_inocnt_t moved_ino = KAFS_INO_NONE;

  *removed_dst_ino = KAFS_INO_NONE;
  rc_locked = kafs_dirent_remove_nolink(ctx, inoent_dir_to, to_base, removed_dst_ino);
  if (rc_locked < 0 && rc_locked != -ENOENT)
  {
    kafs_journal_abort(ctx, jseq, "dst_remove=%d", rc_locked);
    kafs_rename_lock_list_release(ctx, lock_list, lock_n);
    return rc_locked;
  }
  if (rc_locked == -ENOENT)
    *removed_dst_ino = KAFS_INO_NONE;

  rc_locked = kafs_dirent_remove_nolink(ctx, inoent_dir_from, from_base, &moved_ino);
  if (rc_locked < 0)
  {
    kafs_journal_abort(ctx, jseq, "src_remove=%d", rc_locked);
    kafs_rename_lock_list_release(ctx, lock_list, lock_n);
    return rc_locked;
  }
  if (moved_ino != ino_src)
  {
    kafs_journal_abort(ctx, jseq, "ESTALE moved_ino=%u src=%u", (unsigned)moved_ino,
                       (unsigned)ino_src);
    kafs_rename_lock_list_release(ctx, lock_list, lock_n);
    return -ESTALE;
  }

  rc_locked = kafs_dirent_add_nolink(ctx, inoent_dir_to, ino_src, to_base);
  if (rc_locked < 0)
  {
    kafs_journal_abort(ctx, jseq, "dst_add=%d", rc_locked);
    kafs_rename_lock_list_release(ctx, lock_list, lock_n);
    return rc_locked;
  }

  return 0;
}

static int kafs_rename_update_dotdot_locked(struct kafs_context *ctx, uint64_t jseq,
                                            const uint32_t *lock_list, size_t lock_n,
                                            int src_is_dir, uint32_t ino_from_dir,
                                            uint32_t ino_to_dir, kafs_sinode_t *inoent_src)
{
  if (!src_is_dir || ino_from_dir == ino_to_dir)
    return 0;

  int rr = kafs_dirent_remove(ctx, inoent_src, "..");
  if (rr < 0)
  {
    kafs_journal_abort(ctx, jseq, "src_remove_dotdot=%d", rr);
    kafs_rename_lock_list_release(ctx, lock_list, lock_n);
    return rr;
  }
  rr = kafs_dirent_add(ctx, inoent_src, (kafs_inocnt_t)ino_to_dir, "..");
  if (rr < 0)
  {
    kafs_journal_abort(ctx, jseq, "src_add_dotdot=%d", rr);
    kafs_rename_lock_list_release(ctx, lock_list, lock_n);
    return rr;
  }

  return 0;
}

static int kafs_rename_validate_request(const char *from, const char *to, unsigned int flags)
{
  if (kafs_is_ctl_path(from) || kafs_is_ctl_path(to))
    return -EACCES;
  if (from == NULL || to == NULL || from[0] != '/' || to[0] != '/')
    return -EINVAL;
  if (strcmp(from, to) == 0)
    return 0;

  size_t from_len = strlen(from);
  if (from_len > 1 && strncmp(to, from, from_len) == 0 && to[from_len] == '/')
    return -EINVAL;

  if (flags & ~RENAME_NOREPLACE)
    return -EOPNOTSUPP;
  return 1;
}

static int kafs_rename_check_source(struct fuse_context *fctx, struct kafs_context *ctx,
                                    const char *from, kafs_sinode_t **inoent_src_out,
                                    int *src_is_dir_out)
{
  int rc = kafs_access(fctx, ctx, from, NULL, F_OK, inoent_src_out);
  if (rc < 0)
    return rc;

  kafs_mode_t src_mode = kafs_ino_mode_get(*inoent_src_out);
  *src_is_dir_out = S_ISDIR(src_mode) ? 1 : 0;
  if (!S_ISREG(src_mode) && !S_ISLNK(src_mode) && !*src_is_dir_out)
    return -EOPNOTSUPP;
  return 0;
}

static int kafs_rename_parse_paths(const char *from, const char *to, char *from_copy,
                                   const char **from_dir, char **from_base, char *to_copy,
                                   const char **to_dir, char **to_base)
{
  int rc = kafs_split_parent_basename(from, from_copy, from_dir, from_base);
  if (rc < 0)
    return rc;
  return kafs_split_parent_basename(to, to_copy, to_dir, to_base);
}

static int kafs_rename_lookup_parent_dirs(struct fuse_context *fctx, struct kafs_context *ctx,
                                          const char *from_dir, const char *to_dir,
                                          kafs_sinode_t **inoent_dir_from,
                                          kafs_sinode_t **inoent_dir_to, uint32_t *ino_from_dir,
                                          uint32_t *ino_to_dir)
{
  int rc = kafs_access(fctx, ctx, from_dir, NULL, W_OK, inoent_dir_from);
  if (rc < 0)
    return rc;
  rc = kafs_access(fctx, ctx, to_dir, NULL, W_OK, inoent_dir_to);
  if (rc < 0)
    return rc;
  *ino_from_dir = kafs_ctx_ino_no(ctx, *inoent_dir_from);
  *ino_to_dir = kafs_ctx_ino_no(ctx, *inoent_dir_to);
  return 0;
}

static int kafs_rename_check_noreplace(struct fuse_context *fctx, struct kafs_context *ctx,
                                       const char *to, unsigned int flags, uint64_t jseq)
{
  if (!(flags & RENAME_NOREPLACE))
    return 0;

  kafs_sinode_t *inoent_tmp;
  int ex = kafs_access(fctx, ctx, to, NULL, F_OK, &inoent_tmp);
  if (ex == 0)
  {
    kafs_journal_abort(ctx, jseq, "EEXIST");
    return -EEXIST;
  }
  if (ex != -ENOENT)
  {
    kafs_journal_abort(ctx, jseq, "access(to)=%d", ex);
    return ex;
  }
  return 0;
}

static int kafs_rename_check_destination_type(struct fuse_context *fctx, struct kafs_context *ctx,
                                              const char *to, uint64_t jseq, int src_is_dir,
                                              kafs_sinode_t **inoent_to_exist, int *exists_to)
{
  *inoent_to_exist = NULL;
  *exists_to = kafs_access(fctx, ctx, to, NULL, F_OK, inoent_to_exist);
  if (*exists_to != 0)
    return (*exists_to == -ENOENT) ? 0 : *exists_to;

  kafs_mode_t dst_mode = kafs_ino_mode_get(*inoent_to_exist);
  if (src_is_dir)
  {
    if (!S_ISDIR(dst_mode))
    {
      kafs_journal_abort(ctx, jseq, "DST_NOT_DIR");
      return -ENOTDIR;
    }
    return 0;
  }

  if (S_ISDIR(dst_mode))
  {
    kafs_journal_abort(ctx, jseq, "DST_IS_DIR");
    return -EISDIR;
  }
  if (!S_ISREG(dst_mode) && !S_ISLNK(dst_mode))
  {
    kafs_journal_abort(ctx, jseq, "DST_NOT_FILE");
    return -EOPNOTSUPP;
  }
  return 0;
}

static void kafs_rename_finalize_replaced_inode(struct kafs_context *ctx,
                                                kafs_inocnt_t removed_dst_ino)
{
  if (removed_dst_ino == KAFS_INO_NONE)
    return;

  int reclaim_dst_now = kafs_tombstone_pressure(ctx);
  kafs_inode_lock(ctx, (uint32_t)removed_dst_ino);
  int reclaimed_dst = 0;
  (void)kafs_inode_drop_link_locked(ctx, removed_dst_ino, reclaim_dst_now, &reclaimed_dst);
  kafs_inode_unlock(ctx, (uint32_t)removed_dst_ino);

  if (reclaimed_dst)
  {
    kafs_inode_alloc_lock(ctx);
    (void)kafs_sb_inocnt_free_incr(ctx->c_superblock);
    kafs_sb_wtime_set(ctx->c_superblock, kafs_now());
    kafs_inode_alloc_unlock(ctx);
  }
}

static int kafs_op_rename(const char *from, const char *to, unsigned int flags)
{
  // 最小実装: 通常ファイルのみ対応。RENAME_NOREPLACE は尊重。その他のフラグは未対応。
  struct fuse_context *fctx = fuse_get_context();
  struct kafs_context *ctx = fctx->private_data;
  kafs_dlog(2, "%s: enter from=%s to=%s flags=%u\n", __func__, from ? from : "(null)",
            to ? to : "(null)", flags);
  kafs_sinode_t *inoent_src;
  int src_is_dir = 0;
  int rc = kafs_rename_validate_request(from, to, flags);
  if (rc <= 0)
    return rc;
  rc = kafs_rename_check_source(fctx, ctx, from, &inoent_src, &src_is_dir);
  if (rc < 0)
    return rc;

  char from_copy[strlen(from) + 1];
  const char *from_dir = from_copy;
  char *from_base = NULL;
  char to_copy[strlen(to) + 1];
  const char *to_dir = to_copy;
  char *to_base = NULL;
  rc = kafs_rename_parse_paths(from, to, from_copy, &from_dir, &from_base, to_copy, &to_dir,
                               &to_base);
  if (rc < 0)
    return rc;

  uint64_t jseq = kafs_journal_begin(ctx, "RENAME", "from=%s to=%s flags=%u", from, to, flags);

  kafs_sinode_t *inoent_dir_from;
  kafs_sinode_t *inoent_dir_to;
  uint32_t ino_from_dir = 0;
  uint32_t ino_to_dir = 0;
  rc = kafs_rename_lookup_parent_dirs(fctx, ctx, from_dir, to_dir, &inoent_dir_from, &inoent_dir_to,
                                      &ino_from_dir, &ino_to_dir);
  if (rc < 0)
  {
    kafs_journal_abort(ctx, jseq, "parent_lookup=%d", rc);
    return rc;
  }
  rc = kafs_rename_check_noreplace(fctx, ctx, to, flags, jseq);
  if (rc < 0)
    return rc;

  kafs_inocnt_t ino_src = kafs_ctx_ino_no(ctx, inoent_src);

  kafs_sinode_t *inoent_to_exist = NULL;
  int exists_to = 0;
  rc = kafs_rename_check_destination_type(fctx, ctx, to, jseq, src_is_dir, &inoent_to_exist,
                                          &exists_to);
  if (rc < 0)
    return rc;

  uint32_t ino_src_u32 = (uint32_t)ino_src;
  uint32_t ino_dst_u32 = UINT32_MAX;
  if (exists_to == 0 && inoent_to_exist)
    ino_dst_u32 = kafs_ctx_ino_no(ctx, inoent_to_exist);

  uint32_t lock_list[4];
  size_t lock_n =
      kafs_rename_prepare_lock_list(lock_list, ino_from_dir, ino_to_dir, ino_src_u32, ino_dst_u32);
  kafs_rename_lock_list_acquire(ctx, lock_list, lock_n);

  rc = kafs_rename_prepare_existing_destination_locked(ctx, jseq, lock_list, lock_n, src_is_dir,
                                                       exists_to, inoent_to_exist);
  if (rc < 0)
    return rc;

  kafs_inocnt_t removed_dst_ino = KAFS_INO_NONE;
  rc = kafs_rename_move_entries_locked(ctx, jseq, lock_list, lock_n, inoent_dir_from, inoent_dir_to,
                                       ino_src, from_base, to_base, &removed_dst_ino);
  if (rc < 0)
    return rc;

  rc = kafs_rename_update_dotdot_locked(ctx, jseq, lock_list, lock_n, src_is_dir, ino_from_dir,
                                        ino_to_dir, inoent_src);
  if (rc < 0)
    return rc;

  kafs_rename_lock_list_release(ctx, lock_list, lock_n);

  kafs_rename_finalize_replaced_inode(ctx, removed_dst_ino);

  kafs_journal_commit(ctx, jseq);
  kafs_dlog(2, "%s: exit rc=0 from=%s to=%s flags=%u\n", __func__, from, to, flags);
  return 0;
}

static int kafs_op_chmod(const char *path, mode_t mode, struct fuse_file_info *fi)
{
  struct fuse_context *fctx = fuse_get_context();
  struct kafs_context *ctx = fctx->private_data;
  if (kafs_is_ctl_path(path))
    return -EACCES;
  uint64_t jseq = kafs_journal_begin(ctx, "CHMOD", "path=%s mode=%o", path, (unsigned)mode);
  kafs_sinode_t *inoent;
  KAFS_CALL(kafs_access, fctx, ctx, path, fi, F_OK, &inoent);
  uint32_t ino = (uint32_t)kafs_ctx_ino_no(ctx, inoent);
  kafs_inode_lock(ctx, ino);
  kafs_mode_t m = kafs_ino_mode_get(inoent);
  kafs_ino_mode_set(inoent, (m & S_IFMT) | mode);
  kafs_inode_unlock(ctx, ino);
  kafs_journal_commit(ctx, jseq);
  return 0;
}

static int kafs_op_chown(const char *path, uid_t uid, gid_t gid, struct fuse_file_info *fi)
{
  struct fuse_context *fctx = fuse_get_context();
  struct kafs_context *ctx = fctx->private_data;
  if (kafs_is_ctl_path(path))
    return -EACCES;
  uint64_t jseq =
      kafs_journal_begin(ctx, "CHOWN", "path=%s uid=%u gid=%u", path, (unsigned)uid, (unsigned)gid);
  kafs_sinode_t *inoent;
  KAFS_CALL(kafs_access, fctx, ctx, path, fi, F_OK, &inoent);
  uint32_t ino = (uint32_t)kafs_ctx_ino_no(ctx, inoent);
  kafs_inode_lock(ctx, ino);
  kafs_ino_uid_set(inoent, uid);
  kafs_ino_gid_set(inoent, gid);
  kafs_inode_unlock(ctx, ino);
  kafs_journal_commit(ctx, jseq);
  return 0;
}

static int kafs_op_symlink(const char *target, const char *linkpath)
{
  struct fuse_context *fctx = fuse_get_context();
  struct kafs_context *ctx = fctx->private_data;
  if (kafs_is_ctl_path(linkpath))
    return -EACCES;
  uint64_t jseq = kafs_journal_begin(ctx, "SYMLINK", "target=%s linkpath=%s", target, linkpath);
  kafs_inocnt_t ino;
  KAFS_CALL(kafs_create, linkpath, 0777 | S_IFLNK, 0, NULL, &ino);
  kafs_sinode_t *inoent = kafs_ctx_inode(ctx, ino);
  kafs_mode_t created_mode = kafs_ino_mode_get(inoent);
  if (!S_ISLNK(created_mode))
  {
    kafs_log(KAFS_LOG_ERR,
             "%s: create type mismatch linkpath=%s ino=%" PRIuFAST32
             " mode=%o expected=symlink target=%s\n",
             __func__, linkpath ? linkpath : "(null)", (uint_fast32_t)ino, (unsigned)created_mode,
             target ? target : "(null)");
    kafs_journal_abort(ctx, jseq, "symlink type mismatch ino=%u mode=%o", (unsigned)ino,
                       (unsigned)created_mode);
    return -EIO;
  }
  kafs_inode_lock(ctx, (uint32_t)ino);
  kafs_diag_write_scope_t write_scope =
      kafs_diag_write_scope_enter(linkpath ? linkpath : "(null)", (uint32_t)ino);
  ssize_t w = KAFS_CALL(kafs_pwrite, ctx, inoent, target, strlen(target), 0);
  kafs_diag_write_scope_leave(write_scope);
  kafs_inode_unlock(ctx, (uint32_t)ino);
  assert(w == (ssize_t)strlen(target));
  kafs_journal_commit(ctx, jseq);
  return 0;
}

static void kafs_invalidate_path_best_effort(struct fuse_context *fctx, const char *path)
{
  if (!fctx || !fctx->fuse || !path || path[0] == '\0')
    return;
  int rc = fuse_invalidate_path(fctx->fuse, path);
  if (rc != 0 && rc != -ENOENT)
    kafs_dlog(1, "%s: fuse_invalidate_path(%s) rc=%d\n", __func__, path, rc);
}

static int kafs_op_link(const char *from, const char *to)
{
  assert(from != NULL);
  assert(to != NULL);
  if (kafs_is_ctl_path(from) || kafs_is_ctl_path(to))
    return -EACCES;
  if (from[0] != '/' || to[0] != '/' || from[1] == '\0' || to[1] == '\0')
    return -EINVAL;

  struct fuse_context *fctx = fuse_get_context();
  struct kafs_context *ctx = fctx->private_data;

  kafs_sinode_t *inoent_src;
  KAFS_CALL(kafs_access, fctx, ctx, from, NULL, F_OK, &inoent_src);
  kafs_mode_t src_mode = kafs_ino_mode_get(inoent_src);
  if (S_ISDIR(src_mode))
    return -EPERM;
  if (!S_ISREG(src_mode) && !S_ISLNK(src_mode))
    return -EOPNOTSUPP;

  int ex = kafs_access(fctx, ctx, to, NULL, F_OK, NULL);
  if (ex == 0)
    return -EEXIST;
  if (ex != -ENOENT)
    return ex;

  char to_copy[strlen(to) + 1];
  strcpy(to_copy, to);
  const char *to_dir = to_copy;
  char *to_base = strrchr(to_copy, '/');
  if (to_dir == to_base)
    to_dir = "/";
  *to_base = '\0';
  to_base++;
  if (to_base[0] == '\0' || strcmp(to_base, ".") == 0 || strcmp(to_base, "..") == 0)
    return -EINVAL;

  uint64_t jseq = kafs_journal_begin(ctx, "LINK", "from=%s to=%s", from, to);

  kafs_sinode_t *inoent_dir;
  int arc = kafs_access(fctx, ctx, to_dir, NULL, W_OK, &inoent_dir);
  if (arc < 0)
  {
    kafs_journal_abort(ctx, jseq, "parent access=%d", arc);
    return arc;
  }

  uint32_t ino_src = (uint32_t)kafs_ctx_ino_no(ctx, inoent_src);
  uint32_t ino_dir = (uint32_t)kafs_ctx_ino_no(ctx, inoent_dir);
  kafs_inode_lock(ctx, ino_src);
  kafs_ino_linkcnt_incr(inoent_src);
  kafs_inode_unlock(ctx, ino_src);

  kafs_inode_lock(ctx, ino_dir);
  int rc = kafs_dirent_add_nolink(ctx, inoent_dir, (kafs_inocnt_t)ino_src, to_base);
  kafs_inode_unlock(ctx, ino_dir);

  if (rc < 0)
  {
    kafs_inode_lock(ctx, ino_src);
    (void)kafs_ino_linkcnt_decr(inoent_src);
    kafs_inode_unlock(ctx, ino_src);
    kafs_journal_abort(ctx, jseq, "dirent_add=%d", rc);
    return rc;
  }

  kafs_inode_lock(ctx, ino_src);
  kafs_ino_ctime_set(inoent_src, kafs_now());
  kafs_inode_unlock(ctx, ino_src);

  kafs_journal_commit(ctx, jseq);
  kafs_invalidate_path_best_effort(fctx, from);
  kafs_invalidate_path_best_effort(fctx, to);
  return 0;
}

static int kafs_op_flush(const char *path, struct fuse_file_info *fi)
{
  struct fuse_context *fctx = fuse_get_context();
  struct kafs_context *ctx = fctx ? fctx->private_data : NULL;
  uint32_t ino = fi ? (uint32_t)fi->fh : (uint32_t)KAFS_INO_NONE;
  kafs_dlog(2, "%s: enter path=%s ino=%" PRIuFAST32 "\n", __func__, path ? path : "(null)", ino);
  // POSIX-minimal behavior: close/flush should not force global durability sync.
  // Durability is guaranteed by explicit fsync/fdatasync paths.
  (void)ctx;
  kafs_dlog(2, "%s: exit rc=0 path=%s ino=%" PRIuFAST32 "\n", __func__, path ? path : "(null)",
            ino);
  return 0;
}

static uint32_t kafs_fsync_resolve_inode(struct fuse_context *fctx, struct kafs_context *ctx,
                                         const char *path, struct fuse_file_info *fi)
{
  if (fi)
    return (uint32_t)fi->fh;
  if (!path || path[0] == '\0')
    return KAFS_INO_NONE;

  kafs_sinode_t *inoent = NULL;
  int arc = kafs_access(fctx, ctx, path, fi, F_OK, &inoent);
  if (arc == 0 && inoent)
    return (uint32_t)kafs_ctx_ino_no(ctx, inoent);
  return KAFS_INO_NONE;
}

static int kafs_fsync_prepare_inode(struct kafs_context *ctx, const char *path, uint32_t ino)
{
  kafs_inode_lock(ctx, ino);
  int nrc = kafs_tailmeta_normalize_block_layout(ctx, kafs_ctx_inode(ctx, ino));
  kafs_inode_unlock(ctx, ino);
  if (nrc < 0)
  {
    kafs_dlog(2, "%s: exit rc=%d normalize_failed path=%s ino=%" PRIuFAST32 "\n", __func__, nrc,
              path ? path : "(null)", ino);
    return nrc;
  }

  uint32_t saved_mode = KAFS_PENDING_WORKER_PRIO_NORMAL;
  int saved_nice = 0;
  int boosted = 0;
  kafs_pending_worker_begin_boost(ctx, &saved_mode, &saved_nice, &boosted);

  int drc = kafs_pendinglog_drain_inode(ctx, ino);
  kafs_pending_worker_end_boost(ctx, saved_mode, saved_nice, boosted);
  if (drc < 0 && drc != -ETIMEDOUT)
  {
    kafs_dlog(2, "%s: exit rc=%d drain_failed path=%s ino=%" PRIuFAST32 "\n", __func__, drc,
              path ? path : "(null)", ino);
    return drc;
  }
  if (drc == -ETIMEDOUT)
  {
    kafs_log(KAFS_LOG_WARNING, "%s: pending drain timeout path=%s ino=%" PRIuFAST32 " (continue)\n",
             __func__, path ? path : "(null)", ino);
  }
  return 0;
}

static int kafs_fsync_use_journal_only(struct kafs_context *ctx, int isdatasync)
{
  if (!kafs_journal_is_enabled(ctx))
    return 0;
  if (ctx->c_fsync_policy == KAFS_FSYNC_POLICY_JOURNAL_ONLY)
    return 1;
  if (ctx->c_fsync_policy != KAFS_FSYNC_POLICY_ADAPTIVE)
    return 0;
  return isdatasync || ctx->c_pending_ttl_over_soft || ctx->c_pending_ttl_over_hard;
}

static int kafs_fsync_backing_sync(struct kafs_context *ctx, const char *path, uint32_t ino,
                                   int isdatasync)
{
  int src = isdatasync ? fdatasync(ctx->c_fd) : fsync(ctx->c_fd);
  if (src != 0)
  {
    int e = errno;
    kafs_log(KAFS_LOG_WARNING, "%s: %s failed path=%s ino=%" PRIuFAST32 " errno=%d\n", __func__,
             isdatasync ? "fdatasync" : "fsync", path ? path : "(null)", ino, e);
    kafs_dlog(2, "%s: exit rc=%d path=%s ino=%" PRIuFAST32 "\n", __func__, -e,
              path ? path : "(null)", ino);
    return -e;
  }
  return 0;
}

static int kafs_op_fsync(const char *path, int isdatasync, struct fuse_file_info *fi)
{
  struct fuse_context *fctx = fuse_get_context();
  struct kafs_context *ctx = fctx->private_data;
  kafs_dlog(2, "%s: enter path=%s isdatasync=%d\n", __func__, path ? path : "(null)", isdatasync);
  if (!ctx || ctx->c_fd < 0)
  {
    kafs_dlog(2, "%s: exit rc=0 (no backing fd) path=%s\n", __func__, path ? path : "(null)");
    return 0;
  }

  uint32_t ino = kafs_fsync_resolve_inode(fctx, ctx, path, fi);
  if (ino != KAFS_INO_NONE)
  {
    int prc = kafs_fsync_prepare_inode(ctx, path, ino);
    if (prc < 0)
      return prc;
  }

  uint32_t policy = ctx->c_fsync_policy;
  if (kafs_fsync_use_journal_only(ctx, isdatasync))
  {
    kafs_journal_force_flush(ctx);
    kafs_dlog(2,
              "%s: exit rc=0 path=%s ino=%" PRIuFAST32
              " mode=journal-only isdatasync=%d policy=%" PRIuFAST32 "\n",
              __func__, path ? path : "(null)", ino, isdatasync, policy);
    return 0;
  }

  kafs_journal_force_flush(ctx);
  int src = kafs_fsync_backing_sync(ctx, path, ino, isdatasync);
  if (src != 0)
    return src;
  kafs_dlog(2, "%s: exit rc=0 path=%s ino=%" PRIuFAST32 "\n", __func__, path ? path : "(null)",
            ino);
  return 0;
}

static int kafs_op_fsyncdir(const char *path, int isdatasync, struct fuse_file_info *fi)
{
  return kafs_op_fsync(path, isdatasync, fi);
}

static int g_kafs_writeback_cache_enabled = 1;

static void *kafs_op_init(struct fuse_conn_info *conn, struct fuse_config *cfg)
{
  if (cfg)
  {
    // Expose stable inode numbers so hardlinks share inode identity and link count.
    cfg->use_ino = 1;
    cfg->hard_remove = 1;
  }
#ifdef FUSE_CAP_WRITEBACK_CACHE
  if (conn)
  {
    if (g_kafs_writeback_cache_enabled)
      conn->want |= FUSE_CAP_WRITEBACK_CACHE;
    else
      conn->want &= ~((uint32_t)FUSE_CAP_WRITEBACK_CACHE);
  }
#endif
  struct fuse_context *fctx = fuse_get_context();
  kafs_context_t *ctx = fctx ? (kafs_context_t *)fctx->private_data : NULL;
  if (ctx && ctx->c_pendinglog_enabled)
  {
    int prc = kafs_pending_worker_start(ctx);
    if (prc < 0)
    {
      kafs_log(KAFS_LOG_WARNING, "kafs: pending worker start failed in init rc=%d\n", prc);
    }
  }
  if (ctx)
  {
    int trc = kafs_tombstone_gc_worker_start(ctx);
    if (trc < 0)
      kafs_log(KAFS_LOG_WARNING, "kafs: tombstone GC worker start failed in init rc=%d\n", trc);
  }
  if (ctx && ctx->c_bg_dedup_enabled)
  {
    int brc = kafs_bg_dedup_worker_start(ctx);
    if (brc < 0)
      kafs_log(KAFS_LOG_WARNING, "kafs: bg dedup worker start failed in init rc=%d\n", brc);
  }
  return ctx;
}

static void kafs_op_destroy(void *private_data)
{
  kafs_context_t *ctx = (kafs_context_t *)private_data;
  if (!ctx)
    return;
  kafs_bg_dedup_worker_stop(ctx);
  kafs_tombstone_gc_worker_stop(ctx);
  kafs_pending_worker_stop(ctx);
}

static int kafs_op_release(const char *path, struct fuse_file_info *fi)
{
  struct fuse_context *fctx = fuse_get_context();
  struct kafs_context *ctx = fctx ? fctx->private_data : NULL;
  kafs_dlog(2, "%s: enter path=%s ino=%" PRIuFAST32 "\n", __func__, path ? path : "(null)",
            fi ? (uint32_t)fi->fh : (uint32_t)KAFS_INO_NONE);
  if (kafs_is_ctl_path(path))
  {
    kafs_ctl_session_t *sess = (kafs_ctl_session_t *)(uintptr_t)fi->fh;
    free(sess);
    fi->fh = 0;
    kafs_dlog(2, "%s: exit rc=0 ctl path=%s\n", __func__, path ? path : "(null)");
    return 0;
  }
  kafs_inocnt_t ino = fi->fh;
  int reclaimed = 0;
  if (ctx && ctx->c_open_cnt)
  {
    uint32_t after = __atomic_sub_fetch(&ctx->c_open_cnt[ino], 1u, __ATOMIC_RELAXED);
    kafs_dlog(2, "%s: open_cnt after dec ino=%" PRIuFAST32 " after=%" PRIu32 "\n", __func__,
              (uint32_t)ino, after);
    if (after == 0)
    {
      int reclaim_now = kafs_tombstone_pressure(ctx);
      kafs_inode_lock(ctx, (uint32_t)ino);
      if (kafs_inode_is_tombstone(kafs_ctx_inode(ctx, ino)))
      {
        if (!reclaim_now)
        {
          int trc = kafs_tailmeta_try_reclaim_tombstone_payload_locked(ctx, ino);
          if (trc < 0)
          {
            kafs_log(KAFS_LOG_WARNING, "%s: early tail reclaim failed ino=%" PRIuFAST32 " rc=%d\n",
                     __func__, (uint32_t)ino, trc);
          }
        }
        if (reclaim_now)
          (void)kafs_try_reclaim_unlinked_inode_locked(ctx, ino, &reclaimed);
      }
      else
      {
        int nrc = kafs_tailmeta_normalize_block_layout(ctx, kafs_ctx_inode(ctx, ino));
        if (nrc < 0)
          kafs_log(KAFS_LOG_WARNING, "%s: mixed tail normalize failed ino=%" PRIuFAST32 " rc=%d\n",
                   __func__, (uint32_t)ino, nrc);
      }
      kafs_inode_unlock(ctx, (uint32_t)ino);

      if (reclaimed)
      {
        kafs_inode_alloc_lock(ctx);
        (void)kafs_sb_inocnt_free_incr(ctx->c_superblock);
        kafs_sb_wtime_set(ctx->c_superblock, kafs_now());
        kafs_inode_alloc_unlock(ctx);
      }
    }
  }
  int rc = kafs_op_flush(path, fi);
  kafs_dlog(2, "%s: exit rc=%d path=%s ino=%" PRIuFAST32 " reclaimed=%d\n", __func__, rc,
            path ? path : "(null)", (uint32_t)ino, reclaimed);
  return rc;
}

#ifndef KAFS_NO_MAIN
static struct fuse_operations kafs_operations = {
    .init = kafs_op_init,
    .destroy = kafs_op_destroy,
    .getattr = kafs_op_getattr,
    .statfs = kafs_op_statfs,
    .open = kafs_op_open,
    .create = kafs_op_create,
    .mknod = kafs_op_mknod,
    .readlink = kafs_op_readlink,
    .read = kafs_op_read,
    .write = kafs_op_write,
    .flush = kafs_op_flush,
    .fsync = kafs_op_fsync,
    .release = kafs_op_release,
    .opendir = kafs_op_opendir,
    .readdir = kafs_op_readdir,
    .fsyncdir = kafs_op_fsyncdir,
    .utimens = kafs_op_utimens,
    .truncate = kafs_op_truncate,
    .fallocate = kafs_op_fallocate,
    .lseek = kafs_op_lseek,
    .rename = kafs_op_rename,
    .unlink = kafs_op_unlink,
    .mkdir = kafs_op_mkdir,
    .rmdir = kafs_op_rmdir,
    .access = kafs_op_access,
    .chmod = kafs_op_chmod,
    .chown = kafs_op_chown,
    .link = kafs_op_link,
    .symlink = kafs_op_symlink,
    .ioctl = kafs_op_ioctl,
    .copy_file_range = kafs_op_copy_file_range,
};
#endif

static void usage(const char *prog)
{
  fprintf(stderr,
          "Usage:\n"
          "  %s [global-options] <mountpoint> [FUSE options...]\n"
          "  %s <image> <mountpoint> [FUSE options...]\n"
          "  %s [--image <image>] --migrate [--yes] <mountpoint> [FUSE options...]\n"
          "\n"
          "Options:\n"
          "  [Global]\n"
          "    -h, --help                        Show this help and exit\n"
          "    --image <image>                   Image path\n"
          "    --image=<image>                   Image path (inline form)\n"
          "    --migrate                         Pre-start migration entrypoint for v2/v3 -> v4\n"
          "    --migrate-v2                      Deprecated alias of --migrate\n"
          "    --yes                             Skip migration confirmation prompt\n"
          "\n"
          "  [Cache/TRIM]\n"
          "    --writeback-cache                 Enable writeback cache\n"
          "    --no-writeback-cache              Disable writeback cache\n"
          "    --trim-on-free                    Enable TRIM on freed data blocks\n"
          "    --no-trim-on-free                 Disable TRIM on freed data blocks\n"
          "    -o writeback_cache                Enable writeback cache (FUSE -o)\n"
          "    -o no_writeback_cache             Disable writeback cache (FUSE -o)\n"
          "    -o trim_on_free | trim-on-free    Enable TRIM (FUSE -o)\n"
          "    -o no_trim_on_free | no-trim-on-free\n"
          "                                      Disable TRIM (FUSE -o)\n"
          "\n"
          "  [Threading]\n"
          "    -o multi_thread[=N]               Enable MT mode (alias: multi-thread, "
          "multithread)\n"
          "                                      Default: single-threaded unless enabled\n"
          "\n"
          "  [Hotplug]\n"
          "    --hotplug                         Enable hotplug using default UDS\n"
          "    --hotplug=<uds>                   Enable hotplug and set UDS path\n"
          "    --hotplug-uds <uds>               Hotplug UDS path (explicit form)\n"
          "    --hotplug-back-bin <path>         Backend binary path hint\n"
          "    -o hotplug[=<uds>]                Same as --hotplug[=<uds>]\n"
          "    -o hotplug_uds=<uds>              Same as --hotplug-uds\n"
          "    -o hotplug_back_bin=<path>        Same as --hotplug-back-bin\n"
          "\n"
          "  [Pending Worker]\n"
          "    -o pending_worker_prio=<normal|idle>\n"
          "                                      Set pending worker scheduling mode\n"
          "    -o pending_worker_nice=<0..19>    Set pending worker nice value\n"
          "    -o dedup_worker_prio=...          Alias of pending_worker_prio\n"
          "    -o dedup_worker_nice=...          Alias of pending_worker_nice\n"
          "    -o pending_ttl_soft_ms=<N>        Soft TTL for pending entries (ms)\n"
          "    -o pending_ttl_hard_ms=<N>        Hard TTL for pending entries (ms)\n"
          "    -o pendinglog_cap_initial=<N>     Pending queue initial effective capacity\n"
          "    -o pendinglog_cap_min=<N>         Pending queue minimum effective capacity\n"
          "    -o pendinglog_cap_max=<N>         Pending queue maximum effective capacity\n"
          "    -o bg_dedup_scan=<on|off>         Enable/disable idle background dedup scan\n"
          "    -o dedup_scan=<on|off>            Alias of bg_dedup_scan (default: on)\n"
          "    -o bg_dedup_interval_ms=<N>       Dedup scan interval (ms, over start threshold)\n"
          "    -o dedup_interval_ms=<N>          Alias of bg_dedup_interval_ms\n"
          "    -o bg_dedup_quiet_interval_ms=<N> Recheck interval below start threshold (ms)\n"
          "    -o dedup_quiet_interval_ms=<N>    Alias of bg_dedup_quiet_interval_ms\n"
          "    -o bg_dedup_pressure_interval_ms=<N> Dedup interval under capacity pressure (ms)\n"
          "    -o dedup_pressure_interval_ms=<N> Alias of bg_dedup_pressure_interval_ms\n"
          "    -o bg_dedup_start_used_pct=<0..100> Start dedup when used-block ratio reaches N%%\n"
          "    -o dedup_start_used_pct=<0..100> Alias of bg_dedup_start_used_pct\n"
          "    -o bg_dedup_pressure_used_pct=<0..100> Pressure threshold ratio (auto boost)\n"
          "    -o dedup_pressure_used_pct=<0..100> Alias of bg_dedup_pressure_used_pct\n"
          "    -o bg_dedup_worker_prio=<normal|idle> Dedicated bg-dedup worker scheduling mode\n"
          "    -o bg_dedup_worker_nice=<0..19>  Dedicated bg-dedup worker nice value\n"
          "\n"
          "  [Sync Policy]\n"
          "    -o fsync_policy=<journal_only|full|adaptive>\n"
          "                                      fsync/fdatasync runtime policy\n"
          "\n"
          "Environment:\n"
          "    KAFS_IMAGE                        Fallback image path\n"
          "    KAFS_WRITEBACK_CACHE=0|1          Default writeback cache mode\n"
          "    KAFS_TRIM_ON_FREE=0|1             Default TRIM on free mode\n"
          "    KAFS_MT=1                         Enable multi-thread mode\n"
          "    KAFS_MAX_THREADS=<N>              Default MT worker count\n"
          "    KAFS_PENDING_WORKER_PRIO          pending_worker_prio default\n"
          "    KAFS_PENDING_WORKER_NICE          pending_worker_nice default\n"
          "    KAFS_PENDING_TTL_SOFT_MS          pending soft TTL (ms)\n"
          "    KAFS_PENDING_TTL_HARD_MS          pending hard TTL (ms)\n"
          "    KAFS_PENDINGLOG_CAP_INITIAL       pending initial effective capacity\n"
          "    KAFS_PENDINGLOG_CAP_MIN           pending minimum effective capacity\n"
          "    KAFS_PENDINGLOG_CAP_MAX           pending maximum effective capacity\n"
          "    KAFS_BG_DEDUP_SCAN                idle background dedup scan on/off (default: on)\n"
          "    KAFS_BG_DEDUP_INTERVAL_MS         dedup scan interval (ms)\n"
          "    KAFS_BG_DEDUP_QUIET_INTERVAL_MS   recheck interval below start threshold (ms)\n"
          "    KAFS_BG_DEDUP_PRESSURE_INTERVAL_MS dedup interval under pressure (ms)\n"
          "    KAFS_BG_DEDUP_START_USED_PCT      start threshold by used-block ratio (%%)\n"
          "    KAFS_BG_DEDUP_PRESSURE_USED_PCT   pressure threshold by used-block ratio (%%)\n"
          "    KAFS_BG_DEDUP_WORKER_PRIO         dedicated bg-dedup worker prio mode\n"
          "    KAFS_BG_DEDUP_WORKER_NICE         dedicated bg-dedup worker nice value\n"
          "    KAFS_FSYNC_POLICY                 fsync policy default\n"
          "    KAFS_HOTPLUG_UDS                  Hotplug UDS path (legacy/env)\n"
          "    KAFS_HOTPLUG_BACK_BIN             Backend binary path hint\n"
          "\n"
          "Notes:\n"
          "    v2/v3 images are refused by default for v4 mount.\n"
          "    Use kafsctl migrate or --migrate to run offline pre-start migration.\n"
          "\n"
          "Examples:\n"
          "  %s --image test.img mnt -f\n"
          "  %s --image legacy.img mnt -f\n"
          "  %s --image test.img mnt -f -o multi_thread=8\n",
          prog, prog, prog, prog, prog, prog);
}

static int kafs_confirm_yes_stdin(void)
{
  char buf[32];

  for (;;)
  {
    fprintf(stderr, "WARNING: offline migration to v%u is irreversible. Continue? [y/N]: ",
            (unsigned)KAFS_FORMAT_VERSION);
    fflush(stderr);
    if (!fgets(buf, sizeof(buf), stdin))
      return 0;
    buf[strcspn(buf, "\r\n")] = '\0';
    if (buf[0] == '\0' || strcasecmp(buf, "n") == 0 || strcasecmp(buf, "no") == 0)
      return 0;
    if (strcasecmp(buf, "y") == 0 || strcasecmp(buf, "yes") == 0)
      return 1;
    fprintf(stderr, "Please answer yes or no.\n");
  }
}

typedef struct __attribute__((packed))
{
  kafs_sinocnt_t d_ino;
  kafs_sfilenamelen_t d_filenamelen;
} kafs_legacy_dirent_hdr_t;

static int kafs_legacy_dir_snapshot(struct kafs_context *ctx, kafs_sinode_t *inoent_dir, char **out,
                                    size_t *out_len)
{
  *out = NULL;
  *out_len = 0;
  size_t len = (size_t)kafs_ino_size_get(inoent_dir);
  if (len == 0)
    return 0;
  char *buf = (char *)malloc(len);
  if (!buf)
    return -ENOMEM;
  ssize_t r = kafs_pread(ctx, inoent_dir, buf, (kafs_off_t)len, 0);
  if (r < 0 || (size_t)r != len)
  {
    free(buf);
    return -EIO;
  }
  *out = buf;
  *out_len = len;
  return 0;
}

static int kafs_legacy_dirent_iter_next(const char *buf, size_t len, size_t off,
                                        kafs_inocnt_t *out_ino, kafs_filenamelen_t *out_namelen,
                                        const char **out_name, size_t *out_rec_len)
{
  const size_t hdr_sz = sizeof(kafs_legacy_dirent_hdr_t);
  if (off >= len || len - off < hdr_sz)
    return 0;

  kafs_legacy_dirent_hdr_t hdr;
  memcpy(&hdr, buf + off, hdr_sz);
  kafs_inocnt_t ino = kafs_inocnt_stoh(hdr.d_ino);
  kafs_filenamelen_t namelen = kafs_filenamelen_stoh(hdr.d_filenamelen);
  if (ino == 0 || namelen == 0)
    return 0;
  if (namelen >= FILENAME_MAX)
    return -EIO;
  if (len - off < hdr_sz + (size_t)namelen)
    return -EIO;

  *out_ino = ino;
  *out_namelen = namelen;
  *out_name = buf + off + hdr_sz;
  *out_rec_len = hdr_sz + (size_t)namelen;
  return 1;
}

static int kafs_migrate_dir_inode_to_v4(struct kafs_context *ctx, kafs_sinode_t *inoent_dir)
{
  char *old = NULL;
  size_t old_len = 0;
  int rc = kafs_legacy_dir_snapshot(ctx, inoent_dir, &old, &old_len);
  if (rc < 0)
    return rc;

  size_t new_len = sizeof(kafs_sdir_v4_hdr_t);
  uint32_t live_count = 0;
  size_t off = 0;
  while (1)
  {
    kafs_inocnt_t dino;
    kafs_filenamelen_t dlen;
    const char *dname;
    size_t rec_len;
    int step = kafs_legacy_dirent_iter_next(old, old_len, off, &dino, &dlen, &dname, &rec_len);
    if (step == 0)
      break;
    if (step < 0)
    {
      free(old);
      return step;
    }
    new_len += offsetof(kafs_sdirent_v4_t, de_filename) + (size_t)dlen;
    live_count++;
    off += rec_len;
  }

  char *nw = (char *)calloc(1, new_len);
  if (!nw)
  {
    free(old);
    return -ENOMEM;
  }

  kafs_sdir_v4_hdr_t *hdr = (kafs_sdir_v4_hdr_t *)nw;
  kafs_dir_v4_hdr_init(hdr);
  kafs_dir_v4_hdr_live_count_set(hdr, live_count);
  kafs_dir_v4_hdr_tombstone_count_set(hdr, 0);
  kafs_dir_v4_hdr_record_bytes_set(hdr, (uint32_t)(new_len - sizeof(*hdr)));

  off = 0;
  size_t woff = sizeof(*hdr);
  while (1)
  {
    kafs_inocnt_t dino;
    kafs_filenamelen_t dlen;
    const char *dname;
    size_t rec_len;
    int step = kafs_legacy_dirent_iter_next(old, old_len, off, &dino, &dlen, &dname, &rec_len);
    if (step == 0)
      break;
    if (step < 0)
    {
      free(nw);
      free(old);
      return step;
    }

    size_t out_rec_len = offsetof(kafs_sdirent_v4_t, de_filename) + (size_t)dlen;
    kafs_sdirent_v4_t *rec = (kafs_sdirent_v4_t *)(nw + woff);
    kafs_dirent_v4_rec_len_set(rec, (uint16_t)out_rec_len);
    kafs_dirent_v4_flags_set(rec, 0);
    kafs_dirent_v4_ino_set(rec, dino);
    kafs_dirent_v4_filenamelen_set(rec, dlen);
    kafs_dirent_v4_name_hash_set(rec, kafs_dirent_name_hash(dname, dlen));
    memcpy(rec->de_filename, dname, (size_t)dlen);

    woff += out_rec_len;
    off += rec_len;
  }

  rc = kafs_dir_writeback(ctx, inoent_dir, nw, new_len);
  free(nw);
  free(old);
  return rc;
}

static int kafs_migrate_ctx_open(const char *image_path, kafs_context_t *ctx,
                                 kafs_ssuperblock_t *sbdisk)
{
  if (!image_path || !ctx || !sbdisk)
    return -EINVAL;

  memset(ctx, 0, sizeof(*ctx));
  ctx->c_fd = open(image_path, O_RDWR, 0666);
  if (ctx->c_fd < 0)
    return -errno;

  int rc = kafs_ctx_read_superblock_fd(ctx, sbdisk);
  if (rc != 0)
    return rc;
  if (kafs_sb_magic_get(sbdisk) != KAFS_MAGIC)
  {
    kafs_ctx_close_fd(ctx);
    return -EINVAL;
  }

  kafs_inocnt_t inocnt = kafs_inocnt_stoh(sbdisk->s_inocnt);
  rc = kafs_ctx_map_image(ctx, sbdisk);
  if (rc != 0)
    return rc;
  ctx->c_alloc_v3_summary_dirty = 1;
  ctx->c_diag_log_fd = -1;
  ctx->c_ino_epoch = calloc((size_t)inocnt, sizeof(uint32_t));
  if (ctx->c_ino_epoch)
  {
    for (kafs_inocnt_t i = 0; i < inocnt; ++i)
      ctx->c_ino_epoch[i] = 1u;
  }
  if (kafs_extra_diag_enabled())
  {
    ctx->c_diag_create_seq = calloc((size_t)inocnt, sizeof(uint64_t));
    ctx->c_diag_create_mode = calloc((size_t)inocnt, sizeof(uint16_t));
    ctx->c_diag_create_first_write_seen = calloc((size_t)inocnt, sizeof(uint8_t));
    ctx->c_diag_create_paths = calloc((size_t)inocnt, KAFS_DIAG_CREATE_PATH_MAX);
    if (!ctx->c_diag_create_seq || !ctx->c_diag_create_mode ||
        !ctx->c_diag_create_first_write_seen || !ctx->c_diag_create_paths)
    {
      free(ctx->c_diag_create_seq);
      free(ctx->c_diag_create_mode);
      free(ctx->c_diag_create_first_write_seen);
      free(ctx->c_diag_create_paths);
      ctx->c_diag_create_seq = NULL;
      ctx->c_diag_create_mode = NULL;
      ctx->c_diag_create_first_write_seen = NULL;
      ctx->c_diag_create_paths = NULL;
    }
    ctx->c_diag_create_seq_next = 0;
  }
  kafs_diag_log_open(ctx, image_path);

  return kafs_hrl_open(ctx);
}

static void kafs_migrate_ctx_close(kafs_context_t *ctx)
{
  if (!ctx)
    return;
  (void)kafs_hrl_close(ctx);
  free(ctx->c_ino_epoch);
  ctx->c_ino_epoch = NULL;
  free(ctx->c_diag_create_seq);
  free(ctx->c_diag_create_mode);
  free(ctx->c_diag_create_first_write_seen);
  free(ctx->c_diag_create_paths);
  ctx->c_diag_create_seq = NULL;
  ctx->c_diag_create_mode = NULL;
  ctx->c_diag_create_first_write_seen = NULL;
  ctx->c_diag_create_paths = NULL;
  kafs_diag_log_close(ctx);
  if (ctx->c_img_base && ctx->c_img_base != MAP_FAILED)
    munmap(ctx->c_img_base, ctx->c_img_size);
  if (ctx->c_fd >= 0)
    close(ctx->c_fd);
  ctx->c_img_base = NULL;
  ctx->c_fd = -1;
}

int kafs_core_migrate_image(const char *image_path, int assume_yes)
{
  if (!image_path)
    return -EINVAL;

  kafs_ssuperblock_t sb;
  int fd = open(image_path, O_RDWR, 0666);
  if (fd < 0)
    return -errno;
  ssize_t r = pread(fd, &sb, sizeof(sb), 0);
  if (r != (ssize_t)sizeof(sb))
  {
    int err = -errno;
    close(fd);
    return err ? err : -EIO;
  }
  close(fd);
  if (kafs_sb_magic_get(&sb) != KAFS_MAGIC)
    return -EINVAL;

  uint32_t fmt = kafs_sb_format_version_get(&sb);
  if (fmt == KAFS_FORMAT_VERSION)
    return 1;
  if (fmt != KAFS_FORMAT_VERSION_V2 && fmt != KAFS_FORMAT_VERSION_V3)
    return -EPROTONOSUPPORT;
  if (!assume_yes && !kafs_confirm_yes_stdin())
    return -ECANCELED;

  kafs_context_t ctx;
  int rc = kafs_migrate_ctx_open(image_path, &ctx, &sb);
  if (rc != 0)
    return rc;

  kafs_inocnt_t inocnt = kafs_sb_inocnt_get(ctx.c_superblock);
  for (kafs_inocnt_t ino = KAFS_INO_ROOTDIR; ino < inocnt; ++ino)
  {
    kafs_sinode_t *inoent = kafs_ctx_inode(&ctx, ino);
    if (!kafs_ino_get_usage(inoent))
      continue;
    if (!S_ISDIR(kafs_ino_mode_get(inoent)))
      continue;
    kafs_inode_lock(&ctx, (uint32_t)ino);
    rc = kafs_migrate_dir_inode_to_v4(&ctx, inoent);
    kafs_inode_unlock(&ctx, (uint32_t)ino);
    if (rc != 0)
      break;
  }

  if (rc == 0)
  {
    if (fmt == KAFS_FORMAT_VERSION_V2 && kafs_sb_allocator_size_get(ctx.c_superblock) > 0)
    {
      if (kafs_sb_allocator_version_get(ctx.c_superblock) < 2u)
        kafs_sb_allocator_version_set(ctx.c_superblock, 2u);
      uint64_t ff = kafs_sb_feature_flags_get(ctx.c_superblock);
      kafs_sb_feature_flags_set(ctx.c_superblock, ff | KAFS_FEATURE_ALLOC_V2);
    }
    kafs_sb_format_version_set(ctx.c_superblock, KAFS_FORMAT_VERSION);
    kafs_sb_wtime_set(ctx.c_superblock, kafs_now());
    if (msync(ctx.c_img_base, ctx.c_img_size, MS_SYNC) != 0)
      rc = -errno;
    else if (fsync(ctx.c_fd) != 0)
      rc = -errno;
  }

  kafs_migrate_ctx_close(&ctx);
  return rc;
}

typedef struct kafs_main_options
{
  const char *image_path;
  kafs_bool_t auto_migrate;
  kafs_bool_t migrate_yes;
  kafs_bool_t writeback_cache_enabled;
  kafs_bool_t writeback_cache_explicit;
  kafs_bool_t trim_on_free_enabled;
  kafs_bool_t trim_on_free_explicit;
  kafs_bool_t show_help;
  kafs_bool_t enable_mt;
  char hotplug_uds_opt[sizeof(((struct sockaddr_un *)0)->sun_path)];
  char hotplug_back_bin_opt[PATH_MAX];
  unsigned mt_cnt_override;
  int mt_cnt_override_set;
  int saw_max_threads;
  uint32_t pending_worker_prio_mode;
  int pending_worker_nice;
  uint32_t pending_ttl_soft_ms;
  uint32_t pending_ttl_hard_ms;
  uint32_t pending_cap_initial;
  uint32_t pending_cap_min;
  uint32_t pending_cap_max;
  uint32_t bg_dedup_scan_enabled;
  uint32_t bg_dedup_interval_ms;
  uint32_t bg_dedup_quiet_interval_ms;
  uint32_t bg_dedup_pressure_interval_ms;
  uint32_t bg_dedup_start_used_pct;
  uint32_t bg_dedup_pressure_used_pct;
  uint32_t bg_dedup_worker_prio_mode;
  int bg_dedup_worker_nice;
  uint32_t fsync_policy;
} kafs_main_options_t;

static void kafs_main_options_init(kafs_main_options_t *opts)
{
  memset(opts, 0, sizeof(*opts));
  opts->image_path = getenv("KAFS_IMAGE");
  opts->writeback_cache_enabled = KAFS_TRUE;
  opts->trim_on_free_enabled = KAFS_FALSE;
  opts->hotplug_uds_opt[0] = '\0';
  opts->hotplug_back_bin_opt[0] = '\0';
  opts->pending_worker_prio_mode = KAFS_PENDING_WORKER_PRIO_NORMAL;
  opts->pending_worker_nice = 0;
  opts->pending_ttl_soft_ms = 5000;
  opts->pending_ttl_hard_ms = 30000;
  opts->bg_dedup_scan_enabled = 1u;
  opts->bg_dedup_interval_ms = KAFS_BG_DEDUP_INTERVAL_MS_DEFAULT;
  opts->bg_dedup_quiet_interval_ms = KAFS_BG_DEDUP_QUIET_INTERVAL_MS_DEFAULT;
  opts->bg_dedup_pressure_interval_ms = KAFS_BG_DEDUP_PRESSURE_INTERVAL_MS_DEFAULT;
  opts->bg_dedup_start_used_pct = KAFS_BG_DEDUP_START_USED_PCT_DEFAULT;
  opts->bg_dedup_pressure_used_pct = KAFS_BG_DEDUP_PRESSURE_USED_PCT_DEFAULT;
  opts->bg_dedup_worker_prio_mode = KAFS_PENDING_WORKER_PRIO_IDLE;
  opts->bg_dedup_worker_nice = 19;
  opts->fsync_policy = KAFS_FSYNC_POLICY_JOURNAL_ONLY;
}

static void kafs_main_apply_optional_bool_env(const char *value, kafs_bool_t *out)
{
  if (!value || !*value || !out)
    return;
  if (strcmp(value, "0") == 0 || strcasecmp(value, "false") == 0 || strcasecmp(value, "off") == 0 ||
      strcasecmp(value, "no") == 0)
    *out = KAFS_FALSE;
  else if (strcmp(value, "1") == 0 || strcasecmp(value, "true") == 0 ||
           strcasecmp(value, "on") == 0 || strcasecmp(value, "yes") == 0)
    *out = KAFS_TRUE;
}

static const char *kafs_main_getenv_fallback(const char *primary, const char *fallback)
{
  const char *value = getenv(primary);
  if (value && *value)
    return value;
  return getenv(fallback);
}

static int kafs_main_parse_nice_env(const char *name, const char *value, int *out)
{
  if (!value || !*value)
    return 0;

  char *endp = NULL;
  long v = strtol(value, &endp, 10);
  if (!endp || *endp != '\0' || v < 0 || v > 19)
  {
    fprintf(stderr, "invalid %s: '%s'\n", name, value);
    return 2;
  }
  *out = (int)v;
  return 0;
}

static int kafs_main_parse_u32_env(const char *name, const char *value, uint32_t min_value,
                                   uint32_t max_value, uint32_t *out)
{
  if (!value || !*value)
    return 0;
  if (kafs_parse_u32_range(value, min_value, max_value, out) != 0)
  {
    fprintf(stderr, "invalid %s: '%s'\n", name, value);
    return 2;
  }
  return 0;
}

static int kafs_main_apply_pending_env_overrides(kafs_main_options_t *opts)
{
  const char *pprio = getenv("KAFS_PENDING_WORKER_PRIO");
  if (pprio && *pprio &&
      kafs_pending_worker_prio_mode_parse(pprio, &opts->pending_worker_prio_mode) != 0)
  {
    fprintf(stderr, "invalid KAFS_PENDING_WORKER_PRIO: '%s'\n", pprio);
    return 2;
  }
  if (kafs_main_parse_nice_env("KAFS_PENDING_WORKER_NICE", getenv("KAFS_PENDING_WORKER_NICE"),
                               &opts->pending_worker_nice) != 0)
    return 2;
  if (kafs_main_parse_u32_env("KAFS_PENDING_TTL_SOFT_MS", getenv("KAFS_PENDING_TTL_SOFT_MS"), 0,
                              3600000u, &opts->pending_ttl_soft_ms) != 0)
    return 2;
  return kafs_main_parse_u32_env("KAFS_PENDING_TTL_HARD_MS", getenv("KAFS_PENDING_TTL_HARD_MS"), 0,
                                 3600000u, &opts->pending_ttl_hard_ms);
}

static int kafs_main_apply_pending_cap_env_overrides(kafs_main_options_t *opts)
{
  if (kafs_main_parse_u32_env(
          "KAFS_PENDINGLOG_CAP_INITIAL",
          kafs_main_getenv_fallback("KAFS_PENDINGLOG_CAP_INITIAL", "KAFS_PENDING_CAP_INITIAL"), 0,
          1000000000u, &opts->pending_cap_initial) != 0)
    return 2;
  if (kafs_main_parse_u32_env(
          "KAFS_PENDINGLOG_CAP_MIN",
          kafs_main_getenv_fallback("KAFS_PENDINGLOG_CAP_MIN", "KAFS_PENDING_CAP_MIN"), 0,
          1000000000u, &opts->pending_cap_min) != 0)
    return 2;
  return kafs_main_parse_u32_env(
      "KAFS_PENDINGLOG_CAP_MAX",
      kafs_main_getenv_fallback("KAFS_PENDINGLOG_CAP_MAX", "KAFS_PENDING_CAP_MAX"), 0, 1000000000u,
      &opts->pending_cap_max);
}

static int kafs_main_apply_bg_dedup_env_overrides(kafs_main_options_t *opts)
{
  const char *bg_scan = getenv("KAFS_BG_DEDUP_SCAN");
  if (bg_scan && *bg_scan && kafs_parse_onoff(bg_scan, &opts->bg_dedup_scan_enabled) != 0)
  {
    fprintf(stderr, "invalid KAFS_BG_DEDUP_SCAN: '%s'\n", bg_scan);
    return 2;
  }
  if (kafs_main_parse_u32_env("KAFS_BG_DEDUP_INTERVAL_MS", getenv("KAFS_BG_DEDUP_INTERVAL_MS"),
                              KAFS_BG_DEDUP_INTERVAL_MS_MIN, KAFS_BG_DEDUP_INTERVAL_MS_MAX,
                              &opts->bg_dedup_interval_ms) != 0)
    return 2;
  if (kafs_main_parse_u32_env("KAFS_BG_DEDUP_QUIET_INTERVAL_MS",
                              getenv("KAFS_BG_DEDUP_QUIET_INTERVAL_MS"),
                              KAFS_BG_DEDUP_INTERVAL_MS_MIN, KAFS_BG_DEDUP_INTERVAL_MS_MAX,
                              &opts->bg_dedup_quiet_interval_ms) != 0)
    return 2;
  if (kafs_main_parse_u32_env("KAFS_BG_DEDUP_PRESSURE_INTERVAL_MS",
                              getenv("KAFS_BG_DEDUP_PRESSURE_INTERVAL_MS"),
                              KAFS_BG_DEDUP_INTERVAL_MS_MIN, KAFS_BG_DEDUP_INTERVAL_MS_MAX,
                              &opts->bg_dedup_pressure_interval_ms) != 0)
    return 2;
  if (kafs_main_parse_u32_env("KAFS_BG_DEDUP_START_USED_PCT",
                              getenv("KAFS_BG_DEDUP_START_USED_PCT"), 0, 100u,
                              &opts->bg_dedup_start_used_pct) != 0)
    return 2;
  if (kafs_main_parse_u32_env("KAFS_BG_DEDUP_PRESSURE_USED_PCT",
                              getenv("KAFS_BG_DEDUP_PRESSURE_USED_PCT"), 0, 100u,
                              &opts->bg_dedup_pressure_used_pct) != 0)
    return 2;

  const char *bg_prio = getenv("KAFS_BG_DEDUP_WORKER_PRIO");
  if (bg_prio && *bg_prio &&
      kafs_pending_worker_prio_mode_parse(bg_prio, &opts->bg_dedup_worker_prio_mode) != 0)
  {
    fprintf(stderr, "invalid KAFS_BG_DEDUP_WORKER_PRIO: '%s'\n", bg_prio);
    return 2;
  }
  return kafs_main_parse_nice_env("KAFS_BG_DEDUP_WORKER_NICE", getenv("KAFS_BG_DEDUP_WORKER_NICE"),
                                  &opts->bg_dedup_worker_nice);
}

static int kafs_main_apply_misc_env_overrides(kafs_main_options_t *opts)
{
  const char *fsp = getenv("KAFS_FSYNC_POLICY");
  if (fsp && *fsp && kafs_fsync_policy_parse(fsp, &opts->fsync_policy) != 0)
  {
    fprintf(stderr, "invalid KAFS_FSYNC_POLICY: '%s'\n", fsp);
    return 2;
  }

  const char *mt = getenv("KAFS_MT");
  opts->enable_mt = (mt && strcmp(mt, "1") == 0) ? KAFS_TRUE : KAFS_FALSE;
  return 0;
}

static int kafs_main_apply_env_overrides(kafs_main_options_t *opts)
{
  kafs_main_apply_optional_bool_env(getenv("KAFS_WRITEBACK_CACHE"), &opts->writeback_cache_enabled);
  kafs_main_apply_optional_bool_env(getenv("KAFS_TRIM_ON_FREE"), &opts->trim_on_free_enabled);

  if (kafs_main_apply_pending_env_overrides(opts) != 0)
    return 2;
  if (kafs_main_apply_pending_cap_env_overrides(opts) != 0)
    return 2;
  if (kafs_main_apply_bg_dedup_env_overrides(opts) != 0)
    return 2;
  return kafs_main_apply_misc_env_overrides(opts);
}

static int kafs_main_copy_cli_string_option(char *dst, size_t dst_size, const char *value,
                                            const char *error_message)
{
  if (!*value || snprintf(dst, dst_size, "%s", value) >= (int)dst_size)
  {
    fprintf(stderr, "%s\n", error_message);
    return 2;
  }
  return 0;
}

static int kafs_main_handle_flag_arg(kafs_main_options_t *opts, const char *arg)
{
  if (kafs_cli_is_help_arg(arg))
  {
    opts->show_help = KAFS_TRUE;
    return 1;
  }
  if (strcmp(arg, "--migrate") == 0 || strcmp(arg, "--migrate-v2") == 0)
  {
    opts->auto_migrate = KAFS_TRUE;
    return 1;
  }
  if (strcmp(arg, "--yes") == 0)
  {
    opts->migrate_yes = KAFS_TRUE;
    return 1;
  }
  if (strcmp(arg, "--no-writeback-cache") == 0)
  {
    opts->writeback_cache_enabled = KAFS_FALSE;
    opts->writeback_cache_explicit = KAFS_TRUE;
    return 1;
  }
  if (strcmp(arg, "--writeback-cache") == 0)
  {
    opts->writeback_cache_enabled = KAFS_TRUE;
    opts->writeback_cache_explicit = KAFS_TRUE;
    return 1;
  }
  if (strcmp(arg, "--trim-on-free") == 0)
  {
    opts->trim_on_free_enabled = KAFS_TRUE;
    opts->trim_on_free_explicit = KAFS_TRUE;
    return 1;
  }
  if (strcmp(arg, "--no-trim-on-free") == 0)
  {
    opts->trim_on_free_enabled = KAFS_FALSE;
    opts->trim_on_free_explicit = KAFS_TRUE;
    return 1;
  }
  if (strcmp(arg, "--hotplug") == 0)
  {
    snprintf(opts->hotplug_uds_opt, sizeof(opts->hotplug_uds_opt), "%s", KAFS_HOTPLUG_UDS_DEFAULT);
    return 1;
  }
  return 0;
}

static int kafs_main_handle_hotplug_arg(int argc, char **argv, const char *prog, int *index,
                                        kafs_main_options_t *opts, const char *arg)
{
  if (strncmp(arg, "--hotplug=", 10) == 0)
    return kafs_main_copy_cli_string_option(opts->hotplug_uds_opt, sizeof(opts->hotplug_uds_opt),
                                            arg + 10, "invalid --hotplug value");

  if (strcmp(arg, "--hotplug-uds") == 0)
  {
    if (*index + 1 >= argc)
    {
      fprintf(stderr, "--hotplug-uds requires a path argument.\n");
      usage(prog);
      return 2;
    }
    *index += 1;
    return kafs_main_copy_cli_string_option(opts->hotplug_uds_opt, sizeof(opts->hotplug_uds_opt),
                                            argv[*index], "invalid --hotplug-uds value");
  }

  if (strncmp(arg, "--hotplug-uds=", 14) == 0)
    return kafs_main_copy_cli_string_option(opts->hotplug_uds_opt, sizeof(opts->hotplug_uds_opt),
                                            arg + 14, "invalid --hotplug-uds value");

  if (strcmp(arg, "--hotplug-back-bin") == 0)
  {
    if (*index + 1 >= argc)
    {
      fprintf(stderr, "--hotplug-back-bin requires a path argument.\n");
      usage(prog);
      return 2;
    }
    *index += 1;
    return kafs_main_copy_cli_string_option(opts->hotplug_back_bin_opt,
                                            sizeof(opts->hotplug_back_bin_opt), argv[*index],
                                            "invalid --hotplug-back-bin value");
  }

  if (strncmp(arg, "--hotplug-back-bin=", 19) == 0)
    return kafs_main_copy_cli_string_option(opts->hotplug_back_bin_opt,
                                            sizeof(opts->hotplug_back_bin_opt), arg + 19,
                                            "invalid --hotplug-back-bin value");

  return 0;
}

static int kafs_main_handle_passthrough_arg(int argc, char **argv, const char *prog, int *index,
                                            kafs_main_options_t *opts, char **argv_clean,
                                            int *argc_clean, const char *arg)
{
  if (strcmp(arg, "--option") == 0)
  {
    if (*index + 1 >= argc)
    {
      fprintf(stderr, "--option requires an argument.\n");
      usage(prog);
      return 2;
    }
    argv_clean[(*argc_clean)++] = "-o";
    argv_clean[(*argc_clean)++] = argv[++(*index)];
    return 1;
  }

  if (strncmp(arg, "--option=", 9) == 0)
  {
    argv_clean[(*argc_clean)++] = "-o";
    argv_clean[(*argc_clean)++] = (char *)(arg + 9);
    return 1;
  }

  if (strcmp(arg, "--image") == 0)
  {
    if (*index + 1 >= argc)
    {
      fprintf(stderr, "--image requires a path argument.\n");
      usage(prog);
      return 2;
    }
    opts->image_path = argv[++(*index)];
    return 1;
  }

  if (strncmp(arg, "--image=", 8) == 0)
  {
    opts->image_path = arg + 8;
    return 1;
  }

  return 0;
}

static int kafs_main_collect_args(int argc, char **argv, const char *prog,
                                  kafs_main_options_t *opts, char **argv_clean, int *argc_clean)
{
  *argc_clean = 0;
  for (int i = 0; i < argc; ++i)
  {
    const char *a = argv[i];
    if (kafs_main_handle_flag_arg(opts, a) != 0)
      continue;

    int rc = kafs_main_handle_hotplug_arg(argc, argv, prog, &i, opts, a);
    if (rc == 2)
      return 2;
    if (rc == 1)
      continue;

    rc = kafs_main_handle_passthrough_arg(argc, argv, prog, &i, opts, argv_clean, argc_clean, a);
    if (rc == 2)
      return 2;
    if (rc == 1)
      continue;

    argv_clean[(*argc_clean)++] = argv[i];
  }
  return 0;
}

static void kafs_main_set_hotplug_default(kafs_main_options_t *opts)
{
  snprintf(opts->hotplug_uds_opt, sizeof(opts->hotplug_uds_opt), "%s", KAFS_HOTPLUG_UDS_DEFAULT);
}

static int kafs_main_handle_cache_hotplug_token(kafs_main_options_t *opts, const char *tok)
{
  if (strcmp(tok, "writeback_cache") == 0)
  {
    opts->writeback_cache_enabled = KAFS_TRUE;
    opts->writeback_cache_explicit = KAFS_TRUE;
    return 1;
  }
  if (strcmp(tok, "no_writeback_cache") == 0)
  {
    opts->writeback_cache_enabled = KAFS_FALSE;
    opts->writeback_cache_explicit = KAFS_TRUE;
    return 1;
  }
  if (strcmp(tok, "trim_on_free") == 0 || strcmp(tok, "trim-on-free") == 0)
  {
    opts->trim_on_free_enabled = KAFS_TRUE;
    opts->trim_on_free_explicit = KAFS_TRUE;
    return 1;
  }
  if (strcmp(tok, "no_trim_on_free") == 0 || strcmp(tok, "no-trim-on-free") == 0)
  {
    opts->trim_on_free_enabled = KAFS_FALSE;
    opts->trim_on_free_explicit = KAFS_TRUE;
    return 1;
  }
  if (strcmp(tok, "hotplug") == 0)
  {
    kafs_main_set_hotplug_default(opts);
    return 1;
  }

  const char *hotplug_uds_v = NULL;
  if (strncmp(tok, "hotplug=", 8) == 0)
    hotplug_uds_v = tok + 8;
  else if (strncmp(tok, "hotplug_uds=", 12) == 0)
    hotplug_uds_v = tok + 12;
  else if (strncmp(tok, "hotplug-uds=", 12) == 0)
    hotplug_uds_v = tok + 12;
  if (hotplug_uds_v)
  {
    if (!*hotplug_uds_v || snprintf(opts->hotplug_uds_opt, sizeof(opts->hotplug_uds_opt), "%s",
                                    hotplug_uds_v) >= (int)sizeof(opts->hotplug_uds_opt))
    {
      fprintf(stderr, "invalid -o hotplug uds path: '%s'\n", hotplug_uds_v);
      return 2;
    }
    return 1;
  }

  const char *hotplug_back_bin_v = NULL;
  if (strncmp(tok, "hotplug_back_bin=", 17) == 0)
    hotplug_back_bin_v = tok + 17;
  else if (strncmp(tok, "hotplug-back-bin=", 17) == 0)
    hotplug_back_bin_v = tok + 17;
  if (hotplug_back_bin_v)
  {
    if (!*hotplug_back_bin_v ||
        snprintf(opts->hotplug_back_bin_opt, sizeof(opts->hotplug_back_bin_opt), "%s",
                 hotplug_back_bin_v) >= (int)sizeof(opts->hotplug_back_bin_opt))
    {
      fprintf(stderr, "invalid -o hotplug_back_bin path: '%s'\n", hotplug_back_bin_v);
      return 2;
    }
    return 1;
  }

  return 0;
}

static int kafs_main_handle_mt_token(kafs_main_options_t *opts, const char *tok, int *want_mt)
{
  if (strncmp(tok, "max_threads=", 12) == 0 || strcmp(tok, "max_threads") == 0)
    opts->saw_max_threads = 1;

  if (strcmp(tok, "multi_thread") == 0 || strcmp(tok, "multi-thread") == 0 ||
      strcmp(tok, "multithread") == 0)
  {
    *want_mt = 1;
    return 1;
  }

  const char *vstr = NULL;
  if (strncmp(tok, "multi_thread=", 13) == 0)
    vstr = tok + 13;
  else if (strncmp(tok, "multi-thread=", 13) == 0)
    vstr = tok + 13;
  else if (strncmp(tok, "multithread=", 12) == 0)
    vstr = tok + 12;
  if (!vstr)
    return 0;

  char *endp = NULL;
  unsigned long v = strtoul(vstr, &endp, 10);
  if (!endp || *endp != '\0')
  {
    fprintf(stderr, "invalid -o multi_thread=N: '%s'\n", vstr);
    return 2;
  }
  if (v < 1)
    v = 1;
  if (v > 100000)
    v = 100000;
  opts->mt_cnt_override = (unsigned)v;
  opts->mt_cnt_override_set = 1;
  *want_mt = 1;
  return 1;
}

static const char *kafs_main_token_value_alias2(const char *tok, const char *prefix_a,
                                                const char *prefix_b)
{
  size_t prefix_a_len = strlen(prefix_a);
  if (strncmp(tok, prefix_a, prefix_a_len) == 0)
    return tok + prefix_a_len;

  size_t prefix_b_len = strlen(prefix_b);
  if (strncmp(tok, prefix_b, prefix_b_len) == 0)
    return tok + prefix_b_len;

  return NULL;
}

static int kafs_main_parse_token_prio_alias2(const char *tok, const char *prefix_a,
                                             const char *prefix_b,
                                             uint32_t *pending_worker_prio_mode,
                                             const char *error_label)
{
  const char *value = kafs_main_token_value_alias2(tok, prefix_a, prefix_b);
  if (!value)
    return 0;
  if (kafs_pending_worker_prio_mode_parse(value, pending_worker_prio_mode) != 0)
  {
    fprintf(stderr, "invalid -o %s: '%s'\n", error_label, value);
    return 2;
  }
  return 1;
}

static int kafs_main_parse_token_nice_alias2(const char *tok, const char *prefix_a,
                                             const char *prefix_b, int *nice_out,
                                             const char *error_label)
{
  const char *value = kafs_main_token_value_alias2(tok, prefix_a, prefix_b);
  if (!value)
    return 0;

  char *endp = NULL;
  long parsed = strtol(value, &endp, 10);
  if (!endp || *endp != '\0' || parsed < 0 || parsed > 19)
  {
    fprintf(stderr, "invalid -o %s: '%s'\n", error_label, value);
    return 2;
  }
  *nice_out = (int)parsed;
  return 1;
}

static int kafs_main_parse_token_u32(const char *tok, const char *prefix, uint32_t min_value,
                                     uint32_t max_value, uint32_t *value_out,
                                     const char *error_label)
{
  size_t prefix_len = strlen(prefix);
  if (strncmp(tok, prefix, prefix_len) != 0)
    return 0;
  if (kafs_parse_u32_range(tok + prefix_len, min_value, max_value, value_out) != 0)
  {
    fprintf(stderr, "invalid -o %s: '%s'\n", error_label, tok + prefix_len);
    return 2;
  }
  return 1;
}

static int kafs_main_parse_token_u32_alias2(const char *tok, const char *prefix_a,
                                            const char *prefix_b, uint32_t min_value,
                                            uint32_t max_value, uint32_t *value_out,
                                            const char *error_label)
{
  const char *value = kafs_main_token_value_alias2(tok, prefix_a, prefix_b);
  if (!value)
    return 0;
  if (kafs_parse_u32_range(value, min_value, max_value, value_out) != 0)
  {
    fprintf(stderr, "invalid -o %s: '%s'\n", error_label, value);
    return 2;
  }
  return 1;
}

static int kafs_main_parse_token_onoff_alias2(const char *tok, const char *prefix_a,
                                              const char *prefix_b, uint32_t *value_out,
                                              const char *error_label)
{
  const char *value = kafs_main_token_value_alias2(tok, prefix_a, prefix_b);
  if (!value)
    return 0;
  if (kafs_parse_onoff(value, value_out) != 0)
  {
    fprintf(stderr, "invalid -o %s: '%s'\n", error_label, value);
    return 2;
  }
  return 1;
}

static int kafs_main_handle_pending_token(kafs_main_options_t *opts, const char *tok)
{
  int rc = kafs_main_parse_token_prio_alias2(
      tok, "pending_worker_prio=", "dedup_worker_prio=", &opts->pending_worker_prio_mode,
      "pending_worker_prio");
  if (rc != 0)
    return rc;

  rc = kafs_main_parse_token_nice_alias2(tok, "pending_worker_nice=", "dedup_worker_nice=",
                                         &opts->pending_worker_nice, "pending_worker_nice");
  if (rc != 0)
    return rc;

  rc = kafs_main_parse_token_u32(tok, "pending_ttl_soft_ms=", 0, 3600000u,
                                 &opts->pending_ttl_soft_ms, "pending_ttl_soft_ms");
  if (rc != 0)
    return rc;

  rc = kafs_main_parse_token_u32(tok, "pending_ttl_hard_ms=", 0, 3600000u,
                                 &opts->pending_ttl_hard_ms, "pending_ttl_hard_ms");
  if (rc != 0)
    return rc;

  rc = kafs_main_parse_token_u32_alias2(tok, "pendinglog_cap_initial=", "pending_cap_initial=", 0,
                                        1000000000u, &opts->pending_cap_initial,
                                        "pendinglog_cap_initial");
  if (rc != 0)
    return rc;

  rc = kafs_main_parse_token_u32_alias2(tok, "pendinglog_cap_min=", "pending_cap_min=", 0,
                                        1000000000u, &opts->pending_cap_min, "pendinglog_cap_min");
  if (rc != 0)
    return rc;

  rc = kafs_main_parse_token_u32_alias2(tok, "pendinglog_cap_max=", "pending_cap_max=", 0,
                                        1000000000u, &opts->pending_cap_max, "pendinglog_cap_max");
  if (rc != 0)
    return rc;

  const char *fsp_str = kafs_main_token_value_alias2(tok, "fsync_policy=", "fsync_policy=");
  if (fsp_str)
  {
    if (kafs_fsync_policy_parse(fsp_str, &opts->fsync_policy) != 0)
    {
      fprintf(stderr, "invalid -o fsync_policy: '%s'\n", fsp_str);
      return 2;
    }
    return 1;
  }

  return 0;
}

static int kafs_main_handle_bg_dedup_token(kafs_main_options_t *opts, const char *tok)
{
  if (strcmp(tok, "bg_dedup_scan") == 0 || strcmp(tok, "bg_dedup_scan=on") == 0 ||
      strcmp(tok, "dedup_scan") == 0 || strcmp(tok, "dedup_scan=on") == 0)
  {
    opts->bg_dedup_scan_enabled = 1u;
    return 1;
  }
  if (strcmp(tok, "no_bg_dedup_scan") == 0 || strcmp(tok, "bg_dedup_scan=off") == 0 ||
      strcmp(tok, "no_dedup_scan") == 0 || strcmp(tok, "dedup_scan=off") == 0)
  {
    opts->bg_dedup_scan_enabled = 0u;
    return 1;
  }

  int rc = kafs_main_parse_token_onoff_alias2(
      tok, "bg_dedup_scan=", "dedup_scan=", &opts->bg_dedup_scan_enabled,
      "dedup_scan/bg_dedup_scan");
  if (rc != 0)
    return rc;

  rc = kafs_main_parse_token_u32_alias2(
      tok, "bg_dedup_interval_ms=", "dedup_interval_ms=", KAFS_BG_DEDUP_INTERVAL_MS_MIN,
      KAFS_BG_DEDUP_INTERVAL_MS_MAX, &opts->bg_dedup_interval_ms,
      "dedup_interval_ms/bg_dedup_interval_ms");
  if (rc != 0)
    return rc;

  rc = kafs_main_parse_token_u32_alias2(
      tok, "bg_dedup_quiet_interval_ms=", "dedup_quiet_interval_ms=", KAFS_BG_DEDUP_INTERVAL_MS_MIN,
      KAFS_BG_DEDUP_INTERVAL_MS_MAX, &opts->bg_dedup_quiet_interval_ms,
      "dedup_quiet_interval_ms/bg_dedup_quiet_interval_ms");
  if (rc != 0)
    return rc;

  rc = kafs_main_parse_token_u32_alias2(
      tok, "bg_dedup_pressure_interval_ms=", "dedup_pressure_interval_ms=",
      KAFS_BG_DEDUP_INTERVAL_MS_MIN, KAFS_BG_DEDUP_INTERVAL_MS_MAX,
      &opts->bg_dedup_pressure_interval_ms,
      "dedup_pressure_interval_ms/bg_dedup_pressure_interval_ms");
  if (rc != 0)
    return rc;

  rc = kafs_main_parse_token_u32_alias2(tok, "bg_dedup_start_used_pct=", "dedup_start_used_pct=", 0,
                                        100u, &opts->bg_dedup_start_used_pct,
                                        "dedup_start_used_pct/bg_dedup_start_used_pct");
  if (rc != 0)
    return rc;

  rc = kafs_main_parse_token_u32_alias2(
      tok, "bg_dedup_pressure_used_pct=", "dedup_pressure_used_pct=", 0, 100u,
      &opts->bg_dedup_pressure_used_pct, "dedup_pressure_used_pct/bg_dedup_pressure_used_pct");
  if (rc != 0)
    return rc;

  rc = kafs_main_parse_token_prio_alias2(
      tok, "bg_dedup_worker_prio=", "dedup_scan_worker_prio=", &opts->bg_dedup_worker_prio_mode,
      "bg_dedup_worker_prio/dedup_scan_worker_prio");
  if (rc != 0)
    return rc;

  rc = kafs_main_parse_token_nice_alias2(
      tok, "bg_dedup_worker_nice=", "dedup_scan_worker_nice=", &opts->bg_dedup_worker_nice,
      "bg_dedup_worker_nice/dedup_scan_worker_nice");
  if (rc != 0)
    return rc;

  return 0;
}

static void kafs_main_append_filtered_token(char *filtered, size_t *used, const char *tok)
{
  size_t tlen = strlen(tok);
  size_t need = tlen + (*used ? 1 : 0);
  if (!need)
    return;
  if (*used)
    filtered[(*used)++] = ',';
  memcpy(filtered + *used, tok, tlen);
  *used += tlen;
  filtered[*used] = '\0';
}

static int kafs_main_handle_mount_token(kafs_main_options_t *opts, const char *tok, int *want_mt)
{
  int rc = kafs_main_handle_cache_hotplug_token(opts, tok);
  if (rc != 0)
    return rc;

  rc = kafs_main_handle_mt_token(opts, tok, want_mt);
  if (rc != 0)
    return rc;

  rc = kafs_main_handle_pending_token(opts, tok);
  if (rc != 0)
    return rc;

  return kafs_main_handle_bg_dedup_token(opts, tok);
}

static int kafs_main_filter_mount_options(kafs_main_options_t *opts, char **argv_clean,
                                          int *argc_clean)
{
  char *argv_user[*argc_clean];
  int argc_user = 0;
  char *o_owned[*argc_clean];
  int o_owned_cnt = 0;

  for (int i = 0; i < *argc_clean; ++i)
  {
    const char *a = argv_clean[i];
    const char *oval = NULL;
    int is_compact = 0;
    if (strcmp(a, "-o") == 0)
    {
      if (i + 1 < *argc_clean)
        oval = argv_clean[++i];
      else
      {
        argv_user[argc_user++] = argv_clean[i];
        continue;
      }
    }
    else if (strncmp(a, "-o", 2) == 0 && a[2] != '\0')
    {
      oval = a + 2;
      is_compact = 1;
    }

    if (!oval)
    {
      argv_user[argc_user++] = argv_clean[i];
      continue;
    }

    char *dup = strdup(oval);
    if (!dup)
    {
      perror("strdup");
      return 2;
    }
    char filtered[strlen(oval) + 1];
    filtered[0] = '\0';
    size_t used = 0;
    int want_mt = 0;

    char *saveptr = NULL;
    for (char *tok = strtok_r(dup, ",", &saveptr); tok; tok = strtok_r(NULL, ",", &saveptr))
    {
      int rc = kafs_main_handle_mount_token(opts, tok, &want_mt);
      if (rc == 2)
      {
        free(dup);
        return 2;
      }
      if (rc == 1)
        continue;

      kafs_main_append_filtered_token(filtered, &used, tok);
    }

    free(dup);
    if (want_mt)
      opts->enable_mt = KAFS_TRUE;

    if (filtered[0] != '\0')
    {
      char *kept = NULL;
      if (is_compact)
      {
        kept = (char *)malloc(strlen(filtered) + 3);
        if (!kept)
        {
          perror("malloc");
          return 2;
        }
        kept[0] = '-';
        kept[1] = 'o';
        strcpy(kept + 2, filtered);
        argv_user[argc_user++] = kept;
      }
      else
      {
        kept = strdup(filtered);
        if (!kept)
        {
          perror("strdup");
          return 2;
        }
        argv_user[argc_user++] = "-o";
        argv_user[argc_user++] = kept;
      }
      o_owned[o_owned_cnt++] = kept;
    }
  }

  *argc_clean = argc_user;
  for (int i = 0; i < *argc_clean; ++i)
    argv_clean[i] = argv_user[i];

  (void)o_owned;
  (void)o_owned_cnt;
  return 0;
}

static int kafs_main_validate_options(const kafs_main_options_t *opts)
{
  if (opts->pending_ttl_soft_ms > 0 && opts->pending_ttl_hard_ms > 0 &&
      opts->pending_ttl_hard_ms < opts->pending_ttl_soft_ms)
  {
    fprintf(stderr, "invalid pending TTL: hard must be >= soft\n");
    return 2;
  }
  if (opts->pending_cap_min > 0 && opts->pending_cap_max > 0 &&
      opts->pending_cap_max < opts->pending_cap_min)
  {
    fprintf(stderr, "invalid pendinglog capacity: max must be >= min\n");
    return 2;
  }
  if (opts->bg_dedup_pressure_used_pct > 0 && opts->bg_dedup_start_used_pct > 0 &&
      opts->bg_dedup_pressure_used_pct < opts->bg_dedup_start_used_pct)
  {
    fprintf(stderr, "invalid bg dedup thresholds: pressure_used_pct must be >= start_used_pct\n");
    return 2;
  }
  return 0;
}

static void kafs_main_set_mountpoint(kafs_context_t *ctx, const char *mount_arg, char *mnt_abs,
                                     size_t mnt_abs_size)
{
  if (mount_arg && mount_arg[0] == '/')
  {
    snprintf(mnt_abs, mnt_abs_size, "%s", mount_arg);
    ctx->c_mountpoint = mnt_abs;
    return;
  }

  char cwd[PATH_MAX];
  if (getcwd(cwd, sizeof(cwd)) != NULL && mount_arg && mount_arg[0] != '\0')
  {
    if ((size_t)snprintf(mnt_abs, mnt_abs_size, "%s/%s", cwd, mount_arg) < mnt_abs_size)
    {
      ctx->c_mountpoint = mnt_abs;
      return;
    }
  }

  ctx->c_mountpoint = mount_arg;
}

static void kafs_main_init_context(kafs_context_t *ctx, const kafs_main_options_t *opts,
                                   const char *mount_arg, char *mnt_abs, size_t mnt_abs_size)
{
  memset(ctx, 0, sizeof(*ctx));
  kafs_main_set_mountpoint(ctx, mount_arg, mnt_abs, mnt_abs_size);

  ctx->c_hotplug_fd = -1;
  ctx->c_hotplug_state = KAFS_HOTPLUG_STATE_DISABLED;
  ctx->c_hotplug_wait_queue_limit = KAFS_HOTPLUG_WAIT_QUEUE_LIMIT_DEFAULT;
  ctx->c_hotplug_wait_timeout_ms = KAFS_HOTPLUG_WAIT_TIMEOUT_MS_DEFAULT;
  ctx->c_hotplug_data_mode = KAFS_RPC_DATA_INLINE;
  ctx->c_hotplug_front_major = KAFS_RPC_HELLO_MAJOR;
  ctx->c_hotplug_front_minor = KAFS_RPC_HELLO_MINOR;
  ctx->c_hotplug_front_features = KAFS_RPC_HELLO_FEATURES;
  ctx->c_hotplug_compat_result = KAFS_HOTPLUG_COMPAT_UNKNOWN;

  ctx->c_pending_worker_prio_mode = opts->pending_worker_prio_mode;
  ctx->c_pending_worker_nice = opts->pending_worker_nice;
  ctx->c_pending_worker_prio_base_mode = opts->pending_worker_prio_mode;
  ctx->c_pending_worker_nice_base = opts->pending_worker_nice;
  ctx->c_pending_worker_prio_dirty = 1;
  ctx->c_pending_ttl_soft_ms = opts->pending_ttl_soft_ms;
  ctx->c_pending_ttl_hard_ms = opts->pending_ttl_hard_ms;
  ctx->c_pendinglog_capacity = opts->pending_cap_initial;
  ctx->c_pendinglog_capacity_min = opts->pending_cap_min;
  ctx->c_pendinglog_capacity_max = opts->pending_cap_max;
  ctx->c_tombstone_gc_cursor = KAFS_INO_ROOTDIR + 1u;

  ctx->c_bg_dedup_enabled = opts->bg_dedup_scan_enabled;
  ctx->c_bg_dedup_interval_ms = opts->bg_dedup_interval_ms;
  ctx->c_bg_dedup_quiet_interval_ms = opts->bg_dedup_quiet_interval_ms;
  ctx->c_bg_dedup_pressure_interval_ms = opts->bg_dedup_pressure_interval_ms;
  ctx->c_bg_dedup_start_used_pct = opts->bg_dedup_start_used_pct;
  ctx->c_bg_dedup_pressure_used_pct = opts->bg_dedup_pressure_used_pct;
  ctx->c_bg_dedup_worker_prio_mode = opts->bg_dedup_worker_prio_mode;
  ctx->c_bg_dedup_worker_nice = opts->bg_dedup_worker_nice;
  ctx->c_bg_dedup_worker_prio_base_mode = opts->bg_dedup_worker_prio_mode;
  ctx->c_bg_dedup_worker_nice_base = opts->bg_dedup_worker_nice;
  ctx->c_bg_dedup_worker_prio_dirty = 1;
  ctx->c_bg_dedup_mode = KAFS_BG_DEDUP_MODE_COLD;

  ctx->c_fsync_policy = opts->fsync_policy;
}

static void kafs_main_apply_hotplug_env(kafs_context_t *ctx)
{
  const char *data_mode = getenv("KAFS_HOTPLUG_DATA_MODE");
  if (data_mode)
  {
    if (strcmp(data_mode, "inline") == 0)
      ctx->c_hotplug_data_mode = KAFS_RPC_DATA_INLINE;
    else if (strcmp(data_mode, "plan_only") == 0)
      ctx->c_hotplug_data_mode = KAFS_RPC_DATA_PLAN_ONLY;
    else if (strcmp(data_mode, "shm") == 0)
      ctx->c_hotplug_data_mode = KAFS_RPC_DATA_SHM;
  }

  const char *wait_timeout_env = getenv("KAFS_HOTPLUG_WAIT_TIMEOUT_MS");
  if (wait_timeout_env && *wait_timeout_env)
  {
    char *endp = NULL;
    unsigned long v = strtoul(wait_timeout_env, &endp, 10);
    if (endp && *endp == '\0')
      ctx->c_hotplug_wait_timeout_ms = (uint32_t)v;
  }

  const char *wait_limit_env = getenv("KAFS_HOTPLUG_WAIT_QUEUE_LIMIT");
  if (wait_limit_env && *wait_limit_env)
  {
    char *endp = NULL;
    unsigned long v = strtoul(wait_limit_env, &endp, 10);
    if (endp && *endp == '\0')
      ctx->c_hotplug_wait_queue_limit = (uint32_t)v;
  }
}

static int kafs_main_start_hotplug(kafs_context_t *ctx, const char *image_path,
                                   const char *hotplug_uds, const char *hotplug_back_bin,
                                   char *hotplug_uds_path, size_t hotplug_uds_path_size)
{
  if (!hotplug_uds)
    return 0;

  ctx->c_hotplug_state = KAFS_HOTPLUG_STATE_WAITING;
  snprintf(ctx->c_hotplug_uds_path, sizeof(ctx->c_hotplug_uds_path), "%s", hotplug_uds);
  (void)kafs_hotplug_env_set(ctx, "KAFS_HOTPLUG_UDS", hotplug_uds);
  if (image_path && *image_path)
    (void)kafs_hotplug_env_set(ctx, "KAFS_IMAGE", image_path);
  if (hotplug_back_bin && *hotplug_back_bin)
    (void)kafs_hotplug_env_set(ctx, "KAFS_HOTPLUG_BACK_BIN", hotplug_back_bin);

  if (!ctx->c_hotplug_wait_lock_init)
  {
    if (pthread_mutex_init(&ctx->c_hotplug_wait_lock, NULL) == 0 &&
        pthread_cond_init(&ctx->c_hotplug_wait_cond, NULL) == 0)
      ctx->c_hotplug_wait_lock_init = 1;
  }

  if (snprintf(hotplug_uds_path, hotplug_uds_path_size, "%s", hotplug_uds) >=
      (int)hotplug_uds_path_size)
  {
    ctx->c_hotplug_state = KAFS_HOTPLUG_STATE_ERROR;
    ctx->c_hotplug_last_error = -ENAMETOOLONG;
    fprintf(stderr, "hotplug: uds path too long\n");
    return 2;
  }

  int rc_hp = kafs_hotplug_wait_for_back(ctx, hotplug_uds_path, -1);
  if (rc_hp != 0)
  {
    ctx->c_hotplug_state = KAFS_HOTPLUG_STATE_ERROR;
    ctx->c_hotplug_last_error = rc_hp;
    fprintf(stderr, "hotplug: failed to accept back rc=%d\n", rc_hp);
    return 2;
  }
  return 0;
}

static void kafs_main_build_fuse_argv(char **argv_clean, int argc_clean, kafs_bool_t *enable_mt,
                                      int saw_max_threads, unsigned mt_cnt_override,
                                      int mt_cnt_override_set, char **argv_fuse, int *argc_fuse,
                                      char *mt_opt_buf, size_t mt_opt_buf_size)
{
  int saw_single = 0;
  for (int i = 0; i < argc_clean; ++i)
  {
    if (strcmp(argv_clean[i], "-s") == 0)
    {
      saw_single = 1;
      break;
    }
    argv_fuse[i] = argv_clean[i];
  }

  *argc_fuse = argc_clean;
  if (!*enable_mt && !saw_single)
    argv_fuse[(*argc_fuse)++] = "-s";
  if (*enable_mt && saw_single)
    *enable_mt = KAFS_FALSE;
  if (kafs_debug_level() >= 3)
    argv_fuse[(*argc_fuse)++] = "-d";

  if (*enable_mt && !saw_max_threads)
  {
    unsigned mt_cnt = 8;
    if (mt_cnt_override_set)
    {
      mt_cnt = mt_cnt_override;
    }
    else
    {
      const char *mt_env = getenv("KAFS_MAX_THREADS");
      if (mt_env && *mt_env)
      {
        char *endp = NULL;
        unsigned long v = strtoul(mt_env, &endp, 10);
        if (endp && *endp == '\0')
          mt_cnt = (unsigned)v;
      }
      if (mt_cnt < 1)
        mt_cnt = 1;
      if (mt_cnt > 100000)
        mt_cnt = 100000;
    }

    snprintf(mt_opt_buf, mt_opt_buf_size, "max_threads=%u", mt_cnt);
    argv_fuse[(*argc_fuse)++] = "-o";
    argv_fuse[(*argc_fuse)++] = mt_opt_buf;
    kafs_log(KAFS_LOG_INFO, "kafs: enabling multithread with -o %s\n", mt_opt_buf);
  }

  argv_fuse[*argc_fuse] = NULL;
}

static void kafs_main_log_runtime_options(kafs_context_t *ctx, kafs_bool_t writeback_cache_enabled,
                                          kafs_bool_t writeback_cache_explicit,
                                          kafs_bool_t trim_on_free_enabled,
                                          kafs_bool_t trim_on_free_explicit, int argc_fuse,
                                          char **argv_fuse)
{
  g_kafs_writeback_cache_enabled = writeback_cache_enabled ? 1 : 0;
  ctx->c_trim_on_free = trim_on_free_enabled ? 1u : 0u;

  kafs_log(KAFS_LOG_INFO, "kafs: writeback_cache %s (%s)\n",
           writeback_cache_enabled ? "enabled" : "disabled",
           writeback_cache_explicit ? "explicit" : "default");
  kafs_log(KAFS_LOG_INFO, "kafs: trim_on_free %s (%s)\n",
           trim_on_free_enabled ? "enabled" : "disabled",
           trim_on_free_explicit ? "explicit" : "default");

  if (kafs_debug_level() >= 1)
  {
    kafs_log(KAFS_LOG_INFO, "kafs: fuse argv (%d):\n", argc_fuse);
    for (int i = 0; i < argc_fuse; ++i)
      kafs_log(KAFS_LOG_INFO, "  argv[%d]=%s\n", i, argv_fuse[i]);
  }
}

static void kafs_main_validate_image_format(const char *image_path, uint32_t fmt_ver,
                                            kafs_bool_t auto_migrate, kafs_bool_t migrate_yes)
{
  if (fmt_ver == KAFS_FORMAT_VERSION)
    return;
  if (fmt_ver == KAFS_FORMAT_VERSION_V2 || fmt_ver == KAFS_FORMAT_VERSION_V3)
  {
    if (auto_migrate)
    {
      int mrc = kafs_core_migrate_image(image_path, migrate_yes ? 1 : 0);
      if (mrc == 0)
      {
        fprintf(stderr, "migration completed. please restart mount.\n");
        exit(0);
      }
      if (mrc == 1)
      {
        fprintf(stderr, "image already v%u; continue normal mount without --migrate.\n",
                (unsigned)KAFS_FORMAT_VERSION);
        exit(0);
      }
      if (mrc == -EPROTONOSUPPORT)
        fprintf(stderr, "offline migration supports only v2/v3 images.\n");
      else if (mrc == -ECANCELED)
        fprintf(stderr, "migration canceled by user.\n");
      else
        fprintf(stderr, "migration failed rc=%d.\n", mrc);
      exit(2);
    }
    fprintf(stderr,
            "unsupported format version: v%u.\n"
            "Run kafsctl migrate <image> or kafs --migrate before mounting.\n",
            (unsigned)fmt_ver);
    exit(2);
  }
  if (fmt_ver != KAFS_FORMAT_VERSION_V5)
  {
    fprintf(stderr, "unsupported format version: %u (expected %u).\n", fmt_ver,
            (unsigned)KAFS_FORMAT_VERSION);
    exit(2);
  }
}

static void kafs_main_map_runtime_image(kafs_context_t *ctx, const kafs_ssuperblock_t *sbdisk,
                                        uint32_t fmt_ver, kafs_inocnt_t *inocnt_out,
                                        kafs_blkcnt_t *r_blkcnt_out)
{
  kafs_logblksize_t log_blksize = kafs_sb_log_blksize_get(sbdisk);
  kafs_blksize_t blksize = 1u << log_blksize;
  kafs_blksize_t blksizemask = blksize - 1u;
  kafs_inocnt_t inocnt = kafs_inocnt_stoh(sbdisk->s_inocnt);
  kafs_blkcnt_t r_blkcnt = kafs_blkcnt_stoh(sbdisk->s_r_blkcnt);

  off_t mapsize = 0;
  mapsize += sizeof(kafs_ssuperblock_t);
  mapsize = (mapsize + blksizemask) & ~blksizemask;
  void *blkmask_off = (void *)mapsize;
  assert(sizeof(kafs_blkmask_t) <= 8);
  mapsize += (r_blkcnt + 7) >> 3;
  mapsize = (mapsize + 7) & ~7;
  mapsize = (mapsize + blksizemask) & ~blksizemask;
  void *inotbl_off = (void *)mapsize;
  mapsize += (off_t)kafs_inode_table_bytes_for_format(kafs_sb_format_version_get(sbdisk), inocnt);
  mapsize = (mapsize + blksizemask) & ~blksizemask;

  off_t imgsize = (off_t)r_blkcnt << log_blksize;
  {
    uint64_t idx_off = kafs_sb_hrl_index_offset_get(sbdisk);
    uint64_t idx_size = kafs_sb_hrl_index_size_get(sbdisk);
    uint64_t ent_off = kafs_sb_hrl_entry_offset_get(sbdisk);
    uint64_t ent_cnt = kafs_sb_hrl_entry_cnt_get(sbdisk);
    uint64_t ent_size = ent_cnt * (uint64_t)sizeof(kafs_hrl_entry_t);
    uint64_t j_off = kafs_sb_journal_offset_get(sbdisk);
    uint64_t j_size = kafs_sb_journal_size_get(sbdisk);
    uint64_t p_off = kafs_sb_pendinglog_offset_get(sbdisk);
    uint64_t p_size = kafs_sb_pendinglog_size_get(sbdisk);
    uint64_t end1 = (idx_off && idx_size) ? (idx_off + idx_size) : 0;
    uint64_t end2 = (ent_off && ent_size) ? (ent_off + ent_size) : 0;
    uint64_t end3 = (j_off && j_size) ? (j_off + j_size) : 0;
    uint64_t end4 = (p_off && p_size) ? (p_off + p_size) : 0;
    uint64_t max_end = end1;
    if (end2 > max_end)
      max_end = end2;
    if (end3 > max_end)
      max_end = end3;
    if (end4 > max_end)
      max_end = end4;
    if ((off_t)max_end > imgsize)
      imgsize = (off_t)max_end;
    imgsize = (imgsize + blksizemask) & ~blksizemask;
  }

  ctx->c_img_base = mmap(NULL, imgsize, PROT_READ | PROT_WRITE, MAP_SHARED, ctx->c_fd, 0);
  if (ctx->c_img_base == MAP_FAILED)
  {
    perror("mmap");
    exit(2);
  }
  ctx->c_img_size = (size_t)imgsize;
  ctx->c_superblock = (kafs_ssuperblock_t *)ctx->c_img_base;
  ctx->c_mapsize = (size_t)mapsize;
  ctx->c_blkmasktbl = (void *)ctx->c_superblock + (intptr_t)blkmask_off;
  ctx->c_inotbl = (void *)ctx->c_superblock + (intptr_t)inotbl_off;
  if (!kafs_ctx_runtime_mount_supported(ctx))
  {
    fprintf(stderr, "unsupported format version: %u (runtime admission failed).\n", fmt_ver);
    exit(2);
  }
  if (kafs_ctx_validate_runtime_mount_state(ctx) != 0)
  {
    fprintf(stderr, "invalid v5 tail metadata region; refusing runtime mount.\n");
    exit(2);
  }

  *inocnt_out = inocnt;
  *r_blkcnt_out = r_blkcnt;
}

static void kafs_main_init_runtime_diag(kafs_context_t *ctx, const char *image_path,
                                        kafs_inocnt_t inocnt)
{
  ctx->c_diag_log_fd = -1;
  ctx->c_ino_epoch = calloc((size_t)inocnt, sizeof(uint32_t));
  if (ctx->c_ino_epoch)
  {
    for (kafs_inocnt_t i = 0; i < inocnt; ++i)
      ctx->c_ino_epoch[i] = 1u;
  }
  if (kafs_extra_diag_enabled())
  {
    ctx->c_diag_create_seq = calloc((size_t)inocnt, sizeof(uint64_t));
    ctx->c_diag_create_mode = calloc((size_t)inocnt, sizeof(uint16_t));
    ctx->c_diag_create_first_write_seen = calloc((size_t)inocnt, sizeof(uint8_t));
    ctx->c_diag_create_paths = calloc((size_t)inocnt, KAFS_DIAG_CREATE_PATH_MAX);
    if (!ctx->c_diag_create_seq || !ctx->c_diag_create_mode ||
        !ctx->c_diag_create_first_write_seen || !ctx->c_diag_create_paths)
    {
      free(ctx->c_diag_create_seq);
      free(ctx->c_diag_create_mode);
      free(ctx->c_diag_create_first_write_seen);
      free(ctx->c_diag_create_paths);
      ctx->c_diag_create_seq = NULL;
      ctx->c_diag_create_mode = NULL;
      ctx->c_diag_create_first_write_seen = NULL;
      ctx->c_diag_create_paths = NULL;
    }
    ctx->c_diag_create_seq_next = 0;
  }
  kafs_diag_log_open(ctx, image_path);
  ctx->c_alloc_v3_summary_dirty = 1;
}

static void kafs_main_init_runtime_journal(kafs_context_t *ctx, const char *image_path,
                                           kafs_blkcnt_t r_blkcnt)
{
  (void)kafs_hrl_open(ctx);
  (void)kafs_journal_init(ctx, image_path);
  ctx->c_meta_delta_enabled = (uint32_t)kafs_journal_is_enabled(ctx);
  if (ctx->c_meta_delta_enabled)
  {
    size_t bits = sizeof(kafs_blkmask_t) * 8u;
    size_t words = ((size_t)r_blkcnt + bits - 1u) / bits;
    ctx->c_meta_bitmap_words = calloc(words, sizeof(kafs_blkmask_t));
    ctx->c_meta_bitmap_dirty = calloc(words, sizeof(uint8_t));
    if (!ctx->c_meta_bitmap_words || !ctx->c_meta_bitmap_dirty)
    {
      free(ctx->c_meta_bitmap_words);
      free(ctx->c_meta_bitmap_dirty);
      ctx->c_meta_bitmap_words = NULL;
      ctx->c_meta_bitmap_dirty = NULL;
      ctx->c_meta_bitmap_wordcnt = 0;
      ctx->c_meta_bitmap_dirty_count = 0;
      ctx->c_meta_bitmap_words_enabled = 0;
      ctx->c_meta_delta_enabled = 0;
    }
    else
    {
      memcpy(ctx->c_meta_bitmap_words, ctx->c_blkmasktbl, words * sizeof(kafs_blkmask_t));
      ctx->c_meta_bitmap_wordcnt = words;
      ctx->c_meta_bitmap_dirty_count = 0;
      ctx->c_meta_bitmap_words_enabled = 1u;
    }
  }

  (void)kafs_journal_replay(ctx, NULL, NULL);
  (void)kafs_pendinglog_init_or_load(ctx);
  if (ctx->c_pendinglog_enabled)
  {
    (void)kafs_pendinglog_replay_mount(ctx);
    kafs_journal_note(ctx, "PENDINGLOG", "loaded entries=%u cap=%u", kafs_pendinglog_count(ctx),
                      ctx->c_pendinglog_capacity);
  }
}

static void kafs_main_lock_runtime_image(kafs_context_t *ctx, const char *image_path)
{
  struct flock lk = {0};
  lk.l_type = F_WRLCK;
  lk.l_whence = SEEK_SET;
  lk.l_start = 0;
  lk.l_len = 0;
  if (fcntl(ctx->c_fd, F_SETLK, &lk) == -1)
  {
    perror("fcntl(F_SETLK)");
    fprintf(stderr, "image '%s' is busy (already mounted?).\n", image_path);
    exit(2);
  }
}

static void kafs_main_open_runtime_context(kafs_context_t *ctx, const char *image_path,
                                           kafs_bool_t auto_migrate, kafs_bool_t migrate_yes)
{
  ctx->c_fd = open(image_path, O_RDWR, 0666);
  if (ctx->c_fd < 0)
  {
    perror("open image");
    fprintf(stderr, "image not found. run mkfs.kafs first.\n");
    exit(2);
  }
  ctx->c_blo_search = 0;
  ctx->c_ino_search = 0;

  kafs_ssuperblock_t sbdisk;
  ssize_t r = pread(ctx->c_fd, &sbdisk, sizeof(sbdisk), 0);
  if (r != (ssize_t)sizeof(sbdisk))
  {
    perror("pread superblock");
    exit(2);
  }
  if (kafs_sb_magic_get(&sbdisk) != KAFS_MAGIC)
  {
    fprintf(stderr, "invalid magic. run mkfs.kafs to format.\n");
    exit(2);
  }

  uint32_t fmt_ver = kafs_sb_format_version_get(&sbdisk);
  kafs_inocnt_t inocnt = 0;
  kafs_blkcnt_t r_blkcnt = 0;
  kafs_main_validate_image_format(image_path, fmt_ver, auto_migrate, migrate_yes);
  kafs_main_map_runtime_image(ctx, &sbdisk, fmt_ver, &inocnt, &r_blkcnt);
  kafs_main_init_runtime_diag(ctx, image_path, inocnt);
  kafs_main_init_runtime_journal(ctx, image_path, r_blkcnt);
  kafs_main_lock_runtime_image(ctx, image_path);
}

static int kafs_main_cleanup(kafs_context_t *ctx, char *hotplug_uds_path, int rc)
{
  kafs_bg_dedup_worker_stop(ctx);
  kafs_pending_worker_stop(ctx);
  kafs_journal_shutdown(ctx);
  if (ctx->c_hotplug_fd >= 0)
    close(ctx->c_hotplug_fd);
  ctx->c_hotplug_active = 0;
  if (hotplug_uds_path[0] != '\0')
    unlink(hotplug_uds_path);
  if (ctx->c_hotplug_lock_init)
    pthread_mutex_destroy(&ctx->c_hotplug_lock);
  if (ctx->c_hotplug_wait_lock_init)
  {
    pthread_cond_destroy(&ctx->c_hotplug_wait_cond);
    pthread_mutex_destroy(&ctx->c_hotplug_wait_lock);
  }
  free(ctx->c_meta_bitmap_words);
  free(ctx->c_meta_bitmap_dirty);
  free(ctx->c_ino_epoch);
  free(ctx->c_diag_create_seq);
  free(ctx->c_diag_create_mode);
  free(ctx->c_diag_create_first_write_seen);
  free(ctx->c_diag_create_paths);
  kafs_diag_log_close(ctx);
  if (ctx->c_img_base && ctx->c_img_base != MAP_FAILED)
    munmap(ctx->c_img_base, ctx->c_img_size);
  return rc;
}

#ifndef KAFS_NO_MAIN
int main(int argc, char **argv)
{
  kafs_crash_diag_install("kafs");
  kafs_main_options_t opts;
  char *argv_clean[argc];
  int argc_clean = 0;

  kafs_main_options_init(&opts);
  if (kafs_main_apply_env_overrides(&opts) != 0 ||
      kafs_main_collect_args(argc, argv, argv[0], &opts, argv_clean, &argc_clean) != 0)
    return 2;

  if (opts.image_path == NULL && argc_clean >= 3 && argv_clean[1][0] != '-')
  {
    opts.image_path = argv_clean[1];
    for (int i = 1; i + 1 < argc_clean; ++i)
      argv_clean[i] = argv_clean[i + 1];
    argc_clean--;
  }

  if (opts.show_help)
  {
    usage(argv[0]);
    return 0;
  }
  if (opts.image_path == NULL || argc_clean < 2)
  {
    usage(argv[0]);
    return 2;
  }

  if (kafs_main_filter_mount_options(&opts, argv_clean, &argc_clean) != 0 ||
      kafs_main_validate_options(&opts) != 0)
    return 2;

  const char *image_path = opts.image_path;
  kafs_bool_t auto_migrate = opts.auto_migrate;
  kafs_bool_t migrate_yes = opts.migrate_yes;
  kafs_bool_t writeback_cache_enabled = opts.writeback_cache_enabled;
  kafs_bool_t writeback_cache_explicit = opts.writeback_cache_explicit;
  kafs_bool_t trim_on_free_enabled = opts.trim_on_free_enabled;
  kafs_bool_t trim_on_free_explicit = opts.trim_on_free_explicit;
  kafs_bool_t enable_mt = opts.enable_mt;
  unsigned mt_cnt_override = opts.mt_cnt_override;
  int mt_cnt_override_set = opts.mt_cnt_override_set;
  int saw_max_threads = opts.saw_max_threads;
  char *hotplug_uds_opt = opts.hotplug_uds_opt;
  char *hotplug_back_bin_opt = opts.hotplug_back_bin_opt;

  static kafs_context_t ctx;
  static char mnt_abs[PATH_MAX];
  char hotplug_uds_path[sizeof(((struct sockaddr_un *)0)->sun_path)];
  hotplug_uds_path[0] = '\0';
  kafs_main_init_context(&ctx, &opts, argv_clean[1], mnt_abs, sizeof(mnt_abs));
  kafs_main_apply_hotplug_env(&ctx);

  const char *hotplug_uds =
      hotplug_uds_opt[0] != '\0' ? hotplug_uds_opt : getenv("KAFS_HOTPLUG_UDS");
  const char *hotplug_back_bin =
      hotplug_back_bin_opt[0] != '\0' ? hotplug_back_bin_opt : getenv("KAFS_HOTPLUG_BACK_BIN");
  if (kafs_main_start_hotplug(&ctx, image_path, hotplug_uds, hotplug_back_bin, hotplug_uds_path,
                              sizeof(hotplug_uds_path)) != 0)
    return 2;

  kafs_main_open_runtime_context(&ctx, image_path, auto_migrate, migrate_yes);

  char *argv_fuse[argc_clean + 10];
  char mt_opt_buf[64];
  int argc_fuse = 0;
  kafs_main_build_fuse_argv(argv_clean, argc_clean, &enable_mt, saw_max_threads, mt_cnt_override,
                            mt_cnt_override_set, argv_fuse, &argc_fuse, mt_opt_buf,
                            sizeof(mt_opt_buf));
  kafs_main_log_runtime_options(&ctx, writeback_cache_enabled, writeback_cache_explicit,
                                trim_on_free_enabled, trim_on_free_explicit, argc_fuse, argv_fuse);
  fuse_set_log_func(kafs_fuse_log_func);
  int rc = fuse_main(argc_fuse, argv_fuse, &kafs_operations, &ctx);
  fuse_set_log_func(NULL);
  return kafs_main_cleanup(&ctx, hotplug_uds_path, rc);
}
#endif
