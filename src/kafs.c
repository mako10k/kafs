#include "kafs.h"
#include "kafs_context.h"
#include "kafs_superblock.h"
#include "kafs_block.h"
#include "kafs_inode.h"
#include "kafs_dirent.h"
#include "kafs_hash.h"
#include "kafs_journal.h"
#include "kafs_ioctl.h"
#include "kafs_mmap_io.h"
#include "kafs_rpc.h"
#include "kafs_core.h"

#define KAFS_DIRECT_SIZE (sizeof(((struct kafs_sinode *)NULL)->i_blkreftbl))

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

#define KAFS_PENDINGLOG_MAGIC 0x4b504c47u /* 'KPLG' */
#define KAFS_PENDINGLOG_VERSION 1u
#define KAFS_PENDING_REF_FLAG 0x80000000u
#define KAFS_PENDING_REF_MASK 0x7fffffffu
#define KAFS_PENDINGLOG_CAPACITY_FLOOR 2u
#define KAFS_BG_DEDUP_INTERVAL_MS_DEFAULT 250u
#define KAFS_BG_DEDUP_INTERVAL_MS_MIN 1u
#define KAFS_BG_DEDUP_INTERVAL_MS_MAX 60000u
#define KAFS_BG_DEDUP_IDLE_BURST_STEPS 64u
#define KAFS_BG_DEDUP_COOLDOWN_MS 2000u

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

static void kafs_pending_worker_adjust_priority_locked(struct kafs_context *ctx)
{
  if (!ctx || !ctx->c_pending_worker_running)
    return;

  kafs_pendinglog_hdr_t *hdr = kafs_pendinglog_hdr_ptr(ctx);
  if (!hdr || hdr->capacity == 0)
    return;

  // Hysteresis: boost near full, restore after queue is drained enough.
  kafs_pendinglog_adapt_capacity_locked(ctx);
  uint32_t cap = ctx->c_pendinglog_capacity;
  if (cap < KAFS_PENDINGLOG_CAPACITY_FLOOR)
    cap = KAFS_PENDINGLOG_CAPACITY_FLOOR;
  uint32_t high_wm = cap - (cap / 8u); // 87.5%
  uint32_t low_wm = cap / 2u;          // 50%
  if (high_wm <= low_wm)
    high_wm = low_wm + 1u;

  uint32_t qcnt = kafs_pendinglog_count(ctx);
  uint64_t oldest_age_ms = 0;
  uint32_t over_soft = 0;
  uint32_t over_hard = 0;

  if (qcnt > 0)
  {
    kafs_pendinglog_entry_t *head = kafs_pendinglog_entry_ptr(ctx, hdr->head);
    uint64_t now_rt_ns = kafs_now_realtime_ns();
    if (head && head->seq > 0)
    {
      // If timestamp looks far in the future (clock jump/corruption), ignore this sample.
      if (head->seq <= now_rt_ns + 300000000000ull && now_rt_ns >= head->seq)
        oldest_age_ms = (now_rt_ns - head->seq) / 1000000ull;
    }
  }

  if (ctx->c_pending_ttl_soft_ms > 0 && oldest_age_ms >= (uint64_t)ctx->c_pending_ttl_soft_ms)
    over_soft = 1;
  if (ctx->c_pending_ttl_hard_ms > 0 && oldest_age_ms >= (uint64_t)ctx->c_pending_ttl_hard_ms)
    over_hard = 1;

  ctx->c_pending_oldest_age_ms = oldest_age_ms;
  ctx->c_pending_ttl_over_soft = over_soft;
  ctx->c_pending_ttl_over_hard = over_hard;

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
  kafs_sinode_t *inoent = &ctx->c_inotbl[ino];
  if (kafs_ino_get_usage(inoent))
  {
    kafs_off_t cur_size = kafs_ino_size_get(inoent);
    if (cur_size > KAFS_DIRECT_SIZE)
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

  uint32_t idx = hdr->head;
  while (idx != hdr->tail)
  {
    kafs_pendinglog_entry_t *slot = kafs_pendinglog_entry_ptr(ctx, idx);
    if (!slot)
      return -EIO;

    if (slot->state == KAFS_PENDING_HASHED)
    {
      slot->state = KAFS_PENDING_QUEUED;
      if (slot->seq == 0)
        slot->seq = now_rt_ns;
      replay_requeued++;
    }
    else if (slot->state == KAFS_PENDING_RESOLVED)
    {
      int still_pending = 0;
      (void)kafs_pendinglog_inode_has_pending_id(ctx, slot->ino, slot->iblk, slot->pending_id,
                                                 &still_pending);
      if (still_pending)
      {
        slot->state = KAFS_PENDING_QUEUED;
        if (slot->seq == 0)
          slot->seq = now_rt_ns;
        replay_requeued++;
      }
    }
    idx = kafs_pendinglog_next_idx(hdr, idx);
  }

  while (hdr->head != hdr->tail)
  {
    kafs_pendinglog_entry_t *slot = kafs_pendinglog_entry_ptr(ctx, hdr->head);
    if (!slot)
      return -EIO;

    if (slot->state == KAFS_PENDING_FAILED)
    {
      hdr->head = kafs_pendinglog_next_idx(hdr, hdr->head);
      replay_dropped++;
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
        replay_dropped++;
        continue;
      }
      slot->state = KAFS_PENDING_QUEUED;
      if (slot->seq == 0)
        slot->seq = now_rt_ns;
      replay_requeued++;
    }

    break;
  }

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

static int kafs_pending_worker_apply_priority_self(kafs_context_t *ctx)
{
  if (!ctx)
    return -EINVAL;

  int err = 0;
  if (ctx->c_pending_worker_prio_mode == KAFS_PENDING_WORKER_PRIO_IDLE)
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

  if (ctx->c_pending_worker_nice < 0 || ctx->c_pending_worker_nice > 19)
  {
    if (err == 0)
      err = -ERANGE;
  }
  else
  {
#ifdef __linux__
    long tid = syscall(SYS_gettid);
    if (tid <= 0)
      tid = 0;
    if (setpriority(PRIO_PROCESS, (id_t)tid, ctx->c_pending_worker_nice) != 0 && err == 0)
      err = -errno;
#else
    if (setpriority(PRIO_PROCESS, 0, ctx->c_pending_worker_nice) != 0 && err == 0)
      err = -errno;
#endif
  }

  ctx->c_pending_worker_prio_apply_error = err;
  ctx->c_pending_worker_prio_dirty = 0;
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

static void kafs_bg_enter_cooldown(struct kafs_context *ctx)
{
  if (!ctx)
    return;
  ctx->c_bg_dedup_cooldown_until_ns =
      kafs_now_ns() + (uint64_t)KAFS_BG_DEDUP_COOLDOWN_MS * 1000000ull;
  kafs_bg_index_clear(ctx);
  __atomic_add_fetch(&ctx->c_stat_bg_dedup_cooldowns, 1u, __ATOMIC_RELAXED);
}

static void kafs_bg_dedup_step(struct kafs_context *ctx)
{
  if (!ctx || !ctx->c_superblock)
    return;
  if (ctx->c_hrl_bucket_cnt == 0)
    return;

  kafs_inocnt_t inocnt = kafs_sb_inocnt_get(ctx->c_superblock);
  if (inocnt == 0)
    return;

  uint64_t now_ns = kafs_now_ns();
  if (ctx->c_bg_dedup_cooldown_until_ns > now_ns)
    return;

  __atomic_add_fetch(&ctx->c_stat_bg_dedup_steps, 1u, __ATOMIC_RELAXED);

  kafs_blksize_t bs = kafs_sb_blksize_get(ctx->c_superblock);
  char buf[bs];

  for (kafs_inocnt_t scanned = 0; scanned < inocnt; ++scanned)
  {
    uint32_t ino = (ctx->c_bg_dedup_ino_cursor + scanned) % inocnt;
    kafs_inode_lock(ctx, ino);
    kafs_sinode_t *inoent = &ctx->c_inotbl[ino];
    if (!kafs_ino_get_usage(inoent))
    {
      kafs_inode_unlock(ctx, ino);
      if (scanned == 0)
      {
        if (kafs_bg_advance_cursor(ctx, inocnt, 0))
          kafs_bg_enter_cooldown(ctx);
      }
      continue;
    }

    kafs_off_t size = kafs_ino_size_get(inoent);
    if (size <= KAFS_DIRECT_SIZE)
    {
      kafs_inode_unlock(ctx, ino);
      if (scanned == 0)
      {
        if (kafs_bg_advance_cursor(ctx, inocnt, 0))
          kafs_bg_enter_cooldown(ctx);
      }
      continue;
    }

    kafs_iblkcnt_t iblocnt = (kafs_iblkcnt_t)((size + bs - 1) / bs);
    uint32_t direct_cnt = (iblocnt < 12) ? (uint32_t)iblocnt : 12u;
    if (direct_cnt == 0)
    {
      kafs_inode_unlock(ctx, ino);
      if (scanned == 0)
      {
        if (kafs_bg_advance_cursor(ctx, inocnt, 0))
          kafs_bg_enter_cooldown(ctx);
      }
      continue;
    }

    uint32_t start = (scanned == 0) ? (ctx->c_bg_dedup_iblk_cursor % direct_cnt) : 0u;
    for (uint32_t delta = 0; delta < direct_cnt; ++delta)
    {
      uint32_t iblk = (start + delta) % direct_cnt;
      uint32_t next_iblk = (iblk + 1u < direct_cnt) ? (iblk + 1u) : 0u;

      kafs_blkcnt_t raw = KAFS_BLO_NONE;
      if (kafs_ino_ibrk_run(ctx, inoent, (kafs_iblkcnt_t)iblk, &raw, KAFS_IBLKREF_FUNC_GET_RAW) !=
          0)
        continue;
      if (kafs_ref_is_pending(raw))
        continue;

      kafs_blkcnt_t old_blo = KAFS_BLO_NONE;
      if (kafs_ref_resolve_data_blo(ctx, raw, &old_blo) != 0 || old_blo == KAFS_BLO_NONE)
        continue;
      if (kafs_blk_read(ctx, old_blo, buf) != 0)
        continue;

      __atomic_add_fetch(&ctx->c_stat_bg_dedup_scanned_blocks, 1u, __ATOMIC_RELAXED);

      if (kafs_bg_advance_cursor(ctx, inocnt, next_iblk))
        kafs_bg_enter_cooldown(ctx);

      kafs_blkcnt_t match_blo = KAFS_BLO_NONE;
      int mrc = kafs_hrl_match_inc_by_block_excluding_blo(ctx, buf, old_blo, &match_blo);
      if (mrc == 0 && match_blo != KAFS_BLO_NONE)
      {
        kafs_blkcnt_t set_blo = match_blo;
        if (kafs_ino_ibrk_run(ctx, inoent, (kafs_iblkcnt_t)iblk, &set_blo, KAFS_IBLKREF_FUNC_SET) ==
            0)
        {
          __atomic_add_fetch(&ctx->c_stat_bg_dedup_replacements, 1u, __ATOMIC_RELAXED);
          kafs_inode_unlock(ctx, ino);
          (void)kafs_hrl_dec_ref_by_blo(ctx, old_blo);
          return;
        }
        (void)kafs_hrl_dec_ref_by_blo(ctx, match_blo);
      }

      uint64_t fast = kafs_bg_hash64(buf, bs);
      kafs_blkcnt_t dup_direct = kafs_bg_recent_find_dup_blo(ctx, fast, old_blo, buf);
      if (dup_direct == KAFS_BLO_NONE)
        dup_direct = kafs_bg_index_find_dup_blo(ctx, fast, old_blo, buf);
      kafs_bg_recent_note(ctx, fast, old_blo);
      kafs_bg_index_note(ctx, fast, old_blo);
      if (dup_direct == KAFS_BLO_NONE)
      {
        kafs_inode_unlock(ctx, ino);
        return;
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
        kafs_blkcnt_t set_blo = final_blo;
        if (kafs_ino_ibrk_run(ctx, inoent, (kafs_iblkcnt_t)iblk, &set_blo, KAFS_IBLKREF_FUNC_SET) ==
            0)
        {
          __atomic_add_fetch(&ctx->c_stat_bg_dedup_replacements, 1u, __ATOMIC_RELAXED);
          kafs_inode_unlock(ctx, ino);
          (void)kafs_hrl_dec_ref_by_blo(ctx, old_blo);
          return;
        }
        (void)kafs_hrl_dec_ref_by_blo(ctx, final_blo);
      }

      kafs_inode_unlock(ctx, ino);
      return;
    }

    kafs_inode_unlock(ctx, ino);
    if (scanned == 0)
    {
      if (kafs_bg_advance_cursor(ctx, inocnt, 0))
        kafs_bg_enter_cooldown(ctx);
    }
  }
}

static void *kafs_pending_worker_main(void *arg)
{
  kafs_context_t *ctx = (kafs_context_t *)arg;
  if (!ctx)
    return NULL;

  (void)kafs_pending_worker_apply_priority_self(ctx);

  for (;;)
  {
    uint32_t idx = 0;
    kafs_pendinglog_entry_t ent;
    int has_work = 0;

    pthread_mutex_lock(&ctx->c_pending_worker_lock);
    for (;;)
    {
      kafs_pendinglog_hdr_t *hdr = kafs_pendinglog_hdr_ptr(ctx);
      if (ctx->c_pending_worker_stop)
      {
        pthread_mutex_unlock(&ctx->c_pending_worker_lock);
        return NULL;
      }
      if (hdr && hdr->capacity > 0 && hdr->head != hdr->tail)
      {
        idx = hdr->head;
        kafs_pendinglog_entry_t *slot = kafs_pendinglog_entry_ptr(ctx, idx);
        if (slot)
        {
          ent = *slot;
          has_work = 1;
          break;
        }
      }

      if (!ctx->c_bg_dedup_enabled || ctx->c_bg_dedup_interval_ms == 0)
      {
        pthread_cond_wait(&ctx->c_pending_worker_cond, &ctx->c_pending_worker_lock);
        continue;
      }

      struct timespec wake;
      clock_gettime(CLOCK_REALTIME, &wake);
      wake.tv_nsec += (long)(ctx->c_bg_dedup_interval_ms % 1000u) * 1000000l;
      wake.tv_sec += (time_t)(ctx->c_bg_dedup_interval_ms / 1000u);
      if (wake.tv_nsec >= 1000000000l)
      {
        wake.tv_sec += 1;
        wake.tv_nsec -= 1000000000l;
      }

      int wrc =
          pthread_cond_timedwait(&ctx->c_pending_worker_cond, &ctx->c_pending_worker_lock, &wake);
      if (wrc == ETIMEDOUT)
        break;
    }
    pthread_mutex_unlock(&ctx->c_pending_worker_lock);

    if (ctx->c_pending_worker_prio_dirty)
      (void)kafs_pending_worker_apply_priority_self(ctx);

    if (!has_work)
    {
      if (ctx->c_bg_dedup_enabled)
      {
        // Idle windowでは64ステップ単位で進め、各チャンクごとに
        // pendingキューを再確認して、空なら連続実行する。
        for (;;)
        {
          for (uint32_t i = 0; i < KAFS_BG_DEDUP_IDLE_BURST_STEPS; ++i)
            kafs_bg_dedup_step(ctx);

          int keep_scanning = 0;
          pthread_mutex_lock(&ctx->c_pending_worker_lock);
          kafs_pendinglog_hdr_t *hdr = kafs_pendinglog_hdr_ptr(ctx);
          if (ctx->c_pending_worker_stop)
          {
            pthread_mutex_unlock(&ctx->c_pending_worker_lock);
            return NULL;
          }

          if (hdr && hdr->capacity > 0 && hdr->head != hdr->tail)
          {
            idx = hdr->head;
            kafs_pendinglog_entry_t *slot = kafs_pendinglog_entry_ptr(ctx, idx);
            if (slot)
            {
              ent = *slot;
              has_work = 1;
            }
          }
          else if (ctx->c_bg_dedup_enabled && ctx->c_bg_dedup_interval_ms > 0)
          {
            keep_scanning = 1;
          }
          pthread_mutex_unlock(&ctx->c_pending_worker_lock);

          if (has_work || !keep_scanning)
            break;
        }
      }

      if (!has_work)
        continue;
    }

    if (ent.state == KAFS_PENDING_RESOLVED || ent.state == KAFS_PENDING_FAILED)
    {
      pthread_mutex_lock(&ctx->c_pending_worker_lock);
      kafs_pendinglog_hdr_t *hdr = kafs_pendinglog_hdr_ptr(ctx);
      if (hdr && hdr->capacity > 0 && hdr->head == idx)
        hdr->head = kafs_pendinglog_next_idx(hdr, hdr->head);
      kafs_pending_worker_adjust_priority_locked(ctx);
      pthread_mutex_unlock(&ctx->c_pending_worker_lock);
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

        if (ent.ino < kafs_sb_inocnt_get(ctx->c_superblock))
        {
          kafs_inode_lock(ctx, ent.ino);
          kafs_sinode_t *inoent = &ctx->c_inotbl[ent.ino];
          if (kafs_ino_get_usage(inoent))
          {
            uint32_t cur_epoch = kafs_inode_epoch_get(ctx, ent.ino);
            if (ent.ino_epoch == 0 || ent.ino_epoch != cur_epoch)
            {
              kafs_inode_unlock(ctx, ent.ino);
              goto pending_finalize;
            }
            kafs_off_t cur_size = kafs_ino_size_get(inoent);
            if (cur_size > KAFS_DIRECT_SIZE)
            {
              kafs_blksize_t bs = kafs_sb_blksize_get(ctx->c_superblock);
              kafs_iblkcnt_t iblocnt = (kafs_iblkcnt_t)((cur_size + bs - 1) / bs);
              if (ent.iblk < iblocnt)
              {
                kafs_blkcnt_t cur_raw = KAFS_BLO_NONE;
                if (kafs_ino_ibrk_run(ctx, inoent, ent.iblk, &cur_raw, KAFS_IBLKREF_FUNC_GET_RAW) ==
                    0)
                {
                  if (kafs_ref_is_pending(cur_raw) &&
                      kafs_ref_pending_id(cur_raw) == ent.pending_id)
                  {
                    if (kafs_ino_ibrk_run(ctx, inoent, ent.iblk, &final_blo,
                                          KAFS_IBLKREF_FUNC_SET) == 0)
                      installed = 1;
                  }
                }
              }
            }
          }
          kafs_inode_unlock(ctx, ent.ino);
        }

      pending_finalize:;

        kafs_blkcnt_t old_blo = (kafs_blkcnt_t)ent.target_hrid;
        if (installed && old_blo != KAFS_BLO_NONE && old_blo != (kafs_blkcnt_t)ent.temp_blo &&
            old_blo != final_blo)
        {
          uint64_t t_dec0 = kafs_now_ns();
          (void)kafs_hrl_dec_ref_by_blo(ctx, old_blo);
          uint64_t t_dec1 = kafs_now_ns();
          __atomic_add_fetch(&ctx->c_stat_iblk_write_ns_dec_ref, t_dec1 - t_dec0, __ATOMIC_RELAXED);
        }

        if (!installed && final_blo != KAFS_BLO_NONE && final_blo != (kafs_blkcnt_t)ent.temp_blo)
          (void)kafs_hrl_dec_ref_by_blo(ctx, final_blo);

        if ((kafs_blkcnt_t)ent.temp_blo != KAFS_BLO_NONE &&
            (kafs_blkcnt_t)ent.temp_blo != final_blo)
          (void)kafs_hrl_dec_ref_by_blo(ctx, (kafs_blkcnt_t)ent.temp_blo);

        pthread_mutex_lock(&ctx->c_pending_worker_lock);
        kafs_pendinglog_hdr_t *hdr = kafs_pendinglog_hdr_ptr(ctx);
        kafs_pendinglog_entry_t *slot = hdr ? kafs_pendinglog_entry_ptr(ctx, idx) : NULL;
        if (hdr && slot)
        {
          slot->state = KAFS_PENDING_RESOLVED;
          slot->target_hrid = (uint32_t)hrid;
          slot->reserved0 = 0;
          if (hdr->head == idx)
            hdr->head = kafs_pendinglog_next_idx(hdr, hdr->head);
        }
        kafs_pending_worker_adjust_priority_locked(ctx);
        pthread_cond_broadcast(&ctx->c_pending_worker_cond);
        pthread_mutex_unlock(&ctx->c_pending_worker_lock);
        continue;
      }
    }

    pthread_mutex_lock(&ctx->c_pending_worker_lock);
    kafs_pendinglog_hdr_t *hdr = kafs_pendinglog_hdr_ptr(ctx);
    kafs_pendinglog_entry_t *slot = hdr ? kafs_pendinglog_entry_ptr(ctx, idx) : NULL;
    uint32_t retry = 0;
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

    if (retry > 0)
    {
      int hard_ttl_exceeded = 0;
      if (ctx->c_pending_ttl_hard_ms > 0 && ent.seq > 0)
      {
        uint64_t now_rt_ns = kafs_now_realtime_ns();
        if (now_rt_ns >= ent.seq)
        {
          uint64_t age_ms = (now_rt_ns - ent.seq) / 1000000ull;
          if (age_ms >= (uint64_t)ctx->c_pending_ttl_hard_ms)
            hard_ttl_exceeded = 1;
        }
      }

      uint32_t backoff_ms;
      if (hard_ttl_exceeded)
      {
        // Keep drain aggressive under hard TTL pressure, but avoid hot-spin starvation.
        backoff_ms = 1u;
      }
      else
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
  }
}

static int kafs_pending_worker_start(struct kafs_context *ctx)
{
  if (!ctx || !ctx->c_pendinglog_enabled)
    return 0;
  if (ctx->c_pending_worker_running)
    return 0;

  if (!ctx->c_pending_worker_lock_init)
  {
    if (pthread_mutex_init(&ctx->c_pending_worker_lock, NULL) != 0)
      return -EIO;
    if (pthread_cond_init(&ctx->c_pending_worker_cond, NULL) != 0)
    {
      pthread_mutex_destroy(&ctx->c_pending_worker_lock);
      return -EIO;
    }
    ctx->c_pending_worker_lock_init = 1;
  }

  ctx->c_pending_worker_stop = 0;
  int prc = pthread_create(&ctx->c_pending_worker_tid, NULL, kafs_pending_worker_main, ctx);
  if (prc != 0)
    return -prc;

  ctx->c_pending_worker_running = 1;
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
      return 1;
  return 0;
}

static int kafs_ino_ibrk_run(struct kafs_context *ctx, kafs_sinode_t *inoent, kafs_iblkcnt_t iblo,
                             kafs_blkcnt_t *pblo, kafs_iblkref_func_t ifunc)
{
  assert(ctx != NULL);
  assert(pblo != NULL);
  assert(inoent != NULL);
  kafs_dlog(3, "ibrk_run: iblo=%" PRIuFAST32 " ifunc=%d (size=%" PRIuFAST64 ")\n", iblo, (int)ifunc,
            kafs_ino_size_get(inoent));

  if (iblo < 12)
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
      // 直接参照に new blo を設定（中間テーブル不要）
      inoent->i_blkreftbl[iblo] = kafs_blkcnt_htos(*pblo);
      return KAFS_SUCCESS;
    }
  }

  iblo -= 12;
  kafs_blksize_t blksize = kafs_sb_blksize_get(ctx->c_superblock);
  kafs_logblksize_t log_blkrefs_pb = kafs_sb_log_blkref_pb_get(ctx->c_superblock);
  kafs_blkcnt_t blkrefs_pb = kafs_sb_blkref_pb_get(ctx->c_superblock);
  if (iblo < blkrefs_pb)
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
        // 新規に間接テーブルを割り当てた場合は全エントリを 0 で初期化する
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
        // 中間テーブルを割り当て
        KAFS_CALL(kafs_blk_alloc, ctx, &blo_blkreftbl);
        inoent->i_blkreftbl[12] = kafs_blkcnt_htos(blo_blkreftbl);
        memset(blkreftbl, 0, blksize);
      }
      else
      {
        KAFS_CALL(kafs_blk_read, ctx, blo_blkreftbl, blkreftbl);
      }
      blkreftbl[iblo] = kafs_blkcnt_htos(*pblo);
      KAFS_CALL(kafs_blk_write, ctx, blo_blkreftbl, blkreftbl);
      return KAFS_SUCCESS;
    }
  }

  iblo -= blkrefs_pb;
  kafs_logblksize_t log_blkrefs_pb_sq = log_blkrefs_pb << 1;
  kafs_blksize_t blkrefs_pb_sq = 1 << log_blkrefs_pb_sq;
  if (iblo < blkrefs_pb_sq)
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
      }
      else
      {
        KAFS_CALL(kafs_blk_read, ctx, blo_blkreftbl2, blkreftbl2);
      }
      blkreftbl2[iblo2] = kafs_blkcnt_htos(*pblo);
      KAFS_CALL(kafs_blk_write, ctx, blo_blkreftbl2, blkreftbl2);
      return KAFS_SUCCESS;
    }
  }

  iblo -= blkrefs_pb_sq;
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
    }
    else
    {
      KAFS_CALL(kafs_blk_read, ctx, blo_blkreftbl3, blkreftbl3);
    }
    blkreftbl3[iblo3] = kafs_blkcnt_htos(*pblo);
    KAFS_CALL(kafs_blk_write, ctx, blo_blkreftbl3, blkreftbl3);
    return KAFS_SUCCESS;
  }
  return KAFS_SUCCESS;
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
    if (!kafs_blk_is_zero(tbl, blksize))
    {
      *free_blo1 = blo_tbl;
      inoent->i_blkreftbl[12] = kafs_blkcnt_htos(KAFS_BLO_NONE);
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
    if (!kafs_blk_is_zero(tbl2, blksize))
    {
      *free_blo1 = blo_tbl2;
      tbl1[ib1] = kafs_blkcnt_htos(KAFS_BLO_NONE);
      KAFS_CALL(kafs_blk_write, ctx, blo_tbl1, tbl1);
      // 親も空なら切り離し
      if (!kafs_blk_is_zero(tbl1, blksize))
      {
        *free_blo2 = blo_tbl1;
        inoent->i_blkreftbl[13] = kafs_blkcnt_htos(KAFS_BLO_NONE);
      }
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
  if (!kafs_blk_is_zero(tbl3, blksize))
  {
    *free_blo1 = blo_tbl3;
    tbl2[ib2] = kafs_blkcnt_htos(KAFS_BLO_NONE);
    KAFS_CALL(kafs_blk_write, ctx, blo_tbl2, tbl2);
    if (!kafs_blk_is_zero(tbl2, blksize))
    {
      *free_blo2 = blo_tbl2;
      tbl1[ib1] = kafs_blkcnt_htos(KAFS_BLO_NONE);
      KAFS_CALL(kafs_blk_write, ctx, blo_tbl1, tbl1);
      if (!kafs_blk_is_zero(tbl1, blksize))
      {
        *free_blo3 = blo_tbl1;
        inoent->i_blkreftbl[14] = kafs_blkcnt_htos(KAFS_BLO_NONE);
      }
    }
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
  kafs_dlog(3, "%s(ino = %d, iblo = %" PRIuFAST32 ")\n", __func__, inoent - ctx->c_inotbl, iblo);
  assert(ctx != NULL);
  assert(buf != NULL);
  assert(inoent != NULL);
  assert(kafs_ino_get_usage(inoent));
  kafs_blkcnt_t blo;
  KAFS_CALL(kafs_ino_ibrk_run, ctx, inoent, iblo, &blo, KAFS_IBLKREF_FUNC_GET);
  KAFS_CALL(kafs_blk_read, ctx, blo, buf);
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
  kafs_dlog(3, "%s(ino = %d, iblo = %" PRIuFAST32 ")\n", __func__, inoent - ctx->c_inotbl, iblo);
  assert(ctx != NULL);
  assert(buf != NULL);
  assert(inoent != NULL);
  assert(kafs_ino_get_usage(inoent));
  if (ctx->c_pendinglog_enabled && ctx->c_pending_worker_running)
  {
    kafs_blkcnt_t temp_blo = KAFS_BLO_NONE;
    KAFS_CALL(kafs_blk_alloc, ctx, &temp_blo);
    uint64_t t_lw0 = kafs_now_ns();
    KAFS_CALL(kafs_blk_write, ctx, temp_blo, buf);
    uint64_t t_lw1 = kafs_now_ns();
    __atomic_add_fetch(&ctx->c_stat_iblk_write_ns_legacy_blk_write, t_lw1 - t_lw0,
                       __ATOMIC_RELAXED);

    uint32_t ino_idx = (uint32_t)(inoent - ctx->c_inotbl);
    kafs_blkcnt_t old_raw = KAFS_BLO_NONE;
    kafs_blkcnt_t old_blo = KAFS_BLO_NONE;
    uint32_t ino_epoch = kafs_inode_epoch_get(ctx, ino_idx);
    KAFS_CALL(kafs_ino_ibrk_run, ctx, inoent, iblo, &old_raw, KAFS_IBLKREF_FUNC_GET_RAW);
    if (!kafs_ref_is_pending(old_raw))
      KAFS_CALL(kafs_ref_resolve_data_blo, ctx, old_raw, &old_blo);

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
      (void)kafs_hrl_dec_ref_by_blo(ctx, temp_blo);
      if (qrc != -ENOSPC)
        return qrc;

      uint32_t c = __atomic_fetch_add(&s_pendinglog_full_warned, 1u, __ATOMIC_RELAXED);
      if (c < 8u)
      {
        kafs_log(KAFS_LOG_WARNING,
                 "%s: pendinglog full (ino=%" PRIu32 ", iblk=%" PRIu32
                 "), fallback to sync write\n",
                 __func__, ino_idx, (uint32_t)iblo);
      }
    }

    if (qrc == 0)
    {
      kafs_blkcnt_t pref = KAFS_BLO_NONE;
      KAFS_CALL(kafs_ref_pending_encode, pending_id, &pref);
      KAFS_CALL(kafs_ino_ibrk_run, ctx, inoent, iblo, &pref, KAFS_IBLKREF_FUNC_SET);

      kafs_pending_worker_notify(ctx);
      return KAFS_SUCCESS;
    }
  }

  // ゼロ/非ゼロを区別せず、常に通常のデータ書き込み経路を使う。
  kafs_hrid_t hrid;
  int is_new = 0;
  kafs_blkcnt_t new_blo = KAFS_BLO_NONE;
  ctx->c_stat_hrl_put_calls++;
  uint64_t t_hrl0 = kafs_now_ns();
  int rc = kafs_hrl_put(ctx, buf, &hrid, &is_new, &new_blo);
  uint64_t t_hrl1 = kafs_now_ns();
  __atomic_add_fetch(&ctx->c_stat_iblk_write_ns_hrl_put, t_hrl1 - t_hrl0, __ATOMIC_RELAXED);
  if (rc == 0)
  {
    if (is_new)
      ctx->c_stat_hrl_put_misses++;
    else
      ctx->c_stat_hrl_put_hits++;
    // kafs_hrl_put() already takes one reference for the returned hrid.
    kafs_blkcnt_t old_blo;
    KAFS_CALL(kafs_ino_ibrk_run, ctx, inoent, iblo, &old_blo, KAFS_IBLKREF_FUNC_GET);
    KAFS_CALL(kafs_ino_ibrk_run, ctx, inoent, iblo, &new_blo, KAFS_IBLKREF_FUNC_SET);
    // HRLバケットロック順序のため、参照減算は inode ロック外で行う
    uint32_t ino_idx = (uint32_t)(inoent - ctx->c_inotbl);
    if (old_blo != KAFS_BLO_NONE && old_blo != new_blo)
    {
      kafs_inode_unlock(ctx, ino_idx);
      uint64_t t_dec0 = kafs_now_ns();
      (void)kafs_hrl_dec_ref_by_blo(ctx, old_blo);
      uint64_t t_dec1 = kafs_now_ns();
      __atomic_add_fetch(&ctx->c_stat_iblk_write_ns_dec_ref, t_dec1 - t_dec0, __ATOMIC_RELAXED);
      kafs_inode_lock(ctx, ino_idx);
    }
    return KAFS_SUCCESS;
  }

  // HRL 失敗時: レガシー経路
  ctx->c_stat_hrl_put_fallback_legacy++;
  // 先に物理ブロックを割り当ててデータを書き込み、その後参照を更新する
  kafs_blkcnt_t new_blo2 = KAFS_BLO_NONE;
  KAFS_CALL(kafs_blk_alloc, ctx, &new_blo2);
  // 先にデータを書き込む（読取り側はまだ旧参照を見続ける）
  uint64_t t_lw0 = kafs_now_ns();
  KAFS_CALL(kafs_blk_write, ctx, new_blo2, buf);
  uint64_t t_lw1 = kafs_now_ns();
  __atomic_add_fetch(&ctx->c_stat_iblk_write_ns_legacy_blk_write, t_lw1 - t_lw0, __ATOMIC_RELAXED);
  kafs_blkcnt_t old_blo2;
  KAFS_CALL(kafs_ino_ibrk_run, ctx, inoent, iblo, &old_blo2, KAFS_IBLKREF_FUNC_GET);
  KAFS_CALL(kafs_ino_ibrk_run, ctx, inoent, iblo, &new_blo2, KAFS_IBLKREF_FUNC_SET);
  // 旧ブロックの参照を解放（inode ロック外）
  if (old_blo2 != KAFS_BLO_NONE && old_blo2 != new_blo2)
  {
    uint32_t ino_idx = (uint32_t)(inoent - ctx->c_inotbl);
    kafs_inode_unlock(ctx, ino_idx);
    uint64_t t_dec0 = kafs_now_ns();
    (void)kafs_hrl_dec_ref_by_blo(ctx, old_blo2);
    uint64_t t_dec1 = kafs_now_ns();
    __atomic_add_fetch(&ctx->c_stat_iblk_write_ns_dec_ref, t_dec1 - t_dec0, __ATOMIC_RELAXED);
    kafs_inode_lock(ctx, ino_idx);
  }
  return KAFS_SUCCESS;
}

__attribute_maybe_unused__ static int
kafs_ino_iblk_release(struct kafs_context *ctx, kafs_sinode_t *inoent, kafs_iblkcnt_t iblo)
{
  kafs_dlog(3, "%s(ino = %d, iblo = %" PRIuFAST32 ")\n", __func__, inoent - ctx->c_inotbl, iblo);
  assert(ctx != NULL);
  assert(inoent != NULL);
  assert(kafs_ino_get_usage(inoent));
  assert(kafs_ino_size_get(inoent) > KAFS_DIRECT_SIZE);
  kafs_blkcnt_t old;
  KAFS_CALL(kafs_ino_ibrk_run, ctx, inoent, iblo, &old, KAFS_IBLKREF_FUNC_GET);
  if (old != KAFS_BLO_NONE)
  {
    kafs_blkcnt_t none = KAFS_BLO_NONE;
    KAFS_CALL(kafs_ino_ibrk_run, ctx, inoent, iblo, &none, KAFS_IBLKREF_FUNC_SET);
    // 空になった中間テーブルを切り離し（inode ロック内）
    kafs_blkcnt_t f1, f2, f3;
    KAFS_CALL(kafs_ino_prune_empty_indirects, ctx, inoent, iblo, &f1, &f2, &f3);
    // dec_ref は inode ロック外で実施
    uint32_t ino_idx = (uint32_t)(inoent - ctx->c_inotbl);
    kafs_inode_unlock(ctx, ino_idx);
    (void)kafs_hrl_dec_ref_by_blo(ctx, old);
    if (f1 != KAFS_BLO_NONE)
      (void)kafs_hrl_dec_ref_by_blo(ctx, f1);
    if (f2 != KAFS_BLO_NONE)
      (void)kafs_hrl_dec_ref_by_blo(ctx, f2);
    if (f3 != KAFS_BLO_NONE)
      (void)kafs_hrl_dec_ref_by_blo(ctx, f3);
    kafs_inode_lock(ctx, ino_idx);
  }
  return KAFS_SUCCESS;
}

static ssize_t kafs_pread(struct kafs_context *ctx, kafs_sinode_t *inoent, void *buf,
                          kafs_off_t size, kafs_off_t offset)
{
  assert(inoent != NULL);
  assert(kafs_ino_get_usage(inoent));
  kafs_off_t filesize = kafs_ino_size_get(inoent);
  if (offset >= filesize)
    return 0;
  if (offset + size > filesize)
    size = filesize - offset;
  if (size == 0)
    return 0;
  // 60バイト以下は直接
  if (filesize <= KAFS_DIRECT_SIZE)
  {
    memcpy(buf, (void *)inoent->i_blkreftbl + offset, size);
    return size;
  }
  size_t size_read = 0;
  kafs_logblksize_t log_blksize = kafs_sb_log_blksize_get(ctx->c_superblock);
  kafs_blksize_t blksize = kafs_sb_blksize_get(ctx->c_superblock);
  kafs_blksize_t offset_blksize = offset & (blksize - 1);
  if (offset_blksize > 0 || size - size_read < blksize)
  {
    char rbuf[blksize];
    kafs_iblkcnt_t iblo = offset >> log_blksize;
    KAFS_CALL(kafs_ino_iblk_read_or_zero, ctx, inoent, iblo, rbuf);
    if (size < blksize - offset_blksize)
    {
      memcpy(buf, rbuf + offset_blksize, size);
      return size;
    }
    memcpy(buf, rbuf + offset_blksize, blksize - offset_blksize);
    size_read += blksize - offset_blksize;
  }
  while (size_read < size)
  {
    kafs_iblkcnt_t iblo = (offset + size_read) >> log_blksize;
    if (size - size_read <= blksize)
    {
      char rbuf[blksize];
      KAFS_CALL(kafs_ino_iblk_read_or_zero, ctx, inoent, iblo, rbuf);
      memcpy(buf + size_read, rbuf, size - size_read);
      return size;
    }
    KAFS_CALL(kafs_ino_iblk_read_or_zero, ctx, inoent, iblo, buf + size_read);
    size_read += blksize;
  }
  return size;
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
  kafs_dlog(3, "%s(ino = %d, size = %" PRIuFAST64 ", offset = %" PRIuFAST64 ")\n", __func__,
            inoent - ctx->c_inotbl, size, offset);
  assert(ctx != NULL);
  assert(buf != NULL);
  assert(inoent != NULL);
  assert(kafs_ino_get_usage(inoent));

  kafs_off_t filesize = kafs_ino_size_get(inoent);
  kafs_logblksize_t log_blksize = kafs_sb_log_blksize_get(ctx->c_superblock);
  kafs_blksize_t blksize = kafs_sb_blksize_get(ctx->c_superblock);
  kafs_off_t filesize_new = offset + size;

  if (size == 0)
    return 0;

  __atomic_add_fetch(&ctx->c_stat_pwrite_calls, 1u, __ATOMIC_RELAXED);
  __atomic_add_fetch(&ctx->c_stat_pwrite_bytes, (uint64_t)size, __ATOMIC_RELAXED);

  if (filesize < filesize_new)
  {
    // サイズ拡大時
    kafs_ino_size_set(inoent, filesize_new);
    if (filesize != 0 && filesize <= KAFS_DIRECT_SIZE && filesize_new > KAFS_DIRECT_SIZE)
    {
      char wbuf[blksize];
      memset(wbuf, 0, blksize);
      memcpy(wbuf, inoent->i_blkreftbl, filesize);
      memset(inoent->i_blkreftbl, 0, sizeof(inoent->i_blkreftbl));
      KAFS_CALL(kafs_ino_iblk_write, ctx, inoent, 0, wbuf);
    }
    filesize = filesize_new;
  }

  size_t size_written = 0;

  // 60バイト以下は直接
  if (filesize <= sizeof(inoent->i_blkreftbl))
  {
    memcpy((void *)inoent->i_blkreftbl + offset, buf, size);
    return size;
  }

  kafs_blksize_t offset_blksize = offset & (blksize - 1);
  if (offset_blksize > 0 || size - size_written < blksize)
  {
    // 1ブロック目で端数が出る場合
    kafs_iblkcnt_t iblo = offset >> log_blksize;
    // 書き戻しバッファ
    char wbuf[blksize];
    uint64_t t_r0 = kafs_now_ns();
    KAFS_CALL(kafs_ino_iblk_read_or_zero, ctx, inoent, iblo, wbuf);
    uint64_t t_r1 = kafs_now_ns();
    __atomic_add_fetch(&ctx->c_stat_pwrite_ns_iblk_read, t_r1 - t_r0, __ATOMIC_RELAXED);
    if (size < blksize - offset_blksize)
    {
      // 1ブロックのみの場合
      memcpy(wbuf + offset_blksize, buf, size);
      uint64_t t_w0 = kafs_now_ns();
      KAFS_CALL(kafs_ino_iblk_write, ctx, inoent, iblo, wbuf);
      uint64_t t_w1 = kafs_now_ns();
      uint64_t d = t_w1 - t_w0;
      __atomic_add_fetch(&ctx->c_stat_pwrite_ns_iblk_write, d, __ATOMIC_RELAXED);
      kafs_stat_record_pwrite_iblk_write_latency(ctx, d);
      return size;
    }
    // ブロックの残り分を書き込む
    memcpy(wbuf + offset_blksize, buf, blksize - offset_blksize);
    uint64_t t_w0 = kafs_now_ns();
    KAFS_CALL(kafs_ino_iblk_write, ctx, inoent, iblo, wbuf);
    uint64_t t_w1 = kafs_now_ns();
    uint64_t d = t_w1 - t_w0;
    __atomic_add_fetch(&ctx->c_stat_pwrite_ns_iblk_write, d, __ATOMIC_RELAXED);
    kafs_stat_record_pwrite_iblk_write_latency(ctx, d);
    size_written += blksize - offset_blksize;
  }

  while (size_written < size)
  {
    kafs_iblkcnt_t iblo = (offset + size_written) >> log_blksize;
    if (size - size_written < blksize)
    {
      char wbuf[blksize];
      uint64_t t_r0 = kafs_now_ns();
      KAFS_CALL(kafs_ino_iblk_read_or_zero, ctx, inoent, iblo, wbuf);
      uint64_t t_r1 = kafs_now_ns();
      __atomic_add_fetch(&ctx->c_stat_pwrite_ns_iblk_read, t_r1 - t_r0, __ATOMIC_RELAXED);
      memcpy(wbuf, buf + size_written, size - size_written);
      uint64_t t_w0 = kafs_now_ns();
      KAFS_CALL(kafs_ino_iblk_write, ctx, inoent, iblo, wbuf);
      uint64_t t_w1 = kafs_now_ns();
      uint64_t d = t_w1 - t_w0;
      __atomic_add_fetch(&ctx->c_stat_pwrite_ns_iblk_write, d, __ATOMIC_RELAXED);
      kafs_stat_record_pwrite_iblk_write_latency(ctx, d);
      return size;
    }
    uint64_t t_w0 = kafs_now_ns();
    KAFS_CALL(kafs_ino_iblk_write, ctx, inoent, iblo, buf + size_written);
    uint64_t t_w1 = kafs_now_ns();
    uint64_t d = t_w1 - t_w0;
    __atomic_add_fetch(&ctx->c_stat_pwrite_ns_iblk_write, d, __ATOMIC_RELAXED);
    kafs_stat_record_pwrite_iblk_write_latency(ctx, d);
    size_written += blksize;
  }
  return size;
}

static int kafs_truncate(struct kafs_context *ctx, kafs_sinode_t *inoent, kafs_off_t filesize_new)
{
  kafs_dlog(2, "%s(ino = %d, filesize_new = %" PRIuFAST64 ")\n", __func__, inoent - ctx->c_inotbl,
            filesize_new);
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
#define KAFS_TRUNC_PUSH_FREE(_blo)                                                                 \
  do                                                                                               \
  {                                                                                                \
    kafs_blkcnt_t __b = (_blo);                                                                    \
    if (__b != KAFS_BLO_NONE)                                                                      \
    {                                                                                              \
      kafs_dlog(2, "%s: ino=%u queue free blo=%" PRIuFAST32 "\n", __func__, ino_idx, __b);         \
      if (deferred_free_cnt == deferred_free_cap)                                                  \
      {                                                                                            \
        size_t new_cap = deferred_free_cap ? (deferred_free_cap << 1) : 256u;                      \
        kafs_blkcnt_t *nw = realloc(deferred_free, new_cap * sizeof(*nw));                         \
        if (!nw)                                                                                   \
        {                                                                                          \
          free(deferred_free);                                                                     \
          return -ENOMEM;                                                                          \
        }                                                                                          \
        deferred_free = nw;                                                                        \
        deferred_free_cap = new_cap;                                                               \
      }                                                                                            \
      deferred_free[deferred_free_cnt++] = __b;                                                    \
    }                                                                                              \
  } while (0)
  (void)kafs_inode_epoch_bump(ctx, (uint32_t)(inoent - ctx->c_inotbl));
  if (filesize_new > filesize_orig)
  {
    if (filesize_orig <= KAFS_DIRECT_SIZE && filesize_new > KAFS_DIRECT_SIZE)
    {
      char buf[blksize];
      memcpy(buf, inoent->i_blkreftbl, filesize_orig);
      memset(buf + filesize_orig, 0, blksize - filesize_orig);
      KAFS_CALL(kafs_ino_iblk_write, ctx, inoent, 0, buf);
    }
    kafs_ino_size_set(inoent, filesize_new);
    return KAFS_SUCCESS;
  }
  assert(filesize_new < filesize_orig);
  uint32_t ino_idx = (uint32_t)(inoent - ctx->c_inotbl);
  kafs_iblkcnt_t iblooff = filesize_new >> log_blksize;
  kafs_iblkcnt_t iblocnt = (filesize_orig + blksize - 1) >> log_blksize;
  kafs_blksize_t off = (kafs_blksize_t)(filesize_new & (blksize - 1));

  if (filesize_orig <= KAFS_DIRECT_SIZE)
  {
    memset((void *)inoent->i_blkreftbl + filesize_new, 0, filesize_orig - filesize_new);
    kafs_ino_size_set(inoent, filesize_new);
    return KAFS_SUCCESS;
  }

  // Indirect -> direct: copy first block data to inode, then release all blocks.
  if (filesize_new <= KAFS_DIRECT_SIZE)
  {
    char buf[blksize];
    KAFS_CALL(kafs_ino_iblk_read, ctx, inoent, 0, buf);

    // Update size first so readers won't access blocks being freed.
    kafs_ino_size_set(inoent, filesize_new);

    // Release blocks in batches: capture + clear refs under inode lock, dec_ref outside.
    const kafs_iblkcnt_t TRUNC_BATCH = 64;
    kafs_iblkcnt_t cur = 0;
    while (cur < iblocnt)
    {
      kafs_iblkcnt_t end = cur + TRUNC_BATCH;
      if (end > iblocnt)
        end = iblocnt;

      kafs_blkcnt_t to_free[TRUNC_BATCH * 4];
      size_t to_free_cnt = 0;
      for (kafs_iblkcnt_t iblo = cur; iblo < end; iblo++)
      {
        kafs_blkcnt_t old;
        KAFS_CALL(kafs_ino_ibrk_run, ctx, inoent, iblo, &old, KAFS_IBLKREF_FUNC_GET);
        if (old == KAFS_BLO_NONE)
          continue;
        kafs_blkcnt_t none = KAFS_BLO_NONE;
        KAFS_CALL(kafs_ino_ibrk_run, ctx, inoent, iblo, &none, KAFS_IBLKREF_FUNC_SET);
        kafs_blkcnt_t f1, f2, f3;
        KAFS_CALL(kafs_ino_prune_empty_indirects, ctx, inoent, iblo, &f1, &f2, &f3);
        to_free[to_free_cnt++] = old;
        if (f1 != KAFS_BLO_NONE)
          to_free[to_free_cnt++] = f1;
        if (f2 != KAFS_BLO_NONE)
          to_free[to_free_cnt++] = f2;
        if (f3 != KAFS_BLO_NONE)
          to_free[to_free_cnt++] = f3;
      }

      if (to_free_cnt)
      {
        for (size_t i = 0; i < to_free_cnt; i++)
          KAFS_TRUNC_PUSH_FREE(to_free[i]);
      }
      cur = end;
    }

    memcpy(inoent->i_blkreftbl, buf, (size_t)filesize_new);
    if (filesize_new < KAFS_DIRECT_SIZE)
      memset((void *)inoent->i_blkreftbl + filesize_new, 0, KAFS_DIRECT_SIZE - filesize_new);
    if (deferred_free_cnt)
    {
      kafs_inode_unlock(ctx, ino_idx);
      for (size_t i = 0; i < deferred_free_cnt; ++i)
        (void)kafs_hrl_dec_ref_by_blo(ctx, deferred_free[i]);
      kafs_inode_lock(ctx, ino_idx);
    }
    free(deferred_free);
    return KAFS_SUCCESS;
  }

  // Shrink within indirect mode: update size first so readers won't access freed blocks.
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

  const kafs_iblkcnt_t TRUNC_BATCH = 64;
  while (iblooff < iblocnt)
  {
    kafs_iblkcnt_t end = iblooff + TRUNC_BATCH;
    if (end > iblocnt)
      end = iblocnt;

    kafs_blkcnt_t to_free[TRUNC_BATCH * 4];
    size_t to_free_cnt = 0;
    for (kafs_iblkcnt_t iblo = iblooff; iblo < end; iblo++)
    {
      kafs_blkcnt_t old;
      KAFS_CALL(kafs_ino_ibrk_run, ctx, inoent, iblo, &old, KAFS_IBLKREF_FUNC_GET);
      if (old == KAFS_BLO_NONE)
        continue;
      kafs_blkcnt_t none = KAFS_BLO_NONE;
      KAFS_CALL(kafs_ino_ibrk_run, ctx, inoent, iblo, &none, KAFS_IBLKREF_FUNC_SET);
      kafs_blkcnt_t f1, f2, f3;
      KAFS_CALL(kafs_ino_prune_empty_indirects, ctx, inoent, iblo, &f1, &f2, &f3);
      to_free[to_free_cnt++] = old;
      if (f1 != KAFS_BLO_NONE)
        to_free[to_free_cnt++] = f1;
      if (f2 != KAFS_BLO_NONE)
        to_free[to_free_cnt++] = f2;
      if (f3 != KAFS_BLO_NONE)
        to_free[to_free_cnt++] = f3;
    }

    if (to_free_cnt)
    {
      for (size_t i = 0; i < to_free_cnt; i++)
        KAFS_TRUNC_PUSH_FREE(to_free[i]);
    }
    iblooff = end;
  }

  if (deferred_free_cnt)
  {
    kafs_inode_unlock(ctx, ino_idx);
    for (size_t i = 0; i < deferred_free_cnt; ++i)
      (void)kafs_hrl_dec_ref_by_blo(ctx, deferred_free[i]);
    kafs_inode_lock(ctx, ino_idx);
  }
  free(deferred_free);
#undef KAFS_TRUNC_PUSH_FREE
  return KAFS_SUCCESS;
}

__attribute_maybe_unused__ static int kafs_trim(struct kafs_context *ctx, kafs_sinode_t *inoent,
                                                kafs_off_t off, kafs_off_t size)
{
  kafs_dlog(2, "%s(ino = %d, off = %" PRIuFAST64 ", size = %" PRIuFAST64 ")\n", __func__,
            inoent - ctx->c_inotbl, off, size);
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
    memset(inoent, 0, sizeof(struct kafs_sinode));
    // Best-effort accounting (avoid taking inode_alloc_lock here to prevent lock inversion).
    kafs_sb_inocnt_free_incr(ctx->c_superblock);
    kafs_sb_wtime_set(ctx->c_superblock, kafs_now());
  }
  return KAFS_SUCCESS;
}

// Requires: caller holds inode lock for ino.
static int kafs_try_reclaim_unlinked_inode_locked(struct kafs_context *ctx, kafs_inocnt_t ino,
                                                  int *reclaimed)
{
  assert(ctx != NULL);
  assert(reclaimed != NULL);
  *reclaimed = 0;

  kafs_sinode_t *inoent = &ctx->c_inotbl[ino];
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
  memset(inoent, 0, sizeof(*inoent));
  *reclaimed = 1;
  return KAFS_SUCCESS;
}

/// @brief ディレクトリエントリを読み出す
/// @param ctx コンテキスト
/// @param inoent_dir ディレクトリの inodeテーブルのエントリ
/// @param dirent 読み出すディレクトリエントリのバッファ（sizeof(struct kafs_dirent) 以上）
/// @param offset オフセット
/// @return > 0: サイズ, 0: EOF, < 0: 失敗 (-errno)
static ssize_t kafs_dirent_read(struct kafs_context *ctx, kafs_sinode_t *inoent_dir,
                                struct kafs_sdirent *dirent, kafs_off_t offset)
{
  kafs_dlog(3, "%s(ino_dir = %d, offset = %" PRIuFAST64 ")\n", __func__,
            (inoent_dir - ctx->c_inotbl), offset);
  assert(ctx != NULL);
  assert(inoent_dir != NULL);
  assert(dirent != NULL);
  assert(kafs_ino_get_usage(inoent_dir));
  kafs_off_t filesize = kafs_ino_size_get(inoent_dir);
  ssize_t r1 =
      KAFS_CALL(kafs_pread, ctx, inoent_dir, dirent, offsetof(kafs_sdirent_t, d_filename), offset);
  if (r1 == 0)
    return 0;
  if (r1 < (ssize_t)offsetof(kafs_sdirent_t, d_filename))
  {
    kafs_log(KAFS_LOG_WARNING,
             "%s: short read dirent header ino_dir=%d off=%" PRIuFAST64 " size=%" PRIuFAST64
             " got=%zd\n",
             __func__, (int)(inoent_dir - ctx->c_inotbl), offset, filesize, r1);
    return 0;
  }
  kafs_inocnt_t d_ino = kafs_dirent_ino_get(dirent);
  kafs_filenamelen_t filenamelen = kafs_dirent_filenamelen_get(dirent);
  // Treat zeroed/uninitialized entries as EOF (allows sparse tail within directory inode size).
  if (d_ino == 0 || filenamelen == 0)
    return 0;
  if (filenamelen >= FILENAME_MAX)
  {
    kafs_log(KAFS_LOG_ERR,
             "%s: invalid filenamelen=%" PRIuFAST32 " ino_dir=%d off=%" PRIuFAST64 "\n", __func__,
             (uint_fast32_t)filenamelen, (int)(inoent_dir - ctx->c_inotbl), offset);
    return -EIO;
  }
  kafs_off_t want_end = offset + (kafs_off_t)r1 + (kafs_off_t)filenamelen;
  if (want_end > filesize)
  {
    kafs_log(KAFS_LOG_WARNING,
             "%s: dirent tail beyond size ino_dir=%d off=%" PRIuFAST64 " need_end=%" PRIuFAST64
             " size=%" PRIuFAST64 "\n",
             __func__, (int)(inoent_dir - ctx->c_inotbl), offset, want_end, filesize);
    return 0;
  }
  ssize_t r2 = KAFS_CALL(kafs_pread, ctx, inoent_dir, dirent->d_filename, filenamelen, offset + r1);
  if (r2 < (ssize_t)filenamelen)
  {
    kafs_log(KAFS_LOG_ERR,
             "%s: short read dirent name ino_dir=%d off=%" PRIuFAST64 " need=%" PRIuFAST32
             " got=%zd\n",
             __func__, (int)(inoent_dir - ctx->c_inotbl), offset, (uint_fast32_t)filenamelen, r2);
    return (offset + (kafs_off_t)r1 + (kafs_off_t)r2 >= filesize) ? 0 : -EIO;
  }
  dirent->d_filename[r2] = '\0';
  kafs_dlog(3,
            "%s(ino_dir = %d, offset = %" PRIuFAST64 ") return {.d_ino = %" PRIuFAST32
            ", .d_filename = %s, .d_filenamelen = %" PRIuFAST32 "}\n",
            __func__, (inoent_dir - ctx->c_inotbl), offset, kafs_dirent_ino_get(dirent),
            dirent->d_filename, kafs_dirent_filenamelen_get(dirent));
  return r1 + r2;
}

static int kafs_dir_snapshot(struct kafs_context *ctx, kafs_sinode_t *inoent_dir, char **out,
                             size_t *out_len);

static int kafs_dirent_iter_next(const char *buf, size_t len, size_t off, kafs_inocnt_t *out_ino,
                                 kafs_filenamelen_t *out_namelen, const char **out_name,
                                 size_t *out_rec_len);

/// @brief ディレクトリエントリから対象のファイル名を探す
/// @param ctx コンテキスト
/// @param name ファイル名
/// @param namelen ファイル名の長さ
/// @param ino 対象のディレクトリ
/// @param pino_found 見つかったエントリ
/// @return 0: 成功, < 0: 失敗 (-errno)
static int kafs_dirent_search(struct kafs_context *ctx, kafs_sinode_t *inoent, const char *filename,
                              kafs_filenamelen_t filenamelen, kafs_sinode_t **pinoent_found)
{
  kafs_dlog(3, "%s(ino = %d, filename = %.*s, filenamelen = %" PRIuFAST16 ")\n", __func__,
            (inoent - ctx->c_inotbl), (int)filenamelen, filename, filenamelen);
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

  size_t off = 0;
  while (1)
  {
    kafs_inocnt_t d_ino;
    kafs_filenamelen_t d_filenamelen;
    const char *d_filename;
    size_t rec_len;
    int step =
        kafs_dirent_iter_next(snap, snap_len, off, &d_ino, &d_filenamelen, &d_filename, &rec_len);
    if (step == 0)
      break;
    if (step < 0)
    {
      kafs_dlog(1, "%s: parse failed dir_ino=%d off=%zu snap_len=%zu target=%.*s\n", __func__,
                (int)(inoent - ctx->c_inotbl), off, snap_len, (int)filenamelen, filename);
      free(snap);
      return -EIO;
    }

    if (d_filenamelen == filenamelen && memcmp(d_filename, filename, filenamelen) == 0)
    {
      *pinoent_found = &ctx->c_inotbl[d_ino];
      free(snap);
      return KAFS_SUCCESS;
    }
    off += rec_len;
  }
  free(snap);
  return -ENOENT;
}

// Directory entry format on disk: {ino,u16 namelen, name[namelen]} repeated.
typedef struct __attribute__((packed))
{
  kafs_sinocnt_t d_ino;
  kafs_sfilenamelen_t d_filenamelen;
} kafs_dirent_hdr_t;

static int kafs_dir_snapshot(struct kafs_context *ctx, kafs_sinode_t *inoent_dir, char **out,
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
    kafs_dlog(1, "%s: pread failed ino=%d req=%zu got=%zd\n", __func__,
              (int)(inoent_dir - ctx->c_inotbl), len, r);
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

static int kafs_dirent_iter_next(const char *buf, size_t len, size_t off, kafs_inocnt_t *out_ino,
                                 kafs_filenamelen_t *out_namelen, const char **out_name,
                                 size_t *out_rec_len)
{
  const size_t hdr_sz = sizeof(kafs_dirent_hdr_t);
  if (off >= len)
    return 0;
  if (len - off < hdr_sz)
    return 0;
  kafs_dirent_hdr_t hdr;
  memcpy(&hdr, buf + off, hdr_sz);
  kafs_inocnt_t ino = kafs_inocnt_stoh(hdr.d_ino);
  kafs_filenamelen_t namelen = kafs_filenamelen_stoh(hdr.d_filenamelen);
  if (ino == 0 || namelen == 0)
    return 0;
  if (namelen >= FILENAME_MAX)
  {
    kafs_dlog(1,
              "%s: invalid dirent header off=%zu len=%zu ino=%" PRIuFAST16 " namelen=%" PRIuFAST16
              "\n",
              __func__, off, len, (uint_fast16_t)ino, (uint_fast16_t)namelen);
    return -EIO;
  }
  if (len - off < hdr_sz + (size_t)namelen)
    return 0;
  *out_ino = ino;
  *out_namelen = namelen;
  *out_name = buf + off + hdr_sz;
  *out_rec_len = hdr_sz + (size_t)namelen;
  return 1;
}

// NOTE: caller holds dir inode lock. For rename(2) we must not change linkcount of the moved inode.
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

  // scan for duplicates and compute append offset
  size_t off = 0;
  while (1)
  {
    kafs_inocnt_t dino;
    kafs_filenamelen_t dlen;
    const char *dname;
    size_t rec_len;
    int step = kafs_dirent_iter_next(old, old_len, off, &dino, &dlen, &dname, &rec_len);
    if (step == 0)
      break;
    if (step < 0)
    {
      free(old);
      return -EIO;
    }
    if (dlen == filenamelen && memcmp(dname, filename, filenamelen) == 0)
    {
      free(old);
      return -EEXIST;
    }
    off += rec_len;
  }

  const size_t hdr_sz = sizeof(kafs_dirent_hdr_t);
  size_t new_len = off + hdr_sz + (size_t)filenamelen;
  char *nw = (char *)malloc(new_len);
  if (!nw)
  {
    free(old);
    return -ENOMEM;
  }
  if (off)
    memcpy(nw, old, off);
  kafs_dirent_hdr_t hdr;
  hdr.d_ino = kafs_inocnt_htos(ino);
  hdr.d_filenamelen = kafs_filenamelen_htos(filenamelen);
  memcpy(nw + off, &hdr, hdr_sz);
  memcpy(nw + off + hdr_sz, filename, filenamelen);

  rc = kafs_dir_writeback(ctx, inoent_dir, nw, new_len);
  free(nw);
  free(old);
  return rc;
}

static int kafs_dirent_add(struct kafs_context *ctx, kafs_sinode_t *inoent_dir, kafs_inocnt_t ino,
                           const char *filename)
{
  int rc = kafs_dirent_add_nolink(ctx, inoent_dir, ino, filename);
  if (rc == 0)
    kafs_ino_linkcnt_incr(&ctx->c_inotbl[ino]);
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

  size_t off = 0;
  while (1)
  {
    kafs_inocnt_t dino;
    kafs_filenamelen_t dlen;
    const char *dname;
    size_t rec_len;
    int step = kafs_dirent_iter_next(old, old_len, off, &dino, &dlen, &dname, &rec_len);
    if (step == 0)
      break;
    if (step < 0)
    {
      free(old);
      return -EIO;
    }
    if (dlen == filenamelen && memcmp(dname, filename, filenamelen) == 0)
    {
      size_t new_len = old_len - rec_len;
      char *nw = (char *)malloc(new_len);
      if (!nw)
      {
        free(old);
        return -ENOMEM;
      }
      if (off)
        memcpy(nw, old, off);
      if (off + rec_len < old_len)
        memcpy(nw + off, old + off + rec_len, old_len - (off + rec_len));

      rc = kafs_dir_writeback(ctx, inoent_dir, nw, new_len);
      if (rc == 0 && out_ino)
        *out_ino = dino;
      free(nw);
      free(old);
      return rc;
    }
    off += rec_len;
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
    kafs_ino_linkcnt_decr(&ctx->c_inotbl[d_ino]);
  return rc;
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
  {
    kafs_bool_t result = KAFS_FALSE;
    if (mode & S_IROTH)
      result = KAFS_TRUE;
    else if (mode & S_IRUSR && uid == fuid)
      result = KAFS_TRUE;
    else if (mode & S_IRGRP)
    {
      if (gid == fgid)
        result = KAFS_TRUE;
      else
        for (size_t i = 0; i < ngroups; i++)
          if (fgid == groups[i])
          {
            result = KAFS_TRUE;
            break;
          }
    }
    if (!result)
      return -EACCES;
  }
  if (ok & W_OK)
  {
    kafs_bool_t result = KAFS_FALSE;
    if (mode & S_IWOTH)
      result = KAFS_TRUE;
    else if (mode & S_IWUSR && uid == fuid)
      result = KAFS_TRUE;
    else if (mode & S_IWGRP)
    {
      if (gid == fgid)
        result = KAFS_TRUE;
      else
        for (size_t i = 0; i < ngroups; i++)
          if (fgid == groups[i])
          {
            result = KAFS_TRUE;
            break;
          }
    }
    if (!result)
      return -EACCES;
  }
  if (ok & X_OK)
  {
    kafs_bool_t result = KAFS_FALSE;
    if (mode & S_IXOTH)
      result = KAFS_TRUE;
    else if (mode & S_IXUSR && uid == fuid)
      result = KAFS_TRUE;
    else if (mode & S_IXGRP)
    {
      if (gid == fgid)
        result = KAFS_TRUE;
      else
        for (size_t i = 0; i < ngroups; i++)
          if (fgid == groups[i])
          {
            result = KAFS_TRUE;
            break;
          }
    }
    if (!result)
      return -EACCES;
  }
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

  uid_t uid = fctx->uid;
  gid_t gid = fctx->gid;
  // fuse_getgroups(0,NULL) may return 0 or -1; don't use KAFS_IOCALL here and avoid zero-length VLA
  ssize_t ng0 = fuse_getgroups(0, NULL);
  size_t ngroups = (ng0 > 0) ? (size_t)ng0 : 0;
  gid_t groups[(ngroups > 0) ? ngroups : 1];
  if (ngroups > 0)
    (void)fuse_getgroups(ngroups, groups);

  kafs_sinode_t *inoent;
  const char *p;
  if (path == NULL || path[0] == '\0')
  {
    // Path-less operation (read/write etc.): start from file handle
    assert(fi != NULL);
    inoent = &ctx->c_inotbl[(kafs_inocnt_t)fi->fh];
    p = "";
  }
  else
  {
    // Path-based operation: always resolve from root regardless of fi
    inoent = &ctx->c_inotbl[KAFS_INO_ROOTDIR];
    p = path + 1; // skip leading '/'
  }
  int ok_final = ok; // keep original request for the final node
  while (*p != '\0')
  {
    const char *n = strchrnul(p, '/');
    kafs_mode_t cur_mode = kafs_ino_mode_get(inoent);
    kafs_dlog(2, "%s: component='%.*s' checking dir ino=%u mode=%o\n", __func__, (int)(n - p), p,
              (unsigned)(inoent - ctx->c_inotbl), (unsigned)cur_mode);
    // For intermediate directories, require execute (search) permission only
    int ok_dirs = X_OK;
    int rc_ac = kafs_access_check(ok_dirs, inoent, KAFS_TRUE, uid, gid, ngroups, groups);
    if (rc_ac < 0)
      return rc_ac;
    uint32_t ino_dir = (uint32_t)(inoent - ctx->c_inotbl);
    kafs_inode_lock(ctx, ino_dir);
    int rc = kafs_dirent_search(ctx, inoent, p, n - p, &inoent);
    kafs_inode_unlock(ctx, ino_dir);
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
            (unsigned)(inoent - ctx->c_inotbl), (unsigned)final_mode, ok_final);
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
    if (ctx->c_hotplug_uds_path[0] != '\0')
      (void)setenv("KAFS_HOTPLUG_UDS", ctx->c_hotplug_uds_path, 1);
    for (uint32_t i = 0; i < env_count; ++i)
      (void)setenv(envs[i].key, envs[i].value, 1);

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
  req.ino = (uint32_t)(inoent - ctx->c_inotbl);
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

int kafs_core_open_image(const char *image_path, kafs_context_t *ctx)
{
  if (!image_path || !ctx)
    return -EINVAL;

  memset(ctx, 0, sizeof(*ctx));
  ctx->c_fd = open(image_path, O_RDWR, 0666);
  if (ctx->c_fd < 0)
    return -errno;

  kafs_ssuperblock_t sbdisk;
  ssize_t r = pread(ctx->c_fd, &sbdisk, sizeof(sbdisk), 0);
  if (r != (ssize_t)sizeof(sbdisk))
  {
    int err = -errno;
    close(ctx->c_fd);
    ctx->c_fd = -1;
    return err ? err : -EIO;
  }
  if (kafs_sb_magic_get(&sbdisk) != KAFS_MAGIC)
  {
    close(ctx->c_fd);
    ctx->c_fd = -1;
    return -EINVAL;
  }
  if (kafs_sb_format_version_get(&sbdisk) != KAFS_FORMAT_VERSION)
  {
    close(ctx->c_fd);
    ctx->c_fd = -1;
    return -EPROTONOSUPPORT;
  }

  kafs_logblksize_t log_blksize = kafs_sb_log_blksize_get(&sbdisk);
  kafs_blksize_t blksize = 1u << log_blksize;
  kafs_blksize_t blksizemask = blksize - 1u;
  kafs_inocnt_t inocnt = kafs_inocnt_stoh(sbdisk.s_inocnt);
  kafs_blkcnt_t r_blkcnt = kafs_blkcnt_stoh(sbdisk.s_r_blkcnt);

  off_t mapsize = 0;
  mapsize += sizeof(kafs_ssuperblock_t);
  mapsize = (mapsize + blksizemask) & ~blksizemask;
  void *blkmask_off = (void *)mapsize;
  mapsize += (r_blkcnt + 7) >> 3;
  mapsize = (mapsize + 7) & ~7;
  mapsize = (mapsize + blksizemask) & ~blksizemask;
  void *inotbl_off = (void *)mapsize;
  mapsize += sizeof(kafs_sinode_t) * inocnt;
  mapsize = (mapsize + blksizemask) & ~blksizemask;

  off_t imgsize = (off_t)r_blkcnt << log_blksize;
  {
    uint64_t idx_off = kafs_sb_hrl_index_offset_get(&sbdisk);
    uint64_t idx_size = kafs_sb_hrl_index_size_get(&sbdisk);
    uint64_t ent_off = kafs_sb_hrl_entry_offset_get(&sbdisk);
    uint64_t ent_cnt = kafs_sb_hrl_entry_cnt_get(&sbdisk);
    uint64_t ent_size = ent_cnt * (uint64_t)sizeof(kafs_hrl_entry_t);
    uint64_t j_off = kafs_sb_journal_offset_get(&sbdisk);
    uint64_t j_size = kafs_sb_journal_size_get(&sbdisk);
    uint64_t p_off = kafs_sb_pendinglog_offset_get(&sbdisk);
    uint64_t p_size = kafs_sb_pendinglog_size_get(&sbdisk);
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
    int err = -errno;
    close(ctx->c_fd);
    ctx->c_fd = -1;
    return err;
  }
  ctx->c_img_size = (size_t)imgsize;
  ctx->c_superblock = (kafs_ssuperblock_t *)ctx->c_img_base;
  ctx->c_mapsize = (size_t)mapsize;
  ctx->c_blkmasktbl = (void *)ctx->c_superblock + (intptr_t)blkmask_off;
  ctx->c_inotbl = (void *)ctx->c_superblock + (intptr_t)inotbl_off;
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
  kafs_sinode_t *inoent = &ctx->c_inotbl[ino];
  st->st_dev = 0;
  st->st_ino = ino;
  st->st_mode = kafs_ino_mode_get(inoent);
  st->st_nlink = kafs_ino_linkcnt_get(inoent);
  st->st_uid = kafs_ino_uid_get(inoent);
  st->st_gid = kafs_ino_gid_get(inoent);
  st->st_rdev = kafs_ino_dev_get(inoent);
  st->st_size = kafs_ino_size_get(inoent);
  st->st_blksize = kafs_sb_blksize_get(ctx->c_superblock);

  const unsigned blksz = (unsigned)st->st_blksize;
  const unsigned long long alloc =
      blksz ? ((unsigned long long)st->st_size + blksz - 1) / blksz * blksz : 0;
  st->st_blocks = (blkcnt_t)(alloc / 512ull);
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
  ssize_t rr = kafs_pread(ctx, &ctx->c_inotbl[ino], buf, size, offset);
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
  ssize_t ww = kafs_pwrite(ctx, &ctx->c_inotbl[ino], buf, size, offset);
  kafs_inode_unlock(ctx, (uint32_t)ino);
  return ww;
}

int kafs_core_truncate(kafs_context_t *ctx, kafs_inocnt_t ino, off_t size)
{
  if (!ctx)
    return -EINVAL;
  if (ino >= kafs_sb_inocnt_get(ctx->c_superblock))
    return -ENOENT;
  kafs_inode_lock(ctx, (uint32_t)ino);
  int rc = kafs_truncate(ctx, &ctx->c_inotbl[ino], (kafs_off_t)size);
  kafs_inode_unlock(ctx, (uint32_t)ino);
  return rc;
}

static ssize_t kafs_hotplug_call_read(struct fuse_context *fctx, kafs_context_t *ctx,
                                      kafs_inocnt_t ino, char *buf, size_t size, off_t offset)
{
  int wait_rc = kafs_hotplug_wait_ready(ctx);
  if (wait_rc != 0)
    return wait_rc;
  if (ctx->c_hotplug_data_mode == KAFS_RPC_DATA_SHM)
    return -EOPNOTSUPP;
  if (ctx->c_hotplug_data_mode == KAFS_RPC_DATA_INLINE &&
      size > (KAFS_RPC_MAX_PAYLOAD - sizeof(kafs_rpc_read_resp_t)))
    return -EOPNOTSUPP;

  kafs_rpc_read_req_t req;
  req.ino = (uint32_t)ino;
  req.uid = (uint32_t)fctx->uid;
  req.gid = (uint32_t)fctx->gid;
  req.pid = (uint32_t)fctx->pid;
  req.off = (uint64_t)offset;
  req.size = (uint32_t)size;
  req.data_mode = ctx->c_hotplug_data_mode;
  uint64_t req_id = kafs_rpc_next_req_id();

  uint8_t resp_buf[KAFS_RPC_MAX_PAYLOAD];
  if (ctx->c_hotplug_lock_init)
    pthread_mutex_lock(&ctx->c_hotplug_lock);
  int rc = kafs_rpc_send_msg(ctx->c_hotplug_fd, KAFS_RPC_OP_READ, KAFS_RPC_FLAG_ENDIAN_HOST, req_id,
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
      kafs_rpc_read_resp_t *resp = (kafs_rpc_read_resp_t *)resp_buf;
      if (ctx->c_hotplug_data_mode == KAFS_RPC_DATA_INLINE)
      {
        uint32_t data_len = resp_len - (uint32_t)sizeof(*resp);
        if (resp->size > data_len || resp->size > size)
          rc = -EBADMSG;
        else
        {
          memcpy(buf, resp_buf + sizeof(*resp), resp->size);
          rc = (int)resp->size;
        }
      }
      else
      {
        if (resp_len != sizeof(*resp))
        {
          rc = -EBADMSG;
        }
        else
        {
          need_local = 1;
        }
      }
    }
  }
  if (ctx->c_hotplug_lock_init)
    pthread_mutex_unlock(&ctx->c_hotplug_lock);
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

static ssize_t kafs_hotplug_call_write(struct fuse_context *fctx, kafs_context_t *ctx,
                                       kafs_inocnt_t ino, const char *buf, size_t size,
                                       off_t offset)
{
  int wait_rc = kafs_hotplug_wait_ready(ctx);
  if (wait_rc != 0)
    return wait_rc;
  if (ctx->c_hotplug_data_mode == KAFS_RPC_DATA_SHM)
    return -EOPNOTSUPP;
  if (ctx->c_hotplug_data_mode == KAFS_RPC_DATA_INLINE &&
      size > (KAFS_RPC_MAX_PAYLOAD - sizeof(kafs_rpc_write_req_t)))
    return -EOPNOTSUPP;

  uint8_t payload[KAFS_RPC_MAX_PAYLOAD];
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
  uint64_t req_id = kafs_rpc_next_req_id();

  if (ctx->c_hotplug_lock_init)
    pthread_mutex_lock(&ctx->c_hotplug_lock);
  int rc =
      kafs_rpc_send_msg(ctx->c_hotplug_fd, KAFS_RPC_OP_WRITE, KAFS_RPC_FLAG_ENDIAN_HOST, req_id,
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

static int kafs_hotplug_call_truncate(struct fuse_context *fctx, kafs_context_t *ctx,
                                      kafs_sinode_t *inoent, off_t size)
{
  (void)fctx;
  int wait_rc = kafs_hotplug_wait_ready(ctx);
  if (wait_rc != 0)
    return wait_rc;

  kafs_rpc_truncate_req_t req;
  req.ino = (uint32_t)(inoent - ctx->c_inotbl);
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
  st->st_ino = inoent - ctx->c_inotbl;
  st->st_mode = kafs_ino_mode_get(inoent);
  st->st_nlink = kafs_ino_linkcnt_get(inoent);
  st->st_uid = kafs_ino_uid_get(inoent);
  st->st_gid = kafs_ino_gid_get(inoent);
  st->st_rdev = kafs_ino_dev_get(inoent);
  st->st_size = kafs_ino_size_get(inoent);
  st->st_blksize = kafs_sb_blksize_get(ctx->c_superblock);

  /* st_blocks is in 512-byte units; approximate allocation by rounding st_size to fs block size. */
  const unsigned blksz = (unsigned)st->st_blksize;
  const unsigned long long alloc =
      blksz ? ((unsigned long long)st->st_size + blksz - 1) / blksz * blksz : 0;
  st->st_blocks = (blkcnt_t)(alloc / 512ull);
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

#define KAFS_STATS_VERSION 9u

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

static void kafs_stats_snapshot(kafs_context_t *ctx, kafs_stats_t *out)
{
  memset(out, 0, sizeof(*out));
  out->struct_size = (uint32_t)sizeof(*out);
  out->version = KAFS_STATS_VERSION;

  out->blksize = (uint32_t)kafs_sb_blksize_get(ctx->c_superblock);
  out->fs_blocks_total = (uint64_t)kafs_sb_blkcnt_get(ctx->c_superblock);
  kafs_bitmap_lock(ctx);
  out->fs_blocks_free = (uint64_t)kafs_sb_blkcnt_free_get(ctx->c_superblock);
  kafs_bitmap_unlock(ctx);
  out->fs_inodes_total = (uint64_t)kafs_sb_inocnt_get(ctx->c_superblock);
  out->fs_inodes_free = (uint64_t)(kafs_inocnt_t)kafs_sb_inocnt_free_get(ctx->c_superblock);

  uint32_t entry_cnt = kafs_sb_hrl_entry_cnt_get(ctx->c_superblock);
  out->hrl_entries_total = (uint64_t)entry_cnt;

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

  kafs_off_t size;
  char inline_buf[KAFS_DIRECT_SIZE];
  int is_inline = 0;

  uint32_t ino_src = (uint32_t)(src - ctx->c_inotbl);
  kafs_inode_lock(ctx, ino_src);
  size = kafs_ino_size_get(src);
  if (size <= (kafs_off_t)KAFS_DIRECT_SIZE)
  {
    memcpy(inline_buf, (void *)src->i_blkreftbl, (size_t)size);
    is_inline = 1;
  }

  kafs_blkcnt_t *blos = NULL;
  kafs_iblkcnt_t iblocnt = 0;
  if (!is_inline)
  {
    kafs_blksize_t bs = kafs_sb_blksize_get(ctx->c_superblock);
    iblocnt = (kafs_iblkcnt_t)((size + (kafs_off_t)bs - 1) / (kafs_off_t)bs);
    blos = (kafs_blkcnt_t *)calloc((size_t)iblocnt ? (size_t)iblocnt : 1u, sizeof(*blos));
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
  }
  kafs_inode_unlock(ctx, ino_src);

  uint32_t ino_dst = (uint32_t)(dst - ctx->c_inotbl);
  kafs_inode_lock(ctx, ino_dst);
  int trc = kafs_truncate(ctx, dst, 0);
  if (trc < 0)
  {
    kafs_inode_unlock(ctx, ino_dst);
    free(blos);
    return trc;
  }
  memset(dst->i_blkreftbl, 0, sizeof(dst->i_blkreftbl));

  if (is_inline)
  {
    kafs_ino_size_set(dst, size);
    memcpy((void *)dst->i_blkreftbl, inline_buf, (size_t)size);
    kafs_time_t now = kafs_now();
    kafs_ino_mtime_set(dst, now);
    kafs_ino_ctime_set(dst, now);
    kafs_inode_unlock(ctx, ino_dst);
    return 0;
  }

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
      free(blos);
      return (irc == -ENOENT || irc == -ENOSYS) ? -EOPNOTSUPP : irc;
    }

    int s = kafs_ino_ibrk_run(ctx, dst, i, &b, KAFS_IBLKREF_FUNC_SET);
    if (s < 0)
    {
      (void)kafs_truncate(ctx, dst, 0);
      kafs_inode_unlock(ctx, ino_dst);
      free(blos);
      (void)kafs_hrl_dec_ref_by_blo(ctx, b);
      return s;
    }
  }

  kafs_time_t now = kafs_now();
  kafs_ino_mtime_set(dst, now);
  kafs_ino_ctime_set(dst, now);
  kafs_inode_unlock(ctx, ino_dst);
  free(blos);
  return 0;
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
      (void)kafs_hrl_dec_ref_by_blo(ctx, src_blo);
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
      (void)kafs_hrl_dec_ref_by_blo(ctx, release_list[i]);
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
  {
    kafs_rpc_hotplug_status_t st;
    kafs_ctl_fill_status(ctx, &st);
    memcpy(resp_payload, &st, sizeof(st));
    resp_len = (uint32_t)sizeof(st);
    break;
  }
  case KAFS_RPC_OP_CTL_RESTART:
    result = kafs_hotplug_restart_back(ctx);
    break;
  case KAFS_RPC_OP_CTL_SET_TIMEOUT:
    if (hdr.payload_len != sizeof(kafs_rpc_set_timeout_t))
      result = -EINVAL;
    else
    {
      const kafs_rpc_set_timeout_t *req = (const kafs_rpc_set_timeout_t *)payload;
      if (req->timeout_ms == 0)
        result = -EINVAL;
      else
        ctx->c_hotplug_wait_timeout_ms = req->timeout_ms;
    }
    break;
  case KAFS_RPC_OP_CTL_ENV_LIST:
  {
    kafs_rpc_env_list_t env;
    memset(&env, 0, sizeof(env));
    kafs_hotplug_env_lock(ctx);
    env.count = ctx->c_hotplug_env_count;
    for (uint32_t i = 0; i < env.count; ++i)
      env.entries[i] = ctx->c_hotplug_env[i];
    kafs_hotplug_env_unlock(ctx);
    memcpy(resp_payload, &env, sizeof(env));
    resp_len = (uint32_t)sizeof(env);
    break;
  }
  case KAFS_RPC_OP_CTL_ENV_SET:
    if (hdr.payload_len != sizeof(kafs_rpc_env_update_t))
      result = -EINVAL;
    else
    {
      const kafs_rpc_env_update_t *req = (const kafs_rpc_env_update_t *)payload;
      result = kafs_hotplug_env_set(ctx, req->key, req->value);
    }
    break;
  case KAFS_RPC_OP_CTL_ENV_UNSET:
    if (hdr.payload_len != sizeof(kafs_rpc_env_update_t))
      result = -EINVAL;
    else
    {
      const kafs_rpc_env_update_t *req = (const kafs_rpc_env_update_t *)payload;
      result = kafs_hotplug_env_unset(ctx, req->key);
    }
    break;
  case KAFS_RPC_OP_CTL_SET_DEDUP_PRIO:
    if (hdr.payload_len != sizeof(kafs_rpc_set_dedup_prio_t))
      result = -EINVAL;
    else
    {
      const kafs_rpc_set_dedup_prio_t *req = (const kafs_rpc_set_dedup_prio_t *)payload;
      if (req->mode != KAFS_PENDING_WORKER_PRIO_NORMAL &&
          req->mode != KAFS_PENDING_WORKER_PRIO_IDLE)
      {
        result = -EINVAL;
      }
      else if ((req->flags & KAFS_RPC_SET_DEDUP_PRIO_F_HAS_NICE) != 0 &&
               (req->nice_value < 0 || req->nice_value > 19))
      {
        result = -ERANGE;
      }
      else
      {
        ctx->c_pending_worker_prio_mode = req->mode;
        if ((req->flags & KAFS_RPC_SET_DEDUP_PRIO_F_HAS_NICE) != 0)
          ctx->c_pending_worker_nice = req->nice_value;
        ctx->c_pending_worker_prio_base_mode = req->mode;
        if ((req->flags & KAFS_RPC_SET_DEDUP_PRIO_F_HAS_NICE) != 0)
          ctx->c_pending_worker_nice_base = req->nice_value;

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
    }
    break;
  case KAFS_RPC_OP_CTL_SET_RUNTIME:
    if (hdr.payload_len != sizeof(kafs_rpc_set_runtime_t))
      result = -EINVAL;
    else
    {
      const kafs_rpc_set_runtime_t *req = (const kafs_rpc_set_runtime_t *)payload;
      uint32_t next_policy = ctx->c_fsync_policy;
      uint32_t next_soft = ctx->c_pending_ttl_soft_ms;
      uint32_t next_hard = ctx->c_pending_ttl_hard_ms;

      if ((req->flags & KAFS_RPC_SET_RUNTIME_F_HAS_FSYNC_POLICY) != 0)
      {
        if (req->fsync_policy != KAFS_FSYNC_POLICY_FULL &&
            req->fsync_policy != KAFS_FSYNC_POLICY_JOURNAL_ONLY &&
            req->fsync_policy != KAFS_FSYNC_POLICY_ADAPTIVE)
        {
          result = -EINVAL;
          break;
        }
        next_policy = req->fsync_policy;
      }

      if ((req->flags & KAFS_RPC_SET_RUNTIME_F_HAS_PENDING_TTL_SOFT_MS) != 0)
        next_soft = req->pending_ttl_soft_ms;

      if ((req->flags & KAFS_RPC_SET_RUNTIME_F_HAS_PENDING_TTL_HARD_MS) != 0)
        next_hard = req->pending_ttl_hard_ms;

      if (next_soft > 0 && next_hard > 0 && next_hard < next_soft)
      {
        result = -ERANGE;
        break;
      }

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
    }
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

static int kafs_op_ioctl(const char *path, int cmd, void *arg, struct fuse_file_info *fi,
                         unsigned int flags, void *data)
{
  (void)flags;

  struct fuse_context *fctx = fuse_get_context();
  kafs_context_t *ctx = (kafs_context_t *)fctx->private_data;

#ifdef __linux__
  if ((unsigned int)cmd == (unsigned int)FICLONE)
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
      // FICLONE takes an int fd argument (passed as ioctl arg value, not as pointer).
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
  if ((unsigned int)cmd == (unsigned int)FICLONERANGE)
    return -EOPNOTSUPP;
#endif

  if ((unsigned int)cmd == (unsigned int)KAFS_IOCTL_COPY)
  {
    void *buf = data ? data : arg;
    if (!buf)
      return -EINVAL;
    if (_IOC_SIZE((unsigned int)cmd) < sizeof(kafs_ioctl_copy_t))
      return -EINVAL;
    kafs_ioctl_copy_t req;
    memcpy(&req, buf, sizeof(req));
    if (req.struct_size < sizeof(req))
      return -EINVAL;
    if ((req.flags & KAFS_IOCTL_COPY_F_REFLINK) == 0)
      return -EOPNOTSUPP;
    if (req.src[0] != '/' || req.dst[0] != '/' || req.src[1] == '\0' || req.dst[1] == '\0')
      return -EINVAL;

    kafs_sinode_t *ino_src;
    kafs_sinode_t *ino_dst;
    KAFS_CALL(kafs_access, fctx, ctx, req.src, NULL, R_OK, &ino_src);

    int drc = kafs_access(fctx, ctx, req.dst, NULL, F_OK, &ino_dst);
    if (drc == -ENOENT)
    {
      kafs_inocnt_t ino_new;
      KAFS_CALL(kafs_create, req.dst, 0644 | S_IFREG, 0, NULL, &ino_new);
      ino_dst = &ctx->c_inotbl[ino_new];
    }
    else
    {
      if (drc < 0)
        return drc;
      KAFS_CALL(kafs_access, fctx, ctx, req.dst, NULL, W_OK, &ino_dst);
    }

    return kafs_reflink_clone(ctx, ino_src, ino_dst);
  }

  if ((unsigned int)cmd == (unsigned int)KAFS_IOCTL_GET_STATS)
  {
    void *buf = data ? data : arg;
    if (!buf)
      return -EINVAL;
    if (_IOC_SIZE((unsigned int)cmd) < sizeof(kafs_stats_t))
      return -EINVAL;
    kafs_stats_t out;
    kafs_stats_snapshot(ctx, &out);
    memcpy(buf, &out, sizeof(out));
    return 0;
  }
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
    ino_in = &ctx->c_inotbl[fi_in->fh];
  else
    KAFS_CALL(kafs_access, fctx, ctx, path_in, NULL, R_OK, &ino_in);
  if (fi_out)
    ino_out = &ctx->c_inotbl[fi_out->fh];
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

  uint32_t ino_src = (uint32_t)(ino_in - ctx->c_inotbl);
  uint32_t ino_dst = (uint32_t)(ino_out - ctx->c_inotbl);
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
  int dst_is_block_backed = (dst_size > (kafs_off_t)KAFS_DIRECT_SIZE);
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
    kafs_off_t src_off = (kafs_off_t)offset_in + done;
    kafs_off_t dst_off = (kafs_off_t)offset_out + done;
    kafs_off_t remain = max - done;

    if (ctx->c_hrl_bucket_cnt != 0 && remain >= (kafs_off_t)blksize &&
        ((src_off & ((kafs_off_t)blksize - 1)) == 0) &&
        ((dst_off & ((kafs_off_t)blksize - 1)) == 0))
    {
      // Avoid interpreting inline payload as block references.
      if (!dst_is_block_backed)
      {
        if (dst_size == 0 && (kafs_off_t)offset_out == 0)
        {
          memset(ino_out->i_blkreftbl, 0, sizeof(ino_out->i_blkreftbl));
          dst_is_block_backed = 1;
          dst_empty_converted = 1;
        }
        else
        {
          __atomic_add_fetch(&ctx->c_stat_copy_share_skip_dst_inline, 1u, __ATOMIC_RELAXED);
        }
      }

      if (dst_is_block_backed)
      {
        size_t blocks = (size_t)(remain >> log_blksize);
        __atomic_add_fetch(&ctx->c_stat_copy_share_attempt_blocks, (uint64_t)blocks,
                           __ATOMIC_RELAXED);
        size_t copied_blocks = 0;
        for (size_t i = 0; i < blocks; ++i)
        {
          kafs_iblkcnt_t src_iblo = (kafs_iblkcnt_t)(src_off >> log_blksize) + (kafs_iblkcnt_t)i;
          kafs_iblkcnt_t dst_iblo = (kafs_iblkcnt_t)(dst_off >> log_blksize) + (kafs_iblkcnt_t)i;
          int frc =
              kafs_copy_share_one_iblk_locked(ctx, ino_in, ino_out, src_iblo, dst_iblo, ino_dst);
          if (frc == -EAGAIN)
            break;
          if (frc < 0)
          {
            free(buf);
            kafs_inode_unlock(ctx, ino_src);
            kafs_inode_unlock(ctx, ino_dst);
            return frc;
          }
          copied_blocks++;
          done += (kafs_off_t)blksize;
        }

        if (copied_blocks > 0)
        {
          kafs_off_t end = (kafs_off_t)offset_out + done;
          if (kafs_ino_size_get(ino_out) < end)
            kafs_ino_size_set(ino_out, end);
          continue;
        }

        if (dst_empty_converted)
        {
          // Keep inode shape consistent if we converted an empty inline inode but couldn't share.
          memset(ino_out->i_blkreftbl, 0, sizeof(ino_out->i_blkreftbl));
        }
      }
    }
    else
    {
      __atomic_add_fetch(&ctx->c_stat_copy_share_skip_unaligned, 1u, __ATOMIC_RELAXED);
    }

    size_t want = (size_t)((max - done) < (kafs_off_t)bufsz ? (max - done) : (kafs_off_t)bufsz);
    ssize_t r = kafs_pread(ctx, ino_in, buf, (kafs_off_t)want, (kafs_off_t)offset_in + done);
    if (r < 0)
    {
      free(buf);
      kafs_inode_unlock(ctx, ino_src);
      kafs_inode_unlock(ctx, ino_dst);
      return r;
    }
    if (r == 0)
      break;
    ssize_t w = kafs_pwrite(ctx, ino_out, buf, (kafs_off_t)r, (kafs_off_t)offset_out + done);
    if (w < 0)
    {
      free(buf);
      kafs_inode_unlock(ctx, ino_src);
      kafs_inode_unlock(ctx, ino_dst);
      return w;
    }
    done += w;
  }

  free(buf);
  kafs_inode_unlock(ctx, ino_src);
  kafs_inode_unlock(ctx, ino_dst);
  return (ssize_t)done;
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
  fi->fh = inoent - ctx->c_inotbl;
  if (ctx->c_open_cnt)
    __atomic_add_fetch(&ctx->c_open_cnt[fi->fh], 1u, __ATOMIC_RELAXED);
  // Handle O_TRUNC on open for existing files to match POSIX semantics
  if ((fi->flags & O_TRUNC) && (accmode == O_WRONLY || accmode == O_RDWR))
  {
    kafs_inode_lock(ctx, (uint32_t)fi->fh);
    (void)kafs_truncate(ctx, &ctx->c_inotbl[fi->fh], 0);
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
  fi->fh = inoent - ctx->c_inotbl;
  return 0;
}

static int kafs_op_readdir(const char *path, void *buf, fuse_fill_dir_t filler, off_t offset,
                           struct fuse_file_info *fi, enum fuse_readdir_flags flags)
{
  (void)offset;
  (void)fi;
  (void)flags;
  struct fuse_context *fctx = fuse_get_context();
  struct kafs_context *ctx = fctx->private_data;
  kafs_sinode_t *inoent_dir;
  KAFS_CALL(kafs_access, fctx, ctx, path, NULL, R_OK, &inoent_dir);
  uint32_t ino_dir = (uint32_t)(inoent_dir - ctx->c_inotbl);

  kafs_inode_lock(ctx, ino_dir);
  char *snap = NULL;
  size_t snap_len = 0;
  int rc = kafs_dir_snapshot(ctx, inoent_dir, &snap, &snap_len);
  if (rc < 0)
  {
    kafs_inode_unlock(ctx, ino_dir);
    return rc;
  }
  if (filler(buf, ".", NULL, 0, 0))
  {
    free(snap);
    kafs_inode_unlock(ctx, ino_dir);
    return -ENOENT;
  }
  size_t o = 0;
  while (1)
  {
    kafs_inocnt_t d_ino;
    kafs_filenamelen_t d_filenamelen;
    const char *d_filename;
    size_t rec_len;
    int step =
        kafs_dirent_iter_next(snap, snap_len, o, &d_ino, &d_filenamelen, &d_filename, &rec_len);
    if (step == 0)
      break;
    if (step < 0)
    {
      free(snap);
      kafs_inode_unlock(ctx, ino_dir);
      return -EIO;
    }
    char name[FILENAME_MAX];
    memcpy(name, d_filename, d_filenamelen);
    name[d_filenamelen] = '\0';
    if (filler(buf, name, NULL, 0, 0))
    {
      free(snap);
      kafs_inode_unlock(ctx, ino_dir);
      return -ENOENT;
    }
    o += rec_len;
  }
  free(snap);
  kafs_inode_unlock(ctx, ino_dir);
  return 0;
}

static int kafs_create(const char *path, kafs_mode_t mode, kafs_dev_t dev, kafs_inocnt_t *pino_dir,
                       kafs_inocnt_t *pino_new)
{
  (void)dev;
  assert(path != NULL);
  assert(path[0] == '/');
  assert(path[1] != '\0');
  struct fuse_context *fctx = fuse_get_context();
  struct kafs_context *ctx = fctx->private_data;
  char path_copy[strlen(path) + 1];
  strcpy(path_copy, path);
  const char *dirpath = path_copy;
  char *basepath = strrchr(path_copy, '/');
  if (dirpath == basepath)
    dirpath = "/";
  *basepath = '\0';
  basepath++;

  uint64_t jseq = kafs_journal_begin(ctx, "CREATE", "path=%s mode=%o", path, (unsigned)mode);
  kafs_dlog(2, "%s: dirpath='%s' base='%s'\n", __func__, dirpath, basepath);
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

  kafs_sinode_t *inoent_dir;
  ret = kafs_access(fctx, ctx, dirpath, NULL, W_OK, &inoent_dir);
  kafs_dlog(2, "%s: access(dirpath='%s') rc=%d ino_dir=%u\n", __func__, dirpath, ret,
            (unsigned)(inoent_dir ? (inoent_dir - ctx->c_inotbl) : 0));
  if (ret < 0)
  {
    kafs_journal_abort(ctx, jseq, "parent access=%d", ret);
    return ret;
  }
  kafs_mode_t mode_dir = kafs_ino_mode_get(inoent_dir);
  if (!S_ISDIR(mode_dir))
  {
    kafs_journal_abort(ctx, jseq, "ENOTDIR");
    return -ENOTDIR;
  }
  kafs_inocnt_t ino_new;
  kafs_inode_alloc_lock(ctx);
  ret = kafs_ino_find_free(ctx->c_inotbl, &ino_new, &ctx->c_ino_search,
                           kafs_sb_inocnt_get(ctx->c_superblock));
  if (ret < 0)
  {
    kafs_inode_alloc_unlock(ctx);
    kafs_journal_abort(ctx, jseq, "ino_find_free=%d", ret);
    return ret;
  }

  kafs_dlog(2, "%s: alloc ino=%u\n", __func__, (unsigned)ino_new);
  struct kafs_sinode *inoent_new = &ctx->c_inotbl[ino_new];

  // Reserve/initialize the inode while holding alloc lock so no other thread can allocate it.
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
  kafs_ino_dev_set(inoent_new, 0);
  memset(inoent_new->i_blkreftbl, 0, sizeof(inoent_new->i_blkreftbl));

  kafs_inode_alloc_unlock(ctx);

  uint32_t ino_dir_u32 = (uint32_t)(inoent_dir - ctx->c_inotbl);
  uint32_t ino_new_u32 = (uint32_t)ino_new;
  // lock ordering: always by inode number to avoid deadlock with other ops
  if (ino_dir_u32 < ino_new_u32)
  {
    kafs_inode_lock(ctx, ino_dir_u32);
    kafs_inode_lock(ctx, ino_new_u32);
  }
  else
  {
    kafs_inode_lock(ctx, ino_new_u32);
    if (ino_dir_u32 != ino_new_u32)
      kafs_inode_lock(ctx, ino_dir_u32);
  }

  kafs_dlog(2, "%s: dirent_add start dir=%u name='%s'\n", __func__, (unsigned)ino_dir_u32,
            basepath);
  ret = kafs_dirent_add(ctx, inoent_dir, ino_new, basepath);
  kafs_dlog(2, "%s: dirent_add done rc=%d\n", __func__, ret);
  if (ret < 0)
  {
    memset(inoent_new, 0, sizeof(*inoent_new));
    if (ino_dir_u32 < ino_new_u32)
    {
      kafs_inode_unlock(ctx, ino_new_u32);
      kafs_inode_unlock(ctx, ino_dir_u32);
    }
    else
    {
      if (ino_dir_u32 != ino_new_u32)
        kafs_inode_unlock(ctx, ino_dir_u32);
      kafs_inode_unlock(ctx, ino_new_u32);
    }
    kafs_journal_abort(ctx, jseq, "dirent_add=%d", ret);
    return ret;
  }

  if (pino_dir != NULL)
    *pino_dir = inoent_dir - ctx->c_inotbl;
  if (pino_new != NULL)
    *pino_new = ino_new;

  // unlock in reverse order
  if (ino_dir_u32 < ino_new_u32)
  {
    kafs_inode_unlock(ctx, ino_new_u32);
    kafs_inode_unlock(ctx, ino_dir_u32);
  }
  else
  {
    if (ino_dir_u32 != ino_new_u32)
      kafs_inode_unlock(ctx, ino_dir_u32);
    kafs_inode_unlock(ctx, ino_new_u32);
  }

  // Update free inode accounting after allocation (no inode locks held).
  kafs_inode_alloc_lock(ctx);
  (void)kafs_sb_inocnt_free_decr(ctx->c_superblock);
  kafs_sb_wtime_set(ctx->c_superblock, kafs_now());
  kafs_inode_alloc_unlock(ctx);

  kafs_dlog(2, "%s: success ino=%u added to dir ino=%u\n", __func__, (unsigned)ino_new,
            (unsigned)(pino_dir ? *pino_dir : 0));
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
  uint32_t ino = (uint32_t)(inoent - ctx->c_inotbl);
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
  kafs_inode_lock(ctx, (uint32_t)(inoent - ctx->c_inotbl));
  int rc = kafs_truncate(ctx, inoent, (kafs_off_t)size);
  kafs_inode_unlock(ctx, (uint32_t)(inoent - ctx->c_inotbl));
  kafs_dlog(2, "%s: exit rc=%d path=%s ino=%" PRIuFAST32 " size=%" PRIuFAST64 " hotplug=fallback\n",
            __func__, (rc == 0 ? 0 : rc), path ? path : "(null)", ino, (uint64_t)size);
  return rc == 0 ? 0 : rc;
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
  uint32_t ino = (uint32_t)(inoent - ctx->c_inotbl);

#ifdef FALLOC_FL_PUNCH_HOLE
  if ((mode & FALLOC_FL_PUNCH_HOLE) == 0)
  {
    // 拡大系: 元 EOF 以降は hole とみなし、必要時のみ疎拡張する。
    if ((mode & FALLOC_FL_KEEP_SIZE) != 0)
      return 0;

    kafs_inode_lock(ctx, ino);
    kafs_off_t filesize = kafs_ino_size_get(inoent);
    int rc = 0;
    if (end_req > filesize)
      rc = kafs_truncate(ctx, inoent, end_req);
    kafs_inode_unlock(ctx, ino);
    return rc;
  }

  if ((mode & FALLOC_FL_KEEP_SIZE) == 0)
    return -EOPNOTSUPP;
#endif

  kafs_inode_lock(ctx, ino);
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
  char zbuf[blksize];
  memset(zbuf, 0, blksize);

  // 左端の部分ブロックは穴化せずゼロ埋めする。
  if (((kafs_off_t)offset & ((kafs_off_t)blksize - 1)) != 0)
  {
    kafs_iblkcnt_t iblo = (kafs_iblkcnt_t)((kafs_off_t)offset >> log_blksize);
    char wbuf[blksize];
    KAFS_CALL(kafs_ino_iblk_read_or_zero, ctx, inoent, iblo, wbuf);
    kafs_blksize_t s = (kafs_blksize_t)((kafs_off_t)offset & ((kafs_off_t)blksize - 1));
    kafs_blksize_t e = blksize;
    if ((kafs_off_t)(iblo + 1) << log_blksize > end)
      e = (kafs_blksize_t)(end & ((kafs_off_t)blksize - 1));
    if (e > s)
    {
      memset(wbuf + s, 0, e - s);
      KAFS_CALL(kafs_ino_iblk_write, ctx, inoent, iblo, wbuf);
    }
  }

  // 完全に範囲に含まれるブロックのみ穴化する。
  kafs_iblkcnt_t first_full = (kafs_iblkcnt_t)(((kafs_off_t)offset + blksize - 1) >> log_blksize);
  kafs_iblkcnt_t last_full_excl = (kafs_iblkcnt_t)(end >> log_blksize);
  for (kafs_iblkcnt_t iblo = first_full; iblo < last_full_excl; ++iblo)
  {
    if (filesize > KAFS_DIRECT_SIZE)
      KAFS_CALL(kafs_ino_iblk_release, ctx, inoent, iblo);
  }

  // 右端の部分ブロックは穴化せずゼロ埋めする。
  if ((end & ((kafs_off_t)blksize - 1)) != 0)
  {
    kafs_iblkcnt_t iblo = (kafs_iblkcnt_t)(end >> log_blksize);
    if (((kafs_off_t)offset >> log_blksize) != (kafs_off_t)iblo ||
        ((kafs_off_t)offset & ((kafs_off_t)blksize - 1)) == 0)
    {
      kafs_blksize_t e = (kafs_blksize_t)(end & ((kafs_off_t)blksize - 1));
      // 右端が EOF の場合、EOF 外は hole とみなし、論理ファイル範囲が全て対象なら穴化する。
      if (end == filesize)
      {
        kafs_blksize_t valid = (kafs_blksize_t)(filesize & ((kafs_off_t)blksize - 1));
        if (valid == 0)
          valid = blksize;
        if (e == valid && filesize > KAFS_DIRECT_SIZE)
        {
          KAFS_CALL(kafs_ino_iblk_release, ctx, inoent, iblo);
        }
        else
        {
          char wbuf[blksize];
          KAFS_CALL(kafs_ino_iblk_read_or_zero, ctx, inoent, iblo, wbuf);
          memset(wbuf, 0, e);
          KAFS_CALL(kafs_ino_iblk_write, ctx, inoent, iblo, wbuf);
        }
      }
      else
      {
        char wbuf[blksize];
        KAFS_CALL(kafs_ino_iblk_read_or_zero, ctx, inoent, iblo, wbuf);
        memset(wbuf, 0, e);
        KAFS_CALL(kafs_ino_iblk_write, ctx, inoent, iblo, wbuf);
      }
    }
  }

  kafs_inode_unlock(ctx, ino);
  return 0;
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
  uint32_t ino = (uint32_t)(inoent - ctx->c_inotbl);

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

  if (size <= KAFS_DIRECT_SIZE)
  {
    kafs_inode_unlock(ctx, ino);
    if (whence == SEEK_DATA)
      return off;
    return (off_t)size;
  }

  kafs_blksize_t blksize = kafs_sb_blksize_get(ctx->c_superblock);
  kafs_logblksize_t log_blksize = kafs_sb_log_blksize_get(ctx->c_superblock);
  kafs_iblkcnt_t start_iblo = (kafs_iblkcnt_t)((kafs_off_t)off >> log_blksize);
  kafs_iblkcnt_t end_iblo = (kafs_iblkcnt_t)((size + blksize - 1) >> log_blksize);

  if (whence == SEEK_DATA)
  {
    for (kafs_iblkcnt_t iblo = start_iblo; iblo < end_iblo; ++iblo)
    {
      kafs_blkcnt_t blo = KAFS_BLO_NONE;
      KAFS_CALL(kafs_ino_ibrk_run, ctx, inoent, iblo, &blo, KAFS_IBLKREF_FUNC_GET);
      if (blo != KAFS_BLO_NONE)
      {
        kafs_off_t pos = ((kafs_off_t)iblo << log_blksize);
        if (pos < (kafs_off_t)off)
          pos = (kafs_off_t)off;
        kafs_inode_unlock(ctx, ino);
        return (off_t)pos;
      }
    }
    kafs_inode_unlock(ctx, ino);
    return -ENXIO;
  }

  // SEEK_HOLE
  for (kafs_iblkcnt_t iblo = start_iblo; iblo < end_iblo; ++iblo)
  {
    kafs_blkcnt_t blo = KAFS_BLO_NONE;
    KAFS_CALL(kafs_ino_ibrk_run, ctx, inoent, iblo, &blo, KAFS_IBLKREF_FUNC_GET);
    if (blo == KAFS_BLO_NONE)
    {
      kafs_off_t pos = ((kafs_off_t)iblo << log_blksize);
      if (pos < (kafs_off_t)off)
        pos = (kafs_off_t)off;
      if (pos > size)
        pos = size;
      kafs_inode_unlock(ctx, ino);
      return (off_t)pos;
    }
  }

  kafs_inode_unlock(ctx, ino);
  return (off_t)size;
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
  kafs_sinode_t *inoent_new = &ctx->c_inotbl[ino_new];
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
  uint32_t ino_parent = (uint32_t)(inoent_dir - ctx->c_inotbl);
  uint32_t ino_target = (uint32_t)(inoent - ctx->c_inotbl);
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

  // Verify directory emptiness under lock (TOCTOU-safe)
  struct kafs_sdirent dirent;
  off_t offset = 0;
  while (1)
  {
    ssize_t r = kafs_dirent_read(ctx, inoent, &dirent, offset);
    if (r < 0)
    {
      kafs_inode_unlock(ctx, ino_target);
      kafs_inode_unlock(ctx, ino_parent);
      kafs_journal_abort(ctx, jseq, "dirent_read=%zd", r);
      return (int)r;
    }
    if (r == 0)
      break;
    if (strcmp(dirent.d_filename, "..") != 0)
    {
      kafs_inode_unlock(ctx, ino_target);
      kafs_inode_unlock(ctx, ino_parent);
      kafs_journal_abort(ctx, jseq, "ENOTEMPTY");
      return -ENOTEMPTY;
    }
    offset += r;
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
  uint32_t ino = (uint32_t)(inoent - ctx->c_inotbl);
  kafs_inode_lock(ctx, ino);
  ssize_t r = KAFS_CALL(kafs_pread, ctx, inoent, buf, buflen - 1, 0);
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
  ssize_t rr = kafs_pread(ctx, &ctx->c_inotbl[ino], buf, size, offset);
  kafs_inode_unlock(ctx, (uint32_t)ino);
  return rr;
}

static int kafs_op_write(const char *path, const char *buf, size_t size, off_t offset,
                         struct fuse_file_info *fi)
{
  kafs_dlog(3, "%s(path=%s, size=%zu, off=%" PRIuFAST64 ")\n", __func__, path ? path : "(null)",
            size, (uint64_t)offset);
  struct fuse_context *fctx = fuse_get_context();
  struct kafs_context *ctx = fctx->private_data;
  if (kafs_is_ctl_path(path))
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
  kafs_inode_lock(ctx, (uint32_t)ino);
  int rc_write = kafs_pwrite(ctx, &ctx->c_inotbl[ino], buf, size, offset);
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
        kafs_ino_get_usage(&ctx->c_inotbl[fh_ino]))
    {
      ino = (uint32_t)fh_ino;
      inoent = &ctx->c_inotbl[ino];
      KAFS_CALL(kafs_access_check, F_OK, inoent, KAFS_FALSE, fctx->uid, fctx->gid, 0, NULL);
    }
  }

  if (!inoent)
  {
    KAFS_CALL(kafs_access, fctx, ctx, path, fi, F_OK, &inoent);
    ino = (uint32_t)(inoent - ctx->c_inotbl);
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
  uint32_t ino_dir = (uint32_t)(inoent_dir - ctx->c_inotbl);

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
  target_ino = (kafs_inocnt_t)(inoent_target - ctx->c_inotbl);
  // unlink(2) should not remove directories
  if (S_ISDIR(kafs_ino_mode_get(inoent_target)))
  {
    kafs_inode_unlock(ctx, ino_dir);
    kafs_journal_abort(ctx, jseq, "EISDIR");
    return -EISDIR;
  }

  kafs_inocnt_t removed_ino = KAFS_INO_NONE;
  KAFS_CALL(kafs_dirent_remove_nolink, ctx, inoent_dir, basepath, &removed_ino);
  kafs_inode_unlock(ctx, ino_dir);

  if (removed_ino != target_ino)
    return -ESTALE;

  // Decrement link count under target inode lock (keep dir lock hold time short)
  kafs_inode_lock(ctx, (uint32_t)removed_ino);
  kafs_sinode_t *t = &ctx->c_inotbl[removed_ino];
  kafs_linkcnt_t nl = kafs_ino_linkcnt_decr(t);
  int reclaimed_now = 0;
  if (nl == 0)
  {
    kafs_ino_dtime_set(t, kafs_now());
    (void)kafs_try_reclaim_unlinked_inode_locked(ctx, removed_ino, &reclaimed_now);
  }
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

static int kafs_op_rename(const char *from, const char *to, unsigned int flags)
{
  // 最小実装: 通常ファイルのみ対応。RENAME_NOREPLACE は尊重。その他のフラグは未対応。
  struct fuse_context *fctx = fuse_get_context();
  struct kafs_context *ctx = fctx->private_data;
  kafs_dlog(2, "%s: enter from=%s to=%s flags=%u\n", __func__, from ? from : "(null)",
            to ? to : "(null)", flags);
  if (kafs_is_ctl_path(from) || kafs_is_ctl_path(to))
    return -EACCES;
  if (from == NULL || to == NULL || from[0] != '/' || to[0] != '/')
    return -EINVAL;
  if (strcmp(from, to) == 0)
    return 0;

  // ディレクトリを自身の配下へ移動する rename は許可しない（ループ防止）。
  // 例: rename("/a", "/a/b")
  size_t from_len = strlen(from);
  if (from_len > 1 && strncmp(to, from, from_len) == 0 && to[from_len] == '/')
    return -EINVAL;

  const unsigned int supported = RENAME_NOREPLACE;
  if (flags & ~supported)
    return -EOPNOTSUPP;

  // 事前に対象 inode を確認（ディレクトリは同一親ディレクトリ内のみ対応）
  kafs_sinode_t *inoent_src;
  int rc = kafs_access(fctx, ctx, from, NULL, F_OK, &inoent_src);
  if (rc < 0)
    return rc;
  kafs_mode_t src_mode = kafs_ino_mode_get(inoent_src);
  int src_is_dir = S_ISDIR(src_mode) ? 1 : 0;
  if (!S_ISREG(src_mode) && !S_ISLNK(src_mode) && !src_is_dir)
    return -EOPNOTSUPP;

  // パス分解
  char from_copy[strlen(from) + 1];
  strcpy(from_copy, from);
  const char *from_dir = from_copy;
  char *from_base = strrchr(from_copy, '/');
  if (from_dir == from_base)
    from_dir = "/";
  *from_base = '\0';
  from_base++;
  if (from_base[0] == '\0')
    return -EINVAL;
  if (strcmp(from_base, ".") == 0 || strcmp(from_base, "..") == 0)
    return -EINVAL;

  char to_copy[strlen(to) + 1];
  strcpy(to_copy, to);
  const char *to_dir = to_copy;
  char *to_base = strrchr(to_copy, '/');
  if (to_dir == to_base)
    to_dir = "/";
  *to_base = '\0';
  to_base++;
  if (to_base[0] == '\0')
    return -EINVAL;
  if (strcmp(to_base, ".") == 0 || strcmp(to_base, "..") == 0)
    return -EINVAL;

  uint64_t jseq = kafs_journal_begin(ctx, "RENAME", "from=%s to=%s flags=%u", from, to, flags);

  kafs_sinode_t *inoent_dir_from;
  KAFS_CALL(kafs_access, fctx, ctx, from_dir, NULL, W_OK, &inoent_dir_from);
  kafs_sinode_t *inoent_dir_to;
  KAFS_CALL(kafs_access, fctx, ctx, to_dir, NULL, W_OK, &inoent_dir_to);
  uint32_t ino_from_dir = (uint32_t)(inoent_dir_from - ctx->c_inotbl);
  uint32_t ino_to_dir = (uint32_t)(inoent_dir_to - ctx->c_inotbl);

  // RENAME_NOREPLACE: 既存なら EEXIST
  if (flags & RENAME_NOREPLACE)
  {
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
  }

  // 取得しておく（from の inode番号）
  kafs_inocnt_t ino_src = (kafs_inocnt_t)(inoent_src - ctx->c_inotbl);

  // 置換先が存在する場合は削除（通常ファイルのみ）
  kafs_sinode_t *inoent_to_exist = NULL;
  int exists_to = kafs_access(fctx, ctx, to, NULL, F_OK, &inoent_to_exist);
  if (exists_to == 0)
  {
    kafs_mode_t dst_mode = kafs_ino_mode_get(inoent_to_exist);
    int dst_is_dir = S_ISDIR(dst_mode) ? 1 : 0;
    if (src_is_dir)
    {
      if (!dst_is_dir)
      {
        kafs_journal_abort(ctx, jseq, "DST_NOT_DIR");
        return -ENOTDIR;
      }
    }
    else
    {
      if (dst_is_dir)
      {
        kafs_journal_abort(ctx, jseq, "DST_IS_DIR");
        return -EISDIR;
      }
      if (!S_ISREG(dst_mode) && !S_ISLNK(dst_mode))
      {
        kafs_journal_abort(ctx, jseq, "DST_NOT_FILE");
        return -EOPNOTSUPP;
      }
    }
  }

  // ロック順序: 関係 inode を番号昇順に取得（親ディレクトリ2つ + 対象 inode + (置換先 inode)）。
  uint32_t ino_src_u32 = (uint32_t)ino_src;
  uint32_t ino_dst_u32 = UINT32_MAX;
  if (exists_to == 0 && inoent_to_exist)
    ino_dst_u32 = (uint32_t)(inoent_to_exist - ctx->c_inotbl);

  uint32_t lock_list[4];
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

  for (size_t i = 0; i < lock_n; ++i)
    kafs_inode_lock(ctx, lock_list[i]);

  // ディレクトリ置換の場合は、空 (".." のみ) を確認して ".." を除去する（親リンク数整合）。
  if (src_is_dir && exists_to == 0 && inoent_to_exist)
  {
    struct kafs_sdirent dirent;
    off_t off = 0;
    while (1)
    {
      ssize_t r = kafs_dirent_read(ctx, inoent_to_exist, &dirent, off);
      if (r < 0)
      {
        kafs_journal_abort(ctx, jseq, "dst_dirent_read=%zd", r);
        for (size_t i = lock_n; i > 0; --i)
          kafs_inode_unlock(ctx, lock_list[i - 1]);
        return (int)r;
      }
      if (r == 0)
        break;
      if (strcmp(dirent.d_filename, "..") != 0)
      {
        kafs_journal_abort(ctx, jseq, "DST_DIR_NOT_EMPTY");
        for (size_t i = lock_n; i > 0; --i)
          kafs_inode_unlock(ctx, lock_list[i - 1]);
        return -ENOTEMPTY;
      }
      off += r;
    }

    int rr = kafs_dirent_remove(ctx, inoent_to_exist, "..");
    if (rr < 0)
    {
      kafs_journal_abort(ctx, jseq, "dst_remove_dotdot=%d", rr);
      for (size_t i = lock_n; i > 0; --i)
        kafs_inode_unlock(ctx, lock_list[i - 1]);
      return rr;
    }
  }

  // 置換先が存在していればエントリを除去（rename では moved inode の linkcount は変えない）
  kafs_inocnt_t removed_dst_ino = KAFS_INO_NONE;
  if (exists_to == 0)
  {
    // Remove dirent only; decrement linkcount later under inode lock.
    KAFS_CALL(kafs_dirent_remove_nolink, ctx, inoent_dir_to, to_base, &removed_dst_ino);
  }
  // from から削除（rename ではリンク数を変更しない）
  kafs_inocnt_t moved_ino = KAFS_INO_NONE;
  KAFS_CALL(kafs_dirent_remove_nolink, ctx, inoent_dir_from, from_base, &moved_ino);
  if (moved_ino != ino_src)
    return -ESTALE;
  // to に追加（rename ではリンク数を変更しない）
  KAFS_CALL(kafs_dirent_add_nolink, ctx, inoent_dir_to, ino_src, to_base);

  // ディレクトリを親またぎで移動した場合は、".." を新しい親へ付け替える。
  if (src_is_dir && ino_from_dir != ino_to_dir)
  {
    int rr = kafs_dirent_remove(ctx, inoent_src, "..");
    if (rr < 0)
    {
      kafs_journal_abort(ctx, jseq, "src_remove_dotdot=%d", rr);
      for (size_t i = lock_n; i > 0; --i)
        kafs_inode_unlock(ctx, lock_list[i - 1]);
      return rr;
    }
    rr = kafs_dirent_add(ctx, inoent_src, (kafs_inocnt_t)ino_to_dir, "..");
    if (rr < 0)
    {
      kafs_journal_abort(ctx, jseq, "src_add_dotdot=%d", rr);
      for (size_t i = lock_n; i > 0; --i)
        kafs_inode_unlock(ctx, lock_list[i - 1]);
      return rr;
    }
  }

  // ロック解除（取得の逆順）
  for (size_t i = lock_n; i > 0; --i)
    kafs_inode_unlock(ctx, lock_list[i - 1]);

  // If we replaced an existing dst, decrement its linkcount under inode lock.
  if (removed_dst_ino != KAFS_INO_NONE)
  {
    kafs_inode_lock(ctx, (uint32_t)removed_dst_ino);
    kafs_sinode_t *dst = &ctx->c_inotbl[removed_dst_ino];
    kafs_linkcnt_t nl = kafs_ino_linkcnt_decr(dst);
    int reclaimed_dst = 0;
    if (nl == 0)
    {
      kafs_ino_dtime_set(dst, kafs_now());
      (void)kafs_try_reclaim_unlinked_inode_locked(ctx, removed_dst_ino, &reclaimed_dst);
    }
    kafs_inode_unlock(ctx, (uint32_t)removed_dst_ino);

    if (reclaimed_dst)
    {
      kafs_inode_alloc_lock(ctx);
      (void)kafs_sb_inocnt_free_incr(ctx->c_superblock);
      kafs_sb_wtime_set(ctx->c_superblock, kafs_now());
      kafs_inode_alloc_unlock(ctx);
    }
  }

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
  uint32_t ino = (uint32_t)(inoent - ctx->c_inotbl);
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
  uint32_t ino = (uint32_t)(inoent - ctx->c_inotbl);
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
  kafs_sinode_t *inoent = &ctx->c_inotbl[ino];
  ssize_t w = KAFS_CALL(kafs_pwrite, ctx, inoent, target, strlen(target), 0);
  assert(w == (ssize_t)strlen(target));
  kafs_journal_commit(ctx, jseq);
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

  uint32_t ino = KAFS_INO_NONE;
  if (fi)
    ino = (uint32_t)fi->fh;
  else if (path && path[0] != '\0')
  {
    kafs_sinode_t *inoent = NULL;
    int arc = kafs_access(fctx, ctx, path, fi, F_OK, &inoent);
    if (arc == 0 && inoent)
      ino = (uint32_t)(inoent - ctx->c_inotbl);
  }
  if (ino != KAFS_INO_NONE)
  {
    uint32_t saved_mode = KAFS_PENDING_WORKER_PRIO_NORMAL;
    int saved_nice = 0;
    int boosted = 0;
    // If caller is about to block for pending-drain, temporarily boost worker priority.
    kafs_pending_worker_begin_boost(ctx, &saved_mode, &saved_nice, &boosted);

    int drc = kafs_pendinglog_drain_inode(ctx, ino);
    kafs_pending_worker_end_boost(ctx, saved_mode, saved_nice, boosted);
    if (drc < 0)
    {
      if (drc == -ETIMEDOUT)
      {
        // Keep fsync path robust under temporary pending-worker congestion.
        kafs_log(KAFS_LOG_WARNING,
                 "%s: pending drain timeout path=%s ino=%" PRIuFAST32 " (continue)\n", __func__,
                 path ? path : "(null)", ino);
      }
      else
      {
        kafs_dlog(2, "%s: exit rc=%d drain_failed path=%s ino=%" PRIuFAST32 "\n", __func__, drc,
                  path ? path : "(null)", ino);
        return drc;
      }
    }
  }

  // Runtime fsync policy:
  // - full: always issue backing fsync/fdatasync
  // - journal_only: journal durability barrier only
  // - adaptive: journal_only for fdatasync and during pending-TTL pressure, else full
  uint32_t policy = ctx->c_fsync_policy;
  int journal_enabled = kafs_journal_is_enabled(ctx) ? 1 : 0;
  int journal_only_mode = 0;
  if (journal_enabled)
  {
    if (policy == KAFS_FSYNC_POLICY_JOURNAL_ONLY)
      journal_only_mode = 1;
    else if (policy == KAFS_FSYNC_POLICY_ADAPTIVE)
    {
      if (isdatasync || ctx->c_pending_ttl_over_soft || ctx->c_pending_ttl_over_hard)
        journal_only_mode = 1;
    }
  }

  if (journal_only_mode)
  {
    kafs_journal_force_flush(ctx);
    kafs_dlog(2,
              "%s: exit rc=0 path=%s ino=%" PRIuFAST32
              " mode=journal-only isdatasync=%d policy=%" PRIuFAST32 "\n",
              __func__, path ? path : "(null)", ino, isdatasync, policy);
    return 0;
  }

  kafs_journal_force_flush(ctx);
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
    cfg->hard_remove = 1;
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
  return fctx ? fctx->private_data : NULL;
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
      kafs_inode_lock(ctx, (uint32_t)ino);
      (void)kafs_try_reclaim_unlinked_inode_locked(ctx, ino, &reclaimed);
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
          "  %s [--image <image>] --migrate-v2 [--yes] <mountpoint> [FUSE options...]\n"
          "\n"
          "Options:\n"
          "  [Global]\n"
          "    -h, --help                        Show this help and exit\n"
          "    --image <image>                   Image path\n"
          "    --image=<image>                   Image path (inline form)\n"
          "    --migrate-v2                      One-shot v2->v3 migration, then exit\n"
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
          "    -o bg_dedup_interval_ms=<N>       Idle dedup scan interval (ms)\n"
          "    -o dedup_interval_ms=<N>          Alias of bg_dedup_interval_ms\n"
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
          "    KAFS_BG_DEDUP_INTERVAL_MS         idle dedup scan interval (ms)\n"
          "    KAFS_FSYNC_POLICY                 fsync policy default\n"
          "    KAFS_HOTPLUG_UDS                  Hotplug UDS path (legacy/env)\n"
          "    KAFS_HOTPLUG_BACK_BIN             Backend binary path hint\n"
          "\n"
          "Notes:\n"
          "    v2 images are refused by default. Use `kafsctl migrate <image> [--yes]`\n"
          "    or `--migrate-v2` for startup migration.\n"
          "\n"
          "Examples:\n"
          "  %s --image test.img mnt -f\n"
          "  %s --image legacy.img --migrate-v2 --yes mnt -f\n"
          "  %s --image test.img mnt -f -o multi_thread=8\n",
          prog, prog, prog, prog, prog, prog);
}

static int kafs_confirm_yes_stdin(void)
{
  fprintf(stderr, "WARNING: migration is irreversible. type 'YES' to continue: ");
  fflush(stderr);
  char buf[32];
  if (!fgets(buf, sizeof(buf), stdin))
    return 0;
  buf[strcspn(buf, "\r\n")] = '\0';
  return strcmp(buf, "YES") == 0;
}

static int kafs_migrate_v2_image(const char *image_path, int assume_yes)
{
  if (!image_path)
    return -EINVAL;

  int fd = open(image_path, O_RDWR, 0666);
  if (fd < 0)
    return -errno;

  kafs_ssuperblock_t sb;
  ssize_t r = pread(fd, &sb, sizeof(sb), 0);
  if (r != (ssize_t)sizeof(sb))
  {
    int err = -errno;
    close(fd);
    return err ? err : -EIO;
  }
  if (kafs_sb_magic_get(&sb) != KAFS_MAGIC)
  {
    close(fd);
    return -EINVAL;
  }

  uint32_t fmt = kafs_sb_format_version_get(&sb);
  if (fmt == KAFS_FORMAT_VERSION)
  {
    close(fd);
    return 1;
  }
  if (fmt != KAFS_FORMAT_VERSION_V2)
  {
    close(fd);
    return -EPROTONOSUPPORT;
  }

  if (!assume_yes && !kafs_confirm_yes_stdin())
  {
    close(fd);
    return -ECANCELED;
  }

  kafs_sb_format_version_set(&sb, KAFS_FORMAT_VERSION);
  if (kafs_sb_allocator_size_get(&sb) > 0)
  {
    if (kafs_sb_allocator_version_get(&sb) < 2u)
      kafs_sb_allocator_version_set(&sb, 2u);
    uint64_t ff = kafs_sb_feature_flags_get(&sb);
    kafs_sb_feature_flags_set(&sb, ff | KAFS_FEATURE_ALLOC_V2);
  }

  if (pwrite(fd, &sb, sizeof(sb), 0) != (ssize_t)sizeof(sb))
  {
    int err = -errno;
    close(fd);
    return err ? err : -EIO;
  }
  if (fsync(fd) != 0)
  {
    int err = -errno;
    close(fd);
    return err;
  }

  close(fd);
  return 0;
}

static void kafs_signal_handler(int sig)
{
  const char *name = strsignal(sig);
  kafs_log(KAFS_LOG_ERR, "kafs: caught signal %d (%s)\n", sig, name ? name : "?");
#ifdef __linux__
  void *bt[64];
  int n = backtrace(bt, (int)(sizeof(bt) / sizeof(bt[0])));
  char **syms = backtrace_symbols(bt, n);
  if (syms)
  {
    for (int i = 0; i < n; ++i)
      kafs_log(KAFS_LOG_ERR, "  bt[%02d]=%s\n", i, syms[i]);
  }
#endif
  signal(sig, SIG_DFL);
  raise(sig);
}

static void kafs_install_crash_handlers(void)
{
  struct sigaction sa;
  memset(&sa, 0, sizeof(sa));
  sa.sa_handler = kafs_signal_handler;
  sigemptyset(&sa.sa_mask);
  sigaction(SIGSEGV, &sa, NULL);
  sigaction(SIGABRT, &sa, NULL);
  sigaction(SIGBUS, &sa, NULL);
  sigaction(SIGILL, &sa, NULL);
  sigaction(SIGFPE, &sa, NULL);
}

#ifndef KAFS_NO_MAIN
int main(int argc, char **argv)
{
  kafs_install_crash_handlers();
  // 画像ファイル指定を受け取る: --image <path> または --image=<path>、環境変数 KAFS_IMAGE
  const char *image_path = getenv("KAFS_IMAGE");
  kafs_bool_t auto_migrate_v2 = KAFS_FALSE;
  kafs_bool_t migrate_yes = KAFS_FALSE;
  kafs_bool_t writeback_cache_enabled = KAFS_TRUE;
  kafs_bool_t writeback_cache_explicit = KAFS_FALSE;
  kafs_bool_t trim_on_free_enabled = KAFS_FALSE;
  kafs_bool_t trim_on_free_explicit = KAFS_FALSE;
  char hotplug_uds_opt[sizeof(((struct sockaddr_un *)0)->sun_path)];
  hotplug_uds_opt[0] = '\0';
  char hotplug_back_bin_opt[PATH_MAX];
  hotplug_back_bin_opt[0] = '\0';
  const char *wbc_env = getenv("KAFS_WRITEBACK_CACHE");
  if (wbc_env && *wbc_env)
  {
    if (strcmp(wbc_env, "0") == 0 || strcasecmp(wbc_env, "false") == 0 ||
        strcasecmp(wbc_env, "off") == 0 || strcasecmp(wbc_env, "no") == 0)
      writeback_cache_enabled = KAFS_FALSE;
    else if (strcmp(wbc_env, "1") == 0 || strcasecmp(wbc_env, "true") == 0 ||
             strcasecmp(wbc_env, "on") == 0 || strcasecmp(wbc_env, "yes") == 0)
      writeback_cache_enabled = KAFS_TRUE;
  }
  const char *trim_env = getenv("KAFS_TRIM_ON_FREE");
  if (trim_env && *trim_env)
  {
    if (strcmp(trim_env, "0") == 0 || strcasecmp(trim_env, "false") == 0 ||
        strcasecmp(trim_env, "off") == 0 || strcasecmp(trim_env, "no") == 0)
      trim_on_free_enabled = KAFS_FALSE;
    else if (strcmp(trim_env, "1") == 0 || strcasecmp(trim_env, "true") == 0 ||
             strcasecmp(trim_env, "on") == 0 || strcasecmp(trim_env, "yes") == 0)
      trim_on_free_enabled = KAFS_TRUE;
  }
  // argv から --image と --help を取り除き fuse_main へは渡さない
  char *argv_clean[argc];
  int argc_clean = 0;
  kafs_bool_t show_help = KAFS_FALSE;
  for (int i = 0; i < argc; ++i)
  {
    const char *a = argv[i];
    if (strcmp(a, "--help") == 0 || strcmp(a, "-h") == 0)
    {
      show_help = KAFS_TRUE;
      continue;
    }
    if (strcmp(a, "--migrate-v2") == 0)
    {
      auto_migrate_v2 = KAFS_TRUE;
      continue;
    }
    if (strcmp(a, "--yes") == 0)
    {
      migrate_yes = KAFS_TRUE;
      continue;
    }
    if (strcmp(a, "--no-writeback-cache") == 0)
    {
      writeback_cache_enabled = KAFS_FALSE;
      writeback_cache_explicit = KAFS_TRUE;
      continue;
    }
    if (strcmp(a, "--writeback-cache") == 0)
    {
      writeback_cache_enabled = KAFS_TRUE;
      writeback_cache_explicit = KAFS_TRUE;
      continue;
    }
    if (strcmp(a, "--trim-on-free") == 0)
    {
      trim_on_free_enabled = KAFS_TRUE;
      trim_on_free_explicit = KAFS_TRUE;
      continue;
    }
    if (strcmp(a, "--no-trim-on-free") == 0)
    {
      trim_on_free_enabled = KAFS_FALSE;
      trim_on_free_explicit = KAFS_TRUE;
      continue;
    }
    if (strcmp(a, "--hotplug") == 0)
    {
      snprintf(hotplug_uds_opt, sizeof(hotplug_uds_opt), "%s", KAFS_HOTPLUG_UDS_DEFAULT);
      continue;
    }
    if (strncmp(a, "--hotplug=", 10) == 0)
    {
      const char *v = a + 10;
      if (!*v || snprintf(hotplug_uds_opt, sizeof(hotplug_uds_opt), "%s", v) >=
                     (int)sizeof(hotplug_uds_opt))
      {
        fprintf(stderr, "invalid --hotplug value\n");
        return 2;
      }
      continue;
    }
    if (strcmp(a, "--hotplug-uds") == 0)
    {
      if (i + 1 < argc)
      {
        const char *v = argv[++i];
        if (!*v || snprintf(hotplug_uds_opt, sizeof(hotplug_uds_opt), "%s", v) >=
                       (int)sizeof(hotplug_uds_opt))
        {
          fprintf(stderr, "invalid --hotplug-uds value\n");
          return 2;
        }
        continue;
      }
      fprintf(stderr, "--hotplug-uds requires a path argument.\n");
      usage(argv[0]);
      return 2;
    }
    if (strncmp(a, "--hotplug-uds=", 14) == 0)
    {
      const char *v = a + 14;
      if (!*v || snprintf(hotplug_uds_opt, sizeof(hotplug_uds_opt), "%s", v) >=
                     (int)sizeof(hotplug_uds_opt))
      {
        fprintf(stderr, "invalid --hotplug-uds value\n");
        return 2;
      }
      continue;
    }
    if (strcmp(a, "--hotplug-back-bin") == 0)
    {
      if (i + 1 < argc)
      {
        const char *v = argv[++i];
        if (!*v || snprintf(hotplug_back_bin_opt, sizeof(hotplug_back_bin_opt), "%s", v) >=
                       (int)sizeof(hotplug_back_bin_opt))
        {
          fprintf(stderr, "invalid --hotplug-back-bin value\n");
          return 2;
        }
        continue;
      }
      fprintf(stderr, "--hotplug-back-bin requires a path argument.\n");
      usage(argv[0]);
      return 2;
    }
    if (strncmp(a, "--hotplug-back-bin=", 19) == 0)
    {
      const char *v = a + 19;
      if (!*v || snprintf(hotplug_back_bin_opt, sizeof(hotplug_back_bin_opt), "%s", v) >=
                     (int)sizeof(hotplug_back_bin_opt))
      {
        fprintf(stderr, "invalid --hotplug-back-bin value\n");
        return 2;
      }
      continue;
    }
    if (strcmp(a, "--option") == 0)
    {
      if (i + 1 < argc)
      {
        argv_clean[argc_clean++] = "-o";
        argv_clean[argc_clean++] = argv[++i];
        continue;
      }
      fprintf(stderr, "--option requires an argument.\n");
      usage(argv[0]);
      return 2;
    }
    if (strncmp(a, "--option=", 9) == 0)
    {
      argv_clean[argc_clean++] = "-o";
      argv_clean[argc_clean++] = (char *)(a + 9);
      continue;
    }
    if (strcmp(a, "--image") == 0)
    {
      if (i + 1 < argc)
      {
        image_path = argv[++i];
        continue;
      }
      else
      {
        fprintf(stderr, "--image requires a path argument.\n");
        usage(argv[0]);
        return 2;
      }
    }
    if (strncmp(a, "--image=", 8) == 0)
    {
      image_path = a + 8;
      continue;
    }
    // pass other args through to FUSE
    argv_clean[argc_clean++] = argv[i];
  }
  // mount(8) helper compatibility: allow "kafs <image> <mountpoint> [FUSE options...]"
  // When invoked via mount -t fuse.kafs, the image path is typically passed as the first argument.
  if (image_path == NULL && argc_clean >= 3 && argv_clean[1][0] != '-')
  {
    image_path = argv_clean[1];
    for (int i = 1; i + 1 < argc_clean; ++i)
      argv_clean[i] = argv_clean[i + 1];
    argc_clean--;
  }
  if (show_help || image_path == NULL || argc_clean < 2)
  {
    usage(argv[0]);
    return 2;
  }

  // Raspi/低リソース前提: 既定は単一スレッド。MT は -o multi_thread[=N] か KAFS_MT=1 で有効化。
  const char *mt = getenv("KAFS_MT");
  kafs_bool_t enable_mt = (mt && strcmp(mt, "1") == 0) ? KAFS_TRUE : KAFS_FALSE;
  unsigned mt_cnt_override = 0;
  int mt_cnt_override_set = 0;
  int saw_max_threads = 0;
  uint32_t pending_worker_prio_mode = KAFS_PENDING_WORKER_PRIO_NORMAL;
  int pending_worker_nice = 0;
  uint32_t pending_ttl_soft_ms = 5000;
  uint32_t pending_ttl_hard_ms = 30000;
  uint32_t pending_cap_initial = 0;
  uint32_t pending_cap_min = 0;
  uint32_t pending_cap_max = 0;
  uint32_t bg_dedup_scan_enabled = 1u;
  uint32_t bg_dedup_interval_ms = KAFS_BG_DEDUP_INTERVAL_MS_DEFAULT;
  uint32_t fsync_policy = KAFS_FSYNC_POLICY_JOURNAL_ONLY;

  const char *pprio = getenv("KAFS_PENDING_WORKER_PRIO");
  if (pprio && *pprio)
  {
    if (kafs_pending_worker_prio_mode_parse(pprio, &pending_worker_prio_mode) != 0)
    {
      fprintf(stderr, "invalid KAFS_PENDING_WORKER_PRIO: '%s'\n", pprio);
      return 2;
    }
  }
  const char *pnice = getenv("KAFS_PENDING_WORKER_NICE");
  if (pnice && *pnice)
  {
    char *endp = NULL;
    long v = strtol(pnice, &endp, 10);
    if (!endp || *endp != '\0' || v < 0 || v > 19)
    {
      fprintf(stderr, "invalid KAFS_PENDING_WORKER_NICE: '%s'\n", pnice);
      return 2;
    }
    pending_worker_nice = (int)v;
  }

  const char *pttl_soft = getenv("KAFS_PENDING_TTL_SOFT_MS");
  if (pttl_soft && *pttl_soft)
  {
    if (kafs_parse_u32_range(pttl_soft, 0, 3600000u, &pending_ttl_soft_ms) != 0)
    {
      fprintf(stderr, "invalid KAFS_PENDING_TTL_SOFT_MS: '%s'\n", pttl_soft);
      return 2;
    }
  }

  const char *pttl_hard = getenv("KAFS_PENDING_TTL_HARD_MS");
  if (pttl_hard && *pttl_hard)
  {
    if (kafs_parse_u32_range(pttl_hard, 0, 3600000u, &pending_ttl_hard_ms) != 0)
    {
      fprintf(stderr, "invalid KAFS_PENDING_TTL_HARD_MS: '%s'\n", pttl_hard);
      return 2;
    }
  }

  const char *pcap_initial = getenv("KAFS_PENDINGLOG_CAP_INITIAL");
  if (!pcap_initial || !*pcap_initial)
    pcap_initial = getenv("KAFS_PENDING_CAP_INITIAL");
  if (pcap_initial && *pcap_initial)
  {
    if (kafs_parse_u32_range(pcap_initial, 0, 1000000000u, &pending_cap_initial) != 0)
    {
      fprintf(stderr, "invalid KAFS_PENDINGLOG_CAP_INITIAL: '%s'\n", pcap_initial);
      return 2;
    }
  }

  const char *pcap_min = getenv("KAFS_PENDINGLOG_CAP_MIN");
  if (!pcap_min || !*pcap_min)
    pcap_min = getenv("KAFS_PENDING_CAP_MIN");
  if (pcap_min && *pcap_min)
  {
    if (kafs_parse_u32_range(pcap_min, 0, 1000000000u, &pending_cap_min) != 0)
    {
      fprintf(stderr, "invalid KAFS_PENDINGLOG_CAP_MIN: '%s'\n", pcap_min);
      return 2;
    }
  }

  const char *pcap_max = getenv("KAFS_PENDINGLOG_CAP_MAX");
  if (!pcap_max || !*pcap_max)
    pcap_max = getenv("KAFS_PENDING_CAP_MAX");
  if (pcap_max && *pcap_max)
  {
    if (kafs_parse_u32_range(pcap_max, 0, 1000000000u, &pending_cap_max) != 0)
    {
      fprintf(stderr, "invalid KAFS_PENDINGLOG_CAP_MAX: '%s'\n", pcap_max);
      return 2;
    }
  }

  const char *bg_scan = getenv("KAFS_BG_DEDUP_SCAN");
  if (bg_scan && *bg_scan)
  {
    if (kafs_parse_onoff(bg_scan, &bg_dedup_scan_enabled) != 0)
    {
      fprintf(stderr, "invalid KAFS_BG_DEDUP_SCAN: '%s'\n", bg_scan);
      return 2;
    }
  }

  const char *bg_interval = getenv("KAFS_BG_DEDUP_INTERVAL_MS");
  if (bg_interval && *bg_interval)
  {
    if (kafs_parse_u32_range(bg_interval, KAFS_BG_DEDUP_INTERVAL_MS_MIN,
                             KAFS_BG_DEDUP_INTERVAL_MS_MAX, &bg_dedup_interval_ms) != 0)
    {
      fprintf(stderr, "invalid KAFS_BG_DEDUP_INTERVAL_MS: '%s'\n", bg_interval);
      return 2;
    }
  }

  if (pending_ttl_soft_ms > 0 && pending_ttl_hard_ms > 0 &&
      pending_ttl_hard_ms < pending_ttl_soft_ms)
  {
    fprintf(stderr, "invalid pending TTL env: hard must be >= soft\n");
    return 2;
  }

  const char *fsp = getenv("KAFS_FSYNC_POLICY");
  if (fsp && *fsp)
  {
    if (kafs_fsync_policy_parse(fsp, &fsync_policy) != 0)
    {
      fprintf(stderr, "invalid KAFS_FSYNC_POLICY: '%s'\n", fsp);
      return 2;
    }
  }

  // Custom -o option: multi_thread[=N] (alias: multi-thread, multithread).
  // Strip it from argv before passing to libfuse, and translate to max_threads=.
  {
    char *argv_user[argc_clean];
    int argc_user = 0;
    char *o_owned[argc_clean];
    int o_owned_cnt = 0;

    for (int i = 0; i < argc_clean; ++i)
    {
      const char *a = argv_clean[i];
      const char *oval = NULL;
      int is_compact = 0;
      if (strcmp(a, "-o") == 0)
      {
        if (i + 1 < argc_clean)
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

      if (oval)
      {
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
          if (strncmp(tok, "max_threads=", 12) == 0 || strcmp(tok, "max_threads") == 0)
            saw_max_threads = 1;

          if (strcmp(tok, "writeback_cache") == 0)
          {
            writeback_cache_enabled = KAFS_TRUE;
            writeback_cache_explicit = KAFS_TRUE;
            continue;
          }
          if (strcmp(tok, "no_writeback_cache") == 0)
          {
            writeback_cache_enabled = KAFS_FALSE;
            writeback_cache_explicit = KAFS_TRUE;
            continue;
          }
          if (strcmp(tok, "trim_on_free") == 0 || strcmp(tok, "trim-on-free") == 0)
          {
            trim_on_free_enabled = KAFS_TRUE;
            trim_on_free_explicit = KAFS_TRUE;
            continue;
          }
          if (strcmp(tok, "no_trim_on_free") == 0 || strcmp(tok, "no-trim-on-free") == 0)
          {
            trim_on_free_enabled = KAFS_FALSE;
            trim_on_free_explicit = KAFS_TRUE;
            continue;
          }

          if (strcmp(tok, "hotplug") == 0)
          {
            snprintf(hotplug_uds_opt, sizeof(hotplug_uds_opt), "%s", KAFS_HOTPLUG_UDS_DEFAULT);
            continue;
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
            if (!*hotplug_uds_v || snprintf(hotplug_uds_opt, sizeof(hotplug_uds_opt), "%s",
                                            hotplug_uds_v) >= (int)sizeof(hotplug_uds_opt))
            {
              fprintf(stderr, "invalid -o hotplug uds path: '%s'\n", hotplug_uds_v);
              free(dup);
              return 2;
            }
            continue;
          }

          const char *hotplug_back_bin_v = NULL;
          if (strncmp(tok, "hotplug_back_bin=", 17) == 0)
            hotplug_back_bin_v = tok + 17;
          else if (strncmp(tok, "hotplug-back-bin=", 17) == 0)
            hotplug_back_bin_v = tok + 17;
          if (hotplug_back_bin_v)
          {
            if (!*hotplug_back_bin_v ||
                snprintf(hotplug_back_bin_opt, sizeof(hotplug_back_bin_opt), "%s",
                         hotplug_back_bin_v) >= (int)sizeof(hotplug_back_bin_opt))
            {
              fprintf(stderr, "invalid -o hotplug_back_bin path: '%s'\n", hotplug_back_bin_v);
              free(dup);
              return 2;
            }
            continue;
          }

          if (strcmp(tok, "multi_thread") == 0 || strcmp(tok, "multi-thread") == 0 ||
              strcmp(tok, "multithread") == 0)
          {
            want_mt = 1;
            continue;
          }

          const char *vstr = NULL;
          if (strncmp(tok, "multi_thread=", 13) == 0)
            vstr = tok + 13;
          else if (strncmp(tok, "multi-thread=", 13) == 0)
            vstr = tok + 13;
          else if (strncmp(tok, "multithread=", 12) == 0)
            vstr = tok + 12;

          if (vstr)
          {
            char *endp = NULL;
            unsigned long v = strtoul(vstr, &endp, 10);
            if (!endp || *endp != '\0')
            {
              fprintf(stderr, "invalid -o multi_thread=N: '%s'\n", vstr);
              free(dup);
              return 2;
            }
            if (v < 1)
              v = 1;
            if (v > 100000)
              v = 100000;
            mt_cnt_override = (unsigned)v;
            mt_cnt_override_set = 1;
            want_mt = 1;
            continue;
          }

          const char *prio_str = NULL;
          if (strncmp(tok, "pending_worker_prio=", 20) == 0)
            prio_str = tok + 20;
          else if (strncmp(tok, "dedup_worker_prio=", 18) == 0)
            prio_str = tok + 18;
          if (prio_str)
          {
            if (kafs_pending_worker_prio_mode_parse(prio_str, &pending_worker_prio_mode) != 0)
            {
              fprintf(stderr, "invalid -o pending_worker_prio: '%s'\n", prio_str);
              free(dup);
              return 2;
            }
            continue;
          }

          const char *nice_str = NULL;
          if (strncmp(tok, "pending_worker_nice=", 20) == 0)
            nice_str = tok + 20;
          else if (strncmp(tok, "dedup_worker_nice=", 18) == 0)
            nice_str = tok + 18;
          if (nice_str)
          {
            char *endp = NULL;
            long v = strtol(nice_str, &endp, 10);
            if (!endp || *endp != '\0' || v < 0 || v > 19)
            {
              fprintf(stderr, "invalid -o pending_worker_nice: '%s'\n", nice_str);
              free(dup);
              return 2;
            }
            pending_worker_nice = (int)v;
            continue;
          }

          const char *ttl_soft_str = NULL;
          if (strncmp(tok, "pending_ttl_soft_ms=", 20) == 0)
            ttl_soft_str = tok + 20;
          if (ttl_soft_str)
          {
            if (kafs_parse_u32_range(ttl_soft_str, 0, 3600000u, &pending_ttl_soft_ms) != 0)
            {
              fprintf(stderr, "invalid -o pending_ttl_soft_ms: '%s'\n", ttl_soft_str);
              free(dup);
              return 2;
            }
            continue;
          }

          const char *ttl_hard_str = NULL;
          if (strncmp(tok, "pending_ttl_hard_ms=", 20) == 0)
            ttl_hard_str = tok + 20;
          if (ttl_hard_str)
          {
            if (kafs_parse_u32_range(ttl_hard_str, 0, 3600000u, &pending_ttl_hard_ms) != 0)
            {
              fprintf(stderr, "invalid -o pending_ttl_hard_ms: '%s'\n", ttl_hard_str);
              free(dup);
              return 2;
            }
            continue;
          }

          const char *pcap_initial_str = NULL;
          if (strncmp(tok, "pendinglog_cap_initial=", 23) == 0)
            pcap_initial_str = tok + 23;
          else if (strncmp(tok, "pending_cap_initial=", 20) == 0)
            pcap_initial_str = tok + 20;
          if (pcap_initial_str)
          {
            if (kafs_parse_u32_range(pcap_initial_str, 0, 1000000000u, &pending_cap_initial) != 0)
            {
              fprintf(stderr, "invalid -o pendinglog_cap_initial: '%s'\n", pcap_initial_str);
              free(dup);
              return 2;
            }
            continue;
          }

          const char *pcap_min_str = NULL;
          if (strncmp(tok, "pendinglog_cap_min=", 19) == 0)
            pcap_min_str = tok + 19;
          else if (strncmp(tok, "pending_cap_min=", 16) == 0)
            pcap_min_str = tok + 16;
          if (pcap_min_str)
          {
            if (kafs_parse_u32_range(pcap_min_str, 0, 1000000000u, &pending_cap_min) != 0)
            {
              fprintf(stderr, "invalid -o pendinglog_cap_min: '%s'\n", pcap_min_str);
              free(dup);
              return 2;
            }
            continue;
          }

          const char *pcap_max_str = NULL;
          if (strncmp(tok, "pendinglog_cap_max=", 19) == 0)
            pcap_max_str = tok + 19;
          else if (strncmp(tok, "pending_cap_max=", 16) == 0)
            pcap_max_str = tok + 16;
          if (pcap_max_str)
          {
            if (kafs_parse_u32_range(pcap_max_str, 0, 1000000000u, &pending_cap_max) != 0)
            {
              fprintf(stderr, "invalid -o pendinglog_cap_max: '%s'\n", pcap_max_str);
              free(dup);
              return 2;
            }
            continue;
          }

          const char *fsp_str = NULL;
          if (strncmp(tok, "fsync_policy=", 13) == 0)
            fsp_str = tok + 13;
          if (fsp_str)
          {
            if (kafs_fsync_policy_parse(fsp_str, &fsync_policy) != 0)
            {
              fprintf(stderr, "invalid -o fsync_policy: '%s'\n", fsp_str);
              free(dup);
              return 2;
            }
            continue;
          }

          if (strcmp(tok, "bg_dedup_scan") == 0 || strcmp(tok, "bg_dedup_scan=on") == 0 ||
              strcmp(tok, "dedup_scan") == 0 || strcmp(tok, "dedup_scan=on") == 0)
          {
            bg_dedup_scan_enabled = 1u;
            continue;
          }
          if (strcmp(tok, "no_bg_dedup_scan") == 0 || strcmp(tok, "bg_dedup_scan=off") == 0 ||
              strcmp(tok, "no_dedup_scan") == 0 || strcmp(tok, "dedup_scan=off") == 0)
          {
            bg_dedup_scan_enabled = 0u;
            continue;
          }

          const char *bg_scan_str = NULL;
          if (strncmp(tok, "bg_dedup_scan=", 14) == 0)
            bg_scan_str = tok + 14;
          else if (strncmp(tok, "dedup_scan=", 11) == 0)
            bg_scan_str = tok + 11;
          if (bg_scan_str)
          {
            if (kafs_parse_onoff(bg_scan_str, &bg_dedup_scan_enabled) != 0)
            {
              fprintf(stderr, "invalid -o dedup_scan/bg_dedup_scan: '%s'\n", bg_scan_str);
              free(dup);
              return 2;
            }
            continue;
          }

          const char *bg_interval_str = NULL;
          if (strncmp(tok, "bg_dedup_interval_ms=", 21) == 0)
            bg_interval_str = tok + 21;
          else if (strncmp(tok, "dedup_interval_ms=", 18) == 0)
            bg_interval_str = tok + 18;
          if (bg_interval_str)
          {
            if (kafs_parse_u32_range(bg_interval_str, KAFS_BG_DEDUP_INTERVAL_MS_MIN,
                                     KAFS_BG_DEDUP_INTERVAL_MS_MAX, &bg_dedup_interval_ms) != 0)
            {
              fprintf(stderr, "invalid -o dedup_interval_ms/bg_dedup_interval_ms: '%s'\n",
                      bg_interval_str);
              free(dup);
              return 2;
            }
            continue;
          }

          // keep other options
          size_t tlen = strlen(tok);
          size_t need = tlen + (used ? 1 : 0);
          if (need)
          {
            if (used)
              filtered[used++] = ',';
            memcpy(filtered + used, tok, tlen);
            used += tlen;
            filtered[used] = '\0';
          }
        }

        free(dup);
        if (want_mt)
          enable_mt = KAFS_TRUE;

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
        continue;
      }

      argv_user[argc_user++] = argv_clean[i];
    }

    // Copy back filtered argv
    argc_clean = argc_user;
    for (int i = 0; i < argc_clean; ++i)
      argv_clean[i] = argv_user[i];

    // Note: o_owned is intentionally not freed here; argv_clean references it.
    (void)o_owned;
    (void)o_owned_cnt;
  }

  if (pending_ttl_soft_ms > 0 && pending_ttl_hard_ms > 0 &&
      pending_ttl_hard_ms < pending_ttl_soft_ms)
  {
    fprintf(stderr, "invalid pending TTL: hard must be >= soft\n");
    return 2;
  }

  if (pending_cap_min > 0 && pending_cap_max > 0 && pending_cap_max < pending_cap_min)
  {
    fprintf(stderr, "invalid pendinglog capacity: max must be >= min\n");
    return 2;
  }

  static kafs_context_t ctx;
  static char mnt_abs[PATH_MAX];
  char hotplug_uds_path[sizeof(((struct sockaddr_un *)0)->sun_path)];
  hotplug_uds_path[0] = '\0';
  // Store mountpoint as an absolute path for /proc fd resolution (FICLONE).
  if (argv_clean[1] && argv_clean[1][0] == '/')
  {
    snprintf(mnt_abs, sizeof(mnt_abs), "%s", argv_clean[1]);
    ctx.c_mountpoint = mnt_abs;
  }
  else
  {
    char cwd[PATH_MAX];
    if (getcwd(cwd, sizeof(cwd)) != NULL && argv_clean[1] && argv_clean[1][0] != '\0')
    {
      if ((size_t)snprintf(mnt_abs, sizeof(mnt_abs), "%s/%s", cwd, argv_clean[1]) < sizeof(mnt_abs))
        ctx.c_mountpoint = mnt_abs;
      else
        ctx.c_mountpoint = argv_clean[1];
    }
    else
    {
      ctx.c_mountpoint = argv_clean[1];
    }
  }

  ctx.c_hotplug_fd = -1;
  ctx.c_hotplug_active = 0;
  ctx.c_hotplug_lock_init = 0;
  ctx.c_hotplug_session_id = 0;
  ctx.c_hotplug_epoch = 0;
  ctx.c_hotplug_data_mode = KAFS_RPC_DATA_INLINE;
  ctx.c_hotplug_state = KAFS_HOTPLUG_STATE_DISABLED;
  ctx.c_hotplug_last_error = 0;
  ctx.c_hotplug_wait_queue_len = 0;
  ctx.c_hotplug_wait_queue_limit = KAFS_HOTPLUG_WAIT_QUEUE_LIMIT_DEFAULT;
  ctx.c_hotplug_wait_timeout_ms = KAFS_HOTPLUG_WAIT_TIMEOUT_MS_DEFAULT;
  ctx.c_hotplug_wait_lock_init = 0;
  ctx.c_hotplug_connecting = 0;
  ctx.c_hotplug_uds_path[0] = '\0';
  ctx.c_hotplug_front_major = KAFS_RPC_HELLO_MAJOR;
  ctx.c_hotplug_front_minor = KAFS_RPC_HELLO_MINOR;
  ctx.c_hotplug_front_features = KAFS_RPC_HELLO_FEATURES;
  ctx.c_hotplug_back_major = 0;
  ctx.c_hotplug_back_minor = 0;
  ctx.c_hotplug_back_features = 0;
  ctx.c_hotplug_compat_result = KAFS_HOTPLUG_COMPAT_UNKNOWN;
  ctx.c_hotplug_compat_reason = 0;
  ctx.c_pending_worker_prio_mode = pending_worker_prio_mode;
  ctx.c_pending_worker_nice = pending_worker_nice;
  ctx.c_pending_worker_prio_base_mode = pending_worker_prio_mode;
  ctx.c_pending_worker_nice_base = pending_worker_nice;
  ctx.c_pending_worker_auto_boosted = 0;
  ctx.c_pending_ttl_soft_ms = pending_ttl_soft_ms;
  ctx.c_pending_ttl_hard_ms = pending_ttl_hard_ms;
  ctx.c_pending_oldest_age_ms = 0;
  ctx.c_pending_ttl_over_soft = 0;
  ctx.c_pending_ttl_over_hard = 0;
  ctx.c_pending_worker_prio_apply_error = 0;
  ctx.c_pending_worker_prio_dirty = 1;
  ctx.c_pendinglog_capacity = pending_cap_initial;
  ctx.c_pendinglog_capacity_min = pending_cap_min;
  ctx.c_pendinglog_capacity_max = pending_cap_max;
  ctx.c_bg_dedup_enabled = bg_dedup_scan_enabled;
  ctx.c_bg_dedup_interval_ms = bg_dedup_interval_ms;
  ctx.c_fsync_policy = fsync_policy;

  const char *data_mode = getenv("KAFS_HOTPLUG_DATA_MODE");
  if (data_mode)
  {
    if (strcmp(data_mode, "inline") == 0)
      ctx.c_hotplug_data_mode = KAFS_RPC_DATA_INLINE;
    else if (strcmp(data_mode, "plan_only") == 0)
      ctx.c_hotplug_data_mode = KAFS_RPC_DATA_PLAN_ONLY;
    else if (strcmp(data_mode, "shm") == 0)
      ctx.c_hotplug_data_mode = KAFS_RPC_DATA_SHM;
  }

  const char *wait_timeout_env = getenv("KAFS_HOTPLUG_WAIT_TIMEOUT_MS");
  if (wait_timeout_env && *wait_timeout_env)
  {
    char *endp = NULL;
    unsigned long v = strtoul(wait_timeout_env, &endp, 10);
    if (endp && *endp == '\0')
      ctx.c_hotplug_wait_timeout_ms = (uint32_t)v;
  }

  const char *wait_limit_env = getenv("KAFS_HOTPLUG_WAIT_QUEUE_LIMIT");
  if (wait_limit_env && *wait_limit_env)
  {
    char *endp = NULL;
    unsigned long v = strtoul(wait_limit_env, &endp, 10);
    if (endp && *endp == '\0')
      ctx.c_hotplug_wait_queue_limit = (uint32_t)v;
  }

  const char *hotplug_uds =
      hotplug_uds_opt[0] != '\0' ? hotplug_uds_opt : getenv("KAFS_HOTPLUG_UDS");
  const char *hotplug_back_bin =
      hotplug_back_bin_opt[0] != '\0' ? hotplug_back_bin_opt : getenv("KAFS_HOTPLUG_BACK_BIN");
  if (hotplug_uds)
  {
    ctx.c_hotplug_state = KAFS_HOTPLUG_STATE_WAITING;
    snprintf(ctx.c_hotplug_uds_path, sizeof(ctx.c_hotplug_uds_path), "%s", hotplug_uds);
    (void)kafs_hotplug_env_set(&ctx, "KAFS_HOTPLUG_UDS", hotplug_uds);
    if (hotplug_back_bin && *hotplug_back_bin)
      (void)kafs_hotplug_env_set(&ctx, "KAFS_HOTPLUG_BACK_BIN", hotplug_back_bin);
    if (!ctx.c_hotplug_wait_lock_init)
    {
      if (pthread_mutex_init(&ctx.c_hotplug_wait_lock, NULL) == 0 &&
          pthread_cond_init(&ctx.c_hotplug_wait_cond, NULL) == 0)
        ctx.c_hotplug_wait_lock_init = 1;
    }
    if (snprintf(hotplug_uds_path, sizeof(hotplug_uds_path), "%s", hotplug_uds) <
        (int)sizeof(hotplug_uds_path))
    {
      int rc_hp = kafs_hotplug_wait_for_back(&ctx, hotplug_uds_path, -1);
      if (rc_hp != 0)
      {
        ctx.c_hotplug_state = KAFS_HOTPLUG_STATE_ERROR;
        ctx.c_hotplug_last_error = rc_hp;
        fprintf(stderr, "hotplug: failed to accept back rc=%d\n", rc_hp);
        return 2;
      }
    }
    else
    {
      ctx.c_hotplug_state = KAFS_HOTPLUG_STATE_ERROR;
      ctx.c_hotplug_last_error = -ENAMETOOLONG;
      fprintf(stderr, "hotplug: uds path too long\n");
      return 2;
    }
  }

  ctx.c_fd = open(image_path, O_RDWR, 0666);
  if (ctx.c_fd < 0)
  {
    perror("open image");
    fprintf(stderr, "image not found. run mkfs.kafs first.\n");
    exit(2);
  }
  ctx.c_blo_search = 0;
  ctx.c_ino_search = 0;

  // まずスーパーブロックだけ読み出してレイアウトを決定
  kafs_ssuperblock_t sbdisk;
  ssize_t r = pread(ctx.c_fd, &sbdisk, sizeof(sbdisk), 0);
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
  if (kafs_sb_format_version_get(&sbdisk) != KAFS_FORMAT_VERSION)
  {
    uint32_t fmt_ver = kafs_sb_format_version_get(&sbdisk);
    if (fmt_ver == KAFS_FORMAT_VERSION_V2)
    {
      if (auto_migrate_v2)
      {
        int mrc = kafs_migrate_v2_image(image_path, migrate_yes ? 1 : 0);
        if (mrc == 0)
        {
          fprintf(stderr, "migration completed: v2 -> v3. please restart mount.\n");
          exit(0);
        }
        if (mrc == 1)
        {
          fprintf(stderr, "image already v3; continue normal mount without --migrate-v2.\n");
          exit(0);
        }
        if (mrc == -ECANCELED)
          fprintf(stderr, "migration canceled by user.\n");
        else
          fprintf(stderr, "migration failed rc=%d.\n", mrc);
        exit(2);
      }
      fprintf(stderr,
              "unsupported format version: v2.\n"
              "migration guidance:\n"
              "  1) offline migrate command (recommended):\n"
              "     kafsctl migrate %s [--yes]\n"
              "  2) one-shot startup migration:\n"
              "     kafs --image %s --migrate-v2 [--yes] <mountpoint> [FUSE options]\n",
              image_path, image_path);
    }
    else
      fprintf(stderr, "unsupported format version: %u (expected %u).\n", fmt_ver,
              (unsigned)KAFS_FORMAT_VERSION);
    exit(2);
  }
  // 読み出し値からレイアウト計算
  kafs_logblksize_t log_blksize = kafs_sb_log_blksize_get(&sbdisk);
  kafs_blksize_t blksize = 1u << log_blksize;
  kafs_blksize_t blksizemask = blksize - 1u;
  kafs_inocnt_t inocnt = kafs_inocnt_stoh(sbdisk.s_inocnt);
  kafs_blkcnt_t r_blkcnt = kafs_blkcnt_stoh(sbdisk.s_r_blkcnt);

  off_t mapsize = 0;
  mapsize += sizeof(kafs_ssuperblock_t);
  mapsize = (mapsize + blksizemask) & ~blksizemask;
  void *blkmask_off = (void *)mapsize;
  assert(sizeof(kafs_blkmask_t) <= 8);
  mapsize += (r_blkcnt + 7) >> 3;
  mapsize = (mapsize + 7) & ~7;
  mapsize = (mapsize + blksizemask) & ~blksizemask;
  void *inotbl_off = (void *)mapsize;
  mapsize += sizeof(kafs_sinode_t) * inocnt;
  mapsize = (mapsize + blksizemask) & ~blksizemask;
  // Full-image mapping size: use r_blkcnt * blksize (and ensure it also covers HRL/journal)
  off_t imgsize = (off_t)r_blkcnt << log_blksize;
  {
    uint64_t idx_off = kafs_sb_hrl_index_offset_get(&sbdisk);
    uint64_t idx_size = kafs_sb_hrl_index_size_get(&sbdisk);
    uint64_t ent_off = kafs_sb_hrl_entry_offset_get(&sbdisk);
    uint64_t ent_cnt = kafs_sb_hrl_entry_cnt_get(&sbdisk);
    uint64_t ent_size = ent_cnt * (uint64_t)sizeof(kafs_hrl_entry_t);
    uint64_t j_off = kafs_sb_journal_offset_get(&sbdisk);
    uint64_t j_size = kafs_sb_journal_size_get(&sbdisk);
    uint64_t p_off = kafs_sb_pendinglog_offset_get(&sbdisk);
    uint64_t p_size = kafs_sb_pendinglog_size_get(&sbdisk);
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

  ctx.c_img_base = mmap(NULL, imgsize, PROT_READ | PROT_WRITE, MAP_SHARED, ctx.c_fd, 0);
  if (ctx.c_img_base == MAP_FAILED)
  {
    perror("mmap");
    exit(2);
  }
  ctx.c_img_size = (size_t)imgsize;

  // Keep existing pointers for metadata access
  ctx.c_superblock = (kafs_ssuperblock_t *)ctx.c_img_base;
  ctx.c_mapsize = (size_t)mapsize; // metadata region length (subset of img)
  ctx.c_blkmasktbl = (void *)ctx.c_superblock + (intptr_t)blkmask_off;
  ctx.c_inotbl = (void *)ctx.c_superblock + (intptr_t)inotbl_off;
  ctx.c_ino_epoch = calloc((size_t)inocnt, sizeof(uint32_t));
  if (ctx.c_ino_epoch)
  {
    for (kafs_inocnt_t i = 0; i < inocnt; ++i)
      ctx.c_ino_epoch[i] = 1u;
  }
  ctx.c_alloc_v3_summary_dirty = 1;

  // HRL オープン
  (void)kafs_hrl_open(&ctx);

  // Journal 初期化（KAFS_JOURNAL=0/1/パス）
  (void)kafs_journal_init(&ctx, image_path);
  ctx.c_meta_delta_enabled = (uint32_t)kafs_journal_is_enabled(&ctx);
  if (ctx.c_meta_delta_enabled)
  {
    size_t bits = sizeof(kafs_blkmask_t) * 8u;
    size_t words = ((size_t)r_blkcnt + bits - 1u) / bits;
    ctx.c_meta_bitmap_words = calloc(words, sizeof(kafs_blkmask_t));
    ctx.c_meta_bitmap_dirty = calloc(words, sizeof(uint8_t));
    if (!ctx.c_meta_bitmap_words || !ctx.c_meta_bitmap_dirty)
    {
      free(ctx.c_meta_bitmap_words);
      free(ctx.c_meta_bitmap_dirty);
      ctx.c_meta_bitmap_words = NULL;
      ctx.c_meta_bitmap_dirty = NULL;
      ctx.c_meta_bitmap_wordcnt = 0;
      ctx.c_meta_bitmap_dirty_count = 0;
      ctx.c_meta_bitmap_words_enabled = 0;
      ctx.c_meta_delta_enabled = 0;
    }
    else
    {
      memcpy(ctx.c_meta_bitmap_words, ctx.c_blkmasktbl, words * sizeof(kafs_blkmask_t));
      ctx.c_meta_bitmap_wordcnt = words;
      ctx.c_meta_bitmap_dirty_count = 0;
      ctx.c_meta_bitmap_words_enabled = 1u;
    }
  }

  // 起動時リプレイ（in-image のみ対象）。致命的ではなくベストエフォート。
  (void)kafs_journal_replay(&ctx, NULL, NULL);
  (void)kafs_pendinglog_init_or_load(&ctx);
  if (ctx.c_pendinglog_enabled)
  {
    (void)kafs_pendinglog_replay_mount(&ctx);
    (void)kafs_pending_worker_start(&ctx);
    kafs_journal_note(&ctx, "PENDINGLOG", "loaded entries=%u cap=%u", kafs_pendinglog_count(&ctx),
                      ctx.c_pendinglog_capacity);
  }

  // 画像ファイルに排他ロック（複数プロセスによる同時RW起動を防止）
  struct flock lk = {0};
  lk.l_type = F_WRLCK;
  lk.l_whence = SEEK_SET;
  lk.l_start = 0;
  lk.l_len = 0; // whole file
  if (fcntl(ctx.c_fd, F_SETLK, &lk) == -1)
  {
    perror("fcntl(F_SETLK)");
    fprintf(stderr, "image '%s' is busy (already mounted?).\n", image_path);
    exit(2);
  }

  int saw_single = 0;
  for (int i = 0; i < argc_clean; ++i)
  {
    if (strcmp(argv_clean[i], "-s") == 0)
    {
      saw_single = 1;
      break;
    }
  }
  // 余裕を持って追加オプション(-s, -o max_threads=)用のスロットを確保
  char *argv_fuse[argc_clean + 10];
  for (int i = 0; i < argc_clean; ++i)
    argv_fuse[i] = argv_clean[i];
  int argc_fuse = argc_clean;
  // default single-threaded: always add -s unless MT explicitly enabled.
  if (!enable_mt && !saw_single)
  {
    argv_fuse[argc_fuse++] = "-s";
  }
  if (enable_mt && saw_single)
  {
    // -s takes precedence for predictable CLI behavior.
    enable_mt = KAFS_FALSE;
  }
  if (kafs_debug_level() >= 3)
  {
    argv_fuse[argc_fuse++] = "-d"; // libfuse debug
  }
  // MT有効時は max_threads を明示（libfuseの既定/奇妙な値を避ける）
  char mt_opt_buf[64];
  if (enable_mt && !saw_max_threads)
  {
    unsigned mt_cnt = 8; // Raspi向けデフォルト
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
    // 安全のため -o と値を分けて渡す（-omax_threads= 形式のパース差異を回避）
    snprintf(mt_opt_buf, sizeof(mt_opt_buf), "max_threads=%u", mt_cnt);
    argv_fuse[argc_fuse++] = "-o";
    argv_fuse[argc_fuse++] = mt_opt_buf;
    kafs_log(KAFS_LOG_INFO, "kafs: enabling multithread with -o %s\n", mt_opt_buf);
  }
  g_kafs_writeback_cache_enabled = writeback_cache_enabled ? 1 : 0;
  ctx.c_trim_on_free = trim_on_free_enabled ? 1u : 0u;
  if (writeback_cache_explicit)
  {
    kafs_log(KAFS_LOG_INFO, "kafs: writeback_cache %s (explicit)\n",
             writeback_cache_enabled ? "enabled" : "disabled");
  }
  else
  {
    kafs_log(KAFS_LOG_INFO, "kafs: writeback_cache %s (default)\n",
             writeback_cache_enabled ? "enabled" : "disabled");
  }
  if (trim_on_free_explicit)
  {
    kafs_log(KAFS_LOG_INFO, "kafs: trim_on_free %s (explicit)\n",
             trim_on_free_enabled ? "enabled" : "disabled");
  }
  else
  {
    kafs_log(KAFS_LOG_INFO, "kafs: trim_on_free %s (default)\n",
             trim_on_free_enabled ? "enabled" : "disabled");
  }
  // 起動引数を一度だけ情報ログに出力（デバッグ用途）
  if (kafs_debug_level() >= 1)
  {
    kafs_log(KAFS_LOG_INFO, "kafs: fuse argv (%d):\n", argc_fuse);
    for (int i = 0; i < argc_fuse; ++i)
    {
      kafs_log(KAFS_LOG_INFO, "  argv[%d]=%s\n", i, argv_fuse[i]);
    }
  }
  argv_fuse[argc_fuse] = NULL; // ensure NULL-terminated argv for libfuse safety
  fuse_set_log_func(kafs_fuse_log_func);
  int rc = fuse_main(argc_fuse, argv_fuse, &kafs_operations, &ctx);
  fuse_set_log_func(NULL);
  kafs_pending_worker_stop(&ctx);
  kafs_journal_shutdown(&ctx);
  if (ctx.c_hotplug_fd >= 0)
    close(ctx.c_hotplug_fd);
  ctx.c_hotplug_active = 0;
  if (hotplug_uds_path[0] != '\0')
    unlink(hotplug_uds_path);
  if (ctx.c_hotplug_lock_init)
    pthread_mutex_destroy(&ctx.c_hotplug_lock);
  if (ctx.c_hotplug_wait_lock_init)
  {
    pthread_cond_destroy(&ctx.c_hotplug_wait_cond);
    pthread_mutex_destroy(&ctx.c_hotplug_wait_lock);
  }
  free(ctx.c_meta_bitmap_words);
  free(ctx.c_meta_bitmap_dirty);
  free(ctx.c_ino_epoch);
  if (ctx.c_img_base && ctx.c_img_base != MAP_FAILED)
    munmap(ctx.c_img_base, ctx.c_img_size);
  return rc;
}
#endif
