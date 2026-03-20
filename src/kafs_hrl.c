#include "kafs_hash.h"
#include "kafs_locks.h"
#include "kafs_block.h"
#include <inttypes.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <time.h>

#if KAFS_ENABLE_EXTRA_DIAG
extern const char *kafs_diag_current_write_path(void) __attribute__((weak));
extern uint32_t kafs_diag_current_write_ino(void) __attribute__((weak));
#endif

// In-memory helpers derived from superblock HRL fields (entry type is declared in kafs_hash.h)

static inline uint32_t *hrl_index_tbl(kafs_context_t *ctx) { return (uint32_t *)ctx->c_hrl_index; }

static inline kafs_hrl_entry_t *hrl_entries_tbl(kafs_context_t *ctx)
{
  uintptr_t base = (uintptr_t)ctx->c_superblock;
  uintptr_t off = (uintptr_t)kafs_sb_hrl_entry_offset_get(ctx->c_superblock);
  return (kafs_hrl_entry_t *)(base + off);
}

static inline uint32_t hrl_capacity(kafs_context_t *ctx)
{
  return (uint32_t)kafs_sb_hrl_entry_cnt_get(ctx->c_superblock);
}

static inline uint32_t hrl_bucket_count(kafs_context_t *ctx) { return ctx->c_hrl_bucket_cnt; }

static inline kafs_blksize_t hrl_blksize(kafs_context_t *ctx)
{
  return kafs_sb_blksize_get(ctx->c_superblock);
}

static inline uint64_t hrl_now_ns(void)
{
  struct timespec ts;
  clock_gettime(CLOCK_MONOTONIC, &ts);
  return (uint64_t)ts.tv_sec * 1000000000ull + (uint64_t)ts.tv_nsec;
}

static inline kafs_logblksize_t hrl_log_blksize(kafs_context_t *ctx)
{
  return kafs_sb_log_blksize_get(ctx->c_superblock);
}

static inline int hrl_slot_is_reusable(const kafs_hrl_entry_t *e)
{
  return e->refcnt == 0 && e->blo == KAFS_BLO_NONE;
}

static inline void hrl_slot_reset(kafs_hrl_entry_t *e) { memset(e, 0, sizeof(*e)); }

static inline void hrl_free_list_push_raw(kafs_context_t *ctx, uint32_t idx)
{
  kafs_hrl_entry_t *ents = hrl_entries_tbl(ctx);

  ents[idx].next_plus1 = ctx->c_hrl_free_head_plus1;
  ctx->c_hrl_free_head_plus1 = idx + 1u;
  __atomic_add_fetch(&ctx->c_hrl_free_slot_count, 1u, __ATOMIC_RELAXED);
}

static int hrl_free_list_pop_locked(kafs_context_t *ctx, uint32_t *out_index)
{
  uint32_t head = ctx->c_hrl_free_head_plus1;
  kafs_hrl_entry_t *ents = hrl_entries_tbl(ctx);

  if (head == 0)
    return -ENOSPC;

  uint32_t idx = head - 1u;
  if (idx >= hrl_capacity(ctx))
    return -EIO;

  kafs_hrl_entry_t *e = &ents[idx];
  if (!hrl_slot_is_reusable(e))
    return -EIO;

  ctx->c_hrl_free_head_plus1 = e->next_plus1;
  e->next_plus1 = 0;
  e->refcnt = 1u;
  __atomic_sub_fetch(&ctx->c_hrl_free_slot_count, 1u, __ATOMIC_RELAXED);
  *out_index = idx;
  return 0;
}

static int hrl_reserve_free_slot(kafs_context_t *ctx, uint32_t *out_index)
{
  int rc;

  if (__atomic_load_n(&ctx->c_hrl_free_slot_count, __ATOMIC_RELAXED) == 0)
    return -ENOSPC;

  kafs_hrl_global_lock(ctx);
  rc = hrl_free_list_pop_locked(ctx, out_index);
  kafs_hrl_global_unlock(ctx);
  return rc;
}

static void hrl_release_reserved_slot(kafs_context_t *ctx, uint32_t idx)
{
  kafs_hrl_entry_t *ents = hrl_entries_tbl(ctx);

  kafs_hrl_global_lock(ctx);
  hrl_slot_reset(&ents[idx]);
  hrl_free_list_push_raw(ctx, idx);
  kafs_hrl_global_unlock(ctx);
}

static void hrl_publish_free_slot(kafs_context_t *ctx, uint32_t idx)
{
  kafs_hrl_global_lock(ctx);
  hrl_free_list_push_raw(ctx, idx);
  kafs_hrl_global_unlock(ctx);
}

static int hrl_read_blo(kafs_context_t *ctx, kafs_blkcnt_t blo, void *out)
{
  kafs_blksize_t bs = hrl_blksize(ctx);
  kafs_logblksize_t l2 = hrl_log_blksize(ctx);
  ssize_t r = pread(ctx->c_fd, out, bs, (off_t)blo << l2);
  return (r == (ssize_t)bs) ? 0 : -EIO;
}

static int hrl_write_blo(kafs_context_t *ctx, kafs_blkcnt_t blo, const void *buf)
{
  kafs_blksize_t bs = hrl_blksize(ctx);
  kafs_logblksize_t l2 = hrl_log_blksize(ctx);
#if KAFS_ENABLE_EXTRA_DIAG
  if (ctx && buf && blo != KAFS_BLO_NONE && ctx->c_diag_log_fd >= 0)
  {
    kafs_inocnt_t inocnt = kafs_sb_inocnt_get(ctx->c_superblock);
    for (kafs_inocnt_t ino = KAFS_INO_ROOTDIR; ino < inocnt; ++ino)
    {
      kafs_sinode_t *inoent = &ctx->c_inotbl[ino];
      if (!kafs_ino_get_usage(inoent))
        continue;
      kafs_mode_t mode = kafs_ino_mode_get(inoent);
      if (!S_ISDIR(mode))
        continue;
      if (kafs_ino_size_get(inoent) <= sizeof(inoent->i_blkreftbl))
        continue;
      kafs_blkcnt_t cur_ref = kafs_blkcnt_stoh(inoent->i_blkreftbl[0]);
      if (cur_ref != blo)
        continue;

      const unsigned char *p = (const unsigned char *)buf;
      char hex[3 * 16 + 1];
      char ascii[16 + 1];
      size_t hex_off = 0;
      size_t ascii_off = 0;
      hex[0] = '\0';
      ascii[0] = '\0';
      for (size_t i = 0; i < 16u && i < (size_t)bs; ++i)
      {
        int whex = snprintf(hex + hex_off, sizeof(hex) - hex_off, "%s%02x", (i == 0) ? "" : " ",
                            (unsigned)p[i]);
        if (whex < 0 || (size_t)whex >= sizeof(hex) - hex_off)
          break;
        hex_off += (size_t)whex;
        ascii[ascii_off++] = (p[i] >= 32 && p[i] <= 126) ? (char)p[i] : '.';
      }
      ascii[ascii_off] = '\0';

      char line[1024];
      int n = snprintf(line, sizeof(line),
                       "hrl_write_blo: blk=%" PRIuFAST32 " matches live dir block0 ino=%" PRIuFAST32
                       " mode=%o size=%" PRIuFAST64 " src_ino=%" PRIuFAST32
                       " src_path=%s sample_hex=%s sample_ascii='%s'\n",
                       (uint_fast32_t)blo, (uint_fast32_t)ino, (unsigned)mode,
                       (uint_fast64_t)kafs_ino_size_get(inoent),
                       (uint_fast32_t)(kafs_diag_current_write_ino ? kafs_diag_current_write_ino()
                                                                   : KAFS_INO_NONE),
                       (kafs_diag_current_write_path && kafs_diag_current_write_path())
                           ? kafs_diag_current_write_path()
                           : "(null)",
                       hex[0] ? hex : "-", ascii[0] ? ascii : "-");
      if (n > 0)
      {
        size_t len = (size_t)n < sizeof(line) ? (size_t)n : sizeof(line) - 1u;
        size_t off = 0;
        while (off < len)
        {
          ssize_t wr = write(ctx->c_diag_log_fd, line + off, len - off);
          if (wr <= 0)
            break;
          off += (size_t)wr;
        }
      }
    }
  }
#endif
  ssize_t w = pwrite(ctx->c_fd, buf, bs, (off_t)blo << l2);
  return (w == (ssize_t)bs) ? 0 : -EIO;
}

static int hrl_release_blo(kafs_context_t *ctx, kafs_blkcnt_t *pblo)
{
  if (!pblo || *pblo == KAFS_BLO_NONE)
    return 0;
  kafs_blksize_t bs = hrl_blksize(ctx);
  char z[bs];
  memset(z, 0, bs);
  (void)hrl_write_blo(ctx, *pblo, z);
  (void)kafs_blk_set_usage(ctx, *pblo, KAFS_FALSE);
  *pblo = KAFS_BLO_NONE;
  return 0;
}

// FNV-1a 64
static uint64_t hrl_hash64(const void *buf, size_t len)
{
  const unsigned char *p = (const unsigned char *)buf;
  uint64_t h = 1469598103934665603ull; // offset basis
  const uint64_t prime = 1099511628211ull;
  for (size_t i = 0; i < len; ++i)
  {
    h ^= p[i];
    h *= prime;
  }
  return h;
}

static int hrl_bucket_index(kafs_context_t *ctx, uint64_t fast)
{
  uint32_t mask = hrl_bucket_count(ctx) - 1u;
  return (int)(fast & mask);
}

static int hrl_entry_cmp_blo(kafs_context_t *ctx, kafs_hrl_entry_t *e, kafs_blkcnt_t blo,
                             const void *buf)
{
  if (e->blo != blo)
    return 0;
  // confirm equality by reading and comparing when buffer provided
  if (!buf)
    return 1;
  kafs_blksize_t bs = hrl_blksize(ctx);
  char tmp[bs];
  if (hrl_read_blo(ctx, blo, tmp) != 0)
    return 0;
  return memcmp(tmp, buf, bs) == 0;
}

static int hrl_entry_cmp_content(kafs_context_t *ctx, kafs_hrl_entry_t *e, const void *buf,
                                 uint64_t fast)
{
  if (e->fast != fast)
    return 0;
  kafs_blksize_t bs = hrl_blksize(ctx);
  char tmp[bs];
  if (hrl_read_blo(ctx, e->blo, tmp) != 0)
    return 0;
  return memcmp(tmp, buf, bs) == 0;
}

typedef int (*hrl_entry_match_fn)(const kafs_hrl_entry_t *e, void *opaque);

static int hrl_match_live_blo(const kafs_hrl_entry_t *e, void *opaque)
{
  const kafs_blkcnt_t *blo = (const kafs_blkcnt_t *)opaque;
  return e->refcnt != 0 && e->blo == *blo;
}

static int hrl_match_evictable_ref1(const kafs_hrl_entry_t *e, void *opaque)
{
  (void)opaque;
  return e->refcnt == 1u && e->blo != KAFS_BLO_NONE;
}

// Returns with the matching bucket still locked; caller must unlock it.
static int hrl_find_entry_locked(kafs_context_t *ctx, hrl_entry_match_fn match, void *opaque,
                                 uint32_t *out_bucket, uint32_t *out_idx)
{
  uint32_t *index = hrl_index_tbl(ctx);
  kafs_hrl_entry_t *ents = hrl_entries_tbl(ctx);
  uint32_t cap = hrl_capacity(ctx);
  uint32_t bcnt = hrl_bucket_count(ctx);

  for (uint32_t b = 0; b < bcnt; ++b)
  {
    kafs_hrl_bucket_lock(ctx, b);
    uint32_t head = index[b];
    for (uint32_t steps = 0; head != 0 && steps < cap; ++steps)
    {
      uint32_t i = head - 1u;
      if (i >= cap)
      {
        kafs_hrl_bucket_unlock(ctx, b);
        return -EIO;
      }
      kafs_hrl_entry_t *e = &ents[i];
      if (match(e, opaque))
      {
        if (out_bucket)
          *out_bucket = b;
        if (out_idx)
          *out_idx = i;
        return 0;
      }
      head = e->next_plus1;
    }
    kafs_hrl_bucket_unlock(ctx, b);
    if (head != 0)
      return -EIO;
  }

  return -ENOENT;
}

static int hrl_find_by_hash(kafs_context_t *ctx, uint64_t fast, const void *buf,
                            uint32_t *out_index)
{
  uint32_t *index = hrl_index_tbl(ctx);
  kafs_hrl_entry_t *ents = hrl_entries_tbl(ctx);
  int b = hrl_bucket_index(ctx, fast);
  uint32_t head = index[b];
  uint32_t cap = hrl_capacity(ctx);
  // Guard against corrupted/looping chains: cap iterations.
  for (uint32_t steps = 0; head != 0 && steps < cap; ++steps)
  {
    __atomic_add_fetch(&ctx->c_stat_hrl_put_chain_steps, 1u, __ATOMIC_RELAXED);
    uint32_t i = head - 1u;
    if (i >= cap)
      return -EIO;
    kafs_hrl_entry_t *e = &ents[i];
    if (e->refcnt != 0)
    {
      __atomic_add_fetch(&ctx->c_stat_hrl_put_cmp_calls, 1u, __ATOMIC_RELAXED);
      uint64_t t_cmp0 = hrl_now_ns();
      int match = hrl_entry_cmp_content(ctx, e, buf, fast);
      uint64_t t_cmp1 = hrl_now_ns();
      __atomic_add_fetch(&ctx->c_stat_hrl_put_ns_cmp_content, t_cmp1 - t_cmp0, __ATOMIC_RELAXED);
      if (match)
      {
        *out_index = i;
        return 0;
      }
    }
    head = e->next_plus1;
  }
  return (head == 0) ? -ENOENT : -EIO;
}

static void hrl_chain_insert_head(kafs_context_t *ctx, uint32_t idx, uint64_t fast)
{
  uint32_t *index = hrl_index_tbl(ctx);
  int b = hrl_bucket_index(ctx, fast);
  kafs_hrl_entry_t *ents = hrl_entries_tbl(ctx);
  ents[idx].next_plus1 = index[b];
  index[b] = idx + 1u;
}

static int hrl_chain_remove(kafs_context_t *ctx, uint32_t idx, uint64_t fast)
{
  uint32_t *index = hrl_index_tbl(ctx);
  int b = hrl_bucket_index(ctx, fast);
  kafs_hrl_entry_t *ents = hrl_entries_tbl(ctx);
  uint32_t head = index[b];
  uint32_t prev = 0;
  uint32_t cap = hrl_capacity(ctx);
  // Guard against corrupted/looping chains: cap iterations.
  for (uint32_t steps = 0; head != 0 && steps < cap; ++steps)
  {
    uint32_t i = head - 1u;
    if (i >= cap)
      return -EIO;
    if (i == idx)
    {
      uint32_t next = ents[i].next_plus1;
      if (prev == 0)
        index[b] = next;
      else
        ents[prev - 1u].next_plus1 = next;
      return 0;
    }
    prev = head;
    head = ents[i].next_plus1;
  }
  return (head == 0) ? -ENOENT : -EIO;
}

int kafs_hrl_open(kafs_context_t *ctx)
{
  if (!ctx || !ctx->c_superblock)
    return -EINVAL;
  uintptr_t base = (uintptr_t)ctx->c_superblock;
  uint64_t index_off = kafs_sb_hrl_index_offset_get(ctx->c_superblock);
  uint64_t index_size = kafs_sb_hrl_index_size_get(ctx->c_superblock);
  if (index_off == 0 || index_size == 0)
  {
    ctx->c_hrl_index = NULL;
    ctx->c_hrl_bucket_cnt = 0;
    ctx->c_hrl_free_head_plus1 = 0;
    ctx->c_hrl_free_slot_count = 0;
    return 0;
  }
  ctx->c_hrl_index = (void *)(base + index_off);
  ctx->c_hrl_bucket_cnt = (uint32_t)(index_size / sizeof(uint32_t));
  ctx->c_hrl_free_head_plus1 = 0;
  ctx->c_hrl_free_slot_count = 0;
  kafs_hrl_entry_t *ents = hrl_entries_tbl(ctx);
  uint32_t cap = hrl_capacity(ctx);
  for (uint32_t i = cap; i > 0; --i)
  {
    uint32_t idx = i - 1u;
    if (hrl_slot_is_reusable(&ents[idx]))
      hrl_free_list_push_raw(ctx, idx);
  }
  (void)kafs_ctx_locks_init(ctx);
  return 0;
}

int kafs_hrl_close(kafs_context_t *ctx)
{
  if (ctx)
    kafs_ctx_locks_destroy(ctx);
  return 0;
}

int kafs_hrl_format(kafs_context_t *ctx)
{
  // Zero index and entries regions based on sb
  uintptr_t base = (uintptr_t)ctx->c_superblock;
  uint64_t index_off = kafs_sb_hrl_index_offset_get(ctx->c_superblock);
  uint64_t index_size = kafs_sb_hrl_index_size_get(ctx->c_superblock);
  uint64_t entry_off = kafs_sb_hrl_entry_offset_get(ctx->c_superblock);
  uint32_t entry_cnt = kafs_sb_hrl_entry_cnt_get(ctx->c_superblock);
  if (index_off && index_size)
    memset((void *)(base + index_off), 0, (size_t)index_size);
  if (entry_off && entry_cnt)
    memset((void *)(base + entry_off), 0, (size_t)entry_cnt * sizeof(kafs_hrl_entry_t));
  if (ctx)
  {
    ctx->c_hrl_free_head_plus1 = 0;
    ctx->c_hrl_free_slot_count = 0;
    for (uint32_t i = entry_cnt; i > 0; --i)
      hrl_free_list_push_raw(ctx, i - 1u);
  }
  return 0;
}

int kafs_hrl_put(kafs_context_t *ctx, const void *block_data, kafs_hrid_t *out_hrid,
                 int *out_is_new, kafs_blkcnt_t *out_blo)
{
  if (!ctx || !block_data || !out_hrid || !out_is_new || !out_blo)
    return -EINVAL;
  if (ctx->c_hrl_bucket_cnt == 0 || hrl_capacity(ctx) == 0)
    return -ENOSYS;

  uint64_t t_hash0 = hrl_now_ns();
  uint64_t fast = hrl_hash64(block_data, hrl_blksize(ctx));
  uint64_t t_hash1 = hrl_now_ns();
  __atomic_add_fetch(&ctx->c_stat_hrl_put_ns_hash, t_hash1 - t_hash0, __ATOMIC_RELAXED);
  uint32_t idx;
  int b = hrl_bucket_index(ctx, fast);

  kafs_hrl_bucket_lock(ctx, (uint32_t)b);
  uint64_t t_find0 = hrl_now_ns();
  int find_rc = hrl_find_by_hash(ctx, fast, block_data, &idx);
  uint64_t t_find1 = hrl_now_ns();
  __atomic_add_fetch(&ctx->c_stat_hrl_put_ns_find, t_find1 - t_find0, __ATOMIC_RELAXED);
  if (find_rc == 0)
  {
    kafs_hrl_entry_t *e = hrl_entries_tbl(ctx) + idx;
    if (e->refcnt == 0xFFFFFFFFu)
    {
      kafs_hrl_bucket_unlock(ctx, (uint32_t)b);
      return -EOVERFLOW;
    }
    e->refcnt += 1u;
    *out_hrid = idx;
    *out_is_new = 0;
    *out_blo = e->blo;
    kafs_hrl_bucket_unlock(ctx, (uint32_t)b);
    return 0;
  }
  if (find_rc != -ENOENT)
  {
    kafs_hrl_bucket_unlock(ctx, (uint32_t)b);
    return find_rc;
  }

  kafs_hrl_bucket_unlock(ctx, (uint32_t)b);

  // Reserve a free slot outside the bucket lock to preserve lock ordering.
  uint64_t t_slot0 = hrl_now_ns();
  uint32_t reserved_idx = 0;
  int slot_rc = hrl_reserve_free_slot(ctx, &reserved_idx);
  uint64_t t_slot1 = hrl_now_ns();
  __atomic_add_fetch(&ctx->c_stat_hrl_put_ns_slot_alloc, t_slot1 - t_slot0, __ATOMIC_RELAXED);
  if (slot_rc != 0)
    return slot_rc;

  kafs_hrl_bucket_lock(ctx, (uint32_t)b);
  uint32_t found_idx = 0;
  t_find0 = hrl_now_ns();
  find_rc = hrl_find_by_hash(ctx, fast, block_data, &found_idx);
  t_find1 = hrl_now_ns();
  __atomic_add_fetch(&ctx->c_stat_hrl_put_ns_find, t_find1 - t_find0, __ATOMIC_RELAXED);
  if (find_rc == 0)
  {
    kafs_hrl_entry_t *existing = hrl_entries_tbl(ctx) + found_idx;
    if (existing->refcnt == 0xFFFFFFFFu)
    {
      kafs_hrl_bucket_unlock(ctx, (uint32_t)b);
      hrl_release_reserved_slot(ctx, reserved_idx);
      return -EOVERFLOW;
    }
    existing->refcnt += 1u;
    *out_hrid = found_idx;
    *out_is_new = 0;
    *out_blo = existing->blo;
    kafs_hrl_bucket_unlock(ctx, (uint32_t)b);
    hrl_release_reserved_slot(ctx, reserved_idx);
    return 0;
  }
  if (find_rc != -ENOENT)
  {
    kafs_hrl_bucket_unlock(ctx, (uint32_t)b);
    hrl_release_reserved_slot(ctx, reserved_idx);
    return find_rc;
  }

  idx = reserved_idx;
  kafs_hrl_entry_t *e = hrl_entries_tbl(ctx) + idx;
  e->next_plus1 = 0;

  // allocate physical block and write
  kafs_blkcnt_t blo = KAFS_BLO_NONE;
  uint64_t t_blk_alloc0 = hrl_now_ns();
  int rc = kafs_blk_alloc(ctx, &blo);
  uint64_t t_blk_alloc1 = hrl_now_ns();
  __atomic_add_fetch(&ctx->c_stat_hrl_put_ns_blk_alloc, t_blk_alloc1 - t_blk_alloc0,
                     __ATOMIC_RELAXED);
  if (rc != 0)
  {
    kafs_hrl_bucket_unlock(ctx, (uint32_t)b);
    hrl_release_reserved_slot(ctx, idx);
    return rc;
  }
  uint64_t t_blk_write0 = hrl_now_ns();
  rc = hrl_write_blo(ctx, blo, block_data);
  uint64_t t_blk_write1 = hrl_now_ns();
  __atomic_add_fetch(&ctx->c_stat_hrl_put_ns_blk_write, t_blk_write1 - t_blk_write0,
                     __ATOMIC_RELAXED);
  if (rc != 0)
  {
    (void)hrl_release_blo(ctx, &blo);
    kafs_hrl_bucket_unlock(ctx, (uint32_t)b);
    hrl_release_reserved_slot(ctx, idx);
    return rc;
  }

  e->blo = blo;
  e->fast = fast;
  e->next_plus1 = 0;
  hrl_chain_insert_head(ctx, idx, fast);
  kafs_hrl_bucket_unlock(ctx, (uint32_t)b);

  *out_hrid = idx;
  *out_is_new = 1;
  *out_blo = blo;
  return 0;
}

int kafs_hrl_inc_ref(kafs_context_t *ctx, kafs_hrid_t hrid)
{
  kafs_hrl_entry_t *e = hrl_entries_tbl(ctx) + hrid;
  int b = hrl_bucket_index(ctx, e->fast);
  kafs_hrl_bucket_lock(ctx, (uint32_t)b);
  if (e->refcnt == 0xFFFFFFFFu)
  {
    kafs_hrl_bucket_unlock(ctx, (uint32_t)b);
    return -EOVERFLOW;
  }
  e->refcnt += 1u;
  kafs_hrl_bucket_unlock(ctx, (uint32_t)b);
  return 0;
}

int kafs_hrl_dec_ref(kafs_context_t *ctx, kafs_hrid_t hrid)
{
  kafs_hrl_entry_t *e = hrl_entries_tbl(ctx) + hrid;
  int b = hrl_bucket_index(ctx, e->fast);
  kafs_hrl_bucket_lock(ctx, (uint32_t)b);
  if (e->refcnt == 0)
  {
    kafs_hrl_bucket_unlock(ctx, (uint32_t)b);
    return -EINVAL;
  }
  e->refcnt -= 1u;
  if (e->refcnt == 0)
  {
    // free physical block and remove from index chain
    kafs_blkcnt_t blo = e->blo;
    uint64_t fast = e->fast;
    int rc = hrl_release_blo(ctx, &blo);
    if (rc != 0)
    {
      kafs_hrl_bucket_unlock(ctx, (uint32_t)b);
      return rc;
    }
    (void)hrl_chain_remove(ctx, hrid, fast);
    hrl_slot_reset(e);
    kafs_hrl_bucket_unlock(ctx, (uint32_t)b);
    hrl_publish_free_slot(ctx, hrid);
    return 0;
  }
  kafs_hrl_bucket_unlock(ctx, (uint32_t)b);
  return 0;
}

int kafs_hrl_lookup(kafs_context_t *ctx, const kafs_hr_digest_t *dg, kafs_hrid_t *out_hrid)
{
  (void)dg;
  (void)ctx;
  (void)out_hrid;
  return -ENOSYS; // not used in this implementation
}

int kafs_hrl_read_block(kafs_context_t *ctx, kafs_hrid_t hrid, void *out_buf)
{
  kafs_hrl_entry_t *e = hrl_entries_tbl(ctx) + hrid;
  return hrl_read_blo(ctx, e->blo, out_buf);
}

int kafs_hrl_write_block(kafs_context_t *ctx, const void *buf, kafs_hrid_t *out_hrid,
                         int *out_is_new)
{
  kafs_blkcnt_t blo;
  return kafs_hrl_put(ctx, buf, out_hrid, out_is_new, &blo);
}

int kafs_hrl_inc_ref_by_blo(kafs_context_t *ctx, kafs_blkcnt_t blo)
{
  if (!ctx || ctx->c_hrl_bucket_cnt == 0 || hrl_capacity(ctx) == 0)
    return -ENOSYS;

  uint32_t bucket = 0;
  uint32_t idx = 0;
  int rc = hrl_find_entry_locked(ctx, hrl_match_live_blo, &blo, &bucket, &idx);
  if (rc != 0)
    return rc;

  kafs_hrl_entry_t *e = &hrl_entries_tbl(ctx)[idx];
  if (e->refcnt == 0xFFFFFFFFu)
  {
    kafs_hrl_bucket_unlock(ctx, bucket);
    return -EOVERFLOW;
  }
  e->refcnt += 1u;
  kafs_hrl_bucket_unlock(ctx, bucket);
  return 0;
}

int kafs_hrl_dec_ref_by_blo(kafs_context_t *ctx, kafs_blkcnt_t blo)
{
  if (!ctx || ctx->c_hrl_bucket_cnt == 0 || hrl_capacity(ctx) == 0)
  {
    // HRL 未構成: 直接解放
    return hrl_release_blo(ctx, &blo);
  }

  kafs_blksize_t bs = hrl_blksize(ctx);
  char buf[bs];
  int rc = hrl_read_blo(ctx, blo, buf);
  if (rc != 0)
    return rc;

  uint64_t fast = hrl_hash64(buf, bs);
  int b = hrl_bucket_index(ctx, fast);
  uint32_t *index = hrl_index_tbl(ctx);
  kafs_hrl_entry_t *ents = hrl_entries_tbl(ctx);
  uint32_t cap = hrl_capacity(ctx);

  kafs_hrl_bucket_lock(ctx, (uint32_t)b);
  uint32_t head = index[b];
  for (uint32_t steps = 0; head != 0 && steps < cap; ++steps)
  {
    uint32_t i = head - 1u;
    if (i >= cap)
    {
      kafs_hrl_bucket_unlock(ctx, (uint32_t)b);
      return -EIO;
    }
    kafs_hrl_entry_t *e = &ents[i];
    if (e->refcnt != 0 && e->fast == fast && e->blo == blo)
    {
      e->refcnt -= 1u;
      if (e->refcnt == 0)
      {
        kafs_blkcnt_t pblo = e->blo;
        uint32_t free_idx = i;
        int rc2 = hrl_release_blo(ctx, &pblo);
        if (rc2 != 0)
        {
          kafs_hrl_bucket_unlock(ctx, (uint32_t)b);
          return rc2;
        }
        (void)hrl_chain_remove(ctx, i, e->fast);
        hrl_slot_reset(e);
        kafs_hrl_bucket_unlock(ctx, (uint32_t)b);
        hrl_publish_free_slot(ctx, free_idx);
        return 0;
      }
      kafs_hrl_bucket_unlock(ctx, (uint32_t)b);
      return 0;
    }
    head = e->next_plus1;
  }
  kafs_hrl_bucket_unlock(ctx, (uint32_t)b);

  if (head != 0)
    return -EIO;

  // not managed by HRL => legacy, free directly.
  // If it's already free (e.g. another thread just dropped the last HRL ref), treat as success.
  if (kafs_blk_get_usage_locked(ctx, blo) == 0)
    return 0;
  return hrl_release_blo(ctx, &blo);
}

int kafs_hrl_match_inc_by_block_excluding_blo(kafs_context_t *ctx, const void *block_data,
                                              kafs_blkcnt_t exclude_blo, kafs_blkcnt_t *out_blo)
{
  if (!ctx || !block_data || !out_blo)
    return -EINVAL;
  if (ctx->c_hrl_bucket_cnt == 0 || hrl_capacity(ctx) == 0)
    return -ENOSYS;

  uint64_t fast = hrl_hash64(block_data, hrl_blksize(ctx));
  int b = hrl_bucket_index(ctx, fast);
  uint32_t idx = 0;

  kafs_hrl_bucket_lock(ctx, (uint32_t)b);
  int rc = hrl_find_by_hash(ctx, fast, block_data, &idx);
  if (rc == 0)
  {
    kafs_hrl_entry_t *e = hrl_entries_tbl(ctx) + idx;
    if (e->refcnt == 0 || e->blo == KAFS_BLO_NONE || e->blo == exclude_blo)
    {
      kafs_hrl_bucket_unlock(ctx, (uint32_t)b);
      return -ENOENT;
    }
    if (e->refcnt == 0xFFFFFFFFu)
    {
      kafs_hrl_bucket_unlock(ctx, (uint32_t)b);
      return -EOVERFLOW;
    }
    e->refcnt += 1u;
    *out_blo = e->blo;
    kafs_hrl_bucket_unlock(ctx, (uint32_t)b);
    return 0;
  }
  kafs_hrl_bucket_unlock(ctx, (uint32_t)b);
  return (rc == -ENOENT) ? -ENOENT : rc;
}

int kafs_hrl_evict_ref1_to_direct(kafs_context_t *ctx, kafs_blkcnt_t *out_blo)
{
  if (!ctx)
    return -EINVAL;
  if (ctx->c_hrl_bucket_cnt == 0 || hrl_capacity(ctx) == 0)
    return -ENOSYS;

  uint32_t bucket = 0;
  uint32_t idx = 0;
  int rc = hrl_find_entry_locked(ctx, hrl_match_evictable_ref1, NULL, &bucket, &idx);
  if (rc != 0)
    return rc;

  kafs_hrl_entry_t *e = &hrl_entries_tbl(ctx)[idx];
  kafs_blkcnt_t blo = e->blo;
  (void)hrl_chain_remove(ctx, idx, e->fast);
  hrl_slot_reset(e);
  kafs_hrl_bucket_unlock(ctx, bucket);
  hrl_publish_free_slot(ctx, idx);
  if (out_blo)
    *out_blo = blo;
  return 0;
}
