#include "kafs_hash.h"
#include "kafs_locks.h"
#include "kafs_block.h"
#include <string.h>
#include <unistd.h>
#include <errno.h>

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

static inline kafs_logblksize_t hrl_log_blksize(kafs_context_t *ctx)
{
  return kafs_sb_log_blksize_get(ctx->c_superblock);
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
    uint32_t i = head - 1u;
    if (i >= cap)
      return -EIO;
    kafs_hrl_entry_t *e = &ents[i];
    if (e->refcnt != 0 && hrl_entry_cmp_content(ctx, e, buf, fast))
    {
      *out_index = i;
      return 0;
    }
    head = e->next_plus1;
  }
  return (head == 0) ? -ENOENT : -EIO;
}

static int hrl_find_free_slot(kafs_context_t *ctx, uint32_t *out_index)
{
  kafs_hrl_entry_t *ents = hrl_entries_tbl(ctx);
  uint32_t cap = hrl_capacity(ctx);
  for (uint32_t i = 0; i < cap; ++i)
  {
    if (ents[i].refcnt == 0)
    {
      *out_index = i;
      return 0;
    }
  }
  return -ENOSPC;
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
    return 0;
  }
  ctx->c_hrl_index = (void *)(base + index_off);
  ctx->c_hrl_bucket_cnt = (uint32_t)(index_size / sizeof(uint32_t));
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
  return 0;
}

int kafs_hrl_put(kafs_context_t *ctx, const void *block_data, kafs_hrid_t *out_hrid,
                 int *out_is_new, kafs_blkcnt_t *out_blo)
{
  if (!ctx || !block_data || !out_hrid || !out_is_new || !out_blo)
    return -EINVAL;
  if (ctx->c_hrl_bucket_cnt == 0 || hrl_capacity(ctx) == 0)
    return -ENOSYS;
  uint64_t fast = hrl_hash64(block_data, hrl_blksize(ctx));
  uint32_t idx;
  int b = hrl_bucket_index(ctx, fast);
  kafs_hrl_bucket_lock(ctx, (uint32_t)b);
  if (hrl_find_by_hash(ctx, fast, block_data, &idx) == 0)
  {
    kafs_hrl_entry_t *e = hrl_entries_tbl(ctx) + idx;
    *out_hrid = idx;
    *out_is_new = 0;
    *out_blo = e->blo;
    kafs_hrl_bucket_unlock(ctx, (uint32_t)b);
    return 0;
  }
  // allocate entry
  if (hrl_find_free_slot(ctx, &idx) != 0)
  {
    kafs_hrl_bucket_unlock(ctx, (uint32_t)b);
    return -ENOSPC;
  }
  // allocate physical block and write
  kafs_blkcnt_t blo = KAFS_BLO_NONE;
  int rc = kafs_blk_alloc(ctx, &blo);
  if (rc != 0)
  {
    kafs_hrl_bucket_unlock(ctx, (uint32_t)b);
    return rc;
  }
  rc = hrl_write_blo(ctx, blo, block_data);
  if (rc != 0)
  {
    (void)hrl_release_blo(ctx, &blo);
    kafs_hrl_bucket_unlock(ctx, (uint32_t)b);
    return rc;
  }
  kafs_hrl_entry_t *e = hrl_entries_tbl(ctx) + idx;
  e->refcnt = 0; // caller will inc_ref
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
    int rc = hrl_release_blo(ctx, &blo);
    if (rc != 0)
    {
      kafs_hrl_bucket_unlock(ctx, (uint32_t)b);
      return rc;
    }
    (void)hrl_chain_remove(ctx, hrid, e->fast);
    memset(e, 0, sizeof(*e));
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

// Helper: find HRID by physical block by hashing its content
static int kafs_hrl_find_by_blo(kafs_context_t *ctx, kafs_blkcnt_t blo, kafs_hrid_t *out_hrid)
{
  kafs_blksize_t bs = hrl_blksize(ctx);
  char buf[bs];
  int rc = hrl_read_blo(ctx, blo, buf);
  if (rc != 0)
    return rc;
  uint64_t fast = hrl_hash64(buf, bs);
  uint32_t idx;
  if (hrl_find_by_hash(ctx, fast, buf, &idx) == 0)
  {
    kafs_hrl_entry_t *e = hrl_entries_tbl(ctx) + idx;
    if (hrl_entry_cmp_blo(ctx, e, blo, buf))
    {
      *out_hrid = idx;
      return 0;
    }
  }
  return -ENOENT;
}

int kafs_hrl_dec_ref_by_blo(kafs_context_t *ctx, kafs_blkcnt_t blo)
{
  if (!ctx || ctx->c_hrl_bucket_cnt == 0 || hrl_capacity(ctx) == 0)
  {
    // HRL 未構成: 直接解放
    return hrl_release_blo(ctx, &blo);
  }
  kafs_hrid_t hrid;
  int rc = kafs_hrl_find_by_blo(ctx, blo, &hrid);
  if (rc == 0)
  {
    // dec_ref() 内でバケットロックを取得するため、ここではロックしない
    int rc2 = kafs_hrl_dec_ref(ctx, hrid);
    // 参照が既に0になって削除された等の競合では -EINVAL が返り得るが、
    // その場合は解放済みとみなして成功扱いにする
    return (rc2 == -EINVAL) ? 0 : rc2;
  }
  // not managed by HRL => legacy, free directly
  return hrl_release_blo(ctx, &blo);
}
