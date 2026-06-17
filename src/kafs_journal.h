#pragma once
#include "kafs_config.h"
#include <stdint.h>
#include <stdarg.h>
#include <stddef.h>

// In-image journal format (shared with fsck)
#define KJ_MAGIC 0x4b414a4c /* 'KAJL' */
#define KJ_VER 2
#define KJ_HEADER_FLAG_ROTATED (1u << 0)
#define KAFS_JOURNAL_FLAG_ROTATING_HEADERS (1u << 0)
#define KJ_HEADER_ROTATION_SLOTS 8u

typedef struct kj_header
{
  uint32_t magic;
  uint16_t version;
  uint16_t flags;
  uint64_t area_size;
  uint64_t write_off;
  uint64_t seq;
  uint64_t reserved0;  // generation for rotated header slots
  uint32_t header_crc; // CRC over this struct with this field zeroed
} __attribute__((packed)) kj_header_t;

#define KJ_TAG_BEG 0x42454732u  /* 'BEG2' */
#define KJ_TAG_CMT 0x434d5432u  /* 'CMT2' */
#define KJ_TAG_ABR 0x41425232u  /* 'ABR2' */
#define KJ_TAG_NOTE 0x4e4f5432u /* 'NOT2' */
#define KJ_TAG_WRAP 0x57524150u /* 'WRAP' */
#define KJ_TAG_MDT 0x4d445432u  /* 'MDT2' */

typedef struct kj_rec_hdr
{
  uint32_t tag;
  uint32_t size;
  uint64_t seq;
  uint32_t crc32;
} __attribute__((packed)) kj_rec_hdr_t;

static inline uint32_t kj_crc32_update(uint32_t crc, const uint8_t *buf, size_t len)
{
  crc = ~crc;
  for (size_t i = 0; i < len; ++i)
  {
    crc ^= buf[i];
    for (int k = 0; k < 8; ++k)
      crc = (crc >> 1) ^ (0xEDB88320u & (uint32_t)(0u - (crc & 1u)));
  }
  return ~crc;
}

static inline uint32_t kj_crc32(const void *buf, size_t len)
{
  return kj_crc32_update(0, (const uint8_t *)buf, len);
}

static inline size_t kj_header_size(void)
{
  size_t s = sizeof(kj_header_t);
  if (s % 64)
    s += 64 - (s % 64);
  return s;
}

static inline uint32_t kj_header_slot_count(uint32_t journal_flags, uint64_t journal_size)
{
  size_t hsz = kj_header_size();

  if ((journal_flags & KAFS_JOURNAL_FLAG_ROTATING_HEADERS) == 0)
    return 1u;
  if (journal_size <= (uint64_t)hsz * (uint64_t)KJ_HEADER_ROTATION_SLOTS)
    return 1u;
  return KJ_HEADER_ROTATION_SLOTS;
}

static inline uint64_t kj_journal_data_offset(uint64_t journal_offset)
{
  return journal_offset + (uint64_t)kj_header_size();
}

static inline uint64_t kj_journal_area_size(uint64_t journal_size, uint32_t journal_flags)
{
  uint32_t slots = kj_header_slot_count(journal_flags, journal_size);
  uint64_t header_bytes = (uint64_t)kj_header_size() * (uint64_t)slots;

  return (journal_size > header_bytes) ? (journal_size - header_bytes) : 0u;
}

static inline uint64_t kj_header_slot_offset(uint64_t journal_offset, uint64_t area_size,
                                             uint32_t slot_index)
{
  size_t hsz = kj_header_size();

  if (slot_index == 0)
    return journal_offset;
  return journal_offset + (uint64_t)hsz + area_size + ((uint64_t)(slot_index - 1u) * (uint64_t)hsz);
}

static inline uint32_t kj_header_crc_calc(const kj_header_t *hdr)
{
  kj_header_t tmp = *hdr;

  tmp.header_crc = 0;
  return kj_crc32(&tmp, sizeof(tmp));
}

static inline int kj_header_crc_ok(const kj_header_t *hdr)
{
  return kj_header_crc_calc(hdr) == hdr->header_crc;
}

static inline int kj_header_valid_for_area(const kj_header_t *hdr, uint64_t area_size)
{
  if (!hdr)
    return 0;
  if (hdr->magic != KJ_MAGIC || hdr->version != KJ_VER)
    return 0;
  if (hdr->area_size != area_size)
    return 0;
  if (hdr->write_off > area_size)
    return 0;
  return kj_header_crc_ok(hdr);
}

typedef int (*kj_header_slot_reader_t)(void *user, uint32_t slot, kj_header_t *hdr);

static inline int kj_header_select_best(uint32_t slot_count, uint64_t area_size,
                                        kj_header_slot_reader_t read_slot, void *user,
                                        kj_header_t *out, uint32_t *out_slot,
                                        uint32_t *out_valid_count)
{
  int found = 0;
  kj_header_t best = {0};
  uint32_t best_slot = 0;
  uint32_t valid_count = 0;

  if (!read_slot || slot_count == 0)
    return -1;

  for (uint32_t slot = 0; slot < slot_count; ++slot)
  {
    kj_header_t hdr;
    if (read_slot(user, slot, &hdr) != 0)
      continue;
    if (!kj_header_valid_for_area(&hdr, area_size))
      continue;
    valid_count++;
    if (!found || hdr.reserved0 > best.reserved0 || (hdr.reserved0 == best.reserved0 && slot == 0))
    {
      best = hdr;
      best_slot = slot;
      found = 1;
    }
  }

  if (out_valid_count)
    *out_valid_count = valid_count;
  if (!found)
    return -1;
  if (out)
    *out = best;
  if (out_slot)
    *out_slot = best_slot;
  return 0;
}

struct kafs_context;

typedef struct kafs_journal
{
  int enabled;
  int fd;
  uint64_t seq;
  // lightweight mutex; if pthread unavailable, fall back to no-op
  void *mtx; // opaque to avoid leaking pthread headers
  // in-image journal fields
  int use_inimage;    // 1=use image-embedded journal
  uint64_t base_off;  // absolute file offset to journal header start
  uint64_t data_off;  // absolute file offset to journal data start (after header)
  uint64_t area_size; // usable area size for records (ring capacity)
  uint64_t write_off; // current write offset within ring [0..area_size)
  uint32_t header_slot_count;
  uint32_t active_header_slot;
  uint64_t header_generation;
  char *base_ptr; // mapped base pointer (ctx->c_superblock + base_off)
  // group commit controls
  uint64_t gc_delay_ns; // group commit window (nanoseconds), 0 disables grouping
  uint64_t gc_last_ns;  // batch start timestamp (CLOCK_MONOTONIC, ns)
  int gc_pending;       // 1 if there are unflushed records
} kafs_journal_t;

// Initialize journal. In-image journalが存在すれば有効化。KAFS_JOURNAL=0で明示無効。
int kafs_journal_init(struct kafs_context *ctx, const char *image_path);
void kafs_journal_shutdown(struct kafs_context *ctx);
int kafs_journal_is_enabled(struct kafs_context *ctx);
void kafs_journal_force_flush(struct kafs_context *ctx);

// Start and finish a journal entry. begin returns sequence id (0 when disabled).
uint64_t kafs_journal_begin(struct kafs_context *ctx, const char *op, const char *fmt, ...);
void kafs_journal_commit(struct kafs_context *ctx, uint64_t seq);
void kafs_journal_abort(struct kafs_context *ctx, uint64_t seq, const char *reason_fmt, ...);

// Fire-and-forget single-line note (no transaction tracking)
void kafs_journal_note(struct kafs_context *ctx, const char *op, const char *fmt, ...);

// --- Replay support ---
// Callback: op is the operation name (e.g., "CREATE"), args is the formatted argument string
// (e.g., "path=/foo mode=100644"). Return 0 on success; nonzero to abort replay early.
typedef int (*kafs_journal_replay_cb)(struct kafs_context *ctx, const char *op, const char *args,
                                      void *user);

// Scan the in-image journal ring and invoke cb for each committed operation in order.
// External sidecar journals are ignored by replay for now (function returns 0 without actions).
int kafs_journal_replay(struct kafs_context *ctx, kafs_journal_replay_cb cb, void *user);
