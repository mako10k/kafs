#pragma once
#include <stdint.h>
#include <stdarg.h>
#include <stddef.h>

// In-image journal format (shared with fsck)
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

static inline uint32_t kj_crc32_update(uint32_t crc, const uint8_t *buf, size_t len)
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
  char *base_ptr;     // mapped base pointer (ctx->c_superblock + base_off)
  // group commit controls
  uint64_t gc_delay_ns; // group commit window (nanoseconds), 0 disables grouping
  uint64_t gc_last_ns;  // batch start timestamp (CLOCK_MONOTONIC, ns)
  int gc_pending;       // 1 if there are unflushed records
} kafs_journal_t;

// Initialize journal. In-image journalが存在すれば有効化。KAFS_JOURNAL=0で明示無効。
int kafs_journal_init(struct kafs_context *ctx, const char *image_path);
void kafs_journal_shutdown(struct kafs_context *ctx);

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
