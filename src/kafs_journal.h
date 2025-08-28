#pragma once
#include <stdint.h>
#include <stdarg.h>

struct kafs_context;

typedef struct kafs_journal {
  int enabled;
  int fd;
  uint64_t seq;
  // lightweight mutex; if pthread unavailable, fall back to no-op
  void *mtx; // opaque to avoid leaking pthread headers
  // in-image journal fields
  int use_inimage;      // 1=use image-embedded journal
  uint64_t base_off;    // absolute file offset to journal header start
  uint64_t data_off;    // absolute file offset to journal data start (after header)
  uint64_t area_size;   // usable area size for records (ring capacity)
  uint64_t write_off;   // current write offset within ring [0..area_size)
  char *base_ptr;       // mapped base pointer (ctx->c_superblock + base_off)
} kafs_journal_t;

// Initialize journal if enabled by env. When enabled and no path provided,
// a sidecar file "<image>.journal" will be used.
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
typedef int (*kafs_journal_replay_cb)(struct kafs_context *ctx, const char *op, const char *args, void *user);

// Scan the in-image journal ring and invoke cb for each committed operation in order.
// External sidecar journals are ignored by replay for now (function returns 0 without actions).
int kafs_journal_replay(struct kafs_context *ctx, kafs_journal_replay_cb cb, void *user);
