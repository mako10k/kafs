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
