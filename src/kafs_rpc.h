#pragma once

#include <stdint.h>
#include <sys/stat.h>
#include <sys/statvfs.h>
#include "kafs_ioctl.h"
#include "kafs_hotplug.h"

#define KAFS_RPC_MAGIC 0x4b415250u
#define KAFS_RPC_VERSION 1u
#define KAFS_RPC_MAX_PAYLOAD 16384u

#define KAFS_RPC_HELLO_MAJOR 1u
#define KAFS_RPC_HELLO_MINOR 0u
#define KAFS_RPC_HELLO_FEATURES 0u

#define KAFS_RPC_FLAG_ENDIAN_HOST 0x1u

typedef struct
{
  uint32_t magic;
  uint16_t version;
  uint16_t op;
  uint32_t flags;
  uint64_t req_id;
  uint64_t session_id;
  uint32_t epoch;
  uint32_t payload_len;
} kafs_rpc_hdr_t;

typedef struct
{
  uint64_t req_id;
  int32_t result;
  uint32_t payload_len;
} kafs_rpc_resp_hdr_t;

typedef struct
{
  uint16_t major;
  uint16_t minor;
  uint32_t feature_flags;
} kafs_rpc_hello_t;

typedef struct
{
  uint32_t uid;
  uint32_t gid;
  uint32_t pid;
  uint32_t umask;
} kafs_rpc_cred_t;

enum
{
  KAFS_RPC_OP_HELLO = 1,
  KAFS_RPC_OP_READY = 2,
  KAFS_RPC_OP_GETATTR = 3,
  KAFS_RPC_OP_READ = 4,
  KAFS_RPC_OP_WRITE = 5,
  KAFS_RPC_OP_TRUNCATE = 6,
  KAFS_RPC_OP_SESSION_RESTORE = 7,
  KAFS_RPC_OP_CTL_STATUS = 50,
  KAFS_RPC_OP_CTL_COMPAT = 51,
  KAFS_RPC_OP_CTL_RESTART = 52,
  KAFS_RPC_OP_CTL_SET_TIMEOUT = 53,
  KAFS_RPC_OP_CTL_ENV_LIST = 54,
  KAFS_RPC_OP_CTL_ENV_SET = 55,
  KAFS_RPC_OP_CTL_ENV_UNSET = 56,

  // FUSE proxy ops (front <-> back)
  KAFS_RPC_OP_FUSE_GETATTR = 100,
  KAFS_RPC_OP_FUSE_STATFS = 101,
  KAFS_RPC_OP_FUSE_ACCESS = 102,
  KAFS_RPC_OP_FUSE_OPEN = 103,
  KAFS_RPC_OP_FUSE_CREATE = 104,
  KAFS_RPC_OP_FUSE_OPENDIR = 105,
  KAFS_RPC_OP_FUSE_READDIR = 106,
  KAFS_RPC_OP_FUSE_READ = 107,
  KAFS_RPC_OP_FUSE_WRITE = 108,
  KAFS_RPC_OP_FUSE_TRUNCATE = 109,
  KAFS_RPC_OP_FUSE_RELEASE = 110,
  KAFS_RPC_OP_FUSE_RELEASEDIR = 111,
  KAFS_RPC_OP_FUSE_FLUSH = 112,
  KAFS_RPC_OP_FUSE_FSYNC = 113,
  KAFS_RPC_OP_FUSE_FSYNCDIR = 114,
  KAFS_RPC_OP_FUSE_MKDIR = 115,
  KAFS_RPC_OP_FUSE_RMDIR = 116,
  KAFS_RPC_OP_FUSE_UNLINK = 117,
  KAFS_RPC_OP_FUSE_RENAME = 118,
  KAFS_RPC_OP_FUSE_CHMOD = 119,
  KAFS_RPC_OP_FUSE_CHOWN = 120,
  KAFS_RPC_OP_FUSE_UTIMENS = 121,
  KAFS_RPC_OP_FUSE_READLINK = 122,
  KAFS_RPC_OP_FUSE_SYMLINK = 123,
  KAFS_RPC_OP_FUSE_MKNOD = 124,
  KAFS_RPC_OP_FUSE_IOCTL_CLONE = 125,
  KAFS_RPC_OP_FUSE_IOCTL_COPY = 126,
  KAFS_RPC_OP_FUSE_IOCTL_GET_STATS = 127,
  KAFS_RPC_OP_FUSE_COPY_FILE_RANGE = 128
};

  typedef struct
  {
    kafs_rpc_cred_t cred;
    uint32_t max_bytes;
    uint32_t path_len;
  } kafs_rpc_fuse_readlink_req_t;

  typedef struct
  {
    uint32_t size;
    // followed by data[size] (not NUL-terminated)
  } kafs_rpc_fuse_readlink_resp_t;

enum
{
  KAFS_RPC_DATA_INLINE = 1,
  KAFS_RPC_DATA_PLAN_ONLY = 2,
  KAFS_RPC_DATA_SHM = 3
};

typedef struct
{
  uint32_t open_handle_count;
} kafs_rpc_session_restore_t;

// ===== FUSE proxy wire formats =====
// Requests that carry a single path string.
typedef struct
{
  kafs_rpc_cred_t cred;
  uint32_t path_len;
} kafs_rpc_fuse_path_req_t;

typedef struct
{
  struct stat st;
} kafs_rpc_fuse_getattr_resp_t;

typedef struct
{
  struct statvfs st;
} kafs_rpc_fuse_statfs_resp_t;

// Requests that carry two path strings (e.g., rename, symlink).
typedef struct
{
  kafs_rpc_cred_t cred;
  uint32_t a_len;
  uint32_t b_len;
} kafs_rpc_fuse_two_path_req_t;

typedef struct
{
  kafs_rpc_cred_t cred;
  uint32_t mask;
  uint32_t path_len;
} kafs_rpc_fuse_access_req_t;

typedef struct
{
  kafs_rpc_cred_t cred;
  uint32_t flags;
  uint32_t path_len;
} kafs_rpc_fuse_open_req_t;

typedef struct
{
  uint64_t fh;
  uint32_t direct_io;
  uint32_t keep_cache;
} kafs_rpc_fuse_open_resp_t;

typedef struct
{
  kafs_rpc_cred_t cred;
  uint32_t flags;
  uint32_t mode;
  uint32_t path_len;
} kafs_rpc_fuse_create_req_t;

typedef struct
{
  kafs_rpc_cred_t cred;
  uint64_t fh;
  uint64_t off;
  uint32_t size;
  uint32_t reserved;
} kafs_rpc_fuse_read_req_t;

typedef struct
{
  uint32_t size;
} kafs_rpc_fuse_read_resp_t;

typedef struct
{
  kafs_rpc_cred_t cred;
  uint64_t fh;
  uint64_t off;
  uint32_t size;
  uint32_t reserved;
  // followed by data[size]
} kafs_rpc_fuse_write_req_t;

typedef struct
{
  uint32_t size;
} kafs_rpc_fuse_write_resp_t;

typedef struct
{
  kafs_rpc_cred_t cred;
  uint64_t fh;
  uint32_t flags;
  uint32_t reserved;
  uint32_t path_len;
} kafs_rpc_fuse_release_req_t;

typedef struct
{
  kafs_rpc_cred_t cred;
  uint64_t size;
  uint32_t path_len;
  uint32_t reserved;
} kafs_rpc_fuse_truncate_req_t;

typedef struct
{
  kafs_rpc_cred_t cred;
  uint32_t mode;
  uint32_t path_len;
} kafs_rpc_fuse_mkdir_req_t;

typedef struct
{
  kafs_rpc_cred_t cred;
  uint32_t mode;
  uint32_t path_len;
} kafs_rpc_fuse_chmod_req_t;

typedef struct
{
  kafs_rpc_cred_t cred;
  uint32_t uid;
  uint32_t gid;
  uint32_t path_len;
  uint32_t reserved;
} kafs_rpc_fuse_chown_req_t;

typedef struct
{
  kafs_rpc_cred_t cred;
  uint64_t atime_sec;
  uint64_t atime_nsec;
  uint64_t mtime_sec;
  uint64_t mtime_nsec;
  uint32_t path_len;
  uint32_t reserved;
} kafs_rpc_fuse_utimens_req_t;

typedef struct
{
  kafs_rpc_cred_t cred;
  uint32_t flags;
  uint32_t reserved;
  uint32_t path_len;
} kafs_rpc_fuse_opendir_req_t;

typedef struct
{
  uint64_t fh;
} kafs_rpc_fuse_opendir_resp_t;

typedef struct
{
  kafs_rpc_cred_t cred;
  uint32_t max_bytes;
  uint32_t path_len;
} kafs_rpc_fuse_readdir_req_t;

typedef struct
{
  uint32_t count;
  uint32_t truncated;
  // followed by repeated: uint16_t name_len; uint16_t reserved; char name[name_len]
} kafs_rpc_fuse_readdir_resp_t;

typedef struct
{
  kafs_rpc_cred_t cred;
  uint64_t fh;
  uint32_t isdatasync;
  uint32_t reserved;
} kafs_rpc_fuse_fsync_req_t;

typedef struct
{
  kafs_rpc_cred_t cred;
  uint64_t fh;
  uint32_t reserved0;
  uint32_t path_len;
} kafs_rpc_fuse_flush_req_t;

typedef struct
{
  kafs_rpc_cred_t cred;
  uint32_t flags;
  uint32_t reserved0;
  uint32_t a_len;
  uint32_t b_len;
} kafs_rpc_fuse_rename_req_t;

typedef struct
{
  kafs_rpc_cred_t cred;
  uint32_t mode;
  uint32_t dev;
  uint32_t path_len;
} kafs_rpc_fuse_mknod_req_t;

// ioctl(FICLONE): request carries src absolute path and either dst absolute path or dst file handle.
typedef struct
{
  kafs_rpc_cred_t cred;
  uint32_t a_len; // src path length
  uint32_t b_len; // dst path length (0 when dst_has_fh=1)
  uint64_t dst_fh;
  uint32_t dst_has_fh;
  uint32_t reserved0;
} kafs_rpc_fuse_ioctl_clone_req_t;

// KAFS_IOCTL_COPY: inline full request struct.
typedef struct
{
  kafs_rpc_cred_t cred;
  kafs_ioctl_copy_t req;
} kafs_rpc_fuse_ioctl_copy_req_t;

// KAFS_IOCTL_GET_STATS: response returns the stats struct.
typedef struct
{
  kafs_rpc_cred_t cred;
  uint32_t reserved0;
  uint32_t reserved1;
} kafs_rpc_fuse_ioctl_get_stats_req_t;

typedef struct
{
  kafs_stats_t st;
} kafs_rpc_fuse_ioctl_get_stats_resp_t;

// copy_file_range(2)
typedef struct
{
  kafs_rpc_cred_t cred;
  uint64_t fh_in;
  uint64_t fh_out;
  int64_t offset_in;
  int64_t offset_out;
  uint64_t size;
  uint32_t flags;
  uint32_t in_has_fh;
  uint32_t out_has_fh;
  uint32_t path_in_len;
  uint32_t path_out_len;
  // followed by optional: path_in[path_in_len], path_out[path_out_len]
} kafs_rpc_fuse_copy_file_range_req_t;

typedef struct
{
  int64_t n;
} kafs_rpc_fuse_copy_file_range_resp_t;

typedef struct
{
  uint32_t ino;
  uint32_t uid;
  uint32_t gid;
  uint32_t pid;
} kafs_rpc_getattr_req_t;

typedef struct
{
  struct stat st;
} kafs_rpc_getattr_resp_t;

typedef struct
{
  uint32_t ino;
  uint32_t uid;
  uint32_t gid;
  uint32_t pid;
  uint64_t off;
  uint32_t size;
  uint32_t data_mode;
} kafs_rpc_read_req_t;

typedef struct
{
  uint32_t size;
} kafs_rpc_read_resp_t;

typedef struct
{
  uint32_t ino;
  uint32_t uid;
  uint32_t gid;
  uint32_t pid;
  uint64_t off;
  uint32_t size;
  uint32_t data_mode;
} kafs_rpc_write_req_t;

typedef struct
{
  uint32_t size;
} kafs_rpc_write_resp_t;

typedef struct
{
  uint32_t ino;
  uint32_t reserved;
  uint64_t size;
} kafs_rpc_truncate_req_t;

typedef struct
{
  uint64_t size;
} kafs_rpc_truncate_resp_t;

typedef struct
{
  uint32_t version;
  uint32_t state;
  uint32_t data_mode;
  uint64_t session_id;
  uint32_t epoch;
  int32_t last_error;
  uint32_t wait_queue_len;
  uint32_t wait_timeout_ms;
  uint32_t wait_queue_limit;
  uint16_t front_major;
  uint16_t front_minor;
  uint32_t front_features;
  uint16_t back_major;
  uint16_t back_minor;
  uint32_t back_features;
  uint32_t compat_result;
  int32_t compat_reason;
} kafs_rpc_hotplug_status_t;

typedef struct
{
  uint32_t timeout_ms;
} kafs_rpc_set_timeout_t;

typedef struct
{
  uint32_t count;
  kafs_hotplug_env_entry_t entries[KAFS_HOTPLUG_ENV_MAX];
} kafs_rpc_env_list_t;

typedef struct
{
  char key[KAFS_HOTPLUG_ENV_KEY_MAX];
  char value[KAFS_HOTPLUG_ENV_VALUE_MAX];
} kafs_rpc_env_update_t;

uint64_t kafs_rpc_next_req_id(void);
int kafs_rpc_send_msg(int fd, uint16_t op, uint32_t flags, uint64_t req_id, uint64_t session_id,
                      uint32_t epoch, const void *payload, uint32_t payload_len);
int kafs_rpc_recv_msg(int fd, kafs_rpc_hdr_t *hdr, void *payload, uint32_t payload_cap,
                      uint32_t *payload_len);
int kafs_rpc_send_resp(int fd, uint64_t req_id, int32_t result, const void *payload,
                       uint32_t payload_len);
int kafs_rpc_recv_resp(int fd, kafs_rpc_resp_hdr_t *hdr, void *payload, uint32_t payload_cap,
                       uint32_t *payload_len);
