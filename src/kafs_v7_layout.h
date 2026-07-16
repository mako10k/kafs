#pragma once

#include "kafs_superblock.h"

#include <stddef.h>
#include <stdint.h>
#include <sys/types.h>

#define KAFS_V7_SUPERBLOCK_ANCHOR_MAGIC 0x4B375341u /* K7SA */
#define KAFS_V7_SUPERBLOCK_ANCHOR_VERSION 2u
#define KAFS_V7_LAYOUT_MAGIC 0x4B374C44u /* K7LD */
#define KAFS_V7_LAYOUT_VERSION 2u
#define KAFS_V7_CHECKPOINT_MAGIC 0x4B374350u /* K7CP */
#define KAFS_V7_CHECKPOINT_VERSION 1u
#define KAFS_V7_JOURNAL_HEADER_MAGIC 0x4B374A48u /* K7JH */
#define KAFS_V7_JOURNAL_HEADER_VERSION 1u
#define KAFS_V7_JOURNAL_BEGIN_TAG 0x4B374A42u    /* K7JB */
#define KAFS_V7_JOURNAL_MUTATION_TAG 0x4B374A4Du /* K7JM */
#define KAFS_V7_JOURNAL_COMMIT_TAG 0x4B374A43u   /* K7JC */
#define KAFS_V7_JOURNAL_ABORT_TAG 0x4B374A41u    /* K7JA */

#define KAFS_V7_ROOT_LOCATOR_BYTES 32u
#define KAFS_V7_LAYOUT_HEADER_BYTES 128u
#define KAFS_V7_GROUP_DESC_BYTES 96u
#define KAFS_V7_SHARD_DESC_BYTES 96u
#define KAFS_V7_REPLICA_DESC_BYTES 32u
#define KAFS_V7_CHECKPOINT_BYTES 64u
#define KAFS_V7_INODE_BYTES 128u
#define KAFS_V7_HRL_ENTRY_BYTES 24u
#define KAFS_V7_JOURNAL_HEADER_BYTES 64u
#define KAFS_V7_JOURNAL_RECORD_HEADER_BYTES 20u
#define KAFS_V7_JOURNAL_CONTROL_BYTES 32u
#define KAFS_V7_JOURNAL_MUTATION_HEADER_BYTES 56u
#define KAFS_V7_KDIR_HEADER_BYTES 24u
#define KAFS_V7_KDIR_RECORD_PREFIX_BYTES 14u
#define KAFS_V7_KDIR_MAGIC 0x4B444952u /* KDIR */
#define KAFS_V7_KDIR_VERSION 1u
#define KAFS_V7_KDIR_FLAG_TOMBSTONE 0x0001u
#define KAFS_V7_LAYOUT_MAX_BYTES (16u * 1024u * 1024u)
#define KAFS_V7_REPLICA_MAX_COUNT 3u
#define KAFS_V7_GROUP_MAX_COUNT 64u
#define KAFS_V7_SINGLE_GROUP_LOCAL_SHARDS 7u
#define KAFS_V7_GROUP_LOCAL_SHARDS 7u

#define KAFS_V7_HASH_FAST_FNV1A64 2u
#define KAFS_V7_HASH_STRONG_NONE 0u
#define KAFS_V7_REQUIRED_INCOMPAT_FLAGS UINT64_C(0x3f)

enum kafs_v7_storage_class
{
  KAFS_V7_STORAGE_INVALID = 0,
  KAFS_V7_STORAGE_FIXED_RECORD = 1,
  KAFS_V7_STORAGE_BIT_PACKED = 2,
  KAFS_V7_STORAGE_ALLOCATOR_SUMMARY = 3,
  KAFS_V7_STORAGE_BYTE_SPAN = 4,
};

enum kafs_v7_shard_type
{
  KAFS_V7_SHARD_SUPERBLOCK_CHECKPOINT = 0,
  KAFS_V7_SHARD_BLOCK_BITMAP = 1,
  KAFS_V7_SHARD_INODE_TABLE = 2,
  KAFS_V7_SHARD_ALLOCATOR_SUMMARY = 3,
  KAFS_V7_SHARD_HRL_INDEX = 4,
  KAFS_V7_SHARD_HRL_ENTRIES = 5,
  KAFS_V7_SHARD_JOURNAL_HEADER = 6,
  KAFS_V7_SHARD_JOURNAL_DATA = 7,
  KAFS_V7_SHARD_PENDING_LOG = 8,
  KAFS_V7_SHARD_TAIL_METADATA = 9,
  KAFS_V7_SHARD_UNKNOWN = 10,
  KAFS_V7_SHARD_LAYOUT_DESCRIPTOR = 11,
};

enum kafs_v7_replica_role
{
  KAFS_V7_REPLICA_PRIMARY = 0,
  KAFS_V7_REPLICA_TAIL = 1,
  KAFS_V7_REPLICA_MIDPOINT = 2,
};

enum kafs_v7_journal_target_type
{
  KAFS_V7_JOURNAL_TARGET_BLOCK_BITMAP = KAFS_V7_SHARD_BLOCK_BITMAP,
  KAFS_V7_JOURNAL_TARGET_INODE = KAFS_V7_SHARD_INODE_TABLE,
  KAFS_V7_JOURNAL_TARGET_ALLOCATOR_SUMMARY = KAFS_V7_SHARD_ALLOCATOR_SUMMARY,
  KAFS_V7_JOURNAL_TARGET_HRL_INDEX = KAFS_V7_SHARD_HRL_INDEX,
  KAFS_V7_JOURNAL_TARGET_HRL_ENTRY = KAFS_V7_SHARD_HRL_ENTRIES,
};

typedef struct kafs_v7_root_locator
{
  uint32_t magic;
  uint16_t version;
  uint16_t flags;
  uint64_t primary_desc_off;
  uint32_t primary_desc_bytes;
  uint32_t candidate_count;
  uint32_t anchor_crc32;
  uint32_t block_size;
} __attribute__((packed)) kafs_v7_root_locator_t;

typedef struct kafs_v7_layout_header
{
  uint32_t magic;
  uint16_t version;
  uint16_t header_bytes;
  uint32_t descriptor_bytes;
  uint32_t flags;
  uint64_t generation;
  uint64_t image_size_bytes;
  uint32_t block_size;
  uint32_t group_count;
  uint32_t group_desc_off;
  uint16_t group_desc_bytes;
  uint16_t mapping_policy;
  uint32_t shard_count;
  uint32_t shard_desc_off;
  uint16_t shard_desc_bytes;
  uint16_t reserved0;
  uint32_t replica_count;
  uint32_t replica_desc_off;
  uint16_t replica_desc_bytes;
  uint16_t reserved1;
  uint64_t feature_flags;
  uint64_t incompat_flags;
  uint64_t ro_compat_flags;
  uint64_t mapping_seed;
  uint32_t descriptor_crc32;
  uint32_t reserved2;
  uint64_t reserved3;
  uint64_t reserved4;
} __attribute__((packed)) kafs_v7_layout_header_t;

typedef struct kafs_v7_group_desc
{
  uint32_t group_id;
  uint32_t flags;
  uint32_t first_shard_index;
  uint32_t shard_count;
  uint64_t metadata_physical_off;
  uint64_t metadata_physical_bytes;
  uint64_t data_logical_start;
  uint64_t data_logical_count;
  uint64_t data_physical_off;
  uint64_t data_physical_bytes;
  uint64_t generation_floor;
  uint64_t reserved0;
  uint64_t reserved1;
  uint64_t reserved2;
} __attribute__((packed)) kafs_v7_group_desc_t;

typedef struct kafs_v7_shard_desc
{
  uint16_t type;
  uint16_t storage_class;
  uint32_t flags;
  uint32_t group_id;
  uint32_t reserved0;
  uint64_t physical_off;
  uint64_t physical_bytes;
  uint64_t logical_start;
  uint64_t logical_count;
  uint32_t record_bytes;
  uint32_t header_bytes;
  uint64_t generation_floor;
  uint64_t mapping_seed;
  uint64_t reserved1;
  uint64_t reserved2;
  uint64_t reserved3;
} __attribute__((packed)) kafs_v7_shard_desc_t;

typedef struct kafs_v7_replica_desc
{
  uint32_t replica_id;
  uint16_t role;
  uint16_t flags;
  uint64_t physical_off;
  uint32_t descriptor_bytes;
  uint32_t reserved0;
  uint64_t reserved1;
} __attribute__((packed)) kafs_v7_replica_desc_t;

typedef struct kafs_v7_checkpoint
{
  uint32_t magic;
  uint16_t version;
  uint16_t record_bytes;
  uint32_t flags;
  uint32_t reserved0;
  uint64_t generation;
  uint64_t descriptor_generation;
  uint64_t checkpoint_seq;
  uint64_t free_blocks;
  uint64_t free_inodes;
  uint32_t crc32;
  uint32_t reserved1;
} __attribute__((packed)) kafs_v7_checkpoint_t;

typedef struct kafs_v7_journal_header
{
  uint32_t magic;
  uint16_t version;
  uint16_t flags;
  uint32_t segment_id;
  uint32_t slot_bytes;
  uint64_t generation;
  uint64_t data_bytes;
  uint64_t write_bytes;
  uint64_t first_sequence;
  uint64_t last_sequence;
  uint32_t crc32;
  uint32_t reserved;
} __attribute__((packed)) kafs_v7_journal_header_t;

typedef struct kafs_v7_journal_record_header
{
  uint32_t tag;
  uint32_t payload_bytes;
  uint64_t sequence;
  uint32_t crc32;
} __attribute__((packed)) kafs_v7_journal_record_header_t;

typedef struct kafs_v7_journal_control
{
  uint32_t group_id;
  uint32_t mutation_count;
  uint32_t mutation_payload_bytes;
  uint32_t mutation_stream_crc32;
  int64_t free_blocks_delta;
  int64_t free_inodes_delta;
} __attribute__((packed)) kafs_v7_journal_control_t;

typedef struct kafs_v7_journal_mutation
{
  uint16_t target_type;
  uint16_t flags;
  uint32_t group_id;
  uint64_t logical_index;
  uint32_t target_bytes;
  uint32_t patch_off;
  uint32_t patch_bytes;
  uint32_t before_crc32;
  uint32_t after_crc32;
  uint32_t reserved;
  int64_t free_blocks_delta;
  int64_t free_inodes_delta;
} __attribute__((packed)) kafs_v7_journal_mutation_t;

typedef struct kafs_v7_inode
{
  uint16_t mode;
  uint16_t uid;
  uint64_t size;
  uint64_t atime;
  uint64_t ctime;
  uint64_t mtime;
  uint64_t dtime;
  uint16_t gid;
  uint16_t link_count;
  uint32_t blocks;
  uint16_t rdev;
  uint8_t inline_or_block_refs[60];
  uint8_t disabled_tail_bytes[14];
} __attribute__((packed)) kafs_v7_inode_t;

typedef struct kafs_v7_kdir_header
{
  uint32_t magic;
  uint16_t version;
  uint16_t flags;
  uint32_t live_count;
  uint32_t tombstone_count;
  uint32_t record_bytes;
  uint32_t reserved;
} __attribute__((packed)) kafs_v7_kdir_header_t;

typedef struct kafs_v7_kdir_record
{
  uint16_t record_length;
  uint16_t flags;
  uint32_t inode;
  uint16_t name_bytes;
  uint32_t name_hash;
  uint8_t name[];
} __attribute__((packed)) kafs_v7_kdir_record_t;

typedef struct kafs_v7_hrl_entry
{
  uint32_t ref_count;
  uint32_t next_entry_id_plus1;
  uint32_t logical_block_plus1;
  uint32_t reserved;
  uint64_t fast_hash;
} __attribute__((packed)) kafs_v7_hrl_entry_t;

_Static_assert(sizeof(kafs_v7_root_locator_t) == KAFS_V7_ROOT_LOCATOR_BYTES,
               "v7 root locator wire size");
_Static_assert(offsetof(kafs_v7_root_locator_t, anchor_crc32) == 24, "v7 root locator crc offset");
_Static_assert(sizeof(kafs_v7_layout_header_t) == KAFS_V7_LAYOUT_HEADER_BYTES,
               "v7 layout header wire size");
_Static_assert(offsetof(kafs_v7_layout_header_t, descriptor_crc32) == 104, "v7 layout crc offset");
_Static_assert(sizeof(kafs_v7_group_desc_t) == KAFS_V7_GROUP_DESC_BYTES,
               "v7 group descriptor wire size");
_Static_assert(sizeof(kafs_v7_shard_desc_t) == KAFS_V7_SHARD_DESC_BYTES,
               "v7 shard descriptor wire size");
_Static_assert(sizeof(kafs_v7_replica_desc_t) == KAFS_V7_REPLICA_DESC_BYTES,
               "v7 replica descriptor wire size");
_Static_assert(sizeof(kafs_v7_checkpoint_t) == KAFS_V7_CHECKPOINT_BYTES, "v7 checkpoint wire size");
_Static_assert(offsetof(kafs_v7_checkpoint_t, crc32) == 56, "v7 checkpoint crc offset");
_Static_assert(sizeof(kafs_v7_journal_header_t) == KAFS_V7_JOURNAL_HEADER_BYTES,
               "v7 journal header wire size");
_Static_assert(offsetof(kafs_v7_journal_header_t, crc32) == 56, "v7 journal header crc offset");
_Static_assert(sizeof(kafs_v7_journal_record_header_t) == KAFS_V7_JOURNAL_RECORD_HEADER_BYTES,
               "v7 journal record header wire size");
_Static_assert(offsetof(kafs_v7_journal_record_header_t, crc32) == 16,
               "v7 journal record crc offset");
_Static_assert(sizeof(kafs_v7_journal_control_t) == KAFS_V7_JOURNAL_CONTROL_BYTES,
               "v7 journal control wire size");
_Static_assert(sizeof(kafs_v7_journal_mutation_t) == KAFS_V7_JOURNAL_MUTATION_HEADER_BYTES,
               "v7 journal mutation header wire size");
_Static_assert(sizeof(kafs_v7_inode_t) == KAFS_V7_INODE_BYTES, "v7 inode wire size");
_Static_assert(offsetof(kafs_v7_inode_t, inline_or_block_refs) == 54,
               "v7 inode inline data offset");
_Static_assert(sizeof(kafs_v7_hrl_entry_t) == KAFS_V7_HRL_ENTRY_BYTES, "v7 HRL entry wire size");
_Static_assert(sizeof(kafs_v7_kdir_header_t) == KAFS_V7_KDIR_HEADER_BYTES,
               "v7 KDIR header wire size");
_Static_assert(sizeof(kafs_v7_kdir_record_t) == KAFS_V7_KDIR_RECORD_PREFIX_BYTES,
               "v7 KDIR record prefix wire size");

typedef enum kafs_v7_replica_status
{
  KAFS_V7_REPLICA_STATUS_INVALID = 0,
  KAFS_V7_REPLICA_STATUS_VALID = 1,
  KAFS_V7_REPLICA_STATUS_STALE = 2,
  KAFS_V7_REPLICA_STATUS_DIVERGENT = 3,
} kafs_v7_replica_status_t;

typedef struct kafs_v7_copy_report
{
  uint64_t offset;
  uint64_t generation;
  uint32_t bytes;
  uint32_t id;
  uint16_t role;
  uint8_t crc_ok;
  uint8_t selected;
  kafs_v7_replica_status_t status;
} kafs_v7_copy_report_t;

typedef struct kafs_v7_journal_report
{
  uint32_t selected_nonempty_segment_count;
  uint32_t record_count;
  uint32_t transaction_count;
  uint32_t pending_transaction_count;
  uint32_t committed_transaction_count;
  uint32_t aborted_transaction_count;
  uint32_t duplicate_transaction_count;
  uint32_t mutation_count;
  uint32_t already_applied_mutation_count;
  uint32_t replay_mutation_count;
  uint64_t first_sequence;
  uint64_t last_sequence;
  uint32_t last_sequence_group_id;
  uint32_t reserved;
} kafs_v7_journal_report_t;

typedef struct kafs_v7_layout_report
{
  uint8_t primary_identity_valid;
  uint8_t primary_locator_valid;
  uint8_t tail_locator_valid;
  uint8_t selected_found;
  uint8_t degraded;
  uint8_t descriptor_divergent;
  uint8_t checkpoint_divergent;
  uint8_t reserved0;
  uint32_t block_size;
  uint32_t descriptor_bytes;
  uint32_t replica_count;
  uint32_t selected_replica;
  uint32_t selected_checkpoint;
  uint32_t group_count;
  uint32_t shard_count;
  uint32_t journal_segment_count;
  uint64_t selected_generation;
  uint64_t checkpoint_generation;
  uint64_t checkpoint_sequence;
  uint64_t checkpoint_free_blocks;
  uint64_t checkpoint_free_inodes;
  uint64_t free_blocks;
  uint64_t free_inodes;
  uint64_t placement_span_bytes;
  uint64_t placement_arena_bytes;
  uint64_t min_group_data_blocks;
  uint64_t max_group_data_blocks;
  kafs_v7_root_locator_t locator;
  kafs_v7_copy_report_t descriptors[KAFS_V7_REPLICA_MAX_COUNT];
  kafs_v7_copy_report_t checkpoints[KAFS_V7_REPLICA_MAX_COUNT];
  kafs_v7_journal_report_t journal;
  void *descriptor;
} kafs_v7_layout_report_t;

typedef struct kafs_v7_mkfs_options
{
  uint64_t image_size_bytes;
  uint32_t block_size;
  uint32_t inode_count;
  uint64_t journal_bytes;
  double hrl_entry_ratio;
  uint16_t root_uid;
  uint16_t root_gid;
  uint32_t group_count;
} kafs_v7_mkfs_options_t;

uint32_t kafs_v7_crc32(const void *buf, size_t bytes);
const char *kafs_v7_replica_status_name(kafs_v7_replica_status_t status);
const char *kafs_v7_replica_role_name(uint16_t role);
const char *kafs_v7_shard_type_name(uint16_t type);
void kafs_v7_layout_report_clear(kafs_v7_layout_report_t *report);

int kafs_v7_mkfs_fd(int fd, const kafs_v7_mkfs_options_t *options, kafs_v7_layout_report_t *report);
int kafs_v7_validate_image_fd(int fd, const kafs_ssuperblock_t *sb, uint64_t file_size,
                              kafs_v7_layout_report_t *report);

const kafs_v7_layout_header_t *kafs_v7_report_header(const kafs_v7_layout_report_t *report);
const kafs_v7_group_desc_t *kafs_v7_report_groups(const kafs_v7_layout_report_t *report);
const kafs_v7_shard_desc_t *kafs_v7_report_shards(const kafs_v7_layout_report_t *report);
const kafs_v7_replica_desc_t *kafs_v7_report_replicas(const kafs_v7_layout_report_t *report);
