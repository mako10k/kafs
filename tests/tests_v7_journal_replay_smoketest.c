#include "test_utils.h"

#include "kafs_context.h"
#include "kafs_offline_summary.h"
#include "kafs_superblock.h"
#include "kafs_tool_util.h"
#include "kafs_v7_layout.h"
#include "kafs_v7_runtime_view.h"
#include "kafs_v7_sequence.h"

#include <endian.h>
#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <pthread.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

typedef struct replay_target_fixture
{
  uint16_t type;
  uint32_t group_id;
  uint64_t logical_index;
  uint64_t physical_off;
  uint32_t bytes;
  int64_t free_blocks_delta;
  int64_t free_inodes_delta;
  uint8_t *before;
  uint8_t *after;
} replay_target_fixture_t;

typedef struct replay_transaction_fixture
{
  uint64_t sequence;
  uint32_t terminal_tag;
  uint8_t *bytes;
  size_t byte_count;
  replay_target_fixture_t targets[3];
} replay_transaction_fixture_t;

typedef struct replay_image_fixture
{
  int fd;
  uint64_t file_size;
  kafs_ssuperblock_t sb;
  kafs_v7_layout_report_t layout;
} replay_image_fixture_t;

typedef struct sequence_reserve_worker
{
  kafs_v7_sequence_state_t *state;
  uint32_t group_id;
  int rc;
} sequence_reserve_worker_t;

static void *reserve_sequence_worker(void *opaque)
{
  sequence_reserve_worker_t *worker = (sequence_reserve_worker_t *)opaque;
  kafs_v7_sequence_reservation_t reservation;
  memset(&reservation, 0, sizeof(reservation));
  worker->rc = kafs_v7_sequence_reserve(worker->state, worker->group_id, &reservation);
  return NULL;
}

static int run_command(char *const argv[], int expected_exit)
{
  pid_t pid = fork();
  if (pid < 0)
    return -errno;
  if (pid == 0)
  {
    execvp(argv[0], argv);
    _exit(127);
  }
  int status = 0;
  if (waitpid(pid, &status, 0) != pid || !WIFEXITED(status))
    return -1;
  return WEXITSTATUS(status) == expected_exit ? 0 : -1;
}

static int format_image_with_groups(const char *path, uint32_t group_count)
{
  char groups[16];
  if (snprintf(groups, sizeof(groups), "%" PRIu32, group_count) < 0)
    return -1;
  char *argv[] = {(char *)kafs_test_mkfs_bin(),
                  (char *)path,
                  (char *)"--format-version",
                  (char *)"7",
                  (char *)"--size-bytes",
                  group_count == 1u ? (char *)"64M" : (char *)"128M",
                  (char *)"--v7-group-count",
                  groups,
                  (char *)"--yes",
                  NULL};
  return run_command(argv, 0);
}

static int load_superblock_and_size(int fd, kafs_ssuperblock_t *sb, uint64_t *file_size)
{
  int rc = kafs_pread_all(fd, sb, sizeof(*sb), 0);
  if (rc == 0)
    rc = kafs_offline_detect_file_size(fd, file_size);
  return rc;
}

static int open_fixture_with_groups(const char *path, uint32_t group_count,
                                    replay_image_fixture_t *fixture)
{
  if (!path || !fixture || group_count == 0 || format_image_with_groups(path, group_count) != 0)
    return -1;
  memset(fixture, 0, sizeof(*fixture));
  fixture->fd = open(path, O_RDWR);
  if (fixture->fd < 0)
    return -errno;
  int rc = load_superblock_and_size(fixture->fd, &fixture->sb, &fixture->file_size);
  if (rc == 0)
    rc = kafs_v7_validate_image_fd(fixture->fd, &fixture->sb, fixture->file_size,
                                   &fixture->layout);
  if (rc != 0)
  {
    close(fixture->fd);
    fixture->fd = -1;
  }
  return rc;
}

static int open_fixture(const char *path, replay_image_fixture_t *fixture)
{
  return open_fixture_with_groups(path, 1u, fixture);
}

static void close_fixture(replay_image_fixture_t *fixture)
{
  if (!fixture)
    return;
  if (fixture->fd >= 0)
    close(fixture->fd);
  kafs_v7_layout_report_clear(&fixture->layout);
  memset(fixture, 0, sizeof(*fixture));
  fixture->fd = -1;
}

static void transaction_clear(replay_transaction_fixture_t *transaction)
{
  if (!transaction)
    return;
  for (size_t i = 0; i < sizeof(transaction->targets) / sizeof(transaction->targets[0]); ++i)
  {
    free(transaction->targets[i].before);
    free(transaction->targets[i].after);
  }
  free(transaction->bytes);
  memset(transaction, 0, sizeof(*transaction));
}

static int all_zero(const void *buf, size_t bytes)
{
  const uint8_t *p = (const uint8_t *)buf;
  for (size_t i = 0; i < bytes; ++i)
  {
    if (p[i] != 0)
      return 0;
  }
  return 1;
}

static uint32_t crc32_update(uint32_t crc, const void *buf, size_t bytes)
{
  const uint8_t *p = (const uint8_t *)buf;
  for (size_t i = 0; i < bytes; ++i)
  {
    crc ^= p[i];
    for (unsigned bit = 0; bit < 8u; ++bit)
      crc = (crc >> 1u) ^ ((crc & 1u) ? UINT32_C(0xedb88320) : 0u);
  }
  return crc;
}

static void store_i64(void *field, int64_t value)
{
  uint64_t wire = htole64((uint64_t)value);
  memcpy(field, &wire, sizeof(wire));
}

static int allocate_target(replay_target_fixture_t *target, uint32_t bytes)
{
  target->before = (uint8_t *)malloc(bytes);
  target->after = (uint8_t *)malloc(bytes);
  if (!target->before || !target->after)
    return -ENOMEM;
  target->bytes = bytes;
  return 0;
}

static void build_allocator_summary(const uint8_t *bitmap, uint64_t blocks, uint8_t *summary)
{
  uint64_t l0_bytes = (blocks + 7u) / 8u;
  uint64_t l1_bytes = (l0_bytes + 7u) / 8u;
  uint64_t l2_bytes = (l1_bytes + 7u) / 8u;
  memset(summary, 0, (size_t)(l1_bytes + l2_bytes));
  for (uint64_t i = 0; i < l0_bytes; ++i)
  {
    uint8_t valid_mask = 0xffu;
    if (i + 1u == l0_bytes && (blocks & 7u) != 0)
      valid_mask = (uint8_t)((1u << (blocks & 7u)) - 1u);
    if ((bitmap[i] & valid_mask) != valid_mask)
      summary[i / 8u] |= (uint8_t)(1u << (i % 8u));
  }
  for (uint64_t i = 0; i < l1_bytes; ++i)
  {
    if (summary[i] != 0)
      summary[l1_bytes + i / 8u] |= (uint8_t)(1u << (i % 8u));
  }
}

static int append_record(uint8_t *transaction, size_t capacity, size_t *off, uint32_t tag,
                         uint64_t sequence, const void *payload, uint32_t payload_bytes)
{
  size_t record_bytes = KAFS_V7_JOURNAL_RECORD_HEADER_BYTES + (size_t)payload_bytes;
  size_t padded_bytes = (record_bytes + 7u) & ~(size_t)7u;
  if (!transaction || !off || !payload || *off > capacity || padded_bytes > capacity - *off)
    return -EOVERFLOW;
  uint8_t *record_bytes_ptr = transaction + *off;
  memset(record_bytes_ptr, 0, padded_bytes);
  kafs_v7_journal_record_header_t *header =
      (kafs_v7_journal_record_header_t *)record_bytes_ptr;
  header->tag = htole32(tag);
  header->payload_bytes = htole32(payload_bytes);
  header->sequence = htole64(sequence);
  memcpy(record_bytes_ptr + KAFS_V7_JOURNAL_RECORD_HEADER_BYTES, payload, payload_bytes);
  header->crc32 = htole32(kafs_v7_crc32(record_bytes_ptr, record_bytes));
  *off += padded_bytes;
  return 0;
}

static int build_mutation_payload(const replay_target_fixture_t *target, uint8_t **payload_out,
                                  uint32_t *payload_bytes_out)
{
  if (!target || !payload_out || !payload_bytes_out ||
      target->bytes > UINT32_MAX - KAFS_V7_JOURNAL_MUTATION_HEADER_BYTES)
    return -EINVAL;
  uint32_t payload_bytes = KAFS_V7_JOURNAL_MUTATION_HEADER_BYTES + target->bytes;
  uint8_t *payload = (uint8_t *)calloc(1u, payload_bytes);
  if (!payload)
    return -ENOMEM;
  kafs_v7_journal_mutation_t *mutation = (kafs_v7_journal_mutation_t *)payload;
  mutation->target_type = htole16(target->type);
  mutation->group_id = htole32(target->group_id);
  mutation->logical_index = htole64(target->logical_index);
  mutation->target_bytes = htole32(target->bytes);
  mutation->patch_bytes = htole32(target->bytes);
  mutation->before_crc32 = htole32(kafs_v7_crc32(target->before, target->bytes));
  mutation->after_crc32 = htole32(kafs_v7_crc32(target->after, target->bytes));
  store_i64(payload + offsetof(kafs_v7_journal_mutation_t, free_blocks_delta),
            target->free_blocks_delta);
  store_i64(payload + offsetof(kafs_v7_journal_mutation_t, free_inodes_delta),
            target->free_inodes_delta);
  memcpy(payload + KAFS_V7_JOURNAL_MUTATION_HEADER_BYTES, target->after, target->bytes);
  *payload_out = payload;
  *payload_bytes_out = payload_bytes;
  return 0;
}

static int prepare_targets(replay_image_fixture_t *fixture, uint32_t group_id,
                           replay_transaction_fixture_t *transaction)
{
  const kafs_v7_shard_desc_t *shards = kafs_v7_report_shards(&fixture->layout);
  if (group_id >= fixture->layout.group_count)
    return -EINVAL;
  const kafs_v7_shard_desc_t *local =
      &shards[(uint64_t)group_id * KAFS_V7_GROUP_LOCAL_SHARDS];
  const kafs_v7_shard_desc_t *bitmap_shard = &local[0];
  const kafs_v7_shard_desc_t *allocator_shard = &local[2];
  const kafs_v7_shard_desc_t *inode_shard = &local[1];
  uint64_t blocks = le64toh(bitmap_shard->logical_count);
  uint64_t bitmap_bytes = le64toh(bitmap_shard->physical_bytes);
  uint8_t *bitmap = (uint8_t *)malloc((size_t)bitmap_bytes);
  if (!bitmap)
    return -ENOMEM;
  int rc = kafs_pread_all(fixture->fd, bitmap, (size_t)bitmap_bytes,
                          (off_t)le64toh(bitmap_shard->physical_off));
  uint64_t word = 0;
  while (rc == 0 && word < blocks / 64u &&
         all_zero(bitmap + word * 8u, 8u) == 0)
    ++word;
  if (rc == 0 && word == blocks / 64u)
    rc = -ENOSPC;

  replay_target_fixture_t *bitmap_target = &transaction->targets[0];
  if (rc == 0)
    rc = allocate_target(bitmap_target, 8u);
  if (rc == 0)
  {
    bitmap_target->type = KAFS_V7_JOURNAL_TARGET_BLOCK_BITMAP;
    bitmap_target->group_id = group_id;
    bitmap_target->logical_index = le64toh(bitmap_shard->logical_start) + word * 64u;
    bitmap_target->physical_off = le64toh(bitmap_shard->physical_off) + word * 8u;
    bitmap_target->free_blocks_delta = -64;
    memcpy(bitmap_target->before, bitmap + word * 8u, 8u);
    memset(bitmap_target->after, 0xff, 8u);
    memcpy(bitmap + word * 8u, bitmap_target->after, 8u);
  }

  uint64_t l0_bytes = (blocks + 7u) / 8u;
  uint64_t l1_bytes = (l0_bytes + 7u) / 8u;
  uint64_t l2_bytes = (l1_bytes + 7u) / 8u;
  replay_target_fixture_t *allocator_target = &transaction->targets[1];
  if (rc == 0 && l1_bytes + l2_bytes > UINT32_MAX)
    rc = -EOVERFLOW;
  if (rc == 0)
    rc = allocate_target(allocator_target, (uint32_t)(l1_bytes + l2_bytes));
  if (rc == 0)
  {
    allocator_target->type = KAFS_V7_JOURNAL_TARGET_ALLOCATOR_SUMMARY;
    allocator_target->group_id = group_id;
    allocator_target->logical_index = le64toh(bitmap_shard->logical_start);
    allocator_target->physical_off = le64toh(allocator_shard->physical_off);
    rc = kafs_pread_all(fixture->fd, allocator_target->before, allocator_target->bytes,
                        (off_t)allocator_target->physical_off);
    if (rc == 0)
      build_allocator_summary(bitmap, blocks, allocator_target->after);
  }

  replay_target_fixture_t *inode_target = &transaction->targets[2];
  if (rc == 0)
    rc = allocate_target(inode_target, KAFS_V7_INODE_BYTES);
  uint64_t inode_count = le64toh(inode_shard->logical_count);
  uint64_t inode_start = le64toh(inode_shard->logical_start);
  uint64_t inode_local = 0;
  while (rc == 0 && inode_local < inode_count)
  {
    uint64_t inode = inode_start + inode_local;
    rc = kafs_pread_all(fixture->fd, inode_target->before, KAFS_V7_INODE_BYTES,
                        (off_t)(le64toh(inode_shard->physical_off) +
                                inode_local * KAFS_V7_INODE_BYTES));
    if (rc == 0 && inode >= 2u && all_zero(inode_target->before, KAFS_V7_INODE_BYTES))
      break;
    ++inode_local;
  }
  if (rc == 0 && inode_local == inode_count)
    rc = -ENOSPC;
  if (rc == 0)
  {
    inode_target->type = KAFS_V7_JOURNAL_TARGET_INODE;
    inode_target->group_id = group_id;
    inode_target->logical_index = inode_start + inode_local;
    inode_target->physical_off =
        le64toh(inode_shard->physical_off) + inode_local * KAFS_V7_INODE_BYTES;
    inode_target->free_inodes_delta = -1;
    memcpy(inode_target->after, inode_target->before, KAFS_V7_INODE_BYTES);
    kafs_v7_inode_t *inode = (kafs_v7_inode_t *)inode_target->after;
    inode->mode = htole16((uint16_t)(S_IFREG | 0644));
    inode->link_count = htole16(1u);
  }
  free(bitmap);
  return rc;
}

static int build_transaction(replay_image_fixture_t *fixture, uint32_t group_id, uint64_t sequence,
                             uint32_t terminal_tag, replay_transaction_fixture_t *transaction)
{
  memset(transaction, 0, sizeof(*transaction));
  transaction->sequence = sequence;
  transaction->terminal_tag = terminal_tag;
  int rc = prepare_targets(fixture, group_id, transaction);
  uint8_t *payloads[3] = {NULL, NULL, NULL};
  uint32_t payload_bytes[3] = {0, 0, 0};
  uint32_t stream_crc = UINT32_MAX;
  uint32_t stream_bytes = 0;
  for (size_t i = 0; rc == 0 && i < 3u; ++i)
  {
    rc = build_mutation_payload(&transaction->targets[i], &payloads[i], &payload_bytes[i]);
    if (rc == 0 && payload_bytes[i] > UINT32_MAX - stream_bytes)
      rc = -EOVERFLOW;
    if (rc == 0)
    {
      stream_bytes += payload_bytes[i];
      stream_crc = crc32_update(stream_crc, payloads[i], payload_bytes[i]);
    }
  }

  kafs_v7_journal_control_t control;
  memset(&control, 0, sizeof(control));
  control.group_id = htole32(group_id);
  control.mutation_count = htole32(3u);
  control.mutation_payload_bytes = htole32(stream_bytes);
  control.mutation_stream_crc32 = htole32(stream_crc ^ UINT32_MAX);
  store_i64((uint8_t *)&control + offsetof(kafs_v7_journal_control_t, free_blocks_delta),
            transaction->targets[0].free_blocks_delta);
  store_i64((uint8_t *)&control + offsetof(kafs_v7_journal_control_t, free_inodes_delta),
            transaction->targets[2].free_inodes_delta);

  size_t capacity = 2u * 56u;
  for (size_t i = 0; i < 3u; ++i)
    capacity += (KAFS_V7_JOURNAL_RECORD_HEADER_BYTES + payload_bytes[i] + 7u) & ~(size_t)7u;
  if (rc == 0)
  {
    transaction->bytes = (uint8_t *)calloc(1u, capacity);
    if (!transaction->bytes)
      rc = -ENOMEM;
  }
  size_t off = 0;
  if (rc == 0)
    rc = append_record(transaction->bytes, capacity, &off, KAFS_V7_JOURNAL_BEGIN_TAG, sequence,
                       &control, sizeof(control));
  for (size_t i = 0; rc == 0 && i < 3u; ++i)
    rc = append_record(transaction->bytes, capacity, &off, KAFS_V7_JOURNAL_MUTATION_TAG, sequence,
                       payloads[i], payload_bytes[i]);
  if (rc == 0)
    rc = append_record(transaction->bytes, capacity, &off, terminal_tag, sequence, &control,
                       sizeof(control));
  if (rc == 0)
    transaction->byte_count = off;
  for (size_t i = 0; i < 3u; ++i)
    free(payloads[i]);
  if (rc != 0)
    transaction_clear(transaction);
  return rc;
}

static int publish_prefix(replay_image_fixture_t *fixture, uint32_t group_id,
                          uint32_t local_segment, const void *prefix, size_t prefix_bytes,
                          uint64_t first_sequence, uint64_t last_sequence)
{
  const kafs_v7_shard_desc_t *shards = kafs_v7_report_shards(&fixture->layout);
  if (group_id >= fixture->layout.group_count)
    return -EINVAL;
  const kafs_v7_shard_desc_t *local =
      &shards[(uint64_t)group_id * KAFS_V7_GROUP_LOCAL_SHARDS];
  const kafs_v7_shard_desc_t *header_shard = &local[5];
  const kafs_v7_shard_desc_t *data_shard = &local[6];
  uint64_t segment_count = le64toh(data_shard->logical_count);
  uint64_t segment_bytes = le64toh(data_shard->physical_bytes) / segment_count;
  if (!prefix || local_segment >= segment_count || prefix_bytes == 0 ||
      (prefix_bytes & 7u) != 0 || prefix_bytes > segment_bytes)
    return -EINVAL;
  uint64_t data_off = le64toh(data_shard->physical_off) + local_segment * segment_bytes;
  int rc = kafs_pwrite_all(fixture->fd, prefix, prefix_bytes, (off_t)data_off);
  if (rc == 0 && fdatasync(fixture->fd) != 0)
    rc = -errno;
  kafs_v7_journal_header_t header;
  memset(&header, 0, sizeof(header));
  header.magic = htole32(KAFS_V7_JOURNAL_HEADER_MAGIC);
  header.version = htole16(KAFS_V7_JOURNAL_HEADER_VERSION);
  header.segment_id = htole32((uint32_t)le64toh(header_shard->logical_start) + local_segment);
  header.slot_bytes = htole32(KAFS_V7_JOURNAL_HEADER_BYTES);
  header.generation = htole64(2u);
  header.data_bytes = htole64(segment_bytes);
  header.write_bytes = htole64(prefix_bytes);
  header.first_sequence = htole64(first_sequence);
  header.last_sequence = htole64(last_sequence);
  header.crc32 = htole32(kafs_v7_crc32(&header, sizeof(header)));
  uint64_t header_off = le64toh(header_shard->physical_off) +
                        local_segment * fixture->layout.block_size + KAFS_V7_JOURNAL_HEADER_BYTES;
  if (rc == 0)
    rc = kafs_pwrite_all(fixture->fd, &header, sizeof(header), (off_t)header_off);
  if (rc == 0 && fdatasync(fixture->fd) != 0)
    rc = -errno;
  return rc;
}

static int write_torn_higher_header(replay_image_fixture_t *fixture, uint32_t group_id,
                                    uint32_t local_segment)
{
  const kafs_v7_shard_desc_t *shards = kafs_v7_report_shards(&fixture->layout);
  if (group_id >= fixture->layout.group_count)
    return -EINVAL;
  const kafs_v7_shard_desc_t *header_shard =
      &shards[(uint64_t)group_id * KAFS_V7_GROUP_LOCAL_SHARDS + 5u];
  if (local_segment >= le64toh(header_shard->logical_count))
    return -EINVAL;
  uint64_t block_off =
      le64toh(header_shard->physical_off) + local_segment * fixture->layout.block_size;
  kafs_v7_journal_header_t header;
  int rc = kafs_pread_all(fixture->fd, &header, sizeof(header),
                          (off_t)(block_off + KAFS_V7_JOURNAL_HEADER_BYTES));
  if (rc == 0)
  {
    header.generation = htole64(3u);
    rc = kafs_pwrite_all(fixture->fd, &header, sizeof(header),
                         (off_t)(block_off + 2u * KAFS_V7_JOURNAL_HEADER_BYTES));
  }
  return rc;
}

static int apply_targets(replay_image_fixture_t *fixture,
                         const replay_transaction_fixture_t *transaction, uint32_t count)
{
  if (count > 3u)
    return -EINVAL;
  int rc = 0;
  for (uint32_t i = 0; rc == 0 && i < count; ++i)
    rc = kafs_pwrite_all(fixture->fd, transaction->targets[i].after,
                         transaction->targets[i].bytes,
                         (off_t)transaction->targets[i].physical_off);
  return rc;
}

static int validate_path(const char *path, kafs_v7_layout_report_t *report_out)
{
  int fd = open(path, O_RDONLY);
  if (fd < 0)
    return -errno;
  kafs_ssuperblock_t sb;
  uint64_t file_size = 0;
  int rc = load_superblock_and_size(fd, &sb, &file_size);
  kafs_v7_layout_report_t report;
  memset(&report, 0, sizeof(report));
  if (rc == 0)
    rc = kafs_v7_validate_image_fd(fd, &sb, file_size, &report);
  close(fd);
  if (rc == 0 && report_out)
    *report_out = report;
  else if (rc == 0)
    kafs_v7_layout_report_clear(&report);
  return rc;
}

static int runtime_rejects_path(const char *path)
{
  int fd = open(path, O_RDONLY);
  if (fd < 0)
    return -errno;
  kafs_ssuperblock_t sb;
  uint64_t file_size = 0;
  int rc = load_superblock_and_size(fd, &sb, &file_size);
  kafs_context_t ctx;
  memset(&ctx, 0, sizeof(ctx));
  if (rc == 0)
    rc = kafs_v7_runtime_view_admit_fd(&ctx, fd, &sb, file_size);
  close(fd);
  return rc == -ENOTSUP ? 0 : -1;
}

static int test_crash_replay_states(void)
{
  for (uint32_t applied = 0; applied <= 3u; ++applied)
  {
    char path[64];
    int path_bytes =
        snprintf(path, sizeof(path), "v7-journal-applied-%" PRIu32 ".img", applied);
    if (path_bytes < 0 || (size_t)path_bytes >= sizeof(path))
      return -1;
    replay_image_fixture_t fixture;
    replay_transaction_fixture_t transaction;
    int fixture_rc = open_fixture(path, &fixture);
    int transaction_rc = fixture_rc == 0
                             ? build_transaction(&fixture, 0u, 1u, KAFS_V7_JOURNAL_COMMIT_TAG,
                                                 &transaction)
                             : fixture_rc;
    if (fixture_rc != 0 || transaction_rc != 0)
    {
      fprintf(stderr, "crash state setup failed: applied=%" PRIu32 " fixture=%d tx=%d\n",
              applied, fixture_rc, transaction_rc);
      return -1;
    }
    uint64_t checkpoint_blocks = fixture.layout.checkpoint_free_blocks;
    uint64_t checkpoint_inodes = fixture.layout.checkpoint_free_inodes;
    int64_t block_delta = transaction.targets[0].free_blocks_delta;
    int rc = publish_prefix(&fixture, 0u, 0u, transaction.bytes, transaction.byte_count, 1u, 1u);
    if (rc == 0 && applied == 0u)
      rc = write_torn_higher_header(&fixture, 0u, 0u);
    if (rc == 0)
      rc = apply_targets(&fixture, &transaction, applied);
    transaction_clear(&transaction);
    close_fixture(&fixture);
    kafs_v7_layout_report_t report;
    int validate_rc = rc == 0 ? validate_path(path, &report) : rc;
    if (rc != 0 || validate_rc != 0)
    {
      fprintf(stderr, "crash state validation failed: applied=%" PRIu32 " setup=%d validate=%d\n",
              applied, rc, validate_rc);
      return -1;
    }
    int valid = report.journal.selected_nonempty_segment_count == 1u &&
                report.journal.transaction_count == 1u &&
                report.journal.pending_transaction_count == 1u &&
                report.journal.committed_transaction_count == 1u &&
                report.journal.aborted_transaction_count == 0u &&
                report.journal.mutation_count == 3u &&
                report.journal.already_applied_mutation_count == applied &&
                report.journal.replay_mutation_count == 3u - applied &&
                block_delta == -64 && report.free_blocks + 64u == checkpoint_blocks &&
                report.free_inodes + 1u == checkpoint_inodes;
    int runtime_rc = runtime_rejects_path(path);
    if (!valid || runtime_rc != 0)
    {
      fprintf(stderr,
              "crash state report mismatch: applied=%" PRIu32
              " nonempty=%" PRIu32 " tx=%" PRIu32 " pending=%" PRIu32
              " commit=%" PRIu32 " mutations=%" PRIu32 " already=%" PRIu32
              " replay=%" PRIu32 " runtime=%d\n",
              applied, report.journal.selected_nonempty_segment_count,
              report.journal.transaction_count, report.journal.pending_transaction_count,
              report.journal.committed_transaction_count, report.journal.mutation_count,
              report.journal.already_applied_mutation_count,
              report.journal.replay_mutation_count, runtime_rc);
      kafs_v7_layout_report_clear(&report);
      return -1;
    }
    if (applied == 1u)
    {
      char *fsck_argv[] = {(char *)kafs_test_fsck_bin(), path, NULL};
      char command[2048];
      int written = snprintf(
          command, sizeof(command),
          "%s --json %s | python3 -c 'import json,sys; j=json.load(sys.stdin)[\"journal_segments\"]; "
          "assert j[\"status\"] == \"ok\" and j[\"pending_transaction_count\"] == 1 and "
          "j[\"already_applied_mutation_count\"] == 1 and j[\"replay_mutation_count\"] == 2'",
          kafs_test_kafsdump_bin(), path);
      if (run_command(fsck_argv, 0) != 0 || written < 0 || (size_t)written >= sizeof(command) ||
          system(command) != 0)
      {
        kafs_v7_layout_report_clear(&report);
        return -1;
      }
    }
    kafs_v7_layout_report_clear(&report);
  }
  return 0;
}

static int test_global_sequence_and_duplicates(void)
{
  replay_image_fixture_t fixture;
  replay_transaction_fixture_t commit;
  const char *duplicate_path = "v7-journal-duplicate.img";
  if (open_fixture(duplicate_path, &fixture) != 0 ||
      build_transaction(&fixture, 0u, 1u, KAFS_V7_JOURNAL_COMMIT_TAG, &commit) != 0)
    return -1;
  int rc = publish_prefix(&fixture, 0u, 0u, commit.bytes, commit.byte_count, 1u, 1u);
  if (rc == 0)
    rc = publish_prefix(&fixture, 0u, 1u, commit.bytes, commit.byte_count, 1u, 1u);
  transaction_clear(&commit);
  close_fixture(&fixture);
  kafs_v7_layout_report_t report;
  if (rc != 0 || validate_path(duplicate_path, &report) != 0)
    return -1;
  int valid = report.journal.selected_nonempty_segment_count == 2u &&
              report.journal.transaction_count == 1u &&
              report.journal.duplicate_transaction_count == 1u;
  kafs_v7_layout_report_clear(&report);
  if (!valid)
    return -1;

  const char *divergent_path = "v7-journal-divergent.img";
  replay_transaction_fixture_t abort;
  if (open_fixture(divergent_path, &fixture) != 0 ||
      build_transaction(&fixture, 0u, 1u, KAFS_V7_JOURNAL_COMMIT_TAG, &commit) != 0 ||
      build_transaction(&fixture, 0u, 1u, KAFS_V7_JOURNAL_ABORT_TAG, &abort) != 0)
    return -1;
  rc = publish_prefix(&fixture, 0u, 0u, commit.bytes, commit.byte_count, 1u, 1u);
  if (rc == 0)
    rc = publish_prefix(&fixture, 0u, 1u, abort.bytes, abort.byte_count, 1u, 1u);
  transaction_clear(&commit);
  transaction_clear(&abort);
  close_fixture(&fixture);
  if (rc != 0 || validate_path(divergent_path, NULL) == 0)
    return -1;

  const char *gap_path = "v7-journal-gap.img";
  if (open_fixture(gap_path, &fixture) != 0 ||
      build_transaction(&fixture, 0u, 2u, KAFS_V7_JOURNAL_COMMIT_TAG, &commit) != 0)
    return -1;
  rc = publish_prefix(&fixture, 0u, 0u, commit.bytes, commit.byte_count, 2u, 2u);
  transaction_clear(&commit);
  close_fixture(&fixture);
  if (rc != 0 || validate_path(gap_path, NULL) == 0)
    return -1;

  const char *multi_path = "v7-journal-multi-group.img";
  replay_transaction_fixture_t second;
  if (open_fixture_with_groups(multi_path, 2u, &fixture) != 0 ||
      build_transaction(&fixture, 0u, 1u, KAFS_V7_JOURNAL_COMMIT_TAG, &commit) != 0 ||
      build_transaction(&fixture, 1u, 2u, KAFS_V7_JOURNAL_COMMIT_TAG, &second) != 0)
    return -1;
  uint64_t checkpoint_blocks = fixture.layout.checkpoint_free_blocks;
  uint64_t checkpoint_inodes = fixture.layout.checkpoint_free_inodes;
  rc = publish_prefix(&fixture, 0u, 0u, commit.bytes, commit.byte_count, 1u, 1u);
  if (rc == 0)
    rc = publish_prefix(&fixture, 1u, 0u, second.bytes, second.byte_count, 2u, 2u);
  transaction_clear(&commit);
  transaction_clear(&second);
  close_fixture(&fixture);
  if (rc != 0 || validate_path(multi_path, &report) != 0)
    return -1;
  valid = report.journal.selected_nonempty_segment_count == 2u &&
          report.journal.first_sequence == 1u && report.journal.last_sequence == 2u &&
          report.journal.last_sequence_group_id == 1u &&
          report.journal.transaction_count == 2u &&
          report.journal.pending_transaction_count == 2u &&
          report.journal.committed_transaction_count == 2u &&
          report.journal.mutation_count == 6u &&
          report.journal.already_applied_mutation_count == 0u &&
          report.journal.replay_mutation_count == 6u &&
          report.free_blocks + 128u == checkpoint_blocks &&
          report.free_inodes + 2u == checkpoint_inodes;
  kafs_v7_layout_report_clear(&report);
  return valid ? 0 : -1;
}

static int test_abort_torn_and_corruption(void)
{
  replay_image_fixture_t fixture;
  replay_transaction_fixture_t transaction;
  const char *abort_path = "v7-journal-abort.img";
  if (open_fixture(abort_path, &fixture) != 0 ||
      build_transaction(&fixture, 0u, 1u, KAFS_V7_JOURNAL_ABORT_TAG, &transaction) != 0)
    return -1;
  uint64_t checkpoint_blocks = fixture.layout.checkpoint_free_blocks;
  uint64_t checkpoint_inodes = fixture.layout.checkpoint_free_inodes;
  int rc =
      publish_prefix(&fixture, 0u, 0u, transaction.bytes, transaction.byte_count, 1u, 1u);
  transaction_clear(&transaction);
  close_fixture(&fixture);
  kafs_v7_layout_report_t report;
  if (rc != 0 || validate_path(abort_path, &report) != 0)
    return -1;
  int valid = report.journal.pending_transaction_count == 1u &&
              report.journal.committed_transaction_count == 0u &&
              report.journal.aborted_transaction_count == 1u &&
              report.journal.mutation_count == 0u && report.free_blocks == checkpoint_blocks &&
              report.free_inodes == checkpoint_inodes;
  kafs_v7_layout_report_clear(&report);
  if (!valid)
    return -1;

  const char *abort_applied_path = "v7-journal-abort-applied.img";
  if (open_fixture(abort_applied_path, &fixture) != 0 ||
      build_transaction(&fixture, 0u, 1u, KAFS_V7_JOURNAL_ABORT_TAG, &transaction) != 0)
    return -1;
  rc = publish_prefix(&fixture, 0u, 0u, transaction.bytes, transaction.byte_count, 1u, 1u);
  if (rc == 0)
    rc = apply_targets(&fixture, &transaction, 1u);
  transaction_clear(&transaction);
  close_fixture(&fixture);
  if (rc != 0 || validate_path(abort_applied_path, NULL) == 0)
    return -1;

  const char *torn_path = "v7-journal-torn-prefix.img";
  if (open_fixture(torn_path, &fixture) != 0 ||
      build_transaction(&fixture, 0u, 1u, KAFS_V7_JOURNAL_COMMIT_TAG, &transaction) != 0)
    return -1;
  size_t torn_bytes = transaction.byte_count - 56u;
  rc = publish_prefix(&fixture, 0u, 0u, transaction.bytes, torn_bytes, 1u, 1u);
  transaction_clear(&transaction);
  close_fixture(&fixture);
  if (rc != 0 || validate_path(torn_path, NULL) == 0)
    return -1;

  const char *crc_path = "v7-journal-record-crc.img";
  if (open_fixture(crc_path, &fixture) != 0 ||
      build_transaction(&fixture, 0u, 1u, KAFS_V7_JOURNAL_COMMIT_TAG, &transaction) != 0)
    return -1;
  rc = publish_prefix(&fixture, 0u, 0u, transaction.bytes, transaction.byte_count, 1u, 1u);
  const kafs_v7_shard_desc_t *shards = kafs_v7_report_shards(&fixture.layout);
  uint8_t byte;
  uint64_t corrupt_off = le64toh(shards[6].physical_off) + KAFS_V7_JOURNAL_RECORD_HEADER_BYTES;
  if (rc == 0)
    rc = kafs_pread_all(fixture.fd, &byte, sizeof(byte), (off_t)corrupt_off);
  if (rc == 0)
  {
    byte ^= 0x5au;
    rc = kafs_pwrite_all(fixture.fd, &byte, sizeof(byte), (off_t)corrupt_off);
  }
  transaction_clear(&transaction);
  close_fixture(&fixture);
  return rc == 0 && validate_path(crc_path, NULL) != 0 ? 0 : -1;
}

static int test_sequence_publication_state(void)
{
  const char *path = "v7-sequence-publication.img";
  replay_image_fixture_t fixture;
  if (open_fixture_with_groups(path, 2u, &fixture) != 0)
    return -1;
  kafs_v7_lock_state_t *locks = NULL;
  kafs_v7_sequence_state_t *state = NULL;
  kafs_v7_sequence_reservation_t reservation;
  memset(&reservation, 0, sizeof(reservation));
  replay_transaction_fixture_t first;
  replay_transaction_fixture_t second;
  memset(&first, 0, sizeof(first));
  memset(&second, 0, sizeof(second));

  int rc = kafs_v7_locks_init(fixture.layout.group_count, 50u, &locks);
  if (rc == 0)
    rc = kafs_v7_sequence_state_init(locks, &fixture.layout, &state);
  kafs_v7_sequence_reservation_t unchanged = {
      .sequence = UINT64_C(0xaaaaaaaaaaaaaaaa),
      .token = UINT64_C(0xbbbbbbbbbbbbbbbb),
      .group_id = UINT32_MAX,
      .active = 0x5au,
  };
  if (rc == 0 &&
      (kafs_v7_sequence_reserve(state, fixture.layout.group_count, &unchanged) != -EINVAL ||
       unchanged.sequence != UINT64_C(0xaaaaaaaaaaaaaaaa) ||
       unchanged.token != UINT64_C(0xbbbbbbbbbbbbbbbb) || unchanged.group_id != UINT32_MAX ||
       unchanged.active != 0x5au))
    rc = -1;

  kafs_v7_layout_report_t overflow_layout = fixture.layout;
  overflow_layout.checkpoint_sequence = UINT64_MAX;
  kafs_v7_sequence_state_t *overflow_state = NULL;
  if (rc == 0)
    rc = kafs_v7_sequence_state_init(locks, &overflow_layout, &overflow_state);
  if (rc == 0 && kafs_v7_sequence_reserve(overflow_state, 0u, &unchanged) != -EOVERFLOW)
    rc = -1;
  kafs_v7_sequence_state_destroy(overflow_state);

  if (rc == 0)
    rc = kafs_v7_sequence_reserve(state, 0u, &reservation);
  if (rc == 0 && reservation.sequence != 1u)
    rc = -1;
  sequence_reserve_worker_t worker = {.state = state, .group_id = 1u, .rc = 0};
  pthread_t thread;
  int thread_started = 0;
  if (rc == 0)
  {
    int thread_rc = pthread_create(&thread, NULL, reserve_sequence_worker, &worker);
    if (thread_rc != 0)
      rc = -thread_rc;
    else
      thread_started = 1;
  }
  if (thread_started && (pthread_join(thread, NULL) != 0 || worker.rc != -ETIMEDOUT))
    rc = -1;
  if (reservation.active)
  {
    int cancel_rc = kafs_v7_sequence_cancel_reservation_fd(
        state, &reservation, fixture.fd, &fixture.sb, fixture.file_size);
    if (cancel_rc != 0)
      rc = cancel_rc;
  }

  if (rc == 0)
    rc = kafs_v7_sequence_reserve(state, 0u, &reservation);
  if (rc == 0 && reservation.sequence != 1u)
    rc = -1;
  if (rc == 0)
    rc = build_transaction(&fixture, 0u, reservation.sequence, KAFS_V7_JOURNAL_COMMIT_TAG, &first);
  if (rc == 0)
    rc = publish_prefix(&fixture, 0u, 0u, first.bytes, first.byte_count, reservation.sequence,
                        reservation.sequence);
  if (rc == 0)
    rc = kafs_v7_sequence_confirm_publication_fd(state, &reservation, fixture.fd, &fixture.sb,
                                                  fixture.file_size);

  if (rc == 0)
    rc = kafs_v7_sequence_reserve(state, 1u, &reservation);
  if (rc == 0 && reservation.sequence != 2u)
    rc = -1;
  if (rc == 0)
    rc =
        build_transaction(&fixture, 1u, reservation.sequence, KAFS_V7_JOURNAL_COMMIT_TAG, &second);
  if (rc == 0)
    rc = publish_prefix(&fixture, 1u, 0u, second.bytes, second.byte_count, reservation.sequence,
                        reservation.sequence);
  if (rc == 0)
    rc = kafs_v7_sequence_confirm_publication_fd(state, &reservation, fixture.fd, &fixture.sb,
                                                  fixture.file_size);

  if (reservation.active)
  {
    int cancel_rc = kafs_v7_sequence_cancel_reservation_fd(
        state, &reservation, fixture.fd, &fixture.sb, fixture.file_size);
    if (rc == 0)
      rc = cancel_rc;
  }
  transaction_clear(&first);
  transaction_clear(&second);
  kafs_v7_sequence_state_destroy(state);
  state = NULL;
  kafs_v7_locks_destroy(locks);
  locks = NULL;

  if (rc == 0)
  {
    kafs_v7_layout_report_clear(&fixture.layout);
    rc = kafs_v7_validate_image_fd(fixture.fd, &fixture.sb, fixture.file_size, &fixture.layout);
  }
  if (rc == 0)
    rc = kafs_v7_locks_init(fixture.layout.group_count, 50u, &locks);
  if (rc == 0)
    rc = kafs_v7_sequence_state_init(locks, &fixture.layout, &state);
  if (rc == 0)
    rc = kafs_v7_sequence_reserve(state, 0u, &reservation);
  if (rc == 0 && reservation.sequence != 3u)
    rc = -1;
  if (reservation.active)
  {
    int cancel_rc = kafs_v7_sequence_cancel_reservation_fd(
        state, &reservation, fixture.fd, &fixture.sb, fixture.file_size);
    if (rc == 0)
      rc = cancel_rc;
  }
  kafs_v7_sequence_state_destroy(state);
  kafs_v7_locks_destroy(locks);
  close_fixture(&fixture);
  return rc;
}

static int test_sequence_confirmation_poison(void)
{
  const char *path = "v7-sequence-poison.img";
  replay_image_fixture_t fixture;
  if (open_fixture_with_groups(path, 2u, &fixture) != 0)
    return -1;
  kafs_v7_lock_state_t *locks = NULL;
  kafs_v7_sequence_state_t *state = NULL;
  kafs_v7_sequence_reservation_t reservation;
  memset(&reservation, 0, sizeof(reservation));
  replay_transaction_fixture_t transaction;
  memset(&transaction, 0, sizeof(transaction));
  int rc = kafs_v7_locks_init(fixture.layout.group_count, 50u, &locks);
  if (rc == 0)
    rc = kafs_v7_sequence_state_init(locks, &fixture.layout, &state);
  if (rc == 0)
    rc = kafs_v7_sequence_reserve(state, 0u, &reservation);
  if (rc == 0)
    rc = build_transaction(&fixture, 1u, reservation.sequence, KAFS_V7_JOURNAL_COMMIT_TAG,
                           &transaction);
  if (rc == 0)
    rc = publish_prefix(&fixture, 1u, 0u, transaction.bytes, transaction.byte_count,
                        reservation.sequence, reservation.sequence);
  int mismatch_confirmed = 0;
  if (rc == 0)
  {
    int confirm_rc = kafs_v7_sequence_confirm_publication_fd(
        state, &reservation, fixture.fd, &fixture.sb, fixture.file_size);
    mismatch_confirmed = confirm_rc == -EUCLEAN;
    if (!mismatch_confirmed)
      rc = -1;
  }
  if (reservation.active)
  {
    int cancel_rc = kafs_v7_sequence_cancel_reservation_fd(
        state, &reservation, fixture.fd, &fixture.sb, fixture.file_size);
    if (rc == 0)
      rc = cancel_rc;
  }
  if (rc == 0 && mismatch_confirmed &&
      kafs_v7_sequence_reserve(state, 0u, &reservation) != -EUCLEAN)
    rc = -1;
  transaction_clear(&transaction);
  kafs_v7_sequence_state_destroy(state);
  kafs_v7_locks_destroy(locks);
  close_fixture(&fixture);
  return rc;
}

static int test_third_target_state(void)
{
  const char *path = "v7-journal-third-state.img";
  replay_image_fixture_t fixture;
  replay_transaction_fixture_t transaction;
  if (open_fixture(path, &fixture) != 0 ||
      build_transaction(&fixture, 0u, 1u, KAFS_V7_JOURNAL_COMMIT_TAG, &transaction) != 0)
    return -1;
  int rc =
      publish_prefix(&fixture, 0u, 0u, transaction.bytes, transaction.byte_count, 1u, 1u);
  uint8_t third_state = 0x5au;
  if (rc == 0)
    rc = kafs_pwrite_all(fixture.fd, &third_state, sizeof(third_state),
                         (off_t)(transaction.targets[2].physical_off + 8u));
  transaction_clear(&transaction);
  close_fixture(&fixture);
  return rc == 0 && validate_path(path, NULL) != 0 ? 0 : -1;
}

int main(void)
{
  if (kafs_test_enter_tmpdir("v7-journal-replay") != 0)
    return 1;
  if (test_crash_replay_states() != 0)
  {
    fprintf(stderr, "v7 journal crash replay states failed\n");
    return 1;
  }
  if (test_global_sequence_and_duplicates() != 0)
  {
    fprintf(stderr, "v7 journal global sequence/duplicate matrix failed\n");
    return 1;
  }
  if (test_abort_torn_and_corruption() != 0)
  {
    fprintf(stderr, "v7 journal abort/torn/corruption matrix failed\n");
    return 1;
  }
  if (test_third_target_state() != 0)
  {
    fprintf(stderr, "v7 journal third target state was not rejected\n");
    return 1;
  }
  if (test_sequence_publication_state() != 0)
  {
    fprintf(stderr, "v7 global sequence publication state failed\n");
    return 1;
  }
  if (test_sequence_confirmation_poison() != 0)
  {
    fprintf(stderr, "v7 sequence confirmation poison gate failed\n");
    return 1;
  }
  return 0;
}
