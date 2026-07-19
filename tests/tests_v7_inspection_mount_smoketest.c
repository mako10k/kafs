#include "test_utils.h"

#include "kafs_offline_summary.h"
#include "kafs_superblock.h"
#include "kafs_v7_journal_writer.h"
#include "kafs_v7_layout.h"
#include "kafs_v7_locks.h"
#include "kafs_v7_mutation.h"
#include "kafs_v7_sequence.h"

#include <dirent.h>
#include <endian.h>
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/statvfs.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <sys/xattr.h>
#include <time.h>
#include <unistd.h>

typedef struct v7_fixture
{
  uint32_t nested_ino;
  uint32_t inline_ino;
  uint32_t block_ino;
  uint32_t symlink_ino;
  uint64_t free_blocks;
  uint64_t free_inodes;
  uint64_t total_blocks;
  uint32_t block_size;
} v7_fixture_t;

static const char k_inline_payload[] = "v7 inline payload\n";
static const char k_block_payload[] =
    "v7 block-backed payload across a nonzero data group; "
    "the second sentence keeps this payload beyond the sixty-byte inline boundary.\n";
static const char k_symlink_target[] = "nested/inline";

static int run_command(char *const argv[], int expected_exit, char *output, size_t output_size)
{
  int pipefd[2];
  if (pipe(pipefd) != 0)
    return -errno;
  pid_t pid = fork();
  if (pid < 0)
  {
    int saved = errno;
    close(pipefd[0]);
    close(pipefd[1]);
    return -saved;
  }
  if (pid == 0)
  {
    close(pipefd[0]);
    dup2(pipefd[1], STDOUT_FILENO);
    dup2(pipefd[1], STDERR_FILENO);
    close(pipefd[1]);
    execvp(argv[0], argv);
    _exit(127);
  }

  close(pipefd[1]);
  size_t used = 0;
  for (;;)
  {
    char buf[512];
    ssize_t n = read(pipefd[0], buf, sizeof(buf));
    if (n < 0)
    {
      if (errno == EINTR)
        continue;
      close(pipefd[0]);
      return -errno;
    }
    if (n == 0)
      break;
    if (output && output_size > 0u && used < output_size - 1u)
    {
      size_t copy = (size_t)n;
      if (copy > output_size - 1u - used)
        copy = output_size - 1u - used;
      memcpy(output + used, buf, copy);
      used += copy;
    }
  }
  close(pipefd[0]);
  if (output && output_size > 0u)
    output[used] = '\0';

  int status = 0;
  if (waitpid(pid, &status, 0) != pid || !WIFEXITED(status))
    return -1;
  return WEXITSTATUS(status) == expected_exit ? 0 : -1;
}

static int format_image(const char *path)
{
  char output[4096];
  char *argv[] = {(char *)kafs_test_mkfs_bin(),
                  (char *)path,
                  (char *)"--format-version",
                  (char *)"7",
                  (char *)"--size-bytes",
                  (char *)"128M",
                  (char *)"--v7-group-count",
                  (char *)"4",
                  (char *)"--yes",
                  NULL};
  return run_command(argv, 0, output, sizeof(output));
}

static int validate_image_fd(int fd, kafs_ssuperblock_t *sb, uint64_t *file_size,
                             kafs_v7_layout_report_t *report)
{
  int rc = kafs_pread_all(fd, sb, sizeof(*sb), 0);
  if (rc == 0)
    rc = kafs_offline_detect_file_size(fd, file_size);
  if (rc == 0)
    rc = kafs_v7_validate_image_fd(fd, sb, *file_size, report);
  return rc;
}

static uint32_t name_hash(const char *name, size_t name_bytes)
{
  uint32_t hash = 2166136261u;
  for (size_t i = 0; i < name_bytes; ++i)
  {
    hash ^= (uint8_t)name[i];
    hash *= 16777619u;
  }
  return hash;
}

static int append_dir_record(uint8_t *buf, size_t capacity, size_t *used, uint32_t ino,
                             const char *name)
{
  size_t name_bytes = strlen(name);
  size_t record_bytes = KAFS_V7_KDIR_RECORD_PREFIX_BYTES + name_bytes;
  if (!buf || !used || name_bytes == 0u || name_bytes > 255u || record_bytes > UINT16_MAX ||
      *used > capacity || record_bytes > capacity - *used)
    return -EINVAL;

  kafs_v7_kdir_record_t *record = (kafs_v7_kdir_record_t *)(buf + *used);
  memset(record, 0, record_bytes);
  record->record_length = htole16((uint16_t)record_bytes);
  record->inode = htole32(ino);
  record->name_bytes = htole16((uint16_t)name_bytes);
  record->name_hash = htole32(name_hash(name, name_bytes));
  memcpy(record->name, name, name_bytes);
  *used += record_bytes;
  return 0;
}

static int build_directory(uint8_t *buf, size_t capacity, const uint32_t *inos,
                           const char *const *names, size_t count, size_t *payload_bytes)
{
  if (!buf || capacity < KAFS_V7_KDIR_HEADER_BYTES || !payload_bytes)
    return -EINVAL;
  memset(buf, 0, capacity);
  size_t used = KAFS_V7_KDIR_HEADER_BYTES;
  for (size_t i = 0; i < count; ++i)
  {
    int rc = append_dir_record(buf, capacity, &used, inos[i], names[i]);
    if (rc != 0)
      return rc;
  }
  kafs_v7_kdir_header_t *header = (kafs_v7_kdir_header_t *)buf;
  header->magic = htole32(KAFS_V7_KDIR_MAGIC);
  header->version = htole16(KAFS_V7_KDIR_VERSION);
  header->live_count = htole32((uint32_t)count);
  header->record_bytes = htole32((uint32_t)(used - KAFS_V7_KDIR_HEADER_BYTES));
  *payload_bytes = used;
  return 0;
}

static void inode_init(kafs_v7_inode_t *inode, uint16_t mode, uint64_t size, uint16_t links,
                       uint32_t blocks)
{
  memset(inode, 0, sizeof(*inode));
  inode->mode = htole16(mode);
  inode->uid = htole16((uint16_t)getuid());
  inode->gid = htole16((uint16_t)getgid());
  inode->size = htole64(size);
  inode->link_count = htole16(links);
  inode->blocks = htole32(blocks);
}

static const kafs_v7_shard_desc_t *inode_shard_for_group(
    const kafs_v7_layout_report_t *report, uint32_t group_id)
{
  const kafs_v7_group_desc_t *groups = kafs_v7_report_groups(report);
  const kafs_v7_shard_desc_t *shards = kafs_v7_report_shards(report);
  uint32_t first = le32toh(groups[group_id].first_shard_index);
  uint32_t count = le32toh(groups[group_id].shard_count);
  for (uint32_t i = 0; i < count; ++i)
    if (le16toh(shards[first + i].type) == KAFS_V7_SHARD_INODE_TABLE)
      return &shards[first + i];
  return NULL;
}

static uint32_t inode_in_group(const kafs_v7_layout_report_t *report, uint32_t group_id,
                               uint32_t ordinal)
{
  const kafs_v7_shard_desc_t *shard = inode_shard_for_group(report, group_id);
  if (!shard)
    return 0u;
  uint64_t start = le64toh(shard->logical_start);
  uint64_t count = le64toh(shard->logical_count);
  uint64_t candidate = start + ordinal;
  if (candidate < 2u)
    candidate = 2u + ordinal;
  if (candidate < start || candidate - start >= count || candidate > UINT32_MAX)
    return 0u;
  return (uint32_t)candidate;
}

static int write_inode(int fd, const kafs_v7_layout_report_t *report, uint32_t ino,
                       const kafs_v7_inode_t *inode)
{
  const kafs_v7_shard_desc_t *shards = kafs_v7_report_shards(report);
  for (uint32_t i = 0; i < report->shard_count; ++i)
  {
    if (le16toh(shards[i].type) != KAFS_V7_SHARD_INODE_TABLE)
      continue;
    uint64_t start = le64toh(shards[i].logical_start);
    uint64_t count = le64toh(shards[i].logical_count);
    if ((uint64_t)ino < start || (uint64_t)ino - start >= count)
      continue;
    uint64_t off = le64toh(shards[i].physical_off) + ((uint64_t)ino - start) * sizeof(*inode);
    return kafs_pwrite_all(fd, inode, sizeof(*inode), (off_t)off);
  }
  return -ENOENT;
}

static int read_inode(int fd, const kafs_v7_layout_report_t *report, uint32_t ino,
                      kafs_v7_inode_t *inode)
{
  const kafs_v7_shard_desc_t *shards = kafs_v7_report_shards(report);
  for (uint32_t i = 0; i < report->shard_count; ++i)
  {
    if (le16toh(shards[i].type) != KAFS_V7_SHARD_INODE_TABLE)
      continue;
    uint64_t start = le64toh(shards[i].logical_start);
    uint64_t count = le64toh(shards[i].logical_count);
    if ((uint64_t)ino < start || (uint64_t)ino - start >= count)
      continue;
    uint64_t off = le64toh(shards[i].physical_off) + ((uint64_t)ino - start) * sizeof(*inode);
    return kafs_pread_all(fd, inode, sizeof(*inode), (off_t)off);
  }
  return -ENOENT;
}

static int write_data_block(int fd, const kafs_v7_group_desc_t *group, uint64_t logical,
                            const void *buf, uint32_t block_size)
{
  uint64_t start = le64toh(group->data_logical_start);
  uint64_t count = le64toh(group->data_logical_count);
  if (logical < start || logical - start >= count)
    return -ERANGE;
  uint64_t off = le64toh(group->data_physical_off) + (logical - start) * block_size;
  return kafs_pwrite_all(fd, buf, block_size, (off_t)off);
}

static int allocate_group_block(int fd, const kafs_v7_layout_report_t *report, uint32_t group_id,
                                uint64_t logical)
{
  const kafs_v7_group_desc_t *groups = kafs_v7_report_groups(report);
  const kafs_v7_shard_desc_t *shards = kafs_v7_report_shards(report);
  uint32_t first = le32toh(groups[group_id].first_shard_index);
  const kafs_v7_shard_desc_t *bitmap_shard = &shards[first];
  const kafs_v7_shard_desc_t *allocator_shard = &shards[first + 2u];
  uint64_t start = le64toh(bitmap_shard->logical_start);
  uint64_t blocks = le64toh(bitmap_shard->logical_count);
  uint64_t bitmap_bytes = le64toh(bitmap_shard->physical_bytes);
  uint64_t allocator_bytes = le64toh(allocator_shard->physical_bytes);
  if (logical < start || logical - start >= blocks || bitmap_bytes > SIZE_MAX ||
      allocator_bytes > SIZE_MAX)
    return -ERANGE;

  uint8_t *bitmap = malloc((size_t)bitmap_bytes);
  uint8_t *allocator = calloc(1u, (size_t)allocator_bytes);
  if (!bitmap || !allocator)
  {
    free(bitmap);
    free(allocator);
    return -ENOMEM;
  }
  int rc = kafs_pread_all(fd, bitmap, (size_t)bitmap_bytes,
                          (off_t)le64toh(bitmap_shard->physical_off));
  uint64_t local = logical - start;
  if (rc == 0 && (bitmap[local / 8u] & (uint8_t)(1u << (local % 8u))) != 0)
    rc = -EEXIST;
  if (rc == 0)
    bitmap[local / 8u] |= (uint8_t)(1u << (local % 8u));

  uint64_t l0_bytes = (blocks + 7u) / 8u;
  uint64_t l1_bytes = (l0_bytes + 7u) / 8u;
  if (rc == 0)
  {
    for (uint64_t i = 0; i < l0_bytes; ++i)
    {
      uint8_t valid_mask = 0xffu;
      if (i + 1u == l0_bytes && (blocks & 7u) != 0u)
        valid_mask = (uint8_t)((1u << (blocks & 7u)) - 1u);
      if ((bitmap[i] & valid_mask) != valid_mask)
        allocator[i / 8u] |= (uint8_t)(1u << (i % 8u));
    }
    for (uint64_t i = 0; i < l1_bytes; ++i)
      if (allocator[i] != 0u)
        allocator[l1_bytes + i / 8u] |= (uint8_t)(1u << (i % 8u));
    rc = kafs_pwrite_all(fd, bitmap, (size_t)bitmap_bytes,
                         (off_t)le64toh(bitmap_shard->physical_off));
    if (rc == 0)
      rc = kafs_pwrite_all(fd, allocator, (size_t)allocator_bytes,
                           (off_t)le64toh(allocator_shard->physical_off));
  }
  free(bitmap);
  free(allocator);
  return rc;
}

static int update_checkpoints(int fd, const kafs_v7_layout_report_t *report, uint64_t free_blocks,
                              uint64_t free_inodes)
{
  for (uint32_t i = 0; i < report->replica_count; ++i)
  {
    kafs_v7_checkpoint_t checkpoint;
    int rc = kafs_pread_all(fd, &checkpoint, sizeof(checkpoint),
                            (off_t)report->checkpoints[i].offset);
    if (rc != 0)
      return rc;
    checkpoint.free_blocks = htole64(free_blocks);
    checkpoint.free_inodes = htole64(free_inodes);
    checkpoint.crc32 = 0u;
    checkpoint.crc32 = htole32(kafs_v7_crc32(&checkpoint, sizeof(checkpoint)));
    rc = kafs_pwrite_all(fd, &checkpoint, sizeof(checkpoint),
                         (off_t)report->checkpoints[i].offset);
    if (rc != 0)
      return rc;
  }
  return fdatasync(fd) == 0 ? 0 : -errno;
}

static int seed_fixture(const char *path, v7_fixture_t *fixture)
{
  int fd = open(path, O_RDWR);
  if (fd < 0)
    return -errno;
  kafs_ssuperblock_t sb;
  uint64_t file_size = 0;
  kafs_v7_layout_report_t report = {0};
  int rc = validate_image_fd(fd, &sb, &file_size, &report);
  if (rc != 0)
  {
    close(fd);
    return rc;
  }
  if (report.group_count != 4u || report.free_blocks < 2u || report.free_inodes < 4u)
  {
    kafs_v7_layout_report_clear(&report);
    close(fd);
    return -EINVAL;
  }

  fixture->nested_ino = inode_in_group(&report, 1u, 0u);
  fixture->symlink_ino = inode_in_group(&report, 1u, 1u);
  fixture->inline_ino = inode_in_group(&report, 2u, 0u);
  fixture->block_ino = inode_in_group(&report, 3u, 0u);
  if (!fixture->nested_ino || !fixture->symlink_ino || !fixture->inline_ino ||
      !fixture->block_ino || fixture->nested_ino == fixture->symlink_ino)
    rc = -EINVAL;

  const kafs_v7_group_desc_t *groups = kafs_v7_report_groups(&report);
  uint64_t root_block = le64toh(groups[0].data_logical_start);
  uint64_t file_block = le64toh(groups[3].data_logical_start);
  uint32_t block_size = report.block_size;
  fixture->block_size = block_size;
  uint8_t *root_data = calloc(1u, block_size);
  uint8_t *file_data = calloc(1u, block_size);
  if (!root_data || !file_data)
    rc = -ENOMEM;

  size_t root_bytes = 0;
  size_t nested_bytes = 0;
  uint8_t nested_data[KAFS_V7_INODE_BYTES] = {0};
  const uint32_t root_inos[] = {fixture->nested_ino, fixture->block_ino, fixture->symlink_ino};
  const char *const root_names[] = {"nested", "block", "link"};
  const uint32_t nested_inos[] = {KAFS_INO_ROOTDIR, fixture->inline_ino};
  const char *const nested_names[] = {"..", "inline"};
  if (rc == 0)
    rc = build_directory(root_data, block_size, root_inos, root_names, 3u, &root_bytes);
  if (rc == 0)
    rc = build_directory(nested_data, KAFS_INODE_DIRECT_BYTES, nested_inos, nested_names, 2u,
                         &nested_bytes);
  if (rc == 0 && (root_bytes <= KAFS_INODE_DIRECT_BYTES ||
                  nested_bytes > KAFS_INODE_DIRECT_BYTES ||
                  strlen(k_block_payload) <= KAFS_INODE_DIRECT_BYTES))
    rc = -EINVAL;
  if (rc == 0)
    memcpy(file_data, k_block_payload, strlen(k_block_payload));

  kafs_v7_inode_t root_inode;
  kafs_v7_inode_t nested_inode;
  kafs_v7_inode_t inline_inode;
  kafs_v7_inode_t block_inode;
  kafs_v7_inode_t symlink_inode;
  if (rc == 0)
  {
    inode_init(&root_inode, (uint16_t)(S_IFDIR | 0777), root_bytes, 3u, 1u);
    uint32_t root_ref = htole32((uint32_t)root_block + 1u);
    memcpy(root_inode.inline_or_block_refs, &root_ref, sizeof(root_ref));
    inode_init(&nested_inode, (uint16_t)(S_IFDIR | 0777), nested_bytes, 2u, 0u);
    memcpy(nested_inode.inline_or_block_refs, nested_data, nested_bytes);
    inode_init(&inline_inode, (uint16_t)(S_IFREG | 0666), strlen(k_inline_payload), 1u, 0u);
    memcpy(inline_inode.inline_or_block_refs, k_inline_payload, strlen(k_inline_payload));
    inode_init(&block_inode, (uint16_t)(S_IFREG | 0666), block_size, 1u, 1u);
    uint32_t file_ref = htole32((uint32_t)file_block + 1u);
    memcpy(block_inode.inline_or_block_refs, &file_ref, sizeof(file_ref));
    inode_init(&symlink_inode, (uint16_t)(S_IFLNK | 0777), strlen(k_symlink_target), 1u, 0u);
    memcpy(symlink_inode.inline_or_block_refs, k_symlink_target, strlen(k_symlink_target));
  }

  if (rc == 0)
    rc = write_data_block(fd, &groups[0], root_block, root_data, block_size);
  if (rc == 0)
    rc = write_data_block(fd, &groups[3], file_block, file_data, block_size);
  if (rc == 0)
    rc = allocate_group_block(fd, &report, 0u, root_block);
  if (rc == 0)
    rc = allocate_group_block(fd, &report, 3u, file_block);
  if (rc == 0)
    rc = write_inode(fd, &report, KAFS_INO_ROOTDIR, &root_inode);
  if (rc == 0)
    rc = write_inode(fd, &report, fixture->nested_ino, &nested_inode);
  if (rc == 0)
    rc = write_inode(fd, &report, fixture->inline_ino, &inline_inode);
  if (rc == 0)
    rc = write_inode(fd, &report, fixture->block_ino, &block_inode);
  if (rc == 0)
    rc = write_inode(fd, &report, fixture->symlink_ino, &symlink_inode);

  fixture->free_blocks = report.free_blocks - 2u;
  fixture->free_inodes = report.free_inodes - 4u;
  fixture->total_blocks = kafs_sb_r_blkcnt_get(&sb);
  if (rc == 0)
    rc = update_checkpoints(fd, &report, fixture->free_blocks, fixture->free_inodes);
  free(root_data);
  free(file_data);
  kafs_v7_layout_report_clear(&report);
  close(fd);
  if (rc != 0)
    return rc;

  fd = open(path, O_RDONLY);
  if (fd < 0)
    return -errno;
  kafs_v7_layout_report_t verify = {0};
  rc = validate_image_fd(fd, &sb, &file_size, &verify);
  if (rc == 0 && (verify.free_blocks != fixture->free_blocks ||
                  verify.free_inodes != fixture->free_inodes))
    rc = -EINVAL;
  kafs_v7_layout_report_clear(&verify);
  close(fd);
  return rc;
}

static int publish_pending_uid(int fd, const kafs_ssuperblock_t *sb, uint64_t file_size,
                               kafs_v7_layout_report_t *report,
                               kafs_v7_sequence_state_t *sequence, uint32_t ino, uint16_t uid)
{
  kafs_v7_mutation_route_t route;
  int rc = kafs_v7_mutation_route_target(report, KAFS_V7_JOURNAL_TARGET_INODE, ino, &route);
  kafs_v7_sequence_reservation_t reservation;
  memset(&reservation, 0, sizeof(reservation));
  if (rc == 0)
    rc = kafs_v7_sequence_reserve(sequence, route.group_id, &reservation);
  uint16_t wire_uid = htole16(uid);
  kafs_v7_journal_patch_t patch = {
      .target_type = KAFS_V7_JOURNAL_TARGET_INODE,
      .logical_index = ino,
      .patch_off = offsetof(kafs_v7_inode_t, uid),
      .patch_bytes = sizeof(wire_uid),
      .patch = &wire_uid,
  };
  kafs_v7_journal_transaction_t *transaction = NULL;
  if (rc == 0)
    rc = kafs_v7_journal_transaction_encode_fd(fd, report, &reservation, &patch, 1u,
                                                KAFS_V7_JOURNAL_COMMIT_TAG, &transaction);
  kafs_v7_journal_publication_t publication;
  if (rc == 0)
    rc = kafs_v7_journal_transaction_publish_fd(fd, report, &reservation, transaction,
                                                 &publication);
  if (rc == 0)
    rc = kafs_v7_sequence_confirm_publication_fd(sequence, &reservation, fd, sb, file_size);
  else if (reservation.active)
    (void)kafs_v7_sequence_cancel_reservation_fd(sequence, &reservation, fd, sb, file_size);
  kafs_v7_journal_transaction_destroy(transaction);
  if (rc == 0)
  {
    kafs_v7_layout_report_clear(report);
    memset(report, 0, sizeof(*report));
    rc = kafs_v7_validate_image_fd(fd, sb, file_size, report);
  }
  return rc;
}

static int seed_pending_journals(const char *path, uint32_t first_ino, uint32_t second_ino)
{
  int fd = open(path, O_RDWR);
  kafs_ssuperblock_t sb;
  uint64_t file_size = 0u;
  kafs_v7_layout_report_t report = {0};
  int rc = fd < 0 ? -errno : validate_image_fd(fd, &sb, &file_size, &report);
  kafs_v7_lock_state_t *locks = NULL;
  kafs_v7_sequence_state_t *sequence = NULL;
  if (rc == 0)
    rc = kafs_v7_locks_init(report.group_count, 0u, &locks);
  if (rc == 0)
    rc = kafs_v7_sequence_state_init(locks, &report, &sequence);
  if (rc == 0)
    rc = publish_pending_uid(fd, &sb, file_size, &report, sequence, first_ino, 101u);
  if (rc == 0)
    rc = publish_pending_uid(fd, &sb, file_size, &report, sequence, second_ino, 202u);
  if (rc == 0 && report.journal.selected_nonempty_segment_count != 2u)
    rc = -EUCLEAN;
  kafs_v7_sequence_state_destroy(sequence);
  kafs_v7_locks_destroy(locks);
  kafs_v7_layout_report_clear(&report);
  if (fd >= 0)
    close(fd);
  return rc;
}

static int check_nonempty_journal_count(const char *path, uint32_t expected)
{
  int fd = open(path, O_RDONLY);
  kafs_ssuperblock_t sb;
  uint64_t file_size = 0u;
  kafs_v7_layout_report_t report = {0};
  int rc = fd < 0 ? -errno : validate_image_fd(fd, &sb, &file_size, &report);
  if (rc == 0 && report.journal.selected_nonempty_segment_count != expected)
    rc = -EUCLEAN;
  kafs_v7_layout_report_clear(&report);
  if (fd >= 0)
    close(fd);
  return rc;
}

static int file_digest(const char *path, uint64_t *digest_out)
{
  int fd = open(path, O_RDONLY);
  if (fd < 0)
    return -errno;
  uint64_t digest = UINT64_C(14695981039346656037);
  for (;;)
  {
    uint8_t buf[8192];
    ssize_t n = read(fd, buf, sizeof(buf));
    if (n < 0)
    {
      int saved = errno;
      close(fd);
      return -saved;
    }
    if (n == 0)
      break;
    for (ssize_t i = 0; i < n; ++i)
    {
      digest ^= buf[i];
      digest *= UINT64_C(1099511628211);
    }
  }
  close(fd);
  *digest_out = digest;
  return 0;
}

static int copy_image(const char *src, const char *dst)
{
  int in = open(src, O_RDONLY);
  int out = open(dst, O_WRONLY | O_CREAT | O_TRUNC, 0600);
  if (in < 0 || out < 0)
  {
    int saved = errno;
    if (in >= 0)
      close(in);
    if (out >= 0)
      close(out);
    return -saved;
  }
  int rc = 0;
  off_t off = 0;
  for (;;)
  {
    uint8_t buf[65536];
    ssize_t n = read(in, buf, sizeof(buf));
    if (n < 0)
    {
      rc = -errno;
      break;
    }
    if (n == 0)
      break;
    rc = kafs_pwrite_all(out, buf, (size_t)n, off);
    if (rc != 0)
      break;
    off += n;
  }
  close(in);
  close(out);
  return rc;
}

static int directory_has(const char *path, const char *name)
{
  DIR *dir = opendir(path);
  if (!dir)
    return 0;
  int found = 0;
  errno = 0;
  for (struct dirent *entry = readdir(dir); entry; entry = readdir(dir))
    if (strcmp(entry->d_name, name) == 0)
    {
      found = 1;
      break;
    }
  int saved = errno;
  closedir(dir);
  return saved == 0 && found;
}

static int read_equals(const char *path, const char *expected)
{
  int fd = open(path, O_RDONLY);
  if (fd < 0)
    return -errno;
  char buf[512];
  ssize_t n = read(fd, buf, sizeof(buf));
  int saved = errno;
  close(fd);
  if (n < 0)
    return -saved;
  return (size_t)n == strlen(expected) && memcmp(buf, expected, (size_t)n) == 0 ? 0 : -EINVAL;
}

static int read_block_equals(const char *path, const void *expected, size_t expected_bytes)
{
  int fd = open(path, O_RDONLY);
  if (fd < 0)
    return -errno;
  uint8_t *buf = malloc(expected_bytes);
  if (!buf)
  {
    close(fd);
    return -ENOMEM;
  }
  size_t used = 0u;
  while (used < expected_bytes)
  {
    ssize_t n = read(fd, buf + used, expected_bytes - used);
    if (n < 0)
    {
      int saved = errno;
      free(buf);
      close(fd);
      return -saved;
    }
    if (n == 0)
      break;
    used += (size_t)n;
  }
  close(fd);
  int rc = used == expected_bytes && memcmp(buf, expected, expected_bytes) == 0 ? 0 : -EINVAL;
  free(buf);
  return rc;
}

static int expect_erofs(const char *label, int result)
{
  int saved = errno;
  if (result == -1 && saved == EROFS)
    return 0;
  fprintf(stderr, "%s: result=%d errno=%s (expected EROFS)\n", label, result, strerror(saved));
  return -1;
}

static int check_mutations_erofs(const char *mnt)
{
  char inline_path[PATH_MAX];
  char block_path[PATH_MAX];
  char nested_path[PATH_MAX];
  char new_path[PATH_MAX];
  snprintf(inline_path, sizeof(inline_path), "%s/nested/inline", mnt);
  snprintf(block_path, sizeof(block_path), "%s/block", mnt);
  snprintf(nested_path, sizeof(nested_path), "%s/nested", mnt);
  snprintf(new_path, sizeof(new_path), "%s/new", mnt);

  int fd = open(new_path, O_WRONLY | O_CREAT, 0666);
  if (fd >= 0)
    close(fd);
  if (expect_erofs("create", fd) != 0)
    return -1;
  fd = open(inline_path, O_WRONLY);
  if (fd >= 0)
    close(fd);
  if (expect_erofs("open-write", fd) != 0 || expect_erofs("truncate", truncate(block_path, 1)) ||
      expect_erofs("unlink", unlink(block_path)) || expect_erofs("rename", rename(block_path, new_path)) ||
      expect_erofs("link", link(block_path, new_path)) || expect_erofs("mkdir", mkdir(new_path, 0777)) ||
      expect_erofs("rmdir", rmdir(nested_path)) || expect_erofs("chmod", chmod(block_path, 0600)) ||
      expect_erofs("chown", chown(block_path, getuid(), getgid())))
    return -1;

  struct timespec times[2] = {{.tv_nsec = UTIME_NOW}, {.tv_nsec = UTIME_NOW}};
  if (expect_erofs("utimens", utimensat(AT_FDCWD, block_path, times, 0)) ||
      expect_erofs("setxattr", setxattr(block_path, "user.kafs-test", "x", 1u, 0)) ||
      expect_erofs("removexattr", removexattr(block_path, "user.kafs-test")))
    return -1;
  return 0;
}

static int check_mount(const char *image, const char *mnt, const char *log_path,
                       const v7_fixture_t *fixture, int check_mutations)
{
  uint64_t before = 0;
  if (file_digest(image, &before) != 0)
    return -1;
  kafs_test_mount_options_t options = {
      .log_path = log_path,
      .extra_options = "ro",
      .timeout_ms = 15000,
  };
  pid_t pid = kafs_test_start_kafs_v7(image, mnt, &options);
  if (pid <= 0)
  {
    kafs_test_dump_log(log_path, "v7 inspection mount failed");
    return -1;
  }

  int rc = 0;
  char path[PATH_MAX];
  struct stat st;
  if (!directory_has(mnt, "nested") || !directory_has(mnt, "block") ||
      !directory_has(mnt, "link"))
    rc = -1;
  snprintf(path, sizeof(path), "%s/nested", mnt);
  if (rc == 0 && (stat(path, &st) != 0 || !S_ISDIR(st.st_mode) || !directory_has(path, "inline")))
    rc = -1;
  snprintf(path, sizeof(path), "%s/nested/inline", mnt);
  if (rc == 0 && read_equals(path, k_inline_payload) != 0)
    rc = -1;
  snprintf(path, sizeof(path), "%s/block", mnt);
  if (rc == 0)
  {
    uint8_t *expected = calloc(1u, 4096u);
    if (!expected)
      rc = -1;
    else
    {
      memcpy(expected, k_block_payload, strlen(k_block_payload));
      if (read_block_equals(path, expected, 4096u) != 0)
        rc = -1;
      free(expected);
    }
  }
  snprintf(path, sizeof(path), "%s/link", mnt);
  char target[128];
  ssize_t target_bytes = readlink(path, target, sizeof(target) - 1u);
  if (target_bytes >= 0)
    target[target_bytes] = '\0';
  if (rc == 0 && (target_bytes < 0 || strcmp(target, k_symlink_target) != 0))
    rc = -1;

  struct statvfs statfs;
  if (rc == 0 &&
      (statvfs(mnt, &statfs) != 0 || statfs.f_blocks != fixture->total_blocks ||
       statfs.f_bfree != fixture->free_blocks || statfs.f_bavail != fixture->free_blocks ||
       statfs.f_ffree != fixture->free_inodes))
    rc = -1;
  if (rc == 0 && check_mutations && check_mutations_erofs(mnt) != 0)
    rc = -1;

  kafs_test_stop_kafs(mnt, pid);
  uint64_t after = 0;
  if (file_digest(image, &after) != 0 || after != before)
    rc = -1;
  return rc;
}

static int run_fsck(const char *image)
{
  char output[8192];
  char *argv[] = {(char *)kafs_test_fsck_bin(), (char *)"--check", (char *)image, NULL};
  return run_command(argv, 0, output, sizeof(output));
}

static int check_persisted_block(const char *image, uint32_t ino, const void *expected,
                                 uint32_t block_size)
{
  int fd = open(image, O_RDONLY);
  kafs_ssuperblock_t sb;
  uint64_t file_size = 0;
  kafs_v7_layout_report_t report = {0};
  int rc = fd < 0 ? -errno : validate_image_fd(fd, &sb, &file_size, &report);
  kafs_v7_inode_t inode;
  if (rc == 0)
    rc = read_inode(fd, &report, ino, &inode);
  uint32_t reference = 0u;
  if (rc == 0)
  {
    memcpy(&reference, inode.inline_or_block_refs, sizeof(reference));
    reference = le32toh(reference);
    if (reference == 0u)
      rc = -ENOENT;
  }
  uint8_t *actual = rc == 0 ? malloc(block_size) : NULL;
  if (rc == 0 && !actual)
    rc = -ENOMEM;
  const kafs_v7_group_desc_t *groups = kafs_v7_report_groups(&report);
  uint64_t logical = reference ? (uint64_t)reference - 1u : 0u;
  if (rc == 0)
  {
    rc = -ERANGE;
    for (uint32_t i = 0; i < report.group_count; ++i)
    {
      uint64_t start = le64toh(groups[i].data_logical_start);
      uint64_t count = le64toh(groups[i].data_logical_count);
      if (logical < start || logical - start >= count)
        continue;
      uint64_t off = le64toh(groups[i].data_physical_off) + (logical - start) * block_size;
      rc = kafs_pread_all(fd, actual, block_size, (off_t)off);
      break;
    }
  }
  if (rc == 0 && memcmp(actual, expected, block_size) != 0)
    rc = -EIO;
  free(actual);
  kafs_v7_layout_report_clear(&report);
  if (fd >= 0)
    close(fd);
  return rc;
}

static int check_controlled_write_mount(const char *image, uint32_t ino, uint32_t block_size)
{
  const char *mnt = "mnt-controlled";
  const char *log_path = "v7-controlled-write.log";
  kafs_test_mount_options_t options = {
      .log_path = log_path,
      .extra_options =
          "rw,no_writeback_cache,no_trim_on_free,bg_dedup_scan=off,fsync_policy=full",
      .timeout_ms = 15000,
  };
  pid_t pid = kafs_test_start_kafs_v7_controlled_write(image, mnt, &options);
  if (pid <= 0)
  {
    kafs_test_dump_log(log_path, "v7 controlled-write mount failed");
    return -1;
  }

  char path[PATH_MAX];
  snprintf(path, sizeof(path), "%s/block", mnt);
  int fd = open(path, O_RDWR);
  uint8_t *payload = malloc(block_size);
  int rc = fd < 0 || !payload ? -1 : 0;
  if (rc == 0)
  {
    for (uint32_t i = 0; i < block_size; ++i)
      payload[i] = (uint8_t)(i * 29u + 7u);
    errno = 0;
    if (pwrite(fd, payload, block_size - 1u, 0) != -1 || errno != EOPNOTSUPP)
    {
      fprintf(stderr, "partial controlled write returned unexpected result: errno=%d\n", errno);
      rc = -1;
    }
  }
  if (rc == 0 && pwrite(fd, payload, block_size, 0) != (ssize_t)block_size)
  {
    fprintf(stderr, "full controlled write failed: errno=%d\n", errno);
    rc = -1;
  }
  if (rc == 0 && fsync(fd) != 0)
  {
    fprintf(stderr, "controlled write fsync failed: errno=%d\n", errno);
    rc = -1;
  }
  if (fd >= 0 && close(fd) != 0 && rc == 0)
    rc = -1;
  kafs_test_stop_kafs(mnt, pid);
  if (rc != 0)
    kafs_test_dump_log(log_path, "v7 controlled-write operation failed");

  if (rc == 0 && run_fsck(image) != 0)
    rc = -1;
  if (rc == 0 && check_persisted_block(image, ino, payload, block_size) != 0)
    rc = -1;
  if (rc == 0)
  {
    kafs_test_mount_options_t inspect = {
        .log_path = "v7-controlled-remount.log",
        .extra_options = "ro",
        .timeout_ms = 15000,
    };
    pid = kafs_test_start_kafs_v7(image, "mnt-controlled-remount", &inspect);
    if (pid <= 0)
      rc = -1;
    else
    {
      snprintf(path, sizeof(path), "%s/block", "mnt-controlled-remount");
      if (read_block_equals(path, payload, block_size) != 0)
        rc = -1;
      kafs_test_stop_kafs("mnt-controlled-remount", pid);
    }
  }
  free(payload);
  return rc;
}

static int check_controlled_write_recovery(const char *image, uint32_t ino, uint32_t block_size,
                                           const char *crash_env, uint8_t seed)
{
  const char *mnt = "mnt-controlled-crash";
  kafs_test_mount_options_t options = {
      .log_path = "v7-controlled-crash.log",
      .extra_options =
          "rw,no_writeback_cache,no_trim_on_free,bg_dedup_scan=off,fsync_policy=full",
      .timeout_ms = 15000,
  };
  if (setenv(crash_env, "1", 1) != 0)
    return -1;
  pid_t pid = kafs_test_start_kafs_v7_controlled_write(image, mnt, &options);
  unsetenv(crash_env);
  if (pid <= 0)
    return -1;

  uint8_t *payload = malloc(block_size);
  char path[PATH_MAX];
  snprintf(path, sizeof(path), "%s/block", mnt);
  int fd = open(path, O_RDWR);
  int rc = fd < 0 || !payload ? -1 : 0;
  if (rc == 0)
  {
    for (uint32_t i = 0; i < block_size; ++i)
      payload[i] = (uint8_t)(i * 37u + seed);
    if (pwrite(fd, payload, block_size, 0) >= 0)
      rc = -1;
  }
  if (fd >= 0)
    close(fd);
  kafs_test_stop_kafs(mnt, pid);

  options.log_path = "v7-controlled-recovery.log";
  pid = rc == 0 ? kafs_test_start_kafs_v7_controlled_write(
                       image, "mnt-controlled-recovery", &options)
                : -1;
  if (pid <= 0)
    rc = -1;
  else
    kafs_test_stop_kafs("mnt-controlled-recovery", pid);
  if (rc == 0 && run_fsck(image) != 0)
    rc = -1;
  if (rc == 0 && check_persisted_block(image, ino, payload, block_size) != 0)
    rc = -1;
  if (rc != 0)
  {
    kafs_test_dump_log("v7-controlled-crash.log", "v7 controlled-write crash failed");
    kafs_test_dump_log("v7-controlled-recovery.log", "v7 controlled-write recovery failed");
  }
  free(payload);
  return rc;
}

static int check_controlled_reclaim_recovery(const char *image)
{
  kafs_test_mount_options_t options = {
      .log_path = "v7-controlled-reclaim-crash.log",
      .extra_options =
          "rw,no_writeback_cache,no_trim_on_free,bg_dedup_scan=off,fsync_policy=full",
      .timeout_ms = 15000,
  };
  if (setenv("KAFS_V7_TEST_CRASH_AFTER_JOURNAL_RECLAIM", "1", 1) != 0)
    return -1;
  pid_t pid = kafs_test_start_kafs_v7_controlled_write(image, "mnt-reclaim-crash", &options);
  unsetenv("KAFS_V7_TEST_CRASH_AFTER_JOURNAL_RECLAIM");
  if (pid > 0)
  {
    kafs_test_stop_kafs("mnt-reclaim-crash", pid);
    return -1;
  }
  if (check_nonempty_journal_count(image, 1u) != 0)
    return -1;

  options.log_path = "v7-controlled-reclaim-recovery.log";
  pid = kafs_test_start_kafs_v7_controlled_write(image, "mnt-reclaim-recovery", &options);
  if (pid <= 0)
    return -1;
  kafs_test_stop_kafs("mnt-reclaim-recovery", pid);
  return check_nonempty_journal_count(image, 0u) == 0 && run_fsck(image) == 0 ? 0 : -1;
}

static int corrupt_primary_pair(const char *path)
{
  int fd = open(path, O_RDWR);
  if (fd < 0)
    return -errno;
  kafs_ssuperblock_t sb;
  uint64_t file_size = 0;
  kafs_v7_layout_report_t report = {0};
  int rc = validate_image_fd(fd, &sb, &file_size, &report);
  uint8_t byte = 0;
  if (rc == 0)
    rc = kafs_pread_all(fd, &byte, 1u, (off_t)report.descriptors[0].offset);
  byte ^= 0xffu;
  if (rc == 0)
    rc = kafs_pwrite_all(fd, &byte, 1u, (off_t)report.descriptors[0].offset);
  if (rc == 0)
    rc = kafs_pread_all(fd, &byte, 1u, (off_t)report.checkpoints[0].offset);
  byte ^= 0xffu;
  if (rc == 0)
    rc = kafs_pwrite_all(fd, &byte, 1u, (off_t)report.checkpoints[0].offset);
  if (rc == 0)
    rc = fdatasync(fd) == 0 ? 0 : -errno;
  kafs_v7_layout_report_clear(&report);
  close(fd);
  return rc;
}

static int make_unpaired_generation(const char *path)
{
  int fd = open(path, O_RDWR);
  if (fd < 0)
    return -errno;
  kafs_ssuperblock_t sb;
  uint64_t file_size = 0;
  kafs_v7_layout_report_t report = {0};
  int rc = validate_image_fd(fd, &sb, &file_size, &report);
  if (rc == 0)
  {
    kafs_v7_layout_header_t *header = (kafs_v7_layout_header_t *)report.descriptor;
    header->generation = htole64(le64toh(header->generation) + 1u);
    header->descriptor_crc32 = 0u;
    header->descriptor_crc32 =
        htole32(kafs_v7_crc32(report.descriptor, report.descriptor_bytes));
    rc = kafs_pwrite_all(fd, report.descriptor, report.descriptor_bytes,
                         (off_t)report.descriptors[0].offset);
  }
  if (rc == 0)
    rc = fdatasync(fd) == 0 ? 0 : -errno;
  kafs_v7_layout_report_clear(&report);
  close(fd);
  return rc;
}

int main(void)
{
  if (kafs_test_enter_tmpdir("v7-inspection-mount") != 0)
    return 1;
  const char *image = "v7-inspection.img";
  v7_fixture_t fixture = {0};
  if (format_image(image) != 0 || seed_fixture(image, &fixture) != 0)
  {
    fprintf(stderr, "failed to build accepted v7 inspection fixture\n");
    return 1;
  }

  if (access("/dev/fuse", R_OK | W_OK) != 0)
  {
    fprintf(stderr, "skip v7 inspection mount smoke: /dev/fuse unavailable\n");
    return 0;
  }
  if (check_mount(image, "mnt", "v7-inspection.log", &fixture, 1) != 0)
  {
    fprintf(stderr, "pristine v7 inspection mount failed\n");
    return 1;
  }

  const char *controlled = "v7-controlled.img";
  if (copy_image(image, controlled) != 0 ||
      check_controlled_write_mount(controlled, fixture.block_ino, fixture.block_size) != 0)
  {
    fprintf(stderr, "v7 controlled-write mount matrix failed\n");
    return 1;
  }

  const char *recovery = "v7-controlled-recovery.img";
  if (copy_image(image, recovery) != 0 ||
      check_controlled_write_recovery(recovery, fixture.block_ino, fixture.block_size,
                                      "KAFS_V7_TEST_CRASH_AFTER_JOURNAL_PUBLISH", 11u) != 0)
  {
    fprintf(stderr, "v7 controlled-write journal recovery failed\n");
    return 1;
  }

  const char *apply_recovery = "v7-controlled-apply-recovery.img";
  if (copy_image(image, apply_recovery) != 0 ||
      check_controlled_write_recovery(apply_recovery, fixture.block_ino, fixture.block_size,
                                      "KAFS_V7_TEST_CRASH_AFTER_METADATA_APPLY", 13u) != 0)
  {
    fprintf(stderr, "v7 controlled-write metadata apply recovery failed\n");
    return 1;
  }

  const char *checkpoint_recovery = "v7-controlled-checkpoint-recovery.img";
  if (copy_image(image, checkpoint_recovery) != 0 ||
      check_controlled_write_recovery(checkpoint_recovery, fixture.block_ino, fixture.block_size,
                                      "KAFS_V7_TEST_CRASH_AFTER_CHECKPOINT_COPY", 17u) != 0)
  {
    fprintf(stderr, "v7 controlled-write checkpoint recovery failed\n");
    return 1;
  }

  const char *reclaim_recovery = "v7-controlled-reclaim-recovery.img";
  if (copy_image(image, reclaim_recovery) != 0 ||
      seed_pending_journals(reclaim_recovery, fixture.nested_ino, fixture.inline_ino) != 0 ||
      check_controlled_reclaim_recovery(reclaim_recovery) != 0)
  {
    fprintf(stderr, "v7 controlled-write journal reclaim recovery failed\n");
    return 1;
  }

  const char *degraded = "v7-degraded.img";
  if (copy_image(image, degraded) != 0 || corrupt_primary_pair(degraded) != 0)
    return 1;
  int fd = open(degraded, O_RDONLY);
  kafs_ssuperblock_t sb;
  uint64_t file_size = 0;
  kafs_v7_layout_report_t report = {0};
  int rc = fd < 0 ? -errno : validate_image_fd(fd, &sb, &file_size, &report);
  if (fd >= 0)
    close(fd);
  if (rc != 0 || !report.degraded)
  {
    kafs_v7_layout_report_clear(&report);
    fprintf(stderr, "single surviving v7 recovery pair was not admitted degraded\n");
    return 1;
  }
  kafs_v7_layout_report_clear(&report);
  if (check_mount(degraded, "mnt-degraded", "v7-degraded.log", &fixture, 0) != 0)
    return 1;

  const char *unpaired = "v7-unpaired.img";
  if (copy_image(image, unpaired) != 0 || make_unpaired_generation(unpaired) != 0)
    return 1;
  char output[8192];
  char *argv[] = {(char *)kafs_test_kafs_v7_bin(), (char *)"--image", (char *)unpaired,
                  (char *)"--inspection-mount", (char *)"missing-mnt", (char *)"-o",
                  (char *)"ro", NULL};
  if (run_command(argv, 2, output, sizeof(output)) != 0 ||
      !strstr(output, "admission preflight failed") || strstr(output, "selected descriptor retained") ||
      strstr(output, "bad mount point"))
  {
    fprintf(stderr, "unpaired v7 descriptor generation reached FUSE unexpectedly:\n%s\n", output);
    return 1;
  }
  return 0;
}
