#include "test_utils.h"

#include "kafs_offline_summary.h"
#include "kafs_superblock.h"
#include "kafs_v7_journal_writer.h"
#include "kafs_v7_layout.h"
#include "kafs_v7_locks.h"
#include "kafs_v7_mutation.h"
#include "kafs_v7_recovery_diagnostic.h"
#include "kafs_v7_sequence.h"
#include "kafs_v7_test_fault.h"

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
static const char k_promoted_payload[] =
    "v7-created-file grows from inline storage into one direct block while preserving its "
    "original prefix.\n";
static const char k_symlink_target[] = "inline";

static void qualification_case_pass(const char *case_id)
{
  printf("KAFS_V7_QUALIFICATION_CASE %s PASS\n", case_id);
  fflush(stdout);
}

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

static int read_recovery_log(const char *path, kafs_v7_recovery_diagnostic_t *diagnostic)
{
  FILE *fp = fopen(path, "r");
  if (!fp)
    return -errno;
  int rc = kafs_v7_recovery_diagnostic_read(fp, diagnostic);
  fclose(fp);
  return rc;
}

static int check_fuse_contract_log(const char *path, const char *expected_mode, uint32_t block_size,
                                   int writable)
{
  FILE *fp = fopen(path, "r");
  if (!fp)
    return -errno;
  char line[1024];
  int rc = -ENOENT;
  while (fgets(line, sizeof(line), fp))
  {
    char mode[32];
    unsigned kernel = 0u;
    unsigned negotiated = 0u;
    unsigned atomic = 0u;
    unsigned logged_block_size = 0u;
    unsigned direct_blocks = 0u;
    if (sscanf(line,
               "kafs-v7-fuse-contract mode=%31s kernel_max_write=%u negotiated_max_write=%u "
               "atomic_write_max=%u block_size=%u direct_blocks=%u",
               mode, &kernel, &negotiated, &atomic, &logged_block_size, &direct_blocks) != 6)
      continue;
    uint64_t cap = (uint64_t)block_size * 12u;
    unsigned expected_negotiated = writable && kernel > cap ? (unsigned)cap : kernel;
    unsigned expected_atomic = writable ? expected_negotiated : 0u;
    rc = strcmp(mode, expected_mode) == 0 && negotiated == expected_negotiated &&
                 atomic == expected_atomic && logged_block_size == block_size &&
                 direct_blocks == 12u
             ? 0
             : -EINVAL;
    break;
  }
  fclose(fp);
  return rc;
}

static int check_recovery_cli(const char *path, const kafs_v7_recovery_diagnostic_t *diagnostic)
{
  char output[4096];
  char *text_argv[] = {(char *)kafs_test_kafsdump_bin(), "--recovery-log", (char *)path, NULL};
  if (run_command(text_argv, 0, output, sizeof(output)) != 0 ||
      !strstr(output, "recovery status=completed format=v7") ||
      !strstr(output, kafs_v7_recovery_resume_from_name(diagnostic->resume_from)))
  {
    fprintf(stderr, "kafsdump recovery text validation failed: %s\n", output);
    return -1;
  }
  char *json_argv[] = {text_argv[0], "--json", "--recovery-log", (char *)path, NULL};
  if (run_command(json_argv, 0, output, sizeof(output)) != 0 || !strstr(output, "\"recovery\":") ||
      !strstr(output, "\"status\": \"completed\"") ||
      !strstr(output, kafs_v7_recovery_resume_from_name(diagnostic->resume_from)))
  {
    fprintf(stderr, "kafsdump recovery JSON validation failed: %s\n", output);
    return -1;
  }
  return 0;
}

static int check_recovery_log(const char *path, kafs_v7_test_fault_point_t fault,
                              uint64_t mutation_count, uint64_t preapplied_count)
{
  kafs_v7_recovery_diagnostic_t diagnostic;
  if (read_recovery_log(path, &diagnostic) != 0 || diagnostic.final_nonempty_segments != 0u ||
      check_recovery_cli(path, &diagnostic) != 0)
    return -1;
  switch (fault)
  {
  case KAFS_V7_TEST_FAULT_JOURNAL_PUBLISH:
    return diagnostic.resume_from == KAFS_V7_RECOVERY_RESUME_JOURNAL_PUBLISH &&
                   diagnostic.applied_targets == mutation_count - preapplied_count &&
                   diagnostic.applied_mutations == mutation_count - preapplied_count &&
                   diagnostic.already_applied_mutations == preapplied_count
               ? 0
               : -1;
  case KAFS_V7_TEST_FAULT_METADATA_APPLY:
    return diagnostic.resume_from == KAFS_V7_RECOVERY_RESUME_METADATA_APPLY &&
                   diagnostic.applied_targets == 0u && diagnostic.applied_mutations == 0u &&
                   diagnostic.already_applied_mutations == mutation_count
               ? 0
               : -1;
  case KAFS_V7_TEST_FAULT_CHECKPOINT_COPY:
    return diagnostic.resume_from == KAFS_V7_RECOVERY_RESUME_CHECKPOINT_COPY &&
                   diagnostic.checkpoint_publications == 1u && diagnostic.checkpoint_resumes == 1u
               ? 0
               : -1;
  case KAFS_V7_TEST_FAULT_JOURNAL_RECLAIM:
    return diagnostic.resume_from == KAFS_V7_RECOVERY_RESUME_JOURNAL_RECLAIM &&
                   diagnostic.checkpoint_publications == 0u &&
                   diagnostic.checkpoint_resumes == 0u && diagnostic.reclaimed_segments == 1u
               ? 0
               : -1;
  default:
    return -EINVAL;
  }
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

static const kafs_v7_shard_desc_t *inode_shard_for_group(const kafs_v7_layout_report_t *report,
                                                         uint32_t group_id)
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

static int inode_direct_block_offset(const kafs_v7_layout_report_t *report,
                                     const kafs_v7_inode_t *inode, uint32_t block_id,
                                     uint64_t *physical_off)
{
  if (!report || !inode || !physical_off ||
      block_id >= KAFS_V7_INODE_DIRECT_REFERENCE_COUNT || block_id >= le32toh(inode->blocks))
    return -EINVAL;
  uint32_t reference = 0u;
  memcpy(&reference, inode->inline_or_block_refs + block_id * sizeof(reference),
         sizeof(reference));
  reference = le32toh(reference);
  if (reference == 0u)
    return -EINVAL;
  uint64_t logical = (uint64_t)reference - 1u;
  const kafs_v7_group_desc_t *groups = kafs_v7_report_groups(report);
  for (uint32_t group_id = 0u; group_id < report->group_count; ++group_id)
  {
    uint64_t start = le64toh(groups[group_id].data_logical_start);
    uint64_t count = le64toh(groups[group_id].data_logical_count);
    if (logical >= start && logical - start < count)
    {
      *physical_off = le64toh(groups[group_id].data_physical_off) +
                      (logical - start) * report->block_size;
      return 0;
    }
  }
  return -ERANGE;
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
  int rc =
      kafs_pread_all(fd, bitmap, (size_t)bitmap_bytes, (off_t)le64toh(bitmap_shard->physical_off));
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
    int rc =
        kafs_pread_all(fd, &checkpoint, sizeof(checkpoint), (off_t)report->checkpoints[i].offset);
    if (rc != 0)
      return rc;
    checkpoint.free_blocks = htole64(free_blocks);
    checkpoint.free_inodes = htole64(free_inodes);
    checkpoint.crc32 = 0u;
    checkpoint.crc32 = htole32(kafs_v7_crc32(&checkpoint, sizeof(checkpoint)));
    rc = kafs_pwrite_all(fd, &checkpoint, sizeof(checkpoint), (off_t)report->checkpoints[i].offset);
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
  if (report.group_count != 4u || report.free_blocks < 4u || report.free_inodes < 4u)
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
  uint64_t file_blocks[3] = {le64toh(groups[3].data_logical_start),
                             le64toh(groups[3].data_logical_start) + 1u,
                             le64toh(groups[3].data_logical_start) + 2u};
  uint32_t block_size = report.block_size;
  fixture->block_size = block_size;
  uint8_t *root_data = calloc(1u, block_size);
  uint8_t *file_data = calloc(3u, block_size);
  if (!root_data || !file_data)
    rc = -ENOMEM;

  size_t root_bytes = 0;
  size_t nested_bytes = 0;
  uint8_t nested_data[KAFS_V7_INODE_BYTES] = {0};
  const uint32_t root_inos[] = {fixture->nested_ino, fixture->inline_ino, fixture->block_ino,
                                fixture->symlink_ino};
  const char *const root_names[] = {"nested", "inline", "block", "link"};
  const uint32_t nested_inos[] = {KAFS_INO_ROOTDIR};
  const char *const nested_names[] = {".."};
  if (rc == 0)
    rc = build_directory(root_data, block_size, root_inos, root_names, 4u, &root_bytes);
  if (rc == 0)
    rc = build_directory(nested_data, KAFS_INODE_DIRECT_BYTES, nested_inos, nested_names, 1u,
                         &nested_bytes);
  if (rc == 0 && (root_bytes <= KAFS_INODE_DIRECT_BYTES || nested_bytes > KAFS_INODE_DIRECT_BYTES ||
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
    inode_init(&block_inode, (uint16_t)(S_IFREG | 0666), 3u * block_size, 1u, 3u);
    for (size_t i = 0; i < 3u; ++i)
    {
      uint32_t file_ref = htole32((uint32_t)file_blocks[i] + 1u);
      memcpy(block_inode.inline_or_block_refs + i * sizeof(file_ref), &file_ref, sizeof(file_ref));
    }
    inode_init(&symlink_inode, (uint16_t)(S_IFLNK | 0777), strlen(k_symlink_target), 1u, 0u);
    memcpy(symlink_inode.inline_or_block_refs, k_symlink_target, strlen(k_symlink_target));
  }

  if (rc == 0)
    rc = write_data_block(fd, &groups[0], root_block, root_data, block_size);
  if (rc == 0)
    for (size_t i = 0; rc == 0 && i < 3u; ++i)
      rc = write_data_block(fd, &groups[3], file_blocks[i], file_data + i * block_size, block_size);
  if (rc == 0)
    rc = allocate_group_block(fd, &report, 0u, root_block);
  if (rc == 0)
    for (size_t i = 0; rc == 0 && i < 3u; ++i)
      rc = allocate_group_block(fd, &report, 3u, file_blocks[i]);
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

  fixture->free_blocks = report.free_blocks - 4u;
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
  if (rc == 0 &&
      (verify.free_blocks != fixture->free_blocks || verify.free_inodes != fixture->free_inodes))
    rc = -EINVAL;
  kafs_v7_layout_report_clear(&verify);
  close(fd);
  return rc;
}

static int copy_image(const char *src, const char *dst);

enum namespace_payload_fault
{
  NAMESPACE_DIRECT_NAME_HASH,
  NAMESPACE_INLINE_LIVE_COUNT,
  NAMESPACE_MISSING_PARENT,
  NAMESPACE_SYMLINK_NUL,
};

static int apply_namespace_payload_fault(const char *path, const v7_fixture_t *fixture,
                                         enum namespace_payload_fault fault)
{
  int fd = open(path, O_RDWR);
  if (fd < 0)
    return -errno;
  kafs_ssuperblock_t sb;
  uint64_t file_size = 0u;
  kafs_v7_layout_report_t report = {0};
  int rc = validate_image_fd(fd, &sb, &file_size, &report);
  uint32_t ino = fault == NAMESPACE_SYMLINK_NUL ? fixture->symlink_ino : fixture->nested_ino;
  if (fault == NAMESPACE_DIRECT_NAME_HASH)
    ino = KAFS_INO_ROOTDIR;
  kafs_v7_inode_t inode;
  if (rc == 0)
    rc = read_inode(fd, &report, ino, &inode);
  if (rc == 0 && fault == NAMESPACE_DIRECT_NAME_HASH)
  {
    uint64_t physical_off = 0u;
    rc = inode_direct_block_offset(&report, &inode, 0u, &physical_off);
    uint8_t *payload = rc == 0 ? malloc(report.block_size) : NULL;
    if (rc == 0 && !payload)
      rc = -ENOMEM;
    if (rc == 0)
      rc = kafs_pread_all(fd, payload, report.block_size, (off_t)physical_off);
    if (rc == 0)
    {
      kafs_v7_kdir_header_t *header = (kafs_v7_kdir_header_t *)payload;
      kafs_v7_kdir_record_t *record =
          (kafs_v7_kdir_record_t *)(payload + KAFS_V7_KDIR_HEADER_BYTES);
      if (le32toh(header->magic) != KAFS_V7_KDIR_MAGIC ||
          le16toh(record->record_length) < KAFS_V7_KDIR_RECORD_PREFIX_BYTES + 1u)
        rc = -EUCLEAN;
      else
      {
        record->name_hash = htole32(le32toh(record->name_hash) ^ 1u);
        rc = kafs_pwrite_all(fd, payload, report.block_size, (off_t)physical_off);
      }
    }
    free(payload);
  }
  else if (rc == 0 && fault == NAMESPACE_INLINE_LIVE_COUNT)
  {
    kafs_v7_kdir_header_t *header = (kafs_v7_kdir_header_t *)inode.inline_or_block_refs;
    header->live_count = htole32(le32toh(header->live_count) + 1u);
    rc = write_inode(fd, &report, ino, &inode);
  }
  else if (rc == 0 && fault == NAMESPACE_MISSING_PARENT)
  {
    kafs_v7_kdir_record_t *record =
        (kafs_v7_kdir_record_t *)(inode.inline_or_block_refs + KAFS_V7_KDIR_HEADER_BYTES);
    if (le16toh(record->name_bytes) != 2u || memcmp(record->name, "..", 2u) != 0)
      rc = -EUCLEAN;
    else
    {
      memcpy(record->name, "xx", 2u);
      record->name_hash = htole32(name_hash("xx", 2u));
      rc = write_inode(fd, &report, ino, &inode);
    }
  }
  else if (rc == 0 && fault == NAMESPACE_SYMLINK_NUL)
  {
    inode.inline_or_block_refs[0] = '\0';
    rc = write_inode(fd, &report, ino, &inode);
  }
  if (rc == 0 && fdatasync(fd) != 0)
    rc = -errno;
  kafs_v7_layout_report_clear(&report);
  close(fd);
  return rc;
}

static int v7_image_rejected(const char *path)
{
  int fd = open(path, O_RDONLY);
  if (fd < 0)
    return -errno;
  kafs_ssuperblock_t sb;
  uint64_t file_size = 0u;
  kafs_v7_layout_report_t report = {0};
  int rc = validate_image_fd(fd, &sb, &file_size, &report);
  kafs_v7_layout_report_clear(&report);
  close(fd);
  return rc == 0 ? -1 : 0;
}

static int v7_fault_consumers_reject(const char *path)
{
  char output[8192];
  char *dump_argv[] = {(char *)kafs_test_kafsdump_bin(), (char *)"--json", (char *)path, NULL};
  char *fsck_argv[] = {(char *)kafs_test_fsck_bin(), (char *)"--check", (char *)path, NULL};
  char *mount_argv[] = {(char *)kafs_test_kafs_v7_bin(),
                        (char *)"--image",
                        (char *)path,
                        (char *)"--inspection-mount",
                        (char *)"missing-mnt",
                        (char *)"-o",
                        (char *)"ro",
                        NULL};
  if (run_command(dump_argv, 1, output, sizeof(output)) != 0 ||
      run_command(fsck_argv, 13, output, sizeof(output)) != 0 ||
      run_command(mount_argv, 2, output, sizeof(output)) != 0 ||
      !strstr(output, "admission preflight failed"))
    return -1;
  return 0;
}

static int test_namespace_payload_faults(const char *source, const v7_fixture_t *fixture)
{
  const enum namespace_payload_fault faults[] = {
      NAMESPACE_DIRECT_NAME_HASH,
      NAMESPACE_INLINE_LIVE_COUNT,
      NAMESPACE_MISSING_PARENT,
      NAMESPACE_SYMLINK_NUL,
  };
  for (size_t i = 0u; i < sizeof(faults) / sizeof(faults[0]); ++i)
  {
    char path[64];
    snprintf(path, sizeof(path), "v7-namespace-fault-%zu.img", i);
    if (copy_image(source, path) != 0 || apply_namespace_payload_fault(path, fixture, faults[i]) != 0 ||
        v7_image_rejected(path) != 0 || (i == 0u && v7_fault_consumers_reject(path) != 0))
      return -1;
  }
  return 0;
}

enum direct_inode_fault
{
  DIRECT_INODE_BLOCK_COUNT,
  DIRECT_INODE_MISSING_REFERENCE,
  DIRECT_INODE_OUT_OF_RANGE_REFERENCE,
  DIRECT_INODE_FREE_REFERENCE,
  DIRECT_INODE_UNUSED_DIRECT_REFERENCE,
  DIRECT_INODE_INDIRECT_REFERENCE,
};

static int find_free_group_reference(int fd, const kafs_v7_layout_report_t *report,
                                     uint32_t group_id, uint32_t *reference)
{
  if (!report || !reference || group_id >= report->group_count)
    return -EINVAL;
  const kafs_v7_group_desc_t *groups = kafs_v7_report_groups(report);
  const kafs_v7_shard_desc_t *shards = kafs_v7_report_shards(report);
  const kafs_v7_shard_desc_t *bitmap = &shards[le32toh(groups[group_id].first_shard_index)];
  uint64_t bytes = le64toh(bitmap->physical_bytes);
  if (bytes == 0u || bytes > SIZE_MAX)
    return -ERANGE;
  uint8_t *bits = malloc((size_t)bytes);
  if (!bits)
    return -ENOMEM;
  int rc = kafs_pread_all(fd, bits, (size_t)bytes, (off_t)le64toh(bitmap->physical_off));
  uint64_t start = le64toh(bitmap->logical_start);
  uint64_t count = le64toh(bitmap->logical_count);
  for (uint64_t local = 0u; rc == 0 && local < count; ++local)
  {
    uint64_t logical = start + local;
    if ((bits[local / 8u] & (uint8_t)(1u << (local % 8u))) == 0u && logical < UINT32_MAX)
    {
      *reference = (uint32_t)logical + 1u;
      free(bits);
      return 0;
    }
  }
  free(bits);
  return rc == 0 ? -ENOSPC : rc;
}

static int apply_direct_inode_fault(const char *path, const v7_fixture_t *fixture,
                                    enum direct_inode_fault fault)
{
  int fd = open(path, O_RDWR);
  if (fd < 0)
    return -errno;
  kafs_ssuperblock_t sb;
  uint64_t file_size = 0u;
  kafs_v7_layout_report_t report = {0};
  int rc = validate_image_fd(fd, &sb, &file_size, &report);
  kafs_v7_inode_t inode;
  if (rc == 0)
    rc = read_inode(fd, &report, fixture->block_ino, &inode);
  uint32_t first_reference = 0u;
  if (rc == 0)
  {
    memcpy(&first_reference, inode.inline_or_block_refs, sizeof(first_reference));
    if (first_reference == 0u || le32toh(inode.blocks) != 3u)
      rc = -EUCLEAN;
  }
  if (rc == 0 && fault == DIRECT_INODE_BLOCK_COUNT)
    inode.blocks = htole32(2u);
  else if (rc == 0 && fault == DIRECT_INODE_MISSING_REFERENCE)
    memset(inode.inline_or_block_refs + sizeof(uint32_t), 0, sizeof(uint32_t));
  else if (rc == 0 && fault == DIRECT_INODE_OUT_OF_RANGE_REFERENCE)
  {
    uint32_t reference = htole32(UINT32_MAX);
    memcpy(inode.inline_or_block_refs + sizeof(uint32_t), &reference, sizeof(reference));
  }
  else if (rc == 0 && fault == DIRECT_INODE_FREE_REFERENCE)
  {
    uint32_t reference = 0u;
    rc = find_free_group_reference(fd, &report, 3u, &reference);
    if (rc == 0)
    {
      reference = htole32(reference);
      memcpy(inode.inline_or_block_refs + sizeof(uint32_t), &reference, sizeof(reference));
    }
  }
  else if (rc == 0 && fault == DIRECT_INODE_UNUSED_DIRECT_REFERENCE)
    memcpy(inode.inline_or_block_refs + 3u * sizeof(uint32_t), &first_reference,
           sizeof(first_reference));
  else if (rc == 0 && fault == DIRECT_INODE_INDIRECT_REFERENCE)
    memcpy(inode.inline_or_block_refs + KAFS_V7_INODE_DIRECT_REFERENCE_COUNT * sizeof(uint32_t),
           &first_reference, sizeof(first_reference));
  if (rc == 0)
    rc = write_inode(fd, &report, fixture->block_ino, &inode);
  if (rc == 0 && fdatasync(fd) != 0)
    rc = -errno;
  kafs_v7_layout_report_clear(&report);
  close(fd);
  return rc;
}

static int test_direct_inode_faults(const char *source, const v7_fixture_t *fixture)
{
  const enum direct_inode_fault faults[] = {
      DIRECT_INODE_BLOCK_COUNT,
      DIRECT_INODE_MISSING_REFERENCE,
      DIRECT_INODE_OUT_OF_RANGE_REFERENCE,
      DIRECT_INODE_FREE_REFERENCE,
      DIRECT_INODE_UNUSED_DIRECT_REFERENCE,
      DIRECT_INODE_INDIRECT_REFERENCE,
  };
  for (size_t i = 0u; i < sizeof(faults) / sizeof(faults[0]); ++i)
  {
    char path[64];
    snprintf(path, sizeof(path), "v7-direct-inode-fault-%zu.img", i);
    if (copy_image(source, path) != 0 || apply_direct_inode_fault(path, fixture, faults[i]) != 0 ||
        v7_image_rejected(path) != 0 || (i == 0u && v7_fault_consumers_reject(path) != 0))
      return -1;
  }
  return 0;
}

static int seed_full_root_directory(const char *path, const char *next_name,
                                    uint32_t expected_blocks)
{
  int fd = open(path, O_RDWR);
  if (fd < 0)
    return -errno;
  kafs_ssuperblock_t sb;
  uint64_t file_size = 0u;
  kafs_v7_layout_report_t report = {0};
  int rc = validate_image_fd(fd, &sb, &file_size, &report);
  kafs_v7_inode_t root;
  if (rc == 0)
    rc = read_inode(fd, &report, KAFS_INO_ROOTDIR, &root);
  if (rc == 0)
  {
    if (expected_blocks == 0u || expected_blocks > KAFS_V7_INODE_DIRECT_REFERENCE_COUNT ||
        le32toh(root.blocks) != expected_blocks)
      rc = -EINVAL;
  }

  size_t payload_bytes = (size_t)report.block_size * expected_blocks;
  uint8_t *block = rc == 0 ? malloc(payload_bytes) : NULL;
  if (rc == 0 && !block)
    rc = -ENOMEM;
  uint64_t physical_off[KAFS_V7_INODE_DIRECT_REFERENCE_COUNT] = {0};
  for (uint32_t block_id = 0u; rc == 0 && block_id < expected_blocks; ++block_id)
  {
    rc = inode_direct_block_offset(&report, &root, block_id, &physical_off[block_id]);
    if (rc == 0)
      rc = kafs_pread_all(fd, block + block_id * report.block_size, report.block_size,
                          (off_t)physical_off[block_id]);
  }
  kafs_v7_kdir_header_t *header = (kafs_v7_kdir_header_t *)block;
  size_t used = 0u;
  uint32_t tombstone_count = 0u;
  size_t required = KAFS_V7_KDIR_RECORD_PREFIX_BYTES + strlen(next_name);
  if (rc == 0 && (le32toh(header->magic) != KAFS_V7_KDIR_MAGIC ||
                  le16toh(header->version) != KAFS_V7_KDIR_VERSION))
    rc = -EUCLEAN;
  if (rc == 0)
  {
    tombstone_count = le32toh(header->tombstone_count);
    used = KAFS_V7_KDIR_HEADER_BYTES + le32toh(header->record_bytes);
    if (used > payload_bytes || required <= 1u || required > payload_bytes - used)
      rc = -EINVAL;
  }
  while (rc == 0 && payload_bytes - used >= required)
  {
    size_t record_bytes = payload_bytes - used - (required - 1u);
    if (record_bytes > KAFS_V7_KDIR_RECORD_PREFIX_BYTES + 255u)
      record_bytes = KAFS_V7_KDIR_RECORD_PREFIX_BYTES + 255u;
    if (record_bytes <= KAFS_V7_KDIR_RECORD_PREFIX_BYTES)
    {
      rc = -EINVAL;
      break;
    }
    size_t name_bytes = record_bytes - KAFS_V7_KDIR_RECORD_PREFIX_BYTES;
    char filler[256];
    memset(filler, 'p', name_bytes);
    filler[name_bytes] = '\0';
    size_t record_off = used;
    rc = append_dir_record(block, payload_bytes, &used, KAFS_INO_ROOTDIR, filler);
    if (rc == 0)
    {
      kafs_v7_kdir_record_t *record = (kafs_v7_kdir_record_t *)(block + record_off);
      record->flags = htole16(KAFS_V7_KDIR_FLAG_TOMBSTONE);
      if (tombstone_count == UINT32_MAX)
        rc = -EOVERFLOW;
      else
        tombstone_count++;
    }
  }
  if (rc == 0)
  {
    header->record_bytes = htole32((uint32_t)(used - KAFS_V7_KDIR_HEADER_BYTES));
    header->tombstone_count = htole32(tombstone_count);
    root.size = htole64(used);
    for (uint32_t i = 0u; rc == 0 && i < expected_blocks; ++i)
      rc = kafs_pwrite_all(fd, block + i * report.block_size, report.block_size,
                           (off_t)physical_off[i]);
  }
  if (rc == 0)
    rc = write_inode(fd, &report, KAFS_INO_ROOTDIR, &root);
  if (rc == 0 && fdatasync(fd) != 0)
    rc = -errno;
  free(block);
  kafs_v7_layout_report_clear(&report);
  close(fd);
  return rc;
}

static int publish_pending_uid(int fd, const kafs_ssuperblock_t *sb, uint64_t file_size,
                               kafs_v7_layout_report_t *report, kafs_v7_sequence_state_t *sequence,
                               uint32_t ino, uint16_t uid)
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
    rc =
        kafs_v7_journal_transaction_publish_fd(fd, report, &reservation, transaction, &publication);
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
  if (rc != 0)
    fprintf(stderr, "read mismatch path=%s used=%zu expected=%zu first=%u expected_first=%u\n",
            path, used, expected_bytes, used != 0u ? buf[0] : 0u,
            expected_bytes != 0u ? ((const uint8_t *)expected)[0] : 0u);
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
  snprintf(inline_path, sizeof(inline_path), "%s/inline", mnt);
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
      expect_erofs("unlink", unlink(block_path)) ||
      expect_erofs("rename", rename(block_path, new_path)) ||
      expect_erofs("link", link(block_path, new_path)) ||
      expect_erofs("mkdir", mkdir(new_path, 0777)) || expect_erofs("rmdir", rmdir(nested_path)) ||
      expect_erofs("chmod", chmod(block_path, 0600)) ||
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
  if (!directory_has(mnt, "nested") || !directory_has(mnt, "inline") ||
      !directory_has(mnt, "block") || !directory_has(mnt, "link"))
    rc = -1;
  snprintf(path, sizeof(path), "%s/nested", mnt);
  if (rc == 0 && (stat(path, &st) != 0 || !S_ISDIR(st.st_mode)))
    rc = -1;
  snprintf(path, sizeof(path), "%s/inline", mnt);
  if (rc == 0 && read_equals(path, k_inline_payload) != 0)
    rc = -1;
  snprintf(path, sizeof(path), "%s/block", mnt);
  if (rc == 0)
  {
    uint8_t *expected = calloc(3u, fixture->block_size);
    if (!expected)
      rc = -1;
    else
    {
      memcpy(expected, k_block_payload, strlen(k_block_payload));
      if (read_block_equals(path, expected, 3u * fixture->block_size) != 0)
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
  if (rc == 0 && check_fuse_contract_log(log_path, "inspection", fixture->block_size, 0) != 0)
    rc = -1;
  uint64_t after = 0;
  if (file_digest(image, &after) != 0 || after != before)
    rc = -1;
  return rc;
}

static int run_fsck(const char *image)
{
  char output[8192];
  char *argv[] = {(char *)kafs_test_fsck_bin(), (char *)"--check", (char *)image, NULL};
  int rc = run_command(argv, 0, output, sizeof(output));
  if (rc != 0)
    fprintf(stderr, "fsck failed for %s:\n%s\n", image, output);
  return rc;
}

static int check_persisted_blocks(const char *image, uint32_t ino, const void *expected,
                                  uint64_t expected_size, uint32_t block_size, uint32_t block_count)
{
  int fd = open(image, O_RDONLY);
  kafs_ssuperblock_t sb;
  uint64_t file_size = 0;
  kafs_v7_layout_report_t report = {0};
  int rc = fd < 0 ? -errno : validate_image_fd(fd, &sb, &file_size, &report);
  kafs_v7_inode_t inode;
  if (rc == 0)
    rc = read_inode(fd, &report, ino, &inode);
  if (rc == 0 && (le64toh(inode.size) != expected_size || le32toh(inode.blocks) != block_count))
    rc = -EUCLEAN;
  uint8_t *actual = rc == 0 && block_count != 0u ? malloc((size_t)block_size * block_count) : NULL;
  if (rc == 0 && block_count != 0u && !actual)
    rc = -ENOMEM;
  const kafs_v7_group_desc_t *groups = kafs_v7_report_groups(&report);
  for (uint32_t slot = 0; rc == 0 && slot < block_count; ++slot)
  {
    uint32_t reference = 0u;
    memcpy(&reference, inode.inline_or_block_refs + slot * sizeof(reference), sizeof(reference));
    reference = le32toh(reference);
    if (reference == 0u)
    {
      rc = -ENOENT;
      break;
    }
    uint64_t logical = (uint64_t)reference - 1u;
    rc = -ERANGE;
    for (uint32_t i = 0; i < report.group_count; ++i)
    {
      uint64_t start = le64toh(groups[i].data_logical_start);
      uint64_t count = le64toh(groups[i].data_logical_count);
      if (logical < start || logical - start >= count)
        continue;
      uint64_t off = le64toh(groups[i].data_physical_off) + (logical - start) * block_size;
      rc = kafs_pread_all(fd, actual + (size_t)slot * block_size, block_size, (off_t)off);
      break;
    }
  }
  if (rc == 0 && block_count != 0u &&
      memcmp(actual, expected, (size_t)block_size * block_count) != 0)
  {
    size_t bytes = (size_t)block_size * block_count;
    size_t mismatch = 0u;
    while (mismatch < bytes && actual[mismatch] == ((const uint8_t *)expected)[mismatch])
      ++mismatch;
    fprintf(stderr, "persisted block mismatch offset=%zu actual=%u expected=%u\n", mismatch,
            mismatch < bytes ? actual[mismatch] : 0u,
            mismatch < bytes ? ((const uint8_t *)expected)[mismatch] : 0u);
    rc = -EIO;
  }
  free(actual);
  kafs_v7_layout_report_clear(&report);
  if (fd >= 0)
    close(fd);
  return rc;
}

static int check_controlled_write_mount(const char *image, uint32_t ino, uint32_t block_size)
{
  const size_t created_inline_bytes = sizeof("v7-created-file") - 1u;
  const char *mnt = "mnt-controlled";
  const char *log_path = "v7-controlled-write.log";
  kafs_test_mount_options_t options = {
      .log_path = log_path,
      .extra_options = "rw,no_writeback_cache,no_trim_on_free,bg_dedup_scan=off,fsync_policy=full",
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
  uint8_t *payload = malloc(4u * block_size);
  const size_t multi_bytes = (size_t)block_size + 53u;
  uint8_t *multi = malloc(multi_bytes);
  int rc = fd < 0 || !payload || !multi ? -1 : 0;
  if (rc == 0)
  {
    memset(payload, 0, 4u * block_size);
    memcpy(payload, k_block_payload, strlen(k_block_payload));
  }

  const size_t partial_offset = 31u;
  const size_t partial_bytes = 97u;
  uint8_t partial[97];
  for (size_t i = 0u; i < partial_bytes; ++i)
    partial[i] = (uint8_t)(i * 17u + 3u);
  if (rc == 0 && pwrite(fd, partial, partial_bytes, (off_t)partial_offset) !=
                     (ssize_t)partial_bytes)
  {
    fprintf(stderr, "partial controlled overwrite failed: errno=%d\n", errno);
    rc = -1;
  }
  if (rc == 0)
    memcpy(payload + partial_offset, partial, partial_bytes);
  if (rc == 0 && fsync(fd) != 0)
  {
    fprintf(stderr, "partial controlled overwrite fsync failed: errno=%d\n", errno);
    rc = -1;
  }
  if (rc == 0)
    qualification_case_pass("direct_partial_overwrite");

  const size_t multi_offset = (size_t)block_size - 17u;
  if (rc == 0)
  {
    for (size_t i = 0u; i < multi_bytes; ++i)
      multi[i] = (uint8_t)(i * 29u + 7u);
    if (pwrite(fd, multi, multi_bytes, (off_t)multi_offset) != (ssize_t)multi_bytes)
    {
      fprintf(stderr, "multi-block controlled overwrite failed: errno=%d\n", errno);
      rc = -1;
    }
  }
  if (rc == 0)
    memcpy(payload + multi_offset, multi, multi_bytes);
  if (rc == 0 && fsync(fd) != 0)
  {
    fprintf(stderr, "multi-block controlled overwrite fsync failed: errno=%d\n", errno);
    rc = -1;
  }
  if (rc == 0)
    qualification_case_pass("direct_multi_block_overwrite");

  const size_t growth_offset = 3u * (size_t)block_size;
  const size_t growth_bytes = block_size;
  if (rc == 0)
  {
    for (size_t i = 0u; i < growth_bytes; ++i)
      payload[growth_offset + i] = (uint8_t)(i * 31u + 11u);
    if (pwrite(fd, payload + growth_offset, growth_bytes, (off_t)growth_offset) !=
        (ssize_t)growth_bytes)
    {
      fprintf(stderr, "controlled direct growth failed: errno=%d\n", errno);
      rc = -1;
    }
  }
  if (rc == 0 && fsync(fd) != 0)
  {
    fprintf(stderr, "controlled direct growth fsync failed: errno=%d\n", errno);
    rc = -1;
  }
  if (rc == 0)
    qualification_case_pass("direct_growth");

  const uint64_t truncate_size = 2u * (uint64_t)block_size + 31u;
  if (rc == 0 && ftruncate(fd, (off_t)truncate_size) != 0)
  {
    fprintf(stderr, "controlled truncate failed: errno=%d\n", errno);
    rc = -1;
  }
  if (rc == 0 && fsync(fd) != 0)
  {
    fprintf(stderr, "controlled truncate fsync failed: errno=%d\n", errno);
    rc = -1;
  }
  if (rc == 0)
    memset(payload + truncate_size, 0, 3u * block_size - (size_t)truncate_size);
  if (rc == 0)
    qualification_case_pass("direct_truncate");
  if (fd >= 0 && close(fd) != 0 && rc == 0)
    rc = -1;
  if (rc == 0)
  {
    snprintf(path, sizeof(path), "%s/created", mnt);
    int created_fd = open(path, O_WRONLY | O_CREAT | O_EXCL, 0640);
    if (created_fd < 0)
    {
      fprintf(stderr, "controlled create failed: errno=%d\n", errno);
      rc = -1;
    }
    if (rc == 0 && write(created_fd, k_promoted_payload, created_inline_bytes) !=
                       (ssize_t)created_inline_bytes)
    {
      fprintf(stderr, "controlled created-file write failed: errno=%d\n", errno);
      rc = -1;
    }
    if (rc == 0 && fsync(created_fd) != 0)
    {
      fprintf(stderr, "controlled created-file fsync failed: errno=%d\n", errno);
      rc = -1;
    }
    if (rc == 0)
      qualification_case_pass("create_inline_write");
    if (rc == 0 &&
        write(created_fd, k_promoted_payload + created_inline_bytes,
              sizeof(k_promoted_payload) - 1u - created_inline_bytes) !=
            (ssize_t)(sizeof(k_promoted_payload) - 1u - created_inline_bytes))
    {
      fprintf(stderr, "controlled inline-file promotion write failed: errno=%d\n", errno);
      rc = -1;
    }
    if (rc == 0 && fsync(created_fd) != 0)
    {
      fprintf(stderr, "controlled inline-file promotion fsync failed: errno=%d\n", errno);
      rc = -1;
    }
    if (rc == 0)
      qualification_case_pass("regular_inline_promotion");
    errno = 0;
    if (rc == 0 &&
        (ftruncate(created_fd, (off_t)created_inline_bytes) == 0 || errno != EOPNOTSUPP))
    {
      fprintf(stderr, "controlled direct-to-inline truncate did not fail closed: errno=%d\n",
              errno);
      rc = -1;
    }
    if (rc == 0)
      qualification_case_pass("direct_to_inline_truncate_rejection");
    if (created_fd >= 0 && close(created_fd) != 0 && rc == 0)
      rc = -1;
  }
  if (rc == 0)
  {
    snprintf(path, sizeof(path), "%s/nested/x", mnt);
    int inline_created_fd = open(path, O_WRONLY | O_CREAT | O_EXCL, 0600);
    if (inline_created_fd < 0 || close(inline_created_fd) != 0)
    {
      fprintf(stderr, "controlled inline-parent create failed: errno=%d\n", errno);
      rc = -1;
    }
    if (rc == 0)
      qualification_case_pass("directory_inline_append");
  }
  if (rc == 0)
  {
    snprintf(path, sizeof(path), "%s/nested/g", mnt);
    int grown_created_fd = open(path, O_WRONLY | O_CREAT | O_EXCL, 0600);
    if (grown_created_fd < 0 || close(grown_created_fd) != 0)
    {
      fprintf(stderr, "controlled inline-directory growth create failed: errno=%d\n", errno);
      rc = -1;
    }
    if (rc == 0)
      qualification_case_pass("directory_inline_growth");
  }
  kafs_test_stop_kafs(mnt, pid);
  if (rc == 0 && check_fuse_contract_log(log_path, "controlled-write", block_size, 1) != 0)
  {
    fprintf(stderr, "controlled write FUSE contract diagnostic failed\n");
    rc = -1;
  }
  if (rc != 0)
    kafs_test_dump_log(log_path, "v7 controlled-write operation failed");

  if (rc == 0 && run_fsck(image) != 0)
    rc = -1;
  if (rc == 0 && check_persisted_blocks(image, ino, payload, truncate_size, block_size, 3u) != 0)
  {
    fprintf(stderr, "controlled persisted block check failed\n");
    rc = -1;
  }
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
      if (read_block_equals(path, payload, (size_t)truncate_size) != 0)
      {
        fprintf(stderr, "controlled block remount read failed\n");
        rc = -1;
      }
      snprintf(path, sizeof(path), "%s/created", "mnt-controlled-remount");
      struct stat created_st;
      if (rc == 0 && (stat(path, &created_st) != 0 || !S_ISREG(created_st.st_mode) ||
                      created_st.st_size != (off_t)(sizeof(k_promoted_payload) - 1u)))
      {
        fprintf(stderr, "controlled created file remount stat failed: errno=%d size=%jd\n", errno,
                stat(path, &created_st) == 0 ? (intmax_t)created_st.st_size : (intmax_t)-1);
        rc = -1;
      }
      if (rc == 0 &&
          read_block_equals(path, k_promoted_payload, sizeof(k_promoted_payload) - 1u) != 0)
      {
        fprintf(stderr, "controlled created file remount payload failed\n");
        rc = -1;
      }
      snprintf(path, sizeof(path), "%s/nested/x", "mnt-controlled-remount");
      if (rc == 0 &&
          (stat(path, &created_st) != 0 || !S_ISREG(created_st.st_mode) || created_st.st_size != 0))
      {
        fprintf(stderr, "controlled inline-parent file remount stat failed\n");
        rc = -1;
      }
      snprintf(path, sizeof(path), "%s/nested/g", "mnt-controlled-remount");
      if (rc == 0 &&
          (stat(path, &created_st) != 0 || !S_ISREG(created_st.st_mode) || created_st.st_size != 0))
      {
        fprintf(stderr, "controlled grown-directory file remount stat failed\n");
        rc = -1;
      }
      kafs_test_stop_kafs("mnt-controlled-remount", pid);
      if (rc == 0)
        qualification_case_pass("inspection_remount");
    }
  }
  if (rc == 0)
  {
    kafs_test_mount_options_t truncate_options = {
        .log_path = "v7-controlled-open-trunc.log",
        .extra_options =
            "rw,no_writeback_cache,no_trim_on_free,bg_dedup_scan=off,fsync_policy=full",
        .timeout_ms = 15000,
    };
    pid = kafs_test_start_kafs_v7_controlled_write(image, "mnt-controlled-open-trunc",
                                                   &truncate_options);
    if (pid <= 0)
      rc = -1;
    else
    {
      snprintf(path, sizeof(path), "%s/block", "mnt-controlled-open-trunc");
      int truncate_fd = open(path, O_WRONLY | O_TRUNC);
      if (truncate_fd < 0 || close(truncate_fd) != 0)
        rc = -1;
      kafs_test_stop_kafs("mnt-controlled-open-trunc", pid);
      if (rc == 0)
        qualification_case_pass("open_truncate");
    }
  }
  if (rc == 0 && run_fsck(image) != 0)
    rc = -1;
  if (rc == 0 && check_persisted_blocks(image, ino, NULL, 0u, block_size, 0u) != 0)
    rc = -1;
  free(multi);
  free(payload);
  return rc;
}

static int check_controlled_write_recovery(const char *image, uint32_t ino, uint32_t block_size,
                                           kafs_v7_test_fault_point_t fault)
{
  const char *mnt = "mnt-controlled-crash";
  char crash_log[PATH_MAX];
  char recovery_log[PATH_MAX];
  snprintf(crash_log, sizeof(crash_log), "v7-%s-crash.log", kafs_v7_test_fault_name(fault));
  snprintf(recovery_log, sizeof(recovery_log), "v7-%s-recovery.log",
           kafs_v7_test_fault_name(fault));
  kafs_test_mount_options_t options = {
      .log_path = crash_log,
      .extra_options = "rw,no_writeback_cache,no_trim_on_free,bg_dedup_scan=off,fsync_policy=full",
      .timeout_ms = 15000,
  };
  if (setenv(KAFS_V7_TEST_CRASH_POINT_ENV, kafs_v7_test_fault_name(fault), 1) != 0)
    return -1;
  pid_t pid = kafs_test_start_kafs_v7_controlled_write(image, mnt, &options);
  unsetenv(KAFS_V7_TEST_CRASH_POINT_ENV);
  if (pid <= 0)
    return -1;

  uint8_t *payload = malloc(2u * block_size);
  const uint64_t truncate_size = (uint64_t)block_size + 53u;
  char path[PATH_MAX];
  snprintf(path, sizeof(path), "%s/block", mnt);
  int fd = open(path, O_RDWR);
  int rc = fd < 0 || !payload ? -1 : 0;
  if (rc == 0)
  {
    memset(payload, 0, 2u * block_size);
    memcpy(payload, k_block_payload, strlen(k_block_payload));
    if (ftruncate(fd, (off_t)truncate_size) >= 0)
      rc = -1;
  }
  if (fd >= 0)
    close(fd);
  int status = 0;
  if (rc == 0 && (waitpid(pid, &status, 0) != pid || !WIFEXITED(status) ||
                  WEXITSTATUS(status) != kafs_v7_test_fault_exit_status(fault)))
    rc = -1;
  kafs_test_stop_kafs(mnt, pid);

  options.log_path = recovery_log;
  pid = rc == 0
            ? kafs_test_start_kafs_v7_controlled_write(image, "mnt-controlled-recovery", &options)
            : -1;
  if (pid <= 0)
    rc = -1;
  else
    kafs_test_stop_kafs("mnt-controlled-recovery", pid);
  if (rc == 0 && check_recovery_log(recovery_log, fault, 3u, 1u) != 0)
  {
    fprintf(stderr, "truncate recovery diagnostic failed fault=%s\n",
            kafs_v7_test_fault_name(fault));
    rc = -1;
  }
  if (rc == 0 && run_fsck(image) != 0)
  {
    fprintf(stderr, "truncate recovery fsck failed fault=%s\n", kafs_v7_test_fault_name(fault));
    rc = -1;
  }
  if (rc == 0 && check_persisted_blocks(image, ino, payload, truncate_size, block_size, 2u) != 0)
  {
    fprintf(stderr, "truncate recovery payload failed fault=%s\n", kafs_v7_test_fault_name(fault));
    rc = -1;
  }
  if (rc != 0)
  {
    kafs_test_dump_log(crash_log, "v7 controlled-write crash failed");
    kafs_test_dump_log(recovery_log, "v7 controlled-write recovery failed");
  }
  free(payload);
  return rc;
}

static int check_controlled_inline_promotion_recovery(const char *image,
                                                      kafs_v7_test_fault_point_t fault)
{
  char crash_log[PATH_MAX];
  char recovery_log[PATH_MAX];
  snprintf(crash_log, sizeof(crash_log), "v7-inline-promotion-%s-crash.log",
           kafs_v7_test_fault_name(fault));
  snprintf(recovery_log, sizeof(recovery_log), "v7-inline-promotion-%s-recovery.log",
           kafs_v7_test_fault_name(fault));
  kafs_test_mount_options_t options = {
      .log_path = crash_log,
      .extra_options = "rw,no_writeback_cache,no_trim_on_free,bg_dedup_scan=off,fsync_policy=full",
      .timeout_ms = 15000,
  };
  if (setenv(KAFS_V7_TEST_CRASH_POINT_ENV, kafs_v7_test_fault_name(fault), 1) != 0)
    return -1;
  pid_t pid = kafs_test_start_kafs_v7_controlled_write(image, "mnt-inline-promotion-crash",
                                                       &options);
  unsetenv(KAFS_V7_TEST_CRASH_POINT_ENV);
  if (pid <= 0)
    return -1;

  char path[PATH_MAX];
  snprintf(path, sizeof(path), "%s/inline", "mnt-inline-promotion-crash");
  int fd = open(path, O_WRONLY);
  ssize_t written = fd >= 0 ? write(fd, k_promoted_payload, sizeof(k_promoted_payload) - 1u) : -1;
  int rc = fd >= 0 && written != (ssize_t)(sizeof(k_promoted_payload) - 1u) ? 0 : -1;
  if (fd >= 0)
    close(fd);
  int status = 0;
  if (rc == 0 && (waitpid(pid, &status, 0) != pid || !WIFEXITED(status) ||
                  WEXITSTATUS(status) != kafs_v7_test_fault_exit_status(fault)))
    rc = -1;
  kafs_test_stop_kafs("mnt-inline-promotion-crash", pid);

  options.log_path = recovery_log;
  pid = rc == 0 ? kafs_test_start_kafs_v7_controlled_write(
                     image, "mnt-inline-promotion-recovery", &options)
               : -1;
  if (pid <= 0)
    rc = -1;
  else
  {
    struct stat st = {0};
    snprintf(path, sizeof(path), "%s/inline", "mnt-inline-promotion-recovery");
    if (stat(path, &st) != 0 || !S_ISREG(st.st_mode) ||
        st.st_size != (off_t)(sizeof(k_promoted_payload) - 1u) ||
        read_block_equals(path, k_promoted_payload, sizeof(k_promoted_payload) - 1u) != 0)
      rc = -1;
    kafs_test_stop_kafs("mnt-inline-promotion-recovery", pid);
  }
  if (rc == 0 && check_recovery_log(recovery_log, fault, 3u, 1u) != 0)
    rc = -1;
  if (rc == 0 && run_fsck(image) != 0)
    rc = -1;
  if (rc != 0)
  {
    fprintf(stderr, "inline promotion recovery failed fault=%s written=%zd status=%d\n",
            kafs_v7_test_fault_name(fault), written, status);
    kafs_test_dump_log(crash_log, "v7 inline promotion crash failed");
    kafs_test_dump_log(recovery_log, "v7 inline promotion recovery failed");
  }
  return rc;
}

static int check_controlled_create_recovery(const char *image, kafs_v7_test_fault_point_t fault,
                                            const char *scenario, const char *relative_path,
                                            uint64_t mutation_count, uint64_t preapplied_count)
{
  char crash_log[PATH_MAX];
  char recovery_log[PATH_MAX];
  snprintf(crash_log, sizeof(crash_log), "v7-create-%s-%s-crash.log", scenario,
           kafs_v7_test_fault_name(fault));
  snprintf(recovery_log, sizeof(recovery_log), "v7-create-%s-%s-recovery.log", scenario,
           kafs_v7_test_fault_name(fault));
  kafs_test_mount_options_t options = {
      .log_path = crash_log,
      .extra_options = "rw,no_writeback_cache,no_trim_on_free,bg_dedup_scan=off,fsync_policy=full",
      .timeout_ms = 15000,
  };
  if (setenv(KAFS_V7_TEST_CRASH_POINT_ENV, kafs_v7_test_fault_name(fault), 1) != 0)
    return -1;
  pid_t pid = kafs_test_start_kafs_v7_controlled_write(image, "mnt-create-crash", &options);
  unsetenv(KAFS_V7_TEST_CRASH_POINT_ENV);
  if (pid <= 0)
    return -1;
  char created_path[PATH_MAX];
  snprintf(created_path, sizeof(created_path), "mnt-create-crash/%s", relative_path);
  int fd = open(created_path, O_WRONLY | O_CREAT | O_EXCL, 0640);
  int rc = fd >= 0 ? -1 : 0;
  int open_errno = errno;
  if (fd >= 0)
    close(fd);
  int status = 0;
  if (rc == 0 && (waitpid(pid, &status, 0) != pid || !WIFEXITED(status) ||
                  WEXITSTATUS(status) != kafs_v7_test_fault_exit_status(fault)))
    rc = -1;
  if (rc != 0)
    fprintf(stderr, "create crash mismatch path=%s fd=%d errno=%d status=%d expected=%d\n",
            relative_path, fd, open_errno, status, kafs_v7_test_fault_exit_status(fault));
  kafs_test_stop_kafs("mnt-create-crash", pid);

  options.log_path = recovery_log;
  pid = rc == 0 ? kafs_test_start_kafs_v7_controlled_write(image, "mnt-create-recovery", &options)
                : -1;
  if (pid <= 0)
    rc = -1;
  else
  {
    struct stat st = {0};
    snprintf(created_path, sizeof(created_path), "mnt-create-recovery/%s", relative_path);
    if (stat(created_path, &st) != 0 || !S_ISREG(st.st_mode) || st.st_size != 0)
    {
      fprintf(stderr, "create recovery readback mismatch path=%s errno=%d mode=%o size=%jd\n",
              relative_path, errno, (unsigned)st.st_mode, (intmax_t)st.st_size);
      rc = -1;
    }
    kafs_test_stop_kafs("mnt-create-recovery", pid);
  }
  if (rc == 0 && check_recovery_log(recovery_log, fault, mutation_count, preapplied_count) != 0)
    rc = -1;
  if (rc == 0 && run_fsck(image) != 0)
    rc = -1;
  if (rc != 0)
  {
    kafs_test_dump_log(crash_log, "v7 create crash failed");
    kafs_test_dump_log(recovery_log, "v7 create recovery failed");
  }
  return rc;
}

static int check_controlled_create_success(const char *image, const char *relative_path)
{
  kafs_test_mount_options_t options = {
      .log_path = "v7-create-success.log",
      .extra_options = "rw,no_writeback_cache,no_trim_on_free,bg_dedup_scan=off,fsync_policy=full",
      .timeout_ms = 15000,
  };
  pid_t pid = kafs_test_start_kafs_v7_controlled_write(image, "mnt-create-success", &options);
  if (pid <= 0)
    return -1;
  char created_path[PATH_MAX];
  snprintf(created_path, sizeof(created_path), "mnt-create-success/%s", relative_path);
  int fd = open(created_path, O_WRONLY | O_CREAT | O_EXCL, 0640);
  int rc = fd < 0 ? -1 : 0;
  if (fd >= 0 && close(fd) != 0)
    rc = -1;
  if (rc == 0)
  {
    struct stat st;
    if (stat(created_path, &st) != 0 || !S_ISREG(st.st_mode) || st.st_size != 0)
      rc = -1;
  }
  kafs_test_stop_kafs("mnt-create-success", pid);
  if (rc == 0 && run_fsck(image) != 0)
    rc = -1;
  if (rc != 0)
    kafs_test_dump_log(options.log_path, "v7 create success failed");
  return rc;
}

static int build_direct_directory_fixture(const char *source_image, const char *fixture_image,
                                          uint32_t target_blocks)
{
  if (target_blocks == 0u || target_blocks > KAFS_V7_INODE_DIRECT_REFERENCE_COUNT)
    return -EINVAL;
  int rc = copy_image(source_image, fixture_image);
  for (uint32_t blocks = 1u; rc == 0 && blocks < target_blocks; ++blocks)
  {
    char name[32];
    snprintf(name, sizeof(name), "fixture-grow-%u", blocks);
    rc = seed_full_root_directory(fixture_image, name, blocks);
    if (rc == 0)
      rc = check_controlled_create_success(fixture_image, name);
  }
  return rc;
}

static int read_root_state(const char *image, kafs_v7_inode_t *root, uint64_t *free_blocks,
                           uint64_t *free_inodes)
{
  int fd = open(image, O_RDONLY);
  if (fd < 0)
    return -errno;
  kafs_ssuperblock_t sb;
  uint64_t file_size = 0u;
  kafs_v7_layout_report_t report = {0};
  int rc = validate_image_fd(fd, &sb, &file_size, &report);
  if (rc == 0)
    rc = read_inode(fd, &report, KAFS_INO_ROOTDIR, root);
  if (rc == 0)
  {
    *free_blocks = report.free_blocks;
    *free_inodes = report.free_inodes;
  }
  kafs_v7_layout_report_clear(&report);
  close(fd);
  return rc;
}

static int check_controlled_create_limit_rejection(const char *image, const char *relative_path)
{
  kafs_v7_inode_t before;
  uint64_t before_free_blocks = 0u;
  uint64_t before_free_inodes = 0u;
  int rc = read_root_state(image, &before, &before_free_blocks, &before_free_inodes);
  kafs_test_mount_options_t options = {
      .log_path = "v7-create-limit-rejection.log",
      .extra_options = "rw,no_writeback_cache,no_trim_on_free,bg_dedup_scan=off,fsync_policy=full",
      .timeout_ms = 15000,
  };
  pid_t pid = rc == 0
                  ? kafs_test_start_kafs_v7_controlled_write(image, "mnt-create-limit", &options)
                  : -1;
  if (pid <= 0)
    rc = -1;
  char created_path[PATH_MAX];
  snprintf(created_path, sizeof(created_path), "mnt-create-limit/%s", relative_path);
  if (rc == 0)
  {
    int fd = open(created_path, O_WRONLY | O_CREAT | O_EXCL, 0640);
    if (fd >= 0 || errno != ENOSPC)
      rc = -1;
    if (fd >= 0)
      close(fd);
  }
  if (pid > 0)
    kafs_test_stop_kafs("mnt-create-limit", pid);

  kafs_v7_inode_t after;
  uint64_t after_free_blocks = 0u;
  uint64_t after_free_inodes = 0u;
  if (rc == 0)
    rc = read_root_state(image, &after, &after_free_blocks, &after_free_inodes);
  if (rc == 0 && (memcmp(&before, &after, sizeof(before)) != 0 ||
                  before_free_blocks != after_free_blocks || before_free_inodes != after_free_inodes))
    rc = -1;
  if (rc == 0)
    rc = run_fsck(image);
  if (rc != 0)
    kafs_test_dump_log(options.log_path, "v7 direct-limit rejection failed");
  return rc;
}

typedef struct create_recovery_case
{
  const char *transition;
  const char *source_image;
  const char *relative_path;
  const char *fill_for_name;
  uint32_t fill_block_count;
  uint64_t mutation_count;
  uint64_t preapplied_count;
} create_recovery_case_t;

static int run_create_recovery_case(const create_recovery_case_t *test,
                                    kafs_v7_test_fault_point_t fault, size_t fault_id,
                                    size_t case_id)
{
  char recovery_image[PATH_MAX];
  snprintf(recovery_image, sizeof(recovery_image), "v7-create-recovery-%zu-%zu.img", fault_id,
           case_id);
  int rc = copy_image(test->source_image, recovery_image);
  if (rc == 0 && test->fill_for_name)
    rc = seed_full_root_directory(recovery_image, test->fill_for_name, test->fill_block_count);
  if (rc == 0)
    rc = check_controlled_create_recovery(recovery_image, fault, test->transition,
                                          test->relative_path, test->mutation_count,
                                          test->preapplied_count);
  if (rc != 0)
    fprintf(stderr, "v7 controlled create recovery failed transition=%s fault=%s\n",
            test->transition, kafs_v7_test_fault_name(fault));
  return rc;
}

static int check_controlled_reclaim_recovery(const char *image)
{
  int early_exit_status = -1;
  kafs_test_mount_options_t options = {
      .log_path = "v7-controlled-reclaim-crash.log",
      .extra_options = "rw,no_writeback_cache,no_trim_on_free,bg_dedup_scan=off,fsync_policy=full",
      .timeout_ms = 15000,
      .early_exit_status = &early_exit_status,
  };
  if (setenv(KAFS_V7_TEST_CRASH_POINT_ENV,
             kafs_v7_test_fault_name(KAFS_V7_TEST_FAULT_JOURNAL_RECLAIM), 1) != 0)
    return -1;
  pid_t pid = kafs_test_start_kafs_v7_controlled_write(image, "mnt-reclaim-crash", &options);
  unsetenv(KAFS_V7_TEST_CRASH_POINT_ENV);
  if (pid > 0)
  {
    kafs_test_stop_kafs("mnt-reclaim-crash", pid);
    return -1;
  }
  if (early_exit_status != kafs_v7_test_fault_exit_status(KAFS_V7_TEST_FAULT_JOURNAL_RECLAIM))
    return -1;
  if (check_nonempty_journal_count(image, 1u) != 0)
    return -1;

  options.log_path = "v7-controlled-reclaim-recovery.log";
  pid = kafs_test_start_kafs_v7_controlled_write(image, "mnt-reclaim-recovery", &options);
  if (pid <= 0)
    return -1;
  kafs_test_stop_kafs("mnt-reclaim-recovery", pid);
  return check_nonempty_journal_count(image, 0u) == 0 && run_fsck(image) == 0 &&
                 check_recovery_log("v7-controlled-reclaim-recovery.log",
                                    KAFS_V7_TEST_FAULT_JOURNAL_RECLAIM, 0u, 0u) == 0
             ? 0
             : -1;
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
    header->descriptor_crc32 = htole32(kafs_v7_crc32(report.descriptor, report.descriptor_bytes));
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
  qualification_case_pass("format_and_seed");
  if (test_namespace_payload_faults(image, &fixture) != 0)
  {
    fprintf(stderr, "v7 namespace payload fault matrix failed\n");
    return 1;
  }
  if (test_direct_inode_faults(image, &fixture) != 0)
  {
    fprintf(stderr, "v7 direct inode fault matrix failed\n");
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
  qualification_case_pass("inspection_mount");

  const char *controlled = "v7-controlled.img";
  if (copy_image(image, controlled) != 0 ||
      check_controlled_write_mount(controlled, fixture.block_ino, fixture.block_size) != 0)
  {
    fprintf(stderr, "v7 controlled-write mount matrix failed\n");
    return 1;
  }
  qualification_case_pass("controlled_write_normal_matrix");

  const char *recovery = "v7-controlled-recovery.img";
  if (copy_image(image, recovery) != 0 ||
      check_controlled_write_recovery(recovery, fixture.block_ino, fixture.block_size,
                                      KAFS_V7_TEST_FAULT_JOURNAL_PUBLISH) != 0)
  {
    fprintf(stderr, "v7 controlled-write journal recovery failed\n");
    return 1;
  }
  qualification_case_pass("journal_publish_recovery");

  const char *apply_recovery = "v7-controlled-apply-recovery.img";
  if (copy_image(image, apply_recovery) != 0 ||
      check_controlled_write_recovery(apply_recovery, fixture.block_ino, fixture.block_size,
                                      KAFS_V7_TEST_FAULT_METADATA_APPLY) != 0)
  {
    fprintf(stderr, "v7 controlled-write metadata apply recovery failed\n");
    return 1;
  }
  qualification_case_pass("metadata_apply_recovery");

  const char *checkpoint_recovery = "v7-controlled-checkpoint-recovery.img";
  if (copy_image(image, checkpoint_recovery) != 0 ||
      check_controlled_write_recovery(checkpoint_recovery, fixture.block_ino, fixture.block_size,
                                      KAFS_V7_TEST_FAULT_CHECKPOINT_COPY) != 0)
  {
    fprintf(stderr, "v7 controlled-write checkpoint recovery failed\n");
    return 1;
  }
  qualification_case_pass("checkpoint_copy_recovery");

  const char *reclaim_recovery = "v7-controlled-reclaim-recovery.img";
  if (copy_image(image, reclaim_recovery) != 0 ||
      seed_pending_journals(reclaim_recovery, fixture.nested_ino, fixture.inline_ino) != 0 ||
      check_controlled_reclaim_recovery(reclaim_recovery) != 0)
  {
    fprintf(stderr, "v7 controlled-write journal reclaim recovery failed\n");
    return 1;
  }
  qualification_case_pass("journal_reclaim_recovery");

  const kafs_v7_test_fault_point_t mutation_faults[] = {
      KAFS_V7_TEST_FAULT_JOURNAL_PUBLISH,
      KAFS_V7_TEST_FAULT_METADATA_APPLY,
      KAFS_V7_TEST_FAULT_CHECKPOINT_COPY,
  };
  for (size_t i = 0u; i < sizeof(mutation_faults) / sizeof(mutation_faults[0]); ++i)
  {
    char promotion_recovery[PATH_MAX];
    snprintf(promotion_recovery, sizeof(promotion_recovery),
             "v7-inline-promotion-recovery-%zu.img", i);
    if (copy_image(image, promotion_recovery) != 0 ||
        check_controlled_inline_promotion_recovery(promotion_recovery, mutation_faults[i]) != 0)
      return 1;
  }
  qualification_case_pass("regular_inline_promotion_recovery");
  const char *direct_min_growth = "v7-direct-min-growth.img";
  if (copy_image(image, direct_min_growth) != 0 ||
      seed_full_root_directory(direct_min_growth, "expanded", 1u) != 0 ||
      check_controlled_create_success(direct_min_growth, "expanded") != 0)
  {
    fprintf(stderr, "v7 controlled direct-min growth setup failed\n");
    return 1;
  }
  const char *direct_interior_full = "v7-direct-interior-full.img";
  const char *direct_interior_growth = "v7-direct-interior-growth.img";
  if (copy_image(direct_min_growth, direct_interior_full) != 0 ||
      seed_full_root_directory(direct_interior_full, "third", 2u) != 0 ||
      copy_image(direct_interior_full, direct_interior_growth) != 0 ||
      check_controlled_create_success(direct_interior_growth, "third") != 0)
  {
    fprintf(stderr, "v7 controlled direct-interior growth setup failed\n");
    return 1;
  }
  const char *direct_limit_minus_one = "v7-direct-limit-minus-one.img";
  const char *direct_limit_minus_one_full = "v7-direct-limit-minus-one-full.img";
  const char *direct_limit = "v7-direct-limit.img";
  const char *direct_limit_full = "v7-direct-limit-full.img";
  if (build_direct_directory_fixture(image, direct_limit_minus_one,
                                     KAFS_V7_INODE_DIRECT_REFERENCE_COUNT - 1u) != 0 ||
      copy_image(direct_limit_minus_one, direct_limit_minus_one_full) != 0 ||
      seed_full_root_directory(direct_limit_minus_one_full, "limit-growth",
                               KAFS_V7_INODE_DIRECT_REFERENCE_COUNT - 1u) != 0 ||
      copy_image(direct_limit_minus_one_full, direct_limit) != 0 ||
      check_controlled_create_success(direct_limit, "limit-growth") != 0 ||
      copy_image(direct_limit, direct_limit_full) != 0 ||
      seed_full_root_directory(direct_limit_full, "limit-rejected",
                               KAFS_V7_INODE_DIRECT_REFERENCE_COUNT) != 0 ||
      check_controlled_create_limit_rejection(direct_limit_full, "limit-rejected") != 0)
  {
    fprintf(stderr, "v7 controlled direct-limit setup failed\n");
    return 1;
  }
  qualification_case_pass("directory_transition_normal_matrix");
  qualification_case_pass("direct_limit_rejection");
  const create_recovery_case_t create_cases[] = {
      {.transition = "direct-append",
       .source_image = image,
       .relative_path = "recovered",
       .mutation_count = 4u,
       .preapplied_count = 1u},
      {.transition = "inline-append",
       .source_image = image,
       .relative_path = "nested/y",
       .mutation_count = 2u},
      {.transition = "inline-growth",
       .source_image = image,
       .relative_path = "nested/growing",
       .mutation_count = 4u,
       .preapplied_count = 1u},
      {.transition = "direct-growth-min",
       .source_image = image,
       .relative_path = "expanded",
       .fill_for_name = "expanded",
       .fill_block_count = 1u,
       .mutation_count = 4u,
       .preapplied_count = 1u},
      {.transition = "direct-append-interior",
       .source_image = direct_min_growth,
       .relative_path = "appended",
       .mutation_count = 4u,
       .preapplied_count = 1u},
      {.transition = "direct-growth-interior",
       .source_image = direct_interior_full,
       .relative_path = "third",
       .mutation_count = 4u,
       .preapplied_count = 1u},
      {.transition = "direct-append-interior",
       .source_image = direct_interior_growth,
       .relative_path = "third-append",
       .mutation_count = 4u,
       .preapplied_count = 1u},
      {.transition = "direct-append-limit-minus-one",
       .source_image = direct_limit_minus_one,
       .relative_path = "limit-minus-one-append",
       .mutation_count = 4u},
      {.transition = "direct-growth-limit-minus-one",
       .source_image = direct_limit_minus_one_full,
       .relative_path = "limit-growth",
       .mutation_count = 4u},
      {.transition = "direct-append-limit",
       .source_image = direct_limit,
       .relative_path = "limit-append",
       .mutation_count = 4u},
  };
  for (size_t i = 0u; i < sizeof(mutation_faults) / sizeof(mutation_faults[0]); ++i)
    for (size_t j = 0u; j < sizeof(create_cases) / sizeof(create_cases[0]); ++j)
      if (run_create_recovery_case(&create_cases[j], mutation_faults[i], i, j) != 0)
        return 1;
  qualification_case_pass("directory_transition_recovery_matrix");

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
  qualification_case_pass("degraded_inspection");

  const char *unpaired = "v7-unpaired.img";
  if (copy_image(image, unpaired) != 0 || make_unpaired_generation(unpaired) != 0)
    return 1;
  char output[8192];
  char *argv[] = {(char *)kafs_test_kafs_v7_bin(),
                  (char *)"--image",
                  (char *)unpaired,
                  (char *)"--inspection-mount",
                  (char *)"missing-mnt",
                  (char *)"-o",
                  (char *)"ro",
                  NULL};
  if (run_command(argv, 2, output, sizeof(output)) != 0 ||
      !strstr(output, "admission preflight failed") ||
      strstr(output, "selected descriptor retained") || strstr(output, "bad mount point"))
  {
    fprintf(stderr, "unpaired v7 descriptor generation reached FUSE unexpectedly:\n%s\n", output);
    return 1;
  }
  qualification_case_pass("unpaired_fail_closed");
  return 0;
}
