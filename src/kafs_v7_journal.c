#include "kafs_v7_journal.h"

#include "kafs_v7_mutation.h"

#include "kafs_tool_util.h"

#include <endian.h>
#include <errno.h>
#include <limits.h>
#include <stdlib.h>
#include <string.h>

typedef struct kafs_v7_parsed_transaction
{
  uint64_t sequence;
  uint32_t group_id;
  uint32_t terminal_tag;
  uint32_t mutation_count;
  int64_t free_blocks_delta;
  int64_t free_inodes_delta;
  size_t bytes;
  uint8_t *data;
} kafs_v7_parsed_transaction_t;

typedef struct kafs_v7_target_mutation
{
  uint32_t patch_off;
  uint32_t patch_bytes;
  uint32_t after_crc32;
  const uint8_t *patch;
} kafs_v7_target_mutation_t;

typedef struct kafs_v7_replay_target
{
  uint16_t type;
  uint32_t group_id;
  uint64_t logical_index;
  uint64_t physical_off;
  uint32_t target_bytes;
  uint32_t first_before_crc32;
  kafs_v7_target_mutation_t *mutations;
  size_t mutation_count;
  size_t mutation_capacity;
  uint8_t *final_bytes;
} kafs_v7_replay_target_t;

typedef struct kafs_v7_journal_state
{
  kafs_v7_parsed_transaction_t *transactions;
  size_t transaction_count;
  size_t transaction_capacity;
  kafs_v7_replay_target_t *targets;
  size_t target_count;
  size_t target_capacity;
} kafs_v7_journal_state_t;

typedef struct kafs_v7_record_view
{
  const kafs_v7_journal_record_header_t *header;
  const uint8_t *payload;
  uint32_t tag;
  uint32_t payload_bytes;
  uint64_t sequence;
  size_t record_bytes;
  size_t padded_bytes;
} kafs_v7_record_view_t;

static int kafs_v7_journal_add_u32(uint32_t a, uint32_t b, uint32_t *out)
{
  if (!out || b > UINT32_MAX - a)
    return -EOVERFLOW;
  *out = a + b;
  return 0;
}

static int kafs_v7_journal_add_i64(int64_t a, int64_t b, int64_t *out)
{
  if (!out || (b > 0 && a > INT64_MAX - b) || (b < 0 && a < INT64_MIN - b))
    return -EOVERFLOW;
  *out = a + b;
  return 0;
}

static int kafs_v7_journal_add_delta(uint64_t value, int64_t delta, uint64_t *out)
{
  if (!out)
    return -EINVAL;
  if (delta < 0)
  {
    uint64_t magnitude = (uint64_t)(-(delta + 1)) + 1u;
    if (magnitude > value)
      return -ERANGE;
    *out = value - magnitude;
  }
  else
  {
    uint64_t magnitude = (uint64_t)delta;
    if (magnitude > UINT64_MAX - value)
      return -ERANGE;
    *out = value + magnitude;
  }
  return 0;
}

static int64_t kafs_v7_journal_i64(const void *wire)
{
  uint64_t value;
  memcpy(&value, wire, sizeof(value));
  return (int64_t)le64toh(value);
}

static uint32_t kafs_v7_crc32_update(uint32_t crc, const void *buf, size_t bytes)
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

static uint32_t kafs_v7_record_crc(const kafs_v7_record_view_t *record)
{
  static const uint8_t zero_crc[sizeof(uint32_t)] = {0};
  const uint8_t *header = (const uint8_t *)record->header;
  uint32_t crc = UINT32_MAX;
  crc = kafs_v7_crc32_update(crc, header, offsetof(kafs_v7_journal_record_header_t, crc32));
  crc = kafs_v7_crc32_update(crc, zero_crc, sizeof(zero_crc));
  crc = kafs_v7_crc32_update(crc, record->payload, record->payload_bytes);
  return crc ^ UINT32_MAX;
}

static uint32_t kafs_v7_journal_header_crc(const kafs_v7_journal_header_t *header)
{
  kafs_v7_journal_header_t copy = *header;
  copy.crc32 = 0;
  return kafs_v7_crc32(&copy, sizeof(copy));
}

static int kafs_v7_journal_all_zero(const void *buf, size_t bytes)
{
  const uint8_t *p = (const uint8_t *)buf;
  for (size_t i = 0; i < bytes; ++i)
  {
    if (p[i] != 0)
      return 0;
  }
  return 1;
}

static int kafs_v7_journal_record(const uint8_t *prefix, size_t prefix_bytes, size_t off,
                                  kafs_v7_record_view_t *record)
{
  if (!prefix || !record || off > prefix_bytes ||
      prefix_bytes - off < KAFS_V7_JOURNAL_RECORD_HEADER_BYTES)
    return -EINVAL;
  memset(record, 0, sizeof(*record));
  record->header = (const kafs_v7_journal_record_header_t *)(prefix + off);
  record->tag = le32toh(record->header->tag);
  record->payload_bytes = le32toh(record->header->payload_bytes);
  record->sequence = le64toh(record->header->sequence);
  if ((size_t)record->payload_bytes > prefix_bytes - off - KAFS_V7_JOURNAL_RECORD_HEADER_BYTES)
    return -EINVAL;
  record->record_bytes = KAFS_V7_JOURNAL_RECORD_HEADER_BYTES + record->payload_bytes;
  if (record->record_bytes > SIZE_MAX - 7u)
    return -EOVERFLOW;
  record->padded_bytes = (record->record_bytes + 7u) & ~(size_t)7u;
  if (record->padded_bytes > prefix_bytes - off)
    return -EINVAL;
  record->payload = prefix + off + KAFS_V7_JOURNAL_RECORD_HEADER_BYTES;
  if (record->sequence == 0 ||
      (record->tag != KAFS_V7_JOURNAL_BEGIN_TAG && record->tag != KAFS_V7_JOURNAL_MUTATION_TAG &&
       record->tag != KAFS_V7_JOURNAL_COMMIT_TAG && record->tag != KAFS_V7_JOURNAL_ABORT_TAG) ||
      le32toh(record->header->crc32) != kafs_v7_record_crc(record) ||
      !kafs_v7_journal_all_zero(prefix + off + record->record_bytes,
                                record->padded_bytes - record->record_bytes))
    return -EUCLEAN;
  return 0;
}

static int kafs_v7_validate_mutation(const kafs_v7_layout_report_t *layout, uint32_t group_id,
                                     const uint8_t *payload, uint32_t payload_bytes,
                                     int64_t *free_blocks_delta, int64_t *free_inodes_delta)
{
  if (!layout || !payload || payload_bytes < KAFS_V7_JOURNAL_MUTATION_HEADER_BYTES ||
      !free_blocks_delta || !free_inodes_delta)
    return -EINVAL;
  const kafs_v7_journal_mutation_t *mutation = (const kafs_v7_journal_mutation_t *)payload;
  uint16_t type = le16toh(mutation->target_type);
  uint32_t target_bytes = le32toh(mutation->target_bytes);
  uint32_t patch_off = le32toh(mutation->patch_off);
  uint32_t patch_bytes = le32toh(mutation->patch_bytes);
  kafs_v7_mutation_route_t route;
  *free_blocks_delta =
      kafs_v7_journal_i64(payload + offsetof(kafs_v7_journal_mutation_t, free_blocks_delta));
  *free_inodes_delta =
      kafs_v7_journal_i64(payload + offsetof(kafs_v7_journal_mutation_t, free_inodes_delta));
  if (le16toh(mutation->flags) != 0 || le32toh(mutation->group_id) != group_id ||
      le32toh(mutation->reserved) != 0 || patch_bytes == 0 ||
      patch_bytes != payload_bytes - KAFS_V7_JOURNAL_MUTATION_HEADER_BYTES ||
      patch_off > target_bytes || patch_bytes > target_bytes - patch_off ||
      kafs_v7_mutation_route_target_in_group(layout, type, group_id,
                                             le64toh(mutation->logical_index), &route) != 0 ||
      target_bytes != route.target_bytes)
    return -EINVAL;
  if (type == KAFS_V7_JOURNAL_TARGET_BLOCK_BITMAP)
  {
    if (*free_blocks_delta < -64 || *free_blocks_delta > 64 || *free_inodes_delta != 0)
      return -EINVAL;
  }
  else if (type == KAFS_V7_JOURNAL_TARGET_INODE)
  {
    if (*free_inodes_delta < -1 || *free_inodes_delta > 1 || *free_blocks_delta != 0)
      return -EINVAL;
  }
  else if (*free_blocks_delta != 0 || *free_inodes_delta != 0)
    return -EINVAL;
  return 0;
}

static int kafs_v7_append_transaction(kafs_v7_journal_state_t *state, const uint8_t *data,
                                      size_t bytes, uint64_t sequence, uint32_t group_id,
                                      uint32_t terminal_tag,
                                      const kafs_v7_journal_control_t *control)
{
  if (!state || !data || !control || bytes == 0)
    return -EINVAL;
  if (state->transaction_count == state->transaction_capacity)
  {
    size_t capacity = state->transaction_capacity == 0 ? 8u : state->transaction_capacity * 2u;
    if (capacity < state->transaction_capacity ||
        capacity > SIZE_MAX / sizeof(*state->transactions))
      return -EOVERFLOW;
    void *next = realloc(state->transactions, capacity * sizeof(*state->transactions));
    if (!next)
      return -ENOMEM;
    state->transactions = (kafs_v7_parsed_transaction_t *)next;
    state->transaction_capacity = capacity;
  }
  uint8_t *copy = (uint8_t *)malloc(bytes);
  if (!copy)
    return -ENOMEM;
  memcpy(copy, data, bytes);
  kafs_v7_parsed_transaction_t *transaction = &state->transactions[state->transaction_count++];
  *transaction = (kafs_v7_parsed_transaction_t){
      .sequence = sequence,
      .group_id = group_id,
      .terminal_tag = terminal_tag,
      .mutation_count = le32toh(control->mutation_count),
      .free_blocks_delta = kafs_v7_journal_i64(
          (const uint8_t *)control + offsetof(kafs_v7_journal_control_t, free_blocks_delta)),
      .free_inodes_delta = kafs_v7_journal_i64(
          (const uint8_t *)control + offsetof(kafs_v7_journal_control_t, free_inodes_delta)),
      .bytes = bytes,
      .data = copy,
  };
  return 0;
}

static int kafs_v7_parse_prefix(const kafs_v7_layout_report_t *layout, uint32_t group_id,
                                const kafs_v7_journal_header_t *selected, const uint8_t *prefix,
                                size_t prefix_bytes, kafs_v7_journal_state_t *state,
                                kafs_v7_journal_report_t *report)
{
  if (prefix_bytes == 0)
    return le64toh(selected->first_sequence) == 0 && le64toh(selected->last_sequence) == 0
               ? 0
               : -EINVAL;
  if (!prefix || le64toh(selected->first_sequence) == 0 || le64toh(selected->last_sequence) == 0 ||
      le64toh(selected->first_sequence) > le64toh(selected->last_sequence))
    return -EINVAL;

  size_t off = 0;
  size_t transaction_start = 0;
  int in_transaction = 0;
  uint64_t transaction_sequence = 0;
  uint64_t previous_sequence = 0;
  uint64_t first_sequence = 0;
  uint32_t mutation_count = 0;
  uint32_t mutation_payload_bytes = 0;
  uint32_t mutation_stream_crc = UINT32_MAX;
  int64_t free_blocks_delta = 0;
  int64_t free_inodes_delta = 0;
  kafs_v7_journal_control_t begin_control;

  while (off < prefix_bytes)
  {
    kafs_v7_record_view_t record;
    int rc = kafs_v7_journal_record(prefix, prefix_bytes, off, &record);
    if (rc != 0)
      return rc;
    if (report->record_count == UINT32_MAX)
      return -EOVERFLOW;
    ++report->record_count;

    if (record.tag == KAFS_V7_JOURNAL_BEGIN_TAG)
    {
      if (in_transaction || record.payload_bytes != KAFS_V7_JOURNAL_CONTROL_BYTES)
        return -EINVAL;
      memcpy(&begin_control, record.payload, sizeof(begin_control));
      uint32_t expected_mutations = le32toh(begin_control.mutation_count);
      if (le32toh(begin_control.group_id) != group_id || expected_mutations == 0 ||
          expected_mutations > prefix_bytes / (KAFS_V7_JOURNAL_RECORD_HEADER_BYTES +
                                               KAFS_V7_JOURNAL_MUTATION_HEADER_BYTES + 1u))
        return -EINVAL;
      in_transaction = 1;
      transaction_start = off;
      transaction_sequence = record.sequence;
      mutation_count = 0;
      mutation_payload_bytes = 0;
      mutation_stream_crc = UINT32_MAX;
      free_blocks_delta = 0;
      free_inodes_delta = 0;
    }
    else if (record.tag == KAFS_V7_JOURNAL_MUTATION_TAG)
    {
      int64_t block_delta;
      int64_t inode_delta;
      if (!in_transaction || record.sequence != transaction_sequence ||
          mutation_count >= le32toh(begin_control.mutation_count) ||
          kafs_v7_validate_mutation(layout, group_id, record.payload, record.payload_bytes,
                                    &block_delta, &inode_delta) != 0 ||
          kafs_v7_journal_add_u32(mutation_payload_bytes, record.payload_bytes,
                                  &mutation_payload_bytes) != 0 ||
          kafs_v7_journal_add_i64(free_blocks_delta, block_delta, &free_blocks_delta) != 0 ||
          kafs_v7_journal_add_i64(free_inodes_delta, inode_delta, &free_inodes_delta) != 0)
        return -EINVAL;
      mutation_stream_crc =
          kafs_v7_crc32_update(mutation_stream_crc, record.payload, record.payload_bytes);
      ++mutation_count;
    }
    else
    {
      if (!in_transaction || record.sequence != transaction_sequence ||
          record.payload_bytes != KAFS_V7_JOURNAL_CONTROL_BYTES ||
          memcmp(record.payload, &begin_control, sizeof(begin_control)) != 0 ||
          mutation_count != le32toh(begin_control.mutation_count) ||
          mutation_payload_bytes != le32toh(begin_control.mutation_payload_bytes) ||
          (mutation_stream_crc ^ UINT32_MAX) != le32toh(begin_control.mutation_stream_crc32) ||
          free_blocks_delta !=
              kafs_v7_journal_i64((const uint8_t *)&begin_control +
                                  offsetof(kafs_v7_journal_control_t, free_blocks_delta)) ||
          free_inodes_delta !=
              kafs_v7_journal_i64((const uint8_t *)&begin_control +
                                  offsetof(kafs_v7_journal_control_t, free_inodes_delta)) ||
          (previous_sequence != 0 && transaction_sequence <= previous_sequence))
        return -EUCLEAN;
      size_t transaction_end = off + record.padded_bytes;
      int rc = kafs_v7_append_transaction(state, prefix + transaction_start,
                                          transaction_end - transaction_start, transaction_sequence,
                                          group_id, record.tag, &begin_control);
      if (rc != 0)
        return rc;
      if (first_sequence == 0)
        first_sequence = transaction_sequence;
      previous_sequence = transaction_sequence;
      in_transaction = 0;
    }
    off += record.padded_bytes;
  }
  if (in_transaction || first_sequence != le64toh(selected->first_sequence) ||
      previous_sequence != le64toh(selected->last_sequence))
    return -EUCLEAN;
  return 0;
}

static int kafs_v7_select_and_parse_group(int fd, const kafs_v7_layout_report_t *layout,
                                          uint32_t group_id, kafs_v7_journal_state_t *state,
                                          kafs_v7_journal_report_t *report)
{
  const kafs_v7_shard_desc_t *shards = kafs_v7_report_shards(layout);
  const kafs_v7_shard_desc_t *local = &shards[(uint64_t)group_id * KAFS_V7_GROUP_LOCAL_SHARDS];
  const kafs_v7_shard_desc_t *header_shard = &local[5];
  const kafs_v7_shard_desc_t *data_shard = &local[6];
  uint64_t count_u64 = le64toh(header_shard->logical_count);
  uint64_t start_u64 = le64toh(header_shard->logical_start);
  if (count_u64 == 0 || count_u64 > UINT32_MAX || start_u64 > UINT32_MAX ||
      count_u64 > UINT32_MAX - start_u64)
    return -ERANGE;
  uint32_t count = (uint32_t)count_u64;
  uint32_t start = (uint32_t)start_u64;
  uint64_t segment_bytes = le64toh(data_shard->physical_bytes) / count;
  uint8_t *header_block = (uint8_t *)malloc(layout->block_size);
  if (!header_block)
    return -ENOMEM;
  int rc = 0;
  for (uint32_t local_id = 0; rc == 0 && local_id < count; ++local_id)
  {
    uint64_t header_off =
        le64toh(header_shard->physical_off) + (uint64_t)local_id * layout->block_size;
    rc = kafs_pread_all(fd, header_block, layout->block_size, (off_t)header_off);
    uint64_t highest = 0;
    const kafs_v7_journal_header_t *selected = NULL;
    for (uint32_t slot = 0; rc == 0 && slot < layout->block_size / KAFS_V7_JOURNAL_HEADER_BYTES;
         ++slot)
    {
      const kafs_v7_journal_header_t *header =
          (const kafs_v7_journal_header_t *)(header_block +
                                             (uint64_t)slot * KAFS_V7_JOURNAL_HEADER_BYTES);
      if (kafs_v7_journal_all_zero(header, sizeof(*header)))
        continue;
      if (le32toh(header->magic) != KAFS_V7_JOURNAL_HEADER_MAGIC ||
          le16toh(header->version) != KAFS_V7_JOURNAL_HEADER_VERSION ||
          le16toh(header->flags) != 0 || le32toh(header->segment_id) != start + local_id ||
          le32toh(header->slot_bytes) != KAFS_V7_JOURNAL_HEADER_BYTES ||
          le64toh(header->generation) == 0 || le64toh(header->data_bytes) != segment_bytes ||
          le64toh(header->write_bytes) > segment_bytes || le32toh(header->reserved) != 0 ||
          le32toh(header->crc32) != kafs_v7_journal_header_crc(header) ||
          (le64toh(header->generation) - 1u) %
                  (layout->block_size / KAFS_V7_JOURNAL_HEADER_BYTES) !=
              slot)
        continue;
      uint64_t generation = le64toh(header->generation);
      if (generation > highest)
      {
        highest = generation;
        selected = header;
      }
      else if (generation == highest && selected && memcmp(selected, header, sizeof(*header)) != 0)
        rc = -EUCLEAN;
    }
    if (rc != 0)
      break;
    if (!selected)
    {
      rc = -EINVAL;
      break;
    }
    uint64_t write_bytes = le64toh(selected->write_bytes);
    if ((write_bytes == 0 &&
         (le64toh(selected->first_sequence) != 0 || le64toh(selected->last_sequence) != 0)) ||
        (write_bytes != 0 && (write_bytes & 7u) != 0))
    {
      rc = -EINVAL;
      break;
    }
    uint8_t *prefix = NULL;
    if (write_bytes != 0)
    {
      if (write_bytes > SIZE_MAX)
      {
        rc = -EOVERFLOW;
        break;
      }
      prefix = (uint8_t *)malloc((size_t)write_bytes);
      if (!prefix)
      {
        rc = -ENOMEM;
        break;
      }
      uint64_t data_off = le64toh(data_shard->physical_off) + (uint64_t)local_id * segment_bytes;
      rc = kafs_pread_all(fd, prefix, (size_t)write_bytes, (off_t)data_off);
      if (rc == 0 && report->selected_nonempty_segment_count == UINT32_MAX)
        rc = -EOVERFLOW;
      if (rc == 0)
        ++report->selected_nonempty_segment_count;
    }
    if (rc == 0)
      rc = kafs_v7_parse_prefix(layout, group_id, selected, prefix, (size_t)write_bytes, state,
                                report);
    free(prefix);
  }
  free(header_block);
  return rc;
}

static int kafs_v7_transaction_compare(const void *a, const void *b)
{
  const kafs_v7_parsed_transaction_t *left = (const kafs_v7_parsed_transaction_t *)a;
  const kafs_v7_parsed_transaction_t *right = (const kafs_v7_parsed_transaction_t *)b;
  return left->sequence < right->sequence ? -1 : left->sequence > right->sequence ? 1 : 0;
}

static int kafs_v7_deduplicate_transactions(kafs_v7_journal_state_t *state,
                                            kafs_v7_journal_report_t *report)
{
  if (state->transaction_count == 0)
    return 0;
  qsort(state->transactions, state->transaction_count, sizeof(*state->transactions),
        kafs_v7_transaction_compare);
  size_t out = 0;
  for (size_t i = 0; i < state->transaction_count;)
  {
    size_t end = i + 1u;
    while (end < state->transaction_count &&
           state->transactions[end].sequence == state->transactions[i].sequence)
      ++end;
    for (size_t duplicate = i + 1u; duplicate < end; ++duplicate)
    {
      if (state->transactions[duplicate].bytes != state->transactions[i].bytes ||
          memcmp(state->transactions[duplicate].data, state->transactions[i].data,
                 state->transactions[i].bytes) != 0)
        return -EUCLEAN;
      if (report->duplicate_transaction_count == UINT32_MAX)
        return -EOVERFLOW;
      ++report->duplicate_transaction_count;
      free(state->transactions[duplicate].data);
      state->transactions[duplicate].data = NULL;
    }
    if (out != i)
      state->transactions[out] = state->transactions[i];
    ++out;
    i = end;
  }
  state->transaction_count = out;
  if (out > UINT32_MAX)
    return -EOVERFLOW;
  report->transaction_count = (uint32_t)out;
  report->first_sequence = state->transactions[0].sequence;
  report->last_sequence = state->transactions[out - 1u].sequence;
  return 0;
}

static int kafs_v7_append_target_mutation(kafs_v7_journal_state_t *state,
                                          const kafs_v7_layout_report_t *layout,
                                          const kafs_v7_journal_mutation_t *mutation,
                                          const uint8_t *patch, int committed)
{
  uint16_t type = le16toh(mutation->target_type);
  uint32_t group_id = le32toh(mutation->group_id);
  uint64_t logical_index = le64toh(mutation->logical_index);
  uint32_t target_bytes = le32toh(mutation->target_bytes);
  kafs_v7_mutation_route_t route;
  int rc = kafs_v7_mutation_route_target_in_group(layout, type, group_id, logical_index, &route);
  if (rc != 0 || target_bytes != route.target_bytes)
    return rc != 0 ? rc : -EINVAL;
  kafs_v7_replay_target_t *target = NULL;
  for (size_t i = 0; i < state->target_count; ++i)
  {
    kafs_v7_replay_target_t *candidate = &state->targets[i];
    if (candidate->type == type && candidate->group_id == group_id &&
        candidate->logical_index == logical_index)
      target = candidate;
    else if (route.physical_off < candidate->physical_off + candidate->target_bytes &&
             candidate->physical_off < route.physical_off + target_bytes)
      return -EUCLEAN;
  }
  if (!target)
  {
    if (state->target_count == state->target_capacity)
    {
      size_t capacity = state->target_capacity == 0 ? 8u : state->target_capacity * 2u;
      if (capacity < state->target_capacity || capacity > SIZE_MAX / sizeof(*state->targets))
        return -EOVERFLOW;
      void *next = realloc(state->targets, capacity * sizeof(*state->targets));
      if (!next)
        return -ENOMEM;
      state->targets = (kafs_v7_replay_target_t *)next;
      state->target_capacity = capacity;
    }
    target = &state->targets[state->target_count++];
    *target = (kafs_v7_replay_target_t){
        .type = type,
        .group_id = group_id,
        .logical_index = logical_index,
        .physical_off = route.physical_off,
        .target_bytes = target_bytes,
        .first_before_crc32 = le32toh(mutation->before_crc32),
    };
  }
  else
  {
    uint32_t preceding_crc = target->mutation_count == 0
                                 ? target->first_before_crc32
                                 : target->mutations[target->mutation_count - 1u].after_crc32;
    if (target->target_bytes != target_bytes || target->physical_off != route.physical_off ||
        preceding_crc != le32toh(mutation->before_crc32))
      return -EUCLEAN;
  }
  if (!committed)
    return 0;
  if (target->mutation_count == target->mutation_capacity)
  {
    size_t capacity = target->mutation_capacity == 0 ? 4u : target->mutation_capacity * 2u;
    if (capacity < target->mutation_capacity || capacity > SIZE_MAX / sizeof(*target->mutations))
      return -EOVERFLOW;
    void *next = realloc(target->mutations, capacity * sizeof(*target->mutations));
    if (!next)
      return -ENOMEM;
    target->mutations = (kafs_v7_target_mutation_t *)next;
    target->mutation_capacity = capacity;
  }
  target->mutations[target->mutation_count++] = (kafs_v7_target_mutation_t){
      .patch_off = le32toh(mutation->patch_off),
      .patch_bytes = le32toh(mutation->patch_bytes),
      .after_crc32 = le32toh(mutation->after_crc32),
      .patch = patch,
  };
  return 0;
}

static int kafs_v7_collect_transaction_mutations(kafs_v7_journal_state_t *state,
                                                 const kafs_v7_layout_report_t *layout,
                                                 const kafs_v7_parsed_transaction_t *transaction,
                                                 int committed)
{
  size_t off = 0;
  uint32_t seen = 0;
  while (off < transaction->bytes)
  {
    kafs_v7_record_view_t record;
    int rc = kafs_v7_journal_record(transaction->data, transaction->bytes, off, &record);
    if (rc != 0)
      return rc;
    if (record.tag == KAFS_V7_JOURNAL_MUTATION_TAG)
    {
      const kafs_v7_journal_mutation_t *mutation =
          (const kafs_v7_journal_mutation_t *)record.payload;
      rc = kafs_v7_append_target_mutation(state, layout, mutation,
                                          record.payload + KAFS_V7_JOURNAL_MUTATION_HEADER_BYTES,
                                          committed);
      if (rc != 0)
        return rc;
      ++seen;
    }
    off += record.padded_bytes;
  }
  return seen == transaction->mutation_count ? 0 : -EUCLEAN;
}

static int kafs_v7_replay_targets(int fd, kafs_v7_journal_state_t *state,
                                  kafs_v7_journal_report_t *report)
{
  for (size_t id = 0; id < state->target_count; ++id)
  {
    kafs_v7_replay_target_t *target = &state->targets[id];
    target->final_bytes = (uint8_t *)malloc(target->target_bytes);
    if (!target->final_bytes)
      return -ENOMEM;
    int rc =
        kafs_pread_all(fd, target->final_bytes, target->target_bytes, (off_t)target->physical_off);
    if (rc != 0)
      return rc;
    uint32_t current_crc = kafs_v7_crc32(target->final_bytes, target->target_bytes);
    size_t stage = SIZE_MAX;
    if (current_crc == target->first_before_crc32)
      stage = 0;
    for (size_t mutation = 0; mutation < target->mutation_count; ++mutation)
    {
      if (current_crc == target->mutations[mutation].after_crc32)
        stage = mutation + 1u;
    }
    if (stage == SIZE_MAX)
      return -EUCLEAN;
    if (stage > UINT32_MAX - report->already_applied_mutation_count ||
        target->mutation_count - stage > UINT32_MAX - report->replay_mutation_count)
      return -EOVERFLOW;
    report->already_applied_mutation_count += (uint32_t)stage;
    report->replay_mutation_count += (uint32_t)(target->mutation_count - stage);
    for (size_t mutation = stage; mutation < target->mutation_count; ++mutation)
    {
      const kafs_v7_target_mutation_t *entry = &target->mutations[mutation];
      memcpy(target->final_bytes + entry->patch_off, entry->patch, entry->patch_bytes);
      if (kafs_v7_crc32(target->final_bytes, target->target_bytes) != entry->after_crc32)
        return -EUCLEAN;
    }
  }
  return 0;
}

static int kafs_v7_build_replay(int fd, const kafs_v7_layout_report_t *layout,
                                kafs_v7_journal_replay_t *replay, kafs_v7_journal_state_t *state)
{
  uint64_t expected = layout->checkpoint_sequence;
  replay->recovered_free_blocks = layout->checkpoint_free_blocks;
  replay->recovered_free_inodes = layout->checkpoint_free_inodes;
  for (size_t i = 0; i < state->transaction_count; ++i)
  {
    const kafs_v7_parsed_transaction_t *transaction = &state->transactions[i];
    if (transaction->sequence <= layout->checkpoint_sequence)
      continue;
    if (expected == UINT64_MAX || transaction->sequence != expected + 1u)
      return -EUCLEAN;
    expected = transaction->sequence;
    if (replay->report.pending_transaction_count == UINT32_MAX)
      return -EOVERFLOW;
    ++replay->report.pending_transaction_count;
    if (transaction->terminal_tag == KAFS_V7_JOURNAL_ABORT_TAG)
    {
      if (replay->report.aborted_transaction_count == UINT32_MAX)
        return -EOVERFLOW;
      ++replay->report.aborted_transaction_count;
      int rc = kafs_v7_collect_transaction_mutations(state, layout, transaction, 0);
      if (rc != 0)
        return rc;
      continue;
    }
    if (replay->report.committed_transaction_count == UINT32_MAX ||
        transaction->mutation_count > UINT32_MAX - replay->report.mutation_count)
      return -EOVERFLOW;
    ++replay->report.committed_transaction_count;
    replay->report.mutation_count += transaction->mutation_count;
    int rc =
        kafs_v7_journal_add_delta(replay->recovered_free_blocks, transaction->free_blocks_delta,
                                  &replay->recovered_free_blocks);
    if (rc == 0)
      rc = kafs_v7_journal_add_delta(replay->recovered_free_inodes, transaction->free_inodes_delta,
                                     &replay->recovered_free_inodes);
    if (rc == 0)
      rc = kafs_v7_collect_transaction_mutations(state, layout, transaction, 1);
    if (rc != 0)
      return rc;
  }
  return kafs_v7_replay_targets(fd, state, &replay->report);
}

int kafs_v7_journal_analyze_fd(int fd, const kafs_v7_layout_report_t *layout,
                               kafs_v7_journal_replay_t *replay)
{
  if (fd < 0 || !layout || !layout->descriptor || !replay)
    return -EINVAL;
  memset(replay, 0, sizeof(*replay));
  kafs_v7_journal_state_t *state = (kafs_v7_journal_state_t *)calloc(1u, sizeof(*state));
  if (!state)
    return -ENOMEM;
  replay->state = state;
  int rc = 0;
  for (uint32_t group_id = 0; rc == 0 && group_id < layout->group_count; ++group_id)
    rc = kafs_v7_select_and_parse_group(fd, layout, group_id, state, &replay->report);
  if (rc == 0)
    rc = kafs_v7_deduplicate_transactions(state, &replay->report);
  if (rc == 0)
    rc = kafs_v7_build_replay(fd, layout, replay, state);
  if (rc != 0)
    kafs_v7_journal_replay_clear(replay);
  return rc;
}

int kafs_v7_journal_overlay_pread(const kafs_v7_journal_replay_t *replay, int fd, void *buf,
                                  size_t bytes, uint64_t off)
{
  if (!replay || !replay->state || fd < 0 || (!buf && bytes != 0) || off > INT64_MAX ||
      bytes > (uint64_t)INT64_MAX - off)
    return -EINVAL;
  int rc = kafs_pread_all(fd, buf, bytes, (off_t)off);
  if (rc != 0 || bytes == 0)
    return rc;
  const kafs_v7_journal_state_t *state = (const kafs_v7_journal_state_t *)replay->state;
  uint64_t end = off + bytes;
  for (size_t id = 0; id < state->target_count; ++id)
  {
    const kafs_v7_replay_target_t *target = &state->targets[id];
    uint64_t target_end = target->physical_off + target->target_bytes;
    uint64_t overlap_start = off > target->physical_off ? off : target->physical_off;
    uint64_t overlap_end = end < target_end ? end : target_end;
    if (overlap_start < overlap_end)
      memcpy((uint8_t *)buf + (overlap_start - off),
             target->final_bytes + (overlap_start - target->physical_off),
             (size_t)(overlap_end - overlap_start));
  }
  return 0;
}

void kafs_v7_journal_replay_clear(kafs_v7_journal_replay_t *replay)
{
  if (!replay)
    return;
  kafs_v7_journal_state_t *state = (kafs_v7_journal_state_t *)replay->state;
  if (state)
  {
    for (size_t i = 0; i < state->transaction_count; ++i)
      free(state->transactions[i].data);
    for (size_t i = 0; i < state->target_count; ++i)
    {
      free(state->targets[i].mutations);
      free(state->targets[i].final_bytes);
    }
    free(state->transactions);
    free(state->targets);
    free(state);
  }
  memset(replay, 0, sizeof(*replay));
}
