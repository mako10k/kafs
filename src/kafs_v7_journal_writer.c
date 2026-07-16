#include "kafs_v7_journal_writer.h"

#include "kafs_v7_journal.h"
#include "kafs_v7_mutation.h"

#include "kafs_tool_util.h"

#include <endian.h>
#include <errno.h>
#include <limits.h>
#include <pthread.h>
#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

typedef struct kafs_v7_encoded_mutation
{
  uint8_t *payload;
  uint32_t payload_bytes;
} kafs_v7_encoded_mutation_t;

typedef struct kafs_v7_writer_encode_plan
{
  kafs_v7_journal_replay_t replay;
  kafs_v7_mutation_request_t *requests;
  kafs_v7_mutation_route_t *routes;
  kafs_v7_encoded_mutation_t *encoded;
  kafs_v7_journal_control_t control;
  size_t transaction_bytes;
  uint32_t group_id;
} kafs_v7_writer_encode_plan_t;

typedef struct kafs_v7_writer_publication_plan
{
  kafs_v7_journal_segment_t segment;
  kafs_v7_journal_header_t header;
  uint64_t data_write_off;
  uint64_t header_write_off;
  uint64_t previous_write_bytes;
  uint64_t published_write_bytes;
  uint64_t generation;
  uint32_t header_slot;
} kafs_v7_writer_publication_plan_t;

struct kafs_v7_journal_transaction
{
  uint8_t *data;
  size_t bytes;
  uint64_t sequence;
  uint64_t reservation_token;
  pthread_t owner_thread;
  uint32_t group_id;
  uint32_t terminal_tag;
  uint32_t mutation_count;
};

static int kafs_v7_writer_add_size(size_t a, size_t b, size_t *out)
{
  if (!out || b > SIZE_MAX - a)
    return -EOVERFLOW;
  *out = a + b;
  return 0;
}

static int kafs_v7_writer_add_u32(uint32_t a, uint32_t b, uint32_t *out)
{
  if (!out || b > UINT32_MAX - a)
    return -EOVERFLOW;
  *out = a + b;
  return 0;
}

static void kafs_v7_writer_store_i64(void *field, int64_t value)
{
  uint64_t wire = htole64((uint64_t)value);
  memcpy(field, &wire, sizeof(wire));
}

static int kafs_v7_writer_padded_record_bytes(uint32_t payload_bytes, size_t *padded_bytes)
{
  if (!padded_bytes)
    return -EOVERFLOW;
#if SIZE_MAX <= UINT32_MAX
  if (payload_bytes > SIZE_MAX - KAFS_V7_JOURNAL_RECORD_HEADER_BYTES)
    return -EOVERFLOW;
#endif
  size_t record_bytes = KAFS_V7_JOURNAL_RECORD_HEADER_BYTES + (size_t)payload_bytes;
  if (record_bytes > SIZE_MAX - 7u)
    return -EOVERFLOW;
  *padded_bytes = (record_bytes + 7u) & ~(size_t)7u;
  return 0;
}

static uint32_t kafs_v7_writer_popcount(const uint8_t *bytes, uint32_t byte_count)
{
  uint32_t count = 0u;
  for (uint32_t i = 0; i < byte_count; ++i)
    count += (uint32_t)__builtin_popcount((unsigned)bytes[i]);
  return count;
}

static int kafs_v7_writer_validate_transition_delta(const kafs_v7_mutation_route_t *route,
                                                    const kafs_v7_journal_patch_t *patch,
                                                    const uint8_t *before, const uint8_t *after)
{
  if (!route || !patch || !before || !after)
    return -EINVAL;
  if (route->target_type == KAFS_V7_JOURNAL_TARGET_BLOCK_BITMAP)
  {
    int64_t expected = (int64_t)kafs_v7_writer_popcount(before, route->target_bytes) -
                       (int64_t)kafs_v7_writer_popcount(after, route->target_bytes);
    return patch->free_blocks_delta == expected && patch->free_inodes_delta == 0 ? 0 : -EINVAL;
  }
  if (route->target_type == KAFS_V7_JOURNAL_TARGET_INODE)
  {
    uint16_t before_mode;
    uint16_t after_mode;
    memcpy(&before_mode, before + offsetof(kafs_v7_inode_t, mode), sizeof(before_mode));
    memcpy(&after_mode, after + offsetof(kafs_v7_inode_t, mode), sizeof(after_mode));
    int64_t expected = (le16toh(after_mode) == 0 ? 1 : 0) - (le16toh(before_mode) == 0 ? 1 : 0);
    return patch->free_inodes_delta == expected && patch->free_blocks_delta == 0 ? 0 : -EINVAL;
  }
  return patch->free_blocks_delta == 0 && patch->free_inodes_delta == 0 ? 0 : -EINVAL;
}

static int kafs_v7_writer_validate_patch(const kafs_v7_mutation_route_t *route,
                                         const kafs_v7_journal_patch_t *patch)
{
  if (!route || !patch || !patch->patch || patch->reserved != 0u || patch->patch_bytes == 0u ||
      patch->patch_off > route->target_bytes ||
      patch->patch_bytes > route->target_bytes - patch->patch_off ||
      patch->patch_bytes > UINT32_MAX - KAFS_V7_JOURNAL_MUTATION_HEADER_BYTES)
    return -EINVAL;
  return kafs_v7_mutation_deltas_validate(route->target_type, patch->free_blocks_delta,
                                          patch->free_inodes_delta);
}

static int kafs_v7_writer_append_record(uint8_t *transaction, size_t capacity, size_t *off,
                                        uint32_t tag, uint64_t sequence, const void *payload,
                                        uint32_t payload_bytes)
{
  size_t padded_bytes = 0u;
  int rc = kafs_v7_writer_padded_record_bytes(payload_bytes, &padded_bytes);
  if (rc != 0)
    return rc;
  if (!transaction || !off || !payload || *off > capacity || padded_bytes > capacity - *off)
    return -EOVERFLOW;
  size_t record_bytes = KAFS_V7_JOURNAL_RECORD_HEADER_BYTES + (size_t)payload_bytes;
  uint8_t *record = transaction + *off;
  memset(record, 0, padded_bytes);
  kafs_v7_journal_record_header_t *header = (kafs_v7_journal_record_header_t *)record;
  header->tag = htole32(tag);
  header->payload_bytes = htole32(payload_bytes);
  header->sequence = htole64(sequence);
  memcpy(record + KAFS_V7_JOURNAL_RECORD_HEADER_BYTES, payload, payload_bytes);
  header->crc32 = htole32(kafs_v7_crc32(record, record_bytes));
  *off += padded_bytes;
  return 0;
}

static int kafs_v7_writer_visible_sequence(int fd, const kafs_v7_layout_report_t *layout,
                                           kafs_v7_journal_replay_t *replay,
                                           uint64_t *visible_sequence)
{
  if (!layout || !replay || !visible_sequence)
    return -EINVAL;
  memset(replay, 0, sizeof(*replay));
  int rc = kafs_v7_journal_analyze_fd(fd, layout, replay);
  if (rc != 0)
    return rc;
  *visible_sequence = layout->checkpoint_sequence > replay->report.last_sequence
                          ? layout->checkpoint_sequence
                          : replay->report.last_sequence;
  return 0;
}

static int kafs_v7_writer_mutation_crcs(const kafs_v7_journal_replay_t *replay, int fd,
                                        const kafs_v7_mutation_route_t *route,
                                        const kafs_v7_journal_patch_t *patch, uint32_t *before_crc,
                                        uint32_t *after_crc)
{
  uint8_t *before = (uint8_t *)malloc(route->target_bytes);
  uint8_t *after = (uint8_t *)malloc(route->target_bytes);
  if (!before || !after)
  {
    free(after);
    free(before);
    return -ENOMEM;
  }
  int rc =
      kafs_v7_journal_overlay_pread(replay, fd, before, route->target_bytes, route->physical_off);
  if (rc == 0)
  {
    memcpy(after, before, route->target_bytes);
    memcpy(after + patch->patch_off, patch->patch, patch->patch_bytes);
    rc = kafs_v7_writer_validate_transition_delta(route, patch, before, after);
  }
  if (rc == 0)
  {
    *before_crc = kafs_v7_crc32(before, route->target_bytes);
    *after_crc = kafs_v7_crc32(after, route->target_bytes);
  }
  free(after);
  free(before);
  return rc;
}

static int kafs_v7_writer_encode_mutation(const kafs_v7_journal_replay_t *replay, int fd,
                                          const kafs_v7_mutation_route_t *route,
                                          const kafs_v7_journal_patch_t *patch,
                                          kafs_v7_encoded_mutation_t *encoded)
{
  if (!replay || fd < 0 || !encoded || kafs_v7_writer_validate_patch(route, patch) != 0)
    return -EINVAL;
  uint32_t before_crc = 0u;
  uint32_t after_crc = 0u;
  int rc = kafs_v7_writer_mutation_crcs(replay, fd, route, patch, &before_crc, &after_crc);
  if (rc != 0)
    return rc;

  uint32_t payload_bytes = KAFS_V7_JOURNAL_MUTATION_HEADER_BYTES + patch->patch_bytes;
  uint8_t *payload = (uint8_t *)calloc(1u, payload_bytes);
  if (!payload)
    return -ENOMEM;
  kafs_v7_journal_mutation_t *mutation = (kafs_v7_journal_mutation_t *)payload;
  mutation->target_type = htole16(route->target_type);
  mutation->group_id = htole32(route->group_id);
  mutation->logical_index = htole64(route->logical_index);
  mutation->target_bytes = htole32(route->target_bytes);
  mutation->patch_off = htole32(patch->patch_off);
  mutation->patch_bytes = htole32(patch->patch_bytes);
  mutation->before_crc32 = htole32(before_crc);
  mutation->after_crc32 = htole32(after_crc);
  kafs_v7_writer_store_i64(payload + offsetof(kafs_v7_journal_mutation_t, free_blocks_delta),
                           patch->free_blocks_delta);
  kafs_v7_writer_store_i64(payload + offsetof(kafs_v7_journal_mutation_t, free_inodes_delta),
                           patch->free_inodes_delta);
  memcpy(payload + KAFS_V7_JOURNAL_MUTATION_HEADER_BYTES, patch->patch, patch->patch_bytes);
  encoded->payload = payload;
  encoded->payload_bytes = payload_bytes;
  return 0;
}

static void kafs_v7_writer_mutations_clear(kafs_v7_encoded_mutation_t *encoded, size_t count)
{
  if (!encoded)
    return;
  for (size_t i = 0; i < count; ++i)
    free(encoded[i].payload);
  free(encoded);
}

static void kafs_v7_writer_encode_plan_clear(kafs_v7_writer_encode_plan_t *plan, size_t patch_count)
{
  if (!plan)
    return;
  kafs_v7_writer_mutations_clear(plan->encoded, patch_count);
  free(plan->routes);
  free(plan->requests);
  kafs_v7_journal_replay_clear(&plan->replay);
  memset(plan, 0, sizeof(*plan));
}

static int kafs_v7_writer_route_patches(int fd, const kafs_v7_layout_report_t *layout,
                                        const kafs_v7_sequence_reservation_t *reservation,
                                        const kafs_v7_journal_patch_t *patches, size_t patch_count,
                                        kafs_v7_writer_encode_plan_t *plan)
{
  uint64_t visible_sequence = 0u;
  int rc = kafs_v7_writer_visible_sequence(fd, layout, &plan->replay, &visible_sequence);
  if (rc != 0)
    return rc;
  if (visible_sequence == UINT64_MAX || reservation->sequence != visible_sequence + 1u)
    return -ESTALE;
  plan->requests = (kafs_v7_mutation_request_t *)calloc(patch_count, sizeof(*plan->requests));
  plan->routes = (kafs_v7_mutation_route_t *)calloc(patch_count, sizeof(*plan->routes));
  plan->encoded = (kafs_v7_encoded_mutation_t *)calloc(patch_count, sizeof(*plan->encoded));
  if (!plan->requests || !plan->routes || !plan->encoded)
    return -ENOMEM;
  for (size_t i = 0; i < patch_count; ++i)
  {
    plan->requests[i].target_type = patches[i].target_type;
    plan->requests[i].logical_index = patches[i].logical_index;
  }
  rc = kafs_v7_mutation_route_transaction(layout, plan->requests, patch_count, plan->routes,
                                          &plan->group_id);
  if (rc == 0 && plan->group_id != reservation->group_id)
    rc = -EXDEV;
  return rc;
}

static int kafs_v7_writer_measure_transaction(int fd, const kafs_v7_journal_patch_t *patches,
                                              size_t patch_count,
                                              kafs_v7_writer_encode_plan_t *plan)
{
  uint32_t mutation_payload_bytes = 0u;
  uint32_t mutation_stream_crc = UINT32_MAX;
  int64_t free_blocks_delta = 0;
  int64_t free_inodes_delta = 0;
  size_t control_record_bytes = 0u;
  int rc = kafs_v7_writer_padded_record_bytes(KAFS_V7_JOURNAL_CONTROL_BYTES, &control_record_bytes);
  if (rc == 0)
    rc = kafs_v7_writer_add_size(control_record_bytes, control_record_bytes,
                                 &plan->transaction_bytes);
  for (size_t i = 0; rc == 0 && i < patch_count; ++i)
  {
    size_t padded_bytes = 0u;
    rc = kafs_v7_writer_encode_mutation(&plan->replay, fd, &plan->routes[i], &patches[i],
                                        &plan->encoded[i]);
    if (rc == 0)
      rc = kafs_v7_writer_padded_record_bytes(plan->encoded[i].payload_bytes, &padded_bytes);
    if (rc == 0)
      rc = kafs_v7_writer_add_size(plan->transaction_bytes, padded_bytes, &plan->transaction_bytes);
    if (rc == 0)
      rc = kafs_v7_writer_add_u32(mutation_payload_bytes, plan->encoded[i].payload_bytes,
                                  &mutation_payload_bytes);
    if (rc == 0)
      rc = kafs_v7_journal_delta_add(free_blocks_delta, patches[i].free_blocks_delta,
                                     &free_blocks_delta);
    if (rc == 0)
      rc = kafs_v7_journal_delta_add(free_inodes_delta, patches[i].free_inodes_delta,
                                     &free_inodes_delta);
    if (rc == 0)
      mutation_stream_crc = kafs_v7_crc32_update(mutation_stream_crc, plan->encoded[i].payload,
                                                 plan->encoded[i].payload_bytes);
  }
  if (rc != 0)
    return rc;
  plan->control.group_id = htole32(plan->group_id);
  plan->control.mutation_count = htole32((uint32_t)patch_count);
  plan->control.mutation_payload_bytes = htole32(mutation_payload_bytes);
  plan->control.mutation_stream_crc32 = htole32(mutation_stream_crc ^ UINT32_MAX);
  kafs_v7_writer_store_i64((uint8_t *)&plan->control +
                               offsetof(kafs_v7_journal_control_t, free_blocks_delta),
                           free_blocks_delta);
  kafs_v7_writer_store_i64((uint8_t *)&plan->control +
                               offsetof(kafs_v7_journal_control_t, free_inodes_delta),
                           free_inodes_delta);
  return 0;
}

static int kafs_v7_writer_assemble_transaction(const kafs_v7_sequence_reservation_t *reservation,
                                               size_t patch_count, uint32_t terminal_tag,
                                               const kafs_v7_writer_encode_plan_t *plan,
                                               kafs_v7_journal_transaction_t **transaction_out)
{
  kafs_v7_journal_transaction_t *transaction =
      (kafs_v7_journal_transaction_t *)calloc(1u, sizeof(*transaction));
  if (!transaction)
    return -ENOMEM;
  transaction->data = (uint8_t *)calloc(1u, plan->transaction_bytes);
  if (!transaction->data)
  {
    free(transaction);
    return -ENOMEM;
  }
  size_t off = 0u;
  int rc = kafs_v7_writer_append_record(transaction->data, plan->transaction_bytes, &off,
                                        KAFS_V7_JOURNAL_BEGIN_TAG, reservation->sequence,
                                        &plan->control, sizeof(plan->control));
  for (size_t i = 0; rc == 0 && i < patch_count; ++i)
    rc = kafs_v7_writer_append_record(transaction->data, plan->transaction_bytes, &off,
                                      KAFS_V7_JOURNAL_MUTATION_TAG, reservation->sequence,
                                      plan->encoded[i].payload, plan->encoded[i].payload_bytes);
  if (rc == 0)
    rc =
        kafs_v7_writer_append_record(transaction->data, plan->transaction_bytes, &off, terminal_tag,
                                     reservation->sequence, &plan->control, sizeof(plan->control));
  if (rc == 0 && off != plan->transaction_bytes)
    rc = -EUCLEAN;
  if (rc != 0)
  {
    kafs_v7_journal_transaction_destroy(transaction);
    return rc;
  }
  transaction->bytes = plan->transaction_bytes;
  transaction->sequence = reservation->sequence;
  transaction->reservation_token = reservation->token;
  transaction->owner_thread = pthread_self();
  transaction->group_id = plan->group_id;
  transaction->terminal_tag = terminal_tag;
  transaction->mutation_count = (uint32_t)patch_count;
  *transaction_out = transaction;
  return 0;
}

static int kafs_v7_writer_validate_encode_request(int fd, const kafs_v7_layout_report_t *layout,
                                                  const kafs_v7_sequence_reservation_t *reservation,
                                                  const kafs_v7_journal_patch_t *patches,
                                                  size_t patch_count, uint32_t terminal_tag,
                                                  kafs_v7_journal_transaction_t **transaction_out)
{
  if (fd < 0 || !layout || !layout->descriptor || !reservation || reservation->active != 1u ||
      reservation->sequence == 0u || reservation->group_id >= layout->group_count)
    return -EINVAL;
  if (!patches || patch_count == 0u || patch_count > UINT32_MAX || !transaction_out)
    return -EINVAL;
  if (terminal_tag != KAFS_V7_JOURNAL_COMMIT_TAG && terminal_tag != KAFS_V7_JOURNAL_ABORT_TAG)
    return -EINVAL;
  return 0;
}

static int kafs_v7_writer_validate_encode_sizes(size_t patch_count)
{
  if (patch_count > SIZE_MAX / sizeof(kafs_v7_mutation_request_t) ||
      patch_count > SIZE_MAX / sizeof(kafs_v7_mutation_route_t) ||
      patch_count > SIZE_MAX / sizeof(kafs_v7_encoded_mutation_t))
    return -EINVAL;
  return 0;
}

int kafs_v7_journal_transaction_encode_fd(int fd, const kafs_v7_layout_report_t *layout,
                                          const kafs_v7_sequence_reservation_t *reservation,
                                          const kafs_v7_journal_patch_t *patches,
                                          size_t patch_count, uint32_t terminal_tag,
                                          kafs_v7_journal_transaction_t **transaction_out)
{
  if (transaction_out)
    *transaction_out = NULL;
  int rc = kafs_v7_writer_validate_encode_request(fd, layout, reservation, patches, patch_count,
                                                  terminal_tag, transaction_out);
  if (rc == 0)
    rc = kafs_v7_writer_validate_encode_sizes(patch_count);
  if (rc != 0)
    return rc;

  kafs_v7_writer_encode_plan_t plan;
  memset(&plan, 0, sizeof(plan));
  rc = kafs_v7_writer_route_patches(fd, layout, reservation, patches, patch_count, &plan);
  if (rc == 0)
    rc = kafs_v7_writer_measure_transaction(fd, patches, patch_count, &plan);
  if (rc == 0)
    rc = kafs_v7_writer_assemble_transaction(reservation, patch_count, terminal_tag, &plan,
                                             transaction_out);
  kafs_v7_writer_encode_plan_clear(&plan, patch_count);
  return rc;
}

void kafs_v7_journal_transaction_destroy(kafs_v7_journal_transaction_t *transaction)
{
  if (!transaction)
    return;
  free(transaction->data);
  free(transaction);
}

static int kafs_v7_writer_choose_segment(int fd, const kafs_v7_layout_report_t *layout,
                                         const kafs_v7_journal_transaction_t *transaction,
                                         kafs_v7_journal_segment_t *selected)
{
  const kafs_v7_shard_desc_t *shards = kafs_v7_report_shards(layout);
  if (!shards)
    return -EUCLEAN;
  const kafs_v7_shard_desc_t *header_shard =
      &shards[(uint64_t)transaction->group_id * KAFS_V7_GROUP_LOCAL_SHARDS + 5u];
  uint64_t count = le64toh(header_shard->logical_count);
  if (count == 0u || count > UINT32_MAX)
    return -EUCLEAN;
  int found = 0;
  int generation_overflow = 0;
  uint64_t selected_generation = UINT64_MAX;
  for (uint32_t local = 0; local < (uint32_t)count; ++local)
  {
    kafs_v7_journal_segment_t candidate;
    int rc = kafs_v7_journal_segment_read_fd(fd, layout, transaction->group_id, local, &candidate);
    if (rc != 0)
      return rc;
    uint64_t generation = le64toh(candidate.selected_header.generation);
    uint64_t write_bytes = le64toh(candidate.selected_header.write_bytes);
    if (generation == UINT64_MAX)
    {
      generation_overflow = 1;
      continue;
    }
    if (write_bytes > candidate.data_bytes ||
        transaction->bytes > candidate.data_bytes - write_bytes)
      continue;
    if (!found || generation < selected_generation)
    {
      *selected = candidate;
      selected_generation = generation;
      found = 1;
    }
  }
  if (!found)
    return generation_overflow ? -EOVERFLOW : -ENOSPC;
  return 0;
}

static int
kafs_v7_writer_validate_publishable_transaction(const kafs_v7_layout_report_t *layout,
                                                const kafs_v7_journal_transaction_t *transaction)
{
  if (!transaction || !transaction->data || transaction->bytes == 0u ||
      (transaction->bytes & 7u) != 0u || transaction->sequence == 0u ||
      transaction->group_id >= layout->group_count)
    return -EINVAL;
  return 0;
}

static int kafs_v7_writer_validate_publication(int fd, const kafs_v7_layout_report_t *layout,
                                               const kafs_v7_sequence_reservation_t *reservation,
                                               const kafs_v7_journal_transaction_t *transaction,
                                               const kafs_v7_journal_publication_t *publication)
{
  if (fd < 0 || !layout || !layout->descriptor || !reservation || reservation->active != 1u ||
      !publication)
    return -EINVAL;
  if (kafs_v7_writer_validate_publishable_transaction(layout, transaction) != 0)
    return -EINVAL;
  if (transaction->sequence != reservation->sequence ||
      transaction->reservation_token != reservation->token ||
      transaction->group_id != reservation->group_id ||
      !pthread_equal(transaction->owner_thread, pthread_self()))
    return -EINVAL;
  return 0;
}

static int kafs_v7_writer_prepare_publication(int fd, const kafs_v7_layout_report_t *layout,
                                              const kafs_v7_journal_transaction_t *transaction,
                                              kafs_v7_writer_publication_plan_t *plan)
{
  kafs_v7_journal_replay_t replay;
  uint64_t visible_sequence = 0u;
  int rc = kafs_v7_writer_visible_sequence(fd, layout, &replay, &visible_sequence);
  if (rc == 0 && (visible_sequence == UINT64_MAX || transaction->sequence != visible_sequence + 1u))
    rc = -ESTALE;
  kafs_v7_journal_replay_clear(&replay);
  if (rc != 0)
    return rc;
  rc = kafs_v7_writer_choose_segment(fd, layout, transaction, &plan->segment);
  if (rc != 0)
    return rc;

  plan->previous_write_bytes = le64toh(plan->segment.selected_header.write_bytes);
  plan->published_write_bytes = plan->previous_write_bytes + transaction->bytes;
  plan->generation = le64toh(plan->segment.selected_header.generation) + 1u;
  plan->header_slot = (uint32_t)((plan->generation - 1u) % plan->segment.slot_count);
  if (plan->previous_write_bytes > UINT64_MAX - plan->segment.data_off ||
      (uint64_t)plan->header_slot * KAFS_V7_JOURNAL_HEADER_BYTES >
          UINT64_MAX - plan->segment.header_block_off)
    return -EOVERFLOW;
  plan->data_write_off = plan->segment.data_off + plan->previous_write_bytes;
  plan->header_write_off =
      plan->segment.header_block_off + (uint64_t)plan->header_slot * KAFS_V7_JOURNAL_HEADER_BYTES;
  if (plan->data_write_off > INT64_MAX || plan->header_write_off > INT64_MAX)
    return -ERANGE;

  plan->header.magic = htole32(KAFS_V7_JOURNAL_HEADER_MAGIC);
  plan->header.version = htole16(KAFS_V7_JOURNAL_HEADER_VERSION);
  plan->header.segment_id = plan->segment.selected_header.segment_id;
  plan->header.slot_bytes = htole32(KAFS_V7_JOURNAL_HEADER_BYTES);
  plan->header.generation = htole64(plan->generation);
  plan->header.data_bytes = htole64(plan->segment.data_bytes);
  plan->header.write_bytes = htole64(plan->published_write_bytes);
  plan->header.first_sequence = plan->previous_write_bytes == 0u
                                    ? htole64(transaction->sequence)
                                    : plan->segment.selected_header.first_sequence;
  plan->header.last_sequence = htole64(transaction->sequence);
  plan->header.crc32 = htole32(kafs_v7_crc32(&plan->header, sizeof(plan->header)));
  return 0;
}

static int kafs_v7_writer_publish_plan(int fd, const kafs_v7_journal_transaction_t *transaction,
                                       const kafs_v7_writer_publication_plan_t *plan)
{
  int rc = kafs_pwrite_all(fd, transaction->data, transaction->bytes, (off_t)plan->data_write_off);
  if (rc == 0 && fdatasync(fd) != 0)
    rc = -errno;
  if (rc == 0)
    rc = kafs_pwrite_all(fd, &plan->header, sizeof(plan->header), (off_t)plan->header_write_off);
  if (rc == 0 && fdatasync(fd) != 0)
    rc = -errno;
  return rc;
}

int kafs_v7_journal_transaction_publish_fd(int fd, const kafs_v7_layout_report_t *layout,
                                           const kafs_v7_sequence_reservation_t *reservation,
                                           const kafs_v7_journal_transaction_t *transaction,
                                           kafs_v7_journal_publication_t *publication)
{
  int rc = kafs_v7_writer_validate_publication(fd, layout, reservation, transaction, publication);
  kafs_v7_writer_publication_plan_t plan;
  memset(&plan, 0, sizeof(plan));
  if (rc == 0)
    rc = kafs_v7_writer_prepare_publication(fd, layout, transaction, &plan);
  if (rc == 0)
    rc = kafs_v7_writer_publish_plan(fd, transaction, &plan);
  if (rc == 0)
  {
    kafs_v7_journal_publication_t result = {
        .sequence = transaction->sequence,
        .data_off = plan.data_write_off,
        .header_off = plan.header_write_off,
        .previous_write_bytes = plan.previous_write_bytes,
        .published_write_bytes = plan.published_write_bytes,
        .header_generation = plan.generation,
        .transaction_bytes = transaction->bytes,
        .group_id = transaction->group_id,
        .local_segment = plan.segment.local_segment,
        .segment_id = le32toh(plan.header.segment_id),
        .header_slot = plan.header_slot,
    };
    *publication = result;
  }
  return rc;
}
