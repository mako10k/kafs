/* jscpd:ignore-start */
#include "kafs.h"
#include "kafs_hash.h"
#include "kafs_inode.h"
#include "kafs_journal.h"
#include "kafs_meta_region.h"
#include "kafs_offline_summary.h"
#include "kafs_tailmeta.h"
#include "kafs_v6_layout.h"
#include "kafs_cli_opts.h"
#include "kafs_superblock.h"
#include "kafs_tool_util.h"

#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
/* jscpd:ignore-end */

struct hrl_summary
{
  uint64_t entry_count;
  uint64_t live_entries;
  uint64_t total_refcnt;
};

struct journal_summary
{
  int available;
  int crc_ok;
  uint32_t slot_count;
  uint32_t active_slot;
  uint32_t valid_slots;
  kj_header_t header;
};

#define KAFSDUMP_META_REGION_MAX_SPANS 2u

struct metadata_region_span
{
  uint64_t offset;
  uint64_t size;
};

struct metadata_region_summary
{
  uint32_t id;
  int available;
  uint32_t span_count;
  uint64_t total_size;
  struct metadata_region_span spans[KAFSDUMP_META_REGION_MAX_SPANS];
};

struct dump_journal_header_read_ctx
{
  int fd;
  uint64_t joff;
  uint64_t area_size;
};

static int dump_read_journal_header_slot(void *user, uint32_t slot, kj_header_t *hdr)
{
  const struct dump_journal_header_read_ctx *ctx =
      (const struct dump_journal_header_read_ctx *)user;
  uint64_t off = kj_header_slot_offset(ctx->joff, ctx->area_size, slot);

  return kafs_pread_all(ctx->fd, hdr, sizeof(*hdr), (off_t)off);
}

static void usage(const char *prog) { fprintf(stderr, "Usage: %s [--json] <image>\n", prog); }

static const char *rc_to_text(int rc)
{
  if (rc == 0)
    return "ok";
  if (rc < 0)
    return strerror(-rc);
  return "error";
}

static int load_superblock(int fd, kafs_ssuperblock_t *sb)
{
  int rc = kafs_pread_all(fd, sb, sizeof(*sb), 0);
  if (rc != 0)
    return rc;
  return 0;
}

static const char *v6_bitmap_status(const kafs_ssuperblock_t *sb, const kafs_v6_layout_report_t *v6,
                                    int rc_v6_bitmap)
{
  if (kafs_sb_format_version_get(sb) != KAFS_FORMAT_VERSION_V6)
    return "not_applicable";
  if (!v6->selected_found)
    return "descriptor_unavailable";
  return rc_to_text(rc_v6_bitmap);
}

static int collect_hrl_summary(int fd, const kafs_ssuperblock_t *sb, uint64_t file_size,
                               struct hrl_summary *out)
{
  memset(out, 0, sizeof(*out));

  const uint64_t ent_off = kafs_sb_hrl_entry_offset_get(sb);
  const uint64_t ent_cnt = kafs_sb_hrl_entry_cnt_get(sb);
  const uint64_t ent_bytes = ent_cnt * (uint64_t)sizeof(kafs_hrl_entry_t);

  out->entry_count = ent_cnt;

  if (ent_off == 0 || ent_cnt == 0)
    return 0;

  if (kafs_offline_check_bounds(ent_off, ent_bytes, file_size) != 0)
    return -ERANGE;

  kafs_hrl_entry_t *ents = (kafs_hrl_entry_t *)malloc((size_t)ent_bytes);
  if (!ents)
    return -ENOMEM;

  int rc = kafs_pread_all(fd, ents, (size_t)ent_bytes, (off_t)ent_off);
  if (rc != 0)
  {
    free(ents);
    return rc;
  }

  for (uint64_t i = 0; i < ent_cnt; ++i)
  {
    uint32_t refcnt = ents[i].refcnt;
    if (refcnt != 0)
    {
      out->live_entries++;
      out->total_refcnt += refcnt;
    }
  }

  free(ents);
  return 0;
}

static int collect_journal_summary(int fd, const kafs_ssuperblock_t *sb, uint64_t file_size,
                                   struct journal_summary *out)
{
  memset(out, 0, sizeof(*out));

  const uint64_t j_off = kafs_sb_journal_offset_get(sb);
  const uint64_t j_size = kafs_sb_journal_size_get(sb);
  const uint32_t j_flags = kafs_sb_journal_flags_get(sb);

  if (j_off == 0 || j_size < sizeof(kj_header_t))
    return 0;

  if (kafs_offline_check_bounds(j_off, j_size, file_size) != 0)
    return -ERANGE;

  out->available = 1;
  out->slot_count = kj_header_slot_count(j_flags, j_size);
  uint64_t area_size = kj_journal_area_size(j_size, j_flags);
  struct dump_journal_header_read_ctx read_ctx = {
      .fd = fd,
      .joff = j_off,
      .area_size = area_size,
  };

  int found =
      (kj_header_select_best(out->slot_count, area_size, dump_read_journal_header_slot, &read_ctx,
                             &out->header, &out->active_slot, &out->valid_slots) == 0);
  if (!found)
  {
    int rc = kafs_pread_all(fd, &out->header, sizeof(out->header), (off_t)j_off);
    if (rc != 0)
      return rc;
  }

  out->crc_ok = found ? 1 : (kj_header_crc_ok(&out->header) ? 1 : 0);
  return 0;
}

static void metadata_region_add_span(struct metadata_region_summary *region, uint64_t off,
                                     uint64_t size, uint64_t file_size)
{
  if (!region || size == 0)
    return;
  if (kafs_offline_check_bounds(off, size, file_size) != 0)
    return;
  if (region->span_count >= KAFSDUMP_META_REGION_MAX_SPANS)
    return;

  region->spans[region->span_count].offset = off;
  region->spans[region->span_count].size = size;
  region->span_count++;
  region->total_size += size;
  region->available = 1;
}

static void collect_metadata_regions(const kafs_ssuperblock_t *sb, uint64_t file_size,
                                     struct metadata_region_summary regions[KAFS_META_REGION_COUNT])
{
  for (uint32_t i = 0; i < KAFS_META_REGION_COUNT; ++i)
  {
    memset(&regions[i], 0, sizeof(regions[i]));
    regions[i].id = i;
  }

  const uint64_t blksize = (uint64_t)kafs_sb_blksize_get(sb);
  const uint64_t r_blkcnt = (uint64_t)kafs_sb_r_blkcnt_get(sb);
  const uint64_t inocnt = (uint64_t)kafs_sb_inocnt_get(sb);
  uint64_t layout = sizeof(kafs_ssuperblock_t);

  metadata_region_add_span(&regions[KAFS_META_REGION_SUPERBLOCK_CHECKPOINT], 0,
                           sizeof(kafs_ssuperblock_t), file_size);

  layout = kafs_offline_align_up_u64(layout, blksize);
  uint64_t bitmap_off = layout;
  uint64_t bitmap_size = (r_blkcnt + 7u) >> 3;
  metadata_region_add_span(&regions[KAFS_META_REGION_BLOCK_BITMAP], bitmap_off, bitmap_size,
                           file_size);

  layout += bitmap_size;
  layout = kafs_offline_align_up_u64(layout, 8u);
  layout = kafs_offline_align_up_u64(layout, blksize);
  uint64_t inotbl_off = layout;
  uint64_t inotbl_size = kafs_inode_table_bytes_for_format(kafs_sb_format_version_get(sb), inocnt);
  metadata_region_add_span(&regions[KAFS_META_REGION_INODE_TABLE], inotbl_off, inotbl_size,
                           file_size);

  metadata_region_add_span(&regions[KAFS_META_REGION_ALLOCATOR_SUMMARY],
                           kafs_sb_allocator_offset_get(sb), kafs_sb_allocator_size_get(sb),
                           file_size);
  metadata_region_add_span(&regions[KAFS_META_REGION_HRL_INDEX], kafs_sb_hrl_index_offset_get(sb),
                           kafs_sb_hrl_index_size_get(sb), file_size);
  metadata_region_add_span(
      &regions[KAFS_META_REGION_HRL_ENTRIES], kafs_sb_hrl_entry_offset_get(sb),
      (uint64_t)kafs_sb_hrl_entry_cnt_get(sb) * (uint64_t)sizeof(kafs_hrl_entry_t), file_size);

  const uint64_t joff = kafs_sb_journal_offset_get(sb);
  const uint64_t jsize = kafs_sb_journal_size_get(sb);
  if (joff != 0 && jsize >= sizeof(kj_header_t))
  {
    uint32_t slots = kj_header_slot_count(kafs_sb_journal_flags_get(sb), jsize);
    uint64_t hsz = (uint64_t)kj_header_size();
    uint64_t area_size = kj_journal_area_size(jsize, kafs_sb_journal_flags_get(sb));
    metadata_region_add_span(&regions[KAFS_META_REGION_JOURNAL_HEADER], joff, hsz, file_size);
    if (slots > 1u)
      metadata_region_add_span(&regions[KAFS_META_REGION_JOURNAL_HEADER], joff + hsz + area_size,
                               hsz * (uint64_t)(slots - 1u), file_size);
    metadata_region_add_span(&regions[KAFS_META_REGION_JOURNAL_DATA], joff + hsz, area_size,
                             file_size);
  }

  metadata_region_add_span(&regions[KAFS_META_REGION_PENDING_LOG],
                           kafs_sb_pendinglog_offset_get(sb), kafs_sb_pendinglog_size_get(sb),
                           file_size);
  metadata_region_add_span(&regions[KAFS_META_REGION_TAIL_METADATA],
                           kafs_sb_tailmeta_offset_get(sb), kafs_sb_tailmeta_size_get(sb),
                           file_size);
}

struct dump_report
{
  const kafs_ssuperblock_t *sb;
  const struct inode_summary *ino;
  const struct hrl_summary *hrl;
  const struct journal_summary *jr;
  const struct tailmeta_summary *tm;
  const kafs_v6_layout_report_t *v6;
  const kafs_v6_bitmap_coverage_report_t *v6_bitmap;
  const kafs_v6_journal_header_coverage_report_t *v6_journal_header;
  const kafs_v6_journal_data_coverage_report_t *v6_journal_data;
  const kafs_v6_journal_segment_report_t *v6_journal_segments;
  const struct metadata_region_summary *regions;
  int rc_inode;
  int rc_hrl;
  int rc_journal;
  int rc_tailmeta;
  int rc_v6;
  int rc_v6_bitmap;
  int rc_v6_journal_header;
  int rc_v6_journal_data;
  int rc_v6_journal_segments;
};

static void print_text(const struct dump_report *dump)
{
  const kafs_ssuperblock_t *sb = dump->sb;
  const kafs_v6_layout_report_t *v6 = dump->v6;
  const kafs_v6_bitmap_coverage_report_t *v6_bitmap = dump->v6_bitmap;
  const kafs_v6_journal_header_coverage_report_t *v6_journal_header = dump->v6_journal_header;
  const kafs_v6_journal_data_coverage_report_t *v6_journal_data = dump->v6_journal_data;
  const kafs_v6_journal_segment_report_t *v6_journal_segments = dump->v6_journal_segments;
  const struct metadata_region_summary *regions = dump->regions;
  struct sb_geometry g = kafs_offline_superblock_geometry(sb);

  printf("superblock:\n");
  printf("  magic: 0x%08" PRIx32 "\n", kafs_sb_magic_get(sb));
  printf("  format_version: %" PRIu32 "\n", kafs_sb_format_version_get(sb));
  printf("  block_size: %" PRIu64 "\n", g.blksize);
  printf("  inode_count: %" PRIu64 "\n", (uint64_t)kafs_sb_inocnt_get(sb));
  printf("  inode_free: %" PRIu64 "\n", (uint64_t)kafs_sb_inocnt_free_get(sb));
  printf("  blkcnt_user: %" PRIu64 "\n", (uint64_t)kafs_sb_blkcnt_get(sb));
  printf("  blkcnt_root: %" PRIu64 "\n", g.r_blkcnt);
  printf("  blkcnt_free: %" PRIu64 "\n", (uint64_t)kafs_sb_blkcnt_free_get(sb));
  printf("  first_data_block: %" PRIu64 "\n", g.first_data);
  printf("  data_block_count: %" PRIu64 "\n", g.data_blocks);
  printf("  tailmeta_enabled: %s\n",
         (kafs_sb_feature_flags_get(sb) & KAFS_FEATURE_TAIL_META_REGION) ? "true" : "false");
  printf("  tailmeta_offset: %" PRIu64 "\n", kafs_sb_tailmeta_offset_get(sb));
  printf("  tailmeta_size: %" PRIu64 "\n", kafs_sb_tailmeta_size_get(sb));

  printf("v6_layout_descriptor:\n");
  printf("  status: %s\n", (kafs_sb_format_version_get(sb) == KAFS_FORMAT_VERSION_V6)
                               ? rc_to_text(dump->rc_v6)
                               : "not_applicable");
  printf("  available: %s\n",
         (kafs_sb_format_version_get(sb) == KAFS_FORMAT_VERSION_V6 && v6->selected_found)
             ? "true"
             : "false");
  if (kafs_sb_format_version_get(sb) == KAFS_FORMAT_VERSION_V6)
  {
    printf("  anchor_valid: %s\n", v6->anchor_valid ? "true" : "false");
    printf("  selected_replica: %" PRIu32 "\n", v6->selected_replica);
    printf("  generation: %" PRIu64 "\n", v6->selected_generation);
    printf("  descriptor_bytes: %" PRIu32 "\n", v6->descriptor_bytes);
    printf("  group_count: %" PRIu32 "\n", v6->group_count);
    printf("  shard_count: %" PRIu32 "\n", v6->shard_count);
    printf("  replica_count: %" PRIu32 "\n", v6->replica_count);
    for (uint32_t i = 0; i < v6->replica_count; ++i)
    {
      const kafs_v6_replica_report_t *replica = &v6->replicas[i];
      char summary[256];
      kafs_v6_replica_summary(summary, sizeof(summary), replica);
      printf("  replica[%" PRIu32 "]: %s\n", replica->replica_id, summary);
    }
  }

  printf("v6_bitmap_shards:\n");
  printf("  status: %s\n", v6_bitmap_status(sb, v6, dump->rc_v6_bitmap));
  printf("  available: %s\n", (kafs_sb_format_version_get(sb) == KAFS_FORMAT_VERSION_V6 &&
                               v6->selected_found && v6_bitmap->available)
                                  ? "true"
                                  : "false");
  if (kafs_sb_format_version_get(sb) == KAFS_FORMAT_VERSION_V6 && v6->selected_found)
  {
    printf("  shard_count: %" PRIu32 "\n", v6_bitmap->shard_count);
    printf("  expected_start: %" PRIu64 "\n", v6_bitmap->expected_start);
    printf("  expected_blocks: %" PRIu64 "\n", v6_bitmap->expected_count);
    printf("  covered_blocks: %" PRIu64 "\n", v6_bitmap->covered_blocks);
    printf("  has_gap: %s\n", v6_bitmap->has_gap ? "true" : "false");
    printf("  has_overlap: %s\n", v6_bitmap->has_overlap ? "true" : "false");
    printf("  has_physical_overlap: %s\n", v6_bitmap->has_physical_overlap ? "true" : "false");
    printf("  missing_coverage: %s\n", v6_bitmap->missing_coverage ? "true" : "false");
    printf("  first_gap_start: %" PRIu64 "\n", v6_bitmap->first_gap_start);
    printf("  first_gap_count: %" PRIu64 "\n", v6_bitmap->first_gap_count);
    printf("  first_overlap_start: %" PRIu64 "\n", v6_bitmap->first_overlap_start);
    printf("  first_overlap_count: %" PRIu64 "\n", v6_bitmap->first_overlap_count);
    printf("  first_physical_overlap_off: %" PRIu64 "\n", v6_bitmap->first_physical_overlap_off);
    printf("  first_physical_overlap_bytes: %" PRIu64 "\n",
           v6_bitmap->first_physical_overlap_bytes);
    printf("  lookup_available: %s\n", v6_bitmap->lookup_available ? "true" : "false");
    printf("  lookup_blo: %" PRIu64 "\n", v6_bitmap->lookup.blo);
    printf("  lookup_shard: %" PRIu32 "\n", v6_bitmap->lookup.shard_index);
    printf("  lookup_bitmap_byte: %" PRIu64 "\n", v6_bitmap->lookup.bitmap_byte_off);
    printf("  lookup_bitmap_bit: %" PRIu8 "\n", v6_bitmap->lookup.bitmap_bit);
  }

  printf("v6_journal_segments:\n");
  printf("  status: %s\n", (kafs_sb_format_version_get(sb) == KAFS_FORMAT_VERSION_V6)
                               ? rc_to_text(dump->rc_v6_journal_segments)
                               : "not_applicable");
  printf("  available: %s\n", (kafs_sb_format_version_get(sb) == KAFS_FORMAT_VERSION_V6 &&
                               v6->selected_found && v6_journal_segments->available)
                                  ? "true"
                                  : "false");
  if (kafs_sb_format_version_get(sb) == KAFS_FORMAT_VERSION_V6 && v6->selected_found)
  {
    printf("  header_status: %s\n", rc_to_text(dump->rc_v6_journal_header));
    printf("  data_status: %s\n", rc_to_text(dump->rc_v6_journal_data));
    printf("  header_shards: %" PRIu32 "\n", v6_journal_header->shard_count);
    printf("  data_shards: %" PRIu32 "\n", v6_journal_data->shard_count);
    printf("  expected_segments: %" PRIu64 "\n", v6_journal_header->expected_count);
    printf("  segment_count: %" PRIu64 "\n", v6_journal_segments->segment_count);
    printf("  checked: %" PRIu64 "\n", v6_journal_segments->segments_checked);
    printf("  valid: %" PRIu64 "\n", v6_journal_segments->valid_segments);
    printf("  records_checked: %" PRIu64 "\n", v6_journal_segments->records_checked);
    printf("  selected_segment: %" PRIu64 "\n", v6_journal_segments->selected_segment_id);
    printf("  selected_generation: %" PRIu64 "\n", v6_journal_segments->selected_generation);
    printf("  selected_seq: %" PRIu64 "\n", v6_journal_segments->selected_seq);
    printf("  selected_write_off: %" PRIu64 "\n", v6_journal_segments->selected_write_off);
    printf("  selected_group: %" PRIu32 "\n", v6_journal_segments->selected_group_id);
    printf("  has_missing_pair: %s\n", v6_journal_segments->has_missing_pair ? "true" : "false");
    printf("  has_group_mismatch: %s\n",
           v6_journal_segments->has_group_mismatch ? "true" : "false");
    printf("  has_invalid_header: %s\n",
           v6_journal_segments->has_invalid_header ? "true" : "false");
    printf("  has_torn_data: %s\n", v6_journal_segments->has_torn_data ? "true" : "false");
    printf("  has_read_error: %s\n", v6_journal_segments->has_read_error ? "true" : "false");
    printf("  first_bad_segment: %" PRIu64 "\n", v6_journal_segments->first_bad_segment_id);
  }

  printf("metadata_regions:\n");
  for (uint32_t i = 0; i < KAFS_META_REGION_COUNT; ++i)
  {
    const struct metadata_region_summary *region = &regions[i];
    printf("  region[%" PRIu32 "] %s: available=%s total_size=%" PRIu64 " spans=%" PRIu32 "\n",
           region->id, kafs_meta_region_name(region->id), region->available ? "true" : "false",
           region->total_size, region->span_count);
    for (uint32_t s = 0; s < region->span_count; ++s)
      printf("    span[%" PRIu32 "]: offset=%" PRIu64 " size=%" PRIu64 "\n", s,
             region->spans[s].offset, region->spans[s].size);
  }

  printf("tail_metadata:\n");
  printf("  status: %s\n", rc_to_text(dump->rc_tailmeta));
  printf("  available: %s\n", dump->tm->available ? "true" : "false");
  if (dump->tm->available)
  {
    printf("  magic: 0x%08" PRIx32 "\n", kafs_u32_stoh(dump->tm->header.tr_magic));
    printf("  version: %" PRIu16 "\n", kafs_tailmeta_region_hdr_version_get(&dump->tm->header));
    printf("  container_count: %" PRIu32 "\n",
           kafs_tailmeta_region_hdr_container_count_get(&dump->tm->header));
    printf("  class_count: %" PRIu16 "\n",
           kafs_tailmeta_region_hdr_class_count_get(&dump->tm->header));
    printf("  valid_containers: %" PRIu64 "\n", dump->tm->valid_containers);
    printf("  invalid_containers: %" PRIu64 "\n", dump->tm->invalid_containers);
    printf("  live_slots: %" PRIu64 "\n", dump->tm->live_slots);
    printf("  invalid_slots: %" PRIu64 "\n", dump->tm->invalid_slots);
    printf("  live_bytes: %" PRIu64 "\n", dump->tm->live_bytes);
    printf("  free_bytes: %" PRIu64 "\n", dump->tm->free_bytes);
    for (uint16_t index = 0; index < dump->tm->class_summary_count; ++index)
    {
      const struct tailmeta_class_summary *class_summary = &dump->tm->classes[index];

      printf("  class[%u]: bytes=%u valid=%" PRIu64 " invalid=%" PRIu64 " slots=%" PRIu64
             " live_slots=%" PRIu64 " live_bytes=%" PRIu64 " free_bytes=%" PRIu64 "\n",
             (unsigned)index, (unsigned)class_summary->class_bytes, class_summary->valid_containers,
             class_summary->invalid_containers, class_summary->slots, class_summary->live_slots,
             class_summary->live_bytes, class_summary->free_bytes);
    }
  }

  printf("inode_summary:\n");
  printf("  status: %s\n", rc_to_text(dump->rc_inode));
  printf("  total: %" PRIu64 "\n", dump->ino->total);
  printf("  used: %" PRIu64 "\n", dump->ino->used);
  printf("  free: %" PRIu64 "\n", dump->ino->free);
  printf("  linkcnt_zero_used: %" PRIu64 "\n", dump->ino->linkcnt_zero_used);
  printf("  regular_files: %" PRIu64 "\n", dump->ino->regular_files);
  printf("  tail_only_regular: %" PRIu64 "\n", dump->ino->tail_only_regular);
  printf("  mixed_full_tail_regular: %" PRIu64 "\n", dump->ino->mixed_full_tail_regular);

  printf("hrl_summary:\n");
  printf("  status: %s\n", rc_to_text(dump->rc_hrl));
  printf("  entries: %" PRIu64 "\n", dump->hrl->entry_count);
  printf("  live_entries: %" PRIu64 "\n", dump->hrl->live_entries);
  printf("  total_refcnt: %" PRIu64 "\n", dump->hrl->total_refcnt);

  printf("journal_header:\n");
  printf("  status: %s\n", rc_to_text(dump->rc_journal));
  printf("  available: %s\n", dump->jr->available ? "true" : "false");
  if (dump->jr->available)
  {
    printf("  magic: 0x%08" PRIx32 "\n", dump->jr->header.magic);
    printf("  version: %" PRIu16 "\n", dump->jr->header.version);
    printf("  flags: %" PRIu16 "\n", dump->jr->header.flags);
    printf("  slot_count: %" PRIu32 "\n", dump->jr->slot_count);
    printf("  active_slot: %" PRIu32 "\n", dump->jr->active_slot);
    printf("  valid_slots: %" PRIu32 "\n", dump->jr->valid_slots);
    printf("  generation: %" PRIu64 "\n", dump->jr->header.reserved0);
    printf("  area_size: %" PRIu64 "\n", dump->jr->header.area_size);
    printf("  write_off: %" PRIu64 "\n", dump->jr->header.write_off);
    printf("  seq: %" PRIu64 "\n", dump->jr->header.seq);
    printf("  header_crc: 0x%08" PRIx32 "\n", dump->jr->header.header_crc);
    printf("  header_crc_ok: %s\n", dump->jr->crc_ok ? "true" : "false");
  }
}

static void print_json(const struct dump_report *dump)
{
  const kafs_ssuperblock_t *sb = dump->sb;
  const kafs_v6_layout_report_t *v6 = dump->v6;
  const kafs_v6_bitmap_coverage_report_t *v6_bitmap = dump->v6_bitmap;
  const kafs_v6_journal_header_coverage_report_t *v6_journal_header = dump->v6_journal_header;
  const kafs_v6_journal_data_coverage_report_t *v6_journal_data = dump->v6_journal_data;
  const kafs_v6_journal_segment_report_t *v6_journal_segments = dump->v6_journal_segments;
  const struct metadata_region_summary *regions = dump->regions;
  const struct sb_geometry g = kafs_offline_superblock_geometry(sb);

  printf("{\n");
  printf("  \"superblock\": {\n");
  printf("    \"magic\": %" PRIu32 ",\n", kafs_sb_magic_get(sb));
  printf("    \"format_version\": %" PRIu32 ",\n", kafs_sb_format_version_get(sb));
  printf("    \"block_size\": %" PRIu64 ",\n", g.blksize);
  printf("    \"inode_count\": %" PRIu64 ",\n", (uint64_t)kafs_sb_inocnt_get(sb));
  printf("    \"inode_free\": %" PRIu64 ",\n", (uint64_t)kafs_sb_inocnt_free_get(sb));
  printf("    \"blkcnt_user\": %" PRIu64 ",\n", (uint64_t)kafs_sb_blkcnt_get(sb));
  printf("    \"blkcnt_root\": %" PRIu64 ",\n", g.r_blkcnt);
  printf("    \"blkcnt_free\": %" PRIu64 ",\n", (uint64_t)kafs_sb_blkcnt_free_get(sb));
  printf("    \"first_data_block\": %" PRIu64 ",\n", g.first_data);
  printf("    \"data_block_count\": %" PRIu64 ",\n", g.data_blocks);
  printf("    \"tailmeta_enabled\": %s,\n",
         (kafs_sb_feature_flags_get(sb) & KAFS_FEATURE_TAIL_META_REGION) ? "true" : "false");
  printf("    \"tailmeta_offset\": %" PRIu64 ",\n", kafs_sb_tailmeta_offset_get(sb));
  printf("    \"tailmeta_size\": %" PRIu64 "\n", kafs_sb_tailmeta_size_get(sb));
  printf("  },\n");

  printf("  \"v6_layout_descriptor\": {\n");
  printf("    \"status\": \"%s\",\n", (kafs_sb_format_version_get(sb) == KAFS_FORMAT_VERSION_V6)
                                          ? rc_to_text(dump->rc_v6)
                                          : "not_applicable");
  printf("    \"available\": %s,\n",
         (kafs_sb_format_version_get(sb) == KAFS_FORMAT_VERSION_V6 && v6->selected_found)
             ? "true"
             : "false");
  printf("    \"anchor_valid\": %s,\n", v6->anchor_valid ? "true" : "false");
  printf("    \"selected_replica\": %" PRIu32 ",\n", v6->selected_replica);
  printf("    \"generation\": %" PRIu64 ",\n", v6->selected_generation);
  printf("    \"descriptor_bytes\": %" PRIu32 ",\n", v6->descriptor_bytes);
  printf("    \"group_count\": %" PRIu32 ",\n", v6->group_count);
  printf("    \"shard_count\": %" PRIu32 ",\n", v6->shard_count);
  printf("    \"replica_count\": %" PRIu32 ",\n", v6->replica_count);
  printf("    \"replicas\": [");
  for (uint32_t i = 0; i < v6->replica_count; ++i)
  {
    const kafs_v6_replica_report_t *replica = &v6->replicas[i];
    printf("%s{\"replica_id\": %" PRIu32 ", \"role\": \"%s\", \"offset\": %" PRIu64
           ", \"bytes\": %" PRIu32 ", \"status\": \"%s\", \"generation\": %" PRIu64
           ", \"crc_ok\": %s, \"selected\": %s}",
           (i == 0u) ? "" : ", ", replica->replica_id, kafs_v6_replica_role_name(replica->role),
           replica->offset, replica->bytes, kafs_v6_replica_status_name(replica->status),
           replica->generation, replica->crc_ok ? "true" : "false",
           replica->selected ? "true" : "false");
  }
  printf("]\n");
  printf("  },\n");

  printf("  \"v6_bitmap_shards\": {\n");
  printf("    \"status\": \"%s\",\n", v6_bitmap_status(sb, v6, dump->rc_v6_bitmap));
  printf("    \"available\": %s,\n", (kafs_sb_format_version_get(sb) == KAFS_FORMAT_VERSION_V6 &&
                                      v6->selected_found && v6_bitmap->available)
                                         ? "true"
                                         : "false");
  printf("    \"shard_count\": %" PRIu32 ",\n", v6_bitmap->shard_count);
  printf("    \"expected_start\": %" PRIu64 ",\n", v6_bitmap->expected_start);
  printf("    \"expected_blocks\": %" PRIu64 ",\n", v6_bitmap->expected_count);
  printf("    \"covered_blocks\": %" PRIu64 ",\n", v6_bitmap->covered_blocks);
  printf("    \"has_gap\": %s,\n", v6_bitmap->has_gap ? "true" : "false");
  printf("    \"has_overlap\": %s,\n", v6_bitmap->has_overlap ? "true" : "false");
  printf("    \"has_physical_overlap\": %s,\n", v6_bitmap->has_physical_overlap ? "true" : "false");
  printf("    \"missing_coverage\": %s,\n", v6_bitmap->missing_coverage ? "true" : "false");
  printf("    \"first_gap_start\": %" PRIu64 ",\n", v6_bitmap->first_gap_start);
  printf("    \"first_gap_count\": %" PRIu64 ",\n", v6_bitmap->first_gap_count);
  printf("    \"first_overlap_start\": %" PRIu64 ",\n", v6_bitmap->first_overlap_start);
  printf("    \"first_overlap_count\": %" PRIu64 ",\n", v6_bitmap->first_overlap_count);
  printf("    \"first_physical_overlap_off\": %" PRIu64 ",\n",
         v6_bitmap->first_physical_overlap_off);
  printf("    \"first_physical_overlap_bytes\": %" PRIu64 ",\n",
         v6_bitmap->first_physical_overlap_bytes);
  printf("    \"lookup_available\": %s,\n", v6_bitmap->lookup_available ? "true" : "false");
  printf("    \"lookup\": {\"blo\": %" PRIu64 ", \"shard\": %" PRIu32 ", \"bitmap_byte\": %" PRIu64
         ", \"bitmap_bit\": %" PRIu8 "}\n",
         v6_bitmap->lookup.blo, v6_bitmap->lookup.shard_index, v6_bitmap->lookup.bitmap_byte_off,
         v6_bitmap->lookup.bitmap_bit);
  printf("  },\n");

  printf("  \"v6_journal_segments\": {\n");
  printf("    \"status\": \"%s\",\n", (kafs_sb_format_version_get(sb) == KAFS_FORMAT_VERSION_V6)
                                          ? rc_to_text(dump->rc_v6_journal_segments)
                                          : "not_applicable");
  printf("    \"available\": %s,\n", (kafs_sb_format_version_get(sb) == KAFS_FORMAT_VERSION_V6 &&
                                      v6->selected_found && v6_journal_segments->available)
                                         ? "true"
                                         : "false");
  printf("    \"header_status\": \"%s\",\n", rc_to_text(dump->rc_v6_journal_header));
  printf("    \"data_status\": \"%s\",\n", rc_to_text(dump->rc_v6_journal_data));
  printf("    \"header_shards\": %" PRIu32 ",\n", v6_journal_header->shard_count);
  printf("    \"data_shards\": %" PRIu32 ",\n", v6_journal_data->shard_count);
  printf("    \"expected_segments\": %" PRIu64 ",\n", v6_journal_header->expected_count);
  printf("    \"segment_count\": %" PRIu64 ",\n", v6_journal_segments->segment_count);
  printf("    \"checked\": %" PRIu64 ",\n", v6_journal_segments->segments_checked);
  printf("    \"valid\": %" PRIu64 ",\n", v6_journal_segments->valid_segments);
  printf("    \"records_checked\": %" PRIu64 ",\n", v6_journal_segments->records_checked);
  printf("    \"selected_segment\": %" PRIu64 ",\n", v6_journal_segments->selected_segment_id);
  printf("    \"selected_generation\": %" PRIu64 ",\n", v6_journal_segments->selected_generation);
  printf("    \"selected_seq\": %" PRIu64 ",\n", v6_journal_segments->selected_seq);
  printf("    \"selected_write_off\": %" PRIu64 ",\n", v6_journal_segments->selected_write_off);
  printf("    \"selected_group\": %" PRIu32 ",\n", v6_journal_segments->selected_group_id);
  printf("    \"has_missing_pair\": %s,\n",
         v6_journal_segments->has_missing_pair ? "true" : "false");
  printf("    \"has_group_mismatch\": %s,\n",
         v6_journal_segments->has_group_mismatch ? "true" : "false");
  printf("    \"has_invalid_header\": %s,\n",
         v6_journal_segments->has_invalid_header ? "true" : "false");
  printf("    \"has_torn_data\": %s,\n", v6_journal_segments->has_torn_data ? "true" : "false");
  printf("    \"has_read_error\": %s,\n", v6_journal_segments->has_read_error ? "true" : "false");
  printf("    \"first_bad_segment\": %" PRIu64 "\n", v6_journal_segments->first_bad_segment_id);
  printf("  },\n");

  printf("  \"metadata_regions\": [\n");
  for (uint32_t i = 0; i < KAFS_META_REGION_COUNT; ++i)
  {
    const struct metadata_region_summary *region = &regions[i];
    printf("    {\"id\": %" PRIu32 ", \"name\": \"%s\", \"available\": %s, "
           "\"total_size\": %" PRIu64 ", \"spans\": [",
           region->id, kafs_meta_region_name(region->id), region->available ? "true" : "false",
           region->total_size);
    for (uint32_t s = 0; s < region->span_count; ++s)
      printf("%s{\"offset\": %" PRIu64 ", \"size\": %" PRIu64 "}", (s == 0) ? "" : ", ",
             region->spans[s].offset, region->spans[s].size);
    printf("]}%s\n", (i + 1u == KAFS_META_REGION_COUNT) ? "" : ",");
  }
  printf("  ],\n");

  printf("  \"tail_metadata\": {\n");
  printf("    \"status\": \"%s\",\n", rc_to_text(dump->rc_tailmeta));
  printf("    \"available\": %s", dump->tm->available ? "true" : "false");
  if (dump->tm->available)
  {
    printf(",\n    \"magic\": %" PRIu32, kafs_u32_stoh(dump->tm->header.tr_magic));
    printf(",\n    \"version\": %" PRIu16, kafs_tailmeta_region_hdr_version_get(&dump->tm->header));
    printf(",\n    \"container_count\": %" PRIu32,
           kafs_tailmeta_region_hdr_container_count_get(&dump->tm->header));
    printf(",\n    \"class_count\": %" PRIu16,
           kafs_tailmeta_region_hdr_class_count_get(&dump->tm->header));
    printf(",\n    \"valid_containers\": %" PRIu64, dump->tm->valid_containers);
    printf(",\n    \"invalid_containers\": %" PRIu64, dump->tm->invalid_containers);
    printf(",\n    \"live_slots\": %" PRIu64, dump->tm->live_slots);
    printf(",\n    \"invalid_slots\": %" PRIu64, dump->tm->invalid_slots);
    printf(",\n    \"live_bytes\": %" PRIu64, dump->tm->live_bytes);
    printf(",\n    \"free_bytes\": %" PRIu64, dump->tm->free_bytes);
    printf(",\n    \"classes\": [");
    for (uint16_t index = 0; index < dump->tm->class_summary_count; ++index)
    {
      const struct tailmeta_class_summary *class_summary = &dump->tm->classes[index];

      printf(
          "%s{\"class_bytes\": %u, \"valid_containers\": %" PRIu64
          ", \"invalid_containers\": %" PRIu64 ", \"slots\": %" PRIu64 ", \"live_slots\": %" PRIu64
          ", \"live_bytes\": %" PRIu64 ", \"free_bytes\": %" PRIu64 "}",
          (index == 0) ? "" : ", ", (unsigned)class_summary->class_bytes,
          class_summary->valid_containers, class_summary->invalid_containers, class_summary->slots,
          class_summary->live_slots, class_summary->live_bytes, class_summary->free_bytes);
    }
    printf("]");
  }
  printf("\n  },\n");

  printf("  \"inode_summary\": {\n");
  printf("    \"status\": \"%s\",\n", rc_to_text(dump->rc_inode));
  printf("    \"total\": %" PRIu64 ",\n", dump->ino->total);
  printf("    \"used\": %" PRIu64 ",\n", dump->ino->used);
  printf("    \"free\": %" PRIu64 ",\n", dump->ino->free);
  printf("    \"linkcnt_zero_used\": %" PRIu64 ",\n", dump->ino->linkcnt_zero_used);
  printf("    \"regular_files\": %" PRIu64 ",\n", dump->ino->regular_files);
  printf("    \"tail_only_regular\": %" PRIu64 ",\n", dump->ino->tail_only_regular);
  printf("    \"mixed_full_tail_regular\": %" PRIu64 "\n", dump->ino->mixed_full_tail_regular);
  printf("  },\n");

  printf("  \"hrl_summary\": {\n");
  printf("    \"status\": \"%s\",\n", rc_to_text(dump->rc_hrl));
  printf("    \"entries\": %" PRIu64 ",\n", dump->hrl->entry_count);
  printf("    \"live_entries\": %" PRIu64 ",\n", dump->hrl->live_entries);
  printf("    \"total_refcnt\": %" PRIu64 "\n", dump->hrl->total_refcnt);
  printf("  },\n");

  printf("  \"journal_header\": {\n");
  printf("    \"status\": \"%s\",\n", rc_to_text(dump->rc_journal));
  printf("    \"available\": %s", dump->jr->available ? "true" : "false");
  if (dump->jr->available)
  {
    printf(",\n    \"magic\": %" PRIu32, dump->jr->header.magic);
    printf(",\n    \"version\": %" PRIu16, dump->jr->header.version);
    printf(",\n    \"flags\": %" PRIu16, dump->jr->header.flags);
    printf(",\n    \"slot_count\": %" PRIu32, dump->jr->slot_count);
    printf(",\n    \"active_slot\": %" PRIu32, dump->jr->active_slot);
    printf(",\n    \"valid_slots\": %" PRIu32, dump->jr->valid_slots);
    printf(",\n    \"generation\": %" PRIu64, dump->jr->header.reserved0);
    printf(",\n    \"area_size\": %" PRIu64, dump->jr->header.area_size);
    printf(",\n    \"write_off\": %" PRIu64, dump->jr->header.write_off);
    printf(",\n    \"seq\": %" PRIu64, dump->jr->header.seq);
    printf(",\n    \"header_crc\": %" PRIu32, dump->jr->header.header_crc);
    printf(",\n    \"header_crc_ok\": %s", dump->jr->crc_ok ? "true" : "false");
  }
  printf("\n  }\n");
  printf("}\n");
}

int main(int argc, char **argv)
{
  int json = 0;
  const char *image = NULL;

  if (kafs_cli_exit_if_help(argc, argv, usage, argv[0]) == 0)
    return 0;

  for (int i = 1; i < argc; ++i)
  {
    if (strcmp(argv[i], "--json") == 0)
    {
      json = 1;
      continue;
    }

    if (argv[i][0] == '-')
    {
      usage(argv[0]);
      return 2;
    }

    image = argv[i];
  }

  if (!image)
  {
    usage(argv[0]);
    return 2;
  }

  int fd = open(image, O_RDONLY);
  if (fd < 0)
  {
    perror("open");
    return 1;
  }

  uint64_t file_size = 0;
  int rc = kafs_offline_detect_file_size(fd, &file_size);
  if (rc != 0)
  {
    fprintf(stderr, "failed to detect image size: %s\n", rc_to_text(rc));
    close(fd);
    return 1;
  }

  kafs_ssuperblock_t sb;
  rc = load_superblock(fd, &sb);
  if (rc != 0)
  {
    fprintf(stderr, "failed to read superblock: %s\n", strerror(-rc));
    close(fd);
    return 1;
  }

  struct inode_summary ino;
  struct hrl_summary hrl;
  struct journal_summary jr;
  struct tailmeta_summary tm;
  kafs_v6_layout_report_t v6;
  kafs_v6_bitmap_coverage_report_t v6_bitmap;
  kafs_v6_journal_header_coverage_report_t v6_journal_header;
  kafs_v6_journal_data_coverage_report_t v6_journal_data;
  kafs_v6_journal_segment_report_t v6_journal_segments;
  struct metadata_region_summary regions[KAFS_META_REGION_COUNT];
  int rc_inode = collect_inode_summary(fd, &sb, file_size, &ino);
  int rc_hrl = collect_hrl_summary(fd, &sb, file_size, &hrl);
  int rc_journal = collect_journal_summary(fd, &sb, file_size, &jr);
  int rc_tailmeta = collect_tailmeta_summary(fd, &sb, file_size, &tm);
  int rc_v6 = 0;
  int rc_v6_bitmap = 0;
  int rc_v6_journal_header = 0;
  int rc_v6_journal_data = 0;
  int rc_v6_journal_segments = 0;
  memset(&v6, 0, sizeof(v6));
  memset(&v6_bitmap, 0, sizeof(v6_bitmap));
  memset(&v6_journal_header, 0, sizeof(v6_journal_header));
  memset(&v6_journal_data, 0, sizeof(v6_journal_data));
  memset(&v6_journal_segments, 0, sizeof(v6_journal_segments));
  if (kafs_sb_format_version_get(&sb) == KAFS_FORMAT_VERSION_V6)
  {
    rc_v6 = kafs_v6_discover_layout(fd, &sb, file_size, &v6);
    if (rc_v6 == 0)
    {
      void *desc = NULL;
      uint32_t desc_bytes = 0;
      rc_v6_bitmap = kafs_v6_read_selected_descriptor(fd, &v6, &desc, &desc_bytes);
      if (rc_v6_bitmap == 0)
      {
        rc_v6_bitmap =
            kafs_v6_bitmap_validate_coverage(desc, desc_bytes, &sb, file_size, &v6_bitmap);
        rc_v6_journal_header = kafs_v6_journal_header_validate_coverage(
            desc, desc_bytes, &sb, file_size, &v6_journal_header);
        rc_v6_journal_data = kafs_v6_journal_data_validate_coverage(desc, desc_bytes, &sb,
                                                                    file_size, &v6_journal_data);
        if (rc_v6_journal_header == 0 && rc_v6_journal_data == 0)
          rc_v6_journal_segments = kafs_v6_journal_validate_segments_fd(
              fd, desc, desc_bytes, &sb, file_size, &v6_journal_segments);
        else
          rc_v6_journal_segments =
              (rc_v6_journal_header != 0) ? rc_v6_journal_header : rc_v6_journal_data;
      }
      else
      {
        rc_v6_journal_header = rc_v6_bitmap;
        rc_v6_journal_data = rc_v6_bitmap;
        rc_v6_journal_segments = rc_v6_bitmap;
      }
      free(desc);
    }
  }
  collect_metadata_regions(&sb, file_size, regions);

  if (rc_inode != 0)
    fprintf(stderr, "warning: inode summary unavailable: %s\n", rc_to_text(rc_inode));
  if (rc_hrl != 0)
    fprintf(stderr, "warning: hrl summary unavailable: %s\n", rc_to_text(rc_hrl));
  if (rc_journal != 0)
    fprintf(stderr, "warning: journal header unavailable: %s\n", rc_to_text(rc_journal));
  if (rc_tailmeta != 0)
    fprintf(stderr, "warning: tail metadata unavailable: %s\n", rc_to_text(rc_tailmeta));
  if (kafs_sb_format_version_get(&sb) == KAFS_FORMAT_VERSION_V6 && rc_v6 != 0)
    fprintf(stderr, "warning: v6 descriptor discovery failed: %s\n", rc_to_text(rc_v6));
  if (kafs_sb_format_version_get(&sb) == KAFS_FORMAT_VERSION_V6 && rc_v6 == 0 && rc_v6_bitmap != 0)
    fprintf(stderr, "warning: v6 bitmap shard validation failed: %s\n", rc_to_text(rc_v6_bitmap));
  if (kafs_sb_format_version_get(&sb) == KAFS_FORMAT_VERSION_V6 && rc_v6 == 0 &&
      rc_v6_journal_segments != 0)
    fprintf(stderr, "warning: v6 journal segment validation failed: %s\n",
            rc_to_text(rc_v6_journal_segments));

  const struct dump_report dump = {
      .sb = &sb,
      .ino = &ino,
      .hrl = &hrl,
      .jr = &jr,
      .tm = &tm,
      .v6 = &v6,
      .v6_bitmap = &v6_bitmap,
      .v6_journal_header = &v6_journal_header,
      .v6_journal_data = &v6_journal_data,
      .v6_journal_segments = &v6_journal_segments,
      .regions = regions,
      .rc_inode = rc_inode,
      .rc_hrl = rc_hrl,
      .rc_journal = rc_journal,
      .rc_tailmeta = rc_tailmeta,
      .rc_v6 = rc_v6,
      .rc_v6_bitmap = rc_v6_bitmap,
      .rc_v6_journal_header = rc_v6_journal_header,
      .rc_v6_journal_data = rc_v6_journal_data,
      .rc_v6_journal_segments = rc_v6_journal_segments,
  };
  if (json)
    print_json(&dump);
  else
    print_text(&dump);

  close(fd);
  return (rc_inode == 0 && rc_hrl == 0 && rc_journal == 0 && rc_tailmeta == 0 && rc_v6 == 0 &&
          rc_v6_bitmap == 0 && rc_v6_journal_header == 0 && rc_v6_journal_data == 0 &&
          rc_v6_journal_segments == 0)
             ? 0
             : 1;
}
