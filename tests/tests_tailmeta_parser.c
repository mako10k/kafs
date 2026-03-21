#include "kafs_tailmeta.h"

#include <assert.h>
#include <string.h>

int main(void)
{
  kafs_ssuperblock_t sb;
  memset(&sb, 0, sizeof(sb));
  assert(!kafs_tailmeta_region_present(&sb));
  kafs_sb_feature_flags_set(&sb, KAFS_FEATURE_TAIL_META_REGION);
  kafs_sb_tailmeta_offset_set(&sb, 8192);
  kafs_sb_tailmeta_size_set(&sb, 4096);
  assert(kafs_tailmeta_region_present(&sb));

  kafs_tailmeta_region_hdr_t region;
  kafs_tailmeta_region_hdr_init(&region);
  kafs_tailmeta_region_hdr_container_table_off_set(&region, 64);
  kafs_tailmeta_region_hdr_container_table_bytes_set(&region, sizeof(kafs_tailmeta_container_hdr_t));
  kafs_tailmeta_region_hdr_container_count_set(&region, 1);
  kafs_tailmeta_region_hdr_class_count_set(&region, 1);
  assert(kafs_tailmeta_region_hdr_validate(&region, 4096) == 0);
  kafs_tailmeta_region_hdr_slot_desc_bytes_set(&region, sizeof(kafs_tailmeta_slot_desc_t) - 1u);
  assert(kafs_tailmeta_region_hdr_validate(&region, 4096) != 0);
  kafs_tailmeta_region_hdr_slot_desc_bytes_set(&region, sizeof(kafs_tailmeta_slot_desc_t));

  kafs_tailmeta_container_hdr_t container;
  kafs_tailmeta_container_hdr_init(&container);
  kafs_tailmeta_container_hdr_class_bytes_set(&container, 128);
  kafs_tailmeta_container_hdr_slot_count_set(&container, 4);
  kafs_tailmeta_container_hdr_live_count_set(&container, 2);
  kafs_tailmeta_container_hdr_free_bytes_set(&container, 256);
  kafs_tailmeta_container_hdr_slot_table_off_set(&container, 128);
  kafs_tailmeta_container_hdr_slot_table_bytes_set(&container, 4u * sizeof(kafs_tailmeta_slot_desc_t));
  assert(kafs_tailmeta_container_hdr_validate(&container, 4096,
                                              sizeof(kafs_tailmeta_slot_desc_t)) == 0);
  kafs_tailmeta_container_hdr_live_count_set(&container, 5);
  assert(kafs_tailmeta_container_hdr_validate(&container, 4096,
                                              sizeof(kafs_tailmeta_slot_desc_t)) != 0);
  kafs_tailmeta_container_hdr_live_count_set(&container, 2);

  kafs_tailmeta_slot_desc_t slot;
  memset(&slot, 0, sizeof(slot));
  assert(kafs_tailmeta_slot_validate(&slot, 128) == 0);
  kafs_tailmeta_slot_owner_ino_set(&slot, 7);
  kafs_tailmeta_slot_len_set(&slot, 64);
  assert(kafs_tailmeta_slot_validate(&slot, 128) == 0);
  kafs_tailmeta_slot_len_set(&slot, 0);
  assert(kafs_tailmeta_slot_validate(&slot, 128) != 0);

  kafs_tailmeta_inode_desc_t desc;
  kafs_tailmeta_inode_desc_init(&desc);
  assert(kafs_tailmeta_inode_desc_validate(&desc, 128) == 0);
  assert(!kafs_tailmeta_inode_desc_uses_tail_storage(&desc));

  kafs_tailmeta_inode_desc_layout_kind_set(&desc, KAFS_TAIL_LAYOUT_TAIL_ONLY);
  kafs_tailmeta_inode_desc_flags_set(&desc, KAFS_TAILDESC_FLAG_PACKED_SMALL_FILE);
  kafs_tailmeta_inode_desc_fragment_len_set(&desc, 64);
  kafs_tailmeta_inode_desc_container_blo_set(&desc, 9);
  kafs_tailmeta_inode_desc_fragment_off_set(&desc, 32);
  kafs_tailmeta_inode_desc_generation_set(&desc, 11);
  assert(kafs_tailmeta_inode_desc_validate(&desc, 128) == 0);
  assert(kafs_tailmeta_inode_desc_uses_tail_storage(&desc));

  memset(&slot, 0, sizeof(slot));
  kafs_tailmeta_slot_owner_ino_set(&slot, 7);
  slot.ts_generation = kafs_u32_htos(11);
  kafs_tailmeta_slot_len_set(&slot, 64);
  assert(kafs_tailmeta_inode_desc_matches_slot(&desc, &slot, 128, 7) == 0);
  kafs_tailmeta_slot_len_set(&slot, 63);
  assert(kafs_tailmeta_inode_desc_matches_slot(&desc, &slot, 128, 7) != 0);

  kafs_tailmeta_slot_len_set(&slot, 64);
  slot.ts_generation = kafs_u32_htos(12);
  assert(kafs_tailmeta_inode_desc_matches_slot(&desc, &slot, 128, 7) != 0);

  kafs_tailmeta_inode_desc_fragment_off_set(&desc, 96);
  assert(kafs_tailmeta_inode_desc_validate(&desc, 128) != 0);

  return 0;
}