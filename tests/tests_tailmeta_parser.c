#include "kafs_tailmeta.h"

#include <assert.h>
#include <string.h>

static void assert_tailmeta_inode_desc_fields(const kafs_tailmeta_inode_desc_t *desc,
                                                                                                                                                                                        uint8_t layout_kind,
                                                                                                                                                                                        uint8_t flags,
                                                                                                                                                                                        uint16_t fragment_len,
                                                                                                                                                                                        kafs_blkcnt_t container_blo,
                                                                                                                                                                                        uint16_t fragment_off,
                                                                                                                                                                                        uint32_t generation)
{
        assert(kafs_tailmeta_inode_desc_layout_kind_get(desc) == layout_kind);
        assert(kafs_tailmeta_inode_desc_flags_get(desc) == flags);
        assert(kafs_tailmeta_inode_desc_fragment_len_get(desc) == fragment_len);
        assert(kafs_tailmeta_inode_desc_container_blo_get(desc) == container_blo);
        assert(kafs_tailmeta_inode_desc_fragment_off_get(desc) == fragment_off);
        assert(kafs_tailmeta_inode_desc_generation_get(desc) == generation);
}

static void assert_inode_taildesc_fields(const kafs_sinode_taildesc_v5_t *taildesc,
                                                                                                                                                                 uint8_t layout_kind,
                                                                                                                                                                 uint8_t flags,
                                                                                                                                                                 uint16_t fragment_len,
                                                                                                                                                                 kafs_blkcnt_t container_blo,
                                                                                                                                                                 uint16_t fragment_off,
                                                                                                                                                                 uint32_t generation)
{
        assert(kafs_ino_taildesc_v5_layout_kind_get(taildesc) == layout_kind);
        assert(kafs_ino_taildesc_v5_flags_get(taildesc) == flags);
        assert(kafs_ino_taildesc_v5_fragment_len_get(taildesc) == fragment_len);
        assert(kafs_ino_taildesc_v5_container_blo_get(taildesc) == container_blo);
        assert(kafs_ino_taildesc_v5_fragment_off_get(taildesc) == fragment_off);
        assert(kafs_ino_taildesc_v5_generation_get(taildesc) == generation);
}

static void assert_tail_layout_storage_matrix(uint8_t layout_kind, int uses_tail_storage)
{
        kafs_sinode_taildesc_v5_t inode_taildesc;
        kafs_tailmeta_inode_desc_t desc;

        kafs_ino_taildesc_v5_init(&inode_taildesc);
        kafs_ino_taildesc_v5_layout_kind_set(&inode_taildesc, layout_kind);
        kafs_tailmeta_inode_desc_init(&desc);
        kafs_tailmeta_inode_desc_layout_kind_set(&desc, layout_kind);

        assert(kafs_tail_layout_uses_tail_storage(layout_kind) == uses_tail_storage);
        assert(kafs_ino_taildesc_v5_uses_tail_storage(&inode_taildesc) == uses_tail_storage);
        assert(kafs_tailmeta_inode_desc_uses_tail_storage(&desc) == uses_tail_storage);
}

static void assert_tail_layout_known_matrix(uint8_t layout_kind, int is_known)
{
        assert(kafs_tail_layout_is_known(layout_kind) == is_known);
}

static void assert_validate_for_inode_result(const kafs_tailmeta_inode_desc_t *desc,
                                                                                                                                                         kafs_off_t inode_size,
                                                                                                                                                         uint16_t class_bytes,
                                                                                                                                                         kafs_blksize_t blksize,
                                                                                                                                                         int is_valid)
{
        assert((kafs_tailmeta_inode_desc_validate_for_inode(desc, inode_size, class_bytes, blksize) == 0) == is_valid);
}

int main(void)
{
        assert_tail_layout_known_matrix(KAFS_TAIL_LAYOUT_INLINE, 1);
        assert_tail_layout_known_matrix(KAFS_TAIL_LAYOUT_FULL_BLOCK, 1);
        assert_tail_layout_known_matrix(KAFS_TAIL_LAYOUT_TAIL_ONLY, 1);
        assert_tail_layout_known_matrix(KAFS_TAIL_LAYOUT_MIXED_FULL_TAIL, 1);
        assert_tail_layout_known_matrix(UINT8_MAX, 0);

        assert_tail_layout_storage_matrix(KAFS_TAIL_LAYOUT_INLINE, 0);
        assert_tail_layout_storage_matrix(KAFS_TAIL_LAYOUT_FULL_BLOCK, 0);
        assert_tail_layout_storage_matrix(KAFS_TAIL_LAYOUT_TAIL_ONLY, 1);
        assert_tail_layout_storage_matrix(KAFS_TAIL_LAYOUT_MIXED_FULL_TAIL, 1);
        assert_tail_layout_storage_matrix(UINT8_MAX, 0);

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
        kafs_tailmeta_inode_desc_layout_kind_set(&desc, UINT8_MAX);
        assert(kafs_tailmeta_inode_desc_validate(&desc, 128) != 0);
        kafs_tailmeta_inode_desc_init(&desc);

  kafs_tailmeta_inode_desc_layout_kind_set(&desc, KAFS_TAIL_LAYOUT_FULL_BLOCK);
  assert(kafs_tailmeta_inode_desc_validate(&desc, 128) == 0);
  assert_validate_for_inode_result(&desc, 0, 128, 4096, 1);
  assert_validate_for_inode_result(&desc, (kafs_off_t)kafs_inode_inline_bytes() + 1, 128, 4096, 1);
  assert_validate_for_inode_result(&desc, 1, 128, 4096, 0);
  assert_validate_for_inode_result(&desc, (kafs_off_t)kafs_inode_inline_bytes(), 128, 4096, 0);

  kafs_tailmeta_inode_desc_layout_kind_set(&desc, KAFS_TAIL_LAYOUT_TAIL_ONLY);
  kafs_tailmeta_inode_desc_flags_set(&desc, KAFS_TAILDESC_FLAG_PACKED_SMALL_FILE);
  kafs_tailmeta_inode_desc_fragment_len_set(&desc, 64);
  kafs_tailmeta_inode_desc_container_blo_set(&desc, 9);
        kafs_tailmeta_inode_desc_fragment_off_set(&desc, 128);
  kafs_tailmeta_inode_desc_generation_set(&desc, 11);
  assert(kafs_tailmeta_inode_desc_validate(&desc, 128) == 0);
  assert(kafs_tailmeta_inode_desc_uses_tail_storage(&desc));

        {
                kafs_sinode_taildesc_v5_t inode_taildesc;
                kafs_sinode_taildesc_v5_t inode_taildesc_roundtrip;
                kafs_tailmeta_inode_desc_t desc_from_inode;

                kafs_ino_taildesc_v5_init(&inode_taildesc);
                kafs_ino_taildesc_v5_layout_kind_set(&inode_taildesc, KAFS_TAIL_LAYOUT_TAIL_ONLY);
                kafs_ino_taildesc_v5_flags_set(&inode_taildesc, KAFS_TAILDESC_FLAG_PACKED_SMALL_FILE);
                kafs_ino_taildesc_v5_fragment_len_set(&inode_taildesc, 64);
                kafs_ino_taildesc_v5_container_blo_set(&inode_taildesc, 9);
                kafs_ino_taildesc_v5_fragment_off_set(&inode_taildesc, 128);
                kafs_ino_taildesc_v5_generation_set(&inode_taildesc, 11);
                assert(kafs_ino_taildesc_v5_uses_tail_storage(&inode_taildesc));
                assert_inode_taildesc_fields(&inode_taildesc, KAFS_TAIL_LAYOUT_TAIL_ONLY,
                                                                                                                                 KAFS_TAILDESC_FLAG_PACKED_SMALL_FILE, 64, 9, 128, 11);

                kafs_tailmeta_inode_desc_from_inode_taildesc(&desc_from_inode, &inode_taildesc);
                assert_tailmeta_inode_desc_fields(&desc_from_inode, KAFS_TAIL_LAYOUT_TAIL_ONLY,
                                                                                                                                                        KAFS_TAILDESC_FLAG_PACKED_SMALL_FILE, 64, 9, 128, 11);

                kafs_ino_taildesc_v5_init(&inode_taildesc_roundtrip);
                kafs_tailmeta_inode_desc_to_inode_taildesc(&inode_taildesc_roundtrip, &desc_from_inode);
                assert_inode_taildesc_fields(&inode_taildesc_roundtrip, KAFS_TAIL_LAYOUT_TAIL_ONLY,
                                                                                                                                 KAFS_TAILDESC_FLAG_PACKED_SMALL_FILE, 64, 9, 128, 11);
        }

  memset(&slot, 0, sizeof(slot));
  kafs_tailmeta_slot_owner_ino_set(&slot, 7);
  slot.ts_generation = kafs_u32_htos(11);
  kafs_tailmeta_slot_len_set(&slot, 64);
  assert(kafs_tailmeta_inode_desc_matches_slot(&desc, &slot, 128, 7) == 0);
  assert(kafs_tailmeta_inode_desc_report_flags(&desc, &slot, 128, 7, 64, 4096) == 0);
  {
    uint16_t expected_len = 0;
    assert(kafs_tailmeta_slot_expected_len_for_inode(64, 4096, &expected_len) == 0);
    assert(expected_len == 64);
    assert(kafs_tailmeta_slot_matches_inode_size(&slot, 64, 4096) == 0);
    assert(kafs_tailmeta_slot_expected_len_for_inode(4096 + 64, 4096, &expected_len) == 0);
    assert(expected_len == 64);
    assert(kafs_tailmeta_slot_matches_inode_size(&slot, 4096 + 64, 4096) == 0);
    assert(kafs_tailmeta_slot_expected_len_for_inode(8192, 4096, &expected_len) != 0);
    assert(kafs_tailmeta_slot_matches_inode_size(&slot, 8192, 4096) != 0);
  }
  assert(kafs_tailmeta_inode_desc_validate_for_inode(&desc, 64, 128, 4096) == 0);
  assert(kafs_tailmeta_inode_desc_validate_for_inode(&desc, 60, 128, 4096) != 0);
  assert(kafs_tailmeta_inode_desc_matches_slot_for_inode(&desc, &slot, 128, 7, 64, 4096) == 0);
  kafs_tailmeta_slot_len_set(&slot, 63);
  assert(kafs_tailmeta_inode_desc_matches_slot(&desc, &slot, 128, 7) != 0);
  assert(kafs_tailmeta_slot_matches_inode_size(&slot, 64, 4096) != 0);
  assert((kafs_tailmeta_inode_desc_report_flags(&desc, &slot, 128, 7, 64, 4096) &
          KAFS_TAILCHECK_LENGTH_MISMATCH) != 0);

  kafs_tailmeta_slot_len_set(&slot, 64);
  slot.ts_generation = kafs_u32_htos(12);
  assert(kafs_tailmeta_inode_desc_matches_slot(&desc, &slot, 128, 7) != 0);
  assert((kafs_tailmeta_inode_desc_report_flags(&desc, &slot, 128, 7, 64, 4096) &
          KAFS_TAILCHECK_GENERATION_MISMATCH) != 0);

  slot.ts_generation = kafs_u32_htos(11);
  kafs_tailmeta_slot_owner_ino_set(&slot, 8);
  assert((kafs_tailmeta_inode_desc_report_flags(&desc, &slot, 128, 7, 64, 4096) &
          KAFS_TAILCHECK_OWNER_MISMATCH) != 0);
  kafs_tailmeta_slot_owner_ino_set(&slot, 7);

        kafs_tailmeta_inode_desc_fragment_off_set(&desc, 96);
  assert(kafs_tailmeta_inode_desc_validate(&desc, 128) != 0);
        kafs_tailmeta_inode_desc_fragment_off_set(&desc, 128);

  kafs_tailmeta_inode_desc_layout_kind_set(&desc, KAFS_TAIL_LAYOUT_MIXED_FULL_TAIL);
  kafs_tailmeta_inode_desc_flags_set(&desc, KAFS_TAILDESC_FLAG_FINAL_TAIL);
  assert(kafs_tailmeta_inode_desc_validate_for_inode(&desc, 4096 + 64, 128, 4096) == 0);
  assert(kafs_tailmeta_inode_desc_validate_for_inode(&desc, 4000, 128, 4096) != 0);
  assert((kafs_tailmeta_inode_desc_report_flags(&desc, &slot, 128, 7, 4000, 4096) &
          KAFS_TAILCHECK_INVALID_INODE_SIZE) != 0);

  kafs_tailmeta_inode_desc_init(&desc);
  assert(kafs_tailmeta_inode_desc_validate_for_inode(&desc, 32, 128, 4096) == 0);
  assert(kafs_tailmeta_inode_desc_validate_for_inode(&desc, 128, 128, 4096) != 0);

  return 0;
}