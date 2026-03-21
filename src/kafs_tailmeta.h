#pragma once

#include "kafs_superblock.h"

#include <errno.h>
#include <stdint.h>
#include <string.h>

#define KAFS_TAILMETA_REGION_MAGIC 0x4B544D52u /* 'KTMR' */
#define KAFS_TAILMETA_REGION_VERSION 1u
#define KAFS_TAILMETA_CONTAINER_MAGIC 0x4B544D43u /* 'KTMC' */
#define KAFS_TAILMETA_CONTAINER_VERSION 1u

struct kafs_stailmeta_region_hdr
{
  kafs_su32_t tr_magic;
  uint16_t tr_version;
  uint16_t tr_flags;
  kafs_su32_t tr_header_bytes;
  kafs_su32_t tr_container_table_off;
  kafs_su32_t tr_container_table_bytes;
  kafs_su32_t tr_container_count;
  uint16_t tr_class_count;
  uint16_t tr_slot_desc_bytes;
  kafs_su32_t tr_generation;
  kafs_su32_t tr_reserved0;
} __attribute__((packed));

typedef struct kafs_stailmeta_region_hdr kafs_tailmeta_region_hdr_t;

struct kafs_stailmeta_container_hdr
{
  kafs_su32_t tc_magic;
  uint16_t tc_version;
  uint16_t tc_flags;
  uint16_t tc_class_bytes;
  uint16_t tc_slot_count;
  uint16_t tc_live_count;
  uint16_t tc_free_bytes;
  kafs_su32_t tc_generation;
  kafs_su32_t tc_slot_table_off;
  kafs_su32_t tc_slot_table_bytes;
  kafs_su32_t tc_owner_checksum;
  kafs_su32_t tc_reserved0;
} __attribute__((packed));

typedef struct kafs_stailmeta_container_hdr kafs_tailmeta_container_hdr_t;

struct kafs_stailmeta_slot_desc
{
  kafs_sinocnt_t ts_owner_ino;
  kafs_su32_t ts_generation;
  uint16_t ts_len;
  uint16_t ts_flags;
} __attribute__((packed));

typedef struct kafs_stailmeta_slot_desc kafs_tailmeta_slot_desc_t;

_Static_assert(sizeof(kafs_tailmeta_region_hdr_t) == 36,
               "kafs_tailmeta_region_hdr_t must be 36 bytes");
_Static_assert(sizeof(kafs_tailmeta_container_hdr_t) == 36,
               "kafs_tailmeta_container_hdr_t must be 36 bytes");
_Static_assert(sizeof(kafs_tailmeta_slot_desc_t) == 12,
               "kafs_tailmeta_slot_desc_t must be 12 bytes");

static inline uint16_t kafs_tailmeta_region_hdr_version_get(const kafs_tailmeta_region_hdr_t *hdr)
{
  return le16toh(hdr->tr_version);
}

static inline void kafs_tailmeta_region_hdr_version_set(kafs_tailmeta_region_hdr_t *hdr, uint16_t v)
{
  hdr->tr_version = htole16(v);
}

static inline uint16_t kafs_tailmeta_region_hdr_flags_get(const kafs_tailmeta_region_hdr_t *hdr)
{
  return le16toh(hdr->tr_flags);
}

static inline void kafs_tailmeta_region_hdr_flags_set(kafs_tailmeta_region_hdr_t *hdr, uint16_t v)
{
  hdr->tr_flags = htole16(v);
}

static inline uint32_t
kafs_tailmeta_region_hdr_header_bytes_get(const kafs_tailmeta_region_hdr_t *hdr)
{
  return kafs_u32_stoh(hdr->tr_header_bytes);
}

static inline void kafs_tailmeta_region_hdr_header_bytes_set(kafs_tailmeta_region_hdr_t *hdr,
                                                             uint32_t v)
{
  hdr->tr_header_bytes = kafs_u32_htos(v);
}

static inline uint32_t
kafs_tailmeta_region_hdr_container_table_off_get(const kafs_tailmeta_region_hdr_t *hdr)
{
  return kafs_u32_stoh(hdr->tr_container_table_off);
}

static inline void kafs_tailmeta_region_hdr_container_table_off_set(kafs_tailmeta_region_hdr_t *hdr,
                                                                    uint32_t v)
{
  hdr->tr_container_table_off = kafs_u32_htos(v);
}

static inline uint32_t
kafs_tailmeta_region_hdr_container_table_bytes_get(const kafs_tailmeta_region_hdr_t *hdr)
{
  return kafs_u32_stoh(hdr->tr_container_table_bytes);
}

static inline void
kafs_tailmeta_region_hdr_container_table_bytes_set(kafs_tailmeta_region_hdr_t *hdr, uint32_t v)
{
  hdr->tr_container_table_bytes = kafs_u32_htos(v);
}

static inline uint32_t
kafs_tailmeta_region_hdr_container_count_get(const kafs_tailmeta_region_hdr_t *hdr)
{
  return kafs_u32_stoh(hdr->tr_container_count);
}

static inline void kafs_tailmeta_region_hdr_container_count_set(kafs_tailmeta_region_hdr_t *hdr,
                                                                uint32_t v)
{
  hdr->tr_container_count = kafs_u32_htos(v);
}

static inline uint16_t
kafs_tailmeta_region_hdr_class_count_get(const kafs_tailmeta_region_hdr_t *hdr)
{
  return le16toh(hdr->tr_class_count);
}

static inline void kafs_tailmeta_region_hdr_class_count_set(kafs_tailmeta_region_hdr_t *hdr,
                                                            uint16_t v)
{
  hdr->tr_class_count = htole16(v);
}

static inline uint16_t
kafs_tailmeta_region_hdr_slot_desc_bytes_get(const kafs_tailmeta_region_hdr_t *hdr)
{
  return le16toh(hdr->tr_slot_desc_bytes);
}

static inline void kafs_tailmeta_region_hdr_slot_desc_bytes_set(kafs_tailmeta_region_hdr_t *hdr,
                                                                uint16_t v)
{
  hdr->tr_slot_desc_bytes = htole16(v);
}

static inline void kafs_tailmeta_region_hdr_init(kafs_tailmeta_region_hdr_t *hdr)
{
  memset(hdr, 0, sizeof(*hdr));
  hdr->tr_magic = kafs_u32_htos(KAFS_TAILMETA_REGION_MAGIC);
  kafs_tailmeta_region_hdr_version_set(hdr, KAFS_TAILMETA_REGION_VERSION);
  kafs_tailmeta_region_hdr_header_bytes_set(hdr, (uint32_t)sizeof(*hdr));
  kafs_tailmeta_region_hdr_slot_desc_bytes_set(hdr, (uint16_t)sizeof(kafs_tailmeta_slot_desc_t));
}

static inline uint16_t
kafs_tailmeta_container_hdr_version_get(const kafs_tailmeta_container_hdr_t *hdr)
{
  return le16toh(hdr->tc_version);
}

static inline void kafs_tailmeta_container_hdr_version_set(kafs_tailmeta_container_hdr_t *hdr,
                                                           uint16_t v)
{
  hdr->tc_version = htole16(v);
}

static inline uint16_t
kafs_tailmeta_container_hdr_flags_get(const kafs_tailmeta_container_hdr_t *hdr)
{
  return le16toh(hdr->tc_flags);
}

static inline void kafs_tailmeta_container_hdr_flags_set(kafs_tailmeta_container_hdr_t *hdr,
                                                         uint16_t v)
{
  hdr->tc_flags = htole16(v);
}

static inline uint16_t
kafs_tailmeta_container_hdr_class_bytes_get(const kafs_tailmeta_container_hdr_t *hdr)
{
  return le16toh(hdr->tc_class_bytes);
}

static inline void kafs_tailmeta_container_hdr_class_bytes_set(kafs_tailmeta_container_hdr_t *hdr,
                                                               uint16_t v)
{
  hdr->tc_class_bytes = htole16(v);
}

static inline uint16_t
kafs_tailmeta_container_hdr_slot_count_get(const kafs_tailmeta_container_hdr_t *hdr)
{
  return le16toh(hdr->tc_slot_count);
}

static inline void kafs_tailmeta_container_hdr_slot_count_set(kafs_tailmeta_container_hdr_t *hdr,
                                                              uint16_t v)
{
  hdr->tc_slot_count = htole16(v);
}

static inline uint16_t
kafs_tailmeta_container_hdr_live_count_get(const kafs_tailmeta_container_hdr_t *hdr)
{
  return le16toh(hdr->tc_live_count);
}

static inline void kafs_tailmeta_container_hdr_live_count_set(kafs_tailmeta_container_hdr_t *hdr,
                                                              uint16_t v)
{
  hdr->tc_live_count = htole16(v);
}

static inline uint16_t
kafs_tailmeta_container_hdr_free_bytes_get(const kafs_tailmeta_container_hdr_t *hdr)
{
  return le16toh(hdr->tc_free_bytes);
}

static inline void kafs_tailmeta_container_hdr_free_bytes_set(kafs_tailmeta_container_hdr_t *hdr,
                                                              uint16_t v)
{
  hdr->tc_free_bytes = htole16(v);
}

static inline uint32_t
kafs_tailmeta_container_hdr_slot_table_off_get(const kafs_tailmeta_container_hdr_t *hdr)
{
  return kafs_u32_stoh(hdr->tc_slot_table_off);
}

static inline void
kafs_tailmeta_container_hdr_slot_table_off_set(kafs_tailmeta_container_hdr_t *hdr, uint32_t v)
{
  hdr->tc_slot_table_off = kafs_u32_htos(v);
}

static inline uint32_t
kafs_tailmeta_container_hdr_slot_table_bytes_get(const kafs_tailmeta_container_hdr_t *hdr)
{
  return kafs_u32_stoh(hdr->tc_slot_table_bytes);
}

static inline void
kafs_tailmeta_container_hdr_slot_table_bytes_set(kafs_tailmeta_container_hdr_t *hdr, uint32_t v)
{
  hdr->tc_slot_table_bytes = kafs_u32_htos(v);
}

static inline void kafs_tailmeta_container_hdr_init(kafs_tailmeta_container_hdr_t *hdr)
{
  memset(hdr, 0, sizeof(*hdr));
  hdr->tc_magic = kafs_u32_htos(KAFS_TAILMETA_CONTAINER_MAGIC);
  kafs_tailmeta_container_hdr_version_set(hdr, KAFS_TAILMETA_CONTAINER_VERSION);
}

static inline kafs_inocnt_t kafs_tailmeta_slot_owner_ino_get(const kafs_tailmeta_slot_desc_t *slot)
{
  return kafs_inocnt_stoh(slot->ts_owner_ino);
}

static inline void kafs_tailmeta_slot_owner_ino_set(kafs_tailmeta_slot_desc_t *slot,
                                                    kafs_inocnt_t ino)
{
  slot->ts_owner_ino = kafs_inocnt_htos(ino);
}

static inline uint16_t kafs_tailmeta_slot_len_get(const kafs_tailmeta_slot_desc_t *slot)
{
  return le16toh(slot->ts_len);
}

static inline void kafs_tailmeta_slot_len_set(kafs_tailmeta_slot_desc_t *slot, uint16_t v)
{
  slot->ts_len = htole16(v);
}

static inline uint16_t kafs_tailmeta_slot_flags_get(const kafs_tailmeta_slot_desc_t *slot)
{
  return le16toh(slot->ts_flags);
}

static inline void kafs_tailmeta_slot_flags_set(kafs_tailmeta_slot_desc_t *slot, uint16_t v)
{
  slot->ts_flags = htole16(v);
}

static inline int kafs_tailmeta_region_present(const kafs_ssuperblock_t *sb)
{
  if (!sb)
    return 0;
  if ((kafs_sb_feature_flags_get(sb) & KAFS_FEATURE_TAIL_META_REGION) != 0)
    return 1;
  if (kafs_sb_tailmeta_offset_get(sb) != 0)
    return 1;
  if (kafs_sb_tailmeta_size_get(sb) != 0)
    return 1;
  return 0;
}

static inline int kafs_tailmeta_region_hdr_validate(const kafs_tailmeta_region_hdr_t *hdr,
                                                    uint64_t region_size)
{
  uint64_t table_end = 0;
  uint64_t expected_table_bytes = 0;
  uint32_t container_count = 0;
  uint32_t table_bytes = 0;
  uint32_t table_off = 0;
  uint32_t header_bytes = 0;

  if (!hdr)
    return -EINVAL;
  if (region_size < sizeof(*hdr))
    return -ERANGE;
  if (kafs_u32_stoh(hdr->tr_magic) != KAFS_TAILMETA_REGION_MAGIC)
    return -EPROTO;
  if (kafs_tailmeta_region_hdr_version_get(hdr) != KAFS_TAILMETA_REGION_VERSION)
    return -EPROTONOSUPPORT;
  if (kafs_tailmeta_region_hdr_flags_get(hdr) != 0u)
    return -EPROTO;

  header_bytes = kafs_tailmeta_region_hdr_header_bytes_get(hdr);
  table_off = kafs_tailmeta_region_hdr_container_table_off_get(hdr);
  table_bytes = kafs_tailmeta_region_hdr_container_table_bytes_get(hdr);
  container_count = kafs_tailmeta_region_hdr_container_count_get(hdr);

  if (header_bytes < sizeof(*hdr) || (uint64_t)header_bytes > region_size)
    return -ERANGE;
  if (kafs_tailmeta_region_hdr_slot_desc_bytes_get(hdr) != sizeof(kafs_tailmeta_slot_desc_t))
    return -EPROTO;

  if (container_count == 0)
    return (table_bytes == 0u) ? 0 : -EPROTO;

  if (kafs_tailmeta_region_hdr_class_count_get(hdr) == 0u)
    return -EPROTO;

  expected_table_bytes = (uint64_t)container_count * sizeof(kafs_tailmeta_container_hdr_t);
  if ((uint64_t)table_bytes != expected_table_bytes)
    return -EPROTO;
  if (table_off < header_bytes)
    return -EPROTO;

  table_end = (uint64_t)table_off + (uint64_t)table_bytes;
  if (table_end > region_size)
    return -ERANGE;
  return 0;
}

static inline int kafs_tailmeta_container_hdr_validate(const kafs_tailmeta_container_hdr_t *hdr,
                                                       uint64_t region_size,
                                                       uint16_t slot_desc_bytes)
{
  uint64_t slot_table_end = 0;
  uint64_t expected_slot_bytes = 0;
  uint16_t slot_count = 0;
  uint16_t live_count = 0;
  uint16_t class_bytes = 0;
  uint16_t free_bytes = 0;
  uint32_t slot_table_off = 0;
  uint32_t slot_table_bytes = 0;

  if (!hdr)
    return -EINVAL;
  if (kafs_u32_stoh(hdr->tc_magic) != KAFS_TAILMETA_CONTAINER_MAGIC)
    return -EPROTO;
  if (kafs_tailmeta_container_hdr_version_get(hdr) != KAFS_TAILMETA_CONTAINER_VERSION)
    return -EPROTONOSUPPORT;
  if (kafs_tailmeta_container_hdr_flags_get(hdr) != 0u)
    return -EPROTO;

  class_bytes = kafs_tailmeta_container_hdr_class_bytes_get(hdr);
  slot_count = kafs_tailmeta_container_hdr_slot_count_get(hdr);
  live_count = kafs_tailmeta_container_hdr_live_count_get(hdr);
  free_bytes = kafs_tailmeta_container_hdr_free_bytes_get(hdr);
  slot_table_off = kafs_tailmeta_container_hdr_slot_table_off_get(hdr);
  slot_table_bytes = kafs_tailmeta_container_hdr_slot_table_bytes_get(hdr);

  if (class_bytes == 0u || slot_desc_bytes != sizeof(kafs_tailmeta_slot_desc_t))
    return -EPROTO;
  if (live_count > slot_count)
    return -EPROTO;

  expected_slot_bytes = (uint64_t)slot_count * (uint64_t)slot_desc_bytes;
  if ((uint64_t)slot_table_bytes != expected_slot_bytes)
    return -EPROTO;
  if (slot_count == 0u)
    return (slot_table_off == 0u && slot_table_bytes == 0u) ? 0 : -EPROTO;

  slot_table_end = (uint64_t)slot_table_off + (uint64_t)slot_table_bytes;
  if (slot_table_off == 0u || slot_table_end > region_size)
    return -ERANGE;
  if ((uint64_t)free_bytes > (uint64_t)slot_count * (uint64_t)class_bytes)
    return -EPROTO;
  return 0;
}

static inline int kafs_tailmeta_slot_validate(const kafs_tailmeta_slot_desc_t *slot,
                                              uint16_t class_bytes)
{
  kafs_inocnt_t owner_ino = 0;
  uint16_t len = 0;

  if (!slot)
    return -EINVAL;

  owner_ino = kafs_tailmeta_slot_owner_ino_get(slot);
  len = kafs_tailmeta_slot_len_get(slot);
  if (kafs_tailmeta_slot_flags_get(slot) != 0u)
    return -EPROTO;
  if (len > class_bytes)
    return -EPROTO;
  if (owner_ino == KAFS_INO_NONE)
  {
    if (len != 0u || kafs_u32_stoh(slot->ts_generation) != 0u)
      return -EPROTO;
    return 0;
  }

  return (len == 0u) ? -EPROTO : 0;
}