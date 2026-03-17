#pragma once
#include "kafs_config.h"
#include "kafs.h"

#include <string.h>

/// @brief ディレクトリエントリ
struct kafs_sdirent
{
  /// @brief inode 番号
  kafs_sinocnt_t d_ino;
  /// @brief ファイル名の長さ
  kafs_sfilenamelen_t d_filenamelen;
  /// @brief ファイル名
  char d_filename[FILENAME_MAX];
} __attribute__((packed));

typedef struct kafs_sdirent kafs_sdirent_t;

#define KAFS_DIRENT_V4_MAGIC 0x4B444952u /* 'KDIR' */
#define KAFS_DIRENT_V4_FORMAT_VERSION 1u
#define KAFS_DIRENT_FLAG_TOMBSTONE 0x0001u

struct kafs_sdir_v4_hdr
{
  kafs_su32_t dh_magic;
  uint16_t dh_format_version;
  uint16_t dh_flags;
  kafs_su32_t dh_live_count;
  kafs_su32_t dh_tombstone_count;
  kafs_su32_t dh_record_bytes;
  kafs_su32_t dh_reserved0;
} __attribute__((packed));

typedef struct kafs_sdir_v4_hdr kafs_sdir_v4_hdr_t;

struct kafs_sdirent_v4
{
  uint16_t de_rec_len;
  uint16_t de_flags;
  kafs_sinocnt_t de_ino;
  kafs_sfilenamelen_t de_filenamelen;
  kafs_su32_t de_name_hash;
  char de_filename[];
} __attribute__((packed));

typedef struct kafs_sdirent_v4 kafs_sdirent_v4_t;

static inline uint16_t kafs_dir_v4_hdr_format_get(const kafs_sdir_v4_hdr_t *hdr)
{
  return le16toh(hdr->dh_format_version);
}

static inline void kafs_dir_v4_hdr_format_set(kafs_sdir_v4_hdr_t *hdr, uint16_t v)
{
  hdr->dh_format_version = htole16(v);
}

static inline uint16_t kafs_dir_v4_hdr_flags_get(const kafs_sdir_v4_hdr_t *hdr)
{
  return le16toh(hdr->dh_flags);
}

static inline void kafs_dir_v4_hdr_flags_set(kafs_sdir_v4_hdr_t *hdr, uint16_t v)
{
  hdr->dh_flags = htole16(v);
}

static inline uint32_t kafs_dir_v4_hdr_live_count_get(const kafs_sdir_v4_hdr_t *hdr)
{
  return kafs_u32_stoh(hdr->dh_live_count);
}

static inline void kafs_dir_v4_hdr_live_count_set(kafs_sdir_v4_hdr_t *hdr, uint32_t v)
{
  hdr->dh_live_count = kafs_u32_htos(v);
}

static inline uint32_t kafs_dir_v4_hdr_tombstone_count_get(const kafs_sdir_v4_hdr_t *hdr)
{
  return kafs_u32_stoh(hdr->dh_tombstone_count);
}

static inline void kafs_dir_v4_hdr_tombstone_count_set(kafs_sdir_v4_hdr_t *hdr, uint32_t v)
{
  hdr->dh_tombstone_count = kafs_u32_htos(v);
}

static inline uint32_t kafs_dir_v4_hdr_record_bytes_get(const kafs_sdir_v4_hdr_t *hdr)
{
  return kafs_u32_stoh(hdr->dh_record_bytes);
}

static inline void kafs_dir_v4_hdr_record_bytes_set(kafs_sdir_v4_hdr_t *hdr, uint32_t v)
{
  hdr->dh_record_bytes = kafs_u32_htos(v);
}

static inline void kafs_dir_v4_hdr_init(kafs_sdir_v4_hdr_t *hdr)
{
  memset(hdr, 0, sizeof(*hdr));
  hdr->dh_magic = kafs_u32_htos(KAFS_DIRENT_V4_MAGIC);
  kafs_dir_v4_hdr_format_set(hdr, KAFS_DIRENT_V4_FORMAT_VERSION);
}

static inline uint16_t kafs_dirent_v4_rec_len_get(const kafs_sdirent_v4_t *dirent)
{
  return le16toh(dirent->de_rec_len);
}

static inline void kafs_dirent_v4_rec_len_set(kafs_sdirent_v4_t *dirent, uint16_t v)
{
  dirent->de_rec_len = htole16(v);
}

static inline uint16_t kafs_dirent_v4_flags_get(const kafs_sdirent_v4_t *dirent)
{
  return le16toh(dirent->de_flags);
}

static inline void kafs_dirent_v4_flags_set(kafs_sdirent_v4_t *dirent, uint16_t v)
{
  dirent->de_flags = htole16(v);
}

static inline kafs_inocnt_t kafs_dirent_v4_ino_get(const kafs_sdirent_v4_t *dirent)
{
  return kafs_inocnt_stoh(dirent->de_ino);
}

static inline void kafs_dirent_v4_ino_set(kafs_sdirent_v4_t *dirent, kafs_inocnt_t ino)
{
  dirent->de_ino = kafs_inocnt_htos(ino);
}

static inline kafs_filenamelen_t kafs_dirent_v4_filenamelen_get(const kafs_sdirent_v4_t *dirent)
{
  return kafs_filenamelen_stoh(dirent->de_filenamelen);
}

static inline void kafs_dirent_v4_filenamelen_set(kafs_sdirent_v4_t *dirent, kafs_filenamelen_t len)
{
  dirent->de_filenamelen = kafs_filenamelen_htos(len);
}

static inline uint32_t kafs_dirent_v4_name_hash_get(const kafs_sdirent_v4_t *dirent)
{
  return kafs_u32_stoh(dirent->de_name_hash);
}

static inline void kafs_dirent_v4_name_hash_set(kafs_sdirent_v4_t *dirent, uint32_t hash)
{
  dirent->de_name_hash = kafs_u32_htos(hash);
}

static kafs_inocnt_t kafs_dirent_ino_get(const struct kafs_sdirent *dirent)
{
  return kafs_inocnt_stoh(dirent->d_ino);
}

static void kafs_dirent_ino_set(struct kafs_sdirent *dirent, kafs_inocnt_t ino)
{
  dirent->d_ino = kafs_inocnt_htos(ino);
}

static kafs_filenamelen_t kafs_dirent_filenamelen_get(const struct kafs_sdirent *dirent)
{
  return kafs_filenamelen_stoh(dirent->d_filenamelen);
}

static void kafs_dirent_filename_set(struct kafs_sdirent *dirent, const char *filename)
{
  kafs_filenamelen_t filenamelen = strlen(filename);
  memcpy(dirent->d_filename, filename, filenamelen);
  dirent->d_filenamelen = kafs_filenamelen_htos(filenamelen);
}

__attribute_maybe_unused__ static void kafs_dirent_set(struct kafs_sdirent *dirent,
                                                       kafs_inocnt_t ino, const char *filename)
{
  kafs_dirent_ino_set(dirent, ino);
  kafs_dirent_filename_set(dirent, filename);
}
