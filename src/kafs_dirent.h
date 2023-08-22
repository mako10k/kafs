#pragma once
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

static kafs_inocnt_t
kafs_dirent_ino_get (const struct kafs_sdirent *dirent)
{
  return kafs_inocnt_stoh (dirent->d_ino);
}

static kafs_filenamelen_t
kafs_dirent_filenamelen_get (const struct kafs_sdirent *dirent)
{
  return kafs_filenamelen_stoh (dirent->d_filenamelen);
}

static void
kafs_dirent_ino_set (struct kafs_sdirent *dirent, kafs_inocnt_t ino)
{
  dirent->d_ino = kafs_inocnt_htos (ino);
}

static void
kafs_dirent_filenamelen_set (struct kafs_sdirent *dirent, kafs_filenamelen_t filenamelen)
{
  dirent->d_filenamelen = kafs_filenamelen_htos (filenamelen);
}

static void
kafs_dirent_filename_set (struct kafs_sdirent *dirent, const char *filename, kafs_filenamelen_t filenamelen)
{
  memcpy (dirent->d_filename, filename, filenamelen);
  kafs_dirent_filenamelen_set (dirent, filenamelen);
}

static void
kafs_dirent_set (struct kafs_sdirent *dirent, kafs_inocnt_t ino, const char *filename, kafs_filenamelen_t filenamelen)
{
  kafs_dirent_ino_set (dirent, ino);
  kafs_dirent_filename_set (dirent, filename, filenamelen);
}
