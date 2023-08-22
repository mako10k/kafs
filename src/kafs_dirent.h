#pragma once
#include "kafs.h"

/// @brief ディレクトリエントリ
struct kafs_sdirent
{
  /// @brief inode 番号
  kafs_sinocnt_t d_ino;
  /// @brief ファイル名の長さ
  kafs_sfilenamelen_t d_filenamelen;
  /// @brief ファイル名
  char d_filename[0];
} __attribute__((packed));

typedef struct kafs_sdirent kafs_sdirent_t;
