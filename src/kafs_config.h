#pragma once

#ifdef HAVE_CONFIG_H
#include "config.h"
#else
#ifndef _GNU_SOURCE
#define _GNU_SOURCE 1
#endif

#ifndef _FILE_OFFSET_BITS
#define _FILE_OFFSET_BITS 64
#endif
#endif

#if defined(__GNUC__) || defined(__clang__)
#define KAFS_MAYBE_UNUSED __attribute__((__unused__))
#else
#define KAFS_MAYBE_UNUSED
#endif
