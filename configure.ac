#                                               -*- Autoconf -*-
# Process this file with autoconf to produce a configure script.

AC_PREREQ([2.71])
AC_INIT([kafs],[0.3.1],[mako10k@mk10.org])
AC_CONFIG_SRCDIR([src/kafs.c])
AC_CONFIG_HEADERS([config.h])
AM_INIT_AUTOMAKE([foreign])
AC_USE_SYSTEM_EXTENSIONS
AC_SYS_LARGEFILE
PKG_CHECK_MODULES([KAFS], [fuse3])

AC_ARG_ENABLE([lto],
    [AS_HELP_STRING([--enable-lto], [enable link-time optimization (-flto)])],
    [enable_lto="$enableval"],
    [enable_lto="no"])

AS_IF([test "x$enable_lto" = "xyes"], [
    CFLAGS="$CFLAGS -flto"
    LDFLAGS="$LDFLAGS -flto"
    AC_MSG_NOTICE([LTO enabled: appending -flto to CFLAGS/LDFLAGS])
], [
    AC_MSG_NOTICE([LTO disabled])
])

# Checks for programs.
AC_PROG_CC

# Checks for libraries.

# Checks for header files.

# Checks for typedefs, structures, and compiler characteristics.
AC_TYPE_OFF_T
AC_C_RESTRICT
AC_TYPE_SIZE_T
AC_TYPE_SSIZE_T
AC_CHECK_MEMBERS([struct stat.st_blksize])
AC_STRUCT_ST_BLOCKS
AC_CHECK_MEMBERS([struct stat.st_rdev])
AC_TYPE_UINT16_T
AC_TYPE_UINT32_T

# Checks for library functions.
AC_CHECK_FUNCS([memset])

AC_CONFIG_FILES([Makefile
                 src/Makefile
                 tests/Makefile])
AC_OUTPUT
