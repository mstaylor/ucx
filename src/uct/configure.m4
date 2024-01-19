#
# Copyright (c) NVIDIA CORPORATION & AFFILIATES, 2001-2018. ALL RIGHTS RESERVED.
# See file LICENSE for terms.
#

uct_modules=""
m4_include([src/uct/cuda/configure.m4])
m4_include([src/uct/ib/configure.m4])
m4_include([src/uct/rocm/configure.m4])
m4_include([src/uct/sm/configure.m4])
m4_include([src/uct/ugni/configure.m4])

AC_DEFINE_UNQUOTED([uct_MODULES], ["${uct_modules}"], [UCT loadable modules])

AC_CONFIG_FILES([src/uct/Makefile
                 src/uct/ucx-uct.pc])

#
# Include Hiredis for aws lambda support
#
#
AC_ARG_WITH([hiredis-prefix],
[AS_HELP_STRING([--with-hiredis-prefix=PREFIX],
[specify the prefix where hiredis is installed])],
[hiredis_prefix=$withval],
[hiredis_prefix=""])   # Default is empty

# Setup CPPFLAGS and LDFLAGS if a custom prefix is provided
if test "x$hiredis_prefix" != "x"; then
        CPPFLAGS="$CPPFLAGS -I$hiredis_prefix/include"
LDFLAGS="$LDFLAGS -L$hiredis_prefix/lib"
fi

# Check for hiredis library and header
        AC_CHECK_LIB([hiredis], [redisConnect],
[AC_DEFINE([HAVE_HIREDIS], [1], [Have the hiredis library])],
[AC_MSG_ERROR([Cannot find hiredis library])])
AC_CHECK_HEADERS([hiredis/hiredis.h], , [AC_MSG_ERROR([Cannot find hiredis header])])


#
# TCP flags
#
AC_CHECK_DECLS([IPPROTO_TCP, SOL_SOCKET, SO_KEEPALIVE,
                TCP_KEEPCNT, TCP_KEEPIDLE, TCP_KEEPINTVL],
               [],
               [tcp_keepalive_happy=no],
               [[#include <netinet/tcp.h>]
                [#include <netinet/in.h>]])
AS_IF([test "x$tcp_keepalive_happy" != "xno"],
      [AC_DEFINE([UCT_TCP_EP_KEEPALIVE], 1, [Enable TCP keepalive configuration])]);
