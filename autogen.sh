#!/bin/sh
# $%BEGINLICENSE%$
# $%ENDLICENSE%$

# Run this to generate all the initial makefiles, etc.

# LIBTOOLIZE=${LIBTOOLIZE:-libtoolize}
LIBTOOLIZE_FLAGS="--copy --force"
# ACLOCAL=${ACLOCAL:-aclocal}
# AUTOHEADER=${AUTOHEADER:-autoheader}
# AUTOMAKE=${AUTOMAKE:-automake}
AUTOMAKE_FLAGS="--add-missing --copy"
# AUTOCONF=${AUTOCONF:-autoconf}

ARGV0=$0
ARGS="$@"


run() {
	echo "$ARGV0: running \`$@' $ARGS"
	$@ $ARGS
}

## jump out if one of the programs returns 'false'
set -e

## on macosx glibtoolize, others have libtool
if test x$LIBTOOLIZE = x; then
  if test \! "x`which glibtoolize 2> /dev/null | grep -v '^no'`" = x; then
    LIBTOOLIZE=glibtoolize
  elif test \! "x`which libtoolize 2> /dev/null | grep -v '^no'`" = x; then
    LIBTOOLIZE=libtoolize
  else 
    echo "libtoolize wasn't found, try setting LIBTOOLIZE={path-to-libtool}."; exit 0
  fi
fi

if test x$ACLOCAL = x; then
  if test \! "x`which aclocal 2> /dev/null | grep -v '^no'`" = x; then
    ACLOCAL=aclocal
  else 
    echo "aclocal 1.10+ wasn't found, try setting ACLOCAL={path-to-aclocal}."; exit 0
  fi
fi

if test x$AUTOMAKE = x; then
  if test \! "x`which automake 2> /dev/null | grep -v '^no'`" = x; then
    AUTOMAKE=automake
  else 
    echo "automake 1.10+ wasn't found, try setting AUTOMAKE={path-to-automake}."; exit 0
  fi
fi


if test x$AUTOCONF = x; then
  if test \! "x`which autoconf 2> /dev/null | grep -v '^no'`" = x; then
    AUTOCONF=autoconf
  else 
    echo "autoconf 2.62+ wasn't found, try setting AUTOCONF={path-to-autoconf}."; exit 0
  fi
fi

if test x$AUTOHEADER = x; then
  if test \! "x`which autoheader 2> /dev/null | grep -v '^no'`" = x; then
    AUTOHEADER=autoheader
  else 
    echo "autoheader 2.62+ (autoheader) wasn't found, try setting AUTOHEADER={path-to-autoheader}."; exit 0
  fi
fi


run $LIBTOOLIZE $LIBTOOLIZE_FLAGS
run $ACLOCAL $ACLOCAL_FLAGS -I m4
run $AUTOHEADER
run $AUTOMAKE $AUTOMAKE_FLAGS
run $AUTOCONF
test "$ARGS" = "" && echo "Now type './configure --enable-maintainer-mode ...' and 'make' to compile."

