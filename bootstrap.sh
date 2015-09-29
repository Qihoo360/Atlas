#!/bin/sh 
base=$(cd "$(dirname "$0")"; pwd)
cd $base
export LD_LIBRARY_PATH=/usr/local/lib:$LD_LIBRARY_PATH
PKG_CONFIG_PATH=/usr/local/lib/pkgconfig ./configure --with-mysql=/usr --prefix=/usr/local/mysql-proxy CFLAGS="-DHAVE_LUA_H -O2" LDFLAGS="-lm -ldl -lcrypto -ljemalloc" LUA_CFLAGS="-I/usr/local/include/" LUA_LIBS="-L/usr/local/lib -llua"
