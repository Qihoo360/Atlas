#!/bin/sh 
base=$(cd "$(dirname "$0")"; pwd)
cd $base
export LD_LIBRARY_PATH=/usr/local/lib:$LD_LIBRARY_PATH
PKG_CONFIG_PATH=$PKG_CONFIG_PATH:/usr/bin/pkg-config:/usr/local/lib/pkgconfig ./configure --with-mysql=/usr --prefix=/usr/local/mysql-proxy CFLAGS="-DHAVE_LUA_H -O2 -g" LDFLAGS="-lm -ldl -ljemalloc -lcrypto -llemon_parser -L/usr/local/lib" LUA_CFLAGS="-I/usr/local/include/" LUA_LIBS="-L/usr/local/lib -llua"
