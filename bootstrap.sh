#!/bin/sh 
lua_include_dir=$1
lua_lib_dir=$2
base=$(cd "$(dirname "$0")"; pwd)
cd $base

if [ "$lua_include_dir" = "" ];
then
    lua_include_dir="/usr/local/include"
fi

if [ "$lua_lib_dir" = "" ];
then
    lua_lib_dir="/usr/local/lib"
fi

PKG_CONFIG_PATH=/usr/local/lib/pkgconfig ./configure --with-mysql=/usr --prefix=/usr/local/mysql-proxy CFLAGS="-DHAVE_LUA_H -g -O2" LDFLAGS="-lm -ldl -lcrypto -ljemalloc" LUA_CFLAGS="-I$lua_include_dir" LUA_LIBS="-L$lua_lib_dir -llua5.1"
