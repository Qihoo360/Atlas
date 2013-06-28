/* $%BEGINLICENSE%$
 Copyright (c) 2011, Qihoo 360 - Chancey, wangchao3@360.cn

 $%ENDLICENSE%$ */
#include <lua.h>
#include <glib.h>
#include "lua-env.h"
#include "crc32.h"
#include "glib-ext.h"

int crc32_string(lua_State *L) {
	size_t str_len;
	const char *str = luaL_checklstring(L, 1, &str_len);
    g_debug("string:%s, length:%d", str, str_len);
    // crc32 hash
	unsigned int key = crc32(str, str_len);
    g_debug("crc key:%ld", key);

    // push crc number
    lua_pushnumber(L, key);

    return 1;
}

/*
** Assumes the table is on top of the stack.
*/
static void set_info (lua_State *L) {
	lua_pushliteral (L, "_COPYRIGHT");
	lua_pushliteral (L, "Copyright (C) 2011 Qihoo 360 - wangchao3");
	lua_settable (L, -3);
	lua_pushliteral (L, "_DESCRIPTION");
	lua_pushliteral (L, "CRC32 String for Proxy.*");
	lua_settable (L, -3);
	lua_pushliteral (L, "_VERSION");
	lua_pushliteral (L, "LuaCRC32string 0.1");
	lua_settable (L, -3);
}


static const struct luaL_reg crc32_m[] = {
	{"crc32", crc32_string},
	{NULL, NULL},
};

#if defined(_WIN32)
# define LUAEXT_API __declspec(dllexport)
#else
# define LUAEXT_API extern
#endif

LUAEXT_API int luaopen_crc32_string(lua_State *L) {
	luaL_register (L, "crc32", crc32_m);
	set_info (L);
	return 1;
}
