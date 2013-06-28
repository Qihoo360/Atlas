/* $%BEGINLICENSE%$
 Copyright (c) 2007, 2008, Oracle and/or its affiliates. All rights reserved.

 This program is free software; you can redistribute it and/or
 modify it under the terms of the GNU General Public License as
 published by the Free Software Foundation; version 2 of the
 License.

 This program is distributed in the hope that it will be useful,
 but WITHOUT ANY WARRANTY; without even the implied warranty of
 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 GNU General Public License for more details.

 You should have received a copy of the GNU General Public License
 along with this program; if not, write to the Free Software
 Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 02110-1301  USA

 $%ENDLICENSE%$ */
 

#include <glib.h>

#include <lua.h>
#include <lauxlib.h>
#include <lualib.h>

static int lua_g_usleep (lua_State *L) {
	int ms = luaL_checkinteger (L, 1);

	g_usleep(ms);

	return 0;
}

static int lua_g_get_current_time (lua_State *L) {
	GTimeVal t;

	g_get_current_time(&t);

	lua_newtable(L);
	lua_pushinteger(L, t.tv_sec);
	lua_setfield(L, -2, "tv_sec");
	lua_pushinteger(L, t.tv_usec);
	lua_setfield(L, -2, "tv_usec");

	return 1;
}

static int lua_g_checksum_md5 (lua_State *L) {
	size_t str_len;
	const char *str = luaL_checklstring (L, 1, &str_len);
	GChecksum *cs;

	cs = g_checksum_new(G_CHECKSUM_MD5);

	g_checksum_update(cs, (guchar *)str, str_len);

	lua_pushstring(L, g_checksum_get_string(cs));

	g_checksum_free(cs);

	return 1;
}

/*
** Assumes the table is on top of the stack.
*/
static void set_info (lua_State *L) {
	lua_pushliteral (L, "_COPYRIGHT");
	lua_pushliteral (L, "Copyright (c) 2010 Oracle");
	lua_settable (L, -3);
	lua_pushliteral (L, "_DESCRIPTION");
	lua_pushliteral (L, "export glib2-functions as glib.*");
	lua_settable (L, -3);
	lua_pushliteral (L, "_VERSION");
	lua_pushliteral (L, "LuaGlib2 0.1");
	lua_settable (L, -3);
}


static const struct luaL_reg gliblib[] = {
	{"usleep", lua_g_usleep},
	{"md5", lua_g_checksum_md5},
	{"get_current_time", lua_g_get_current_time},
	{NULL, NULL},
};

#if defined(_WIN32)
# define LUAEXT_API __declspec(dllexport)
#else
# define LUAEXT_API extern
#endif

LUAEXT_API int luaopen_glib2 (lua_State *L) {
	luaL_register (L, "glib2", gliblib);
	set_info (L);
	return 1;
}
