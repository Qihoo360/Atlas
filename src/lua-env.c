/* $%BEGINLICENSE%$
 Copyright (c) 2008, Oracle and/or its affiliates. All rights reserved.

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
#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include <glib.h>

#include "lua-env.h"

/**
 * convinience functions for some lua lib/api functions 
 */

/**
 * taken from lapi.c 
 */
/* convert a stack index to positive */
#define abs_index(L, i)         ((i) > 0 || (i) <= LUA_REGISTRYINDEX ? (i) : \
		                                        lua_gettop(L) + (i) + 1)
void lua_getfield_literal (lua_State *L, int idx, const char *k, size_t k_len) {
	idx = abs_index(L, idx);

	lua_pushlstring(L, k, k_len);

	lua_gettable(L, idx);
}

/**
 * check pass through the userdata as is 
 */
void *luaL_checkself (lua_State *L) {
	return lua_touserdata(L, 1);
}

/**
 * emulate luaL_newmetatable() with lightuserdata instead of strings 
 *
 * this is a lot faster than doing hashing on strings as we can just
 * hash on a fixed memory-address
 *
 * to make this work, the methods array has to be declared static to
 * keep its location
 */
int proxy_getmetatable(lua_State *L, const luaL_reg *methods) {
	/* check if the */

	lua_pushlightuserdata(L, (luaL_reg *)methods);
	lua_gettable(L, LUA_REGISTRYINDEX);

	if (lua_isnil(L, -1)) {
		/* not found */
		lua_pop(L, 1);

		lua_newtable(L);
		luaL_register(L, NULL, methods);

		lua_pushlightuserdata(L, (luaL_reg *)methods);
		lua_pushvalue(L, -2);
		lua_settable(L, LUA_REGISTRYINDEX);
	}
	g_assert(lua_istable(L, -1));

	return 1;
}


