/* $%BEGINLICENSE%$
 Copyright (c) 2009, Oracle and/or its affiliates. All rights reserved.

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
 

/**
 * expose the chassis functions into the lua space
 */


#include <lua.h>
#include <lauxlib.h>
#include <lualib.h>

#include "network-mysqld-proto.h"
#include "network-mysqld-packet.h"
#include "glib-ext.h"
#include "lua-env.h"

#define C(x) x, sizeof(x) - 1
#define S(x) x->str, x->len

static int lua_password_hash(lua_State *L) {
	size_t password_len;
	const char *password = luaL_checklstring(L, 1, &password_len);
	GString *response;

	response = g_string_new(NULL);	
	network_mysqld_proto_password_hash(response, password, password_len);

	lua_pushlstring(L, S(response));
	
	g_string_free(response, TRUE);

	return 1;
}

static int lua_password_scramble(lua_State *L) {
	size_t challenge_len;
	const char *challenge = luaL_checklstring(L, 1, &challenge_len);
	size_t hashed_password_len;
	const char *hashed_password = luaL_checklstring(L, 2, &hashed_password_len);
	GString *response;

	response = g_string_new(NULL);	
	network_mysqld_proto_password_scramble(response,
			challenge, challenge_len,
			hashed_password, hashed_password_len);

	lua_pushlstring(L, S(response));
	
	g_string_free(response, TRUE);

	return 1;
}

static int lua_password_unscramble(lua_State *L) {
	size_t challenge_len;
	const char *challenge = luaL_checklstring(L, 1, &challenge_len);
	size_t response_len;
	const char *response = luaL_checklstring(L, 2, &response_len);
	size_t dbl_hashed_password_len;
	const char *dbl_hashed_password = luaL_checklstring(L, 3, &dbl_hashed_password_len);

	GString *hashed_password = g_string_new(NULL);

	network_mysqld_proto_password_unscramble(
			hashed_password,
			challenge, challenge_len,
			response, response_len,
			dbl_hashed_password, dbl_hashed_password_len);
	
	lua_pushlstring(L, S(hashed_password));

	g_string_free(hashed_password, TRUE);
	
	return 1;
}


static int lua_password_check(lua_State *L) {
	size_t challenge_len;
	const char *challenge = luaL_checklstring(L, 1, &challenge_len);
	size_t response_len;
	const char *response = luaL_checklstring(L, 2, &response_len);
	size_t dbl_hashed_password_len;
	const char *dbl_hashed_password = luaL_checklstring(L, 3, &dbl_hashed_password_len);

	lua_pushboolean(L, network_mysqld_proto_password_check(
			challenge, challenge_len,
			response, response_len,
			dbl_hashed_password, dbl_hashed_password_len));
	
	return 1;
}

/*
** Assumes the table is on top of the stack.
*/
static void set_info (lua_State *L) {
	lua_pushliteral (L, "_COPYRIGHT");
	lua_pushliteral (L, "Copyright (C) 2008 MySQL AB, 2008 Sun Microsystems, Inc");
	lua_settable (L, -3);
	lua_pushliteral (L, "_DESCRIPTION");
	lua_pushliteral (L, "export mysql password encoders to mysql.*");
	lua_settable (L, -3);
	lua_pushliteral (L, "_VERSION");
	lua_pushliteral (L, "LuaMySQLPassword 0.1");
	lua_settable (L, -3);
}


static const struct luaL_reg mysql_passwordlib[] = {
	{"hash", lua_password_hash},
	{"scramble", lua_password_scramble},
	{"unscramble", lua_password_unscramble},
	{"check", lua_password_check},
	{NULL, NULL},
};

#if defined(_WIN32)
# define LUAEXT_API __declspec(dllexport)
#else
# define LUAEXT_API extern
#endif

LUAEXT_API int luaopen_mysql_password (lua_State *L) {
	luaL_register (L, "password", mysql_passwordlib);
	set_info (L);
	return 1;
}
