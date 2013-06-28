/* $%BEGINLICENSE%$
 Copyright (c) 2008, 2009, Oracle and/or its affiliates. All rights reserved.

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

#include <lua.h>

#include "lua-env.h"
#include "glib-ext.h"

#include "network-socket.h"
#include "network-mysqld-packet.h"
#include "network-address-lua.h"
#include "network-socket-lua.h"

#define C(x) x, sizeof(x) - 1
#define S(x) x->str, x->len

static int proxy_socket_get(lua_State *L) {
	network_socket *sock = *(network_socket **)luaL_checkself(L);
	gsize keysize = 0;
	const char *key = luaL_checklstring(L, 2, &keysize);

	/**
	 * we to split it in .client and .server here
	 */

	if (strleq(key, keysize, C("default_db"))) {
		lua_pushlstring(L, sock->default_db->str, sock->default_db->len);
		return 1;
	}else if (strleq(key, keysize, C("charset_client"))) {
		lua_pushlstring(L, sock->charset_client->str, sock->charset_client->len);
		return 1;
	}else if (strleq(key, keysize, C("charset_connection"))) {
		lua_pushlstring(L, sock->charset_connection->str, sock->charset_connection->len);
		return 1;
	}else if (strleq(key, keysize, C("charset_results"))) {
		lua_pushlstring(L, sock->charset_results->str, sock->charset_results->len);
		return 1;
	}else if (strleq(key, keysize, C("address"))) {
		return luaL_error(L, ".address is deprecated. Use .src.name or .dst.name instead");
	} else if (strleq(key, keysize, C("src"))) {
		return network_address_lua_push(L, sock->src);
	} else if (strleq(key, keysize, C("dst"))) {
		return network_address_lua_push(L, sock->dst);
	}
      
	if (sock->response) {
		if (strleq(key, keysize, C("username"))) {
			lua_pushlstring(L, S(sock->response->username));
			return 1;
		} else if (strleq(key, keysize, C("scrambled_password"))) {
			lua_pushlstring(L, S(sock->response->response));
			return 1;
		}
	}

	if (sock->challenge) { /* only the server-side has mysqld_version set */
		if (strleq(key, keysize, C("mysqld_version"))) {
			lua_pushinteger(L, sock->challenge->server_version);
			return 1;
		} else if (strleq(key, keysize, C("thread_id"))) {
			lua_pushinteger(L, sock->challenge->thread_id);
			return 1;
		} else if (strleq(key, keysize, C("scramble_buffer"))) {
			lua_pushlstring(L, S(sock->challenge->challenge));
			return 1;
		}
	}
	g_critical("%s: sock->challenge: %p, sock->response: %p (looking for %s)", 
			G_STRLOC,
			(void *)sock->challenge,
			(void *)sock->response,
			key
			);

	lua_pushnil(L);

	return 1;
}

int network_socket_lua_getmetatable(lua_State *L) {
	static const struct luaL_reg methods[] = {
		{ "__index", proxy_socket_get },
		{ NULL, NULL },
	};
	return proxy_getmetatable(L, methods);
}


