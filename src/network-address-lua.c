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

#include "config.h"
#include <lua.h>

#ifdef WIN32
#include <winsock2.h>
#include <ws2tcpip.h>
#else
#include <arpa/inet.h>
#endif

#include "lua-env.h"
#include "glib-ext.h"

#include "network-address.h"
#include "network-address-lua.h"

#define C(x) x, sizeof(x) - 1
#define S(x) x->str, x->len

static int proxy_address_get(lua_State *L) {
	network_address *addr = *(network_address **)luaL_checkself(L);
	gsize keysize = 0;
	const char *key = luaL_checklstring(L, 2, &keysize);

	if (strleq(key, keysize, C("type"))) {
		lua_pushinteger(L, addr->addr.common.sa_family);
	} else if (strleq(key, keysize, C("name"))) {
		lua_pushlstring(L, S(addr->name));
	} else if (strleq(key, keysize, C("address"))) {
#ifdef HAVE_INET_NTOP
		char dst_addr[INET6_ADDRSTRLEN];
#endif
		const char *str = NULL;

		switch (addr->addr.common.sa_family) {
		case AF_INET:
			str = inet_ntoa(addr->addr.ipv4.sin_addr);
			if (!str) {
				/* it shouldn't really fail, how about logging it ? */ 
			}
			break;
#ifdef HAVE_INET_NTOP
		case AF_INET6:
			str = inet_ntop(addr->addr.common.sa_family, &addr->addr.ipv6.sin6_addr, dst_addr, sizeof(dst_addr));
			if (!str) {
				/* it shouldn't really fail, how about logging it ? */ 
			}
			break;
#endif
#ifndef WIN32
		case AF_UNIX:
			str = addr->addr.un.sun_path;
			break;
#endif
		default:
			break;
		}

		if (NULL == str) {
			lua_pushnil(L);
		} else {
			lua_pushstring(L, str);
		}
	} else if (strleq(key, keysize, C("port"))) {
		switch (addr->addr.common.sa_family) {
		case AF_INET:
			lua_pushinteger(L, ntohs(addr->addr.ipv4.sin_port));
			break;
		case AF_INET6:
			lua_pushinteger(L, ntohs(addr->addr.ipv6.sin6_port));
			break;
		default:
			lua_pushnil(L);
			break;
		}
	} else {
		lua_pushnil(L);
	}

	return 1;
}

int network_address_lua_getmetatable(lua_State *L) {
	static const struct luaL_reg methods[] = {
		{ "__index", proxy_address_get },
		{ NULL, NULL },
	};
	return proxy_getmetatable(L, methods);
}

int network_address_lua_push(lua_State *L, network_address *addr) {
	network_address **address_p;

	if (!addr) {
		lua_pushnil(L);
		return 1;
	}

	address_p = lua_newuserdata(L, sizeof(network_address));
	*address_p = addr;

	network_address_lua_getmetatable(L);
	lua_setmetatable(L, -2); /* tie the metatable to the table   (sp -= 1) */

	return 1;
}

