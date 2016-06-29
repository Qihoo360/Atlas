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

#define C(x) x, sizeof(x) - 1
#define S(x) x->str, x->len

#define ADD_PWD		1
#define ADD_ENPWD	2
#define REMOVE_PWD	3

#include "network-backend.h"
#include "network-mysqld.h"
#include "network-conn-pool-lua.h"
#include "network-backend-lua.h"
#include "network-address-lua.h"
#include "network-mysqld-lua.h"

/**
 * get the info about a backend
 *
 * proxy.backend[0].
 *   connected_clients => clients using this backend
 *   address           => ip:port or unix-path of to the backend
 *   state             => int(BACKEND_STATE_UP|BACKEND_STATE_DOWN) 
 *   type              => int(BACKEND_TYPE_RW|BACKEND_TYPE_RO) 
 *
 * @return nil or requested information
 * @see backend_state_t backend_type_t
 */
static int proxy_backend_get(lua_State *L) {
	network_backend_t *backend = *(network_backend_t **)luaL_checkself(L);
	gsize keysize = 0;
	const char *key = luaL_checklstring(L, 2, &keysize);

	if (strleq(key, keysize, C("connected_clients"))) {
		lua_pushinteger(L, backend->connected_clients);
	} else if (strleq(key, keysize, C("dst"))) {
		network_address_lua_push(L, backend->addr);
	} else if (strleq(key, keysize, C("state"))) {
		lua_pushinteger(L, backend->state);
	} else if (strleq(key, keysize, C("type"))) {
		lua_pushinteger(L, backend->type);
	} else if (strleq(key, keysize, C("uuid"))) {
		if (backend->uuid->len) {
			lua_pushlstring(L, S(backend->uuid));
		} else {
			lua_pushnil(L);
		}
	} else if (strleq(key, keysize, C("weight"))) {
		lua_pushinteger(L, backend->weight);
	} else {
		lua_pushnil(L);
	}

	return 1;
}

static int proxy_backend_set(lua_State *L) {
	network_backend_t *backend = *(network_backend_t **)luaL_checkself(L);
	gsize keysize = 0;
	const char *key = luaL_checklstring(L, 2, &keysize);

	if (strleq(key, keysize, C("state"))) {
		backend->state = lua_tointeger(L, -1);
	} else if (strleq(key, keysize, C("uuid"))) {
		if (lua_isstring(L, -1)) {
			size_t s_len = 0;
			const char *s = lua_tolstring(L, -1, &s_len);

			g_string_assign_len(backend->uuid, s, s_len);
		} else if (lua_isnil(L, -1)) {
			g_string_truncate(backend->uuid, 0);
		} else {
			return luaL_error(L, "proxy.global.backends[...].%s has to be a string", key);
		}
	} else {
		return luaL_error(L, "proxy.global.backends[...].%s is not writable", key);
	}
	return 1;
}

int network_backend_lua_getmetatable(lua_State *L) {
	static const struct luaL_reg methods[] = {
		{ "__index", proxy_backend_get },
		{ "__newindex", proxy_backend_set },
		{ NULL, NULL },
	};

	return proxy_getmetatable(L, methods);
}

/**
 * get proxy.global.backends[ndx]
 *
 * get the backend from the array of mysql backends.
 *
 * @return nil or the backend
 * @see proxy_backend_get
 */
static int proxy_backends_get(lua_State *L) {
	network_backend_t *backend; 
	network_backend_t **backend_p;

	network_backends_t *bs = *(network_backends_t **)luaL_checkself(L);
	int backend_ndx = luaL_checkinteger(L, 2) - 1; /** lua is indexes from 1, C from 0 */
	
	/* check that we are in range for a _int_ */
	if (NULL == (backend = network_backends_get(bs, backend_ndx))) {
		lua_pushnil(L);

		return 1;
	}

	backend_p = lua_newuserdata(L, sizeof(backend)); /* the table underneath proxy.global.backends[ndx] */
	*backend_p = backend;

	network_backend_lua_getmetatable(L);
	lua_setmetatable(L, -2);

	return 1;
}

static int proxy_clients_get(lua_State *L) {
	GPtrArray *raw_ips = *(GPtrArray **)luaL_checkself(L);
	int index = luaL_checkinteger(L, 2) - 1; /** lua is indexes from 1, C from 0 */
	gchar *ip = g_ptr_array_index(raw_ips, index);
	lua_pushlstring(L, ip, strlen(ip));
	return 1;
}

static int proxy_pwds_get(lua_State *L) {
	GPtrArray *raw_pwds = *(GPtrArray **)luaL_checkself(L);
	int index = luaL_checkinteger(L, 2) - 1; /** lua is indexes from 1, C from 0 */
	gchar *user_pwd = g_ptr_array_index(raw_pwds, index);
	lua_pushlstring(L, user_pwd, strlen(user_pwd));
	return 1;
}

/**
 * set proxy.global.backends.addslave
 *
 * add slave server into mysql backends
 *
 * @return nil or the backend
 */
static int proxy_backends_set(lua_State *L) {
	network_backends_t *bs = *(network_backends_t **)luaL_checkself(L);
	gsize keysize = 0;
	const char *key = luaL_checklstring(L, 2, &keysize);

	if (strleq(key, keysize, C("addslave"))) {
		gchar *address = g_strdup(lua_tostring(L, -1));
		network_backends_add(bs, address, BACKEND_TYPE_RO);
		g_free(address);
	} else if (strleq(key, keysize, C("addmaster"))) {
		gchar *address = g_strdup(lua_tostring(L, -1));
		network_backends_add(bs, address, BACKEND_TYPE_RW);
		g_free(address);
	} else if (strleq(key, keysize, C("removebackend"))) {
		network_backends_remove(bs, lua_tointeger(L, -1));
	} else if (strleq(key, keysize, C("addclient"))) {
		gchar *address = g_strdup(lua_tostring(L, -1));
		network_backends_addclient(bs, address);
		g_free(address);
	} else if (strleq(key, keysize, C("removeclient"))) {
		gchar *address = g_strdup(lua_tostring(L, -1));
		network_backends_removeclient(bs, address);
		g_free(address);
	} else if (strleq(key, keysize, C("saveconfig"))) {
		network_backends_save(bs);
	} else {
		return luaL_error(L, "proxy.global.backends.%s is not writable", key);
	}
	return 1;
}

static int proxy_backends_len(lua_State *L) {
	network_backends_t *bs = *(network_backends_t **)luaL_checkself(L);
	lua_pushinteger(L, network_backends_count(bs));
	return 1;
}

static int proxy_clients_len(lua_State *L) {
	GPtrArray *raw_ips = *(GPtrArray **)luaL_checkself(L);
	lua_pushinteger(L, raw_ips->len);
	return 1;
}

static int proxy_pwds_len(lua_State *L) {
	GPtrArray *raw_pwds = *(GPtrArray **)luaL_checkself(L);
	lua_pushinteger(L, raw_pwds->len);
	return 1;
}

static int proxy_clients_exist(lua_State *L) {
	GPtrArray *raw_ips = *(GPtrArray **)luaL_checkself(L);
	gchar *client = lua_tostring(L, -1);
	guint i;
	for (i = 0; i < raw_ips->len; ++i) {
		if (strcmp(client, g_ptr_array_index(raw_ips, i)) == 0) {
			lua_pushinteger(L, 1);
			return 1;
		}
	}
	lua_pushinteger(L, 0);
	return 1;
}

static gboolean proxy_pwds_exist(network_backends_t *bs, gchar *user) {
	GPtrArray *raw_pwds = bs->raw_pwds;

	guint i;
	for (i = 0; i < raw_pwds->len; ++i) {
		gchar *raw_pwd = g_ptr_array_index(raw_pwds, i);
		gchar *raw_pos = strchr(raw_pwd, ':');
		g_assert(raw_pos);
		*raw_pos = '\0';
		if (strcmp(user, raw_pwd) == 0) {
			*raw_pos = ':';
			return TRUE;
		}
		*raw_pos = ':';
	}

	return FALSE;
}

static int proxy_backends_pwds(lua_State *L) {
	network_backends_t *bs = *(network_backends_t **)luaL_checkself(L);
	guint type  = lua_tointeger(L, -1);
	gchar *pwd  = lua_tostring(L, -2);
	gchar *user = lua_tostring(L, -3);

	gboolean is_user_exist = proxy_pwds_exist(bs, user);
	int ret = -1;

	switch (type) {
	case ADD_PWD:
		if (is_user_exist) {
			ret = ERR_USER_EXIST;
		} else {
			ret = network_backends_addpwd(bs, user, pwd, FALSE);
		}
		break;

	case ADD_ENPWD:
		if (is_user_exist) {
			ret = ERR_USER_EXIST;
		} else {
			ret = network_backends_addpwd(bs, user, pwd, TRUE);
		}
		break;

	case REMOVE_PWD:
		if (!is_user_exist) {
			ret = ERR_USER_NOT_EXIST;
		} else {
			ret = network_backends_removepwd(bs, user);
		}
		break;

	default:
		g_assert_not_reached();
	}

	lua_pushinteger(L, ret);
	return 1;
}

int network_backends_lua_getmetatable(lua_State *L) {
	static const struct luaL_reg methods[] = {
		{ "__index", proxy_backends_get },
		{ "__newindex", proxy_backends_set },
		{ "__len", proxy_backends_len },
		{ "__call", proxy_backends_pwds },
		{ NULL, NULL },
	};

	return proxy_getmetatable(L, methods);
}

int network_clients_lua_getmetatable(lua_State *L) {
	static const struct luaL_reg methods[] = {
		{ "__index", proxy_clients_get },
		{ "__len", proxy_clients_len },
		{ "__call", proxy_clients_exist },
		{ NULL, NULL },
	};

	return proxy_getmetatable(L, methods);
}

int network_pwds_lua_getmetatable(lua_State *L) {
	static const struct luaL_reg methods[] = {
		{ "__index", proxy_pwds_get },
		{ "__len", proxy_pwds_len },
		{ NULL, NULL },
	};

	return proxy_getmetatable(L, methods);
}
