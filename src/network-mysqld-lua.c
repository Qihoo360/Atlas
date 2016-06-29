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
#ifdef HAVE_CONFIG_H
#include "config.h"
#endif


#ifdef _WIN32
#include <winsock2.h> /* mysql.h needs SOCKET */
#endif

#include <mysql.h>
#include <mysqld_error.h>
#include <errno.h>
#include <string.h>


#include "network-backend.h"
#include "glib-ext.h"
#include "lua-env.h"

#include "network-mysqld.h"
#include "network-mysqld-proto.h"
#include "network-mysqld-lua.h"
#include "network-socket-lua.h"
#include "network-backend-lua.h"
#include "network-conn-pool.h"
#include "network-conn-pool-lua.h"
#include "network-injection-lua.h"

#define C(x) x, sizeof(x) - 1

network_mysqld_con_lua_t *network_mysqld_con_lua_new() {
	network_mysqld_con_lua_t *st;

	st = g_new0(network_mysqld_con_lua_t, 1);

	st->injected.queries = network_injection_queue_new();
	
	return st;
}

void network_mysqld_con_lua_free(network_mysqld_con_lua_t *st) {
	if (!st) return;

	network_injection_queue_free(st->injected.queries);

	g_free(st);
}


/**
 * get the connection information
 *
 * note: might be called in connect_server() before con->server is set 
 */
static int proxy_connection_get(lua_State *L) {
	network_mysqld_con *con = *(network_mysqld_con **)luaL_checkself(L); 
	network_mysqld_con_lua_t *st;
	gsize keysize = 0;
	const char *key = luaL_checklstring(L, 2, &keysize);

	st = con->plugin_con_state;

	/**
	 * we to split it in .client and .server here
	 */

	if (strleq(key, keysize, C("default_db"))) {
		return luaL_error(L, "proxy.connection.default_db is deprecated, use proxy.connection.client.default_db or proxy.connection.server.default_db instead");
	} else if (strleq(key, keysize, C("thread_id"))) {
		return luaL_error(L, "proxy.connection.thread_id is deprecated, use proxy.connection.server.thread_id instead");
	} else if (strleq(key, keysize, C("mysqld_version"))) {
		return luaL_error(L, "proxy.connection.mysqld_version is deprecated, use proxy.connection.server.mysqld_version instead");
	} else if (strleq(key, keysize, C("backend_ndx"))) {
		lua_pushinteger(L, st->backend_ndx + 1);
	} else if ((con->server && (strleq(key, keysize, C("server")))) ||
	           (con->client && (strleq(key, keysize, C("client"))))) {
		network_socket **socket_p;

		socket_p = lua_newuserdata(L, sizeof(network_socket)); /* the table underneat proxy.socket */

		if (key[0] == 's') {
			*socket_p = con->server;
		} else {
			*socket_p = con->client;
		}

		network_socket_lua_getmetatable(L);
		lua_setmetatable(L, -2); /* tie the metatable to the table   (sp -= 1) */
	} else {
		lua_pushnil(L);
	}

	return 1;
}

/**
 * set the connection information
 *
 * note: might be called in connect_server() before con->server is set 
 */
static int proxy_connection_set(lua_State *L) {
	network_mysqld_con *con = *(network_mysqld_con **)luaL_checkself(L);
	network_mysqld_con_lua_t *st;
	gsize keysize = 0;
	const char *key = luaL_checklstring(L, 2, &keysize);

	st = con->plugin_con_state;

	if (strleq(key, keysize, C("backend_ndx"))) {
		/**
		 * in lua-land the ndx is based on 1, in C-land on 0 */
		int backend_ndx = luaL_checkinteger(L, 3) - 1;
		network_socket *send_sock;
			
		if (backend_ndx == -1) {
			/** drop the backend for now
			 */
			network_connection_pool_lua_add_connection(con);
		} else if (NULL != (send_sock = network_connection_pool_lua_swap(con, backend_ndx, NULL))) {
			con->server = send_sock;
		} else if (backend_ndx == -2) {
			if (st->backend != NULL) {
				st->backend->connected_clients--;
				st->backend = NULL;
			}
			st->backend_ndx = -1;
			network_socket_free(con->server);
			con->server = NULL;
		} else {
			st->backend_ndx = backend_ndx;
		}
	} else if (0 == strcmp(key, "connection_close")) {
		luaL_checktype(L, 3, LUA_TBOOLEAN);

		st->connection_close = lua_toboolean(L, 3);
	} else {
		return luaL_error(L, "proxy.connection.%s is not writable", key);
	}

	return 0;
}

int network_mysqld_con_getmetatable(lua_State *L) {
	static const struct luaL_reg methods[] = {
		{ "__index", proxy_connection_get },
		{ "__newindex", proxy_connection_set },
		{ NULL, NULL },
	};
	return proxy_getmetatable(L, methods);
}

/**
 * Set up the global structures for a script.
 * 
 * @see lua_register_callback - for connection local setup
 */
void network_mysqld_lua_setup_global(lua_State *L , chassis *chas) {
	network_backends_t **backends_p;

	int stack_top = lua_gettop(L);

	/* TODO: if we share "proxy." with other plugins, this may fail to initialize it correctly, 
	 * because maybe they already have registered stuff in there.
	 * It would be better to have different namespaces, or any other way to make sure we initialize correctly.
	 */
	lua_getglobal(L, "proxy");
	if (lua_isnil(L, -1)) {
		lua_pop(L, 1);

		network_mysqld_lua_init_global_fenv(L);
	
		lua_getglobal(L, "proxy");
	}
	g_assert(lua_istable(L, -1));

	/* at this point we have set up:
	 *  - the script
	 *  - _G.proxy and a bunch of constants in that table
	 *  - _G.proxy.global
	 */
	
	/**
	 * register proxy.global.backends[]
	 *
	 * @see proxy_backends_get()
	 */
	lua_getfield(L, -1, "global");

    // set instance name
	// proxy.global.config.instance , value assigned when cmd start use --instance
	lua_getfield(L, -1, "config");

    lua_pushstring(L, chas->instance_name);
	lua_setfield(L, -2, "instance");

    lua_pushstring(L, chas->log_path);
	lua_setfield(L, -2, "logpath");

    lua_pop(L, 1);

    // 
	backends_p = lua_newuserdata(L, sizeof(network_backends_t *));
	*backends_p = chas->backends;

	network_backends_lua_getmetatable(L);
	lua_setmetatable(L, -2);          /* tie the metatable to the table   (sp -= 1) */

	lua_setfield(L, -2, "backends");

	GPtrArray **raw_ips_p = lua_newuserdata(L, sizeof(GPtrArray *));
	*raw_ips_p = chas->backends->raw_ips;
	network_clients_lua_getmetatable(L);
	lua_setmetatable(L, -2);
	lua_setfield(L, -2, "clients");

	GPtrArray **raw_pwds_p = lua_newuserdata(L, sizeof(GPtrArray *));
	*raw_pwds_p = chas->backends->raw_pwds;
	network_pwds_lua_getmetatable(L);
	lua_setmetatable(L, -2);
	lua_setfield(L, -2, "pwds");

	lua_pop(L, 2);  /* _G.proxy.global and _G.proxy */

	g_assert(lua_gettop(L) == stack_top);
}


/**
 * Load a lua script and leave the wrapper function on the stack.
 *
 * @return 0 on success, -1 on error
 */
int network_mysqld_lua_load_script(lua_scope *sc, const char *lua_script) {
	int stack_top = lua_gettop(sc->L);

	if (!lua_script) return -1;
	
	/* a script cache
	 *
	 * we cache the scripts globally in the registry and move a copy of it 
	 * to the new script scope on success.
	 */
	lua_scope_load_script(sc, lua_script);

	if (lua_isstring(sc->L, -1)) {
		g_critical("%s: lua_load_file(%s) failed: %s", 
				G_STRLOC, 
				lua_script, lua_tostring(sc->L, -1));

		lua_pop(sc->L, 1); /* remove the error-msg from the stack */
		
		return -1;
	} else if (!lua_isfunction(sc->L, -1)) {
		g_error("%s: luaL_loadfile(%s): returned a %s", 
				G_STRLOC, 
				lua_script, lua_typename(sc->L, lua_type(sc->L, -1)));
	}

	g_assert(lua_gettop(sc->L) - stack_top == 1);

	return 0;
}

/**
 * setup the local script environment before we call the hook function
 *
 * has to be called before any lua_pcall() is called to start a hook function
 *
 * - we use a global lua_State which is split into child-states with lua_newthread()
 * - luaL_ref() moves the state into the registry and cleans up the global stack
 * - on connection close we call luaL_unref() to hand the thread to the GC
 *
 * @see proxy_lua_free_script
 *
 *
 * if the script is cached we have to point the global proxy object
 *
 * @retval 0 success (even if we do not have a script)
 * @retval -1 The script failed to load, most likely because of a syntax error.
 * @retval -2 The script failed to execute.
 */
network_mysqld_register_callback_ret network_mysqld_con_lua_register_callback(network_mysqld_con *con, const char *lua_script) {
	lua_State *L = NULL;
	network_mysqld_con_lua_t *st   = con->plugin_con_state;

	lua_scope  *sc = con->srv->sc;

	GQueue **q_p;
	network_mysqld_con **con_p;
	int stack_top;

	if (!lua_script) return REGISTER_CALLBACK_SUCCESS;

	if (st->L) {
		/* we have to rewrite _G.proxy to point to the local proxy */
		L = st->L;

		g_assert(lua_isfunction(L, -1));

		lua_getfenv(L, -1);
		g_assert(lua_istable(L, -1));

		lua_getglobal(L, "proxy");
		lua_getmetatable(L, -1); /* meta(_G.proxy) */

		lua_getfield(L, -3, "__proxy"); /* fenv.__proxy */
		lua_setfield(L, -2, "__index"); /* meta[_G.proxy].__index = fenv.__proxy */

		lua_getfield(L, -3, "__proxy"); /* fenv.__proxy */
		lua_setfield(L, -2, "__newindex"); /* meta[_G.proxy].__newindex = fenv.__proxy */

		lua_pop(L, 3);

		g_assert(lua_isfunction(L, -1));

		return REGISTER_CALLBACK_SUCCESS; /* the script-env already setup, get out of here */
	}

	/* handles loading the file from disk/cache*/
	if (0 != network_mysqld_lua_load_script(sc, lua_script)) {
		/* loading script failed */
		return REGISTER_CALLBACK_LOAD_FAILED;
	}

	/* sets up global tables */
	network_mysqld_lua_setup_global(sc->L, con->srv);

	/**
	 * create a side thread for this connection
	 *
	 * (this is not pre-emptive, it is just a new stack in the global env)
	 */
	L = lua_newthread(sc->L);

	st->L_ref = luaL_ref(sc->L, LUA_REGISTRYINDEX);

	stack_top = lua_gettop(L);

	/* get the script from the global stack */
	lua_xmove(sc->L, L, 1);
	g_assert(lua_isfunction(L, -1));

	lua_newtable(L); /* my empty environment aka {}              (sp += 1) 1 */

	lua_newtable(L); /* the meta-table for the new env           (sp += 1) 2 */

	lua_pushvalue(L, LUA_GLOBALSINDEX);                       /* (sp += 1) 3 */
	lua_setfield(L, -2, "__index"); /* { __index = _G }          (sp -= 1) 2 */
	lua_setmetatable(L, -2); /* setmetatable({}, {__index = _G}) (sp -= 1) 1 */

	lua_newtable(L); /* __proxy = { }                            (sp += 1) 2 */

	g_assert(lua_istable(L, -1));

	q_p = lua_newuserdata(L, sizeof(GQueue *));               /* (sp += 1) 3 */
	*q_p = st->injected.queries;

	/*
	 * proxy.queries
	 *
	 * implement a queue
	 *
	 * - append(type, query)
	 * - prepend(type, query)
	 * - reset()
	 * - len() and #proxy.queue
	 *
	 */
	proxy_getqueuemetatable(L);

	lua_pushvalue(L, -1); /* meta.__index = meta */
	lua_setfield(L, -2, "__index");

	lua_setmetatable(L, -2);


	lua_setfield(L, -2, "queries"); /* proxy.queries = <userdata> */

	/*
	 * proxy.connection is (mostly) read-only
	 *
	 * .thread_id  = ... thread-id against this server
	 * .backend_id = ... index into proxy.global.backends[ndx]
	 *
	 */

	con_p = lua_newuserdata(L, sizeof(con));                          /* (sp += 1) */
	*con_p = con;

	network_mysqld_con_getmetatable(L);
	lua_setmetatable(L, -2);          /* tie the metatable to the udata   (sp -= 1) */

	lua_setfield(L, -2, "connection"); /* proxy.connection = <udata>     (sp -= 1) */

	/*
	 * proxy.response knows 3 fields with strict types:
	 *
	 * .type = <int>
	 * .errmsg = <string>
	 * .resultset = { 
	 *   fields = { 
	 *     { type = <int>, name = <string > }, 
	 *     { ... } }, 
	 *   rows = { 
	 *     { ..., ... }, 
	 *     { ..., ... } }
	 * }
	 */
	lua_newtable(L);
#if 0
	lua_newtable(L); /* the meta-table for the response-table    (sp += 1) */
	lua_pushcfunction(L, response_get);                       /* (sp += 1) */
	lua_setfield(L, -2, "__index");                           /* (sp -= 1) */
	lua_pushcfunction(L, response_set);                       /* (sp += 1) */
	lua_setfield(L, -2, "__newindex");                        /* (sp -= 1) */
	lua_setmetatable(L, -2); /* tie the metatable to response    (sp -= 1) */
#endif
	lua_setfield(L, -2, "response");

	lua_setfield(L, -2, "__proxy");

	/* patch the _G.proxy to point here */
	lua_getglobal(L, "proxy");
	g_assert(lua_istable(L, -1));

	if (0 == lua_getmetatable(L, -1)) { /* meta(_G.proxy) */
		/* no metatable yet */

		lua_newtable(L);
	}
	g_assert(lua_istable(L, -1));

	lua_getfield(L, -3, "__proxy"); /* fenv.__proxy */
	g_assert(lua_istable(L, -1));
	lua_setfield(L, -2, "__index"); /* meta[_G.proxy].__index = fenv.__proxy */

	lua_getfield(L, -3, "__proxy"); /* fenv.__proxy */
	lua_setfield(L, -2, "__newindex"); /* meta[_G.proxy].__newindex = fenv.__proxy */

	lua_setmetatable(L, -2);

	lua_pop(L, 1);  /* _G.proxy */

	g_assert(lua_isfunction(L, -2));
	g_assert(lua_istable(L, -1));

	lua_setfenv(L, -2); /* on the stack should be a modified env (sp -= 1) */

	/* cache the script in this connection */
	g_assert(lua_isfunction(L, -1));
	lua_pushvalue(L, -1);

	/* run the script once to get the functions set in the global scope */
	if (lua_pcall(L, 0, 0, 0) != 0) {
		g_critical("(lua-error) [%s]\n%s", lua_script, lua_tostring(L, -1));

		lua_pop(L, 1); /* errmsg */

		luaL_unref(sc->L, LUA_REGISTRYINDEX, st->L_ref);

		return REGISTER_CALLBACK_EXECUTE_FAILED;
	}

	st->L = L;

	g_assert(lua_isfunction(L, -1));
	g_assert(lua_gettop(L) - stack_top == 1);

	return REGISTER_CALLBACK_SUCCESS;
}

/**
 * init the global proxy object 
 */
void network_mysqld_lua_init_global_fenv(lua_State *L) {
	
	lua_newtable(L); /* my empty environment aka {}              (sp += 1) */
#define DEF(x) \
	lua_pushinteger(L, x); \
	lua_setfield(L, -2, #x);
	
	DEF(PROXY_SEND_QUERY);
	DEF(PROXY_SEND_RESULT);
	DEF(PROXY_IGNORE_RESULT);

	DEF(MYSQLD_PACKET_OK);
	DEF(MYSQLD_PACKET_ERR);
	DEF(MYSQLD_PACKET_RAW);

	DEF(BACKEND_STATE_UNKNOWN);
	DEF(BACKEND_STATE_UP);
	DEF(BACKEND_STATE_DOWN);
	DEF(BACKEND_STATE_OFFLINE);

	DEF(BACKEND_TYPE_UNKNOWN);
	DEF(BACKEND_TYPE_RW);
	DEF(BACKEND_TYPE_RO);

	DEF(COM_SLEEP);
	DEF(COM_QUIT);
	DEF(COM_INIT_DB);
	DEF(COM_QUERY);
	DEF(COM_FIELD_LIST);
	DEF(COM_CREATE_DB);
	DEF(COM_DROP_DB);
	DEF(COM_REFRESH);
	DEF(COM_SHUTDOWN);
	DEF(COM_STATISTICS);
	DEF(COM_PROCESS_INFO);
	DEF(COM_CONNECT);
	DEF(COM_PROCESS_KILL);
	DEF(COM_DEBUG);
	DEF(COM_PING);
	DEF(COM_TIME);
	DEF(COM_DELAYED_INSERT);
	DEF(COM_CHANGE_USER);
	DEF(COM_BINLOG_DUMP);
	DEF(COM_TABLE_DUMP);
	DEF(COM_CONNECT_OUT);
	DEF(COM_REGISTER_SLAVE);
	DEF(COM_STMT_PREPARE);
	DEF(COM_STMT_EXECUTE);
	DEF(COM_STMT_SEND_LONG_DATA);
	DEF(COM_STMT_CLOSE);
	DEF(COM_STMT_RESET);
	DEF(COM_SET_OPTION);
#if MYSQL_VERSION_ID >= 50000
	DEF(COM_STMT_FETCH);
#if MYSQL_VERSION_ID >= 50100
	DEF(COM_DAEMON);
#endif
#endif
	DEF(MYSQL_TYPE_DECIMAL);
#if MYSQL_VERSION_ID >= 50000
	DEF(MYSQL_TYPE_NEWDECIMAL);
#endif
	DEF(MYSQL_TYPE_TINY);
	DEF(MYSQL_TYPE_SHORT);
	DEF(MYSQL_TYPE_LONG);
	DEF(MYSQL_TYPE_FLOAT);
	DEF(MYSQL_TYPE_DOUBLE);
	DEF(MYSQL_TYPE_NULL);
	DEF(MYSQL_TYPE_TIMESTAMP);
	DEF(MYSQL_TYPE_LONGLONG);
	DEF(MYSQL_TYPE_INT24);
	DEF(MYSQL_TYPE_DATE);
	DEF(MYSQL_TYPE_TIME);
	DEF(MYSQL_TYPE_DATETIME);
	DEF(MYSQL_TYPE_YEAR);
	DEF(MYSQL_TYPE_NEWDATE);
	DEF(MYSQL_TYPE_ENUM);
	DEF(MYSQL_TYPE_SET);
	DEF(MYSQL_TYPE_TINY_BLOB);
	DEF(MYSQL_TYPE_MEDIUM_BLOB);
	DEF(MYSQL_TYPE_LONG_BLOB);
	DEF(MYSQL_TYPE_BLOB);
	DEF(MYSQL_TYPE_VAR_STRING);
	DEF(MYSQL_TYPE_STRING);
	DEF(MYSQL_TYPE_GEOMETRY);
#if MYSQL_VERSION_ID >= 50000
	DEF(MYSQL_TYPE_BIT);
#endif

	/* cheat with DEF() a bit :) */
#define PROXY_VERSION PACKAGE_VERSION_ID
	DEF(PROXY_VERSION);
#undef DEF

	/**
	 * create 
	 * - proxy.global 
	 * - proxy.global.config
	 */
	lua_newtable(L);
	lua_newtable(L);

	lua_setfield(L, -2, "config");
	lua_setfield(L, -2, "global");

	lua_setglobal(L, "proxy");
}

/**
 * handle the proxy.response.* table from the lua script
 *
 * proxy.response
 *   .type can be either ERR, OK or RAW
 *   .resultset (in case of OK)
 *     .fields
 *     .rows
 *   .errmsg (in case of ERR)
 *   .packet (in case of nil)
 *
 */
int network_mysqld_con_lua_handle_proxy_response(network_mysqld_con *con, const gchar *lua_script) {
	network_mysqld_con_lua_t *st = con->plugin_con_state;
	int resp_type = 1;
	const char *str;
	size_t str_len;
	lua_State *L = st->L;

	/**
	 * on the stack should be the fenv of our function */
	g_assert(lua_istable(L, -1));
	
	lua_getfield(L, -1, "proxy"); /* proxy.* from the env  */
	g_assert(lua_istable(L, -1));

	lua_getfield(L, -1, "response"); /* proxy.response */
	if (lua_isnil(L, -1)) {
		g_message("%s.%d: proxy.response isn't set in %s", __FILE__, __LINE__, 
				lua_script);

		lua_pop(L, 2); /* proxy + nil */

		return -1;
	} else if (!lua_istable(L, -1)) {
		g_message("%s.%d: proxy.response has to be a table, is %s in %s", __FILE__, __LINE__,
				lua_typename(L, lua_type(L, -1)),
				lua_script);

		lua_pop(L, 2); /* proxy + response */
		return -1;
	}

	lua_getfield(L, -1, "type"); /* proxy.response.type */
	if (lua_isnil(L, -1)) {
		/**
		 * nil is fine, we expect to get a raw packet in that case
		 */
		g_message("%s.%d: proxy.response.type isn't set in %s", __FILE__, __LINE__, 
				lua_script);

		lua_pop(L, 3); /* proxy + nil */

		return -1;

	} else if (!lua_isnumber(L, -1)) {
		g_message("%s.%d: proxy.response.type has to be a number, is %s in %s", __FILE__, __LINE__,
				lua_typename(L, lua_type(L, -1)),
				lua_script);
		
		lua_pop(L, 3); /* proxy + response + type */

		return -1;
	} else {
		resp_type = lua_tonumber(L, -1);
	}
	lua_pop(L, 1);

	switch(resp_type) {
	case MYSQLD_PACKET_OK: {
		GPtrArray *fields = NULL;
		GPtrArray *rows = NULL;
		gsize field_count = 0;

		lua_getfield(L, -1, "resultset"); /* proxy.response.resultset */
		if (lua_istable(L, -1)) {
			guint i;
			lua_getfield(L, -1, "fields"); /* proxy.response.resultset.fields */
			g_assert(lua_istable(L, -1));

			fields = network_mysqld_proto_fielddefs_new();
		
			for (i = 1, field_count = 0; ; i++, field_count++) {
				lua_rawgeti(L, -1, i);
				
				if (lua_istable(L, -1)) { /** proxy.response.resultset.fields[i] */
					MYSQL_FIELD *field;
	
					field = network_mysqld_proto_fielddef_new();
	
					lua_getfield(L, -1, "name"); /* proxy.response.resultset.fields[].name */
	
					if (!lua_isstring(L, -1)) {
						field->name = g_strdup("no-field-name");
	
						g_warning("%s.%d: proxy.response.type = OK, "
								"but proxy.response.resultset.fields[%u].name is not a string (is %s), "
								"using default", 
								__FILE__, __LINE__,
								i,
								lua_typename(L, lua_type(L, -1)));
					} else {
						field->name = g_strdup(lua_tostring(L, -1));
					}
					lua_pop(L, 1);
	
					lua_getfield(L, -1, "type"); /* proxy.response.resultset.fields[].type */
					if (!lua_isnumber(L, -1)) {
						g_warning("%s.%d: proxy.response.type = OK, "
								"but proxy.response.resultset.fields[%u].type is not a integer (is %s), "
								"using MYSQL_TYPE_STRING", 
								__FILE__, __LINE__,
								i,
								lua_typename(L, lua_type(L, -1)));
	
						field->type = MYSQL_TYPE_STRING;
					} else {
						field->type = lua_tonumber(L, -1);
					}
					lua_pop(L, 1);
					field->flags = PRI_KEY_FLAG;
					field->length = 32;
					g_ptr_array_add(fields, field);
					
					lua_pop(L, 1); /* pop key + value */
				} else if (lua_isnil(L, -1)) {
					lua_pop(L, 1); /* pop the nil and leave the loop */
					break;
				} else {
					g_error("proxy.response.resultset.fields[%d] should be a table, but is a %s", 
							i,
							lua_typename(L, lua_type(L, -1)));
				}
			}
			lua_pop(L, 1);
	
			rows = g_ptr_array_new();
			lua_getfield(L, -1, "rows"); /* proxy.response.resultset.rows */
			g_assert(lua_istable(L, -1));
			for (i = 1; ; i++) {
				lua_rawgeti(L, -1, i);
	
				if (lua_istable(L, -1)) { /** proxy.response.resultset.rows[i] */
					GPtrArray *row;
					gsize j;
	
					row = g_ptr_array_new();
	
					/* we should have as many columns as we had fields */
		
					for (j = 1; j < field_count + 1; j++) {
						lua_rawgeti(L, -1, j);
	
						if (lua_isnil(L, -1)) {
							g_ptr_array_add(row, NULL);
						} else {
							g_ptr_array_add(row, g_strdup(lua_tostring(L, -1)));
						}
	
						lua_pop(L, 1);
					}
	
					g_ptr_array_add(rows, row);
	
					lua_pop(L, 1); /* pop value */
				} else if (lua_isnil(L, -1)) {
					lua_pop(L, 1); /* pop the nil and leave the loop */
					break;
				} else {
					g_error("proxy.response.resultset.rows[%d] should be a table, but is a %s", 
							i,
							lua_typename(L, lua_type(L, -1)));
				}
			}
			lua_pop(L, 1);

			network_mysqld_con_send_resultset(con->client, fields, rows);
		} else {
			guint64 affected_rows = 0;
			guint64 insert_id = 0;

			lua_getfield(L, -2, "affected_rows"); /* proxy.response.affected_rows */
			if (lua_isnumber(L, -1)) {
				affected_rows = lua_tonumber(L, -1);
			}
			lua_pop(L, 1);

			lua_getfield(L, -2, "insert_id"); /* proxy.response.affected_rows */
			if (lua_isnumber(L, -1)) {
				insert_id = lua_tonumber(L, -1);
			}
			lua_pop(L, 1);

			network_mysqld_con_send_ok_full(con->client, affected_rows, insert_id, 0x0002, 0);
		}

		/**
		 * someone should cleanup 
		 */
		if (fields) {
			network_mysqld_proto_fielddefs_free(fields);
			fields = NULL;
		}

		if (rows) {
			guint i;
			for (i = 0; i < rows->len; i++) {
				GPtrArray *row = rows->pdata[i];
				guint j;

				for (j = 0; j < row->len; j++) {
					if (row->pdata[j]) g_free(row->pdata[j]);
				}

				g_ptr_array_free(row, TRUE);
			}
			g_ptr_array_free(rows, TRUE);
			rows = NULL;
		}

		
		lua_pop(L, 1); /* .resultset */
		
		break; }
	case MYSQLD_PACKET_ERR: {
		gint errorcode = ER_UNKNOWN_ERROR;
		const gchar *sqlstate = "07000"; /** let's call ourself Dynamic SQL ... 07000 is "dynamic SQL error" */
		
		lua_getfield(L, -1, "errcode"); /* proxy.response.errcode */
		if (lua_isnumber(L, -1)) {
			errorcode = lua_tonumber(L, -1);
		}
		lua_pop(L, 1);

		lua_getfield(L, -1, "sqlstate"); /* proxy.response.sqlstate */
		sqlstate = lua_tostring(L, -1);
		lua_pop(L, 1);

		lua_getfield(L, -1, "errmsg"); /* proxy.response.errmsg */
		if (lua_isstring(L, -1)) {
			str = lua_tolstring(L, -1, &str_len);

			network_mysqld_con_send_error_full(con->client, str, str_len, errorcode, sqlstate);
		} else {
			network_mysqld_con_send_error(con->client, C("(lua) proxy.response.errmsg is nil"));
		}
		lua_pop(L, 1);

		break; }
	case MYSQLD_PACKET_RAW: {
		guint i;
		/**
		 * iterate over the packet table and add each packet to the send-queue
		 */
		lua_getfield(L, -1, "packets"); /* proxy.response.packets */
		if (lua_isnil(L, -1)) {
			g_message("%s.%d: proxy.response.packets isn't set in %s", __FILE__, __LINE__,
					lua_script);

			lua_pop(L, 2 + 1); /* proxy + response + nil */

			return -1;
		} else if (!lua_istable(L, -1)) {
			g_message("%s.%d: proxy.response.packets has to be a table, is %s in %s", __FILE__, __LINE__,
					lua_typename(L, lua_type(L, -1)),
					lua_script);

			lua_pop(L, 2 + 1); /* proxy + response + packets */
			return -1;
		}

		for (i = 1; ; i++) {
			lua_rawgeti(L, -1, i);

			if (lua_isstring(L, -1)) { /** proxy.response.packets[i] */
				str = lua_tolstring(L, -1, &str_len);

				network_mysqld_queue_append(con->client, con->client->send_queue,
						str, str_len);
	
				lua_pop(L, 1); /* pop value */
			} else if (lua_isnil(L, -1)) {
				lua_pop(L, 1); /* pop the nil and leave the loop */
				break;
			} else {
				g_error("%s.%d: proxy.response.packets should be array of strings, field %u was %s", 
						__FILE__, __LINE__, 
						i,
						lua_typename(L, lua_type(L, -1)));
			}
		}

		lua_pop(L, 1); /* .packets */

		network_mysqld_queue_reset(con->client); /* reset the packet-id checks */

		break; }
	default:
		g_message("proxy.response.type is unknown: %d", resp_type);

		lua_pop(L, 2); /* proxy + response */

		return -1;
	}

	lua_pop(L, 2);

	return 0;
}


