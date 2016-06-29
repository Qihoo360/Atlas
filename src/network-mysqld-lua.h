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
#ifndef __NETWORK_MYSQLD_LUA__
#define __NETWORK_MYSQLD_LUA__

#include <lua.h>

#include "network-backend.h" /* query-status */
#include "network-injection.h" /* query-status */

#include "network-exports.h"

typedef enum {
	PROXY_NO_DECISION,
	PROXY_SEND_QUERY,
	PROXY_SEND_RESULT,
	PROXY_SEND_INJECTION,
	PROXY_IGNORE_RESULT       /** for read_query_result */
} network_mysqld_lua_stmt_ret;

typedef enum {
	REGISTER_CALLBACK_SUCCESS,
	REGISTER_CALLBACK_LOAD_FAILED,
	REGISTER_CALLBACK_EXECUTE_FAILED
} network_mysqld_register_callback_ret;

NETWORK_API int network_mysqld_con_getmetatable(lua_State *L);
NETWORK_API void network_mysqld_lua_init_global_fenv(lua_State *L);

NETWORK_API void network_mysqld_lua_setup_global(lua_State *L, chassis *chas);

/**
 * Encapsulates injected queries information passed back from the a Lua callback function.
 * 
 * @todo Simplify this structure, it should be folded into network_mysqld_con_lua_t.
 */
struct network_mysqld_con_lua_injection {
	network_injection_queue *queries;	/**< An ordered list of queries we want to have executed. */
	int sent_resultset;					/**< Flag to make sure we send only one result back to the client. */
};
/**
 * Contains extra connection state used for Lua-based plugins.
 */
typedef struct {
	struct network_mysqld_con_lua_injection injected;	/**< A list of queries to send to the backend.*/

	lua_State *L;                  /**< The Lua interpreter state of the current connection. */
	int L_ref;                     /**< The reference into the lua_scope's registry (a global structure in the Lua interpreter) */

	network_backend_t *backend;
	int backend_ndx;               /**< [lua] index into the backend-array */

	gboolean connection_close;     /**< [lua] set by the lua code to close a connection */

	struct timeval interval;       /**< The interval to be used for evt_timer, currently unused. */
	struct event evt_timer;        /**< The event structure used to implement the timer callback, currently unused. */

	gboolean is_reconnecting;      /**< if true, critical messages concerning failed connect() calls are suppressed, as they are expected errors */
} network_mysqld_con_lua_t;

NETWORK_API network_mysqld_con_lua_t *network_mysqld_con_lua_new();
NETWORK_API void network_mysqld_con_lua_free(network_mysqld_con_lua_t *st);

/** be sure to include network-mysqld.h */
NETWORK_API network_mysqld_register_callback_ret network_mysqld_con_lua_register_callback(network_mysqld_con *con, const char *lua_script);
NETWORK_API int network_mysqld_con_lua_handle_proxy_response(network_mysqld_con *con, const char *lua_script);

#endif
