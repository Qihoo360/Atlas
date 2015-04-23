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
#ifndef __NETWORK_CONN_POOL_LUA_H__
#define __NETWORK_CONN_POOL_LUA_H__

#include <lua.h>

#include "network-socket.h"
#include "network-mysqld.h"

#include "network-exports.h"

struct chassis_plugin_config {
    gchar *address;                   /**< listening address of the proxy */

    gchar **backend_addresses;        /**< read-write backends */
    gchar **read_only_backend_addresses; /**< read-only  backends */

    gint fix_bug_25371;               /**< suppress the second ERR packet of bug #25371 */

    gint profiling;                   /**< skips the execution of the read_query() function */

    gchar *lua_script;                /**< script to load at the start the connection */

    gint pool_change_user;            /**< don't reset the connection, when a connection is taken from the pool
                                        - this safes a round-trip, but we also don't cleanup the connection
                                        - another name could be "fast-pool-connect", but that's too friendly
                                        */

    gint start_proxy;

    gchar **client_ips;
    GHashTable *ip_table[2];
    gint ip_table_index;

    gchar **lvs_ips;
    GHashTable *lvs_table;

    gchar **tables;
    GHashTable *dt_table;

    gchar **pwds;
    GHashTable *pwd_table[2];
    gint pwd_table_index;

    network_mysqld_con *listen_con;

    FILE *sql_log;
    gchar *sql_log_type;
    gint sql_log_slow_ms;

    gchar *charset;

    /**
     * sharding config
     */
    GHashTable *table_shard_rules;  // GHashTable<(gchar*)tablename, sharding_table_t>
    GPtrArray  *db_groups;      // prt array of db_group_t
};

NETWORK_API int network_connection_pool_getmetatable(lua_State *L);

NETWORK_API int network_connection_pool_lua_add_connection(network_mysqld_con *con);
NETWORK_API network_socket *network_connection_pool_lua_swap(network_mysqld_con *con, int backend_ndx, GHashTable *pwd_table);

/**
 * sharding get socket
 */ 
NETWORK_API network_socket* network_connection_pool_get_socket(network_mysqld_con* con, network_backend_t* backend, GHashTable* pwd_table);
NETWORK_API int network_connection_pool_sharding_add_connection(network_mysqld_con* con, void* dbgroup_context);
#endif
