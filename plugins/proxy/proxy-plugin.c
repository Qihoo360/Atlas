/* $%BEGINLICENSE%$
 Copyright (c) 2007, 2010, Oracle and/or its affiliates. All rights reserved.

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

/**
 * @page page-plugin-proxy Proxy plugin
 *
 * The MySQL Proxy implements the MySQL Protocol in its own way.
 *
 *   -# connect @msc
 *   client, proxy, backend;
 *   --- [ label = "connect to backend" ];
 *   client->proxy  [ label = "INIT" ];
 *   proxy->backend [ label = "CONNECT_SERVER", URL="\ref proxy_connect_server" ];
 * @endmsc
 *   -# auth @msc
 *   client, proxy, backend;
 *   --- [ label = "authenticate" ];
 *   backend->proxy [ label = "READ_HANDSHAKE", URL="\ref proxy_read_handshake" ];
 *   proxy->client  [ label = "SEND_HANDSHAKE" ];
 *   client->proxy  [ label = "READ_AUTH", URL="\ref proxy_read_auth" ];
 *   proxy->backend [ label = "SEND_AUTH" ];
 *   backend->proxy [ label = "READ_AUTH_RESULT", URL="\ref proxy_read_auth_result" ];
 *   proxy->client  [ label = "SEND_AUTH_RESULT" ];
 * @endmsc
 *   -# query @msc
 *   client, proxy, backend;
 *   --- [ label = "query result phase" ];
 *   client->proxy  [ label = "READ_QUERY", URL="\ref proxy_read_query" ];
 *   proxy->backend [ label = "SEND_QUERY" ];
 *   backend->proxy [ label = "READ_QUERY_RESULT", URL="\ref proxy_read_query_result" ];
 *   proxy->client  [ label = "SEND_QUERY_RESULT", URL="\ref proxy_send_query_result" ];
 * @endmsc
 *
 *   - network_mysqld_proxy_connection_init()
 *     -# registers the callbacks
 *   - proxy_connect_server() (CON_STATE_CONNECT_SERVER)
 *     -# calls the connect_server() function in the lua script which might decide to
 *       -# send a handshake packet without contacting the backend server (CON_STATE_SEND_HANDSHAKE)
 *       -# closing the connection (CON_STATE_ERROR)
 *       -# picking a active connection from the connection pool
 *       -# pick a backend to authenticate against
 *       -# do nothing
 *     -# by default, pick a backend from the backend list on the backend with the least active connctions
 *     -# opens the connection to the backend with connect()
 *     -# when done CON_STATE_READ_HANDSHAKE
 *   - proxy_read_handshake() (CON_STATE_READ_HANDSHAKE)
 *     -# reads the handshake packet from the server
 *   - proxy_read_auth() (CON_STATE_READ_AUTH)
 *     -# reads the auth packet from the client
 *   - proxy_read_auth_result() (CON_STATE_READ_AUTH_RESULT)
 *     -# reads the auth-result packet from the server
 *   - proxy_send_auth_result() (CON_STATE_SEND_AUTH_RESULT)
 *   - proxy_read_query() (CON_STATE_READ_QUERY)
 *     -# reads the query from the client
 *   - proxy_read_query_result() (CON_STATE_READ_QUERY_RESULT)
 *     -# reads the query-result from the server
 *   - proxy_send_query_result() (CON_STATE_SEND_QUERY_RESULT)
 *     -# called after the data is written to the client
 *     -# if scripts wants to close connections, goes to CON_STATE_ERROR
 *     -# if queries are in the injection queue, goes to CON_STATE_SEND_QUERY
 *     -# otherwise goes to CON_STATE_READ_QUERY
 *     -# does special handling for COM_BINLOG_DUMP (go to CON_STATE_READ_QUERY_RESULT)

 */

#ifdef HAVE_SYS_FILIO_H
/**
 * required for FIONREAD on solaris
 */
#include <sys/filio.h>
#endif

#ifndef _WIN32
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#endif

#include <sys/stat.h>
#include <string.h>
#include <stdlib.h>
#include <fcntl.h>
#include <time.h>
#include <stdio.h>
#include <execinfo.h>

#include <errno.h>

#include <glib.h>

#ifdef HAVE_LUA_H
/**
 * embedded lua support
 */
#include <lua.h>
#include <lauxlib.h>
#include <lualib.h>
#endif

/* for solaris 2.5 and NetBSD 1.3.x */
#ifndef HAVE_SOCKLEN_T
typedef int socklen_t;
#endif


#include <mysqld_error.h> /** for ER_UNKNOWN_ERROR */

#include <math.h>
#include <openssl/evp.h>
#include <lemon/sqliteInt.h>

#include "network-mysqld.h"
#include "network-mysqld-proto.h"
#include "network-mysqld-packet.h"

#include "network-mysqld-lua.h"

#include "network-conn-pool.h"
#include "network-conn-pool-lua.h"

#include "sys-pedantic.h"
#include "network-injection.h"
#include "network-injection-lua.h"
#include "network-backend.h"
#include "glib-ext.h"
#include "lua-env.h"

#include "proxy-plugin.h"

#include "lua-load-factory.h"

#include "chassis-timings.h"
#include "chassis-gtimeval.h"

//#include "lib/sql-tokenizer.h"
#include "chassis-event-thread.h"
#include "chassis-sharding.h"

#define C(x) x, sizeof(x) - 1
#define S(x) x->str, x->len

#define HASH_INSERT(hash, key, expr) \
		do { \
			GString *hash_value; \
			if ((hash_value = g_hash_table_lookup(hash, key))) { \
				expr; \
			} else { \
				hash_value = g_string_new(NULL); \
				expr; \
				g_hash_table_insert(hash, g_strdup(key), hash_value); \
			} \
		} while(0);

#define CRASHME() do { char *_crashme = NULL; *_crashme = 0; } while(0);

static gboolean online = TRUE;

static gchar op = COM_QUERY;

typedef struct {
	gchar* table_name;
	gchar* column_name;
	guint table_num;
} db_table_t;

typedef enum {
	OFF,
	ON,
	REALTIME
} SQL_LOG_TYPE;

SQL_LOG_TYPE sql_log_type = OFF;

char* charset[64] = {NULL, "big5", NULL, NULL, NULL, NULL, NULL, NULL, "latin1", NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, "gb2312", NULL, NULL, NULL, "gbk", NULL, NULL, NULL, NULL, "utf8", NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, "utf8mb4", NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, "binary"};


chassis_plugin_config *config = NULL;


/**
 * find the first split key column, only handle the first one
 */
void find_column_expr_inwhere(Expr *expr, gchar *split_column, Expr **column_expr) {
    if (expr == NULL) return;

    if (expr->op == TK_EQ ) {
        if (expr->pLeft != NULL && LEMON_TOKEN_STRING(expr->pLeft->op) &&
            expr->pRight != NULL && expr->pRight->op == TK_INTEGER && 
            strncasecmp(split_column, (const char*)expr->pLeft->token.z, expr->pLeft->token.n) == 0) 
        {
            *column_expr = expr;
            return;
        }
    } else if (expr->op == TK_IN ) {
        if (expr->pLeft != NULL && LEMON_TOKEN_STRING(expr->pLeft->op) && expr->pList != NULL && 
                strncasecmp(split_column, (const char*)expr->pLeft->token.z, expr->pLeft->token.n) == 0) 
        {
            *column_expr = expr;
            return;
        }
    }

    if (expr->pLeft != NULL) {
         find_column_expr_inwhere(expr->pLeft, split_column, column_expr);
         if (*column_expr != NULL) { return; }
    }

    if (expr->pRight != NULL) {
        find_column_expr_inwhere(expr->pRight, split_column, column_expr);
        if (*column_expr != NULL) { return; }
    }
}

typedef struct InsertValues{
    int key_index;
    ValuesList *values_list;
    ExprList *set_list;
}InsertValues;

void find_column_expr(Parse *parse_obj, gchar *split_column, Expr **column_expr, InsertValues **insert_values) {
    int sqltype = parse_obj->parsed.array[0].sqltype;
    if (sqltype == SQLTYPE_SELECT || sqltype == SQLTYPE_DELETE || sqltype == SQLTYPE_UPDATE) { // find where
        Expr *where_expr = parse_get_where_expr(parse_obj);
        if (where_expr == NULL || column_expr == NULL) return;
        
        find_column_expr_inwhere(where_expr, split_column, column_expr);
    } else if (sqltype == SQLTYPE_INSERT || sqltype == SQLTYPE_REPLACE) {
        if (insert_values == NULL) { return; }
        
        *insert_values = NULL;
        Insert *insert_obj = parse_obj->parsed.array[0].result.insertObj;
        int i = 0;
        if (insert_obj->pValuesList) {
            for (i = 0; i < insert_obj->pColumn->nId; i++) {
                if (strcmp(split_column, insert_obj->pColumn->a[i].zName) == 0) {
                    break;
                }
            }

            if (i == insert_obj->pColumn->nId) { return; } // no split_column
        } else if (insert_obj->pSetList) {
            for (i = 0; i < insert_obj->pSetList->nExpr; i++) {
                if (strcmp(split_column, insert_obj->pSetList->a[i].zName) == 0) {
                    break;
                }
            }

            if (i == insert_obj->pSetList->nExpr) { return; }
        } else { 
            return; 
        }
    
        *insert_values = g_new0(InsertValues, 1);
        (*insert_values)->key_index = i;
        (*insert_values)->values_list = insert_obj->pValuesList; // values_list and set_list never be not null the same time.
        (*insert_values)->set_list = insert_obj->pSetList;
    }   
}

void rewrite_sql_from_in(parse_info_t *parse_info, Expr *column_expr, guint subtable_num, GPtrArray *sqls) {
    char value[64] = {0};
    char *orig_sql = parse_info->orig_sql;
    char *orig_sql_end = orig_sql + parse_info->orig_sql_len;
    Token table_token = parse_info->table_token;
    char *str_after_table = (char*)table_token.z + table_token.n;
    gchar op = COM_QUERY;

    if ( column_expr->pList->nExpr == 1) {
        Expr *value_expr = column_expr->pList->a[0].pExpr;
        dup_token2buff(value, sizeof(value), value_expr->token);
        guint subtable = g_ascii_strtoull(value, NULL, 10) % subtable_num;
        GString *sql = g_string_new(&op);
        g_string_append_printf(sql, "%.*s%s_%u%.*s", (const char*)table_token.z - orig_sql, orig_sql, 
                parse_info->table_name, subtable, orig_sql_end - str_after_table, str_after_table);

        g_ptr_array_add(sqls, sql);
    } else if (column_expr->pList->nExpr > 1) {
        GArray *multi_value[subtable_num];
        int i;
        for (i = 0; i < subtable_num; i++) { 
            multi_value[i] = g_array_new(FALSE, FALSE, sizeof(guint));
        }

        for (i = 0; i < column_expr->pList->nExpr; i++) {
            dup_token2buff(value, sizeof(value), column_expr->pList->a[i].pExpr->token);                     
            guint column_value = atoi(value);
            g_array_append_val(multi_value[column_value%subtable_num], column_value);
        }

        int j, k;
        for (j = 0; j < subtable_num; i++) {
            GString *tmp = g_string_new("IN(");
            g_string_append_printf(tmp, "%u", g_array_index(multi_value[j], guint, 0));

            for (k = 0; k < multi_value[j]->len; k++) {
                g_string_append_printf(tmp, "%u", g_array_index(multi_value[j], guint, k));
            }
            g_string_append_c(tmp, ')');

            GString* sql = g_string_new(&op);
            g_string_append_printf(sql, "%.*s%s_%u", (const char*)table_token.z - orig_sql, orig_sql, parse_info->table_name, j);
            // append str before IN
            g_string_append_printf(sql, "%.*s", (const char*)column_expr->span.z - str_after_table, str_after_table);
            char *str_after_in = (char*)column_expr->span.z + column_expr->span.n;
            g_string_append_printf(sql, "%s", tmp);
            g_string_append_printf(sql, "%.*s", orig_sql_end - str_after_in, str_after_in);

            g_ptr_array_add(sqls, sql);
        }

        for (i = 0; i < subtable_num; i++) {
            g_array_free(multi_value[i], TRUE);
        }
    }  
}

void rewrite_sql_insert_sql(parse_info_t *parse_info, InsertValues *insert_values, guint subtable_num, GPtrArray *sqls, char **errmsg) {
    char value[64] = {0};
    char *orig_sql = parse_info->orig_sql;
    char *orig_sql_end = orig_sql + parse_info->orig_sql_len;
    Token table_token = parse_info->table_token;
    char *str_after_table = (char*)table_token.z + table_token.n;
    gchar op = COM_QUERY;

    if (insert_values->set_list) {
        dup_token2buff(value, sizeof(value), insert_values->set_list->a[insert_values->key_index].pExpr->token);
        guint subtable = g_ascii_strtoull(value, NULL, 10) % subtable_num;
        GString *sql = g_string_new(&op);
        g_string_append_printf(sql, "%.*s%s_%u%.*s", (const char*)table_token.z - orig_sql, orig_sql, 
                parse_info->table_name, subtable, orig_sql_end - str_after_table, str_after_table);

        g_ptr_array_add(sqls, sql);
    } else if (insert_values->values_list) {
        if (insert_values->values_list->nValues > 1) {
            *errmsg = g_strdup("Proxy Warning - Not support insert multi values in sub-table yet!");
        } else {
            ExprList *exprlist = insert_values->values_list->a[0];
            dup_token2buff(value, sizeof(value), exprlist->a[insert_values->key_index].pExpr->token);
            guint subtable = g_ascii_strtoull(value, NULL, 10) % subtable_num;
            GString *sql = g_string_new(&op);
            g_string_append_printf(sql, "%.*s%s_%u%.*s", (const char*)table_token.z - orig_sql, orig_sql, 
                    parse_info->table_name, subtable, orig_sql_end - str_after_table, str_after_table);

            g_ptr_array_add(sqls, sql);
        }          
    }
}

void rewrite_sql_from_eq(parse_info_t *parse_info, Expr *column_expr, guint subtable_num, GPtrArray *sqls) {
    char value[64] = {0};
    char *orig_sql = parse_info->orig_sql;
    char *orig_sql_end = orig_sql + parse_info->orig_sql_len;
    Token table_token = parse_info->table_token;
    char *str_after_table = (char*)table_token.z + table_token.n;
    gchar op = COM_QUERY;

    dup_token2buff(value, sizeof(value), column_expr->pRight->token);
    guint subtable = g_ascii_strtoull(value, NULL, 10) % subtable_num;
    GString *sql = g_string_new(&op);
    g_string_append_printf(sql, "%.*s%s_%u%.*s", (const char*)table_token.z - orig_sql, orig_sql, 
            parse_info->table_name, subtable, orig_sql_end - str_after_table, str_after_table);

    g_ptr_array_add(sqls, sql);

}

// rewrite_sql is deprecated, and the code is without testing
GPtrArray* rewrite_sql(parse_info_t *parse_info, gchar *split_column, guint subtable_num, char **errmsg) {
    GPtrArray *sqls = NULL;
    Expr *column_expr = NULL;
    InsertValues *insert_values = NULL;

    Parse *parse_obj = parse_info->parse_obj;
    find_column_expr(parse_obj, split_column, &column_expr, &insert_values);

    if (column_expr) {
        sqls = g_ptr_array_new();
        if (column_expr->op == TK_EQ) {
            rewrite_sql_from_eq(parse_info, column_expr, subtable_num, sqls);
        } else if (column_expr->op == TK_IN) {   
            rewrite_sql_from_in(parse_info, column_expr, subtable_num, sqls);
        }     
    } else if (insert_values) {
        sqls = g_ptr_array_new();
        rewrite_sql_insert_sql(parse_info, insert_values, subtable_num, sqls, errmsg);
    } 
exit:
    if (insert_values != NULL) { g_free(insert_values); }
    if (*errmsg != NULL || (sqls != NULL && sqls->len == 0)) { 
        g_ptr_array_free(sqls, TRUE); 
        sqls = NULL;
    }
    return sqls;
}

GPtrArray* sql_parse(network_mysqld_con *con, parse_info_t *parse_info, char **errmsg) {
	if (parse_info->table_name == NULL) return NULL;

	gchar* table_name = NULL;
	if (parse_info->db_name == NULL) {
		table_name = g_strdup_printf("%s.%s", con->client->default_db->str, parse_info->table_name);
	} else {
		table_name = g_strdup_printf("%s.%s", parse_info->db_name, parse_info->table_name);
	}

	db_table_t* dt = g_hash_table_lookup(config->dt_table, table_name); 
	if (dt == NULL) {
		g_free(table_name);
		return NULL;
	}
    
	g_free(table_name);
    
	GPtrArray* sqls = rewrite_sql(parse_info, dt->column_name, dt->table_num, errmsg);
	return sqls;
}

int idle_rw(network_mysqld_con* con) {
	int ret = -1;
	guint i;

	network_backends_t* backends = con->srv->backends;

	guint count = network_backends_count(backends);
	for (i = 0; i < count; ++i) {
		network_backend_t* backend = network_backends_get(backends, i);
		if (backend == NULL) continue;

		if (chassis_event_thread_pool(backend) == NULL) continue;

		if (backend->type == BACKEND_TYPE_RW && backend->state == BACKEND_STATE_UP) {
			ret = i;
			break;
		}
	}

	return ret;
}

int idle_ro(network_mysqld_con* con) {
	int max_conns = -1;
	guint i;

	network_backends_t* backends = con->srv->backends;

	guint count = network_backends_count(backends);
	for(i = 0; i < count; ++i) {
		network_backend_t* backend = network_backends_get(backends, i);
		if (backend == NULL) continue;

		if (chassis_event_thread_pool(backend) == NULL) continue;

		if (backend->type == BACKEND_TYPE_RO && backend->state == BACKEND_STATE_UP) {
			if (max_conns == -1 || backend->connected_clients < max_conns) {
				max_conns = backend->connected_clients;
			}
		}
	}

	return max_conns;
}

int wrr_ro(network_mysqld_con *con) {
	guint i;

	network_backends_t* backends = con->srv->backends;
	g_wrr_poll* rwsplit = backends->global_wrr;
	guint ndx_num = network_backends_count(backends);

	// set max weight if no init
	if (rwsplit->max_weight == 0) {
		for(i = 0; i < ndx_num; ++i) {
			network_backend_t* backend = network_backends_get(backends, i);
			if (backend == NULL) continue;
			if (rwsplit->max_weight < backend->weight) {
				rwsplit->max_weight = backend->weight;
				rwsplit->cur_weight = backend->weight;
			}
		}
	}

	guint max_weight = rwsplit->max_weight;
	guint cur_weight = rwsplit->cur_weight;
	guint next_ndx   = rwsplit->next_ndx;

	// get backend index by slave wrr
	gint ndx = -1;
	for(i = 0; i < ndx_num; ++i) {
		network_backend_t* backend = network_backends_get(backends, next_ndx);
		if (backend == NULL) goto next;

		if (chassis_event_thread_pool(backend) == NULL) goto next;

		if (backend->type == BACKEND_TYPE_RO && backend->weight >= cur_weight && backend->state == BACKEND_STATE_UP) ndx = next_ndx;

	next:
		if (next_ndx >= ndx_num - 1) {
			--cur_weight;
			next_ndx = 0;

			if (cur_weight == 0) cur_weight = max_weight;
		} else {
			++next_ndx;
		}

		if (ndx != -1) break;
	}

	rwsplit->cur_weight = cur_weight;
	rwsplit->next_ndx = next_ndx;
	return ndx;
}

/**
 * call the lua function to intercept the handshake packet
 *
 * @return PROXY_SEND_QUERY  to send the packet from the client
 *         PROXY_NO_DECISION to pass the server packet unmodified
 */
static network_mysqld_lua_stmt_ret proxy_lua_read_handshake(network_mysqld_con *con) {
	network_mysqld_lua_stmt_ret ret = PROXY_NO_DECISION; /* send what the server gave us */
#ifdef HAVE_LUA_H
	network_mysqld_con_lua_t *st = con->plugin_con_state;

	lua_State *L;

	/* call the lua script to pick a backend
	   ignore the return code from network_mysqld_con_lua_register_callback, because we cannot do anything about it,
	   it would always show up as ERROR 2013, which is not helpful.
	 */
	(void)network_mysqld_con_lua_register_callback(con, config->lua_script);

	if (!st->L) return ret;

	L = st->L;

	g_assert(lua_isfunction(L, -1));
	lua_getfenv(L, -1);
	g_assert(lua_istable(L, -1));

	lua_getfield_literal(L, -1, C("read_handshake"));
	if (lua_isfunction(L, -1)) {
		/* export
		 *
		 * every thing we know about it
		 *  */

		if (lua_pcall(L, 0, 1, 0) != 0) {
			g_critical("(read_handshake) %s", lua_tostring(L, -1));

			lua_pop(L, 1); /* errmsg */

			/* the script failed, but we have a useful default */
		} else {
			if (lua_isnumber(L, -1)) {
				ret = lua_tonumber(L, -1);
			}
			lua_pop(L, 1);
		}

		switch (ret) {
		case PROXY_NO_DECISION:
			break;
		case PROXY_SEND_QUERY:
			g_warning("%s.%d: (read_handshake) return proxy.PROXY_SEND_QUERY is deprecated, use PROXY_SEND_RESULT instead",
					__FILE__, __LINE__);

			ret = PROXY_SEND_RESULT;
		case PROXY_SEND_RESULT:
			/**
			 * proxy.response.type = ERR, RAW, ...
			 */

			if (network_mysqld_con_lua_handle_proxy_response(con, config->lua_script)) {
				/**
				 * handling proxy.response failed
				 *
				 * send a ERR packet
				 */

				network_mysqld_con_send_error(con->client, C("(lua) handling proxy.response failed, check error-log"));
			}

			break;
		default:
			ret = PROXY_NO_DECISION;
			break;
		}
	} else if (lua_isnil(L, -1)) {
		lua_pop(L, 1); /* pop the nil */
	} else {
		g_message("%s.%d: %s", __FILE__, __LINE__, lua_typename(L, lua_type(L, -1)));
		lua_pop(L, 1); /* pop the ... */
	}
	lua_pop(L, 1); /* fenv */

	g_assert(lua_isfunction(L, -1));
#endif
	return ret;
}


/**
 * parse the hand-shake packet from the server
 *
 *
 * @note the SSL and COMPRESS flags are disabled as we can't
 *       intercept or parse them.
 */
NETWORK_MYSQLD_PLUGIN_PROTO(proxy_read_handshake) {
	network_packet packet;
	network_socket *recv_sock, *send_sock;
	network_mysqld_auth_challenge *challenge;
	GString *challenge_packet;
	guint8 status = 0;
	int err = 0;

	send_sock = con->client;
	recv_sock = con->server;

	packet.data = g_queue_peek_tail(recv_sock->recv_queue->chunks);
	packet.offset = 0;

	err = err || network_mysqld_proto_skip_network_header(&packet);
	if (err) return NETWORK_SOCKET_ERROR;

	err = err || network_mysqld_proto_peek_int8(&packet, &status);
	if (err) return NETWORK_SOCKET_ERROR;

	/* handle ERR packets directly */
	if (status == 0xff) {
		/* move the chunk from one queue to the next */
		guint16 errcode;
		gchar *errmsg = NULL;

		// get error message from packet
		packet.offset += 1; // skip 0xff
		err = err || network_mysqld_proto_get_int16(&packet, &errcode);
		if (packet.offset < packet.data->len) {
		    err = err || network_mysqld_proto_get_string_len(&packet, &errmsg, packet.data->len - packet.offset);
		}

		g_warning("[%s]: error packet from server (%s -> %s): %s(%d)", G_STRLOC, recv_sock->dst->name->str, recv_sock->src->name->str, errmsg, errcode);
		if (errmsg) g_free(errmsg);

		network_mysqld_queue_append_raw(send_sock, send_sock->send_queue, g_queue_pop_tail(recv_sock->recv_queue->chunks));

		network_mysqld_con_lua_t *st = con->plugin_con_state;
		if (st->backend->state != BACKEND_STATE_OFFLINE) st->backend->state = BACKEND_STATE_DOWN;
	//	chassis_gtime_testset_now(&st->backend->state_since, NULL);
		network_socket_free(con->server);
		con->server = NULL;

		return NETWORK_SOCKET_ERROR; /* it sends what is in the send-queue and hangs up */
	}

	challenge = network_mysqld_auth_challenge_new();
	if (network_mysqld_proto_get_auth_challenge(&packet, challenge)) {
 		g_string_free(g_queue_pop_tail(recv_sock->recv_queue->chunks), TRUE);

		network_mysqld_auth_challenge_free(challenge);

		return NETWORK_SOCKET_ERROR;
	}

 	con->server->challenge = challenge;

	/* we can't sniff compressed packets nor do we support SSL */
	challenge->capabilities &= ~(CLIENT_COMPRESS);
	challenge->capabilities &= ~(CLIENT_SSL);

	switch (proxy_lua_read_handshake(con)) {
	case PROXY_NO_DECISION:
		break;
	case PROXY_SEND_RESULT:
		/* the client overwrote and wants to send its own packet
		 * it is already in the queue */

 		g_string_free(g_queue_pop_tail(recv_sock->recv_queue->chunks), TRUE);

		return NETWORK_SOCKET_ERROR;
	default:
		g_error("%s.%d: ...", __FILE__, __LINE__);
		break;
	}

	challenge_packet = g_string_sized_new(packet.data->len); /* the packet we generate will be likely as large as the old one. should save some reallocs */
	network_mysqld_proto_append_auth_challenge(challenge_packet, challenge);
	network_mysqld_queue_sync(send_sock, recv_sock);
	network_mysqld_queue_append(send_sock, send_sock->send_queue, S(challenge_packet));

	g_string_free(challenge_packet, TRUE);

	g_string_free(g_queue_pop_tail(recv_sock->recv_queue->chunks), TRUE);

	/* copy the pack to the client */
	con->state = CON_STATE_SEND_HANDSHAKE;

	return NETWORK_SOCKET_SUCCESS;
}

static network_mysqld_lua_stmt_ret proxy_lua_read_auth(network_mysqld_con *con) {
	network_mysqld_lua_stmt_ret ret = PROXY_NO_DECISION;

#ifdef HAVE_LUA_H
	network_mysqld_con_lua_t *st = con->plugin_con_state;
	lua_State *L;

	/* call the lua script to pick a backend
	   ignore the return code from network_mysqld_con_lua_register_callback, because we cannot do anything about it,
	   it would always show up as ERROR 2013, which is not helpful.
	*/
	(void)network_mysqld_con_lua_register_callback(con, config->lua_script);

	if (!st->L) return 0;

	L = st->L;

	g_assert(lua_isfunction(L, -1));
	lua_getfenv(L, -1);
	g_assert(lua_istable(L, -1));

	lua_getfield_literal(L, -1, C("read_auth"));
	if (lua_isfunction(L, -1)) {

		/* export
		 *
		 * every thing we know about it
		 *  */

		if (lua_pcall(L, 0, 1, 0) != 0) {
			g_critical("(read_auth) %s", lua_tostring(L, -1));

			lua_pop(L, 1); /* errmsg */

			/* the script failed, but we have a useful default */
		} else {
			if (lua_isnumber(L, -1)) {
				ret = lua_tonumber(L, -1);
			}
			lua_pop(L, 1);
		}

		switch (ret) {
		case PROXY_NO_DECISION:
			break;
		case PROXY_SEND_RESULT:
			/* answer directly */

			if (network_mysqld_con_lua_handle_proxy_response(con, config->lua_script)) {
				/**
				 * handling proxy.response failed
				 *
				 * send a ERR packet
				 */

				network_mysqld_con_send_error(con->client, C("(lua) handling proxy.response failed, check error-log"));
			}

			break;
		case PROXY_SEND_QUERY:
			/* something is in the injection queue, pull it from there and replace the content of
			 * original packet */

			if (st->injected.queries->length) {
				ret = PROXY_SEND_INJECTION;
			} else {
				ret = PROXY_NO_DECISION;
			}
			break;
		default:
			ret = PROXY_NO_DECISION;
			break;
		}

		/* ret should be a index into */

	} else if (lua_isnil(L, -1)) {
		lua_pop(L, 1); /* pop the nil */
	} else {
		g_message("%s.%d: %s", __FILE__, __LINE__, lua_typename(L, lua_type(L, -1)));
		lua_pop(L, 1); /* pop the ... */
	}
	lua_pop(L, 1); /* fenv */

	g_assert(lua_isfunction(L, -1));
#endif
	return ret;
}

NETWORK_MYSQLD_PLUGIN_PROTO(proxy_read_auth) {
	/* read auth from client */
	network_packet packet;
	network_socket *recv_sock, *send_sock;
	network_mysqld_auth_response *auth;
	int err = 0;
	gboolean free_client_packet = TRUE;
	network_mysqld_con_lua_t *st = con->plugin_con_state;

	recv_sock = con->client;
	send_sock = con->server;

	packet.data = g_queue_pop_tail(recv_sock->recv_queue->chunks);
	packet.offset = 0;

	err = err || network_mysqld_proto_skip_network_header(&packet);
	if (err) return NETWORK_SOCKET_ERROR;

	auth = network_mysqld_auth_response_new();

	err = err || network_mysqld_proto_get_auth_response(&packet, auth);

	g_string_free(packet.data, TRUE);

	if (err) {
		network_mysqld_auth_response_free(auth);
		return NETWORK_SOCKET_ERROR;
	}
	if (!(auth->capabilities & CLIENT_PROTOCOL_41)) {
		/* should use packet-id 0 */
		network_mysqld_queue_append(con->client, con->client->send_queue, C("\xff\xd7\x07" "4.0 protocol is not supported"));
		network_mysqld_auth_response_free(auth);
		return NETWORK_SOCKET_ERROR;
	}

	con->client->response = auth;

//	g_string_assign_len(con->client->default_db, S(auth->database));

	con->state = CON_STATE_SEND_AUTH_RESULT;

	GString *hashed_password = g_hash_table_lookup(config->pwd_table[config->pwd_table_index], auth->username->str);
	if (hashed_password) {
		GString *expected_response = g_string_sized_new(20);
		network_mysqld_proto_password_scramble(expected_response, S(con->challenge), S(hashed_password));
		if (g_string_equal(expected_response, auth->response)) {
			g_string_assign_len(recv_sock->default_db, S(auth->database));

			char *client_charset = NULL;
			if (config->charset == NULL) client_charset = charset[auth->charset];
			else client_charset = config->charset;

			g_string_assign(recv_sock->charset_client,     client_charset);
			g_string_assign(recv_sock->charset_results,    client_charset);
			g_string_assign(recv_sock->charset_connection, client_charset);

			network_mysqld_con_send_ok(recv_sock);
		} else {
			GString *error = g_string_sized_new(64);
			g_string_printf(error, "Access denied for user '%s'@'%s' (using password: YES)", auth->username->str, recv_sock->src->name->str);
			network_mysqld_con_send_error_full(recv_sock, S(error), ER_ACCESS_DENIED_ERROR, "28000");
			g_string_free(error, TRUE);
		}
		g_string_free(expected_response, TRUE);
	} else {
		GString *error = g_string_sized_new(64);
		g_string_printf(error, "Access denied for user '%s'@'%s' (using password: YES)", auth->username->str, recv_sock->src->name->str);
		network_mysqld_con_send_error_full(recv_sock, S(error), ER_ACCESS_DENIED_ERROR, "28000");
		g_string_free(error, TRUE);
	}

	return NETWORK_SOCKET_SUCCESS;
}

NETWORK_MYSQLD_PLUGIN_PROTO(proxy_read_auth_result) {
	GString *packet;
	GList *chunk;
	network_socket *recv_sock, *send_sock;

	recv_sock = con->server;
	send_sock = con->client;

	chunk = recv_sock->recv_queue->chunks->tail;
	packet = chunk->data;

	/* send the auth result to the client */
	if (con->server->is_authed) {
		/**
		 * we injected a COM_CHANGE_USER above and have to correct to
		 * packet-id now
         * if config->pool_change_user is false, we don't inject a COM_CHANGE_USER and jump to send_auth_result directly,
         * will not reach here.
		 */
		packet->str[3] = 2;
	}

	/**
	 * copy the
	 * - default-db,
     * - charset,
	 * - username,
	 * - scrambed_password
	 *
	 * to the server-side
	 */
    g_string_assign_len(recv_sock->charset_client, S(send_sock->charset_client));
    g_string_assign_len(recv_sock->charset_connection, S(send_sock->charset_connection));
    g_string_assign_len(recv_sock->charset_results, S(send_sock->charset_results));
	g_string_assign_len(recv_sock->default_db, S(send_sock->default_db));

	if (con->server->response) {
		/* in case we got the connection from the pool it has the response from the previous auth */
		network_mysqld_auth_response_free(con->server->response);
		con->server->response = NULL;
	}
	con->server->response = network_mysqld_auth_response_copy(con->client->response);

	/**
	 * recv_sock still points to the old backend that
	 * we received the packet from.
	 *
	 * backend_ndx = 0 might have reset con->server
	 */
/*
	switch (proxy_lua_read_auth_result(con)) {
	case PROXY_SEND_RESULT:
		 // we already have content in the send-sock
		 // chunk->packet is not forwarded, free it

		g_string_free(packet, TRUE);

		break;
	case PROXY_NO_DECISION:
		network_mysqld_queue_append_raw(
				send_sock,
				send_sock->send_queue,
				packet);

		break;
	default:
		g_error("%s.%d: ... ", __FILE__, __LINE__);
		break;
	}
*/
	if (packet->str[NET_HEADER_SIZE] == MYSQLD_PACKET_OK) {
		network_connection_pool_lua_add_connection(con);
	}/*else {
		network_backend_t* backend = ((network_mysqld_con_lua_t*)(con->plugin_con_state))->backend;
		if (backend->state != BACKEND_STATE_OFFLINE) backend->state = BACKEND_STATE_DOWN;
	}*/
	network_mysqld_queue_append_raw(
			send_sock,
			send_sock->send_queue,
			packet);
	/**
	 * we handled the packet on the server side, free it
	 */
	g_queue_delete_link(recv_sock->recv_queue->chunks, chunk);

	/* the auth phase is over
	 *
	 * reset the packet-id sequence
	 */
	network_mysqld_queue_reset(send_sock);
	network_mysqld_queue_reset(recv_sock);

	con->state = CON_STATE_SEND_AUTH_RESULT;

	return NETWORK_SOCKET_SUCCESS;
}

int rw_split(Parse *parse_obj, network_mysqld_con* con) {
    if (parse_obj == NULL) { return idle_rw(con); }

    TokenArray *tokens_array = &parse_obj->tokens;
    if (tokens_array->curSize > 0) {
        TokenItem *token_item = &tokens_array->array[0];
        if (token_item->tokenType == TK_COMMENT && strncasecmp("/*MASTER*/", (const char*)token_item->token.z, token_item->token.n) == 0) {
            return idle_rw(con);
        }
    }

	if (g_hash_table_size(con->locks) > 0) return idle_rw(con);

	return wrr_ro(con);
}

void modify_user(network_mysqld_con* con) {
	if (con->server == NULL) return;

	GString* client_user = con->client->response->username;
	GString* server_user = con->server->response->username;

	if (!g_string_equal(client_user, server_user)) {
		GString* com_change_user = g_string_new(NULL);

		g_string_append_c(com_change_user, COM_CHANGE_USER);
		g_string_append_len(com_change_user, client_user->str, client_user->len + 1);

		GString* hashed_password = g_hash_table_lookup(config->pwd_table[config->pwd_table_index], client_user->str);
		if (!hashed_password) return;

		GString* expected_response = g_string_sized_new(20);
		network_mysqld_proto_password_scramble(expected_response, S(con->server->challenge->challenge), S(hashed_password));

		g_string_append_c(com_change_user, (expected_response->len & 0xff));
		g_string_append_len(com_change_user, S(expected_response));
		g_string_append_c(com_change_user, 0);

		injection* inj = injection_new(6, com_change_user);
		inj->resultset_is_needed = TRUE;
		network_mysqld_con_lua_t* st = con->plugin_con_state;
		g_queue_push_head(st->injected.queries, inj);

		g_string_truncate(con->client->response->response, 0);
		g_string_assign(con->client->response->response, expected_response->str);
		g_string_free(expected_response, TRUE);
	}
}

void inject_trans_begin_sql(network_mysqld_con* con, gboolean is_crud_sql) {
    trans_context_t* trans_ctx = con->trans_context;
    if (is_crud_sql == FALSE || trans_ctx->trans_stage != TRANS_STAGE_BEFORE_SEND_BEGIN) { return; }
    g_assert(trans_ctx->is_default_dbgroup_in_trans == FALSE && trans_ctx->in_trans_dbgroup_ctx == NULL && 
            trans_ctx->origin_begin_sql != NULL);
    
    injection* inj = injection_new(INJECTION_TRANS_BEGIN_SQL, trans_ctx->origin_begin_sql);
    inj->resultset_is_needed = TRUE;
    trans_ctx->origin_begin_sql = NULL;
    
    trans_ctx->trans_stage = TRANS_STAGE_SENDING_BEGIN;
    network_mysqld_con_lua_t* st = (network_mysqld_con_lua_t*)con->plugin_con_state;
    network_injection_queue_prepend(st->injected.queries, inj);
}

void modify_db(network_mysqld_con* con) {
	char* default_db = con->client->default_db->str;
    
	if (default_db != NULL && con->server && con->server->default_db && 
            strcmp(default_db, con->server->default_db->str) != 0) 
    {
		char cmd = COM_INIT_DB;
		GString* query = g_string_new_len(&cmd, 1);
		g_string_append(query, default_db);
		injection* inj = injection_new(2, query);
		inj->resultset_is_needed = TRUE;
		network_mysqld_con_lua_t* st = con->plugin_con_state;
		g_queue_push_head(st->injected.queries, inj);
	}
}

void modify_charset(parse_info_t *parse_info, network_mysqld_con *con) {
    if (parse_info->parse_obj == NULL) { return; }

    g_string_truncate(con->charset_client, 0);
    g_string_truncate(con->charset_results, 0);
    g_string_truncate(con->charset_connection, 0);

    if (con->server == NULL) return;

    gboolean is_set_client = FALSE;
    gboolean is_set_results = FALSE;
    gboolean is_set_connection = FALSE;

    Parse *parse_obj = parse_info->parse_obj;
    ParsedResultItem *parsed_result = &parse_obj->parsed.array[0];
    if (parsed_result->sqltype == SQLTYPE_SET_NAMES || parsed_result->sqltype == SQLTYPE_SET_CHARACTER_SET) {
        SetStatement *set_obj = parsed_result->result.setObj;
        is_set_client = is_set_results = is_set_connection = TRUE;
        g_string_append_len(con->charset_client, (const char*)set_obj->value.z, set_obj->value.n);
        g_string_append_len(con->charset_results, (const char*)set_obj->value.z, set_obj->value.n);
        g_string_append_len(con->charset_connection, (const char*)set_obj->value.z, set_obj->value.n);
    } else if (parsed_result->sqltype == SQLTYPE_SET) {
        SetStatement *set_obj = parsed_result->result.setObj;
        int i;
        for (i = 0; i < set_obj->pSetList->nExpr; i++) {
            Expr *value_expr = set_obj->pSetList->a[i].pExpr;
            if (strcasecmp("CHARACTER_SET_RESULTS", set_obj->pSetList->a[i].zName) == 0) {
                is_set_results = TRUE;
                g_string_append_len(con->charset_results, (const char*)value_expr->token.z, value_expr->token.n);
            } else if (strcasecmp("CHARACTER_SET_CLIENT", set_obj->pSetList->a[i].zName) == 0) {
                is_set_client = TRUE;
                g_string_append_len(con->charset_client, (const char*)value_expr->token.z, value_expr->token.n);
            } else if (strcasecmp("CHARACTER_SET_CONNECTION", set_obj->pSetList->a[i].zName) == 0) {
                is_set_connection = TRUE;
                g_string_append_len(con->charset_connection, (const char*)value_expr->token.z, value_expr->token.n);
            }
        }
    }

	network_socket* client = con->client;
	network_socket* server = con->server;
	char cmd = COM_QUERY;
	network_mysqld_con_lua_t* st = con->plugin_con_state;

	if (!is_set_client && !g_string_equal(client->charset_client, server->charset_client)) {
		GString* query = g_string_new_len(&cmd, 1);
		g_string_append(query, "SET CHARACTER_SET_CLIENT=");
		g_string_append(query, client->charset_client->str);
		g_string_assign(con->charset_client, client->charset_client->str);

		injection* inj = injection_new(3, query);
		inj->resultset_is_needed = TRUE;
		g_queue_push_head(st->injected.queries, inj);
	}
	if (!is_set_results && !g_string_equal(client->charset_results, server->charset_results)) {
		GString* query = g_string_new_len(&cmd, 1);
		g_string_append(query, "SET CHARACTER_SET_RESULTS=");
		g_string_append(query, client->charset_results->str);
		g_string_assign(con->charset_results, client->charset_results->str);

		injection* inj = injection_new(4, query);
		inj->resultset_is_needed = TRUE;
		g_queue_push_head(st->injected.queries, inj);
	}
	if (!is_set_connection && !g_string_equal(client->charset_connection, server->charset_connection)) {
		GString* query = g_string_new_len(&cmd, 1);
		g_string_append(query, "SET CHARACTER_SET_CONNECTION=");
		g_string_append(query, client->charset_connection->str);
		g_string_assign(con->charset_connection, client->charset_connection->str);

		injection* inj = injection_new(5, query);
		inj->resultset_is_needed = TRUE;
		g_queue_push_head(st->injected.queries, inj);
	}
}

void check_flags(parse_info_t *parse_info, network_mysqld_con* con) {
    Parse *parse_obj = parse_info->parse_obj;
	con->is_in_select_calc_found_rows = FALSE;
    if (parse_obj == NULL) { return; }

    ParsedResultItem *parsed_item = &parse_obj->parsed.array[0];
    if (parsed_item->sqltype == SQLTYPE_SELECT) {
        Select *select_obj = parsed_item->result.selectObj;
        if (select_obj->pEList) {
            int i;
            for (i = 0; i < select_obj->pEList->nExpr; i++) {
                Expr *expr = select_obj->pEList->a[i].pExpr;
                if (expr->op == TK_FUNCTION && strncasecmp("GET_LOCK", expr->token.z, expr->token.n) == 0) {
                    ExprList *param_list = expr->pList;
                    char key[128] = {0};
                    dup_token2buff(key, sizeof(key), param_list->a[0].pExpr->token);
                    if (!g_hash_table_lookup(con->locks, key)) g_hash_table_add(con->locks, g_strdup(key));
                }
            }
        }
    } else if (parsed_item->sqltype == SQLTYPE_SET) {
        SetStatement *set_obj = parsed_item->result.setObj;
        if (set_obj->pSetList) {
            int i;
            for (i = 0; i < set_obj->pSetList->nExpr; i++) {
                Expr *expr = set_obj->pSetList->a[i].pExpr;
                if (strcasecmp("AUTOCOMMIT", set_obj->pSetList->a[i].zName) == 0) {
                    if (strncmp("0", (const char*)expr->token.z, expr->token.n) == 0) {
                        con->is_not_autocommit = TRUE;
                    } else if (strncmp("1", (const char*)expr->token.z, expr->token.n) == 0) {
                        con->is_not_autocommit = FALSE;
                    }
                }
            }
        }
    }

	/* guint i; */
	/* for (i = 1; i < len; ++i) { */
	/* 	sql_token* token = ts[i]; */
	/* 	if (ts[i]->token_id == TK_SQL_SQL_CALC_FOUND_ROWS) { */
	/* 		con->is_in_select_calc_found_rows = TRUE; */
	/* 		break; */
	/* 	} */
	/* } */
}


gboolean sql_is_write(Parse *parse_obj) {
    if (parse_obj == NULL) { return TRUE; }
    ParsedResultItem *parsed_result = &parse_obj->parsed.array[0];
    
    int sqltype = parsed_result->sqltype;
    switch (sqltype) {
        case SQLTYPE_SELECT:
        case SQLTYPE_SET_NAMES:
        case SQLTYPE_SET_CHARACTER_SET:
        case SQLTYPE_SET:
        case SQLTYPE_SHOW:
            return FALSE;
        default:
            return TRUE;
    }

    return TRUE;
}

static network_socket_retval_t forwarding_sql_to_dbgroup(db_group_t *dbgroup_obj, GString* packets, network_mysqld_con *con, parse_info_t *parse_info) {
    injection_id_t injection_id = parse_info->is_write_sql ? INJECTION_SHARDING_WRITE_SQL : INJECTION_SHARDING_READ_SQL;
    injection* inj = injection_new(injection_id, packets);
    inj->resultset_is_needed = TRUE;

    sharding_dbgroup_context_t* dbgroup_context = sharding_lookup_dbgroup_context(con->sharding_dbgroup_contexts, dbgroup_obj->group_id);
    sharding_reset_st(dbgroup_context->st);
    network_injection_queue_append(dbgroup_context->st->injected.queries, inj);
    //sharding_set_connection_flags(sql_tokens_param->sql_tokens, con);

    network_backend_t* backend = NULL;
    if (dbgroup_context->backend_sock == NULL) {

        if (parse_info->is_write_sql) {
            backend = sharding_get_rw_backend(dbgroup_obj->bs);
        } else {
            backend = sharding_read_write_split(parse_info->parse_obj, dbgroup_obj->bs);
        }

        if (backend == NULL) {
            g_critical("%s: database group:%d has no server backend, closing connection", G_STRLOC, dbgroup_obj->group_id);
            return NETWORK_SOCKET_ERROR;
        }

        dbgroup_context->backend_sock = network_connection_pool_get_socket(con, backend, config->pwd_table[config->pwd_table_index]);
        if (dbgroup_context->backend_sock == NULL) {
            g_critical("%s: database group:%d connect failed, address:'%s'", G_STRLOC, dbgroup_obj->group_id, backend->addr->name->str);
            return NETWORK_SOCKET_ERROR;
        }
        dbgroup_context->st->backend = backend; // store current quering backend
    }
    dbgroup_context->cur_injection_finished = FALSE;
    sharding_querying_dbgroup_context_add(con->sharding_context->querying_dbgroups, dbgroup_context);

    sharding_modify_db(con, dbgroup_context);
    sharding_modify_charset(con, dbgroup_context);
    sharding_modify_user(con, dbgroup_context, config->pwd_table[config->pwd_table_index]);
    sharding_inject_trans_begin_sql(con, dbgroup_context);

    sharding_proxy_send_injections(dbgroup_context);
    sharding_dbgroup_reset_command_response_state(&dbgroup_context->parse);

    return NETWORK_SOCKET_SUCCESS;
}

G_INLINE_FUNC void free_recvd_packets(network_socket* sockobj) {
    GString* packet = NULL;
    while((packet = g_queue_pop_head(sockobj->recv_queue->chunks))) { g_string_free(packet, TRUE); }
}

G_INLINE_FUNC void hit_all_dbgroup(sharding_table_t *sharding_rule, GArray* hit_dbgroups) {
    g_array_set_size(hit_dbgroups, 0);
    GArray *shard_groups = sharding_rule->shard_group;
    guint i;
    for (i = 0; i < shard_groups->len; ++i) {
        guint db_group_index = 0;
        if (sharding_rule->shard_type == SHARDING_TYPE_RANGE) {
            group_range_map_t *range_map = &g_array_index(shard_groups, group_range_map_t, i);
            db_group_index = range_map->db_group_index;
        } else if (sharding_rule->shard_type == SHARDING_TYPE_HASH) {
            group_hash_map_t *hash_map = &g_array_index(shard_groups, group_hash_map_t, i);
            db_group_index = hash_map->db_group_index;
        }
        g_array_append_val(hit_dbgroups, db_group_index);
    }
}

void sharding_proxy_send_error_result(const gchar* errmsg, network_mysqld_con* con, network_socket* recv_sock, GString* packets, guint errorcode, const gchar *sqlstate) {
    g_warning("%s: %s: %s", errmsg, recv_sock->src->name->str, packets->str+1);
    g_string_free(packets, TRUE);
    network_mysqld_con_send_error_full(con->client, errmsg, strlen(errmsg), errorcode, "sqlstate");
    // PROXY_SEND_RESULT
    sharding_proxy_error_send_result(con);
}

static network_socket_retval_t sharding_query_handle(parse_info_t *parse_info, GString* packets, network_mysqld_con *con,
        sharding_table_t *sharding_table_rule)
{
    network_socket_retval_t return_val = NETWORK_SOCKET_SUCCESS;
    network_socket* recv_sock = con->client;
    GArray* hit_dbgroups = NULL;
    trans_context_t* trans_ctx = con->trans_context;
    Parse *parse_obj = parse_info->parse_obj;
    if (trans_ctx->trans_stage != TRANS_STAGE_INIT && trans_ctx->is_default_dbgroup_in_trans) {
        sharding_proxy_send_error_result("Proxy Warning - sharding dbgroup is in trans, transaction will not work across multi dbgroup", con,
                recv_sock, packets, ER_CANT_DO_THIS_DURING_AN_TRANSACTION, "25000");
        goto exit;
    } else if (sharding_is_support_sql(parse_obj) == FALSE) { 
        sharding_proxy_send_error_result("Proxy Warning - Sharing Not Support SQL", con, recv_sock, packets, ER_UNKNOWN_ERROR, "HY000");
        goto exit;
    } 

    hit_dbgroups = g_array_sized_new(FALSE, TRUE, sizeof(guint), config->db_groups->len);
    sharding_result_t sharding_ret = sharding_get_dbgroups(hit_dbgroups, sharding_table_rule, parse_info);
    
    /*"select * from test;" OR "select * from test where noshardkey = 'test';" */
    if (sharding_ret == SHARDING_RET_ALL_SHARD || sharding_ret == SHARDING_RET_ERR_NO_SHARDKEY) {
        hit_all_dbgroup(sharding_table_rule, hit_dbgroups);
        sharding_ret = SHARDING_RET_ALL_SHARD;
    }

    if (sharding_ret == SHARDING_RET_OK || sharding_ret == SHARDING_RET_ALL_SHARD) {
        if (hit_dbgroups->len > 1) { 
            if (sharding_is_contain_multishard_notsupport_feature(parse_obj)) {// multi shard need to check if the sql is support
                sharding_proxy_send_error_result("Proxy Warning - Sharing Hit Multi Dbgroup Not Support SQL", con, recv_sock, packets, ER_UNKNOWN_ERROR, "HY000");
                goto exit;
            } else if ( trans_ctx->trans_stage == TRANS_STAGE_BEFORE_SEND_BEGIN || 
                    (trans_ctx->trans_stage == TRANS_STAGE_IN_TRANS && trans_ctx->in_trans_dbgroup_ctx != NULL)) 
            {
                sharding_proxy_send_error_result("Proxy Warning - sharding dbgroup is in trans, transaction will not work across multi dbgroup", con,
                        recv_sock, packets, ER_CANT_DO_THIS_DURING_AN_TRANSACTION, "25000");
                goto exit;
            }
        } 
        sharding_context_reset(con->sharding_context);
        guint i;
        for (i = 0; i < hit_dbgroups->len; ++i) {
            guint dbgroup_index = g_array_index(hit_dbgroups, guint, i);
            db_group_t *dbgroup_obj = g_ptr_array_index(config->db_groups, dbgroup_index);
            if (dbgroup_obj == NULL) { continue; }

            if (trans_ctx->trans_stage == TRANS_STAGE_IN_TRANS && trans_ctx->in_trans_dbgroup_ctx != NULL && trans_ctx->in_trans_dbgroup_ctx->group_id != dbgroup_obj->group_id) {
                sharding_proxy_send_error_result("Proxy Warning - sharding dbgroup is in trans, transaction will not work across multi dbgroup", con,
                        recv_sock, packets, ER_CANT_DO_THIS_DURING_AN_TRANSACTION, "25000");
                goto exit;
            }

            GString* inj_packet = NULL;
            if (i >= 1) { // the in param packets is use by the first injection
                inj_packet = g_string_new(packets->str);
            } else {
                inj_packet = packets;
            }
            if ((return_val = forwarding_sql_to_dbgroup(dbgroup_obj, inj_packet, con, parse_info)) != NETWORK_SOCKET_SUCCESS) { goto exit; }
        }
        sharding_merge_result_t* merge_result = &con->sharding_context->merge_result;
        merge_result->shard_num = hit_dbgroups->len;
        merge_result->limit = parse_get_sql_limit(parse_obj);

        free_recvd_packets(con->client);
        con->state = CON_STATE_SHARDING_SEND_QUERY;
    } else { // error occured
        const gchar* errmsg = sharding_get_error_msg(sharding_ret);
        sharding_proxy_send_error_result(errmsg, con, recv_sock, packets, ER_UNKNOWN_ERROR, "HY000");
        goto exit;
    }

exit:
    if (hit_dbgroups) { g_array_free(hit_dbgroups, TRUE); }
    return return_val;
}

/**
 * select, insert, update, delete
 */
static gboolean is_crud_sql(Parse *parse_obj) {
    if (parse_obj == NULL) { return FALSE; }
    SqlType sqltype = parse_obj->parsed.array[0].sqltype;
    Select *select_obj = NULL;
    int i;
    switch(sqltype) {
        case SQLTYPE_SELECT:
            /* select_obj = parse_obj->parsed.array[0].result.selectObj; */
            /* for (i = 0; i < select_obj->pEList->nExpr; i++) { */
            /*     Expr *expr = select_obj->pEList->a[i].pExpr; */
            /*     if (expr->op == TK_FUNCTION && strncasecmp(expr->token.z, "DATABASE", expr->token.n) == 0) { */
            /*         return FALSE; */
            /*     } */
            /* } */
        case SQLTYPE_INSERT:
        case SQLTYPE_REPLACE:
        case SQLTYPE_DELETE:
        case SQLTYPE_UPDATE:
            return TRUE;
        defalut:
            return FALSE;
    }
    return FALSE;
}

static network_mysqld_lua_stmt_ret nosharding_query_handle(parse_info_t *parse_info, GString* packets, network_mysqld_con_lua_t *st, network_mysqld_con *con) {
	network_mysqld_lua_stmt_ret ret;
	network_socket *send_sock = NULL;
	char type = packets->str[0];
    Parse *parse_obj = parse_info->parse_obj;
	int i;
    gboolean is_crud = is_crud_sql(parse_info->parse_obj);
    if (con->trans_context->trans_stage != TRANS_STAGE_INIT && con->trans_context->in_trans_dbgroup_ctx != NULL && is_crud) {
        g_warning("%s: sharding dbgroup is in trans, transaction will not work across multi dbgroup, %s: %s", G_STRLOC, con->client->src->name->str, packets->str+1);  
        g_string_free(packets, TRUE);
        network_mysqld_con_send_error_full(con->client, C("Proxy Warning - sharding dbgroup is in trans, transaction will not work across multi dbgroup"), 
                ER_CANT_DO_THIS_DURING_AN_TRANSACTION, "25000");
        return PROXY_SEND_RESULT;
    }

	GPtrArray* sqls = NULL;
    // sub-table is replace with sharding, it is deprecated.
	/* if (type == COM_QUERY && config->tables && parse_obj != NULL) { */
        /* char *errmsg = NULL; */
	/* 	sqls = sql_parse(con, parse_info, &errmsg);  */

        /* if (errmsg != NULL) { */
            /* g_warning("%s: %s :%s", G_STRLOC, con->client->src->name->str, errmsg); */  
            /* g_string_free(packets, TRUE); */
            /* network_mysqld_con_send_error_full(con->client, errmsg, strlen(errmsg), ER_UNKNOWN_ERROR, "HY000"); */
            /* g_free(errmsg); */
            /* return PROXY_SEND_RESULT; */
        /* } */
	/* } */

	gboolean is_write = sql_is_write(parse_obj);

	ret = PROXY_SEND_INJECTION;
	injection* inj = NULL;
	if (sqls == NULL) {
		inj = injection_new(1, packets);
		inj->resultset_is_needed = is_write;
		g_queue_push_tail(st->injected.queries, inj);
	} else {
		g_string_free(packets, TRUE);

		if (sqls->len == 1) {
			inj = injection_new(1, sqls->pdata[0]);
			inj->resultset_is_needed = is_write;
			g_queue_push_tail(st->injected.queries, inj);
		} else {
			merge_res_t* merge_res = con->merge_res;

			merge_res->sub_sql_num = sqls->len;
			merge_res->sub_sql_exed = 0;
			merge_res->limit = G_MAXINT;
			merge_res->affected_rows = 0;
			merge_res->warnings = 0;
            merge_res->limit = parse_get_sql_limit(parse_obj);

			// 释放以前的结果
			GPtrArray* rows = merge_res->rows;
			for (i = 0; i < rows->len; ++i) {
				GPtrArray* row = g_ptr_array_index(rows, i);
				guint j;
				for (j = 0; j < row->len; ++j) {
					g_free(g_ptr_array_index(row, j));
				}
				g_ptr_array_free(row, TRUE);
			}
			g_ptr_array_set_size(rows, 0);

			int id = is_write ? 8 : 7;
			for (i = 0; i < sqls->len; ++i) {
				inj = injection_new(id, sqls->pdata[i]);
				inj->resultset_is_needed = TRUE;
				g_queue_push_tail(st->injected.queries, inj);
			}
		}

		g_ptr_array_free(sqls, TRUE);
	}

	check_flags(parse_info, con);

	if (con->server == NULL) {
		int backend_ndx = -1;

		if (!con->is_in_transaction && !con->is_not_autocommit && g_hash_table_size(con->locks) == 0) {
			if (type == COM_QUERY) {
				if (is_write) {
					backend_ndx = idle_rw(con);
				} else {
					backend_ndx = rw_split(parse_obj, con);
				}
				send_sock = network_connection_pool_lua_swap(con, backend_ndx, config->pwd_table[config->pwd_table_index]);
			} else if (type == COM_INIT_DB || type == COM_SET_OPTION || type == COM_FIELD_LIST) {
				backend_ndx = wrr_ro(con);
				send_sock = network_connection_pool_lua_swap(con, backend_ndx, config->pwd_table[config->pwd_table_index]);
			}
		}

		if (send_sock == NULL) {
			backend_ndx = idle_rw(con);
			send_sock = network_connection_pool_lua_swap(con, backend_ndx, config->pwd_table[config->pwd_table_index]);
		}
		con->server = send_sock;
	}

	modify_db(con);
	modify_charset(parse_info, con);
	modify_user(con);

    inject_trans_begin_sql(con, is_crud);
	return ret;
}

static network_socket_retval_t begin_trans_ctrl_query_handle(GString* packets, network_mysqld_con *con, gboolean* is_return) {
    network_socket_retval_t ret = NETWORK_SOCKET_SUCCESS;
    trans_context_t* trans_ctx = con->trans_context;
    if (trans_ctx->trans_stage == TRANS_STAGE_INIT) {
        trans_ctx->origin_begin_sql = packets;
        trans_ctx->trans_stage = TRANS_STAGE_BEFORE_SEND_BEGIN;
    } else { // already in trans
        g_string_free(packets, TRUE);
    }
    network_mysqld_con_send_ok(con->client);
    free_recvd_packets(con->client);

    con->state = CON_STATE_SEND_QUERY_RESULT;
    *is_return = TRUE;
    return ret;
}

static network_socket_retval_t forwarding_trans_sql_to_dbgroup(Parse *parse_obj, GString* packets, network_mysqld_con* con) {
    trans_context_t* trans_ctx = con->trans_context;
    sharding_dbgroup_context_t* dbgroup_ctx = trans_ctx->in_trans_dbgroup_ctx;
    if (dbgroup_ctx == NULL || dbgroup_ctx->backend_sock == NULL) { 
        g_critical("%s: in transaction stage, but when proxy send %s, the dbgroup_ctx or dbgroup_ctx->backend_sock is NULL", G_STRLOC, packets->str + 1);
        g_string_free(packets, TRUE);
        return NETWORK_SOCKET_ERROR;
    }

    injection* inj = injection_new(INJECTION_TRANS_FINISH_SQL, packets);
    inj->resultset_is_needed = TRUE;

    sharding_reset_st(dbgroup_ctx->st);
    network_injection_queue_append(dbgroup_ctx->st->injected.queries, inj);
    //sharding_set_connection_flags(sql_tokens_param->sql_tokens, con);

    dbgroup_ctx->cur_injection_finished = FALSE;
    sharding_querying_dbgroup_context_add(con->sharding_context->querying_dbgroups, dbgroup_ctx);
    sharding_proxy_send_injections(dbgroup_ctx);
    sharding_dbgroup_reset_command_response_state(&dbgroup_ctx->parse);
    con->sharding_context->merge_result.shard_num = 1;

    free_recvd_packets(con->client);
    con->state = CON_STATE_SHARDING_SEND_QUERY;
    return NETWORK_SOCKET_SUCCESS;
}

static network_socket_retval_t finish_trans_ctrl_query_handle(Parse *parse_obj, GString* packets, 
        network_mysqld_con *con, gboolean* is_return) 
{
    trans_context_t* trans_ctx = con->trans_context;
    if (trans_ctx->trans_stage == TRANS_STAGE_IN_TRANS && trans_ctx->is_default_dbgroup_in_trans) {
        sharding_reset_trans_context_t(trans_ctx);
        *is_return = FALSE; // use nonsharding_query_handle to send it
    } else if (trans_ctx->trans_stage == TRANS_STAGE_IN_TRANS && trans_ctx->in_trans_dbgroup_ctx) {
        *is_return = TRUE;
        return forwarding_trans_sql_to_dbgroup(parse_obj, packets, con);
    } else { // no backend in trans
        network_mysqld_con_send_ok(con->client);
        free_recvd_packets(con->client);
        g_string_free(packets, TRUE);
        con->state = CON_STATE_SEND_QUERY_RESULT;
        sharding_reset_trans_context_t(trans_ctx);
        *is_return = TRUE;
    }
    return NETWORK_SOCKET_SUCCESS;
}

static network_socket_retval_t trans_ctrl_query_handle(Parse *parse_obj, GString* packets, network_mysqld_con *con, gboolean* is_return) 
{
    ParsedResultItem *parsed_result = &parse_obj->parsed.array[0];
    g_assert(is_return != NULL);
    *is_return = FALSE; // set to TRUE means need to return immediatelly when exit trans_ctrl_query_handle
    switch(parsed_result->sqltype) {
        case SQLTYPE_TRANSACTION_BEGIN:
        case SQLTYPE_TRANSACTION_START:
            return begin_trans_ctrl_query_handle(packets, con, is_return);
        case SQLTYPE_TRANSACTION_COMMIT:
        case SQLTYPE_TRANSACTION_ROLLBACK:
            return finish_trans_ctrl_query_handle(parse_obj, packets, con, is_return);
        default: // never come here
            g_critical("parsed_result->sqltype:%d unexpected!", parsed_result->sqltype);
            *is_return = TRUE;
            return NETWORK_SOCKET_SUCCESS;
    }
}


static network_mysqld_lua_stmt_ret error_response(const gchar *errmsg, GString *packets, network_mysqld_con *con) {
    g_warning(errmsg);
    g_string_free(packets, TRUE);
    network_mysqld_con_send_error_full(con->client, errmsg, strlen(errmsg), ER_UNKNOWN_ERROR, "HY000");
    return PROXY_SEND_RESULT;
} 

/* static gboolean is_forbidden_sql(parse_info_t *parse_info) { */
/*     Parse *parse_obj = parse_info->parse_obj; */
/*     if (parse_info->is_parse_error == FALSE) { */
/*         if (parse_obj->parsed.curSize > 0) { */
/*             SqlType sqltype = parse_obj->parsed.array[0].sqltype; */
/*             if (sqltype == SQLTYPE_UPDATE) { */
/*                 if ( !parse_obj->parsed.array[0].result.updateObj->pWhere) { */
/*                     return TRUE; */
/*                 } */
/*             } else if (sqltype == TK_SQL_DELETE) { */
/*                 if (!parse_obj->parsed.array[0].result.deleteObj->pWhere) { */
/*                     return TRUE; */
/*                 } */
/*             } else if (sqltype == TK_SQL_SELECT) { */
/*                 Select *selectObj = parse_obj->parsed.array[0].result.selectObj; */
/*                 int i; */
/*                 for (i = 0; i < selectObj->pEList->nExpr; i++) { */
/*                     Expr *expr = selectObj->pEList->a[i].pExpr; */
/*                     if (expr->op == TK_FUNCTION && strncasecmp(expr->token.z, "SLEEP", expr->token.n) == 0) { */
/*                         return TRUE; */
/*                     } */
/*                 } */
/*             } */
/*         } */     
/*     } else { */
/*         int i = 0; */
/*         TokenArray *tokens_array = &parse_obj->tokens; */
/*         while (i < tokens_array->curSize ) { */
/*             int token_type = tokens_array->array[i].tokenType; */
/*             if (token_type == TK_COMMENT || tokentype == TK_LP /1* TK_LP is "(" *1/) { */ 
/*                 i++; */ 
/*             } else { */ 
/*                 break; */ 
/*             } */
/*         } */
/*         int token_type = tokens_array->array[i].tokenType; */
/*         if (token_type == TK_DELETE || token_type == TK_UPDATE) { */
/*             for (i < tokens_array->curSize; i++) { */
/*                 token_type = tokens_array->array[i].tokenType; */
/*                 if (token_type == TK_WHERE) { break; } */
/*             } */
/*             if (i == tokens_array->curSize) { return TRUE; } */
/*         } else if (token_type == TK_SELECT) { */
/*             for (i < tokens_array->curSize; i++) { */
/*                 token_type = tokens_array->array[i].tokenType; */
/*                 if (token_type == TK_LP && i-1 > 0 && tokens_array->array[i-1].tokenType == TK_ID) { */
/*                     Token *token = &tokens_array->array[i-1].token; */
/*                     if (strncasecmp(token->z, "SLEEP", token->n) == 0) { return TRUE; } */
/*                 } */
/*             } */
/*         } */
/*     } */

/*     return FALSE; */
/* } */

static gboolean is_forbidden_sql(Parse *parse_obj) {
    if (parse_obj->parsed.curSize > 0) {
        SqlType sqltype = parse_obj->parsed.array[0].sqltype;
        if (sqltype == SQLTYPE_UPDATE) {
            if ( !parse_obj->parsed.array[0].result.updateObj->pWhere) {
                return TRUE;
            }
        } else if (sqltype == SQLTYPE_DELETE) {
            if (!parse_obj->parsed.array[0].result.deleteObj->pWhere) {
                return TRUE;
            }
        } else if (sqltype == SQLTYPE_SELECT) {
            Select *selectObj = parse_obj->parsed.array[0].result.selectObj;
            int i;
            for (i = 0; i < selectObj->pEList->nExpr; i++) {
                Expr *expr = selectObj->pEList->a[i].pExpr;
                if (expr->op == TK_FUNCTION && strncasecmp(expr->token.z, "SLEEP", expr->token.n) == 0) {
                    return TRUE;
                }
            }
        } else if (sqltype == SQLTYPE_CREATE_TABLE || sqltype == SQLTYPE_DROP_TABLE) {
            return TRUE;
        }  
    }     
    return FALSE;
}

static gboolean is_sharding_query(parse_info_t *parse_info, network_mysqld_con *con, sharding_table_t **sharding_rule) {
    g_assert(sharding_rule != NULL);
    Parse *parse_obj = parse_info->parse_obj;
    
    *sharding_rule = sharding_lookup_table_shard_rule(config->table_shard_rules, con->client->default_db->str, parse_info);
    return (*sharding_rule != NULL);
}

/**
 * gets called after a query has been read
 *
 * - calls the lua script via network_mysqld_con_handle_proxy_stmt()
 *
 * @see network_mysqld_con_handle_proxy_stmt
 */
NETWORK_MYSQLD_PLUGIN_PROTO(proxy_read_query) {
	GString *packet;
	network_socket *recv_sock, *send_sock;
	network_mysqld_con_lua_t *st = con->plugin_con_state;
	int proxy_query = 1;
	network_mysqld_lua_stmt_ret ret;

	NETWORK_MYSQLD_CON_TRACK_TIME(con, "proxy::ready_query::enter");

	send_sock = NULL;
	recv_sock = con->client;
	st->injected.sent_resultset = 0;

	NETWORK_MYSQLD_CON_TRACK_TIME(con, "proxy::ready_query::enter_lua");
	network_injection_queue_reset(st->injected.queries);

	GString* packets = g_string_new(NULL);
	int i;
	for (i = 0; NULL != (packet = g_queue_peek_nth(recv_sock->recv_queue->chunks, i)); i++) {
		g_string_append_len(packets, packet->str + NET_HEADER_SIZE, packet->len - NET_HEADER_SIZE);
	}

	char type = packets->str[0];
	if (type == COM_QUIT || type == COM_PING) {
		g_string_free(packets, TRUE);
		network_mysqld_con_send_ok_full(con->client, 0, 0, 0x0002, 0);
		ret = PROXY_SEND_RESULT;
	} else {
        parse_info_t parse_info;
        if (type == COM_QUERY ) {
            if (packets->len <= 1) {
                ret = error_response("Proxy Warning - Empty COM_QUERY Packet!", packets, con);
                goto network_handle;           
            }
            char *parse_errmsg = NULL;
            Parse *parse_obj = chassis_thread_lemon_parse_obj(con->srv);
            sqlite3ParseReset(parse_obj);
            sqlite3RunParser1(parse_obj, packets->str+1, packets->len-1, &parse_errmsg);

            if (parse_errmsg != NULL) {
                g_warning("%s: sql error: %s, origin sql: %.*s", G_STRLOC, parse_errmsg, packets->len-1, packets->str+1);
                gchar *errmsg_string = g_strdup_printf("Proxy Warning - %s", parse_errmsg);
                ret = error_response(errmsg_string, packets, con);
                g_free(errmsg_string);
                sqliteFree(parse_errmsg);
                sqlite3ParseClean(parse_obj);
                goto network_handle;
            } else if (parse_obj->parsed.curSize == 0) {
                g_warning("%s: parsed.curSize == 0, origin sql: %.*s", G_STRLOC, packets->len-1, packets->str+1);
                ret = error_response("Proxy Warning - sql parse unknown error, Please communicate with us.", packets, con);
                sqlite3ParseClean(parse_obj);
                goto network_handle;
            }

            if (is_forbidden_sql(parse_obj)) {
                ret = error_response("Proxy Warning - Syntax Forbidden!", packets, con);
                sqlite3ParseClean(parse_obj);
                goto network_handle;           
            }
            
            parse_info_init(&parse_info, parse_obj, (const char*)packets->str+1, packets->len-1);
            if (g_hash_table_size(config->table_shard_rules) != 0) {
                if (IS_TRANSACTION_CTRL_SQL(parse_obj->parsed.array[0].sqltype)) {
                    gboolean is_return = FALSE;
                    network_socket_retval_t retval = trans_ctrl_query_handle(parse_obj, packets, con, &is_return);
                    if (is_return) { 
                        sqlite3ParseClean(parse_obj);
                        return retval;
                    }
                } else {
                    sharding_table_t *sharding_rule = NULL;
                    if (is_sharding_query(&parse_info, con, &sharding_rule)) {
                        return sharding_query_handle(&parse_info, packets, con, sharding_rule);
                    }
                }
            } 
            ret = nosharding_query_handle(&parse_info, packets, st, con);
            sqlite3ParseClean(parse_obj);
        } else { // is not COM_QUERY
            parse_info_init(&parse_info, NULL, NULL, 0);
            ret = nosharding_query_handle(&parse_info, packets, st, con);
        }      
	}
	NETWORK_MYSQLD_CON_TRACK_TIME(con, "proxy::ready_query::leave_lua");

network_handle:
	/**
	 * if we disconnected in read_query_result() we have no connection open
	 * when we try to execute the next query
	 *
	 * for PROXY_SEND_RESULT we don't need a server
	 */
	if (ret != PROXY_SEND_RESULT &&
	    con->server == NULL) {
		g_critical("%s.%d: I have no server backend, closing connection", __FILE__, __LINE__);
		return NETWORK_SOCKET_ERROR;
	}

	switch (ret) {
	case PROXY_NO_DECISION:
	case PROXY_SEND_QUERY:
		send_sock = con->server;

		/* no injection, pass on the chunks as is */
		while ((packet = g_queue_pop_head(recv_sock->recv_queue->chunks))) {
			network_mysqld_queue_append_raw(send_sock, send_sock->send_queue, packet);
		}
		con->resultset_is_needed = FALSE; /* we don't want to buffer the result-set */

		break;
	case PROXY_SEND_RESULT: {
		gboolean is_first_packet = TRUE;
		proxy_query = 0;

		send_sock = con->client;

		/* flush the recv-queue and track the command-states */
		while ((packet = g_queue_pop_head(recv_sock->recv_queue->chunks))) {
			if (is_first_packet) {
				network_packet p;

				p.data = packet;
				p.offset = 0;

				network_mysqld_con_reset_command_response_state(con);

				if (0 != network_mysqld_con_command_states_init(con, &p)) {
					g_debug("%s: ", G_STRLOC);
				}

				is_first_packet = FALSE;
			}

			g_string_free(packet, TRUE);
		}

		break; }
	case PROXY_SEND_INJECTION: {
		injection *inj;

		inj = g_queue_peek_head(st->injected.queries);
		con->resultset_is_needed = inj->resultset_is_needed; /* let the lua-layer decide if we want to buffer the result or not */

		send_sock = con->server;

		network_mysqld_queue_reset(send_sock);
		network_mysqld_queue_append(send_sock, send_sock->send_queue, S(inj->query));

		while ((packet = g_queue_pop_head(recv_sock->recv_queue->chunks))) g_string_free(packet, TRUE);

		break; }
	default:
		g_error("%s.%d: ", __FILE__, __LINE__);
	}

	if (proxy_query) {
		con->state = CON_STATE_SEND_QUERY;
	} else {
		GList *cur;

		/* if we don't send the query to the backend, it won't be tracked. So track it here instead
		 * to get the packet tracking right (LOAD DATA LOCAL INFILE, ...) */

		for (cur = send_sock->send_queue->chunks->head; cur; cur = cur->next) {
			network_packet p;
			int r;

			p.data = cur->data;
			p.offset = 0;

			r = network_mysqld_proto_get_query_result(&p, con);
		}

		con->state = CON_STATE_SEND_QUERY_RESULT;
		con->resultset_is_finished = TRUE; /* we don't have more too send */
	}
	NETWORK_MYSQLD_CON_TRACK_TIME(con, "proxy::ready_query::done");

	return NETWORK_SOCKET_SUCCESS;
}

/**
 * decide about the next state after the result-set has been written
 * to the client
 *
 * if we still have data in the queue, back to proxy_send_query()
 * otherwise back to proxy_read_query() to pick up a new client query
 *
 * @note we should only send one result back to the client
 */
NETWORK_MYSQLD_PLUGIN_PROTO(proxy_send_query_result) {
	network_socket *recv_sock, *send_sock;
	injection *inj;
	network_mysqld_con_lua_t *st = con->plugin_con_state;

	send_sock = con->server;
	recv_sock = con->client;

	if (st->connection_close) {
		con->state = CON_STATE_ERROR;

		return NETWORK_SOCKET_SUCCESS;
	}

	if (con->parse.command == COM_BINLOG_DUMP) {
		/**
		 * the binlog dump is different as it doesn't have END packet
		 *
		 * @todo in 5.0.x a NON_BLOCKING option as added which sends a EOF
		 */
		con->state = CON_STATE_READ_QUERY_RESULT;

		return NETWORK_SOCKET_SUCCESS;
	}

	/* if we don't have a backend, don't try to forward queries
	 */
	if (!send_sock) {
		network_injection_queue_reset(st->injected.queries);
	}

	if (st->injected.queries->length == 0) {
		/* we have nothing more to send, let's see what the next state is */

		con->state = CON_STATE_READ_QUERY;

		return NETWORK_SOCKET_SUCCESS;
	}

	/* looks like we still have queries in the queue,
	 * push the next one
	 */
	inj = g_queue_peek_head(st->injected.queries);
	con->resultset_is_needed = inj->resultset_is_needed;

	if (!inj->resultset_is_needed && st->injected.sent_resultset > 0) {
		/* we already sent a resultset to the client and the next query wants to forward it's result-set too, that can't work */
		g_critical("%s: proxy.queries:append() in %s can only have one injected query without { resultset_is_needed = true } set. We close the client connection now.",
				G_STRLOC,
				config->lua_script);

		return NETWORK_SOCKET_ERROR;
	}

	g_assert(inj);
	g_assert(send_sock);

	network_mysqld_queue_reset(send_sock);
	network_mysqld_queue_append(send_sock, send_sock->send_queue, S(inj->query));

	network_mysqld_con_reset_command_response_state(con);

	con->state = CON_STATE_SEND_QUERY;

	return NETWORK_SOCKET_SUCCESS;
}

void merge_rows(network_mysqld_con* con, injection* inj) {
	if (!inj->resultset_is_needed || !con->server->recv_queue->chunks || inj->qstat.binary_encoded) return;

	proxy_resultset_t* res = proxy_resultset_new();

	res->result_queue = con->server->recv_queue->chunks;
	res->qstat = inj->qstat;
	res->rows  = inj->rows;
	res->bytes = inj->bytes;

	parse_resultset_fields(res);

	GList* res_row = res->rows_chunk_head;
	while (res_row) {
		network_packet packet;
		packet.data = res_row->data;
		packet.offset = 0;

		network_mysqld_proto_skip_network_header(&packet);
		network_mysqld_lenenc_type lenenc_type;
		network_mysqld_proto_peek_lenenc_type(&packet, &lenenc_type);

		switch (lenenc_type) {
			case NETWORK_MYSQLD_LENENC_TYPE_ERR:
			case NETWORK_MYSQLD_LENENC_TYPE_EOF:
				proxy_resultset_free(res);
				return;

			case NETWORK_MYSQLD_LENENC_TYPE_INT:
			case NETWORK_MYSQLD_LENENC_TYPE_NULL:
				break;
		}

		GPtrArray* row = g_ptr_array_new();

		guint len = res->fields->len;
		guint i;
		for (i = 0; i < len; i++) {
			guint64 field_len;

			network_mysqld_proto_peek_lenenc_type(&packet, &lenenc_type);

			switch (lenenc_type) {
				case NETWORK_MYSQLD_LENENC_TYPE_NULL:
                    g_ptr_array_add(row, NULL);
					network_mysqld_proto_skip(&packet, 1);
					break;

				case NETWORK_MYSQLD_LENENC_TYPE_INT:
					network_mysqld_proto_get_lenenc_int(&packet, &field_len);
					g_ptr_array_add(row, g_strndup(packet.data->str + packet.offset, field_len));
					network_mysqld_proto_skip(&packet, field_len);
					break;

				default:
					break;
			}
		}

		g_ptr_array_add(con->merge_res->rows, row);
		if (con->merge_res->rows->len >= con->merge_res->limit) break;
		res_row = res_row->next;
	}

	proxy_resultset_free(res);
}

void sharding_log_sql(network_socket* client_sock, network_socket* server_sock, injection* inj) {
    if (sql_log_type == OFF) return;

    float latency_ms = (inj->ts_read_query_result_last - inj->ts_read_query)/1000.0;
    if ((gint)latency_ms < config->sql_log_slow_ms) return;

    GString* message = g_string_new(NULL);

    time_t t = time(NULL);
    struct tm* tm = localtime(&t);
    g_string_printf(message, "[%02d/%02d/%d %02d:%02d:%02d] C:%s S:", tm->tm_mon+1, tm->tm_mday, tm->tm_year+1900, tm->tm_hour, tm->tm_min, tm->tm_sec, client_sock->src->name->str);

    if (inj->qstat.query_status == MYSQLD_PACKET_OK) {
        g_string_append_printf(message, "%s OK %.3f \"%s\"\n", server_sock->dst->name->str, latency_ms, inj->query->str+1);
    } else {
        g_string_append_printf(message, "%s ERR %.3f \"%s\"\n", server_sock->dst->name->str, latency_ms, inj->query->str+1);
    }

    fwrite(message->str, message->len, 1, config->sql_log);
    g_string_free(message, TRUE);

    if (sql_log_type == REALTIME) fflush(config->sql_log);

}

void log_sql(network_mysqld_con* con, injection* inj) {
	if (sql_log_type == OFF) return;

	float latency_ms = (inj->ts_read_query_result_last - inj->ts_read_query)/1000.0;
	if ((gint)latency_ms < config->sql_log_slow_ms) return;

	GString* message = g_string_new(NULL);

	time_t t = time(NULL);
	struct tm* tm = localtime(&t);
	g_string_printf(message, "[%02d/%02d/%d %02d:%02d:%02d] C:%s S:", tm->tm_mon+1, tm->tm_mday, tm->tm_year+1900, tm->tm_hour, tm->tm_min, tm->tm_sec, con->client->src->name->str);

	if (inj->qstat.query_status == MYSQLD_PACKET_OK) {
		g_string_append_printf(message, "%s OK %.3f \"%s\"\n", con->server->dst->name->str, latency_ms, inj->query->str+1);
	} else {
		g_string_append_printf(message, "%s ERR %.3f \"%s\"\n", con->server->dst->name->str, latency_ms, inj->query->str+1);
	}

	fwrite(message->str, message->len, 1, config->sql_log);
	g_string_free(message, TRUE);

	if (sql_log_type == REALTIME) fflush(config->sql_log);
}

/**
 * handle the query-result we received from the server
 *
 * - decode the result-set to track if we are finished already
 * - handles BUG#25371 if requested
 * - if the packet is finished, calls the network_mysqld_con_handle_proxy_resultset
 *   to handle the resultset in the lua-scripts
 *
 * @see network_mysqld_con_handle_proxy_resultset
 */
NETWORK_MYSQLD_PLUGIN_PROTO(proxy_read_query_result) {
	int is_finished = 0;
	network_packet packet;
	network_socket *recv_sock, *send_sock;
	network_mysqld_con_lua_t *st = con->plugin_con_state;
	injection *inj = NULL;

	NETWORK_MYSQLD_CON_TRACK_TIME(con, "proxy::ready_query_result::enter");

	recv_sock = con->server;
	send_sock = con->client;

	/* check if the last packet is valid */
	packet.data = g_queue_peek_tail(recv_sock->recv_queue->chunks);
	packet.offset = 0;

	if (0 != st->injected.queries->length) {
		inj = g_queue_peek_head(st->injected.queries);
	}

	if (inj && inj->ts_read_query_result_first == 0) {
		/**
		 * log the time of the first received packet
		 */
		inj->ts_read_query_result_first = chassis_get_rel_microseconds();
		/* g_get_current_time(&(inj->ts_read_query_result_first)); */
	}

	// FIX
	//if(inj) {
	//	g_string_assign_len(con->current_query, inj->query->str, inj->query->len);
	//}

	is_finished = network_mysqld_proto_get_query_result(&packet, con);
	if (is_finished == -1) return NETWORK_SOCKET_ERROR; /* something happend, let's get out of here */

	con->resultset_is_finished = is_finished;

	/* copy the packet over to the send-queue if we don't need it */
	if (!con->resultset_is_needed) { // 不需要缓存结果集, 直接转发给client
		network_mysqld_queue_append_raw(send_sock, send_sock->send_queue, g_queue_pop_tail(recv_sock->recv_queue->chunks));
	}

	if (is_finished) {
		network_mysqld_lua_stmt_ret ret;

		/**
		 * the resultset handler might decide to trash the send-queue
		 *
		 * */
		if (inj) {
			if (con->parse.command == COM_QUERY || con->parse.command == COM_STMT_EXECUTE) {
				network_mysqld_com_query_result_t *com_query = con->parse.data;

				inj->bytes = com_query->bytes;
				inj->rows  = com_query->rows;
				inj->qstat.was_resultset = com_query->was_resultset;
				inj->qstat.binary_encoded = com_query->binary_encoded;

				/* INSERTs have a affected_rows */
				if (!com_query->was_resultset) {
					inj->qstat.affected_rows = com_query->affected_rows;
					inj->qstat.insert_id     = com_query->insert_id;
				}
				inj->qstat.server_status = com_query->server_status;
				inj->qstat.warning_count = com_query->warning_count;
				inj->qstat.query_status  = com_query->query_status;
			}
			inj->ts_read_query_result_last = chassis_get_rel_microseconds();
			/* g_get_current_time(&(inj->ts_read_query_result_last)); */
		}

		network_mysqld_queue_reset(recv_sock); /* reset the packet-id checks as the server-side is finished */

		NETWORK_MYSQLD_CON_TRACK_TIME(con, "proxy::ready_query_result::enter_lua");
		GString* p;
		if (0 != st->injected.queries->length) {
			inj = g_queue_pop_head(st->injected.queries);
			char* str = inj->query->str + 1;
			if (inj->id == 1) {
				if (*(str-1) == COM_QUERY) log_sql(con, inj);
				ret = PROXY_SEND_RESULT;
			} else if (inj->id == 7) { // not a write sql
				log_sql(con, inj);

				merge_res_t* merge_res = con->merge_res;
				if (inj->qstat.query_status == MYSQLD_PACKET_OK && merge_res->rows->len < merge_res->limit) merge_rows(con, inj);

				if ((++merge_res->sub_sql_exed) < merge_res->sub_sql_num) {
					ret = PROXY_IGNORE_RESULT;
				} else {
					network_injection_queue_reset(st->injected.queries);
					ret = PROXY_SEND_RESULT;

					if (inj->qstat.query_status == MYSQLD_PACKET_OK) {
						proxy_resultset_t* res = proxy_resultset_new();

						if (inj->resultset_is_needed && !inj->qstat.binary_encoded) res->result_queue = con->server->recv_queue->chunks;
						res->qstat = inj->qstat;
						res->rows  = inj->rows;
						res->bytes = inj->bytes;
						parse_resultset_fields(res);

						while ((p = g_queue_pop_head(recv_sock->recv_queue->chunks))) g_string_free(p, TRUE);
						network_mysqld_con_send_resultset(send_sock, res->fields, merge_res->rows);

						proxy_resultset_free(res);
					}
				}
			} else if (inj->id == 8) { // write sql
				log_sql(con, inj);

				if (inj->qstat.query_status == MYSQLD_PACKET_OK) {
					network_mysqld_ok_packet_t *ok_packet = network_mysqld_ok_packet_new();
					packet.offset = NET_HEADER_SIZE;
					if (network_mysqld_proto_get_ok_packet(&packet, ok_packet)) {
						network_mysqld_ok_packet_free(ok_packet);
						injection_free(inj);
						return NETWORK_SOCKET_ERROR;
					}

					merge_res_t *merge_res = con->merge_res;
					merge_res->affected_rows += ok_packet->affected_rows;
					merge_res->warnings += ok_packet->warnings;

					if ((++merge_res->sub_sql_exed) < merge_res->sub_sql_num) {
						ret = PROXY_IGNORE_RESULT;
					} else {
						network_mysqld_con_send_ok_full(con->client, merge_res->affected_rows, 0, ok_packet->server_status, merge_res->warnings);
						network_injection_queue_reset(st->injected.queries);
						while ((p = g_queue_pop_head(recv_sock->recv_queue->chunks))) g_string_free(p, TRUE);
						ret = PROXY_SEND_RESULT;
					}

					network_mysqld_ok_packet_free(ok_packet);
				} else {
					ret = PROXY_SEND_RESULT;
				}
            } else if (inj->id == INJECTION_TRANS_BEGIN_SQL) {
                if (inj->qstat.query_status != MYSQLD_PACKET_OK) { // begin error 
                    ret = PROXY_SEND_RESULT;
                    sharding_reset_trans_context_t(con->trans_context);
                    network_injection_queue_reset(st->injected.queries);
                } else {
                    ret = PROXY_IGNORE_RESULT;
                }
            } else {
				ret = PROXY_IGNORE_RESULT;

				if (inj->id == 6) {
					if (con->server->response) {
						/* in case we got the connection from the pool it has the response from the previous auth */
						network_mysqld_auth_response_free(con->server->response);
						con->server->response = NULL;
					}
					con->server->response = network_mysqld_auth_response_copy(con->client->response);
				}
			}

			switch (ret) {
			case PROXY_SEND_RESULT:
				if (!con->is_in_transaction || (inj->qstat.server_status & SERVER_STATUS_IN_TRANS)) {
					con->is_in_transaction = (inj->qstat.server_status & SERVER_STATUS_IN_TRANS);
                    if (con->is_in_transaction) {
                        con->trans_context->is_default_dbgroup_in_trans = TRUE;
                        con->trans_context->trans_stage = TRANS_STAGE_IN_TRANS;
                    }
				} else {
					if (strcasestr(str, "COMMIT") == str || strcasestr(str, "ROLLBACK") == str) con->is_in_transaction = FALSE;
				}

				if (g_hash_table_size(con->locks) > 0 && strcasestr(str, "SELECT RELEASE_LOCK") == str) {
					gchar* b = strchr(str+strlen("SELECT RELEASE_LOCK"), '(') + 1;
					if (b) {
						while (*b == ' ') ++b;
						gchar* e = NULL;
						if (*b == '\'') {
							++b;
							e = strchr(b, '\'');
						} else if (*b == '\"') {
							++b;
							e = strchr(b+1, '\"');
						}
						if (e) {
							gchar* key = g_strndup(b, e-b);
							g_hash_table_remove(con->locks, key);
							g_free(key);
						}
					}
				}

				gboolean have_last_insert_id = inj->qstat.insert_id > 0;

				++st->injected.sent_resultset;
				if (st->injected.sent_resultset == 1) {
					while ((p = g_queue_pop_head(recv_sock->recv_queue->chunks))) network_mysqld_queue_append_raw(send_sock, send_sock->send_queue, p);
				} else { // ??
					if (con->resultset_is_needed) {
						while ((p = g_queue_pop_head(recv_sock->recv_queue->chunks))) g_string_free(p, TRUE);
					}
				}

				if (!con->is_in_transaction && !con->is_not_autocommit && !con->is_in_select_calc_found_rows &&
						!have_last_insert_id && g_hash_table_size(con->locks) == 0)
					network_connection_pool_lua_add_connection(con);

				break;
			case PROXY_IGNORE_RESULT:
				if (con->resultset_is_needed) { // 不需要结果集的情况已经把recv_sock->recv_queue->chunks放入client的发送队列了
					while ((p = g_queue_pop_head(recv_sock->recv_queue->chunks))) g_string_free(p, TRUE);
				}

			default:
				break;
			}

			injection_free(inj);
		}

		NETWORK_MYSQLD_CON_TRACK_TIME(con, "proxy::ready_query_result::leave_lua");

		if (PROXY_IGNORE_RESULT != ret) {
			/* reset the packet-id checks, if we sent something to the client */
			network_mysqld_queue_reset(send_sock);
		}

		/**
		 * if the send-queue is empty, we have nothing to send
		 * and can read the next query */
		if (send_sock->send_queue->chunks) {
			con->state = CON_STATE_SEND_QUERY_RESULT;
		} else {
			g_assert_cmpint(con->resultset_is_needed, ==, 1); /* we already forwarded the resultset, no way someone has flushed the resultset-queue */

			con->state = CON_STATE_READ_QUERY;
		}
	}
	NETWORK_MYSQLD_CON_TRACK_TIME(con, "proxy::ready_query_result::leave");

	return NETWORK_SOCKET_SUCCESS;
}

static void sharding_merge_rows(network_mysqld_con* con, injection* inj, network_socket* recv_sock) {
    if (!inj->resultset_is_needed || !recv_sock->recv_queue->chunks || inj->qstat.binary_encoded) {
        return;
    }

    proxy_resultset_t* resultset = proxy_resultset_new();
    proxy_resultset_init1(resultset, inj);
    resultset->result_queue = recv_sock->recv_queue->chunks;

    parse_resultset_fields(resultset);
    GList* res_row = resultset->rows_chunk_head;
    while(res_row) {
        network_packet packet;
        packet.data = res_row->data;
        packet.offset = 0;

        network_mysqld_proto_skip_network_header(&packet);
        network_mysqld_lenenc_type lenenc_type;
        network_mysqld_proto_peek_lenenc_type(&packet, &lenenc_type);
		switch (lenenc_type) {
			case NETWORK_MYSQLD_LENENC_TYPE_ERR:
			case NETWORK_MYSQLD_LENENC_TYPE_EOF:
                goto exit;

			case NETWORK_MYSQLD_LENENC_TYPE_INT:
			case NETWORK_MYSQLD_LENENC_TYPE_NULL:
				break;
		}

		guint len = resultset->fields->len;
		GPtrArray* row = g_ptr_array_new_full(len, g_free);
		guint i;
		for (i = 0; i < len; i++) {
			guint64 field_len;

			network_mysqld_proto_peek_lenenc_type(&packet, &lenenc_type);

			switch (lenenc_type) {
				case NETWORK_MYSQLD_LENENC_TYPE_NULL:
                    g_ptr_array_add(row, NULL);
					network_mysqld_proto_skip(&packet, 1);
					break;

				case NETWORK_MYSQLD_LENENC_TYPE_INT:
					network_mysqld_proto_get_lenenc_int(&packet, &field_len);
					g_ptr_array_add(row, g_strndup(packet.data->str + packet.offset, field_len));
					network_mysqld_proto_skip(&packet, field_len);
					break;

				default:
					break;
			}
		}

		g_ptr_array_add(con->sharding_context->merge_result.recvd_rows, row);
		res_row = res_row->next;
    }

exit:
    proxy_resultset_free(resultset);
}

G_INLINE_FUNC void init_injection_from_com_query_result(injection* inj, network_mysqld_com_query_result_t* com_query) {
    if (inj == NULL || com_query == NULL) { return; }

    inj->bytes = com_query->bytes;
    inj->rows = com_query->rows;
    inj->qstat.was_resultset = com_query->was_resultset;

    if (!com_query->was_resultset) { /* INSERTs have a affected_rows */
        inj->qstat.affected_rows = com_query->affected_rows;
        inj->qstat.insert_id = com_query->insert_id;
    }
    inj->qstat.server_status = com_query->server_status;
    inj->qstat.warning_count = com_query->warning_count;
    inj->qstat.query_status = com_query->query_status;
}

/**
 * decide next stage to be CON_STATE_SHARDING_SEND_QUERY_RESULT or send next injection to the same dbgroup
 */
static network_socket_retval_t sharding_finish_injection_decide_next_stage(gboolean is_resultset_finish, injection* inj, network_mysqld_con* con, 
        sharding_dbgroup_context_t* dbgroup_context) 
{
    network_socket_retval_t ret = NETWORK_SOCKET_SUCCESS;
    network_mysqld_con_lua_t* st = dbgroup_context->st;
    network_socket* send_sock = con->client;

    if (is_resultset_finish) {
        /**
         * if the send-queue is empty, we have nothing to send
         * and can read the next query */
        if (send_sock->send_queue->chunks) {
            con->state = CON_STATE_SHARDING_SEND_QUERY_RESULT;
        } else { // ?? -- TODO -- never reach this, because send_sock->send_queue->chunks is always not eault to NULL
            g_assert(con->resultset_is_needed == 1); /* if we have already forwarded the resultset, never reach here */

            con->state = CON_STATE_READ_QUERY;
        }
    } else {
        /**
         * one injection is finish, this dbgroup is available to send next injection
         * !!NOTICE: the con->state can't change, because other backend is in CON_STATE_SHARDING_READ_QUERY_RESULT,
         * if we change the con->state, will make mistakens.
         */
        if (st->injected.queries->length > 0) {
            ret = NETWORK_SOCKET_SHARDING_AVAILABLE_SEND_NEXT_INJECTION;
            sharding_proxy_send_injections(dbgroup_context);
            sharding_dbgroup_reset_command_response_state(&dbgroup_context->parse);
        } else {
            ret = NETWORK_SOCKET_SHARDING_NOMORE_INJECTION;
            gboolean has_last_insert_id = inj->qstat.insert_id > 0;
            if (!has_last_insert_id && con->trans_context->trans_stage != TRANS_STAGE_IN_TRANS) {
                network_connection_pool_sharding_add_connection(con, dbgroup_context);
            }
        }
    }

    return ret;
}

static network_socket_retval_t sharding_injection_read_sql_finish_handle(network_mysqld_con* con, injection* inj, sharding_dbgroup_context_t* dbgroup_context) {
    network_socket_retval_t ret = NETWORK_SOCKET_SUCCESS;
    network_socket* send_sock = con->client;
    network_socket* recv_sock = dbgroup_context->backend_sock;
    sharding_merge_result_t* merge_result = &con->sharding_context->merge_result;
    network_mysqld_con_lua_t* st = dbgroup_context->st;
    gboolean is_resultset_finish = FALSE;

    sharding_log_sql(con->client, recv_sock, inj);

    if (inj->qstat.query_status == MYSQLD_PACKET_OK && merge_result->recvd_rows->len < merge_result->limit) { sharding_merge_rows(con, inj, recv_sock); }

    if ((++merge_result->result_recvd_num) < merge_result->shard_num) {
        // PROXY_IGNORE_RESULT;
        sharding_proxy_ignore_result(dbgroup_context);
    } else {
        is_resultset_finish = TRUE;
        network_injection_queue_reset(st->injected.queries);

        if (inj->qstat.query_status == MYSQLD_PACKET_OK) {

            proxy_resultset_t* resultset = proxy_resultset_new();

            proxy_resultset_init1(resultset, inj);
            if (inj->resultset_is_needed && !inj->qstat.binary_encoded) { resultset->result_queue = recv_sock->recv_queue->chunks; }

            parse_resultset_fields(resultset);
            free_recvd_packets(recv_sock);
            network_mysqld_con_send_resultset(send_sock, resultset->fields, merge_result->recvd_rows);
            proxy_resultset_free(resultset);
        }

        // PROXY_SEND_RESULT
        sharding_proxy_send_result(con, dbgroup_context, inj);
    }
    
    ret = sharding_finish_injection_decide_next_stage(is_resultset_finish, inj, con, dbgroup_context);
    return ret;
} 

static network_socket_retval_t sharding_injection_write_sql_finish_handle(network_mysqld_con* con, injection* inj, sharding_dbgroup_context_t* dbgroup_context) {
    network_socket_retval_t ret = NETWORK_SOCKET_SUCCESS;
    network_socket* send_sock = con->client;
    network_socket* recv_sock = dbgroup_context->backend_sock;
    sharding_merge_result_t* merge_result = &con->sharding_context->merge_result;
    network_mysqld_con_lua_t* st = dbgroup_context->st;
    gboolean is_resultset_finish = FALSE;

    sharding_log_sql(con->client, recv_sock, inj);

    if (inj->qstat.query_status == MYSQLD_PACKET_OK) {
        network_packet last_packet;
        last_packet.data = g_queue_peek_tail(recv_sock->recv_queue->chunks);
        last_packet.offset = 0;

        network_mysqld_ok_packet_t* ok_packet = network_mysqld_ok_packet_new();
        network_mysqld_proto_skip_network_header(&last_packet);
        if (network_mysqld_proto_get_ok_packet(&last_packet, ok_packet)) {
            network_mysqld_ok_packet_free(ok_packet);
            ret = NETWORK_SOCKET_ERROR;
            goto exit;
        }

        merge_result->affected_rows += ok_packet->affected_rows;
        merge_result->warnings += ok_packet->warnings;

        if ((++merge_result->result_recvd_num < merge_result->shard_num)) { 
            /**
             * now we only support one shard write, so it never reach here.
             */ 
            // PROXY_IGNORE_RESULT;   
            sharding_proxy_ignore_result(dbgroup_context);
        } else {
            is_resultset_finish = TRUE;
            network_mysqld_con_send_ok_full(send_sock, merge_result->affected_rows, 0, ok_packet->server_status, merge_result->warnings);
            network_injection_queue_reset(st->injected.queries);

            free_recvd_packets(recv_sock);
            // PROXY_SEND_RESULT;
            sharding_proxy_send_result(con, dbgroup_context, inj);
        }

        network_mysqld_ok_packet_free(ok_packet);
    } else {
        // PROXY_SEND_RESULT
        is_resultset_finish = TRUE;
        sharding_proxy_send_result(con, dbgroup_context, inj);
    }

    ret = sharding_finish_injection_decide_next_stage(is_resultset_finish, inj, con, dbgroup_context);
exit:
    return ret;
}

static network_socket_retval_t sharding_injection_other_sql_finish_handle(network_mysqld_con* con, injection* inj, sharding_dbgroup_context_t* dbgroup_context) {
    // PROXY_IGNORE_RESULT
    gboolean is_resultset_finish = FALSE;
    if (inj->id == INJECTION_MODIFY_USER_SQL) {
        if (dbgroup_context->backend_sock->response) {
            network_mysqld_auth_response_free(dbgroup_context->backend_sock->response);
            dbgroup_context->backend_sock->response = NULL;
        }

        dbgroup_context->backend_sock->response = network_mysqld_auth_response_copy(con->client->response);
        sharding_proxy_ignore_result(dbgroup_context);
    } else if (inj->id == INJECTION_TRANS_BEGIN_SQL) {
        if (inj->qstat.query_status == MYSQLD_PACKET_OK) {
            trans_context_t* trans_ctx = con->trans_context;
            trans_ctx->in_trans_dbgroup_ctx = dbgroup_context;
            if (inj->qstat.server_status & SERVER_STATUS_IN_TRANS) {
                trans_ctx->trans_stage = TRANS_STAGE_IN_TRANS;
            }
          sharding_proxy_ignore_result(dbgroup_context);  
        } else {
            is_resultset_finish = TRUE;
            sharding_reset_trans_context_t(con->trans_context);
            network_injection_queue_reset(dbgroup_context->st->injected.queries);
            sharding_proxy_send_result(con, dbgroup_context, inj);
        }
    } else if (inj->id == INJECTION_TRANS_FINISH_SQL) {
        is_resultset_finish = TRUE;
        network_injection_queue_reset(dbgroup_context->st->injected.queries);
        sharding_proxy_send_result(con, dbgroup_context, inj);
        sharding_reset_trans_context_t(con->trans_context);
    } else {
        sharding_proxy_ignore_result(dbgroup_context);
    }

    return sharding_finish_injection_decide_next_stage(is_resultset_finish, inj, con, dbgroup_context);
}


static network_socket_retval_t sharding_finish_injection_handle(injection* inj, sharding_dbgroup_context_t* dbgroup_context, network_mysqld_con* con) {
    network_mysqld_lua_stmt_ret ret = NETWORK_SOCKET_SUCCESS;
    if (inj == NULL) { goto exit; }

    network_socket* recv_sock = dbgroup_context->backend_sock;
    network_mysqld_con_lua_t* st = dbgroup_context->st;

    if (dbgroup_context->parse.command == COM_QUERY /*|| dbgroup_context->parse.command == COM_STMT_EXECUTE*/) {
        network_mysqld_com_query_result_t* com_query = (network_mysqld_com_query_result_t*)dbgroup_context->parse.data;
        init_injection_from_com_query_result(inj, com_query);
    }
    inj->ts_read_query_result_last = chassis_get_rel_microseconds();
    network_mysqld_queue_reset(recv_sock);

    g_assert(st->injected.queries->length > 0);
    g_queue_pop_head(st->injected.queries);
    if (inj->id == INJECTION_SHARDING_READ_SQL) {
        ret = sharding_injection_read_sql_finish_handle(con, inj, dbgroup_context);
    } else if (inj->id == INJECTION_SHARDING_WRITE_SQL) {
        ret = sharding_injection_write_sql_finish_handle(con, inj, dbgroup_context);
    } else { // change charset or user or database
        ret = sharding_injection_other_sql_finish_handle(con, inj, dbgroup_context);
    }

exit:
    if (inj) { injection_free(inj); }
    return ret;
}

//static network_socket_retval_t proxy_sharding_read_query_result(chassis G_GNUC_UNUSED *chas, network_mysqld_con *con) {
NETWORK_MYSQLD_PLUGIN_PROTO(proxy_sharding_read_query_result) {
    sharding_dbgroup_context_t* dbgroup_context = con->event_dbgroup_context;
    network_socket* recv_sock = dbgroup_context->backend_sock;
    network_socket* send_sock = con->client;
    network_mysqld_con_lua_t* st = dbgroup_context->st;

    network_packet last_packet;
    last_packet.data = g_queue_peek_tail(recv_sock->recv_queue->chunks);
    last_packet.offset = 0;

    injection* inj = NULL;
    if (0 != st->injected.queries->length) {
        inj = g_queue_peek_head(st->injected.queries);
    } else { // never reached here
        g_error("we should never reach here!");
    }

    if (inj && inj->ts_read_query_result_first == 0) {
		 // log the time of the first received packet
         inj->ts_read_query_result_first = chassis_get_rel_microseconds();
    }

    gint is_finished = sharding_parse_get_query_result(&dbgroup_context->parse, con, &last_packet);
    if (is_finished == -1) return NETWORK_SOCKET_ERROR;
    //dbgroup_context->resultset_is_finished = is_finished;

    if(!dbgroup_context->resultset_is_needed) { // just forward the packets to client
        network_mysqld_queue_append_raw(send_sock, send_sock->send_queue, g_queue_pop_tail(recv_sock->recv_queue->chunks));
    }

    if (is_finished) {
        dbgroup_context->cur_injection_finished = TRUE;
        return sharding_finish_injection_handle(inj, dbgroup_context, con);
    }

    return NETWORK_SOCKET_SUCCESS;
}

//static network_socket_retval_t proxy_sharding_send_query_result(chassis G_GNUC_UNUSED *chas, network_mysqld_con *con) {
NETWORK_MYSQLD_PLUGIN_PROTO(proxy_sharding_send_query_result) {
    g_hash_table_remove_all(con->sharding_context->querying_dbgroups);
    con->state = CON_STATE_READ_QUERY;
    return NETWORK_SOCKET_SUCCESS;
}

/**
 * connect to a backend
 *
 * @return
 *   NETWORK_SOCKET_SUCCESS        - connected successfully
 *   NETWORK_SOCKET_ERROR_RETRY    - connecting backend failed, call again to connect to another backend
 *   NETWORK_SOCKET_ERROR          - no backends available, adds a ERR packet to the client queue
 */
NETWORK_MYSQLD_PLUGIN_PROTO(proxy_connect_server) {
	guint i;

	guint client_ip = con->client->src->addr.ipv4.sin_addr.s_addr;
	if (!online && g_hash_table_contains(config->lvs_table, &client_ip)) {
		network_mysqld_con_send_error_full(con->client, C("Proxy Warning - Offline Now"), ER_UNKNOWN_ERROR, "07000");
		return NETWORK_SOCKET_SUCCESS;
	} else {
		GHashTable *ip_table = config->ip_table[config->ip_table_index];
		if (g_hash_table_size(ip_table) != 0) {
			for (i = 0; i < 3; ++i) {
				if (g_hash_table_contains(ip_table, &client_ip)) break;
				client_ip <<= 8;
			}

			if (i == 3 && !g_hash_table_contains(config->lvs_table, &client_ip)) {
				network_mysqld_con_send_error_full(con->client, C("Proxy Warning - IP Forbidden"), ER_UNKNOWN_ERROR, "07000");
				g_warning("Forbidden IP: %s", con->client->src->name->str);
				return NETWORK_SOCKET_SUCCESS;
			}
		}
	}

	network_mysqld_auth_challenge *challenge = network_mysqld_auth_challenge_new();

	challenge->protocol_version = 0;
	challenge->server_version_str = g_strdup("5.0.81-log");
	challenge->server_version = 50081;
	static guint32 thread_id = 0;
	challenge->thread_id = ++thread_id;

	GString *str = con->challenge;
	for (i = 0; i < 20; ++i) g_string_append_c(str, rand()%127+1);
	g_string_assign(challenge->challenge, str->str);

	challenge->capabilities = 41484;
	challenge->charset = 8;
	challenge->server_status = 2;

	GString *auth_packet = g_string_new(NULL);
	network_mysqld_proto_append_auth_challenge(auth_packet, challenge);
	network_mysqld_auth_challenge_free(challenge);
	network_mysqld_queue_append(con->client, con->client->send_queue, S(auth_packet));
	g_string_free(auth_packet, TRUE);
	con->state = CON_STATE_SEND_HANDSHAKE;

	return NETWORK_SOCKET_SUCCESS;
}

NETWORK_MYSQLD_PLUGIN_PROTO(proxy_init) {
	network_mysqld_con_lua_t *st = con->plugin_con_state;

	g_assert(con->plugin_con_state == NULL);

	st = network_mysqld_con_lua_new();

	con->plugin_con_state = st;

	con->state = CON_STATE_CONNECT_SERVER;

	return NETWORK_SOCKET_SUCCESS;
}

/**
 * cleanup the proxy specific data on the current connection
 *
 * move the server connection into the connection pool in case it is a
 * good client-side close
 *
 * @return NETWORK_SOCKET_SUCCESS
 * @see plugin_call_cleanup
 */
NETWORK_MYSQLD_PLUGIN_PROTO(proxy_disconnect_client) {
	network_mysqld_con_lua_t *st = con->plugin_con_state;
	lua_scope  *sc = con->srv->sc;
//	gboolean use_pooled_connection = FALSE;

	if (st == NULL) return NETWORK_SOCKET_SUCCESS;

	/**
	 * let the lua-level decide if we want to keep the connection in the pool
	 */
/*
	switch (proxy_lua_disconnect_client(con)) {
	case PROXY_NO_DECISION:
		// just go on

		break;
	case PROXY_IGNORE_RESULT:
		break;
	default:
		g_error("%s.%d: ... ", __FILE__, __LINE__);
		break;
	}
*/
	/**
	 * check if one of the backends has to many open connections
	 */

//	if (use_pooled_connection &&
//	    con->state == CON_STATE_CLOSE_CLIENT) {
/*
	if (con->state == CON_STATE_CLOSE_CLIENT) {
		if (con->server && con->server->is_authed) {
			network_connection_pool_lua_add_connection(con);
		}
	} else if (st->backend) {
		st->backend->connected_clients--;
	}
*/
#ifdef HAVE_LUA_H
	/* remove this cached script from registry */
	if (st->L_ref > 0) {
		luaL_unref(sc->L, LUA_REGISTRYINDEX, st->L_ref);
	}
#endif

	network_mysqld_con_lua_free(st);

	con->plugin_con_state = NULL;

	/**
	 * walk all pools and clean them up
	 */

	return NETWORK_SOCKET_SUCCESS;
}

/**
 * read the load data infile data from the client
 *
 * - decode the result-set to track if we are finished already
 * - gets called once for each packet
 *
 * @FIXME stream the data to the backend
 */
NETWORK_MYSQLD_PLUGIN_PROTO(proxy_read_local_infile_data) {
	int query_result = 0;
	network_packet packet;
	network_socket *recv_sock, *send_sock;
	network_mysqld_com_query_result_t *com_query = con->parse.data;

	NETWORK_MYSQLD_CON_TRACK_TIME(con, "proxy::ready_query_result::enter");

	recv_sock = con->client;
	send_sock = con->server;

	/* check if the last packet is valid */
	packet.data = g_queue_peek_tail(recv_sock->recv_queue->chunks);
	packet.offset = 0;

	/* if we get here from another state, src/network-mysqld.c is broken */
	g_assert_cmpint(con->parse.command, ==, COM_QUERY);
	g_assert_cmpint(com_query->state, ==, PARSE_COM_QUERY_LOCAL_INFILE_DATA);

	query_result = network_mysqld_proto_get_query_result(&packet, con);
	if (query_result == -1) return NETWORK_SOCKET_ERROR; /* something happend, let's get out of here */

	if (con->server) {
		network_mysqld_queue_append_raw(send_sock, send_sock->send_queue,
				g_queue_pop_tail(recv_sock->recv_queue->chunks));
	} else {
		GString *s;
		/* we don't have a backend
		 *
		 * - free the received packets early
		 * - send a OK later
		 */
		while ((s = g_queue_pop_head(recv_sock->recv_queue->chunks))) g_string_free(s, TRUE);
	}

	if (query_result == 1) { /* we have everything, send it to the backend */
		if (con->server) {
			con->state = CON_STATE_SEND_LOCAL_INFILE_DATA;
		} else {
			network_mysqld_con_send_ok(con->client);
			con->state = CON_STATE_SEND_LOCAL_INFILE_RESULT;
		}
		g_assert_cmpint(com_query->state, ==, PARSE_COM_QUERY_LOCAL_INFILE_RESULT);
	}

	return NETWORK_SOCKET_SUCCESS;
}

/**
 * read the load data infile result from the server
 *
 * - decode the result-set to track if we are finished already
 */
NETWORK_MYSQLD_PLUGIN_PROTO(proxy_read_local_infile_result) {
	int query_result = 0;
	network_packet packet;
	network_socket *recv_sock, *send_sock;

	NETWORK_MYSQLD_CON_TRACK_TIME(con, "proxy::ready_local_infile_result::enter");

	recv_sock = con->server;
	send_sock = con->client;

	/* check if the last packet is valid */
	packet.data = g_queue_peek_tail(recv_sock->recv_queue->chunks);
	packet.offset = 0;

	query_result = network_mysqld_proto_get_query_result(&packet, con);
	if (query_result == -1) return NETWORK_SOCKET_ERROR; /* something happend, let's get out of here */

	network_mysqld_queue_append_raw(send_sock, send_sock->send_queue,
			g_queue_pop_tail(recv_sock->recv_queue->chunks));

	if (query_result == 1) {
		con->state = CON_STATE_SEND_LOCAL_INFILE_RESULT;
	}

	return NETWORK_SOCKET_SUCCESS;
}

/**
 * cleanup after we sent to result of the LOAD DATA INFILE LOCAL data to the client
 */
NETWORK_MYSQLD_PLUGIN_PROTO(proxy_send_local_infile_result) {
	network_socket *recv_sock, *send_sock;

	NETWORK_MYSQLD_CON_TRACK_TIME(con, "proxy::send_local_infile_result::enter");

	recv_sock = con->server;
	send_sock = con->client;

	/* reset the packet-ids */
	if (send_sock) network_mysqld_queue_reset(send_sock);
	if (recv_sock) network_mysqld_queue_reset(recv_sock);

	con->state = CON_STATE_READ_QUERY;

	return NETWORK_SOCKET_SUCCESS;
}


int network_mysqld_proxy_connection_init(network_mysqld_con *con) {
	con->plugins.con_init                      = proxy_init;
	con->plugins.con_connect_server            = proxy_connect_server;
	con->plugins.con_read_handshake            = proxy_read_handshake;
	con->plugins.con_read_auth                 = proxy_read_auth;
	con->plugins.con_read_auth_result          = proxy_read_auth_result;
	con->plugins.con_read_query                = proxy_read_query;
	con->plugins.con_read_query_result         = proxy_read_query_result;
	con->plugins.con_send_query_result         = proxy_send_query_result;
	con->plugins.con_read_local_infile_data = proxy_read_local_infile_data;
	con->plugins.con_read_local_infile_result = proxy_read_local_infile_result;
	con->plugins.con_send_local_infile_result = proxy_send_local_infile_result;
	con->plugins.con_cleanup                   = proxy_disconnect_client;

    /**
     * sharding func
     */
    con->plugins.con_sharding_read_query_result = proxy_sharding_read_query_result;
    con->plugins.con_sharding_send_query_result = proxy_sharding_send_query_result;
	return 0;
}

/**
 * free the global scope which is shared between all connections
 *
 * make sure that is called after all connections are closed
 */
void network_mysqld_proxy_free(network_mysqld_con G_GNUC_UNUSED *con) {
}

void *string_free(GString *s) {
	g_string_free(s, TRUE);
}

chassis_plugin_config * network_mysqld_proxy_plugin_new(void) {
	config = g_new0(chassis_plugin_config, 1);

	config->fix_bug_25371   = 0; /** double ERR packet on AUTH failures */
	config->profiling       = 1;
	config->start_proxy     = 1;
	config->pool_change_user = 1; /* issue a COM_CHANGE_USER to cleanup the connection
					 when we get back the connection from the pool */
	config->ip_table[0] = g_hash_table_new_full(g_int_hash, g_int_equal, g_free, NULL);
	config->ip_table[1] = g_hash_table_new_full(g_int_hash, g_int_equal, g_free, NULL);
	config->ip_table_index = 0;
	config->lvs_table = g_hash_table_new_full(g_int_hash, g_int_equal, g_free, NULL);
	config->dt_table = g_hash_table_new_full(g_str_hash, g_str_equal, g_free, g_free);
	config->pwd_table[0] = g_hash_table_new_full(g_str_hash, g_str_equal, g_free, string_free);
	config->pwd_table[1] = g_hash_table_new_full(g_str_hash, g_str_equal, g_free, string_free);
    config->table_shard_rules = g_hash_table_new_full(g_str_hash, g_str_equal, g_free, sharding_table_free);
    config->db_groups = g_ptr_array_new();

	config->pwd_table_index = 0;
	config->sql_log = NULL;
	config->sql_log_type = NULL;
	config->charset = NULL;
	config->sql_log_slow_ms = 0;

	return config;
}

void network_mysqld_proxy_plugin_free(chassis_plugin_config *oldconfig) {
	gsize i;

	if (config->listen_con) {
		/**
		 * the connection will be free()ed by the network_mysqld_free()
		 */
#if 0
		event_del(&(config->listen_con->server->event));
		network_mysqld_con_free(config->listen_con);
#endif
	}

	g_strfreev(config->backend_addresses);
	g_strfreev(config->read_only_backend_addresses);

	if (config->address) {
		/* free the global scope */
		network_mysqld_proxy_free(NULL);

		g_free(config->address);
	}

	if (config->lua_script) g_free(config->lua_script);
	if (config->client_ips) {
		for (i = 0; config->client_ips[i]; i++) {
			g_free(config->client_ips[i]);
		}
		g_free(config->client_ips);
	}

	g_hash_table_remove_all(config->ip_table[0]);
	g_hash_table_destroy(config->ip_table[0]);
	g_hash_table_remove_all(config->ip_table[1]);
	g_hash_table_destroy(config->ip_table[1]);

	if (config->lvs_ips) {
		for (i = 0; config->lvs_ips[i]; i++) {
			g_free(config->lvs_ips[i]);
		}
		g_free(config->lvs_ips);
	}

	g_hash_table_remove_all(config->lvs_table);
	g_hash_table_destroy(config->lvs_table);

	if (config->tables) {
		for (i = 0; config->tables[i]; i++) {
			g_free(config->tables[i]);
		}
		g_free(config->tables);
	}

	g_hash_table_remove_all(config->dt_table);
	g_hash_table_destroy(config->dt_table);

	g_hash_table_remove_all(config->pwd_table[0]);
	g_hash_table_destroy(config->pwd_table[0]);
	g_hash_table_remove_all(config->pwd_table[1]);
	g_hash_table_destroy(config->pwd_table[1]);

    g_hash_table_remove_all(config->table_shard_rules);
    g_hash_table_destroy(config->table_shard_rules);

	if (config->sql_log) fclose(config->sql_log);
	if (config->sql_log_type) g_free(config->sql_log_type);

	if (config->charset) g_free(config->charset);
    if (config->db_groups) g_ptr_array_free(config->db_groups, TRUE);

	g_free(config);
}

/**
 * plugin options
 */
static GOptionEntry * network_mysqld_proxy_plugin_get_options(chassis_plugin_config *oldconfig) {
	guint i;

	/* make sure it isn't collected */
	static GOptionEntry config_entries[] =
	{
		{ "proxy-address",            'P', 0, G_OPTION_ARG_STRING, NULL, "listening address:port of the proxy-server (default: :4040)", "<host:port>" },
		{ "proxy-read-only-backend-addresses",
					      'r', 0, G_OPTION_ARG_STRING_ARRAY, NULL, "address:port of the remote slave-server (default: not set)", "<host:port>" },
		{ "proxy-backend-addresses",  'b', 0, G_OPTION_ARG_STRING_ARRAY, NULL, "address:port of the remote backend-servers (default: 127.0.0.1:3306)", "<host:port>" },

		{ "proxy-skip-profiling",     0, G_OPTION_FLAG_REVERSE, G_OPTION_ARG_NONE, NULL, "disables profiling of queries (default: enabled)", NULL },

		{ "proxy-fix-bug-25371",      0, 0, G_OPTION_ARG_NONE, NULL, "fix bug #25371 (mysqld > 5.1.12) for older libmysql versions", NULL },
		{ "proxy-lua-script",         's', 0, G_OPTION_ARG_FILENAME, NULL, "filename of the lua script (default: not set)", "<file>" },

		{ "no-proxy",                 0, G_OPTION_FLAG_REVERSE, G_OPTION_ARG_NONE, NULL, "don't start the proxy-module (default: enabled)", NULL },

		{ "proxy-pool-no-change-user", 0, G_OPTION_FLAG_REVERSE, G_OPTION_ARG_NONE, NULL, "don't use CHANGE_USER to reset the connection coming from the pool (default: enabled)", NULL },

		{ "client-ips", 0, 0, G_OPTION_ARG_STRING_ARRAY, NULL, "all permitted client ips", NULL },

		{ "lvs-ips", 0, 0, G_OPTION_ARG_STRING_ARRAY, NULL, "all lvs ips", NULL },

		{ "tables", 0, 0, G_OPTION_ARG_STRING_ARRAY, NULL, "sub-table settings", NULL },

		{ "pwds", 0, 0, G_OPTION_ARG_STRING_ARRAY, NULL, "password settings", NULL },

		{ "charset", 0, 0, G_OPTION_ARG_STRING, NULL, "original charset(default: LATIN1)", NULL },

		{ "sql-log", 0, 0, G_OPTION_ARG_STRING, NULL, "sql log type(default: OFF)", NULL },
		{ "sql-log-slow", 0, 0, G_OPTION_ARG_INT, NULL, "only log sql which takes longer than this milliseconds (default: 0)", NULL },

		{ NULL,                       0, 0, G_OPTION_ARG_NONE,   NULL, NULL, NULL }
	};

	i = 0;
	config_entries[i++].arg_data = &(config->address);
	config_entries[i++].arg_data = &(config->read_only_backend_addresses);
	config_entries[i++].arg_data = &(config->backend_addresses);

	config_entries[i++].arg_data = &(config->profiling);

	config_entries[i++].arg_data = &(config->fix_bug_25371);
	config_entries[i++].arg_data = &(config->lua_script);
	config_entries[i++].arg_data = &(config->start_proxy);
	config_entries[i++].arg_data = &(config->pool_change_user);
	config_entries[i++].arg_data = &(config->client_ips);
	config_entries[i++].arg_data = &(config->lvs_ips);
	config_entries[i++].arg_data = &(config->tables);
	config_entries[i++].arg_data = &(config->pwds);
	config_entries[i++].arg_data = &(config->charset);
	config_entries[i++].arg_data = &(config->sql_log_type);
	config_entries[i++].arg_data = &(config->sql_log_slow_ms);

	return config_entries;
}

void handler(int sig) {
	switch (sig) {
	case SIGUSR1:
		online = TRUE;
		break;
	case SIGUSR2:
		online = FALSE;
		break;
	}
}

gpointer check_state(chassis *chas) {
    MYSQL mysql;
    gint i, j, tm = 1;
    char *user = NULL, *pwd_decr = NULL, *pwd_encr = NULL, *pwds_str = NULL;
    GPtrArray* bs_array = g_ptr_array_new();
    mysql_init(&mysql);
    chassis_plugin *p = chas->modules->pdata[1]; //proxy plugin
    chassis_plugin_config *config = p->config;
    g_ptr_array_add(bs_array, chas->backends);
    for (i = 0; i < config->db_groups->len; i++ ) {
        db_group_t *dg = g_ptr_array_index(config->db_groups, i);
        g_ptr_array_add(bs_array, dg->bs);
    }
    if(config->pwds && config->pwds[0]) {
        pwds_str = strdup(config->pwds[0]);
        user = strsep(&pwds_str, ":");
        pwd_encr = strsep(&pwds_str, ":");
        pwd_decr = decrypt(pwd_encr);
    }
    sleep(1);
    while (TRUE) {
        for(j = 0; j < bs_array->len; j++) {
            network_backends_t *nbs = g_ptr_array_index(bs_array, j);
            GPtrArray *backends = nbs->backends;
            guint len = backends->len;
            for (i = 0; i < len; ++i) {
                network_backend_t* backend = g_ptr_array_index(backends, i);
                if (backend == NULL || backend->state == BACKEND_STATE_OFFLINE) continue;
                gchar* ip = inet_ntoa(backend->addr->addr.ipv4.sin_addr);
                guint port = ntohs(backend->addr->addr.ipv4.sin_port);
                mysql_options(&mysql, MYSQL_OPT_CONNECT_TIMEOUT, &tm);
                mysql_real_connect(&mysql, ip, user, pwd_decr, NULL, port, NULL, 0);

                if(backend->state == BACKEND_STATE_UP) {
                    if(mysql_errno(&mysql) != 0) {
                        backend->state = BACKEND_STATE_DOWN;
                    }
                } else if(backend->state == BACKEND_STATE_DOWN) {
                    if(mysql_errno(&mysql) == 0) {
                        backend->state = BACKEND_STATE_UP;
                    }
                } else if(backend->state == BACKEND_STATE_UNKNOWN) {
                    if(mysql_errno(&mysql) == 0)
                        backend->state = BACKEND_STATE_UP;
                    else
                        backend->state = BACKEND_STATE_DOWN;
                }
                mysql_close(&mysql);
            }
        }
        sleep(4);
    }
}

/*get the shard rule from GKeyFile, and insert the shard rule into a hashtable*/
int proxy_plugin_get_shard_rules(GKeyFile *keyfile, chassis *chas, chassis_plugin_config *config) {
    GError *gerr = NULL;
    gchar **groups, **gname;
    gsize length;
    int i, j;

    network_backends_t *bs = chas->backends;
    groups = g_key_file_get_groups(keyfile, &length);
    // must get config->db_groups firstly
    for(i = 0; i < length; i++) {
        gname = g_strsplit(groups[i], "-", 2);
        if(gname == NULL || gname[0] == NULL || strcasecmp(gname[0], "mysql") == 0) continue;
        if(strcasecmp(gname[0], "group") == 0) {
            db_group_t *dg = keyfile_to_db_group(keyfile, groups[i], chas->event_thread_count);
            g_ptr_array_add(config->db_groups, dg);
        }
        g_strfreev(gname);
    }

    for(i = 0; i < length; i++) {
        gname = g_strsplit(groups[i], "-", 2);
        if(gname == NULL || gname[0] == NULL || strcasecmp(gname[0], "mysql") == 0) continue;
        if(strcasecmp(gname[0], "shardrule") == 0) {
            sharding_table_t *stable = keyfile_to_sharding_table(keyfile, groups[i], config->db_groups);
            if (stable != NULL) {
                g_hash_table_insert(config->table_shard_rules, g_strdup(stable->table_name->str), stable);
            }
        }      
        g_strfreev(gname);
    }
    g_strfreev(groups);
    return 0;
}

/**
 * init the plugin with the parsed config
 */
int network_mysqld_proxy_plugin_apply_config(chassis *chas, chassis_plugin_config *oldconfig) {
	network_mysqld_con *con;
	network_socket *listen_sock;
	guint i;

	if (!config->start_proxy) {
		return 0;
	}

	if (!config->address) {
		g_critical("%s: Failed to get bind address, please set by --proxy-address=<host:port>", G_STRLOC);
		return -1;
	}

	if (!config->backend_addresses) {
		config->backend_addresses = g_new0(char *, 2);
		config->backend_addresses[0] = g_strdup("127.0.0.1:3306");
	}

	/**
	 * create a connection handle for the listen socket
	 */
	con = network_mysqld_con_new();
	network_mysqld_add_connection(chas, con);

	config->listen_con = con;

	listen_sock = network_socket_new();
	con->server = listen_sock;

	/* set the plugin hooks as we want to apply them to the new connections too later */
	network_mysqld_proxy_connection_init(con);

	if (0 != network_address_set_address(listen_sock->dst, config->address)) {
		return -1;
	}

	if (0 != network_socket_bind(listen_sock)) {
		return -1;
	}
	g_message("proxy listening on port %s", config->address);

	for (i = 0; config->backend_addresses && config->backend_addresses[i]; i++) {
		if (-1 == network_backends_add(chas->backends, config->backend_addresses[i], BACKEND_TYPE_RW)) {
			return -1;
		}
	}

	for (i = 0; config->read_only_backend_addresses && config->read_only_backend_addresses[i]; i++) {
		if (-1 == network_backends_add(chas->backends, config->read_only_backend_addresses[i], BACKEND_TYPE_RO)) {
			return -1;
		}
	}

	for (i = 0; config->client_ips && config->client_ips[i]; i++) {
		g_ptr_array_add(chas->backends->raw_ips, g_strdup(config->client_ips[i]));
		guint* sum = g_new0(guint, 1);
		char* token;
		while ((token = strsep(&config->client_ips[i], ".")) != NULL) {
			*sum = (*sum << 8) + atoi(token);
		}
		*sum = htonl(*sum);
		g_hash_table_add(config->ip_table[config->ip_table_index], sum);
	}
	chas->backends->ip_table = config->ip_table;
	chas->backends->ip_table_index = &(config->ip_table_index);

	for (i = 0; config->lvs_ips && config->lvs_ips[i]; i++) {
		guint* lvs_ip = g_new0(guint, 1);
		*lvs_ip = inet_addr(config->lvs_ips[i]);
		g_hash_table_add(config->lvs_table, lvs_ip);
	}
	signal(SIGUSR1, handler);
	signal(SIGUSR2, handler);

	if (config->sql_log_type) {
		if (strcasecmp(config->sql_log_type, "ON") == 0) {
			sql_log_type = ON;
		} else if (strcasecmp(config->sql_log_type, "REALTIME") == 0) {
			sql_log_type = REALTIME;
		}
	}

	if (sql_log_type != OFF) {
		gchar* sql_log_filename = g_strdup_printf("%s/sql_%s.log", chas->log_path, chas->instance_name);
		config->sql_log = fopen(sql_log_filename, "a");
		if (config->sql_log == NULL) {
			g_critical("Failed to open %s", sql_log_filename);
			g_free(sql_log_filename);
			return -1;
		}
		g_free(sql_log_filename);
	}

	for (i = 0; config->tables && config->tables[i]; i++) {
		db_table_t* dt = g_new0(db_table_t, 1);
		char *db = NULL, *token = NULL;
		gboolean is_complete = FALSE;

		if ((db = strsep(&config->tables[i], ".")) != NULL) {
			if ((token = strsep(&config->tables[i], ".")) != NULL) {
				dt->table_name = token;
				if ((token = strsep(&config->tables[i], ".")) != NULL) {
					dt->column_name = token;
					if ((token = strsep(&config->tables[i], ".")) != NULL) {
						dt->table_num = atoi(token);
						is_complete = TRUE;
					}
				}
			}
		}

		if (is_complete) {
			gchar* key = g_strdup_printf("%s.%s", db, dt->table_name);
			g_hash_table_insert(config->dt_table, key, dt);
		} else {
			g_critical("incorrect sub-table settings");
			g_free(dt);
			return -1;
		}
	}

	for (i = 0; config->pwds && config->pwds[i]; i++) {
		char *user = NULL, *pwd = NULL;
		gboolean is_complete = FALSE;
        char *pwds_str = strdup(config->pwds[i]);
		if ((user = strsep(&pwds_str, ":")) != NULL) {
			if ((pwd = strsep(&pwds_str, ":")) != NULL) {
				is_complete = TRUE;
			}
		}

		if (is_complete) {
			char* raw_pwd = decrypt(pwd);
			if (raw_pwd) {
				GString* hashed_password = g_string_new(NULL);
				network_mysqld_proto_password_hash(hashed_password, raw_pwd, strlen(raw_pwd));
				g_free(raw_pwd);
				g_hash_table_insert(config->pwd_table[config->pwd_table_index], g_strdup(user), hashed_password);
				g_ptr_array_add(chas->backends->raw_pwds, g_strdup_printf("%s:%s", user, pwd));
			} else {
				g_critical("password decrypt failed");
				return -1;
			}
		} else {
			g_critical("incorrect password settings");
			return -1;
		}
	}
	chas->backends->pwd_table = config->pwd_table;
	chas->backends->pwd_table_index = &(config->pwd_table_index);

	/* load the script and setup the global tables */
	network_mysqld_lua_setup_global(chas->sc->L, chas);

	/**
	 * call network_mysqld_con_accept() with this connection when we are done
	 */

	event_set(&(listen_sock->event), listen_sock->fd, EV_READ|EV_PERSIST, network_mysqld_con_accept, con);
	event_base_set(chas->event_base, &(listen_sock->event));
	event_add(&(listen_sock->event), NULL);

	g_thread_create((GThreadFunc)check_state, chas, FALSE, NULL);

	return 0;
}

G_MODULE_EXPORT int plugin_init(chassis_plugin *p) {
	p->magic        = CHASSIS_PLUGIN_MAGIC;
	p->name         = g_strdup("proxy");
	p->version	= g_strdup(PACKAGE_VERSION);

	p->init         = network_mysqld_proxy_plugin_new;
	p->get_options  = network_mysqld_proxy_plugin_get_options;
	p->apply_config = network_mysqld_proxy_plugin_apply_config;
	p->destroy      = network_mysqld_proxy_plugin_free;
        p->get_shard_rules = proxy_plugin_get_shard_rules;
	return 0;
}
