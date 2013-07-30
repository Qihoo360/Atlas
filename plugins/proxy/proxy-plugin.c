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

#include "lib/sql-tokenizer.h"
#include "chassis-event-thread.h"

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

//GMutex mutex;

guint get_table_index(GPtrArray* tokens, gint* d, gint* t) {
	*d = *t = -1;

	sql_token** ts = (sql_token**)(tokens->pdata);
	guint len = tokens->len;

	guint i = 1, j;
	while (ts[i]->token_id == TK_COMMENT && ++i < len);
	sql_token_id token_id = ts[i]->token_id;

	if (token_id == TK_SQL_SELECT || token_id == TK_SQL_DELETE) {
		for (; i < len; ++i) {
			if (ts[i]->token_id == TK_SQL_FROM) {
				for (j = i+1; j < len; ++j) {
					if (ts[j]->token_id == TK_SQL_WHERE) break;

					if (ts[j]->token_id == TK_LITERAL) {
						if (j + 2 < len && ts[j+1]->token_id == TK_DOT) {
							*d = j;
							*t = j + 2;
						} else {
							*t = j;
						}

						break;
					}
				}

				break;
			}
		}

		return 1;
	} else if (token_id == TK_SQL_UPDATE) {
		for (; i < len; ++i) {
			if (ts[i]->token_id == TK_SQL_SET) break;

			if (ts[i]->token_id == TK_LITERAL) {
				if (i + 2 < len && ts[i+1]->token_id == TK_DOT) {
					*d = i;
					*t = i + 2;
				} else {
					*t = i;
				}

				break;
			}
		}

		return 2;
	} else if (token_id == TK_SQL_INSERT || token_id == TK_SQL_REPLACE) {
		for (; i < len; ++i) {
			gchar* str = ts[i]->text->str;
			if (strcasecmp(str, "VALUES") == 0 || strcasecmp(str, "VALUE") == 0) break;

			sql_token_id token_id = ts[i]->token_id;
			if (token_id == TK_LITERAL && i + 2 < len && ts[i+1]->token_id == TK_DOT) {
				*d = i;
				*t = i + 2;
				break;
			} else if (token_id == TK_LITERAL || token_id == TK_FUNCTION) {
				if (i == len - 1) {
					*t = i;
					break;
				} else {
					str = ts[i+1]->text->str;
					token_id = ts[i+1]->token_id;
					if (strcasecmp(str, "VALUES") == 0 || strcasecmp(str, "VALUE") == 0 || token_id == TK_OBRACE || token_id == TK_SQL_SET) {
						*t = i;
						break;
					}
				}
			}
		}

		return 3;
	}

	return 0;
}

GArray* get_column_index(GPtrArray* tokens, gchar* table_name, gchar* column_name, guint sql_type, gint start) {
	GArray* columns = g_array_new(FALSE, FALSE, sizeof(guint));

	sql_token** ts = (sql_token**)(tokens->pdata);
	guint len = tokens->len;
	guint i, j, k;

	if (sql_type == 1) {
		for (i = start; i < len; ++i) {
			if (ts[i]->token_id == TK_SQL_WHERE) {
				for (j = i+1; j < len-2; ++j) {
					if (ts[j]->token_id == TK_LITERAL && strcasecmp(ts[j]->text->str, column_name) == 0) {
						if (ts[j+1]->token_id == TK_EQ) {
							if (ts[j-1]->token_id != TK_DOT || strcmp(ts[j-2]->text->str, table_name) == 0) {
								k = j + 2;
								g_array_append_val(columns, k);
								break;
							}
						} else if (j + 3 < len && strcasecmp(ts[j+1]->text->str, "IN") == 0 && ts[j+2]->token_id == TK_OBRACE) {
							k = j + 3;
							g_array_append_val(columns, k);
							while ((k += 2) < len && ts[k-1]->token_id != TK_CBRACE) {
								g_array_append_val(columns, k);
							}
						}
					}
				}
			}
		}
	} else if (sql_type == 2) {
		for (i = start; i < len; ++i) {
			if (ts[i]->token_id == TK_SQL_WHERE) {
				for (j = i+1; j < len-2; ++j) {
					if (ts[j]->token_id == TK_LITERAL && strcmp(ts[j]->text->str, column_name) == 0 && ts[j+1]->token_id == TK_EQ) {
						if (ts[j-1]->token_id != TK_DOT || strcmp(ts[j-2]->text->str, table_name) == 0) {
							k = j + 2;
							g_array_append_val(columns, k);
							break;
						}
					}
				}
			}
		}
	} else if (sql_type == 3) {
		sql_token_id token_id = ts[start]->token_id;

		if (token_id == TK_SQL_SET) {
			for (i = start+1; i < len-2; ++i) {
				if (ts[i]->token_id == TK_LITERAL && strcmp(ts[i]->text->str, column_name) == 0) {
					k = i + 2;
					g_array_append_val(columns, k);
					break;
				}
			}
		} else {
			k = 2;
			if (token_id == TK_OBRACE) {
				gint found = -1;
				for (j = start+1; j < len; ++j) {
					token_id = ts[j]->token_id;
					if (token_id == TK_CBRACE) break;
					if (token_id == TK_LITERAL && strcmp(ts[j]->text->str, column_name) == 0) {
						if (ts[j-1]->token_id != TK_DOT || strcmp(ts[j-2]->text->str, table_name) == 0) {
							found = j;
							break;
						}
					}
				}
				k = found - start + 1;
			}

			for (i = start; i < len-1; ++i) {
				gchar* str = ts[i]->text->str;
				if ((strcasecmp(str, "VALUES") == 0 || strcasecmp(str, "VALUE") == 0) && ts[i+1]->token_id == TK_OBRACE) {
					k += i;
					if (k < len) g_array_append_val(columns, k);
					break;
				}
			}
		}
	}

	return columns;
}

GPtrArray* combine_sql(GPtrArray* tokens, gint table, GArray* columns, guint num) {
	GPtrArray* sqls = g_ptr_array_new();

	sql_token** ts = (sql_token**)(tokens->pdata);
	guint len = tokens->len;
	guint i;

	if (columns->len == 1) {
		GString* sql = g_string_new(&op);

		if (ts[1]->token_id == TK_COMMENT) {
			g_string_append_printf(sql, "/*%s*/", ts[1]->text->str);
		} else {
			g_string_append(sql, ts[1]->text->str);
		}

		for (i = 2; i < len; ++i) {
			sql_token_id token_id = ts[i]->token_id;

			if (token_id != TK_OBRACE) g_string_append_c(sql, ' '); 

			if (i == table) {
				g_string_append_printf(sql, "%s_%u", ts[i]->text->str, atoi(ts[g_array_index(columns, guint, 0)]->text->str) % num);
			} else if (token_id == TK_STRING) {
				g_string_append_printf(sql, "'%s'", ts[i]->text->str);
			} else if (token_id == TK_COMMENT) {
				g_string_append_printf(sql, "/*%s*/", ts[i]->text->str);
			} else {
				g_string_append(sql, ts[i]->text->str);
			}
		}

		g_ptr_array_add(sqls, sql);
	} else {
		GArray* mt[num];
		for (i = 0; i < num; ++i) mt[i] = g_array_new(FALSE, FALSE, sizeof(guint));

		guint clen = columns->len;
		for (i = 0; i < clen; ++i) {
			guint column_value = atoi(ts[g_array_index(columns, guint, i)]->text->str);
			g_array_append_val(mt[column_value%num], column_value);
		}

		guint property_index   = g_array_index(columns, guint, 0) - 3;
		guint start_skip_index = property_index + 1;
		guint end_skip_index   = property_index + (clen + 1) * 2;

		guint m;
		for (m = 0; m < num; ++m) {
			if (mt[m]->len > 0) {
				GString* tmp = g_string_new(" IN(");
				g_string_append_printf(tmp, "%u", g_array_index(mt[m], guint, 0));
				guint k;
				for (k = 1; k < mt[m]->len; ++k) {
					g_string_append_printf(tmp, ",%u", g_array_index(mt[m], guint, k));
				}
				g_string_append_c(tmp, ')');

				GString* sql = g_string_new(&op);
				if (ts[1]->token_id == TK_COMMENT) {
					g_string_append_printf(sql, "/*%s*/", ts[1]->text->str);
				} else {
					g_string_append(sql, ts[1]->text->str);
				}
				for (i = 2; i < len; ++i) {
					if (i < start_skip_index || i > end_skip_index) {
						if (ts[i]->token_id != TK_OBRACE) g_string_append_c(sql, ' ');

						if (i == table) {
							g_string_append_printf(sql, "%s_%u", ts[i]->text->str, m);
						} else if (i == property_index) {
							g_string_append_printf(sql, "%s%s", ts[i]->text->str, tmp->str);
						} else if (ts[i]->token_id == TK_COMMENT) {
							g_string_append_printf(sql, "/*%s*/", ts[i]->text->str);
						} else {
							g_string_append(sql, ts[i]->text->str);
						}
					}
				}
				g_string_free(tmp, TRUE);

				g_ptr_array_add(sqls, sql);
			}

			g_array_free(mt[m], TRUE);
		}
	}

	return sqls;
}

GPtrArray* sql_parse(network_mysqld_con* con, GPtrArray* tokens) {
	//1. 解析库名和表名
	gint db, table;
	guint sql_type = get_table_index(tokens, &db, &table);
	if (table == -1) return NULL;

	//2. 解析列
	gchar* table_name = NULL;
	if (db == -1) {
		table_name = g_strdup_printf("%s.%s", con->client->default_db->str, ((sql_token*)tokens->pdata[table])->text->str);
	} else {
		table_name = g_strdup_printf("%s.%s", ((sql_token*)tokens->pdata[db])->text->str, ((sql_token*)tokens->pdata[table])->text->str);
	}

	db_table_t* dt = g_hash_table_lookup(con->config->dt_table, table_name);
	if (dt == NULL) {
		g_free(table_name);
		return NULL;
	}

	GArray* columns = get_column_index(tokens, table_name, dt->column_name, sql_type, table+1);
	g_free(table_name);
	if (columns->len == 0) {
		g_array_free(columns, TRUE);
		return NULL;
	}

	//3. 拼接SQL
	GPtrArray* sqls = combine_sql(tokens, table, columns, dt->table_num);
	g_array_free(columns, TRUE);
	return sqls;
}

int idle_rw(network_mysqld_con* con) {
	int ret = -1;
	guint i;

	network_backends_t* backends = con->srv->priv->backends;

	guint count = network_backends_count(backends);
	for (i = 0; i < count; ++i) {
		network_backend_t* backend = network_backends_get(backends, i);
		if (backend == NULL) continue;

		network_connection_pool* pool = chassis_event_thread_pool(backend);
		if (pool == NULL) continue;

		if (backend->type == BACKEND_TYPE_RW && backend->state == BACKEND_STATE_UP && pool->length > 0) {
			ret = i;
			break;
		}
	}

	return ret;
}

int idle_ro(network_mysqld_con* con) {
	int max_conns = -1;
	guint i;

	network_backends_t* backends = con->srv->priv->backends;

	guint count = network_backends_count(backends);
	for(i = 0; i < count; ++i) {
		network_backend_t* backend = network_backends_get(backends, i);
		if (backend == NULL) continue;

		network_connection_pool* pool = chassis_event_thread_pool(backend);
		if (pool == NULL) continue;

		if (backend->type == BACKEND_TYPE_RO && backend->state == BACKEND_STATE_UP && pool->length > 0) {
			if (max_conns == -1 || backend->connected_clients < max_conns) {
				max_conns = backend->connected_clients;
			}
		}
	}

	return max_conns;
}

int wrr_ro(network_mysqld_con *con) {
	guint i;

	network_backends_t* backends = con->srv->priv->backends;
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

		network_connection_pool* pool = chassis_event_thread_pool(backend);
		if (pool == NULL) goto next;

		if (backend->type == BACKEND_TYPE_RO && backend->weight >= cur_weight && backend->state == BACKEND_STATE_UP && pool->length > 0) ndx = next_ndx;

	next:
		if (next_ndx == ndx_num - 1) {
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
	(void)network_mysqld_con_lua_register_callback(con, con->config->lua_script);

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

			if (network_mysqld_con_lua_handle_proxy_response(con, con->config->lua_script)) {
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

        g_warning("[%s]: error packet from server (%s -> %s): %s(%d)", G_STRLOC,
                recv_sock->dst->name->str, recv_sock->src->name->str, errmsg, errcode);
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
	(void)network_mysqld_con_lua_register_callback(con, con->config->lua_script);

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

			if (network_mysqld_con_lua_handle_proxy_response(con, con->config->lua_script)) {
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
	chassis_plugin_config *config = con->config;
	network_mysqld_auth_response *auth;
	int err = 0;
	gboolean free_client_packet = TRUE;
	network_mysqld_con_lua_t *st = con->plugin_con_state;

	recv_sock = con->client;
	send_sock = con->server;

 	packet.data = g_queue_peek_tail(recv_sock->recv_queue->chunks);
	packet.offset = 0;

	err = err || network_mysqld_proto_skip_network_header(&packet);
	if (err) return NETWORK_SOCKET_ERROR;

	auth = network_mysqld_auth_response_new();

	err = err || network_mysqld_proto_get_auth_response(&packet, auth);

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

	g_string_assign_len(con->client->default_db, S(auth->database));

	/**
	 * looks like we finished parsing, call the lua function
	 */
	switch (proxy_lua_read_auth(con)) {
	case PROXY_SEND_RESULT:
		con->state = CON_STATE_SEND_AUTH_RESULT;

		break;
	case PROXY_SEND_INJECTION: {
		injection *inj;

		/* replace the client challenge that is sent to the server */
		inj = g_queue_pop_head(st->injected.queries);

		network_mysqld_queue_append(send_sock, send_sock->send_queue, S(inj->query));

		injection_free(inj);

		con->state = CON_STATE_SEND_AUTH;

		break; }
	case PROXY_NO_DECISION:
		/* if we don't have a backend (con->server), we just ack the client auth
		 */
		if (!con->server) {
			con->state = CON_STATE_SEND_AUTH_RESULT;

			network_mysqld_con_send_ok(recv_sock);

			break;
		}
		/* if the server-side of the connection is already up and authed
		 * we send a COM_CHANGE_USER to reauth the connection and remove
		 * all temp-tables and session-variables
		 *
		 * for performance reasons this extra reauth can be disabled. But
		 * that leaves temp-tables on the connection.
		 */
		if (con->server->is_authed) {
			if (config->pool_change_user) {
				GString *com_change_user = g_string_new(NULL);

				/* copy incl. the nul */
				g_string_append_c(com_change_user, COM_CHANGE_USER);
				g_string_append_len(com_change_user, con->client->response->username->str, con->client->response->username->len + 1); /* nul-term */

				g_assert_cmpint(con->client->response->response->len, <, 250);

				g_string_append_c(com_change_user, (con->client->response->response->len & 0xff));
				g_string_append_len(com_change_user, S(con->client->response->response));

				g_string_append_len(com_change_user, con->client->default_db->str, con->client->default_db->len + 1);
				
				network_mysqld_queue_append(
						send_sock,
						send_sock->send_queue, 
						S(com_change_user));

				/**
				 * the server is already authenticated, the client isn't
				 *
				 * transform the auth-packet into a COM_CHANGE_USER
				 */

				g_string_free(com_change_user, TRUE);
			
				con->state = CON_STATE_SEND_AUTH;
			} else {
				GString *auth_resp;

				/* check if the username and client-scramble are the same as in the previous authed
				 * connection */

				auth_resp = g_string_new(NULL);

				con->state = CON_STATE_SEND_AUTH_RESULT;

				if (!g_string_equal(con->client->response->username, con->server->response->username) ||
				    !g_string_equal(con->client->response->response, con->server->response->response)) {
					network_mysqld_err_packet_t *err_packet;

					err_packet = network_mysqld_err_packet_new();
					g_string_assign_len(err_packet->errmsg, C("(proxy-pool) login failed"));
					g_string_assign_len(err_packet->sqlstate, C("28000"));
					err_packet->errcode = ER_ACCESS_DENIED_ERROR;

					network_mysqld_proto_append_err_packet(auth_resp, err_packet);

					network_mysqld_err_packet_free(err_packet);
				} else {
					network_mysqld_ok_packet_t *ok_packet;

					ok_packet = network_mysqld_ok_packet_new();
					ok_packet->server_status = SERVER_STATUS_AUTOCOMMIT;

					network_mysqld_proto_append_ok_packet(auth_resp, ok_packet);
					
					network_mysqld_ok_packet_free(ok_packet);
				}

				network_mysqld_queue_append(recv_sock, recv_sock->send_queue, 
						S(auth_resp));

				g_string_free(auth_resp, TRUE);


                // modify , add for packet_id sync 
	            network_mysqld_queue_reset(send_sock);
	            network_mysqld_queue_reset(recv_sock);
			}
		} else {
			packet.data->str[6] = 0x03;
			network_mysqld_queue_append_raw(send_sock, send_sock->send_queue, packet.data);
			con->state = CON_STATE_SEND_AUTH;

			free_client_packet = FALSE; /* the packet.data is now part of the send-queue, don't free it further down */
		}

		break;
	default:
		g_assert_not_reached();
		break;
	}

	if (free_client_packet) {
		g_string_free(g_queue_pop_tail(recv_sock->recv_queue->chunks), TRUE);
	} else {
		/* just remove the link to the packet, the packet itself is part of the next queue already */
		g_queue_pop_tail(recv_sock->recv_queue->chunks);
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
	//g_mutex_lock(&mutex);
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
	//g_mutex_unlock(&mutex);

	con->state = CON_STATE_SEND_AUTH_RESULT;

	return NETWORK_SOCKET_SUCCESS;
}

int rw_split(GPtrArray* tokens, network_mysqld_con* con) {
	if (tokens->len < 2 || g_hash_table_size(con->locks) > 0 || con->is_insert_id) return idle_rw(con);

	sql_token* first_token = tokens->pdata[1];
	sql_token_id token_id = first_token->token_id;

	if (token_id == TK_COMMENT) {
		if (strcasecmp(first_token->text->str, "MASTER") == 0) {
			return idle_rw(con);
		} else {
			guint i = 1; 
			while (token_id == TK_COMMENT && ++i < tokens->len) {
				first_token = tokens->pdata[i];
				token_id = first_token->token_id;
			}    
		}    
	}    

	if (token_id == TK_SQL_SELECT || token_id == TK_SQL_SET || token_id == TK_SQL_EXPLAIN || token_id == TK_SQL_SHOW || token_id == TK_SQL_DESC) {
		return wrr_ro(con);
	} else {
		return idle_rw(con);
	}    
}

void modify_user(network_mysqld_con* con) {
	if (con->server == NULL) return;

	GString* client_user = con->client->response->username;
	GString* server_user = con->server->response->username;

	if (!g_string_equal(client_user, server_user)) {
		GString* com_change_user = g_string_new(NULL);

		g_string_append_c(com_change_user, COM_CHANGE_USER);
		g_string_append_len(com_change_user, client_user->str, client_user->len + 1);

		GString* hashed_password = g_hash_table_lookup(con->config->pwd_table, client_user->str);
		if (!hashed_password) return;

		GString* expected_response = g_string_new(NULL);
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

void modify_db(network_mysqld_con* con) {
	char* default_db = con->client->default_db->str;

	if (default_db != NULL && strcmp(default_db, "") != 0) {
		char cmd = COM_INIT_DB;
		GString* query = g_string_new_len(&cmd, 1);
		g_string_append(query, default_db);
		injection* inj = injection_new(2, query);
		inj->resultset_is_needed = TRUE;
		network_mysqld_con_lua_t* st = con->plugin_con_state;
		g_queue_push_head(st->injected.queries, inj);
	}
}

void modify_charset(GPtrArray* tokens, network_mysqld_con* con) {
	g_string_truncate(con->charset_client, 0);
	g_string_truncate(con->charset_results, 0);
	g_string_truncate(con->charset_connection, 0);

	if (con->server == NULL) return;

	gboolean is_set_client     = FALSE;
	gboolean is_set_results    = FALSE;
	gboolean is_set_connection = FALSE;

	//1.检查第一个词是不是SET
	if (tokens->len > 3) {
		sql_token* token = tokens->pdata[1];
		if (token->token_id == TK_SQL_SET) {
			//2.检查第二个词是不是NAMES或CHARACTER_SET_CLIENT或CHARACTER_SET_RESULTS或CHARACTER_SET_CONNECTION
			token = tokens->pdata[2];
			char* str = token->text->str;
			if (strcasecmp(str, "NAMES") == 0) {
				is_set_client = is_set_results = is_set_connection = TRUE;

				str = ((sql_token*)(tokens->pdata[3]))->text->str;
				g_string_assign(con->charset_client, str);
				g_string_assign(con->charset_results, str);
				g_string_assign(con->charset_connection, str);
			} else if (strcasecmp(str, "CHARACTER_SET_CLIENT") == 0) {
				is_set_client = TRUE;

				str = ((sql_token*)(tokens->pdata[3]))->text->str;
				g_string_assign(con->charset_client, str);
			} else if (strcasecmp(str, "CHARACTER_SET_RESULTS") == 0) {
				is_set_results = TRUE;

				str = ((sql_token*)(tokens->pdata[3]))->text->str;
				g_string_assign(con->charset_results, str);
			} else if (strcasecmp(str, "CHARACTER_SET_CONNECTION") == 0) {
				is_set_connection = TRUE;

				str = ((sql_token*)(tokens->pdata[3]))->text->str;
				g_string_assign(con->charset_connection, str);
			}
		}
	}

	//3.检查client和server两端的字符集是否相同
	network_socket* client = con->client;
	network_socket* server = con->server;
	char* default_charset = con->config->charset;
	GString* empty_charset = g_string_new("");
	char cmd = COM_QUERY;
	network_mysqld_con_lua_t* st = con->plugin_con_state;

	if (!is_set_client && !g_string_equal(client->charset_client, server->charset_client)) {
		GString* query = g_string_new_len(&cmd, 1);
		g_string_append(query, "SET CHARACTER_SET_CLIENT=");

		if (g_string_equal(client->charset_client, empty_charset)) {
			g_string_append(query, default_charset);
			g_string_assign(con->charset_client, default_charset);
		} else {
			g_string_append(query, client->charset_client->str);
			g_string_assign(con->charset_client, client->charset_client->str);
		}

		injection* inj = injection_new(3, query);
		inj->resultset_is_needed = TRUE;
		g_queue_push_head(st->injected.queries, inj);
	}
	if (!is_set_results && !g_string_equal(client->charset_results, server->charset_results)) {
		GString* query = g_string_new_len(&cmd, 1);
		g_string_append(query, "SET CHARACTER_SET_RESULTS=");

		if (g_string_equal(client->charset_results, empty_charset)) {
			g_string_append(query, default_charset);
			g_string_assign(con->charset_results, default_charset);
		} else {
			g_string_append(query, client->charset_results->str);
			g_string_assign(con->charset_results, client->charset_results->str);
		}

		injection* inj = injection_new(4, query);
		inj->resultset_is_needed = TRUE;
		g_queue_push_head(st->injected.queries, inj);
	}
	if (!is_set_connection && !g_string_equal(client->charset_connection, server->charset_connection)) {
		GString* query = g_string_new_len(&cmd, 1);
		g_string_append(query, "SET CHARACTER_SET_CONNECTION=");

		if (g_string_equal(client->charset_connection, empty_charset)) {
			g_string_append(query, default_charset);
			g_string_assign(con->charset_connection, default_charset);
		} else {
			g_string_append(query, client->charset_connection->str);
			g_string_assign(con->charset_connection, client->charset_connection->str);
		}

		injection* inj = injection_new(5, query);
		inj->resultset_is_needed = TRUE;
		g_queue_push_head(st->injected.queries, inj);
	}

	g_string_free(empty_charset, TRUE);
}

void check_flags(GPtrArray* tokens, network_mysqld_con* con) {
	con->is_in_select_calc_found_rows = con->is_insert_id = FALSE;

	sql_token** ts = (sql_token**)(tokens->pdata);
	guint len = tokens->len;

	if (len > 2) {
		if (ts[1]->token_id == TK_SQL_SELECT && strcasecmp(ts[2]->text->str, "GET_LOCK") == 0) {
			gchar* key = ts[4]->text->str;
			if (!g_hash_table_lookup(con->locks, key)) g_hash_table_add(con->locks, g_strdup(key));
		}

		if (len > 4) {	//SET AUTOCOMMIT = {0 | 1}
			if (ts[1]->token_id == TK_SQL_SET && ts[3]->token_id == TK_EQ) {
				if (strcasecmp(ts[2]->text->str, "AUTOCOMMIT") == 0) {
					char* str = ts[4]->text->str;
					if (strcmp(str, "0") == 0) con->is_not_autocommit = TRUE;
					else if (strcmp(str, "1") == 0) con->is_not_autocommit = FALSE;
				}
			}
		}
	}

	guint i;
	for (i = 1; i < len; ++i) {
		sql_token* token = ts[i];
		if (token->token_id == TK_SQL_SQL_CALC_FOUND_ROWS) {
			con->is_in_select_calc_found_rows = TRUE;
		} else {
			char* str = token->text->str;
			if (strcasecmp(str, "LAST_INSERT_ID") == 0 || strcasecmp(str, "@@INSERT_ID") == 0 || strcasecmp(str, "@@LAST_INSERT_ID") == 0) con->is_insert_id = TRUE;
		}
		if (con->is_in_select_calc_found_rows && con->is_insert_id) break;
	}
}

gboolean is_in_blacklist(GPtrArray* tokens) {
	guint len = tokens->len;
	guint i;

	sql_token* token = tokens->pdata[1];
	if (token->token_id == TK_SQL_DELETE) {
		for (i = 2; i < len; ++i) {
			token = tokens->pdata[i];
			if (token->token_id == TK_SQL_WHERE) break;
		}
		if (i == len) return TRUE;
	}
	/*
	else if (token->token_id == TK_SQL_SET) {
		if (tokens->len >= 5) {
			token = tokens->pdata[2];
			if (strcasecmp(token->text->str, "AUTOCOMMIT") == 0) {
				token = tokens->pdata[3];
				if (token->token_id == TK_EQ) return TRUE;
			}
		}
	}
	*/
	for (i = 2; i < len; ++i) {
		token = tokens->pdata[i];
		if (token->token_id == TK_OBRACE) {
			token = tokens->pdata[i-1];
			if (strcasecmp(token->text->str, "SLEEP") == 0) return TRUE;
		}
	}

	return FALSE;
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
	if (type == COM_QUIT) {
		g_string_free(packets, TRUE);
		network_mysqld_con_send_ok_full(con->client, 0, 0, 0x0002, 0);
		ret = PROXY_SEND_RESULT;
	} else {
		GPtrArray *tokens = sql_tokens_new();
		sql_tokenizer(tokens, packets->str, packets->len);

		if (type == COM_QUERY && is_in_blacklist(tokens)) {
			g_string_free(packets, TRUE);
			network_mysqld_con_send_error_full(con->client, C("Proxy Warning - Syntax Forbidden"), ER_UNKNOWN_ERROR, "07000");
			ret = PROXY_SEND_RESULT;
		} else {
			GPtrArray* sqls = NULL;
			if (type == COM_QUERY && con->config->tables) {
				sqls = sql_parse(con, tokens);
			}

			ret = PROXY_SEND_INJECTION;
			injection* inj = NULL;
			if (sqls == NULL) {
				inj = injection_new(1, packets);
				inj->resultset_is_needed = TRUE;
				g_queue_push_tail(st->injected.queries, inj);
			} else {
				g_string_free(packets, TRUE);

				if (sqls->len == 1) {
					inj = injection_new(1, sqls->pdata[0]);
					inj->resultset_is_needed = TRUE;
					g_queue_push_tail(st->injected.queries, inj);
				} else {
					merge_res_t* merge_res = con->merge_res;

					merge_res->sub_sql_num = sqls->len;
					merge_res->sub_sql_exed = 0;
					merge_res->limit = G_MAXINT;

					sql_token** ts = (sql_token**)(tokens->pdata);
					for (i = tokens->len-2; i >= 0; --i) {
						if (ts[i]->token_id == TK_SQL_LIMIT && ts[i+1]->token_id == TK_INTEGER) {
							merge_res->limit = atoi(ts[i+1]->text->str);
							break;
						}
					}

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

					for (i = 0; i < sqls->len; ++i) {
						inj = injection_new(7, sqls->pdata[i]);
						inj->resultset_is_needed = TRUE;
						g_queue_push_tail(st->injected.queries, inj);
					}
				}

				g_ptr_array_free(sqls, TRUE);
			}

			check_flags(tokens, con);

			if (con->server == NULL) {
				int backend_ndx = -1;

				if (!con->is_in_transaction && !con->is_not_autocommit && g_hash_table_size(con->locks) == 0) {
					if (type == COM_QUERY) {
						backend_ndx = rw_split(tokens, con);
						//g_mutex_lock(&mutex);
						send_sock = network_connection_pool_lua_swap(con, backend_ndx);
						//g_mutex_unlock(&mutex);
					} else if (type == COM_INIT_DB) {
						backend_ndx = wrr_ro(con);
						//g_mutex_lock(&mutex);
						send_sock = network_connection_pool_lua_swap(con, backend_ndx);
						//g_mutex_unlock(&mutex);
					}
				}

				if (send_sock == NULL) {
					backend_ndx = idle_rw(con);
					//g_mutex_lock(&mutex);
					send_sock = network_connection_pool_lua_swap(con, backend_ndx);
					//g_mutex_unlock(&mutex);
				}
				con->server = send_sock;
			}

			modify_db(con);
			modify_charset(tokens, con);
			modify_user(con);
		}

		sql_tokens_free(tokens);
	}
	NETWORK_MYSQLD_CON_TRACK_TIME(con, "proxy::ready_query::leave_lua");

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
				con->config->lua_script);

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

void log_sql(network_mysqld_con* con, injection* inj) {
	chassis_plugin_config *config = con->config;
	GString* message = g_string_new(NULL);

	time_t t = time(NULL);
	struct tm* tm = localtime(&t);
	g_string_printf(message, "[%02d/%02d/%d %02d:%02d:%02d] C:%s S:", tm->tm_mon+1, tm->tm_mday, tm->tm_year+1900, tm->tm_hour, tm->tm_min, tm->tm_sec, inet_ntoa(con->client->src->addr.ipv4.sin_addr));
	gint latency = inj->ts_read_query_result_last - inj->ts_read_query;

	if (inj->qstat.query_status == MYSQLD_PACKET_OK) {
		g_string_append_printf(message, "%s OK %.3f \"%s\"\n", inet_ntoa(con->server->dst->addr.ipv4.sin_addr), latency/1000.0, inj->query->str+1);
	} else {
		g_string_append_printf(message, "%s ERR %.3f \"%s\"\n", inet_ntoa(con->server->dst->addr.ipv4.sin_addr), latency/1000.0, inj->query->str+1);
	}

	fwrite(message->str, message->len, 1, config->sql_log);

	g_string_free(message, TRUE);
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
	if (!con->resultset_is_needed) {
		network_mysqld_queue_append_raw(send_sock, send_sock->send_queue, g_queue_pop_tail(recv_sock->recv_queue->chunks));
	}

	if (is_finished) {
		network_mysqld_lua_stmt_ret ret;

		/**
		 * the resultset handler might decide to trash the send-queue
		 * 
		 * */
		//g_mutex_lock(&mutex);
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
			} else if (inj->id == 7) {
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

				if (!con->is_in_transaction && !con->is_not_autocommit && !con->is_in_select_calc_found_rows && !have_last_insert_id && g_hash_table_size(con->locks) == 0) network_connection_pool_lua_add_connection(con);

				++st->injected.sent_resultset;
				if (st->injected.sent_resultset == 1) {
					while ((p = g_queue_pop_head(recv_sock->recv_queue->chunks))) network_mysqld_queue_append_raw(send_sock, send_sock->send_queue, p);
					break;
				}

			case PROXY_IGNORE_RESULT:
				if (con->resultset_is_needed) {
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
		//g_mutex_unlock(&mutex);
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

/**
 * connect to a backend
 *
 * @return
 *   NETWORK_SOCKET_SUCCESS        - connected successfully
 *   NETWORK_SOCKET_ERROR_RETRY    - connecting backend failed, call again to connect to another backend
 *   NETWORK_SOCKET_ERROR          - no backends available, adds a ERR packet to the client queue
 */
NETWORK_MYSQLD_PLUGIN_PROTO(proxy_connect_server) {
	network_mysqld_con_lua_t *st = con->plugin_con_state;
//	guint min_connected_clients = G_MAXUINT;
	guint i;
	network_backend_t *cur;

	chassis_plugin_config *config = con->config;

	guint client_ip = con->client->src->addr.ipv4.sin_addr.s_addr;
	if (!online && g_hash_table_contains(config->lvs_table, &client_ip)) {
		network_mysqld_con_send_error_full(con->client, C("Proxy Warning - Offline Now"), ER_UNKNOWN_ERROR, "07000");
		return NETWORK_SOCKET_SUCCESS;
	} else if (config->client_ips != NULL) {
		for (i = 0; i < 3; ++i) {
			if (g_hash_table_contains(config->ip_table, &client_ip)) break;
			client_ip <<= 8;
		}

		if (i == 3 && !g_hash_table_contains(config->lvs_table, &(con->client->src->addr.ipv4.sin_addr.s_addr))) {
			network_mysqld_con_send_error_full(con->client, C("Proxy Warning - IP Forbidden"), ER_UNKNOWN_ERROR, "07000");
			return NETWORK_SOCKET_SUCCESS;
		}    
	}

	if (con->server) {
		switch (network_socket_connect_finish(con->server)) {
		case NETWORK_SOCKET_SUCCESS:
			break;
		case NETWORK_SOCKET_ERROR:
        case NETWORK_SOCKET_ERROR_RETRY:
		g_message("%s.%d: connect(%s) failed: %s. backend maybe down, Retrying with different backend if backends_state is auto.", 
			__FILE__, __LINE__,
			con->server->dst->name->str, g_strerror(errno));

		if (st->backend->state != BACKEND_STATE_OFFLINE) st->backend->state = BACKEND_STATE_DOWN;
		network_socket_free(con->server);
		con->server = NULL;

			return NETWORK_SOCKET_ERROR_RETRY;
		default:
			g_assert_not_reached();
			break;
		}

		con->state = CON_STATE_READ_HANDSHAKE;

		return NETWORK_SOCKET_SUCCESS;
	}

	st->backend = NULL;
	st->backend_ndx = -1;

	guint min_idle_connections = config->min_idle_connections;
	int backend_ndx = -1;
	gboolean use_pooled_connection = TRUE;

	//g_mutex_lock(&mutex);

	network_backends_t* backends = con->srv->priv->backends;
	guint count = network_backends_count(backends);
	for (i = 0; i < count; ++i) {
		cur = network_backends_get(backends, i);
		if (cur == NULL || cur->state != BACKEND_STATE_UP) continue;

		network_connection_pool* pool = chassis_event_thread_pool(cur);
		if (pool->length < min_idle_connections) {
			backend_ndx = i;
			use_pooled_connection = FALSE;
			break;
		}
	}

	if (backend_ndx == -1) {
		for (i = count-1; i < count; --i) {
			cur = network_backends_get(backends, i);
			if (cur == NULL || cur->state != BACKEND_STATE_UP) continue;
			backend_ndx = i;
			break;
		}
	}

	if (backend_ndx == -1) {
		network_mysqld_con_send_error_pre41(con->client, C("(proxy) all backends are down"));
		//g_mutex_unlock(&mutex);
		g_critical("%s.%d: Cannot connect, all backends are down.", __FILE__, __LINE__);
		return NETWORK_SOCKET_ERROR;
	} else {
		st->backend = network_backends_get(backends, backend_ndx);
		st->backend_ndx = backend_ndx;

		if (use_pooled_connection) con->server = network_connection_pool_lua_swap(con, backend_ndx);
	}

	//g_mutex_unlock(&mutex);

	/**
	 * check if we have a connection in the pool for this backend
	 */
	if (NULL == con->server) {
		con->server = network_socket_new();
		network_address_copy(con->server->dst, st->backend->addr);
	
		st->backend->connected_clients++;

		switch(network_socket_connect(con->server)) {
		case NETWORK_SOCKET_ERROR_RETRY:
			/* the socket is non-blocking already, 
			 * call getsockopt() to see if we are done */
			return NETWORK_SOCKET_ERROR_RETRY;
		case NETWORK_SOCKET_SUCCESS:
			break;
		default:
			g_message("%s.%d: connecting to backend (%s) failed, marking it as down for ...", 
				__FILE__, __LINE__, con->server->dst->name->str);

			if (st->backend->state != BACKEND_STATE_OFFLINE) st->backend->state = BACKEND_STATE_DOWN;
			network_socket_free(con->server);
			con->server = NULL;

			return NETWORK_SOCKET_ERROR_RETRY;
		}

		con->state = CON_STATE_READ_HANDSHAKE;
	} else {
		GString *auth_packet;

		/**
		 * send the old hand-shake packet
		 */

		auth_packet = g_string_new(NULL);
		network_mysqld_proto_append_auth_challenge(auth_packet, con->server->challenge);

		network_mysqld_queue_append(con->client, con->client->send_queue, S(auth_packet));

		g_string_free(auth_packet, TRUE);
		
		con->state = CON_STATE_SEND_HANDSHAKE;

		/**
		 * connect_clients is already incremented 
		 */
	}

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
	lua_scope  *sc = con->srv->priv->sc;
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
	if (con->state == CON_STATE_CLOSE_CLIENT) {
		/* move the connection to the connection pool
		 *
		 * this disconnects con->server and safes it from getting free()ed later
		 */
		if (con->server && con->server->is_authed) {
			//g_mutex_lock(&mutex);
			network_connection_pool_lua_add_connection(con);
			//g_mutex_unlock(&mutex);
		}
	} else if (st->backend) {
		/* we have backend assigned and want to close the connection to it */
		st->backend->connected_clients--;
	}

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

	return 0;
}

/**
 * free the global scope which is shared between all connections
 *
 * make sure that is called after all connections are closed
 */
void network_mysqld_proxy_free(network_mysqld_con G_GNUC_UNUSED *con) {
}

chassis_plugin_config * network_mysqld_proxy_plugin_new(void) {
	chassis_plugin_config *config;

	config = g_new0(chassis_plugin_config, 1);
	config->fix_bug_25371   = 0; /** double ERR packet on AUTH failures */
	config->profiling       = 1;
	config->start_proxy     = 1;
	config->pool_change_user = 1; /* issue a COM_CHANGE_USER to cleanup the connection 
					 when we get back the connection from the pool */
	config->ip_table = g_hash_table_new_full(g_int_hash, g_int_equal, g_free, NULL);
	config->lvs_table = g_hash_table_new_full(g_int_hash, g_int_equal, g_free, NULL);
	config->dt_table = g_hash_table_new_full(g_str_hash, g_str_equal, g_free, g_free);
	config->pwd_table = g_hash_table_new(g_str_hash, g_str_equal);
	config->sql_log = NULL;
	config->charset = NULL;

	//g_mutex_init(&mutex);

	return config;
}

void network_mysqld_proxy_plugin_free(chassis_plugin_config *config) {
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

	if (config->backend_addresses) {
		for (i = 0; config->backend_addresses[i]; i++) {
			g_free(config->backend_addresses[i]);
		}
		g_free(config->backend_addresses);
	}

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

	g_hash_table_remove_all(config->ip_table);
	g_hash_table_destroy(config->ip_table);

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

	g_hash_table_remove_all(config->pwd_table);
	g_hash_table_destroy(config->pwd_table);

	if (config->sql_log) fclose(config->sql_log);

	if (config->charset) g_free(config->charset);

	g_free(config);

	//g_mutex_clear(&mutex);
}

/**
 * plugin options 
 */
static GOptionEntry * network_mysqld_proxy_plugin_get_options(chassis_plugin_config *config) {
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

		{ "min-idle-connections", 0, 0, G_OPTION_ARG_INT, NULL, "min idle connections of each backend", NULL },

		{ "client-ips", 0, 0, G_OPTION_ARG_STRING_ARRAY, NULL, "all permitted client ips", NULL },
	
		{ "lvs-ips", 0, 0, G_OPTION_ARG_STRING_ARRAY, NULL, "all lvs ips", NULL },

		{ "tables", 0, 0, G_OPTION_ARG_STRING_ARRAY, NULL, "sub-table settings", NULL },
	
		{ "pwds", 0, 0, G_OPTION_ARG_STRING_ARRAY, NULL, "password settings", NULL },
		
		{ "charset", 0, 0, G_OPTION_ARG_STRING, NULL, "original charset(default: LATIN1)", NULL },

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
	config_entries[i++].arg_data = &(config->min_idle_connections);
	config_entries[i++].arg_data = &(config->client_ips);
	config_entries[i++].arg_data = &(config->lvs_ips);
	config_entries[i++].arg_data = &(config->tables);
	config_entries[i++].arg_data = &(config->pwds);
	config_entries[i++].arg_data = &(config->charset);

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

char* decrypt(char* in) {
	//1. Base64解码
	EVP_ENCODE_CTX dctx;
	EVP_DecodeInit(&dctx);

	int inl = strlen(in);
	unsigned char inter[512] = {};
	int interl = 0;

	if (EVP_DecodeUpdate(&dctx, inter, &interl, in, inl) == -1) return NULL;
	int len = interl;
	if (EVP_DecodeFinal(&dctx, inter+len, &interl) != 1) return NULL;
	len += interl;

	//2. DES解码
	EVP_CIPHER_CTX ctx;
	EVP_CIPHER_CTX_init(&ctx);
	const EVP_CIPHER* cipher = EVP_des_ecb();

	unsigned char key[] = "aCtZlHaUs";
	if (EVP_DecryptInit_ex(&ctx, cipher, NULL, key, NULL) != 1) return NULL;

	char* out = g_malloc0(512);
	int outl = 0;

	if (EVP_DecryptUpdate(&ctx, out, &outl, inter, len) != 1) {
		g_free(out);
		return NULL;
	}
	len = outl;
	if (EVP_DecryptFinal_ex(&ctx, out+len, &outl) != 1) {
		g_free(out);
		return NULL;
	}
	len += outl;

	EVP_CIPHER_CTX_cleanup(&ctx);

	out[len] = '\0';
	return out;
}

void* check_state(network_backends_t* bs) {
	MYSQL mysql;
	mysql_init(&mysql);
	guint i, tm = 1;
	sleep(1);

	while (TRUE) {
		GPtrArray* backends = bs->backends;
		guint len = backends->len;

		for (i = 0; i < len; ++i) {
			network_backend_t* backend = g_ptr_array_index(backends, i);
			if (backend == NULL || backend->state == BACKEND_STATE_OFFLINE) continue;

			gchar* ip = inet_ntoa(backend->addr->addr.ipv4.sin_addr);
			guint port = ntohs(backend->addr->addr.ipv4.sin_port);
			mysql_options(&mysql, MYSQL_OPT_CONNECT_TIMEOUT, &tm);
			mysql_real_connect(&mysql, ip, NULL, NULL, NULL, port, NULL, 0);

			if (mysql_errno(&mysql) == 1045 || mysql_errno(&mysql) == 0) backend->state = BACKEND_STATE_UP;
			else backend->state = BACKEND_STATE_DOWN;

			mysql_close(&mysql);
		}

		sleep(4);
	}
}

/**
 * init the plugin with the parsed config
 */
int network_mysqld_proxy_plugin_apply_config(chassis *chas, chassis_plugin_config *config) {
	network_mysqld_con *con;
	network_socket *listen_sock;
	chassis_private *g = chas->priv;
	guint i;

	if (!config->start_proxy) {
		return 0;
	}

	//if (!config->address) config->address = g_strdup(":4040");
    if (!config->address) {
        g_critical("%s: Failed to get bind address, please set by --proxy-address=<host:port>",
                G_STRLOC);
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
	con->config = config;

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
		if (-1 == network_backends_add(g->backends, config->backend_addresses[i], BACKEND_TYPE_RW)) {		
			return -1;
		}
	}
	
	for (i = 0; config->read_only_backend_addresses && config->read_only_backend_addresses[i]; i++) {
		if (-1 == network_backends_add(g->backends, config->read_only_backend_addresses[i], BACKEND_TYPE_RO)) {
			return -1;
		}
	}

	for (i = 0; config->client_ips && config->client_ips[i]; i++) {
		guint* sum = g_new0(guint, 1);
		char* token;
		while ((token = strsep(&config->client_ips[i], ".")) != NULL) {
			*sum = (*sum << 8) + atoi(token);
		}
		*sum = htonl(*sum);
		g_hash_table_add(config->ip_table, sum);
	}

	for (i = 0; config->lvs_ips && config->lvs_ips[i]; i++) {
		guint* lvs_ip = g_new0(guint, 1);
		*lvs_ip = inet_addr(config->lvs_ips[i]);
		g_hash_table_add(config->lvs_table, lvs_ip);
	}
	signal(SIGUSR1, handler);
	signal(SIGUSR2, handler);

	gchar* sql_log_filename = g_strdup_printf("%s/sql_%s.log", chas->log_path, chas->instance_name);
	config->sql_log = fopen(sql_log_filename, "a");
	if (config->sql_log == NULL) {
		g_critical("Failed to open %s", sql_log_filename);
		g_free(sql_log_filename);
		return -1; 
	}
	g_free(sql_log_filename);

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

		if ((user = strsep(&config->pwds[i], ":")) != NULL) {
			if ((pwd = strsep(&config->pwds[i], ":")) != NULL) {
				is_complete = TRUE;
			}
		}

		if (is_complete) {
			char* raw_pwd = decrypt(pwd);
			if (raw_pwd) {
				GString* hashed_password = g_string_new(NULL);
				network_mysqld_proto_password_hash(hashed_password, raw_pwd, strlen(raw_pwd));

				g_hash_table_insert(config->pwd_table, user, hashed_password);
			} else {
				g_critical("password decrypt failed");
				return -1;
			}
		} else {
			g_critical("incorrect password settings");
			return -1;
		}
	}

	if (!config->charset) config->charset = g_strdup("LATIN1");

	config->min_idle_connections = ceil(config->min_idle_connections * 1.0 / chas->event_thread_count);

	/* load the script and setup the global tables */
	network_mysqld_lua_setup_global(chas->priv->sc->L, g, chas);

	/**
	 * call network_mysqld_con_accept() with this connection when we are done
	 */

	event_set(&(listen_sock->event), listen_sock->fd, EV_READ|EV_PERSIST, network_mysqld_con_accept, con);
	event_base_set(chas->event_base, &(listen_sock->event));
	event_add(&(listen_sock->event), NULL);

	g_thread_create((GThreadFunc)check_state, g->backends, FALSE, NULL);

	return 0;
}

G_MODULE_EXPORT int plugin_init(chassis_plugin *p) {
	p->magic        = CHASSIS_PLUGIN_MAGIC;
	p->name         = g_strdup("proxy");
	p->version		= g_strdup(PACKAGE_VERSION);

	p->init         = network_mysqld_proxy_plugin_new;
	p->get_options  = network_mysqld_proxy_plugin_get_options;
	p->apply_config = network_mysqld_proxy_plugin_apply_config;
	p->destroy      = network_mysqld_proxy_plugin_free;

	return 0;
}

