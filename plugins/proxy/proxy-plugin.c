/* $%BEGINLICENSE%$
 Copyright (c) 2007, 2010, Oracle and/or its affiliates. All rights reserved.

eThis program is free software; you can redistribute it and/or
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

//#include "lib/sql-tokenizer.h"
#include "chassis-event-thread.h"
#include "network_mysqld_type.h"
#include "chassis-sharding.h"
#include "../../util/parse_config_file.h"
#include "../../util/id_generator_local.h"
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

    gboolean sql_safe_update;
    gboolean set_router_rule;
    gint max_connection_in_pool;

};

chassis_plugin_config *config = NULL;


static network_mysqld_lua_stmt_ret response_ok(const char *msg, network_mysqld_con *con) {
    if (msg) {
       g_message(msg); 
    }
    network_mysqld_con_send_ok_full(con->client, 0, 0, 0x0002, 0);
    return PROXY_SEND_RESULT;
}
static network_mysqld_lua_stmt_ret response_error(const gchar *errmsg, network_mysqld_con *con) {
    if (errmsg) {
        g_warning(errmsg);
    } 
    network_mysqld_con_send_error_full(con->client, errmsg, strlen(errmsg), ER_UNKNOWN_ERROR, "HY000");
    return PROXY_SEND_RESULT;
}
static network_mysqld_lua_stmt_ret send_response_to_client(const char *msg, network_mysqld_con *con, gboolean ok_response) {
    return TRUE == ok_response ? response_ok(msg, con) : response_error(msg, con); 
} 
static gboolean parse_sql(Parse *parse_obj, GString *packets, network_mysqld_con *con) {
    gboolean ret = FALSE;
    if (NULL == parse_obj || NULL == packets || NULL == con) {
        return ret;
    }

    gchar *err_msg = NULL;
    sqlite3ParseReset(parse_obj);
    sqlite3RunParser1(parse_obj, packets->str + 1, packets->len - 1, &err_msg);
    if (NULL != err_msg) {
        sqliteFree(err_msg); 
        return ret;
    }
    return TRUE; 
}
static gboolean is_empty_sql(Parse *parse_obj) {
    return parse_obj == NULL || parse_obj->parsed.curSize == 0 ? TRUE : FALSE;
}
static gboolean is_forbidden_sql(Parse *parse_obj) {
    gboolean ret = FALSE;
    if (NULL == parse_obj || parse_obj->parsed.curSize <= 0) {
        return TRUE;
    }  

    ParsedResultItem *item = &parse_obj->parsed.array[0]; 
    switch (item->sqltype) {
        case SQLTYPE_UPDATE:
            ret = item->result.updateObj->pWhere == NULL ? TRUE : FALSE;
            break;
        case SQLTYPE_DELETE: 
            ret = item->result.deleteObj->pWhere == NULL ? TRUE : FALSE;
            break;
        case SQLTYPE_SELECT: {
                                 Select *selectObj = item->result.selectObj;
                                 int i;
                                 for (i = 0; i < selectObj->pEList->nExpr; i++) {
                                     Expr *expr = selectObj->pEList->a[i].pExpr;
                                     if (expr->op == TK_FUNCTION && strncasecmp(expr->token.z, "SLEEP", expr->token.n) == 0) {
                                         ret = TRUE; 
                                         break;
                                     }
                                 }
                             }
                             break;
        case SQLTYPE_DROP_TABLE: 
        case SQLTYPE_CREATE_TABLE:
                             ret = TRUE;
                             break;
        default:
                             break;
    }
    return ret;
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
        g_string_free(com_change_user, TRUE);

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
        injection* inj = injection_new(INJECTION_INIT_DB, query);
        inj->resultset_is_needed = TRUE;
        network_mysqld_con_lua_t* st = con->plugin_con_state;
        g_queue_push_head(st->injected.queries, inj);
        g_string_free(query, TRUE); // TODO(dengyihao):must must free
    }
}

void modify_charset(parse_info_t *parse_info, network_mysqld_con* con) {
    if (parse_info->parse_obj == NULL) { return;  }

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

        injection* inj = injection_new(INJECTION_SET_CHARACTER_SET_CLIENT_SQL, query);
        inj->resultset_is_needed = TRUE;
        g_queue_push_head(st->injected.queries, inj);
        g_string_free(query, TRUE);
    }
    if (!is_set_results && !g_string_equal(client->charset_results, server->charset_results)) {
        GString* query = g_string_new_len(&cmd, 1);
        g_string_append(query, "SET CHARACTER_SET_RESULTS=");
        g_string_append(query, client->charset_results->str);
        g_string_assign(con->charset_results, client->charset_results->str);

        injection* inj = injection_new(INJECTION_SET_CHARACTER_SET_RESULTS_SQL, query);
        inj->resultset_is_needed = TRUE;
        g_queue_push_head(st->injected.queries, inj);
        g_string_free(query, TRUE);

    }

    if (!is_set_connection && !g_string_equal(client->charset_connection, server->charset_connection)) {
        GString* query = g_string_new_len(&cmd, 1);
        g_string_append(query, "SET CHARACTER_SET_CONNECTION=");
        g_string_append(query, client->charset_connection->str);
        g_string_assign(con->charset_connection, client->charset_connection->str);

        injection* inj = injection_new(INJECTION_SET_CHARACTER_SET_CONNECTION_SQL, query);
        inj->resultset_is_needed = TRUE;
        g_queue_push_head(st->injected.queries, inj);
        g_string_free(query, TRUE);

    }

}

static is_crud_sql(Parse *parse_obj) {
    if (parse_obj == NULL) { return FALSE;  }
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
            /*      } */
            /*  } */
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
static is_write_sql(Parse *parse_obj) {
    if (parse_obj == NULL) { return TRUE;  }
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

static check_flags(parse_info_t *parse_info, network_mysqld_con *con) {
    Parse *parse_obj = parse_info->parse_obj;
    con->is_in_select_calc_found_rows = FALSE; // select FOUND_ROWS()
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
}
static annotate_in_sql(Parse *parse_obj, gchar *annotate) {
    if (NULL == parse_obj) { return;}

    TokenArray *tokens = &parse_obj->tokens;
    if (tokens->curSize > 0) {
        gint i;
        for (i = 0; i < tokens->curSize; i++) {
            TokenItem *item = &tokens->array[i];
            if (item->tokenType == TK_COMMENT) {
                if (item->token.n > 4) {
                    memcpy(annotate, item->token.z + 2, item->token.n - 4);
                    annotate[item->token.n - 4] = 0;
                } 
            }
        }
    }
}

void stmt_execute_parameter_append(GString *message, network_mysqld_type_t *param){
    switch(param->type){
        case MYSQL_TYPE_TINY:
        case MYSQL_TYPE_SHORT:
        case MYSQL_TYPE_LONG:
        case MYSQL_TYPE_INT24:
        case MYSQL_TYPE_LONGLONG:
            {
                guint64 value;
                gboolean is_unsigned;
                param->get_int(param, &value, &is_unsigned);
                if (is_unsigned) {
                    g_string_append_printf(message, "%u", value);
                } else {
                    g_string_append_printf(message, "%d", value);
                }
                break;
            }
        case MYSQL_TYPE_FLOAT: /* 4 bytes */
            {
                double value;
                param->get_double(param, &value);
                g_string_append_printf(message, "%f", value);
                break;
            }
        case MYSQL_TYPE_DOUBLE: /* 8 bytes */
            {
                double value;
                param->get_double(param,&value);
                g_string_append_printf(message, "%f", value);
                break;
            }
        case MYSQL_TYPE_DATETIME:
        case MYSQL_TYPE_DATE:
        case MYSQL_TYPE_TIMESTAMP:
            {
                gchar *value;
                guint value_len;
                param->get_string(param, &value, (gsize *)&value_len);
                g_string_append_len(message,value, value_len);
                break;
            }
        case MYSQL_TYPE_TIME:
            {
                gchar *value;
                guint value_len;
                param->get_string(param, &value, (gsize *)&value_len);
                g_string_append_len(message, value, value_len);
                break;
            }
        case MYSQL_TYPE_NEWDECIMAL:
        case MYSQL_TYPE_BLOB:
        case MYSQL_TYPE_TINY_BLOB:
        case MYSQL_TYPE_MEDIUM_BLOB:
        case MYSQL_TYPE_LONG_BLOB:
        case MYSQL_TYPE_STRING:
        case MYSQL_TYPE_VAR_STRING:
        case MYSQL_TYPE_VARCHAR:
            {
                guint value_len;
                gchar *value = NULL;
                param->get_string_const(param, &value, &value_len);
                g_string_append_len(message, value, value_len);
                break;
            }
        case MYSQL_TYPE_NULL:
            break;
        default:
            break;
    }
}

static guint find_delimit_index(char*query, guint start, guint end, char delimit){
    guint i = start;
    for(; i <= end; i++){
        if(query[i] == delimit) return i;
    }
    return i;
}
gint prepare_delimit_replace_by_execute_parameter(GString *prepare_query, network_mysqld_stmt_execute_packet_t *execute_packet, char delimit, GString *result){
    if(!prepare_query || !execute_packet) return; 

    char *query = prepare_query->str;  
    guint len = prepare_query->len;
    guint offset = 0, i = 0;

    for(;i < execute_packet->params->len; i++){
        guint index = find_delimit_index(query, offset, len - 1, delimit); 
        if(index >= len){
            return -1;
        }   
        g_string_append_len(result, query + offset, index - 1 - offset + 1);

        network_mysqld_type_t *param = g_ptr_array_index(execute_packet->params, i);
        stmt_execute_parameter_append(result, param);

        offset = index + 1; 
        if(offset >= len){
            break;
        }
        if(i == execute_packet->params->len - 1 && find_delimit_index(query, offset, len - 1, delimit) != len){
            return -1; 
        }
    }

    if(offset < len){
        g_string_append_len(result, query + offset, len - 1 - offset + 1);
    }
    return 0;
}
void set_stmt_state(stmt_state_t *t, char stmt_state){
    t->type = stmt_state;
    t->prepare_response_first_ok_packet = FALSE;
}
void set_stmt_token(stmt_paras_t *t, gint token){
    t->token_id = token;
}
void set_stmt_id(stmt_state_t *t, guint32 stmt_id){
    t->stmt_id = stmt_id;
}
void stmt_close_clear(stmt_context_t *stmt_ctx, network_socket *recv_sock){
    if(stmt_ctx || !stmt_ctx->stmt || !recv_sock || !recv_sock->recv_queue) return;
    GHashTable *stmt = stmt_ctx->stmt; 
    network_packet stmt_close_packet;
    stmt_close_packet.data = g_queue_peek_head(recv_sock->recv_queue->chunks);
    stmt_close_packet.offset = 0;
    network_mysqld_proto_skip_network_header(&stmt_close_packet);
    network_mysqld_stmt_close_packet_t *stmt_close_variable = network_mysqld_stmt_close_packet_new();
    if(0 == network_mysqld_proto_get_stmt_close_packet(&stmt_close_packet, stmt_close_variable)){
        if(g_hash_table_lookup(stmt, &stmt_close_variable->stmt_id)){
            g_hash_table_remove(stmt, &stmt_close_variable->stmt_id);
        }
    }
    network_mysqld_stmt_close_packet_free(stmt_close_variable);
}

void stmt_execute_parameter_fetch(stmt_context_t *stmt_ctx, network_socket *recv_sock){
    if(!stmt_ctx || !stmt_ctx->stmt || !recv_sock || !recv_sock->recv_queue) return; 
    network_packet stmt_execute_packet;
    stmt_execute_packet.data = g_queue_peek_head(recv_sock->recv_queue->chunks);
    stmt_execute_packet.offset = 0;
    network_mysqld_proto_skip_network_header(&stmt_execute_packet);
    guint32 stmt_id;
    if(0 == network_mysqld_proto_get_stmt_execute_packet_stmt_id(&stmt_execute_packet, &stmt_id)){
        stmt_execute_packet.offset = 0;
        network_mysqld_proto_skip_network_header(&stmt_execute_packet);

        set_stmt_id(&stmt_ctx->stmt_state, stmt_id);

        stmt_paras_t *stmt_paras_value = g_hash_table_lookup(stmt_ctx->stmt, &stmt_id);
        if(stmt_paras_value){
            if(stmt_paras_value->stmt_execute_packet){
                network_mysqld_stmt_execute_packet_free(stmt_paras_value->stmt_execute_packet);
            }
            stmt_paras_value->stmt_execute_packet = network_mysqld_stmt_execute_packet_new();
            if(stmt_paras_value->stmt_prepare_ok_packet){
                network_mysqld_proto_get_stmt_execute_packet(&stmt_execute_packet,stmt_paras_value->stmt_execute_packet, stmt_paras_value->stmt_prepare_ok_packet->num_params);
            }
        }
    }
}  
void stmt_handle_wrapper_in_read_query(stmt_context_t *stmt_ctx, gchar type, network_socket *recv_sock) {
    stmt_state_t *stmt_state = &stmt_ctx->stmt_state;
    if (type < COM_STMT_PREPARE || type > COM_STMT_RESET) {
        stmt_state->type = 0; 
        return;
    }
    stmt_state->type = type; 
    if (type == COM_STMT_PREPARE) {
        stmt_state->prepare_response_first_ok_packet = FALSE;  // init prepare
    } else if (type == COM_STMT_EXECUTE) {
        stmt_execute_parameter_fetch(stmt_ctx, recv_sock); 
    } else if (type == COM_STMT_CLOSE) {
        stmt_close_clear(stmt_ctx, recv_sock);
    }
}
void log_sql_close_stmt(network_mysqld_con *con) {    
    float time_waste = 0.0;
    const char *s = "Close stmt";
    if (sql_log_type == OFF) return;

    if(!con || !con->stmt_context) return;

    GString* message = g_string_new(NULL);

    time_t t = time(NULL);
    struct tm* tm = localtime(&t);

    g_string_printf(message, "[%02d/%02d/%d %02d:%02d:%02d] user:%s C:%s S:", tm->tm_mon+1, tm->tm_mday, tm->tm_year+1900, tm->tm_hour, tm->tm_min, tm->tm_sec, con->client->response->username->str, con->client->src->name->str);
    if (con->server) { 
        g_string_append_printf(message, "%s OK %.3fms \"%s\" \n", con->server->dst->name->str, time_waste, s);
    } else {
        g_string_append_printf(message, "\"%s\" \n",s);
    }

    fwrite(message->str, message->len, 1, config->sql_log);
    g_string_free(message, TRUE);
    if (sql_log_type == REALTIME) fflush(config->sql_log);
}

void log_sql_stmt_execute(network_mysqld_con *con, injection* inj){
    if(sql_log_type == OFF || !con || !inj) { 
        return; 
    } 

    stmt_context_t *stmt_ctx = con->stmt_context;
    if (!stmt_ctx || stmt_ctx->stmt) {
        return;
    }

    const char *prefix = "Execute ";
    stmt_paras_t *stmt_value = g_hash_table_lookup(stmt_ctx->stmt, &stmt_ctx->stmt_state.stmt_id);
    if(!stmt_value || !stmt_value->stmt_execute_packet || !stmt_value->stmt_execute_packet->params) return;

    float latency_ms = (inj->ts_read_query_result_last - inj->ts_read_query)/1000.0;
    if ((gint)latency_ms < config->sql_log_slow_ms) return;

    GString* message = g_string_new(NULL);
    time_t t = time(NULL);
    struct tm* tm = localtime(&t);
    g_string_printf(message, "[%02d/%02d/%d %02d:%02d:%02d] user:%s C:%s S:", tm->tm_mon+1, tm->tm_mday, tm->tm_year+1900, tm->tm_hour, tm->tm_min, tm->tm_sec, con->client->response->username->str, con->client->src->name->str);

    GString *execute_query = g_string_new(NULL);
    gint replace_success = prepare_delimit_replace_by_execute_parameter(stmt_value->prepare_Query, stmt_value->stmt_execute_packet, '?', execute_query); 

    if (inj->qstat.query_status == MYSQLD_PACKET_OK && 0 == replace_success) {
        if(stmt_value->token_id == SQLTYPE_SELECT){
            g_string_append_printf(message, "%s OK %.3fms \"%s %s\" %d\n", con->server->dst->name->str, latency_ms, prefix, execute_query->str + 1, inj->rows);
        }else{
            g_string_append_printf(message, "%s OK %.3fms \"%s %s\"\n", con->server->dst->name->str, latency_ms, prefix, execute_query->str + 1);
        }
    } else {
        if (0 == replace_success) {
            g_string_append_printf(message, "%s ERR %.3fms \"%s %s\"\n", con->server->dst->name->str, latency_ms, prefix, execute_query->str + 1);
        }else{
            g_string_append_printf(message, "%s ERR %.3fms \"%s %s\"\n", con->server->dst->name->str, latency_ms, prefix, stmt_value->prepare_Query->str + 1);
        }
    }

    g_string_free(execute_query, TRUE);
    fwrite(message->str, message->len, 1, config->sql_log);
    g_string_free(message, TRUE);

    if (sql_log_type == REALTIME) fflush(config->sql_log);

}
void log_sql_stmt_prepare(network_mysqld_con *con, injection *inj){
    if (sql_log_type == OFF) return;
    const char *prefix = "Prepare ";
    float latency_ms = (inj->ts_read_query_result_last - inj->ts_read_query)/1000.0;
    if ((gint)latency_ms < config->sql_log_slow_ms) return;

    GString* message = g_string_new(NULL);

    time_t t = time(NULL);
    struct tm* tm = localtime(&t);
    g_string_printf(message, "[%02d/%02d/%d %02d:%02d:%02d] user:%s C:%s S:", tm->tm_mon+1, tm->tm_mday, tm->tm_year+1900, tm->tm_hour, tm->tm_min, tm->tm_sec, con->client->response->username->str, con->client->src->name->str);

    if (inj->qstat.query_status == MYSQLD_PACKET_OK) {
        g_string_append_printf(message, "%s OK %.3fms \"%s %s\"\n", con->server->dst->name->str, latency_ms, prefix, inj->query->str+1);
    } else {
        g_string_append_printf(message, "%s ERR %.3fms \"%s %s\"\n", con->server->dst->name->str, latency_ms, prefix, inj->query->str+1);
    }

    fwrite(message->str, message->len, 1, config->sql_log);
    g_string_free(message, TRUE);

    if (sql_log_type == REALTIME) fflush(config->sql_log);
}
static network_mysqld_lua_stmt_ret nosharding_query_handle(network_mysqld_con *con, parse_info_t *parse_info, GString *packets, gchar *group_name) {
    network_mysqld_lua_stmt_ret ret = PROXY_SEND_INJECTION;
    network_mysqld_con_lua_t *st = con->plugin_con_state; 
    stmt_context_t *stmt_ctx = con->stmt_context;
    Parse *parse_obj = parse_info->parse_obj;
    gchar type = packets->str[0];

    stmt_handle_wrapper_in_read_query(stmt_ctx, type, con->client);

    if (COM_STMT_CLOSE == stmt_ctx->stmt_state.type) {
        log_sql_close_stmt(con);
    } else {
        chassis_query_statistics_update(con, NULL, 1, 1);
    }

    injection *inj = NULL;
    inj = injection_new(INJECTION_ORIGINAL_SQL, packets); 
    if (parse_obj) {
        inj->token_id = (gint)(parse_obj->parsed.array[0].sqltype);  
    } 
    inj->resultset_is_needed = parse_info->is_write_sql;
    g_queue_push_tail(st->injected.queries, inj);
    st->injected.sent_resultset = 0;

    gchar annotate[50];
    annotate_in_sql(parse_info->parse_obj, annotate); 
    if (con->server == NULL) {
        g_message("<NO> backend and query is %s", packets->str); 
    } else {
        g_message("<HAVE> backend(%s:%d) and queryis %s", inet_ntoa(con->server->dst->addr.ipv4.sin_addr), con->server->dst->addr.ipv4.sin_port, packets->str); 
    }

    if (con->server == NULL) {
        if (!is_in_transaction_ctrl(con) &&  !is_in_stmt_ctrl(con->stmt_context) && is_in_table_lock(con->locks) == 0) {
            if (type == COM_QUERY) {
                network_conn_get(con, annotate, parse_info->is_write_sql, config->pwd_table[config->pwd_table_index], group_name); 
            } else if (type == COM_INIT_DB || type == COM_SET_OPTION || type == COM_FIELD_LIST) {
                network_conn_get(con, annotate, FALSE, config->pwd_table[config->pwd_table_index], group_name); 
            }
        }
        // forcer to router to default master  
        if (con->server == NULL) {
            network_conn_get(con, annotate, TRUE, config->pwd_table[config->pwd_table_index], group_name); 
        }
    }

    if (con->server) {
        g_message("<New> backend(%s:%d) and queryis %s and group_name is %s", inet_ntoa(con->server->dst->addr.ipv4.sin_addr), con->server->dst->addr.ipv4.sin_port, packets->str, group_name); 
    }
    modify_db(con);
    modify_charset(parse_info, con);
    modify_user(con);
    return ret;
}
static void dbgroups_ctx_create(GArray *hit_groups, GArray *arr,shard_type_t type) {
    if (type == SHARDING_TYPE_RANGE) {
        guint i; 
        for (i = 0; i < arr->len; i++) {
            group_range_map_t *t = &g_array_index(arr,  group_range_map_t, i);           
            dbgroup_context_t *dbgroup_ctx = dbgroup_context_new(t->group_name); 
            g_array_append_val(hit_groups, dbgroup_ctx);
        }
    } else if (type == SHARDING_TYPE_HASH) {
        guint i; 
        for (i = 0; i < arr->len; i++) {
            group_hash_map_t *t = &g_array_index(arr,  group_hash_map_t, i);           
            dbgroup_context_t* dbgroup_ctx = dbgroup_context_new(t->group_name); 
            g_array_append_val(hit_groups, dbgroup_ctx);
        }
    }
}
static void dbgroups_ctx_delete(GArray *hit_groups) { 
    if (hit_groups == NULL) { return; } 
    g_array_set_clear_func(hit_groups, dbgroup_context_free);
    g_array_free(hit_groups, TRUE); 
}
static network_socket_retval_t send_sql_to_dbgroup(dbgroup_context_t *dbgroup_ctx, GString *packets, network_mysqld_con *con, parse_info_t *parse_info) {
    injection_id_t injection_id = parse_info->is_write_sql ? INJECTION_SHARDING_WRITE_SQL : INJECTION_SHARDING_READ_SQL;
    injection* inj = injection_new(injection_id, packets);
    inj->resultset_is_needed = TRUE; 
    dbgroup_st_reset(dbgroup_ctx->st);   
    network_injection_queue_append(dbgroup_ctx->st->injected.queries, inj);
    if (NULL == dbgroup_ctx->server) {
        dbgroup_con_get(con, parse_info->is_write_sql, config->pwd_table[config->pwd_table_index], dbgroup_ctx);
    }
    if (NULL == dbgroup_ctx->server) {
        g_message("init server fail",__FILE__, __LINE__);
        return NETWORK_SOCKET_ERROR; 
    } 
    dbgroup_ctx->cur_injection_finished = FALSE;
    dbgroup_modify_db(con, dbgroup_ctx); 
    dbgroup_modify_charset(con, dbgroup_ctx);
    dbgroup_modify_user(con, dbgroup_ctx, config->pwd_table[config->pwd_table_index]);
    dbgroup_send_injection(dbgroup_ctx);
    sharding_network_mysqld_con_reset_command_response_state(dbgroup_ctx);         
    return NETWORK_SOCKET_SUCCESS;
}

static client_packets_clear(network_socket *client) {
    GString* packet = NULL;
    while((packet = g_queue_pop_head(client->recv_queue->chunks))) { 
        g_string_free(packet, TRUE); 
    }
} 

static gboolean sharding_query_handle(network_mysqld_con *con, 
        parse_info_t *parse_info, 
        GString **packets, 
        sharding_table_t *sharding_table_rule, 
        gchar *full_table_name,
        network_socket_retval_t *network_state, 
        gchar **group_name) {

    gboolean status = TRUE;
    *network_state = NETWORK_SOCKET_SUCCESS; 
    
    sharding_context_reset(con->sharding_context);
    g_message("merge_result size = %d", con->sharding_context->merge_result.func_array->len);
    if (sharding_table_rule->auto_inc && parse_info->is_write_sql) {
        glong v;
        gchar *id_key = g_strdup_printf("%s_%s", full_table_name, sharding_table_rule->primary_key->str);
        if (FALSE == (status = unique_id_generator(&con->srv->id_generators, id_key, &v))) {
            *network_state = NETWORK_SOCKET_ERROR;
            send_response_to_client("Proxy Waring - cannot generate global unique id, please check id generator", con, FALSE); 
            g_free(id_key);
            return status; 
        }
        g_free(id_key);

        if (FALSE == (status = rewrite_sql(parse_info, sharding_table_rule, packets, v))) {
            *network_state = NETWORK_SOCKET_ERROR;
            send_response_to_client("Proxy Waring - insert error, please check you insert sql", con, FALSE); 
            return status;
        }
    }   
    /*
     * to analyse sql to get groups
     */
    GArray *dbgroups_ctx = g_array_sized_new(FALSE, TRUE, sizeof(dbgroup_context_t*), 4); 
    sharding_result_t ret = sharding_get_dbgroups(dbgroups_ctx, parse_info, sharding_table_rule);   
    if (ret != SHARDING_RET_OK && ret != SHARDING_RET_ALL_SHARD) {
        dbgroups_ctx_delete(dbgroups_ctx); // iff one failed,terminate all this sharding process
        send_response_to_client("Proxy Waring - sharding sql error", con, FALSE); 
        *network_state = NETWORK_SOCKET_ERROR;
        return FALSE;
    }
    /*
     * cannot write two or more groups simultaneously and if it's write sql, router to nosharding_handle 
     */
    if (parse_info->is_write_sql) {
        if (dbgroups_ctx->len >= 2) {
            *network_state = NETWORK_SOCKET_ERROR;
            *group_name = NULL; 
            switch (get_sql_type(parse_info)) {
                case SQLTYPE_INSERT: 
                    send_response_to_client("Proxy Warning - cannot support insert on multi group, please specify sharding key", con, FALSE); 
                    break;
                case SQLTYPE_DELETE: 
                    send_response_to_client("Proxy Warning - cannot support delete on multi group, please specify sharding key", con, FALSE); 
                    break;
                case SQLTYPE_UPDATE:
                    send_response_to_client("Proxy Warning - cannot support update on multi group, please specify sharding key", con, FALSE); 
                    break;
                case SQLTYPE_REPLACE:
                    send_response_to_client("Proxy Warning - cannot support replace on multi group, please specify sharding key", con, FALSE); 
                    break;
                default:
                    send_response_to_client("Proxy Warning - cannot support write on multi group, please specify sharding key", con, FALSE); 
                    break; 
            }
        } else if (dbgroups_ctx->len == 1) {
            dbgroup_context_t *dbgroup_ctx = g_array_index(dbgroups_ctx, dbgroup_context_t*, 0);
            if (dbgroup_ctx != NULL) {
                *group_name = g_strdup(dbgroup_ctx->group_name); 
            } else {
                *group_name = NULL; 
            }
        } 
        dbgroups_ctx_delete(dbgroups_ctx);
        return FALSE;
    } 

    /*
     *  sharding handle
     * */
    guint i;     
    for (i = 0; i < dbgroups_ctx->len; i++) {
        dbgroup_context_t *dbgroup_ctx = g_array_index(dbgroups_ctx, dbgroup_context_t*, i); 
        if (send_sql_to_dbgroup(dbgroup_ctx, *packets, con, parse_info) != NETWORK_SOCKET_SUCCESS) {
            send_response_to_client("Proxy Warning - sharding sql error", con, FALSE); 
            dbgroups_ctx_delete(dbgroups_ctx); // iff one failed,terminate all this sharding process
            *network_state = NETWORK_SOCKET_ERROR; 
            return status; 
        } 
    }

    sharding_context_setup(con->sharding_context, dbgroups_ctx, parse_info->parse_obj); 
    g_message("%s:%d all dbgroups init sucessfully", __FILE__,__LINE__);
    g_array_free(dbgroups_ctx, TRUE); // without free ele of dbgroups_ctx 
    client_packets_clear(con->client);    
    con->state = CON_STATE_SHARDING_SEND_QUERY;
    return status; 
} 

void stmt_statitstics_update(network_mysqld_con *con, statistics_t *stat){
    if (!con || !con->stmt_context || !stat) return;
    stmt_context_t *stmt_ctx = con->stmt_context;
    stmt_paras_t *stmt_value = g_hash_table_lookup(stmt_ctx->stmt, &stmt_ctx->stmt_state.stmt_id);
    if (!stmt_value)  return;

    if (stmt_value->stmt_execute_packet && 0 == stmt_value->stmt_execute_packet->response_cnt) {
        if (stmt_value->token_id == SQLTYPE_SELECT) {
            stat->com_select += 1;
        } else if (stmt_value->token_id == SQLTYPE_DELETE) {
            stat->com_delete += 1;
        } else if (stmt_value->token_id == SQLTYPE_UPDATE) {
            stat->com_update += 1;
        } else if (stmt_value->token_id == SQLTYPE_REPLACE) {
            stat->com_replace += 1;
        } else if (stmt_value->token_id == SQLTYPE_INSERT) {
            stat->com_insert += 1;
        }
        stmt_value->stmt_execute_packet->response_cnt += 1;
    }
}

void chassis_query_statistics_update(network_mysqld_con *con, injection *inj, int success, int running){
    if (!con || !con->srv || !con->srv->statistics) {
        return;
    }
    statistics_t *stat =  con->srv->statistics;
    g_mutex_lock(&stat->mutex);
    if (!inj) {
        if (running) {
            stat->threads_running += 1;
            con->is_running = TRUE;
        } else {
            if (stat->threads_running > 0) {
                stat->threads_running = con->is_running ? stat->threads_running - 1: stat->threads_running;
                con->is_running = FALSE;
            }
        }
        g_mutex_unlock(&stat->mutex);
        return;
    }

    if (MYSQLD_PACKET_ERR == inj->qstat.query_status) {
        stat->com_error += 1;
        g_mutex_unlock(&stat->mutex);
        return;
    }

    stmt_state_t *stmt_state = &con->stmt_context->stmt_state;
    if (stmt_state->type == COM_STMT_EXECUTE) {
        stmt_statitstics_update(con, stat);
    } 
    if (inj->token_id == SQLTYPE_SELECT) {
        stat->com_select += 1;
    } else if (inj->token_id == SQLTYPE_DELETE) {
        stat->com_delete += 1;
    } else if (inj->token_id == SQLTYPE_UPDATE) {
        stat->com_update += 1;
    } else if (inj->token_id == SQLTYPE_REPLACE) {
        stat->com_replace += 1;
    } else if (inj->token_id == SQLTYPE_INSERT) {
        stat->com_insert += 1;
    }
    g_mutex_unlock(&stat->mutex);
}

// update statictics
void chassis_conn_statistics_update(network_mysqld_con *con, int connect){
    if (!con || !con->srv || !con->srv->statistics) {
        return;
    }
    statistics_t *t = con->srv->statistics;
    g_mutex_lock(&t->mutex);
    if (connect == 1) {
        t->threads_connected += 1;
    } else {
        if (t->threads_connected > 0) {
            t->threads_connected -= 1;
        }
        if (t->threads_running > 0) {
            t->threads_running = con->is_running ? t->threads_running - 1: t->threads_running;
            con->is_running = FALSE;
        }
    }
    g_mutex_unlock(&t->mutex);
}

//  reset statistics
void chassis_statistics_reset(chassis *chas){
    if (!chas || !chas->statistics) {
        return;
    }
    statistics_t *t = chas->statistics;
    g_mutex_lock(&t->mutex);
    t->com_select = 0;
    t->com_insert = 0;
    t->com_delete = 0;
    t->com_replace = 0;
    t->com_update = 0;
    t->threads_connected = 0;
    g_mutex_unlock(&t->mutex);
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
        if (st->backend->b->state != BACKEND_STATE_OFFLINE) st->backend->b->state = BACKEND_STATE_DOWN;
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

/**
 * for GUI tools compatibility, who send 'use xxxx' than com_init_db to change database
 * return COM_INIT_DB packets or origin packets
 */
//GString* convert_use_database2com_init_db(char type, GString *origin_packets, GPtrArray *tokens) {
//    if (type == COM_QUERY) {
//        sql_token **ts = (sql_token**)(tokens->pdata);
//        guint tokens_len = tokens->len;
//        if (tokens_len > 1) {
//            guint i = 1;
//            sql_token_id token_id = ts[i]->token_id;
//
//            while (token_id == TK_COMMENT && ++i < tokens_len) {
//                token_id = ts[i]->token_id;
//            }
//
//            if (token_id == TK_SQL_USE && (i+1) < tokens_len && ts[i+1]->token_id == TK_LITERAL) {
//                g_string_truncate(origin_packets, 0);
//                g_string_append_c(origin_packets, COM_INIT_DB);
//                g_string_append_printf(origin_packets, "%s", ts[i+1]->text->str);
//            }
//        }
//    }
//
//    return origin_packets;
//}

gboolean is_prepare_first_ok_packet(stmt_state_t *t) {
    if (!t) { return FALSE; }
    return t->prepare_response_first_ok_packet == FALSE;
}
gboolean is_stmt_prepare(stmt_state_t *t) {
    return t->type == COM_STMT_PREPARE;
}
gboolean is_stmt_execute(stmt_state_t *t) {
    return t->type == COM_STMT_EXECUTE; 
}
gboolean is_stmt_close(stmt_state_t *t) {
    return t->type == COM_STMT_CLOSE;
}
gboolean is_com_query(stmt_state_t *t) {
    return t->type == COM_QUERY;
}

/*
 * get stmt_prepare's parameter 
 */
void stmt_prepare_parameter_fetch(stmt_context_t *stmt_ctx, network_socket *recv_sock, injection *inj){
    if(stmt_ctx || !recv_sock || !recv_sock->recv_queue || !inj) return;

    network_packet stmt_prepare_packet;
    stmt_prepare_packet.data = g_queue_peek_tail(recv_sock->recv_queue->chunks);
    stmt_prepare_packet.offset = 0;
    network_mysqld_proto_skip_network_header(&stmt_prepare_packet);

    network_mysqld_stmt_prepare_ok_packet_t *prepare_ok_packet = network_mysqld_stmt_prepare_ok_packet_new();
    if (0 == network_mysqld_proto_get_stmt_prepare_ok_packet(&stmt_prepare_packet, prepare_ok_packet)){
        stmt_ctx->stmt_state.prepare_response_first_ok_packet = TRUE;

        stmt_paras_t *stmt_paras = g_new0(stmt_paras_t, 1);
        stmt_paras->stmt_prepare_ok_packet = prepare_ok_packet;
        stmt_paras->prepare_Query = g_string_new(inj->query->str);
        guint32 *hash_int_ptr = g_new(guint32, 1);
        *hash_int_ptr = prepare_ok_packet->stmt_id;

        set_stmt_id(&stmt_ctx->stmt_state, *hash_int_ptr);
        set_stmt_token(stmt_paras, inj->token_id);
        g_hash_table_insert(stmt_ctx->stmt, hash_int_ptr, stmt_paras);
    } else {
        network_mysqld_stmt_prepare_ok_packet_free(prepare_ok_packet);
    }
}
/**
 * stmt wrapper 
 **/
void stmt_handle_wrapper_in_read_result(stmt_context_t *stmt_ctx, network_socket *recv_sock, injection *inj) {
    stmt_state_t *state = &stmt_ctx->stmt_state;
    if (is_stmt_prepare(state) || is_prepare_first_ok_packet(state)) {
        stmt_prepare_parameter_fetch(stmt_ctx, recv_sock, inj);
    } 
}
/*
 * in server-side stmt(prepare)
 */
gboolean is_in_stmt_ctrl(stmt_context_t *ctx) {
    if (NULL == ctx) { 
        return FALSE; 
    } 
    gchar type = ctx->stmt_state.type;
    return type >= COM_STMT_PREPARE && type <= COM_STMT_RESET || 0 != g_hash_table_size(ctx->stmt);
} 

/*
 * in transaction or not 
 */
gboolean is_in_transaction_ctrl(network_mysqld_con *con) {
    return con->is_in_transaction || con->is_not_autocommit ? TRUE: FALSE;
}
/* 
 * in lock or not 
 */
gboolean is_in_table_lock(GHashTable *locks) {
    return 0 != g_hash_table_size(locks);
}

NETWORK_MYSQLD_PLUGIN_PROTO(proxy_read_query) {
    GString *packet;
    network_socket *recv_sock, *send_sock;
    network_mysqld_con_lua_t *st = con->plugin_con_state;
    int proxy_query = 1;
    network_mysqld_lua_stmt_ret ret;

    NETWORK_MYSQLD_CON_TRACK_TIME(con, "proxy::ready_query::enter");

    recv_sock = con->client;
    //st->injected.sent_resultset = 0;

    NETWORK_MYSQLD_CON_TRACK_TIME(con, "proxy::ready_query::enter_lua");
    network_injection_queue_clear(st->injected.queries);

    GString* packets = g_string_new(NULL);
    int i;
    for (i = 0; NULL != (packet = g_queue_peek_nth(recv_sock->recv_queue->chunks, i)); i++) {
        g_string_append_len(packets, packet->str + NET_HEADER_SIZE, packet->len - NET_HEADER_SIZE);
    }

    gchar *group_name = NULL;
    parse_info_t parse_info;
    gchar type = packets->str[0];

    if (type == COM_QUIT || type == COM_PING) {
        ret = send_response_to_client(NULL, con, TRUE); 
    } else if (COM_QUERY == type) {

        Parse *parse_obj = chassis_thread_lemon_parse_obj(con->srv);
        if (FALSE == parse_sql(parse_obj, packets, con)) {
            ret = send_response_to_client("Proxy Warning - parse sql error, please check you sql", con, FALSE); 
        } else if (is_empty_sql(parse_obj)) {
            ret = send_response_to_client("Proxy Warning - empty sql", con, FALSE); 
        } else if(is_forbidden_sql(parse_obj) && FALSE == con->srv->router_para->sql_safe_update) {
            ret = send_response_to_client("Proxy Warning - Forbidden sql",con, FALSE); 
        } 

        if (PROXY_SEND_RESULT == ret) {
            g_string_free(packets, TRUE);
            sqlite3ParseClean(parse_obj);
            goto end;
        }

        parse_info_init(&parse_info, parse_obj, (gchar *)packets->str + 1, packets->len - 1); 

        check_flags(&parse_info, con); 
        /**
         *  sharding handle
         * */
        gchar *full_table_name = NULL; 
        sharding_table_t *sharding_table_rule = sharding_lookup_table_rule(con->srv->nodes->sharding_tables, &parse_info, con->client->default_db->str, &full_table_name);
        if (sharding_table_rule != NULL && !is_in_transaction_ctrl(con) && !is_in_stmt_ctrl(con->stmt_context) && !is_in_table_lock(con->locks)) {
            network_socket_retval_t network_state; 
            gboolean is_sharding_sql = sharding_query_handle(con, &parse_info, &packets, sharding_table_rule, full_table_name, &network_state, &group_name); 
            if (NULL != full_table_name) {
                g_free(full_table_name);
                full_table_name = NULL;
            }

            if (TRUE == is_sharding_sql) {
                g_free(group_name);
                group_name = NULL;
                g_string_free(packets, TRUE);
                packets = NULL; 
                parse_info_clear(&parse_info);

                if (NETWORK_SOCKET_ERROR == network_state) {
                    ret = PROXY_SEND_RESULT;
                    goto end; 
                } else {
                    return network_state;
                } 
            } else {
                if (NETWORK_SOCKET_ERROR == network_state) {
                    g_free(group_name);
                    group_name = NULL;
                    g_string_free(packets, TRUE);
                    packets = NULL; 
                    parse_info_clear(&parse_info);
                    ret = PROXY_SEND_RESULT;
                    goto end;
                }  
            }
        }  
        // nosharding handle
        ret = nosharding_query_handle(con, &parse_info, packets, group_name);
        parse_info_clear(&parse_info);
    } else {
        parse_info_init(&parse_info, NULL, NULL, 0); 
        ret = nosharding_query_handle(con, &parse_info, packets, group_name);
        parse_info_clear(&parse_info);
    }

    g_free(group_name);
    group_name = NULL;
    g_string_free(packets, TRUE);
    packets = NULL; 
    NETWORK_MYSQLD_CON_TRACK_TIME(con, "proxy::ready_query::leave_lua");


    /**
     * if we disconnected in read_query_result() we have no connection open
     * when we try to execute the next query
     *
     * for PROXY_SEND_RESULT we don't need a server
     */
end:
    if (ret != PROXY_SEND_RESULT &&
            con->server == NULL) {
        chassis_query_statistics_update(con, NULL, 0, 0);

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

                                    break; 
                                }
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
        chassis_query_statistics_update(con, NULL, 0, 0);
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
        network_injection_queue_clear(st->injected.queries);
    }

    if (st->injected.queries->length == 0) {
        /* we have nothing more to send, let's see what the next state is */
        con->state = CON_STATE_READ_QUERY;
        chassis_query_statistics_update(con, NULL, 0, 0);
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
NETWORK_MYSQLD_PLUGIN_PROTO(proxy_sharding_read_query_result) {
    network_packet packet; 
    network_socket *client, *server;
    dbgroup_context_t *dbgroup_ctx = con->sharding_context->ctx_in_use;
    network_mysqld_con_lua_t *st = dbgroup_ctx->st;
    injection *inj = NULL;
    gint is_finished = FALSE;    

    NETWORK_MYSQLD_CON_TRACK_TIME(con, "proxy::ready_query_result::enter");

    server = dbgroup_ctx->server;
    client = con->client; 

    if (st->injected.queries->length) {
        inj = g_queue_peek_head(st->injected.queries); 
        if (inj && 0 == inj->ts_read_query_result_first) {
            inj->ts_read_query_result_first = chassis_get_rel_microseconds();
        } 
    }

    packet.data = g_queue_peek_tail(server->recv_queue->chunks);
    packet.offset = 0;
    is_finished = sharding_mysqld_get_query_result_meta(dbgroup_ctx, client, &packet);
    if (1 == is_finished) {
        inj = g_queue_pop_head(st->injected.queries);
        if (inj->id == INJECTION_SHARDING_WRITE_SQL) {
            //TODO(dengyihao): write_sql can only hit one group and cannot 
        } else if (inj->id == INJECTION_SHARDING_READ_SQL) {
            network_mysqld_lua_stmt_ret ret = sharding_merge_query_result(con, inj); 
            g_message("%s:%d send_query_result ret(%d)", __FILE__, __LINE__, ret);
            if (ret == PROXY_SEND_RESULT) {
                g_message("%s:%d send_query_result", __FILE__, __LINE__);
                con->state = CON_STATE_SHARDING_SEND_QUERY_RESULT; // we have get all result from multi backends 
            } 
        } 
        /* free intermediate packet*/ 
        injection_free(inj);
        GString *p = NULL;
        while ((p = g_queue_pop_head(server->recv_queue->chunks))) { 
            g_string_free(p, TRUE);
        }
        /* continue send other querys in quries*/
        if (0 != st->injected.queries->length) {
            dbgroup_send_injection(dbgroup_ctx); 
            return NETWORK_SOCKET_SHARDING_SEND_NEXT_INJECTION;
        } else {
            sharding_network_group_add_connection(con, dbgroup_ctx);
        }  
    } else if (0 == is_finished) {
        return NETWORK_SOCKET_WAIT_MORE_PACKET;
    } else if (-1 == is_finished) {
        return NETWORK_SOCKET_ERROR; 
    }
    return NETWORK_SOCKET_SUCCESS;
} 

NETWORK_MYSQLD_PLUGIN_PROTO(proxy_sharding_send_query_result) {
    sharding_context_reset(con->sharding_context);
    con->state = CON_STATE_READ_QUERY;
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
        if (con->merge_res->rows->len >= con->merge_res->limit)
            break;
        res_row = res_row->next;
    }

    proxy_resultset_free(res);
}


void log_sql(network_mysqld_con* con, injection* inj) {
    if (sql_log_type == OFF) return;

    float latency_ms = (inj->ts_read_query_result_last - inj->ts_read_query)/1000.0;
    if ((gint)latency_ms < config->sql_log_slow_ms) return;

    GString* message = g_string_new(NULL);

    time_t t = time(NULL);
    struct tm* tm = localtime(&t);
    g_string_printf(message, "[%02d/%02d/%d %02d:%02d:%02d] user:%s C:%s S:", tm->tm_mon+1, tm->tm_mday, tm->tm_year+1900, tm->tm_hour, tm->tm_min, tm->tm_sec, con->client->response->username->str, con->client->src->name->str);

    if (inj->qstat.query_status == MYSQLD_PACKET_OK) {
        if(inj->token_id == SQLTYPE_SELECT){
            g_string_append_printf(message, "%s OK %.3fms \"%s\" %d\n", con->server->dst->name->str, latency_ms, inj->query->str+1, inj->rows);
        }else{
            g_string_append_printf(message, "%s OK %.3fms \"%s\"\n", con->server->dst->name->str, latency_ms, inj->query->str+1);
        }
    } else {
        g_string_append_printf(message, "%s ERR %.3fms \"%s\"\n", con->server->dst->name->str, latency_ms, inj->query->str+1);
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
    stmt_context_t *stmt_ctx = con->stmt_context;
    injection *inj = NULL;

    NETWORK_MYSQLD_CON_TRACK_TIME(con, "proxy::ready_query_result::enter");

    recv_sock = con->server;
    send_sock = con->client;


    //int len = g_queue_get_length(recv_sock->recv_queue->chunks);
    if (0 != st->injected.queries->length) {
        inj = g_queue_peek_head(st->injected.queries);
    }
    if (inj && inj->ts_read_query_result_first == 0) {
        /**
         * log the time of the first received packet
         */
        inj->ts_read_query_result_first = chassis_get_rel_microseconds();
    }
    stmt_handle_wrapper_in_read_result(con->stmt_context, recv_sock, inj);
    /* check if the last packet is valid */
    packet.data = g_queue_peek_tail(recv_sock->recv_queue->chunks);
    packet.offset = 0;

    is_finished = network_mysqld_proto_get_query_result(&packet, con);
    if (is_finished == -1){
        return NETWORK_SOCKET_ERROR; /* something happend, let's get out of here */
    }

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

                con->is_in_transaction = com_query->server_status & SERVER_STATUS_IN_TRANS;
                con->is_not_autocommit = !(com_query->server_status & SERVER_STATUS_AUTOCOMMIT);
            }
            inj->ts_read_query_result_last = chassis_get_rel_microseconds();
        }
        chassis_query_statistics_update(con, inj, is_finished, 0);

        network_mysqld_queue_reset(recv_sock); /* reset the packet-id checks as the server-side is finished */

        NETWORK_MYSQLD_CON_TRACK_TIME(con, "proxy::ready_query_result::enter_lua");
        GString* p;
        if (0 != st->injected.queries->length) {
            inj = g_queue_pop_head(st->injected.queries);
            char* str = inj->query->str + 1;
            if (inj->id == INJECTION_ORIGINAL_SQL) {
                if (is_com_query(&stmt_ctx->stmt_state)) {
                    log_sql(con, inj);
                } else if (is_stmt_prepare(&stmt_ctx->stmt_state)) {
                    log_sql_stmt_prepare(con, inj); 
                } else if (is_stmt_execute(&stmt_ctx->stmt_state)) {
                    log_sql_stmt_execute(con, inj);
                }
                ret = PROXY_SEND_RESULT;
            } else if (inj->id == INJECTION_READ_SQL) { // sharding table
                log_sql(con, inj);

                merge_res_t* merge_res = con->merge_res;
                if (inj->qstat.query_status == MYSQLD_PACKET_OK && merge_res->rows->len < merge_res->limit) merge_rows(con, inj);

                if ((++merge_res->sub_sql_exed) < merge_res->sub_sql_num) {
                    ret = PROXY_IGNORE_RESULT;
                } else {
                    network_injection_queue_clear(st->injected.queries);
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
            } else if (inj->id == INJECTION_WRITE_SQL) {
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
                        network_injection_queue_clear(st->injected.queries);
                        while ((p = g_queue_pop_head(recv_sock->recv_queue->chunks))) g_string_free(p, TRUE);
                        ret = PROXY_SEND_RESULT;
                    }
                    network_mysqld_ok_packet_free(ok_packet);
                } else {
                    ret = PROXY_SEND_RESULT;
                }
            } else {
                ret = PROXY_IGNORE_RESULT;

                if (inj->id == INJECTION_MODIFY_USER_SQL) {
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
                    /* if (!con->is_in_transaction || (inj->qstat.server_status & SERVER_STATUS_IN_TRANS)) { */
                    /* 	con->is_in_transaction = (inj->qstat.server_status & SERVER_STATUS_IN_TRANS); */
                    /* } else { */
                    /* 	if (strcasestr(str, "COMMIT") == str || strcasestr(str, "ROLLBACK") == str) con->is_in_transaction = FALSE; */
                    /* } */

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
                    } else {
                        if (con->resultset_is_needed) {
                            while ((p = g_queue_pop_head(recv_sock->recv_queue->chunks))) g_string_free(p, TRUE);
                        }
                    }

                    if (!is_in_transaction_ctrl(con)                            
                            && !is_in_stmt_ctrl(con->stmt_context)
                            && !is_in_table_lock(con->locks) 
                            && !con->is_in_select_calc_found_rows  //   
                            /*&& !have_last_insert_id*/) {
                        g_message("<RET> backend(%s:%d) and queryis %s", inet_ntoa(con->server->dst->addr.ipv4.sin_addr), con->server->dst->addr.ipv4.sin_port, inj->query->str);
                        network_connection_pool_lua_add_connection(con);

                    } 

                    break;
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
    guint i;

    chassis_conn_statistics_update(con, 1);
    guint client_ip = con->client->src->addr.ipv4.sin_addr.s_addr;
    if (!online && g_hash_table_contains(config->lvs_table, &client_ip)) {
        network_mysqld_con_send_error_full(con->client, C("Proxy Warning - Offline Now"), ER_UNKNOWN_ERROR, "07000");
        return NETWORK_SOCKET_SUCCESS;
    } else {
        GHashTable *ip_table = con->srv->nodes->ip_table[con->srv->nodes->ip_table_index];
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
        } else {
            network_mysqld_con_send_error_full(con->client, C("Proxy Warning - IP Forbidden"), ER_UNKNOWN_ERROR, "07000");
            g_warning("Forbidden IP: %s", con->client->src->name->str);
            return NETWORK_SOCKET_SUCCESS;
        }
    }
    network_mysqld_auth_challenge *challenge = network_mysqld_auth_challenge_new();

    challenge->protocol_version = 0;
    challenge->server_version_str = g_strdup("5.6.19-log");
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
    //chassis_process_list_delete(con->srv, con->client);
    chassis_conn_statistics_update(con, 0);

    if (st == NULL) return NETWORK_SOCKET_SUCCESS;

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

    con->plugins.con_sharding_read_query_result = proxy_sharding_read_query_result;
    con->plugins.con_sharding_send_query_result = proxy_sharding_send_query_result;

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

void string_free(gpointer s) {
    GString *t = s;
    g_string_free(t, TRUE);
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
    config->pwd_table_index = 0;
    config->sql_log = NULL;
    config->sql_log_type = NULL;
    config->charset = NULL;
    config->sql_log_slow_ms = 0;
    config->sql_safe_update = TRUE;
    config->max_connection_in_pool = 10000;
    config->set_router_rule = FALSE; 

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
    config->ip_table[0] = NULL;
    g_hash_table_remove_all(config->ip_table[1]);
    g_hash_table_destroy(config->ip_table[1]);
    config->ip_table[1] = NULL;

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

    if (config->sql_log) fclose(config->sql_log);
    if (config->sql_log_type) g_free(config->sql_log_type);

    if (config->charset) g_free(config->charset);

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

        {"sql-safe-update", 0, 0, G_OPTION_ARG_NONE, NULL, "whether to forbidden sqls like deleter/update without where(default: false)", NULL },
        { "max-connection-in-pool", 0, 0, G_OPTION_ARG_INT, NULL, "max connection between proxy and mysql (default: 10000)", NULL },
        { "set-router-rule", 0, 0, G_OPTION_ARG_NONE, NULL, "router sqls like(like set names) to master/slave(default: slave)", NULL  },

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
    config_entries[i++].arg_data = &(config->sql_safe_update);
    config_entries[i++].arg_data = &(config->max_connection_in_pool);
    config_entries[i++].arg_data = &(config->set_router_rule);

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

gpointer check_state(network_nodes_t *nodes) {
    GPtrArray *backends = nodes->group_backends;
    GPtrArray *raw_pwds = nodes->raw_pwds;
    MYSQL mysql;
    mysql_init(&mysql);
    guint i, tm = 1;
    sleep(1);
    while (TRUE) {
        g_mutex_lock(&nodes->nodes_mutex); 
        guint len = backends->len;
        for (i = 0; i < len; ++i) {
            network_group_backend_t *gp_backend = g_ptr_array_index(backends, i);
            if (NULL == gp_backend) {
                continue;
            }
            network_backend_t *backend = gp_backend->b;
            g_mutex_lock(&backend->mutex);
            if (backend == NULL || backend->state == BACKEND_STATE_UP || backend->state == BACKEND_STATE_OFFLINE) {
                g_mutex_unlock(&backend->mutex);
                continue;
            }

            gchar *ip = inet_ntoa(backend->addr->addr.ipv4.sin_addr);
            guint port = ntohs(backend->addr->addr.ipv4.sin_port);
            mysql_options(&mysql, MYSQL_OPT_CONNECT_TIMEOUT, &tm);

            gchar *user = NULL; 
            gchar *pwd = g_malloc(512);
            if (raw_pwds->len > 0) {
                gchar *user_pwd = g_ptr_array_index(raw_pwds, 0);
                gchar *pos = strchr(user_pwd, ':');
                g_assert(pos);
                user = g_strndup(user_pwd, pos-user_pwd);
                if(0 == decrypt_new(pos+1, pwd)){ 
                    g_free(pwd); 
                    g_mutex_unlock(&backend->mutex);
                    continue;
                }
            }
            if (mysql_real_connect(&mysql, ip, user, pwd, NULL, port, NULL, 0) && mysql_query(&mysql, "SELECT 1") == 0) {
                backend->state = BACKEND_STATE_UP;
            } else if (backend->state == BACKEND_STATE_UNKNOWN) {
                backend->state = BACKEND_STATE_DOWN;
            }

            if (BACKEND_STATE_UP != backend->state) {
                network_connection_pools_free(backend->pools); // add mutex or not
                backend->pools = NULL;
            } else {
                if (backend->pools == NULL) {
                    for (i = 0; i <= nodes->event_thread_count; ++i) {
                        network_connection_pool* pool = network_connection_pool_new();
                        g_ptr_array_add(backend->pools, pool);
                    }
                }
            }
            g_mutex_unlock(&backend->mutex);
            mysql_close(&mysql);
            g_free(user);
            g_free(pwd);
        }
        g_mutex_unlock(&nodes->nodes_mutex);
        sleep(4);
    }

    return NULL;
}
/**
 * init the plugin with the parsed config
 */

int network_mysqld_proxy_plugin_apply_config(chassis *chas, chassis_config_t  *oldconfig, chassis_plugin_config *plugin_config) {
    network_mysqld_con *con;
    network_socket *listen_sock;
    guint i;

    //if (!config->start_proxy) {
    //    return 0;
    //}

    if(!oldconfig->proxy_address){
        g_critical("%s: Failed to get bind address, please set by --proxy-address=<host:port>", G_STRLOC);
        return -1;
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

    if (0 != network_address_set_address(listen_sock->dst, oldconfig->proxy_address)) {
        return -1;
    }
    if (0 != network_socket_bind(listen_sock)) {
        return -1;
    }
    g_message("proxy listening on port %s", oldconfig->proxy_address);

    //set_forbidden_sql(chas->nodes->forbidden_sql, oldconfig->forbiddenSQL);
    set_nodes_backends(chas->nodes, oldconfig->nodes);
    set_dynamic_router_para(chas->router_para, oldconfig); 
    set_nodes_comm_para(chas->nodes, chas->router_para, oldconfig->forbiddenSQL); 
    set_sharding_tables(chas->nodes, oldconfig->schema);

    for (i = 0; oldconfig->client_ips && i < oldconfig->client_ips->len; i++) {
        gchar *client = g_ptr_array_index(oldconfig->client_ips, i);
        save_client_ip(chas->nodes, client);
        guint* sum = g_new0(guint, 1);
        char* token;
        while ((token = strsep(&client, ".")) != NULL) {
            *sum = (*sum << 8) + atoi(token);
        }
        *sum = htonl(*sum);
        g_hash_table_add(config->ip_table[config->ip_table_index], sum);
    }

    chas->nodes->ip_table = config->ip_table;
    chas->nodes->ip_table_index = config->ip_table_index;

    for (i = 0; oldconfig->lvs_ips && i < oldconfig->lvs_ips->len; i++) {
        gchar *lvs = g_ptr_array_index(oldconfig->lvs_ips, i);
        guint* lvs_ip = g_new0(guint, 1);
        *lvs_ip = inet_addr(lvs);
        g_hash_table_add(config->lvs_table, lvs_ip);
    }
    signal(SIGUSR1, handler);
    signal(SIGUSR2, handler);

    if (oldconfig->sql_log_type) {
        if (strcasecmp(oldconfig->sql_log_type, "ON") == 0) {
            sql_log_type = ON;
        } else if (strcasecmp(oldconfig->sql_log_type, "REALTIME") == 0) {
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

    for (i = 0; oldconfig->pwds && i < oldconfig->pwds->len; i++) {
        char *user = NULL, *pwd = NULL;
        gboolean is_complete = FALSE;
        gchar *t = g_ptr_array_index(oldconfig->pwds, i); 
        gchar *p = g_strdup(t);
        if ((user = strsep(&p, ":")) != NULL) {
            if ((pwd = strsep(&p, ":")) != NULL) {
                is_complete = TRUE;
            }
        }

        if (is_complete) {
            gchar *raw_pwd = g_malloc(512);
            if(decrypt_new(pwd, raw_pwd)){
                GString* hashed_password = g_string_new(NULL);
                network_mysqld_proto_password_hash(hashed_password, raw_pwd, strlen(raw_pwd));
                g_hash_table_insert(config->pwd_table[config->pwd_table_index], g_strdup(user), hashed_password);
                save_user_info(chas->nodes, user, pwd);
            } else {
                g_critical("password decrypt failed");
                g_free(p);
                g_free(raw_pwd);
                return -1;
            }
            g_free(p);
            g_free(raw_pwd);
        } else {
            g_critical("incorrect password settings");
            g_free(p);
            return -1;
        }
    }
    chas->nodes->pwd_table = config->pwd_table;
    chas->nodes->pwd_table_index = &(config->pwd_table_index);

    /* load the script and setup the global tables */
    if(oldconfig->charset){
        chas->proxy_config->charset = oldconfig->charset;
    }
    //TODO:dengyihao show global variable 
    network_mysqld_lua_setup_global(chas->sc->L, chas); 

    /**
     * call network_mysqld_con_accept() with this connection when we are done
     */

    event_set(&(listen_sock->event), listen_sock->fd, EV_READ|EV_PERSIST, network_mysqld_con_accept, con);
    event_base_set(chas->event_base, &(listen_sock->event));
    event_add(&(listen_sock->event), NULL);

    //g_thread_create((GThreadFunc)check_state, chas->nodes, FALSE, NULL);
    g_thread_new("check backends state", (GThreadFunc)check_state, chas->nodes);
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

    return 0;
}
