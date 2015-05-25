/* $%BEGINLICENSE%$
 Copyright (c) 2012, 2015, Qihoo 360 and/or its affiliates. All rights reserved.

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

#ifndef _CHASSIS_SHARDING_H_
#define _CHASSIS_SHARDING_H_

#include <lemon/sqliteInt.h>
#include <glib.h>

#include "network-injection.h"
#include "network-mysqld.h"
#include "network-backend.h"
#include "network-mysqld-lua.h"
#include "network-mysqld-proto.h"
#include "network-mysqld-packet.h"

typedef struct {
     int group_id; 
     network_backends_t *bs; 
}db_group_t;

typedef struct {
     int plan_id;
     GString *original_sql; 
     GHashTable *rewritten_sql; 
}plan_t;

typedef enum {
     MASTER_PLAN,
     SLAVE_PLAN
}plan_id_t;

typedef struct {
     gint64 range_begin;
     gint64 range_end;
        guint     db_group_index;
}group_range_map_t;

typedef struct {
     guint db_group_index;
}group_hash_map_t;

typedef enum {
     SHARDING_TYPE_RANGE,
     SHARDING_TYPE_HASH
}shardtype_t;

typedef struct {
     GString* table_name;
     GString* shard_key;
     shardtype_t shard_type;
     GArray*  shard_group; // element is group_range_map_t, if it is SHARDING_TYPE_HASH, orderd by group_id, element is group_hash_map_t
}sharding_table_t;

typedef struct {
    gint db_index;
    gint table_index;
    guint sql_type; 
    GPtrArray* sql_tokens;   
}sql_tokens_param_t;

#define LEMON_TOKEN_STRING(type) (type == TK_STRING || type == TK_ID)

typedef struct {
    Parse *parse_obj;
    
    /* Token table_token; */
    /* char *table_name; */ 
    /* char *db_name; */    
    SrcList *srclist;
    char *orig_sql;
    guint32 orig_sql_len;
    gboolean is_write_sql;
}parse_info_t;

typedef struct {
    guint shard_num;
    guint result_recvd_num; // resultset recvd count

    GPtrArray* recvd_rows;
    guint limit;
    guint64 affected_rows;
    guint16 warnings;
}sharding_merge_result_t;

typedef enum {
    TRANS_STAGE_INIT,
    TRANS_STAGE_BEFORE_SEND_BEGIN,
    TRANS_STAGE_SENDING_BEGIN,
    TRANS_STAGE_IN_TRANS, // injection successfully sent BEGIN
    TRANS_STAGE_BEGIN_FAILED,
}trans_stage_t;

/* typedef enum { */
/*     TRANS_CTRL_SQL_START_TRANSACTION, */
/*     TRANS_CTRL_SQL_BEGIN, */
/*     TRANS_CTRL_SQL_COMMIT, */
/*     TRANS_CTRL_SQL_ROLLBACK, */
/*     TRANS_CTRL_SQL_NOTTRANS, */
/* }trans_control_sql_t; */

typedef enum {
    SHARDING_SHARDKEY_VALUE_EQ,  // eg. id = 1 or id IN (1, 100)
    // include range_begin and range_end, eg. "between 1 and 100" or "1 <= id and id <= 100"
    // and "id > 1 and id < 100", the range_begin is 2, range_end is 99
    SHARDING_SHARDKEY_RANGE,
    SHARDING_SHARDKEY_VALUE_NE,
    SHARDING_SHARDKEY_VALUE_GT,  // greater than value, eg. id > 100
    SHARDING_SHARDKEY_VALUE_GTE, // greater than value or equal to, eg. id >= 100
    SHARDING_SHARDKEY_VALUE_LT,  // less than value, eg. id < 100
    SHARDING_SHARDKEY_VALUE_LTE, // less than value, eg. id <= 100
    SHARDING_SHARDKEY_VALUE_UNKNOWN,
}shard_key_type_t;

/* typedef enum { */
/*     SHARDING_LOGICAL_OPR_AND, */
/*     SHARDING_LOGICAL_OPR_OR, */
/*     SHARDING_LOGICAL_OPR_NONE */  
/* }shard_key_logical_opr_t; */

typedef struct {
	shard_key_type_t type;
	gint64 value;
	gint64 range_begin;
	gint64 range_end;
    ExprList *not_in_list;
}shard_key_t;

typedef enum {
	SHARDING_RET_OK,
    SHARDING_RET_ALL_SHARD,  // distribute to all db group

	// eg. sql like insert into table(id, other) values(...); but the id is not given, just give other column
	// like insert into table(other) values(...);
	SHARDING_RET_ERR_NO_SHARDCOLUMN_GIVEN,
    SHARDING_RET_ERR_NO_SHARDKEY,
    SHARDING_RET_ERR_HIT_NOTHING,
	// write operation is only allowed to hit one dbgroup, because if it write multi dbgroups, and one
	// of them failed, then roll back is a complicated task to do. Now we just do not allow these sqls, like
	// update table set other = xx where id in (1, 2000); is not allowed
	SHARDING_RET_ERR_MULTI_SHARD_WRITE,
	SHARDING_RET_ERR_OUT_OF_RANGE,
    SHARDING_RET_ERR_WRONG_RANGE,
	SHARDING_RET_ERR_NOT_SUPPORT,
	SHARDING_RET_ERR_UNKNOWN,
    SHARDING_RET_ERR_WRONG_HASH,
}sharding_result_t;

/**
 * store sql query context, every backend has a sharding_dbgroup_context_t
 */ 
struct sharding_dbgroup_context_t {
    guint group_id;
    network_mysqld_con_lua_t *st;
	network_socket* backend_sock;       /** backend socket*/

    gboolean resultset_is_needed;
    //gboolean resultset_is_finished;
    gboolean cur_injection_finished;
    gboolean com_quit_seen;

	/**
	 * Contains the parsed packet.
	 */
	struct network_mysqld_con_parse parse;

    /**
     * trace charset change
     */
    GString* charset_client;
    GString* charset_results;
    GString* charset_connection;
};

struct trans_context_t {
    gboolean is_default_dbgroup_in_trans;
    sharding_dbgroup_context_t* in_trans_dbgroup_ctx;
    trans_stage_t trans_stage;
    GString*    origin_begin_sql;
}; 

/**
 * store connection sharding context
 */ 
struct sharding_context_t{
    sharding_merge_result_t merge_result;
    GHashTable*  querying_dbgroups;
    guint       query_sent_count;  /* count of query already sent*/
    gboolean    is_continue_from_send_query; /* flag figure out if run __network_sharding_read_query_result is from __network_sharding_send_query*/
};

sharding_dbgroup_context_t* sharding_dbgroup_context_new(guint group_id);
void sharding_dbgroup_context_free(gpointer data);
sharding_dbgroup_context_t* sharding_lookup_dbgroup_context(GHashTable* dbgroup_contexts, guint groupid);
void sharding_reset_st(network_mysqld_con_lua_t* st);

/**
 * trans_context_t operations
 */
void sharding_reset_trans_context_t(trans_context_t* trans_ctx);

/**
 * parse packets
 */
void sharding_dbgroup_reset_command_response_state(struct network_mysqld_con_parse* parse);
gint sharding_parse_command_states_init(struct network_mysqld_con_parse* parse, network_mysqld_con* con, network_packet* packet, sharding_dbgroup_context_t* dbgroup_ctx);
gint sharding_parse_get_query_result(struct network_mysqld_con_parse* parse, network_mysqld_con* con, network_packet* packet);
gint sharding_mysqld_proto_get_com_query_result(network_packet *packet, network_mysqld_com_query_result_t *query, network_mysqld_con *con, gboolean use_binary_row_data);
gint sharding_mysqld_proto_get_com_init_db(network_packet *packet, network_mysqld_com_init_db_result_t *udata, network_mysqld_con *con);

void sharding_trace_charset(network_mysqld_con* con);

sharding_context_t* sharding_context_new();
void sharding_context_free(gpointer data);
void sharding_context_reset(sharding_context_t* context);
void sharding_reset_merge_result(sharding_context_t* context);

void sharding_querying_dbgroup_context_add(GHashTable* querying_dbgroups, sharding_dbgroup_context_t* dbgroup_ctx);

/**
 * @param hit_db_groups:  elements is guint, element is index of db_group_t array which the sql belongs to,
 * 						  it will append by sharding_get_dbgroups()
 * @param sharing_table_rule: the sharding rules of the table
 * @param sql_tokens: parsed sql tokens
 * @param table_index: table name index in sql_token array
 * @parram sql_type
 * @return:
 */
sharding_result_t sharding_get_dbgroups(GArray* hit_db_groups, const sharding_table_t* sharding_table_rule, parse_info_t *parse_info);
const gchar* sharding_get_error_msg(sharding_result_t ret);

gboolean sharding_is_support_sql(Parse *parse_obj);

gboolean sharding_is_contain_multishard_notsupport_feature(Parse *parse_obj);

sharding_table_t* sharding_lookup_table_shard_rule(GHashTable* table_shard_rules, gchar* default_db, parse_info_t *parse_info);

void sharding_set_connection_flags(GPtrArray* sql_tokens, network_mysqld_con* con);


void sharding_modify_db(network_mysqld_con* con, sharding_dbgroup_context_t* dbgroup_context);
void sharding_modify_charset(network_mysqld_con* con, sharding_dbgroup_context_t* dbgroup_context);
void sharding_modify_user(network_mysqld_con* con, sharding_dbgroup_context_t* dbgroup_context, GHashTable *pwd_table);
void sharding_inject_trans_begin_sql(network_mysqld_con* con, sharding_dbgroup_context_t* dbgroup_ctx);

/**
 * Sharding network operations
 */
void sharding_proxy_error_send_result(network_mysqld_con* con);
void sharding_proxy_send_injections(sharding_dbgroup_context_t* dbgroup_context);
void sharding_proxy_send_result(network_mysqld_con* con, sharding_dbgroup_context_t* dbgroup_context, injection* inj);
void sharding_proxy_ignore_result(sharding_dbgroup_context_t* dbgroup_context);

/**
 * sharding dbgroup read write split
 */
network_backend_t* sharding_get_rw_backend(network_backends_t *backends);
network_backend_t* sharding_get_ro_backend(network_backends_t* backends);
network_backend_t* sharding_read_write_split(Parse *parse_obj, network_backends_t* backends);

/**
 * sharding config
 */ 
sharding_table_t* keyfile_to_sharding_table(GKeyFile *keyfile, gchar *group_name, GPtrArray *dbgroups);
db_group_t* keyfile_to_db_group(GKeyFile* keyfile, gchar* group_name, guint event_thread_count);
void sharding_table_free(sharding_table_t *table);

void parse_info_init(parse_info_t *parse_info, Parse *parse_obj, const char *sql, guint sql_len);
Expr* parse_get_where_expr(Parse *parse_obj);
gint parse_get_sql_limit(Parse *parse_obj);

void dup_token2buff(char *buff, int buff_len, Token token);

#define IS_TRANSACTION_CTRL_SQL(type) (          \
        type == SQLTYPE_TRANSACTION_BEGIN    ||  \
        type == SQLTYPE_TRANSACTION_START    ||  \
        type == SQLTYPE_TRANSACTION_ROLLBACK ||  \
        type == SQLTYPE_TRANSACTION_COMMIT)

#endif // _CHASSIS_SHARDING_H_
