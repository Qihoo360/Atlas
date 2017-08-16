#ifndef _CHASSIS_SHARDING_H_
#define _CHASSIS_SHARDING_H_

#include <lemon/sqliteInt.h>
#include <glib.h>
#include "chassis-exports.h"
#include "network-socket.h"
#include "network-mysqld.h"
#include "network-mysqld-lua.h"
#include "network-mysqld-packet.h"

typedef struct network_mysqld_con_parse network_mysqld_con_parse;


typedef enum {
   AGGREGATION_UNKNOWN = 0,
   AGGREGATION_COUNT,
   AGGREGATION_SUM, 
   AGGREGATION_AVG, 
   AGGREGATION_MAX, 
   AGGREGATION_MIN, 
   AGGREGATION_OTHER 
} buildin_func_type;   

typedef struct {
    buildin_func_type type;
    void (*func)(void *arg, void *arg2, void *result);
    gchar *column; 
    gint column_len;
    gchar *id;
    gint id_len;
    gint idx; 
    gboolean is_grouped_column;
} buildin_func_t; 

//typedef struct i
struct dbgroup_context_t {
    gchar *group_name;      
    network_socket *server;
    network_mysqld_con_lua_t *st;     
    gboolean cur_injection_finished;
    network_mysqld_con_parse parse;

    gboolean com_quit_seen;
    gboolean resultset_is_needed;
    /**
     *      * trace charset change
     **/
    GString* charset_client;
    GString* charset_results;
    GString* charset_connection;
};

typedef struct {
    guint shard_num;
    guint result_recvd_num;
    guint sql_sent_num;

    GPtrArray *rows;
    guint limit;  
    guint affected_rows; // no use 
    guint warning_num;
    GPtrArray *func_array;
    
} sharding_merge_result_t; 

struct sharding_context_t {
    sharding_merge_result_t merge_result;
    GHashTable *sql_groups;
    gboolean is_from_send_query;
    struct dbgroup_context_t *ctx_in_use;
}; 
struct parse_info_t {
    Parse *parse_obj; 
    SrcList *table_list;
    gchar *orig_sql; 
    guint32 orig_sql_len; 
    gboolean is_write_sql;
};  

typedef enum {
    SHARDING_SHARDKEY_VALUE_EQ,  // eg. id = 1 or id IN (1, 100)
    // include range_begin and range_end, eg. "between 1 and 100" or "1 <= id and id <= 100"
    //     // and "id > 1 and id < 100", the range_begin is 2, range_end is 99
    SHARDING_SHARDKEY_RANGE,
    SHARDING_SHARDKEY_VALUE_NE,
    SHARDING_SHARDKEY_VALUE_GT,  // greater than value, eg. id > 100
    SHARDING_SHARDKEY_VALUE_GTE, // greater than value or equal to, eg. id >= 100
    SHARDING_SHARDKEY_VALUE_LT,  // less than value, eg. id < 100
    SHARDING_SHARDKEY_VALUE_LTE, // less than value, eg. id <= 100
    SHARDING_SHARDKEY_VALUE_UNKNOWN,
} sharding_key_type_t;

typedef struct {
    sharding_key_type_t type;
    gint64 value;
    gint64 range_begin;
    gint64 range_end;
    ExprList *not_in_list;
} sharding_key_t;

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
} sharding_result_t;

CHASSIS_API void parse_info_init(parse_info_t *parse_info, Parse *parse_obj, gchar *sql, guint sql_len); 
CHASSIS_API void parse_info_clear(parse_info_t *parse_info); 

void dup_token2buff(char *buff, int buff_len, Token token); 

/**
 * sharding ctx operation
 **/
sharding_context_t *sharding_context_new(void);
void sharding_context_free(sharding_context_t *ctx);
void sharding_context_reset(sharding_context_t *ctx);
void sharding_context_setup(sharding_context_t *sharding_ctx, GArray *dbgroups_ctx, Parse *parse_Obj);

void sharding_merge_result_new(sharding_merge_result_t *merge_value);
void sharding_merge_result_free(sharding_merge_result_t *merge_value);
void sharding_merge_result_reset(sharding_merge_result_t *merge_value);
void sharding_merge_result_setup(sharding_merge_result_t *merge_value, gint shard_num, Parse *parse_Obj);

/**
 * merge_rows
 * */
void sharding_merge_rows(dbgroup_context_t* dbgroup_ctx, sharding_merge_result_t *merge_result, injection* inj);
/*
 * group ctx opeartion 
 * */
dbgroup_context_t* dbgroup_context_new(char *group_name);
void dbgroup_context_free(gpointer data);
void dbgroup_st_reset(network_mysqld_con_lua_t *st);
dbgroup_context_t* sharding_lookup_dbgroup_context(sharding_context_t *ctx, guint fd);

/*
 *  dbgroup modify db/charset/user
 * */
void dbgroup_modify_db(network_mysqld_con *con, dbgroup_context_t *dbgroup_ctx);
void dbgroup_modify_user(network_mysqld_con *con, dbgroup_context_t *dbgroup_ctx, GHashTable *pwd_table);
void dbgroup_modify_charset(network_mysqld_con *con, dbgroup_context_t *dbgroup_ctx);

// ananlysi sql

SqlType get_sql_type(parse_info_t *parse_info);
gboolean rewrite_sql(parse_info_t *parse_info, sharding_table_t *rule, GString **packets, glong newid);

sharding_result_t sharding_get_dbgroups(GArray *dbgroups, parse_info_t *parse_info, sharding_table_t *sharding_table_rule);   

#endif

