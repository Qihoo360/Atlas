#include "chassis-sharding.h"
#include "network-mysqld-lua.c"
#include "chassis-event-thread.h"
#include "network-conn-pool-lua.h"
// 
#define S(x) x->str, x->len
#define LEMON_TOKEN_STRING(type) (type == TK_STRING || type == TK_ID || type == TK_DOT)
#define LEMON_TOKEN_INTEGER(type) (type == TK_INTEGER)
#define LEMON_TOKEN_DOT(type) (type == TK_DOT)
#define EXPR_BUILDIN__FUNC(type) (type == TK_AGG_FUNCTION || type == TK_FUNCTION)



gint parse_get_sql_limit(Parse *parse_obj) {
    gint limit = G_MAXINT;
    if (parse_obj == NULL) { return limit;  }
    ParsedResultItem *parsed_item = &parse_obj->parsed.array[0];
    Expr *limit_expr = NULL;
    Expr *offset_expr = NULL;
    switch(parsed_item->sqltype) {
        case SQLTYPE_SELECT:
            limit_expr = parsed_item->result.selectObj->pLimit;
            offset_expr = parsed_item->result.selectObj->pOffset;
            break;
        case SQLTYPE_UPDATE:
            limit_expr = parsed_item->result.updateObj->pLimit;
            offset_expr = parsed_item->result.updateObj->pOffset;
            break;
        case SQLTYPE_DELETE:
            limit_expr = parsed_item->result.deleteObj->pLimit;
            offset_expr = parsed_item->result.deleteObj->pOffset;
            break;
        defalut:
            break;

    }
    if (limit_expr) {
        char limit_str[64] = {0};
        dup_token2buff(limit_str, sizeof(limit_str), limit_expr->token);
        limit = strtoul(limit_str, NULL, 0);
    }
    return limit;
}

buildin_func_t *get_buildin_func(Expr *expr, gint idx, gboolean is_grouped_column) {
    if (NULL == expr) {
        return NULL; 
    }
    buildin_func_t *t = g_slice_new(buildin_func_t);
    t->func = NULL;
    t->idx = idx;

    t->column = g_strndup(expr->span.z, expr->span.n); //  n + 1
    t->column_len = expr->span.n;

    t->id = g_strndup(expr->token.z, expr->token.n);
    t->id_len = expr->token.n;   // 

    //t->type = (t->column_len == t->id_len) ? AGGREGATION_UNKNOWN : AGGREGATION_OTHER; 
    if (t->column_len <= t->id_len) {
        t->type = AGGREGATION_UNKNOWN;
    } else {
        if (t->id_len >= 3 && 0 == strncasecmp(t->id, "sum", 3)) {
            t->type = AGGREGATION_SUM;
        } else if (t->id_len >= 3 && 0 == strncasecmp(t->id, "max", 3)) {
            t->type = AGGREGATION_MAX;
        } else if (t->id_len >= 3 && 0 == strncasecmp(t->id, "min", 3)) {
            t->type = AGGREGATION_MIN;
        } else if (t->id_len >= 3 && 0 == strncasecmp(t->id, "avg", 3)) {
            t->type = AGGREGATION_AVG;
        } else if (t->id_len >= 5 && 0 == strncasecmp(t->id, "count", 5)) {
            t->type = AGGREGATION_COUNT;
        } else {
            t->type = AGGREGATION_UNKNOWN;
        }
    }  

    t->is_grouped_column = is_grouped_column; 
    return t; 
} 

void parse_get_sql_buildin_func(GPtrArray *func_array, Parse *parse_obj) {
    if (NULL == parse_obj || parse_obj->parsed.curSize <= 0 || NULL == func_array) {
        return;
    }     

    ParsedResultItem *parsed_item = &parse_obj->parsed.array[0];
    switch (parsed_item->sqltype) {
        case SQLTYPE_SELECT: {
                                 Select* selectObj = parsed_item->result.selectObj;   
                                 gint i; 
                                 ExprList *grouped_list = selectObj->pGroupBy;
                                 struct ExprList_item *grouped_item = NULL; 
                                 if (NULL != grouped_list && grouped_list->nExpr > 0) {
                                     grouped_item = &grouped_list->a[0];  // only support groupby one colume
                                 }

                                 ExprList *expr_list = selectObj->pEList; 
                                 for (i = 0; i < expr_list->nExpr; i++) {
                                     struct ExprList_item *item = &expr_list->a[i];
                                     if (grouped_item 
                                             && grouped_item->pExpr->span.n == item->pExpr->span.n 
                                             && strcasecmp(item->pExpr->span.z, grouped_item->pExpr->span.z)) {
                                         g_ptr_array_add(func_array, get_buildin_func(item->pExpr, i, TRUE));                       
                                     } else {
                                         g_ptr_array_add(func_array, get_buildin_func(item->pExpr, i, FALSE));                       
                                     }               
                                 } 
                                 break;
                             }
        case SQLTYPE_UPDATE:
                             break;
        case SQLTYPE_DELETE:
                             break;
        default: 
                             break;
    }
}

G_INLINE_FUNC sharding_key_type_t sql_token_id2sharding_key_type(int token_id) {
    switch (token_id) {
        case TK_GT:
            return SHARDING_SHARDKEY_VALUE_GT;
        case TK_GE:
            return SHARDING_SHARDKEY_VALUE_GTE;
        case TK_LT:
            return SHARDING_SHARDKEY_VALUE_LT;
        case TK_LE:
            return SHARDING_SHARDKEY_VALUE_LTE;
        case TK_NE:
            return SHARDING_SHARDKEY_VALUE_NE;
        case TK_EQ:
            return SHARDING_SHARDKEY_VALUE_EQ;
        defalut:
            return SHARDING_SHARDKEY_VALUE_UNKNOWN;
    }
    return SHARDING_SHARDKEY_VALUE_UNKNOWN;  // never come here
}

static parse_info_get_table_list(parse_info_t *parse_info, Parse *parse_obj) {
    if (NULL == parse_info || NULL == parse_obj)  {
        return;
    }
    ParsedResultItem *item = &parse_obj->parsed.array[0];  
    switch (item->sqltype) {
        case SQLTYPE_SELECT:
            parse_info->table_list = item->result.selectObj->pSrc;
            break;
        case SQLTYPE_INSERT:
            parse_info->table_list = item->result.insertObj->pTabList;
            break;
        case SQLTYPE_DELETE:
            parse_info->table_list =  item->result.deleteObj->pTabList;
            break;
        case SQLTYPE_UPDATE:
            parse_info->table_list = item->result.updateObj->pTabList; 
            break;
        default:
            break;
    } 
}

static is_write_sql(Parse *parse_obj) {
    if (parse_obj == NULL) { return FALSE; }
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
void parse_info_init(parse_info_t *parse_info, Parse *parse_obj, gchar *sql, guint sql_len) {
    if (parse_info == NULL) return;

    memset(parse_info, 0, sizeof(*parse_info));
    parse_info->parse_obj = parse_obj;
    parse_info_get_table_list(parse_info, parse_obj);
    parse_info->orig_sql = sql; //g_strdup
    parse_info->orig_sql_len = sql_len;
    parse_info->is_write_sql = is_write_sql(parse_obj);
}
void parse_info_clear(parse_info_t *parse_info) {
    if (parse_info == NULL) return;
    if (parse_info->parse_obj) {
        sqlite3ParseClean(parse_info->parse_obj);        
        parse_info->parse_obj = NULL; 
    }
    parse_info->table_list = NULL; 
    parse_info->orig_sql = NULL;
}

void dup_token2buff(char *buff, int buff_len, Token token) {
    if (buff == NULL || token.z == NULL) return;
    int cp_size = MIN(buff_len-1, token.n);

    memcpy(buff, token.z, cp_size);
    buff[cp_size] = '\0';
}
/*
 * db group opertationnn
 * */
dbgroup_context_t* dbgroup_context_new(gchar* group_name) {
    dbgroup_context_t *context = g_slice_new(dbgroup_context_t);
    //dbgroup_context_t context;
    context->group_name = g_strdup(group_name);
    context->st = network_mysqld_con_lua_new();

    context->charset_client = g_string_new(NULL);
    context->charset_results = g_string_new(NULL);
    context->charset_connection = g_string_new(NULL);

    context->parse.command = -1;
    context->parse.data = NULL;
    context->parse.data_free = NULL;
    return context;
}

void dbgroup_context_free(gpointer data) {
    dbgroup_context_t **ctx_ = data;
    dbgroup_context_t *ctx = *ctx_;
    if (ctx->st) {
        network_mysqld_con_lua_free(ctx->st);
    } 
    if (ctx->server) {
        network_socket_free(ctx->server);
    }

    if (ctx->parse.data && ctx->parse.data_free) {
        ctx->parse.data_free(ctx->parse.data);
    }
    g_string_free(ctx->charset_client, TRUE);
    g_string_free(ctx->charset_results, TRUE);
    g_string_free(ctx->charset_connection, TRUE);  
    g_free(ctx->group_name); 
    g_slice_free(dbgroup_context_t, ctx);
} 

void dbgroup_st_reset(network_mysqld_con_lua_t *st) {
    if (st == NULL) { return;  }

    st->injected.sent_resultset = 0;
    network_injection_queue_clear(st->injected.queries);
}
/*
 * sharding context operation
 **/
static void merge_rows_elem_free(gpointer data) {
    if (data) {
        g_ptr_array_free((GPtrArray *)data, TRUE);
    }
}
static void merge_func_array_free(gpointer data) {
    buildin_func_t *t = data;
    if (t) {
        g_free(t->column);
        g_free(t->id);
        g_slice_free(buildin_func_t, t);
    }
}


void dbgroup_context_hash_free(gpointer data) {
    dbgroup_context_t *ctx = data;
    if (ctx->st) {
        network_mysqld_con_lua_free(ctx->st);
        ctx->st = NULL;
    }    
    if (ctx->server) {
        network_socket_free(ctx->server);
        ctx->server = NULL;
    }

    if (ctx->parse.data && ctx->parse.data_free) {
        ctx->parse.data_free(ctx->parse.data);
    }
    g_string_free(ctx->charset_client, TRUE);
    g_string_free(ctx->charset_results, TRUE);
    g_string_free(ctx->charset_connection, TRUE);  
    g_free(ctx->group_name); 
    g_slice_free(dbgroup_context_t, ctx);
} 

sharding_context_t *sharding_context_new(void) {
    sharding_context_t *ctx = g_slice_new(sharding_context_t); 
    ctx->sql_groups = g_hash_table_new_full(g_int_hash, g_int_equal, g_free, dbgroup_context_hash_free);

    ctx->merge_result.rows = g_ptr_array_new_with_free_func(merge_rows_elem_free);
    sharding_merge_result_new(&ctx->merge_result);
    return ctx;
}
void sharding_context_setup(sharding_context_t *sharding_ctx, GArray *dbgroups_ctx, Parse *parse_obj) {
    if (sharding_ctx == NULL || NULL == dbgroups_ctx) {
        return;
    }
    guint i;
    for (i = 0; i < dbgroups_ctx->len; i++) {
        dbgroup_context_t *v = g_array_index(dbgroups_ctx, dbgroup_context_t*, i);
        gint *k = g_new0(gint, 1);
        *k = v->server->fd;
        g_hash_table_insert(sharding_ctx->sql_groups, k, v);
    }
    sharding_merge_result_setup(&sharding_ctx->merge_result, dbgroups_ctx->len, parse_obj); //get row limit by parse sql 
}
void sharding_context_free(sharding_context_t *ctx) {
    if (NULL == ctx) { return; }  

    sharding_merge_result_free(&ctx->merge_result);

    if (ctx->sql_groups) {
        g_hash_table_remove_all(ctx->sql_groups);
        g_hash_table_destroy(ctx->sql_groups);
        ctx->sql_groups = NULL;
    }
    g_slice_free(sharding_context_t, ctx);
} 

void sharding_merge_result_new(sharding_merge_result_t *merge_result) {
    if (NULL == merge_result) {
        return; 
    } 

    merge_result->rows = g_ptr_array_new_with_free_func(merge_rows_elem_free);
    merge_result->func_array = g_ptr_array_new_with_free_func(merge_func_array_free);
} 

void sharding_merge_result_free(sharding_merge_result_t *merge_result) {
    if (NULL == merge_result) {
        return;
    } 
    if (merge_result->rows) {
        g_ptr_array_set_size(merge_result->rows, 0); // sharding_merge_rows_elem_free will be called automatelly
        g_ptr_array_free(merge_result->rows, TRUE);
        merge_result->rows = NULL;
    }
    if (merge_result->func_array) {
        g_ptr_array_set_size(merge_result->func_array, 0); // sharding_merge_rows_elem_free will be called automatelly
        g_ptr_array_free(merge_result->func_array, TRUE);
        merge_result->func_array = NULL;
    }
}
void sharding_merge_result_reset(sharding_merge_result_t *merge_result) {
    if (NULL == merge_result) { return; }
    if (merge_result->rows) {
        g_ptr_array_set_size(merge_result->rows, 0);
    }  

    if (merge_result->func_array) {
        g_ptr_array_set_size(merge_result->func_array, 0);
    } 
    merge_result->result_recvd_num = 0;
    merge_result->sql_sent_num = 0;
    merge_result->warning_num = 0;
    merge_result->affected_rows = 0;
    merge_result->shard_num = 0;
    merge_result->limit = G_MAXINT;
}
void sharding_merge_result_setup(sharding_merge_result_t *merge_result, gint shard_num, Parse *parse_obj) {
    merge_result->shard_num = shard_num; 
    merge_result->limit = parse_get_sql_limit(parse_obj); 
    parse_get_sql_buildin_func(merge_result->func_array, parse_obj);
}
void sharding_context_reset(sharding_context_t *ctx) {
    ctx->ctx_in_use = NULL;
    ctx->is_from_send_query = FALSE; 
    g_hash_table_remove_all(ctx->sql_groups);
    sharding_merge_result_reset(&ctx->merge_result);
}


void dbgroup_modify_db(network_mysqld_con *con, dbgroup_context_t *dbgroup_ctx) {
    if (dbgroup_ctx->server == NULL || dbgroup_ctx->server->default_db == NULL) { return;  }

    char* default_db = con->client->default_db->str;
    if (default_db != NULL && strcmp(default_db, dbgroup_ctx->server->default_db->str) != 0) {
        gchar cmd = COM_INIT_DB;
        GString* query = g_string_new_len(&cmd, 1);
        g_string_append(query, default_db);
        injection* inj = injection_new(INJECTION_INIT_DB, query);
        inj->resultset_is_needed = TRUE;
        network_injection_queue_prepend(dbgroup_ctx->st->injected.queries, inj);
        g_string_free(query, TRUE);
    }
}
void dbgroup_modify_charset(network_mysqld_con *con, dbgroup_context_t *dbgroup_ctx) {
    g_string_truncate(dbgroup_ctx->charset_client, 0);
    g_string_truncate(dbgroup_ctx->charset_results, 0);
    g_string_truncate(dbgroup_ctx->charset_connection, 0);

    if (dbgroup_ctx->server == NULL) return;

    gboolean is_set_client      = FALSE;
    gboolean is_set_results     = FALSE;
    gboolean is_set_connection  = FALSE;

    // check if the charset is the same between client socket and backend socket
    network_socket* client = con->client;
    network_socket* server = dbgroup_ctx->server;

    gchar cmd = COM_QUERY;

    if (!is_set_client && !g_string_equal(client->charset_client, server->charset_client)) {
        GString* query = g_string_new_len(&cmd, 1);
        g_string_append(query, "SET CHARACTER_SET_CLIENT=");
        g_string_append(query, client->charset_client->str);
        g_string_assign(dbgroup_ctx->charset_client, client->charset_client->str);

        injection* inj = injection_new(INJECTION_SET_CHARACTER_SET_CLIENT_SQL, query);
        inj->resultset_is_needed = TRUE;
        network_injection_queue_prepend(dbgroup_ctx->st->injected.queries, inj);
        g_string_free(query, TRUE);
    }

    if (!is_set_results && !g_string_equal(client->charset_results, server->charset_results)) {
        GString* query = g_string_new_len(&cmd, 1);
        g_string_append(query, "SET CHARACTER_SET_RESULTS=");
        g_string_append(query, client->charset_results->str);
        g_string_assign(dbgroup_ctx->charset_results, client->charset_results->str);

        injection* inj = injection_new(INJECTION_SET_CHARACTER_SET_RESULTS_SQL, query);
        inj->resultset_is_needed = TRUE;
        network_injection_queue_prepend(dbgroup_ctx->st->injected.queries, inj);

        g_string_free(query, TRUE);
    }

    if (!is_set_connection && !g_string_equal(client->charset_connection, server->charset_connection)) {
        GString* query = g_string_new_len(&cmd, 1);
        g_string_append(query, "SET CHARACTER_SET_CONNECTION=");
        g_string_append(query, client->charset_connection->str);
        g_string_assign(dbgroup_ctx->charset_connection, client->charset_connection->str);

        injection* inj = injection_new(INJECTION_SET_CHARACTER_SET_CONNECTION_SQL, query);
        inj->resultset_is_needed = TRUE;
        network_injection_queue_prepend(dbgroup_ctx->st->injected.queries, inj);
        g_string_free(query, TRUE);

    }
}
void dbgroup_modify_user(network_mysqld_con *con, dbgroup_context_t *dbgroup_ctx, GHashTable *pwd_table) {
    if (dbgroup_ctx->server == NULL) { return; }

    network_socket *client = con->client;
    network_socket *server = dbgroup_ctx->server; 

    GString* client_user = client->response->username;
    GString* server_user = server->response->username;

    if (!g_string_equal(client_user, server_user)) {
        GString* com_change_user = g_string_new(NULL);

        g_string_append_c(com_change_user, COM_CHANGE_USER);
        g_string_append_len(com_change_user, client_user->str, client_user->len + 1);

        GString* hashed_password = g_hash_table_lookup(pwd_table, client_user->str);
        if (!hashed_password) return;

        GString* expected_response = g_string_sized_new(20);
        network_mysqld_proto_password_scramble(expected_response, S(dbgroup_ctx->server->challenge->challenge), S(hashed_password));

        g_string_append_c(com_change_user, (expected_response->len & 0xff));
        g_string_append_len(com_change_user, S(expected_response));
        g_string_append_c(com_change_user, 0);

        injection* inj = injection_new(INJECTION_MODIFY_USER_SQL, com_change_user);
        inj->resultset_is_needed = TRUE;
        network_injection_queue_prepend(dbgroup_ctx->st->injected.queries, inj);
        g_string_free(com_change_user, TRUE);
        g_string_truncate(client->response->response, 0);
        g_string_assign(client->response->response, expected_response->str);
        g_string_free(expected_response, TRUE);
    } 
}
void dbgroup_send_injection(dbgroup_context_t *ctx) {
    injection *inj = g_queue_peek_head(ctx->st->injected.queries);
    ctx->resultset_is_needed = inj->resultset_is_needed;

    network_socket* server = ctx->server;
    network_mysqld_queue_reset(server);
    network_mysqld_queue_append(server, server->send_queue, S(inj->query)); 
}
/**
 *
 * 
 */
void dbgroup_con_get(network_mysqld_con *con, gboolean is_write_sql, GHashTable *pwd_table, dbgroup_context_t *ctx) {
    if (!con || !ctx || !pwd_table)  return;

    network_nodes_t *nodes = con->srv->nodes;
    group_info_t *group = g_hash_table_lookup(nodes->nodes, ctx->group_name);
    network_socket *send_sock = NULL;
    network_group_backend_t *backend = NULL; 
    network_mysqld_con_lua_t *st = ctx->st;

    if (is_write_sql) {
        backend = master_backend_get(group);
    } else {
        backend = slave_backend_get(group, "");
    }
    if (NULL == backend) { return; }
#ifdef DEBUG_CONN_POOL
    g_debug("%s: (swap) check if we have a connection for this user in the pool '%s'", G_STRLOC, con->client->response ? con->client->response->username->str: "empty_user");
#endif
    network_connection_pool* pool = chassis_event_thread_pool(backend->b);
    if (NULL == (send_sock = network_connection_pool_get(pool))) {
        /*
         * no new connection will be created when the number of connection exceeds the configure limit
         * */
        if(network_mysqld_conn_count(con) >= con->srv->router_para->max_conn_in_pool){
            st->backend_ndx = -1;
            return;
        } 
        /**
         * no connections in the pool
         */
        if (NULL == (send_sock = self_connect(con, backend->b, pwd_table))) {
            st->backend_ndx = -1;
            return;
        }
    }

    /* the backend is up and cool, take and move the current backend into the pool */
#ifdef DEBUG_CONN_POOL
    g_debug("%s: (swap) added the previous connection to the pool", G_STRLOC);
#endif
    //	network_connection_pool_lua_add_connection(con);

    /* connect to the new backend */
    st->backend = backend;
    //	st->backend->connected_clients++;
    //st->backend_ndx = backend_ndx;
    st->backend_ndx = 1;

    ctx->server = send_sock;
} 
network_mysqld_lua_stmt_ret sharding_merge_query_result(network_mysqld_con *con, injection *inj) { 
    network_mysqld_lua_stmt_ret ret;

    sharding_context_t *sharding_ctx = con->sharding_context;
    sharding_merge_result_t *merge_result = &sharding_ctx->merge_result;

    dbgroup_context_t *dbgroup_ctx = sharding_ctx->ctx_in_use; 

    network_socket *client = con->client;
    network_socket *server = dbgroup_ctx->server;  
    network_mysqld_con_lua_t *st = dbgroup_ctx->st;  

    /*TODO(dengyihao):currently only support com_query*/
    if (dbgroup_ctx->parse.command == COM_QUERY) {
        network_mysqld_com_query_result_t *com_query = dbgroup_ctx->parse.data;

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

    if (inj->qstat.query_status == MYSQLD_PACKET_OK && merge_result->rows->len < merge_result->limit) { 
        sharding_merge_rows(dbgroup_ctx, merge_result, inj);
    }

    if (++merge_result->result_recvd_num < merge_result->shard_num) {
        ret = PROXY_IGNORE_RESULT;  
    } else {
        network_injection_queue_clear(dbgroup_ctx->st->injected.queries);

        if (inj->qstat.query_status == MYSQLD_PACKET_OK) {
            proxy_resultset_t* res = proxy_resultset_new();

            if (inj->resultset_is_needed && !inj->qstat.binary_encoded) {
                res->result_queue = server->recv_queue->chunks;
            } 
            res->qstat = inj->qstat;
            res->rows  = inj->rows;
            res->bytes = inj->bytes;
            parse_resultset_fields(res);

            //network_mysqld_con_send_resultset(client, res->fields, merge_result->rows); //move merge result to client queue
            //if (NULL == merge_result->func_array || 0 == merge_result->func_array->len) {
            //network_mysqld_con_send_resultset(client, res->fields, merge_result->rows); //move merge result to client queue
            //} else {
            network_mysqld_con_send_merged_resultset(client, res->fields, merge_result->rows, merge_result->func_array);
            //}
            proxy_resultset_free(res);

        } else if (inj->qstat.query_status == MYSQLD_PACKET_ERR) {
            GString *p = NULL; 
            while ((p = g_queue_pop_head(dbgroup_ctx->server->recv_queue->chunks))) network_mysqld_queue_append_raw(client, client->send_queue, p);
        }
        ret = PROXY_SEND_RESULT;
    }
    return ret; 

}    

/*
 *  deal with query result exception like com_querm、init_db 
 */
gint sharding_mysqld_get_com_init_db(dbgroup_context_t *dbgroup_ctx, network_socket *client, network_packet *packet) {
    network_mysqld_com_init_db_result_t *udata = dbgroup_ctx->parse.data;
    network_socket *server = dbgroup_ctx->server;
    guint8 status;
    int is_finished;
    int err = 0;

    /**
     * in case we have a init-db statement we track the db-change on the server-side
     * connection
     */
    err = err || network_mysqld_proto_get_int8(packet, &status);
    switch (status) {
        case MYSQLD_PACKET_ERR:
            is_finished = 1;
            break;
        case MYSQLD_PACKET_OK:
            /**
             * track the change of the init_db */
            if (server) g_string_truncate(server->default_db, 0);
            g_string_truncate(client->default_db, 0);

            if (udata->db_name && udata->db_name->len) {
                if (server) {
                    g_string_append_len(server->default_db, 
                            S(udata->db_name));
                }

                g_string_append_len(client->default_db, 
                        S(udata->db_name));
            }

            is_finished = 1;
            break;
        default:
            g_critical("%s.%d: COM_INIT_DB should be (ERR|OK), got %02x",
                    __FILE__, __LINE__,
                    status);

            return -1;
    }

    if (err) return -1;

    return is_finished;
}  
void sharding_update_charset(dbgroup_context_t *dbgroup_ctx, network_socket *client) {
    network_socket *server = dbgroup_ctx->server;
    GString* charset_client     = dbgroup_ctx->charset_client;
    GString* charset_results    = dbgroup_ctx->charset_results;
    GString* charset_connection = dbgroup_ctx->charset_connection;

    if (charset_client->len > 0) {
        if (server) g_string_assign_len(server->charset_client, S(charset_client));
        g_string_assign_len(client->charset_client, S(charset_client));
    }

    if (charset_results->len > 0) {
        if (server) g_string_assign_len(server->charset_results, S(charset_results));
        g_string_assign_len(client->charset_results, S(charset_results));
    }

    if (charset_connection->len > 0) {
        if (server) g_string_assign_len(server->charset_connection, S(charset_connection));
        g_string_assign_len(client->charset_connection, S(charset_connection));
    }
}
gint sharding_mysqld_get_com_query_result_meta(dbgroup_context_t *dbgroup_ctx, network_socket *client, network_packet *packet) {
    int is_finished = 0;
    guint8 status;
    int err = 0;
    network_mysqld_com_query_result_t *query = dbgroup_ctx->parse.data;
    network_mysqld_eof_packet_t *eof_packet;
    network_mysqld_ok_packet_t *ok_packet;
    gboolean use_binary_row_data;
    switch (query->state) {
        case PARSE_COM_QUERY_INIT:
            err = err || network_mysqld_proto_peek_int8(packet, &status);
            if (err) break;

            switch (status) {
                case MYSQLD_PACKET_ERR: /* e.g. SELECT * FROM dual -> ERROR 1096 (HY000): No tables used */
                    query->query_status = MYSQLD_PACKET_ERR;
                    is_finished = 1;
                    break;
                case MYSQLD_PACKET_OK:  /* e.g. DELETE FROM tbl */

                    /**
                     * trace the change of charset
                     */
                    sharding_update_charset(dbgroup_ctx, client);	
                    query->query_status = MYSQLD_PACKET_OK;

                    ok_packet = network_mysqld_ok_packet_new();

                    err = err || network_mysqld_proto_get_ok_packet(packet, ok_packet);

                    if (!err) {
                        if (ok_packet->server_status & SERVER_MORE_RESULTS_EXISTS) {

                        } else {
                            is_finished = 1;
                        }

                        query->server_status = ok_packet->server_status;
                        query->warning_count = ok_packet->warnings;
                        query->affected_rows = ok_packet->affected_rows;
                        query->insert_id     = ok_packet->insert_id;
                        query->was_resultset = 0;
                        query->binary_encoded = FALSE; 
                    }

                    network_mysqld_ok_packet_free(ok_packet);

                    break;
                case MYSQLD_PACKET_NULL:
                    /* OH NO, LOAD DATA INFILE :) */
                    query->state = PARSE_COM_QUERY_LOCAL_INFILE_DATA;
                    is_finished = 1;

                    break;
                case MYSQLD_PACKET_EOF:
                    g_critical("%s: COM_QUERY packet should not be (EOF), got: 0x%02x",
                            G_STRLOC,
                            status);

                    err = 1;

                    break;
                default:
                    query->query_status = MYSQLD_PACKET_OK;
                    /* looks like a result */
                    query->state = PARSE_COM_QUERY_FIELD;
                    break;
            }
            break;
        case PARSE_COM_QUERY_FIELD:
            err = err || network_mysqld_proto_peek_int8(packet, &status);
            if (err) break;

            switch (status) {
                case MYSQLD_PACKET_ERR:
                case MYSQLD_PACKET_OK:
                case MYSQLD_PACKET_NULL:
                    g_critical("%s: COM_QUERY should not be (OK|NULL|ERR), got: 0x%02x",
                            G_STRLOC,
                            status);

                    err = 1;

                    break;
                case MYSQLD_PACKET_EOF:
                    /**
                     * in 5.0 we have CURSORs which have no rows, just a field definition
                     *
                     * TODO: find a test-case for it, is it COM_STMT_EXECUTE only?
                     */
                    if (packet->data->len == 9) {
                        eof_packet = network_mysqld_eof_packet_new();

                        err = err || network_mysqld_proto_get_eof_packet(packet, eof_packet);

                        if (!err) {
#if MYSQL_VERSION_ID >= 50000
                            /* 5.5 may send a SERVER_MORE_RESULTS_EXISTS as part of the first 
                             * EOF together with SERVER_STATUS_CURSOR_EXISTS. In that case,
                             * we aren't finished. (#61998)
                             *
                             * Only if _CURSOR_EXISTS is set alone, we have a field-definition-only
                             * resultset
                             */
                            if (eof_packet->server_status & SERVER_STATUS_CURSOR_EXISTS &&
                                    !(eof_packet->server_status & SERVER_MORE_RESULTS_EXISTS)) {
                                is_finished = 1;
                            } else {
                                query->state = PARSE_COM_QUERY_RESULT;
                            }
#else
                            query->state = PARSE_COM_QUERY_RESULT;
#endif
                            /* track the server_status of the 1st EOF packet */
                            query->server_status = eof_packet->server_status;
                        }

                        network_mysqld_eof_packet_free(eof_packet);
                    } else {
                        query->state = PARSE_COM_QUERY_RESULT;
                    }
                    break;
                default:
                    break;
            }
            break;
        case PARSE_COM_QUERY_RESULT:
            err = err || network_mysqld_proto_peek_int8(packet, &status);
            if (err) break;

            switch (status) {
                case MYSQLD_PACKET_EOF:
                    if (packet->data->len == 9) {
                        eof_packet = network_mysqld_eof_packet_new();

                        err = err || network_mysqld_proto_get_eof_packet(packet, eof_packet);

                        if (!err) {
                            query->was_resultset = 1;

#ifndef SERVER_PS_OUT_PARAMS
#define SERVER_PS_OUT_PARAMS 4096
#endif
                            /**
                             * a PS_OUT_PARAMS is set if a COM_STMT_EXECUTE executes a CALL sp(?) where sp is a PROCEDURE with OUT params 
                             *
                             * ...
                             * 05 00 00 12 fe 00 00 0a 10 -- end column-def (auto-commit, more-results, ps-out-params)
                             * ...
                             * 05 00 00 14 fe 00 00 02 00 -- end of rows (auto-commit), see the missing (more-results, ps-out-params)
                             * 07 00 00 15 00 00 00 02 00 00 00 -- OK for the CALL
                             *
                             * for all other resultsets we trust the status-flags of the 2nd EOF packet
                             */
                            if (!(query->server_status & SERVER_PS_OUT_PARAMS)) {
                                query->server_status = eof_packet->server_status;
                            }
                            query->warning_count = eof_packet->warnings;

                            if (query->server_status & SERVER_MORE_RESULTS_EXISTS) {
                                query->state = PARSE_COM_QUERY_INIT;
                            } else {
                                is_finished = 1;
                            }
                        }

                        network_mysqld_eof_packet_free(eof_packet);
                    }

                    break;
                case MYSQLD_PACKET_ERR:
                    /* like 
                     * 
                     * EXPLAIN SELECT * FROM dual; returns an error
                     * 
                     * EXPLAIN SELECT 1 FROM dual; returns a result-set
                     * */
                    is_finished = 1;
                    break;
                case MYSQLD_PACKET_OK:
                case MYSQLD_PACKET_NULL:
                    if (use_binary_row_data) {
                        /* fallthrough to default:
                           0x00 is part of the protocol for binary row packets
                           */
                    } else {
                        /* the first field might be a NULL for a text row packet */
                        break;
                    }
                default:
                    query->rows++;
                    query->bytes += packet->data->len;
                    break;
            }
            break;
        case PARSE_COM_QUERY_LOCAL_INFILE_DATA: 
            /* we will receive a empty packet if we are done */
            if (packet->data->len == packet->offset) {
                query->state = PARSE_COM_QUERY_LOCAL_INFILE_RESULT;
                is_finished = 1;
            }
            break;
        case PARSE_COM_QUERY_LOCAL_INFILE_RESULT:
            err = err || network_mysqld_proto_get_int8(packet, &status);
            if (err) break;

            switch (status) {
                case MYSQLD_PACKET_OK:
                    is_finished = 1;
                    break;
                case MYSQLD_PACKET_NULL:
                case MYSQLD_PACKET_ERR:
                case MYSQLD_PACKET_EOF:
                default:
                    g_critical("%s: COM_QUERY,should be (OK), got: 0x%02x",
                            G_STRLOC,
                            status);

                    err = 1;

                    break;
            }

            break;
    }

    if (err) return -1;

    return is_finished;

}
gint sharding_mysqld_get_query_result_meta(dbgroup_context_t *dbgroup_ctx, network_socket *client, network_packet *packet) {
    guint8 status;
    int is_finished = 0;
    int err = 0;
    network_mysqld_eof_packet_t *eof_packet;

    err = err || network_mysqld_proto_skip_network_header(packet);
    if (err) return -1;
    switch (dbgroup_ctx->parse.command) {
        case COM_CHANGE_USER: 
            /**
             * - OK
             * - ERR (in 5.1.12+ + a duplicate ERR)
             */
            err = err || network_mysqld_proto_get_int8(packet, &status);
            if (err) return -1;

            switch (status) {
                case MYSQLD_PACKET_ERR:
                    is_finished = 1;
                    break;
                case MYSQLD_PACKET_OK:
                    is_finished = 1;
                    break;
                default:
                    g_error("%s.%d: COM_(0x%02x) should be (ERR|OK), got %02x",
                            __FILE__, __LINE__,
                            dbgroup_ctx->parse.command, status);
                    break;
            }
            break;
        case COM_INIT_DB:
            is_finished = sharding_mysqld_get_com_init_db(dbgroup_ctx, client, packet);

            break;
        case COM_REFRESH:
        case COM_STMT_RESET:
        case COM_PING:
        case COM_TIME:
        case COM_REGISTER_SLAVE:
        case COM_PROCESS_KILL:
            err = err || network_mysqld_proto_get_int8(packet, &status);
            if (err) return -1;

            switch (status) {
                case MYSQLD_PACKET_ERR:
                case MYSQLD_PACKET_OK:
                    is_finished = 1;
                    break;
                default:
                    g_error("%s.%d: COM_(0x%02x) should be (ERR|OK), got 0x%02x",
                            __FILE__, __LINE__,
                            dbgroup_ctx->parse.command, (guint8)status);
                    break;
            }
            break;
        case COM_DEBUG:
        case COM_SET_OPTION:
        case COM_SHUTDOWN:
            err = err || network_mysqld_proto_get_int8(packet, &status);
            if (err) return -1;

            switch (status) {
                case MYSQLD_PACKET_ERR: /* COM_DEBUG may not have the right permissions */
                case MYSQLD_PACKET_EOF:
                    is_finished = 1;
                    break;
                default:
                    g_error("%s.%d: COM_(0x%02x) should be EOF, got x%02x",
                            __FILE__, __LINE__,
                            dbgroup_ctx->parse.command, (guint8)status);
                    break;
            }
            break;

        case COM_FIELD_LIST:
            err = err || network_mysqld_proto_get_int8(packet, &status);
            if (err) return -1;

            /* we transfer some data and wait for the EOF */
            switch (status) {
                case MYSQLD_PACKET_ERR:
                case MYSQLD_PACKET_EOF:
                    is_finished = 1;
                    break;
                case MYSQLD_PACKET_NULL:
                case MYSQLD_PACKET_OK:
                    g_error("%s.%d: COM_(0x%02x) should not be (OK|ERR|NULL), got: %02x",
                            __FILE__, __LINE__,
                            dbgroup_ctx->parse.command, status);

                    break;
                default:
                    break;
            }
            break;
#if MYSQL_VERSION_ID >= 50000
        case COM_STMT_FETCH:
            /*  */
            err = err || network_mysqld_proto_peek_int8(packet, &status);
            if (err) return -1;

            switch (status) {
                case MYSQLD_PACKET_EOF: 
                    eof_packet = network_mysqld_eof_packet_new();

                    err = err || network_mysqld_proto_get_eof_packet(packet, eof_packet);
                    if (!err) {
                        if ((eof_packet->server_status & SERVER_STATUS_LAST_ROW_SENT) ||
                                (eof_packet->server_status & SERVER_STATUS_CURSOR_EXISTS)) {
                            is_finished = 1;
                        }
                    }

                    network_mysqld_eof_packet_free(eof_packet);

                    break; 
                case MYSQLD_PACKET_ERR:
                    is_finished = 1;
                    break;
                default:
                    break;
            }
            break;
#endif
        case COM_QUIT: /* sometimes we get a packet before the connection closes */
        case COM_STATISTICS:
            /* just one packet, no EOF */
            is_finished = 1;

            break;
        case COM_STMT_PREPARE:
            is_finished = network_mysqld_proto_get_com_stmt_prepare_result(packet, dbgroup_ctx->parse.data);
            break;
        case COM_STMT_EXECUTE:
            /* COM_STMT_EXECUTE result packets are basically the same as COM_QUERY ones,
             * the only difference is the encoding of the actual data - fields are in there, too.
             */
            //is_finished = network_mysqld_proto_get_com_query_result(packet, con->parse.data, con, TRUE);
            //is_finished = sharding_mysqld_get_com_query_result_meta(dbgroup_ctx, client, packet);
            break;
        case COM_PROCESS_INFO:
        case COM_QUERY:
#ifdef DEBUG_TRACE_QUERY
            g_debug("now con->current_query is %s\n", con->current_query->str);
#endif
            is_finished = sharding_mysqld_get_com_query_result_meta(dbgroup_ctx, client, packet);
            break;
        case COM_BINLOG_DUMP:
            /**
             * the binlog-dump event stops, forward all packets as we see them
             * and keep the command active
             */
            is_finished = 1;
            break;
        default:
            g_critical("%s: COM_(0x%02x) is not handled", 
                    G_STRLOC,
                    dbgroup_ctx->parse.command);
            err = 1;
            break;
    }

    if (err) return -1;

    return is_finished;

    /* forward the response to the client */
}

void sharding_merge_rows(dbgroup_context_t* dbgroup_ctx, sharding_merge_result_t *merge_result, injection* inj) {
    network_socket *server = dbgroup_ctx->server;
    if (!server || !inj->resultset_is_needed || inj->qstat.binary_encoded) return;

    proxy_resultset_t* res = proxy_resultset_new();

    res->result_queue = server->recv_queue->chunks;
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

        g_ptr_array_add(merge_result->rows, row);
        if (merge_result->rows->len >= merge_result->limit)
            break;
        res_row = res_row->next;
    }
    proxy_resultset_free(res);
}

gboolean rewrite_sql(parse_info_t *parse_info, sharding_table_t *rule, GString **packets, glong newid) {
    gboolean status = FALSE; 
    if (NULL == rule || NULL == parse_info || NULL == parse_info->parse_obj || NULL == packets || NULL == *packets) {
        return status;
    }  
    Parse *parse_obj = parse_info->parse_obj;

    ParsedResultItem *t = &parse_obj->parsed.array[0];
    gchar op = COM_QUERY;
    GString *_packets = g_string_new(&op);
    switch(t->sqltype) {
        case SQLTYPE_INSERT:  
            {
                Insert *insert_obj = t->result.insertObj;
                SrcList *src_list = insert_obj->pTabList; 
                IdList *column_list = insert_obj->pColumn;
                ValuesList *value_List = insert_obj->pValuesList; 
                ExprList *set_List = insert_obj->pSetList;                
                gint i;
                gint id_column_idx = -1;

                if (value_List->nValues >= 2 || value_List->nValues <= 0) {
                    return status; // multi write
                }  
                // column define     
                for (i = 0; NULL != column_list && i < column_list->nId; i++) {
                    struct IdList_item *column_item = &column_list->a[i];                  
                    if(0 == strcasecmp(rule->primary_key->str, column_item->zName)) {
                        id_column_idx = i;
                        break;   
                    }                           
                } 
                // only 
                if (NULL == column_list || i >= column_list->nId || -1 == id_column_idx) {
                    g_string_free(_packets, TRUE);
                    return status;
                } 

                if (NULL == value_List->a) {
                    g_string_free(_packets, TRUE);
                    return status;
                }

                g_string_append_printf(_packets, "%s ", "insert into");
                ExprList *expr_list = value_List->a[0]; 
                struct ExprList_item  *value_item = &expr_list->a[id_column_idx]; 
                gchar *empty = "null";
                if (strlen(empty) == value_item->pExpr->token.n && 0 != strncasecmp(empty, value_item->pExpr->token.z, value_item->pExpr->token.n) ) {
                    return status;  
                }
                struct SrcList_item *tab_item = &(src_list->a[0]);   
                if (NULL != tab_item->zDatabase) {
                    g_string_append_printf(_packets, " %s.", tab_item->zDatabase); 
                } 
                g_string_append_printf(_packets, "%s(", tab_item->zName);
                for (i = 0; i < column_list->nId; i++) {
                    struct IdList_item *column_item = &column_list->a[i];                  
                    g_string_append_printf(_packets, "%s,",column_item->zName); 
                }
                _packets = g_string_truncate(_packets, _packets->len - 1);
                g_string_append_c(_packets, ')');
                g_string_append_c(_packets, ' ');
                g_string_append(_packets, "values(");
                for (i = 0; i < column_list->nId; i++) {
                    if (i == id_column_idx) {
                        g_string_append_printf(_packets, "%ld,",newid); 
                    } else {
                        struct ExprList_item *value_item = &expr_list->a[i]; 
                        g_string_append_len(_packets, value_item->pExpr->token.z, value_item->pExpr->token.n);
                        g_string_append_c(_packets, ',');

                    } 
                }  
                _packets = g_string_truncate(_packets, _packets->len - 1);
                g_string_append_c(_packets, ')');
                g_string_free(*packets, TRUE);
                *packets = _packets;
                status = TRUE;
                break;
            }
        case SQLTYPE_SELECT:
            break;
        case SQLTYPE_DELETE:
            status = TRUE;
            break;
        case SQLTYPE_UPDATE:
            status = TRUE;
            break;
        case SQLTYPE_REPLACE:
            status = TRUE;
            break;
        default:
            break;
    } 
    return status;
}
SqlType get_sql_type(parse_info_t *parse_info) {
    if (NULL == parse_info || NULL == parse_info->parse_obj) {
        return SQLTYPE_UNKNOWN; 
    } 
    Parse *parse_obj = parse_info->parse_obj;
    ParsedResultItem *t = &parse_obj->parsed.array[0]; 
    return t->sqltype;
}


G_INLINE_FUNC sharding_key_type_t reverse_sharding_key_type(sharding_key_type_t type) {
    switch(type) {
        case SHARDING_SHARDKEY_VALUE_GT:
            return SHARDING_SHARDKEY_VALUE_LTE;
        case SHARDING_SHARDKEY_VALUE_GTE:
            return SHARDING_SHARDKEY_VALUE_LT;
        case SHARDING_SHARDKEY_VALUE_LT:
            return SHARDING_SHARDKEY_VALUE_GTE;
        case SHARDING_SHARDKEY_VALUE_LTE:
            return SHARDING_SHARDKEY_VALUE_GT;
        case SHARDING_SHARDKEY_VALUE_EQ:
            return SHARDING_SHARDKEY_VALUE_NE;
        case SHARDING_SHARDKEY_VALUE_NE:
            return SHARDING_SHARDKEY_VALUE_EQ;
defalut:
            return SHARDING_SHARDKEY_VALUE_UNKNOWN;

    }

}
G_INLINE_FUNC sharding_key_init(sharding_key_t *shard_key_obj, sharding_key_type_t type, gint64 value, gint64 begin, gint64 end) {
    if (type == SHARDING_SHARDKEY_RANGE) {
        shard_key_obj->type = type;
        shard_key_obj->value = -1;
        shard_key_obj->range_begin = begin;
        shard_key_obj->range_end = end;
    } else {
        shard_key_obj->type = type;
        shard_key_obj->value = value;
        shard_key_obj->range_begin = shard_key_obj->range_end = -1;
    }

}

G_INLINE_FUNC Expr *sharding_get_where_expr(parse_info_t *parse_info) {
    //ParsedResult *result_item = parse_info->parse_obj->parsed.array[0] 
    ParsedResultItem *result_item = &parse_info->parse_obj->parsed.array[0];
    //SqlType sqltype = parse_info->parse_obj->parsed.array[0].sqltype; 
    switch (result_item->sqltype) {
        case SQLTYPE_SELECT:
            return result_item->result.selectObj->pWhere;
        case SQLTYPE_DELETE:
            return result_item->result.deleteObj->pWhere; 
        case SQLTYPE_UPDATE:
            return result_item->result.updateObj->pWhere; 
        default:    
            return NULL;
    } 
    return NULL;
} 

G_INLINE_FUNC gint guint_compare_func(gconstpointer value1, gconstpointer value2) {
    if (*((guint*)value1) > *((guint*)value2)) {
        return 1;
    } else if (*((guint*)value1) == *((guint*)value2)) {
        return 0;
    } else {
        return -1;
    }

}
G_INLINE_FUNC gint gint64_compare_func(gconstpointer value1, gconstpointer value2) {
    if (*((gint64*)value1) > *((gint64*)value2)) {
        return 1;
    } else if (*((gint64*)value1) == *((gint64*)value2)) {
        return 0;
    } else {
        return -1;
    }
}
sharding_result_t value_list_got_sharding_key(GArray *shard_keys, Expr *expr, gboolean is_not_opr) {
    char value_buf[128] = {0};
    gint i;
    if (is_not_opr) {
        GArray *value_list = g_array_sized_new(FALSE, FALSE, sizeof(gint64), expr->pList->nExpr);
        for (i = 0; i < expr->pList->nExpr; i++) {
            Expr *value_expr = expr->pList->a[i].pExpr;
            if (value_expr->op != TK_INTEGER) { continue;  }

            dup_token2buff(value_buf, sizeof(value_buf), value_expr->token);
            gint64 shard_key_value = g_ascii_strtoll(value_buf, NULL, 10);
            g_array_unique_append_val(value_list, &shard_key_value, gint64_compare_func); // value in value_list is sorted by g_array_unique_append_val

        }
        if (value_list->len > 0) {
            sharding_key_t shardkey_obj;
            gint64 shardkey_value1 = g_array_index(value_list, gint64, 0);
            sharding_key_init(&shardkey_obj, SHARDING_SHARDKEY_VALUE_LT, shardkey_value1, -1, -1);
            g_array_append_val(shard_keys, shardkey_obj);

            for (i = 1; i < value_list->len; i++) {
                gint64 shardkey_value2 = g_array_index(value_list, gint64, i);
                gint64 range_begin = shardkey_value1+1, range_end = shardkey_value2-1;
                if (range_begin <= range_end) {
                    sharding_key_init(&shardkey_obj, SHARDING_SHARDKEY_RANGE, -1, range_begin, range_end);
                    g_array_append_val(shard_keys, shardkey_obj);

                }
                shardkey_value1 = shardkey_value2;

            }

            shardkey_value1 = g_array_index(value_list, gint64, value_list->len-1);
            sharding_key_init(&shardkey_obj, SHARDING_SHARDKEY_VALUE_GT, shardkey_value1, -1, -1);
            g_array_append_val(shard_keys, shardkey_obj);

        }
        g_array_free(value_list, TRUE);
    } else {
        for (i = 0; i < expr->pList->nExpr; i++) {
            Expr *value_expr = expr->pList->a[i].pExpr;
            if (value_expr->op != TK_INTEGER) { continue;  }

            dup_token2buff(value_buf, sizeof(value_buf), value_expr->token);
            gint64 shard_key_value = g_ascii_strtoll(value_buf, NULL, 10);
            sharding_key_t shardkey_obj;
            sharding_key_init(&shardkey_obj, SHARDING_SHARDKEY_VALUE_EQ, shard_key_value, -1, -1);
            g_array_append_val(shard_keys, shardkey_obj);
        }
    }
    return SHARDING_RET_OK;
}

G_INLINE_FUNC sharding_result_t sharding_key_list_append(GArray *sharding_keys, Expr *where, sharding_table_t *sharding_table_rule, gboolean is_reverse) {
    Expr *left_expr = where->pLeft;
    const char *shardkey_name = sharding_table_rule->shard_key->str, *shard_table = sharding_table_rule->table_name->str;

    if (left_expr != NULL && LEMON_TOKEN_STRING(left_expr->op) && where->pList != NULL &&
            strncasecmp(shardkey_name, (const char*) left_expr->token.z, left_expr->token.n) == 0) {
        return value_list_got_sharding_key(sharding_keys, where, is_reverse);

    } else if (left_expr != NULL && where->pList != NULL && left_expr->op == TK_DOT && LEMON_TOKEN_STRING(left_expr->pLeft->op) &&
            strncasecmp(shard_table, (const char*)left_expr->pLeft->token.z, left_expr->pLeft->token.n) == 0 &&
            strncasecmp(shardkey_name, (const char*)left_expr->pRight->token.z, left_expr->pRight->token.n) == 0 ) {
        return value_list_got_sharding_key(sharding_keys, where, is_reverse);
    } else {
        return SHARDING_RET_ERR_NO_SHARDKEY;

    }

}
G_INLINE_FUNC sharding_result_t sharding_key_append(GArray *sharding_keys, Expr *where, sharding_table_t *sharding_table_rule, gboolean is_reverse) {
    if (NULL == sharding_table_rule->table_name || NULL == sharding_table_rule->shard_key) {
        return SHARDING_RET_ERR_NOT_SUPPORT; 
    }
    sharding_result_t ret = SHARDING_RET_OK; 
    gchar buf[128] = {0};
    sharding_key_t sharding_key1, sharding_key2; 
    gchar *table_name = sharding_table_rule->table_name->str, *key_name = sharding_table_rule->shard_key->str; 

    Expr *value = NULL; 
    Expr *left = NULL, *right = NULL; 
    if (LEMON_TOKEN_INTEGER(where->pRight->op)) {
        left = where->pLeft; 
        right = where->pRight;
    } else if (LEMON_TOKEN_INTEGER(where->pLeft->op)) {
        left = where->pRight;
        right = where->pLeft;
    } 

    if (left != NULL && right != NULL) {
        if (LEMON_TOKEN_STRING(left->op)) {
            if (strlen(key_name) == left->token.n && 0 == strncasecmp(key_name, left->token.z, left->token.n)) {
                value = right; 
            }
        } else if (LEMON_TOKEN_DOT(left->op) && NULL != left->pLeft && NULL != left->pRight) {
            if (LEMON_TOKEN_STRING(left->pLeft->op) && LEMON_TOKEN_STRING(left->pRight->op)) {
                if (strlen(table_name) == left->pLeft->token.n &&  0 == strncasecmp(table_name, left->pLeft->token.z, left->pLeft->token.n) && 
                        strlen(key_name) == left->pRight->token.n && 0 == strncasecmp(key_name, left->pRight->token.z, left->pRight->token.n)) {
                    value = right;
                }  
            }   
        }
    }
    if (NULL == value) {
        return SHARDING_RET_ERR_NO_SHARDKEY; 
    } 

    sharding_key_type_t key_type = sql_token_id2sharding_key_type(where->op);  
    dup_token2buff(buf, sizeof(buf), value->token); 
    gint64 key_value = g_ascii_strtoll(buf, NULL, 10);
    if (is_reverse) {
        key_type = reverse_sharding_key_type(key_type);  
    } 
    if (SHARDING_SHARDKEY_VALUE_NE == key_type) {
        sharding_key_init(&sharding_key1, SHARDING_SHARDKEY_VALUE_GT, key_value, -1, -1); 
        sharding_key_init(&sharding_key2, SHARDING_SHARDKEY_VALUE_LT, key_value, -1, -1); 
        g_array_append_val(sharding_keys, sharding_key1);
        g_array_append_val(sharding_keys, sharding_key2);
    } else {
        sharding_key_init(&sharding_key1, key_type, key_value, -1, -1); 
        g_array_append_val(sharding_keys, sharding_key1);
    } 
    return ret;
}  

G_INLINE_FUNC void merge_eq_and_eq(GArray *shard_keys, sharding_key_t *shardkey1, sharding_key_t *shardkey2) {
    if (shardkey1->value == shardkey2->value) {
        g_array_append_val(shard_keys, *shardkey1);

    }

}

G_INLINE_FUNC void merge_eq_and_ne(GArray *shard_keys, sharding_key_t *shardkey1, sharding_key_t *shardkey2) {
    if (shardkey1->value != shardkey2->value) {
        g_array_append_val(shard_keys, *shardkey1);

    }

}

G_INLINE_FUNC void merge_eq_and_gt(GArray *shard_keys, sharding_key_t *shardkey1, sharding_key_t *shardkey2) {
    if (shardkey1->value > shardkey2->value) { // id = 10 AND id > 10
        g_array_append_val(shard_keys, *shardkey1);
    }

}

G_INLINE_FUNC void merge_eq_and_gte(GArray *shard_keys, sharding_key_t *shardkey1, sharding_key_t *shardkey2) {
    if (shardkey1->value >= shardkey2->value) { // id = 5 AND id >= 10
        g_array_append_val(shard_keys, *shardkey1);
    }

}
G_INLINE_FUNC void merge_eq_and_lt(GArray *shard_keys, sharding_key_t *shardkey1, sharding_key_t *shardkey2) {
    if (shardkey1->value < shardkey2->value) {
        g_array_append_val(shard_keys, *shardkey1);

    }

}

G_INLINE_FUNC void merge_eq_and_lte(GArray *shard_keys, sharding_key_t *shardkey1, sharding_key_t *shardkey2) {
    if (shardkey1->value <= shardkey2->value) {
        g_array_append_val(shard_keys, *shardkey1);

    }

}

G_INLINE_FUNC void merge_eq_and_range(GArray *shard_keys, sharding_key_t *shardkey1, sharding_key_t *shardkey2) {
    if (shardkey1->value >= shardkey2->range_begin && shardkey1->value <= shardkey2->range_end) {
        g_array_append_val(shard_keys, *shardkey1);

    }

}

G_INLINE_FUNC void merge_ne_and_ne(GArray *shard_keys, sharding_key_t *shardkey1, sharding_key_t *shardkey2) {
    sharding_key_t shardkey_merge1, shardkey_merge2, shardkey_merge3;
    if (shardkey1->value == shardkey2->value) { // id != 10 AND id != 10 ---> id > 10 OR id < 10
        sharding_key_init(&shardkey_merge1, SHARDING_SHARDKEY_VALUE_GT, shardkey1->value, -1, -1);
        sharding_key_init(&shardkey_merge2, SHARDING_SHARDKEY_VALUE_LT, shardkey1->value, -1, -1);
        g_array_append_val(shard_keys, shardkey_merge1);
        g_array_append_val(shard_keys, shardkey_merge2);
    } else if (shardkey1->value > shardkey2->value) { // id != 10 AND id != 5 ---> id > 10 OR (id > 5 AND id < 10) OR id < 5
        sharding_key_init(&shardkey_merge1, SHARDING_SHARDKEY_VALUE_GT, shardkey1->value, -1, -1);
        sharding_key_init(&shardkey_merge2, SHARDING_SHARDKEY_VALUE_LT, shardkey2->value, -1, -1);
        sharding_key_init(&shardkey_merge3, SHARDING_SHARDKEY_RANGE, -1, shardkey2->value+1, shardkey1->value-1);
        g_array_append_val(shard_keys, shardkey_merge1);
        g_array_append_val(shard_keys, shardkey_merge2);
        g_array_append_val(shard_keys, shardkey_merge3);
    } else {// id != 5 AND id != 10 ---> id > 10 OR (id > 5 AND id < 10) OR id < 5
        sharding_key_init(&shardkey_merge1, SHARDING_SHARDKEY_VALUE_GT, shardkey2->value, -1, -1);
        sharding_key_init(&shardkey_merge2, SHARDING_SHARDKEY_VALUE_LT, shardkey1->value, -1, -1);
        sharding_key_init(&shardkey_merge3, SHARDING_SHARDKEY_RANGE, -1, shardkey1->value + 1, shardkey2->value - 1);
        g_array_append_val(shard_keys, shardkey_merge1);
        g_array_append_val(shard_keys, shardkey_merge2);
        g_array_append_val(shard_keys, shardkey_merge3);
    }

}
G_INLINE_FUNC void merge_ne_and_gt(GArray *shard_keys, sharding_key_t *shardkey1, sharding_key_t *shardkey2) {
    sharding_key_t shardkey_merge1, shardkey_merge2;

    if (shardkey1->value <= shardkey2->value) { // id != 10 AND id > 10 --> id > 10
        g_array_append_val(shard_keys, *shardkey2);
    } else { // "id != 10 AND id > 5" ---> "id > 5 AND id < 10 OR id > 10"
        sharding_key_init(&shardkey_merge1, SHARDING_SHARDKEY_RANGE, -1,  shardkey2->value+1, shardkey1->value-1);
        sharding_key_init(&shardkey_merge2, SHARDING_SHARDKEY_VALUE_GT, shardkey1->value, -1, -1);
        g_array_append_val(shard_keys, shardkey_merge1);
        g_array_append_val(shard_keys, shardkey_merge2);
    }

}

G_INLINE_FUNC void merge_ne_and_gte(GArray *shard_keys, sharding_key_t *shardkey1, sharding_key_t *shardkey2) {
    sharding_key_t shardkey_merge1, shardkey_merge2;

    if (shardkey1->value < shardkey2->value) { // id != 10 AND id >= 50 ---> id >= 50
        g_array_append_val(shard_keys, *shardkey2);
    } else if (shardkey1->value == shardkey2->value) { // id != 10 AND id >= 100 ---> id > 100
        shardkey2->type = SHARDING_SHARDKEY_VALUE_GT;
        g_array_append_val(shard_keys, *shardkey2);
    } else { // "id != 10 AND id >= 5" ---> "id >= 5 AND id < 10 OR id > 10"
        sharding_key_init(&shardkey_merge1, SHARDING_SHARDKEY_RANGE, -1, shardkey2->value, shardkey1->value-1);
        sharding_key_init(&shardkey_merge2, SHARDING_SHARDKEY_VALUE_GT, shardkey1->value, -1, -1);
        g_array_append_val(shard_keys, shardkey_merge1);
        g_array_append_val(shard_keys, shardkey_merge2);
    }

}

G_INLINE_FUNC void merge_ne_and_lt(GArray *shard_keys, sharding_key_t *shardkey1, sharding_key_t *shardkey2) {
    sharding_key_t shardkey_merge1, shardkey_merge2;

    if (shardkey1->value >= shardkey2->value) { // id != 5 and id < 5 ---> id < 5
        g_array_append_val(shard_keys, *shardkey2);
    } else { // id != 10 and id < 20 --> "id > 10 AND id < 20 OR id < 10"
        sharding_key_init(&shardkey_merge1, SHARDING_SHARDKEY_RANGE, -1, shardkey1->value+1, shardkey2->value-1);
        sharding_key_init(&shardkey_merge2, SHARDING_SHARDKEY_VALUE_LT, shardkey1->value, -1, -1);
        g_array_append_val(shard_keys, shardkey_merge1);
        g_array_append_val(shard_keys, shardkey_merge2);
    }

}

G_INLINE_FUNC void merge_ne_and_lte(GArray *shard_keys, sharding_key_t *shardkey1, sharding_key_t *shardkey2) {
    sharding_key_t shardkey_merge1, shardkey_merge2;

    if (shardkey1->value > shardkey2->value) { // id != 6 and id <= 5 ---> id <= 5
        g_array_append_val(shard_keys, *shardkey2);
    } else if (shardkey1->value == shardkey2->value) {
        shardkey2->type = SHARDING_SHARDKEY_VALUE_LT;
        g_array_append_val(shard_keys, *shardkey2);

    } else { // id != 10 and id <= 20 --> "id > 10 AND id <= 20 OR id < 10"
        sharding_key_init(&shardkey_merge1,SHARDING_SHARDKEY_RANGE, -1, shardkey1->value+1, shardkey2->value);
        sharding_key_init(&shardkey_merge2, SHARDING_SHARDKEY_VALUE_LT, shardkey1->value, -1, -1);
        g_array_append_val(shard_keys, shardkey_merge1);
        g_array_append_val(shard_keys, shardkey_merge2);
    }

}

G_INLINE_FUNC void merge_ne_and_range(GArray *shard_keys, sharding_key_t *shardkey1, sharding_key_t *shardkey2) {
    sharding_key_t shardkey_merge1, shardkey_merge2;

    if (shardkey1->value < shardkey2->range_begin || shardkey1->value > shardkey2->range_end) {
        g_array_append_val(shard_keys, *shardkey2);

    } else { // id != 10 AND id >= 5 AND id <= 20 ---> id >= 5 AND id < 10 OR id > 10 AND id <= 20
        sharding_key_init(&shardkey_merge1, SHARDING_SHARDKEY_RANGE, -1, shardkey2->range_begin, shardkey1->value-1);
        sharding_key_init(&shardkey_merge2, SHARDING_SHARDKEY_RANGE, -1, shardkey1->value+1, shardkey2->range_end);
        g_array_append_val(shard_keys, shardkey_merge1);
        g_array_append_val(shard_keys, shardkey_merge2);
    }

}

G_INLINE_FUNC void merge_lt_and_gt(GArray *shard_keys, sharding_key_t *shardkey1, sharding_key_t *shardkey2) {
    sharding_key_t shardkey_merge1;

    if (shardkey1->value > shardkey2->value) {
        // id < 10 AND id > 5 ---> id > 5 AND id < 10
        sharding_key_init(&shardkey_merge1, SHARDING_SHARDKEY_RANGE, -1, shardkey2->value+1, shardkey1->value-1);
        g_array_append_val(shard_keys, shardkey_merge1);

    } // id < 5 AND id > 5 ---> empty.

}

G_INLINE_FUNC void merge_lt_and_gte(GArray *shard_keys, sharding_key_t *shardkey1, sharding_key_t *shardkey2) {
    sharding_key_t shardkey_merge1;

    if (shardkey1->value > shardkey2->value) {
        // id < 10 AND id >= 5 ---> id >= 5 AND id < 10
        sharding_key_init(&shardkey_merge1, SHARDING_SHARDKEY_RANGE, -1, shardkey2->value, shardkey1->value-1);
        g_array_append_val(shard_keys, shardkey_merge1);
    }// id < 5 AND id >= 6 ---> empty.

}

G_INLINE_FUNC void merge_lt_and_lt(GArray *shard_keys, sharding_key_t *shardkey1, sharding_key_t *shardkey2) {
    if (shardkey1->value <= shardkey2->value) { // id < 5 AND id < 5
        g_array_append_val(shard_keys, *shardkey1);
    } else { // id < 10 AND id < 5 --> id < 5
        g_array_append_val(shard_keys, *shardkey2);
    }

}

G_INLINE_FUNC void merge_lt_and_lte(GArray *shard_keys, sharding_key_t *shardkey1, sharding_key_t *shardkey2) {
    if (shardkey1->value <= shardkey2->value) { // id < 5 AND id <= 5
        g_array_append_val(shard_keys, *shardkey1);
    } else { // id < 6 AND id <= 5
        g_array_append_val(shard_keys, *shardkey2);
    }

}

G_INLINE_FUNC void merge_lt_and_range(GArray *shard_keys, sharding_key_t *shardkey1, sharding_key_t *shardkey2) {
    if (shardkey1->value <= shardkey2->range_begin) {
        return;

    } else if (shardkey1->value > shardkey2->range_end) {
        g_array_append_val(shard_keys, *shardkey2);

    } else { // id < 10 AND id >= 5 AND id <= 20 ---> id >= 5 AND id < 10
        shardkey2->range_end = shardkey1->value - 1;
        g_array_append_val(shard_keys, *shardkey2);
    }

}

G_INLINE_FUNC void merge_lte_and_gt(GArray *shard_keys, sharding_key_t *shardkey1, sharding_key_t *shardkey2) {
    sharding_key_t shardkey_merge1;

    if (shardkey2->value < shardkey1->value) {
        // id <= 10 and id > 5 ---> id > 5 and id <= 10
        //         init_range_sharding_key_t(&shardkey_merge1, shardkey2->value+1, shardkey1->value);
        //                 g_array_append_val(shard_keys, shardkey_merge1);
        //                     
    }

}

G_INLINE_FUNC void merge_lte_and_gte(GArray *shard_keys, sharding_key_t *shardkey1, sharding_key_t *shardkey2) {
    sharding_key_t shardkey_merge1;

    if (shardkey2->value > shardkey1->value) {
        return;

    } else if (shardkey1->value == shardkey2->value) {
        shardkey1->type = SHARDING_SHARDKEY_VALUE_EQ;
        g_array_append_val(shard_keys, *shardkey1);

    } else { // id <= 10 and id >= 5 ---> id >= 5 and id <= 10
        sharding_key_init(&shardkey_merge1, SHARDING_SHARDKEY_RANGE, -1, shardkey2->value, shardkey1->value);
        g_array_append_val(shard_keys, shardkey_merge1);
    }

}

G_INLINE_FUNC void merge_lte_and_lte(GArray *shard_keys, sharding_key_t *shardkey1, sharding_key_t *shardkey2) {
    if (shardkey1->value <= shardkey2->value) { // id <= 5 AND id <= 5
        g_array_append_val(shard_keys, *shardkey1);
    } else { // id <= 10 AND id <= 5 --> id <= 5
        g_array_append_val(shard_keys, *shardkey2);
    }

}

G_INLINE_FUNC void merge_lte_and_range(GArray *shard_keys, sharding_key_t *shardkey1, sharding_key_t *shardkey2) {
    if (shardkey1->value < shardkey2->range_begin) {
        return;

    } else if (shardkey1->value >= shardkey2->range_end) {
        g_array_append_val(shard_keys, *shardkey2);

    } else { // id < 10 AND id >= 5 AND id <= 20 ---> id >= 5 AND id < 10
        shardkey2->range_end = shardkey1->value;
        g_array_append_val(shard_keys, *shardkey2);
    }

}
G_INLINE_FUNC void merge_gt_and_gt(GArray *shard_keys, sharding_key_t *shardkey1, sharding_key_t *shardkey2) {
    if (shardkey1->value >= shardkey2->value) {
        g_array_append_val(shard_keys, *shardkey1);

    } else {
        g_array_append_val(shard_keys, *shardkey2);

    }

}

G_INLINE_FUNC void merge_gt_and_gte(GArray *shard_keys, sharding_key_t *shardkey1, sharding_key_t *shardkey2) {
    if (shardkey1->value >= shardkey2->value) {
        g_array_append_val(shard_keys, *shardkey1);

    } else {
        g_array_append_val(shard_keys, *shardkey2);

    }

}

G_INLINE_FUNC void merge_gt_and_range(GArray *shard_keys, sharding_key_t *shardkey1, sharding_key_t *shardkey2) {
    if (shardkey1->value >= shardkey2->range_end) {
        return;

    } else if (shardkey1->value < shardkey2->range_begin) {
        g_array_append_val(shard_keys, *shardkey2);

    } else {
        shardkey2->range_begin = shardkey1->value + 1;
        g_array_append_val(shard_keys, *shardkey2);

    }

}
G_INLINE_FUNC void merge_gte_and_gte(GArray *shard_keys, sharding_key_t *shardkey1, sharding_key_t *shardkey2) {
    if (shardkey1->value >= shardkey2->value) {
        g_array_append_val(shard_keys, *shardkey1);

    } else {
        g_array_append_val(shard_keys, *shardkey2);

    }

}

G_INLINE_FUNC void merge_gte_and_range(GArray *shard_keys, sharding_key_t *shardkey1, sharding_key_t *shardkey2) {
    if (shardkey1->value > shardkey2->range_end) {
        return;

    } else if (shardkey1->value <= shardkey2->range_begin) {
        g_array_append_val(shard_keys, *shardkey2);

    } else {
        shardkey2->range_begin = shardkey1->value;
        g_array_append_val(shard_keys, *shardkey2);

    }

}

G_INLINE_FUNC void merge_range_and_range(GArray *shard_keys, sharding_key_t *shardkey1, sharding_key_t *shardkey2) {
    if (shardkey1->range_begin > shardkey2->range_end || shardkey2->range_begin > shardkey1->range_end) {
        return;

    } else if (shardkey1->range_begin <= shardkey2->range_end) {
        shardkey1->range_end = shardkey2->range_end;
        g_array_append_val(shard_keys, *shardkey1);

    } else if (shardkey2->range_begin <= shardkey1->range_end) {
        shardkey1->range_begin = shardkey2->range_begin;
        g_array_append_val(shard_keys, *shardkey1);

    } else if (shardkey1->range_begin <= shardkey2->range_begin && shardkey1->range_end >= shardkey2->range_end) {
        g_array_append_val(shard_keys, *shardkey2);

    } else if (shardkey2->range_begin <= shardkey1->range_begin && shardkey2->range_end >= shardkey1->range_end) {
        g_array_append_val(shard_keys, *shardkey1);

    }

}

void and_opr_merge_shardkey1_value_eq(GArray *shard_keys, sharding_key_t *shardkey1, sharding_key_t *shardkey2) {
    switch(shardkey2->type) {
        case SHARDING_SHARDKEY_VALUE_EQ:
            merge_eq_and_eq(shard_keys, shardkey1, shardkey2);
            break;
        case SHARDING_SHARDKEY_VALUE_NE:
            merge_eq_and_ne(shard_keys, shardkey1, shardkey2);
            break;
        case SHARDING_SHARDKEY_VALUE_GT:
            merge_eq_and_gt(shard_keys, shardkey1, shardkey2);
            break;
        case SHARDING_SHARDKEY_VALUE_GTE:
            merge_eq_and_gte(shard_keys, shardkey1, shardkey2);
            break;
        case SHARDING_SHARDKEY_VALUE_LT:
            merge_eq_and_lt(shard_keys, shardkey1, shardkey2);
            break;
        case SHARDING_SHARDKEY_VALUE_LTE:
            merge_eq_and_lte(shard_keys, shardkey1, shardkey2);
            break;
        case SHARDING_SHARDKEY_RANGE:
            merge_eq_and_range(shard_keys, shardkey1, shardkey2);
            break;
        default:
            break;

    }
}
void and_opr_merge_shardkey1_value_ne(GArray *shard_keys, sharding_key_t *shardkey1, sharding_key_t *shardkey2) {
    switch(shardkey2->type) {
        case SHARDING_SHARDKEY_VALUE_EQ:
            merge_eq_and_ne(shard_keys, shardkey2, shardkey1);
            break;
        case SHARDING_SHARDKEY_VALUE_NE:
            merge_ne_and_ne(shard_keys, shardkey1, shardkey2);
            break;
        case SHARDING_SHARDKEY_VALUE_GT:
            merge_ne_and_gt(shard_keys, shardkey1, shardkey2);
            break;
        case SHARDING_SHARDKEY_VALUE_GTE:
            merge_ne_and_gte(shard_keys, shardkey1, shardkey2);
            break;
        case SHARDING_SHARDKEY_VALUE_LT:
            merge_ne_and_lt(shard_keys, shardkey1, shardkey2);
            break;
        case SHARDING_SHARDKEY_VALUE_LTE:
            merge_ne_and_lte(shard_keys, shardkey1, shardkey2);
            break;
        case SHARDING_SHARDKEY_RANGE:
            merge_ne_and_range(shard_keys, shardkey1, shardkey2);
            break;
        default:
            break;
    }
}
void and_opr_merge_shardkey1_value_lt(GArray *shard_keys, sharding_key_t *shardkey1, sharding_key_t *shardkey2) {
    switch(shardkey2->type) {
        case SHARDING_SHARDKEY_VALUE_EQ:
            merge_eq_and_lt(shard_keys, shardkey2, shardkey1);
            break;
        case SHARDING_SHARDKEY_VALUE_NE:
            merge_ne_and_lt(shard_keys, shardkey2, shardkey1);
            break;
        case SHARDING_SHARDKEY_VALUE_GT:
            merge_lt_and_gt(shard_keys, shardkey1, shardkey2);
            break;
        case SHARDING_SHARDKEY_VALUE_GTE:
            merge_lt_and_gte(shard_keys, shardkey1, shardkey2);
            break;
        case SHARDING_SHARDKEY_VALUE_LT:
            merge_lt_and_lt(shard_keys, shardkey1, shardkey2);
            break;
        case SHARDING_SHARDKEY_VALUE_LTE:
            merge_lt_and_lte(shard_keys, shardkey1, shardkey2);
            break;
        case SHARDING_SHARDKEY_RANGE:
            merge_lt_and_range(shard_keys, shardkey1, shardkey2);
            break;
        default:
            break;

    }

}
void and_opr_merge_shardkey1_value_lte(GArray *shard_keys, sharding_key_t *shardkey1, sharding_key_t *shardkey2) {
    switch(shardkey2->type) {
        case SHARDING_SHARDKEY_VALUE_EQ:
            merge_eq_and_lte(shard_keys, shardkey2, shardkey1);
            break;
        case SHARDING_SHARDKEY_VALUE_NE:
            merge_ne_and_lte(shard_keys, shardkey2, shardkey1);
            break;
        case SHARDING_SHARDKEY_VALUE_GT:
            merge_lte_and_gt(shard_keys, shardkey1, shardkey2);
            break;
        case SHARDING_SHARDKEY_VALUE_GTE:
            merge_lte_and_gte(shard_keys, shardkey1, shardkey2);
            break;
        case SHARDING_SHARDKEY_VALUE_LT:
            merge_lt_and_lte(shard_keys, shardkey2, shardkey1);
            break;
        case SHARDING_SHARDKEY_VALUE_LTE:
            merge_lte_and_lte(shard_keys, shardkey1, shardkey2);
            break;
        case SHARDING_SHARDKEY_RANGE:
            merge_lte_and_range(shard_keys, shardkey1, shardkey2);
            break;
        default:
            break;

    }

}

void and_opr_merge_shardkey1_value_gt(GArray *shard_keys, sharding_key_t *shardkey1, sharding_key_t *shardkey2) {
    switch(shardkey2->type) {
        case SHARDING_SHARDKEY_VALUE_EQ:
            merge_eq_and_gt(shard_keys, shardkey2, shardkey1);
            break;
        case SHARDING_SHARDKEY_VALUE_NE:
            merge_ne_and_gt(shard_keys, shardkey2, shardkey1);
            break;
        case SHARDING_SHARDKEY_VALUE_GT:
            merge_gt_and_gt(shard_keys, shardkey1, shardkey2);
            break;
        case SHARDING_SHARDKEY_VALUE_GTE:
            merge_gt_and_gte(shard_keys, shardkey1, shardkey2);
            break;
        case SHARDING_SHARDKEY_VALUE_LT:
            merge_lt_and_gt(shard_keys, shardkey2, shardkey1);
            break;
        case SHARDING_SHARDKEY_VALUE_LTE:
            merge_lte_and_gt(shard_keys, shardkey2, shardkey1);
            break;
        case SHARDING_SHARDKEY_RANGE:
            merge_gt_and_range(shard_keys, shardkey1, shardkey2);
            break;
        default:
            break;
    }
}

void and_opr_merge_shardkey1_value_gte(GArray *shard_keys, sharding_key_t *shardkey1, sharding_key_t *shardkey2) {
    switch(shardkey2->type) {
        case SHARDING_SHARDKEY_VALUE_EQ:
            merge_eq_and_gte(shard_keys, shardkey2, shardkey1);
            break;
        case SHARDING_SHARDKEY_VALUE_NE:
            merge_ne_and_gte(shard_keys, shardkey2, shardkey1);
            break;
        case SHARDING_SHARDKEY_VALUE_GT:
            merge_gt_and_gte(shard_keys, shardkey2, shardkey1);
            break;
        case SHARDING_SHARDKEY_VALUE_GTE:
            merge_gte_and_gte(shard_keys, shardkey1, shardkey2);
            break;
        case SHARDING_SHARDKEY_VALUE_LT:
            merge_lt_and_gte(shard_keys, shardkey2, shardkey1);
            break;
        case SHARDING_SHARDKEY_VALUE_LTE:
            merge_lte_and_gte(shard_keys, shardkey2, shardkey1);
            break;
        case SHARDING_SHARDKEY_RANGE:
            merge_gte_and_range(shard_keys, shardkey1, shardkey2);
            break;
        default:
            break;

    }

}

void and_opr_merge_shardkey1_value_range(GArray *shard_keys, sharding_key_t *shardkey1, sharding_key_t *shardkey2) {
    switch(shardkey2->type) {
        case SHARDING_SHARDKEY_VALUE_EQ:
            merge_eq_and_range(shard_keys, shardkey2, shardkey1);
            break;
        case SHARDING_SHARDKEY_VALUE_NE:
            merge_ne_and_range(shard_keys, shardkey2, shardkey1);
            break;
        case SHARDING_SHARDKEY_VALUE_GT:
            merge_gt_and_range(shard_keys, shardkey2, shardkey1);
            break;
        case SHARDING_SHARDKEY_VALUE_GTE:
            merge_gte_and_range(shard_keys, shardkey2, shardkey1);
            break;
        case SHARDING_SHARDKEY_VALUE_LT:
            merge_lt_and_range(shard_keys, shardkey2, shardkey1);
            break;
        case SHARDING_SHARDKEY_VALUE_LTE:
            merge_lte_and_range(shard_keys, shardkey2, shardkey1);
            break;
        case SHARDING_SHARDKEY_RANGE:
            merge_range_and_range(shard_keys, shardkey1, shardkey2);
            break;
        default:
            break;
    }
}

void sharding_merge_keys(GArray *sharding_keys, GArray *left_keys, GArray *right_keys) {
    if (NULL == sharding_keys || NULL == left_keys || NULL == right_keys) {
        return;
    }
    guint i, j;
    // empty AND xxx ---> empty
    // xxx  AND empty ---> empty
    for (i = 0; i < left_keys->len; i++) {
        sharding_key_t *shardkey1 = &g_array_index(left_keys, sharding_key_t, i);

        for (j = 0; j < right_keys->len; j++) {
            sharding_key_t *shardkey2 = &g_array_index(right_keys, sharding_key_t, j);

            switch(shardkey1->type) {
                case SHARDING_SHARDKEY_VALUE_EQ:
                    and_opr_merge_shardkey1_value_eq(sharding_keys, shardkey1, shardkey2);
                    break;
                case SHARDING_SHARDKEY_VALUE_NE:
                    and_opr_merge_shardkey1_value_ne(sharding_keys, shardkey1, shardkey2);
                    break;
                case SHARDING_SHARDKEY_VALUE_LT:
                    and_opr_merge_shardkey1_value_lt(sharding_keys, shardkey1, shardkey2);
                    break;
                case SHARDING_SHARDKEY_VALUE_LTE:
                    and_opr_merge_shardkey1_value_lte(sharding_keys, shardkey1, shardkey2);
                    break;
                case SHARDING_SHARDKEY_VALUE_GT:
                    and_opr_merge_shardkey1_value_gt(sharding_keys, shardkey1, shardkey2);
                    break;
                case SHARDING_SHARDKEY_VALUE_GTE:
                    and_opr_merge_shardkey1_value_gte(sharding_keys, shardkey1, shardkey2);
                    break;
                case SHARDING_SHARDKEY_RANGE:
                    and_opr_merge_shardkey1_value_range(sharding_keys, shardkey1, shardkey2);
                    break;
                default:
                    break;
            }
        }
    }
}


sharding_result_t between_got_shardkey(GArray *shard_keys, Expr *expr, gboolean is_not_opr) {
    char value_buf[128] = {0};
    sharding_key_t shardkey1, shardkey2;
    Expr *begin_expr = expr->pList->a[0].pExpr, *end_expr = expr->pList->a[1].pExpr;

    if (begin_expr->op != TK_INTEGER || end_expr->op != TK_INTEGER) {
        return SHARDING_RET_OK;

    }

    dup_token2buff(value_buf, sizeof(value_buf), begin_expr->token);
    gint range_begin = g_ascii_strtoll(value_buf, NULL, 10);
    dup_token2buff(value_buf, sizeof(value_buf), end_expr->token);
    gint range_end = g_ascii_strtoll(value_buf, NULL, 10);
    if (is_not_opr) {
        sharding_key_init(&shardkey1, SHARDING_SHARDKEY_VALUE_LT, range_begin, -1, -1);
        sharding_key_init(&shardkey2, SHARDING_SHARDKEY_VALUE_GT, range_end, -1, -1);
        g_array_append_val(shard_keys, shardkey1);
        g_array_append_val(shard_keys, shardkey2);

    } else {
        if (range_begin > range_end) {
            return SHARDING_RET_OK;
        }
        sharding_key_init(&shardkey1, SHARDING_SHARDKEY_RANGE, -1, range_begin, range_end);
        g_array_append_val(shard_keys, shardkey1);

    }
    return SHARDING_RET_OK;

}
sharding_result_t sharding_key_between_append(GArray *shard_keys, Expr *expr, const sharding_table_t *shard_rule, gboolean is_not_opr) {
    Expr *left_expr = expr->pLeft;
    const char *shardkey_name = shard_rule->shard_key->str, *shard_table = shard_rule->table_name->str;

    if (left_expr != NULL && LEMON_TOKEN_STRING(left_expr->op) && expr->pList != NULL && expr->pList->nExpr == 2 &&
            strncasecmp(shardkey_name, (const char*)left_expr->token.z, left_expr->token.n) == 0) {
        return between_got_shardkey(shard_keys, expr, is_not_opr);
    } else if (left_expr != NULL && left_expr->op == TK_DOT && LEMON_TOKEN_STRING(left_expr->pLeft->op) && expr->pList != NULL &&
            expr->pList->nExpr == 2 && strncasecmp(shard_table, (const char*)left_expr->pLeft->token.z, left_expr->pLeft->token.n) == 0 &&
            strncasecmp(shardkey_name, (const char*)left_expr->pRight->token.z, left_expr->pRight->token.n) == 0) {
        return between_got_shardkey(shard_keys, expr, is_not_opr);
    } else {
        return SHARDING_RET_ERR_NO_SHARDKEY;

    }

}
/*
 * find sharding keys
 **/
G_INLINE_FUNC sharding_result_t sharding_find_keys_inwhere(GArray *sharding_keys, Expr *where, sharding_table_t *sharding_table_rule, gboolean is_reverse) {
    GArray *left_keys = NULL, *right_keys = NULL;
    sharding_result_t ret = SHARDING_RET_OK;  
    switch (where->op) {
        case TK_GE:
        case TK_GT:
        case TK_LE:
        case TK_LT:
        case TK_EQ:
        case TK_NE:
            return sharding_key_append(sharding_keys, where, sharding_table_rule, is_reverse); 
        case TK_IN:
            return sharding_key_list_append(sharding_keys, where, sharding_table_rule, is_reverse);
        case TK_BETWEEN:  
            return sharding_key_between_append(sharding_keys, where, sharding_table_rule, is_reverse);
        case TK_NOT:
            return sharding_find_keys_inwhere(sharding_keys, where->pRight, sharding_table_rule, !is_reverse); // cannot support
        case TK_AND:
        case TK_OR: {
                        GArray *left_keys = g_array_sized_new(FALSE, FALSE, sizeof(sharding_key_t), 2);                      
                        GArray *right_keys = g_array_sized_new(FALSE, FALSE, sizeof(sharding_key_t), 2);                      
                        sharding_result_t left_ret = SHARDING_RET_OK, right_ret = SHARDING_RET_OK;
                        left_ret = sharding_find_keys_inwhere(left_keys, where->pLeft, sharding_table_rule, is_reverse);
                        right_ret = sharding_find_keys_inwhere(right_keys, where->pRight, sharding_table_rule, is_reverse);
                        if (TK_AND == where->op && SHARDING_RET_OK == left_ret && SHARDING_RET_OK == right_ret) {
                            sharding_merge_keys(sharding_keys, left_keys, right_keys);            
                        } else {
                            g_array_append_vals(sharding_keys, left_keys->data, left_keys->len); 
                            g_array_append_vals(sharding_keys, right_keys->data, right_keys->len); 
                        }
                        g_array_free(left_keys, TRUE);
                        g_array_free(right_keys, TRUE);

                        return (left_ret == SHARDING_RET_ERR_NO_SHARDKEY &&
                                right_ret == SHARDING_RET_ERR_NO_SHARDKEY) ? left_ret : SHARDING_RET_OK;
                    }
defalut:
                    return ret;
    };
    return ret;
}
G_INLINE_FUNC sharding_result_t sharding_parse_keys_from_expr(GArray *sharding_keys,parse_info_t *parse_info, sharding_table_t *sharding_table_rule) {
    sharding_result_t ret = SHARDING_RET_OK;
    Expr *where = sharding_get_where_expr(parse_info); 
    sharding_key_t sharding_key; 
    if (NULL == where) {
        sharding_key_init(&sharding_key,SHARDING_SHARDKEY_RANGE, -1, G_MININT64, G_MAXINT64);
        g_array_append_val(sharding_keys, sharding_key); 
        return SHARDING_RET_ALL_SHARD;  
    } 
    gboolean is_reverse = FALSE; // 
    ret = sharding_find_keys_inwhere(sharding_keys, where, sharding_table_rule, is_reverse);     
    if (SHARDING_RET_ERR_NO_SHARDKEY == ret) {
        sharding_key_init(&sharding_key,SHARDING_SHARDKEY_RANGE, -1, G_MININT64, G_MAXINT64);
        g_array_append_val(sharding_keys, sharding_key); 
        return SHARDING_RET_ALL_SHARD;  
    }
    return ret;
} 
G_INLINE_FUNC sharding_result_t sharding_parse_keys_in_expr(GArray *sharding_keys,parse_info_t *parse_info, sharding_table_t *sharding_table_rule) {
    sharding_result_t ret = SHARDING_RET_OK;
    Insert *insert_obj = parse_info->parse_obj->parsed.array[0].result.insertObj;
    gint i = 0;
    gchar value_buf[64] = {0}; 
    if (insert_obj->pSetList != NULL) { // INSERT INTO test(name) SET name = 'test';
        for (i = 0; i < insert_obj->pSetList->nExpr; i++) {
            Expr *value_expr = insert_obj->pSetList->a[i].pExpr;
            if (value_expr->op == TK_INTEGER && strcasecmp(sharding_table_rule->shard_key->str, insert_obj->pSetList->a[i].zName) == 0) {
                sharding_key_t key;
                dup_token2buff(value_buf, sizeof(value_buf), value_expr->token);
                gint64 value = g_ascii_strtoll(value_buf, NULL, 10);
                sharding_key_init(&key, SHARDING_SHARDKEY_VALUE_EQ, value, -1, -1);
                g_array_append_val(sharding_keys, key);
                break;
            }
        }
        if (i == insert_obj->pSetList->nExpr) {
            return SHARDING_RET_ERR_NO_SHARDCOLUMN_GIVEN;
        }
    } else if(insert_obj->pValuesList != NULL){
        if (insert_obj->pColumn != NULL) {
            for (i = 0; i < insert_obj->pColumn->nId; i++) {
                if (strcasecmp(sharding_table_rule->shard_key->str, insert_obj->pColumn->a[i].zName) == 0) {
                    break;
                }
            }
            if (i == insert_obj->pColumn->nId) {
                return SHARDING_RET_ERR_NO_SHARDCOLUMN_GIVEN;
            }
        } else {
            i = 0;  // default is 0
        }

        gint key_idx = i;
        for (i = 0; i < insert_obj->pValuesList->nValues; i++) {
            ExprList *value_list = insert_obj->pValuesList->a[i]; // 
            Expr *value_expr = value_list->a[key_idx].pExpr;
            if (value_expr->op == TK_INTEGER) {
                sharding_key_t key;
                dup_token2buff(value_buf, sizeof(value_buf), value_expr->token);
                gint64 value = g_ascii_strtoll(value_buf, NULL, 10);
                sharding_key_init(&key, SHARDING_SHARDKEY_VALUE_EQ, value, -1, -1);
                g_array_append_val(sharding_keys, key);
            }
        } 
    }       
    return ret;
} 


G_INLINE_FUNC void sharding_get_distinct_index_by_hash(GHashTable *hited_index, const sharding_table_t *sharding_table_rule, sharding_key_t *sharding_key) {
    if (!hited_index || !sharding_table_rule || !sharding_key) {
        return;
    }
    gint *key = g_new0(gint, 1); 
    *key = (sharding_key->value)%(sharding_table_rule->shard_groups->len); 
    g_hash_table_add(hited_index, key);
} 

G_INLINE_FUNC void sharding_get_distinct_index_by_range(GHashTable *hited_index, const sharding_table_t *sharding_table_rule, sharding_key_t *sharding_key) {
    guint i = 0;
    GArray *sharding_groups = sharding_table_rule->shard_groups;
    for (i = 0; i < sharding_groups->len; ++i) {
        group_range_map_t *range_map = &g_array_index(sharding_groups, group_range_map_t, i);
        if( sharding_key->type == SHARDING_SHARDKEY_VALUE_EQ) {
            if (range_map->range_begin <= sharding_key->value && sharding_key->value <= range_map->range_end) {
                g_hash_table_add(hited_index, &range_map->group_index); 
                break;
            }

        } else if (sharding_key->type == SHARDING_SHARDKEY_RANGE) { // if shardkey range begin or end are contained in range_map range, will hit the db group
            if ((range_map->range_begin <= sharding_key->range_begin && sharding_key->range_begin <= range_map->range_end) ||
                    (range_map->range_begin <= sharding_key->range_end && sharding_key->range_end <= range_map->range_end) ||
                    (sharding_key->range_begin < range_map->range_begin && range_map->range_end < sharding_key->range_end)) {// shardkey's range is include range_map's range
                g_hash_table_add(hited_index, &range_map->group_index); 
            }
        } else if (sharding_key->type == SHARDING_SHARDKEY_VALUE_GT) {
            if (sharding_key->value < range_map->range_end) {
                g_hash_table_add(hited_index, &range_map->group_index); 
                continue;

            }
        } else if (sharding_key->type == SHARDING_SHARDKEY_VALUE_GTE) {
            if (sharding_key->value <= range_map->range_end) {
                g_hash_table_add(hited_index, &range_map->group_index); 
                continue;
            }

        } else if (sharding_key->type == SHARDING_SHARDKEY_VALUE_LT) {
            if (range_map->range_begin < sharding_key->value ) {
                g_hash_table_add(hited_index, &range_map->group_index); 
                continue;

            }

        } else if (sharding_key->type == SHARDING_SHARDKEY_VALUE_LTE) {
            if (range_map->range_begin <= sharding_key->value) {
                g_hash_table_add(hited_index, &range_map->group_index); 
                continue;
            }
        }
    }
}
/*
 *  hit groups 
 **/
G_INLINE_FUNC sharding_result_t sharding_get_dbgroups_by_hash(GArray *hit_groups, sharding_table_t *sharding_table_rule, GArray *sharding_keys) {
    sharding_result_t ret = SHARDING_RET_OK;
    GHashTable *hited_index = g_hash_table_new_full(g_int_hash, g_int_equal, g_free, NULL);

    gboolean simple_query = TRUE;
    guint i; 
    for (i = 0; i < sharding_keys->len; i++) {
        sharding_key_t *sharding_key = &g_array_index(sharding_keys, sharding_key_t, i);
        if (SHARDING_SHARDKEY_VALUE_EQ != sharding_key->type) {
            simple_query = FALSE;
            break;            
        } 
        sharding_get_distinct_index_by_hash(hited_index, sharding_table_rule, sharding_key);
    } 

    GArray *sharding_groups = sharding_table_rule->shard_groups;
    if (FALSE == simple_query) {
        for (i = 0; i < sharding_groups->len; i++) {
            group_hash_map_t *hash_map = &g_array_index(sharding_groups, group_hash_map_t, i);
            dbgroup_context_t *dbgroup_ctx = dbgroup_context_new(hash_map->group_name); 
            g_array_append_val(hit_groups, dbgroup_ctx);
        }
    } else {
        for (i = 0; i < sharding_groups->len; i++) {
            if (g_hash_table_contains(hited_index, &i)) {
                group_hash_map_t *hash_map = &g_array_index(sharding_groups, group_hash_map_t, i);
                dbgroup_context_t *dbgroup_ctx = dbgroup_context_new(hash_map->group_name); 
                g_array_append_val(hit_groups, dbgroup_ctx);
            }
        }
    }

    if (0 == hit_groups->len) {
        ret = SHARDING_RET_ERR_HIT_NOTHING; 
    }

    g_hash_table_remove_all(hited_index);
    g_hash_table_destroy(hited_index); 
    return ret; 
}


G_INLINE_FUNC sharding_result_t sharding_get_dbgroups_by_range(GArray *hited_groups, sharding_table_t *sharding_table_rule, GArray *sharding_keys) {
    sharding_result_t ret = SHARDING_RET_OK;
    GHashTable *hited_index = g_hash_table_new(g_int_hash, g_int_equal);

    guint i;
    for (i = 0; i < sharding_keys->len; ++i) {
        sharding_key_t *sharding_key = &g_array_index(sharding_keys, sharding_key_t, i);
        sharding_get_distinct_index_by_range(hited_index, sharding_table_rule, sharding_key);
    }

    GArray *sharding_groups = sharding_table_rule->shard_groups;
    for (i = 0; i < sharding_groups->len; i++) {
        group_range_map_t *range_map = &g_array_index(sharding_groups, group_range_map_t, i);
        if (g_hash_table_contains(hited_index, &range_map->group_index)) {
            dbgroup_context_t *dbgroup_ctx = dbgroup_context_new(range_map->group_name); 
            g_array_append_val(hited_groups, dbgroup_ctx);
        }

    }
    if (0 == hited_groups->len) {
        ret = SHARDING_RET_ERR_HIT_NOTHING; 
    }
    g_hash_table_remove_all(hited_index);
    g_hash_table_destroy(hited_index); 
    return ret;
}


G_INLINE_FUNC sharding_result_t sharding_get_sharding_keys(GArray *sharding_keys, parse_info_t *parse_info, sharding_table_t *sharding_table_rule) {
    sharding_result_t ret = SHARDING_RET_ERR_NOT_SUPPORT;    
    if (parse_info->parse_obj->parsed.curSize <= 0) {
        return SHARDING_RET_ERR_HIT_NOTHING; 
    } 
    SqlType sqltype = parse_info->parse_obj->parsed.array[0].sqltype; 

    if (sqltype == SQLTYPE_SELECT || sqltype == SQLTYPE_DELETE || sqltype == SQLTYPE_UPDATE) {
        ret = sharding_parse_keys_from_expr(sharding_keys, parse_info, sharding_table_rule);  // get id range 
    } else if (sqltype == SQLTYPE_INSERT) {
        ret = sharding_parse_keys_in_expr(sharding_keys, parse_info, sharding_table_rule);  // parse column value 
    }  
    return ret;
}
sharding_result_t sharding_get_dbgroups(GArray *dbgroups, parse_info_t *parse_info, sharding_table_t *sharding_table_rule) {

    sharding_result_t ret = SHARDING_RET_ERR_HIT_NOTHING; 
    if (NULL == dbgroups || NULL == parse_info || NULL == sharding_table_rule) {
        return ret; 
    }
    GArray *sharding_keys = g_array_sized_new(FALSE, TRUE, sizeof(sharding_key_t), 4);
    ret = sharding_get_sharding_keys(sharding_keys, parse_info, sharding_table_rule);
    if (0 == sharding_keys->len) {
        ret = SHARDING_RET_ERR_HIT_NOTHING; 
        g_array_free(sharding_keys, TRUE);
        return ret;
    }  
    if (SHARDING_TYPE_RANGE == sharding_table_rule->shard_type) {
        ret = sharding_get_dbgroups_by_range(dbgroups, sharding_table_rule, sharding_keys); 
    } else if(SHARDING_TYPE_HASH == sharding_table_rule->shard_type) {
        ret = sharding_get_dbgroups_by_hash(dbgroups, sharding_table_rule, sharding_keys); 
    } else {
        //TODO(dengyihao):to add more sharding type
    } 
    g_array_free(sharding_keys, TRUE);
    return ret;
}   
