
#include <glib/gstdio.h>
#include "parse_config_file.h"
#include "json.h"
#include "json_util.h"

static void string_free(gpointer data, gpointer user_data){
    g_free(data);
}
void string_array_free(GPtrArray *arr){
    if (NULL == arr) { 
        return; 
    }
    g_ptr_array_foreach(arr, string_free, NULL);
    g_ptr_array_free(arr, TRUE); 
}


gint get_json_object_int(json_object *json, const gchar *key){
    gint ret = 0;
    if (json == NULL || key == NULL) { 
        return ret;
    } 

    json_object *obj = json_object_object_get(json, key);
    if (obj == NULL || FALSE == json_object_is_type(obj, json_type_int)) { 
        return ret; 
    } 

    ret = json_object_get_int(obj);
    return ret;
}

gboolean get_json_object_bool(json_object *json, const gchar *key){
    gboolean ret = FALSE;
    if (json == NULL || key == NULL) {
        return ret;
    } 

    json_object *obj = json_object_object_get(json, key);
    if (obj == NULL || FALSE == json_object_is_type(obj, json_type_boolean)) { 
        return ret; 
    } 

    ret = json_object_get_boolean(obj);
    return ret;
}
gchar *get_json_object_string(json_object *json, const gchar *key) {
    gchar *ret = NULL;
    if (NULL == json || NULL == key) { 
        return ret;
    } 

    json_object *obj = json_object_object_get(json, (char *)key);
    if (NULL == obj || FALSE == json_object_is_type(obj, json_type_string)) {
        return ret; 
    } 

    ret = (gchar *)json_object_get_string(obj);
    return ret;
} 
GPtrArray *get_json_object_string_array(json_object *json, const gchar *key) {
    if (NULL == json|| NULL == key) { 
        return NULL;
    }
    json_object *obj = json_object_object_get(json, key);     
    if (NULL == obj) { 
        return NULL; 
    }
    gint i;
    GPtrArray *ret = g_ptr_array_new(); 
    gint len = json_object_array_length(obj); 
    for (i = 0; i < len; ++i) {
         json_object *t = json_object_array_get_idx(obj, i); 
         gchar *val = g_strdup(json_object_get_string(t)); 
         g_ptr_array_add(ret, val);
    }
    return ret; 
}

// 
node_info_t* node_info_new() {
    node_info_t *t = g_slice_new0(node_info_t);
    t->slaves = g_ptr_array_new(); 
    return t;
}
void node_info_free(gpointer data, gpointer user_data) {
    node_info_t *node = (node_info_t *)data;
    if (NULL == node) { 
        return;
    } 
    string_free(node->node_name, NULL);
    string_free(node->master, NULL);

    GPtrArray *slaves = node->slaves;

    if (slaves != NULL) {
        guint j;       
        for(j = 0; j < slaves->len; ++j) {
            slaves_info_t *slave;
            slave = g_ptr_array_index(slaves, j);
            string_free(slave->slaves_name, NULL);
            string_array_free(slave->ips);
            g_slice_free(slaves_info_t, slave); 
        }
    }
    g_slice_free(node_info_t, node);
}
void nodes_info_free(GPtrArray *nodes){
   if (NULL == nodes) { 
       return; 
   } 
   g_ptr_array_foreach(nodes, node_info_free, NULL);
   g_ptr_array_free(nodes, TRUE);
}
schema_info_t* schema_info_new(){
    schema_info_t *t= g_slice_new0(schema_info_t); 
    t->sharding_rules = g_ptr_array_new();
    return t;
}
void schema_info_free(schema_info_t *schema) {
    if (NULL == schema) {
        return;
    }
    string_free(schema->default_node, NULL);
    string_array_free(schema->nodes);
    GPtrArray *rules = schema->sharding_rules;
    if (rules != NULL) {
        guint i; 
        for (i = 0; i < rules->len; ++i) {
            sharding_rule_t *rule = g_ptr_array_index(rules, i);         
            string_free(rule->db_name, NULL);
            string_free(rule->table, NULL);
            string_free(rule->sharding_key, NULL);
            string_free(rule->primary_key, NULL);
            string_free(rule->type, NULL);
            string_array_free(rule->nodes);
            
            g_slice_free(sharding_rule_t, rule);
        }
    }
    g_slice_free(schema_info_t, schema);
}

chassis_config_t* chassis_config_new(){
    chassis_config_t *config;
    config = g_slice_new0(chassis_config_t);
    config->nodes = g_ptr_array_new(); 
    config->schema = schema_info_new();
    return config; 
}
//TODO free config
void chassis_config_free(chassis_config_t* config){
    if (!config) { 
        return;     
    }
     // comm para free
    if(config->plugin_names) g_strfreev(config->plugin_names);
    if(config->lua_subdirs) g_strfreev(config->lua_subdirs);

    string_free(config->user, NULL); 
    string_free(config->base_dir, NULL); 
    string_free(config->default_file, NULL); 
    string_free(config->pid_file, NULL);
    string_free(config->log_level, NULL);
    string_free(config->log_path, NULL);
    string_free(config->lua_path, NULL);
    string_free(config->lua_cpath, NULL);
    string_free(config->instance_name, NULL);
    string_free(config->charset, NULL);
    string_free(config->admin_username, NULL); 
    string_free(config->admin_passwd, NULL); 
    string_free(config->admin_address, NULL);
    string_free(config->admin_lua_script, NULL);
    string_free(config->proxy_address, NULL);
    string_free(config->sql_log_type, NULL);

    string_array_free(config->forbiddenSQL);     
    string_array_free(config->client_ips);     
    string_array_free(config->pwds);     
    string_array_free(config->lvs_ips);     


    //nodes info free
    nodes_info_free(config->nodes);
    //schema info free
    schema_info_free(config->schema);   

    g_slice_free(chassis_config_t, config);
}

gboolean parse_nodes_para(chassis_config_t *config, json_object *jso, const gchar *key) {
    gboolean ret = FALSE;
    if (config == NULL || jso == NULL || key == NULL) {
        return ret;
    }
    int i = 0, len;
    json_object *nodes =json_object_object_get(jso, key);
    if (FALSE == (ret = json_object_is_type(nodes, json_type_array))) {
        return ret;
    }
    len = json_object_array_length(nodes);
    for (i = 0; i < len; i++) {
        json_object *res; 
        json_object *sub_node_array;
        gchar *node_name; 
        gchar *node_master;
        gchar *slave_name; 
        gint  j, sub_node_len;
        
        res = json_object_array_get_idx(nodes, i);
        /*node name*/
        node_info_t *node = node_info_new(); 
        node->node_name = g_strdup(get_json_object_string(res, "name"));
        node->master = g_strdup(get_json_object_string(res, "master"));

        /*slavs info*/        
        sub_node_array = json_object_object_get(res, "slaves");
        if ( FALSE == (ret = json_object_is_type(sub_node_array, json_type_array))) {
            return ret;
        }
        sub_node_len = json_object_array_length(sub_node_array); 
    
        for (j = 0; j < sub_node_len; ++j) {
            json_object *sub_node;
            slaves_info_t *slaves; 
            gchar *slave_name; 
            GPtrArray *ips = NULL;
            
            sub_node = json_object_array_get_idx(sub_node_array, j); 
            slave_name = g_strdup(get_json_object_string(sub_node, "name"));  
            ips = get_json_object_string_array(sub_node, "ips"); 

            slaves = g_slice_new0(slaves_info_t); 
            slaves->slaves_name = slave_name;
            slaves->ips = ips;

            g_ptr_array_add(node->slaves, slaves);
        }
        g_ptr_array_add(config->nodes, node);
    }
    return ret;
}
gboolean parse_schema_para(chassis_config_t *config, json_object *jso, const gchar *key) {
    gboolean ret = FALSE; 
    if (config == NULL || jso == NULL || key == NULL) {
        return ret;
    }
    gint i, len; 
    schema_info_t *schema = config->schema; 
    json_object *json_schema = json_object_object_get(jso, key);
    schema->nodes = get_json_object_string_array(json_schema, "groups"); 
    schema->default_node = g_strdup(get_json_object_string(json_schema, "default"));

    json_object *sharding_rules = json_object_object_get(json_schema, "sharding-rule");
    if (FALSE == (ret = json_object_is_type(sharding_rules, json_type_array))) {
        return ret;
    }
    len = json_object_array_length(sharding_rules);
    for (i = 0; i < len; ++i) {
        json_object *sub = json_object_array_get_idx(sharding_rules, i);
        sharding_rule_t *rule = g_slice_new0(sharding_rule_t);        
        rule->db_name = g_strdup(get_json_object_string(sub, "db"));
        rule->table = g_strdup(get_json_object_string(sub, "table"));
        rule->sharding_key = g_strdup(get_json_object_string(sub, "sharding-key")); 
        rule->primary_key = g_strdup(get_json_object_string(sub, "primary-key")); 
        rule->nodes = get_json_object_string_array(sub, "policy");
        rule->type = g_strdup(get_json_object_string(sub, "type"));
        rule->auto_inc = get_json_object_bool(sub, "autoIncrement");
        g_ptr_array_add(schema->sharding_rules, rule);
    }
    config->schema = schema;
    return ret;
}
gboolean  parse_comm_para(chassis_config_t *config, json_object *jso){
    /*get forbidden SQL*/ 
    gboolean ret = FALSE;
    config->forbiddenSQL =  get_json_object_string_array(jso, "forbidden-sql");
    /*comm configure*/
    config->pwds = get_json_object_string_array(jso, "pwds");
    config->admin_username = g_strdup(get_json_object_string(jso, "admin-username"));
    config->admin_passwd = g_strdup(get_json_object_string(jso, "admin-password")); 
    config->admin_address = g_strdup(get_json_object_string(jso, "admin-address")); 
    config->admin_lua_script = g_strdup(get_json_object_string(jso, "admin-lua-script")); 
    config->proxy_address = g_strdup(get_json_object_string(jso, "proxy-address"));
    config->charset = g_strdup(get_json_object_string(jso, "charset")); 
    config->event_thread_count = get_json_object_int(jso, "event-threads"); 
    config->daemon_mode = get_json_object_bool(jso, "daemon"); 
    config->log_level = g_strdup(get_json_object_string(jso, "log-level"));
    config->log_path = g_strdup(get_json_object_string(jso, "log-path"));
    config->instance_name = g_strdup(get_json_object_string(jso, "instance"));
    config->auto_restart = get_json_object_bool(jso, "keepalive");
    config->sql_log_type = g_strdup(get_json_object_string(jso, "sql-log"));

    config->sql_safe_update = get_json_object_bool(jso, "sql-safe-update");
    config->max_conn_in_pool = get_json_object_int(jso, "max-conn-in-pool"); 
    if (0 == config->max_conn_in_pool) {
        config->max_conn_in_pool = 5000; 
    }
    config->set_router_rule = get_json_object_bool(jso, "set-router-rule"); 
    config->auto_sql_filter = get_json_object_bool(jso, "auto-sql-filter");

    config->lvs_ips = get_json_object_string_array(jso, "lvs-ips"); 
    config->client_ips = get_json_object_string_array(jso, "clients-ips"); 

    config->shutdown_timeout = get_json_object_int(jso, "shutdown-timeout");
    config->offline_timeout = get_json_object_int(jso, "offline-timeout");
    config->wait_timeout = get_json_object_int(jso, "wait-timeout");

    ret = TRUE;
    return ret; 
}
gboolean parse_config_file(chassis_config_t *config){
    //const char *filename = "./mysql-proxy.json";
    gboolean ret = FALSE; 
    if(config == NULL || config->default_file == NULL)    
        return ret;
    gchar *file = config->default_file;    
    json_object *jso = json_object_from_file(file);
    if (NULL == jso) {
        /*TODO(dengyihao):add err to log */
        json_util_get_last_err();
        return ret;
    }

    ret = parse_nodes_para(config, jso, "groups") /*groups parameter*/
        && parse_schema_para(config, jso, "schema") /*schema parameter*/
        && parse_comm_para(config, jso); /*common parameter*/
    json_object_put(jso); 
    return ret;
}




