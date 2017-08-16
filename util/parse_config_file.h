
#ifndef _PARSE_CONFIG_FILE_H_
#define _PARSE_CONFIG_FILE_H_

#include <glib.h> 
//#include "json.h"
//#include "json_util.h"


typedef gboolean status;
/*
 * node info  
 */
typedef struct slaves_info_t{
    gchar *slaves_name;
    GPtrArray *ips; 
}slaves_info_t;

typedef struct node_info_t{
    gchar *node_name;
    gchar *master;  
    GPtrArray *slaves;
} node_info_t;


/*
 * schema ruler  
 */
typedef struct sharding_rule_t {
    gchar *db_name;
    gchar *table; 
    gchar *sharding_key;
    gchar *primary_key;
    GPtrArray *nodes;
    //GHashTable *nodes;
    gchar *type; 
    gboolean auto_inc;
    
} sharding_rule_t; 

typedef struct schema_info_t {
    GPtrArray *nodes; 
    gchar *default_node;
    GPtrArray *sharding_rules;
} schema_info_t;


typedef struct chassis_config_t {
    gint print_version; 
    gint verbose_shutdown;
    gint daemon_mode;
    gchar *user;

    gchar *base_dir;
    gint auto_base_dir;

    gchar *default_file;
    GKeyFile *keyfile;

    //chassis_plugin *p;
    GOptionEntry *config_entries;

    gchar *pid_file;

    gchar *plugin_dir;
    gchar **plugin_names;

    guint invoke_dbg_on_crash;

#ifndef _WIN32
    /* the --keepalive option isn't available on Unix */
    guint auto_restart;
#endif

    gint max_files_number;

    gint event_thread_count;

    gchar *log_level;
    gchar *log_path;
    int    use_syslog;

    char *lua_path;
    char *lua_cpath;
    char **lua_subdirs;

    gchar *instance_name;
    gchar *charset;

    gint wait_timeout;

    GPtrArray *nodes;    
    schema_info_t *schema;
    GPtrArray *forbiddenSQL;
    GPtrArray *client_ips;
    GPtrArray *pwds;
    GPtrArray *lvs_ips; 

    gchar *admin_username;
    gchar *admin_passwd;
    gchar *admin_address;
    gchar *admin_lua_script;
    gchar *proxy_address;
    gchar *sql_log_type; 

    gboolean sql_safe_update;
    gint max_conn_in_pool;
    gboolean set_router_rule;
    gboolean auto_sql_filter;

    gint shutdown_timeout;
    gint offline_timeout;
} chassis_config_t;

//void comm_configure(json_object *json);
chassis_config_t* chassis_config_new();
void chassis_config_free(chassis_config_t *config); 
gboolean parse_config_file(chassis_config_t *config);

//gchar* get_json_object_string(json_object*, gchar*);

//gint get_json_object_int(json_object *json, const gchar *key);
//gboolean get_json_object_bool(json_object *json, const gchar *key);
//gchar *get_json_object_string(json_object *json, const gchar *key);
//GPtrArray *get_json_object_string_array(json_object *json, const gchar *key);

// a serial of set function 


#endif
