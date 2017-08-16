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
 

#ifndef _CHASSIS_MAINLOOP_H_
#define _CHASSIS_MAINLOOP_H_

#include <glib.h>    /* GPtrArray */

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#ifdef HAVE_SYS_TIME_H
#include <sys/time.h>  /* event.h needs struct tm */
#endif
#ifdef HAVE_SYS_TYPES_H
#include <sys/types.h>
#endif
#ifdef _WIN32
#include <winsock2.h>
#endif
#include <event.h>     /* struct event_base */

#include <gmodule.h>

#include "chassis-exports.h"

/* current magic is 0.8.0-1 */
#define CHASSIS_PLUGIN_MAGIC 0x00080001L
#include "chassis-exports.h"
#include "chassis-log.h"
#include "chassis-stats.h"
#include "chassis-shutdown-hooks.h"
#include "lua-scope.h"
#include "network-backend.h"
#include "../util/parse_config_file.h"
#include "../util/id_generator_local.h"
//#include "mysql-proxy-cli.h"

/** @defgroup chassis Chassis
 * 
 * the chassis contains the set of functions that are used by all programs
 *
 * */
/*@{*/

typedef struct chassis_plugin_stats chassis_plugin_stats_t;
typedef struct chassis_plugin_config chassis_plugin_config;

typedef struct chassis chassis;
typedef struct chassis_plugin {
	long      magic;    /**< a magic token to verify that the plugin API matches */

	gchar    *option_grp_name;     /**< name of the option group (used in --help-<option-grp> */
	gchar    *name;     /**< user visible name of this plugin */
	gchar    *version;  /**< the plugin's version number */
	GModule  *module;   /**< the plugin handle when loaded */
	
	chassis_plugin_stats_t *stats;	/**< contains the plugin-specific statistics */

	chassis_plugin_stats_t *(*new_stats)(void);		/**< handler function to initialize the plugin-specific stats */
	void (*free_stats)(chassis_plugin_stats_t *user_data);	/**< handler function to dealloc the plugin-specific stats */
	GHashTable *(*get_stats)(chassis_plugin_stats_t *user_data);	/**< handler function to retrieve the plugin-specific stats */

	chassis_plugin_config *config;  /**< contains the plugin-specific config data */

	chassis_plugin_config *(*init)(void);   /**< handler function to allocate/initialize a chassis_plugin_config struct */
	void     (*destroy)(chassis_plugin_config *user_data);  /**< handler function used to deallocate the chassis_plugin_config */
	GOptionEntry * (*get_options)(chassis_plugin_config *user_data); /**< handler function to obtain the command line argument information */
	//int      (*apply_config)(chassis *chas, chassis_plugin_config * user_data); /**< handler function to set the argument values in the plugin's config */
	int      (*apply_config)(chassis *chas, chassis_config_t * config, chassis_plugin_config *plugin); /**< handler function to set the argument values in the plugin's config */
    
    void*    (*get_global_state)(chassis_plugin_config *user_data, const char* member);     /**< handler function to retrieve the plugin's global state */
    
}chassis_plugin;

CHASSIS_API chassis_plugin *chassis_plugin_init(void) G_GNUC_DEPRECATED;
CHASSIS_API chassis_plugin *chassis_plugin_new(void);
CHASSIS_API chassis_plugin *chassis_plugin_load(const gchar *name);
CHASSIS_API void chassis_plugin_free(chassis_plugin *p);
CHASSIS_API GOptionEntry * chassis_plugin_get_options(chassis_plugin *p);

/**
 * Retrieve the chassis plugin for a particular name.
 * 
 * @param chas        a pointer to the chassis
 * @param plugin_name The name of the plugin to look up.
 * @return A pointer to a chassis_plugin structure
 * @retval NULL if there is no loaded chassis with this name
 */
CHASSIS_API chassis_plugin* chassis_plugin_for_name(chassis *chas, gchar* plugin_name);
typedef struct chassis_frontend_t{
    int print_version;
    int verbose_shutdown;

    int daemon_mode;
    gchar *user;

    gchar *base_dir;
    int auto_base_dir;

    gchar *default_file;
    GKeyFile *keyfile;

    chassis_plugin *p;
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

} chassis_frontend_t;

//  statistics of all the com_query, added in 20160226

//;
typedef struct {
    guint com_select;
    guint com_insert;
    guint com_update;
    guint com_delete;
    guint com_replace;

    guint com_error;

    guint threads_connected;
    gint threads_running;

    GMutex mutex;

    gint64 start_time;
    gint64 up_time;
} statistics_t;

//typedef struct{
//    GHashTable *process_list;  // select * from processlist       
//    GMutex mutex; 
//}process_list_t;

typedef struct{
    gchar *query;    
    gchar *client_addr;
    gchar *server_addr;
    gchar *database;    
    gchar *user;
}process_list_content_t;

struct chassis {
	struct event_base *event_base;
	gchar *event_hdr_version;

	GPtrArray *modules;                       /**< array(chassis_plugin) */

	gchar *base_dir;				/**< base directory for all relative paths referenced */
	gchar *log_path;				/**< log directory */
	gchar *user;					/**< user to run as */
	gchar *instance_name;					/**< instance name*/

	chassis_log *log;
	
	chassis_stats_t *stats;			/**< the overall chassis stats, includes lua and glib allocation stats */

	/* network-io threads */
	guint event_thread_count;

	GPtrArray *threads;

	chassis_shutdown_hooks_t *shutdown_hooks;

	lua_scope *sc;

	//network_backends_t *backends;
    network_nodes_t *nodes;

    chassis_frontend_t *proxy_config;
    chassis_config_t *config; 
    statistics_t *statistics; 
    //GPtrArray *list;
    gint wait_timeout;
    
    //guint conn_cnt;
    dynamic_router_para_t *router_para;

    GPtrArray *parse_objs;  
    
    //id_generator *id_generators;
    id_generators_wrapper_t id_generators;
    
    gint read_timeout;    
    //GHashTable *sharding_rules; 
};

CHASSIS_API chassis *chassis_new(void);
CHASSIS_API void chassis_free(chassis *chas);
CHASSIS_API int chassis_check_version(const char *lib_version, const char *hdr_version);

/**
 * the mainloop for all chassis apps 
 *
 * can be called directly or as gthread_* functions 
 */
CHASSIS_API int chassis_mainloop(void *user_data, chassis_config_t *config);

CHASSIS_API void chassis_set_shutdown_location(const gchar* location);
CHASSIS_API gboolean chassis_is_shutdown(void);

#define chassis_set_shutdown() chassis_set_shutdown_location(G_STRLOC)

/*@}*/

#endif
