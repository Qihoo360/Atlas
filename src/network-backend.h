/* $%BEGINLICENSE%$
 Copyright (c) 2008, 2009, Oracle and/or its affiliates. All rights reserved.

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
 

#ifndef _BACKEND_H_
#define _BACKEND_H_

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#define PWD_SUCCESS		0
#define ERR_USER_EXIST		1
#define ERR_USER_NOT_EXIST	1
#define ERR_PWD_ENCRYPT		2
#define ERR_PWD_DECRYPT		2

#include "network-conn-pool.h"
#include "network-exports.h"
#include "util/parse_config_file.h"
//#include "util/save_config_file.h"

typedef struct network_nodes_t network_nodes_t;
typedef struct parse_info_t parse_info_t;

typedef enum { 
	BACKEND_STATE_UNKNOWN, 
	BACKEND_STATE_UP, 
	BACKEND_STATE_DOWN,
	BACKEND_STATE_OFFLINE
} backend_state_t;

typedef enum { 
	BACKEND_TYPE_UNKNOWN, 
	BACKEND_TYPE_RW, 
	BACKEND_TYPE_RO
} backend_type_t;

typedef struct {
	network_address *addr;
   
	backend_state_t state;   /**< UP or DOWN */
	//backend_type_t type;     /**< ReadWrite or ReadOnly */
//	GTimeVal state_since;    /**< timestamp of the last state-change */
//	network_connection_pool *pool; /**< the pool of open connections */
	GPtrArray *pools;

	guint connected_clients; /**< number of open connections to this backend for SQF */

	GString *uuid;           /**< the UUID of the backend */
	//guint weight;
    //gchar *slave_with_tag; // slave with tag, if this backend is master then slave_with_tag is null
    //gchar *group_name;
    gchar *raw_addr;  // ip:port 

    gint cite_cnt; 

    GMutex mutex; 
} network_backend_t;

typedef struct {
    network_backend_t *b; 
    gchar *group_name; 
    gchar *tag_name;    
    guint weight;
    backend_type_t type;
    guint idx; 
} network_group_backend_t;

typedef struct {
    guint max_weight;
    guint cur_weight;
    guint next_ndx;
} g_wrr_poll;


typedef struct {
    GPtrArray *backends;
    GMutex    backends_mutex;  /*remove lock*/
    g_wrr_poll *global_wrr;
    guint event_thread_count;
    gchar *default_file;
    GHashTable **ip_table;
    gint *ip_table_index;
    GPtrArray *raw_ips;
    GHashTable **pwd_table;
    gint *pwd_table_index;
    GPtrArray *raw_pwds;
} network_backends_t;

typedef struct {
    GPtrArray *arr[2];
    gint *idx;  
    GHashTable *sets; 
} swap_buffer_t;

//TODO(dengyihao):to add more paras to control router dengyiha
typedef struct {
    gboolean sql_safe_update; 
    gboolean auto_sql_filter; 
    gboolean set_router_rule;
    guint max_conn_in_pool;
    guint read_timeout;
} dynamic_router_para_t; 

struct network_nodes_t {
    GHashTable *nodes;    
    GMutex    nodes_mutex;	/*remove lock*/

    guint event_thread_count;
    gchar *default_file;
    GHashTable **ip_table;
    gint ip_table_index;
    GPtrArray *raw_ips;
    GHashTable **pwd_table;
    gint *pwd_table_index;
    GPtrArray *raw_pwds;
    gchar *default_node; // master node, only master node support slave_tag
    GHashTable *conn; 
    GPtrArray *group_backends;  // pointer to all the group_backend(backend with group id)
    dynamic_router_para_t *router_para; // pointer to all some specific router rule, it's global parameter

    swap_buffer_t *forbidden_sql;
    //sharding rules 
    GHashTable *sharding_tables; 
};

typedef struct{
    gchar *tag_name;
    GPtrArray *addrs;
    GPtrArray *index; 
} slaves_tag_t;

typedef struct{
    gchar *name;
    network_group_backend_t *master; 
    GPtrArray *slaves;
} group_info_t;

/**
 *  TODO(dengyihao):to add more sharding func
 * */
typedef enum {
    SHARDING_TYPE_UNKOWN,
    SHARDING_TYPE_RANGE,
    SHARDING_TYPE_HASH
} shard_type_t;

typedef struct {
    gint64 range_begin;
    gint64 range_end;
    gchar *group_name;
    gint64 group_index;
} group_range_map_t;

typedef struct {
    gchar *group_name;
} group_hash_map_t;

typedef struct {
    GString* table_name;
    GString* shard_key;
    GString* primary_key;
    shard_type_t shard_type;
    GArray*  shard_groups; // element is group_range_map_t, if it is SHARDING_TYPE_HASH, orderd by group_id, element is group_hash_map_t
    gboolean auto_inc;
} sharding_table_t;


void set_sharding_tables(network_nodes_t *nodes, schema_info_t *schema);

NETWORK_API network_backend_t *network_backend_new();
NETWORK_API void network_backend_free(gpointer b);


NETWORK_API network_backends_t *network_backends_new(guint event_thread_count, gchar *default_file);
NETWORK_API void network_backends_free(network_backends_t *);
NETWORK_API int network_backends_add(network_nodes_t *backends, gchar *address, backend_type_t type);
NETWORK_API int network_backends_remove(network_nodes_t *backends, guint index);
NETWORK_API int network_backends_addclient(network_nodes_t *backends, gchar *address);
NETWORK_API int network_backends_removeclient(network_nodes_t *backends, gchar *address);
NETWORK_API int network_backends_addpwd(network_nodes_t *backends, gchar *user, gchar *pwd, gboolean is_encrypt);
NETWORK_API int network_backends_removepwd(network_nodes_t *backends, gchar *address);
NETWORK_API int network_backends_check(network_backends_t *backends);
NETWORK_API network_group_backend_t* network_backends_get(network_nodes_t *backends, guint ndx);
NETWORK_API guint network_backends_count(network_nodes_t *backends);

NETWORK_API g_wrr_poll *g_wrr_poll_new();
NETWORK_API void g_wrr_poll_free(g_wrr_poll *global_wrr);
NETWORK_API network_group_backend_t *network_group_backend_new(gchar *grp_name, \
        gchar *tag_name, \
        network_backend_t *b,\
        guint weight, \
        backend_type_t type, \
        guint idx);
NETWORK_API void network_group_backend_free(network_group_backend_t *t);

NETWORK_API network_nodes_t *network_nodes_new();
NETWORK_API void network_nodes_free(network_nodes_t *t);
/*
 * slave group info
 */
NETWORK_API slaves_tag_t *slaves_tag_new(gchar *tag_name);
NETWORK_API void slaves_tag_free(slaves_tag_t *s);
NETWORK_API char *decrypt(char *in);


void set_forbidden_sql(swap_buffer_t *buffer, GPtrArray *forfidden_sql);
void set_nodes_backends(network_nodes_t *groups, GPtrArray *config);
void set_sharding_rules(GHashTable *, schema_info_t *);


// a serie of swap_buffer func
swap_buffer_t *swap_buffer_new(void (*destroy)(gpointer data));
gchar *swap_buffer_get(swap_buffer_t *t, gint index);
gboolean swap_buffer_add(swap_buffer_t *t, gchar *ele);
gboolean swap_buffer_remove(swap_buffer_t *t, gint i); 
void swap_buffer_free(swap_buffer_t *t);
GPtrArray *swap_buffer_in_use(swap_buffer_t *t);


//sharding table 
sharding_table_t *sharding_lookup_table_rule(GHashTable *sharding_tables, parse_info_t *parse_info, gchar *default_db, gchar **group_name);
#endif /* _BACKEND_H_ */
