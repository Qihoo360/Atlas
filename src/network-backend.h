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
	backend_type_t type;     /**< ReadWrite or ReadOnly */

//	GTimeVal state_since;    /**< timestamp of the last state-change */

//	network_connection_pool *pool; /**< the pool of open connections */
	GPtrArray *pools;

	gint connected_clients; /**< number of open connections to this backend for SQF */

	GString *uuid;           /**< the UUID of the backend */

	guint weight;
} network_backend_t;

NETWORK_API network_backend_t *network_backend_new();
NETWORK_API void network_backend_free(network_backend_t *b);

typedef struct {
    guint max_weight;
    guint cur_weight;
    guint next_ndx;
} g_wrr_poll;

typedef struct {
	GPtrArray *backends;
	GMutex    *backends_mutex;	/*remove lock*/
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

NETWORK_API network_backends_t *network_backends_new(guint event_thread_count, gchar *default_file);
NETWORK_API void network_backends_free(network_backends_t *);
NETWORK_API int network_backends_add(network_backends_t *backends, gchar *address, backend_type_t type);
NETWORK_API int network_backends_remove(network_backends_t *backends, guint index);
NETWORK_API int network_backends_addclient(network_backends_t *backends, gchar *address);
NETWORK_API int network_backends_removeclient(network_backends_t *backends, gchar *address);
NETWORK_API int network_backends_addpwd(network_backends_t *backends, gchar *user, gchar *pwd, gboolean is_encrypt);
NETWORK_API int network_backends_removepwd(network_backends_t *backends, gchar *address);
NETWORK_API int network_backends_check(network_backends_t *backends);
NETWORK_API network_backend_t * network_backends_get(network_backends_t *backends, guint ndx);
NETWORK_API guint network_backends_count(network_backends_t *backends);

NETWORK_API g_wrr_poll *g_wrr_poll_new();
NETWORK_API void g_wrr_poll_free(g_wrr_poll *global_wrr);

NETWORK_API char *decrypt(char *in);

#endif /* _BACKEND_H_ */
