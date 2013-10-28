/* $%BEGINLICENSE%$
 Copyright (c) 2007, 2009, Oracle and/or its affiliates. All rights reserved.

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
 

#include <glib.h>

#include "network-conn-pool.h"
#include "network-mysqld-packet.h"
#include "glib-ext.h"
#include "sys-pedantic.h"

/** @file
 * connection pools
 *
 * in the pool we manage idle connections
 * - keep them up as long as possible
 * - make sure we don't run out of seconds
 * - if the client is authed, we have to pick connection with the same user
 * - ...  
 */

/**
 * create a empty connection pool entry
 *
 * @return a connection pool entry
 */
network_connection_pool_entry *network_connection_pool_entry_new(void) {
	network_connection_pool_entry *e;

	e = g_new0(network_connection_pool_entry, 1);

	return e;
}

/**
 * free a conn pool entry
 *
 * @param e the pool entry to free
 * @param free_sock if true, the attached server-socket will be freed too
 */
void network_connection_pool_entry_free(network_connection_pool_entry *e, gboolean free_sock) {
	if (!e) return;

	if (e->sock && free_sock) {
		network_socket *sock = e->sock;
			
		event_del(&(sock->event));
		network_socket_free(sock);
	}

	g_free(e);
}

/**
 * init a connection pool
 */
network_connection_pool *network_connection_pool_new(void) {
	network_connection_pool *pool = g_queue_new();
	return pool;
}

/**
 * free all entries of the pool
 *
 */
void network_connection_pool_free(network_connection_pool *pool) {
	if (pool) {
		network_connection_pool_entry *entry = NULL;
		while ((entry = g_queue_pop_head(pool))) network_connection_pool_entry_free(entry, TRUE);
		g_queue_free(pool);
	}
}

/**
 * get a connection from the pool
 *
 * make sure we have at lease <min-conns> for each user
 * if we have more, reuse a connect to reauth it to another user
 *
 * @param pool connection pool to get the connection from
 * @param username (optional) name of the auth connection
 * @param default_db (unused) unused name of the default-db
 */
network_socket *network_connection_pool_get(network_connection_pool *pool) {
	network_connection_pool_entry *entry = NULL;

	if (pool->length > 0) {
	//	entry = g_queue_pop_head(pool);
		entry = g_queue_pop_tail(pool);
	}

	/**
	 * if we know this use, return a authed connection 
	 */

	if (!entry) return NULL;

	network_socket *sock = entry->sock;

	network_connection_pool_entry_free(entry, FALSE);

	/* remove the idle handler from the socket */	
	event_del(&(sock->event));
		
	return sock;
}

/**
 * add a connection to the connection pool
 *
 */
network_connection_pool_entry *network_connection_pool_add(network_connection_pool *pool, network_socket *sock) {
	if (pool) {
		network_connection_pool_entry *entry = network_connection_pool_entry_new();
		if (entry) {
			entry->sock = sock;
			entry->pool = pool;
			g_queue_push_tail(pool, entry);

			return entry;
		}
	}

	network_socket_free(sock);
	return NULL;
}

/**
 * remove the connection referenced by entry from the pool 
 */
void network_connection_pool_remove(network_connection_pool *pool, network_connection_pool_entry *entry) {
	network_socket *sock = entry->sock;

	if (sock->response == NULL) {
		g_critical("%s: (remove) remove socket from pool, response is NULL, src is %s, dst is %s", G_STRLOC, sock->src->name->str, sock->dst->name->str);
	}

	network_connection_pool_entry_free(entry, TRUE);

	g_queue_remove(pool, entry);
}
