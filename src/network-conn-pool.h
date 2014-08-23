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
 

#ifndef _NETWORK_CONN_POOL_H_
#define _NETWORK_CONN_POOL_H_

#include <glib.h>

#include "lua-scope.h"

#include "network-socket.h"
#include "network-exports.h"

typedef GQueue network_connection_pool;

typedef struct {
	network_socket *sock;          /** the idling socket */
	
	network_connection_pool *pool; /** a pointer back to the pool */
} network_connection_pool_entry;

NETWORK_API network_socket *network_connection_pool_get(network_connection_pool *pool);
NETWORK_API network_connection_pool_entry *network_connection_pool_add(network_connection_pool *pool, network_socket *sock);
NETWORK_API void network_connection_pool_remove(network_connection_pool *pool, network_connection_pool_entry *entry);

NETWORK_API network_connection_pool *network_connection_pool_new(void);
NETWORK_API void network_connection_pool_free(network_connection_pool *pool);

#endif
