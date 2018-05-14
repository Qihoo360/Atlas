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
#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#ifdef HAVE_SYS_FILIO_H
/**
 * required for FIONREAD on solaris
 */
#include <sys/filio.h>
#endif

#ifndef _WIN32
#include <sys/ioctl.h>

#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#define ioctlsocket ioctl
#endif

#include <errno.h>
#include <lua.h>

#include "lua-env.h"
#include "glib-ext.h"

#include "network-mysqld.h"
#include "network-mysqld-packet.h"
#include "chassis-event-thread.h"
#include "network-mysqld-lua.h"

#include "network-conn-pool.h"
#include "network-conn-pool-lua.h"

/**
 * lua wrappers around the connection pool
 */

#define C(x) x, sizeof(x) - 1
#define S(x) x->str, x->len

/**
 * handle the events of a idling server connection in the pool 
 *
 * make sure we know about connection close from the server side
 * - wait_timeout
 */
static void network_mysqld_con_idle_handle(int event_fd, short events, void *user_data) {
	network_connection_pool_entry *pool_entry = user_data;
	network_connection_pool *pool             = pool_entry->pool;

	if (events == EV_READ) {
		int b = -1;

		/**
		 * @todo we have to handle the case that the server really sent us something
		 *        up to now we just ignore it
		 */
		if (ioctlsocket(event_fd, FIONREAD, &b)) {
			g_critical("ioctl(%d, FIONREAD, ...) failed: %s", event_fd, g_strerror(errno));
		} else if (b != 0) {
			g_critical("ioctl(%d, FIONREAD, ...) said there is something to read, oops: %d", event_fd, b);
		} else {
			/* the server decided to close the connection (wait_timeout, crash, ... )
			 *
			 * remove us from the connection pool and close the connection */
		

			network_connection_pool_remove(pool, pool_entry); // not in lua, so lock like lua_lock
		}
	}
}

/**
 * move the con->server into connection pool and disconnect the 
 * proxy from its backend 
 */
int network_connection_pool_lua_add_connection(network_mysqld_con *con) {
	network_connection_pool_entry *pool_entry = NULL;
	network_mysqld_con_lua_t *st = con->plugin_con_state;

	/* con-server is already disconnected, got out */
	if (!con->server) return 0;

    /* TODO bug fix */
    /* when mysql return unkonw packet, response is null, insert the socket into pool cause segment fault. */
    /* ? should init socket->challenge  ? */
    /* if response is null, conn has not been authed, use an invalid username. */
    if(!con->server->response)
    {
        g_warning("%s: (remove) remove socket from pool, response is NULL, src is %s, dst is %s",
            G_STRLOC, con->server->src->name->str, con->server->dst->name->str);

        con->server->response = network_mysqld_auth_response_new();
        g_string_assign_len(con->server->response->username, C("mysql_proxy_invalid_user"));
    }

	/* the server connection is still authed */
	con->server->is_authed = 1;

	/* insert the server socket into the connection pool */
	network_connection_pool* pool = chassis_event_thread_pool(st->backend);
	pool_entry = network_connection_pool_add(pool, con->server);

	if (pool_entry) {
		event_set(&(con->server->event), con->server->fd, EV_READ, network_mysqld_con_idle_handle, pool_entry);
		chassis_event_add_local(con->srv, &(con->server->event)); /* add a event, but stay in the same thread */
	}

    if (!g_atomic_int_compare_and_exchange(&st->backend->connected_clients, 0, 0)) {
        g_atomic_int_dec_and_test(&st->backend->connected_clients);    
        //g_critical("add_connection: %08x's connected_clients is %d\n", backend,  backend->connected_clients);
    }

//	st->backend->connected_clients--;
	st->backend = NULL;
	st->backend_ndx = -1;
	
	con->server = NULL;

	return 0;
}

network_socket *self_connect(network_mysqld_con *con, network_backend_t *backend, GHashTable *pwd_table) {

    /*make sure that the max conn for the backend is no more than the config number
     *when max_conn_for_a_backend is no more than 0, there is no limitation for max connection for a backend;
     * */
    if (con->srv->max_conn_for_a_backend > 0 && backend->connected_clients >= con->srv->max_conn_for_a_backend) {
        g_critical("%s.%d: self_connect:%08x's connected_clients is %d, which are too many!",__FILE__, __LINE__, backend,  backend->connected_clients);
        return NULL;
    }
    
    //1. connect DB
	network_socket *sock = network_socket_new();
	network_address_copy(sock->dst, backend->addr);
	if (-1 == (sock->fd = socket(sock->dst->addr.common.sa_family, sock->socket_type, 0))) {
		g_critical("%s.%d: socket(%s) failed: %s (%d)", __FILE__, __LINE__, sock->dst->name->str, g_strerror(errno), errno);
		network_socket_free(sock);
		return NULL;
	}
	if (-1 == (connect(sock->fd, &sock->dst->addr.common, sock->dst->len))) {
		g_message("%s.%d: connecting to backend (%s) failed, marking it as down for ...", __FILE__, __LINE__, sock->dst->name->str);
		network_socket_free(sock);
		if (backend->state != BACKEND_STATE_OFFLINE) backend->state = BACKEND_STATE_DOWN;
		return NULL;
	}

	//2. read handshake，重点是获取20个字节的随机串
	off_t to_read = NET_HEADER_SIZE;
	guint offset = 0;
	guchar header[NET_HEADER_SIZE];
	while (to_read > 0) {
		gssize len = recv(sock->fd, header + offset, to_read, 0);
		if (len == -1 || len == 0) {
			network_socket_free(sock);
			return NULL;
		}
		offset += len;
		to_read -= len;
	}

	to_read = header[0] + (header[1] << 8) + (header[2] << 16);
	offset = 0;
	GString *data = g_string_sized_new(to_read);
	while (to_read > 0) {
		gssize len = recv(sock->fd, data->str + offset, to_read, 0);
		if (len == -1 || len == 0) {
			network_socket_free(sock);
			g_string_free(data, TRUE);
			return NULL;
		}
		offset += len;
		to_read -= len;
	}
	data->len = offset;

	network_packet packet;
	packet.data = data;
	packet.offset = 0;
	network_mysqld_auth_challenge *challenge = network_mysqld_auth_challenge_new();
	network_mysqld_proto_get_auth_challenge(&packet, challenge);

	//3. 生成response
	GString *response = g_string_sized_new(20);
	GString *hashed_password = g_hash_table_lookup(pwd_table, con->client->response->username->str);
	if (hashed_password) {
		network_mysqld_proto_password_scramble(response, S(challenge->challenge), S(hashed_password));
	} else {
		network_socket_free(sock);
		g_string_free(data, TRUE);
		network_mysqld_auth_challenge_free(challenge);
		g_string_free(response, TRUE);
		return NULL;
	}

	//4. send auth
	off_t to_write = 58 + con->client->response->username->len;
	offset = 0;
	g_string_truncate(data, 0);
	char tmp[] = {to_write - 4, 0, 0, 1, 0x85, 0xa6, 3, 0, 0, 0, 0, 1, 8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
	g_string_append_len(data, tmp, 36);
	g_string_append_len(data, con->client->response->username->str, con->client->response->username->len);
	g_string_append_len(data, "\0\x14", 2);
	g_string_append_len(data, response->str, 20);
	g_string_free(response, TRUE);
	while (to_write > 0) {
		gssize len = send(sock->fd, data->str + offset, to_write, 0);
		if (len == -1) {
			network_socket_free(sock);
			g_string_free(data, TRUE);
			network_mysqld_auth_challenge_free(challenge);
			return NULL;
		}
		offset += len;
		to_write -= len;
	}

	//5. read auth result
	to_read = NET_HEADER_SIZE;
	offset = 0;
	while (to_read > 0) {
		gssize len = recv(sock->fd, header + offset, to_read, 0);
		if (len == -1 || len == 0) {
			network_socket_free(sock);
			g_string_free(data, TRUE);
			network_mysqld_auth_challenge_free(challenge);
			return NULL;
		}
		offset += len;
		to_read -= len;
	}

	to_read = header[0] + (header[1] << 8) + (header[2] << 16);
	offset = 0;
	g_string_truncate(data, 0);
	g_string_set_size(data, to_read);
	while (to_read > 0) {
		gssize len = recv(sock->fd, data->str + offset, to_read, 0);
		if (len == -1 || len == 0) {
			network_socket_free(sock);
			g_string_free(data, TRUE);
			network_mysqld_auth_challenge_free(challenge);
			return NULL;
		}
		offset += len;
		to_read -= len;
	}
	data->len = offset;

	if (data->str[0] != MYSQLD_PACKET_OK) {
		network_socket_free(sock);
		g_string_free(data, TRUE);
		network_mysqld_auth_challenge_free(challenge);
		return NULL;
	}
	g_string_free(data, TRUE);

	//6. set non-block
	network_socket_set_non_blocking(sock);
	network_socket_connect_setopts(sock);	//此句是否需要？是否应该放在第1步末尾？

	sock->challenge = challenge;
	sock->response = network_mysqld_auth_response_copy(con->client->response);
    g_atomic_int_inc(&backend->connected_clients);
	return sock;
}

/**
 * swap the server connection with a connection from
 * the connection pool
 *
 * we can only switch backends if we have a authed connection in the pool.
 *
 * @return NULL if swapping failed
 *         the new backend on success
 */
network_socket *network_connection_pool_lua_swap(network_mysqld_con *con, int backend_ndx, GHashTable *pwd_table) {
	network_backend_t *backend = NULL;
	network_socket *send_sock;
	network_mysqld_con_lua_t *st = con->plugin_con_state;
//	GString empty_username = { "", 0, 0 };

	/*
	 * we can only change to another backend if the backend is already
	 * in the connection pool and connected
	 */

	backend = network_backends_get(con->srv->backends, backend_ndx);
	if (!backend) return NULL;


	/**
	 * get a connection from the pool which matches our basic requirements
	 * - username has to match
	 * - default_db should match
	 */
		
#ifdef DEBUG_CONN_POOL
	g_debug("%s: (swap) check if we have a connection for this user in the pool '%s'", G_STRLOC, con->client->response ? con->client->response->username->str: "empty_user");
#endif
       int flag = 0;
	network_connection_pool* pool = chassis_event_thread_pool(backend);
	if (NULL == (send_sock = network_connection_pool_get(pool))) {
		/**
		 * no connections in the pool
		 */
        flag = 1;
		if (NULL == (send_sock = self_connect(con, backend, pwd_table))) {
			st->backend_ndx = -1;
			return NULL;
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
	st->backend_ndx = backend_ndx;
    
        if (flag == 0 && !g_atomic_int_compare_and_exchange(&st->backend->connected_clients, 0, 0)) {
            g_atomic_int_dec_and_test(&st->backend->connected_clients);
            //g_critical("pool_lua_swap:%08x's connected_clients is %d\n", backend,  backend->connected_clients);
        }

	return send_sock;
}
