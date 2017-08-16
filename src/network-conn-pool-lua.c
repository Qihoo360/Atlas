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
#include <stdlib.h>
#include <time.h>
#include "lua-env.h"
#include "glib-ext.h"

#include "network-mysqld.h"
#include "network-mysqld-packet.h"
#include "chassis-event-thread.h"
#include "network-mysqld-lua.h"
#include "chassis-sharding.h"
#include "network-conn-pool.h"
#include "network-conn-pool-lua.h"

/**
 * lua wrappers around the connection pool
 */

#define C(x) x, sizeof(x) - 1
#define S(x) x->str, x->len


/*
 *  * count the all connections between proxy and mysql server
 *   * */

void conn_count(gpointer key, gpointer value, gpointer user_data){
    if (!key || !value || !user_data) { return; } 
    network_backend_t *t = (network_backend_t *)value; 
    network_connection_pool *pool = NULL;
    guint j; 
    guint *cnt = (guint *)user_data;

    for (j = 0; t->pools && j < t->pools->len; ++j) {
        pool = g_ptr_array_index(t->pools, j);
        *cnt += g_queue_get_length(pool);
    }
}
int network_mysqld_conn_count(network_mysqld_con *con){
    int i, j;
    int count = 0;
    network_backend_t *t = NULL;

    g_hash_table_foreach(con->srv->nodes->conn, conn_count, &count);
    //for(i = 0; i < con->srv->nodes->backends->len; i++){
    //    t = network_backends_get(con->srv->backends, i);
    //    for(j = 0; j < t->pools->len; j++){
    //        pool = g_ptr_array_index(t->pools, j);
    //        count += g_queue_get_length(pool);
    //    }
    //}
    return count;

}
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
    network_connection_pool* pool = chassis_event_thread_pool(st->backend->b);
    pool_entry = network_connection_pool_add(pool, con->server);

    if (pool_entry) {
        event_set(&(con->server->event), con->server->fd, EV_READ, network_mysqld_con_idle_handle, pool_entry);
        chassis_event_add_local(con->srv, &(con->server->event)); /* add a event, but stay in the same thread */
    }

    //	st->backend->connected_clients--;
    st->backend = NULL;
    st->backend_ndx = -1;

    con->server = NULL;

    return 0;
}

int sharding_network_group_add_connection(network_mysqld_con *con, dbgroup_context_t *dbgroup_ctx) {
    network_connection_pool_entry *pool_entry = NULL;
    network_mysqld_con_lua_t *st = dbgroup_ctx->st;

    /* con-server is already disconnected, got out */
    network_socket *server = dbgroup_ctx->server;
    if (!server) return 0;

    /* TODO bug fix */
    /* when mysql return unkonw packet, response is null, insert the socket into pool cause segment fault. */
    /* ? should init socket->challenge  ? */
    /* if response is null, conn has not been authed, use an invalid username. */
    if (!server->response) {
        g_warning("%s: (remove) remove socket from pool, response is NULL, src is %s, dst is %s",
                G_STRLOC, server->src->name->str, server->dst->name->str);

        server->response = network_mysqld_auth_response_new();
        g_string_assign_len(server->response->username, C("mysql_proxy_invalid_user"));
    }

    /* the server connection is still authed */
    server->is_authed = 1;

    /* insert the server socket into the connection pool */
    network_connection_pool* pool = chassis_event_thread_pool(st->backend->b);
    pool_entry = network_connection_pool_add(pool, server);

    if (pool_entry) {
        event_set(&(server->event), server->fd, EV_READ, network_mysqld_con_idle_handle, pool_entry);
        chassis_event_add_local(con->srv, &(server->event)); /* add a event, but stay in the same thread */
    }

    //	st->backend->connected_clients--;
    st->backend = NULL;
    st->backend_ndx = -1;

    dbgroup_ctx->server = NULL;

    return 0;
}
network_socket *self_connect(network_mysqld_con *con, network_backend_t *backend, GHashTable *pwd_table) {
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
        g_mutex_lock(&backend->mutex);
        if (backend->state != BACKEND_STATE_OFFLINE) {
            backend->state = BACKEND_STATE_DOWN;
            g_mutex_unlock(&backend->mutex);
            return NULL;
        }
        g_mutex_unlock(&backend->mutex);
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

    return sock;
}


/*
 *
 *  get server connection from the connection pool
 * */


/**
 * swap the server connection with a connection from
 * the connection pool
 *
 * we can only switch backends if we have a authed connection in the pool.
 *
 * @return NULL if swapping failed
 *         the new backend on success
 */
network_socket *network_connection_pool_lua_swap(network_mysqld_con *con, int backend_ndx, GHashTable *pwd_table, gchar *node_name) {
    network_group_backend_t *backend = NULL;
    network_socket *send_sock;
    network_mysqld_con_lua_t *st = con->plugin_con_state;
    //	GString empty_username = { "", 0, 0 };

    /*
     * we can only change to another backend if the backend is already
     * in the connection pool and connected
     */

    backend = network_backends_get(con->srv->nodes, backend_ndx);
    if (!backend) return NULL;


    /**
     * get a connection from the pool which matches our basic requirements
     * - username has to match
     * - default_db should match
     */

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
            return NULL;

        } 
        /**
         * no connections in the pool
         */
        if (NULL == (send_sock = self_connect(con, backend->b, pwd_table))) {
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

    return send_sock;
}
gboolean is_force_router_to_master(gchar *note, gchar *name){
    if (note && 0 == strcasecmp(note, name)) { 
        return TRUE;
    }
    return FALSE;
}

network_group_backend_t *slave_backend_choose(slaves_tag_t *tag) {
    network_group_backend_t *gp_backend = NULL;
    if (NULL == tag || NULL == tag->index || NULL == tag->addrs) { 
        return gp_backend; 
    }

    srand(time(NULL));
    guint t = rand()%(tag->index->len);                 
    guint *idx = g_ptr_array_index(tag->index, t); 
    if (idx && *idx < tag->index->len) {
        gp_backend = g_ptr_array_index(tag->addrs, *idx);  
        g_mutex_lock(&gp_backend->b->mutex);
        if (BACKEND_STATE_UP == gp_backend->b->state) {
            g_mutex_unlock(&gp_backend->b->mutex);
            return gp_backend;  
        } /*check backend state*/
        g_mutex_unlock(&gp_backend->b->mutex);
    }

    guint i; 
    for (i = 0; i < tag->addrs->len; i++) {
        gp_backend = g_ptr_array_index(tag->addrs, i);  
        g_mutex_lock(&gp_backend->b->mutex);
        if (BACKEND_STATE_UP == gp_backend->b->state) {
            g_mutex_unlock(&gp_backend->b->mutex);
            return gp_backend;  
        } /*check backend state*/
        g_mutex_unlock(&gp_backend->b->mutex);
    }
    return NULL;
}

network_group_backend_t* master_backend_get(group_info_t *group) {
    g_mutex_lock(&group->master->b->mutex);
    if (group && group->master && group->master->type == BACKEND_STATE_UP) {
        g_mutex_unlock(&group->master->b->mutex);
        return group->master;
    }
    g_mutex_unlock(&group->master->b->mutex);
    return NULL;
}

network_group_backend_t* slave_backend_get(group_info_t *t, gchar *annotate) {
    if (NULL == t || NULL == t->slaves) { 
        return NULL; 
    }
    slaves_tag_t *tag = NULL;
    network_group_backend_t *backend = NULL;

    if (annotate) {
        guint i;
        for (i = 0; i < t->slaves->len; ++i) {
            tag = g_ptr_array_index(t->slaves, i);
            if (0 == strcasecmp(annotate, tag->tag_name)) {
                backend = slave_backend_choose(tag);
                if (backend) { 
                    return backend;
                }
            }
        }
    }
    //TODO(dengyihao): default to tag0, go to add tag support 
    tag = g_ptr_array_index(t->slaves, 0);
    backend = slave_backend_choose(tag);

    return backend;
}

void network_conn_get(network_mysqld_con* con, gchar* annotate, gboolean is_write, GHashTable *pwd_table, gchar *group_name){
    if (!con || !con->srv || !con->srv->nodes || !pwd_table) { 
        return; 
    }

    network_nodes_t *nodes = con->srv->nodes;
    group_info_t *group = NULL; 
    if (NULL == group_name) {
        group = g_hash_table_lookup(nodes->nodes, nodes->default_node);
    } else {
        group = g_hash_table_lookup(nodes->nodes, group_name);
    } 

    network_socket *send_sock = NULL;
    network_group_backend_t *backend = NULL; 
    network_mysqld_con_lua_t *st = con->plugin_con_state;

    if (is_write || is_force_router_to_master(annotate, "MASTER")) {
        backend = master_backend_get(group); 
    } else {
        backend = slave_backend_get(group, annotate);
    }    

    if (!backend) { return; }
    /**
     * get a connection from the pool which matches our basic requirements
     * - username has to match
     * - default_db should match
     */

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

    con->server = send_sock;
}
