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
 

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include <sys/types.h>

#ifdef HAVE_SYS_FILIO_H
#include <sys/filio.h> /* required for FIONREAD on solaris */
#endif

#ifndef _WIN32
#include <sys/ioctl.h>
#include <sys/socket.h>

#include <arpa/inet.h> /** inet_ntoa */
#include <netinet/in.h>
#include <netinet/tcp.h>

#include <netdb.h>
#include <unistd.h>
#else
#include <winsock2.h>
#include <io.h>
#define ioctl ioctlsocket
#endif

#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <fcntl.h>
#include <errno.h>
#ifdef HAVE_SIGNAL_H
#include <signal.h>
#endif

#ifdef HAVE_SYS_UIO_H
#include <sys/uio.h>
#endif

#include <glib.h>

#include <mysql.h>
#include <mysqld_error.h>

#include "network-debug.h"
#include "network-mysqld.h"
#include "network-mysqld-proto.h"
#include "network-mysqld-packet.h"
#include "network-conn-pool.h"
#include "chassis-mainloop.h"
#include "chassis-event-thread.h"
#include "lua-scope.h"
#include "glib-ext.h"
#include "network-mysqld-lua.h"
#include "chassis-sharding.h"
//#include "network-backend-new.h"

#if defined(HAVE_SYS_SDT_H) && defined(ENABLE_DTRACE)
#include <sys/sdt.h>
#include "proxy-dtrace-provider.h"
#else
#include "disable-dtrace.h"
#endif

#ifdef HAVE_WRITEV
#define USE_BUFFERED_NETIO 
#else
#undef USE_BUFFERED_NETIO 
#endif

#ifdef _WIN32
#define E_NET_CONNRESET WSAECONNRESET
#define E_NET_CONNABORTED WSAECONNABORTED
#define E_NET_WOULDBLOCK WSAEWOULDBLOCK
#define E_NET_INPROGRESS WSAEINPROGRESS
#else
#define E_NET_CONNRESET ECONNRESET
#define E_NET_CONNABORTED ECONNABORTED
#define E_NET_INPROGRESS EINPROGRESS
#if EWOULDBLOCK == EAGAIN
/**
 * some system make EAGAIN == EWOULDBLOCK which would lead to a 
 * error in the case handling
 *
 * set it to -1 as this error should never happen
 */
#define E_NET_WOULDBLOCK -1
#else
#define E_NET_WOULDBLOCK EWOULDBLOCK
#endif
#endif

/**
 * a handy marco for constant strings 
 */
#define C(x) x, sizeof(x) - 1
#define S(x) x->str, x->len

//static GMutex con_mutex;

/**
 * call the cleanup callback for the current connection
 *
 * @param srv    global context
 * @param con    connection context
 *
 * @return       NETWORK_SOCKET_SUCCESS on success
 */
network_socket_retval_t plugin_call(chassis *srv, network_mysqld_con *con, int state);

network_socket_retval_t plugin_call_cleanup(chassis *srv, network_mysqld_con *con) {
	NETWORK_MYSQLD_PLUGIN_FUNC(func) = NULL;
	network_socket_retval_t retval = NETWORK_SOCKET_SUCCESS;

	func = con->plugins.con_cleanup;
	
	if (!func) return retval;

//	LOCK_LUA(srv->priv->sc);	/*remove lock*/
	retval = (*func)(srv, con);
//	UNLOCK_LUA(srv->priv->sc);	/*remove lock*/

	return retval;
}


int network_mysqld_init(chassis *srv, gchar *default_file) {
	/* store the pointer to the chassis in the Lua registry */
	srv->sc = lua_scope_new();
	lua_State *L = srv->sc->L;
	lua_pushlightuserdata(L, (void*)srv);
	lua_setfield(L, LUA_REGISTRYINDEX, CHASSIS_LUA_REGISTRY_KEY);

    srv->nodes = network_nodes_new(srv->event_thread_count, default_file); 
    //srv->forbidden_sql = swap_buffer_new(g_free); 
    srv->router_para = g_new0(dynamic_router_para_t, 1);
	return 0;
}
/*
 * free stmt
 */
/**
 * create a connection 
 *
 * @return       a stmt context
 */
stmt_paras_t *stmt_paras_new(){
    stmt_paras_t *paras;
    paras = g_new(stmt_paras_t, 1);
    paras->state = 0;
    paras->stmt_prepare_ok_packet = NULL;
    paras->stmt_execute_packet = NULL;
    paras->prepare_Query = NULL;
    return paras;
}
void stmt_hash_value_free(gpointer value){
     if(!value) return;   
     stmt_paras_t *t = (stmt_paras_t *)value;

     if(t->prepare_Query) g_string_free(t->prepare_Query, TRUE);
     if(t->stmt_execute_packet) network_mysqld_stmt_execute_packet_free(t->stmt_execute_packet);         
     if(t->stmt_prepare_ok_packet) network_mysqld_stmt_prepare_ok_packet_free(t->stmt_prepare_ok_packet);   

     g_free(t);
}
// stmt ctx operation new/free
stmt_context_t *stmt_context_new(void) {
    stmt_context_t *ctx = g_slice_new0(stmt_context_t);
    ctx->stmt = g_hash_table_new_full(g_int_hash, g_int_equal, g_free, stmt_hash_value_free);    
    return ctx;
}
void stmt_context_free(stmt_context_t *ctx) {
    if (NULL == ctx) { return; } 
    if (ctx->stmt) {
        g_hash_table_remove_all(ctx->stmt);
        g_hash_table_destroy(ctx->stmt);
        ctx->stmt = NULL;
    } 
    g_slice_free(stmt_context_t, ctx);
}

network_mysqld_con *network_mysqld_con_new() {
	network_mysqld_con *con;

	con = g_slice_new0(network_mysqld_con);
	con->parse.command = -1;

	con->is_in_transaction = con->is_in_select_calc_found_rows = con->is_not_autocommit = FALSE;

	con->charset_client     = g_string_new(NULL);
	con->charset_results    = g_string_new(NULL);
	con->charset_connection = g_string_new(NULL);

	con->locks = g_hash_table_new_full(g_str_hash, g_str_equal, g_free, NULL);

	con->merge_res = g_new(merge_res_t, 1);
	con->merge_res->rows = g_ptr_array_new();

	con->challenge = g_string_sized_new(20);

    //con->stmt = g_hash_table_new_full(g_int_hash, g_int_equal, g_free, stmt_hash_value_free);    
    //con->current_con_stmt_state = g_slice_new0(stmt_state_t);

    con->stmt_context = stmt_context_new(); 
    
    con->trans_context = g_slice_new0(trans_context_t);

    con->sharding_context = sharding_context_new();
	return con;
}

void network_mysqld_add_connection(chassis *srv, network_mysqld_con *con) {
	con->srv = srv;
/*
	g_mutex_lock(&con_mutex);
	g_ptr_array_add(srv->priv->cons, con);
	g_mutex_unlock(&con_mutex);
*/
}

G_INLINE_FUNC void wait_for_event(network_socket* socket, gint event_type, gint timeout, network_mysqld_con* con) {
    event_set(&(socket->event), socket->fd, event_type, network_mysqld_con_handle, con);
    //g_message("%s:%d event register", __FILE__, __LINE__);
    chassis_event_add_self(con->srv, &(socket->event), timeout); 
}

G_INLINE_FUNC gboolean sharding_send_query_start(int evend_fd, short event, network_mysqld_con *con) {
    chassis *chas = con->srv; 
    network_mysqld_con_state_t ostate = con->state;
    sharding_context_t *sharding_context = con->sharding_context; 
    //g_message("%s:%d, state:SNED_QUERY_START", __FILE__, __LINE__);

    gboolean is_waiting_write = FALSE;  
    GHashTableIter iter; 
    gpointer key, value;
    g_hash_table_iter_init(&iter, sharding_context->sql_groups);
    while (g_hash_table_iter_next(&iter, &key, &value)) {
        dbgroup_context_t *gp_context = value;  
        network_socket *sock = gp_context->server;
        if (0 == sock->send_queue->offset) {
            /**
             * TODO(dengyihao):to parse this query, muse parse to get resultset status 
             * **/ 
            network_packet packet;
            packet.data = g_queue_peek_head(sock->send_queue->chunks);
            packet.offset = 0;

            if (sharding_parse_command_states_init(gp_context, &packet) != 0) {
                //g_message("%s: tracking mysql protocol states failed", G_STRLOC);
                con->state = CON_STATE_ERROR;
                return TRUE;
            }
        }
       network_socket_retval_t ret = network_mysqld_write(chas, sock);    
       switch (ret) {
           case NETWORK_SOCKET_SUCCESS:
               sharding_context->merge_result.sql_sent_num += 1;
               //g_message("%s:%d send query(%d)", __FILE__, __LINE__, sharding_context->merge_result.sql_sent_num);
               break;
           case NETWORK_SOCKET_WAIT_FOR_EVENT:     
               is_waiting_write = TRUE;
               wait_for_event(sock, EV_WRITE, 0, con);
               break;
           case NETWORK_SOCKET_ERROR:
           case NETWORK_SOCKET_ERROR_RETRY:
               g_debug("%s.%d: network_mysqld_write(CON_STATE_SHARDING_SEND_QUERY) returned an error", __FILE__, __LINE__);
               con->state = CON_STATE_ERROR; 
       }
       if (con->state != ostate) {
           return TRUE;
           //TODO(dengyihao)
       }
    }  
    if(is_waiting_write) {
        return FALSE; 
    } 
    con->state = CON_STATE_SHARDING_READ_QUERY_RESULT; 
    sharding_context->is_from_send_query = TRUE;
    return TRUE;
}

G_INLINE_FUNC gboolean sharding_send_query_continue(int event_fd, short event, network_mysqld_con *con) {
    chassis *chas = con->srv; 
    network_mysqld_con_state_t ostate = con->state;

    //g_message("%s:%d, state:SNED_QUERY_CONTINUE", __FILE__, __LINE__);
    sharding_context_t *sharding_context = con->sharding_context;  
    dbgroup_context_t *gp_context = g_hash_table_lookup(sharding_context->sql_groups, &event_fd);  
    if (gp_context == NULL) {
        return FALSE;
    }
    network_socket *sock = gp_context->server; 
    if (0 == sock->send_queue->offset) {
        network_packet packet; 
        packet.data = g_queue_peek_head(sock->send_queue->chunks);
        packet.offset = 0;
        //TODO(dengyihao)
        /**parse query, eg. prepare、com_quit、com_init**/  
    }  

    gboolean is_waiting_write = FALSE;
    network_socket_retval_t ret = network_mysqld_write(chas, sock);    
    switch (ret) {
        case NETWORK_SOCKET_SUCCESS:
            sharding_context->merge_result.sql_sent_num += 1;
            break;
        case NETWORK_SOCKET_WAIT_FOR_EVENT:     
            is_waiting_write = TRUE;
            wait_for_event(sock, EV_WRITE, 0, con);
            break;
        case NETWORK_SOCKET_ERROR:
        case NETWORK_SOCKET_ERROR_RETRY:
            g_debug("%s.%d: network_mysqld_write(CON_STATE_SHARDING_SEND_QUERY) returned an error", __FILE__, __LINE__);
            con->state = CON_STATE_ERROR; 
    }
    if (con->state != ostate) {
        return TRUE;
        //TODO(dengyihao)
    }

    if (is_waiting_write || sharding_context->merge_result.sql_sent_num < sharding_context->merge_result.shard_num) {
        return FALSE; 
    } 
    con->state = CON_STATE_SHARDING_READ_QUERY_RESULT; 
    sharding_context->is_from_send_query = TRUE;
    return TRUE;
}
G_INLINE_FUNC gboolean sharding_send_query(int event_fd, short event, network_mysqld_con *con) {
    if (event_fd == con->client->fd) {
        return sharding_send_query_start(event_fd, event, con);
    } else {
        return sharding_send_query_continue(event_fd, event, con);
    }
}
G_INLINE_FUNC network_socket_retval_t  dbgroup_read_query_result(dbgroup_context_t *dbgroup_context, network_mysqld_con *con) {
    network_mysqld_con_state_t ostate = con->state;  
    network_socket_retval_t ret = NETWORK_SOCKET_SUCCESS;
    network_socket *server = dbgroup_context->server;         
    do {
        ret = network_mysqld_read(con->srv, server);  
        switch (ret) {
            case NETWORK_SOCKET_SUCCESS:
                break;
            case NETWORK_SOCKET_WAIT_FOR_EVENT:
                wait_for_event(server, EV_READ, con->srv->router_para->read_timeout, con);
                //g_message("%s:%d,read for event", __FILE__, __LINE__);
                NETWORK_MYSQLD_CON_TRACK_TIME(con, "wait_for_event::read_query_result");
                return ret;
            case NETWORK_SOCKET_ERROR_RETRY:
            case NETWORK_SOCKET_ERROR:
                g_critical("%s.%d: network_mysqld_read(CON_STATE_READ_QUERY_RESULT) returned an error", __FILE__, __LINE__);
                con->state = CON_STATE_ERROR;
                return ret; 
        }
       //if (con->state != ostate) break; 
        ret = plugin_call(con->srv, con, con->state); 
        //g_message("%s:%d,read stage 4(plugin call ret: %d)", __FILE__, __LINE__, ret);
        switch (ret) {
            case NETWORK_SOCKET_WAIT_MORE_PACKET:
                continue;
            case NETWORK_SOCKET_SUCCESS:
            case NETWORK_SOCKET_SHARDING_SEND_NEXT_INJECTION: 
                return ret; 
            case NETWORK_SOCKET_ERROR:
                /* something nasty happend, let's close the connection */
                con->state = CON_STATE_ERROR;
                return ret; 
            default:
                g_critical("%s.%d: ...", __FILE__, __LINE__);
                con->state = CON_STATE_ERROR;
                return ret; 
        }

    } while(con->state == CON_STATE_SHARDING_READ_QUERY_RESULT);
    return ret;
}
G_INLINE_FUNC  network_socket_retval_t dbgroup_send_next_injection(dbgroup_context_t *dbgroup_context, network_mysqld_con *con) {
    network_mysqld_con_state_t ostate = con->state;
    sharding_context_t* sharding_context = con->sharding_context;

    network_socket* send_sock = dbgroup_context->server;
    if (send_sock->send_queue->offset == 0) {
        // -- TODO --
        // ?? bug ?? if the packet len is larger than max_packet, and one packet is divided to multi chunks, when sending the second chunks,
        // and the send_queue->offset may also be 0 while wait for write event, so in this case it parse twice for one packet?

        /* only parse the packets once */
        network_packet packet;

        packet.data = g_queue_peek_head(send_sock->send_queue->chunks);
        packet.offset = 0;

        if (sharding_parse_command_states_init(dbgroup_context, &packet) != 0) {
            //g_message("%s: tracking mysql protocol states failed", G_STRLOC);
            con->state = CON_STATE_ERROR;
            return TRUE;
        }
    }
    network_socket_retval_t ret = network_mysqld_write(con->srv, send_sock);
    switch(ret) {
        case NETWORK_SOCKET_SUCCESS:
            dbgroup_context->cur_injection_finished = FALSE;
            break;
        case NETWORK_SOCKET_WAIT_FOR_EVENT:
            wait_for_event(send_sock, EV_WRITE, 0, con);
            return ret;
        case NETWORK_SOCKET_ERROR_RETRY:
        case NETWORK_SOCKET_ERROR:
            g_debug("%s.%d: network_mysqld_write(CON_STATE_SEND_QUERY) returned an error", __FILE__, __LINE__);
            /**
             *              * write() failed, close the connections
             *                           */
            con->state = CON_STATE_ERROR;
            break;
    }
    /** -- TODO --
     *      * how to handle partial backend is down. if ignore, must reduce the merge_result->shard_num
     *           */
    if (con->state != ostate) { /* the state has changed (e.g. CON_STATE_ERROR) */
        //--sharding_context->merge_result.shard_num;
        return ret;
    }
    return ret; 
}
G_INLINE_FUNC network_socket_retval_t sharding_read_mysql_packet_wrapper(short events, dbgroup_context_t *dbgroup_context, network_mysqld_con *con) {
    //gboolean ret = TRUE;
    network_socket_retval_t ret = NETWORK_SOCKET_SUCCESS;
    do {
        if (events == EV_READ) {
            ret = dbgroup_read_query_result(dbgroup_context, con); //         
            if (ret == NETWORK_SOCKET_SHARDING_SEND_NEXT_INJECTION) {
                ret = dbgroup_send_next_injection(dbgroup_context, con); 
                if (ret == NETWORK_SOCKET_SUCCESS) {
                    events = EV_READ;
                    continue;
                } 
            } else { // error/success/wait_for_event, must ret
                break;
            }    
        } else if (events == EV_WRITE && dbgroup_context->cur_injection_finished == TRUE) {
            //g_message("%s:%d write", __FILE__, __LINE__);
            ret = dbgroup_send_next_injection(dbgroup_context, con);    
            if (ret = NETWORK_SOCKET_SUCCESS) {
                events = EV_READ;
                continue;
            } else {
                break;        
            }
        } else {
            break;
        }
    } while (con->state == CON_STATE_SHARDING_READ_QUERY_RESULT);
    //g_message("%s:%d exit at state (%d)", __FILE__, __LINE__,ret);
    return ret; 
}
G_INLINE_FUNC gboolean sharding_read_query_result_start(int event_fd, short events, network_mysqld_con *con) {
    //chassis *chas = con->srv; 
    //network_mysqld_con_state_t ostate = con->state;
    //g_message("%s:%d, state:READ_QUERY_RESULT_START", __FILE__, __LINE__);
    sharding_context_t *sharding_context = con->sharding_context; 
    //gboolean is_waiting_read = FALSE;  
    //gboolean is_finish = FALSE;
    network_socket_retval_t ret = NETWORK_SOCKET_SUCCESS;
    GHashTableIter iter; 
    gpointer key, value;
    g_hash_table_iter_init(&iter, sharding_context->sql_groups);
    gboolean no_wait = FALSE; 
    //TODO(dengyihao): do while until to get complete mysql packet 
    while (g_hash_table_iter_next(&iter, &key, &value)) {
        dbgroup_context_t *gp_context = value;  
        sharding_context->ctx_in_use = gp_context;
        network_socket *server = gp_context->server;
        events = EV_READ;
        ret = sharding_read_mysql_packet_wrapper(events, gp_context, con);
        if (ret != NETWORK_SOCKET_WAIT_FOR_EVENT) {
            no_wait = TRUE; 
        }
    } 
    //g_message("%s:%d no_wait", __FILE__, __LINE__, no_wait); 
    return no_wait;
}
G_INLINE_FUNC gboolean sharding_read_query_result_continue(int event_fd, short events, network_mysqld_con *con) {
    sharding_context_t *sharding_context = con->sharding_context; 
    dbgroup_context_t *gp_context = g_hash_table_lookup(sharding_context->sql_groups, &event_fd);

    //g_message("%s:%d, state:READ_QUERY_RESULT_CONTINUE", __FILE__, __LINE__);
    sharding_context->ctx_in_use = gp_context;
    gboolean no_wait = FALSE;
    network_socket_retval_t ret = sharding_read_mysql_packet_wrapper(events, gp_context, con);  
    if (ret != NETWORK_SOCKET_WAIT_FOR_EVENT) {
        no_wait = TRUE;
    } 
    return no_wait; 
} 


G_INLINE_FUNC gboolean sharding_read_query_result(int event_fd, short event, network_mysqld_con *con) {
    if (con->sharding_context->is_from_send_query) {
        con->sharding_context->is_from_send_query = FALSE; 
        return sharding_read_query_result_start(event_fd, event, con);
    } else {
        //g_message("%s:%d, new_event start", __FILE__, __LINE__);
        return sharding_read_query_result_continue(event_fd, event, con); 
    } 
}
/**
 * free a connection 
 *
 * closes the client and server sockets 
 *
 * @param con    connection context
 */
void network_mysqld_con_free(network_mysqld_con *con) {
    if (!con) return;

    if (con->parse.data && con->parse.data_free) {
        con->parse.data_free(con->parse.data);
    }

    if (con->server) network_socket_free(con->server);
    if (con->client) network_socket_free(con->client);

    /* we are still in the conns-array */
    /*
       g_mutex_lock(&con_mutex);
       g_ptr_array_remove_fast(con->srv->priv->cons, con);
       g_mutex_unlock(&con_mutex);
       */
    g_string_free(con->charset_client, TRUE);
    g_string_free(con->charset_results, TRUE);
    g_string_free(con->charset_connection, TRUE);

    g_hash_table_remove_all(con->locks);
    g_hash_table_destroy(con->locks);

    if (con->merge_res) {
        GPtrArray* rows = con->merge_res->rows;
        if (rows) {
            guint i;
            for (i = 0; i < rows->len; ++i) {
                GPtrArray* row = g_ptr_array_index(rows, i);
                guint j;
                for (j = 0; j < row->len; ++j) {
                    g_free(g_ptr_array_index(row, j));
                }
                g_ptr_array_free(row, TRUE);
            }
            g_ptr_array_free(rows, TRUE);
        }
        g_free(con->merge_res);
    }

    if (con->challenge) {
        g_string_free(con->challenge, TRUE); 
        con->challenge = NULL;
    }

    stmt_context_free(con->stmt_context);
    con->stmt_context = NULL;

    g_slice_free(trans_context_t, con->trans_context);
    con->trans_context = NULL;   

    sharding_context_free(con->sharding_context);
    con->sharding_context = NULL;
    g_slice_free(network_mysqld_con, con);
}

#if 0 
static void dump_str(const char *msg, const unsigned char *s, size_t len) {
    GString *hex;
    size_t i;

    hex = g_string_new(NULL);

    for (i = 0; i < len; i++) {
        g_string_append_printf(hex, "%02x", s[i]);

        if ((i + 1) % 16 == 0) {
            g_string_append(hex, "\n");
        } else {
            g_string_append_c(hex, ' ');
        }

    }

    g_message("(%s): %s", msg, hex->str);

    g_string_free(hex, TRUE);
}
#endif

int network_mysqld_queue_reset(network_socket *sock) {
    sock->packet_id_is_reset = TRUE;

    return 0;
}

/**
 * synchronize the packet-ids of two network-sockets 
 */
int network_mysqld_queue_sync(network_socket *dst, network_socket *src) {
    g_assert_cmpint(src->packet_id_is_reset, ==, FALSE);

    if (dst->packet_id_is_reset == FALSE) {
        /* this shouldn't really happen */
    }

    dst->last_packet_id = src->last_packet_id - 1;

    return 0;
}

/**
 * appends a raw MySQL packet to the queue 
 *
 * the packet is append the queue directly and shouldn't be used by the caller afterwards anymore
 * and has to by in the MySQL Packet format
 *
 */
int network_mysqld_queue_append_raw(network_socket *sock, network_queue *queue, GString *data) {
    guint32 packet_len;
    guint8  packet_id;

    /* check that the length header is valid */
    if (queue != sock->send_queue &&
            queue != sock->recv_queue) {
        g_critical("%s: queue = %p doesn't belong to sock %p",
                G_STRLOC,
                (void *)queue,
                (void *)sock);
        return -1;
    }

    g_assert_cmpint(data->len, >=, 4);

    packet_len = network_mysqld_proto_get_packet_len(data);
    packet_id  = network_mysqld_proto_get_packet_id(data);

    g_assert_cmpint(packet_len, ==, data->len - 4);

    if (sock->packet_id_is_reset) {
        /* the ->last_packet_id is undefined, accept what we get */
        sock->last_packet_id = packet_id;
        sock->packet_id_is_reset = FALSE;
    } else if (packet_id != (guint8)(sock->last_packet_id + 1)) {
        sock->last_packet_id++;
#if 0
        g_critical("%s: packet-id %d doesn't match for socket's last packet %d, patching it",
                G_STRLOC,
                packet_id,
                sock->last_packet_id);
#endif
        network_mysqld_proto_set_packet_id(data, sock->last_packet_id);
    } else {
        sock->last_packet_id++;
    }

    network_queue_append(queue, data);

    return 0;
}

/**
 * appends a payload to the queue
 *
 * the packet is copied and prepened with the mysql packet header before it is appended to the queue
 * if neccesary the payload is spread over multiple mysql packets
 */
int network_mysqld_queue_append(network_socket *sock, network_queue *queue, const char *data, size_t packet_len) {
    gsize packet_offset = 0;

    do {
        GString *s;
        gsize cur_packet_len = MIN(packet_len, PACKET_LEN_MAX);

        s = g_string_sized_new(packet_len + 4);

        if (sock->packet_id_is_reset) {
            sock->packet_id_is_reset = FALSE;
            sock->last_packet_id = 0xff; /** the ++last_packet_id will make sure we send a 0 */
        }

        network_mysqld_proto_append_packet_len(s, cur_packet_len);
        network_mysqld_proto_append_packet_id(s, ++sock->last_packet_id);
        g_string_append_len(s, data + packet_offset, cur_packet_len);

        network_queue_append(queue, s);

        if (packet_len == PACKET_LEN_MAX) {
            s = g_string_sized_new(4);

            network_mysqld_proto_append_packet_len(s, 0);
            network_mysqld_proto_append_packet_id(s, ++sock->last_packet_id);

            network_queue_append(queue, s);
        }

        packet_len -= cur_packet_len;
        packet_offset += cur_packet_len;
    } while (packet_len > 0);

    return 0;
}


/**
 * create a OK packet and append it to the send-queue
 *
 * @param con             a client socket 
 * @param affected_rows   affected rows 
 * @param insert_id       insert_id 
 * @param server_status   server_status (bitfield of SERVER_STATUS_*) 
 * @param warnings        number of warnings to fetch with SHOW WARNINGS 
 * @return 0
 *
 * @todo move to network_mysqld_proto
 */
int network_mysqld_con_send_ok_full(network_socket *con, guint64 affected_rows, guint64 insert_id, guint16 server_status, guint16 warnings ) {
    GString *packet = g_string_new(NULL);
    network_mysqld_ok_packet_t *ok_packet;

    ok_packet = network_mysqld_ok_packet_new();
    ok_packet->affected_rows = affected_rows;
    ok_packet->insert_id     = insert_id;
    ok_packet->server_status = server_status;
    ok_packet->warnings      = warnings;

    network_mysqld_proto_append_ok_packet(packet, ok_packet);

    network_mysqld_queue_append(con, con->send_queue, S(packet));
    network_mysqld_queue_reset(con);

    g_string_free(packet, TRUE);
    network_mysqld_ok_packet_free(ok_packet);

    return 0;
}

/**
 * send a simple OK packet
 *
 * - no affected rows
 * - no insert-id
 * - AUTOCOMMIT
 * - no warnings
 *
 * @param con             a client socket 
 */
int network_mysqld_con_send_ok(network_socket *con) {
    return network_mysqld_con_send_ok_full(con, 0, 0, SERVER_STATUS_AUTOCOMMIT, 0);
}

static int network_mysqld_con_send_error_full_all(network_socket *con,
        const char *errmsg, gsize errmsg_len,
        guint errorcode,
        const gchar *sqlstate,
        gboolean is_41_protocol) {
    GString *packet;
    network_mysqld_err_packet_t *err_packet;

    packet = g_string_sized_new(10 + errmsg_len);

    err_packet = is_41_protocol ? network_mysqld_err_packet_new() : network_mysqld_err_packet_new_pre41();
    err_packet->errcode = errorcode;
    if (errmsg) g_string_assign_len(err_packet->errmsg, errmsg, errmsg_len);
    if (sqlstate) g_string_assign_len(err_packet->sqlstate, sqlstate, strlen(sqlstate));

    network_mysqld_proto_append_err_packet(packet, err_packet);

    network_mysqld_queue_append(con, con->send_queue, S(packet));
    network_mysqld_queue_reset(con);

    network_mysqld_err_packet_free(err_packet);
    g_string_free(packet, TRUE);

    return 0;
}

/**
 * send a error packet to the client connection
 *
 * @note the sqlstate has to match the SQL standard. If no matching SQL state is known, leave it at NULL
 *
 * @param con         the client connection
 * @param errmsg      the error message
 * @param errmsg_len  byte-len of the error-message
 * @param errorcode   mysql error-code we want to send
 * @param sqlstate    if none-NULL, 5-char SQL state to send, if NULL, default SQL state is used
 *
 * @return 0 on success
 */
int network_mysqld_con_send_error_full(network_socket *con, const char *errmsg, gsize errmsg_len, guint errorcode, const gchar *sqlstate) {
    return network_mysqld_con_send_error_full_all(con, errmsg, errmsg_len, errorcode, sqlstate, TRUE);
}


/**
 * send a error-packet to the client connection
 *
 * errorcode is 1000, sqlstate is NULL
 *
 * @param con         the client connection
 * @param errmsg      the error message
 * @param errmsg_len  byte-len of the error-message
 *
 * @see network_mysqld_con_send_error_full
 */
int network_mysqld_con_send_error(network_socket *con, const char *errmsg, gsize errmsg_len) {
    return network_mysqld_con_send_error_full(con, errmsg, errmsg_len, ER_UNKNOWN_ERROR, NULL);
}

/**
 * send a error packet to the client connection (pre-4.1 protocol)
 *
 * @param con         the client connection
 * @param errmsg      the error message
 * @param errmsg_len  byte-len of the error-message
 * @param errorcode   mysql error-code we want to send
 *
 * @return 0 on success
 */
int network_mysqld_con_send_error_pre41_full(network_socket *con, const char *errmsg, gsize errmsg_len, guint errorcode) {
    return network_mysqld_con_send_error_full_all(con, errmsg, errmsg_len, errorcode, NULL, FALSE);
}

/**
 * send a error-packet to the client connection (pre-4.1 protocol)
 *
 * @param con         the client connection
 * @param errmsg      the error message
 * @param errmsg_len  byte-len of the error-message
 *
 * @see network_mysqld_con_send_error_pre41_full
 */
int network_mysqld_con_send_error_pre41(network_socket *con, const char *errmsg, gsize errmsg_len) {
    return network_mysqld_con_send_error_pre41_full(con, errmsg, errmsg_len, ER_UNKNOWN_ERROR);
}


/**
 * get a full packet from the raw queue and move it to the packet queue 
 */
network_socket_retval_t network_mysqld_con_get_packet(chassis G_GNUC_UNUSED*chas, network_socket *con) {
    GString *packet = NULL;
    GString header;
    char header_str[NET_HEADER_SIZE + 1] = "";
    guint32 packet_len;
    guint8  packet_id;

    /** 
     * read the packet header (4 bytes)
     */
    header.str = header_str;
    header.allocated_len = sizeof(header_str);
    header.len = 0;

    /* read the packet len if the leading packet */
    if (!network_queue_peek_string(con->recv_queue_raw, NET_HEADER_SIZE, &header)) {
        /* too small */

        return NETWORK_SOCKET_WAIT_FOR_EVENT;
    }

    packet_len = network_mysqld_proto_get_packet_len(&header);
    packet_id  = network_mysqld_proto_get_packet_id(&header);

    /* move the packet from the raw queue to the recv-queue */
    if ((packet = network_queue_pop_string(con->recv_queue_raw, packet_len + NET_HEADER_SIZE, NULL))) {
#ifdef NETWORK_DEBUG_TRACE_IO
        /* to trace the data we received from the socket, enable this */
        g_debug_hexdump(G_STRLOC, S(packet));
#endif

        if (con->packet_id_is_reset) {
            con->last_packet_id = packet_id;
            con->packet_id_is_reset = FALSE;
        } else if (packet_id != (guint8)(con->last_packet_id + 1)) {
            g_critical("%s: received packet-id %d, but expected %d ... out of sync.",
                    G_STRLOC,
                    packet_id,
                    con->last_packet_id + 1);
            return NETWORK_SOCKET_ERROR;
        } else {
            con->last_packet_id = packet_id;
        }

        network_queue_append(con->recv_queue, packet);
    } else {
        return NETWORK_SOCKET_WAIT_FOR_EVENT;
    }

    return NETWORK_SOCKET_SUCCESS;
}

/**
 * read a MySQL packet from the socket
 *
 * the packet is added to the con->recv_queue and contains a full mysql packet
 * with packet-header and everything 
 */
network_socket_retval_t network_mysqld_read(chassis G_GNUC_UNUSED*chas, network_socket *con) {
    switch (network_socket_read(con)) {
        case NETWORK_SOCKET_WAIT_FOR_EVENT:
            return NETWORK_SOCKET_WAIT_FOR_EVENT;
        case NETWORK_SOCKET_ERROR:
            return NETWORK_SOCKET_ERROR;
        case NETWORK_SOCKET_SUCCESS:
            break;
        case NETWORK_SOCKET_ERROR_RETRY:
            g_error("NETWORK_SOCKET_ERROR_RETRY wasn't expected");
            break;
    }

    return network_mysqld_con_get_packet(chas, con);
}

network_socket_retval_t network_mysqld_write(chassis G_GNUC_UNUSED*chas, network_socket *con) {
    network_socket_retval_t ret;

    ret = network_socket_write(con, -1);

    return ret;
}

/**
 * call the hooks of the plugins for each state
 *
 * if the plugin doesn't implement a hook, we provide a default operation
 *
 * @param srv      the global context
 * @param con      the connection context
 * @param state    state to handle
 * @return         NETWORK_SOCKET_SUCCESS on success
 */
network_socket_retval_t plugin_call(chassis *srv, network_mysqld_con *con, int state) {
    network_socket_retval_t ret;
    NETWORK_MYSQLD_PLUGIN_FUNC(func) = NULL;

    switch (state) {
        case CON_STATE_INIT:
            func = con->plugins.con_init;

            if (!func) { /* default implementation */
                con->state = CON_STATE_CONNECT_SERVER;
            }
            break;
        case CON_STATE_CONNECT_SERVER:
            func = con->plugins.con_connect_server;

            if (!func) { /* default implementation */
                con->state = CON_STATE_READ_HANDSHAKE;
            }

            break;
        case CON_STATE_SEND_HANDSHAKE:
            func = con->plugins.con_send_handshake;

            if (!func) { /* default implementation */
                con->state = CON_STATE_READ_AUTH;
            }

            break;
        case CON_STATE_READ_HANDSHAKE:
            func = con->plugins.con_read_handshake;

            break;
        case CON_STATE_READ_AUTH:
            func = con->plugins.con_read_auth;

            break;
        case CON_STATE_SEND_AUTH:
            func = con->plugins.con_send_auth;

            if (!func) { /* default implementation */
                con->state = CON_STATE_READ_AUTH_RESULT;
            }
            break;
        case CON_STATE_READ_AUTH_RESULT:
            func = con->plugins.con_read_auth_result;
            break;
        case CON_STATE_SEND_AUTH_RESULT:
            func = con->plugins.con_send_auth_result;

            if (!func) { /* default implementation */
                switch (con->auth_result_state) {
                    case MYSQLD_PACKET_OK:
                        con->state = CON_STATE_READ_QUERY;
                        break;
                    case MYSQLD_PACKET_ERR:
                        con->state = CON_STATE_ERROR;
                        break;
                    case MYSQLD_PACKET_EOF:
                        /**
                         * the MySQL 4.0 hash in a MySQL 4.1+ connection
                         */
                        con->state = CON_STATE_READ_AUTH_OLD_PASSWORD;
                        break;
                    default:
                        g_error("%s.%d: unexpected state for SEND_AUTH_RESULT: %02x", 
                                __FILE__, __LINE__,
                                con->auth_result_state);
                }
            }
            break;
        case CON_STATE_READ_AUTH_OLD_PASSWORD: {
                                                   /** move the packet to the send queue */
                                                   GString *packet;
                                                   GList *chunk;
                                                   network_socket *recv_sock, *send_sock;

                                                   recv_sock = con->client;
                                                   send_sock = con->server;

                                                   if (NULL == con->server) {
                                                       /**
                                                        * we have to auth against same backend as we did before
                                                        * but the user changed it
                                                        */

                                                       g_message("%s.%d: (lua) read-auth-old-password failed as backend_ndx got reset.", __FILE__, __LINE__);

                                                       network_mysqld_con_send_error(con->client, C("(lua) read-auth-old-password failed as backend_ndx got reset."));
                                                       con->state = CON_STATE_SEND_ERROR;
                                                       break;
                                                   }

                                                   chunk = recv_sock->recv_queue->chunks->head;
                                                   packet = chunk->data;

                                                   /* we aren't finished yet */
                                                   network_queue_append(send_sock->send_queue, packet);

                                                   g_queue_delete_link(recv_sock->recv_queue->chunks, chunk);

                                                   /**
                                                    * send it out to the client 
                                                    */
                                                   con->state = CON_STATE_SEND_AUTH_OLD_PASSWORD;
                                                   break; }
        case CON_STATE_SEND_AUTH_OLD_PASSWORD:
                                               /**
                                                * data is at the server, read the response next 
                                                */
                                               con->state = CON_STATE_READ_AUTH_RESULT;
                                               break;
        case CON_STATE_READ_QUERY:
                                               func = con->plugins.con_read_query;
                                               break;
        case CON_STATE_READ_QUERY_RESULT:
                                               func = con->plugins.con_read_query_result;
                                               break;
        case CON_STATE_SEND_QUERY_RESULT:
                                               func = con->plugins.con_send_query_result;

                                               if (!func) { /* default implementation */
                                                   con->state = CON_STATE_READ_QUERY;
                                               }
                                               break;
        case CON_STATE_SHARDING_READ_QUERY_RESULT:
                                               func = con->plugins.con_sharding_read_query_result;
                                               break;
        case CON_STATE_SHARDING_SEND_QUERY_RESULT:                                     
                                               func = con->plugins.con_sharding_send_query_result;
                                               if (!func) {
                                                   con->state = CON_STATE_READ_QUERY; 
                                               } 
                                               break;
        case CON_STATE_SEND_LOCAL_INFILE_DATA:
                                               func = con->plugins.con_send_local_infile_data;

                                               if (!func) { /* default implementation */
                                                   con->state = CON_STATE_READ_LOCAL_INFILE_RESULT;
                                               }

                                               break;
        case CON_STATE_READ_LOCAL_INFILE_DATA:
                                               func = con->plugins.con_read_local_infile_data;

                                               if (!func) { /* the plugins have to implement this function to track LOAD DATA LOCAL INFILE handling work */
                                                   con->state = CON_STATE_ERROR;
                                               }

                                               break;
        case CON_STATE_SEND_LOCAL_INFILE_RESULT:
                                               func = con->plugins.con_send_local_infile_result;

                                               if (!func) { /* default implementation */
                                                   con->state = CON_STATE_READ_QUERY;
                                               }

                                               break;
        case CON_STATE_READ_LOCAL_INFILE_RESULT:
                                               func = con->plugins.con_read_local_infile_result;

                                               if (!func) { /* the plugins have to implement this function to track LOAD DATA LOCAL INFILE handling work */
                                                   con->state = CON_STATE_ERROR;
                                               }

                                               break;
        case CON_STATE_ERROR:
                                               g_debug("%s.%d: not executing plugin function in state CON_STATE_ERROR", __FILE__, __LINE__);
                                               return NETWORK_SOCKET_SUCCESS;
        default:
                                               g_error("%s.%d: unhandled state: %d", 
                                                       __FILE__, __LINE__,
                                                       state);
    }
    if (!func) return NETWORK_SOCKET_SUCCESS;

    //	LOCK_LUA(srv->priv->sc);	/*remove lock*/
    ret = (*func)(srv, con);
    //	UNLOCK_LUA(srv->priv->sc);	/*remove lock*/

    return ret;
}

/**
 * reset the command-response parsing
 *
 * some commands needs state information and we have to 
 * reset the parsing as soon as we add a new command to the send-queue
 */
void network_mysqld_con_reset_command_response_state(network_mysqld_con *con) {
    con->parse.command = -1;
    if (con->parse.data && con->parse.data_free) {
        con->parse.data_free(con->parse.data);

        con->parse.data = NULL;
        con->parse.data_free = NULL;
    }
}

/**
 *  
 * reset the command-response parsing  
 *  
 */
void sharding_network_mysqld_con_reset_command_response_state(dbgroup_context_t *ctx) {
    ctx->parse.command = -1;
    if (ctx->parse.data && ctx->parse.data_free) {
        ctx->parse.data_free(ctx->parse.data);

        ctx->parse.data = NULL;
        ctx->parse.data_free = NULL;
    }

}
gint sharding_parse_command_states_init(dbgroup_context_t *ctx, network_packet *packet) {
    struct network_mysqld_con_parse* parse = &ctx->parse; 
    guint8 cmd;
    int err = 0;

    err = err || network_mysqld_proto_skip_network_header(packet);
    err = err || network_mysqld_proto_get_int8(packet, &cmd);

    if (err) return -1;
    parse->command = cmd;
    packet->offset = 0; /* reset the offset again for the next function*/

    switch(parse->command) {
        case COM_QUERY:
        case COM_PROCESS_INFO:
        case COM_STMT_EXECUTE:
            parse->data = network_mysqld_com_query_result_new();
            parse->data_free = (GDestroyNotify) network_mysqld_com_query_result_free;
            break;
        case COM_STMT_PREPARE:
            parse->data = network_mysqld_com_stmt_prepare_result_new();
            parse->data_free = (GDestroyNotify)network_mysqld_com_stmt_prepare_result_free;
            break;
        case COM_INIT_DB:
            parse->data = network_mysqld_com_init_db_result_new();
            parse->data_free = (GDestroyNotify)network_mysqld_com_init_db_result_free;

            network_mysqld_com_init_db_result_track_state(packet, parse->data);
            break;
        case COM_QUIT:
            /* track COM_QUIT going to the server, to be able to tell if the server
               1969              * a) simply went away or
               1970              * b) closed the connection because the client asked it to
               1971              * If b) we should not print a message at the next EV_READ event from the server fd
               1972              */
            ctx->com_quit_seen = TRUE;
        default:
            break;
    }
    return 0;
} 
/**
 * get the name of a connection state
 */
const char *network_mysqld_con_state_get_name(network_mysqld_con_state_t state) {
    switch (state) {
        case CON_STATE_INIT: return "CON_STATE_INIT";
        case CON_STATE_CONNECT_SERVER: return "CON_STATE_CONNECT_SERVER";
        case CON_STATE_READ_HANDSHAKE: return "CON_STATE_READ_HANDSHAKE";
        case CON_STATE_SEND_HANDSHAKE: return "CON_STATE_SEND_HANDSHAKE";
        case CON_STATE_READ_AUTH: return "CON_STATE_READ_AUTH";
        case CON_STATE_SEND_AUTH: return "CON_STATE_SEND_AUTH";
        case CON_STATE_READ_AUTH_OLD_PASSWORD: return "CON_STATE_READ_AUTH_OLD_PASSWORD";
        case CON_STATE_SEND_AUTH_OLD_PASSWORD: return "CON_STATE_SEND_AUTH_OLD_PASSWORD";
        case CON_STATE_READ_AUTH_RESULT: return "CON_STATE_READ_AUTH_RESULT";
        case CON_STATE_SEND_AUTH_RESULT: return "CON_STATE_SEND_AUTH_RESULT";
        case CON_STATE_READ_QUERY: return "CON_STATE_READ_QUERY";
        case CON_STATE_SEND_QUERY: return "CON_STATE_SEND_QUERY";
        case CON_STATE_READ_QUERY_RESULT: return "CON_STATE_READ_QUERY_RESULT";
        case CON_STATE_SEND_QUERY_RESULT: return "CON_STATE_SEND_QUERY_RESULT";
        case CON_STATE_READ_LOCAL_INFILE_DATA: return "CON_STATE_READ_LOCAL_INFILE_DATA";
        case CON_STATE_SEND_LOCAL_INFILE_DATA: return "CON_STATE_SEND_LOCAL_INFILE_DATA";
        case CON_STATE_READ_LOCAL_INFILE_RESULT: return "CON_STATE_READ_LOCAL_INFILE_RESULT";
        case CON_STATE_SEND_LOCAL_INFILE_RESULT: return "CON_STATE_SEND_LOCAL_INFILE_RESULT";
        case CON_STATE_CLOSE_CLIENT: return "CON_STATE_CLOSE_CLIENT";
        case CON_STATE_CLOSE_SERVER: return "CON_STATE_CLOSE_SERVER";
        case CON_STATE_ERROR: return "CON_STATE_ERROR";
        case CON_STATE_SEND_ERROR: return "CON_STATE_SEND_ERROR";
    }

    return "unknown";
}
/**
 * handle the different states of the MySQL protocol
 *
 * @param event_fd     fd on which the event was fired
 * @param events       the event that was fired
 * @param user_data    the connection handle
 */
void network_mysqld_con_handle(int event_fd, short events, void *user_data) {
    network_mysqld_con_state_t ostate;
    network_mysqld_con *con = user_data;
    chassis *srv = con->srv;
    int retval;
    network_socket_retval_t call_ret;

    g_assert(srv);
    g_assert(con);

    if (events == EV_READ) {
        int b = -1;

        /**
         * check how much data there is to read
         *
         * ioctl()
         * - returns 0 if connection is closed
         * - or -1 and ECONNRESET on solaris
         *   or -1 and EPIPE on HP/UX
         */
        if (ioctl(event_fd, FIONREAD, &b)) {
            switch (errno) {
                case E_NET_CONNRESET: /* solaris */
                case EPIPE: /* hp/ux */
                    if (con->client && event_fd == con->client->fd) {
                        /* the client closed the connection, let's keep the server side open */
                        con->state = CON_STATE_CLOSE_CLIENT;
                    } else if (con->server && event_fd == con->server->fd && con->com_quit_seen) {
                        con->state = CON_STATE_CLOSE_SERVER;
                    } else {
                        /* server side closed on use, oops, close both sides */
                        con->state = CON_STATE_ERROR;
                    }
                    break;
                default:
                    g_critical("ioctl(%d, FIONREAD, ...) failed: %s", event_fd, g_strerror(errno));

                    con->state = CON_STATE_ERROR;
                    break;
            }
        } else if (b != 0) {
            if (con->client && event_fd == con->client->fd) {
                con->client->to_read = b;
            } else if (con->server && event_fd == con->server->fd) {
                con->server->to_read = b;
            } else {
                sharding_context_t *sharding_context = con->sharding_context; 
                dbgroup_context_t *group_context = (dbgroup_context_t *)g_hash_table_lookup(sharding_context->sql_groups, &event_fd);
                if (NULL == group_context) {
                    //g_message("%s.%d: neither nor", __FILE__, __LINE__);
                } else {
                    //g_message("%s:%d server read %d", __FILE__, __LINE__, b);
                    group_context->server->to_read = b;
                }
            }
        } else { /* Linux */
            if (con->client && event_fd == con->client->fd) {
                /* the client closed the connection, let's keep the server side open */
                con->state = CON_STATE_CLOSE_CLIENT;
            } else if (con->server && event_fd == con->server->fd && con->com_quit_seen) {
                con->state = CON_STATE_CLOSE_SERVER;
            } else {
                /* server side closed on use, oops, close both sides */
                con->state = CON_STATE_ERROR;
            }
        }
    }
    /**
     * loop on the same connection as long as we don't end up in a stable state
     */

    if (event_fd != -1) {
        NETWORK_MYSQLD_CON_TRACK_TIME(con, "wait_for_event::done");
    } else {
        NETWORK_MYSQLD_CON_TRACK_TIME(con, "con_handle_start");
    }

    do {
        ostate = con->state;
#ifdef NETWORK_DEBUG_TRACE_STATE_CHANGES
        /* if you need the state-change information without dtrace, enable this */
        g_debug("%s: [%d] %s",
                G_STRLOC,
                getpid(),
                network_mysqld_con_state_get_name(con->state));
#endif

        MYSQLPROXY_STATE_CHANGE(event_fd, events, con->state);
        switch (con->state) {
            case CON_STATE_ERROR:
                /* we can't go on, close the connection */
                /*
                   {
                   gchar *which_connection = "a";
                   if (con->server && event_fd == con->server->fd) {
                   which_connection = "server";
                   } else if (con->client && event_fd == con->client->fd) {
                   which_connection = "client";
                   }
                   g_debug("[%s]: error on %s connection (fd: %d event: %d). closing client connection.",
                   G_STRLOC, which_connection,	event_fd, events);
                   }
                   */
                plugin_call_cleanup(srv, con);
                network_mysqld_con_free(con);

                con = NULL;

                return;
            case CON_STATE_CLOSE_CLIENT:
            case CON_STATE_CLOSE_SERVER:
                /* FIXME: this comment has nothing to do with reality...
                 * the server connection is still fine, 
                 * let's keep it open for reuse */

                plugin_call_cleanup(srv, con);

                network_mysqld_con_free(con);

                con = NULL;

                return;
            case CON_STATE_INIT:
                /* if we are a proxy ask the remote server for the hand-shake packet 
                 * if not, we generate one */
                switch (plugin_call(srv, con, con->state)) {
                    case NETWORK_SOCKET_SUCCESS:
                        break;
                    default:
                        /**
                         * no luck, let's close the connection
                         */
                        g_critical("%s.%d: plugin_call(CON_STATE_INIT) != NETWORK_SOCKET_SUCCESS", __FILE__, __LINE__);

                        con->state = CON_STATE_ERROR;

                        break;
                }

                break;
            case CON_STATE_CONNECT_SERVER:
                if (events == EV_TIMEOUT) {
                    network_mysqld_con_lua_t *st = con->plugin_con_state;
                    if (st->backend->b->state != BACKEND_STATE_OFFLINE) st->backend->b->state = BACKEND_STATE_DOWN;
                    network_socket_free(con->server);
                    con->server = NULL;
                    ostate = CON_STATE_INIT;

                    g_warning("timeout in connecting server");
                    break;
                }
                switch ((retval = plugin_call(srv, con, con->state))) {
                    case NETWORK_SOCKET_SUCCESS:

                        /**
                         * hmm, if this is success and we have something in the clients send-queue
                         * we just send it out ... who needs a server ? */

                        if ((con->client != NULL && con->client->send_queue->chunks->length > 0) && 
                                con->server == NULL) {
                            /* we want to send something to the client */

                            con->state = CON_STATE_SEND_HANDSHAKE;
                        } else {
                            g_assert(con->server);
                        }

                        break;
                    case NETWORK_SOCKET_ERROR_RETRY:
                        if (con->server) {
                            /**
                             * we have a server connection waiting to begin writable
                             */
                            wait_for_event(con->server, EV_WRITE, 5, con);
                            NETWORK_MYSQLD_CON_TRACK_TIME(con, "wait_for_event::connect_server");
                            return;
                        } else {
                            /* try to get a connection to another backend,
                             *
                             * setting ostate = CON_STATE_INIT is a hack to make sure
                             * the loop is coming back to this function again */
                            ostate = CON_STATE_INIT;
                        }

                        break;
                    case NETWORK_SOCKET_ERROR:
                        /**
                         * connecting failed and no option to retry
                         *
                         * close the connection
                         */
                        con->state = CON_STATE_SEND_ERROR;
                        break;
                    default:
                        g_critical("%s: hook for CON_STATE_CONNECT_SERVER return invalid return code: %d", 
                                G_STRLOC, 
                                retval);

                        con->state = CON_STATE_ERROR;

                        break;
                }

                break;
            case CON_STATE_READ_HANDSHAKE: {
                                               /**
                                                * read auth data from the remote mysql-server 
                                                */
                                               network_socket *recv_sock;
                                               recv_sock = con->server;
                                               g_assert(events == 0 || event_fd == recv_sock->fd);

                                               switch (network_mysqld_read(srv, recv_sock)) {
                                                   case NETWORK_SOCKET_SUCCESS:
                                                       break;
                                                   case NETWORK_SOCKET_WAIT_FOR_EVENT:
                                                       /* call us again when you have a event */
                                                       wait_for_event(con->server, EV_READ, 0, con);
                                                       NETWORK_MYSQLD_CON_TRACK_TIME(con, "wait_for_event::read_handshake");
                                                       return;
                                                   case NETWORK_SOCKET_ERROR_RETRY:
                                                   case NETWORK_SOCKET_ERROR:
                                                       g_critical("%s.%d: network_mysqld_read(CON_STATE_READ_HANDSHAKE) returned an error", __FILE__, __LINE__);
                                                       con->state = CON_STATE_ERROR;
                                                       break;
                                               }

                                               if (con->state != ostate) break; /* the state has changed (e.g. CON_STATE_ERROR) */

                                               switch (plugin_call(srv, con, con->state)) {
                                                   case NETWORK_SOCKET_SUCCESS:
                                                       break;
                                                   case NETWORK_SOCKET_ERROR:
                                                       /**
                                                        * we couldn't understand the pack from the server 
                                                        * 
                                                        * we have something in the queue and will send it to the client
                                                        * and close the connection afterwards
                                                        */

                                                       con->state = CON_STATE_SEND_ERROR;

                                                       break;
                                                   default:
                                                       g_critical("%s.%d: ...", __FILE__, __LINE__);
                                                       con->state = CON_STATE_ERROR;
                                                       break;
                                               }

                                               break; }
            case CON_STATE_SEND_HANDSHAKE: 
                                           /* send the hand-shake to the client and wait for a response */
                                           switch (network_mysqld_write(srv, con->client)) {
                                               case NETWORK_SOCKET_SUCCESS:
                                                   break;
                                               case NETWORK_SOCKET_WAIT_FOR_EVENT:
                                                   wait_for_event(con->client, EV_WRITE, 0, con);
                                                   NETWORK_MYSQLD_CON_TRACK_TIME(con, "wait_for_event::send_handshake");

                                                   return;
                                               case NETWORK_SOCKET_ERROR_RETRY:
                                               case NETWORK_SOCKET_ERROR:
                                                   /**
                                                    * writing failed, closing connection
                                                    */
                                                   con->state = CON_STATE_ERROR;
                                                   break;
                                           }

                                           if (con->state != ostate) break; /* the state has changed (e.g. CON_STATE_ERROR) */

                                           switch (plugin_call(srv, con, con->state)) {
                                               case NETWORK_SOCKET_SUCCESS:
                                                   break;
                                               default:
                                                   g_critical("%s.%d: plugin_call(CON_STATE_SEND_HANDSHAKE) != NETWORK_SOCKET_SUCCESS", __FILE__, __LINE__);
                                                   con->state = CON_STATE_ERROR;
                                                   break;
                                           }

                                           break;
            case CON_STATE_READ_AUTH: {
                                          /* read auth from client */
                                          if (events == EV_TIMEOUT) {
                                              //g_message("%s: close the noninteractive connection(%s) now.", G_STRLOC, recv_sock->src->name->str);
                                              con->state = CON_STATE_ERROR;
                                              break;
                                          }
                                          network_socket *recv_sock;

                                          recv_sock = con->client;

                                          g_assert(events == 0 || event_fd == recv_sock->fd);

                                          switch (network_mysqld_read(srv, recv_sock)) {
                                              case NETWORK_SOCKET_SUCCESS:
                                                  break;
                                              case NETWORK_SOCKET_WAIT_FOR_EVENT:
                                                  wait_for_event(con->client, EV_READ, srv->wait_timeout, con);
                                                  NETWORK_MYSQLD_CON_TRACK_TIME(con, "wait_for_event::read_auth");

                                                  return;
                                              case NETWORK_SOCKET_ERROR_RETRY:
                                              case NETWORK_SOCKET_ERROR:
                                                  g_critical("%s.%d: network_mysqld_read(CON_STATE_READ_AUTH) returned an error", __FILE__, __LINE__);
                                                  con->state = CON_STATE_ERROR;
                                                  break;
                                          }

                                          if (con->state != ostate) break; /* the state has changed (e.g. CON_STATE_ERROR) */

                                          switch (plugin_call(srv, con, con->state)) {
                                              case NETWORK_SOCKET_SUCCESS:
                                                  break;
                                              case NETWORK_SOCKET_ERROR:
                                                  con->state = CON_STATE_SEND_ERROR;
                                                  break;
                                              default:
                                                  g_critical("%s.%d: plugin_call(CON_STATE_READ_AUTH) != NETWORK_SOCKET_SUCCESS", __FILE__, __LINE__);
                                                  con->state = CON_STATE_ERROR;
                                                  break;
                                          }

                                          break; }
            case CON_STATE_SEND_AUTH:
                                          /* send the auth-response to the server */
                                          switch (network_mysqld_write(srv, con->server)) {
                                              case NETWORK_SOCKET_SUCCESS:
                                                  break;
                                              case NETWORK_SOCKET_WAIT_FOR_EVENT:
                                                  wait_for_event(con->server, EV_WRITE, 0, con);
                                                  NETWORK_MYSQLD_CON_TRACK_TIME(con, "wait_for_event::send_auth");

                                                  return;
                                              case NETWORK_SOCKET_ERROR_RETRY:
                                              case NETWORK_SOCKET_ERROR:
                                                  /* might be a connection close, we should just close the connection and be happy */
                                                  con->state = CON_STATE_ERROR;

                                                  break;
                                          }

                                          if (con->state != ostate) break; /* the state has changed (e.g. CON_STATE_ERROR) */

                                          switch (plugin_call(srv, con, con->state)) {
                                              case NETWORK_SOCKET_SUCCESS:
                                                  break;
                                              default:
                                                  g_critical("%s.%d: plugin_call(CON_STATE_SEND_AUTH) != NETWORK_SOCKET_SUCCESS", __FILE__, __LINE__);
                                                  con->state = CON_STATE_ERROR;
                                                  break;
                                          }

                                          break;
            case CON_STATE_READ_AUTH_RESULT: {
                                                 /* read the auth result from the server */
                                                 network_socket *recv_sock;
                                                 GList *chunk;
                                                 GString *packet;
                                                 recv_sock = con->server;

                                                 g_assert(events == 0 || event_fd == recv_sock->fd);

                                                 switch (network_mysqld_read(srv, recv_sock)) {
                                                     case NETWORK_SOCKET_SUCCESS:
                                                         break;
                                                     case NETWORK_SOCKET_WAIT_FOR_EVENT:
                                                         //WAIT_FOR_EVENT(con->server, EV_READ, 0);
                                                         wait_for_event(con->server, EV_READ, 0, con);
                                                         NETWORK_MYSQLD_CON_TRACK_TIME(con, "wait_for_event::read_auth_result");
                                                         return;
                                                     case NETWORK_SOCKET_ERROR_RETRY:
                                                     case NETWORK_SOCKET_ERROR:
                                                         g_critical("%s.%d: network_mysqld_read(CON_STATE_READ_AUTH_RESULT) returned an error", __FILE__, __LINE__);
                                                         con->state = CON_STATE_ERROR;
                                                         break;
                                                 }
                                                 if (con->state != ostate) break; /* the state has changed (e.g. CON_STATE_ERROR) */

                                                 /**
                                                  * depending on the result-set we have different exit-points
                                                  * - OK  -> READ_QUERY
                                                  * - EOF -> (read old password hash) 
                                                  * - ERR -> ERROR
                                                  */
                                                 chunk = recv_sock->recv_queue->chunks->head;
                                                 packet = chunk->data;
                                                 g_assert(packet);
                                                 g_assert(packet->len > NET_HEADER_SIZE);

                                                 con->auth_result_state = packet->str[NET_HEADER_SIZE];

                                                 switch (plugin_call(srv, con, con->state)) {
                                                     case NETWORK_SOCKET_SUCCESS:
                                                         break;
                                                     default:
                                                         g_critical("%s.%d: plugin_call(CON_STATE_READ_AUTH_RESULT) != NETWORK_SOCKET_SUCCESS", __FILE__, __LINE__);

                                                         con->state = CON_STATE_ERROR;
                                                         break;
                                                 }

                                                 break; }
            case CON_STATE_SEND_AUTH_RESULT: {
                                                 /* send the hand-shake to the client and wait for a response */

                                                 switch (network_mysqld_write(srv, con->client)) {
                                                     case NETWORK_SOCKET_SUCCESS:
                                                         break;
                                                     case NETWORK_SOCKET_WAIT_FOR_EVENT:
                                                         //WAIT_FOR_EVENT(con->client, EV_WRITE, 0);
                                                         wait_for_event(con->client, EV_WRITE, 0, con);
                                                         NETWORK_MYSQLD_CON_TRACK_TIME(con, "wait_for_event::send_auth_result");
                                                         return;
                                                     case NETWORK_SOCKET_ERROR_RETRY:
                                                     case NETWORK_SOCKET_ERROR:
                                                         g_debug("%s.%d: network_mysqld_write(CON_STATE_SEND_AUTH_RESULT) returned an error", __FILE__, __LINE__);

                                                         con->state = CON_STATE_ERROR;
                                                         break;
                                                 }

                                                 if (con->state != ostate) break; /* the state has changed (e.g. CON_STATE_ERROR) */

                                                 switch (plugin_call(srv, con, con->state)) {
                                                     case NETWORK_SOCKET_SUCCESS:
                                                         break;
                                                     default:
                                                         g_critical("%s.%d: ...", __FILE__, __LINE__);
                                                         con->state = CON_STATE_ERROR;
                                                         break;
                                                 }

                                                 break; }
            case CON_STATE_READ_AUTH_OLD_PASSWORD: 
                                                 /* read auth from client */
                                                 switch (network_mysqld_read(srv, con->client)) {
                                                     case NETWORK_SOCKET_SUCCESS:
                                                         break;
                                                     case NETWORK_SOCKET_WAIT_FOR_EVENT:
                                                         wait_for_event(con->client, EV_READ, 0, con);
                                                         NETWORK_MYSQLD_CON_TRACK_TIME(con, "wait_for_event::read_auth_old_password");

                                                         return;
                                                     case NETWORK_SOCKET_ERROR_RETRY:
                                                     case NETWORK_SOCKET_ERROR:
                                                         g_critical("%s.%d: network_mysqld_read(CON_STATE_READ_AUTH_OLD_PASSWORD) returned an error", __FILE__, __LINE__);
                                                         con->state = CON_STATE_ERROR;
                                                         break;
                                                 }

                                                 if (con->state != ostate) break; /* the state has changed (e.g. CON_STATE_ERROR) */

                                                 switch (plugin_call(srv, con, con->state)) {
                                                     case NETWORK_SOCKET_SUCCESS:
                                                         break;
                                                     default:
                                                         g_critical("%s.%d: plugin_call(CON_STATE_READ_AUTH_OLD_PASSWORD) != NETWORK_SOCKET_SUCCESS", __FILE__, __LINE__);
                                                         con->state = CON_STATE_ERROR;
                                                         break;
                                                 }

                                                 break; 
            case CON_STATE_SEND_AUTH_OLD_PASSWORD:
                                                 /* send the auth-response to the server */
                                                 switch (network_mysqld_write(srv, con->server)) {
                                                     case NETWORK_SOCKET_SUCCESS:
                                                         break;
                                                     case NETWORK_SOCKET_WAIT_FOR_EVENT:
                                                         wait_for_event(con->server, EV_WRITE, 0, con);
                                                         NETWORK_MYSQLD_CON_TRACK_TIME(con, "wait_for_event::send_auth_old_password");

                                                         return;
                                                     case NETWORK_SOCKET_ERROR_RETRY:
                                                     case NETWORK_SOCKET_ERROR:
                                                         /* might be a connection close, we should just close the connection and be happy */
                                                         g_debug("%s.%d: network_mysqld_write(CON_STATE_SEND_AUTH_OLD_PASSWORD) returned an error", __FILE__, __LINE__);
                                                         con->state = CON_STATE_ERROR;
                                                         break;
                                                 }
                                                 if (con->state != ostate) break; /* the state has changed (e.g. CON_STATE_ERROR) */

                                                 switch (plugin_call(srv, con, con->state)) {
                                                     case NETWORK_SOCKET_SUCCESS:
                                                         break;
                                                     default:
                                                         g_critical("%s.%d: plugin_call(CON_STATE_SEND_AUTH_OLD_PASSWORD) != NETWORK_SOCKET_SUCCESS", __FILE__, __LINE__);
                                                         con->state = CON_STATE_ERROR;
                                                         break;
                                                 }

                                                 break;

            case CON_STATE_READ_QUERY: {
                                           network_socket *recv_sock = con->client;

                                           if (events == EV_TIMEOUT) {
                                               //g_message("%s: close the noninteractive connection(%s) now.", G_STRLOC, recv_sock->src->name->str);
                                               con->state = CON_STATE_ERROR;
                                               break;
                                           }

                                           g_assert(events == 0 || event_fd == recv_sock->fd);

                                           network_packet last_packet;
                                           do { 
                                               switch (network_mysqld_read(srv, recv_sock)) {
                                                   case NETWORK_SOCKET_SUCCESS:
                                                       break;
                                                   case NETWORK_SOCKET_WAIT_FOR_EVENT:
                                                       wait_for_event(recv_sock, EV_READ, srv->wait_timeout, con);
                                                       NETWORK_MYSQLD_CON_TRACK_TIME(con, "wait_for_event::read_query");
                                                       //g_message("%s:%d, wait for read query event", __FILE__, __LINE__);
                                                       return;
                                                   case NETWORK_SOCKET_ERROR_RETRY:
                                                   case NETWORK_SOCKET_ERROR:
                                                       g_critical("%s.%d: network_mysqld_read(CON_STATE_READ_QUERY) returned an error", __FILE__, __LINE__);
                                                       con->state = CON_STATE_ERROR;
                                                       break;
                                               }
                                               if (con->state != ostate) break; /* the state has changed (e.g. CON_STATE_ERROR) */

                                               last_packet.data = g_queue_peek_tail(recv_sock->recv_queue->chunks);

                                           } while (last_packet.data->len == PACKET_LEN_MAX + NET_HEADER_SIZE); /* read all chunks of the overlong data */

                                           if (con->server &&
                                                   con->server->challenge &&
                                                   con->server->challenge->server_version > 50113 && con->server->challenge->server_version < 50118) {
                                               /**
                                                * Bug #25371
                                                *
                                                * COM_CHANGE_USER returns 2 ERR packets instead of one
                                                *
                                                * we can auto-correct the issue if needed and remove the second packet
                                                * Some clients handle this issue and expect a double ERR packet.
                                                */
                                               network_packet packet;
                                               guint8 com;

                                               packet.data = g_queue_peek_head(recv_sock->recv_queue->chunks);
                                               packet.offset = 0; 
                                               if (0 == network_mysqld_proto_skip_network_header(&packet) && 0 == network_mysqld_proto_get_int8(&packet, &com) && com == COM_CHANGE_USER) { 
                                                   network_mysqld_con_send_error(con->client, C("COM_CHANGE_USER is broken on 5.1.14-.17, please upgrade the MySQL Server"));
                                                   con->state = CON_STATE_SEND_QUERY_RESULT;
                                                   break;
                                               }
                                           }

                                           switch (plugin_call(srv, con, con->state)) {
                                               case NETWORK_SOCKET_SUCCESS:
                                                   break;
                                               default:
                                                   g_critical("%s.%d: plugin_call(CON_STATE_READ_QUERY) failed", __FILE__, __LINE__);

                                                   con->state = CON_STATE_ERROR;
                                                   break;
                                           }

                                           /**
                                            * there should be 3 possible next states from here:
                                            *
                                            * - CON_STATE_ERROR (if something went wrong and we want to close the connection
                                            * - CON_STATE_SEND_QUERY (if we want to send data to the con->server)
                                            * - CON_STATE_SEND_QUERY_RESULT (if we want to send data to the con->client)
                                            *
                                            * @todo verify this with a clean switch ()
                                            */

                                           /* reset the tracked command
                                            *
                                            * if the plugin decided to send a result, it has to track the commands itself
                                            * otherwise LOAD DATA LOCAL INFILE and friends will fail
                                            */

                                           //g_message("%s:%d, state:READ_QUERY  ", __FILE__, __LINE__);
                                           if (con->state == CON_STATE_SEND_QUERY) {
                                               network_mysqld_con_reset_command_response_state(con);
                                           } else if (con->state == CON_STATE_SHARDING_SEND_QUERY) {
                                               //g_message("%s:%d, state:CON_STATE_SHARDING_SEND_QUERY  ", __FILE__, __LINE__);
                                               goto no_reset_eventfd;
                                           }

                                           break; 
                                       }
            case CON_STATE_SEND_QUERY:
                                       /* send the query to the server
                                        *
                                        * this state will loop until all the packets from the send-queue are flushed 
                                        */

                                       //g_message("%s:%d, state:SEND_QUERY  ", __FILE__, __LINE__);
                                       if (con->server->send_queue->offset == 0) {
                                           /* only parse the packets once */
                                           network_packet packet;

                                           packet.data = g_queue_peek_head(con->server->send_queue->chunks);
                                           packet.offset = 0;

                                           if (0 != network_mysqld_con_command_states_init(con, &packet)) {
                                               g_debug("%s: tracking mysql protocol states failed",
                                                       G_STRLOC);
                                               con->state = CON_STATE_ERROR;

                                               break;
                                           }
                                       }

                                       switch (network_mysqld_write(srv, con->server)) {
                                           case NETWORK_SOCKET_SUCCESS:
                                               break;
                                           case NETWORK_SOCKET_WAIT_FOR_EVENT:
                                               wait_for_event(con->server, EV_WRITE, 0, con);
                                               NETWORK_MYSQLD_CON_TRACK_TIME(con, "wait_for_event::send_query");
                                               return;
                                           case NETWORK_SOCKET_ERROR_RETRY:
                                           case NETWORK_SOCKET_ERROR:
                                               g_debug("%s.%d: network_mysqld_write(CON_STATE_SEND_QUERY) returned an error", __FILE__, __LINE__);

                                               /**
                                                * write() failed, close the connections 
                                                */
                                               con->state = CON_STATE_ERROR;
                                               break;
                                       }

                                       if (con->state != ostate) break; /* the state has changed (e.g. CON_STATE_ERROR) */

                                       /* some statements don't have a server response */
                                       switch (con->parse.command) {
                                           case COM_STMT_SEND_LONG_DATA: /* not acked */
                                           case COM_STMT_CLOSE:
                                               con->state = CON_STATE_READ_QUERY;
                                               if (con->client) network_mysqld_queue_reset(con->client);
                                               if (con->server) network_mysqld_queue_reset(con->server);
                                               break;
                                           default:
                                               con->state = CON_STATE_READ_QUERY_RESULT;
                                               break;
                                       }

                                       break; 
            case CON_STATE_READ_QUERY_RESULT: 
                                       /* read all packets of the resultset 
                                        *
                                        * depending on the backend we may forward the data to the client right away
                                        */
                                       if (events == EV_TIMEOUT) {
                                           con->state = CON_STATE_ERROR;
                                           g_critical("%s.%d: network_mysqld_read(CON_STATE_READ_QUERY_RESULT) read timeout", __FILE__, __LINE__);
                                           break;
                                       }

                                       //g_message("%s:%d, state:READ_QUERY_RESULT", __FILE__, __LINE__);
                                       do {
                                           network_socket *recv_sock;

                                           recv_sock = con->server;

                                           g_assert(events == 0 || event_fd == recv_sock->fd);
                                           // change state
                                           switch (network_mysqld_read(srv, recv_sock)) {
                                               case NETWORK_SOCKET_SUCCESS:
                                                   break;
                                               case NETWORK_SOCKET_WAIT_FOR_EVENT:
                                                   wait_for_event(con->server, EV_READ, con->srv->router_para->read_timeout, con);
                                                   NETWORK_MYSQLD_CON_TRACK_TIME(con, "wait_for_event::read_query_result");
                                                   return;
                                               case NETWORK_SOCKET_ERROR_RETRY:
                                               case NETWORK_SOCKET_ERROR:
                                                   g_critical("%s.%d: network_mysqld_read(CON_STATE_READ_QUERY_RESULT) returned an error", __FILE__, __LINE__);
                                                   con->state = CON_STATE_ERROR;
                                                   break;
                                           }
                                           if (con->state != ostate) break; /* the state has changed (e.g. CON_STATE_ERROR) */

                                           switch (plugin_call(srv, con, con->state)) {
                                               case NETWORK_SOCKET_SUCCESS:
                                                   /* if we don't need the resultset, forward it to the client */
                                                   if (!con->resultset_is_finished && !con->resultset_is_needed) {
                                                       /* check how much data we have in the queue waiting, no need to try to send 5 bytes */
                                                       if (con->client->send_queue->len > 64 * 1024) {
                                                           con->state = CON_STATE_SEND_QUERY_RESULT;
                                                       }
                                                   }
                                                   break;
                                               case NETWORK_SOCKET_ERROR:
                                                   /* something nasty happend, let's close the connection */
                                                   con->state = CON_STATE_ERROR;
                                                   break;
                                               default:
                                                   g_critical("%s.%d: ...", __FILE__, __LINE__);
                                                   con->state = CON_STATE_ERROR;
                                                   break;
                                           }


                                       } while (con->state == CON_STATE_READ_QUERY_RESULT);

                                       break; 
            case CON_STATE_SEND_QUERY_RESULT:
                                       /**
                                        * send the query result-set to the client */

                                       //g_message("%s:%d, state:SNED_QUERY_RESULT", __FILE__, __LINE__);
                                       switch (network_mysqld_write(srv, con->client)) {
                                           case NETWORK_SOCKET_SUCCESS:
                                               break;
                                           case NETWORK_SOCKET_WAIT_FOR_EVENT:
                                               wait_for_event(con->client, EV_WRITE, 0, con);
                                               NETWORK_MYSQLD_CON_TRACK_TIME(con, "wait_for_event::send_query_result");
                                               return;
                                           case NETWORK_SOCKET_ERROR_RETRY:
                                           case NETWORK_SOCKET_ERROR:
                                               /**
                                                * client is gone away
                                                *
                                                * close the connection and clean up
                                                */
                                               con->state = CON_STATE_ERROR;
                                               break;
                                       }

                                       /* if the write failed, don't call the plugin handlers */
                                       if (con->state != ostate) break; /* the state has changed (e.g. CON_STATE_ERROR) */

                                       /* in case we havn't read the full resultset from the server yet, go back and read more
                                       */
                                       if (!con->resultset_is_finished && con->server) {
                                           con->state = CON_STATE_READ_QUERY_RESULT;
                                           break;
                                       }

                                       switch (plugin_call(srv, con, con->state)) {
                                           case NETWORK_SOCKET_SUCCESS:
                                               break;
                                           default:
                                               con->state = CON_STATE_ERROR;
                                               break;
                                       }

                                       /* special treatment for the LOAD DATA LOCAL INFILE command */
                                       if (con->state != CON_STATE_ERROR &&
                                               con->parse.command == COM_QUERY &&
                                               1 == network_mysqld_com_query_result_is_local_infile(con->parse.data)) {
                                           con->state = CON_STATE_READ_LOCAL_INFILE_DATA;
                                       }

                                       break;
            case CON_STATE_SHARDING_SEND_QUERY: 
                                       if (FALSE == sharding_send_query(event_fd, events, con)) {
                                           //g_message("%s:%d wait for in stage(CON_STATE_SHARDING_SEND_QUERY)", __FILE__, __LINE__);
                                           return; 
                                       }
                                       //g_message("%s:%d, send query sucessfully", __FILE__,__LINE__);
                                       break;

            case CON_STATE_SHARDING_READ_QUERY_RESULT: 
                                       if (events == EV_TIMEOUT) {
                                           con->state = CON_STATE_ERROR;
                                           g_critical("%s.%d: network_mysqld_read(CON_STATE_READ_QUERY_RESULT) read timeout", __FILE__, __LINE__);
                                           break;
                                       }
                                       if (FALSE == sharding_read_query_result(event_fd, events, con)) {
                                           //g_message("%s:%d wait for event in stage(CON_STATE_SHARDING_READ_QUERY_RESULT)", __FILE__, __LINE__);
                                           return;
                                       } 
                                       //g_message("%s:%d change to other  event in stage(CON_STATE_SHARDING_READ_QUERY_RESULT)", __FILE__, __LINE__);
                                       break;
            case CON_STATE_SHARDING_SEND_QUERY_RESULT:
                                       //g_message("%s:%d, state:SHARDING:SEND_QUERY_RESULT", __FILE__, __LINE__);

                                       switch (network_mysqld_write(srv, con->client)) {
                                           case NETWORK_SOCKET_SUCCESS:
                                               break;
                                           case NETWORK_SOCKET_WAIT_FOR_EVENT:
                                               wait_for_event(con->client, EV_WRITE, 0, con);
                                               NETWORK_MYSQLD_CON_TRACK_TIME(con, "wait_for_event::send_query_result");
                                               return;
                                           case NETWORK_SOCKET_ERROR_RETRY:
                                           case NETWORK_SOCKET_ERROR:
                                               /**
                                                * client is gone away
                                                *
                                                * close the connection and clean up
                                                */
                                               con->state = CON_STATE_ERROR;
                                               break;
                                       }

                                       /* if the write failed, don't call the plugin handlers */
                                       if (con->state != ostate) break; /* the state has changed (e.g. CON_STATE_ERROR) */

                                       /* in case we havn't read the full resultset from the server yet, go back and read more
                                       */
                                       //if (!con->resultset_is_finished && con->server) {
                                       //    con->state = CON_STATE_READ_QUERY_RESULT;
                                       //    break;
                                       //}

                                       switch (plugin_call(srv, con, con->state)) {
                                           case NETWORK_SOCKET_SUCCESS:
                                               break;
                                           default:
                                               con->state = CON_STATE_ERROR;
                                               break;
                                       }

                                       /* special treatment for the LOAD DATA LOCAL INFILE command */
                                       //if (con->state != CON_STATE_ERROR &&
                                       //        con->parse.command == COM_QUERY &&
                                       //        1 == network_mysqld_com_query_result_is_local_infile(con->parse.data)) {
                                       //    con->state = CON_STATE_READ_LOCAL_INFILE_DATA;
                                       //}


                                       break;
            case CON_STATE_READ_LOCAL_INFILE_DATA: {
                                                       /**
                                                        * read the file content from the client 
                                                        */
                                                       network_socket *recv_sock;

                                                       recv_sock = con->client;

                                                       /**
                                                        * LDLI is usually a whole set of packets
                                                        */
                                                       do {
                                                           switch (network_mysqld_read(srv, recv_sock)) {
                                                               case NETWORK_SOCKET_SUCCESS:
                                                                   break;
                                                               case NETWORK_SOCKET_WAIT_FOR_EVENT:
                                                                   /* call us again when you have a event */
                                                                   wait_for_event(recv_sock, EV_READ, 0, con);
                                                                   NETWORK_MYSQLD_CON_TRACK_TIME(con, "wait_for_event::read_load_infile_data");

                                                                   return;
                                                               case NETWORK_SOCKET_ERROR_RETRY:
                                                               case NETWORK_SOCKET_ERROR:
                                                                   g_critical("%s: network_mysqld_read(%s) returned an error",
                                                                           G_STRLOC,
                                                                           network_mysqld_con_state_get_name(ostate));
                                                                   con->state = CON_STATE_ERROR;
                                                                   break;
                                                           }

                                                           if (con->state != ostate) break; /* the state has changed (e.g. CON_STATE_ERROR) */

                                                           switch ((call_ret = plugin_call(srv, con, con->state))) {
                                                               case NETWORK_SOCKET_SUCCESS:
                                                                   break;
                                                               default:
                                                                   g_critical("%s: plugin_call(%s) unexpected return value: %d",
                                                                           G_STRLOC,
                                                                           network_mysqld_con_state_get_name(ostate),
                                                                           call_ret);

                                                                   con->state = CON_STATE_ERROR;
                                                                   break;
                                                           }
                                                       } while (con->state == ostate); /* read packets from the network until the plugin decodes to go to the next state */

                                                       break; }
            case CON_STATE_SEND_LOCAL_INFILE_DATA: 
                                                       /* send the hand-shake to the client and wait for a response */

                                                       switch (network_mysqld_write(srv, con->server)) {
                                                           case NETWORK_SOCKET_SUCCESS:
                                                               break;
                                                           case NETWORK_SOCKET_WAIT_FOR_EVENT:
                                                               wait_for_event(con->server, EV_WRITE, 0, con);
                                                               NETWORK_MYSQLD_CON_TRACK_TIME(con, "wait_for_event::send_load_infile_data");

                                                               return;
                                                           case NETWORK_SOCKET_ERROR_RETRY:
                                                           case NETWORK_SOCKET_ERROR:
                                                               /**
                                                                * writing failed, closing connection
                                                                */
                                                               con->state = CON_STATE_ERROR;
                                                               break;
                                                       }

                                                       if (con->state != ostate) break; /* the state has changed (e.g. CON_STATE_ERROR) */

                                                       switch ((call_ret = plugin_call(srv, con, con->state))) {
                                                           case NETWORK_SOCKET_SUCCESS:
                                                               break;
                                                           default:
                                                               g_critical("%s: plugin_call(%s) unexpected return value: %d",
                                                                       G_STRLOC,
                                                                       network_mysqld_con_state_get_name(ostate),
                                                                       call_ret);

                                                               con->state = CON_STATE_ERROR;
                                                               break;
                                                       }

                                                       break;
            case CON_STATE_READ_LOCAL_INFILE_RESULT: {
                                                         /**
                                                          * read auth data from the remote mysql-server 
                                                          */
                                                         network_socket *recv_sock;
                                                         recv_sock = con->server;
                                                         g_assert(events == 0 || event_fd == recv_sock->fd);

                                                         switch (network_mysqld_read(srv, recv_sock)) {
                                                             case NETWORK_SOCKET_SUCCESS:
                                                                 break;
                                                             case NETWORK_SOCKET_WAIT_FOR_EVENT:
                                                                 /* call us again when you have a event */
                                                                 wait_for_event(recv_sock, EV_READ, 0, con);
                                                                 NETWORK_MYSQLD_CON_TRACK_TIME(con, "wait_for_event::read_load_infile_result");

                                                                 return;
                                                             case NETWORK_SOCKET_ERROR_RETRY:
                                                             case NETWORK_SOCKET_ERROR:
                                                                 g_critical("%s: network_mysqld_read(%s) returned an error",
                                                                         G_STRLOC,
                                                                         network_mysqld_con_state_get_name(ostate));

                                                                 con->state = CON_STATE_ERROR;
                                                                 break;
                                                         }

                                                         if (con->state != ostate) break; /* the state has changed (e.g. CON_STATE_ERROR) */

                                                         switch ((call_ret = plugin_call(srv, con, con->state))) {
                                                             case NETWORK_SOCKET_SUCCESS:
                                                                 break;
                                                             default:
                                                                 g_critical("%s: plugin_call(%s) unexpected return value: %d",
                                                                         G_STRLOC,
                                                                         network_mysqld_con_state_get_name(ostate),
                                                                         call_ret);

                                                                 con->state = CON_STATE_ERROR;
                                                                 break;
                                                         }

                                                         break; }
            case CON_STATE_SEND_LOCAL_INFILE_RESULT: 
                                                         /* send the hand-shake to the client and wait for a response */

                                                         switch (network_mysqld_write(srv, con->client)) {
                                                             case NETWORK_SOCKET_SUCCESS:
                                                                 break;
                                                             case NETWORK_SOCKET_WAIT_FOR_EVENT:
                                                                 wait_for_event(con->client, EV_WRITE, 0, con);
                                                                 NETWORK_MYSQLD_CON_TRACK_TIME(con, "wait_for_event::send_load_infile_result");

                                                                 return;
                                                             case NETWORK_SOCKET_ERROR_RETRY:
                                                             case NETWORK_SOCKET_ERROR:
                                                                 /**
                                                                  * writing failed, closing connection
                                                                  */
                                                                 con->state = CON_STATE_ERROR;
                                                                 break;
                                                         }

                                                         if (con->state != ostate) break; /* the state has changed (e.g. CON_STATE_ERROR) */

                                                         switch ((call_ret = plugin_call(srv, con, con->state))) {
                                                             case NETWORK_SOCKET_SUCCESS:
                                                                 break;
                                                             default:
                                                                 g_critical("%s: plugin_call(%s) unexpected return value: %d",
                                                                         G_STRLOC,
                                                                         network_mysqld_con_state_get_name(ostate),
                                                                         call_ret);

                                                                 con->state = CON_STATE_ERROR;
                                                                 break;
                                                         }

                                                         break;
            case CON_STATE_SEND_ERROR:
                                                         /**
                                                          * send error to the client
                                                          * and close the connections afterwards
                                                          *  */
                                                         switch (network_mysqld_write(srv, con->client)) {
                                                             case NETWORK_SOCKET_SUCCESS:
                                                                 break;
                                                             case NETWORK_SOCKET_WAIT_FOR_EVENT:
                                                                 wait_for_event(con->client, EV_WRITE, 0, con);
                                                                 NETWORK_MYSQLD_CON_TRACK_TIME(con, "wait_for_event::send_error");
                                                                 return;
                                                             case NETWORK_SOCKET_ERROR_RETRY:
                                                             case NETWORK_SOCKET_ERROR:
                                                                 g_critical("%s.%d: network_mysqld_write(CON_STATE_SEND_ERROR) returned an error", __FILE__, __LINE__);

                                                                 con->state = CON_STATE_ERROR;
                                                                 break;
                                                         }

                                                         con->state = CON_STATE_ERROR;

                                                         break;
        }

        event_fd = -1;
        events   = 0;
no_reset_eventfd:
        continue;
    } while (ostate != con->state);
    NETWORK_MYSQLD_CON_TRACK_TIME(con, "con_handle_end");

    return;
}

/**
 * accept a connection
 *
 * event handler for listening connections
 *
 * @param event_fd     fd on which the event was fired
 * @param events       the event that was fired
 * @param user_data    the listening connection handle
 * 
 */
void network_mysqld_con_accept(int G_GNUC_UNUSED event_fd, short events, void *user_data) {
    network_mysqld_con *listen_con = user_data;
    network_mysqld_con *client_con;
    network_socket *client;

    g_assert(events == EV_READ);
    g_assert(listen_con->server);

    client = network_socket_accept(listen_con->server);
    if (!client) return;

    /* looks like we open a client connection */
    client_con = network_mysqld_con_new();
    client_con->client = client;

    NETWORK_MYSQLD_CON_TRACK_TIME(client_con, "accept");

    network_mysqld_add_connection(listen_con->srv, client_con);

    /**
     * inherit the config to the new connection 
     */

    client_con->plugins = listen_con->plugins;
    //	client_con->config  = listen_con->config;

    //network_mysqld_con_handle(-1, 0, client_con);
    //此处将client_con放入异步队列，然后ping工作线程，由工作线程去执行network_mysqld_con_handle，不再由主线程直接执行network_mysqld_con_handle
    chassis_event_add(client_con);
}

void network_mysqld_admin_con_accept(int G_GNUC_UNUSED event_fd, short events, void *user_data) {
    network_mysqld_con *listen_con = user_data;
    network_mysqld_con *client_con;
    network_socket *client;

    g_assert(events == EV_READ);
    g_assert(listen_con->server);

    client = network_socket_accept(listen_con->server);
    if (!client) return;

    /* looks like we open a client connection */
    client_con = network_mysqld_con_new();
    client_con->client = client;

    NETWORK_MYSQLD_CON_TRACK_TIME(client_con, "accept");

    network_mysqld_add_connection(listen_con->srv, client_con);

    /**
     * inherit the config to the new connection 
     */

    client_con->plugins = listen_con->plugins;
    client_con->config  = listen_con->config;

    network_mysqld_con_handle(-1, 0, client_con);
}


GPtrArray *get_formated_rows(GPtrArray *fields, GPtrArray *rows) {
    GPtrArray *result = g_ptr_array_sized_new(rows->len);
    gint i, j;
    for (i = 0; i < rows->len; i++) {
        GPtrArray *row = rows->pdata[i];
        GPtrArray *new_row = g_ptr_array_sized_new(fields->len); 
        for (j = 0; j < fields->len; j++) {

            if (NULL == row->pdata || NULL == row->pdata[j]) {
                gchar *p = NULL;
                g_ptr_array_add(new_row, p);
                continue;
            }

            MYSQL_FIELD *field = fields->pdata[j];
            switch (field->type) {
                case MYSQL_TYPE_SHORT:              
                case MYSQL_TYPE_TINY: 
                case MYSQL_TYPE_NEWDECIMAL:
                case MYSQL_TYPE_DECIMAL: {

                                             gchar *data = row->pdata[j];
                                             gint *v = g_new(gint, 1);     
                                             *v = atoi(data);
                                             g_ptr_array_add(new_row, v);
                                             break;
                                         } 
                case MYSQL_TYPE_FLOAT: {
                                           gchar *data = row->pdata[j];
                                           gfloat *v = g_new(gfloat, 1);     
                                           *v = atof(data);
                                           g_ptr_array_add(new_row, v);
                                           break;
                                       } 
                case MYSQL_TYPE_LONGLONG:
                case MYSQL_TYPE_LONG: {
                                          gchar *data = row->pdata[j];
                                          glong *v = g_new(glong, 1);     
                                          *v = atol(data);
                                          g_ptr_array_add(new_row, v);
                                          break;
                                      }
                default : {

                              gchar *data = row->pdata[j];
                              g_ptr_array_add(new_row, data);
                              break;
                          }                     
            }

        } 
        g_ptr_array_add(result, new_row);
    } 
    return result; 
} 


gboolean del_duplicate_rows(GPtrArray *formated_rows, GPtrArray *fields, GPtrArray *buildin_funcs, gint m, gint n) {
    if (m < 0 || m >= formated_rows->len || n < 0 || n >= formated_rows->len) {
        return FALSE;
    }

    GPtrArray *row1 = formated_rows->pdata[m]; 
    GPtrArray *row2= formated_rows->pdata[n]; 

    gint j; 
    for (j = 0; j < fields->len; j++) {
        buildin_func_t *func = g_ptr_array_index(buildin_funcs, j);        
        if (func->type == AGGREGATION_UNKNOWN) {
            continue;
        }
        MYSQL_FIELD *field = fields->pdata[j];
        switch (field->type) {
            case MYSQL_TYPE_SHORT:              
            case MYSQL_TYPE_TINY: 
            case MYSQL_TYPE_NEWDECIMAL:
            case MYSQL_TYPE_DECIMAL: { 
                                         gint *t1 = NULL; 
                                         gint *t2 = NULL; 
                                         if (NULL == row1->pdata &&  NULL == row2->pdata) {
                                             continue;
                                         } else if (NULL != row1->pdata && NULL == row2->pdata) {
                                             t1 = row1->pdata[j];          
                                         } else if (NULL == row1->pdata && NULL != row2->pdata) {
                                             t2 = row2->pdata[j];                                           
                                         } else {
                                             t1 = row1->pdata[j];
                                             t2 = row2->pdata[j];
                                         }

                                         switch (func->type) {
                                             case AGGREGATION_COUNT: 
                                             case AGGREGATION_SUM:
                                                 if (t1 != NULL && t2 != NULL) {
                                                     *t1 += *t2;
                                                 } else if (t1 == NULL && t2 != NULL) {
                                                     *t1 = *t2;
                                                 }
                                                 break;
                                             case AGGREGATION_MAX:    
                                                 if (t1 != NULL && t2 != NULL) {
                                                     *t1 = (*t1 > *t2) ? *t1 : *t2;
                                                 } else if (t1 == NULL && t2 != NULL) {
                                                     *t1 = *t2;
                                                 } 
                                                 break;
                                             case AGGREGATION_MIN:
                                                 if (t1 != NULL && t2 != NULL) {
                                                     *t1 = (*t1 > *t2) ? *t2 : *t1;
                                                 } else if (t1 == NULL && t2 != NULL) {
                                                     *t1 = *t2;
                                                 }
                                                 break;
                                             case AGGREGATION_AVG:
                                                 if (t1 != NULL && t2 != NULL) {
                                                     *t1 += *t2;
                                                 } else if (t1 == NULL && t2 != NULL) {
                                                     *t1 = *t2;
                                                 }
                                                 break;
                                             default:
                                                 break;
                                         }                
                                         g_free(t2);
                                         break;
                                     } 
            case MYSQL_TYPE_FLOAT: {
                                       gfloat *t1 = NULL; 
                                       gfloat *t2 = NULL; 
                                       if (NULL == row1->pdata &&  NULL == row2->pdata) {
                                           continue;
                                       } else if (NULL != row1->pdata && NULL == row2->pdata) {
                                           t1 = row1->pdata[j];          
                                       } else if (NULL == row1->pdata && NULL != row2->pdata) {
                                           t2 = row2->pdata[j];                                           
                                       } else {
                                           t1 = row1->pdata[j];
                                           t2 = row2->pdata[j];
                                       }

                                       switch (func->type) {
                                           case AGGREGATION_COUNT: 
                                           case AGGREGATION_SUM:
                                               if (t1 != NULL && t2 != NULL) {
                                                   *t1 += *t2;
                                               } else if (t1 == NULL && t2 != NULL) {
                                                   *t1 = *t2;
                                               }
                                               break;
                                           case AGGREGATION_MAX:    
                                               if (t1 != NULL && t2 != NULL) {
                                                   *t1 = (*t1 > *t2) ? *t1 : *t2;
                                               } else if (t1 == NULL && t2 != NULL) {
                                                   *t1 = *t2;
                                               } 
                                               break;
                                           case AGGREGATION_MIN:
                                               if (t1 != NULL && t2 != NULL) {
                                                   *t1 = (*t1 > *t2) ? *t2 : *t1;
                                               } else if (t1 == NULL && t2 != NULL) {
                                                   *t1 = *t2;
                                               }
                                               break;
                                           case AGGREGATION_AVG:
                                               if (t1 != NULL && t2 != NULL) {
                                                   *t1 += *t2;
                                               } else if (t1 == NULL && t2 != NULL) {
                                                   *t1 = *t2;
                                               }
                                               break;
                                           default:
                                               break;
                                       }                
                                       g_free(t2);
                                       break;
                                   }
            case MYSQL_TYPE_LONGLONG:
            case MYSQL_TYPE_LONG: {
                                      glong *t1 = NULL;
                                      glong *t2 = NULL;

                                      if (NULL == row1->pdata &&  NULL == row2->pdata) {
                                          continue;
                                      } else if (NULL != row1->pdata && NULL == row2->pdata) {
                                          t1 = row1->pdata[j];          
                                      } else if (NULL == row1->pdata && NULL != row2->pdata) {
                                          t2 = row2->pdata[j];                                           
                                      } else {
                                          t1 = row1->pdata[j];
                                          t2 = row2->pdata[j];
                                      }

                                      switch (func->type) {
                                          case AGGREGATION_COUNT: 
                                          case AGGREGATION_SUM:
                                              if (t1 != NULL && t2 != NULL) {
                                                  *t1 += *t2;
                                              } else if (t1 == NULL && t2 != NULL) {
                                                  *t1 = *t2;
                                              }
                                              break;
                                          case AGGREGATION_MAX:    
                                              if (t1 != NULL && t2 != NULL) {
                                                  *t1 = (*t1 > *t2) ? *t1 : *t2;
                                              } else if (t1 == NULL && t2 != NULL) {
                                                  *t1 = *t2;
                                              } 
                                              break;
                                          case AGGREGATION_MIN:
                                              if (t1 != NULL && t2 != NULL) {
                                                  *t1 = (*t1 > *t2) ? *t2 : *t1;
                                              } else if (t1 == NULL && t2 != NULL) {
                                                  *t1 = *t2;
                                              }
                                              break;
                                          case AGGREGATION_AVG:
                                              if (t1 != NULL && t2 != NULL) {
                                                  *t1 += *t2;
                                              } else if (t1 == NULL && t2 != NULL) {
                                                  *t1 = *t2;
                                              }
                                              break;
                                          default:
                                              break;
                                      }                
                                      g_free(t2);
                                      break;
                                  }
            default: {
                         break;
                     } 
        }
    }  
    g_ptr_array_remove_index(formated_rows, n);
    g_ptr_array_free(row2, TRUE);
    return TRUE;

}

guint hash_grouped_key(gconstpointer k) {
    GString *key = (GString *)k; 
    return g_str_hash(key->str);
} 
gboolean equal_grouped_key(gconstpointer a, gconstpointer b) {
    GString *key1 = (GString *)a;
    GString *key2 = (GString *)b; 
    return g_str_equal(key1->str, key2->str);
}
void free_grouped_key(gpointer k) {
    GString *key = k;
    g_string_free(key, TRUE);
}

void merge_grouped_rows(GPtrArray *formated_rows, GPtrArray *fields, GPtrArray *buildin_funcs) {
    gint i, j;
    GHashTable* grouped_row = g_hash_table_new_full(hash_grouped_key, equal_grouped_key, free_grouped_key, g_free);
    for (i = 0; i < formated_rows->len; i++) {
        GString *key = g_string_new(NULL); 
        for (j = 0; j < fields->len; j++) {
            buildin_func_t *func = g_ptr_array_index(buildin_funcs, j);        
            MYSQL_FIELD *field = fields->pdata[j];
            if (func->is_grouped_column) {
                GPtrArray *row = formated_rows->pdata[i];
                gchar *data = row->pdata[j]; 
                key = g_string_append(key, data);                
            }
        }

        gint *pos = NULL;
        if (pos = g_hash_table_lookup(grouped_row, key)) {
            if (del_duplicate_rows(formated_rows, fields, buildin_funcs, *pos, i)) {
                i--;
            } 
        } else {
            gint *p = g_new(int, 1);
            *p = i;
            g_hash_table_insert(grouped_row, key, p);
            continue;
        }
    }
    g_hash_table_destroy(grouped_row);
}

void merge_no_grouped_rows(GPtrArray *formated_rows, GPtrArray *fields, GPtrArray *buildin_funcs) {
    gint j;
    for (j = 1; j < formated_rows->len; j++) {
        if (del_duplicate_rows(formated_rows, fields, buildin_funcs, 0, j)) {
            j--; 
        }    
    }  
}

gboolean recover_formated_colunm(gchar *temp, GPtrArray *row, gint column_index, MYSQL_FIELD *field) {
    if (row->len <= column_index) {
        return TRUE;
    }
    if (NULL == row->pdata || NULL == row->pdata[column_index]) {
        return FALSE;
    }                                    

    switch (field->type) {
        case MYSQL_TYPE_SHORT:              
        case MYSQL_TYPE_TINY: 
        case MYSQL_TYPE_NEWDECIMAL:
        case MYSQL_TYPE_DECIMAL: { 
                                     gint *t1 = row->pdata[column_index];
                                     g_sprintf(temp, "%d", *t1);
                                     g_free(t1);
                                     break;
                                 } 
        case MYSQL_TYPE_FLOAT: {
                                   gfloat *t1 = row->pdata[column_index];
                                   g_sprintf(temp, "%f", *t1);
                                   g_free(t1);
                                   break;
                               }
        case MYSQL_TYPE_LONG: 
        case MYSQL_TYPE_LONGLONG: {
                                      glong *t1 = row->pdata[column_index];
                                      g_sprintf(temp, "%ld", *t1);
                                      g_free(t1);
                                      break;
                                  }
        default: {
                     gchar *t1 = row->pdata[column_index];
                     g_stpcpy(temp, t1); 
                     break;
                 } 
    }
    return TRUE;
}

int network_mysqld_con_send_merged_resultset(network_socket *con, GPtrArray *fields, GPtrArray *rows, GPtrArray *buildin_funcs) {
    GString *s;
    gsize i, j;
    gboolean is_aggregation = FALSE; 
    gboolean grouped = FALSE;
    for (j = 0; j < buildin_funcs->len; j++) {
        buildin_func_t *func = g_ptr_array_index(buildin_funcs, j);        
        if (func->is_grouped_column) {
            grouped = TRUE;
        } 
        if (func->type != AGGREGATION_UNKNOWN) {
            is_aggregation  = TRUE;
        } 
    }
    if (!is_aggregation || fields->len != buildin_funcs->len) {
        network_mysqld_con_send_resultset(con, fields, rows);
        return 0;
    }

    g_assert(fields->len > 0);

    s = g_string_new(NULL);

    network_mysqld_proto_append_lenenc_int(s, fields->len); /* the field-count */
    network_mysqld_queue_append(con, con->send_queue, S(s));

    //network_mysqld_type_t 
    for (i = 0; i < fields->len; i++) {
        MYSQL_FIELD *field = fields->pdata[i];

        g_string_truncate(s, 0);

        network_mysqld_proto_append_lenenc_string(s, field->catalog ? field->catalog : "def");   /* catalog */
        network_mysqld_proto_append_lenenc_string(s, field->db ? field->db : "");                /* database */
        network_mysqld_proto_append_lenenc_string(s, field->table ? field->table : "");          /* table */
        network_mysqld_proto_append_lenenc_string(s, field->org_table ? field->org_table : "");  /* org_table */
        network_mysqld_proto_append_lenenc_string(s, field->name ? field->name : "");            /* name */
        network_mysqld_proto_append_lenenc_string(s, field->org_name ? field->org_name : "");    /* org_name */

        g_string_append_c(s, '\x0c');                  /* length of the following block, 12 byte */
        g_string_append_len(s, "\x08\x00", 2);         /* charset */
        g_string_append_c(s, (field->length >> 0) & 0xff); /* len */
        g_string_append_c(s, (field->length >> 8) & 0xff); /* len */
        g_string_append_c(s, (field->length >> 16) & 0xff); /* len */
        g_string_append_c(s, (field->length >> 24) & 0xff); /* len */
        g_string_append_c(s, field->type);             /* type */
        g_string_append_c(s, field->flags & 0xff);     /* flags */
        g_string_append_c(s, (field->flags >> 8) & 0xff); /* flags */
        g_string_append_c(s, 0);                       /* decimals */
        g_string_append_len(s, "\x00\x00", 2);         /* filler */
        network_mysqld_queue_append(con, con->send_queue, S(s));
    }
    g_string_truncate(s, 0);
    /* EOF */	
    g_string_append_len(s, "\xfe", 1); /* EOF */
    g_string_append_len(s, "\x00\x00", 2); /* warning count */
    g_string_append_len(s, "\x02\x00", 2); /* flags */
    network_mysqld_queue_append(con, con->send_queue, S(s));

    GPtrArray *formated_rows = get_formated_rows(fields, rows); 
    if (grouped == TRUE) {
        merge_grouped_rows(formated_rows, fields, buildin_funcs); 
    } else {
        merge_no_grouped_rows(formated_rows, fields, buildin_funcs);
    }

    gchar temp[1024] = {0}; // Maybe a Bug, string too long 
    for (i = 0; i < formated_rows->len; i++) {
        GPtrArray *row = formated_rows->pdata[i]; 
        g_string_truncate(s, 0);

        for (j = 0; j < row->len; j++) {
            MYSQL_FIELD *field = fields->pdata[j];
            if (recover_formated_colunm(temp, row, j, field))  {
                network_mysqld_proto_append_lenenc_string(s, temp);
            } else {
                network_mysqld_proto_append_lenenc_string(s, NULL);
            }

        }
        g_ptr_array_free(row, TRUE);
        network_mysqld_queue_append(con, con->send_queue, S(s));
    }
    g_ptr_array_free(formated_rows, TRUE);

    g_string_truncate(s, 0);
    /* EOF */	
    g_string_append_len(s, "\xfe", 1); /* EOF */
    g_string_append_len(s, "\x00\x00", 2); /* warning count */
    g_string_append_len(s, "\x02\x00", 2); /* flags */

    network_mysqld_queue_append(con, con->send_queue, S(s));
    network_mysqld_queue_reset(con);

    g_string_free(s, TRUE);

    return 0;
} 
/**
 * @todo move to network_mysqld_proto
 */
int network_mysqld_con_send_resultset(network_socket *con, GPtrArray *fields, GPtrArray *rows) {
    GString *s;
    gsize i, j;

    g_assert(fields->len > 0);

    s = g_string_new(NULL);

    /* - len = 99
     *  \1\0\0\1 
     *    \1 - one field
     *  \'\0\0\2 
     *    \3def 
     *    \0 
     *    \0 
     *    \0 
     *    \21@@version_comment 
     *    \0            - org-name
     *    \f            - filler
     *    \10\0         - charset
     *
     *
     *
     *    \34\0\0\0     - length
     *    \375          - type 
     *    \1\0          - flags
     *    \37           - decimals
     *    \0\0          - filler 
     *  \5\0\0\3 
     *    \376\0\0\2\0
     *  \35\0\0\4
     *    \34MySQL Community Server (GPL)
     *  \5\0\0\5
     *    \376\0\0\2\0
     */

    network_mysqld_proto_append_lenenc_int(s, fields->len); /* the field-count */
    network_mysqld_queue_append(con, con->send_queue, S(s));

    for (i = 0; i < fields->len; i++) {
        MYSQL_FIELD *field = fields->pdata[i];

        g_string_truncate(s, 0);

        network_mysqld_proto_append_lenenc_string(s, field->catalog ? field->catalog : "def");   /* catalog */
        network_mysqld_proto_append_lenenc_string(s, field->db ? field->db : "");                /* database */
        network_mysqld_proto_append_lenenc_string(s, field->table ? field->table : "");          /* table */
        network_mysqld_proto_append_lenenc_string(s, field->org_table ? field->org_table : "");  /* org_table */
        network_mysqld_proto_append_lenenc_string(s, field->name ? field->name : "");            /* name */
        network_mysqld_proto_append_lenenc_string(s, field->org_name ? field->org_name : "");    /* org_name */

        g_string_append_c(s, '\x0c');                  /* length of the following block, 12 byte */
        g_string_append_len(s, "\x08\x00", 2);         /* charset */
        g_string_append_c(s, (field->length >> 0) & 0xff); /* len */
        g_string_append_c(s, (field->length >> 8) & 0xff); /* len */
        g_string_append_c(s, (field->length >> 16) & 0xff); /* len */
        g_string_append_c(s, (field->length >> 24) & 0xff); /* len */
        g_string_append_c(s, field->type);             /* type */
        g_string_append_c(s, field->flags & 0xff);     /* flags */
        g_string_append_c(s, (field->flags >> 8) & 0xff); /* flags */
        g_string_append_c(s, 0);                       /* decimals */
        g_string_append_len(s, "\x00\x00", 2);         /* filler */
#if 0
        /* this is in the docs, but not on the network */
        network_mysqld_proto_append_lenenc_string(s, field->def);         /* default-value */
#endif
        network_mysqld_queue_append(con, con->send_queue, S(s));
    }

    g_string_truncate(s, 0);

    /* EOF */	
    g_string_append_len(s, "\xfe", 1); /* EOF */
    g_string_append_len(s, "\x00\x00", 2); /* warning count */
    g_string_append_len(s, "\x02\x00", 2); /* flags */

    network_mysqld_queue_append(con, con->send_queue, S(s));

    for (i = 0; i < rows->len; i++) {
        GPtrArray *row = rows->pdata[i];

        g_string_truncate(s, 0);
        for (j = 0; j < row->len; j++) {
            network_mysqld_proto_append_lenenc_string(s, row->pdata[j]);
        }

        network_mysqld_queue_append(con, con->send_queue, S(s));
    }

    g_string_truncate(s, 0);

    /* EOF */	
    g_string_append_len(s, "\xfe", 1); /* EOF */
    g_string_append_len(s, "\x00\x00", 2); /* warning count */
    g_string_append_len(s, "\x02\x00", 2); /* flags */

    network_mysqld_queue_append(con, con->send_queue, S(s));
    network_mysqld_queue_reset(con);

    g_string_free(s, TRUE);

    return 0;
}


