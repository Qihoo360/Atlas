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

static GMutex con_mutex;

/**
 * call the cleanup callback for the current connection
 *
 * @param srv    global context
 * @param con    connection context
 *
 * @return       NETWORK_SOCKET_SUCCESS on success
 */
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

chassis_private *network_mysqld_priv_init(guint event_thread_count) {
	chassis_private *priv;

	priv = g_new0(chassis_private, 1);

	priv->cons = g_ptr_array_new();
	priv->sc = lua_scope_new();
	priv->backends = network_backends_new(event_thread_count);

	return priv;
}

void network_mysqld_priv_shutdown(chassis *chas, chassis_private *priv) {
	if (!priv) return;

	/* network_mysqld_con_free() changes the priv->cons directly
	 *
	 * always free the first element until all are gone 
	 */
	while (0 != priv->cons->len) {
		network_mysqld_con *con = priv->cons->pdata[0];

		plugin_call_cleanup(chas, con);
		network_mysqld_con_free(con);
	}
}

void network_mysqld_priv_free(chassis G_GNUC_UNUSED *chas, chassis_private *priv) {
	if (!priv) return;

	g_ptr_array_free(priv->cons, TRUE);

	network_backends_free(priv->backends);

	lua_scope_free(priv->sc);

	g_free(priv);
}

int network_mysqld_init(chassis *srv) {
	lua_State *L;
	srv->priv_free = network_mysqld_priv_free;
	srv->priv_shutdown = network_mysqld_priv_shutdown;
	srv->priv      = network_mysqld_priv_init(srv->event_thread_count);

	/* store the pointer to the chassis in the Lua registry */
	L = srv->priv->sc->L;
	lua_pushlightuserdata(L, (void*)srv);
	lua_setfield(L, LUA_REGISTRYINDEX, CHASSIS_LUA_REGISTRY_KEY);
	
	return 0;
}

/**
 * create a connection 
 *
 * @return       a connection context
 */
network_mysqld_con *network_mysqld_con_new() {
	network_mysqld_con *con;

	con = g_new0(network_mysqld_con, 1);
	con->parse.command = -1;

	con->is_in_transaction = con->is_in_select_calc_found_rows = con->is_insert_id = con->is_not_autocommit = FALSE;

	con->charset_client     = g_string_new(NULL);
	con->charset_results    = g_string_new(NULL);
	con->charset_connection = g_string_new(NULL);

	con->locks = g_hash_table_new_full(g_str_hash, g_str_equal, g_free, NULL);

	con->merge_res = g_new(merge_res_t, 1);
	con->merge_res->rows = g_ptr_array_new();

	return con;
}

void network_mysqld_add_connection(chassis *srv, network_mysqld_con *con) {
	con->srv = srv;
	g_mutex_lock(&con_mutex);
	g_ptr_array_add(srv->priv->cons, con);
	g_mutex_unlock(&con_mutex);
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
	g_mutex_lock(&con_mutex);
	g_ptr_array_remove_fast(con->srv->priv->cons, con);
	g_mutex_unlock(&con_mutex);

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

	g_free(con);
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
				g_error("%s.%d: neither nor", __FILE__, __LINE__);
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

#define WAIT_FOR_EVENT(ev_struct, ev_type, timeout) \
	event_set(&(ev_struct->event), ev_struct->fd, ev_type, network_mysqld_con_handle, user_data); \
	chassis_event_add_self(srv, &(ev_struct->event), timeout);

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
				if (st->backend->state != BACKEND_STATE_OFFLINE) st->backend->state = BACKEND_STATE_DOWN;
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
					WAIT_FOR_EVENT(con->server, EV_WRITE, 5);
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
				WAIT_FOR_EVENT(con->server, EV_READ, 0);
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
				WAIT_FOR_EVENT(con->client, EV_WRITE, 0);
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
			network_socket *recv_sock;

			recv_sock = con->client;

			g_assert(events == 0 || event_fd == recv_sock->fd);

			switch (network_mysqld_read(srv, recv_sock)) {
			case NETWORK_SOCKET_SUCCESS:
				break;
			case NETWORK_SOCKET_WAIT_FOR_EVENT:
				WAIT_FOR_EVENT(con->client, EV_READ, 0);
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
				WAIT_FOR_EVENT(con->server, EV_WRITE, 0);
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
				WAIT_FOR_EVENT(con->server, EV_READ, 0);
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
				WAIT_FOR_EVENT(con->client, EV_WRITE, 0);
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
				WAIT_FOR_EVENT(con->client, EV_READ, 0);
				NETWORK_MYSQLD_CON_TRACK_TIME(con, "wait_for_event::read_auth_old_password");

				return;
			case NETWORK_SOCKET_ERROR_RETRY:
			case NETWORK_SOCKET_ERROR:
				g_critical("%s.%d: network_mysqld_read(CON_STATE_READ_AUTH_OLD_PASSWORD) returned an error", __FILE__, __LINE__);
				con->state = CON_STATE_ERROR;
				return;
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
				WAIT_FOR_EVENT(con->server, EV_WRITE, 0);
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
			network_socket *recv_sock;
			network_packet last_packet;

			recv_sock = con->client;

			g_assert(events == 0 || event_fd == recv_sock->fd);

			do { 
				switch (network_mysqld_read(srv, recv_sock)) {
				case NETWORK_SOCKET_SUCCESS:
					break;
				case NETWORK_SOCKET_WAIT_FOR_EVENT:
					WAIT_FOR_EVENT(con->client, EV_READ, 0);
					NETWORK_MYSQLD_CON_TRACK_TIME(con, "wait_for_event::read_query");
					return;
				case NETWORK_SOCKET_ERROR_RETRY:
				case NETWORK_SOCKET_ERROR:
					g_critical("%s.%d: network_mysqld_read(CON_STATE_READ_QUERY) returned an error", __FILE__, __LINE__);
					con->state = CON_STATE_ERROR;
					return;
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

				if (0 == network_mysqld_proto_skip_network_header(&packet) &&
				    0 == network_mysqld_proto_get_int8(&packet, &com)  &&
				    com == COM_CHANGE_USER) {
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
			if (con->state == CON_STATE_SEND_QUERY) {
				network_mysqld_con_reset_command_response_state(con);
			}

			break; }
		case CON_STATE_SEND_QUERY:
			/* send the query to the server
			 *
			 * this state will loop until all the packets from the send-queue are flushed 
			 */

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
				WAIT_FOR_EVENT(con->server, EV_WRITE, 0);
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
			do {
				network_socket *recv_sock;

				recv_sock = con->server;

				g_assert(events == 0 || event_fd == recv_sock->fd);

				switch (network_mysqld_read(srv, recv_sock)) {
				case NETWORK_SOCKET_SUCCESS:
					break;
				case NETWORK_SOCKET_WAIT_FOR_EVENT:
					WAIT_FOR_EVENT(con->server, EV_READ, 0);
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
			switch (network_mysqld_write(srv, con->client)) {
			case NETWORK_SOCKET_SUCCESS:
				break;
			case NETWORK_SOCKET_WAIT_FOR_EVENT:
				WAIT_FOR_EVENT(con->client, EV_WRITE, 0);
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
					WAIT_FOR_EVENT(recv_sock, EV_READ, 0);
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
				WAIT_FOR_EVENT(con->server, EV_WRITE, 0);
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
				WAIT_FOR_EVENT(recv_sock, EV_READ, 0);
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
				WAIT_FOR_EVENT(con->client, EV_WRITE, 0);
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
				WAIT_FOR_EVENT(con->client, EV_WRITE, 0);
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
	client_con->config  = listen_con->config;

	//network_mysqld_con_handle(-1, 0, client_con);
	//此处将client_con放入异步队列，然后ping工作线程，由工作线程去执行network_mysqld_con_handle，不再由主线程直接执行network_mysqld_con_handle
	chassis_event_add(client_con);

	return;
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


