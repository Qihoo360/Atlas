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

#ifndef _WIN32
#include <sys/ioctl.h>
#include <sys/socket.h>
#include <sys/uio.h> /* writev */

#ifdef HAVE_SYS_TYPES_H
#include <sys/types.h>
#endif

#ifdef HAVE_SYS_FILIO_H
/**
 * required for FIONREAD on solaris
 */
#include <sys/filio.h>
#endif


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
#include <errno.h>
#include <fcntl.h>

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

#include "network-debug.h"
#include "network-socket.h"
#include "network-mysqld-proto.h"
#include "network-mysqld-packet.h"
#include "string-len.h"
#include "glib-ext.h"

network_socket *network_socket_new() {
	network_socket *s;
	
	s = g_new0(network_socket, 1);

	s->send_queue = network_queue_new();
	s->recv_queue = network_queue_new();
	s->recv_queue_raw = network_queue_new();

	s->default_db = g_string_new(NULL);
	s->charset_client = g_string_new(NULL);
	s->charset_connection = g_string_new(NULL);
	s->charset_results = g_string_new(NULL);



	s->fd           = -1;
	s->socket_type  = SOCK_STREAM; /* let's default to TCP */
	s->packet_id_is_reset = TRUE;

	s->src = network_address_new();
	s->dst = network_address_new();

	return s;
}

void network_socket_free(network_socket *s) {
	if (!s) return;

	network_queue_free(s->send_queue);
	network_queue_free(s->recv_queue);
	network_queue_free(s->recv_queue_raw);

	if (s->response) network_mysqld_auth_response_free(s->response);
	if (s->challenge) network_mysqld_auth_challenge_free(s->challenge);

	network_address_free(s->dst);
	network_address_free(s->src);

	event_del(&(s->event));

	if (s->fd != -1) {
		closesocket(s->fd);
	}

	g_string_free(s->default_db, TRUE);
	g_string_free(s->charset_client, TRUE);
	g_string_free(s->charset_connection, TRUE);
	g_string_free(s->charset_results, TRUE);

	g_free(s);
}

/**
 * portable 'set non-blocking io'
 *
 * @param sock    a socket
 * @return        NETWORK_SOCKET_SUCCESS on success, NETWORK_SOCKET_ERROR on error
 */
network_socket_retval_t network_socket_set_non_blocking(network_socket *sock) {
	int ret;
#ifdef _WIN32
	int ioctlvar;

	ioctlvar = 1;
	ret = ioctlsocket(sock->fd, FIONBIO, &ioctlvar);
#else
	ret = fcntl(sock->fd, F_SETFL, O_NONBLOCK | O_RDWR);
#endif
	if (ret != 0) {
#ifdef _WIN32
		errno = WSAGetLastError();
#endif
		g_critical("%s.%d: set_non_blocking() failed: %s (%d)", 
				__FILE__, __LINE__,
				g_strerror(errno), errno);
		return NETWORK_SOCKET_ERROR;
	}
	return NETWORK_SOCKET_SUCCESS;
}

/**
 * accept a connection
 *
 * event handler for listening connections
 *
 * @param srv    a listening socket 
 * 
 */
network_socket *network_socket_accept(network_socket *srv) {
	network_socket *client;

	g_return_val_if_fail(srv, NULL);
	g_return_val_if_fail(srv->socket_type == SOCK_STREAM, NULL); /* accept() only works on stream sockets */

	client = network_socket_new();

#if LINUX_VERSION_CODE >= KERNEL_VERSION(2, 6, 28)
	if (-1 == (client->fd = accept4(srv->fd, &client->src->addr.common, &(client->src->len), SOCK_NONBLOCK))) {
		network_socket_free(client);

		return NULL;
	}
#else
	if (-1 == (client->fd = accept(srv->fd, &client->src->addr.common, &(client->src->len)))) {
		network_socket_free(client);

		return NULL;
	}
	network_socket_set_non_blocking(client);
#endif

	if (network_address_refresh_name(client->src)) {
		network_socket_free(client);
		return NULL;
	}

	/* the listening side may be INADDR_ANY, let's get which address the client really connected to */
	if (-1 == getsockname(client->fd, &client->dst->addr.common, &(client->dst->len))) {
		network_address_reset(client->dst);
	} else if (network_address_refresh_name(client->dst)) {
		network_address_reset(client->dst);
	}

	return client;
}

network_socket_retval_t network_socket_connect_setopts(network_socket *sock) {
#ifdef WIN32
	char val = 1;	/* Win32 setsockopt wants a const char* instead of the UNIX void*...*/
#else
	int val = 1;
#endif
	/**
	 * set the same options as the mysql client 
	 */
#ifdef IP_TOS
	val = 8;
	setsockopt(sock->fd, IPPROTO_IP,     IP_TOS, &val, sizeof(val));
#endif
	val = 1;
	setsockopt(sock->fd, IPPROTO_TCP,    TCP_NODELAY, &val, sizeof(val) );
	val = 1;
	setsockopt(sock->fd, SOL_SOCKET, SO_KEEPALIVE, &val, sizeof(val) );

	/* the listening side may be INADDR_ANY, let's get which address the client really connected to */
	if (-1 == getsockname(sock->fd, &sock->src->addr.common, &(sock->src->len))) {
		g_debug("%s: getsockname() failed: %s (%d)",
				G_STRLOC,
				g_strerror(errno),
				errno);
		network_address_reset(sock->src);
	} else if (network_address_refresh_name(sock->src)) {
		g_debug("%s: network_address_refresh_name() failed",
				G_STRLOC);
		network_address_reset(sock->src);
	}

	return NETWORK_SOCKET_SUCCESS;
}

/**
 * finish the non-blocking connect()
 *
 * sets 'errno' as if connect() would have failed
 *
 */
network_socket_retval_t network_socket_connect_finish(network_socket *sock) {
	int so_error = 0;
	network_socklen_t so_error_len = sizeof(so_error);

	/**
	 * we might get called a 2nd time after a connect() == EINPROGRESS
	 */
#ifdef _WIN32
	/* need to cast to get rid of the compiler warning. otherwise identical to the UNIX version below. */
	if (getsockopt(sock->fd, SOL_SOCKET, SO_ERROR, (char*)&so_error, &so_error_len)) {
		errno = WSAGetLastError();
#else
	if (getsockopt(sock->fd, SOL_SOCKET, SO_ERROR, &so_error, &so_error_len)) {
#endif
		/* getsockopt failed */
		g_critical("%s: getsockopt(%s) failed: %s (%d)", 
				G_STRLOC,
				sock->dst->name->str, g_strerror(errno), errno);
		return NETWORK_SOCKET_ERROR;
	}

	switch (so_error) {
	case 0:
		network_socket_connect_setopts(sock);

		return NETWORK_SOCKET_SUCCESS;
	default:
		errno = so_error;

		return NETWORK_SOCKET_ERROR_RETRY;
	}
}

/**
 * connect a socket
 *
 * the sock->addr has to be set before 
 * 
 * @param sock    a socket 
 * @return        NETWORK_SOCKET_SUCCESS on connected, NETWORK_SOCKET_ERROR on error, NETWORK_SOCKET_ERROR_RETRY for try again
 * @see network_address_set_address()
 */
network_socket_retval_t network_socket_connect(network_socket *sock) {
	g_return_val_if_fail(sock->dst, NETWORK_SOCKET_ERROR); /* our _new() allocated it already */
	g_return_val_if_fail(sock->dst->name->len, NETWORK_SOCKET_ERROR); /* we want to use the ->name in the error-msgs */
	g_return_val_if_fail(sock->fd < 0, NETWORK_SOCKET_ERROR); /* we already have a valid fd, we don't want to leak it */
	g_return_val_if_fail(sock->socket_type == SOCK_STREAM, NETWORK_SOCKET_ERROR);

	/**
	 * create a socket for the requested address
	 *
	 * if the dst->addr isn't set yet, socket() will fail with unsupported type
	 */
	if (-1 == (sock->fd = socket(sock->dst->addr.common.sa_family, sock->socket_type, 0))) {
#ifdef _WIN32
		errno = WSAGetLastError();
#endif
		g_critical("%s.%d: socket(%s) failed: %s (%d)", 
				__FILE__, __LINE__,
				sock->dst->name->str, g_strerror(errno), errno);
		return NETWORK_SOCKET_ERROR;
	}

	/**
	 * make the connect() call non-blocking
	 *
	 */
	network_socket_set_non_blocking(sock);

	if (-1 == connect(sock->fd, &sock->dst->addr.common, sock->dst->len)) {
#ifdef _WIN32
		errno = WSAGetLastError();
#endif
		/**
		 * in most TCP cases we connect() will return with 
		 * EINPROGRESS ... 3-way handshake
		 */
		switch (errno) {
		case E_NET_INPROGRESS:
		case E_NET_WOULDBLOCK: /* win32 uses WSAEWOULDBLOCK */
			return NETWORK_SOCKET_ERROR_RETRY;
		default:
			g_critical("%s.%d: connect(%s) failed: %s (%d)", 
					__FILE__, __LINE__,
					sock->dst->name->str,
					g_strerror(errno), errno);
			return NETWORK_SOCKET_ERROR;
		}
	}

	network_socket_connect_setopts(sock);

	return NETWORK_SOCKET_SUCCESS;
}

/**
 * connect a socket
 *
 * the con->dst->addr has to be set before 
 * 
 * @param con    a socket 
 * @return       NETWORK_SOCKET_SUCCESS on connected, NETWORK_SOCKET_ERROR on error
 *
 * @see network_address_set_address()
 */
network_socket_retval_t network_socket_bind(network_socket * con) {
#ifdef WIN32
	char val = 1;	/* Win32 setsockopt wants a const char* instead of the UNIX void*...*/
#else
	int val = 1;
#endif
	g_return_val_if_fail(con->fd < 0, NETWORK_SOCKET_ERROR); /* socket is already bound */
	g_return_val_if_fail((con->socket_type == SOCK_DGRAM) || (con->socket_type == SOCK_STREAM), NETWORK_SOCKET_ERROR);

	if (con->socket_type == SOCK_STREAM) {
		g_return_val_if_fail(con->dst, NETWORK_SOCKET_ERROR);
		g_return_val_if_fail(con->dst->name->len > 0, NETWORK_SOCKET_ERROR);

		if (-1 == (con->fd = socket(con->dst->addr.common.sa_family, con->socket_type, 0))) {
			g_critical("%s: socket(%s) failed: %s (%d)", 
					G_STRLOC,
					con->dst->name->str,
					g_strerror(errno), errno);
			return NETWORK_SOCKET_ERROR;
		}

		if (con->dst->addr.common.sa_family == AF_INET || 
		    con->dst->addr.common.sa_family == AF_INET6) {
			if (0 != setsockopt(con->fd, IPPROTO_TCP, TCP_NODELAY, &val, sizeof(val))) {
				g_critical("%s: setsockopt(%s, IPPROTO_TCP, TCP_NODELAY) failed: %s (%d)", 
						G_STRLOC,
						con->dst->name->str,
						g_strerror(errno), errno);
				return NETWORK_SOCKET_ERROR;
			}
			
			if (0 != setsockopt(con->fd, SOL_SOCKET, SO_REUSEADDR, &val, sizeof(val))) {
				g_critical("%s: setsockopt(%s, SOL_SOCKET, SO_REUSEADDR) failed: %s (%d)", 
						G_STRLOC,
						con->dst->name->str,
						g_strerror(errno), errno);
				return NETWORK_SOCKET_ERROR;
			}
		}

		if (-1 == bind(con->fd, &con->dst->addr.common, con->dst->len)) {
			g_critical("%s: bind(%s) failed: %s (%d)", 
					G_STRLOC,
					con->dst->name->str,
					g_strerror(errno), errno);
			return NETWORK_SOCKET_ERROR;
		}

		if (-1 == listen(con->fd, 128)) {
			g_critical("%s: listen(%s, 128) failed: %s (%d)",
					G_STRLOC,
					con->dst->name->str,
					g_strerror(errno), errno);
			return NETWORK_SOCKET_ERROR;
		}
	} else {
		/* UDP sockets bind the ->src address */
		g_return_val_if_fail(con->src, NETWORK_SOCKET_ERROR);
		g_return_val_if_fail(con->src->name->len > 0, NETWORK_SOCKET_ERROR);

		if (-1 == (con->fd = socket(con->src->addr.common.sa_family, con->socket_type, 0))) {
			g_critical("%s: socket(%s) failed: %s (%d)", 
					G_STRLOC,
					con->src->name->str,
					g_strerror(errno), errno);
			return NETWORK_SOCKET_ERROR;
		}

		if (-1 == bind(con->fd, &con->src->addr.common, con->src->len)) {
			g_critical("%s: bind(%s) failed: %s (%d)", 
					G_STRLOC,
					con->src->name->str,
					g_strerror(errno), errno);
			return NETWORK_SOCKET_ERROR;
		}
	}

	con->dst->can_unlink_socket = TRUE;
	return NETWORK_SOCKET_SUCCESS;
}

/**
 * read a data from the socket
 *
 * @param sock the socket
 */
network_socket_retval_t network_socket_read(network_socket *sock) {
	gssize len;

	if (sock->to_read > 0) {
		GString *packet = g_string_sized_new(sock->to_read);

		g_queue_push_tail(sock->recv_queue_raw->chunks, packet);

		if (sock->socket_type == SOCK_STREAM) {
			len = recv(sock->fd, packet->str, sock->to_read, 0);
		} else {
			/* UDP */
			network_socklen_t dst_len = sizeof(sock->dst->addr.common);
			len = recvfrom(sock->fd, packet->str, sock->to_read, 0, &(sock->dst->addr.common), &(dst_len));
			sock->dst->len = dst_len;
		}
		if (-1 == len) {
#ifdef _WIN32
			errno = WSAGetLastError();
#endif
			switch (errno) {
			case E_NET_CONNABORTED:
			case E_NET_CONNRESET: /** nothing to read, let's let ioctl() handle the close for us */
			case E_NET_WOULDBLOCK: /** the buffers are empty, try again later */
			case EAGAIN:     
				return NETWORK_SOCKET_WAIT_FOR_EVENT;
			default:
				g_debug("%s: recv() failed: %s (errno=%d)", G_STRLOC, g_strerror(errno), errno);
				return NETWORK_SOCKET_ERROR;
			}
		} else if (len == 0) {
			/**
			 * connection close
			 *
			 * let's call the ioctl() and let it handle it for use
			 */
			return NETWORK_SOCKET_WAIT_FOR_EVENT;
		}

		sock->to_read -= len;
		sock->recv_queue_raw->len += len;
#if 0
		sock->recv_queue_raw->offset = 0; /* offset into the first packet */
#endif
		packet->len = len;
	}

	return NETWORK_SOCKET_SUCCESS;
}

#ifdef HAVE_WRITEV
/**
 * write data to the socket
 *
 */
static network_socket_retval_t network_socket_write_writev(network_socket *con, int send_chunks) {
	/* send the whole queue */
	GList *chunk;
	struct iovec *iov;
	gint chunk_id;
	gint chunk_count;
	gssize len;
	int os_errno;
	gint max_chunk_count;

	if (send_chunks == 0) return NETWORK_SOCKET_SUCCESS;

	chunk_count = send_chunks > 0 ? send_chunks : (gint)con->send_queue->chunks->length;
	
	if (chunk_count == 0) return NETWORK_SOCKET_SUCCESS;

	max_chunk_count = sysconf(_SC_IOV_MAX);

	if (max_chunk_count < 0) { /* option is unknown */
#if defined(UIO_MAXIOV)
		max_chunk_count = UIO_MAXIOV; /* as defined in POSIX */
#elif defined(IOV_MAX)
		max_chunk_count = IOV_MAX; /* on older Linux'es */
#else
		g_assert_not_reached(); /* make sure we provide a work-around in case sysconf() fails on us */
#endif
	}

	chunk_count = chunk_count > max_chunk_count ? max_chunk_count : chunk_count;

	g_assert_cmpint(chunk_count, >, 0); /* make sure it is never negative */

	iov = g_new0(struct iovec, chunk_count);

	for (chunk = con->send_queue->chunks->head, chunk_id = 0; 
	     chunk && chunk_id < chunk_count; 
	     chunk_id++, chunk = chunk->next) {
		GString *s = chunk->data;
	
		if (chunk_id == 0) {
			g_assert(con->send_queue->offset < s->len);

			iov[chunk_id].iov_base = s->str + con->send_queue->offset;
			iov[chunk_id].iov_len  = s->len - con->send_queue->offset;
		} else {
			iov[chunk_id].iov_base = s->str;
			iov[chunk_id].iov_len  = s->len;
		}
	}

	len = writev(con->fd, iov, chunk_count);
	os_errno = errno;

	g_free(iov);

	if (-1 == len) {
		switch (os_errno) {
		case E_NET_WOULDBLOCK:
		case EAGAIN:
			return NETWORK_SOCKET_WAIT_FOR_EVENT;
		case EPIPE:
		case E_NET_CONNRESET:
		case E_NET_CONNABORTED:
			/** remote side closed the connection */
			return NETWORK_SOCKET_ERROR;
		default:
			g_message("%s.%d: writev(%s, ...) failed: %s", 
					__FILE__, __LINE__, 
					con->dst->name->str, 
					g_strerror(errno));
			return NETWORK_SOCKET_ERROR;
		}
	} else if (len == 0) {
		return NETWORK_SOCKET_ERROR;
	}

	con->send_queue->offset += len;
	con->send_queue->len    -= len;

	/* check all the chunks which we have sent out */
	for (chunk = con->send_queue->chunks->head; chunk; ) {
		GString *s = chunk->data;

		if (con->send_queue->offset >= s->len) {
			con->send_queue->offset -= s->len;
#ifdef NETWORK_DEBUG_TRACE_IO
			/* to trace the data we sent to the socket, enable this */
			g_debug_hexdump(G_STRLOC, S(s));
#endif
			g_string_free(s, TRUE);
			
			g_queue_delete_link(con->send_queue->chunks, chunk);

			chunk = con->send_queue->chunks->head;
		} else {
			return NETWORK_SOCKET_WAIT_FOR_EVENT;
		}
	}

	return NETWORK_SOCKET_SUCCESS;
}
#endif

/**
 * write data to the socket
 *
 * use a loop over send() to be compatible with win32
 */
static network_socket_retval_t network_socket_write_send(network_socket *con, int send_chunks) {
	/* send the whole queue */
	GList *chunk;

	if (send_chunks == 0) return NETWORK_SOCKET_SUCCESS;

	for (chunk = con->send_queue->chunks->head; chunk; ) {
		GString *s = chunk->data;
		gssize len;

		g_assert(con->send_queue->offset < s->len);

		if (con->socket_type == SOCK_STREAM) {
			len = send(con->fd, s->str + con->send_queue->offset, s->len - con->send_queue->offset, 0);
		} else {
			len = sendto(con->fd, s->str + con->send_queue->offset, s->len - con->send_queue->offset, 0, &(con->dst->addr.common), con->dst->len);
		}
		if (-1 == len) {
#ifdef _WIN32
			errno = WSAGetLastError();
#endif
			switch (errno) {
			case E_NET_WOULDBLOCK:
			case EAGAIN:
				return NETWORK_SOCKET_WAIT_FOR_EVENT;
			case EPIPE:
			case E_NET_CONNRESET:
			case E_NET_CONNABORTED:
				/** remote side closed the connection */
				return NETWORK_SOCKET_ERROR;
			default:
				g_message("%s: send(%s, %"G_GSIZE_FORMAT") failed: %s", 
						G_STRLOC, 
						con->dst->name->str, 
						s->len - con->send_queue->offset, 
						g_strerror(errno));
				return NETWORK_SOCKET_ERROR;
			}
		} else if (len == 0) {
			return NETWORK_SOCKET_ERROR;
		}

		con->send_queue->offset += len;

		if (con->send_queue->offset == s->len) {
			g_string_free(s, TRUE);
			
			g_queue_delete_link(con->send_queue->chunks, chunk);
			con->send_queue->offset = 0;

			if (send_chunks > 0 && --send_chunks == 0) break;

			chunk = con->send_queue->chunks->head;
		} else {
			return NETWORK_SOCKET_WAIT_FOR_EVENT;
		}
	}

	return NETWORK_SOCKET_SUCCESS;
}

/**
 * write a content of con->send_queue to the socket
 *
 * @param con         socket to read from
 * @param send_chunks number of chunks to send, if < 0 send all
 *
 * @returns NETWORK_SOCKET_SUCCESS on success, NETWORK_SOCKET_ERROR on error and NETWORK_SOCKET_WAIT_FOR_EVENT if the call would have blocked 
 */
network_socket_retval_t network_socket_write(network_socket *con, int send_chunks) {
	if (con->socket_type == SOCK_STREAM) {
#ifdef HAVE_WRITEV
		return network_socket_write_writev(con, send_chunks);
#else
		return network_socket_write_send(con, send_chunks);
#endif
	} else {
		return network_socket_write_send(con, send_chunks);
	}
}

network_socket_retval_t network_socket_to_read(network_socket *sock) {
	int b = -1;

#ifdef SO_NREAD
	/* on MacOS X ioctl(..., FIONREAD) returns _more_ than what we have in the queue */
	if (sock->socket_type == SOCK_DGRAM) {
		network_socklen_t b_len = sizeof(b);

		if (0 != getsockopt(sock->fd, SOL_SOCKET, SO_NREAD, &b, &b_len)) {
			g_critical("%s: getsockopt(%d, SO_NREAD, ...) failed: %s (%d)",
					G_STRLOC,
					sock->fd,
					g_strerror(errno), errno);
			return NETWORK_SOCKET_ERROR;
		} else if (b < 0) {
			g_critical("%s: getsockopt(%d, SO_NREAD, ...) succeeded, but is negative: %d",
					G_STRLOC,
					sock->fd,
					b);

			return NETWORK_SOCKET_ERROR;
		} else {
			sock->to_read = b;
			return NETWORK_SOCKET_SUCCESS;
		}
	}
#endif

	if (0 != ioctl(sock->fd, FIONREAD, &b)) {
		g_critical("%s: ioctl(%d, FIONREAD, ...) failed: %s (%d)",
				G_STRLOC,
				sock->fd,
				g_strerror(errno), errno);
		return NETWORK_SOCKET_ERROR;
	} else if (b < 0) {
		g_critical("%s: ioctl(%d, FIONREAD, ...) succeeded, but is negative: %d",
				G_STRLOC,
				sock->fd,
				b);

		return NETWORK_SOCKET_ERROR;
	} else {
		sock->to_read = b;
		return NETWORK_SOCKET_SUCCESS;
	}

}
