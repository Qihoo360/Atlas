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

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#ifndef _WIN32
#include <signal.h>
#endif

#ifndef WIN32
#include <unistd.h>
#include <stdlib.h>
#include <fcntl.h>
#endif /* WIN32 */

#include <glib.h>
#include <glib/gstdio.h> /* for g_unlink */

#include "network-socket.h"

#if GLIB_CHECK_VERSION(2, 16, 0)
#define C(x) x, sizeof(x) - 1

#ifndef WIN32
#define LOCAL_SOCK "/tmp/mysql-proxy-test.socket"

typedef struct {
	char	sockname[sizeof(LOCAL_SOCK) + 10];
} local_unix_t;

static local_unix_t	*pp = NULL;
static local_unix_t local_test_arg;
#endif /* WIN32 */

void test_network_socket_new() {
	network_socket *sock;

	sock = network_socket_new();
	g_assert(sock);

	network_socket_free(sock);
}

void test_network_queue_append() {
	network_queue *q;

	q = network_queue_new();
	g_assert(q);

	network_queue_append(q, g_string_new("123"));
	network_queue_append(q, g_string_new("345"));

	network_queue_free(q);
}

void test_network_queue_peek_string() {
	network_queue *q;
	GString *s;

	q = network_queue_new();
	g_assert(q);

	network_queue_append(q, g_string_new("123"));
	g_assert_cmpint(q->len, ==, 3);
	network_queue_append(q, g_string_new("456"));
	g_assert_cmpint(q->len, ==, 6);

	s = network_queue_peek_string(q, 3, NULL);
	g_assert(s);
	g_assert_cmpint(s->len, ==, 3);
	g_assert_cmpstr(s->str, ==, "123");
	g_string_free(s, TRUE);

	s = network_queue_peek_string(q, 4, NULL);
	g_assert(s);
	g_assert_cmpint(s->len, ==, 4);
	g_assert_cmpstr(s->str, ==, "1234");
	g_string_free(s, TRUE);

	s = network_queue_peek_string(q, 7, NULL);
	g_assert(s == NULL);
	
	g_assert_cmpint(q->len, ==, 6);

	network_queue_free(q);
}

void test_network_queue_pop_string() {
	network_queue *q;
	GString *s;

	q = network_queue_new();
	g_assert(q);

	network_queue_append(q, g_string_new("123"));
	g_assert_cmpint(q->len, ==, 3);
	network_queue_append(q, g_string_new("456"));
	g_assert_cmpint(q->len, ==, 6);
	network_queue_append(q, g_string_new("789"));
	g_assert_cmpint(q->len, ==, 9);

	s = network_queue_pop_string(q, 3, NULL);
	g_assert(s);
	g_assert_cmpint(s->len, ==, 3);
	g_assert_cmpstr(s->str, ==, "123");
	g_string_free(s, TRUE);
	
	g_assert_cmpint(q->len, ==, 6);

	s = network_queue_pop_string(q, 4, NULL);
	g_assert(s);
	g_assert_cmpint(s->len, ==, 4);
	g_assert_cmpstr(s->str, ==, "4567");
	g_string_free(s, TRUE);
	g_assert_cmpint(q->len, ==, 2);

	s = network_queue_pop_string(q, 7, NULL);
	g_assert(s == NULL);

	s = network_queue_peek_string(q, 2, NULL);
	g_assert(s);
	g_assert_cmpint(s->len, ==, 2);
	g_assert_cmpstr(s->str, ==, "89");
	g_string_free(s, TRUE);
	g_assert_cmpint(q->len, ==, 2);

	s = network_queue_pop_string(q, 2, NULL);
	g_assert(s);
	g_assert_cmpint(s->len, ==, 2);
	g_assert_cmpstr(s->str, ==, "89");
	g_string_free(s, TRUE);
	g_assert_cmpint(q->len, ==, 0);

	network_queue_free(q);
}

#define TEST_ADDR_IP "127.0.0.1:57684"
#define TEST_ADDR_CLIENT_UDP "127.0.0.1:0"

void t_network_socket_bind(void) {
	network_socket *sock;
	
	g_log_set_always_fatal(G_LOG_FATAL_MASK); /* we log g_critical() which is fatal for the test-suite */

	sock = network_socket_new();

	/* w/o a address set it should fail */
	g_assert_cmpint(NETWORK_SOCKET_ERROR, ==, network_socket_bind(sock)); /* should fail, no address */

	g_assert_cmpint(0, ==, network_address_set_address(sock->dst, TEST_ADDR_IP));
	
	g_assert_cmpint(NETWORK_SOCKET_SUCCESS, ==, network_socket_bind(sock));

	network_socket_free(sock);

	/* bind again, to test if REUSEADDR works */
	sock = network_socket_new();
	
	g_assert_cmpint(0, ==, network_address_set_address(sock->dst, TEST_ADDR_IP));
	
	g_assert_cmpint(NETWORK_SOCKET_SUCCESS, ==, network_socket_bind(sock));

	g_assert_cmpint(NETWORK_SOCKET_ERROR, ==, network_socket_bind(sock)); /* bind a socket that is already bound, should fail */

	network_socket_free(sock);
}

/**
 * @test  check if the network_socket_connect() works by 
 *   - setting up a listening socket
 *   - connection to it
 *   - write data to it
 *   - read it back
 *   - closing it
 *   
 */
void t_network_socket_connect(void) {
	network_socket *sock;
	network_socket *client;
	network_socket *client_connected;
	fd_set read_fds;
	struct timeval timeout;
	network_socket_retval_t ret;
	
	g_log_set_always_fatal(G_LOG_FATAL_MASK); /* we log g_critical() which is fatal for the test-suite */

	sock = network_socket_new();

	g_assert_cmpint(0, ==, network_address_set_address(sock->dst, TEST_ADDR_IP));
	
	g_assert_cmpint(NETWORK_SOCKET_SUCCESS, ==, network_socket_bind(sock));
	
	client = network_socket_new();
	g_assert_cmpint(0, ==, network_address_set_address(client->dst, TEST_ADDR_IP));

	switch ((ret = network_socket_connect(client))) {
	case NETWORK_SOCKET_ERROR_RETRY:
		client_connected = network_socket_accept(sock);
	
		g_assert_cmpint(NETWORK_SOCKET_SUCCESS, ==, network_socket_connect_finish(client));
	
		break;
	case NETWORK_SOCKET_SUCCESS:
		/* looks like solaris makes a successful connect() even if the listen-socket isn't in accept() yet */
		client_connected = network_socket_accept(sock);
		break;
	default:
		client_connected = NULL;
		g_assert_cmpint(NETWORK_SOCKET_ERROR_RETRY,  ==, ret);
		break;
	}
	
	g_assert(client_connected);
	g_assert_cmpint(NETWORK_SOCKET_ERROR, ==, network_socket_connect(client)); /* we are already connected, sure fail */

	/* we are connected */

	network_queue_append(client->send_queue, g_string_new_len(C("foo")));
	g_assert_cmpint(NETWORK_SOCKET_SUCCESS, ==, network_socket_write(client, -1)); /* send all */

	FD_ZERO(&read_fds);
	FD_SET(client_connected->fd, &read_fds);
	timeout.tv_sec = 1;
	timeout.tv_usec = 500 * 000; /* wait 500ms */
	g_assert_cmpint(1, ==, select(client_connected->fd + 1, &read_fds, NULL, NULL, &timeout));
	
	/* socket_read() needs ->to_read set */
	g_assert_cmpint(NETWORK_SOCKET_SUCCESS, ==, network_socket_to_read(client_connected));
	g_assert_cmpint(3, ==, client_connected->to_read);
	g_assert_cmpint(NETWORK_SOCKET_SUCCESS, ==, network_socket_read(client_connected)); /* read all */
	g_assert_cmpint(0, ==, client_connected->to_read);
	
	network_socket_free(client);
	client = NULL;

	/* try to read from closed socket */
	g_assert_cmpint(NETWORK_SOCKET_SUCCESS, ==, network_socket_to_read(client_connected));
	g_assert_cmpint(0, ==, client_connected->to_read);

	network_socket_free(client_connected);
	network_socket_free(sock);
}

/**
 * @test  check if the network_socket_connect() works by 
 *   - setting up a listening socket
 *   - connection to it
 *   - write data to it
 *   - read it back
 *   - closing it
 *   
 */
void t_network_socket_connect_udp(void) {
	network_socket *server;
	network_socket *client;
	fd_set read_fds;
	struct timeval timeout;
	network_socket_retval_t ret;
	
	g_log_set_always_fatal(G_LOG_FATAL_MASK); /* we log g_critical() which is fatal for the test-suite */

	server = network_socket_new();
	server->socket_type = SOCK_DGRAM;

	g_assert_cmpint(0, ==, network_address_set_address(server->src, TEST_ADDR_IP)); /* our UDP port */
	
	g_assert_cmpint(NETWORK_SOCKET_SUCCESS, ==, network_socket_bind(server));
	g_assert_cmpint(NETWORK_SOCKET_SUCCESS, ==, network_socket_to_read(server));
	g_assert_cmpint(0, ==, server->to_read);
	
	client = network_socket_new();
	client->socket_type = SOCK_DGRAM;
	g_assert_cmpint(0, ==, network_address_set_address(client->dst, TEST_ADDR_IP)); /* the server's port */
	g_assert_cmpint(0, ==, network_address_set_address(client->src, TEST_ADDR_CLIENT_UDP)); /* a random port */

	g_assert_cmpint(NETWORK_SOCKET_SUCCESS, ==, network_socket_bind(client));

	/* we are connected */

	network_queue_append(client->send_queue, g_string_new_len(C("foo")));
	g_assert_cmpint(NETWORK_SOCKET_SUCCESS, ==, network_socket_write(client, -1)); /* send all */

	FD_ZERO(&read_fds);
	FD_SET(server->fd, &read_fds);
	timeout.tv_sec = 1;
	timeout.tv_usec = 500 * 000; /* wait 500ms */
	g_assert_cmpint(1, ==, select(server->fd + 1, &read_fds, NULL, NULL, &timeout));
	
	/* socket_read() needs ->to_read set */
	g_assert_cmpint(NETWORK_SOCKET_SUCCESS, ==, network_socket_to_read(server));
	g_assert_cmpint(3, ==, server->to_read);
	g_assert_cmpint(NETWORK_SOCKET_SUCCESS, ==, network_socket_read(server)); /* read all */
	g_assert_cmpint(0, ==, server->to_read);
	
	network_socket_free(client);
	network_socket_free(server);
}

#ifndef WIN32

/**
 * test if _is_local() works on unix-domain sockets
 *
 * MacOS X 10.4 doesn't report a .sa_family for one of the sides of the connection if it is a unix-socket 
 */
void t_network_socket_is_local_unix() {
	network_socket *s_sock; /* the server side socket, listening for requests */
	network_socket *c_sock; /* the client side socket, that connects */
	network_socket *a_sock; /* the server side, accepted socket */

	g_test_bug("42220");
	g_log_set_always_fatal(G_LOG_FATAL_MASK); /* gtest modifies the fatal-mask */

	s_sock = network_socket_new();
	network_address_set_address(s_sock->dst, "/tmp/mysql-proxy-test.socket");

	c_sock = network_socket_new();
	network_address_set_address(c_sock->dst, "/tmp/mysql-proxy-test.socket");


	/* hack together a network_socket_accept() which we don't have in this tree yet */
	g_assert_cmpint(NETWORK_SOCKET_SUCCESS, ==, network_socket_bind(s_sock));

	g_assert_cmpint(NETWORK_SOCKET_SUCCESS, ==, network_socket_connect(c_sock));

	a_sock = network_socket_accept(s_sock);
	g_assert(a_sock);

	g_assert_cmpint(TRUE, ==, network_address_is_local(s_sock->dst, a_sock->dst));

	network_socket_free(a_sock);
	network_socket_free(c_sock);
	network_socket_free(s_sock);

	g_unlink("/tmp/mysql-proxy-test.socket");
}

void t_network_localsocket_setup(local_unix_t *p, gconstpointer G_GNUC_UNUSED _test_data) {
	g_assert(p != NULL);
	snprintf(p->sockname, sizeof(p->sockname), LOCAL_SOCK ".%d", (int)getpid());
	pp = p;
}

void t_network_localsocket_teardown(local_unix_t *p, gconstpointer G_GNUC_UNUSED _test_data) {
	g_assert(p != NULL);
	if (p->sockname[0] != '\0') {
		(void) g_unlink(p->sockname);
		p->sockname[0] = '\0';
	}
	pp = NULL;
}

void exitfunc(int sig) {
	if (pp != NULL && pp->sockname[0] != '\0')
		(void) g_unlink(pp->sockname);

	abort();
}

/**
 * test if local sockets are removed at shutdown
 * this is an extension of _is_local_unix(), therefore looks much like it;
 * just in case, we leave all tests from that function in as well.
 */
void t_network_socket_rem_local_unix(local_unix_t *p, gconstpointer G_GNUC_UNUSED _test_data) {
	network_socket *s_sock; /* the server side socket, listening for requests */
	network_socket *c_sock; /* the client side socket, that connects */
	network_socket *a_sock; /* the server side, accepted socket */

	/*
	 * if an assertion fails, we receive the ABORT signal. We need to
	 * close the unix socket in that case too. The regular teardown function
	 * is not called in this case, so we install our own handler.
	 */
	signal(SIGABRT, exitfunc);
	g_test_bug("42220");
	g_log_set_always_fatal(G_LOG_FATAL_MASK); /* gtest modifies the fatal-mask */

	s_sock = network_socket_new();
	network_address_set_address(s_sock->dst, p->sockname);

	c_sock = network_socket_new();
	network_address_set_address(c_sock->dst, p->sockname);

	g_assert_cmpint(NETWORK_SOCKET_SUCCESS, ==, network_socket_bind(s_sock));

	g_assert_cmpint(g_access(p->sockname, 0), ==, 0);

	g_assert_cmpint(NETWORK_SOCKET_SUCCESS, ==, network_socket_connect(c_sock));

	a_sock = network_socket_accept(s_sock);
	g_assert(a_sock);

	g_assert_cmpint(TRUE, ==, network_address_is_local(s_sock->dst, a_sock->dst));

	network_socket_free(a_sock);
	g_assert_cmpint(g_access(p->sockname, 0), ==, 0);
	network_socket_free(c_sock);
	g_assert_cmpint(g_access(p->sockname, 0), ==, 0);
	network_socket_free(s_sock);
	g_assert_cmpint(g_access(p->sockname, 0), ==, -1);

	/* re-establish default signal disposition */
	signal(SIGABRT, SIG_DFL);
}

#endif /* WIN32 */

int main(int argc, char **argv) {
	g_test_init(&argc, &argv, NULL);
	g_test_bug_base("http://bugs.mysql.com/");

	g_test_add_func("/core/network_socket_new", test_network_socket_new);
	g_test_add_func("/core/network_socket_bind", t_network_socket_bind);
	g_test_add_func("/core/network_socket_connect", t_network_socket_connect);
	g_test_add_func("/core/network_queue_append", test_network_queue_append);
	g_test_add_func("/core/network_queue_peek_string", test_network_queue_peek_string);
	g_test_add_func("/core/network_queue_pop_string", test_network_queue_pop_string);
#ifndef WIN32
	g_test_add_func("/core/network_socket_is_local_unix",t_network_socket_is_local_unix);

	g_test_add("/core/network_socket_rem_local_unix", local_unix_t,
			&local_test_arg,
			t_network_localsocket_setup, t_network_socket_rem_local_unix, 
			t_network_localsocket_teardown);
#endif /* WIN32 */
#if 0
	/**
	 * disabled for now until we fixed the _to_read() on HP/UX and AIX (and MacOS X)
	 *
	 * ERROR:(t_network_socket.c:287):???: assertion failed (3 == server->to_read): (3 == 19)
	 */
	g_test_add_func("/core/network_socket_udp", t_network_socket_connect_udp);
#endif

	return g_test_run();
}
#else /* GLIB_CHECK_VERSION */
int main() {
	return 77;
}
#endif /* GLIB_CHECK_VERSION */
