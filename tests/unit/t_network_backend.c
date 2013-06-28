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
#include <glib.h>

#include "network-backend.h"

#if GLIB_CHECK_VERSION(2, 16, 0)
#define C(x) x, sizeof(x) - 1

/**
 * test of network_backends_new() allocates a memory 
 */
void t_network_backends_new() {
	network_backends_t *backends;

	backends = network_backends_new();
	g_assert(backends);

	g_assert_cmpint(network_backends_count(backends), ==, 0);

	/* free against a empty pool */
	network_backends_free(backends);
}

void t_network_backend_new() {
	network_backend_t *b;

	b = network_backend_new();
	g_assert(b);

	network_backend_free(b);
}

void t_network_backends_add() {
	network_backends_t *backends;

	g_log_set_always_fatal(G_LOG_FATAL_MASK);
	backends = network_backends_new();
	g_assert(backends);
	
	g_assert_cmpint(network_backends_count(backends), ==, 0);

	/* insert should work */
	g_assert_cmpint(network_backends_add(backends, "127.0.0.1", BACKEND_TYPE_RW), ==, 0);
	
	g_assert_cmpint(network_backends_count(backends), ==, 1);

	/* is duplicate, should fail */
	g_assert_cmpint(network_backends_add(backends, "127.0.0.1", BACKEND_TYPE_RW), !=, 0);
	
	g_assert_cmpint(network_backends_count(backends), ==, 1);

	/* unfolds to the same default */
	g_assert_cmpint(network_backends_add(backends, "127.0.0.1:3306", BACKEND_TYPE_RW), !=, 0);
	
	g_assert_cmpint(network_backends_count(backends), ==, 1);

	/* make sure bad port numbers also fail */
	g_assert_cmpint(network_backends_add(backends, "127.0.0.1:113306", BACKEND_TYPE_RW), ==, -1);
	
	network_backends_free(backends);
}

/**
 * check if the timeout handle of backends_check() works 
 *
 * it should remove the _DOWN state back to something reasonable after 4 seconds 
 */
void t_network_backends_check() {
	network_backends_t *backends;
	network_backend_t *backend;

	backends = network_backends_new();
	g_assert(backends);
	
	g_assert_cmpint(network_backends_add(backends, "127.0.0.1", BACKEND_TYPE_RW), ==, 0);

	/* setup the test 
	 *
	 * mark the backend as down */
	backend = network_backends_get(backends, 0);
	backend->state = BACKEND_STATE_DOWN;
	g_get_current_time(&backend->state_since);
	
	/* we have to wait 4sec before it goes from DOWN to UNKNOWN */
	g_assert_cmpint(0, ==, network_backends_check(backends));
	
	/* travel back in time and pretend this state is already 5 sec old */
	backend->state_since.tv_sec -= 5;
	
	/* we should hit the gatekeeper to reduce the number of calls to iterate the 
	 * all backends */
	g_assert_cmpint(0, ==, network_backends_check(backends));

	/* open the gate */
	backends->backend_last_check.tv_sec -= 1; /* pretend we checked last 1sec ago */

 	/* we havn't checked any backends yet, so our backend should be set to _UNKNOWN */
	g_assert_cmpint(1, ==, network_backends_check(backends));

	g_assert_cmpint(BACKEND_STATE_UNKNOWN, ==, backend->state);

	/* set it to down again and check if we the caching works */
	backend->state = BACKEND_STATE_DOWN; 
	backend->state_since.tv_sec -= 5;

 	/* as long as we are below 1sec, no updates */
	g_assert_cmpint(0, ==, network_backends_check(backends));
	g_assert_cmpint(BACKEND_STATE_DOWN, ==, backend->state);

	network_backends_free(backends);
}

int main(int argc, char **argv) {
#ifdef WIN32
	WSADATA wsaData;
#endif

	g_thread_init(NULL);
	g_test_init(&argc, &argv, NULL);
	g_test_bug_base("http://bugs.mysql.com/");

#ifdef _WIN32
	if (0 != WSAStartup(MAKEWORD( 2, 2 ), &wsaData)) {
		g_critical("WSAStartup failed to initialize the socket library.\n");
		return -1;
	}
#endif

	g_test_add_func("/core/network_backends_new", t_network_backends_new);
	g_test_add_func("/core/network_backend_new", t_network_backend_new);
	g_test_add_func("/core/network_backends_add", t_network_backends_add);
	g_test_add_func("/core/network_backends_check", t_network_backends_check);

	return g_test_run();
}
#else
int main() {
	return 77;
}
#endif
