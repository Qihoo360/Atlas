/* $%BEGINLICENSE%$
 Copyright (c) 2009, Oracle and/or its affiliates. All rights reserved.

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

#include <glib.h>

#include "network-socket.h"

#if GLIB_CHECK_VERSION(2, 16, 0)
#define C(x) x, sizeof(x) - 1

void t_network_address_new() {
	network_address *addr;

	addr = network_address_new();

	network_address_free(addr);
}

void t_network_address_set() {
	network_address *addr;

	addr = network_address_new();

	g_assert_cmpint(network_address_set_address(addr, "127.0.0.1:3306"), ==, 0);
	g_assert_cmpint(network_address_set_address(addr, "127.0.0.1"), ==, 0);

	g_log_set_always_fatal(G_LOG_FATAL_MASK);

	/* should fail */	
	g_assert_cmpint(network_address_set_address(addr, "500.0.0.1"), ==, -1);
	g_assert_cmpint(network_address_set_address(addr, "127.0.0.1:"), ==, -1);
	g_assert_cmpint(network_address_set_address(addr, "127.0.0.1:65536"), ==, -1);
	g_assert_cmpint(network_address_set_address(addr, "127.0.0.1:-1"), ==, -1);

	network_address_free(addr);
}

/**
 * test if we decode the port number correctly
 */
void t_network_address_resolve() {
	network_address *addr;

	g_test_bug("43313");

	addr = network_address_new();
	network_address_set_address(addr, "127.0.0.1:3306");

	/* _set_address() should set the port number */
	g_assert_cmpint(ntohs(addr->addr.ipv4.sin_port), ==, 3306);

	/* reset the name to see that _refresh_name() updates to the right value */
	g_string_truncate(addr->name, 0);

	network_address_refresh_name(addr);

	g_assert_cmpstr(addr->name->str, ==, "127.0.0.1:3306");

	network_address_free(addr);
}


int main(int argc, char **argv) {
	g_test_init(&argc, &argv, NULL);
	g_test_bug_base("http://bugs.mysql.com/");

	g_test_add_func("/core/network_address_new", t_network_address_new);
	g_test_add_func("/core/network_address_set", t_network_address_set);
	g_test_add_func("/core/network_address_resolve", t_network_address_resolve);

	return g_test_run();
}
#else
int main() {
	return 77;
}
#endif
