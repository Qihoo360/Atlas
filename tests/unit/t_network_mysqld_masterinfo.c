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

#include <glib.h>

#include "network-mysqld-masterinfo.h"
#include "glib-ext.h"

#if GLIB_CHECK_VERSION(2, 16, 0)
#define C(x) x, sizeof(x) - 1
#define S(x) x->str, x->len

/**
 * Tests for the MySQL Protocol Codec functions
 * @ingroup proto
 */

/*@{*/
/**
 * @test check that network_mysqld_masterinfo_new() returns non-NULL 
 *   and that network_mysqld_masterinfo_free() doesn't crash
 */
void t_masterinfo_new(void) {
	network_mysqld_masterinfo_t *info;

	info = network_mysqld_masterinfo_new();
	g_assert(info);

	network_mysqld_masterinfo_free(info);
}

/**
 * @test 
 *   network_mysqld_masterinfo_get() can decode a protocol string and
 *   network_mysqld_masterinfo_append() can encode the internal structure and 
 *     turns it back into the orignal string
 */
void t_masterinfo_get(void) {
#define PACKET "15\nhostname-bin.000024\n2143897\n127.0.0.1\nroot\n123\n3306\n60\n0\n\n\n\n\n\n0\n"
	network_mysqld_masterinfo_t *info;
	network_packet *packet;
	GString *s;

	info = network_mysqld_masterinfo_new();
	g_assert(info);

	packet = network_packet_new();
	packet->data = g_string_new_len(C(PACKET));
	packet->offset = 0;

	g_assert_cmpint(network_mysqld_masterinfo_get(packet, info), !=, -1);

	g_assert_cmpstr(info->master_log_file->str, ==, "hostname-bin.000024");
	g_assert_cmpint(info->master_log_pos,       ==, 2143897);
	g_assert_cmpstr(info->master_host->str,     ==, "127.0.0.1");
	g_assert_cmpstr(info->master_user->str,     ==, "root");
	g_assert_cmpstr(info->master_password->str, ==, "123");
	g_assert_cmpint(info->master_port,          ==, 3306);
	g_assert_cmpint(info->master_connect_retry, ==, 60);
	g_assert_cmpint(info->master_ssl,           ==, 0); /* is disabled */
	g_assert_cmpint(info->master_ssl_verify_server_cert, ==, 0);

	s = g_string_new(NULL);
	g_assert_cmpint(network_mysqld_masterinfo_append(s, info), ==, 0);
	g_assert_cmpint(s->len, ==, sizeof(PACKET) - 1);
	g_assert_cmpint(TRUE, ==, g_memeq(S(s), C(PACKET)));
	g_string_free(s, TRUE);

	g_string_free(packet->data, TRUE);
	network_packet_free(packet);

	network_mysqld_masterinfo_free(info);
}

/**
 * @cond
 *   don't include the main() function the docs
 */
int main(int argc, char **argv) {
	g_test_init(&argc, &argv, NULL);
	g_test_bug_base("http://bugs.mysql.com/");

	g_test_add_func("/core/masterinfo-new", t_masterinfo_new);
	g_test_add_func("/core/masterinfo-get", t_masterinfo_get);

	return g_test_run();
}
/** @endcond */
#else
int main() {
	return 77;
}
#endif
