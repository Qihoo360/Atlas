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
#include <errno.h>

#include <glib.h>

#include "network-socket.h"

#if GLIB_CHECK_VERSION(2, 16, 0)
#define C(x) x, sizeof(x) - 1

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

int main(int argc, char **argv) {
	g_test_init(&argc, &argv, NULL);
	g_test_bug_base("http://bugs.mysql.com/");

	g_test_add_func("/core/network_queue_append", test_network_queue_append);
	g_test_add_func("/core/network_queue_peek_string", test_network_queue_peek_string);
	g_test_add_func("/core/network_queue_pop_string", test_network_queue_pop_string);

	return g_test_run();
}
#else
int main() {
	return 77;
}
#endif
