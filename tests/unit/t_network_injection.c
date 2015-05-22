/* $%BEGINLICENSE%$
 Copyright (c) 2008, Oracle and/or its affiliates. All rights reserved.

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

#include "network-injection.h"

#if GLIB_CHECK_VERSION(2, 16, 0)
#define C(x) x, sizeof(x) - 1

/**
 * check if the common case works
 */
void t_network_injection_new() {
	injection *inj;

	inj = injection_new(1, g_string_new("123"));
	g_assert(inj);

	injection_free(inj);
}

/**
 * a NULL shouldn't cause problems
 */
void t_network_injection_new_null() {
	injection *inj;

	inj = injection_new(1, NULL);
	g_assert(inj);

	injection_free(inj);
}

void t_network_injection_queue_new() {
	network_injection_queue *q;

	q = network_injection_queue_new();
	g_assert(q);

	network_injection_queue_free(q);
}

void t_network_injection_queue_append() {
	network_injection_queue *q;

	q = network_injection_queue_new();
	g_assert(q);

	g_assert_cmpint(0, ==, network_injection_queue_len(q));
	network_injection_queue_append(q, injection_new(1, NULL));
	g_assert_cmpint(1, ==, network_injection_queue_len(q));
	network_injection_queue_append(q, injection_new(1, NULL));
	g_assert_cmpint(2, ==, network_injection_queue_len(q));

	network_injection_queue_free(q);
}

void t_network_injection_queue_prepend() {
	network_injection_queue *q;

	q = network_injection_queue_new();
	g_assert(q);

	g_assert_cmpint(0, ==, network_injection_queue_len(q));
	network_injection_queue_prepend(q, injection_new(1, NULL));
	g_assert_cmpint(1, ==, network_injection_queue_len(q));
	network_injection_queue_prepend(q, injection_new(1, NULL));
	g_assert_cmpint(2, ==, network_injection_queue_len(q));

	network_injection_queue_free(q);
}

/**
 * reseting a used and empty queue 
 */
void t_network_injection_queue_reset() {
	network_injection_queue *q;

	q = network_injection_queue_new();
	g_assert(q);

	/* add something to the queue and check if resetting works */
	g_assert_cmpint(0, ==, network_injection_queue_len(q));
	network_injection_queue_append(q, injection_new(1, NULL));
	g_assert_cmpint(1, ==, network_injection_queue_len(q));
	network_injection_queue_reset(q);
	g_assert_cmpint(0, ==, network_injection_queue_len(q));

	/* reset a empty queue */
	network_injection_queue_reset(q);
	g_assert_cmpint(0, ==, network_injection_queue_len(q));

	network_injection_queue_reset(q);
	g_assert_cmpint(0, ==, network_injection_queue_len(q));

	network_injection_queue_free(q);
}

int main(int argc, char **argv) {
	g_thread_init(NULL);
	g_test_init(&argc, &argv, NULL);
	g_test_bug_base("http://bugs.mysql.com/");

	g_test_add_func("/core/network_injection_new", t_network_injection_new);
	g_test_add_func("/core/network_injection_new_null", t_network_injection_new_null);
	g_test_add_func("/core/network_injection_queue_new", t_network_injection_queue_new);
	g_test_add_func("/core/network_injection_queue_append", t_network_injection_queue_append);
	g_test_add_func("/core/network_injection_queue_prepend", t_network_injection_queue_prepend);
	g_test_add_func("/core/network_injection_queue_reset", t_network_injection_queue_reset);

	return g_test_run();
}
#else
int main() {
	return 77;
}
#endif
