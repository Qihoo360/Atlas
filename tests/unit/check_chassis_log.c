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

/** @addtogroup unittests Unit tests */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <glib.h>

#include "chassis-log.h"

#if GLIB_CHECK_VERSION(2, 16, 0)
#define C(x) x, sizeof(x) - 1

#define START_TEST(x) void (x)(void)

/*@{*/

/**
 * @test Test log message coalescing.
 */
START_TEST(test_log_compress) {
	chassis_log *l;
	GLogFunc old_log_func;

	l = chassis_log_new();

	g_log_set_always_fatal(G_LOG_FATAL_MASK);

	old_log_func = g_log_set_default_handler(chassis_log_func, l);

	g_critical("I am duplicate");
	g_critical("I am duplicate");
	g_critical("I am duplicate");
	g_critical("above should be 'last message repeated 2 times'");

	g_log_set_default_handler(old_log_func, NULL);

	chassis_log_free(l);
}
/*@}*/

/**
 * @test Test that we strip filenames from the log-messages if there is any
 */
static void test_log_skip_topsrcdir(void) {
	chassis_log *l;
	GLogFunc old_log_func;

	l = chassis_log_new();

	g_log_set_always_fatal(G_LOG_FATAL_MASK); /* disable the FATAL handling of critical messages in testing */

	old_log_func = g_log_set_default_handler(chassis_log_func, l);

	/* if there is no prefix, make sure it is unchanged */
	g_critical("no prefix");
	g_assert_cmpstr("no prefix", ==, l->last_msg->str);

	/* if we are built with absolute source filenames, make sure they are stripped */ 
	g_critical("%s: with G_STRLOC", G_STRLOC); g_assert_cmpint(__LINE__, ==, 80); /* keep this in one line as the next ones are referencing it */
	if (g_path_is_absolute(__FILE__)) {
		g_assert_cmpstr("tests" G_DIR_SEPARATOR_S "unit" G_DIR_SEPARATOR_S "check_chassis_log.c:80: with G_STRLOC", ==, l->last_msg->str);
	} else {
		g_assert_cmpstr(__FILE__ ":80: with G_STRLOC", ==, l->last_msg->str);
	}

	/* Bug#58941
	 *
	 * if __FILE__ isn't absolute don't try to strip filenames
	 *
	 * especially don't strip them if they start with the same characters as the reference-filename 'chassis-log.c'
	 */
	g_critical("chassis-log.c: name");
	g_assert_cmpstr("chassis-log.c: name", ==, l->last_msg->str);

	g_critical("charm");
	g_assert_cmpstr("charm", ==, l->last_msg->str);

	g_log_set_default_handler(old_log_func, NULL);

	chassis_log_free(l);
}
/*@}*/

/**
 * @test Test log timestamp resolution
 */
START_TEST(test_log_timestamp) {
	chassis_log *l;
	GLogFunc old_log_func;

	l = chassis_log_new();
	chassis_set_logtimestamp_resolution(l, CHASSIS_RESOLUTION_SEC);

	g_log_set_always_fatal(G_LOG_FATAL_MASK);

	old_log_func = g_log_set_default_handler(chassis_log_func, l);

	g_critical("this message has a second-resolution timestamp");
	chassis_set_logtimestamp_resolution(l, CHASSIS_RESOLUTION_MS);
	g_critical("this message has a millisecond-resolution timestamp");

	g_assert_cmpint(CHASSIS_RESOLUTION_MS, ==, chassis_get_logtimestamp_resolution(l));
	/* try an illegal value, we should see no change */
	chassis_set_logtimestamp_resolution(l, -1);
	g_assert_cmpint(CHASSIS_RESOLUTION_MS, ==, chassis_get_logtimestamp_resolution(l));
	/* tset back top _SEC resolution */
	chassis_set_logtimestamp_resolution(l, CHASSIS_RESOLUTION_SEC);
	g_assert_cmpint(CHASSIS_RESOLUTION_SEC, ==, chassis_get_logtimestamp_resolution(l));

	

	g_log_set_default_handler(old_log_func, NULL);

	chassis_log_free(l);
}
/*@}*/

int main(int argc, char **argv) {
	g_test_init(&argc, &argv, NULL);
	g_test_bug_base("http://bugs.mysql.com/");

	g_test_add_func("/core/log_compress", test_log_compress);
	g_test_add_func("/core/log_timestamp", test_log_timestamp);
	g_test_add_func("/core/log_strip_absfilename", test_log_skip_topsrcdir);

	return g_test_run();
}
#else
int main() {
	return 77;
}
#endif
