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

#include "chassis-path.h"

#if GLIB_CHECK_VERSION(2, 16, 0)
#define C(x) x, sizeof(x) - 1

#define START_TEST(x) void (x)(void)

/*@{*/

/**
 * @test Resolve a relative path against an absolute base directory.
 */
START_TEST(test_path_basedir) {
	gchar *filename;
	gchar *test_filename;

	filename = g_build_filename("some", "relative", "path", "file", NULL);
	
	/* resolving this path must lead to changing the filename */
	g_assert_cmpint(chassis_resolve_path(G_DIR_SEPARATOR_S "tmp", &filename), ==, 1);
	
	test_filename = g_build_filename(G_DIR_SEPARATOR_S "tmp", "some", "relative", "path", "file", NULL);

	g_assert_cmpstr(test_filename, ==, filename);

	g_free(filename);
	g_free(test_filename);
}

/**
 * @test Resolving a relative path against a missing base directory does not modify the relative directory.
 */
START_TEST(test_no_basedir) {
	gchar *filename;
	gchar *test_filename;
	
	filename = g_build_filename("some", "relative", "path", "file", NULL);
	test_filename = g_strdup(filename);
	
	/* resolving this path must not lead to changing the filename */
	g_assert_cmpint(chassis_resolve_path(NULL, &filename), ==, 0);
	
	g_assert_cmpstr(test_filename, ==, filename);

	g_free(filename);
	g_free(test_filename);
}

/**
 * @test Resolving an absolute path against an absolute basedir has no effect and does not change the directory to be resolved.
 */
START_TEST(test_abspath_basedir) {
	gchar *filename;
	gchar *test_filename;
	
	filename = g_build_filename(G_DIR_SEPARATOR_S "some", "relative", "path", "file", NULL);
	test_filename = g_strdup(filename);
	
	/* resolving this path must lead to no change in the filename */
	g_assert_cmpint(chassis_resolve_path(G_DIR_SEPARATOR_S "tmp", &filename), ==, 0);
	
	g_assert_cmpstr(test_filename, ==, filename);

	g_free(filename);
	g_free(test_filename);
}
/*@}*/

int main(int argc, char **argv) {
	g_thread_init(NULL);

	g_test_init(&argc, &argv, NULL);
	g_test_bug_base("http://bugs.mysql.com/");
	
	g_test_add_func("/core/basedir/relpath", test_path_basedir);
	g_test_add_func("/core/basedir/nobasedir", test_no_basedir);
	g_test_add_func("/core/basedir/abspath", test_abspath_basedir);
	
	return g_test_run();
}
#else
int main() {
	return 77;
}
#endif
