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

#include <glib.h>

#include <errno.h>
#ifdef WIN32
/* need something compatible, taken from MSDN docs */
#define PATH_MAX MAX_PATH
#include <windows.h>
#else
#include <stdlib.h> /* for realpath */
#endif
#include "chassis-path.h"

gchar *chassis_get_basedir(const gchar *prgname) {
	gchar *absolute_path;
	gchar *bin_dir;
	gchar r_path[PATH_MAX];
	gchar *base_dir;
	
	if (g_path_is_absolute(prgname)) {
		absolute_path = g_strdup(prgname); /* No need to dup, just to get free right */
	} else {
		/**
		 * the path wasn't absolute
		 *
		 * Either it is
		 * - in the $PATH 
		 * - relative like ./bin/... or
		 */

		absolute_path = g_find_program_in_path(prgname);
		if (absolute_path == NULL) {
			g_critical("can't find myself (%s) in PATH", prgname);

			return NULL;
		}

		if (!g_path_is_absolute(absolute_path)) {
			gchar *cwd = g_get_current_dir();

			g_free(absolute_path);

			absolute_path = g_build_filename(cwd, prgname, NULL);

			g_free(cwd);
		}
	}

	/* assume that the binary is in ./s?bin/ and that the the basedir is right above it
	 *
	 * to get this working we need a "clean" basedir, no .../foo/./bin/ 
	 */
#ifdef WIN32
	if (0 == GetFullPathNameA(absolute_path, PATH_MAX, r_path, NULL)) {
		g_critical("%s: GetFullPathNameA(%s) failed: %s",
				G_STRLOC,
				absolute_path,
				g_strerror(errno));

		return NULL;
	}
#else
	if (NULL == realpath(absolute_path, r_path)) {
		g_critical("%s: realpath(%s) failed: %s",
				G_STRLOC,
				absolute_path,
				g_strerror(errno));

		return NULL;
	}
#endif
	bin_dir = g_path_get_dirname(r_path);
	base_dir = g_path_get_dirname(bin_dir);
	
	/* don't free base_dir, because we need it later */
	g_free(absolute_path);
	g_free(bin_dir);

	return base_dir;
}

/**
 * Helper function to correctly take into account the users base-dir setting for
 * paths that might be relative.
 * Note: Because this function potentially frees the pointer to gchar* that's passed in and cannot lock
 *       on that, it is _not_ threadsafe. You have to ensure threadsafety yourself!
 * @returns TRUE if it modified the filename, FALSE if it didn't
 */
gboolean chassis_resolve_path(const char *base_dir, gchar **filename) {
	gchar *new_path = NULL;

	if (!base_dir ||
		!filename ||
		!*filename)
		return FALSE;
	
	/* don't even look at absolute paths */
	if (g_path_is_absolute(*filename)) return FALSE;
	
	new_path = g_build_filename(base_dir, G_DIR_SEPARATOR_S, *filename, NULL);
	
	g_debug("%s.%d: adjusting relative path (%s) to base_dir (%s). New path: %s", __FILE__, __LINE__, *filename, base_dir, new_path);

	g_free(*filename);
	*filename = new_path;
	return TRUE;
}


