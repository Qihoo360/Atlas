/* $%BEGINLICENSE%$
 Copyright (c) 2009, 2010, Oracle and/or its affiliates. All rights reserved.

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
 
#include <sys/types.h>
#include <sys/stat.h>
#include <errno.h>
#include <string.h>

#include <gmodule.h>

#include "chassis-filemode.h"

int chassis_filemode_check(const gchar *filename) {
	return chassis_filemode_check_full(filename, CHASSIS_FILEMODE_SECURE_MASK, NULL);
}

/*
 * check whether the given filename points to a file the permissions
 * of which are 0 for group and other (ie read/writable only by owner).
 * return 0 for "OK", -1 of the file cannot be accessed or is the wrong
 * type of file, and 1 if permissions are wrong
 *
 * since Windows has no concept of owner/group/other, this function
 * just return 0 for windows
 *
 * FIXME? this function currently ignores ACLs
 */
int chassis_filemode_check_full(const gchar *filename, int required_filemask, GError **gerr) {
#ifndef _WIN32
	struct stat stbuf;
	mode_t		fmode;
	
	if (stat(filename, &stbuf) == -1) {
		g_set_error(gerr, G_FILE_ERROR, g_file_error_from_errno(errno),
				"cannot stat(%s): %s", filename,
				g_strerror(errno));
		return -1;
	}

	fmode = stbuf.st_mode;
	if ((fmode & S_IFMT) != S_IFREG) {
		g_set_error(gerr, G_FILE_ERROR, G_FILE_ERROR_INVAL,
				"%s isn't a regular file", filename);
		return -1;
	}

	if ((fmode & required_filemask) != 0) {
		g_set_error(gerr, G_FILE_ERROR, G_FILE_ERROR_PERM,
				"permissions of %s aren't secure (0660 or stricter required)", filename);
		return 1;
	}
	
#undef MASK

#endif /* _WIN32 */
	return 0;
}
