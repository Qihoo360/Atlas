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

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include <glib.h>

#include <sys/types.h>
#ifdef HAVE_SYS_TIME_H
#include <sys/time.h>
#endif
#ifdef HAVE_SYS_RESOURCE_H
#include <sys/resource.h>
#endif
#ifdef _WIN32
#include <stdio.h> /* for _getmaxstdio() */
#endif
#include <errno.h>

#include "chassis-limits.h"

gint64 chassis_fdlimit_get() {
#ifdef _WIN32
	return _getmaxstdio();
#else
	struct rlimit max_files_rlimit;

	if (-1 == getrlimit(RLIMIT_NOFILE, &max_files_rlimit)) {
		return -1;
	} else {
		return max_files_rlimit.rlim_cur;
	}
#endif

}

/**
 * redirect the old call 
 */
int chassis_set_fdlimit(int max_files_number) {
	return chassis_fdlimit_set(max_files_number);
}

/**
 * set the upper limit of open files
 *
 * @return -1 on error, 0 on success
 */
int chassis_fdlimit_set(gint64 max_files_number) {
#ifdef _WIN32
	int max_files_number_set;

	max_files_number_set = _setmaxstdio(max_files_number);

	if (-1 == max_files_number_set) {
		return -1;
	} else if (max_files_number_set != max_files_number) {
		g_critical("%s: failed to increase the maximum number of open files for stdio: %s (%d)", G_STRLOC, g_strerror(errno), errno);
		return -1;
	}

	return 0;
#else
	struct rlimit max_files_rlimit;
	rlim_t soft_limit;
	rlim_t hard_limit;

	if (-1 == getrlimit(RLIMIT_NOFILE, &max_files_rlimit)) {
		return -1;
	}

	soft_limit = max_files_rlimit.rlim_cur;
	hard_limit = max_files_rlimit.rlim_max;

	max_files_rlimit.rlim_cur = max_files_number;
	if (hard_limit < max_files_number) { /* raise the hard-limit too in case it is smaller than the soft-limit, otherwise we get a EINVAL */
		max_files_rlimit.rlim_max = max_files_number;
	}

	if (-1 == setrlimit(RLIMIT_NOFILE, &max_files_rlimit)) {
		return -1;
	}

	return 0;
#endif
}

