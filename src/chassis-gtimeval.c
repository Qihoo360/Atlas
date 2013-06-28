/* $%BEGINLICENSE%$
 Copyright (c) 2010, Oracle and/or its affiliates. All rights reserved.

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

#include "chassis-gtimeval.h"

void chassis_gtime_testset_now(GTimeVal *gt, gint64 *delay)
{
	GTimeVal	now;
	gint64		tdiff;

	if (gt == NULL)
		return;

	g_get_current_time(&now);
	ge_gtimeval_diff(gt, &now, &tdiff);

	if (tdiff < 0) {
		g_critical("%s: time went backwards (%"G_GINT64_FORMAT" usec)!",
				G_STRLOC, tdiff);
		gt->tv_usec = gt->tv_sec = 0;
		goto out;
	}

	*gt = now;
out:
	if (delay != NULL)
		*delay = tdiff;
	return;
}
