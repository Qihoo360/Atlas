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

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <glib.h>

#include "chassis-timings.h"

#if GLIB_CHECK_VERSION(2, 16, 0)

void t_chassis_timings() {
	chassis_timestamps_global_t timings;
	chassis_timestamps_t *ts;
	GList *node;

	chassis_timestamps_global_init(&timings);

	ts = chassis_timestamps_new();

	chassis_timestamps_add(ts, "hello", __FILE__, __LINE__);
	chassis_timestamps_add(ts, "world", __FILE__, __LINE__);
	chassis_timestamps_add(ts, "I think", __FILE__, __LINE__);
	chassis_timestamps_add(ts, "this works", __FILE__, __LINE__);
	chassis_timestamps_add(ts, "really", __FILE__, __LINE__);

	/* print the timestamps */
	for (node = ts->timestamps->head; node; node = node->next) {
		chassis_timestamp_t *prev = node->prev ? node->prev->data : NULL;
		chassis_timestamp_t *cur = node->data;

		g_debug("%s:%d %s usec=%"G_GUINT64_FORMAT", cycles=%"G_GUINT64_FORMAT", ticks=%"G_GUINT64_FORMAT,
				cur->filename, cur->line, cur->name,
				prev ? cur->usec - prev->usec: 0,
				prev ? cur->cycles - prev->cycles - timings.cycles_overhead: 0,
				prev ? cur->ticks - prev->ticks : 0
		       );
	}

	chassis_timestamps_free(ts);
}

int main(int argc, char **argv) {
	g_test_init(&argc, &argv, NULL);
	g_test_bug_base("http://bugs.mysql.com/");

	g_test_add_func("/core/chassis_timings", t_chassis_timings);

	return g_test_run();
}
#else
int main() {
	return 77;
}
#endif
