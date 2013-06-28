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

#ifndef __CHASSIS_TIMINGS_H__
#define __CHASSIS_TIMINGS_H__

#include <glib.h>
#include "my_rdtsc.h"
#include "chassis-exports.h"

typedef struct {
	const gchar *name;
	guint64 usec;
	guint64 cycles;
	guint64 ticks;

	const gchar *filename;
	gint line;
} chassis_timestamp_t;

CHASSIS_API chassis_timestamp_t *chassis_timestamp_new(void);
void chassis_timestamp_init_now(chassis_timestamp_t *ts,
		const char *name,
		const char *filename,
		gint line);
CHASSIS_API void chassis_timestamp_free(chassis_timestamp_t *ts);

typedef struct {
	GQueue *timestamps; /* list of chassis_timestamp_t */
} chassis_timestamps_t;

CHASSIS_API chassis_timestamps_t *chassis_timestamps_new(void);
CHASSIS_API void chassis_timestamps_free(chassis_timestamps_t *ts);

CHASSIS_API void chassis_timestamps_add(chassis_timestamps_t *ts,
		const char *name,
		const char *filename,
		gint line);

/**
 * Retrieve a timestamp with a millisecond resolution.
 *
 * @note The return value must not be assumed to be based on any specific epoch, it is only to be used as a relative measure.
 * @return the current timestamp
 */
CHASSIS_API guint64 chassis_get_rel_milliseconds();

/**
 * Retrieve a timestamp with a microsecond resolution.
 *
 * @note The return value must not be assumed to be based on any specific epoch, it is only to be used as a relative measure.
 * @return the current timestamp
 */
CHASSIS_API guint64 chassis_get_rel_microseconds();

/**
 * Calculate the difference between two relative microsecond readings, taking into account a potential timer frequency.
 *
 * @note This is especially necessary for Windows, do _not_ simply subtract the relative readings, those are _not_ in microseconds!
 * @param start the start time
 * @param stop the end time
 * @return the difference of stop and start, adjusted to microseconds
 */
CHASSIS_API guint64 chassis_calc_rel_microseconds(guint64 start, guint64 stop);

/**
 * Retrieve a timestamp with a nanosecond resolution.
 *
 * @note The return value must not be assumed to be based on any specific epoch, it is only to be used as a relative measure.
 * @return the current timestamp
 */
CHASSIS_API guint64 chassis_get_rel_nanoseconds();

typedef struct my_timer_info chassis_timestamps_global_t;

/**
 * The default, global timer information.
 *
 * It is initialized by chassis_timings_global_init(NULL), otherwise it will
 * stay NULL (in case client code needs a separate timer base for whatever reason).
 * The timer information contains metadata about the support timers, like
 * resolution, overhead and frequency.
 * This is necessary to convert a cycle measurement to a time value, for example.
 */
CHASSIS_API chassis_timestamps_global_t *chassis_timestamps_global;

/**
 * Creates a new timer base, which will calibrate itself during creation.
 * @note This function is not threadsafe.
 * @param gl the new timer or NULL to initialize the global timer base
 */
CHASSIS_API void chassis_timestamps_global_init(chassis_timestamps_global_t *gl);

CHASSIS_API void chassis_timestamps_global_free(chassis_timestamps_t *gl);

#endif
