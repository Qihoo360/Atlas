/* $%BEGINLICENSE%$
 Copyright (c) 2007, 2010, Oracle and/or its affiliates. All rights reserved.

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
 

#ifndef _CHASSIS_LOG_H_
#define _CHASSIS_LOG_H_

#include <glib.h>
#ifdef _WIN32
#include <windows.h>
#endif

#include "chassis-exports.h"

#define CHASSIS_RESOLUTION_SEC	0x0
#define CHASSIS_RESOLUTION_MS	0x1

#define CHASSIS_RESOLUTION_DEFAULT	CHASSIS_RESOLUTION_SEC

/** @addtogroup chassis */
/*@{*/
typedef struct {
	GLogLevelFlags min_lvl;

	gchar *log_filename;
	gint log_file_fd;

	gboolean use_syslog;

#ifdef _WIN32
	HANDLE event_source_handle;
	gboolean use_windows_applog;
#endif
	gboolean rotate_logs;

	GString *log_ts_str;
	gint	 log_ts_resolution;	/*<< timestamp resolution (sec, ms) */

	GString *last_msg;
	time_t   last_msg_ts;
	guint    last_msg_count;
} chassis_log;


CHASSIS_API chassis_log *chassis_log_init(void) G_GNUC_DEPRECATED;
CHASSIS_API chassis_log *chassis_log_new(void);
CHASSIS_API int chassis_log_set_level(chassis_log *log, const gchar *level);
CHASSIS_API void chassis_log_free(chassis_log *log);
CHASSIS_API int chassis_log_open(chassis_log *log);
CHASSIS_API void chassis_log_func(const gchar *log_domain, GLogLevelFlags log_level, const gchar *message, gpointer user_data);
CHASSIS_API void chassis_log_set_logrotate(chassis_log *log);
CHASSIS_API int chassis_log_set_event_log(chassis_log *log, const char *app_name);
CHASSIS_API const char *chassis_log_skip_topsrcdir(const char *message);
CHASSIS_API void chassis_set_logtimestamp_resolution(chassis_log *log, int res);
CHASSIS_API int chassis_get_logtimestamp_resolution(chassis_log *log);
/*@}*/

#endif
