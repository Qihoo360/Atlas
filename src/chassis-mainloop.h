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
 

#ifndef _CHASSIS_MAINLOOP_H_
#define _CHASSIS_MAINLOOP_H_

#include <glib.h>    /* GPtrArray */

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#ifdef HAVE_SYS_TIME_H
#include <sys/time.h>  /* event.h needs struct tm */
#endif
#ifdef HAVE_SYS_TYPES_H
#include <sys/types.h>
#endif
#ifdef _WIN32
#include <winsock2.h>
#endif
#include <event.h>     /* struct event_base */

#include "chassis-exports.h"
#include "chassis-log.h"
#include "chassis-stats.h"
#include "chassis-shutdown-hooks.h"

/** @defgroup chassis Chassis
 * 
 * the chassis contains the set of functions that are used by all programs
 *
 * */
/*@{*/
typedef struct chassis_private chassis_private;
typedef struct chassis chassis;
typedef struct chassis_event_threads_t chassis_event_threads_t;

struct chassis {
	struct event_base *event_base;
	gchar *event_hdr_version;

	GPtrArray *modules;                       /**< array(chassis_plugin) */

	gchar *base_dir;				/**< base directory for all relative paths referenced */
	gchar *log_path;				/**< log directory */
	gchar *user;					/**< user to run as */
	gchar *instance_name;					/**< instance name*/

	chassis_private *priv;
	void (*priv_shutdown)(chassis *chas, chassis_private *priv);
	void (*priv_free)(chassis *chas, chassis_private *priv);

	chassis_log *log;
	
	chassis_stats_t *stats;			/**< the overall chassis stats, includes lua and glib allocation stats */

	/* network-io threads */
	guint event_thread_count;

	chassis_event_threads_t *threads;

	chassis_shutdown_hooks_t *shutdown_hooks;

//	struct event_base *remove_base;
};

CHASSIS_API chassis *chassis_init(void) G_GNUC_DEPRECATED;
CHASSIS_API chassis *chassis_new(void);
CHASSIS_API void chassis_free(chassis *chas);
CHASSIS_API int chassis_check_version(const char *lib_version, const char *hdr_version);

/**
 * the mainloop for all chassis apps 
 *
 * can be called directly or as gthread_* functions 
 */
CHASSIS_API int chassis_mainloop(void *user_data);

CHASSIS_API void chassis_set_shutdown_location(const gchar* location);
CHASSIS_API gboolean chassis_is_shutdown(void);

#define chassis_set_shutdown() chassis_set_shutdown_location(G_STRLOC)

/*@}*/

#endif
