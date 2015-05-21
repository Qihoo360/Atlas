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
 

#include <sys/types.h>
#include <signal.h>
#include <string.h>
#include <errno.h>
#include <stdio.h>

#ifndef _WIN32
#include <unistd.h>
#endif

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#ifdef HAVE_SYS_TIME_H
#include <sys/time.h> /* event.h need struct timeval */
#endif

#ifdef HAVE_PWD_H
#include <pwd.h>	 /* getpwnam() */
#endif

#include <glib.h>
#include "chassis-plugin.h"
#include "chassis-mainloop.h"
#include "chassis-event-thread.h"
#include "chassis-log.h"
#include "chassis-stats.h"
#include "chassis-timings.h"

#ifdef _WIN32
static volatile int signal_shutdown;
#else
static volatile sig_atomic_t signal_shutdown;
#endif

/**
 * check if the libevent headers we built against match the 
 * library we run against
 */
int chassis_check_version(const char *lib_version, const char *hdr_version) {
	int lib_maj, lib_min, lib_pat;
	int hdr_maj, hdr_min, hdr_pat;
	int scanned_fields;

	if (3 != (scanned_fields = sscanf(lib_version, "%d.%d.%d%*s", &lib_maj, &lib_min, &lib_pat))) {
		g_critical("%s: library version %s failed to parse: %d",
				G_STRLOC, lib_version, scanned_fields);
		return -1;
	}
	if (3 != (scanned_fields = sscanf(hdr_version, "%d.%d.%d%*s", &hdr_maj, &hdr_min, &hdr_pat))) {
		g_critical("%s: header version %s failed to parse: %d",
				G_STRLOC, hdr_version, scanned_fields);
		return -1;
	}
	
	if (lib_maj == hdr_maj &&
	    lib_min == hdr_min &&
	    lib_pat >= hdr_pat) {
		return 0;
	}

	return -1;
}


/**
 * create a global context
 */
chassis *chassis_new() {
	chassis *chas;

	if (0 != chassis_check_version(event_get_version(), _EVENT_VERSION)) {
		g_critical("%s: chassis is build against libevent %s, but now runs against %s",
				G_STRLOC, _EVENT_VERSION, event_get_version());
		return NULL;
	}

	chas = g_new0(chassis, 1);

	chas->modules = g_ptr_array_new();
	
	chas->stats = chassis_stats_new();

	/* create a new global timer info */
	chassis_timestamps_global_init(NULL);

	chas->threads = g_ptr_array_new();

	chas->event_hdr_version = g_strdup(_EVENT_VERSION);

	chas->shutdown_hooks = chassis_shutdown_hooks_new();

	return chas;
}


/**
 * free the global scope
 *
 * closes all open connections, cleans up all plugins
 *
 * @param chas      global context
 */
void chassis_free(chassis *chas) {
	guint i;
#ifdef HAVE_EVENT_BASE_FREE
	const char *version;
#endif

	if (!chas) return;

	/* call the destructor for all plugins */
	for (i = 0; i < chas->modules->len; i++) {
		chassis_plugin *p = chas->modules->pdata[i];

		g_assert(p->destroy);
		p->destroy(p->config);
	}
	
	chassis_shutdown_hooks_call(chas->shutdown_hooks); /* cleanup the global 3rd party stuff before we unload the modules */

	for (i = 0; i < chas->modules->len; i++) {
		chassis_plugin *p = chas->modules->pdata[i];

		chassis_plugin_free(p);
	}
	
	g_ptr_array_free(chas->modules, TRUE);

	if (chas->base_dir) g_free(chas->base_dir);
	if (chas->log_path) g_free(chas->log_path);
	if (chas->user) g_free(chas->user);
	
	if (chas->stats) chassis_stats_free(chas->stats);

	chassis_timestamps_global_free(NULL);

	GPtrArray *threads = chas->threads;
	if (threads) {
		for (i = 0; i < threads->len; ++i) {
			chassis_event_thread_free(threads->pdata[i]);
		}

		g_ptr_array_free(threads, TRUE);
	}

	if (chas->instance_name) g_free(chas->instance_name);

#ifdef HAVE_EVENT_BASE_FREE
	/* only recent versions have this call */

	version = event_get_version();

	/* libevent < 1.3e doesn't cleanup its own fds from the event-queue in signal_init()
	 * calling event_base_free() would cause a assert() on shutdown
	 */
	if (version && (strcmp(version, "1.3e") >= 0)) {
		if (chas->event_base) event_base_free(chas->event_base);
	}
#endif
	g_free(chas->event_hdr_version);

	chassis_shutdown_hooks_free(chas->shutdown_hooks);

	lua_scope_free(chas->sc);

	network_backends_free(chas->backends);

	g_free(chas);
}

void chassis_set_shutdown_location(const gchar* location) {
	if (signal_shutdown == 0) g_message("Initiating shutdown, requested from %s", (location != NULL ? location : "signal handler"));
	signal_shutdown = 1;
}

gboolean chassis_is_shutdown() {
	return signal_shutdown == 1;
}

static void sigterm_handler(int G_GNUC_UNUSED fd, short G_GNUC_UNUSED event_type, void G_GNUC_UNUSED *_data) {
	chassis_set_shutdown_location(NULL);
}

static void sighup_handler(int G_GNUC_UNUSED fd, short G_GNUC_UNUSED event_type, void *_data) {
	chassis *chas = _data;

	g_message("received a SIGHUP, closing log file"); /* this should go into the old logfile */

	chassis_log_set_logrotate(chas->log);
	
	g_message("re-opened log file after SIGHUP"); /* ... and this into the new one */
}


/**
 * forward libevent messages to the glib error log 
 */
static void event_log_use_glib(int libevent_log_level, const char *msg) {
	/* map libevent to glib log-levels */

	GLogLevelFlags glib_log_level = G_LOG_LEVEL_DEBUG;

	if (libevent_log_level == _EVENT_LOG_DEBUG) glib_log_level = G_LOG_LEVEL_DEBUG;
	else if (libevent_log_level == _EVENT_LOG_MSG) glib_log_level = G_LOG_LEVEL_MESSAGE;
	else if (libevent_log_level == _EVENT_LOG_WARN) glib_log_level = G_LOG_LEVEL_WARNING;
	else if (libevent_log_level == _EVENT_LOG_ERR) glib_log_level = G_LOG_LEVEL_CRITICAL;
    
    /*only error to be logged*/
    if (libevent_log_level == _EVENT_LOG_ERR) {
        g_log(G_LOG_DOMAIN, glib_log_level, "(libevent) %s", msg);
    }
}


int chassis_mainloop(void *_chas) {
	chassis *chas = _chas;
	guint i;
	struct event ev_sigterm, ev_sigint;
#ifdef SIGHUP
	struct event ev_sighup;
#endif
	chassis_event_thread_t *mainloop_thread;

	/* redirect logging from libevent to glib */
	event_set_log_callback(event_log_use_glib);


	/* add a event-handler for the "main" events */
	mainloop_thread = chassis_event_thread_new(0);
	chassis_event_threads_init_thread(mainloop_thread, chas);
	g_ptr_array_add(chas->threads, mainloop_thread);

	chas->event_base = mainloop_thread->event_base; /* all global events go to the 1st thread */

	g_assert(chas->event_base);


	/* setup all plugins all plugins */
	for (i = 0; i < chas->modules->len; i++) {
		chassis_plugin *p = chas->modules->pdata[i];

		g_assert(p->apply_config);
		if (0 != p->apply_config(chas, p->config)) {
			g_critical("%s: applying config of plugin %s failed",
					G_STRLOC, p->name);
			return -1;
		}
	}

	/*
	 * drop root privileges if requested
	 */
#ifndef _WIN32
	if (chas->user) {
		struct passwd *user_info;
		uid_t user_id= geteuid();

		/* Don't bother if we aren't superuser */
		if (user_id) {
			g_critical("can only use the --user switch if running as root");
			return -1;
		}

		if (NULL == (user_info = getpwnam(chas->user))) {
			g_critical("unknown user: %s", chas->user);
			return -1;
		}

		if (chas->log->log_filename) {
			/* chown logfile */
			if (-1 == chown(chas->log->log_filename, user_info->pw_uid, user_info->pw_gid)) {
				g_critical("%s.%d: chown(%s) failed: %s",
							__FILE__, __LINE__,
							chas->log->log_filename,
							g_strerror(errno) );

				return -1;
			}
		}

		setgid(user_info->pw_gid);
		setuid(user_info->pw_uid);
		g_debug("now running as user: %s (%d/%d)",
				chas->user,
				user_info->pw_uid,
				user_info->pw_gid );
	}
#endif

	signal_set(&ev_sigterm, SIGTERM, sigterm_handler, NULL);
	event_base_set(chas->event_base, &ev_sigterm);
	signal_add(&ev_sigterm, NULL);

	/* signal_set(&ev_sigint, SIGINT, sigterm_handler, NULL); */
	/* event_base_set(chas->event_base, &ev_sigint); */
	/* signal_add(&ev_sigint, NULL); */

#ifdef SIGHUP
	signal_set(&ev_sighup, SIGHUP, sighup_handler, chas);
	event_base_set(chas->event_base, &ev_sighup);
	if (signal_add(&ev_sighup, NULL)) {
		g_critical("%s: signal_add(SIGHUP) failed", G_STRLOC);
	}
#endif

	if (chas->event_thread_count < 1) chas->event_thread_count = 1;

	/* create the event-threads
	 *
	 * - dup the async-queue-ping-fds
	 * - setup the events notification
	 * */
	for (i = 1; i <= (guint)chas->event_thread_count; i++) { /* we already have 1 event-thread running, the main-thread */
		chassis_event_thread_t *thread = chassis_event_thread_new(i);
		chassis_event_threads_init_thread(thread, chas);
		g_ptr_array_add(chas->threads, thread);
	}

	/* start the event threads */
	chassis_event_threads_start(chas->threads);

	/**
	 * handle signals and all basic events into the main-thread
	 *
	 * block until we are asked to shutdown
	 */
	chassis_event_thread_loop(mainloop_thread);

	signal_del(&ev_sigterm);
	signal_del(&ev_sigint);
#ifdef SIGHUP
	signal_del(&ev_sighup);
#endif
	return 0;
}
