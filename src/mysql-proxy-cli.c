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
 
/** @file
 * the user-interface for the MySQL Proxy @see main()
 *
 *  -  command-line handling 
 *  -  config-file parsing
 * 
 *
 * network_mysqld_thread() is the real proxy thread 
 * 
 * @todo move the SQL based help out into a lua script
 */

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include <sys/types.h>
#include <sys/stat.h>

#ifdef HAVE_SIGNAL_H
#include <signal.h>
#endif
#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <fcntl.h>
#include <errno.h>

#ifdef _WIN32
#include <process.h> /* getpid() */
#include <io.h>      /* open() */
#else
#include <unistd.h>
#include <sys/wait.h>
#include <sys/resource.h> /* for rusage in wait() */
#endif

#include <glib.h>
#include <gmodule.h>

#ifdef HAVE_LUA_H
#include <lua.h>
#include <stdio.h>
#endif

#ifdef HAVE_VALGRIND_VALGRIND_H
#include <valgrind/valgrind.h>
#endif

#ifndef HAVE_VALGRIND_VALGRIND_H
#define RUNNING_ON_VALGRIND 0
#endif

#include "network-mysqld.h"
#include "network-mysqld-proto.h"
#include "sys-pedantic.h"

#include "chassis-log.h"
#include "chassis-keyfile.h"
#include "chassis-mainloop.h"
#include "chassis-path.h"
#include "chassis-limits.h"
#include "chassis-filemode.h"
#include "chassis-win32-service.h"
#include "chassis-unix-daemon.h"
#include "chassis-frontend.h"
#include "chassis-options.h"

#ifdef WIN32
#define CHASSIS_NEWLINE "\r\n"
#else
#define CHASSIS_NEWLINE "\n"
#endif

#define GETTEXT_PACKAGE "mysql-proxy"

/**
 * options of the MySQL Proxy frontend
 */
typedef struct {
	int print_version;
	int verbose_shutdown;

	int daemon_mode;
	gchar *user;

	gchar *base_dir;
	int auto_base_dir;

	gchar *default_file;
	GKeyFile *keyfile;

	chassis_plugin *p;
	GOptionEntry *config_entries;

	gchar *pid_file;

	gchar *plugin_dir;
	gchar **plugin_names;

	guint invoke_dbg_on_crash;

#ifndef _WIN32
	/* the --keepalive option isn't available on Unix */
	guint auto_restart;
#endif

	gint max_files_number;

    gint max_conn_for_a_backend;

	gint event_thread_count;

	gchar *log_level;
	gchar *log_path;
	int    use_syslog;

	char *lua_path;
	char *lua_cpath;
	char **lua_subdirs;

	gchar *instance_name;

	gint wait_timeout;
} chassis_frontend_t;

/**
 * create a new the frontend for the chassis
 */
chassis_frontend_t *chassis_frontend_new(void) {
	chassis_frontend_t *frontend;

	frontend = g_slice_new0(chassis_frontend_t);
	frontend->event_thread_count = 1;
	frontend->max_files_number = 0;
	frontend->wait_timeout = 0;
    frontend->max_conn_for_a_backend = 0;

	return frontend;
}

/**
 * free the frontend of the chassis
 */
void chassis_frontend_free(chassis_frontend_t *frontend) {
	if (!frontend) return;

	if (frontend->keyfile) g_key_file_free(frontend->keyfile);
	if (frontend->default_file) g_free(frontend->default_file);
	if (frontend->base_dir) g_free(frontend->base_dir);
	if (frontend->user) g_free(frontend->user);
	if (frontend->pid_file) g_free(frontend->pid_file);
	if (frontend->log_level) g_free(frontend->log_level);
	if (frontend->plugin_dir) g_free(frontend->plugin_dir);

	g_strfreev(frontend->plugin_names);

	if (frontend->lua_path) g_free(frontend->lua_path);
	if (frontend->lua_cpath) g_free(frontend->lua_cpath);
	if (frontend->lua_subdirs) g_strfreev(frontend->lua_subdirs);
	if (frontend->instance_name) g_free(frontend->instance_name);

	g_slice_free(chassis_frontend_t, frontend);
}

/**
 * setup the options of the chassis
 */
int chassis_frontend_set_chassis_options(chassis_frontend_t *frontend, chassis_options_t *opts) {
	chassis_options_add(opts, "verbose-shutdown", 0, 0, G_OPTION_ARG_NONE, &(frontend->verbose_shutdown), "Always log the exit code when shutting down", NULL);
	chassis_options_add(opts, "daemon", 0, 0, G_OPTION_ARG_NONE, &(frontend->daemon_mode), "Start in daemon-mode", NULL);
	chassis_options_add(opts, "user", 0, 0, G_OPTION_ARG_STRING, &(frontend->user), "Run mysql-proxy as user", "<user>");
	chassis_options_add(opts, "basedir", 0, 0, G_OPTION_ARG_STRING, &(frontend->base_dir), "Base directory to prepend to relative paths in the config", "<absolute path>");
	chassis_options_add(opts, "plugin-dir", 0, 0, G_OPTION_ARG_STRING, &(frontend->plugin_dir), "path to the plugins", "<path>");
	chassis_options_add(opts, "plugins", 0, 0, G_OPTION_ARG_STRING_ARRAY, &(frontend->plugin_names), "plugins to load", "<name>");
	chassis_options_add(opts, "log-level", 0, 0, G_OPTION_ARG_STRING, &(frontend->log_level), "log all messages of level ... or higher", "(error|warning|info|message|debug)");
	chassis_options_add(opts, "log-path", 0, 0, G_OPTION_ARG_STRING, &(frontend->log_path), "log all messages in a path", "<path>");
	chassis_options_add(opts, "log-use-syslog", 0, 0, G_OPTION_ARG_NONE, &(frontend->use_syslog), "log all messages to syslog", NULL);
	chassis_options_add(opts, "log-backtrace-on-crash", 0, 0, G_OPTION_ARG_NONE, &(frontend->invoke_dbg_on_crash), "try to invoke debugger on crash", NULL);
	chassis_options_add(opts, "keepalive", 0, 0, G_OPTION_ARG_NONE, &(frontend->auto_restart), "try to restart the proxy if it crashed", NULL);
	chassis_options_add(opts, "max-open-files", 0, 0, G_OPTION_ARG_INT, &(frontend->max_files_number), "maximum number of open files (ulimit -n)", NULL);
	chassis_options_add(opts, "event-threads", 0, 0, G_OPTION_ARG_INT, &(frontend->event_thread_count), "number of event-handling threads (default: 1)", NULL);
	chassis_options_add(opts, "lua-path", 0, 0, G_OPTION_ARG_STRING, &(frontend->lua_path), "set the LUA_PATH", "<...>");
	chassis_options_add(opts, "lua-cpath", 0, 0, G_OPTION_ARG_STRING, &(frontend->lua_cpath), "set the LUA_CPATH", "<...>");
	chassis_options_add(opts, "instance", 0, 0, G_OPTION_ARG_STRING, &(frontend->instance_name), "instance name", "<name>");
	chassis_options_add(opts, "wait-timeout", 0, 0, G_OPTION_ARG_INT, &(frontend->wait_timeout), "the number of seconds which Atlas waits for activity on a connection before closing it (default:0)", NULL);
	chassis_options_add(opts, "max_conn_for_a_backend", 0, 0, G_OPTION_ARG_INT, &(frontend->max_conn_for_a_backend), "max conn for a backend(default: 0)", NULL);
    
	return 0;	
}

static void sigsegv_handler(int G_GNUC_UNUSED signum) {
	g_on_error_stack_trace(g_get_prgname());
	abort(); /* trigger a SIGABRT instead of just exiting */
}

/**
 * This is the "real" main which is called both on Windows and UNIX platforms.
 * For the Windows service case, this will also handle the notifications and set
 * up the logging support appropriately.
 */
int main_cmdline(int argc, char **argv) {
	chassis *srv = NULL;
#ifdef HAVE_SIGACTION
	static struct sigaction sigsegv_sa;
#endif
	/* read the command-line options */
	GOptionContext *option_ctx = NULL;
	GOptionEntry *main_entries = NULL;
	chassis_frontend_t *frontend = NULL;
	chassis_options_t *opts = NULL;

	GError *gerr = NULL;
	chassis_log *log = NULL;

	/* a little helper macro to set the src-location that we stepped out at to exit */
#define GOTO_EXIT(status) \
	exit_code = status; \
	exit_location = G_STRLOC; \
	goto exit_nicely;

	int exit_code = EXIT_SUCCESS;
	const gchar *exit_location = G_STRLOC;

	if (chassis_frontend_init_glib()) { /* init the thread, module, ... system */
		GOTO_EXIT(EXIT_FAILURE);
	}

	/* start the logging ... to stderr */
	log = chassis_log_new();
	log->min_lvl = G_LOG_LEVEL_MESSAGE; /* display messages while parsing or loading plugins */
	g_log_set_default_handler(chassis_log_func, log);

#ifdef _WIN32
	if (chassis_win32_is_service() && chassis_log_set_event_log(log, g_get_prgname())) {
		GOTO_EXIT(EXIT_FAILURE);
	}

	if (chassis_frontend_init_win32()) { /* setup winsock */
		GOTO_EXIT(EXIT_FAILURE);
	}
#endif

	/* may fail on library mismatch */
	if (NULL == (srv = chassis_new())) {
		GOTO_EXIT(EXIT_FAILURE);
	}

	srv->log = log; /* we need the log structure for the log-rotation */

	frontend = chassis_frontend_new();
	option_ctx = g_option_context_new("- MySQL Proxy");
	/**
	 * parse once to get the basic options like --defaults-file and --version
	 *
	 * leave the unknown options in the list
	 */
	if (chassis_frontend_init_base_options(option_ctx, &argc, &argv, &(frontend->print_version), &(frontend->default_file), &gerr)) {
		g_critical("%s: %s", G_STRLOC, gerr->message);
		g_clear_error(&gerr);
		GOTO_EXIT(EXIT_FAILURE);
	}

	if (frontend->default_file) {
		if (!(frontend->keyfile = chassis_frontend_open_config_file(frontend->default_file, &gerr))) {
			g_critical("%s: loading config from '%s' failed: %s", G_STRLOC, frontend->default_file, gerr->message);
			g_clear_error(&gerr);
			GOTO_EXIT(EXIT_FAILURE);
		}
	}

	/* print the main version number here, but don't exit
	 * we check for print_version again, after loading the plugins (if any)
	 * and print their version numbers, too. then we exit cleanly.
	 */
	if (frontend->print_version) {
#ifndef CHASSIS_BUILD_TAG
#define CHASSIS_BUILD_TAG PACKAGE_STRING
#endif
		g_print("%s" CHASSIS_NEWLINE, CHASSIS_BUILD_TAG); 
		chassis_frontend_print_version();
	}

	/* add the other options which can also appear in the configfile */
	opts = chassis_options_new();
	chassis_frontend_set_chassis_options(frontend, opts);
	main_entries = chassis_options_to_g_option_entries(opts);
	g_option_context_add_main_entries(option_ctx, main_entries, NULL);

	/**
	 * parse once to get the basic options 
	 *
	 * leave the unknown options in the list
	 */
	if (FALSE == g_option_context_parse(option_ctx, &argc, &argv, &gerr)) {
		g_critical("%s", gerr->message);
		GOTO_EXIT(EXIT_FAILURE);
	}

	if (frontend->keyfile) {
		if (chassis_keyfile_to_options(frontend->keyfile, "mysql-proxy", main_entries)) {
			GOTO_EXIT(EXIT_FAILURE);
		}
	}

	if (chassis_frontend_init_basedir(argv[0], &(frontend->base_dir))) {
		GOTO_EXIT(EXIT_FAILURE);
	}

	/* basic setup is done, base-dir is known, ... */
	frontend->lua_subdirs = g_new(char *, 2);
	frontend->lua_subdirs[0] = g_strdup("mysql-proxy");
	frontend->lua_subdirs[1] = NULL;

	if (chassis_frontend_init_lua_path(frontend->lua_path, frontend->base_dir, frontend->lua_subdirs)) {
		GOTO_EXIT(EXIT_FAILURE);
	}

	if (chassis_frontend_init_lua_cpath(frontend->lua_cpath, frontend->base_dir, frontend->lua_subdirs)) {
		GOTO_EXIT(EXIT_FAILURE);
	}

	/* make sure that he max-thread-count isn't negative */
	if (frontend->event_thread_count < 1) {
		g_critical("--event-threads has to be >= 1, is %d", frontend->event_thread_count);
		GOTO_EXIT(EXIT_FAILURE);
	}
	srv->event_thread_count = frontend->event_thread_count;

	if (frontend->wait_timeout < 0) {
		g_critical("--wait-timeout has to be >= 0, is %d", frontend->wait_timeout);
		GOTO_EXIT(EXIT_FAILURE);
	}
	srv->wait_timeout = frontend->wait_timeout;

	if (frontend->max_conn_for_a_backend <= 0) {
		g_critical("--max_conn_for_a_backend is %d, which means there is not any conn limitation for a backend", frontend->max_conn_for_a_backend);
	} else {
		g_message("--max_conn_for_a_backend is %d ", frontend->max_conn_for_a_backend);
    }

    srv->max_conn_for_a_backend = frontend->max_conn_for_a_backend;

	/* assign the mysqld part to the */
	network_mysqld_init(srv, frontend->default_file); /* starts the also the lua-scope, LUA_PATH and LUA_CPATH have to be set before this being called */

#ifdef HAVE_SIGACTION
	/* register the sigsegv interceptor */

	memset(&sigsegv_sa, 0, sizeof(sigsegv_sa));
	sigsegv_sa.sa_handler = sigsegv_handler;
	sigemptyset(&sigsegv_sa.sa_mask);

	if (frontend->invoke_dbg_on_crash && !(RUNNING_ON_VALGRIND)) {
		sigaction(SIGSEGV, &sigsegv_sa, NULL);
	}
#endif

	/*
	 * some plugins cannot see the chassis struct from the point
	 * where they open files, hence we must make it available
	 */
	srv->base_dir = g_strdup(frontend->base_dir);

	chassis_frontend_init_plugin_dir(&frontend->plugin_dir, srv->base_dir);
	
	/* 
	 * these are used before we gathered all the options
	 * from the plugins, thus we need to fix them up before
	 * dealing with all the rest.
	 */
	if (chassis_frontend_init_logdir(frontend->log_path)) {
		GOTO_EXIT(EXIT_FAILURE);
	}
	chassis_resolve_path(srv->base_dir, &frontend->log_path);
	chassis_resolve_path(srv->base_dir, &frontend->plugin_dir);

	/*
	 * start the logging and pid
	 */
	if (frontend->log_path) {
		if (frontend->instance_name == NULL) {
			gchar *default_file = frontend->default_file;

			gchar *slash = strrchr(default_file, '/');
			if (slash != NULL) ++slash;
			else slash = default_file;

			gchar *dot = strrchr(default_file, '.');
			if (dot != NULL && dot >= slash) frontend->instance_name = g_strndup(slash, dot-slash);
			else frontend->instance_name = g_strdup(slash);
		}
		log->log_filename = g_strdup_printf("%s/%s.log", frontend->log_path, frontend->instance_name);
		frontend->pid_file = g_strdup_printf("%s/%s.pid", frontend->log_path, frontend->instance_name);
		srv->log_path = g_strdup(frontend->log_path);
	}

	log->use_syslog = frontend->use_syslog;

	if (log->log_filename && log->use_syslog) {
		g_critical("%s: log-file and log-use-syslog were given, but only one is allowed", G_STRLOC);
		GOTO_EXIT(EXIT_FAILURE);
	}

	if (log->log_filename && FALSE == chassis_log_open(log)) {
		g_critical("can't open log-file '%s': %s", log->log_filename, g_strerror(errno));
		GOTO_EXIT(EXIT_FAILURE);
	}

	/* handle log-level after the config-file is read, just in case it is specified in the file */
	if (frontend->log_level) {
		if (0 != chassis_log_set_level(log, frontend->log_level)) {
			g_critical("--log-level=... failed, level '%s' is unknown ", frontend->log_level);
			GOTO_EXIT(EXIT_FAILURE);
		}
	} else {
		/* if it is not set, use "critical" as default */
		log->min_lvl = G_LOG_LEVEL_CRITICAL;
	}

	/*
	 * the MySQL Proxy should load 'admin' and 'proxy' plugins
	 */
	if (!frontend->plugin_names) {
		frontend->plugin_names = g_new(char *, 3);

		frontend->plugin_names[0] = g_strdup("admin");
		frontend->plugin_names[1] = g_strdup("proxy");
		frontend->plugin_names[2] = NULL;
	}

	if (chassis_frontend_load_plugins(srv->modules, frontend->plugin_dir, frontend->plugin_names)) {
		GOTO_EXIT(EXIT_FAILURE);
	}
/*
	if (chassis_frontend_init_plugins(srv->modules, option_ctx, &argc, &argv, frontend->keyfile, "mysql-proxy", srv->base_dir, &gerr)) {
		g_critical("%s: %s", G_STRLOC, gerr->message);
		g_clear_error(&gerr);
		GOTO_EXIT(EXIT_FAILURE);
	}
*/
	/* if we only print the version numbers, exit and don't do any more work */
	if (frontend->print_version) {
		chassis_frontend_print_lua_version();
		chassis_frontend_print_plugin_versions(srv->modules);
		GOTO_EXIT(EXIT_SUCCESS);
	}

	/* we know about the options now, lets parse them */
	g_option_context_set_help_enabled(option_ctx, TRUE);
	g_option_context_set_ignore_unknown_options(option_ctx, FALSE);

	/* handle unknown options */
	if (FALSE == g_option_context_parse(option_ctx, &argc, &argv, &gerr)) {
		if (gerr->domain == G_OPTION_ERROR && gerr->code == G_OPTION_ERROR_UNKNOWN_OPTION) {
			g_critical("%s: %s (use --help to show all options)", G_STRLOC, gerr->message);
		} else {
			g_critical("%s: %s (code = %d, domain = %s)", G_STRLOC, gerr->message, gerr->code, g_quark_to_string(gerr->domain));
		}
		
		GOTO_EXIT(EXIT_FAILURE);
	}
/*
	g_option_context_free(option_ctx);
	option_ctx = NULL;
*/
	/* after parsing the options we should only have the program name left */
	if (argc > 1) {
		g_critical("unknown option: %s", argv[1]);
		GOTO_EXIT(EXIT_FAILURE);
	}
	
#ifndef _WIN32	
	signal(SIGPIPE, SIG_IGN);

	if (frontend->daemon_mode) {
		chassis_unix_daemonize();
	}

	if (frontend->auto_restart) {
		int child_exit_status = EXIT_SUCCESS; /* forward the exit-status of the child */
		int ret = chassis_unix_proc_keepalive(&child_exit_status, frontend->pid_file);

		if (ret > 0) {
			/* the agent stopped */

			exit_code = child_exit_status;
			goto exit_nicely;
		} else if (ret < 0) {
			GOTO_EXIT(EXIT_FAILURE);
		} else {
			/* we are the child, go on */
		}
	}
#endif
	if (frontend->keyfile) {
		g_key_file_free(frontend->keyfile);
		frontend->keyfile = NULL;
	}
	if (frontend->default_file) {
		if (!(frontend->keyfile = chassis_frontend_open_config_file(frontend->default_file, &gerr))) {
			g_critical("%s: loading config from '%s' failed: %s", G_STRLOC, frontend->default_file, gerr->message);
			g_clear_error(&gerr);
			GOTO_EXIT(EXIT_FAILURE);
		}
	}
	if (chassis_frontend_init_plugins(srv->modules, option_ctx, &argc, &argv, frontend->keyfile, "mysql-proxy", srv->base_dir, &gerr)) {
		g_critical("%s: %s", G_STRLOC, gerr->message);
		g_clear_error(&gerr);
		GOTO_EXIT(EXIT_FAILURE);
	}
	g_option_context_free(option_ctx);
	option_ctx = NULL;

	if (frontend->pid_file && access(frontend->pid_file, F_OK)) {
		if (0 != chassis_frontend_write_pidfile(frontend->pid_file, &gerr)) {
			g_critical("%s", gerr->message);
			g_clear_error(&gerr);

			GOTO_EXIT(EXIT_FAILURE);
		}
	}

	// we need instance name that read by lua, may be use in lua log and ...
	srv->instance_name = g_strdup(frontend->instance_name);

	/* the message has to be _after_ the g_option_content_parse() to 
	 * hide from the output if the --help is asked for
	 */
	g_message("%s started - instance: %s", PACKAGE_STRING, srv->instance_name); /* add tag to the logfile (after we opened the logfile) */

#ifdef _WIN32
	if (chassis_win32_is_service()) chassis_win32_service_set_state(SERVICE_RUNNING, 0);
#endif

	/*
	 * we have to drop root privileges in chassis_mainloop() after
	 * the plugins opened the ports, so we need the user there
	 */
	srv->user = g_strdup(frontend->user);

	if (frontend->max_files_number) {
		if (0 != chassis_fdlimit_set(frontend->max_files_number)) {
			g_critical("%s: setting fdlimit = %d failed: %s (%d)", G_STRLOC, frontend->max_files_number, g_strerror(errno), errno);
			GOTO_EXIT(EXIT_FAILURE);
		}
	}
	g_debug("max open file-descriptors = %"G_GINT64_FORMAT, chassis_fdlimit_get());

	if (chassis_mainloop(srv)) {
		/* looks like we failed */
		g_critical("%s: Failure from chassis_mainloop. Shutting down.", G_STRLOC);
		GOTO_EXIT(EXIT_FAILURE);
	}

exit_nicely:
	/* necessary to set the shutdown flag, because the monitor will continue
	 * to schedule timers otherwise, causing an infinite loop in cleanup
	 */
	if (!exit_code) {
		exit_location = G_STRLOC;
	}
	chassis_set_shutdown_location(exit_location);

	if (!frontend->print_version) {
		g_log(G_LOG_DOMAIN, (frontend->verbose_shutdown ? G_LOG_LEVEL_CRITICAL : G_LOG_LEVEL_MESSAGE), "shutting down normally, exit code is: %d", exit_code); /* add a tag to the logfile */
	}

#ifdef _WIN32
	if (chassis_win32_is_service()) chassis_win32_service_set_state(SERVICE_STOP_PENDING, 0);
#endif

	if (gerr) g_error_free(gerr);
	if (option_ctx) g_option_context_free(option_ctx);
	if (srv) chassis_free(srv);
	if (opts) chassis_options_free(opts);
	if (main_entries) g_free(main_entries);

	chassis_log_free(log);
	
#ifdef _WIN32
	if (chassis_win32_is_service()) chassis_win32_service_set_state(SERVICE_STOPPED, 0);
#endif

#ifdef HAVE_SIGACTION
	/* reset the handler */
	sigsegv_sa.sa_handler = SIG_DFL;
	if (frontend->invoke_dbg_on_crash && !(RUNNING_ON_VALGRIND)) {
		sigaction(SIGSEGV, &sigsegv_sa, NULL);
	}
#endif
	chassis_frontend_free(frontend);	

	return exit_code;
}

/**
 * On Windows we first look if we are started as a service and 
 * set that up if appropriate.
 * We eventually fall down through to main_cmdline, even on Windows.
 */
int main(int argc, char **argv) {
#ifdef WIN32_AS_SERVICE
	return main_win32(argc, argv, main_cmdline);
#else
	return main_cmdline(argc, argv);
#endif
}

