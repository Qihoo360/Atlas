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
 

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include <fcntl.h>
#include <stdlib.h>
#include <string.h>
#ifdef HAVE_UNISTD_H
#include <unistd.h>
#endif
#ifdef _WIN32
#include <windows.h>
#include <winsock2.h>
#include <io.h> /* open, close, ...*/
#include <process.h> /* getpid() */
#endif
#include <errno.h>

#include <glib.h>
#include <gmodule.h>
#include <lua.h> /* for LUA_PATH */
#include <lualib.h>
#include <lauxlib.h>

#include <event.h>

#include "chassis-frontend.h"
#include "chassis-path.h"
#include "chassis-plugin.h"
#include "chassis-keyfile.h"
#include "chassis-filemode.h"
#include "chassis-options.h"

#include "string-len.h"

/**
 * initialize the basic components of the chassis
 */
int chassis_frontend_init_glib() {
	const gchar *check_str = NULL;
#if 0
	g_mem_set_vtable(glib_mem_profiler_table);
#endif

	if (!GLIB_CHECK_VERSION(2, 6, 0)) {
		g_critical("the glib header are too old, need at least 2.6.0, got: %d.%d.%d", 
				GLIB_MAJOR_VERSION, GLIB_MINOR_VERSION, GLIB_MICRO_VERSION);

		return -1;
	}

	check_str = glib_check_version(GLIB_MAJOR_VERSION, GLIB_MINOR_VERSION, GLIB_MICRO_VERSION);

	if (check_str) {
		g_critical("%s, got: lib=%d.%d.%d, headers=%d.%d.%d", 
			check_str,
			glib_major_version, glib_minor_version, glib_micro_version,
			GLIB_MAJOR_VERSION, GLIB_MINOR_VERSION, GLIB_MICRO_VERSION);

		return -1;
	}

	if (!g_module_supported()) {
		g_critical("loading modules is not supported on this platform");
		return -1;
	}

	g_thread_init(NULL);

	return 0;
}

/**
 * init the win32 specific components
 *
 * - setup winsock32
 */
int chassis_frontend_init_win32() {
#ifdef _WIN32
	WSADATA wsaData;

	if (0 != WSAStartup(MAKEWORD( 2, 2 ), &wsaData)) {
		g_critical("%s: WSAStartup(2, 2) failed to initialize the socket library",
				G_STRLOC);

		return -1;
	}

	return 0;
#else
	return -1;
#endif
}

/**
 * setup and check the logdir
 */
int chassis_frontend_init_logdir(char *log_path) {
	if (!log_path) {
		g_critical("%s: Failed to get log directory, please set by --log-path",
				G_STRLOC);
		return -1;
	}

    return 0;
}

/**
 * setup and check the basedir if nessesary 
 */
int chassis_frontend_init_basedir(const char *prg_name, char **_base_dir) {
	char *base_dir = *_base_dir;

	if (base_dir) { /* basedir is already known, check if it is absolute */
		if (!g_path_is_absolute(base_dir)) {
			g_critical("%s: --basedir option must be an absolute path, but was %s",
					G_STRLOC,
					base_dir);
			return -1;
		} else {
			return 0;
		}
	}

	/* find our installation directory if no basedir was given
	 * this is necessary for finding files when we daemonize
	 */
	base_dir = chassis_get_basedir(prg_name);
	if (!base_dir) {
		g_critical("%s: Failed to get base directory",
				G_STRLOC);
		return -1;
	}

	*_base_dir = base_dir;

	return 0;

}

/**
 * set the environment as Lua expects it
 *
 * on Win32 glib uses _wputenv to set the env variable,
 * but Lua uses getenv. Those two don't see each other,
 * so we use _putenv. Since we only set ASCII chars, this
 * is safe.
 */
static int chassis_frontend_lua_setenv(const char *key, const char *value) {
	int r;
#if _WIN32
	r = _putenv_s(key, value);
#else
	r = g_setenv(key, value, 1) ? 0 : -1; /* g_setenv() returns TRUE/FALSE */
#endif

	if (0 == r) {
		/* the setenv() succeeded, double-check it */
		if (!getenv(key)) {
			/* check that getenv() returns what we did set */
			g_critical("%s: setting %s = %s failed: (getenv() == NULL)", G_STRLOC,
					key, value);
		} else if (0 != strcmp(getenv(key), value)) {
			g_critical("%s: setting %s = %s failed: (getenv() == %s)", G_STRLOC,
					key, value,
					getenv(key));
		}
	}

	return r;
}

/**
 * get the default value for LUA_PATH
 */
char *chassis_frontend_get_default_lua_path(const char *base_dir, const char *prg_name) {
	return g_build_filename(base_dir, "lib", prg_name, "lua", "?.lua", NULL);
}

/**
 * get the default value for LUA_PATH
 */
char *chassis_frontend_get_default_lua_cpath(const char *base_dir, const char *prg_name) {
	/* each OS has its own way of declaring a shared-lib extension
	 *
	 * win32 has .dll
	 * macosx has .so or .dylib
	 * hpux has .sl
	 */ 
#  if _WIN32
	return g_build_filename(base_dir, "bin", "lua-?." G_MODULE_SUFFIX, NULL);
#  else
	return g_build_filename(base_dir, "lib", prg_name, "lua", "?." G_MODULE_SUFFIX, NULL);
#  endif
}

/**
 * set the lua specific path env-variables
 *
 * if 'set_path' is not-NULL, we setenv() that path
 * otherwise we construct a value based on the sub_names and the basedir if the env-var is
 * not set yet
 *
 * @param set_path      if not NULL, set that path
 * @param base_dir      base directory to prepend to the sub_names
 * @param subdir_names  list of sub directories to add to the path
 * @param is_lua_path   TRUE if LUA_PATH should be set, FALSE if LUA_CPATH
 */
static int chassis_frontend_init_lua_paths(const char *set_path,
		const char *base_dir, char **lua_subdirs,
		gboolean is_lua_path) {
	const char *env_var = is_lua_path ? LUA_PATH : LUA_CPATH;
	int ret = 0;

	if (set_path) {
		if (0 != chassis_frontend_lua_setenv(env_var, set_path)) {
			g_critical("%s: setting %s = %s failed: %s", G_STRLOC,
					env_var, set_path,
					g_strerror(errno));
			ret = -1;
		}
	} else if (!g_getenv(env_var)) {
		GString *lua_path = g_string_new(NULL);
		guint i;
		gboolean all_in_one_folder = FALSE;

#ifdef _WIN32
		/**
		 * call the get_default_lua_cpath() only once on win32 as it has
		 * all the lua-module-DLLs in one folder
		 */
		if (!is_lua_path) all_in_one_folder = TRUE;
#endif

		/* build a path for each sub_name */
		for (i = 0; (all_in_one_folder && i == 0) || (!all_in_one_folder && lua_subdirs[i] != NULL); i++) {
			gchar *path;
			const char *sub_name = all_in_one_folder ? NULL : lua_subdirs[i];

			if (is_lua_path) {
				path = chassis_frontend_get_default_lua_path(base_dir, sub_name);
			} else {
				path = chassis_frontend_get_default_lua_cpath(base_dir, sub_name);
			}

			if (lua_path->len > 0) {
				g_string_append_len(lua_path, C(LUA_PATHSEP));
			}

			g_string_append(lua_path, path);

			g_free(path);
		}

		if (lua_path->len) {
			if (chassis_frontend_lua_setenv(env_var, lua_path->str)) {
				g_critical("%s: setting %s = %s failed: %s", G_STRLOC,
						env_var, lua_path->str,
						g_strerror(errno));
				ret = -1;
			}
		}
		g_string_free(lua_path, TRUE);
	}

	return 0;
}

/**
 * set the LUA_PATH 
 *
 * if 'set_path' is not-NULL, we setenv() that path
 * otherwise we construct a value based on the sub_names and the basedir if the env-var is
 * not set yet
 *
 * @param set_path      if not NULL, set that path
 * @param base_dir      base directory to prepend to the sub_names
 * @param subdir_names  list of sub directories to add to the path
  */
int chassis_frontend_init_lua_path(const char *set_path, const char *base_dir, char **lua_subdirs) {
	return chassis_frontend_init_lua_paths(set_path, base_dir, lua_subdirs, TRUE);
}

/**
 * set the LUA_CPATH 
 *
 * if 'set_path' is not-NULL, we setenv() that path
 * otherwise we construct a value based on the sub_names and the basedir if the env-var is
 * not set yet
 *
 * @param set_path      if not NULL, set that path
 * @param base_dir      base directory to prepend to the sub_names
 * @param subdir_names  list of sub directories to add to the path
 */
int chassis_frontend_init_lua_cpath(const char *set_path, const char *base_dir, char **lua_subdirs) {
	return chassis_frontend_init_lua_paths(set_path, base_dir, lua_subdirs, FALSE);
}

int chassis_frontend_init_plugin_dir(char **_plugin_dir, const char *base_dir) {
	char *plugin_dir = *_plugin_dir;

	if (plugin_dir) return 0;

#ifdef WIN32
	plugin_dir = g_build_filename(base_dir, "bin", NULL);
#else
	plugin_dir = g_build_filename(base_dir, "lib", PACKAGE, "plugins", NULL);
#endif

	*_plugin_dir = plugin_dir;

	return 0;
}

int chassis_frontend_load_plugins(GPtrArray *plugins, const gchar *plugin_dir, gchar **plugin_names) {
	int i;

	/* load the plugins */
	for (i = 0; plugin_names && plugin_names[i]; i++) {
		chassis_plugin *p;
#ifdef WIN32
#define G_MODULE_PREFIX "plugin-" /* we build the plugins with a prefix on win32 to avoid name-clashing in bin/ */
#else
#define G_MODULE_PREFIX "lib"
#endif
/* we have to hack around some glib distributions that
 * don't set the correct G_MODULE_SUFFIX, notably MacPorts
 */
#ifndef SHARED_LIBRARY_SUFFIX
#define SHARED_LIBRARY_SUFFIX G_MODULE_SUFFIX
#endif
		char *plugin_filename;
		/* skip trying to load a plugin when the parameter was --plugins= 
		   that will never work...
		*/
		if (!g_strcmp0("", plugin_names[i])) {
			continue;
		}

		plugin_filename = g_strdup_printf("%s%c%s%s.%s", 
				plugin_dir, 
				G_DIR_SEPARATOR, 
				G_MODULE_PREFIX,
				plugin_names[i],
				SHARED_LIBRARY_SUFFIX);

		p = chassis_plugin_load(plugin_filename);
		g_free(plugin_filename);

		if (NULL == p) {
			g_critical("setting --plugin-dir=<dir> might help");
			return -1;
		}
		p->option_grp_name = g_strdup(plugin_names[i]);

		g_ptr_array_add(plugins, p);
	}
	return 0;
}

int chassis_frontend_init_plugins(GPtrArray *plugins,
		GOptionContext *option_ctx,
		int *argc_p, char ***argv_p,
		GKeyFile *keyfile,
		const char *keyfile_section_name,
		const char *base_dir,
		GError **gerr) {
	guint i;

	for (i = 0; i < plugins->len; i++) {
		GOptionEntry *config_entries;
		chassis_plugin *p = plugins->pdata[i];

		if (NULL != (config_entries = chassis_plugin_get_options(p))) {
			gchar *group_desc = g_strdup_printf("%s-module", p->option_grp_name);
			gchar *help_msg = g_strdup_printf("Show options for the %s-module", p->option_grp_name);
			const gchar *group_name = p->option_grp_name;

			GOptionGroup *option_grp = g_option_group_new(group_name, group_desc, help_msg, NULL, NULL);
			g_option_group_add_entries(option_grp, config_entries);
			g_option_context_add_group(option_ctx, option_grp);

			g_free(help_msg);
			g_free(group_desc);

			/* parse the new options */
			if (FALSE == g_option_context_parse(option_ctx, argc_p, argv_p, gerr)) {
				return -1;
			}
	
			if (keyfile) {
				if (chassis_keyfile_to_options(keyfile, keyfile_section_name, config_entries)) {
					return -1;
				}
			}

			/* resolve the path names for these config entries */
			chassis_keyfile_resolve_path(base_dir, config_entries); 
		}
	}

	return 0;
}


int chassis_frontend_init_base_options(GOptionContext *option_ctx,
		int *argc_p, char ***argv_p,
		int *print_version,
		char **config_file,
		GError **gerr) {
	chassis_options_t *opts;
	GOptionEntry *base_main_entries;
	int ret = 0;

	opts = chassis_options_new();
	chassis_options_set_cmdline_only_options(opts, print_version, config_file);
	base_main_entries = chassis_options_to_g_option_entries(opts);

	g_option_context_add_main_entries(option_ctx, base_main_entries, NULL);
	g_option_context_set_help_enabled(option_ctx, FALSE);
	g_option_context_set_ignore_unknown_options(option_ctx, TRUE);

	if (FALSE == g_option_context_parse(option_ctx, argc_p, argv_p, gerr)) {
		ret = -1;
	}

	/* do not use chassis_options_free_g_options... here, we need to hang on to the data until the end of the program! */
	g_free(base_main_entries);
	chassis_options_free(opts);

	return ret;
}

GKeyFile *chassis_frontend_open_config_file(const char *filename, GError **gerr) {
	GKeyFile *keyfile;
/*
	if (chassis_filemode_check_full(filename, CHASSIS_FILEMODE_SECURE_MASK, gerr) != 0) {
		return NULL;
	}
*/
	keyfile = g_key_file_new();
	g_key_file_set_list_separator(keyfile, ',');

	if (FALSE == g_key_file_load_from_file(keyfile, filename, G_KEY_FILE_NONE, gerr)) {
		g_key_file_free(keyfile);

		return NULL;
	}

	return keyfile;
}

/**
 * setup the options that can only appear on the command-line
 */
int chassis_options_set_cmdline_only_options(chassis_options_t *opts,
		int *print_version,
		char **config_file) {

	chassis_options_add(opts,
		"version", 'V', 0, G_OPTION_ARG_NONE, print_version, "Show version", NULL);

	chassis_options_add(opts,
		"defaults-file", 0, 0, G_OPTION_ARG_STRING, config_file, "configuration file", "<file>");

	return 0;
}


int chassis_frontend_print_version() {
	/* allow to pass down a build-tag at build-time which gets hard-coded into the binary */
#ifndef CHASSIS_BUILD_TAG
#define CHASSIS_BUILD_TAG PACKAGE_STRING
#endif
	g_print("  chassis: %s" CHASSIS_NEWLINE, CHASSIS_BUILD_TAG); 
	g_print("  glib2: %d.%d.%d" CHASSIS_NEWLINE, GLIB_MAJOR_VERSION, GLIB_MINOR_VERSION, GLIB_MICRO_VERSION);
	g_print("  libevent: %s" CHASSIS_NEWLINE, event_get_version());

	return 0;
}

int chassis_frontend_print_plugin_versions(GPtrArray *plugins) {
	guint i;

	g_print("-- modules" CHASSIS_NEWLINE);

	for (i = 0; i < plugins->len; i++) {
		chassis_plugin *p = plugins->pdata[i];

		g_print("  %s: %s" CHASSIS_NEWLINE, p->name, p->version); 
	}

	return 0;
}

void chassis_frontend_print_lua_version() {
	lua_State *L;

	g_print("  LUA: %s" CHASSIS_NEWLINE, LUA_RELEASE);
	L = luaL_newstate();
	luaL_openlibs(L);
	lua_getglobal(L, "package");
	g_assert_cmpint(lua_type(L, -1), ==, LUA_TTABLE);

	lua_getfield(L, -1, "path");
	g_assert_cmpint(lua_type(L, -1), ==, LUA_TSTRING);
	g_print("    package.path: %s" CHASSIS_NEWLINE, lua_tostring(L, -1));
	lua_pop(L, 1);

	lua_getfield(L, -1, "cpath");
	g_assert_cmpint(lua_type(L, -1), ==, LUA_TSTRING);
	g_print("    package.cpath: %s" CHASSIS_NEWLINE, lua_tostring(L, -1));
	lua_pop(L, 2);

	lua_close(L);
}


int chassis_frontend_write_pidfile(const char *pid_file, GError **gerr) {
	int fd;
	int ret = 0;

	gchar *pid_str;

	/**
	 * write the PID file
	 */

	if (-1 == (fd = open(pid_file, O_WRONLY|O_TRUNC|O_CREAT, 0600))) {
		g_set_error(gerr,
				G_FILE_ERROR,
				g_file_error_from_errno(errno),
				"%s: open(%s) failed: %s", 
				G_STRLOC,
				pid_file,
				g_strerror(errno));

		return -1;
	}

	pid_str = g_strdup_printf("%d", getpid());

	if (write(fd, pid_str, strlen(pid_str)) < 0) {
		g_set_error(gerr,
				G_FILE_ERROR,
				g_file_error_from_errno(errno),
				"%s: write(%s) of %s failed: %s", 
				G_STRLOC,
				pid_file,
				pid_str,
				g_strerror(errno));
		ret = -1;
	}
	g_free(pid_str);

	close(fd);

	return ret;
}

