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

#ifndef __CHASSIS_FRONTEND_H__
#define __CHASSIS_FRONTEND_H__

#ifdef WIN32
#define CHASSIS_NEWLINE "\r\n"
#else
#define CHASSIS_NEWLINE "\n"
#endif

#include <glib.h>

#include "chassis-exports.h"
#include "chassis-options.h"

/**
 * @file
 * a collections of common functions used by chassis frontends 
 *
 * take a look at mysql-proxy-cli.c on what sequence to call these functions
 */

/**
 * setup glib, gthread and gmodule
 *
 * may abort of glib headers and glib libs don't match
 *
 * @return 0 on success, -1 on error
 */
CHASSIS_API int chassis_frontend_init_glib(void);

/**
 * setup win32 libs
 *
 * init winsock32 
 *
 * @return 0 on success, -1 on error
 */
CHASSIS_API int chassis_frontend_init_win32(void);

/**
 * setup and check the logdir
 */
CHASSIS_API int chassis_frontend_init_logdir(gchar *log_path);

/**
 * detect the basedir
 *
 * if *_basedir is not NULL, don't change it
 * otherwise extract the basedir from the prg_name
 *
 * @param prg_name program-name (usually argv[0])
 * @param _basedir user-supplied basedir 
 * @return 0 on success, -1 on error
 */
CHASSIS_API int chassis_frontend_init_basedir(const char *prg_name, char **_base_dir);

/**
 * open the configfile 
 *
 * @param filename       name of the configfile
 * @param gerr           a pointer to a clean GError * or NULL in case the possible error should be ignored
 *
 * @see g_key_file_free
 */
CHASSIS_API GKeyFile *chassis_frontend_open_config_file(const char *filename, GError **gerr);

CHASSIS_API int chassis_frontend_init_plugin_dir(char **_plugin_dir, const char *base_dir);
CHASSIS_API int chassis_frontend_init_lua_path(const char *set_path, const char *base_dir, char **lua_subdirs);
CHASSIS_API int chassis_frontend_init_lua_cpath(const char *set_path, const char *base_dir, char **lua_subdirs);

/**
 * extract --version and --defaults-file from comandline options
 *
 * @param option_ctx     a fresh GOptionContext
 * @param argc_p         pointer to the number of args in argv_p
 * @param argv_p         pointer to arguments to parse
 * @param print_version  pointer to int to set if --version is specified
 * @param config_file    pointer to char * to store if --defaults-file is specified
 * @param gerr           a pointer to a clean GError * or NULL in case the possible error should be ignored
 */
CHASSIS_API int chassis_frontend_init_base_options(GOptionContext *option_ctx,
		int *argc_p, char ***argv_p,
		int *print_version,
		char **config_file,
		GError **gerr);

/**
 * load the plugins
 *
 * loads the plugins from 'plugin_names' from the 'plugin_dir' and store their chassis_plugin structs
 * in 'plugins'
 *
 * the filename of the plugin is constructed based depending on the platform
 *
 * @param plugins      empty array
 * @param plugin_dir   directory to load the plugins from
 * @param plugin_names NULL terminated list of plugin names
 *
 * @see chassis_frontend_init_plugins
 */
CHASSIS_API int chassis_frontend_load_plugins(GPtrArray *plugins,
		const gchar *plugin_dir,
		gchar **plugin_names);

/**
 * init the loaded plugins and setup their config
 *
 * @param plugins        array of chassis_plugin structs
 * @param option_ctx     a fresh GOptionContext
 * @param argc_p         pointer to the number of args in argv_p
 * @param argv_p         pointer to arguments to parse
 * @param keyfile        the configfile 
 * @param base_dir       base directory
 * @param gerr           a pointer to a clean GError * or NULL in case the possible error should be ignored
 *
 * @see chassis_frontend_init_basedir, chassis_frontend_init_plugins
 */
CHASSIS_API int chassis_frontend_init_plugins(GPtrArray *plugins,
		GOptionContext *option_ctx,
		int *argc_p, char ***argv_p,
		GKeyFile *keyfile,
		const char *keyfile_section_name,
		const char *base_dir,
		GError **gerr);

/**
 * print the version of the program 
 */
CHASSIS_API int chassis_frontend_print_version(void);

/**
 * print the versions of the initialized plugins
 */
CHASSIS_API int chassis_frontend_print_plugin_versions(GPtrArray *plugins);

CHASSIS_API void chassis_frontend_print_lua_version();

/**
 * write the PID to a file
 *
 * @param  pid_file name of the PID file
 * @param  gerr     GError
 * @return 0 on success, -1 on error
 */
CHASSIS_API int chassis_frontend_write_pidfile(const char *pid_file, GError **gerr);

CHASSIS_API int chassis_options_set_cmdline_only_options(chassis_options_t *opts,
		int *print_version,
		char **config_file);

#endif
