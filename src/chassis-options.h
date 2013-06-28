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

#ifndef __CHASSIS_OPTIONS_H__
#define __CHASSIS_OPTIONS_H__

#include <glib.h>

#include "chassis-exports.h"

/**
 * @file
 *
 * a _new()/_free()-able version of GOptionEntry
 */

/**
 * 'const'-free version of GOptionEntry
 */
typedef struct {
	char *long_name;
	gchar short_name;
	gint flags;
	GOptionArg arg;
	gpointer   arg_data;
	char *description;
	char *arg_description;
} chassis_option_t;

/**
 * create a chassis_option_t 
 */
CHASSIS_API chassis_option_t *chassis_option_new(void);
CHASSIS_API void chassis_option_free(chassis_option_t *opt);
CHASSIS_API int chassis_option_set(chassis_option_t *opt, 
		const char *long_name,
		gchar short_name,
		gint flags,
		GOptionArg arg,
		gpointer   arg_data,
		const char *description,
		const char *arg_description);

typedef struct {
	GList *options; /* List of chassis_option_t */
	GOptionContext *ctx;
} chassis_options_t;

CHASSIS_API chassis_options_t *chassis_options_new(void);
CHASSIS_API void chassis_options_free(chassis_options_t *opts);
CHASSIS_API int chassis_options_add_option(chassis_options_t *opts, chassis_option_t *opt);
CHASSIS_API int chassis_options_add(chassis_options_t *opts, 
		const char *long_name,
		gchar short_name,
		gint flags,
		GOptionArg arg,
		gpointer   arg_data,
		const char *description,
		const char *arg_description);

CHASSIS_API GOptionEntry *chassis_options_to_g_option_entries(chassis_options_t *opts);
CHASSIS_API void chassis_options_free_g_option_entries(chassis_options_t G_GNUC_UNUSED *opts, GOptionEntry *entries);

#endif
