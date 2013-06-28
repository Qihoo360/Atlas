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

#include <stdlib.h>
#include <string.h>
#ifdef HAVE_UNISTD_H
#include <unistd.h>
#endif
#include <errno.h>

#include <glib.h>
#include <gmodule.h>

#include "chassis-options.h"

/**
 * create a command-line option
 */
chassis_option_t *chassis_option_new() {
	chassis_option_t *opt;

	opt = g_slice_new0(chassis_option_t);

	return opt;
}

/**
 * free the option
 */
void chassis_option_free(chassis_option_t *opt) {
	if (!opt) return;

	if (opt->long_name) g_free(opt->long_name);
	if (opt->description) g_free(opt->description);
	if (opt->arg_description) g_free(opt->arg_description);

	g_slice_free(chassis_option_t, opt);
}

/**
 * add a option
 *
 * GOptionEntry
 */
int chassis_option_set(chassis_option_t *opt, 
		const char *long_name,
		gchar short_name,
		gint flags,
		GOptionArg arg,
		gpointer   arg_data,
		const char *description,
		const char *arg_description) {
	opt->long_name       = g_strdup(long_name);
	opt->short_name      = short_name;
	opt->flags           = flags;
	opt->arg             = arg;
	opt->arg_data        = arg_data;
	opt->description     = g_strdup(description);
	opt->arg_description = g_strdup(arg_description);

	return 0;
}

/**
 * create a command-line option
 */
chassis_options_t *chassis_options_new() {
	chassis_options_t *opt;

	opt = g_slice_new0(chassis_options_t);

	return opt;
}


/**
 * free the options
 */
void chassis_options_free(chassis_options_t *opts) {
	GList *node;

	if (!opts) return;

	/* free the options itself */
	for (node = opts->options; node; node = node->next) {
		chassis_option_t *opt = node->data;

		chassis_option_free(opt);
	}
	g_list_free(opts->options);

	g_slice_free(chassis_options_t, opts);
}

/**
 * add a option
 *
 * GOptionEntry
 */
int chassis_options_add_option(chassis_options_t *opts, 
		chassis_option_t *opt) {

	opts->options = g_list_append(opts->options, opt);

	return 0;
}

/**
 * convert the chassis_options into a GOptionEntry
 *
 * 
 *
 * @see chassis_options_free_g_option_entries
 */
GOptionEntry *chassis_options_to_g_option_entries(chassis_options_t *opts) {
	GOptionEntry *entries;
	int count;
	GList *node;

	/* build the GOptionEntry block */
	for (node = opts->options, count = 0; node; node = node->next) {
		count++;
	}

	entries = g_new0(GOptionEntry, count + 1);
	for (node = opts->options, count = 0; node; node = node->next) {
		chassis_option_t *opt = node->data;

		entries[count].long_name       = g_strdup(opt->long_name);
		entries[count].short_name      = opt->short_name;
		entries[count].flags           = opt->flags;
		entries[count].arg             = opt->arg;
		entries[count].arg_data        = opt->arg_data;
		entries[count].description     = g_strdup(opt->description);
		entries[count].arg_description = g_strdup(opt->arg_description);
		count++;
	}

	entries[count].long_name       = NULL;
	entries[count].short_name      = 0;
	entries[count].flags           = 0;
	entries[count].arg             = 0;
	entries[count].arg_data        = NULL;
	entries[count].description     = NULL;
	entries[count].arg_description = NULL;

	return entries;
}

/**
 * free the GOptionEntry created by chassis_options_to_g_option_entries()
 */
void chassis_options_free_g_option_entries(chassis_options_t G_GNUC_UNUSED *opts, GOptionEntry *entries) {
	GOptionEntry *entries_copy;

	if (NULL == entries)
		return;

	for (entries_copy = entries; entries_copy->long_name != NULL; entries_copy++) {
		/* type-cast to 'char *' to silence gcc's warning:
		 *
		 *   warning: passing argument 1 of 'g_free' discards qualifiers from pointer target type
		 *
		 * as the declaration of GOptionEntry is a 'const char *'
		 */
		if (NULL != entries_copy->long_name) g_free((char *)entries_copy->long_name);
		if (NULL != entries_copy->description) g_free((char *)entries_copy->description);
		if (NULL != entries_copy->arg_description) g_free((char *)entries_copy->arg_description);
	}

	g_free(entries);
}

int chassis_options_add(chassis_options_t *opts, 
		const char *long_name,
		gchar short_name,
		gint flags,
		GOptionArg arg,
		gpointer   arg_data,
		const char *description,
		const char *arg_description) {
	chassis_option_t *opt;

	opt = chassis_option_new();
	if (0 != chassis_option_set(opt,
			long_name,
			short_name,
			flags,
			arg,
			arg_data,
			description,
			arg_description) || 
	    0 != chassis_options_add_option(opts, opt)) {
		chassis_option_free(opt);
		return -1;
	} else {
		return 0;
	}
}


