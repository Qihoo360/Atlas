/* $%BEGINLICENSE%$
 Copyright (c) 2007, 2009, Oracle and/or its affiliates. All rights reserved.

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
 

#include "chassis-path.h"
#include "chassis-keyfile.h"

int chassis_keyfile_to_options(GKeyFile *keyfile, const gchar *ini_group_name, GOptionEntry *config_entries) {
	GError *gerr = NULL;
	int ret = 0;
	int i, j;
	
	/* all the options are in the group for "mysql-proxy" */

	if (!keyfile) return -1;
	if (!g_key_file_has_group(keyfile, ini_group_name)) return 0;

	/* set the defaults */
	for (i = 0; config_entries[i].long_name; i++) {
		GOptionEntry *entry = &(config_entries[i]);
		gchar *arg_string;
		gchar **arg_string_array;
		gboolean arg_bool = 0;
		gint arg_int = 0;
		gdouble arg_double = 0;
		gsize len = 0;

		switch (entry->arg) {
		case G_OPTION_ARG_FILENAME:
		case G_OPTION_ARG_STRING: 
			/* is this option set already */
			if (NULL == entry->arg_data || NULL != *(gchar **)(entry->arg_data)) break;

			arg_string = g_key_file_get_string(keyfile, ini_group_name, entry->long_name, &gerr);
			if (!gerr) {
				/* strip trailing spaces */
				*(gchar **)(entry->arg_data) = g_strchomp(arg_string);
			}
			break;
		case G_OPTION_ARG_FILENAME_ARRAY:
		case G_OPTION_ARG_STRING_ARRAY: 
			/* is this option set already */
			if (NULL == entry->arg_data || NULL != *(gchar ***)(entry->arg_data)) break;

			arg_string_array = g_key_file_get_string_list(keyfile, ini_group_name, entry->long_name, &len, &gerr);
			if (!gerr) {
				for (j = 0; arg_string_array[j]; j++) {
					arg_string_array[j] = g_strstrip(arg_string_array[j]);
				}	
				*(gchar ***)(entry->arg_data) = arg_string_array;
			}
			break;
		case G_OPTION_ARG_NONE: 
			arg_bool = g_key_file_get_boolean(keyfile, ini_group_name, entry->long_name, &gerr);
			if (!gerr) {
				*(int *)(entry->arg_data) = arg_bool;
			}
			break;
		case G_OPTION_ARG_INT: 
			arg_int = g_key_file_get_integer(keyfile, ini_group_name, entry->long_name, &gerr);
			if (!gerr) {
				*(gint *)(entry->arg_data) = arg_int;
			}
			break;
#if GLIB_MAJOR_VERSION >= 2 && GLIB_MINOR_VERSION >= 12 
		case G_OPTION_ARG_DOUBLE: 
			arg_double = g_key_file_get_double(keyfile, ini_group_name, entry->long_name, &gerr);
			if (!gerr) {
				*(gint *)(entry->arg_data) = arg_double;
			}
			break;
#endif
		default:
			g_error("%s: (keyfile) the option %d can't be handled", G_STRLOC, entry->arg);
			break;
		}

		if (gerr) {
			if (gerr->code != G_KEY_FILE_ERROR_KEY_NOT_FOUND) {
				g_message("%s", gerr->message);
				ret = -1;
			}

			g_error_free(gerr);
			gerr = NULL;
		}
	}

	return ret;
}

/* check for relative paths among the newly added options
 * and resolve them to an absolute path if we have --basedir
 */
int chassis_keyfile_resolve_path(const char *base_dir, GOptionEntry *config_entries) {
	int entry_idx;

	for (entry_idx = 0; config_entries[entry_idx].long_name; entry_idx++) {
		GOptionEntry entry = config_entries[entry_idx];
		
		switch(entry.arg) {
		case G_OPTION_ARG_FILENAME: {
			gchar **data = entry.arg_data;
			chassis_resolve_path(base_dir, data);
			break;
		}
		case G_OPTION_ARG_FILENAME_ARRAY: {
			gchar ***data = entry.arg_data;
			gchar **files = *data;
			if (NULL != files) {
				gint j;
				for (j = 0; files[j]; j++) chassis_resolve_path(base_dir, &files[j]);
			}
			break;
		}
		default:
			/* ignore other option types */
			break;
		}
	}

	return 0;
}
