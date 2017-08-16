#ifndef __SAVA_CONFIG_FILE_H__
#define __SAVA_CONFIG_FILE_H__

#include <glib.h>
#include "json.h"
#include "json_util.h"
json_object *open_file(gchar *file);
gint set_jsonobject_bool(json_object *obj, gchar *k, gboolean v);
gint set_json_object_int(json_object *obj, gchar *k, gint v);
gint set_json_object_string(json_object *obj, gchar *k, gchar *v);
gint set_json_object_string_array(json_object *obj, gchar *k, GPtrArray *arr);
void save_config_file(json_object *fd, gchar *name);

#endif
