#include <glib.h>
#include "json.h"
#include "json_util.h"
#include "save_config_file.h" 


json_object *open_file(gchar *file) {
    if (NULL == file) {
        return NULL;
    }
    json_object *jso = json_object_from_file(file);
    return jso;
}
gint set_json_object_bool(json_object *obj, gchar *k, gboolean v) {
    gint ret = 0;
    if (NULL == obj || NULL == k) {
        return ret;
    }
    json_object *obj_  = json_object_object_get(obj, k);
    if (NULL == obj_ || json_object_is_type(obj_, json_type_boolean)) {
        obj_ = json_object_new_boolean(v); 
        json_object_object_add(obj, k, obj_); 
        ret = 1;
    } else {
        ret = json_object_set_boolean(obj_, v);
    } 
    return ret;
}

gint set_json_object_int(json_object *obj, gchar *k, gint v) {
    gint ret = 0; 
    if (NULL == obj || NULL == k) {
        return ret;
    } 

    json_object *obj_ = json_object_object_get(obj, k);
    if (NULL == obj_ || FALSE == json_object_is_type(obj_, json_type_int)) {
        obj_ = json_object_new_int(v); 
        json_object_object_add(obj, k, obj_); 
        ret = 1;
    } else {
        ret = json_object_set_int(obj_, v);
    }

    return ret;
}
gint set_json_object_string(json_object *obj, gchar *k, gchar *v) {
    gint ret = 0;  

    if (NULL == obj || NULL == k || NULL == v) {
        return ret;
    } 
    json_object *obj_  = json_object_object_get(obj, k);
    if (FALSE == json_object_is_type(obj_, json_type_string)) {
        obj_ = json_object_new_string(v); 
        json_object_object_add(obj, k, obj_); 
        ret = 1;
    } else {
        ret = json_object_set_string(obj_, v);
    }
    return  ret;
}
gint set_json_object_string_array(json_object *obj, gchar *k, GPtrArray *arr) {
    gint ret = 0; 
    if (NULL == obj || NULL == arr) {
        return ret;
    }
    ret = 1;
    json_object *obj_ = json_object_object_get(obj, k);
    if (NULL == obj_) {
        obj_ = json_object_new_array(); 
        json_object_object_add(obj, k, obj_);
    }

    gint len = json_object_array_length(obj_);
    gint i; 
    for (i  = 0 ;i < arr->len; i++) {
        gchar *ch = g_ptr_array_index(arr, i); 
        if (NULL == ch) {
            continue;
        }
        json_object_array_put_idx(obj_, i, json_object_new_string(ch));
    }

    if (i < len) {
        json_object_array_del_idx(obj_, i, len - i);
    }
    return ret;
}
void save_config_file(json_object *fd, gchar *file) {
    if (NULL == fd || NULL == file) {
        return; 
    } 
    json_object_to_file_ext(file, fd, JSON_C_TO_STRING_PRETTY);
    json_object_put(fd);
    return;
}
