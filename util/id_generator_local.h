#ifndef _ID_GENERATOR_LOCAL_H_
#define _ID_GENERATOR_LOCAL_H_

#include <glib.h>

typedef struct id_generators_wrapper_t id_generators_wrapper_t;
typedef struct id_generator_t id_generator_t; 

struct id_generator_t {
    gint64 last_time_stamp;    
    gint64 inc;
    gint64 machine_id;
    gint64 centor_id; 
    GMutex mutex;
};
struct id_generators_wrapper_t {
    GHashTable *table;
    GRWLock rw_mutex;
    guint machine_id;
};

gboolean unique_id_generator(id_generators_wrapper_t *generators, gchar *key, gint64 *value);
gboolean id_generators_init(id_generators_wrapper_t *);

#endif
