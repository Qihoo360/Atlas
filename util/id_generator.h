#ifndef _ID_GENERATE_H_
#define _ID_GENERATE_H_
#include <glib.h>
#include <hiredis.h>
typedef struct id_generate_t id_generate_t;

struct id_generate_t {
    gchar *addr;
    gint port;
    void *conn; 
    void *(*open)(gchar *addr, gint port); 
    gboolean (*next)(void *conn, gchar *key, glong *v);
    void (*free)(void *);
};

static void *redis_open(gchar *addr, gint port);
//redis_set(void *conn, gchar *key, glong *value);
static gboolean redis_get(void *conn, gchar *key, glong *value);
static void redis_close(void *conn);

id_generate_t *id_generate_init(gchar *addr, gint port); 

//gboolean id_generate_next(void *conn, gchar *key, glong *v);
void id_generate_free(id_generate_t *);
#endif

