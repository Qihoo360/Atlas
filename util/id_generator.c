#include "id_generator.h" 


//TODO(dengyihao):
static void *redis_open(gchar *addr, gint port) {
    redisContext *conn = NULL; 
    if (NULL == addr) {
        return NULL;
    }
    conn = redisConnect(addr, port);
    if (conn->err) {
        redisFree(conn);
        return NULL;
    }
    return (void *)conn;
} 

static gboolean redis_get(void *conn, gchar *k, glong *v) {
    redisContext *conn_ = conn;
    gboolean ret = FALSE; 
    if (NULL == conn_ || NULL == k) {
        return ret;
    } 

    redisReply *r = (redisReply*)redisCommand(conn_, "get %s", k);
    switch (r->type) {
        case REDIS_REPLY_STRING:
            *v = atol(r->str);
            ret = TRUE;
            break;
        case REDIS_REPLY_INTEGER:
            *v = r->integer;
            ret = TRUE;
            break;
        case REDIS_REPLY_NIL: 
            {// if key's value is nil, initial it  
                *v = 1L;
                redisReply *r_ = (redisReply *)redisCommand(conn_, "set %s %ld", k, *v);
                ret = (REDIS_REPLY_STATUS == r_->type && 0 == g_strcasecmp(r_->str, "OK"));
                freeReplyObject(r_);
                break;
            }
        default:
            break;
    }
    freeReplyObject(r);
    return ret;
}    

static void redis_close(void *conn) {
    redisContext *conn_ = conn;
    if (NULL == conn_) {
        return;
    } 
    redisFree(conn_); 
} 

id_generate_t *id_generate_init(gchar *addr, gint port) { 
    id_generate_t *t = g_slice_new(id_generate_t); 
    t->addr = addr;
    t->port = port;
    // inii handle 
    t->open = redis_open;
    t->next = redis_get;
    t->free = redis_close;
    
    t->conn = t->open(addr, port);
    return t;
}


void id_generate_free(id_generate_t *generator) {
    if (NULL == generator) { 
        return; 
    }  
    generator->free(generator->conn);
    g_slice_free(id_generate_t, generator);
}  
