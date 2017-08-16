
#include <sys/time.h>
#include <time.h>
#include "id_generator_local.h"

gint64 twepoch;
gint64 workerIdBits; /** machine id */
gint64 datacenterIdBits;/**datacenter id*/
gint64 maxWorkerId;
gint64 maxDatacenterId;
gint64 sequenceBits;
gint64 workerIdShift; 
gint64 datacenterIdShift;
gint64 timestampLeftShift;
gint64 sequenceMask;

gint64 current_timestamp_get(void) {
    struct timeval tv; 
    gettimeofday(&tv, NULL);
    return (gint64)tv.tv_sec * 1000 + (gint64)tv.tv_usec / 1000;
}

id_generator_t *id_generator_new(gint64 machine_id, gint64 centor_id) {
    id_generator_t *t = g_slice_new(id_generator_t);
    t->machine_id = machine_id;
    t->inc = 0L;
    t->centor_id = centor_id;
    t->last_time_stamp = -1L; 
    g_mutex_init(&t->mutex);
}
void generator_free(gpointer value) {
    id_generator_t *id_gen = (id_generator_t *)value;    
    g_mutex_clear(&id_gen->mutex);
    g_slice_free(id_generator_t, id_gen);
}

gboolean id_generators_init(id_generators_wrapper_t *generators) {
    if (NULL == generators) {
        return FALSE;
    }

    generators->table = g_hash_table_new_full(g_str_hash, g_str_equal, g_free, generator_free);
    if (NULL == generators->table) {
        return FALSE;
    }
    g_rw_lock_init(&generators->rw_mutex);

    // init const value
    twepoch = 1420041600000L;
    workerIdBits = 5L; /** machine id */
    datacenterIdBits = 5L;  /**datacenter id*/
    maxWorkerId = -1L^(-1L << workerIdBits);
    maxDatacenterId = -1L^(-1L << datacenterIdBits);
    sequenceBits = 12L;
    workerIdShift = sequenceBits;
    datacenterIdShift = sequenceBits + workerIdBits;
    timestampLeftShift = sequenceBits + workerIdBits + datacenterIdBits;
    sequenceMask = -1L^(-1L << sequenceBits);
    return TRUE;
}

void id_generators_free(id_generators_wrapper_t *generators) {
    if (NULL == generators) {
        return;
    }
    g_rw_lock_clear(&generators->rw_mutex); 
    g_hash_table_remove_all(generators->table);
    g_hash_table_destroy(generators->table);
    generators->table = NULL;
}

gint64 wait_Next_Mills(gint64 last_time_stamp) {
    gint64 time_stamp = current_timestamp_get();
    while(time_stamp <= last_time_stamp) {
        time_stamp = current_timestamp_get();
    }
    return time_stamp;
}

gboolean id_generate(id_generator_t *id_gen, gint64 *value) {
    if (NULL == id_gen) {
        return FALSE;
    }
    g_mutex_lock(&id_gen->mutex);
    gint64 time_stamp = current_timestamp_get(); 
    if (time_stamp < id_gen->last_time_stamp) {
        g_mutex_unlock(&id_gen->mutex);
        return FALSE;
    }

    if (time_stamp == id_gen->last_time_stamp) {
        id_gen->inc = (id_gen->inc + 1) & sequenceMask;
        if (0 == id_gen->inc) { time_stamp = wait_Next_Mills(id_gen->last_time_stamp);}
    } else {
        id_gen->inc = 0L;
    }
    id_gen->last_time_stamp = time_stamp;

    *value = ((id_gen->last_time_stamp - twepoch)<<timestampLeftShift) | (id_gen->centor_id << datacenterIdShift) | (id_gen->machine_id << workerIdShift) | (id_gen->inc);
    g_mutex_unlock(&id_gen->mutex);
    return TRUE; 

}
gboolean unique_id_generator(id_generators_wrapper_t *generators, gchar *key, gint64 *value) {
    g_rw_lock_reader_lock(&generators->rw_mutex);
    id_generator_t *id_gen = g_hash_table_lookup(generators->table, key);
    g_rw_lock_reader_unlock(&generators->rw_mutex);

    if (NULL == id_gen) {
        gchar *k = g_strdup(key); 
        id_gen = id_generator_new(generators->machine_id, 0L); 

        g_rw_lock_writer_lock(&generators->rw_mutex);      
        g_hash_table_insert(generators->table, k, id_gen); 
        g_rw_lock_writer_unlock(&generators->rw_mutex);      
    }  
    return id_generate(id_gen, value);
} 

//int main() {
//    id_generators_wrapper_t generators; 
//    id_generators_init(&generators);
//    gint64 t; 
//    gint i = 0;
//    GArray *array = g_array_new(FALSE, TRUE, sizeof(glong));
//    while(i < 1000000 && global_id_get(&generators, "key", &t)) {
//        i++;
//        g_array_append_val(array, t);
//    }
//    for(i = 0; i < array->len - 1; i++) {
//        if(g_array_index(array, glong, i) >= g_array_index(array, glong, i+1)) {
//            g_printf("error");
//        } 
//    }
//    g_printf("array len = %ld\n", array->len);
//    g_array_free(array, TRUE);
//    return 1;
//}
