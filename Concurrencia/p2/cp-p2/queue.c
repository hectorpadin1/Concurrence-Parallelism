#include <stdlib.h>

// circular array
typedef struct _queue {
    int size;
    int used;
    int first;
    void **data;
    pthread_mutex_t *mutex;
    pthread_cond_t *qfull;
    pthread_cond_t *qempty;
} _queue;

#include "queue.h"

queue q_create(int size) {
    queue q = malloc(sizeof(_queue));
    
    q->size   = size;
    q->used   = 0;
    q->first  = 0;
    q->data   = malloc(size*sizeof(void *));
    q->mutex  = malloc(sizeof(pthread_mutex_t));
    q->qfull  = malloc(sizeof(pthread_cond_t));
    q->qempty = malloc(sizeof(pthread_cond_t));

    pthread_mutex_init(q->mutex,NULL);
    pthread_cond_init(q->qfull,NULL);
    pthread_cond_init(q->qempty,NULL);

    return q;
}

int q_elements(queue q) {
    return q->used;
}

int q_insert(queue q, void *elem) {
	pthread_mutex_lock(q->mutex);
    
    if(q->size == q->used) 
    	pthread_cond_wait(q->qfull,q->mutex);

    q->data[(q->first+q->used) % q->size] = elem;
    q->used++;
    
    if (q->used==1)
    	pthread_cond_broadcast(q->qempty);
    pthread_mutex_unlock(q->mutex);

    return 1;
}

void *q_remove(queue q) {
    void *res;

    pthread_mutex_lock(q->mutex);
    if (!q->used) 
    	pthread_cond_wait(q->qempty,q->mutex);
    
    res = q->data[q->first];
    
    q->first = (q->first+1) % q->size;
    q->used--;

    if (q->used==(q->size+1))
    	pthread_cond_broadcast(q->qfull);
    pthread_mutex_unlock(q->mutex);
    
    return res;
}

void q_destroy(queue q) {

    pthread_cond_destroy(q->qfull);
    pthread_cond_destroy(q->qempty);
    pthread_mutex_destroy(q->mutex);

    free(q->qfull);
    free(q->qempty);
    free(q->mutex);

    free(q->data);
    free(q);
}
