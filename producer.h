#ifndef PRODUCER_H
#define PRODUCER_H

typedef struct {
    int thread_id;
    int items_produced;
    double total_wait_time;
} producer_stats;

void* producer(void* arg);

#endif