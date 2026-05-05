#ifndef PRODUCER_H
#define PRODUCER_H

typedef struct {
    int thread_id;
    int items_produced;
    double total_wait_time;
    int items_limit;// how many items this thread should produce (set at startup)
    int sleep_delay;//sleep duration before inserts
} producer_stats;

void* producer(void* arg);

#endif