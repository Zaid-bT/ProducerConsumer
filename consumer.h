#ifndef CONSUMER_H
#define CONSUMER_H

typedef struct {
    int thread_id;
    int items_consumed;
    double total_wait_time;
    int items_limit;
    int sleep_delay;
} thread_stats;

void* consumer(void* arg);

#endif