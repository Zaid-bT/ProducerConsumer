#include <stdio.h>
#define _POSIX_C_SOURCE 199309L
#include <time.h>
#include <unistd.h>
#include "buffer.h"
#include "producer.h"
#include "logger.h"

#define ITEMS_PER_PRODUCER 5
extern volatile int stop_flag; //will be made by the gui to stop the code

void* producer(void* arg) {
    producer_stats* stats = (producer_stats*)arg;
    for (int i = 0; i < ITEMS_PER_PRODUCER && !stop_flag; i++) {
        struct timespec start, end;
        
        clock_gettime(CLOCK_MONOTONIC, &start); // notes the time before it inserts
        int item = (stats->thread_id * 10) + i;
        insert(item); // depends get the permission or not if waits then that time also recorded
        clock_gettime(CLOCK_MONOTONIC, &end); // get the time after the insert

        stats->total_wait_time += (end.tv_sec - start.tv_sec) + (end.tv_nsec - start.tv_nsec) / 1e9; // total time calculated between insetion and coming out even the wait time if any
        stats->items_produced++;
        char msg[128];
        snprintf(msg, sizeof(msg), "Waiter %d placed Order#%d | produced: %d | avg wait: %.2fs", stats->thread_id, item, stats->items_produced, stats->total_wait_time / stats->items_produced);
        log_event(msg);
        sleep(1);
    }
    char msg[64];
    snprintf(msg, sizeof(msg), "Waiter %d finished shift.", stats->thread_id);
    log_event(msg);

    return NULL;
}

