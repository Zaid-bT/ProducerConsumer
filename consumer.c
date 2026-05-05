#include <stdio.h>
#define _POSIX_C_SOURCE 199309L //fixed CLOCK_MONOTONIC error
#include <time.h>
#include <unistd.h>
#include "buffer.h"
#include "consumer.h"
#include "logger.h"

#define ITEMS_PER_CONSUMER 5  // match items_limit in chef.c

extern volatile int stop_flag;  // set by main/GUI to stop threads

void* consumer(void* arg) {
    thread_stats* stats = (thread_stats*)arg;

    for (int i = 0; i < ITEMS_PER_CONSUMER && !stop_flag; i++) {
        struct timespec start, end;
        clock_gettime(CLOCK_MONOTONIC, &start);

        int item = removeItem();

        clock_gettime(CLOCK_MONOTONIC, &end);
        stats->total_wait_time += (end.tv_sec - start.tv_sec) +
                                  (end.tv_nsec - start.tv_nsec) / 1e9;

        stats->items_consumed++;

        char msg[128];
        snprintf(msg, sizeof(msg),
                 "Waiter %d served Order#%d | consumed: %d | avg wait: %.2fs",
                 stats->thread_id, item, stats->items_consumed,
                 stats->total_wait_time / stats->items_consumed);
        log_event(msg);

        sleep(2);
    }

    char msg[64];
    snprintf(msg, sizeof(msg), "Waiter %d finished shift.", stats->thread_id);
    log_event(msg);

    return NULL;
}