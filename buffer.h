#ifndef BUFFER_H
#define BUFFER_H
#include <pthread.h>
#include <semaphore.h>
#define SIZE 5

extern int buffer[SIZE];
extern int in, out, count;
extern pthread_mutex_t lock;
extern sem_t full, empty;

void init();
void insert(int item);
int removeItem();
void destroy();
#endif