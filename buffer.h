#ifndef BUFFER_H
#define BUFFER_H
#include <pthread.h>
#include <semaphore.h>
//#define SIZE 5 c//cannot keep it fixed size

extern int size;
extern int* buffer; //dynamically allocated in init()
extern int in, out, count;
extern pthread_mutex_t lock;
extern sem_t full, empty;

void init(int buffer_size);//takes buffer size at runtime
void insert(int item);
int removeItem();
void destroy();
#endif