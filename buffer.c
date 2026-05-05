#include"logger.h"
#include<stdio.h>
#include<stdlib.h>
#include<fcntl.h> //for O_CREAT error
#include"buffer.h" //made the func declaration in header file definition here

int size=0;  //buffer size to be set at runtime(bw 5-50)
int* buffer = NULL; // shared buffer memory...dynamically allocated in init()
int in = 0;         //insert index
int out = 0;        // remove index
int count = 0;      // to keep check for buffer overflow
pthread_mutex_t lock;
sem_t full, empty;

void init(int buffer_size) { // initializing the mutex and sema...takes size at runtime
    size = buffer_size;
    buffer = malloc(size*sizeof(int));//buffer size based on user input
    pthread_mutex_init(&lock, NULL);    // mutex lock to maintain sync between waiter and chef
    sem_init(&full, 0, 0); // sema full to used by waiter (value > 0) dish available (count = 0) no dishes
    sem_init(&empty, 0, size);//sema empty to used by chef (value > 0) table empty (count = 0) table full
}

void insert(int item){ //placing order
    sem_wait(&empty);
    pthread_mutex_lock(&lock);
    buffer[in] = item;
    in = (in + 1)%size;
    count++;
    printf("inserted: %d | count: %d/%d\n", item, count, size);
    pthread_mutex_unlock(&lock);
    sem_post(&full);
}

int removeItem(){ //picking order from table
    sem_wait(&full);
    pthread_mutex_lock(&lock);
    int item = buffer[out];
    out = (out + 1)%size;
    count--;
    printf("removed: %d | count: %d/%d\n", item, count, size);
    pthread_mutex_unlock(&lock);
    sem_post(&empty);
    return item;
}

void destroy() {
    pthread_mutex_destroy(&lock);
    sem_destroy(&full);
    sem_destroy(&empty);
    free(buffer);
    buffer = NULL;
}



