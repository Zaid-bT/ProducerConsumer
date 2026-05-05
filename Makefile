CC     = gcc
CFLAGS = -Wall -Wextra -g -D_POSIX_C_SOURCE=199309L \
         $(shell pkg-config --cflags gtk+-3.0)
LIBS   = $(shell pkg-config --libs gtk+-3.0) -lpthread
 
SRCS   = main.c buffer.c producer.c consumer.c logger.c
OBJS   = $(SRCS:.c=.o)
TARGET = sim
 
all: $(TARGET)
 
$(TARGET): $(OBJS)
	$(CC) $(OBJS) -o $(TARGET) $(LIBS)
 
%.o: %.c
	$(CC) $(CFLAGS) -c $< -o $@
 
clean:
	rm -f $(OBJS) $(TARGET) activity_log.txt
 
.PHONY: all clean
 
