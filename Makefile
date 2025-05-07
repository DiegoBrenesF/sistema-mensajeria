# Compiler and flags
CC = gcc
CFLAGS = -Wall -Wextra -g

# Directories
SRC_DIR = src
BIN_DIR = bin

# Source files
BROKER_SRC = $(SRC_DIR)/broker.c
PRODUCER_SRC = $(SRC_DIR)/producer.c
CONSUMER_SRC = $(SRC_DIR)/consumer.c

# Executables
BROKER = $(BIN_DIR)/broker
PRODUCER = $(BIN_DIR)/producer
CONSUMER = $(BIN_DIR)/consumer

# Default target
all: create_bin $(BROKER) $(PRODUCER) $(CONSUMER)

# Create bin directory if it doesn't exist
create_bin:
	mkdir -p $(BIN_DIR)

# Compile broker
$(BROKER): $(BROKER_SRC)
	$(CC) $(CFLAGS) -o $@ $<

# Compile producer
$(PRODUCER): $(PRODUCER_SRC)
	$(CC) $(CFLAGS) -o $@ $<

# Compile consumer
$(CONSUMER): $(CONSUMER_SRC)
	$(CC) $(CFLAGS) -o $@ $<

# Clean target
clean:
	rm -rf $(BIN_DIR)

# Run broker
run_broker: $(BROKER)
	$(BROKER)

# Run producer
run_producer: $(PRODUCER)
	$(PRODUCER)

# Run consumer
run_consumer: $(CONSUMER)
	$(CONSUMER)

# Phony targets
.PHONY: all clean create_bin run_broker run_producer run_consumer
