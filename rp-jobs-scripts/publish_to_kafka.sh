#!/bin/bash

# Kafka broker address
BROKER="localhost:9092"

# Kafka topic
TOPIC="test"

# Number of messages to send in each batch
BATCH_SIZE=1000

# Total number of messages to send (set to -1 for infinite)
TOTAL_MESSAGES=100000

# Delay between batches in seconds
DELAY=1

# Function to generate a random string
generate_random_string() {
    < /dev/urandom tr -dc 'a-zA-Z0-9' | head -c"${1:-$1}"
}

# Function to generate a batch of messages
generate_batch() {
    for ((i=1; i<=BATCH_SIZE; i++))
    do
        key=$(generate_random_string 10)
        value=$(generate_random_string 20)
        echo "$key,$value"
    done
}

# Function to publish a batch of messages
publish_batch() {
    local batch_data=$(generate_batch)
    echo "$batch_data" | kafkacat -P -b $BROKER -t $TOPIC -K ','
    echo "Published batch of $BATCH_SIZE messages"
}

# Main loop
message_count=0
while true; do
    publish_batch
    message_count=$((message_count + BATCH_SIZE))
    
    if [ $TOTAL_MESSAGES -ne -1 ] && [ $message_count -ge $TOTAL_MESSAGES ]; then
        echo "Reached total message count of $TOTAL_MESSAGES. Exiting."
        break
    fi
    
    sleep $DELAY
done

echo "Total messages sent: $message_count"
