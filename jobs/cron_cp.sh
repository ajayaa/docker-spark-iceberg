#!/bin/bash

# Configuration
SOURCE_FILE="/home/admin/events/spark_kafka_stream.py"
CONTAINER_NAME="spark-iceberg"
DESTINATION_PATH="/tmp/foo.py"

docker cp "$SOURCE_FILE" "$CONTAINER_NAME:$DESTINATION_PATH"

# Function to get file hash
get_file_hash() {
    md5sum "$1" | awk '{ print $1 }'
}

# Initialize last known hash
LAST_HASH=$(get_file_hash "$SOURCE_FILE")

while true; do
    # Get current hash
    CURRENT_HASH=$(get_file_hash "$SOURCE_FILE")

    # Check if file has changed
    if [ "$CURRENT_HASH" != "$LAST_HASH" ]; then
        echo "File changed. Copying to container..."
        docker cp "$SOURCE_FILE" "$CONTAINER_NAME:$DESTINATION_PATH"
        LAST_HASH=$CURRENT_HASH
        echo "File copied successfully."
    else
        echo "No changes detected."
    fi

    # Wait for 2 seconds
    sleep 2
done
