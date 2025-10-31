#!/bin/bash
set -e

echo "Installing Python dependencies..."
pip install -r requirements.txt

echo "Waiting for Kafka broker to be fully ready..."
# Additional wait to ensure Kafka is fully initialized (health check passed)
for i in $(seq 10 -1 1); do
    echo "Starting in: $i seconds"
    sleep 1
done

echo "_____Running kafka_stream.py_____"

# Run Python script with auto-restart on failure (helps with transient connection issues)
MAX_RETRIES=5
RETRY_COUNT=0

while [ $RETRY_COUNT -lt $MAX_RETRIES ]; do
    echo "Attempt $((RETRY_COUNT + 1)) of $MAX_RETRIES"
    
    if python /my_kafka/kafka_stream.py; then
        echo "kafka_stream.py completed successfully"
        break
    else
        RETRY_COUNT=$((RETRY_COUNT + 1))
        if [ $RETRY_COUNT -lt $MAX_RETRIES ]; then
            echo "Failed. Retrying in 5 seconds..."
            sleep 5
        else
            echo "Max retries reached. Exiting."
            exit 1
        fi
    fi
done

tail -f /dev/null