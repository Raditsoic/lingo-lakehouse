#!/bin/bash

printf "Setting up Environment...\n"
docker compose up -d
sleep 1

printf "Starting Consumer...\n"
python3 /home/soic/yukino/duolingo-birdbrain/infra/data-storage/consumer.py &
consumer_pid=$!  # Capture the process ID of the consumer

printf "Starting Producer...\n"
python3 /home/soic/yukino/duolingo-birdbrain/infra/ingestion/producer.py &
producer_pid=$!  # Capture the process ID of the producer

printf "Starting API...\n"
python3 /home/soic/yukino/duolingo-birdbrain/infra/api/app.py &
api_pid=$!  # Capture the process ID of the API

# Wait for all background processes to finish
wait $consumer_pid $producer_pid $api_pid

