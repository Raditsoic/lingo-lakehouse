#!/bin/bash

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' 

# Configuration
PYTHON_EXECUTABLE="python3"
PRODUCER_SCRIPT="./infra/ingestion/prod.py"
CONSUMER_SCRIPT="./infra/ingestion/cons.py"
VIRTUAL_ENV_NAME="duolingo_env"

# Check command
command_exists() {
    command -v "$1" >/dev/null 2>&1
}

# Create Virtual Environment
create_virtual_env() {
    echo -e "${YELLOW}Creating virtual environment...${NC}"
    if [ ! -d "$VIRTUAL_ENV_NAME" ]; then
        python3 -m venv "$VIRTUAL_ENV_NAME"
    fi

    source "$VIRTUAL_ENV_NAME/bin/activate"

    pip install \
        confluent-kafka \
        pandas \
        minio \
        pyarrow \
        json
}

start_docker() {
    echo -e "${YELLOW}Starting Services...${NC}"
    docker-compose up -d 
    sleep 10 
}

# Run Producer
run_producer() {
    echo -e "${GREEN}Starting Kafka Producer...${NC}"
    "$PYTHON_EXECUTABLE" "$PRODUCER_SCRIPT" &
    PRODUCER_PID=$!
}

# Run Consumer
run_consumer() {
    echo -e "${GREEN}Starting Kafka Consumer...${NC}"
    "$PYTHON_EXECUTABLE" "$CONSUMER_SCRIPT" &
    CONSUMER_PID=$!
}

# Stop All Processes
cleanup() {
    echo -e "${RED}Stopping processes...${NC}"
    if [ ! -z "$PRODUCER_PID" ]; then
        kill "$PRODUCER_PID"
    fi
    
    if [ ! -z "$CONSUMER_PID" ]; then
        kill "$CONSUMER_PID"
    fi
    
    # Deactivate Virtual Environment
    deactivate
    
    # echo -e "${YELLOW}Stopping Docker services...${NC}"
    # docker-compose down
    
    exit 0
}

main() {
    # Trap interrupt signals to ensure clean exit
    trap cleanup SIGINT SIGTERM

    # Check Prerequisites
    if ! command_exists python3; then
        echo -e "${RED}Python3 is not installed!${NC}"
        exit 1
    fi

    if ! command_exists docker-compose; then
        echo -e "${RED}Docker Compose is not installed!${NC}"
        exit 1
    fi

    # create_virtual_env

    # start_docker

    run_producer
    run_consumer

    wait
}

main