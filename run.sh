#!/bin/bash

# Run script for IoT Sensor Data Pipeline
# Starts all components of the pipeline

set -e

echo "=================================================="
echo "IoT Sensor Data Pipeline - Run Script"
echo "=================================================="

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

print_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

# Function to cleanup on exit
cleanup() {
    print_info "Cleaning up..."
    pkill -f kafka_producer.py 2>/dev/null || true
    pkill -f streaming_etl.py 2>/dev/null || true
    pkill -f streamlit 2>/dev/null || true
}

trap cleanup EXIT

# Start Docker services
print_info "Starting Docker services..."
cd docker
docker-compose up -d
cd ..

# Wait for services to be ready
print_info "Waiting for services to be ready (30 seconds)..."
sleep 30

# Activate virtual environment if it exists
if [ -d "venv" ]; then
    print_info "Activating virtual environment..."
    source venv/bin/activate
fi

# Create output directories
mkdir -p output/raw_data
mkdir -p output/aggregated_data
mkdir -p logs
mkdir -p checkpoint

# Start Kafka Producer in background
print_info "Starting Kafka Producer..."
python data_producer/kafka_producer.py \
    --bootstrap-servers localhost:9092 \
    --topic iot-sensor-data \
    --interval 1 \
    > logs/producer.log 2>&1 &
PRODUCER_PID=$!
print_info "Producer started (PID: $PRODUCER_PID)"

# Wait a bit for producer to start
sleep 5

# Start Spark Streaming job in background
print_info "Starting Spark Streaming ETL..."
python spark_streaming/streaming_etl.py \
    --kafka-bootstrap-servers localhost:9092 \
    --kafka-topic iot-sensor-data \
    --checkpoint-location ./checkpoint \
    --output-path ./output \
    --mode all \
    > logs/spark.log 2>&1 &
SPARK_PID=$!
print_info "Spark job started (PID: $SPARK_PID)"

# Wait for data to be processed
print_info "Waiting for data to be processed (30 seconds)..."
sleep 30

# Start Streamlit Dashboard
print_info "Starting Streamlit Dashboard..."
export DATA_PATH=./output
streamlit run dashboard/app.py \
    --server.port 8501 \
    --server.address localhost \
    > logs/dashboard.log 2>&1 &
DASHBOARD_PID=$!

echo ""
echo "=================================================="
print_info "All services started!"
echo "=================================================="
echo ""
echo "Services running:"
echo "  - Kafka: http://localhost:9092"
echo "  - Kafka UI: http://localhost:8080"
echo "  - Spark Master UI: http://localhost:8081"
echo "  - Dashboard: http://localhost:8501"
echo ""
echo "Process IDs:"
echo "  - Producer: $PRODUCER_PID"
echo "  - Spark: $SPARK_PID"
echo "  - Dashboard: $DASHBOARD_PID"
echo ""
echo "Logs are available in ./logs/"
echo ""
echo "Press Ctrl+C to stop all services"
echo "=================================================="

# Wait for user interrupt
wait
