#!/bin/bash

# Setup script for IoT Sensor Data Pipeline
# This script sets up the environment for local development

set -e

echo "=================================================="
echo "IoT Sensor Data Pipeline - Setup Script"
echo "=================================================="

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Function to print colored output
print_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Check if Python is installed
if ! command -v python3 &> /dev/null; then
    print_error "Python 3 is not installed. Please install Python 3.8 or higher."
    exit 1
fi

print_info "Python version: $(python3 --version)"

# Check if Docker is installed
if ! command -v docker &> /dev/null; then
    print_warning "Docker is not installed. You'll need Docker to run Kafka and Hive."
    print_info "Install Docker from: https://docs.docker.com/get-docker/"
else
    print_info "Docker version: $(docker --version)"
fi

# Check if Docker Compose is installed
if ! command -v docker-compose &> /dev/null; then
    print_warning "Docker Compose is not installed."
else
    print_info "Docker Compose version: $(docker-compose --version)"
fi

# Create virtual environment
print_info "Creating Python virtual environment..."
if [ ! -d "venv" ]; then
    python3 -m venv venv
    print_info "Virtual environment created"
else
    print_info "Virtual environment already exists"
fi

# Activate virtual environment
print_info "Activating virtual environment..."
source venv/bin/activate

# Upgrade pip
print_info "Upgrading pip..."
pip install --upgrade pip

# Install dependencies
print_info "Installing dependencies..."

# Install producer dependencies
if [ -f "data_producer/requirements.txt" ]; then
    print_info "Installing producer dependencies..."
    pip install -r data_producer/requirements.txt
fi

# Install Spark dependencies
if [ -f "spark_streaming/requirements.txt" ]; then
    print_info "Installing Spark streaming dependencies..."
    pip install -r spark_streaming/requirements.txt
fi

# Install dashboard dependencies
if [ -f "dashboard/requirements.txt" ]; then
    print_info "Installing dashboard dependencies..."
    pip install -r dashboard/requirements.txt
fi

# Create necessary directories
print_info "Creating output directories..."
mkdir -p output/raw_data
mkdir -p output/aggregated_data
mkdir -p logs
mkdir -p checkpoint

# Download Spark if not present (optional)
print_info "Checking for Apache Spark..."
if ! command -v spark-submit &> /dev/null; then
    print_warning "Spark is not installed in PATH"
    print_info "You can:"
    print_info "1. Download from: https://spark.apache.org/downloads.html"
    print_info "2. Use PySpark in Python (already installed)"
    print_info "3. Use Docker Spark containers (recommended)"
fi

# Setup Kafka topics (if Docker is running)
if command -v docker &> /dev/null && docker ps &> /dev/null; then
    print_info "Setting up Kafka topics..."
    
    # Start Docker containers
    print_info "Starting Docker containers..."
    cd docker
    docker-compose up -d zookeeper kafka
    cd ..
    
    # Wait for Kafka to be ready
    print_info "Waiting for Kafka to be ready (30 seconds)..."
    sleep 30
    
    # Create Kafka topic
    print_info "Creating Kafka topic 'iot-sensor-data'..."
    docker exec kafka kafka-topics --create \
        --topic iot-sensor-data \
        --bootstrap-server localhost:9092 \
        --partitions 3 \
        --replication-factor 1 \
        --if-not-exists || print_warning "Topic might already exist"
    
    print_info "Kafka setup complete"
else
    print_warning "Docker is not running. Skipping Kafka setup."
    print_info "To setup Kafka manually:"
    print_info "  cd docker && docker-compose up -d"
fi

# Create .env file
print_info "Creating environment configuration..."
cat > .env << EOF
# Environment Configuration
DATA_PATH=/tmp/output
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_TOPIC=iot-sensor-data
CHECKPOINT_LOCATION=/tmp/checkpoint
EOF

print_info ".env file created"

echo ""
echo "=================================================="
print_info "Setup Complete!"
echo "=================================================="
echo ""
echo "Next steps:"
echo "  1. Start Docker services: cd docker && docker-compose up -d"
echo "  2. Run the producer: python data_producer/kafka_producer.py"
echo "  3. Run Spark streaming: python spark_streaming/streaming_etl.py"
echo "  4. Launch dashboard: streamlit run dashboard/app.py"
echo ""
echo "For detailed instructions, see README.md"
echo "=================================================="
