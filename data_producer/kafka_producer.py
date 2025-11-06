"""
Kafka Producer for IoT Sensor Data Simulation
Simulates real-time IoT sensor data including temperature, humidity, and pressure readings.
"""

import json
import time
import random
from datetime import datetime
from kafka import KafkaProducer
from kafka.errors import KafkaError
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class IoTSensorDataProducer:
    """
    Simulates IoT sensors and produces data to Kafka topic.
    """
    
    def __init__(self, bootstrap_servers='localhost:9092', topic='iot-sensor-data'):
        """
        Initialize Kafka producer with configuration.
        
        Args:
            bootstrap_servers: Kafka broker address
            topic: Kafka topic name
        """
        self.topic = topic
        self.producer = None
        self.bootstrap_servers = bootstrap_servers
        
        # Sensor IDs for simulation
        self.sensor_ids = ['SENSOR_001', 'SENSOR_002', 'SENSOR_003', 'SENSOR_004', 'SENSOR_005']
        
        # Sensor locations
        self.locations = ['Building_A', 'Building_B', 'Building_C', 'Building_D', 'Building_E']
        
        self._initialize_producer()
    
    def _initialize_producer(self):
        """Initialize Kafka producer with retry logic."""
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                acks='all',  # Wait for all replicas to acknowledge
                retries=3,
                max_in_flight_requests_per_connection=1
            )
            logger.info(f"Kafka producer initialized successfully. Connected to {self.bootstrap_servers}")
        except KafkaError as e:
            logger.error(f"Failed to initialize Kafka producer: {e}")
            raise
    
    def generate_sensor_data(self):
        """
        Generate simulated IoT sensor data.
        
        Returns:
            dict: Sensor data with temperature, humidity, pressure, and metadata
        """
        sensor_id = random.choice(self.sensor_ids)
        location = random.choice(self.locations)
        
        # Simulate realistic sensor readings with some variance
        data = {
            'sensor_id': sensor_id,
            'location': location,
            'timestamp': datetime.now().isoformat(),
            'temperature': round(random.uniform(15.0, 35.0), 2),  # Celsius
            'humidity': round(random.uniform(30.0, 80.0), 2),      # Percentage
            'pressure': round(random.uniform(980.0, 1040.0), 2),   # hPa
            'battery_level': round(random.uniform(20.0, 100.0), 2), # Percentage
            'status': random.choice(['ACTIVE', 'ACTIVE', 'ACTIVE', 'WARNING']),  # Mostly active
        }
        
        return data
    
    def send_data(self, data):
        """
        Send data to Kafka topic.
        
        Args:
            data: Dictionary containing sensor data
        """
        try:
            future = self.producer.send(self.topic, value=data)
            # Block for 'synchronous' sends
            record_metadata = future.get(timeout=10)
            logger.debug(
                f"Sent data to topic {record_metadata.topic} "
                f"partition {record_metadata.partition} "
                f"offset {record_metadata.offset}"
            )
            return True
        except KafkaError as e:
            logger.error(f"Failed to send data to Kafka: {e}")
            return False
    
    def run(self, interval=1, count=None):
        """
        Start producing sensor data at specified interval.
        
        Args:
            interval: Time in seconds between messages (default: 1)
            count: Number of messages to send (None for infinite)
        """
        logger.info(f"Starting IoT sensor data producer. Sending to topic: {self.topic}")
        logger.info(f"Message interval: {interval} second(s)")
        
        messages_sent = 0
        
        try:
            while count is None or messages_sent < count:
                # Generate and send sensor data
                sensor_data = self.generate_sensor_data()
                
                if self.send_data(sensor_data):
                    messages_sent += 1
                    logger.info(
                        f"Message {messages_sent}: {sensor_data['sensor_id']} - "
                        f"Temp: {sensor_data['temperature']}°C, "
                        f"Humidity: {sensor_data['humidity']}%, "
                        f"Pressure: {sensor_data['pressure']} hPa"
                    )
                
                time.sleep(interval)
                
        except KeyboardInterrupt:
            logger.info("Producer interrupted by user")
        except Exception as e:
            logger.error(f"Error in producer loop: {e}")
        finally:
            self.close()
            logger.info(f"Total messages sent: {messages_sent}")
    
    def close(self):
        """Close Kafka producer connection."""
        if self.producer:
            self.producer.flush()
            self.producer.close()
            logger.info("Kafka producer closed")


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='IoT Sensor Data Producer for Kafka')
    parser.add_argument(
        '--bootstrap-servers',
        default='localhost:9092',
        help='Kafka bootstrap servers (default: localhost:9092)'
    )
    parser.add_argument(
        '--topic',
        default='iot-sensor-data',
        help='Kafka topic name (default: iot-sensor-data)'
    )
    parser.add_argument(
        '--interval',
        type=float,
        default=1.0,
        help='Message interval in seconds (default: 1.0)'
    )
    parser.add_argument(
        '--count',
        type=int,
        default=None,
        help='Number of messages to send (default: infinite)'
    )
    
    args = parser.parse_args()
    
    # Create and run producer
    producer = IoTSensorDataProducer(
        bootstrap_servers=args.bootstrap_servers,
        topic=args.topic
    )
    
    producer.run(interval=args.interval, count=args.count)
