"""
Unit tests for Kafka Producer
"""

import pytest
import json
from datetime import datetime
from data_producer.kafka_producer import IoTSensorDataProducer


class TestIoTSensorDataProducer:
    """Test suite for IoT Sensor Data Producer."""
    
    def test_generate_sensor_data_structure(self):
        """Test that generated data has correct structure."""
        producer = IoTSensorDataProducer()
        data = producer.generate_sensor_data()
        
        # Check all required fields exist
        required_fields = [
            'sensor_id', 'location', 'timestamp',
            'temperature', 'humidity', 'pressure',
            'battery_level', 'status'
        ]
        
        for field in required_fields:
            assert field in data, f"Missing field: {field}"
    
    def test_generate_sensor_data_types(self):
        """Test that generated data has correct types."""
        producer = IoTSensorDataProducer()
        data = producer.generate_sensor_data()
        
        # Check types
        assert isinstance(data['sensor_id'], str)
        assert isinstance(data['location'], str)
        assert isinstance(data['timestamp'], str)
        assert isinstance(data['temperature'], float)
        assert isinstance(data['humidity'], float)
        assert isinstance(data['pressure'], float)
        assert isinstance(data['battery_level'], float)
        assert isinstance(data['status'], str)
    
    def test_temperature_range(self):
        """Test that temperature values are within expected range."""
        producer = IoTSensorDataProducer()
        
        # Generate multiple samples
        for _ in range(100):
            data = producer.generate_sensor_data()
            temperature = data['temperature']
            
            # Temperature should be between 15°C and 35°C
            assert 15.0 <= temperature <= 35.0, \
                f"Temperature {temperature} out of range"
    
    def test_humidity_range(self):
        """Test that humidity values are within valid range."""
        producer = IoTSensorDataProducer()
        
        for _ in range(100):
            data = producer.generate_sensor_data()
            humidity = data['humidity']
            
            # Humidity should be between 30% and 80%
            assert 30.0 <= humidity <= 80.0, \
                f"Humidity {humidity} out of range"
    
    def test_pressure_range(self):
        """Test that pressure values are within valid range."""
        producer = IoTSensorDataProducer()
        
        for _ in range(100):
            data = producer.generate_sensor_data()
            pressure = data['pressure']
            
            # Pressure should be between 980 and 1040 hPa
            assert 980.0 <= pressure <= 1040.0, \
                f"Pressure {pressure} out of range"
    
    def test_battery_level_range(self):
        """Test that battery level is within valid range."""
        producer = IoTSensorDataProducer()
        
        for _ in range(100):
            data = producer.generate_sensor_data()
            battery = data['battery_level']
            
            # Battery should be between 20% and 100%
            assert 20.0 <= battery <= 100.0, \
                f"Battery level {battery} out of range"
    
    def test_sensor_id_format(self):
        """Test that sensor IDs follow expected format."""
        producer = IoTSensorDataProducer()
        
        for _ in range(50):
            data = producer.generate_sensor_data()
            sensor_id = data['sensor_id']
            
            # Should be one of the predefined sensor IDs
            assert sensor_id in producer.sensor_ids, \
                f"Unexpected sensor ID: {sensor_id}"
    
    def test_location_format(self):
        """Test that locations are from predefined list."""
        producer = IoTSensorDataProducer()
        
        for _ in range(50):
            data = producer.generate_sensor_data()
            location = data['location']
            
            # Should be one of the predefined locations
            assert location in producer.locations, \
                f"Unexpected location: {location}"
    
    def test_timestamp_format(self):
        """Test that timestamp is in ISO format."""
        producer = IoTSensorDataProducer()
        data = producer.generate_sensor_data()
        timestamp_str = data['timestamp']
        
        # Should be parseable as ISO format
        try:
            datetime.fromisoformat(timestamp_str)
        except ValueError:
            pytest.fail(f"Invalid timestamp format: {timestamp_str}")
    
    def test_status_values(self):
        """Test that status contains valid values."""
        producer = IoTSensorDataProducer()
        valid_statuses = ['ACTIVE', 'WARNING']
        
        for _ in range(100):
            data = producer.generate_sensor_data()
            status = data['status']
            
            assert status in valid_statuses, \
                f"Invalid status: {status}"
    
    def test_json_serialization(self):
        """Test that generated data can be serialized to JSON."""
        producer = IoTSensorDataProducer()
        data = producer.generate_sensor_data()
        
        # Should be serializable to JSON
        try:
            json_str = json.dumps(data)
            # Should be deserializable
            deserialized = json.loads(json_str)
            assert deserialized == data
        except (TypeError, json.JSONDecodeError) as e:
            pytest.fail(f"JSON serialization failed: {e}")
    
    def test_data_precision(self):
        """Test that numeric values have appropriate precision."""
        producer = IoTSensorDataProducer()
        data = producer.generate_sensor_data()
        
        # Check precision (should be rounded to 2 decimal places)
        temp_str = str(data['temperature']).split('.')[1] if '.' in str(data['temperature']) else ''
        assert len(temp_str) <= 2, "Temperature has too many decimal places"
        
        humid_str = str(data['humidity']).split('.')[1] if '.' in str(data['humidity']) else ''
        assert len(humid_str) <= 2, "Humidity has too many decimal places"
    
    def test_multiple_generations_unique_timestamps(self):
        """Test that consecutive generations have different timestamps."""
        import time
        producer = IoTSensorDataProducer()
        
        data1 = producer.generate_sensor_data()
        time.sleep(0.001)  # Small delay
        data2 = producer.generate_sensor_data()
        
        # Timestamps should be different
        assert data1['timestamp'] != data2['timestamp'], \
            "Consecutive generations should have different timestamps"


class TestProducerConfiguration:
    """Test producer configuration and initialization."""
    
    def test_custom_bootstrap_servers(self):
        """Test initialization with custom bootstrap servers."""
        producer = IoTSensorDataProducer(
            bootstrap_servers='custom:9092',
            topic='custom-topic'
        )
        
        assert producer.bootstrap_servers == 'custom:9092'
        assert producer.topic == 'custom-topic'
    
    def test_default_configuration(self):
        """Test default configuration values."""
        producer = IoTSensorDataProducer()
        
        assert producer.bootstrap_servers == 'localhost:9092'
        assert producer.topic == 'iot-sensor-data'
        assert len(producer.sensor_ids) == 5
        assert len(producer.locations) == 5


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
