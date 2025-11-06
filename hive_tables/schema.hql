-- =====================================================
-- Hive Table Schemas for IoT Sensor Data Pipeline
-- =====================================================

-- Drop existing tables if they exist
DROP TABLE IF EXISTS iot_sensor_raw;
DROP TABLE IF EXISTS iot_sensor_aggregated;

-- =====================================================
-- Raw Sensor Data Table
-- =====================================================
-- Stores raw sensor readings with event-level granularity
CREATE EXTERNAL TABLE IF NOT EXISTS iot_sensor_raw (
    sensor_id STRING COMMENT 'Unique sensor identifier',
    location STRING COMMENT 'Sensor location/building',
    event_timestamp TIMESTAMP COMMENT 'Time when event occurred',
    temperature DOUBLE COMMENT 'Temperature reading in Celsius',
    humidity DOUBLE COMMENT 'Humidity percentage',
    pressure DOUBLE COMMENT 'Atmospheric pressure in hPa',
    battery_level DOUBLE COMMENT 'Battery level percentage',
    status STRING COMMENT 'Sensor status (ACTIVE, WARNING, etc.)',
    processing_timestamp TIMESTAMP COMMENT 'Time when data was processed'
)
PARTITIONED BY (
    date STRING COMMENT 'Event date (YYYY-MM-DD)',
    hour STRING COMMENT 'Event hour (HH)'
)
STORED AS PARQUET
LOCATION '/tmp/output/raw_data'
TBLPROPERTIES (
    'parquet.compression'='SNAPPY',
    'description'='Raw IoT sensor readings',
    'created_by'='streaming_etl',
    'data_classification'='sensor_telemetry'
);

-- =====================================================
-- Aggregated Sensor Data Table
-- =====================================================
-- Stores aggregated sensor metrics by 1-minute windows
CREATE EXTERNAL TABLE IF NOT EXISTS iot_sensor_aggregated (
    window_start TIMESTAMP COMMENT 'Aggregation window start time',
    window_end TIMESTAMP COMMENT 'Aggregation window end time',
    sensor_id STRING COMMENT 'Unique sensor identifier',
    location STRING COMMENT 'Sensor location/building',
    avg_temperature DOUBLE COMMENT 'Average temperature in window',
    max_temperature DOUBLE COMMENT 'Maximum temperature in window',
    min_temperature DOUBLE COMMENT 'Minimum temperature in window',
    avg_humidity DOUBLE COMMENT 'Average humidity in window',
    max_humidity DOUBLE COMMENT 'Maximum humidity in window',
    min_humidity DOUBLE COMMENT 'Minimum humidity in window',
    avg_pressure DOUBLE COMMENT 'Average pressure in window',
    avg_battery_level DOUBLE COMMENT 'Average battery level in window',
    reading_count BIGINT COMMENT 'Number of readings in window'
)
PARTITIONED BY (
    date STRING COMMENT 'Window start date (YYYY-MM-DD)',
    hour STRING COMMENT 'Window start hour (HH)'
)
STORED AS PARQUET
LOCATION '/tmp/output/aggregated_data'
TBLPROPERTIES (
    'parquet.compression'='SNAPPY',
    'description'='Aggregated IoT sensor metrics (1-minute windows)',
    'created_by'='streaming_etl',
    'data_classification'='sensor_analytics',
    'aggregation_window'='1 minute'
);

-- =====================================================
-- Repair partitions to discover existing data
-- =====================================================
MSCK REPAIR TABLE iot_sensor_raw;
MSCK REPAIR TABLE iot_sensor_aggregated;

-- =====================================================
-- Useful Queries for Analytics
-- =====================================================

-- Query 1: Get latest sensor readings
-- SELECT sensor_id, location, temperature, humidity, pressure, event_timestamp
-- FROM iot_sensor_raw
-- WHERE date = '2024-01-01' AND hour = '10'
-- ORDER BY event_timestamp DESC
-- LIMIT 10;

-- Query 2: Get aggregated metrics by location
-- SELECT 
--     location,
--     AVG(avg_temperature) as location_avg_temp,
--     AVG(avg_humidity) as location_avg_humidity,
--     COUNT(*) as window_count
-- FROM iot_sensor_aggregated
-- WHERE date = '2024-01-01'
-- GROUP BY location
-- ORDER BY location_avg_temp DESC;

-- Query 3: Identify sensors with low battery
-- SELECT DISTINCT sensor_id, location, avg_battery_level
-- FROM iot_sensor_aggregated
-- WHERE avg_battery_level < 30
-- AND date = '2024-01-01'
-- ORDER BY avg_battery_level ASC;

-- Query 4: Temperature anomaly detection (readings outside normal range)
-- SELECT sensor_id, location, temperature, event_timestamp
-- FROM iot_sensor_raw
-- WHERE (temperature < 18 OR temperature > 30)
-- AND date = '2024-01-01'
-- ORDER BY event_timestamp DESC;

-- Query 5: Hourly sensor statistics
-- SELECT 
--     hour,
--     COUNT(DISTINCT sensor_id) as active_sensors,
--     AVG(avg_temperature) as hourly_avg_temp,
--     MAX(max_temperature) as hourly_max_temp,
--     MIN(min_temperature) as hourly_min_temp
-- FROM iot_sensor_aggregated
-- WHERE date = '2024-01-01'
-- GROUP BY hour
-- ORDER BY hour;
