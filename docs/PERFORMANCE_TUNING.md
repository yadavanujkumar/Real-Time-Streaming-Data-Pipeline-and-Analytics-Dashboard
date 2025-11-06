# Performance Tuning Guide

This document provides guidelines for optimizing the performance of the Real-Time Streaming Data Pipeline.

## Table of Contents

1. [Kafka Performance Tuning](#kafka-performance-tuning)
2. [Spark Performance Tuning](#spark-performance-tuning)
3. [Storage Optimization](#storage-optimization)
4. [Network Optimization](#network-optimization)
5. [Memory Management](#memory-management)
6. [Monitoring & Profiling](#monitoring--profiling)

## Kafka Performance Tuning

### Producer Optimization

```python
# Producer configuration for high throughput
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    acks='all',                    # Durability
    retries=3,                     # Retry failed sends
    batch_size=32768,              # Larger batches (32KB)
    linger_ms=10,                  # Wait 10ms to batch messages
    buffer_memory=67108864,        # 64MB buffer
    compression_type='snappy',     # Fast compression
    max_in_flight_requests_per_connection=5
)
```

**Key Settings**:
- `batch_size`: Larger batches = better throughput (trade-off: latency)
- `linger_ms`: Wait time to accumulate messages
- `compression_type`: `snappy` for speed, `lz4` for better compression
- `buffer_memory`: Increase for high-volume producers

### Consumer Optimization

```python
# Consumer configuration for high throughput
consumer = KafkaConsumer(
    'iot-sensor-data',
    bootstrap_servers='localhost:9092',
    group_id='iot-consumer-group',
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    max_poll_records=500,          # Fetch more records per poll
    fetch_min_bytes=1048576,       # Wait for 1MB before returning
    fetch_max_wait_ms=500          # Max wait time 500ms
)
```

### Broker Configuration

```properties
# server.properties
num.network.threads=8
num.io.threads=16
socket.send.buffer.bytes=102400
socket.receive.buffer.bytes=102400
socket.request.max.bytes=104857600

# Replication
default.replication.factor=3
min.insync.replicas=2

# Retention
log.retention.hours=168
log.segment.bytes=1073741824
log.retention.check.interval.ms=300000

# Compression
compression.type=snappy
```

### Partitioning Strategy

**Rule of Thumb**: 
```
# Partitions = max(
#     producers_per_topic,
#     (throughput_per_topic / throughput_per_partition)
# )
```

**Example**:
- Target throughput: 10,000 msg/sec
- Per partition throughput: 3,000 msg/sec
- Recommended partitions: 4-6

```bash
# Create topic with optimal partitions
kafka-topics.sh --create \
    --topic iot-sensor-data \
    --partitions 6 \
    --replication-factor 3 \
    --config compression.type=snappy \
    --config min.insync.replicas=2
```

## Spark Performance Tuning

### Core Configuration

```python
spark = SparkSession.builder \
    .appName("IoT-Streaming-ETL") \
    .config("spark.executor.memory", "4g") \
    .config("spark.executor.cores", "4") \
    .config("spark.driver.memory", "2g") \
    .config("spark.sql.shuffle.partitions", "200") \
    .config("spark.default.parallelism", "200") \
    .config("spark.streaming.kafka.maxRatePerPartition", "1000") \
    .getOrCreate()
```

### Adaptive Query Execution (AQE)

```python
# Enable AQE for dynamic optimization
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.minPartitionNum", "1")
spark.conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", "64MB")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
```

### Memory Management

**Executor Memory Allocation**:
```
Total Executor Memory = Executor Memory * (1 + Memory Overhead)
```

**Memory Breakdown**:
- Execution Memory: 40%
- Storage Memory: 40%
- Overhead: 20%

**Configuration**:
```python
spark.conf.set("spark.executor.memory", "8g")
spark.conf.set("spark.executor.memoryOverhead", "2g")
spark.conf.set("spark.memory.fraction", "0.8")
spark.conf.set("spark.memory.storageFraction", "0.5")
```

### Streaming Optimization

```python
# Trigger configuration
query = df.writeStream \
    .trigger(processingTime='30 seconds') \  # Micro-batch interval
    .outputMode("append") \
    .start()

# Watermark for late data
df_with_watermark = df.withWatermark("event_timestamp", "10 minutes")

# Optimize state store
spark.conf.set("spark.sql.streaming.stateStore.providerClass", 
               "org.apache.spark.sql.execution.streaming.state.HDFSBackedStateStoreProvider")
spark.conf.set("spark.sql.streaming.stateStore.maintenanceInterval", "60")
```

### Join Optimization

```python
# Broadcast join for small tables
from pyspark.sql.functions import broadcast

# If dimension table < 10MB
result = fact_df.join(broadcast(dim_df), "key")

# Salting for skewed joins
from pyspark.sql.functions import rand, concat

# Add salt to skewed key
fact_df_salted = fact_df.withColumn("salt", (rand() * 10).cast("int"))
dim_df_salted = dim_df.withColumn("salt", lit(0))
# Join with salt
```

### Caching Strategy

```python
# Cache frequently accessed DataFrames
df_cached = df.persist(StorageLevel.MEMORY_AND_DISK)

# Unpersist when done
df_cached.unpersist()

# Choose appropriate storage level
# MEMORY_ONLY: Fast, limited by memory
# MEMORY_AND_DISK: Spills to disk
# DISK_ONLY: Slow but saves memory
```

### Partition Tuning

```python
# Repartition for balanced workload
df_balanced = df.repartition(200)

# Coalesce to reduce partitions (no shuffle)
df_coalesced = df.coalesce(10)

# Partition by column for skewed data
df_partitioned = df.repartition(200, "sensor_id")
```

### Shuffle Optimization

```python
# Reduce shuffle partitions for small data
spark.conf.set("spark.sql.shuffle.partitions", "20")

# Increase for large data
spark.conf.set("spark.sql.shuffle.partitions", "1000")

# Tune shuffle settings
spark.conf.set("spark.shuffle.file.buffer", "64k")
spark.conf.set("spark.shuffle.compress", "true")
spark.conf.set("spark.shuffle.spill.compress", "true")
```

## Storage Optimization

### Parquet Configuration

```python
# Write with optimal configuration
df.write \
    .mode("append") \
    .option("compression", "snappy") \
    .option("parquet.block.size", 134217728) \  # 128MB
    .option("parquet.page.size", 1048576) \      # 1MB
    .partitionBy("date", "hour") \
    .parquet(output_path)
```

### Partitioning Best Practices

**Guidelines**:
1. Partition by frequently filtered columns
2. Avoid over-partitioning (< 1GB per partition ideal)
3. Use hierarchical partitioning (year/month/day)
4. Limit partition columns (2-3 max)

**Example**:
```python
# Good partitioning
df.write.partitionBy("date", "hour").parquet(path)

# Avoid over-partitioning
# df.write.partitionBy("date", "hour", "minute", "sensor_id").parquet(path)
```

### File Size Optimization

```python
# Control file size with coalesce
df.coalesce(10).write.parquet(path)

# Or repartition for specific size
# Target: 128MB - 1GB per file
num_files = total_data_size_bytes / (256 * 1024 * 1024)  # 256MB per file
df.repartition(num_files).write.parquet(path)
```

### Compaction

```python
# Periodic compaction for small files
from pyspark.sql import SparkSession

def compact_partition(path, partition_date):
    spark = SparkSession.builder.getOrCreate()
    
    # Read partition
    df = spark.read.parquet(f"{path}/date={partition_date}")
    
    # Write back with optimal file count
    df.repartition(10).write \
        .mode("overwrite") \
        .parquet(f"{path}_temp/date={partition_date}")
    
    # Replace old partition
    # mv {path}_temp/date={partition_date} {path}/date={partition_date}
```

## Network Optimization

### Data Locality

```python
# Maximize data locality
spark.conf.set("spark.locality.wait", "3s")

# Prefer local tasks
spark.conf.set("spark.locality.wait.process", "1s")
spark.conf.set("spark.locality.wait.node", "2s")
spark.conf.set("spark.locality.wait.rack", "3s")
```

### Serialization

```python
# Use Kryo serialization (faster than Java)
spark.conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
spark.conf.set("spark.kryo.registrationRequired", "false")

# Register custom classes
spark.conf.set("spark.kryo.classesToRegister", 
               "com.example.CustomClass,com.example.AnotherClass")
```

### Network Compression

```python
# Enable RPC compression
spark.conf.set("spark.rpc.message.maxSize", "256")
spark.conf.set("spark.io.compression.codec", "snappy")
spark.conf.set("spark.io.compression.snappy.blockSize", "32k")
```

## Memory Management

### Garbage Collection Tuning

```python
# G1GC (recommended for large heaps)
spark.conf.set("spark.executor.extraJavaOptions", 
               "-XX:+UseG1GC -XX:InitiatingHeapOccupancyPercent=35 -XX:ConcGCThreads=12")

# Or CMS for smaller heaps
# spark.conf.set("spark.executor.extraJavaOptions", 
#                "-XX:+UseConcMarkSweepGC -XX:+CMSIncrementalMode")
```

### Off-Heap Memory

```python
# Enable off-heap memory for Arrow/Columnar operations
spark.conf.set("spark.memory.offHeap.enabled", "true")
spark.conf.set("spark.memory.offHeap.size", "2g")
```

### Dynamic Allocation

```python
# Enable dynamic allocation
spark.conf.set("spark.dynamicAllocation.enabled", "true")
spark.conf.set("spark.dynamicAllocation.initialExecutors", "2")
spark.conf.set("spark.dynamicAllocation.minExecutors", "1")
spark.conf.set("spark.dynamicAllocation.maxExecutors", "20")
spark.conf.set("spark.dynamicAllocation.executorIdleTimeout", "60s")
```

## Monitoring & Profiling

### Spark UI Metrics

**Key Metrics to Monitor**:
1. Stage duration and task distribution
2. Shuffle read/write sizes
3. Memory usage (execution + storage)
4. GC time
5. Data skew

### Logging

```python
# Configure logging
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

# Add custom metrics
logger = logging.getLogger(__name__)
logger.info(f"Processing rate: {records_per_sec} records/sec")
```

### Performance Profiling

```bash
# Run with detailed metrics
spark-submit \
    --conf spark.eventLog.enabled=true \
    --conf spark.eventLog.dir=/tmp/spark-events \
    --conf spark.executor.extraJavaOptions="-XX:+PrintGCDetails" \
    streaming_etl.py
```

### Benchmarking

```python
import time

# Measure operation time
start_time = time.time()
df.count()
end_time = time.time()
print(f"Operation took {end_time - start_time:.2f} seconds")

# Use Spark's explain for query plans
df.explain(mode="extended")
```

## Performance Checklist

### Before Production

- [ ] Test with production data volume
- [ ] Profile GC behavior
- [ ] Validate partition sizes (128MB - 1GB)
- [ ] Check for data skew
- [ ] Optimize shuffle partitions
- [ ] Enable adaptive query execution
- [ ] Configure appropriate memory settings
- [ ] Set up monitoring and alerting
- [ ] Test failover and recovery
- [ ] Document baseline metrics

### Continuous Optimization

- [ ] Monitor Spark UI regularly
- [ ] Review slow stages/tasks
- [ ] Identify and fix data skew
- [ ] Optimize hot paths
- [ ] Compact small files periodically
- [ ] Review and adjust configurations
- [ ] Update based on workload changes

## Recommended Configurations by Scale

### Small Scale (< 1GB/hour)

```python
spark.executor.memory = 2g
spark.executor.cores = 2
spark.sql.shuffle.partitions = 20
kafka.maxRatePerPartition = 100
```

### Medium Scale (1-10GB/hour)

```python
spark.executor.memory = 4g
spark.executor.cores = 4
spark.sql.shuffle.partitions = 100
kafka.maxRatePerPartition = 500
```

### Large Scale (> 10GB/hour)

```python
spark.executor.memory = 8g
spark.executor.cores = 8
spark.sql.shuffle.partitions = 500
kafka.maxRatePerPartition = 2000
```

## Common Performance Issues

### Issue: High GC Time

**Solution**:
```python
# Increase executor memory
spark.conf.set("spark.executor.memory", "8g")

# Tune GC
spark.conf.set("spark.executor.extraJavaOptions", "-XX:+UseG1GC")
```

### Issue: Data Skew

**Solution**:
```python
# Add salt to keys
from pyspark.sql.functions import rand

df_balanced = df.withColumn("salt_key", 
    concat(col("key"), lit("_"), (rand() * 10).cast("int")))
```

### Issue: Small Files

**Solution**:
```python
# Compact files
df.coalesce(num_files).write.parquet(path)
```

### Issue: Slow Shuffles

**Solution**:
```python
# Reduce shuffle data
df.persist()  # Cache before shuffle

# Increase parallelism
spark.conf.set("spark.sql.shuffle.partitions", "1000")
```

## References

- [Spark Performance Tuning](https://spark.apache.org/docs/latest/tuning.html)
- [Kafka Performance](https://kafka.apache.org/documentation/#performance)
- [Parquet Best Practices](https://parquet.apache.org/docs/)
