# Troubleshooting Guide

Common issues and their solutions for the Real-Time Streaming Data Pipeline.

## Table of Contents

1. [Kafka Issues](#kafka-issues)
2. [Spark Streaming Issues](#spark-streaming-issues)
3. [Docker Issues](#docker-issues)
4. [Dashboard Issues](#dashboard-issues)
5. [Data Issues](#data-issues)
6. [Performance Issues](#performance-issues)

## Kafka Issues

### Issue: Cannot connect to Kafka

**Symptoms**:
```
kafka.errors.NoBrokersAvailable: NoBrokersAvailable
```

**Solutions**:

1. **Check if Kafka is running**:
   ```bash
   docker ps | grep kafka
   ```

2. **Start Kafka containers**:
   ```bash
   cd docker
   docker-compose up -d zookeeper kafka
   ```

3. **Check Kafka logs**:
   ```bash
   docker logs kafka
   ```

4. **Verify network connectivity**:
   ```bash
   telnet localhost 9092
   ```

5. **Check bootstrap servers configuration**:
   ```python
   # Should match your Kafka setup
   bootstrap_servers='localhost:9092'
   ```

### Issue: Topic does not exist

**Symptoms**:
```
UnknownTopicOrPartitionError
```

**Solutions**:

1. **List existing topics**:
   ```bash
   docker exec kafka kafka-topics --list --bootstrap-server localhost:9092
   ```

2. **Create topic manually**:
   ```bash
   docker exec kafka kafka-topics --create \
       --topic iot-sensor-data \
       --bootstrap-server localhost:9092 \
       --partitions 3 \
       --replication-factor 1
   ```

3. **Enable auto-create topics** (not recommended for production):
   ```properties
   auto.create.topics.enable=true
   ```

### Issue: Producer is slow

**Symptoms**:
- High latency
- Low throughput
- Producer lag

**Solutions**:

1. **Increase batch size**:
   ```python
   producer = KafkaProducer(
       batch_size=32768,  # 32KB
       linger_ms=10
   )
   ```

2. **Enable compression**:
   ```python
   producer = KafkaProducer(
       compression_type='snappy'
   )
   ```

3. **Check broker resources**:
   ```bash
   docker stats kafka
   ```

### Issue: Consumer lag

**Symptoms**:
```
Consumer group is lagging behind
```

**Solutions**:

1. **Check consumer lag**:
   ```bash
   docker exec kafka kafka-consumer-groups \
       --bootstrap-server localhost:9092 \
       --group iot-streaming-consumer \
       --describe
   ```

2. **Increase number of consumers**:
   - Ensure consumers ≤ partitions
   - Scale horizontally

3. **Increase fetch size**:
   ```python
   consumer = KafkaConsumer(
       max_poll_records=1000
   )
   ```

## Spark Streaming Issues

### Issue: Spark job fails to start

**Symptoms**:
```
Exception in thread "main" java.lang.NoClassDefFoundError
```

**Solutions**:

1. **Check Spark installation**:
   ```bash
   python -c "import pyspark; print(pyspark.__version__)"
   ```

2. **Install missing dependencies**:
   ```bash
   pip install pyspark==3.4.1
   ```

3. **Add Kafka package**:
   ```bash
   spark-submit \
       --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1 \
       streaming_etl.py
   ```

### Issue: Out of Memory Error

**Symptoms**:
```
java.lang.OutOfMemoryError: Java heap space
```

**Solutions**:

1. **Increase executor memory**:
   ```python
   spark = SparkSession.builder \
       .config("spark.executor.memory", "4g") \
       .config("spark.driver.memory", "2g") \
       .getOrCreate()
   ```

2. **Reduce batch size**:
   ```python
   .trigger(processingTime='60 seconds')  # Increase interval
   ```

3. **Enable dynamic allocation**:
   ```python
   spark.conf.set("spark.dynamicAllocation.enabled", "true")
   ```

4. **Check for memory leaks**:
   ```python
   # Unpersist cached DataFrames
   df.unpersist()
   ```

### Issue: Checkpoint directory error

**Symptoms**:
```
java.io.IOException: Checkpoint directory does not exist
```

**Solutions**:

1. **Create checkpoint directory**:
   ```bash
   mkdir -p checkpoint
   ```

2. **Use absolute paths**:
   ```python
   checkpoint_location = os.path.abspath("./checkpoint")
   ```

3. **Clear corrupted checkpoint**:
   ```bash
   rm -rf checkpoint/*
   ```

### Issue: Slow micro-batches

**Symptoms**:
- Micro-batch duration > trigger interval
- Growing queue of batches

**Solutions**:

1. **Check Spark UI** (http://localhost:4040):
   - Identify slow stages
   - Check for data skew

2. **Optimize shuffle partitions**:
   ```python
   spark.conf.set("spark.sql.shuffle.partitions", "100")
   ```

3. **Increase parallelism**:
   ```python
   spark.conf.set("spark.default.parallelism", "100")
   ```

4. **Cache frequently accessed data**:
   ```python
   df.persist()
   ```

### Issue: Data loss or duplication

**Symptoms**:
- Missing records
- Duplicate records

**Solutions**:

1. **Enable checkpointing**:
   ```python
   .option("checkpointLocation", checkpoint_path)
   ```

2. **Set Kafka offsets correctly**:
   ```python
   .option("startingOffsets", "earliest")  # or "latest"
   ```

3. **Ensure idempotent writes**:
   ```python
   .option("path", output_path)  # Use fixed path
   ```

4. **Check for failures**:
   ```python
   spark.conf.set("spark.streaming.stopGracefullyOnShutdown", "true")
   ```

## Docker Issues

### Issue: Docker containers not starting

**Symptoms**:
```
ERROR: Container failed to start
```

**Solutions**:

1. **Check Docker daemon**:
   ```bash
   docker info
   ```

2. **Check available resources**:
   ```bash
   docker system df
   ```

3. **Free up resources**:
   ```bash
   docker system prune -a
   ```

4. **Check logs**:
   ```bash
   docker-compose logs
   ```

### Issue: Port already in use

**Symptoms**:
```
Error: bind: address already in use
```

**Solutions**:

1. **Find process using port**:
   ```bash
   lsof -i :9092  # For Kafka
   lsof -i :8501  # For Streamlit
   ```

2. **Kill process**:
   ```bash
   kill -9 <PID>
   ```

3. **Change port in docker-compose.yml**:
   ```yaml
   ports:
     - "9093:9092"  # External:Internal
   ```

### Issue: Container keeps restarting

**Symptoms**:
```
Container xyz is restarting, attempt 5
```

**Solutions**:

1. **Check container logs**:
   ```bash
   docker logs <container_id>
   ```

2. **Check resource limits**:
   ```bash
   docker stats
   ```

3. **Increase memory/CPU in Docker settings**

4. **Fix configuration errors**:
   ```bash
   docker-compose config
   ```

## Dashboard Issues

### Issue: Dashboard shows no data

**Symptoms**:
- Empty charts
- "No data available" message

**Solutions**:

1. **Check if data files exist**:
   ```bash
   ls -lh output/raw_data/
   ls -lh output/aggregated_data/
   ```

2. **Verify data path**:
   ```bash
   export DATA_PATH=./output
   streamlit run dashboard/app.py
   ```

3. **Check Parquet files**:
   ```python
   import pandas as pd
   df = pd.read_parquet('output/raw_data/')
   print(df.head())
   ```

4. **Ensure streaming job is running**:
   ```bash
   ps aux | grep streaming_etl
   ```

### Issue: Dashboard is slow

**Symptoms**:
- Slow page loads
- Lag when interacting

**Solutions**:

1. **Reduce data limit**:
   ```python
   # In dashboard/app.py
   df = self.load_raw_data(limit=500)  # Reduce from 1000
   ```

2. **Add caching**:
   ```python
   @st.cache_data(ttl=30)  # Cache for 30 seconds
   def load_data():
       # ...
   ```

3. **Optimize queries**:
   ```python
   # Read only recent partitions
   df = pd.read_parquet('output/raw_data/date=2024-01-01/')
   ```

### Issue: Streamlit connection error

**Symptoms**:
```
ConnectionError: Failed to connect to Streamlit server
```

**Solutions**:

1. **Check if Streamlit is running**:
   ```bash
   ps aux | grep streamlit
   ```

2. **Restart Streamlit**:
   ```bash
   pkill -f streamlit
   streamlit run dashboard/app.py
   ```

3. **Check firewall settings**:
   ```bash
   # Allow port 8501
   sudo ufw allow 8501
   ```

## Data Issues

### Issue: Corrupted Parquet files

**Symptoms**:
```
ParquetInvalidFileError: Invalid Parquet file
```

**Solutions**:

1. **Remove corrupted files**:
   ```bash
   rm -rf output/raw_data/date=XXXX/
   ```

2. **Reset checkpoint**:
   ```bash
   rm -rf checkpoint/*
   ```

3. **Restart streaming job**

### Issue: Schema mismatch

**Symptoms**:
```
AnalysisException: Schema mismatch
```

**Solutions**:

1. **Check schema evolution**:
   ```python
   spark.conf.set("spark.sql.parquet.mergeSchema", "true")
   ```

2. **Define explicit schema**:
   ```python
   schema = StructType([
       StructField("sensor_id", StringType(), True),
       # ...
   ])
   ```

3. **Recreate tables**:
   ```bash
   rm -rf output/*
   # Restart pipeline
   ```

### Issue: Partition explosion

**Symptoms**:
- Too many small files
- Slow queries

**Solutions**:

1. **Reduce partition columns**:
   ```python
   # Only use date, not hour
   .partitionBy("date")
   ```

2. **Compact partitions**:
   ```python
   df = spark.read.parquet('output/raw_data/')
   df.coalesce(10).write.mode("overwrite").parquet('output/raw_data/')
   ```

3. **Adjust partition strategy**:
   ```python
   # Use dynamic partitioning
   spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
   ```

## Performance Issues

### Issue: High latency

**Symptoms**:
- End-to-end latency > 5 minutes
- Slow processing

**Solutions**:

1. **Reduce micro-batch interval**:
   ```python
   .trigger(processingTime='10 seconds')
   ```

2. **Increase Kafka fetch rate**:
   ```python
   spark.conf.set("spark.streaming.kafka.maxRatePerPartition", "2000")
   ```

3. **Optimize transformations**:
   ```python
   # Avoid unnecessary operations
   # Use broadcast joins for small tables
   ```

### Issue: High resource usage

**Symptoms**:
- 100% CPU usage
- High memory consumption

**Solutions**:

1. **Tune executor resources**:
   ```python
   spark.conf.set("spark.executor.cores", "2")
   spark.conf.set("spark.executor.memory", "2g")
   ```

2. **Enable dynamic allocation**:
   ```python
   spark.conf.set("spark.dynamicAllocation.enabled", "true")
   ```

3. **Optimize data processing**:
   - Add filters early
   - Use predicate pushdown
   - Avoid wide transformations

### Issue: Backpressure

**Symptoms**:
- Queue of unprocessed batches
- Growing lag

**Solutions**:

1. **Enable backpressure**:
   ```python
   spark.conf.set("spark.streaming.backpressure.enabled", "true")
   ```

2. **Adjust rate limits**:
   ```python
   spark.conf.set("spark.streaming.kafka.maxRatePerPartition", "500")
   ```

3. **Scale horizontally**:
   - Add more executors
   - Increase parallelism

## Getting Help

### Collect Diagnostic Information

```bash
# System info
uname -a
python --version
docker --version

# Process info
ps aux | grep python
ps aux | grep java

# Resource usage
free -h
df -h
docker stats

# Logs
docker-compose logs > logs.txt
```

### Enable Debug Logging

```python
# In Python scripts
import logging
logging.basicConfig(level=logging.DEBUG)

# In Spark
spark.sparkContext.setLogLevel("DEBUG")
```

### Check Spark UI

1. Open http://localhost:4040
2. Check:
   - SQL tab for query plans
   - Stages tab for task distribution
   - Storage tab for cached data
   - Executors tab for resource usage

### Report Issues

When reporting issues, include:
1. Error messages (full stack trace)
2. Configuration files
3. Versions of all components
4. Steps to reproduce
5. Expected vs actual behavior

## Common Gotchas

1. **Checkpoint directory must be persistent**
   - Don't use `/tmp` in production
   - Use S3 or HDFS for cloud deployments

2. **Kafka retention affects offset reset**
   - If data expired, `earliest` won't help
   - Increase retention or use recent offsets

3. **Parquet schema evolution**
   - Adding columns: OK
   - Removing columns: May cause issues
   - Changing types: Usually causes errors

4. **Time zones matter**
   - Use UTC consistently
   - Be aware of DST changes

5. **File system limits**
   - Too many small files = slow
   - Aim for 128MB-1GB per file

## Additional Resources

- [Spark Documentation](https://spark.apache.org/docs/latest/)
- [Kafka Documentation](https://kafka.apache.org/documentation/)
- [Streamlit Documentation](https://docs.streamlit.io/)
- [Docker Documentation](https://docs.docker.com/)
- [Project Issues](https://github.com/yadavanujkumar/Real-Time-Streaming-Data-Pipeline-and-Analytics-Dashboard/issues)
