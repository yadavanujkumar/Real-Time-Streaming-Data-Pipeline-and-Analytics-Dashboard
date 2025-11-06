# Quick Start Guide

Get up and running with the Real-Time Streaming Data Pipeline in 5 minutes!

## Prerequisites

- Docker and Docker Compose installed
- Python 3.8+ installed
- At least 4GB RAM available
- 10GB disk space

## 🚀 Fast Setup (5 Minutes)

### Step 1: Clone Repository (30 seconds)

```bash
git clone https://github.com/yadavanujkumar/Real-Time-Streaming-Data-Pipeline-and-Analytics-Dashboard.git
cd Real-Time-Streaming-Data-Pipeline-and-Analytics-Dashboard
```

### Step 2: Run Setup Script (2 minutes)

```bash
chmod +x setup.sh
./setup.sh
```

This automatically:
- Creates Python virtual environment
- Installs all dependencies
- Starts Docker containers
- Creates Kafka topics

### Step 3: Start the Pipeline (2 minutes)

```bash
./run.sh
```

This starts:
- Kafka and supporting services
- Data producer (simulating IoT sensors)
- Spark streaming job (processing data)
- Streamlit dashboard

### Step 4: View Dashboard (30 seconds)

Open your browser and navigate to:
- **Dashboard**: http://localhost:8501
- **Kafka UI**: http://localhost:8080

🎉 **That's it! Your pipeline is running!**

## 📖 Manual Setup

If you prefer to run components individually:

### 1. Setup Environment

```bash
# Create virtual environment
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate

# Install dependencies
pip install -r data_producer/requirements.txt
pip install -r spark_streaming/requirements.txt
pip install -r dashboard/requirements.txt
```

### 2. Start Infrastructure

```bash
cd docker
docker-compose up -d
cd ..
```

Wait 30 seconds for services to start.

### 3. Start Producer (Terminal 1)

```bash
python data_producer/kafka_producer.py
```

### 4. Start Spark Job (Terminal 2)

```bash
python spark_streaming/streaming_etl.py
```

### 5. Start Dashboard (Terminal 3)

```bash
export DATA_PATH=./output
streamlit run dashboard/app.py
```

## 🔍 Verify Everything Works

### Check Docker Services

```bash
docker-compose ps
```

Expected output:
```
NAME                STATUS
zookeeper          Up (healthy)
kafka              Up (healthy)
kafka-ui           Up
```

### Check Producer

You should see messages like:
```
[INFO] Message 1: SENSOR_001 - Temp: 23.5°C, Humidity: 65.2%
[INFO] Message 2: SENSOR_003 - Temp: 24.1°C, Humidity: 62.8%
```

### Check Spark Job

You should see:
```
[INFO] Spark session initialized successfully
[INFO] Reading stream from Kafka topic: iot-sensor-data
[INFO] Writing stream to Parquet
```

### Check Dashboard

Navigate to http://localhost:8501 and you should see:
- Live metrics (active sensors, temperatures)
- Real-time charts
- Data tables

## 🛑 Stop the Pipeline

Press `Ctrl+C` in the terminal running `run.sh`, or:

```bash
# Stop individual components
pkill -f kafka_producer.py
pkill -f streaming_etl.py
pkill -f streamlit

# Stop Docker services
cd docker
docker-compose down
```

## 🧪 Quick Test

Run a quick test to verify everything works:

```bash
# Activate environment
source venv/bin/activate

# Test producer (10 messages)
python data_producer/kafka_producer.py --count 10

# Check Kafka topic
docker exec kafka kafka-console-consumer \
    --bootstrap-server localhost:9092 \
    --topic iot-sensor-data \
    --from-beginning \
    --max-messages 5
```

## 🐛 Common Issues

### Issue: Port 9092 already in use

```bash
# Find and kill process
lsof -i :9092
kill -9 <PID>
```

### Issue: Cannot connect to Docker

```bash
# Restart Docker
sudo systemctl restart docker  # Linux
# Or restart Docker Desktop (Mac/Windows)
```

### Issue: No data in dashboard

```bash
# Check if data files exist
ls -lh output/raw_data/
ls -lh output/aggregated_data/

# Restart streaming job
pkill -f streaming_etl.py
python spark_streaming/streaming_etl.py
```

## 📚 Next Steps

Now that your pipeline is running:

1. **Explore the Dashboard**: Try different views and filters
2. **Customize Data**: Modify `data_producer/kafka_producer.py` to change sensor data
3. **Tune Performance**: See [docs/PERFORMANCE_TUNING.md](docs/PERFORMANCE_TUNING.md)
4. **Query Data**: Run Hive queries on the processed data
5. **Deploy to Cloud**: Follow [docs/AWS_DEPLOYMENT.md](docs/AWS_DEPLOYMENT.md)

## 🔗 Useful Links

- **Full Documentation**: [README.md](README.md)
- **Architecture Details**: [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md)
- **Troubleshooting**: [docs/TROUBLESHOOTING.md](docs/TROUBLESHOOTING.md)
- **Contributing**: [CONTRIBUTING.md](CONTRIBUTING.md)

## 💡 Tips

1. **Monitor Resources**: Use `docker stats` to check resource usage
2. **Check Logs**: All logs are in `./logs/` directory
3. **Auto-refresh Dashboard**: Enable in sidebar for real-time updates
4. **Kafka UI**: Use http://localhost:8080 to monitor Kafka

## 📊 What You Built

Congratulations! You now have:

✅ Real-time data ingestion with Kafka  
✅ Stream processing with PySpark  
✅ Partitioned data lake with Parquet  
✅ Interactive analytics dashboard  
✅ Production-ready architecture  
✅ Docker containerized infrastructure

## 🎓 Learning Resources

- **PySpark Tutorial**: https://spark.apache.org/docs/latest/api/python/
- **Kafka Guide**: https://kafka.apache.org/quickstart
- **Streamlit Docs**: https://docs.streamlit.io/

---

**Need Help?** Open an issue on [GitHub](https://github.com/yadavanujkumar/Real-Time-Streaming-Data-Pipeline-and-Analytics-Dashboard/issues)

Happy Streaming! 🚀
