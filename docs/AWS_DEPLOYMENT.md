# AWS Cloud Deployment Guide

This guide provides instructions for deploying the IoT Sensor Data Pipeline to AWS cloud infrastructure.

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────┐
│                      AWS Cloud                               │
│                                                              │
│  ┌──────────────┐     ┌──────────────┐     ┌─────────────┐ │
│  │   EC2/ECS    │────▶│  Amazon MSK  │────▶│  Amazon EMR │ │
│  │  (Producer)  │     │   (Kafka)    │     │   (Spark)   │ │
│  └──────────────┘     └──────────────┘     └─────────────┘ │
│                                                    │         │
│                                                    ▼         │
│                                             ┌─────────────┐ │
│                                             │  Amazon S3  │ │
│                                             │ (Data Lake) │ │
│                                             └─────────────┘ │
│                                                    │         │
│                                                    ▼         │
│  ┌──────────────┐     ┌──────────────┐     ┌─────────────┐ │
│  │   EC2/ECS    │────▶│  AWS Glue   │◀────│    Athena   │ │
│  │ (Dashboard)  │     │ (Metastore) │     │   (Query)   │ │
│  └──────────────┘     └──────────────┘     └─────────────┘ │
│                                                              │
└─────────────────────────────────────────────────────────────┘
```

## Prerequisites

1. AWS Account with appropriate permissions
2. AWS CLI installed and configured
3. Terraform (optional, for IaC)
4. Basic understanding of AWS services

## Step 1: Setup Amazon MSK (Managed Kafka)

### Create MSK Cluster

```bash
# Create configuration
aws kafka create-configuration \
    --name iot-sensor-config \
    --kafka-versions "2.8.1" \
    --server-properties file://msk-config.properties

# Create cluster
aws kafka create-cluster \
    --cluster-name iot-sensor-cluster \
    --kafka-version "2.8.1" \
    --number-of-broker-nodes 3 \
    --broker-node-group-info file://broker-info.json \
    --encryption-info file://encryption-info.json \
    --enhanced-monitoring PER_BROKER
```

**broker-info.json:**
```json
{
    "InstanceType": "kafka.m5.large",
    "ClientSubnets": [
        "subnet-xxxxx",
        "subnet-yyyyy",
        "subnet-zzzzz"
    ],
    "SecurityGroups": ["sg-xxxxx"],
    "StorageInfo": {
        "EbsStorageInfo": {
            "VolumeSize": 100
        }
    }
}
```

### Create Kafka Topic

```bash
# Get bootstrap servers
BOOTSTRAP_SERVERS=$(aws kafka get-bootstrap-brokers \
    --cluster-arn <cluster-arn> \
    --query "BootstrapBrokerString" \
    --output text)

# Create topic
kafka-topics.sh --create \
    --bootstrap-server $BOOTSTRAP_SERVERS \
    --topic iot-sensor-data \
    --partitions 6 \
    --replication-factor 3
```

## Step 2: Setup Amazon S3 Data Lake

### Create S3 Buckets

```bash
# Create main data bucket
aws s3 mb s3://iot-sensor-data-lake-${ACCOUNT_ID}

# Create folder structure
aws s3api put-object --bucket iot-sensor-data-lake-${ACCOUNT_ID} --key raw/
aws s3api put-object --bucket iot-sensor-data-lake-${ACCOUNT_ID} --key aggregated/
aws s3api put-object --bucket iot-sensor-data-lake-${ACCOUNT_ID} --key checkpoints/
aws s3api put-object --bucket iot-sensor-data-lake-${ACCOUNT_ID} --key scripts/

# Enable versioning
aws s3api put-bucket-versioning \
    --bucket iot-sensor-data-lake-${ACCOUNT_ID} \
    --versioning-configuration Status=Enabled

# Enable encryption
aws s3api put-bucket-encryption \
    --bucket iot-sensor-data-lake-${ACCOUNT_ID} \
    --server-side-encryption-configuration file://encryption-config.json
```

**encryption-config.json:**
```json
{
    "Rules": [
        {
            "ApplyServerSideEncryptionByDefault": {
                "SSEAlgorithm": "AES256"
            }
        }
    ]
}
```

## Step 3: Setup AWS Glue Data Catalog

### Create Database

```bash
aws glue create-database \
    --database-input '{
        "Name": "iot_analytics",
        "Description": "IoT sensor analytics database"
    }'
```

### Create Tables

```bash
# Raw data table
aws glue create-table \
    --database-name iot_analytics \
    --table-input file://raw-table-definition.json

# Aggregated data table
aws glue create-table \
    --database-name iot_analytics \
    --table-input file://aggregated-table-definition.json
```

**raw-table-definition.json:**
```json
{
    "Name": "iot_sensor_raw",
    "StorageDescriptor": {
        "Columns": [
            {"Name": "sensor_id", "Type": "string"},
            {"Name": "location", "Type": "string"},
            {"Name": "event_timestamp", "Type": "timestamp"},
            {"Name": "temperature", "Type": "double"},
            {"Name": "humidity", "Type": "double"},
            {"Name": "pressure", "Type": "double"},
            {"Name": "battery_level", "Type": "double"},
            {"Name": "status", "Type": "string"},
            {"Name": "processing_timestamp", "Type": "timestamp"}
        ],
        "Location": "s3://iot-sensor-data-lake-${ACCOUNT_ID}/raw/",
        "InputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
        "OutputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
        "SerdeInfo": {
            "SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
        }
    },
    "PartitionKeys": [
        {"Name": "date", "Type": "string"},
        {"Name": "hour", "Type": "string"}
    ]
}
```

## Step 4: Setup Amazon EMR for Spark

### Create EMR Cluster

```bash
aws emr create-cluster \
    --name "IoT-Streaming-Cluster" \
    --release-label emr-6.12.0 \
    --applications Name=Spark Name=Hadoop Name=Hive \
    --ec2-attributes KeyName=my-key,SubnetId=subnet-xxxxx \
    --instance-type m5.xlarge \
    --instance-count 3 \
    --use-default-roles \
    --log-uri s3://iot-sensor-data-lake-${ACCOUNT_ID}/logs/ \
    --configurations file://emr-configurations.json \
    --bootstrap-actions file://bootstrap-actions.json
```

**emr-configurations.json:**
```json
[
    {
        "Classification": "spark-defaults",
        "Properties": {
            "spark.sql.adaptive.enabled": "true",
            "spark.sql.adaptive.coalescePartitions.enabled": "true",
            "spark.streaming.stopGracefullyOnShutdown": "true",
            "spark.sql.parquet.compression.codec": "snappy",
            "spark.dynamicAllocation.enabled": "true"
        }
    },
    {
        "Classification": "spark-env",
        "Configurations": [
            {
                "Classification": "export",
                "Properties": {
                    "PYSPARK_PYTHON": "/usr/bin/python3"
                }
            }
        ]
    }
]
```

### Submit Spark Streaming Job

```bash
# Upload Spark job to S3
aws s3 cp spark_streaming/streaming_etl.py \
    s3://iot-sensor-data-lake-${ACCOUNT_ID}/scripts/

# Submit step
aws emr add-steps \
    --cluster-id j-xxxxx \
    --steps Type=Spark,Name="IoT Streaming ETL",\
ActionOnFailure=CONTINUE,\
Args=[--deploy-mode,cluster,\
--master,yarn,\
--conf,spark.sql.warehouse.dir=s3://iot-sensor-data-lake-${ACCOUNT_ID}/warehouse/,\
s3://iot-sensor-data-lake-${ACCOUNT_ID}/scripts/streaming_etl.py,\
--kafka-bootstrap-servers,${BOOTSTRAP_SERVERS},\
--kafka-topic,iot-sensor-data,\
--checkpoint-location,s3://iot-sensor-data-lake-${ACCOUNT_ID}/checkpoints/,\
--output-path,s3://iot-sensor-data-lake-${ACCOUNT_ID}/]
```

## Step 5: Deploy Data Producer

### Option A: EC2 Deployment

```bash
# Launch EC2 instance
aws ec2 run-instances \
    --image-id ami-xxxxx \
    --instance-type t3.small \
    --key-name my-key \
    --security-group-ids sg-xxxxx \
    --subnet-id subnet-xxxxx \
    --iam-instance-profile Name=iot-producer-role \
    --user-data file://producer-user-data.sh
```

**producer-user-data.sh:**
```bash
#!/bin/bash
yum update -y
yum install -y python3 git

# Clone repository
cd /home/ec2-user
git clone https://github.com/your-repo/iot-pipeline.git
cd iot-pipeline

# Install dependencies
pip3 install -r data_producer/requirements.txt

# Run producer
python3 data_producer/kafka_producer.py \
    --bootstrap-servers ${BOOTSTRAP_SERVERS} \
    --topic iot-sensor-data \
    --interval 1 &
```

### Option B: ECS Deployment

```bash
# Create task definition
aws ecs register-task-definition \
    --cli-input-json file://producer-task-definition.json

# Create service
aws ecs create-service \
    --cluster iot-cluster \
    --service-name iot-producer \
    --task-definition iot-producer:1 \
    --desired-count 1 \
    --launch-type FARGATE \
    --network-configuration file://network-config.json
```

## Step 6: Deploy Dashboard

### EC2 Deployment

```bash
# Launch EC2 instance
aws ec2 run-instances \
    --image-id ami-xxxxx \
    --instance-type t3.medium \
    --key-name my-key \
    --security-group-ids sg-xxxxx \
    --subnet-id subnet-xxxxx \
    --iam-instance-profile Name=iot-dashboard-role \
    --user-data file://dashboard-user-data.sh
```

**dashboard-user-data.sh:**
```bash
#!/bin/bash
yum update -y
yum install -y python3 git

# Clone repository
cd /home/ec2-user
git clone https://github.com/your-repo/iot-pipeline.git
cd iot-pipeline

# Install dependencies
pip3 install -r dashboard/requirements.txt

# Set environment
export DATA_PATH=s3://iot-sensor-data-lake-${ACCOUNT_ID}/

# Run dashboard
streamlit run dashboard/app.py \
    --server.port 8501 \
    --server.address 0.0.0.0 &
```

## Step 7: Setup Monitoring

### CloudWatch Alarms

```bash
# MSK cluster alarms
aws cloudwatch put-metric-alarm \
    --alarm-name iot-msk-cpu-high \
    --alarm-description "MSK CPU > 80%" \
    --metric-name CpuUser \
    --namespace AWS/Kafka \
    --statistic Average \
    --period 300 \
    --threshold 80 \
    --comparison-operator GreaterThanThreshold

# EMR cluster alarms
aws cloudwatch put-metric-alarm \
    --alarm-name iot-emr-apps-failed \
    --alarm-description "EMR applications failed" \
    --metric-name AppsRunning \
    --namespace AWS/ElasticMapReduce \
    --statistic Sum \
    --period 300 \
    --threshold 0 \
    --comparison-operator LessThanThreshold
```

### Enable MSK Enhanced Monitoring

```bash
aws kafka update-monitoring \
    --cluster-arn <cluster-arn> \
    --current-version <version> \
    --enhanced-monitoring PER_TOPIC_PER_BROKER
```

## Step 8: Setup Auto Scaling

### EMR Auto Scaling

```bash
aws emr put-auto-scaling-policy \
    --cluster-id j-xxxxx \
    --instance-group-id ig-xxxxx \
    --auto-scaling-policy file://autoscaling-policy.json
```

**autoscaling-policy.json:**
```json
{
    "Constraints": {
        "MinCapacity": 2,
        "MaxCapacity": 10
    },
    "Rules": [
        {
            "Name": "ScaleUp",
            "Action": {
                "SimpleScalingPolicyConfiguration": {
                    "AdjustmentType": "CHANGE_IN_CAPACITY",
                    "ScalingAdjustment": 2,
                    "CoolDown": 300
                }
            },
            "Trigger": {
                "CloudWatchAlarmDefinition": {
                    "ComparisonOperator": "GREATER_THAN",
                    "EvaluationPeriods": 2,
                    "MetricName": "YARNMemoryAvailablePercentage",
                    "Period": 300,
                    "Threshold": 15,
                    "Statistic": "AVERAGE"
                }
            }
        }
    ]
}
```

## Cost Estimation

### Monthly Costs (Approximate)

| Service | Configuration | Monthly Cost |
|---------|---------------|--------------|
| Amazon MSK | 3x kafka.m5.large | ~$600 |
| Amazon EMR | 3x m5.xlarge (24/7) | ~$1,200 |
| Amazon S3 | 100GB + requests | ~$5 |
| EC2 (Producer) | 1x t3.small | ~$15 |
| EC2 (Dashboard) | 1x t3.medium | ~$30 |
| Data Transfer | 100GB/month | ~$9 |
| **Total** | | **~$1,860** |

### Cost Optimization Tips

1. **Use Spot Instances** for EMR core/task nodes (60-70% savings)
2. **EMR Auto-scaling** to match workload demand
3. **S3 Lifecycle Policies** to archive old data to Glacier
4. **Reserved Instances** for long-running workloads (up to 75% savings)
5. **VPC Endpoints** for S3 to avoid data transfer charges

## Security Best Practices

### IAM Roles

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "s3:GetObject",
                "s3:PutObject",
                "s3:DeleteObject"
            ],
            "Resource": "arn:aws:s3:::iot-sensor-data-lake-*/*"
        },
        {
            "Effect": "Allow",
            "Action": [
                "kafka:DescribeCluster",
                "kafka:GetBootstrapBrokers"
            ],
            "Resource": "*"
        },
        {
            "Effect": "Allow",
            "Action": [
                "glue:GetDatabase",
                "glue:GetTable",
                "glue:CreateTable",
                "glue:UpdateTable"
            ],
            "Resource": "*"
        }
    ]
}
```

### Network Security

1. Place MSK in private subnets
2. Use security groups to control access
3. Enable VPC Flow Logs
4. Use AWS PrivateLink for service access

### Data Security

1. Enable S3 encryption at rest
2. Enable MSK encryption in transit and at rest
3. Use AWS KMS for key management
4. Enable CloudTrail for audit logging

## Disaster Recovery

### Backup Strategy

1. **MSK**: Enable automatic backups
2. **S3**: Enable versioning and cross-region replication
3. **Glue**: Export catalog definitions regularly

### Recovery Procedures

1. **MSK Failure**: Automatic failover to standby broker
2. **EMR Failure**: Restart cluster from checkpoint
3. **S3 Failure**: Restore from cross-region replica

## Monitoring Dashboards

### CloudWatch Dashboard

```bash
aws cloudwatch put-dashboard \
    --dashboard-name IoT-Pipeline-Dashboard \
    --dashboard-body file://dashboard-definition.json
```

Key Metrics to Monitor:
- MSK: Message rate, broker CPU, disk usage
- EMR: YARN memory, applications running
- S3: Request rate, error rate
- EC2: CPU, memory, network

## Conclusion

This deployment guide provides a production-ready setup for the IoT sensor data pipeline on AWS. Adjust configurations based on your specific requirements for performance, cost, and reliability.
