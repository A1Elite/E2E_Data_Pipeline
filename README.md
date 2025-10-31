# E2E Streaming Data Pipeline

![Architecture](readme_pic/E2E_Architecture.png)

## 📊 Overview

**E2E Streaming Data Pipeline** is an end-to-end real-time data engineering solution that demonstrates modern streaming architecture and cloud-native data processing. The pipeline continuously ingests user data from the RandomUser API, processes it in real-time using Apache Kafka and Apache Spark, stores it in AWS S3 as a data lake, and automatically loads it into AWS Redshift for analytics and visualization.

**🎯 [View Live Dashboard](https://lookerstudio.google.com/reporting/581cb65a-beb0-45b6-a14c-8f86a316fd18)**

## 🏗️ Architecture

The pipeline follows a modern Lambda Architecture pattern with the following data flow:

1. **Data Ingestion**: Python producer fetches random user data from RandomUser API and publishes to Kafka topic
2. **Stream Processing**: Apache Spark consumes messages from Kafka, transforms the data, and structures it
3. **Data Lake Storage**: Processed data is stored in AWS S3 in Parquet format for efficient querying
4. **Automated ETL**: AWS Lambda function is triggered on new S3 objects, loading data into Redshift
5. **Data Warehouse**: AWS Redshift stores structured data for analytics workloads
6. **Visualization**: Looker Studio connects to Redshift for interactive dashboards and insights

### Key Design Principles
- **Fault Tolerance**: Automatic retry logic in Kafka producer and Lambda functions
- **Scalability**: Containerized services can scale horizontally
- **Decoupling**: Kafka acts as a buffer between data ingestion and processing
- **Cost Efficiency**: S3 data lake with Parquet compression reduces storage costs
- **Real-time Processing**: Stream processing enables near real-time analytics

## 🛠️ Technology Stack

| Component | Technology | Purpose |
|-----------|-----------|---------|
| **Orchestration** | Docker, Docker Compose | Container management and service orchestration |
| **Message Broker** | Apache Kafka 4.0.0 (KRaft) | Distributed streaming platform for message queuing |
| **Stream Processing** | Apache Spark 3.5.3 | Real-time data processing and transformation |
| **Data Lake** | AWS S3 | Object storage for processed Parquet files |
| **Serverless Computing** | AWS Lambda | Event-driven data loading into data warehouse |
| **Data Warehouse** | AWS Redshift | Analytics and reporting database |
| **Visualization** | Google Looker Studio | Interactive dashboards and data visualization |
| **Programming** | Python 3.9+ | Application logic and data processing scripts |

## 📁 Project Structure

```
E2E-Streaming-Data-Pipeline/
│
├── docker_kafka.yml              # Kafka broker and producer container configuration
├── docker_spark.yml              # Spark master and worker container configuration
├── requirements.txt              # Python dependencies
├── lambda_function.py            # AWS Lambda function for S3 to Redshift ETL
│
├── my_kafka/
│   ├── kafka_stream.py           # Kafka producer - fetches API data and publishes to topic
│   └── k_entrypoint.sh           # Entrypoint script for Kafka client container
│
├── my_spark/
│   ├── spark_stream_s3.py        # Spark streaming job - consumes from Kafka, writes to S3
│   └── s_entrypoints.sh          # Entrypoint script for Spark worker container
│
└── readme_pic/
    ├── E2E_Architecture.png
    └── E2E_Dashboard.png
```

## 🎯 Features

- ✅ **Real-time Data Streaming**: Continuous ingestion from RandomUser API
- ✅ **Distributed Processing**: Apache Spark handles large-scale data transformation
- ✅ **Event-Driven Architecture**: Lambda functions automatically triggered on new data
- ✅ **Containerized Deployment**: Easy setup with Docker Compose
- ✅ **Schema Evolution**: Structured data format with defined schemas
- ✅ **Monitoring**: Health checks and retry mechanisms for fault tolerance
- ✅ **Columnar Storage**: Parquet format with Snappy compression for efficiency
- ✅ **Interactive Dashboard**: Real-time insights with Looker Studio

## 📊 Data Schema

The pipeline processes user data with the following schema:

```
user_data
├── user_id (Primary Key)
├── gender
├── title
├── first_name
├── last_name
├── city
├── state
├── country
├── email
├── username
├── password (hashed)
├── dob_date
├── dob_age
├── registered_date
├── registered_age
├── phone
└── nat (nationality)
```

## 🚀 Getting Started

### Prerequisites

- Docker Desktop (20.10+)
- Docker Compose (v2.0+)
- AWS Account with access to S3, Lambda, and Redshift
- Python 3.9+ (for local development)
- Git

### Installation

#### 1. Clone the Repository

```bash
git clone https://github.com/A1Elite/E2E_Data_Pipeline.git
cd E2E_Data_Pipeline
```

#### 2. Configure Environment Variables

Create a `.env` file in the project root directory:

```bash
# AWS Configuration
AWS_ACCESS_KEY_ID=your_aws_access_key
AWS_SECRET_ACCESS_KEY=your_aws_secret_key
AWS_REGION=us-east-1

# Kafka Configuration
kafka_servers=kafka-broker:9092
kafka_cid=user-insight-client
kafka_topic_name=user_data_topic
link_api=https://randomuser.me/api/

# S3 Configuration
s3_output_path=s3://your-bucket-name/user-data/

# Redshift Configuration
redshift_host=your-redshift-cluster.redshift.amazonaws.com
redshift_port=5439
redshift_db=user_insights
redshift_user=admin
redshift_password=your_secure_password
iam_role=arn:aws:iam::your-account-id:role/your-redshift-s3-role
```

⚠️ **Security Note**: Never commit your `.env` file to version control. Add it to `.gitignore`.

#### 3. Set Up AWS Infrastructure

##### Create S3 Bucket
```bash
aws s3 mb s3://your-bucket-name
```

##### Create Redshift Cluster
```bash
# Using AWS Console or CLI
aws redshift create-cluster \
    --cluster-identifier user-insights-cluster \
    --node-type dc2.large \
    --master-username admin \
    --master-user-password YourPassword123 \
    --number-of-nodes 1
```

##### Create Redshift Table
Connect to Redshift and run:

```sql
CREATE TABLE user_data (
    user_id VARCHAR(256) PRIMARY KEY,
    gender VARCHAR(20),
    title VARCHAR(20),
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    city VARCHAR(100),
    state VARCHAR(100),
    country VARCHAR(100),
    email VARCHAR(255),
    username VARCHAR(100),
    password VARCHAR(255),
    dob_date TIMESTAMP,
    dob_age INTEGER,
    registered_date TIMESTAMP,
    registered_age INTEGER,
    phone VARCHAR(50),
    nat VARCHAR(10)
);
```

##### Configure Lambda Function

1. Create a new Lambda function in AWS Console
2. Runtime: Python 3.9+
3. Copy code from `lambda_function.py`
4. Add environment variables (redshift credentials, IAM role)
5. Configure S3 trigger for your bucket (suffix: `.parquet`)
6. Attach execution role with policies for:
   - S3 read access
   - Redshift data API access
   - CloudWatch Logs

#### 4. Deploy the Pipeline

Start all services using Docker Compose:

```bash
# Start Kafka and Spark services
docker-compose -f docker_kafka.yml -f docker_spark.yml up -d
```

Verify services are running:

```bash
docker ps
```

You should see:
- `kafka-broker` (running on port 19092)
- `kafka-client` (running the producer)
- `spark-master` (Web UI on port 9090)
- `spark-worker` (processing data)

#### 5. Monitor the Pipeline

**Kafka Logs** (Producer Activity):
```bash
docker logs -f kafka-client
```

**Spark Logs** (Processing Activity):
```bash
docker logs -f spark-worker
```

**Spark Web UI** (Cluster Monitoring):
```
http://localhost:9090
```

**Verify S3 Data**:
```bash
aws s3 ls s3://your-bucket-name/user-data/ --recursive
```

**Check Redshift Data**:
```sql
SELECT COUNT(*) FROM user_data;
SELECT * FROM user_data LIMIT 10;
```

## 🔧 Configuration Details

### Kafka Configuration

- **Mode**: KRaft (Kafka Raft) - no Zookeeper required
- **Partitions**: 3 (for parallel processing)
- **Replication Factor**: 1 (single broker setup)
- **Ports**: 
  - Internal: 9092 (container-to-container)
  - External: 19092 (host access)

### Spark Configuration

- **Master**: 1 node (resource manager)
- **Workers**: 1 node (executor)
- **Packages**:
  - `spark-sql-kafka-0-10` (Kafka integration)
  - `hadoop-aws` (S3 access)
  - `aws-java-sdk-bundle` (AWS SDK)

### Lambda Configuration

- **Trigger**: S3 PUT events with `.parquet` suffix
- **Runtime**: Python 3.9+
- **Timeout**: 5 minutes
- **Memory**: 256 MB (adjust based on file size)

## 📈 Dashboard

![Dashboard](readme_pic/E2E_Dashboard.png)

The Looker Studio dashboard provides insights including:
- **User Demographics**: Gender distribution, age groups, nationality breakdown
- **Geographic Analysis**: User distribution by country and state
- **Registration Trends**: New user registrations over time
- **Activity Metrics**: Total users, active users, growth rate

**Note**: Due to AWS free tier limitations, the live dashboard uses exported CSV data from Redshift, but functionality remains identical to a direct connection.

## 🧪 Testing

### Manual Testing

**Test Kafka Producer**:
```bash
# Exec into Kafka container
docker exec -it kafka-broker bash

# List topics
kafka-topics.sh --bootstrap-server localhost:9092 --list

# Consume messages
kafka-console-consumer.sh --bootstrap-server localhost:9092 \
    --topic user_data_topic --from-beginning
```

**Test Spark Processing**:
```bash
# Check Spark logs
docker logs -f spark-worker

# Verify S3 output
aws s3 ls s3://your-bucket-name/user-data/ --recursive
```

**Test Lambda Function**:
```bash
# Manually upload a test Parquet file
aws s3 cp test.parquet s3://your-bucket-name/user-data/

# Check Lambda logs
aws logs tail /aws/lambda/your-function-name --follow
```

## 🐛 Troubleshooting

### Common Issues

**Kafka Connection Failed**
```bash
# Check if Kafka is healthy
docker exec kafka-broker kafka-broker-api-versions.sh \
    --bootstrap-server localhost:9092
```

**Spark Cannot Write to S3**
- Verify AWS credentials are set in Docker environment
- Check IAM permissions for S3 write access
- Ensure bucket exists and path is correct

**Lambda Function Timeout**
- Increase Lambda timeout (default: 3s → 5 minutes)
- Check Redshift cluster availability
- Verify security group allows Lambda → Redshift connection

**No Data in Redshift**
- Check Lambda CloudWatch logs for errors
- Verify IAM role has `redshift:GetCredentials` permission
- Ensure S3 trigger is configured correctly

## 🔐 Security Best Practices

1. **Environment Variables**: Store secrets in `.env`, never in code
2. **IAM Roles**: Use least-privilege principle for AWS permissions
3. **Network Security**: Configure security groups to restrict access
4. **Encryption**: Enable S3 bucket encryption and Redshift encryption at rest
5. **Credentials Rotation**: Regularly rotate database passwords and API keys

## 🚦 Performance Optimization

- **Kafka**: Increase partitions for higher throughput
- **Spark**: Add more worker nodes for parallel processing
- **S3**: Use partitioning strategy (e.g., by date) for faster queries
- **Redshift**: Use SORTKEY and DISTKEY for query optimization
- **Lambda**: Increase memory for faster execution

## 📝 Future Enhancements

- [ ] Add Apache Airflow for pipeline orchestration
- [ ] Implement data quality checks with Great Expectations
- [ ] Add monitoring with Prometheus and Grafana
- [ ] Implement CDC (Change Data Capture) for incremental loads
- [ ] Add authentication layer for API endpoints
- [ ] Implement data deduplication logic
- [ ] Add unit tests and integration tests
- [ ] Create CI/CD pipeline with GitHub Actions

## 📚 Resources & Documentation

### Official Documentation
- [RandomUser API](https://randomuser.me/documentation)
- [Apache Kafka](https://kafka.apache.org/documentation/)
- [Apache Kafka Docker](https://hub.docker.com/r/apache/kafka)
- [Apache Spark](https://spark.apache.org/docs/latest/)
- [Bitnami Spark Docker](https://hub.docker.com/r/bitnami/spark/)
- [AWS S3 Documentation](https://docs.aws.amazon.com/s3/)
- [AWS Lambda Documentation](https://docs.aws.amazon.com/lambda/)
- [AWS Redshift Documentation](https://docs.aws.amazon.com/redshift/)
- [Google Looker Studio](https://support.google.com/looker-studio)

### Tutorials & References
- [Kafka Streams Processing](https://kafka.apache.org/documentation/streams/)
- [Spark Structured Streaming](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html)
- [AWS Lambda Best Practices](https://docs.aws.amazon.com/lambda/latest/dg/best-practices.html)

## 🤝 Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/AmazingFeature`)
3. Commit your changes (`git commit -m 'Add some AmazingFeature'`)
4. Push to the branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request

## 📄 License

This project is open source and available for educational purposes.

## 👤 Author

**Mike Certon**

- GitHub: [@mikecerton](https://github.com/mikecerton)
- Project Link: [E2E-Streaming-Data-Pipeline](https://github.com/mikecerton/E2E-Streaming-Data-Pipeline)

## 🙏 Acknowledgments

- RandomUser API for providing free user data
- Apache Software Foundation for Kafka and Spark
- AWS for cloud infrastructure
- Docker community for containerization tools

---

**⭐ If you find this project helpful, please consider giving it a star!**
