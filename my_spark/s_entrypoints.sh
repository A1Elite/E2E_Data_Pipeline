#!/bin/bash
set -e

echo "Installing Python dependencies..."

# Create writable directories for pip
mkdir -p /tmp/pip-cache
mkdir -p /tmp/python-packages

# Install to /tmp/python-packages to avoid permission issues
pip3 install --target=/tmp/python-packages --cache-dir=/tmp/pip-cache --no-warn-script-location -r /opt/spark/requirements.txt

# Set PYTHONPATH to include our installed packages
export PYTHONPATH="/tmp/python-packages:${PYTHONPATH}"
echo "PYTHONPATH set to: ${PYTHONPATH}"

# Verify installation
echo "Verifying python-dotenv installation..."
python3 -c "import dotenv; print('✓ dotenv installed successfully')" || echo "✗ dotenv installation failed"

for i in $(seq 10 -1 1); do
    echo "start in : $i"
    sleep 1
done

echo "_____Ready for run spark_stream_s3.py_____"

# Find the actual Python3 location
PYTHON_PATH=$(which python3)
echo "Python3 found at: ${PYTHON_PATH}"

# Set Python path for Spark to use
export PYSPARK_PYTHON=${PYTHON_PATH}
export PYSPARK_DRIVER_PYTHON=${PYTHON_PATH}

# Create writable Ivy cache directory for JAR dependencies
mkdir -p /tmp/.ivy2
export IVY_CACHE_DIR=/tmp/.ivy2

# Run spark-submit with required packages for Kafka and AWS S3
/opt/spark/bin/spark-submit \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
    --conf "spark.jars.ivy=/tmp/.ivy2" \
    --conf "spark.pyspark.python=${PYTHON_PATH}" \
    --conf "spark.pyspark.driver.python=${PYTHON_PATH}" \
    --conf "spark.executorEnv.PYTHONPATH=/tmp/python-packages" \
    --conf "spark.yarn.appMasterEnv.PYTHONPATH=/tmp/python-packages" \
    /opt/spark/my_spark/spark_stream_s3.py