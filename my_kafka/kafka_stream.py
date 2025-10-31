from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

import os 
from dotenv import load_dotenv
import requests
import json

# ----------------------------
# Function: Fetch data from API
# ----------------------------
def fetch_api_data(api):  
    # Send GET request to the API
    res = requests.get(api)
    res = res.json()
    res = res["results"][0]   
     # Return the data  
    return res

# ----------------------------
# Function: Create the Kafka topic if it doesn't exist
# ----------------------------
def create_kafka_topic(servers, cid, topic_name, max_retries=10, retry_delay=5):
    import time
    
    for attempt in range(max_retries):
        try:
            print(f"Attempting to connect to Kafka (attempt {attempt + 1}/{max_retries})...")
            
            # admin client to talk to Kafka
            admin_client = KafkaAdminClient(
                bootstrap_servers = servers,
                client_id = cid,
                request_timeout_ms = 10000,
                api_version_auto_timeout_ms = 10000
            )

            # Define the topic
            topic = NewTopic(
                name = topic_name,
                num_partitions = 1,
                replication_factor = 1)

            try:
                # create the topic
                admin_client.create_topics(new_topics = [topic], validate_only = False)
                print(f"Topic '{topic_name}' created successfully!")
            except TopicAlreadyExistsError:
                print(f"Topic '{topic_name}' already exists.")
            
            admin_client.close()
            return  # Success - exit function
            
        except Exception as e:
            print(f"Connection attempt {attempt + 1} failed: {e}")
            if attempt < max_retries - 1:
                print(f"Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
            else:
                print(f"Failed to connect to Kafka after {max_retries} attempts")
                raise  # Re-raise the exception after all retries

# ----------------------------
# Function: for loop send API data to Kafka topic
# ----------------------------
def stream_user_data(servers, topic_name, api):
    import time
    
    # Initialize Kafka producer with retry logic
    max_retries = 5
    for attempt in range(max_retries):
        try:
            print(f"Connecting to Kafka producer (attempt {attempt + 1}/{max_retries})...")
            producer = KafkaProducer(
                bootstrap_servers=servers,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                request_timeout_ms=10000,
                max_block_ms=10000)
            print("Kafka producer connected successfully!")
            break
        except Exception as e:
            print(f"Producer connection attempt {attempt + 1} failed: {e}")
            if attempt < max_retries - 1:
                time.sleep(5)
            else:
                raise

    while True:
        try:
            data = fetch_api_data(api)
            print(f"Sending data: {data}") 
            producer.send(topic_name, value=data)
            print("send complete")
            producer.flush()
            time.sleep(1)  # Small delay to avoid flooding
        except Exception as e:
            print(f"Error sending data: {e}")
            time.sleep(5)  # Wait before retrying

if __name__ == "__main__":

    load_dotenv("/.env")

    servers = os.getenv("kafka_servers")
    cid = os.getenv("kafka_cid")
    topic_name = os.getenv("kafka_topic_name")
    api = os.getenv("link_api")

    # Create a Kafka topic 
    create_kafka_topic(servers = servers,cid = cid, topic_name = topic_name)
    # Start streaming data to the topic
    stream_user_data(servers = servers, topic_name = topic_name, api = api)