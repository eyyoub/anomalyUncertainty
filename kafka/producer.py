from kafka import KafkaConsumer, KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import NoBrokersAvailable, TopicAlreadyExistsError, UnknownTopicOrPartitionError

import json
from csv import DictReader
import time

def is_kafka_broker_up(bootstrap_servers):
    try:
        # Try to create a temporary Kafka producer with a short timeout
        temp_producer = KafkaProducer(bootstrap_servers=bootstrap_servers, request_timeout_ms=500)
        temp_producer.close()
        return True
    except NoBrokersAvailable:
        return False

def delete_topic(bootstrap_servers, topicname):
    admin_client = KafkaAdminClient(bootstrap_servers=bootstrap_servers)
    try:
        admin_client.delete_topics([topicname])
        print(f"Topic {topicname} deleted.")
    except UnknownTopicOrPartitionError:
        print(f"Topic {topicname} does not exist.")
    except Exception as e:
        print(f"An error occurred while deleting topic {topicname}: {e}")
    finally:
        admin_client.close()

def kafka_producer():
    # Set up for Kafka Producer
    bootstrap_servers = ['localhost:9092']
    topicname = 'thesis-test-topic'

    # Check if Kafka broker is up before attempting to create the producer
    while not is_kafka_broker_up(bootstrap_servers):
        print("Kafka broker is not up. Waiting for 30 seconds...")
        time.sleep(30)

    
    delete_topic(bootstrap_servers, topicname)

    producer = KafkaProducer(bootstrap_servers=bootstrap_servers)

    with open('../datasets/SWaT_Test.csv', 'r') as new_obj:
        csv_dict_reader = DictReader(new_obj)
        index = 0
        count = 0
        for row in csv_dict_reader:
            producer.send(topicname, json.dumps(row).encode('utf-8'))
            count += 1
            print("Data sent successfully", count)

            index += 1
            if (index % 20) == 0:
                time.sleep(5)

kafka_producer()
