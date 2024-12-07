from time import sleep
from kafka import KafkaConsumer, KafkaProducer
from config_streaming_processing import (
    kafka_address,
    kafka_topics,
    kafka_connection_attempts,
    kafka_connection_attempts_delay,
)


def create_kafka_consumer():
    attempts = 0
    while attempts < kafka_connection_attempts:
        try:
            consumer = KafkaConsumer(
                *kafka_topics,
                bootstrap_servers=[kafka_address],
                auto_offset_reset="earliest",
                enable_auto_commit=True,
            )
            consumer.subscribe(kafka_topics)
            return consumer
        except Exception as e:
            print(f"Attempt {attempts + 1} failed: {e}")
            attempts += 1
            sleep(kafka_connection_attempts_delay)
    else:
        print(f"Failed to connect to Kafka after {kafka_connection_attempts} attempts")
        exit(1)


def create_kafka_producer():
    attempts = 0
    while attempts < kafka_connection_attempts:
        try:
            producer = KafkaProducer(bootstrap_servers=[kafka_address])
            return producer
        except Exception as e:
            print(f"Attempt {attempts + 1} failed: {e}")
            attempts += 1
            sleep(kafka_connection_attempts_delay)
    else:
        print(f"Failed to connect to Kafka after {kafka_connection_attempts} attempts")
        exit(1)
