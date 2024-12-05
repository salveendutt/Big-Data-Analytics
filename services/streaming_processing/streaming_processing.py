from kafka import KafkaConsumer, KafkaProducer
from config_streaming_processing import (
    kafka_address,
    kafka_topics,
    kafka_connection_attempts,
    kafka_connection_attempts_delay,
)
from time import sleep
import json

if __name__ == "__main__":

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
            producer = KafkaProducer(bootstrap_servers=[kafka_address])
            break
        except Exception as e:
            print(f"Attempt {attempts + 1} failed: {e}")
            attempts += 1
            sleep(kafka_connection_attempts_delay)
    else:
        print(f"Failed to connect to Kafka after {kafka_connection_attempts} attempts")
        exit(1)

    consumer.subscribe(kafka_topics)

    for message in consumer:
        # Process each message, perform the classification etc.
        print(
            f"Received message from topic {message.topic}: {message.value.decode('utf-8')}"
        )
        try:
            data = json.loads(message.value.decode("utf-8"))
            # Perform your processing on the data here
            data["processed"] = True
            data["source"] = message.topic
            result = json.dumps(data)  # Convert the processed data back to JSON
        except json.JSONDecodeError as e:
            print(f"Failed to decode JSON: {e}")

        # Push the result to kafka topic

        producer.send("processed_messages", value=result.encode("utf-8"))
