from config_streaming_processing import kafka_topics
import json
from utils_streaming_processing import create_kafka_consumer, create_kafka_producer


if __name__ == "__main__":
    consumer = create_kafka_consumer()
    producer = create_kafka_producer()

    consumer.subscribe(kafka_topics)

    for message in consumer:
        print(
            f"Received message from topic {message.topic}: {message.value.decode('utf-8')}"
        )
        try:
            data = json.loads(message.value.decode("utf-8"))
            # TODO: Perform your processing on the data here
            # Simple example: mark the data as processed and add the source topic
            data["processed"] = True
            data["source"] = message.topic

            # Then convert the data back to a JSON string
            result = json.dumps(data)
        except json.JSONDecodeError as e:
            print(f"Failed to decode JSON: {e}")

        # Push the result to kafka topic
        producer.send("processed_messages", value=result.encode("utf-8"))
