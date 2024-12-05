from kafka import KafkaConsumer
from config_streaming_processing import kafka_address, kafka_group_name

if __name__ == "__main__":
    topics = ["dataset1", "dataset2", "dataset3"]
    consumer = KafkaConsumer(
        *topics,
        bootstrap_servers=[kafka_address],
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id=kafka_group_name,
    )

    for message in consumer:
        print(
            f"Topic: {message.topic}, Partition: {message.partition}, Offset: {message.offset}, Key: {message.key}, Value: {message.value}"
        )
